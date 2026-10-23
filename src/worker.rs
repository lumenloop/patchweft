use crate::{
    api,
    authority::{self, Choice, Scope},
    engine, file_hash, identity,
    model::*,
    now, source,
    store::{Store, event},
    unique,
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{ErrorKind, Write},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop_signal(_: i32) {
    STOP.store(true, Ordering::SeqCst);
}

pub fn process_start(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, fields) = text.rsplit_once(')')?;
    let values: Vec<_> = fields.split_whitespace().collect();
    if values.first() == Some(&"Z") {
        return None;
    }
    values.get(19).map(|s| s.to_string())
}

pub fn alive(pid: u32, start: &str) -> bool {
    process_start(pid).as_deref() == Some(start)
}

fn descendants(parent: u32) -> Vec<u32> {
    let mut relationships = Vec::new();
    if let Ok(entries) = fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let Some((_, fields)) = stat.rsplit_once(')') else {
                continue;
            };
            let Some(ppid) = fields
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u32>().ok())
            else {
                continue;
            };
            relationships.push((pid, ppid));
        }
    }
    let mut found = vec![parent];
    loop {
        let old = found.len();
        for (pid, ppid) in &relationships {
            if found.contains(ppid) && !found.contains(pid) {
                found.push(*pid);
            }
        }
        if found.len() == old {
            break;
        }
    }
    found.into_iter().filter(|pid| *pid != parent).collect()
}

fn signal_tree(signal: i32) {
    // Restricted to descendants of this dedicated supervisor, including adopted setsid children.
    for pid in descendants(std::process::id()).into_iter().rev() {
        unsafe {
            libc::kill(pid as i32, signal);
        }
    }
}

fn reap_adopted(main: u32) {
    for pid in descendants(std::process::id()) {
        if pid != main {
            unsafe {
                libc::waitpid(pid as i32, std::ptr::null_mut(), libc::WNOHANG);
            }
        }
    }
}

pub fn claim(store: &mut Store) -> Result<Option<Attempt>> {
    let transaction = store.transaction()?;
    let local = authority::load(&transaction, authority::LOCAL)?
        .context("local executor record missing")?;
    let capacity = authority::capacity(&transaction, &local)?;
    // `prepare` verifies the runtime of each claimed profile, so every registered one qualifies.
    let verified: BTreeSet<String> = {
        let mut stmt = transaction.prepare("SELECT id FROM profiles")?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    let selection = authority::select(
        &transaction,
        &local,
        &Scope::all(),
        &verified,
        &BTreeSet::new(),
        capacity,
    )?;
    let Some(Choice {
        owner,
        action,
        reservation,
        timeout_seconds,
    }) = selection.choice
    else {
        return Ok(None);
    };
    // Drain for the next owner's action. Later tiny tasks cannot starve this reservation.
    if !authority::fits(
        authority::held(&transaction, authority::LOCAL)?,
        reservation,
        capacity,
    ) {
        return Ok(None);
    }
    let attempt = Attempt {
        id: unique(),
        action: action.id.clone(),
        worker_pid: std::process::id(),
        worker_start: process_start(std::process::id())
            .context("Linux process identity unavailable")?,
        supervisor_pid: None,
        supervisor_start: None,
        slots: reservation.slots,
        memory_mb: reservation.memory_mb,
        created: now(),
        started: None,
        ended: None,
        exit_code: None,
        result: None,
        timeout_seconds: Some(timeout_seconds),
        executor: None,
        lease: None,
        lease_expires: None,
        claim_key: None,
        completion: None,
        stop: None,
    };
    transaction.execute(
        "INSERT INTO attempts VALUES (?,?,'claimed',0,?)",
        params![attempt.id, action.id, serde_json::to_string(&attempt)?],
    )?;
    transaction.execute(
        "UPDATE actions SET state='running',attempt=? WHERE id=? AND state='queued'",
        params![attempt.id, action.id],
    )?;
    transaction.execute(
        "INSERT INTO dispatches VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET last=excluded.last",
        params![owner, now()],
    )?;
    event(
        &transaction,
        "attempt.claimed",
        &attempt.id,
        &json!({"action":action.id,"owner":owner,"slots":reservation.slots,
                "memory_mb":reservation.memory_mb,"timeout_seconds":timeout_seconds}),
    )?;
    transaction.commit()?;
    Ok(Some(attempt))
}

/// Operator scheduling priority for an owner; 0 (the default) removes it. Priority only orders
/// admission: it never preempts running work or changes action identity.
pub fn set_priority(store: &Store, owner: &str, priority: i64) -> Result<Value> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    store.db.execute_batch(
        "CREATE TABLE IF NOT EXISTS owner_priorities (owner TEXT PRIMARY KEY, priority INTEGER NOT NULL)",
    )?;
    if priority == 0 {
        store
            .db
            .execute("DELETE FROM owner_priorities WHERE owner=?", [owner])?;
    } else {
        store.db.execute(
            "INSERT INTO owner_priorities VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET priority=excluded.priority",
            params![owner, priority],
        )?;
    }
    event(
        &store.db,
        "owner.priority",
        owner,
        &json!({"priority":priority}),
    )?;
    Ok(json!({"owner":owner,"priority":priority}))
}

fn launch(store: &Store, attempt: &Attempt) -> Result<std::process::Child> {
    let directory = store.root.join("attempts").join(&attempt.id);
    fs::create_dir_all(&directory)?;
    let diagnostic = File::create(directory.join("supervisor.log"))?;
    let child = Command::new(std::env::current_exe()?)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .args(["--state"])
        .arg(&store.root)
        .args(["_execute", &attempt.id])
        .stdin(Stdio::null())
        .stdout(diagnostic.try_clone()?)
        .stderr(diagnostic)
        .process_group(0)
        .spawn()?;
    Ok(child)
}

pub fn recover(store: &mut Store, confirmed: Option<&str>) -> Result<Value> {
    let attempts: Vec<(String, Attempt)> = {
        let mut stmt = store
            .db
            .prepare("SELECT state,body FROM attempts WHERE drained=0")?;
        stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .map(|v| {
                let (state, body) = v?;
                Ok((state, serde_json::from_str(&body)?))
            })
            .collect::<Result<_>>()?
    };
    let mut recovered = Vec::new();
    let mut uncertain = Vec::new();
    for (state, attempt) in attempts {
        if state == "interrupted" && confirmed != Some(&attempt.id) {
            uncertain.push(attempt.id);
            continue;
        }
        if state == "claimed" && alive(attempt.worker_pid, &attempt.worker_start) {
            continue;
        }
        if let (Some(pid), Some(start)) =
            (attempt.supervisor_pid, attempt.supervisor_start.as_ref())
            && alive(pid, start)
        {
            ensure!(
                confirmed != Some(&attempt.id),
                "supervisor is still alive; cannot confirm drain"
            );
            continue;
        }
        let transaction = store.transaction()?;
        let current: String = transaction.query_row(
            "SELECT state FROM attempts WHERE id=?",
            [&attempt.id],
            |r| r.get(0),
        )?;
        if current != state {
            continue;
        } // Supervisor registered before this transaction.
        // No execution can start before registration. A surviving late launcher is fenced by state.
        let drained = state == "claimed" || confirmed == Some(&attempt.id);
        transaction.execute(
            "UPDATE attempts SET state='interrupted',drained=? WHERE id=?",
            params![drained, attempt.id],
        )?;
        transaction.execute(
            "UPDATE actions SET state='interrupted' WHERE id=? AND attempt=?",
            params![attempt.action, attempt.id],
        )?;
        event(
            &transaction,
            "attempt.interrupted",
            &attempt.id,
            &json!({"drained":drained,"operator_confirmed":confirmed == Some(&attempt.id)}),
        )?;
        transaction.commit()?;
        if drained {
            recovered.push(attempt.id);
        } else {
            uncertain.push(attempt.id);
        }
    }
    Ok(json!({"interrupted_and_drained":recovered,"needs_drain_confirmation":uncertain}))
}

pub fn run(store: &mut Store, drain: bool) -> Result<Value> {
    recover(store, None)?;
    let mut children: Vec<std::process::Child> = Vec::new();
    let mut launched = 0;
    loop {
        let mut keep = Vec::new();
        for mut child in children {
            if child.try_wait()?.is_none() {
                keep.push(child);
            }
        }
        children = keep;
        recover(store, None)?;
        if let Some(attempt) = claim(store)? {
            match launch(store, &attempt) {
                Ok(child) => {
                    children.push(child);
                    launched += 1;
                }
                Err(error) => {
                    // Launch failed before any supervisor existed.
                    let transaction = store.transaction()?;
                    transaction.execute(
                        "UPDATE attempts SET state='interrupted',drained=1 WHERE id=?",
                        [&attempt.id],
                    )?;
                    transaction.execute(
                        "UPDATE actions SET state='interrupted' WHERE id=? AND attempt=?",
                        params![attempt.action, attempt.id],
                    )?;
                    event(
                        &transaction,
                        "attempt.launch_failed",
                        &attempt.id,
                        &json!({"error":error.to_string()}),
                    )?;
                    transaction.commit()?;
                }
            }
            continue;
        }
        if drain {
            let active: i64 = store.db.query_row(
                "SELECT COUNT(*) FROM attempts WHERE drained=0 AND state IN ('claimed','running')",
                [],
                |r| r.get(0),
            )?;
            if children.is_empty() && active == 0 {
                break;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(json!({"launched":launched}))
}

/// Source and dependencies for the job, from the repository and files the claim names.
fn prepare(claim: &ClaimFile, job: &Job, directory: &Path) -> Result<(PathBuf, Value)> {
    let source_started = Instant::now();
    engine::verify_runtime(&claim.profile).map_err(RuntimeMismatch)?;
    let view = directory.join("work");
    let (repo, commit) = (&claim.repository, &claim.revision.commit);
    if job.reuse == Reuse::Files {
        fs::create_dir(&view)?;
        for input in &job.inputs {
            let path = source::relative_path(input)?;
            let output = source::git_command(repo)
                .args(["show", &format!("{commit}:{input}")])
                .output()?;
            ensure!(output.status.success(), "declared source input unavailable");
            let target = view.join(path);
            fs::create_dir_all(target.parent().context("input parent")?)?;
            fs::write(&target, output.stdout)?;
            let entry = source::git(repo, &["ls-tree", commit, "--", input])?;
            if entry.starts_with("100755 ") {
                fs::set_permissions(target, fs::Permissions::from_mode(0o755))?;
            }
        }
    } else {
        source::checkout_from(repo, commit, &view)?;
    }
    let source_seconds = source_started.elapsed().as_secs_f64();
    let dependencies_started = Instant::now();
    let mut copied_bytes = 0_u64;
    let mut copy_seconds = 0.0;
    let mut restored = BTreeMap::new();
    for dependency in &claim.dependencies {
        let name = &dependency.name;
        let relative = source::relative_path(name)?;
        ensure!(
            !fs::symlink_metadata(&dependency.path)?
                .file_type()
                .is_symlink(),
            "dependency artifact is a symlink"
        );
        let hash = file_hash(&dependency.path)?;
        ensure!(
            dependency.sha256 == hash,
            "dependency artifact is corrupt: {name}"
        );
        if let Some(old) = restored.get(name) {
            ensure!(old == &hash, "dependency artifacts conflict: {name}");
            continue;
        }
        let target = view.join(&relative);
        ensure!(
            matches!(fs::symlink_metadata(&target),Err(error) if error.kind()==ErrorKind::NotFound),
            "dependency artifact would overwrite source: {name}"
        );
        contained_parent(&view, &target)?;
        let copy_started = Instant::now();
        copied_bytes += fs::copy(&dependency.path, &target)?;
        fs::set_permissions(&target, fs::Permissions::from_mode(dependency.mode))?;
        copy_seconds += copy_started.elapsed().as_secs_f64();
        restored.insert(name.clone(), hash);
    }
    Ok((
        view,
        json!({
            "source_seconds": source_seconds,
            "dependency_seconds": dependencies_started.elapsed().as_secs_f64(),
            "dependency_copy_seconds": copy_seconds,
            "dependency_copy_bytes": copied_bytes,
            "dependency_files": restored.len(),
            "transfer_seconds": claim.transfer["seconds"].as_f64().unwrap_or(0.0),
            "transfer_bytes": claim.transfer["bytes"].as_u64().unwrap_or(0)
        }),
    ))
}

/// How a running job hears of stops decided elsewhere and reports the stops its supervisor
/// decides: `_execute` through the store, `_supervise` through its attempt's control files.
trait Control {
    fn command_started(&mut self) -> Result<()>;
    /// A stop decided outside the supervisor, once one is recorded.
    fn requested(&mut self) -> Result<Option<String>>;
    /// Reports a stop the supervisor decided itself: `supervisor-stop` or `timeout`.
    fn stopping(&mut self, reason: &str) -> Result<()>;
}

/// `_execute`: the supervisor records stops in the store itself.
struct StoreControl<'a> {
    db: &'a Connection,
    action: &'a str,
    attempt: &'a str,
}

impl StoreControl<'_> {
    fn active(&self, db: &Connection) -> Result<i64> {
        Ok(db.query_row(
            "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
            [self.action],
            |r| r.get(0),
        )?)
    }
    fn record(&self, db: &Connection, reason: &str) -> Result<()> {
        db.execute(
            "UPDATE actions SET state='stopping' WHERE id=? AND attempt=?",
            params![self.action, self.attempt],
        )?;
        event(
            db,
            "attempt.stopping",
            self.attempt,
            &json!({"reason":reason}),
        )
    }
}

impl Control for StoreControl<'_> {
    fn command_started(&mut self) -> Result<()> {
        Ok(())
    }
    fn requested(&mut self) -> Result<Option<String>> {
        if self.active(self.db)? != 0 {
            return Ok(None);
        }
        let transaction = rusqlite::Transaction::new_unchecked(
            self.db,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        // A requester may have joined since.
        let stopped = self.active(&transaction)? == 0;
        if stopped {
            self.record(&transaction, "no-requesters")?;
        }
        transaction.commit()?;
        Ok(stopped.then(|| "no-requesters".to_owned()))
    }
    fn stopping(&mut self, reason: &str) -> Result<()> {
        let transaction = rusqlite::Transaction::new_unchecked(
            self.db,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        self.record(&transaction, reason)?;
        Ok(transaction.commit()?)
    }
}

/// `_supervise`: the coordinator relays stops both ways through `control/`.
struct FileControl<'a> {
    control: &'a Path,
}

impl Control for FileControl<'_> {
    fn command_started(&mut self) -> Result<()> {
        write_atomically(&self.control.join("command-started"), b"")
    }
    fn requested(&mut self) -> Result<Option<String>> {
        stop_reason(self.control)
    }
    fn stopping(&mut self, reason: &str) -> Result<()> {
        write_atomically(
            &self.control.join("stopping"),
            json!({"reason":reason}).to_string().as_bytes(),
        )
    }
}

/// Readers of a control file never see it half written.
fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().context("control directory")?)?;
    temp.write_all(bytes)?;
    temp.persist(path)?;
    Ok(())
}

/// The reason in `control/stop`, once the coordinator wrote one.
fn stop_reason(control: &Path) -> Result<Option<String>> {
    let bytes = match fs::read(control.join("stop")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let stop: Value = serde_json::from_slice(&bytes)?;
    Ok(Some(
        stop["reason"]
            .as_str()
            .context("stop file lacks a reason")?
            .to_owned(),
    ))
}

/// Tool drift on this host: nothing of the job ran and nothing about the revision failed.
#[derive(Debug)]
struct RuntimeMismatch(anyhow::Error);

impl std::fmt::Display for RuntimeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}
impl std::error::Error for RuntimeMismatch {}

/// Output a result may carry, per file (artifacts and logs) and per attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Limits {
    file: u64,
    attempt: u64,
}

/// Every host enforces the same limits, so a result recorded on one is deliverable from all.
const LIMITS: Limits = Limits {
    file: api::MAX_ARTIFACT_BYTES,
    attempt: api::MAX_ATTEMPT_OUTPUT_BYTES,
};

/// The attempt's logs so far, each within the per-file limit; returns their total size.
fn logs_within(directory: &Path, limits: Limits) -> Result<u64> {
    let mut total = 0;
    for log in ["output.log", "cleanup.log"] {
        let bytes = match fs::metadata(directory.join(log)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        ensure!(bytes <= limits.file, "{log} exceeds {} bytes", limits.file);
        total += bytes;
    }
    Ok(total)
}

/// Rechecked after cleanup, whose log counts toward the attempt's output.
fn outputs_within(directory: &Path, evidence: &Value, limits: Limits) -> Result<()> {
    let artifacts: u64 = evidence["artifacts"]
        .as_object()
        .into_iter()
        .flat_map(|artifacts| artifacts.values())
        .filter_map(|artifact| artifact["bytes"].as_u64())
        .sum();
    ensure!(
        logs_within(directory, limits)? + artifacts <= limits.attempt,
        "attempt outputs exceed {} bytes",
        limits.attempt
    );
    Ok(())
}

fn execute_command(
    claim: &ClaimFile,
    job: &Job,
    directory: &Path,
    control: &mut dyn Control,
) -> Result<(i32, Value)> {
    let (action, profile) = (&claim.action, &claim.profile);
    let started = Instant::now();
    if let Some(identity) = &action.identity {
        let current = engine::recipes_for(profile, &identity.version)
            .ok()
            .and_then(|r| r.get(&job.name).cloned());
        ensure!(
            current.as_ref() == Some(&identity.recipe),
            "job identity changed since request"
        );
    }
    let timeout_seconds = claim.attempt.timeout_seconds;
    let (view, preparation) = prepare(claim, job, directory)?;
    let scratch = directory.join("scratch");
    let home = directory.join("home");
    fs::create_dir(&scratch)?;
    fs::create_dir(&home)?;
    if ["vitest", "junit"].contains(&job.report.kind.as_str()) {
        let report = view.join(source::relative_path(&job.report.path)?);
        contained_parent(&view, &report)?;
        if fs::symlink_metadata(&report).is_ok() {
            fs::remove_file(&report)?;
        }
    }
    let log = File::create(directory.join("output.log"))?;
    let paths: Vec<PathBuf> = profile
        .definition
        .tools
        .values()
        .filter_map(|p| p.parent().map(Path::to_path_buf))
        .chain([PathBuf::from("/usr/bin"), PathBuf::from("/bin")])
        .collect();
    let path = std::env::join_paths(paths)?;
    let mut command = Command::new(&profile.definition.tools[&job.command[0]]);
    command
        .args(&job.command[1..])
        .current_dir(&view)
        .env_clear()
        .env("PATH", path)
        .env("HOME", &home)
        .env("TMPDIR", &scratch)
        .env("LANG", "C.UTF-8")
        .env("LC_ALL", "C.UTF-8")
        .env("TZ", "UTC")
        .env("CI", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .envs(&profile.definition.environment)
        .envs(&job.environment)
        .env("PATCHWEFT_STATE", &claim.root)
        .env("PATCHWEFT_ATTEMPT", &claim.attempt.id)
        .env("PATCHWEFT_REVISION", &action.revision)
        .env("PATCHWEFT_REPORT", &job.report.path)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    if job.export_slots {
        command.env("PATCHWEFT_SLOTS", job.slots.to_string());
    }
    let command_started = Instant::now();
    let timer_seconds;
    control.command_started()?;
    let mut child = command.spawn()?;
    let mut status = None;
    let mut terminating: Option<(Instant, String)> = None;
    loop {
        if terminating.is_none() {
            // A stop recorded elsewhere wins over this supervisor's own timeout.
            let reason = if STOP.load(Ordering::SeqCst) {
                control.stopping("supervisor-stop")?;
                Some("supervisor-stop".to_owned())
            } else if let Some(reason) = control.requested()? {
                Some(reason)
            } else if started.elapsed().as_secs() >= timeout_seconds {
                control.stopping("timeout")?;
                Some("timeout".to_owned())
            } else {
                None
            };
            terminating = reason.map(|reason| (Instant::now(), reason));
        }
        if let Some((time, _)) = &terminating {
            signal_tree(if time.elapsed().as_secs() < 3 {
                libc::SIGTERM
            } else {
                libc::SIGKILL
            });
        }
        if status.is_none() {
            status = child.try_wait()?;
        }
        reap_adopted(child.id());
        if status.is_some() && descendants(std::process::id()).is_empty() {
            // Descendants keep the timer running, so this bounds every timeout check above.
            timer_seconds = started.elapsed().as_secs_f64();
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let command_seconds = command_started.elapsed().as_secs_f64();
    let mut code = status
        .context("child result missing")?
        .code()
        .unwrap_or(128);
    let reason = terminating.map(|(_, reason)| reason);
    if reason.is_some() {
        code = 124;
    }
    logs_within(directory, LIMITS)?;
    let report = if code == 0 {
        validate_report(job, &view, &directory.join("output.log"))?
    } else {
        json!({"execution_failed":true})
    };
    let capture_started = Instant::now();
    let artifacts = if code == 0 {
        capture_artifacts(job, &view, directory, LIMITS)?
    } else {
        BTreeMap::new()
    };
    Ok((
        code,
        json!({"exit_code":code,"reason":reason,"elapsed_seconds":started.elapsed().as_secs_f64(),
                    "command_seconds":command_seconds,"preparation":preparation,
                    "artifact_capture_seconds":capture_started.elapsed().as_secs_f64(),
                    "report":report,"log_sha256":file_hash(&directory.join("output.log"))?,
                    "artifacts":artifacts,"revision":action.revision,"profile":profile.id,"job":job.name,
                    "identity":evidence_identity(action),"timer_seconds":timer_seconds,"host":host(),
                    "executor":claim.executor,
                    "budget":{"slots":claim.attempt.slots,"memory_mb":claim.attempt.memory_mb,
                              "timeout_seconds":timeout_seconds}}),
    ))
}

pub(crate) fn evidence_identity(action: &Action) -> Value {
    action.identity.as_ref().map_or(
        Value::Null,
        |i| json!({"version":i.version,"recipe":i.recipe,"class":i.class,"key":action.key}),
    )
}

fn host() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|name| name.trim().to_owned())
}

fn contained_parent(view: &Path, target: &Path) -> Result<()> {
    let parent = target.parent().context("output parent")?;
    let mut existing = parent;
    loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing.parent().context("output ancestor")?
            }
            Err(error) => return Err(error.into()),
        }
    }
    ensure!(
        existing.canonicalize()?.starts_with(view.canonicalize()?),
        "output path escapes workspace"
    );
    fs::create_dir_all(parent)?;
    ensure!(
        parent.canonicalize()?.starts_with(view.canonicalize()?),
        "output path escapes workspace"
    );
    Ok(())
}

/// Makes this process the dedicated supervisor of one job: it adopts the job's descendants,
/// turns SIGTERM and SIGINT into a stop and yields the CPU to coordinators.
fn dedicate() -> Result<()> {
    STOP.store(false, Ordering::SeqCst);
    unsafe {
        // The dedicated process owns all adopted descendants; shared coordinators are never reapers.
        ensure!(
            cfg!(target_os = "linux"),
            "supervised execution requires Linux child-subreaper support"
        );
        #[cfg(target_os = "linux")]
        ensure!(
            libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0,
            "subreaper unavailable"
        );
        libc::signal(
            libc::SIGTERM,
            stop_signal as *const () as libc::sighandler_t,
        );
        libc::signal(libc::SIGINT, stop_signal as *const () as libc::sighandler_t);
        libc::nice(10);
    }
    Ok(())
}

/// Evidence of an attempt whose command produced no result.
fn failure(claim: &ClaimFile, job: &Job, error: Option<String>) -> Value {
    let action = &claim.action;
    json!({"error":error,"revision":action.revision,"profile":claim.profile.id,"job":job.name,
           "identity":evidence_identity(action),"key":action.key,"executor":claim.executor})
}

fn not_started(
    claim: &ClaimFile,
    job: &Job,
    reason: &str,
    error: Option<String>,
) -> SupervisorResult {
    SupervisorResult {
        version: CONTROL_VERSION,
        outcome: Outcome::NotStarted,
        reason: Some(reason.into()),
        exit_code: 125,
        evidence: failure(claim, job, error),
        drained: true,
    }
}

/// Ends a registered attempt. Even after an error nothing is reported before the job's
/// processes are gone and its external cleanup ran.
fn conclude(
    claim: &ClaimFile,
    job: &Job,
    directory: &Path,
    result: Result<(i32, Value)>,
) -> SupervisorResult {
    let deadline = Instant::now();
    while !descendants(std::process::id()).is_empty() {
        signal_tree(if deadline.elapsed().as_secs() < 3 {
            libc::SIGTERM
        } else {
            libc::SIGKILL
        });
        reap_adopted(0);
        thread::sleep(Duration::from_millis(50));
    }
    let (outcome, reason, mut code, mut evidence) = match result {
        Ok((code, evidence)) => {
            let reason = evidence["reason"].as_str().map(String::from);
            (Outcome::Completed, reason, code, evidence)
        }
        Err(error) if error.is::<RuntimeMismatch>() => (
            Outcome::NotStarted,
            Some("runtime-mismatch".into()),
            125,
            failure(claim, job, Some(format!("{error:#}"))),
        ),
        Err(error) => (
            Outcome::Completed,
            None,
            125,
            failure(claim, job, Some(format!("{error:#}"))),
        ),
    };
    let cleanup = cleanup_external(claim, job, directory);
    let drained = cleanup.is_ok();
    if let Err(error) = cleanup {
        code = 125;
        evidence["cleanup_error"] = json!(format!("{error:#}"));
    }
    if let Err(error) = outputs_within(directory, &evidence, LIMITS) {
        code = 125;
        if evidence["error"].is_null() {
            evidence["error"] = json!(format!("{error:#}"));
        }
    }
    SupervisorResult {
        version: CONTROL_VERSION,
        outcome,
        reason,
        exit_code: code,
        evidence,
        drained,
    }
}

pub fn execute(store: &mut Store, attempt_id: &str) -> Result<Value> {
    dedicate()?;
    let mut attempt = store.attempt(attempt_id)?;
    let action = store.action(&attempt.action)?;
    let profile = store.profile(&action.profile)?;
    let job = engine::job(&profile, &action.job)?.clone();
    let revision = store.revision(&action.revision)?;
    let transaction = store.transaction()?;
    let state: String =
        transaction.query_row("SELECT state FROM attempts WHERE id=?", [attempt_id], |r| {
            r.get(0)
        })?;
    ensure!(
        state == "claimed",
        "attempt is already registered or was fenced"
    );
    attempt.supervisor_pid = Some(std::process::id());
    attempt.supervisor_start = process_start(std::process::id());
    attempt.started = Some(now());
    transaction.execute(
        "UPDATE attempts SET state='running',body=? WHERE id=? AND state='claimed'",
        params![serde_json::to_string(&attempt)?, attempt_id],
    )?;
    event(
        &transaction,
        "attempt.started",
        attempt_id,
        &json!({"pid":std::process::id()}),
    )?;
    transaction.commit()?;
    let directory = store.root.join("attempts").join(attempt_id);
    fs::create_dir_all(&directory)?;
    let mut claim = local_claim(store, &attempt, action, profile, revision, &job);
    let result = local_dependencies(store, &claim.action).and_then(|dependencies| {
        claim.dependencies = dependencies;
        let mut control = StoreControl {
            db: &store.db,
            action: &claim.action.id,
            attempt: attempt_id,
        };
        execute_command(&claim, &job, &directory, &mut control)
    });
    let result = conclude(&claim, &job, &directory, result);
    // This worker keeps no refused profiles and would claim a drifted one again at once, so
    // here a runtime mismatch still fails the attempt.
    let request = CompleteRequest {
        attempt: attempt_id.into(),
        lease: claim.attempt.lease,
        outcome: Outcome::Completed,
        reason: result.evidence["reason"].as_str().map(String::from),
        exit_code: result.exit_code,
        evidence: result.evidence,
        drained: result.drained,
        log: None,
        cleanup_log: None,
        artifacts: BTreeMap::new(),
    };
    let transaction = store.transaction()?;
    let state = authority::record_result(&transaction, &request, &identity(&request)?)?;
    transaction.commit()?;
    Ok(json!({"attempt":attempt_id,"state":state}))
}

/// The claim `_execute` runs: the store's own repository and attempt directories.
fn local_claim(
    store: &Store,
    attempt: &Attempt,
    action: Action,
    profile: RegisteredProfile,
    revision: Revision,
    job: &Job,
) -> ClaimFile {
    ClaimFile {
        version: CONTROL_VERSION,
        executor: attempt
            .executor
            .clone()
            .unwrap_or_else(|| authority::LOCAL.into()),
        root: store.root.clone(),
        attempt: Grant {
            id: attempt.id.clone(),
            lease: attempt.lease.clone().unwrap_or_default(),
            lease_expires: attempt.lease_expires.unwrap_or_default(),
            slots: attempt.slots,
            memory_mb: attempt.memory_mb,
            timeout_seconds: attempt.timeout_seconds.unwrap_or(job.timeout_seconds),
        },
        action,
        profile,
        repository: source::repository(store, &revision.project),
        revision: RevisionRef {
            id: revision.id,
            project: revision.project,
            commit: revision.commit,
        },
        dependencies: Vec::new(),
        transfer: json!({"seconds":0.0,"bytes":0}),
    }
}

/// Dependency artifacts in their producers' attempt directories.
fn local_dependencies(store: &Store, action: &Action) -> Result<Vec<DependencyFile>> {
    let mut files = Vec::new();
    for dependency in &action.dependencies {
        ensure!(
            engine::action_state(&store.db, dependency)? == "passed",
            "dependency no longer passes"
        );
        let attempt_id: String = store.db.query_row(
            "SELECT attempt FROM actions WHERE id=?",
            [dependency],
            |r| r.get(0),
        )?;
        let receipt = store
            .attempt(&attempt_id)?
            .result
            .context("dependency lacks evidence")?;
        let artifacts = receipt["artifacts"]
            .as_object()
            .context("dependency lacks artifacts")?;
        for (name, expected) in artifacts {
            files.push(DependencyFile {
                action: dependency.clone(),
                attempt: attempt_id.clone(),
                name: name.clone(),
                // A missing digest is refused as corrupt when the file is checked.
                sha256: expected["sha256"].as_str().unwrap_or_default().into(),
                mode: expected["mode"].as_u64().context("artifact mode")? as u32,
                bytes: expected["bytes"].as_u64().unwrap_or_default(),
                path: store
                    .root
                    .join("attempts")
                    .join(&attempt_id)
                    .join("artifacts")
                    .join(source::relative_path(name)?),
            });
        }
    }
    Ok(files)
}

/// The coordinator gave up on a supervisor it never registered.
const REGISTRATION_SECONDS: u64 = 3600;

/// `_supervise ROOT ATTEMPT`: runs one claimed attempt from its control files alone. It never
/// opens the store and holds no credentials; its coordinator relays registration and stops.
pub fn supervise(root: &Path, attempt: &str) -> Result<Value> {
    dedicate()?;
    let root = root.canonicalize()?;
    let directory = root.join("attempts").join(attempt);
    let control = directory.join("control");
    let mut claim: ClaimFile = serde_json::from_slice(&fs::read(control.join("claim.json"))?)?;
    ensure!(
        claim.version == CONTROL_VERSION,
        "unsupported claim version {}",
        claim.version
    );
    ensure!(
        claim.attempt.id == attempt && claim.root.canonicalize()? == root,
        "claim belongs to another attempt"
    );
    // Jobs find their attempt directory as `$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT`.
    claim.root = root.clone();
    let job = engine::job(&claim.profile, &claim.action.job)?;
    let result = match registration(&claim, job, &control)? {
        Some(result) => result,
        None => {
            let result = execute_command(
                &claim,
                job,
                &directory,
                &mut FileControl { control: &control },
            );
            conclude(&claim, job, &directory, result)
        }
    };
    write_atomically(&control.join("result.json"), &serde_json::to_vec(&result)?)?;
    Ok(json!({"attempt":attempt,"outcome":result.outcome,"exit_code":result.exit_code}))
}

/// Waits until the coordinator reports the attempt registered: nothing of the job runs before
/// the store recorded it running. A stop instead yields a not-started result.
fn registration(claim: &ClaimFile, job: &Job, control: &Path) -> Result<Option<SupervisorResult>> {
    let started = Instant::now();
    loop {
        if let Some(reason) = stop_reason(control)? {
            return Ok(Some(not_started(claim, job, &reason, None)));
        }
        if STOP.load(Ordering::SeqCst) {
            return Ok(Some(not_started(claim, job, "supervisor-stop", None)));
        }
        if control.join("registered").exists() {
            return Ok(None);
        }
        if started.elapsed().as_secs() >= REGISTRATION_SECONDS {
            let error = format!("not registered within {REGISTRATION_SECONDS} s");
            return Ok(Some(not_started(
                claim,
                job,
                "registration-timeout",
                Some(error),
            )));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn cleanup_external(claim: &ClaimFile, job: &Job, directory: &Path) -> Result<()> {
    let profile = &claim.profile;
    if job.cleanup.is_empty() {
        return Ok(());
    }
    let log = File::create(directory.join("cleanup.log"))?;
    let paths: Vec<PathBuf> = profile
        .definition
        .tools
        .values()
        .filter_map(|p| p.parent().map(Path::to_path_buf))
        .chain([PathBuf::from("/usr/bin"), PathBuf::from("/bin")])
        .collect();
    let work = directory.join("work");
    let mut child = Command::new(&profile.definition.tools[&job.cleanup[0]])
        .args(&job.cleanup[1..])
        .env_clear()
        .env("PATH", std::env::join_paths(paths)?)
        .envs(&profile.definition.environment)
        .envs(&job.environment)
        .env("PATCHWEFT_STATE", &claim.root)
        .env("PATCHWEFT_ATTEMPT", &claim.attempt.id)
        .env("PATCHWEFT_WORKSPACE", &work)
        .env("TMPDIR", directory.join("scratch"))
        .env("HOME", directory.join("home"))
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()?;
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if start.elapsed().as_secs() >= 60 {
            signal_tree(libc::SIGKILL);
            child.wait()?;
            bail!("external cleanup timed out; capacity remains held");
        }
        thread::sleep(Duration::from_millis(100));
    };
    while !descendants(std::process::id()).is_empty() {
        signal_tree(libc::SIGKILL);
        reap_adopted(child.id());
        thread::sleep(Duration::from_millis(50));
    }
    ensure!(
        status.success(),
        "external cleanup failed; capacity remains held"
    );
    Ok(())
}

pub fn validate_report(job: &Job, view: &Path, log: &Path) -> Result<Value> {
    if ["vitest", "junit"].contains(&job.report.kind.as_str()) {
        let report = view.join(source::relative_path(&job.report.path)?);
        ensure!(
            fs::symlink_metadata(&report)?.is_file()
                && report.canonicalize()?.starts_with(view.canonicalize()?),
            "report must be a fresh regular file inside the workspace"
        );
    }
    let (passed, failed, skipped) = match job.report.kind.as_str() {
        "command" => return Ok(json!({"contract":"command-exit-zero"})),
        "node-tap" => {
            let text = fs::read_to_string(log)?;
            let count = |name: &str| -> Result<u64> {
                text.lines()
                    .rev()
                    .find_map(|line| line.strip_prefix(&format!("# {name} ")))
                    .context(format!("TAP missing {name}"))?
                    .parse()
                    .context("invalid TAP count")
            };
            let passed = count("pass")?;
            let failed = count("fail")? + count("cancelled")?;
            let skipped = count("skipped")? + count("todo")?;
            ensure!(
                count("tests")? == passed + failed + skipped,
                "TAP counts do not reconcile"
            );
            (passed, failed, skipped)
        }
        "vitest" => {
            let report: Value = serde_json::from_slice(&fs::read(view.join(&job.report.path))?)?;
            let cases: Vec<&Value> = report["testResults"]
                .as_array()
                .context("missing Vitest results")?
                .iter()
                .flat_map(|v| v["assertionResults"].as_array().into_iter().flatten())
                .collect();
            let passed = cases.iter().filter(|v| v["status"] == "passed").count() as u64;
            let skipped = cases
                .iter()
                .filter(|v| {
                    ["pending", "todo", "skipped"]
                        .iter()
                        .any(|s| v["status"] == *s)
                })
                .count() as u64;
            let failed = cases.len() as u64 - passed - skipped;
            ensure!(
                report["success"] == true
                    && report["numFailedTestSuites"].as_u64() == Some(0)
                    && report["numFailedTests"].as_u64() == Some(0),
                "Vitest reported failure"
            );
            ensure!(
                report["numTotalTests"].as_u64() == Some(cases.len() as u64),
                "Vitest case counts do not reconcile"
            );
            (passed, failed, skipped)
        }
        "junit" => {
            use quick_xml::{Reader, events::Event};
            let data = fs::read(view.join(&job.report.path))?;
            let mut reader = Reader::from_reader(data.as_slice());
            let (mut total, mut failed, mut skipped) = (0, 0, 0);
            loop {
                match reader.read_event()? {
                    Event::Start(e) | Event::Empty(e) => match e.name().as_ref() {
                        b"testcase" => total += 1,
                        b"failure" | b"error" => failed += 1,
                        b"skipped" => skipped += 1,
                        _ => {}
                    },
                    Event::Eof => break,
                    _ => {}
                }
            }
            ensure!(total >= failed + skipped, "invalid JUnit case counts");
            (total - failed - skipped, failed, skipped)
        }
        _ => bail!("unknown report contract"),
    };
    ensure!(
        failed == 0 && passed >= job.report.minimum && (job.report.allow_skips || skipped == 0),
        "execution contract not met: passed={passed}, failed={failed}, skipped={skipped}, minimum={}",
        job.report.minimum
    );
    Ok(json!({"passed":passed,"failed":failed,"skipped":skipped,"minimum":job.report.minimum}))
}

fn copy_files(
    source: &Path,
    destination: &Path,
    relative: &Path,
    manifest: &mut BTreeMap<String, Value>,
    limits: Limits,
    total: &mut u64,
) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "artifact symlinks are not accepted"
    );
    if metadata.is_dir() {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let path = relative.join(entry.file_name());
            ensure!(!source::protected(&path), "protected artifact path");
            copy_files(
                &entry.path(),
                &destination.join(entry.file_name()),
                &path,
                manifest,
                limits,
                total,
            )?;
        }
    } else {
        ensure!(metadata.is_file(), "artifact is not a regular file");
        ensure!(
            metadata.len() <= limits.file,
            "artifact {} exceeds {} bytes",
            relative.display(),
            limits.file
        );
        *total += metadata.len();
        ensure!(
            *total <= limits.attempt,
            "attempt outputs exceed {} bytes",
            limits.attempt
        );
        fs::create_dir_all(destination.parent().context("artifact parent")?)?;
        fs::copy(source, destination)?;
        let mode = metadata.permissions().mode() & 0o777;
        fs::set_permissions(destination, fs::Permissions::from_mode(mode & !0o222))?;
        manifest.insert(
            relative.to_str().context("non-UTF8 artifact name")?.into(),
            json!({"sha256":file_hash(destination)?,"mode":mode,"bytes":metadata.len()}),
        );
    }
    Ok(())
}

/// Artifacts count toward the attempt's output together with its log.
fn capture_artifacts(
    job: &Job,
    view: &Path,
    directory: &Path,
    limits: Limits,
) -> Result<BTreeMap<String, Value>> {
    let mut total = logs_within(directory, limits)?;
    let output = directory.join("artifacts");
    fs::create_dir(&output)?;
    let mut manifest = BTreeMap::new();
    for path in &job.artifacts {
        let relative = source::relative_path(path)?;
        let target = view.join(&relative);
        ensure!(
            target.canonicalize()?.starts_with(view.canonicalize()?),
            "artifact escapes job workspace"
        );
        copy_files(
            &target,
            &output.join(&relative),
            &relative,
            &mut manifest,
            limits,
            &mut total,
        )?;
    }
    fs::write(
        directory.join("artifacts.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

pub fn deliver(store: &Store, action_id: &str, destination: &Path) -> Result<Value> {
    ensure!(
        engine::action_state(&store.db, action_id)? == "passed",
        "action has no passing result"
    );
    ensure!(
        !destination.exists(),
        "delivery requires a new owned directory; author files are never overwritten"
    );
    let attempt_id: String =
        store
            .db
            .query_row("SELECT attempt FROM actions WHERE id=?", [action_id], |r| {
                r.get(0)
            })?;
    let attempt = store.attempt(&attempt_id)?;
    let evidence = attempt.result.context("missing execution evidence")?;
    let artifacts = evidence["artifacts"]
        .as_object()
        .context("missing artifact manifest")?;
    let parent = destination
        .parent()
        .context("delivery needs a parent directory")?;
    fs::create_dir_all(parent)?;
    let temp = tempfile::tempdir_in(parent)?;
    for (name, expected) in artifacts {
        let relative = source::relative_path(name)?;
        let source = store
            .root
            .join("attempts")
            .join(&attempt_id)
            .join("artifacts")
            .join(&relative);
        ensure!(
            file_hash(&source)?
                == expected["sha256"]
                    .as_str()
                    .context("missing artifact hash")?,
            "retained artifact is corrupt: {name}"
        );
        let target = temp.path().join(relative);
        fs::create_dir_all(target.parent().context("delivery parent")?)?;
        fs::copy(&source, &target)?;
        fs::set_permissions(
            &target,
            fs::Permissions::from_mode(expected["mode"].as_u64().context("artifact mode")? as u32),
        )?;
    }
    fs::write(
        temp.path().join(".patchweft-receipt.json"),
        serde_json::to_vec_pretty(
            &json!({"action":action_id,"attempt":attempt_id,"evidence":evidence}),
        )?,
    )?;
    publish_directory(temp.path(), destination)?;
    event(
        &store.db,
        "artifact.delivered",
        action_id,
        &json!({"attempt":attempt_id,"destination":destination}),
    )?;
    Ok(json!({"action":action_id,"attempt":attempt_id,"destination":destination}))
}

pub fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes())?;
    let destination = CString::new(destination.as_os_str().as_bytes())?;
    #[cfg(target_os = "linux")]
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    // Remote clients deliver on macOS; RENAME_EXCL has the same no-overwrite semantics.
    #[cfg(target_os = "macos")]
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(value: Value) -> Job {
        serde_json::from_value(value).unwrap()
    }

    /// An attempt directory whose job wrote `output.log` (4 bytes), `dist/a` (8) and `dist/b` (12).
    fn finished() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        let view = temp.path().join("work");
        fs::create_dir_all(view.join("dist")).unwrap();
        fs::write(temp.path().join("output.log"), "log\n").unwrap();
        fs::write(view.join("dist/a"), "12345678").unwrap();
        fs::write(view.join("dist/b"), "123456789012").unwrap();
        temp
    }

    #[test]
    fn capture_enforces_artifact_and_attempt_limits() {
        let build = job(json!({"name":"build","command":["sh"],"artifacts":["dist"]}));
        let capture = |file, attempt| {
            let temp = finished();
            capture_artifacts(
                &build,
                &temp.path().join("work"),
                temp.path(),
                Limits { file, attempt },
            )
            .map_err(|error| format!("{error:#}"))
        };
        let error = capture(11, 100).unwrap_err();
        assert!(error.contains("dist/b exceeds 11 bytes"), "{error}");
        let error = capture(3, 100).unwrap_err();
        assert!(error.contains("output.log exceeds 3 bytes"), "{error}");
        // Artifacts and the log together: 24 bytes.
        let error = capture(12, 23).unwrap_err();
        assert!(error.contains("outputs exceed 23 bytes"), "{error}");
        let manifest = capture(12, 24).unwrap();
        assert_eq!(
            (&manifest["dist/a"]["bytes"], &manifest["dist/b"]["bytes"]),
            (&json!(8), &json!(12))
        );

        // The cleanup log is counted once cleanup ran.
        let temp = finished();
        let limits = Limits {
            file: 12,
            attempt: 24,
        };
        let artifacts =
            capture_artifacts(&build, &temp.path().join("work"), temp.path(), limits).unwrap();
        let evidence = json!({"artifacts":artifacts});
        outputs_within(temp.path(), &evidence, limits).unwrap();
        fs::write(temp.path().join("cleanup.log"), "x").unwrap();
        let error = format!(
            "{:#}",
            outputs_within(temp.path(), &evidence, limits).unwrap_err()
        );
        assert!(error.contains("outputs exceed 24 bytes"), "{error}");
        fs::write(temp.path().join("cleanup.log"), "1234567890123").unwrap();
        let error = format!(
            "{:#}",
            outputs_within(temp.path(), &evidence, limits).unwrap_err()
        );
        assert!(error.contains("cleanup.log exceeds 12 bytes"), "{error}");

        // Every host passes the same constants, so a result is deliverable from every host.
        assert_eq!(
            LIMITS,
            Limits {
                file: api::MAX_ARTIFACT_BYTES,
                attempt: api::MAX_ATTEMPT_OUTPUT_BYTES
            }
        );
    }

    #[test]
    fn files_only_job_reads_the_repository_named_in_the_claim() {
        let temp = tempfile::tempdir().unwrap();
        // The claim's repository lies outside the root, which has no repos/ at all.
        let repository = temp.path().join("mirror");
        fs::create_dir(&repository).unwrap();
        let git = |args: &[&str]| source::git(&repository, args).unwrap();
        git(&["init", "--quiet"]);
        fs::write(repository.join("input.txt"), "from the claim\n").unwrap();
        fs::write(repository.join("tool.sh"), "#!/bin/sh\n").unwrap();
        fs::set_permissions(
            repository.join("tool.sh"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::write(repository.join("unlisted.txt"), "not an input\n").unwrap();
        git(&["add", "."]);
        git(&[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@local.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ]);
        let commit = git(&["rev-parse", "HEAD"]);
        let root = temp.path().join("root");
        let definition: Profile = serde_json::from_value(json!({
            "version":1,"name":"fixture","tools":{},"jobs":[
                {"name":"lint","command":["sh"],"reuse":"files","inputs":["input.txt","tool.sh"]},
                {"name":"full","command":["sh"]}]}))
        .unwrap();
        let profile = RegisteredProfile {
            id: "f-1".into(),
            definition,
            tools: BTreeMap::new(),
            engine: file_hash(&std::env::current_exe().unwrap()).unwrap(),
            platform: engine::platform(),
        };
        let claim = ClaimFile {
            version: CONTROL_VERSION,
            executor: "mindy".into(),
            root: root.clone(),
            attempt: Grant {
                id: "a1".into(),
                lease: "lease".into(),
                lease_expires: 0.0,
                slots: 1,
                memory_mb: 512,
                timeout_seconds: 60,
            },
            action: Action {
                id: "act".into(),
                key: "key".into(),
                revision: "rev".into(),
                profile: "f-1".into(),
                job: "lint".into(),
                dependencies: vec![],
                created: 0.0,
                identity: None,
                reservation: None,
                origin: None,
            },
            profile: profile.clone(),
            revision: RevisionRef {
                id: "rev".into(),
                project: "p".into(),
                commit: commit.clone(),
            },
            repository: repository.clone(),
            dependencies: vec![],
            transfer: json!({"seconds":1.5,"bytes":42}),
        };
        let directory = root.join("attempts").join("a1");
        fs::create_dir_all(&directory).unwrap();
        let (view, preparation) =
            prepare(&claim, engine::job(&profile, "lint").unwrap(), &directory).unwrap();
        assert_eq!(
            fs::read_to_string(view.join("input.txt")).unwrap(),
            "from the claim\n"
        );
        let mode = fs::metadata(view.join("tool.sh"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755);
        assert!(!view.join("unlisted.txt").exists());
        assert!(!view.join(".git").exists());
        assert!(!root.join("repos").exists());
        assert_eq!(
            (
                &preparation["transfer_seconds"],
                &preparation["transfer_bytes"]
            ),
            (&json!(1.5), &json!(42))
        );

        // Every other job checks the claimed commit out of the same repository.
        let directory = root.join("attempts").join("a2");
        fs::create_dir_all(&directory).unwrap();
        let (view, _) =
            prepare(&claim, engine::job(&profile, "full").unwrap(), &directory).unwrap();
        assert!(view.join("unlisted.txt").exists());
        assert_eq!(source::git(&view, &["rev-parse", "HEAD"]).unwrap(), commit);
    }
}
