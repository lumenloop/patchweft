use crate::{
    engine, file_hash,
    model::*,
    now, source,
    store::{Store, event, record},
    unique,
};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, File},
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
    let capacity = store.capacity()?;
    let transaction = store.transaction()?;
    let held: Vec<Attempt> = {
        let mut stmt = transaction.prepare("SELECT body FROM attempts WHERE drained=0")?;
        stmt.query_map([], |r| r.get::<_, String>(0))?
            .map(|v| Ok(serde_json::from_str(&v?)?))
            .collect::<Result<_>>()?
    };
    let used_slots: u32 = held.iter().map(|a| a.slots).sum();
    let used_memory: u32 = held.iter().map(|a| a.memory_mb).sum();
    let queued: Vec<Action> = {
        let mut stmt = transaction
            .prepare("SELECT body FROM actions WHERE state='queued' ORDER BY created,id")?;
        stmt.query_map([], |r| r.get::<_, String>(0))?
            .map(|v| Ok(serde_json::from_str(&v?)?))
            .collect::<Result<_>>()?
    };
    let priorities = owner_priorities(&transaction)?;
    let mut choices = Vec::new();
    for action in queued {
        if !engine::eligible(&transaction, &action)? {
            continue;
        }
        let mut stmt = transaction
            .prepare("SELECT DISTINCT owner FROM interests WHERE action=? AND active=1")?;
        let owners: Vec<String> = stmt
            .query_map([&action.id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for owner in owners {
            let served = transaction
                .query_row("SELECT last FROM dispatches WHERE owner=?", [&owner], |r| {
                    r.get::<_, f64>(0)
                })
                .optional()?
                .unwrap_or(0.0);
            let priority = priorities.get(&owner).copied().unwrap_or(0);
            choices.push((priority, served, action.created, owner, action.clone()));
        }
    }
    // Higher operator priority first (a landing-ready train), then fair share between owners.
    choices.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.total_cmp(&b.1))
            .then(a.2.total_cmp(&b.2))
            .then(a.3.cmp(&b.3))
    });
    let Some((_, _, _, owner, action)) = choices.first() else {
        return Ok(None);
    };
    let profile: RegisteredProfile = record(&transaction, "profiles", &action.profile)?;
    let job = engine::job(&profile, &action.job)?;
    // Drain for the next owner's action. Later tiny tasks cannot starve this reservation.
    if used_slots + job.slots > capacity.0 || used_memory + job.memory_mb > capacity.1 {
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
        slots: job.slots,
        memory_mb: job.memory_mb,
        created: now(),
        started: None,
        ended: None,
        exit_code: None,
        result: None,
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
        &json!({"action":action.id,"owner":owner,"slots":job.slots,"memory_mb":job.memory_mb}),
    )?;
    transaction.commit()?;
    Ok(Some(attempt))
}

fn owner_priorities(db: &Connection) -> Result<BTreeMap<String, i64>> {
    let exists: i64 = db.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='owner_priorities'",
        [],
        |r| r.get(0),
    )?;
    if exists == 0 {
        return Ok(BTreeMap::new());
    }
    let mut stmt = db.prepare("SELECT owner, priority FROM owner_priorities")?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
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

fn prepare(
    store: &Store,
    action: &Action,
    profile: &RegisteredProfile,
    job: &Job,
    directory: &Path,
) -> Result<(PathBuf, Value)> {
    let source_started = Instant::now();
    engine::verify_runtime(profile)?;
    let view = directory.join("work");
    if job.reuse == Reuse::Files {
        fs::create_dir(&view)?;
        let revision = store.revision(&action.revision)?;
        let repo = source::repository(store, &revision.project);
        for input in &job.inputs {
            let path = source::relative_path(input)?;
            let output = source::git_command(&repo)
                .args(["show", &format!("{}:{input}", revision.commit)])
                .output()?;
            ensure!(output.status.success(), "declared source input unavailable");
            let target = view.join(path);
            fs::create_dir_all(target.parent().context("input parent")?)?;
            fs::write(&target, output.stdout)?;
            let entry = source::git(&repo, &["ls-tree", &revision.commit, "--", input])?;
            if entry.starts_with("100755 ") {
                fs::set_permissions(target, fs::Permissions::from_mode(0o755))?;
            }
        }
    } else {
        source::checkout(store, &action.revision, &view)?;
    }
    let source_seconds = source_started.elapsed().as_secs_f64();
    let dependencies_started = Instant::now();
    let mut copied_bytes = 0_u64;
    let mut copy_seconds = 0.0;
    let mut restored = BTreeMap::new();
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
            let relative = source::relative_path(name)?;
            let source = store
                .root
                .join("attempts")
                .join(&attempt_id)
                .join("artifacts")
                .join(&relative);
            ensure!(
                !fs::symlink_metadata(&source)?.file_type().is_symlink(),
                "dependency artifact is a symlink"
            );
            let hash = file_hash(&source)?;
            ensure!(
                expected["sha256"] == hash,
                "dependency artifact is corrupt: {name}"
            );
            if let Some(old) = restored.get(name) {
                ensure!(old == &hash, "dependency artifacts conflict: {name}");
                continue;
            }
            let target = view.join(&relative);
            ensure!(
                matches!(fs::symlink_metadata(&target),Err(error) if error.kind()==std::io::ErrorKind::NotFound),
                "dependency artifact would overwrite source: {name}"
            );
            contained_parent(&view, &target)?;
            let copy_started = Instant::now();
            copied_bytes += fs::copy(&source, &target)?;
            fs::set_permissions(
                &target,
                fs::Permissions::from_mode(
                    expected["mode"].as_u64().context("artifact mode")? as u32
                ),
            )?;
            copy_seconds += copy_started.elapsed().as_secs_f64();
            restored.insert(name.clone(), hash);
        }
    }
    Ok((
        view,
        json!({
            "source_seconds": source_seconds,
            "dependency_seconds": dependencies_started.elapsed().as_secs_f64(),
            "dependency_copy_seconds": copy_seconds,
            "dependency_copy_bytes": copied_bytes,
            "dependency_files": restored.len()
        }),
    ))
}

fn execute_command(
    store: &Store,
    attempt: &Attempt,
    action: &Action,
    profile: &RegisteredProfile,
    job: &Job,
    directory: &Path,
) -> Result<(i32, Value)> {
    let started = Instant::now();
    let (view, preparation) = prepare(store, action, profile, job, directory)?;
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
        .env("PATCHWEFT_STATE", &store.root)
        .env("PATCHWEFT_ATTEMPT", &attempt.id)
        .env("PATCHWEFT_REVISION", &action.revision)
        .env("PATCHWEFT_SLOTS", job.slots.to_string())
        .env("PATCHWEFT_REPORT", &job.report.path)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    let command_started = Instant::now();
    let mut child = command.spawn()?;
    let mut status = None;
    let mut terminating: Option<(Instant, &str)> = None;
    loop {
        let active: i64 = store.db.query_row(
            "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
            [&action.id],
            |r| r.get(0),
        )?;
        if terminating.is_none() {
            let reason = if STOP.load(Ordering::SeqCst) {
                Some("supervisor-stop")
            } else if active == 0 {
                Some("no-requesters")
            } else if started.elapsed().as_secs() >= job.timeout_seconds {
                Some("timeout")
            } else {
                None
            };
            if let Some(reason) = reason {
                let transaction = rusqlite::Transaction::new_unchecked(
                    &store.db,
                    rusqlite::TransactionBehavior::Immediate,
                )?;
                let current_active: i64 = transaction.query_row(
                    "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
                    [&action.id],
                    |r| r.get(0),
                )?;
                if reason != "no-requesters" || current_active == 0 {
                    transaction.execute(
                        "UPDATE actions SET state='stopping' WHERE id=? AND attempt=?",
                        params![action.id, attempt.id],
                    )?;
                    event(
                        &transaction,
                        "attempt.stopping",
                        &attempt.id,
                        &json!({"reason":reason}),
                    )?;
                    terminating = Some((Instant::now(), reason));
                }
                transaction.commit()?;
            }
        }
        if let Some((time, _)) = terminating {
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
    let report = if code == 0 {
        validate_report(job, &view, &directory.join("output.log"))?
    } else {
        json!({"execution_failed":true})
    };
    let capture_started = Instant::now();
    let artifacts = if code == 0 {
        capture_artifacts(job, &view, directory)?
    } else {
        BTreeMap::new()
    };
    Ok((
        code,
        json!({"exit_code":code,"reason":reason,"elapsed_seconds":started.elapsed().as_secs_f64(),
                    "command_seconds":command_seconds,"preparation":preparation,
                    "artifact_capture_seconds":capture_started.elapsed().as_secs_f64(),
                    "report":report,"log_sha256":file_hash(&directory.join("output.log"))?,
                    "artifacts":artifacts,"revision":action.revision,"profile":profile.id,"job":job.name}),
    ))
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

pub fn execute(store: &mut Store, attempt_id: &str) -> Result<Value> {
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
    let mut attempt = store.attempt(attempt_id)?;
    let action = store.action(&attempt.action)?;
    let profile = store.profile(&action.profile)?;
    let job = engine::job(&profile, &action.job)?.clone();
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
    let result = execute_command(store, &attempt, &action, &profile, &job, &directory);
    // Even an I/O/database error cannot release resources before the job's children are drained.
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
    let (mut code, mut evidence) = match result {
        Ok(value) => value,
        Err(error) => (
            125,
            json!({"error":format!("{error:#}"),"revision":action.revision,
                                 "profile":profile.id,"job":job.name}),
        ),
    };
    let cleanup = cleanup_external(store, &attempt, &profile, &job, &directory);
    let drained = cleanup.is_ok();
    if let Err(error) = cleanup {
        code = 125;
        evidence["cleanup_error"] = json!(format!("{error:#}"));
    }
    attempt.ended = Some(now());
    attempt.exit_code = Some(code);
    attempt.result = Some(evidence.clone());
    let state = if code == 0 {
        "passed"
    } else if evidence["reason"] == "no-requesters" {
        "canceled"
    } else {
        "failed"
    };
    let transaction = store.transaction()?;
    let current: Option<String> = transaction.query_row(
        "SELECT attempt FROM actions WHERE id=?",
        [&action.id],
        |r| r.get(0),
    )?;
    ensure!(
        current.as_deref() == Some(attempt_id),
        "attempt result was fenced"
    );
    transaction.execute(
        "UPDATE attempts SET state=?,drained=?,body=? WHERE id=?",
        params![state, drained, serde_json::to_string(&attempt)?, attempt_id],
    )?;
    transaction.execute(
        "UPDATE actions SET state=? WHERE id=? AND attempt=?",
        params![state, action.id, attempt_id],
    )?;
    if state == "failed" {
        transaction.execute(
            "INSERT INTO repairs VALUES (?,?,?,'execution','open',?,NULL,?)",
            params![
                unique(),
                action.revision,
                action.id,
                evidence.to_string(),
                now()
            ],
        )?;
    }
    event(
        &transaction,
        "attempt.completed",
        attempt_id,
        &json!({"state":state,"evidence":evidence}),
    )?;
    transaction.commit()?;
    Ok(json!({"attempt":attempt_id,"state":state}))
}

fn cleanup_external(
    store: &Store,
    attempt: &Attempt,
    profile: &RegisteredProfile,
    job: &Job,
    directory: &Path,
) -> Result<()> {
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
        .env("PATCHWEFT_STATE", &store.root)
        .env("PATCHWEFT_ATTEMPT", &attempt.id)
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
            )?;
        }
    } else {
        ensure!(metadata.is_file(), "artifact is not a regular file");
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

fn capture_artifacts(job: &Job, view: &Path, directory: &Path) -> Result<BTreeMap<String, Value>> {
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
        copy_files(&target, &output.join(&relative), &relative, &mut manifest)?;
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
