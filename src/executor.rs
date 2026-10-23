//! The coordinator of one executor. `worker` runs it for the local executor; a remote executor
//! runs the same code over another transport. It claims work, keeps the leases of its attempts,
//! launches `_supervise`, relays stops in both directions and publishes results. Its supervisors
//! reach it only through files in their attempt's `control/` directory.
use crate::{
    authority::{self, AuthorityError, LOCAL, MAX_LEASES, Outputs, Scope},
    engine, file_hash,
    model::*,
    source,
    store::Store,
    unique,
    worker::{self, write_atomically},
};
use anyhow::{Context, Result, anyhow, ensure};
use fs2::FileExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::ErrorKind,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

/// Refused profiles are judged again after this long: their tools may have been repaired.
const REVERIFY: Duration = Duration::from_secs(600);
/// Transient failures while fetching an attempt's inputs are tried this often in all.
const MATERIALIZE_TRIES: u32 = 5;
const PUBLISH_RETRY: Duration = Duration::from_secs(1);
const PUBLISH_RETRY_MAX: Duration = Duration::from_secs(60);

/// How a coordinator reaches the authority and the inputs of its attempts.
pub trait Transport: Send + Sync {
    /// `PATCHWEFT_STATE` for jobs: attempt `ID` lives in `root/attempts/ID`.
    fn root(&self) -> &Path;
    /// The coordinator's own files: `lock`, `pending-claim` and `ledger/`.
    fn control(&self) -> &Path;
    fn hello(&self, ids: &[String]) -> Result<Hello, TransportError>;
    fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError>;
    fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError>;
    /// A remote transport uploads the attempt's blobs first.
    fn complete(
        &self,
        request: &CompleteRequest,
        attempt: &Path,
    ) -> Result<CompleteResponse, TransportError>;
    /// A repository holding the revision's commit.
    fn source(&self, revision: &RevisionRef) -> Result<PathBuf, TransportError>;
    /// A local file holding one dependency artifact, for the supervisor to copy.
    fn dependency(
        &self,
        dependency: &DependencyEvidence,
        name: &str,
        entry: &ArtifactEntry,
    ) -> Result<PathBuf, TransportError>;
    /// Disposes of an attempt directory once its report was acknowledged.
    fn retire(&self, attempt: &Path, response: &CompleteResponse) -> Result<()>;
}

#[derive(Debug)]
pub enum TransportError {
    /// Worth retrying: the authority is unreachable or busy.
    Transient(anyhow::Error),
    /// The attempt is no longer this executor's to report.
    Fenced,
    /// The executor's credential or registration is gone.
    Unauthorized(anyhow::Error),
    /// The same request can never succeed.
    Permanent(anyhow::Error),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(error) | Self::Unauthorized(error) | Self::Permanent(error) => {
                write!(f, "{error:#}")
            }
            Self::Fenced => write!(f, "attempt is fenced"),
        }
    }
}
impl std::error::Error for TransportError {}

impl From<AuthorityError> for TransportError {
    fn from(error: AuthorityError) -> Self {
        match error {
            // Another completion was recorded: there is nothing left to report.
            AuthorityError::Fenced | AuthorityError::CompletionConflict => Self::Fenced,
            AuthorityError::Forbidden | AuthorityError::Unregistered => {
                Self::Unauthorized(anyhow::Error::new(error))
            }
            AuthorityError::Internal(error) => Self::Transient(error),
            error => Self::Permanent(anyhow::Error::new(error)),
        }
    }
}

/// The local executor: the authority in-process, the store's own repositories and attempt
/// directories. It copies nothing.
pub struct LocalTransport {
    root: PathBuf,
    control: PathBuf,
}

impl LocalTransport {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.into(),
            control: root.join("executors").join(LOCAL),
        }
    }
    fn store(&self) -> Result<Store, TransportError> {
        Store::open(&self.root).map_err(TransportError::Transient)
    }
}

impl Transport for LocalTransport {
    fn root(&self) -> &Path {
        &self.root
    }
    fn control(&self) -> &Path {
        &self.control
    }
    fn hello(&self, ids: &[String]) -> Result<Hello, TransportError> {
        Ok(authority::hello(
            &self.store()?.db,
            LOCAL,
            &Scope::all(),
            ids,
        )?)
    }
    fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError> {
        Ok(authority::claim_for(
            &mut self.store()?,
            LOCAL,
            &Scope::all(),
            request,
        )?)
    }
    fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError> {
        Ok(authority::renew(
            &mut self.store()?,
            LOCAL,
            &Scope::all(),
            request,
        )?)
    }
    fn complete(
        &self,
        request: &CompleteRequest,
        _: &Path,
    ) -> Result<CompleteResponse, TransportError> {
        Ok(authority::complete(
            &mut self.store()?,
            LOCAL,
            &Scope::all(),
            request,
            Outputs::InPlace,
        )?)
    }
    fn source(&self, revision: &RevisionRef) -> Result<PathBuf, TransportError> {
        Ok(self.root.join("repos").join(&revision.project))
    }
    fn dependency(
        &self,
        dependency: &DependencyEvidence,
        name: &str,
        _: &ArtifactEntry,
    ) -> Result<PathBuf, TransportError> {
        let relative = source::relative_path(name).map_err(TransportError::Permanent)?;
        Ok(self
            .root
            .join("attempts")
            .join(&dependency.attempt)
            .join("artifacts")
            .join(relative))
    }
    /// The attempt directory is the store's evidence.
    fn retire(&self, _: &Path, _: &CompleteResponse) -> Result<()> {
        Ok(())
    }
}

pub struct Options {
    /// Exit once nothing is claimable and every attempt was reported.
    pub drain: bool,
    pub poll: Duration,
    pub heartbeat: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Inputs are fetched and the supervisor is not yet spawned.
    Materializing,
    Launched,
    /// A report waits for the publisher.
    Publishing,
}

#[derive(Debug, Clone, PartialEq)]
enum Report {
    /// The supervisor's `result.json`.
    Result,
    /// The supervisor is gone without a result.
    Lost,
    /// Nothing of the job ran.
    NotStarted {
        reason: String,
        error: Option<String>,
    },
    /// The authority refused the report for good; capacity is released only if it drained.
    Undeliverable { error: String, drained: bool },
}

/// One ledger entry: an attempt this coordinator claimed and has not yet seen acknowledged.
struct Entry {
    claim: Claim,
    phase: Phase,
    supervisor: Option<SupervisorIdentity>,
    registered: bool,
    /// The reason for `control/stop`, once the coordinator decided or relayed one.
    stop: Option<String>,
    stop_written: bool,
    fenced: bool,
    /// The supervisor's own stop from `control/stopping`, sent with every renewal.
    stopping: Option<String>,
    stopping_relayed: bool,
    report: Option<Report>,
}

impl Entry {
    fn new(claim: Claim, phase: Phase) -> Self {
        Self {
            claim,
            phase,
            supervisor: None,
            registered: false,
            stop: None,
            stop_written: false,
            fenced: false,
            stopping: None,
            stopping_relayed: false,
            report: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Claimed {
    Attempt(String),
    /// Nothing claimable, and every profile asked about was judged.
    Idle,
    /// The exchange did not settle; try again at the next poll.
    Retry,
}

struct State {
    ledger: BTreeMap<String, Entry>,
    verified: BTreeSet<String>,
    refused: BTreeMap<String, (String, Instant)>,
    lease_seconds: u64,
    /// When the last renewal that covered every held lease was sent.
    last_renewal: Instant,
    partitioned: bool,
    revoked: Option<String>,
    children: Vec<Child>,
    /// Attempts waiting for the publisher, in order.
    queue: VecDeque<String>,
    wake: bool,
    shutdown: bool,
    launched: u64,
}

pub struct Coordinator<T> {
    transport: T,
    executor: String,
    options: Options,
    /// sha256 of this binary, the runner every profile run here must have been registered with.
    engine: String,
    host: Option<String>,
    state: Mutex<State>,
    heartbeat: Condvar,
    publisher: Condvar,
}

fn log(value: Value) {
    eprintln!("{value}");
}

/// The control-file view of the attempt the authority acknowledged as fenced.
fn unreported(attempt: &str) -> CompleteResponse {
    CompleteResponse {
        attempt: attempt.into(),
        status: "fenced".into(),
        state: None,
        drained: false,
        replayed: false,
        drain_recorded: false,
    }
}

fn blob(path: &Path) -> Result<Option<BlobRef>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(BlobRef {
            sha256: file_hash(path)?,
            bytes: metadata.len(),
        })),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

impl<T: Transport> Coordinator<T> {
    fn new(
        transport: T,
        executor: String,
        lease_seconds: u64,
        options: Options,
        engine: String,
    ) -> Self {
        Self {
            transport,
            executor,
            options,
            engine,
            host: worker::host(),
            state: Mutex::new(State {
                ledger: BTreeMap::new(),
                verified: BTreeSet::new(),
                refused: BTreeMap::new(),
                lease_seconds,
                last_renewal: Instant::now(),
                partitioned: false,
                revoked: None,
                children: Vec::new(),
                queue: VecDeque::new(),
                wake: false,
                shutdown: false,
                launched: 0,
            }),
            heartbeat: Condvar::new(),
            publisher: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn control(&self, attempt: &str) -> PathBuf {
        self.transport
            .root()
            .join("attempts")
            .join(attempt)
            .join("control")
    }

    fn ledger(&self, attempt: &str) -> PathBuf {
        self.transport
            .control()
            .join("ledger")
            .join(format!("{attempt}.json"))
    }

    /// Judges profiles against this host's tools, runner and platform.
    fn verify(&self, profiles: &[RegisteredProfile]) {
        for profile in profiles {
            let verdict = engine::verify_runtime_for(profile, &self.engine);
            let mut state = self.lock();
            match verdict {
                Ok(()) => {
                    state.refused.remove(&profile.id);
                    state.verified.insert(profile.id.clone());
                }
                Err(error) => self.refuse(&mut state, &profile.id, format!("{error:#}")),
            }
        }
    }

    fn refuse(&self, state: &mut State, profile: &str, error: String) {
        log(json!({"event":"profile_refused","executor":self.executor,
                   "profile":profile,"error":error}));
        state.verified.remove(profile);
        state
            .refused
            .insert(profile.into(), (error, Instant::now()));
    }

    /// The executor may no longer act: every job stops, nothing more is claimed, and reports
    /// keep being tried.
    fn revoke(&self, error: &anyhow::Error) {
        let attempts: Vec<String> = {
            let mut state = self.lock();
            if state.revoked.is_none() {
                log(json!({"event":"revoked","executor":self.executor,
                           "error":format!("{error:#}")}));
                state.revoked = Some(format!("{error:#}"));
            }
            state.ledger.keys().cloned().collect()
        };
        for attempt in attempts {
            self.stop(&attempt, "revoked");
        }
    }

    /// Records a stop and hands it to the supervisor once `claim.json` exists; before that,
    /// launch sees it and reports the attempt not started. The first reason wins.
    fn stop(&self, attempt: &str, reason: &str) {
        let mut state = self.lock();
        let Some(entry) = state.ledger.get_mut(attempt) else {
            return;
        };
        if entry.phase == Phase::Publishing {
            return;
        }
        let reason = entry.stop.get_or_insert_with(|| reason.into()).clone();
        let control = self.control(attempt);
        if entry.stop_written || !control.join("claim.json").exists() {
            return;
        }
        match write_atomically(
            &control.join("stop"),
            json!({"reason":reason}).to_string().as_bytes(),
        ) {
            Ok(()) => entry.stop_written = true,
            Err(error) => log(json!({"event":"stop_failed","executor":self.executor,
                                     "attempt":attempt,"error":format!("{error:#}")})),
        }
    }

    /// Hands an attempt to the publisher.
    fn report(&self, attempt: &str, report: Report) {
        let mut state = self.lock();
        let Some(entry) = state.ledger.get_mut(attempt) else {
            return;
        };
        if entry.phase == Phase::Publishing {
            return;
        }
        entry.phase = Phase::Publishing;
        entry.report = Some(report);
        state.queue.push_back(attempt.into());
        self.publisher.notify_all();
    }

    fn wake(&self) {
        self.lock().wake = true;
        self.heartbeat.notify_all();
    }

    /// Takes up this coordinator's attempts from its ledger: results are published, live
    /// supervisors resumed and dead ones reported. Returns the attempts still to materialize.
    /// `recovering` launches nothing and leaves live supervisors to the next coordinator.
    fn reconcile(&self, recovering: bool) -> Result<Vec<String>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(self.transport.control().join("ledger"))?
            .map(|entry| Ok(entry?.path()))
            .collect::<Result<_>>()?;
        paths.sort();
        let mut materialize = Vec::new();
        for path in paths {
            // Partly written entries are temporary files without the extension.
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let claim: Claim = serde_json::from_slice(&fs::read(&path)?)
                .with_context(|| format!("unreadable ledger entry {}", path.display()))?;
            let attempt = claim.attempt.id.clone();
            let control = self.control(&attempt);
            let supervisor: Option<SupervisorIdentity> =
                match fs::read(control.join("supervisor.json")) {
                    Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
                    Err(error) if error.kind() == ErrorKind::NotFound => None,
                    Err(error) => return Err(error.into()),
                };
            let alive = supervisor
                .as_ref()
                .is_some_and(|s| worker::alive(s.pid, &s.start));
            if recovering && alive {
                continue;
            }
            let launched = control.join("claim.json").exists();
            let finished = || control.join("result.json").exists();
            let mut entry = Entry::new(claim, Phase::Launched);
            entry.registered = control.join("registered").exists();
            entry.stop = worker::stop_reason(&control)?;
            entry.stop_written = entry.stop.is_some();
            entry.stopping = worker::control_reason(&control.join("stopping"))?;
            let spawned = supervisor.is_some();
            entry.supervisor = supervisor;
            if !launched && !recovering {
                entry.phase = Phase::Materializing;
                materialize.push(attempt.clone());
            }
            self.lock().ledger.insert(attempt.clone(), entry);
            if finished() {
                self.report(&attempt, Report::Result);
            } else if !launched {
                if recovering {
                    let reason = "coordinator-lost".into();
                    self.report(
                        &attempt,
                        Report::NotStarted {
                            reason,
                            error: None,
                        },
                    );
                }
            } else if !alive {
                // A supervisor spawned just before the crash must not start its job now.
                if !spawned {
                    self.stop(&attempt, "coordinator-lost");
                }
                let report = if finished() {
                    Report::Result
                } else {
                    Report::Lost
                };
                self.report(&attempt, report);
            }
        }
        Ok(materialize)
    }

    /// One claim exchange. Profiles the authority asks about are verified and the claim is sent
    /// again, so admission never depends on verification lag.
    fn claim(&self) -> Result<Claimed> {
        let pending = self.transport.control().join("pending-claim");
        // A key whose response was lost is sent again first: the authority replays its claim.
        let key = match fs::read(&pending) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)?["key"]
                .as_str()
                .context("pending claim lacks a key")?
                .to_owned(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let key = unique();
                write_atomically(&pending, json!({"key":key}).to_string().as_bytes())?;
                key
            }
            Err(error) => return Err(error.into()),
        };
        loop {
            let request = {
                let mut state = self.lock();
                let now = Instant::now();
                state
                    .refused
                    .retain(|_, (_, at)| now.duration_since(*at) < REVERIFY);
                ClaimRequest {
                    claim_key: key.clone(),
                    verified: state.verified.iter().cloned().collect(),
                    refused: state
                        .refused
                        .iter()
                        .map(|(profile, (error, _))| (profile.clone(), error.clone()))
                        .collect(),
                    memory_limit_mb: None,
                    disk_free_mb: None,
                    host: self.host.clone(),
                    engine: Some(self.engine.clone()),
                }
            };
            let sent = Instant::now();
            let response = match self.transport.claim(&request) {
                Ok(response) => response,
                Err(error) => return self.unclaimed(error),
            };
            if let Some(attempt) = response.fenced {
                // The key's attempt ended before this coordinator recorded it; nothing of it ran.
                log(json!({"event":"claim_fenced","executor":self.executor,"attempt":attempt}));
                fs::remove_file(&pending)?;
                return Ok(Claimed::Retry);
            }
            if let Some(claim) = response.claim {
                let attempt = claim.attempt.id.clone();
                write_atomically(&self.ledger(&attempt), &serde_json::to_vec(&claim)?)?;
                fs::remove_file(&pending)?;
                let mut state = self.lock();
                // The new lease starts now; held ones keep the time of their last renewal.
                if state.ledger.is_empty() {
                    state.last_renewal = sent;
                }
                let entry = Entry::new(claim, Phase::Materializing);
                state.ledger.insert(attempt.clone(), entry);
                return Ok(Claimed::Attempt(attempt));
            }
            if response.unverified.is_empty() {
                return Ok(Claimed::Idle);
            }
            let hello = match self.transport.hello(&response.unverified) {
                Ok(hello) => hello,
                Err(error) => return self.unclaimed(error),
            };
            self.verify(&hello.profiles);
            let mut state = self.lock();
            for profile in &response.unverified {
                if !state.verified.contains(profile) && !state.refused.contains_key(profile) {
                    let error = "profile is not available to this executor".into();
                    self.refuse(&mut state, profile, error);
                }
            }
        }
    }

    fn unclaimed(&self, error: TransportError) -> Result<Claimed> {
        match error {
            TransportError::Unauthorized(error) => self.revoke(&error),
            TransportError::Transient(error) => {
                log(json!({"event":"claim_failed","executor":self.executor,
                           "error":format!("{error:#}")}));
            }
            error => return Err(anyhow!(error)),
        }
        Ok(Claimed::Retry)
    }

    /// Fetches the attempt's inputs, writes `claim.json` and spawns its supervisor.
    fn materialize(&self, attempt: &str) {
        if let Err(report) = self.launch(attempt) {
            self.report(attempt, report);
        }
    }

    fn launch(&self, attempt: &str) -> Result<(), Report> {
        let not_started = |reason: &str, error: String| Report::NotStarted {
            reason: reason.into(),
            error: Some(error),
        };
        let Some(claim) = self.lock().ledger.get(attempt).map(|e| e.claim.clone()) else {
            return Ok(());
        };
        // Tools may have drifted since the profile was verified.
        if let Err(error) = engine::verify_runtime_for(&claim.profile, &self.engine) {
            let error = format!("{error:#}");
            self.refuse(&mut self.lock(), &claim.profile.id, error.clone());
            return Err(not_started("runtime-mismatch", error));
        }
        let fetching = Instant::now();
        let repository = self
            .retrying(|| self.transport.source(&claim.revision))
            .map_err(|error| not_started("materialize-failed", error))?;
        let mut dependencies = Vec::new();
        for dependency in &claim.dependencies {
            for (name, entry) in &dependency.artifacts {
                let path = self
                    .retrying(|| self.transport.dependency(dependency, name, entry))
                    .map_err(|error| not_started("materialize-failed", error))?;
                dependencies.push(DependencyFile {
                    action: dependency.action.clone(),
                    attempt: dependency.attempt.clone(),
                    name: name.clone(),
                    sha256: entry.sha256.clone(),
                    mode: entry.mode,
                    bytes: entry.bytes,
                    path,
                });
            }
        }
        let file = ClaimFile {
            version: CONTROL_VERSION,
            executor: self.executor.clone(),
            root: self.transport.root().into(),
            attempt: claim.attempt.clone(),
            action: claim.action.clone(),
            profile: claim.profile.clone(),
            revision: claim.revision.clone(),
            repository,
            dependencies,
            transfer: json!({"seconds":fetching.elapsed().as_secs_f64(),"bytes":0}),
        };
        let control = self.control(attempt);
        fs::create_dir_all(&control)
            .map_err(anyhow::Error::from)
            .and_then(|()| {
                write_atomically(&control.join("claim.json"), &serde_json::to_vec(&file)?)
            })
            .map_err(|error| not_started("launch-failed", format!("{error:#}")))?;
        // A stop that came meanwhile ends the attempt before anything of it runs.
        if let Some(reason) = self.lock().ledger.get(attempt).and_then(|e| e.stop.clone()) {
            return Err(Report::NotStarted {
                reason,
                error: None,
            });
        }
        let child = self
            .spawn(attempt)
            .map_err(|error| not_started("launch-failed", format!("{error:#}")))?;
        let pid = child.id();
        // A supervisor that already exited is reported lost by the next poll.
        let supervisor = worker::process_start(pid).map(|start| SupervisorIdentity { pid, start });
        if let Some(supervisor) = &supervisor
            && let Err(error) = serde_json::to_vec(supervisor)
                .map_err(anyhow::Error::from)
                .and_then(|bytes| write_atomically(&control.join("supervisor.json"), &bytes))
        {
            log(
                json!({"event":"supervisor_unrecorded","executor":self.executor,
                       "attempt":attempt,"error":format!("{error:#}")}),
            );
        }
        let mut state = self.lock();
        state.children.push(child);
        state.launched += 1;
        if let Some(entry) = state.ledger.get_mut(attempt) {
            entry.phase = Phase::Launched;
            entry.supervisor = supervisor;
        }
        // Registration is due at once.
        state.wake = true;
        self.heartbeat.notify_all();
        Ok(())
    }

    fn spawn(&self, attempt: &str) -> Result<Child> {
        let directory = self.transport.root().join("attempts").join(attempt);
        let log = File::create(directory.join("supervisor.log"))?;
        Ok(Command::new(std::env::current_exe()?)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .arg("_supervise")
            .arg(self.transport.root())
            .arg(attempt)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .spawn()?)
    }

    /// Transient failures are retried with backoff; any other ends the attempt.
    fn retrying<V>(
        &self,
        mut operation: impl FnMut() -> Result<V, TransportError>,
    ) -> Result<V, String> {
        let mut delay = PUBLISH_RETRY;
        let mut tries = 1;
        loop {
            match operation() {
                Ok(value) => return Ok(value),
                Err(TransportError::Transient(error)) if tries < MATERIALIZE_TRIES => {
                    log(json!({"event":"materialize_retry","executor":self.executor,
                               "error":format!("{error:#}")}));
                    thread::sleep(delay);
                    delay *= 2;
                    tries += 1;
                }
                Err(error) => return Err(format!("{error:#}")),
            }
        }
    }

    /// One renewal of every lease this coordinator holds.
    fn heartbeat_once(&self) -> Result<(), TransportError> {
        let reports: Vec<LeaseReport> = {
            let mut state = self.lock();
            if state.ledger.is_empty() {
                state.last_renewal = Instant::now();
                return Ok(());
            }
            state
                .ledger
                .iter()
                .map(|(attempt, entry)| LeaseReport {
                    attempt: attempt.clone(),
                    lease: entry.claim.attempt.lease.clone(),
                    started: (entry.phase == Phase::Launched && !entry.registered)
                        .then(|| entry.supervisor.clone())
                        .flatten(),
                    stopping: entry.stopping.clone(),
                })
                .collect()
        };
        let sent = Instant::now();
        let mut statuses = Vec::new();
        let mut lease_seconds = None;
        for leases in reports.chunks(MAX_LEASES) {
            let request = RenewRequest {
                leases: leases.to_vec(),
            };
            let response = match self.transport.renew(&request) {
                Ok(response) => response,
                Err(TransportError::Unauthorized(error)) => {
                    self.revoke(&error);
                    return Err(TransportError::Unauthorized(error));
                }
                Err(error) => return Err(error),
            };
            lease_seconds = Some(response.lease_seconds);
            statuses.extend(response.leases);
        }
        {
            let mut state = self.lock();
            state.last_renewal = sent;
            state.partitioned = false;
            if let Some(lease_seconds) = lease_seconds {
                state.lease_seconds = lease_seconds;
            }
        }
        let sent: BTreeMap<String, Option<String>> = reports
            .into_iter()
            .map(|report| (report.attempt, report.stopping))
            .collect();
        for status in statuses {
            let stopping = sent.get(&status.attempt).cloned().flatten();
            self.apply(status, stopping.as_deref());
        }
        Ok(())
    }

    fn apply(&self, status: LeaseStatus, sent: Option<&str>) {
        let attempt = status.attempt.as_str();
        if status.status != "ok" {
            if let Some(entry) = self.lock().ledger.get_mut(attempt) {
                entry.fenced = true;
            }
            // A live supervisor stops and then reports its drain; a dead one is reported lost.
            self.stop(attempt, "fenced");
            return;
        }
        {
            let mut state = self.lock();
            let Some(entry) = state.ledger.get_mut(attempt) else {
                return;
            };
            if sent.is_some() && entry.stopping.as_deref() == sent {
                entry.stopping_relayed = true;
            }
            let control = self.control(attempt);
            if status.registered && !entry.registered && control.join("claim.json").exists() {
                match write_atomically(&control.join("registered"), b"") {
                    Ok(()) => entry.registered = true,
                    Err(error) => log(json!({"event":"registration_unrecorded",
                        "executor":self.executor,"attempt":attempt,"error":format!("{error:#}")})),
                }
            }
        }
        if let Some(reason) = status.stop {
            self.stop(attempt, &reason);
        }
    }

    /// The heartbeat thread: renews when due or woken, backing off while renewals fail.
    fn beat(&self) {
        let mut delay = self.options.heartbeat;
        loop {
            {
                let state = self.lock();
                let (mut state, _) = self
                    .heartbeat
                    .wait_timeout_while(state, delay, |s| !s.wake && !s.shutdown)
                    .unwrap_or_else(PoisonError::into_inner);
                if state.shutdown {
                    return;
                }
                state.wake = false;
            }
            delay = match self.heartbeat_once() {
                Ok(()) => self.options.heartbeat,
                Err(error) => {
                    log(json!({"event":"renew_failed","executor":self.executor,
                               "error":format!("{error:#}")}));
                    let cap = Duration::from_secs(self.lock().lease_seconds) / 4;
                    (delay * 2).min(cap).max(self.options.heartbeat)
                }
            };
        }
    }

    /// Watches launched attempts: relays their supervisors' own stops, hands results to the
    /// publisher, reports lost supervisors, and stops every job before an unreachable authority
    /// could let its lease expire.
    fn poll(&self, now: Instant) {
        let launched: Vec<(String, Option<SupervisorIdentity>, bool)> = {
            let mut state = self.lock();
            state
                .children
                .retain_mut(|child| matches!(child.try_wait(), Ok(None)));
            state
                .ledger
                .iter()
                .filter(|(_, entry)| entry.phase == Phase::Launched)
                .map(|(attempt, entry)| {
                    let stopping = entry.stopping.is_some();
                    (attempt.clone(), entry.supervisor.clone(), stopping)
                })
                .collect()
        };
        for (attempt, supervisor, stopping) in launched {
            let control = self.control(&attempt);
            if !stopping && let Ok(Some(reason)) = worker::control_reason(&control.join("stopping"))
            {
                if let Some(entry) = self.lock().ledger.get_mut(&attempt) {
                    entry.stopping = Some(reason);
                }
                self.wake();
            }
            let result = control.join("result.json");
            if result.exists() {
                self.report(&attempt, Report::Result);
            } else if !supervisor.is_some_and(|s| worker::alive(s.pid, &s.start)) {
                // It may have written its result just before exiting.
                let report = if result.exists() {
                    Report::Result
                } else {
                    Report::Lost
                };
                self.report(&attempt, report);
            }
        }
        self.partition(now);
    }

    fn partition(&self, now: Instant) {
        let attempts: Vec<String> = {
            let mut state = self.lock();
            let lease = Duration::from_secs(state.lease_seconds);
            let margin = (self.options.heartbeat * 2).max(lease / 5);
            if now <= state.last_renewal + lease.saturating_sub(margin) {
                return;
            }
            let attempts: Vec<String> = state
                .ledger
                .iter()
                .filter(|(_, entry)| entry.phase == Phase::Launched && entry.stop.is_none())
                .map(|(attempt, _)| attempt.clone())
                .collect();
            if !attempts.is_empty() && !state.partitioned {
                state.partitioned = true;
                log(json!({"event":"partition_stop","executor":self.executor,
                           "attempts":attempts}));
            }
            attempts
        };
        for attempt in attempts {
            self.stop(&attempt, "partition");
        }
    }

    /// Reports one attempt and retires it once the authority acknowledged it. None when the
    /// attempt is no longer in the ledger.
    fn publish(&self, attempt: &str) -> Result<Option<CompleteResponse>, TransportError> {
        let directory = self.transport.root().join("attempts").join(attempt);
        loop {
            let (claim, report, relay) = {
                let state = self.lock();
                let Some(entry) = state.ledger.get(attempt) else {
                    return Ok(None);
                };
                let relay = entry
                    .stopping
                    .clone()
                    .filter(|_| !entry.stopping_relayed && !entry.fenced);
                let report = entry.report.clone().unwrap_or(Report::Lost);
                (entry.claim.clone(), report, relay)
            };
            // The store records a stop the supervisor began before it records the result.
            if let Some(reason) = relay {
                self.relay(attempt, &claim, &reason)?;
            }
            let request = match self.request(attempt, &claim, &report) {
                Ok(request) => request,
                Err(error) => {
                    let drained = !self.control(attempt).join("command-started").exists();
                    self.undeliverable(attempt, format!("{error:#}"), drained);
                    continue;
                }
            };
            let response = match self.transport.complete(&request, &directory) {
                Ok(response) => response,
                Err(TransportError::Fenced) => unreported(attempt),
                Err(TransportError::Permanent(error)) => {
                    if matches!(report, Report::Undeliverable { .. }) {
                        log(json!({"event":"report_abandoned","executor":self.executor,
                                   "attempt":attempt,"error":format!("{error:#}")}));
                        unreported(attempt)
                    } else {
                        self.undeliverable(attempt, format!("{error:#}"), request.drained);
                        continue;
                    }
                }
                Err(TransportError::Unauthorized(error)) => {
                    self.revoke(&error);
                    return Err(TransportError::Unauthorized(error));
                }
                Err(error) => return Err(error),
            };
            // The supervisor found this host's tools drifted: claim no more of the profile.
            if request.outcome == Outcome::NotStarted
                && request.reason.as_deref() == Some("runtime-mismatch")
                && report == Report::Result
            {
                let error = request.evidence["error"]
                    .as_str()
                    .unwrap_or("runtime mismatch")
                    .to_owned();
                self.refuse(&mut self.lock(), &claim.profile.id, error);
            }
            if let Err(error) = self.transport.retire(&directory, &response) {
                log(json!({"event":"retire_failed","executor":self.executor,
                           "attempt":attempt,"error":format!("{error:#}")}));
            }
            match fs::remove_file(self.ledger(attempt)) {
                Err(error) if error.kind() != ErrorKind::NotFound => {
                    return Err(TransportError::Transient(error.into()));
                }
                _ => {}
            }
            self.lock().ledger.remove(attempt);
            return Ok(Some(response));
        }
    }

    fn undeliverable(&self, attempt: &str, error: String, drained: bool) {
        log(json!({"event":"undeliverable","executor":self.executor,
                   "attempt":attempt,"error":error}));
        if let Some(entry) = self.lock().ledger.get_mut(attempt) {
            entry.report = Some(Report::Undeliverable { error, drained });
        }
    }

    fn relay(&self, attempt: &str, claim: &Claim, reason: &str) -> Result<(), TransportError> {
        let request = RenewRequest {
            leases: vec![LeaseReport {
                attempt: attempt.into(),
                lease: claim.attempt.lease.clone(),
                started: None,
                stopping: Some(reason.into()),
            }],
        };
        let response = match self.transport.renew(&request) {
            Ok(response) => response,
            Err(TransportError::Unauthorized(error)) => {
                self.revoke(&error);
                return Err(TransportError::Unauthorized(error));
            }
            Err(error) => return Err(error),
        };
        for status in response.leases {
            self.apply(status, Some(reason));
        }
        Ok(())
    }

    /// The completion for a report, rebuilt identically after a restart so a lost response is
    /// answered as a replay.
    fn request(&self, attempt: &str, claim: &Claim, report: &Report) -> Result<CompleteRequest> {
        let directory = self.transport.root().join("attempts").join(attempt);
        let control = directory.join("control");
        let base = CompleteRequest {
            attempt: attempt.into(),
            lease: claim.attempt.lease.clone(),
            outcome: Outcome::Lost,
            reason: None,
            exit_code: 125,
            evidence: Value::Null,
            drained: true,
            log: None,
            cleanup_log: None,
            artifacts: BTreeMap::new(),
        };
        Ok(match report {
            Report::Result => {
                let result: SupervisorResult =
                    serde_json::from_slice(&fs::read(control.join("result.json"))?)?;
                let artifacts = match result.evidence.get("artifacts") {
                    None | Some(Value::Null) => BTreeMap::new(),
                    Some(artifacts) => serde_json::from_value(artifacts.clone())?,
                };
                CompleteRequest {
                    outcome: result.outcome,
                    reason: result.reason,
                    exit_code: result.exit_code,
                    evidence: result.evidence,
                    drained: result.drained,
                    log: blob(&directory.join("output.log"))?,
                    cleanup_log: blob(&directory.join("cleanup.log"))?,
                    artifacts,
                    ..base
                }
            }
            Report::Lost => {
                let supervisor = fs::read(control.join("supervisor.json"))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .unwrap_or(Value::Null);
                CompleteRequest {
                    evidence: json!({"error":"supervisor exited without a result",
                                     "supervisor":supervisor}),
                    // Only a command that never started is known to have left nothing behind.
                    drained: !control.join("command-started").exists(),
                    ..base
                }
            }
            Report::NotStarted { reason, error } => CompleteRequest {
                outcome: Outcome::NotStarted,
                reason: Some(reason.clone()),
                evidence: json!({"error":error}),
                ..base
            },
            Report::Undeliverable { error, drained } => CompleteRequest {
                outcome: Outcome::Undeliverable,
                evidence: json!({"error":error}),
                drained: *drained,
                ..base
            },
        })
    }

    /// Publishes every queued report now, in place of the publisher thread.
    fn publish_queued(&self) -> Result<Vec<CompleteResponse>, TransportError> {
        let mut responses = Vec::new();
        loop {
            let next = self.lock().queue.front().cloned();
            let Some(attempt) = next else {
                return Ok(responses);
            };
            responses.extend(self.publish(&attempt)?);
            self.lock().queue.pop_front();
        }
    }

    /// The publisher thread: one report at a time, retried with backoff while heartbeats go on.
    fn publisher(&self) {
        let mut delay = PUBLISH_RETRY;
        loop {
            let attempt = {
                let mut state = self.lock();
                loop {
                    if state.shutdown {
                        return;
                    }
                    if let Some(attempt) = state.queue.front() {
                        break attempt.clone();
                    }
                    state = self
                        .publisher
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            };
            match self.publish(&attempt) {
                Ok(_) => {
                    self.lock().queue.pop_front();
                    delay = PUBLISH_RETRY;
                }
                Err(error) => {
                    log(json!({"event":"publish_retry","executor":self.executor,
                               "attempt":attempt,"error":format!("{error:#}"),
                               "retry_seconds":delay.as_secs()}));
                    let state = self.lock();
                    let _ = self
                        .publisher
                        .wait_timeout_while(state, delay, |s| !s.shutdown)
                        .unwrap_or_else(PoisonError::into_inner);
                    delay = (delay * 2).min(PUBLISH_RETRY_MAX);
                }
            }
        }
    }

    fn shutdown(&self) {
        self.lock().shutdown = true;
        self.heartbeat.notify_all();
        self.publisher.notify_all();
    }
}

impl<T: Transport + 'static> Coordinator<T> {
    fn materialize_later(self: &Arc<Self>, attempt: String) {
        let coordinator = Arc::clone(self);
        thread::spawn(move || coordinator.materialize(&attempt));
    }

    /// The main loop: watch, then claim until nothing is claimable.
    fn coordinate(self: &Arc<Self>) -> Result<Value> {
        loop {
            self.poll(Instant::now());
            let (revoked, idle) = {
                let state = self.lock();
                (state.revoked.clone(), state.ledger.is_empty())
            };
            if let Some(error) = &revoked
                && idle
            {
                anyhow::bail!("executor {} may no longer run work: {error}", self.executor);
            }
            let claimed = if revoked.is_some() {
                Claimed::Idle
            } else {
                self.claim()?
            };
            match claimed {
                Claimed::Attempt(attempt) => {
                    self.materialize_later(attempt);
                    continue;
                }
                // Checked before the claim: a report published since may have unblocked work.
                Claimed::Idle if self.options.drain && idle => break,
                _ => {}
            }
            thread::sleep(self.options.poll);
        }
        let launched = self.lock().launched;
        Ok(json!({"launched":launched,"executor":self.executor}))
    }
}

/// The file whose lock makes one coordinator the executor's only active one.
fn lock_file(transport: &impl Transport) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(transport.control().join("lock"))?)
}

/// Takes the executor's lock. A second coordinator for the same executor waits as a standby
/// until the first one exits.
fn exclusive(transport: &impl Transport, executor: &str) -> Result<(File, bool)> {
    let file = lock_file(transport)?;
    if file.try_lock_exclusive().is_ok() {
        return Ok((file, false));
    }
    log(json!({"waiting_for":"executor lock","executor":executor}));
    file.lock_exclusive()?;
    Ok((file, true))
}

/// Runs a coordinator over `transport` until it drains, or forever.
pub fn run<T: Transport + 'static>(transport: T, options: Options) -> Result<Value> {
    fs::create_dir_all(transport.control().join("ledger"))?;
    let hello = transport.hello(&[])?;
    let (_lock, waited) = exclusive(&transport, &hello.executor)?;
    // A standby may have waited for hours.
    let hello = if waited { transport.hello(&[])? } else { hello };
    ensure!(
        options.heartbeat * 3 <= Duration::from_secs(hello.lease_seconds),
        "heartbeat ({} s) must be at most a third of the lease ({} s)",
        options.heartbeat.as_secs_f64(),
        hello.lease_seconds
    );
    let engine = file_hash(&std::env::current_exe()?)?;
    let coordinator = Arc::new(Coordinator::new(
        transport,
        hello.executor.clone(),
        hello.lease_seconds,
        options,
        engine,
    ));
    coordinator.verify(&hello.profiles);
    for attempt in coordinator.reconcile(false)? {
        coordinator.materialize_later(attempt);
    }
    let heartbeat = {
        let coordinator = Arc::clone(&coordinator);
        thread::spawn(move || coordinator.beat())
    };
    let publisher = {
        let coordinator = Arc::clone(&coordinator);
        thread::spawn(move || coordinator.publisher())
    };
    let result = coordinator.coordinate();
    coordinator.shutdown();
    let _ = heartbeat.join();
    let _ = publisher.join();
    result
}

/// `patchweft worker`: the local executor's coordinator.
pub fn local(store: &Store, options: Options) -> Result<Value> {
    authority::prepare(&store.db)?;
    let legacy: Vec<String> = {
        let mut stmt = store.db.prepare(
            "SELECT id FROM attempts WHERE state IN ('claimed','running')
             AND json_extract(body,'$.lease') IS NULL ORDER BY id",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    ensure!(
        legacy.is_empty(),
        "drain attempts started by the previous binary before upgrading: {}",
        legacy.join(", ")
    );
    run(LocalTransport::new(&store.root), options)
}

/// `patchweft recover`: when no coordinator runs, reports the local executor's attempts it can
/// settle (published results, lost supervisors, attempts never launched); live supervisors are
/// left to the next coordinator. Then an operator may confirm one attempt drained.
pub fn recover(store: &mut Store, confirmed: Option<&str>) -> Result<Value> {
    authority::prepare(&store.db)?;
    let transport = LocalTransport::new(&store.root);
    fs::create_dir_all(transport.control().join("ledger"))?;
    let mut drained = Vec::new();
    // A running coordinator takes care of its own attempts.
    let lock = lock_file(&transport)?;
    if lock.try_lock_exclusive().is_ok() {
        let lease_seconds = authority::load(&store.db, LOCAL)?
            .context("local executor record missing")?
            .lease_seconds;
        let options = Options {
            drain: true,
            poll: Duration::from_millis(100),
            heartbeat: Duration::from_secs(1),
        };
        let coordinator = Coordinator::new(
            transport,
            LOCAL.into(),
            lease_seconds,
            options,
            String::new(),
        );
        coordinator.reconcile(true)?;
        for response in coordinator.publish_queued()? {
            if response.drained && response.state.as_deref() == Some("interrupted") {
                drained.push(response.attempt);
            }
        }
    }
    if let Some(attempt) = confirmed {
        authority::confirm_drained(store, attempt)?;
        drained.push(attempt.into());
    }
    let uncertain: Vec<String> = {
        let mut stmt = store.db.prepare(
            "SELECT id FROM attempts WHERE drained=0 AND state NOT IN ('claimed','running')
             ORDER BY rowid",
        )?;
        stmt.query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    Ok(json!({"interrupted_and_drained":drained,"needs_drain_confirmation":uncertain}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    enum Call {
        Hello(Vec<String>),
        Claim(ClaimRequest),
        Renew(RenewRequest),
        Complete(CompleteRequest),
        Retire(String),
    }

    /// Records every call. Claims answer from a script, then with no work; renewals and
    /// completions follow the configured verdict.
    struct Fake {
        root: PathBuf,
        control: PathBuf,
        profiles: Vec<RegisteredProfile>,
        claims: Mutex<VecDeque<ClaimResponse>>,
        /// "ok", "fenced" or "unreachable".
        renewal: &'static str,
        /// "recorded" or "fenced".
        completion: &'static str,
        calls: Mutex<Vec<Call>>,
    }

    impl Fake {
        fn record(&self, call: Call) {
            self.calls.lock().unwrap().push(call);
        }
    }

    impl Transport for Fake {
        fn root(&self) -> &Path {
            &self.root
        }
        fn control(&self) -> &Path {
            &self.control
        }
        fn hello(&self, ids: &[String]) -> Result<Hello, TransportError> {
            self.record(Call::Hello(ids.to_vec()));
            Ok(Hello {
                executor: "mindy".into(),
                lease_seconds: 10,
                profiles: self
                    .profiles
                    .iter()
                    .filter(|p| ids.contains(&p.id))
                    .cloned()
                    .collect(),
            })
        }
        fn claim(&self, request: &ClaimRequest) -> Result<ClaimResponse, TransportError> {
            self.record(Call::Claim(request.clone()));
            Ok(self
                .claims
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ClaimResponse {
                    claim: None,
                    fenced: None,
                    unverified: vec![],
                }))
        }
        fn renew(&self, request: &RenewRequest) -> Result<RenewResponse, TransportError> {
            self.record(Call::Renew(request.clone()));
            if self.renewal == "unreachable" {
                return Err(TransportError::Transient(anyhow!("connection refused")));
            }
            let leases = request
                .leases
                .iter()
                .map(|lease| LeaseStatus {
                    attempt: lease.attempt.clone(),
                    status: self.renewal.into(),
                    stop: None,
                    registered: self.renewal == "ok",
                    lease_expires: Some(0.0),
                })
                .collect();
            Ok(RenewResponse {
                lease_seconds: 10,
                leases,
            })
        }
        fn complete(
            &self,
            request: &CompleteRequest,
            _: &Path,
        ) -> Result<CompleteResponse, TransportError> {
            self.record(Call::Complete(request.clone()));
            let fenced = self.completion == "fenced";
            Ok(CompleteResponse {
                attempt: request.attempt.clone(),
                status: self.completion.into(),
                state: Some("interrupted".into()),
                drained: request.drained,
                replayed: false,
                drain_recorded: fenced && request.drained,
            })
        }
        fn source(&self, _: &RevisionRef) -> Result<PathBuf, TransportError> {
            Ok(self.root.join("repos/p"))
        }
        fn dependency(
            &self,
            _: &DependencyEvidence,
            name: &str,
            _: &ArtifactEntry,
        ) -> Result<PathBuf, TransportError> {
            Ok(self.root.join("cas").join(name))
        }
        fn retire(&self, attempt: &Path, _: &CompleteResponse) -> Result<()> {
            let name = attempt.file_name().unwrap().to_string_lossy().into_owned();
            self.record(Call::Retire(name));
            Ok(())
        }
    }

    fn profile(id: &str, engine: &str) -> RegisteredProfile {
        let definition: Profile = serde_json::from_value(json!({
            "version":1,"name":id,"tools":{},"jobs":[{"name":"job","command":["sh"]}]}))
        .unwrap();
        RegisteredProfile {
            id: id.into(),
            definition,
            tools: BTreeMap::new(),
            engine: engine.into(),
            platform: engine::platform(),
        }
    }

    fn claim(attempt: &str) -> Claim {
        Claim {
            attempt: Grant {
                id: attempt.into(),
                lease: format!("lease-{attempt}"),
                lease_expires: 0.0,
                slots: 1,
                memory_mb: 512,
                timeout_seconds: 60,
            },
            action: Action {
                id: format!("action-{attempt}"),
                key: "key".into(),
                revision: "rev".into(),
                profile: "p-ok".into(),
                job: "job".into(),
                dependencies: vec![],
                created: 0.0,
                identity: None,
                reservation: None,
                origin: None,
            },
            profile: profile("p-ok", "e"),
            revision: RevisionRef {
                id: "rev".into(),
                project: "p".into(),
                commit: "c".repeat(40),
            },
            dependencies: vec![],
        }
    }

    fn fake(root: &Path) -> Fake {
        Fake {
            root: root.join("work"),
            control: root.join("work"),
            profiles: vec![profile("p-ok", "e"), profile("p-bad", "other")],
            claims: Mutex::new(VecDeque::new()),
            renewal: "ok",
            completion: "recorded",
            calls: Mutex::new(vec![]),
        }
    }

    /// A coordinator for executor mindy: lease 10 s, heartbeat 1 s, engine "e".
    fn coordinator(fake: Fake) -> Coordinator<Fake> {
        fs::create_dir_all(fake.control.join("ledger")).unwrap();
        let options = Options {
            drain: true,
            poll: Duration::from_millis(10),
            heartbeat: Duration::from_secs(1),
        };
        Coordinator::new(fake, "mindy".into(), 10, options, "e".into())
    }

    /// A claimed attempt whose supervisor was spawned: ledger entry, `claim.json` and, given an
    /// identity, `supervisor.json`.
    fn launched(
        coordinator: &Coordinator<Fake>,
        attempt: &str,
        supervisor: Option<&SupervisorIdentity>,
    ) -> PathBuf {
        fs::write(
            coordinator.ledger(attempt),
            serde_json::to_vec(&claim(attempt)).unwrap(),
        )
        .unwrap();
        let control = coordinator.control(attempt);
        fs::create_dir_all(&control).unwrap();
        fs::write(control.join("claim.json"), "{}").unwrap();
        if let Some(supervisor) = supervisor {
            fs::write(
                control.join("supervisor.json"),
                serde_json::to_vec(supervisor).unwrap(),
            )
            .unwrap();
        }
        control
    }

    /// The identity of a process that no longer exists.
    fn dead() -> SupervisorIdentity {
        SupervisorIdentity {
            pid: std::process::id(),
            start: "0".into(),
        }
    }

    /// A live stand-in for a supervisor, killed when dropped.
    struct Live(Child);

    impl Live {
        fn new() -> Self {
            Self(Command::new("sleep").arg("30").spawn().unwrap())
        }
        fn identity(&self) -> SupervisorIdentity {
            let pid = self.0.id();
            SupervisorIdentity {
                pid,
                start: worker::process_start(pid).unwrap(),
            }
        }
    }

    impl Drop for Live {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn calls(coordinator: &Coordinator<Fake>) -> Vec<Call> {
        coordinator.transport.calls.lock().unwrap().clone()
    }

    fn completions(coordinator: &Coordinator<Fake>) -> Vec<CompleteRequest> {
        calls(coordinator)
            .into_iter()
            .filter_map(|call| match call {
                Call::Complete(request) => Some(request),
                _ => None,
            })
            .collect()
    }

    fn result(control: &Path, result: SupervisorResult) {
        fs::write(
            control.join("result.json"),
            serde_json::to_vec(&result).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn reconcile_publishes_a_finished_attempt() {
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        let control = launched(&coordinator, "a1", Some(&dead()));
        let directory = control.parent().unwrap();
        fs::write(directory.join("output.log"), "log\n").unwrap();
        let artifacts = json!({"dist/out":{"sha256":"s","mode":0o444,"bytes":3}});
        result(
            &control,
            SupervisorResult {
                version: CONTROL_VERSION,
                outcome: Outcome::Completed,
                reason: Some("timeout".into()),
                exit_code: 124,
                evidence: json!({"exit_code":124,"reason":"timeout","artifacts":artifacts}),
                drained: true,
            },
        );
        // The supervisor timed out on its own; the store must hear of it before the result.
        fs::write(control.join("stopping"), r#"{"reason":"timeout"}"#).unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        let calls = calls(&coordinator);
        assert_eq!(calls.len(), 3, "{calls:?}");
        let Call::Renew(renewal) = &calls[0] else {
            panic!("{calls:?}")
        };
        assert_eq!(renewal.leases.len(), 1);
        assert_eq!(renewal.leases[0].lease, "lease-a1");
        assert_eq!(renewal.leases[0].stopping.as_deref(), Some("timeout"));
        let Call::Complete(request) = &calls[1] else {
            panic!("{calls:?}")
        };
        assert_eq!(
            (request.attempt.as_str(), request.lease.as_str()),
            ("a1", "lease-a1")
        );
        assert_eq!(
            (
                request.outcome,
                request.reason.as_deref(),
                request.exit_code
            ),
            (Outcome::Completed, Some("timeout"), 124)
        );
        assert!(request.drained);
        assert_eq!(
            request.log,
            Some(BlobRef {
                sha256: crate::hash(b"log\n"),
                bytes: 4
            })
        );
        assert_eq!(request.cleanup_log, None);
        assert_eq!(json!(request.artifacts), artifacts);
        assert!(matches!(&calls[2], Call::Retire(attempt) if attempt == "a1"));
        assert!(!coordinator.ledger("a1").exists());
        assert!(coordinator.lock().ledger.is_empty());
    }

    #[test]
    fn reconcile_reports_never_started_dead_supervisor_as_drained() {
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        launched(&coordinator, "a1", Some(&dead()));
        // Spawned but never recorded: the crash came between the spawn and supervisor.json.
        let unrecorded = launched(&coordinator, "a2", None);
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        let requests = completions(&coordinator);
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert_eq!(request.outcome, Outcome::Lost);
            // Its command never started, so nothing of it can be left behind.
            assert!(request.drained, "{}", request.attempt);
            assert_eq!(
                request.evidence["error"],
                "supervisor exited without a result"
            );
        }
        assert_eq!(requests[0].evidence["supervisor"], json!(dead()));
        assert_eq!(requests[1].evidence["supervisor"], Value::Null);
        // A supervisor nobody recorded may still be waiting for registration: it must not start.
        assert_eq!(
            fs::read_to_string(unrecorded.join("stop")).unwrap(),
            r#"{"reason":"coordinator-lost"}"#
        );
        assert!(coordinator.lock().ledger.is_empty());
    }

    #[test]
    fn reconcile_reports_started_dead_supervisor_as_undrained() {
        let temp = tempfile::tempdir().unwrap();
        let coordinator = coordinator(fake(temp.path()));
        let control = launched(&coordinator, "a1", Some(&dead()));
        fs::write(control.join("registered"), "").unwrap();
        fs::write(control.join("command-started"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.publish_queued().unwrap();
        let requests = completions(&coordinator);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].outcome, Outcome::Lost);
        // Its job may have left processes or external resources: capacity stays held.
        assert!(!requests[0].drained);
        assert!(!control.join("stop").exists());
    }

    #[test]
    fn fenced_renew_stops_live_supervisor_then_reports_drain() {
        let temp = tempfile::tempdir().unwrap();
        let mut fake = fake(temp.path());
        fake.renewal = "fenced";
        fake.completion = "fenced";
        let coordinator = coordinator(fake);
        let supervisor = Live::new();
        let control = launched(&coordinator, "a1", Some(&supervisor.identity()));
        fs::write(control.join("registered"), "").unwrap();
        fs::write(control.join("command-started"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        coordinator.poll(Instant::now());
        assert!(coordinator.lock().queue.is_empty(), "a live supervisor");
        coordinator.heartbeat_once().unwrap();
        assert_eq!(
            fs::read_to_string(control.join("stop")).unwrap(),
            r#"{"reason":"fenced"}"#
        );
        assert!(completions(&coordinator).is_empty());
        // The supervisor stops its job, cleans up and reports the attempt drained.
        result(
            &control,
            SupervisorResult {
                version: CONTROL_VERSION,
                outcome: Outcome::Completed,
                reason: Some("fenced".into()),
                exit_code: 124,
                evidence: json!({"exit_code":124,"reason":"fenced"}),
                drained: true,
            },
        );
        drop(supervisor);
        coordinator.poll(Instant::now());
        let responses = coordinator.publish_queued().unwrap();
        assert_eq!(responses.len(), 1);
        assert!(responses[0].drain_recorded);
        let requests = completions(&coordinator);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].reason.as_deref(), Some("fenced"));
        assert!(requests[0].drained);
        assert!(
            calls(&coordinator)
                .iter()
                .any(|call| matches!(call, Call::Retire(attempt) if attempt == "a1"))
        );
        assert!(coordinator.lock().ledger.is_empty());
    }

    #[test]
    fn partition_deadline_stops_jobs_before_lease_expiry() {
        let temp = tempfile::tempdir().unwrap();
        let mut fake = fake(temp.path());
        fake.renewal = "unreachable";
        let coordinator = coordinator(fake);
        let supervisor = Live::new();
        let control = launched(&coordinator, "a1", Some(&supervisor.identity()));
        fs::write(control.join("registered"), "").unwrap();
        assert!(coordinator.reconcile(false).unwrap().is_empty());
        let last = coordinator.lock().last_renewal;
        assert!(matches!(
            coordinator.heartbeat_once(),
            Err(TransportError::Transient(_))
        ));
        assert_eq!(coordinator.lock().last_renewal, last);
        // Lease 10 s, heartbeat 1 s: the margin is max(2 × 1 s, 10 s / 5) = 2 s.
        coordinator.poll(last + Duration::from_millis(7_900));
        assert!(!control.join("stop").exists());
        coordinator.poll(last + Duration::from_millis(8_100));
        assert_eq!(
            fs::read_to_string(control.join("stop")).unwrap(),
            r#"{"reason":"partition"}"#
        );
        // The job keeps its lease until it reports; nothing is completed yet.
        assert!(completions(&coordinator).is_empty());
        assert!(coordinator.lock().partitioned);
    }

    #[test]
    fn pending_claim_is_replayed_first() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        let pending = fake.control.join("pending-claim");
        fake.claims.lock().unwrap().extend([
            ClaimResponse {
                claim: None,
                fenced: None,
                unverified: vec![],
            },
            ClaimResponse {
                claim: Some(claim("a1")),
                fenced: None,
                unverified: vec![],
            },
        ]);
        let coordinator = coordinator(fake);
        // Written before a crash lost the response to it.
        fs::write(&pending, r#"{"key":"replayed"}"#).unwrap();
        assert_eq!(coordinator.claim().unwrap(), Claimed::Idle);
        // A poll without work keeps the key.
        assert!(pending.exists());
        assert_eq!(coordinator.claim().unwrap(), Claimed::Attempt("a1".into()));
        assert!(!pending.exists());
        let ledger: Claim =
            serde_json::from_slice(&fs::read(coordinator.ledger("a1")).unwrap()).unwrap();
        assert_eq!(ledger.attempt, claim("a1").attempt);
        assert_eq!(coordinator.lock().ledger["a1"].phase, Phase::Materializing);
        assert_eq!(coordinator.claim().unwrap(), Claimed::Idle);
        let keys: Vec<String> = calls(&coordinator)
            .into_iter()
            .filter_map(|call| match call {
                Call::Claim(request) => Some(request.claim_key),
                _ => None,
            })
            .collect();
        assert_eq!(keys[..2], ["replayed", "replayed"]);
        assert_ne!(keys[2], "replayed");
        let next: Value = serde_json::from_slice(&fs::read(&pending).unwrap()).unwrap();
        assert_eq!(next["key"], keys[2]);
    }

    #[test]
    fn drain_waits_for_unverified_profiles() {
        let temp = tempfile::tempdir().unwrap();
        let fake = fake(temp.path());
        fake.claims.lock().unwrap().push_back(ClaimResponse {
            claim: None,
            fenced: None,
            unverified: vec!["p-ok".into(), "p-bad".into(), "p-gone".into()],
        });
        let coordinator = Arc::new(coordinator(fake));
        assert_eq!(
            coordinator.coordinate().unwrap(),
            json!({"launched":0,"executor":"mindy"})
        );
        let calls = calls(&coordinator);
        assert_eq!(calls.len(), 3, "{calls:?}");
        let Call::Claim(first) = &calls[0] else {
            panic!("{calls:?}")
        };
        assert!(first.verified.is_empty() && first.refused.is_empty());
        assert!(matches!(&calls[1], Call::Hello(ids) if ids == &["p-ok", "p-bad", "p-gone"]));
        // Asked again at once, with every profile judged; only then is the queue empty.
        let Call::Claim(second) = &calls[2] else {
            panic!("{calls:?}")
        };
        assert_eq!(second.claim_key, first.claim_key);
        assert_eq!(second.verified, ["p-ok"]);
        assert_eq!(
            second.refused.keys().collect::<Vec<_>>(),
            ["p-bad", "p-gone"]
        );
        assert!(second.refused["p-bad"].contains("runner binary changed"));
        assert_eq!(second.engine.as_deref(), Some("e"));
    }
}
