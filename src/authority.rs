//! The only code that changes attempt and lease state. The local worker calls it in-process;
//! remote executors reach the same functions through the authenticated API.
use crate::{
    engine,
    model::*,
    now,
    store::{self, Store, event, record},
    unique,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

pub const LOCAL: &str = "local";
pub const DEFAULT_LEASE_SECONDS: u64 = 300;
const MAX_UNVERIFIED: usize = 32;
/// The executor of an attempt row. Attempts claimed before executors existed are local.
const EXECUTOR: &str = "COALESCE(json_extract(body,'$.executor'),'local')";

/// Projects and profiles an executor may run; `None` is unrestricted.
#[derive(Debug, Clone, Default)]
pub struct Scope {
    pub projects: Option<BTreeSet<String>>,
    pub profiles: Option<BTreeSet<String>>,
}

impl Scope {
    pub fn all() -> Self {
        Self::default()
    }
    pub fn allows_profile(&self, profile: &str) -> bool {
        self.profiles.as_ref().is_none_or(|p| p.contains(profile))
    }
    pub fn allows_project(&self, project: &str) -> bool {
        self.projects.as_ref().is_none_or(|p| p.contains(project))
    }
}

#[derive(Debug)]
pub enum AuthorityError {
    Invalid(String),
    Forbidden,
    Unregistered,
    Fenced,
    CompletionConflict,
    MissingBlobs(Vec<String>),
    InvalidEvidence(String),
    Internal(anyhow::Error),
}

impl std::fmt::Display for AuthorityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) | Self::InvalidEvidence(message) => write!(f, "{message}"),
            Self::Forbidden => write!(f, "permission does not cover this operation"),
            Self::Unregistered => write!(f, "executor is not registered"),
            Self::Fenced => write!(f, "attempt is fenced"),
            Self::CompletionConflict => write!(f, "attempt was completed by a different request"),
            Self::MissingBlobs(missing) => write!(f, "uploads missing: {}", missing.join(",")),
            Self::Internal(error) => write!(f, "{error:#}"),
        }
    }
}
impl std::error::Error for AuthorityError {}
impl From<anyhow::Error> for AuthorityError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error)
    }
}
impl From<rusqlite::Error> for AuthorityError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Internal(error.into())
    }
}
impl From<serde_json::Error> for AuthorityError {
    fn from(error: serde_json::Error) -> Self {
        Self::Internal(error.into())
    }
}

/// Additive and idempotent; the store schema itself is unchanged.
pub fn prepare(db: &Connection) -> Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS executors (id TEXT PRIMARY KEY, body TEXT NOT NULL);
         CREATE INDEX IF NOT EXISTS attempts_held ON attempts(drained, state);
         CREATE INDEX IF NOT EXISTS attempts_claim_key ON attempts(json_extract(body,'$.claim_key'));",
    )?;
    db.execute(
        "INSERT OR IGNORE INTO executors VALUES (?,?)",
        params![LOCAL, serde_json::to_string(&default_record(LOCAL))?],
    )?;
    Ok(())
}

pub fn default_record(id: &str) -> ExecutorRecord {
    ExecutorRecord {
        id: id.into(),
        slots: None,
        memory_mb: None,
        lease_seconds: DEFAULT_LEASE_SECONDS,
        placement: Placement::default(),
        created: now(),
        observed: Observed::default(),
    }
}

fn table_exists(db: &Connection, name: &str) -> Result<bool> {
    let count: i64 = db.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        [name],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

/// A store not yet prepared knows only the local executor.
pub fn load(db: &Connection, id: &str) -> Result<Option<ExecutorRecord>> {
    if !table_exists(db, "executors")? {
        return Ok((id == LOCAL).then(|| default_record(LOCAL)));
    }
    let body: Option<String> = db
        .query_row("SELECT body FROM executors WHERE id=?", [id], |r| r.get(0))
        .optional()?;
    Ok(body.map(|body| serde_json::from_str(&body)).transpose()?)
}

pub fn save(db: &Connection, record: &ExecutorRecord) -> Result<()> {
    db.execute(
        "INSERT INTO executors VALUES (?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
        params![record.id, serde_json::to_string(record)?],
    )?;
    Ok(())
}

/// The executor's total budget. Memory never exceeds the cgroup limit its coordinator reported.
pub fn capacity(db: &Connection, record: &ExecutorRecord) -> Result<Reservation> {
    let (slots, memory_mb) = if record.id == LOCAL {
        store::capacity(db)?
    } else {
        (
            record.slots.context("executor has no slots")?,
            record.memory_mb.context("executor has no memory")?,
        )
    };
    let memory_mb = record
        .observed
        .memory_limit_mb
        .map_or(memory_mb, |limit| limit.min(memory_mb));
    Ok(Reservation { slots, memory_mb })
}

/// Reservations of this executor's undrained attempts. Other executors' holds never count.
pub fn held(db: &Connection, executor: &str) -> Result<Reservation> {
    Ok(db.query_row(
        &format!(
            "SELECT COALESCE(SUM(json_extract(body,'$.slots')),0),
             COALESCE(SUM(json_extract(body,'$.memory_mb')),0)
             FROM attempts WHERE drained=0 AND {EXECUTOR}=?"
        ),
        [executor],
        |r| {
            Ok(Reservation {
                slots: r.get(0)?,
                memory_mb: r.get(1)?,
            })
        },
    )?)
}

pub fn fits(held: Reservation, wanted: Reservation, capacity: Reservation) -> bool {
    held.slots + wanted.slots <= capacity.slots
        && held.memory_mb + wanted.memory_mb <= capacity.memory_mb
}

pub struct Choice {
    pub owner: String,
    pub action: Action,
    pub reservation: Reservation,
    pub timeout_seconds: u64,
}

pub struct Selection {
    pub choice: Option<Choice>,
    /// Profiles of admissible work the executor has not verified (at most 32).
    pub unverified: Vec<String>,
}

/// The head of one executor's queue: work it may run and could ever fit, ordered by owner
/// priority then fair share. An unverified head admits nothing until it is verified, so
/// verification lag never reorders admission.
pub fn select(
    db: &Connection,
    record: &ExecutorRecord,
    scope: &Scope,
    verified: &BTreeSet<String>,
    refused: &BTreeSet<String>,
    capacity: Reservation,
) -> Result<Selection> {
    let queued: Vec<Action> = {
        let mut stmt =
            db.prepare("SELECT body FROM actions WHERE state='queued' ORDER BY created,id")?;
        stmt.query_map([], |r| r.get::<_, String>(0))?
            .map(|v| Ok(serde_json::from_str(&v?)?))
            .collect::<Result<_>>()?
    };
    let priorities = owner_priorities(db)?;
    let mut profiles = BTreeMap::new();
    let mut unverified = Vec::new();
    let mut choices = Vec::new();
    for action in queued {
        if !scope.allows_profile(&action.profile)
            || refused.contains(&action.profile)
            || !record.placement.permits(&action.job)
            || !engine::eligible(db, &action)?
        {
            continue;
        }
        if scope.projects.is_some() {
            let project: String = db.query_row(
                "SELECT project FROM revisions WHERE id=?",
                [&action.revision],
                |r| r.get(0),
            )?;
            if !scope.allows_project(&project) {
                continue;
            }
        }
        let (reservation, timeout_seconds) = budget(db, &mut profiles, &action)?;
        // Work this executor can never fit must not idle it behind the head.
        if reservation.slots > capacity.slots || reservation.memory_mb > capacity.memory_mb {
            continue;
        }
        let trusted = verified.contains(&action.profile);
        if !trusted && unverified.len() < MAX_UNVERIFIED && !unverified.contains(&action.profile) {
            unverified.push(action.profile.clone());
        }
        let mut stmt =
            db.prepare("SELECT DISTINCT owner FROM interests WHERE action=? AND active=1")?;
        let owners: Vec<String> = stmt
            .query_map([&action.id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for owner in owners {
            let served = db
                .query_row("SELECT last FROM dispatches WHERE owner=?", [&owner], |r| {
                    r.get::<_, f64>(0)
                })
                .optional()?
                .unwrap_or(0.0);
            let priority = priorities.get(&owner).copied().unwrap_or(0);
            let choice = Choice {
                owner,
                action: action.clone(),
                reservation,
                timeout_seconds,
            };
            choices.push((priority, served, trusted, choice));
        }
    }
    // Higher operator priority first (a landing-ready train), then fair share between owners.
    choices.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(a.1.total_cmp(&b.1))
            .then(a.3.action.created.total_cmp(&b.3.action.created))
            .then(a.3.owner.cmp(&b.3.owner))
    });
    let choice = choices
        .into_iter()
        .next()
        .filter(|(_, _, trusted, _)| *trusted)
        .map(|(.., choice)| choice);
    Ok(Selection { choice, unverified })
}

/// Legacy actions carry neither field; their creating profile's job values apply.
fn budget(
    db: &Connection,
    profiles: &mut BTreeMap<String, RegisteredProfile>,
    action: &Action,
) -> Result<(Reservation, u64)> {
    if let (Some(reservation), Some(identity)) = (action.reservation, &action.identity) {
        return Ok((reservation, identity.timeout_seconds));
    }
    let profile = match profiles.entry(action.profile.clone()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(record(db, "profiles", &action.profile)?),
    };
    let job = engine::job(profile, &action.job)?;
    Ok((
        action.reservation.unwrap_or_else(|| Reservation::from(job)),
        action
            .identity
            .as_ref()
            .map_or(job.timeout_seconds, |i| i.timeout_seconds),
    ))
}

fn owner_priorities(db: &Connection) -> Result<BTreeMap<String, i64>> {
    if !table_exists(db, "owner_priorities")? {
        return Ok(BTreeMap::new());
    }
    let mut stmt = db.prepare("SELECT owner, priority FROM owner_priorities")?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Admit the head of this executor's queue. The same claim key returns the attempt it already
/// claimed, so a lost response never claims twice.
pub fn claim_for(
    store: &mut Store,
    executor: &str,
    scope: &Scope,
    request: &ClaimRequest,
) -> Result<ClaimResponse, AuthorityError> {
    let key = &request.claim_key;
    if key.is_empty() || key.len() > 128 || !key.bytes().all(|c| c.is_ascii_graphic()) {
        return Err(AuthorityError::Invalid(
            "A 1–128 byte visible-ASCII claim key is required.".into(),
        ));
    }
    let verified: BTreeSet<String> = request.verified.iter().cloned().collect();
    if !verified.iter().all(|profile| scope.allows_profile(profile)) {
        return Err(AuthorityError::Forbidden);
    }
    let transaction = store.transaction()?;
    let mut record = load(&transaction, executor)?.ok_or(AuthorityError::Unregistered)?;
    observe(&transaction, &mut record, request, verified.clone())?;
    if let Some(response) = replay(&transaction, executor, key)? {
        transaction.commit()?;
        return Ok(response);
    }
    let capacity = capacity(&transaction, &record)?;
    let refused = request.refused.keys().cloned().collect();
    let Selection { choice, unverified } =
        select(&transaction, &record, scope, &verified, &refused, capacity)?;
    let choice = match choice {
        Some(choice) if fits(held(&transaction, executor)?, choice.reservation, capacity) => choice,
        // Drain for the head: later small actions cannot starve its reservation.
        _ => {
            transaction.commit()?;
            return Ok(ClaimResponse {
                claim: None,
                fenced: None,
                unverified,
            });
        }
    };
    let created = now();
    let attempt = Attempt {
        id: unique(),
        action: choice.action.id.clone(),
        worker_pid: 0,
        worker_start: String::new(),
        supervisor_pid: None,
        supervisor_start: None,
        slots: choice.reservation.slots,
        memory_mb: choice.reservation.memory_mb,
        created,
        started: None,
        ended: None,
        exit_code: None,
        result: None,
        timeout_seconds: Some(choice.timeout_seconds),
        executor: Some(executor.into()),
        lease: Some(unique()),
        lease_expires: Some(created + record.lease_seconds as f64),
        claim_key: Some(key.clone()),
        completion: None,
        stop: None,
    };
    transaction.execute(
        "INSERT INTO attempts VALUES (?,?,'claimed',0,?)",
        params![attempt.id, attempt.action, serde_json::to_string(&attempt)?],
    )?;
    transaction.execute(
        "UPDATE actions SET state='running',attempt=? WHERE id=? AND state='queued'",
        params![attempt.id, attempt.action],
    )?;
    // Fair share is global: every executor's claims serve the same owner history.
    transaction.execute(
        "INSERT INTO dispatches VALUES (?,?) ON CONFLICT(owner) DO UPDATE SET last=excluded.last",
        params![choice.owner, now()],
    )?;
    event(
        &transaction,
        "attempt.claimed",
        &attempt.id,
        &json!({"action":attempt.action,"owner":choice.owner,"executor":executor,
                "slots":attempt.slots,"memory_mb":attempt.memory_mb,
                "timeout_seconds":choice.timeout_seconds}),
    )?;
    let claim = claim_of(&transaction, &attempt, choice.action)?;
    transaction.commit()?;
    Ok(ClaimResponse {
        claim: Some(claim),
        fenced: None,
        unverified,
    })
}

/// Steady polling rewrites the record at most every 30 s; changed verification is an event.
fn observe(
    db: &Connection,
    record: &mut ExecutorRecord,
    request: &ClaimRequest,
    verified: BTreeSet<String>,
) -> Result<()> {
    let seen = now();
    let observed = Observed {
        last_seen: Some(seen),
        verified,
        refused: request.refused.clone(),
        memory_limit_mb: request.memory_limit_mb,
        disk_free_mb: request.disk_free_mb,
        host: request.host.clone(),
        engine: request.engine.clone(),
    };
    let previous = &record.observed;
    let profiles_changed =
        previous.verified != observed.verified || previous.refused != observed.refused;
    if !profiles_changed
        && previous.memory_limit_mb == observed.memory_limit_mb
        && previous.host == observed.host
        && previous.engine == observed.engine
        && previous.last_seen.is_some_and(|last| seen - last <= 30.0)
    {
        return Ok(());
    }
    db.execute(
        "UPDATE executors SET body=json_set(body,'$.observed',json(?)) WHERE id=?",
        params![serde_json::to_string(&observed)?, record.id],
    )?;
    if profiles_changed {
        event(
            db,
            "executor.observed",
            &record.id,
            &json!({"verified":observed.verified,"refused":observed.refused}),
        )?;
    }
    record.observed = observed;
    Ok(())
}

/// A key already used by this executor: a still-held attempt is the same claim again, any other
/// is reported fenced. Only expiry judges lease time, so no clock is compared here.
fn replay(db: &Connection, executor: &str, key: &str) -> Result<Option<ClaimResponse>> {
    let row: Option<(String, String, bool, String)> = db
        .query_row(
            &format!(
                "SELECT id,state,drained,body FROM attempts
                 WHERE json_extract(body,'$.claim_key')=?1 AND {EXECUTOR}=?2"
            ),
            params![key, executor],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((id, state, drained, body)) = row else {
        return Ok(None);
    };
    let claim = if !drained && ["claimed", "running"].contains(&state.as_str()) {
        let attempt: Attempt = serde_json::from_str(&body)?;
        let action = record(db, "actions", &attempt.action)?;
        Some(claim_of(db, &attempt, action)?)
    } else {
        None
    };
    let fenced = claim.is_none().then_some(id);
    Ok(Some(ClaimResponse {
        claim,
        fenced,
        unverified: Vec::new(),
    }))
}

/// Everything a coordinator needs to run the attempt; rebuilt from the store on replay.
fn claim_of(db: &Connection, attempt: &Attempt, action: Action) -> Result<Claim> {
    let profile: RegisteredProfile = record(db, "profiles", &action.profile)?;
    let revision: Revision = record(db, "revisions", &action.revision)?;
    let mut dependencies = Vec::new();
    for dependency in &action.dependencies {
        let (state, producer): (String, Option<String>) = db.query_row(
            "SELECT state,attempt FROM actions WHERE id=?",
            [dependency],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(state == "passed", "dependency no longer passes");
        let producer = producer.context("passed dependency lacks an attempt")?;
        let evidence: Value = record::<Attempt>(db, "attempts", &producer)?
            .result
            .context("dependency lacks evidence")?;
        let artifacts = serde_json::from_value(evidence["artifacts"].clone())
            .context("dependency lacks artifacts")?;
        dependencies.push(DependencyEvidence {
            action: dependency.clone(),
            attempt: producer,
            artifacts,
        });
    }
    Ok(Claim {
        attempt: Grant {
            id: attempt.id.clone(),
            lease: attempt.lease.clone().context("attempt has no lease")?,
            lease_expires: attempt.lease_expires.context("attempt has no lease")?,
            slots: attempt.slots,
            memory_mb: attempt.memory_mb,
            timeout_seconds: attempt.timeout_seconds.context("attempt has no timeout")?,
        },
        action,
        profile,
        revision: RevisionRef {
            id: revision.id,
            project: revision.project,
            commit: revision.commit,
        },
        dependencies,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty() -> (tempfile::TempDir, Store) {
        let root = tempfile::tempdir().unwrap();
        let store = Store::init(root.path(), 4, 4096).unwrap();
        (root, store)
    }

    fn rows(db: &Connection) -> Vec<(String, String)> {
        let mut stmt = db
            .prepare("SELECT id,body FROM executors ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    #[test]
    fn prepare_is_idempotent_and_seeds_local() {
        let (_root, store) = empty();
        // A store created before executors existed reads as the local executor only.
        assert_eq!(load(&store.db, LOCAL).unwrap().unwrap().id, LOCAL);
        assert!(load(&store.db, "mindy").unwrap().is_none());
        prepare(&store.db).unwrap();
        let seeded = rows(&store.db);
        assert_eq!(seeded.len(), 1);
        let local: ExecutorRecord = serde_json::from_str(&seeded[0].1).unwrap();
        assert_eq!(
            (local.id.as_str(), local.slots, local.memory_mb),
            (LOCAL, None, None)
        );
        assert_eq!(local.lease_seconds, DEFAULT_LEASE_SECONDS);
        assert_eq!(local.placement, Placement::default());
        assert_eq!(local.observed, Observed::default());
        let mindy = ExecutorRecord {
            slots: Some(2),
            memory_mb: Some(2048),
            ..default_record("mindy")
        };
        save(&store.db, &mindy).unwrap();
        let configured = rows(&store.db);
        prepare(&store.db).unwrap();
        prepare(&store.db).unwrap();
        assert_eq!(rows(&store.db), configured);
        assert_eq!(configured[0].1, seeded[0].1);
        assert_eq!(load(&store.db, "mindy").unwrap(), Some(mindy));
        let indexes: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index'
                 AND name IN ('attempts_held','attempts_claim_key')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(indexes, 2);
        // The claim replay lookup must hit the expression index, not scan every attempt.
        let plan: String = store
            .db
            .query_row(
                &format!(
                    "EXPLAIN QUERY PLAN SELECT id FROM attempts
                     WHERE json_extract(body,'$.claim_key')=?1 AND {EXECUTOR}=?2"
                ),
                params!["key", LOCAL],
                |r| r.get(3),
            )
            .unwrap();
        assert!(plan.contains("attempts_claim_key"), "{plan}");
        // The API boundary prepares the same table.
        let (_other, fresh) = empty();
        crate::api::Api::open(&fresh.root).unwrap();
        assert_eq!(rows(&fresh.db).len(), 1);
    }

    #[test]
    fn executor_record_defaults_round_trip() {
        // Only identity, lease and creation are required; capacity, placement and observations default.
        let minimal: ExecutorRecord =
            serde_json::from_value(json!({"id":"mindy","lease_seconds":300,"created":1.5}))
                .unwrap();
        assert_eq!((minimal.slots, minimal.memory_mb), (None, None));
        assert_eq!(minimal.placement, Placement::default());
        assert_eq!(minimal.observed, Observed::default());
        let text = serde_json::to_string(&minimal).unwrap();
        assert_eq!(
            serde_json::from_str::<ExecutorRecord>(&text).unwrap(),
            minimal
        );
        let local = default_record(LOCAL);
        assert_eq!(
            serde_json::from_str::<ExecutorRecord>(&serde_json::to_string(&local).unwrap())
                .unwrap(),
            local
        );
        let full = ExecutorRecord {
            id: "mindy".into(),
            slots: Some(3),
            memory_mb: Some(12288),
            lease_seconds: 60,
            placement: Placement {
                allow: Some(["test".to_owned()].into()),
                deny: ["prepare".to_owned(), "images".to_owned()].into(),
            },
            created: 1_792_511_574.900_662_7,
            observed: Observed {
                last_seen: Some(1_792_511_600.25),
                verified: ["f-1".to_owned()].into(),
                refused: [("f-2".to_owned(), "tool drift".to_owned())].into(),
                memory_limit_mb: Some(10240),
                disk_free_mb: Some(70_000),
                host: Some("mindy".into()),
                engine: Some("e".repeat(64)),
            },
        };
        let text = serde_json::to_string(&full).unwrap();
        assert_eq!(serde_json::from_str::<ExecutorRecord>(&text).unwrap(), full);
        // Configuration typos fail loudly instead of silently widening placement or capacity.
        for typo in [
            json!({"id":"x","lease_seconds":300,"created":0.0,"slot":2}),
            json!({"id":"x","lease_seconds":300,"created":0.0,"placement":{"alow":["test"]}}),
        ] {
            assert!(serde_json::from_value::<ExecutorRecord>(typo).is_err());
        }
    }

    /// Two projects, one revision each, profiles `f-1`..`f-3` (jobs `build`, `test`) and
    /// `f-wide` whose `wide` job reserves two slots. Local capacity is 4 slots, 4096 MiB.
    fn lab() -> (tempfile::TempDir, Store) {
        let (root, store) = empty();
        prepare(&store.db).unwrap();
        store
            .db
            .execute_batch(
                "INSERT INTO projects VALUES ('p-a','alpha','/dev/null'),('p-b','beta','/dev/null')",
            )
            .unwrap();
        for (id, project) in [("r-a", "p-a"), ("r-b", "p-b")] {
            let revision = json!({"id":id,"project":project,"commit":"c".repeat(40),"tree":"t",
                "parents":[],"owner":"author","message":"m","created":0.0});
            store
                .db
                .execute(
                    "INSERT INTO revisions VALUES (?,?,?)",
                    params![id, project, revision.to_string()],
                )
                .unwrap();
        }
        for id in ["f-1", "f-2", "f-3", "f-wide"] {
            let profile = json!({"id":id,"engine":"e","platform":"linux-x86_64","tools":{},
                "definition":{"version":1,"name":id,"tools":{},"jobs":[
                    {"name":"build","command":["sh"]},{"name":"test","command":["sh"]},
                    {"name":"wide","command":["sh"],"slots":2,"memory_mb":512,"timeout_seconds":90}]}});
            crate::store::insert(&store.db, "profiles", id, &profile).unwrap();
        }
        (root, store)
    }

    struct Queue<'a> {
        id: &'a str,
        revision: &'a str,
        profile: &'a str,
        job: &'a str,
        owner: &'a str,
        created: f64,
        /// None writes a legacy action whose reservation and timeout come from its profile.
        reservation: Option<(u32, u32)>,
    }

    impl Default for Queue<'_> {
        fn default() -> Self {
            Self {
                id: "",
                revision: "r-a",
                profile: "f-1",
                job: "test",
                owner: "lane",
                created: 1.0,
                reservation: Some((1, 512)),
            }
        }
    }

    fn queue(db: &Connection, q: Queue) {
        let action = Action {
            id: q.id.into(),
            key: q.id.into(),
            revision: q.revision.into(),
            profile: q.profile.into(),
            job: q.job.into(),
            dependencies: vec![],
            created: q.created,
            identity: q.reservation.map(|_| ActionIdentity {
                version: JOB_RECIPE.into(),
                recipe: "recipe".into(),
                class: "class".into(),
                timeout_seconds: 60,
            }),
            reservation: q
                .reservation
                .map(|(slots, memory_mb)| Reservation { slots, memory_mb }),
            origin: None,
        };
        db.execute(
            "INSERT INTO actions VALUES (?,?,'queued',NULL,?,?)",
            params![
                q.id,
                q.id,
                serde_json::to_string(&action).unwrap(),
                q.created
            ],
        )
        .unwrap();
        db.execute(
            "INSERT INTO interests VALUES (?,?,'',1)",
            params![q.id, q.owner],
        )
        .unwrap();
    }

    /// A held attempt: `executor` None writes a body from before executors existed.
    fn hold(db: &Connection, id: &str, executor: Option<&str>, slots: u32, memory_mb: u32) {
        let mut body = json!({"id":id,"action":"elsewhere","worker_pid":1,"worker_start":"1",
            "supervisor_pid":null,"supervisor_start":null,"slots":slots,"memory_mb":memory_mb,
            "created":0.0,"started":null,"ended":null,"exit_code":null,"result":null});
        if let Some(executor) = executor {
            body["executor"] = json!(executor);
        }
        db.execute(
            "INSERT INTO attempts VALUES (?,'elsewhere','running',0,?)",
            params![id, body.to_string()],
        )
        .unwrap();
    }

    fn register(db: &Connection, id: &str, slots: u32, memory_mb: u32) -> ExecutorRecord {
        let record = ExecutorRecord {
            slots: Some(slots),
            memory_mb: Some(memory_mb),
            ..default_record(id)
        };
        save(db, &record).unwrap();
        record
    }

    fn ask(key: &str, verified: &[&str]) -> ClaimRequest {
        ClaimRequest {
            claim_key: key.into(),
            verified: verified.iter().map(|v| v.to_string()).collect(),
            ..ClaimRequest::default()
        }
    }

    fn claimed(response: &ClaimResponse) -> Option<&str> {
        response.claim.as_ref().map(|c| c.action.id.as_str())
    }

    fn state(db: &Connection, action: &str) -> String {
        crate::engine::action_state(db, action).unwrap()
    }

    fn events(db: &Connection, kind: &str) -> Vec<(String, Value)> {
        let mut stmt = db
            .prepare("SELECT subject,detail FROM events WHERE kind=? ORDER BY seq")
            .unwrap();
        stmt.query_map([kind], |r| {
            Ok((
                r.get(0)?,
                serde_json::from_str(&r.get::<_, String>(1)?).unwrap(),
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    #[test]
    fn claim_for_counts_only_its_own_executor() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 1, 1024);
        // The local budget is fully held by an attempt from before executors existed.
        hold(&store.db, "legacy", None, 4, 4096);
        queue(
            &store.db,
            Queue {
                id: "first",
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "second",
                created: 2.0,
                ..Queue::default()
            },
        );
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        let claim = response.claim.unwrap();
        assert_eq!(claim.action.id, "first");
        assert_eq!(
            (claim.revision.id.as_str(), claim.revision.project.as_str()),
            ("r-a", "p-a")
        );
        assert_eq!(claim.profile.id, "f-1");
        assert!(claim.dependencies.is_empty());
        let grant = &claim.attempt;
        assert_eq!(
            (grant.slots, grant.memory_mb, grant.timeout_seconds),
            (1, 512, 60)
        );
        let attempt = store.attempt(&grant.id).unwrap();
        assert_eq!(attempt.executor.as_deref(), Some("mindy"));
        assert_eq!(attempt.lease.as_deref(), Some(grant.lease.as_str()));
        assert_eq!(attempt.lease_expires, Some(grant.lease_expires));
        assert!((grant.lease_expires - now() - DEFAULT_LEASE_SECONDS as f64).abs() < 5.0);
        assert_eq!(attempt.claim_key.as_deref(), Some("k1"));
        assert_eq!((attempt.worker_pid, attempt.worker_start.as_str()), (0, ""));
        assert_eq!(attempt.timeout_seconds, Some(60));
        let row: (String, String) = store
            .db
            .query_row(
                "SELECT state,attempt FROM actions WHERE id='first'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, ("running".into(), grant.id.clone()));
        assert_eq!(
            store
                .db
                .query_row("SELECT state FROM attempts WHERE id=?", [&grant.id], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
            "claimed"
        );
        let claims = events(&store.db, "attempt.claimed");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].0, grant.id);
        assert_eq!(
            claims[0].1,
            json!({"action":"first","owner":"lane","executor":"mindy","slots":1,
                   "memory_mb":512,"timeout_seconds":60})
        );
        let held_by = |store: &Store, executor: &str| held(&store.db, executor).unwrap();
        assert_eq!(
            held_by(&store, "mindy"),
            Reservation {
                slots: 1,
                memory_mb: 512
            }
        );
        assert_eq!(
            held_by(&store, LOCAL),
            Reservation {
                slots: 4,
                memory_mb: 4096
            }
        );
        // mindy is full; its hold never reaches the local budget and vice versa.
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert!(response.claim.is_none() && response.fenced.is_none());
        assert_eq!(state(&store.db, "second"), "queued");
        assert!(crate::worker::claim(&mut store).unwrap().is_none());
        store
            .db
            .execute("UPDATE attempts SET drained=1 WHERE id='legacy'", [])
            .unwrap();
        assert_eq!(
            held_by(&store, LOCAL),
            Reservation {
                slots: 0,
                memory_mb: 0
            }
        );
        let local = crate::worker::claim(&mut store).unwrap().unwrap();
        assert_eq!(local.action, "second");
        assert_eq!(
            held_by(&store, LOCAL),
            Reservation {
                slots: 1,
                memory_mb: 512
            }
        );
        assert_eq!(
            held_by(&store, "mindy"),
            Reservation {
                slots: 1,
                memory_mb: 512
            }
        );
    }

    #[test]
    fn claim_skips_actions_that_never_fit_the_executor() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 1, 1024);
        // Older than everything else: without the fit rule each would idle mindy forever.
        queue(
            &store.db,
            Queue {
                id: "heavy",
                created: 0.5,
                reservation: Some((1, 2048)),
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "wide",
                profile: "f-wide",
                job: "wide",
                reservation: None,
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "small",
                created: 2.0,
                ..Queue::default()
            },
        );
        let verified = ["f-1", "f-wide"];
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k", &verified)).unwrap();
        assert_eq!(claimed(&response), Some("small"));
        assert_eq!(state(&store.db, "heavy"), "queued");
        assert_eq!(state(&store.db, "wide"), "queued");
        // The legacy job's two slots fit the local executor, which still admits it in order.
        let local = default_record(LOCAL);
        let selection = select(
            &store.db,
            &local,
            &Scope::all(),
            &verified.map(String::from).into(),
            &BTreeSet::new(),
            capacity(&store.db, &local).unwrap(),
        )
        .unwrap();
        let head = selection.choice.unwrap();
        assert_eq!(head.action.id, "heavy");
        let heavy = crate::worker::claim(&mut store).unwrap().unwrap();
        assert_eq!(heavy.action, "heavy");
        let wide = crate::worker::claim(&mut store).unwrap().unwrap();
        assert_eq!(wide.action, "wide");
        assert_eq!(
            (wide.slots, wide.memory_mb, wide.timeout_seconds),
            (2, 512, Some(90))
        );
    }

    #[test]
    fn claim_still_drains_for_a_fitting_head_action() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 2, 2048);
        hold(&store.db, "busy", Some("mindy"), 1, 512);
        queue(
            &store.db,
            Queue {
                id: "wide",
                reservation: Some((2, 512)),
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "small",
                created: 2.0,
                ..Queue::default()
            },
        );
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        // The head fits mindy's total but not its free capacity: nothing smaller jumps ahead.
        assert!(response.claim.is_none() && response.unverified.is_empty());
        assert_eq!(state(&store.db, "small"), "queued");
        store
            .db
            .execute("UPDATE attempts SET drained=1 WHERE id='busy'", [])
            .unwrap();
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("wide"));
    }

    #[test]
    fn claim_filters_scope_verified_and_placement() {
        let (_root, mut store) = lab();
        let mut mindy = register(&store.db, "mindy", 4, 4096);
        mindy.placement.deny.insert("build".into());
        save(&store.db, &mindy).unwrap();
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "other-project",
                revision: "r-b",
                created: 1.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "other-profile",
                profile: "f-2",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "denied-job",
                job: "build",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "refused",
                profile: "f-3",
                created: 4.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "allowed",
                created: 5.0,
                ..base
            },
        );
        let scope = Scope {
            projects: Some(["p-a".to_owned()].into()),
            profiles: Some(["f-1".to_owned(), "f-3".to_owned()].into()),
        };
        let outside = claim_for(&mut store, "mindy", &scope, &ask("k0", &["f-1", "f-2"]));
        assert!(matches!(outside, Err(AuthorityError::Forbidden)));
        let mut request = ask("k1", &["f-1"]);
        request.refused.insert("f-3".into(), "tool drift".into());
        let response = claim_for(&mut store, "mindy", &scope, &request).unwrap();
        assert_eq!(claimed(&response), Some("allowed"));
        assert!(response.unverified.is_empty());
        for id in ["other-project", "other-profile", "denied-job", "refused"] {
            assert_eq!(state(&store.db, id), "queued", "{id}");
        }
        // Placement: deny wins over allow, and an allow list admits only the jobs it names.
        let placement = Placement {
            allow: Some(["build".to_owned(), "test".to_owned()].into()),
            deny: ["build".to_owned()].into(),
        };
        assert!(placement.permits("test") && !placement.permits("build"));
        assert!(!placement.permits("wide") && Placement::default().permits("wide"));
        // The executor's report is recorded for operators.
        let observed = load(&store.db, "mindy").unwrap().unwrap().observed;
        assert_eq!(observed.verified, BTreeSet::from(["f-1".to_owned()]));
        assert_eq!(observed.refused["f-3"], "tool drift");
        assert!(observed.last_seen.is_some());
        let reports = events(&store.db, "executor.observed");
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].1,
            json!({"verified":["f-1"],"refused":{"f-3":"tool drift"}})
        );
    }

    #[test]
    fn claim_reports_unverified_profiles() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        let base = Queue::default();
        queue(&store.db, Queue { id: "head", ..base });
        queue(
            &store.db,
            Queue {
                id: "later",
                profile: "f-2",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "again",
                profile: "f-2",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "third",
                profile: "f-3",
                created: 4.0,
                ..base
            },
        );
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&response), Some("head"));
        assert_eq!(response.unverified, ["f-2", "f-3"]);
        // Reports stay bounded however many profiles wait.
        let profiles: Vec<String> = (0..40).map(|n| format!("u-{n:02}")).collect();
        for (n, profile) in profiles.iter().enumerate() {
            let id = format!("many-{n}");
            let created = 10.0 + n as f64;
            queue(
                &store.db,
                Queue {
                    id: &id,
                    profile: profile.as_str(),
                    created,
                    ..base
                },
            );
        }
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k2", &["f-1"])).unwrap();
        assert!(response.claim.is_none());
        assert_eq!(response.unverified.len(), 32);
        assert_eq!(response.unverified[..3], ["f-2", "f-3", "u-00"]);
    }

    #[test]
    fn unverified_head_blocks_admission_until_verified() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "head",
                profile: "f-2",
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "later",
                created: 2.0,
                ..base
            },
        );
        // Verification lag must not reorder admission: nothing runs until the head is judged.
        let response = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert!(response.claim.is_none());
        assert_eq!(response.unverified, ["f-2"]);
        assert_eq!(state(&store.db, "later"), "queued");
        // A refused profile is skipped rather than waited for.
        let mut refused = ask("k2", &["f-1"]);
        refused.refused.insert("f-2".into(), "tool drift".into());
        let response = claim_for(&mut store, "mindy", &Scope::all(), &refused).unwrap();
        assert_eq!(claimed(&response), Some("later"));
        assert!(response.unverified.is_empty());
        queue(
            &store.db,
            Queue {
                id: "after",
                created: 3.0,
                ..base
            },
        );
        let response = claim_for(
            &mut store,
            "mindy",
            &Scope::all(),
            &ask("k3", &["f-1", "f-2"]),
        )
        .unwrap();
        assert_eq!(claimed(&response), Some("head"));
    }

    #[test]
    fn claim_key_replays_held_attempt_or_reports_fenced() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 4, 4096);
        queue(
            &store.db,
            Queue {
                id: "first",
                ..Queue::default()
            },
        );
        queue(
            &store.db,
            Queue {
                id: "second",
                created: 2.0,
                ..Queue::default()
            },
        );
        for key in [
            String::new(),
            "k".repeat(129),
            "has space".into(),
            "naïve".into(),
        ] {
            let result = claim_for(&mut store, "mindy", &Scope::all(), &ask(&key, &["f-1"]));
            assert!(matches!(result, Err(AuthorityError::Invalid(_))), "{key:?}");
        }
        let ghost = claim_for(&mut store, "ghost", &Scope::all(), &ask("k1", &["f-1"]));
        assert!(matches!(ghost, Err(AuthorityError::Unregistered)));
        let first = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"]))
            .unwrap()
            .claim
            .unwrap();
        // A lost response: the same key returns the same grant instead of a second attempt.
        let replay = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"]))
            .unwrap()
            .claim
            .unwrap();
        assert_eq!(replay.attempt, first.attempt);
        assert_eq!(replay.action.id, "first");
        assert_eq!(state(&store.db, "second"), "queued");
        assert_eq!(events(&store.db, "attempt.claimed").len(), 1);
        store
            .db
            .execute(
                "UPDATE attempts SET state='running' WHERE id=?",
                [&first.attempt.id],
            )
            .unwrap();
        let replay = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(replay.claim.unwrap().attempt, first.attempt);
        // Keys belong to one executor: another executor's identical key claims fresh work.
        let local = claim_for(&mut store, LOCAL, &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert_eq!(claimed(&local), Some("second"));
        store
            .db
            .execute(
                "UPDATE attempts SET state='interrupted' WHERE id=?",
                [&first.attempt.id],
            )
            .unwrap();
        let fenced = claim_for(&mut store, "mindy", &Scope::all(), &ask("k1", &["f-1"])).unwrap();
        assert!(fenced.claim.is_none());
        assert_eq!(fenced.fenced.as_deref(), Some(first.attempt.id.as_str()));
        assert_eq!(events(&store.db, "attempt.claimed").len(), 2);
    }

    #[test]
    fn claim_order_is_priority_then_fair_share() {
        let (_root, mut store) = lab();
        register(&store.db, "mindy", 8, 8192);
        let base = Queue::default();
        queue(
            &store.db,
            Queue {
                id: "lane",
                owner: "lane",
                created: 1.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "alice-1",
                owner: "alice",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "alice-2",
                owner: "alice",
                created: 2.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "bob-1",
                owner: "bob",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "bob-2",
                owner: "bob",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "carol",
                owner: "carol",
                created: 3.0,
                ..base
            },
        );
        queue(
            &store.db,
            Queue {
                id: "train",
                owner: "train",
                created: 4.0,
                ..base
            },
        );
        store
            .db
            .execute_batch("INSERT INTO dispatches VALUES ('lane',100.0),('alice',50.0)")
            .unwrap();
        crate::worker::set_priority(&store, "train", 10).unwrap();
        let remote = |store: &mut Store, key: &str| {
            let response = claim_for(store, "mindy", &Scope::all(), &ask(key, &["f-1"])).unwrap();
            claimed(&response).unwrap().to_owned()
        };
        let mut order = Vec::new();
        for key in ["k1", "k2", "k3"] {
            order.push(remote(&mut store, key));
        }
        // Fair share is global: local and remote claims are served from one owner history.
        order.push(crate::worker::claim(&mut store).unwrap().unwrap().action);
        for key in ["k4", "k5", "k6"] {
            order.push(remote(&mut store, key));
        }
        assert_eq!(
            order,
            [
                "train", "bob-1", "carol", "alice-1", "lane", "bob-2", "alice-2"
            ]
        );
    }
}
