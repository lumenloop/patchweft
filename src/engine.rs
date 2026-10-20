use crate::{
    file_hash, identity,
    model::*,
    now, source,
    store::{Store, event, record},
    unique,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

pub fn tool_identity(profile: &Profile) -> Result<BTreeMap<String, String>> {
    profile
        .tools
        .iter()
        .map(|(name, path)| Ok((name.clone(), file_hash(path)?)))
        .collect()
}

pub fn validate_profile(profile: &Profile, capacity: (u32, u32)) -> Result<()> {
    ensure!(
        profile.version == 1 && !profile.name.is_empty() && !profile.jobs.is_empty(),
        "profile requires version 1, a name and nonempty obligations"
    );
    for (name, path) in &profile.tools {
        ensure!(
            !name.is_empty() && !name.contains('/') && path.is_absolute() && path.is_file(),
            "tools must name absolute local executables"
        );
    }
    let mut names = BTreeSet::new();
    for job in &profile.jobs {
        ensure!(
            !job.name.is_empty() && names.insert(job.name.clone()),
            "duplicate or empty job name"
        );
        ensure!(
            !job.command.is_empty() && profile.tools.contains_key(&job.command[0]),
            "command executable must name a declared tool: {}",
            job.name
        );
        ensure!(
            job.cleanup.is_empty() || profile.tools.contains_key(&job.cleanup[0]),
            "cleanup executable must name a declared tool"
        );
        ensure!(
            job.slots > 0
                && job.slots <= capacity.0
                && job.memory_mb > 0
                && job.memory_mb <= capacity.1
                && job.timeout_seconds > 0,
            "job exceeds configured capacity: {}",
            job.name
        );
        ensure!(
            ["command", "node-tap", "vitest", "junit"].contains(&job.report.kind.as_str()),
            "job needs an explicit execution/report contract: {}",
            job.name
        );
        if job.report.kind != "command" {
            ensure!(
                job.report.minimum > 0,
                "test report must require executed cases"
            );
        }
        if ["vitest", "junit"].contains(&job.report.kind.as_str()) {
            source::relative_path(&job.report.path)?;
        }
        if [Reuse::Exact, Reuse::Files].contains(&job.reuse) {
            ensure!(
                job.audit.len() >= 20,
                "completed-result reuse requires a recorded input audit"
            );
        }
        if job.reuse == Reuse::Files {
            ensure!(
                !job.inputs.is_empty() && job.dependencies.is_empty(),
                "files-only actions require a nonempty complete input list and no ambient dependencies"
            );
        } else {
            ensure!(
                job.inputs.is_empty(),
                "input projection only applies to files reuse"
            );
        }
        for path in job.inputs.iter().chain(job.artifacts.iter()) {
            source::relative_path(path)?;
        }
        for (key, _) in profile.environment.iter().chain(job.environment.iter()) {
            let name = key.to_ascii_uppercase();
            ensure!(
                ![
                    "HOME",
                    "PATH",
                    "TMPDIR",
                    "GIT_DIR",
                    "GIT_WORK_TREE",
                    "LD_PRELOAD"
                ]
                .contains(&name.as_str())
                    && !name.contains("TOKEN")
                    && !name.contains("PASSWORD")
                    && !name.contains("SECRET"),
                "profile cannot override protected process environment: {key}"
            );
        }
    }
    let mut ordered = BTreeSet::new();
    loop {
        let old = ordered.len();
        for job in &profile.jobs {
            ensure!(
                job.dependencies.iter().all(|d| names.contains(d)),
                "unknown dependency"
            );
            if job.dependencies.iter().all(|d| ordered.contains(d)) {
                ordered.insert(job.name.clone());
            }
        }
        if ordered.len() == names.len() {
            break;
        }
        ensure!(old != ordered.len(), "profile contains a dependency cycle");
    }
    Ok(())
}

pub fn register(store: &mut Store, path: &Path) -> Result<RegisteredProfile> {
    let definition: Profile = serde_json::from_slice(&fs::read(path)?)?;
    for path in definition.tools.values() {
        // Resolve for validation/hash reads, but preserve argv[0]. Multicall tools such as
        // rustup dispatch cargo/rustc through their symlink invocation name.
        path.canonicalize()?;
    }
    validate_profile(&definition, store.capacity()?)?;
    let tools = tool_identity(&definition)?;
    let engine = file_hash(&std::env::current_exe()?)?;
    let platform = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let id = identity(
        &json!({"definition":definition,"tools":tools,"engine":engine,"platform":platform}),
    )?;
    let profile = RegisteredProfile {
        id,
        definition,
        tools,
        engine,
        platform,
    };
    let transaction = store.transaction()?;
    transaction.execute(
        "INSERT OR IGNORE INTO profiles VALUES (?,?)",
        params![profile.id, serde_json::to_string(&profile)?],
    )?;
    event(
        &transaction,
        "profile.registered",
        &profile.id,
        &json!({"name":profile.definition.name}),
    )?;
    transaction.commit()?;
    Ok(profile)
}

pub fn verify_runtime(profile: &RegisteredProfile) -> Result<()> {
    ensure!(
        tool_identity(&profile.definition)? == profile.tools,
        "declared tool bytes changed; register a new profile"
    );
    ensure!(
        file_hash(&std::env::current_exe()?)? == profile.engine,
        "runner binary changed; register a new profile"
    );
    Ok(())
}

pub fn job<'a>(profile: &'a RegisteredProfile, name: &str) -> Result<&'a Job> {
    profile
        .definition
        .jobs
        .iter()
        .find(|j| j.name == name)
        .context("unknown job")
}

pub fn input_identity(store: &Store, revision: &Revision, job: &Job) -> Result<Value> {
    if job.reuse != Reuse::Files {
        return Ok(json!({"commit":revision.commit}));
    }
    let repo = source::repository(store, &revision.project);
    let mut files = BTreeMap::new();
    for path in &job.inputs {
        let result = source::git(&repo, &["ls-tree", &revision.commit, "--", path])?;
        ensure!(
            result.lines().count() == 1,
            "declared input missing: {path}"
        );
        let (metadata, actual) = result.split_once('\t').context("invalid git entry")?;
        let fields: Vec<_> = metadata.split_whitespace().collect();
        ensure!(
            actual == path
                && fields.len() == 3
                && fields[1] == "blob"
                && ["100644", "100755"].contains(&fields[0]),
            "input must be an exact regular file: {path}"
        );
        files.insert(path.clone(), metadata.to_owned());
    }
    Ok(json!({"files":files}))
}

fn request_job(
    store: &mut Store,
    revision: &Revision,
    profile: &RegisteredProfile,
    name: &str,
    owner: &str,
    context: &str,
    requested: &mut BTreeMap<String, String>,
) -> Result<String> {
    if let Some(id) = requested.get(name) {
        return Ok(id.clone());
    }
    let definition = job(profile, name)?.clone();
    let mut dependencies = Vec::new();
    for dependency in &definition.dependencies {
        dependencies.push(request_job(
            store, revision, profile, dependency, owner, context, requested,
        )?);
    }
    let inputs = input_identity(store, revision, &definition)?;
    let key = identity(
        &json!({"project":revision.project,"profile":profile.id,"job":definition.name,
                              "inputs":inputs, "dependencies":dependencies}),
    )?;
    let transaction = store.transaction()?;
    let mut action_id = None;
    if definition.reuse != Reuse::Never {
        let states = if [Reuse::Exact, Reuse::Files].contains(&definition.reuse) {
            "'queued','running','passed'"
        } else {
            "'queued','running'"
        };
        action_id = transaction.query_row(
            &format!("SELECT id FROM actions WHERE key=? AND state IN ({states}) ORDER BY created LIMIT 1"),
            [&key], |r| r.get::<_, String>(0)).optional()?;
    }
    let shared = action_id.is_some();
    let id = action_id.unwrap_or_else(unique);
    if !shared {
        let action = Action {
            id: id.clone(),
            key: key.clone(),
            revision: revision.id.clone(),
            profile: profile.id.clone(),
            job: name.into(),
            dependencies,
            created: now(),
        };
        transaction.execute(
            "INSERT INTO actions VALUES (?,?,'queued',NULL,?,?)",
            params![id, key, serde_json::to_string(&action)?, action.created],
        )?;
        event(
            &transaction,
            "action.queued",
            &id,
            &json!({"revision":revision.id,"job":name}),
        )?;
    }
    transaction.execute("INSERT INTO interests VALUES (?,?,?,1) ON CONFLICT(action,owner,context) DO UPDATE SET active=1",
                        params![id, owner, context])?;
    event(
        &transaction,
        if shared {
            "action.shared"
        } else {
            "action.requested"
        },
        &id,
        &json!({"owner":owner,"context":context,"revision":revision.id}),
    )?;
    transaction.commit()?;
    requested.insert(name.into(), id.clone());
    Ok(id)
}

pub fn request(
    store: &mut Store,
    revision: &str,
    profile: &str,
    name: &str,
    owner: &str,
) -> Result<Action> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    let revision = store.revision(revision)?;
    let profile = store.profile(profile)?;
    verify_runtime(&profile)?;
    let id = request_job(
        store,
        &revision,
        &profile,
        name,
        owner,
        "",
        &mut BTreeMap::new(),
    )?;
    store.action(&id)
}

pub fn submit(
    store: &mut Store,
    revision: &str,
    profile: &str,
    owner: &str,
    prerequisites: Vec<String>,
) -> Result<Candidate> {
    store.atomic(|store| submit_inner(store, revision, profile, owner, prerequisites))
}

fn submit_inner(
    store: &mut Store,
    revision: &str,
    profile: &str,
    owner: &str,
    mut prerequisites: Vec<String>,
) -> Result<Candidate> {
    ensure!(!owner.is_empty(), "owner must be nonempty");
    let revision_row = store.revision(revision)?;
    let profile_row = store.profile(profile)?;
    verify_runtime(&profile_row)?;
    for prerequisite in &prerequisites {
        store.candidate(prerequisite)?;
    }
    prerequisites.sort();
    prerequisites.dedup();
    let id =
        identity(&json!({"revision":revision, "profile":profile, "prerequisites":prerequisites}))?;
    if let Ok(existing) = store.candidate(&id)
        && existing.required.len() == profile_row.definition.jobs.len()
    {
        for action in existing.required.values() {
            store.db.execute("INSERT INTO interests VALUES (?,?,?,1) ON CONFLICT(action,owner,context) DO UPDATE SET active=1",
                                 params![action,owner,id])?;
        }
        return Ok(existing);
    }
    // Candidate and obligations become visible together when the outer transaction commits.
    let mut candidate = Candidate {
        id: id.clone(),
        revision: revision.into(),
        profile: profile.into(),
        required: BTreeMap::new(),
        prerequisites,
        owner: owner.into(),
        created: now(),
    };
    let transaction = store.transaction()?;
    transaction.execute(
        "INSERT OR IGNORE INTO candidates VALUES (?,?)",
        params![id, serde_json::to_string(&candidate)?],
    )?;
    event(&transaction, "candidate.submitted", &id, &candidate)?;
    transaction.commit()?;
    let mut requested = BTreeMap::new();
    for job in &profile_row.definition.jobs {
        request_job(
            store,
            &revision_row,
            &profile_row,
            &job.name,
            owner,
            &id,
            &mut requested,
        )?;
    }
    candidate.required = requested;
    store.db.execute(
        "UPDATE candidates SET body=? WHERE id=?",
        params![serde_json::to_string(&candidate)?, id],
    )?;
    Ok(candidate)
}

pub fn action_state(db: &Connection, id: &str) -> Result<String> {
    Ok(db.query_row("SELECT state FROM actions WHERE id=?", [id], |r| r.get(0))?)
}

pub fn candidate_status(store: &Store, id: &str) -> Result<Value> {
    candidate_status_db(&store.db, id, &mut BTreeSet::new(), &mut BTreeMap::new())
}

fn candidate_status_db(
    db: &Connection,
    id: &str,
    visited: &mut BTreeSet<String>,
    memo: &mut BTreeMap<String, Value>,
) -> Result<Value> {
    if let Some(value) = memo.get(id) {
        return Ok(value.clone());
    }
    ensure!(visited.insert(id.into()), "candidate dependency cycle");
    let candidate: Candidate = record(db, "candidates", id)?;
    let profile: RegisteredProfile = record(db, "profiles", &candidate.profile)?;
    let mut prerequisites = BTreeMap::new();
    for prerequisite in &candidate.prerequisites {
        let result = candidate_status_db(db, prerequisite, visited, memo)?;
        prerequisites.insert(prerequisite, result["state"].clone());
    }
    visited.remove(id);
    let mut jobs = BTreeMap::new();
    for (name, action) in &candidate.required {
        jobs.insert(
            name,
            json!({"action":action,"state":action_state(db, action)?}),
        );
    }
    let state = if candidate.required.len() != profile.definition.jobs.len() {
        "incomplete"
    } else if prerequisites.values().any(|v| v != "passed") {
        "blocked"
    } else if jobs.values().all(|v| v["state"] == "passed") {
        "passed"
    } else if jobs
        .values()
        .any(|v| v["state"] == "failed" || v["state"] == "interrupted")
    {
        "failed"
    } else if jobs.values().all(|v| v["state"] == "canceled") {
        "canceled"
    } else {
        "pending"
    };
    let value = json!({"id":id,"revision":candidate.revision,"profile":candidate.profile,"state":state,
                    "jobs":jobs,"prerequisites":prerequisites});
    memo.insert(id.into(), value.clone());
    Ok(value)
}

pub fn eligible(db: &Connection, action: &Action) -> Result<bool> {
    for dependency in &action.dependencies {
        if action_state(db, dependency)? != "passed" {
            return Ok(false);
        }
    }
    let mut stmt = db.prepare("SELECT context FROM interests WHERE action=? AND active=1")?;
    let contexts: Vec<String> = stmt
        .query_map([&action.id], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut memo = BTreeMap::new();
    for context in contexts {
        if context.is_empty() {
            return Ok(true);
        }
        let candidate: Candidate = record(db, "candidates", &context)?;
        if candidate.required.is_empty() {
            continue;
        }
        let mut ready = true;
        for prerequisite in &candidate.prerequisites {
            if candidate_status_db(db, prerequisite, &mut BTreeSet::new(), &mut memo)?["state"]
                != "passed"
            {
                ready = false;
                break;
            }
        }
        if ready {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn status(store: &Store) -> Result<Value> {
    let mut stmt = store
        .db
        .prepare("SELECT id FROM candidates ORDER BY rowid")?;
    let ids: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut memo = BTreeMap::new();
    let candidates: Vec<Value> = ids
        .iter()
        .map(|id| candidate_status_db(&store.db, id, &mut BTreeSet::new(), &mut memo))
        .collect::<Result<_>>()?;
    let mut stmt = store
        .db
        .prepare("SELECT id,state,attempt,body FROM actions ORDER BY created")?;
    let actions: Vec<Value> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?
        .map(|r| {
            let (id, state, attempt, body) = r?;
            Ok(json!({"id":id,"state":state,"attempt":attempt,
                      "definition":serde_json::from_str::<Value>(&body)?}))
        })
        .collect::<Result<_>>()?;
    Ok(json!({"capacity":store.capacity()?,"candidates":candidates,"actions":actions}))
}

pub fn retry(store: &mut Store, candidate_id: &str, selected: &[String]) -> Result<Value> {
    retry_owned(store, candidate_id, selected, None)
}

pub fn retry_owned(
    store: &mut Store,
    candidate_id: &str,
    selected: &[String],
    owner: Option<&str>,
) -> Result<Value> {
    let candidate = store.candidate(candidate_id)?;
    ensure!(
        selected
            .iter()
            .all(|name| candidate.required.contains_key(name)),
        "unknown retry job"
    );
    let mut retried = Vec::new();
    let transaction = store.transaction()?;
    for (name, id) in &candidate.required {
        if !selected.is_empty() && !selected.contains(name) {
            continue;
        }
        let state = action_state(&transaction, id)?;
        if !["failed", "interrupted", "canceled"].contains(&state.as_str()) {
            continue;
        }
        let held: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM attempts WHERE action=? AND drained=0",
            [id],
            |r| r.get(0),
        )?;
        ensure!(
            held == 0,
            "attempt still owns capacity; confirmed drain is required"
        );
        transaction.execute(
            "UPDATE actions SET state='queued',attempt=NULL WHERE id=?",
            [id],
        )?;
        transaction.execute(
            "UPDATE interests SET active=1 WHERE action=? AND context=? AND (? IS NULL OR owner=?)",
            params![id, candidate_id, owner, owner],
        )?;
        event(
            &transaction,
            "action.retried",
            id,
            &json!({"candidate":candidate_id}),
        )?;
        retried.push(name);
    }
    transaction.commit()?;
    Ok(json!({"retried":retried,"retained_successes":true}))
}

pub fn cancel(store: &mut Store, action: &str, owner: &str) -> Result<Value> {
    store.action(action)?;
    let transaction = store.transaction()?;
    let changed = transaction.execute(
        "UPDATE interests SET active=0 WHERE action=? AND owner=?",
        params![action, owner],
    )?;
    ensure!(changed > 0, "owner has no interest in this action");
    let active: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM interests WHERE action=? AND active=1",
        [action],
        |r| r.get(0),
    )?;
    if active == 0 {
        transaction.execute(
            "UPDATE actions SET state='canceled' WHERE id=? AND state='queued'",
            [action],
        )?;
    }
    event(
        &transaction,
        "interest.detached",
        action,
        &json!({"owner":owner,"remaining":active}),
    )?;
    transaction.commit()?;
    Ok(json!({"action":action,"active_interests":active}))
}

pub fn review(store: &mut Store, revision: &str, reviewer: &str) -> Result<Value> {
    store.revision(revision)?;
    ensure!(!reviewer.is_empty(), "reviewer must be identified");
    store.db.execute(
        "INSERT OR REPLACE INTO reviews VALUES (?,?,?)",
        params![revision, reviewer, now()],
    )?;
    event(
        &store.db,
        "revision.reviewed",
        revision,
        &json!({"reviewer":reviewer}),
    )?;
    Ok(json!({"revision":revision,"reviewer":reviewer}))
}

pub fn promote(store: &mut Store, candidate_id: &str, expected: Option<&str>) -> Result<Value> {
    let candidate = store.candidate(candidate_id)?;
    let revision = store.revision(&candidate.revision)?;
    let profile = store.profile(&candidate.profile)?;
    let repository_root = source::repository(store, &revision.project);
    ensure!(
        profile.definition.certifying,
        "profile is exploratory, not a certification policy"
    );
    let transaction = store.transaction()?;
    ensure!(
        candidate_status_db(
            &transaction,
            candidate_id,
            &mut BTreeSet::new(),
            &mut BTreeMap::new()
        )?["state"]
            == "passed",
        "candidate does not have complete passing evidence"
    );
    let reviews: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM reviews WHERE revision=?",
        [&candidate.revision],
        |r| r.get(0),
    )?;
    ensure!(reviews > 0, "exact composition has no review");
    let previous: Option<String> = transaction
        .query_row(
            "SELECT candidate FROM frontiers WHERE project=?",
            [&revision.project],
            |r| r.get(0),
        )
        .optional()?;
    ensure!(
        previous.as_deref() == expected,
        "validated frontier changed; inspect and compose before promotion"
    );
    if let Some(previous) = &previous {
        let previous_candidate: Candidate = record(&transaction, "candidates", previous)?;
        let previous_revision: Revision =
            record(&transaction, "revisions", &previous_candidate.revision)?;
        ensure!(
            source::git_command(&repository_root)
                .args([
                    "merge-base",
                    "--is-ancestor",
                    &previous_revision.commit,
                    &revision.commit
                ])
                .status()?
                .success(),
            "candidate does not contain the current validated frontier; compose it first"
        );
    }
    let mut receipts = BTreeMap::new();
    for (name, action) in &candidate.required {
        let attempt: String =
            transaction.query_row("SELECT attempt FROM actions WHERE id=?", [action], |r| {
                r.get(0)
            })?;
        let evidence: Attempt = record(&transaction, "attempts", &attempt)?;
        let drained: bool =
            transaction.query_row("SELECT drained FROM attempts WHERE id=?", [&attempt], |r| {
                r.get(0)
            })?;
        ensure!(
            drained && evidence.result.is_some(),
            "missing completed execution evidence"
        );
        receipts.insert(
            name,
            json!({"action":action,"attempt":attempt,"evidence":evidence.result}),
        );
    }
    let certificate = json!({"candidate":candidate_id,"revision":revision.id,"commit":revision.commit,
                            "tree":revision.tree,"profile":profile.id,"receipts":receipts,"time":now()});
    transaction.execute("INSERT INTO frontiers VALUES (?,?,?) ON CONFLICT(project) DO UPDATE SET candidate=excluded.candidate,certificate=excluded.certificate",
                        params![revision.project, candidate_id, certificate.to_string()])?;
    event(
        &transaction,
        "frontier.advanced",
        candidate_id,
        &certificate,
    )?;
    transaction.commit()?;
    Ok(certificate)
}

pub fn repairs(store: &Store) -> Result<Vec<Value>> {
    let mut stmt = store.db.prepare(
        "SELECT id,revision,action,kind,state,detail,resolved_by FROM repairs ORDER BY created",
    )?;
    let values = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, Option<String>>(6)?,
        ))
    })?;
    values
        .map(|v| {
            let (id, revision, action, kind, state, detail, resolved_by) = v?;
            Ok(
                json!({"id":id,"revision":revision,"action":action,"kind":kind,"state":state,
                  "detail":serde_json::from_str::<Value>(&detail)?,"resolved_by":resolved_by}),
            )
        })
        .collect()
}

pub fn resolve_repair(store: &mut Store, repair: &str, candidate: &str) -> Result<Value> {
    ensure!(
        candidate_status(store, candidate)?["state"] == "passed",
        "repair candidate has not passed"
    );
    let changed = store.db.execute(
        "UPDATE repairs SET state='resolved',resolved_by=? WHERE id=? AND state='open'",
        params![candidate, repair],
    )?;
    ensure!(changed == 1, "repair is absent or already resolved");
    event(
        &store.db,
        "repair.resolved",
        repair,
        &json!({"candidate":candidate}),
    )?;
    Ok(json!({"repair":repair,"candidate":candidate}))
}

#[cfg(test)]
mod graph_tests {
    use super::*;

    #[test]
    fn shared_dependency_graph_is_evaluated_once_per_node_and_still_rejects_cycles() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE profiles(id TEXT PRIMARY KEY,body TEXT);
            CREATE TABLE candidates(id TEXT PRIMARY KEY,body TEXT);
            CREATE TABLE actions(id TEXT PRIMARY KEY,state TEXT);
            INSERT INTO actions VALUES('passed-action','passed');",
        )
        .unwrap();
        let profile = json!({"id":"profile","engine":"fixture","platform":"fixture","tools":{},
            "definition":{"version":1,"name":"graph-fixture","tools":{},
                "jobs":[{"name":"check","command":["unused"]}]}});
        db.execute(
            "INSERT INTO profiles VALUES('profile',?)",
            [profile.to_string()],
        )
        .unwrap();
        let mut previous: Vec<String> = vec![];
        let mut final_id = String::new();
        for level in 0..20 {
            let mut next = vec![];
            for branch in 0..2 {
                let id = format!("node-{level}-{branch}");
                let value = json!({"id":id,"revision":"revision","profile":"profile",
                    "required":{"check":"passed-action"},"prerequisites":previous,"owner":"fixture","created":0});
                db.execute(
                    "INSERT INTO candidates VALUES (?,?)",
                    params![id, value.to_string()],
                )
                .unwrap();
                next.push(id.clone());
                final_id = id;
            }
            previous = next;
        }
        let mut memo = BTreeMap::new();
        let result = candidate_status_db(&db, &final_id, &mut BTreeSet::new(), &mut memo).unwrap();
        assert_eq!(result["state"], "passed");
        // One root plus both branches of each preceding level; millions of paths share 39 nodes.
        assert_eq!(memo.len(), 39);
        assert_eq!(result["prerequisites"].as_object().unwrap().len(), 2);
        db.execute("UPDATE candidates SET body=json_set(body,'$.prerequisites',json(?)) WHERE id='node-0-0'",
            [json!([final_id]).to_string()]).unwrap();
        let error = candidate_status_db(&db, &final_id, &mut BTreeSet::new(), &mut BTreeMap::new())
            .unwrap_err();
        assert!(error.to_string().contains("cycle"));
    }
}
