use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const AW: &str = env!("CARGO_BIN_EXE_patchweft");

struct Lab {
    _temp: tempfile::TempDir,
    state: PathBuf,
    repo: PathBuf,
    base: String,
}

fn parsed(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON response")
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git")
        .args(["-C"])
        .arg(repo)
        .args([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@local.invalid",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().into()
}

impl Lab {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("source");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        fs::write(repo.join("provider.mjs"), "export const offset = 0;\n").unwrap();
        fs::write(repo.join("consumer.mjs"), "export const factor = 1;\n").unwrap();
        fs::write(
            repo.join("contract.test.mjs"),
            "import { test } from 'node:test';\nimport assert from 'node:assert/strict';\n\
             import { offset } from './provider.mjs';\nimport { factor } from './consumer.mjs';\n\
             test('provider and consumer compose', () => assert.ok(offset * factor < 2));\n",
        )
        .unwrap();
        fs::write(repo.join("README"), "initial\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let state = temp.path().join("state");
        parsed(
            Command::new(AW)
                .arg("--state")
                .arg(&state)
                .args(["init", "--slots", "2", "--memory-mb", "4096"])
                .output()
                .unwrap(),
        );
        let base = parsed(
            Command::new(AW)
                .arg("--state")
                .arg(&state)
                .args(["import", "--name", "fixture", "--repo"])
                .arg(&repo)
                .args(["--owner", "root"])
                .output()
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .into();
        Self {
            _temp: temp,
            state,
            repo,
            base,
        }
    }
    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(AW);
        command.arg("--state").arg(&self.state).args(args);
        command
    }
    fn call(&self, args: &[&str]) -> Value {
        parsed(self.command(args).output().unwrap())
    }
    fn refuses(&self, args: &[&str], message: &str) {
        let output = self.command(args).output().unwrap();
        assert!(!output.status.success(), "expected refusal");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(message),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    fn profile(&self, jobs: Value) -> String {
        let path = self.state.join("profile.json");
        let node = std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
            .map(|directory| directory.join("node"))
            .find(|path| path.is_file())
            .expect("Node must be on PATH to run the real node:test contract fixture");
        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "version":1,"name":"fixture","certifying":true,
                "tools":{"node":node,"sh":"/usr/bin/sh"},"jobs":jobs
            }))
            .unwrap(),
        )
        .unwrap();
        self.call(&["register", path.to_str().unwrap()])["id"]
            .as_str()
            .unwrap()
            .into()
    }
    fn contract_profile(&self) -> String {
        self.profile(
            json!([{"name":"contract","command":["node","--test","contract.test.mjs"],
                            "report":{"kind":"node-tap","minimum":1}}]),
        )
    }
    fn edit(&self, base: &str, owner: &str, path: &str, text: &str) -> (String, Value) {
        let view = self.call(&["workspace", "--revision", base, "--owner", owner]);
        fs::write(Path::new(view["path"].as_str().unwrap()).join(path), text).unwrap();
        let revision = self.call(&[
            "checkpoint",
            view["id"].as_str().unwrap(),
            "--message",
            owner,
        ]);
        (revision["id"].as_str().unwrap().into(), view)
    }
    fn submit(&self, revision: &str, profile: &str, owner: &str) -> Value {
        self.call(&[
            "submit",
            "--revision",
            revision,
            "--profile",
            profile,
            "--owner",
            owner,
        ])
    }
    fn state(&self, candidate: &Value) -> String {
        self.call(&["status", candidate["id"].as_str().unwrap()])["state"]
            .as_str()
            .unwrap()
            .into()
    }
    fn drain(&self) {
        self.call(&["worker", "--drain"]);
    }
}

#[test]
fn incompatible_changes_fail_without_stalling_independent_work() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (a, a_view) = lab.edit(
        &lab.base,
        "provider",
        "provider.mjs",
        "export const offset = 1;\n",
    );
    let (b, _) = lab.edit(
        &lab.base,
        "consumer",
        "consumer.mjs",
        "export const factor = 2;\n",
    );
    let ca = lab.submit(&a, &profile, "provider");
    let cb = lab.submit(&b, &profile, "consumer");
    lab.drain();
    assert_eq!(lab.state(&ca), "passed");
    assert_eq!(lab.state(&cb), "passed");
    let combined = lab.call(&[
        "compose",
        "--base",
        &lab.base,
        "--change",
        &a,
        "--change",
        &b,
        "--owner",
        "integrator",
    ]);
    let cc = lab.submit(combined["id"].as_str().unwrap(), &profile, "integrator");
    let (independent, _) = lab.edit(
        &lab.base,
        "independent",
        "README",
        "useful independent change\n",
    );
    let ci = lab.submit(&independent, &profile, "independent");
    lab.drain();
    assert_eq!(lab.state(&cc), "failed");
    assert_eq!(lab.state(&ci), "passed");
    assert_eq!(lab.call(&["repairs"]).as_array().unwrap().len(), 1);
    lab.refuses(
        &["promote", cc["id"].as_str().unwrap()],
        "complete passing evidence",
    );
    // Checkpoint did not rewrite the author's base, index or working files.
    let path = Path::new(a_view["path"].as_str().unwrap());
    assert_eq!(
        git(path, &["rev-parse", "HEAD"]),
        git(&lab.repo, &["rev-parse", "HEAD"])
    );
    assert_eq!(
        fs::read_to_string(path.join("provider.mjs")).unwrap(),
        "export const offset = 1;\n"
    );
    lab.call(&["review", &independent, "--reviewer", "reviewer"]);
    lab.call(&["promote", ci["id"].as_str().unwrap()]);
    lab.refuses(&["promote", ci["id"].as_str().unwrap()], "frontier changed");
}

#[test]
fn concurrent_requests_share_one_execution_and_detachment_is_per_owner() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let mut children = Vec::new();
    for n in 0..8 {
        children.push(
            lab.command(&[
                "request",
                "--revision",
                &lab.base,
                "--profile",
                &profile,
                "--job",
                "contract",
                "--owner",
                &format!("agent-{n}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        );
    }
    let actions: Vec<_> = children
        .into_iter()
        .map(|p| parsed(p.wait_with_output().unwrap()))
        .collect();
    let first = actions[0]["id"].as_str().unwrap();
    assert!(actions.iter().all(|a| a["id"] == first));
    assert_eq!(
        lab.call(&["cancel", first, "--owner", "agent-0"])["active_interests"],
        7
    );
    lab.drain();
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        1
    );
    assert_eq!(lab.call(&["status"])["actions"][0]["state"], "passed");
}

#[test]
fn successful_jobs_survive_retry_of_a_failed_obligation() {
    let lab = Lab::new();
    let profile = lab.profile(json!([
        {"name":"contract","command":["node","--test","contract.test.mjs"],
         "report":{"kind":"node-tap","minimum":1}},
        {"name":"transient","command":["sh","-c","test -f \"$PATCHWEFT_STATE/ready\""],
         "report":{"kind":"command"}}
    ]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    fs::write(lab.state.join("ready"), "operator repaired prerequisite").unwrap();
    let retry = lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    assert_eq!(retry["retried"], json!(["transient"]));
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        3
    );
}

#[test]
fn skipped_success_is_not_passing_evidence() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"dishonest-exit","command":["sh","-c",
            "printf '# tests 1\\n# pass 0\\n# fail 0\\n# cancelled 0\\n# skipped 1\\n# todo 0\\n'"],
        "report":{"kind":"node-tap","minimum":1}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    assert!(
        lab.call(&["repairs"])[0]["detail"]["error"]
            .as_str()
            .unwrap()
            .contains("execution contract not met")
    );
}

#[test]
fn failed_prerequisite_blocks_only_its_dependent_candidate() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (bad, _) = lab.edit(
        &lab.base,
        "bad",
        "provider.mjs",
        "export const offset = 3;\n",
    );
    let failed = lab.submit(&bad, &profile, "bad");
    lab.drain();
    let (dependent, _) = lab.edit(&bad, "dependent", "README", "dependent\n");
    let blocked = lab.call(&[
        "submit",
        "--revision",
        &dependent,
        "--profile",
        &profile,
        "--owner",
        "dependent",
        "--requires",
        failed["id"].as_str().unwrap(),
    ]);
    let independent = lab.submit(&lab.base, &profile, "independent");
    lab.drain();
    assert_eq!(lab.state(&blocked), "blocked");
    assert_eq!(lab.state(&independent), "passed");
    let status = lab.call(&["status", blocked["id"].as_str().unwrap()]);
    assert_eq!(status["jobs"]["contract"]["state"], "queued");
}

#[test]
fn late_artifacts_go_to_an_owned_view_and_corruption_is_refused() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"build","command":["sh","-c","mkdir dist; cp provider.mjs dist/provider.mjs"],
        "artifacts":["dist"],"report":{"kind":"command"}
    }]));
    let view = lab.call(&["workspace", "--revision", &lab.base, "--owner", "author"]);
    let candidate = lab.submit(&lab.base, &profile, "author");
    fs::write(
        Path::new(view["path"].as_str().unwrap()).join("provider.mjs"),
        "newer edit\n",
    )
    .unwrap();
    lab.drain();
    let action = candidate["required"]["build"].as_str().unwrap();
    lab.refuses(
        &[
            "deliver",
            action,
            "--destination",
            view["path"].as_str().unwrap(),
        ],
        "never overwritten",
    );
    let destination = lab.state.join("delivered");
    let receipt = lab.call(&[
        "deliver",
        action,
        "--destination",
        destination.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(destination.join("dist/provider.mjs")).unwrap(),
        "export const offset = 0;\n"
    );
    assert_eq!(
        fs::read_to_string(Path::new(view["path"].as_str().unwrap()).join("provider.mjs")).unwrap(),
        "newer edit\n"
    );
    let retained = lab
        .state
        .join("attempts")
        .join(receipt["attempt"].as_str().unwrap())
        .join("artifacts/dist/provider.mjs");
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&retained, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(retained, "corrupt\n").unwrap();
    lab.refuses(
        &[
            "deliver",
            action,
            "--destination",
            lab.state.join("bad-delivery").to_str().unwrap(),
        ],
        "corrupt",
    );
}

#[test]
fn coordinator_loss_retains_running_execution() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"survivor","command":["sh","-c","touch \"$PATCHWEFT_STATE/started\"; sleep 1; mkdir dist; echo result > dist/value"],
        "artifacts":["dist"],"report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let mut coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("started").exists() {
        assert!(start.elapsed().as_secs() < 10, "worker did not start");
        thread::sleep(Duration::from_millis(10));
    }
    coordinator.kill().unwrap();
    coordinator.wait().unwrap();
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        1
    );
}

#[test]
fn detached_descendants_hold_capacity_and_complete_artifacts() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"descendant","command":["sh","-c",
            "mkdir dist; setsid sh -c 'sleep 0.3; echo finished > dist/value' >/dev/null 2>&1 & exit 0"],
        "slots":2,"artifacts":["dist"],"report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let output = lab.state.join("output");
    lab.call(&[
        "deliver",
        candidate["required"]["descendant"].as_str().unwrap(),
        "--destination",
        output.to_str().unwrap(),
    ]);
    assert_eq!(
        fs::read_to_string(output.join("dist/value")).unwrap(),
        "finished\n"
    );
}

#[test]
fn audited_projection_reuses_unchanged_inputs_but_invalidates_changed_ones() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"contract","command":["node","--test","contract.test.mjs"],
        "reuse":"files","inputs":["provider.mjs","consumer.mjs","contract.test.mjs"],
        "audit":"Fixture imports exactly these three files and Node builtins. It has no external data inputs.",
        "report":{"kind":"node-tap","minimum":1}
    }]));
    let first = lab.submit(&lab.base, &profile, "first");
    lab.drain();
    let (docs, _) = lab.edit(&lab.base, "docs", "README", "new unrelated documentation\n");
    let second = lab.submit(&docs, &profile, "second");
    assert_eq!(
        first["required"]["contract"],
        second["required"]["contract"]
    );
    assert_eq!(lab.state(&second), "passed");
    let (bad, _) = lab.edit(
        &lab.base,
        "bad",
        "provider.mjs",
        "export const offset = 9;\n",
    );
    let third = lab.submit(&bad, &profile, "third");
    assert_ne!(third["required"]["contract"], first["required"]["contract"]);
    lab.drain();
    assert_eq!(lab.state(&third), "failed");
}

#[test]
fn textual_conflicts_and_export_races_are_explicit() {
    let lab = Lab::new();
    let (a, _) = lab.edit(&lab.base, "a", "README", "a\n");
    let (b, _) = lab.edit(&lab.base, "b", "README", "b\n");
    lab.refuses(
        &[
            "compose",
            "--base",
            &lab.base,
            "--change",
            &a,
            "--change",
            &b,
            "--owner",
            "integrator",
        ],
        "conflicts",
    );
    assert_eq!(lab.call(&["repairs"])[0]["kind"], "source-conflict");
    let destination = lab.state.join("published.git");
    fs::create_dir(&destination).unwrap();
    git(&destination, &["init", "--bare", "--quiet"]);
    let receipt = lab.call(&["export", &a, "--destination", destination.to_str().unwrap()]);
    lab.refuses(
        &["export", &b, "--destination", destination.to_str().unwrap()],
        "update-ref",
    );
    lab.call(&[
        "export",
        &b,
        "--destination",
        destination.to_str().unwrap(),
        "--expected",
        receipt["commit"].as_str().unwrap(),
    ]);
    // The original fixture branch was never changed.
    assert_eq!(
        fs::read_to_string(lab.repo.join("README")).unwrap(),
        "initial\n"
    );
}

#[test]
fn promotion_preserves_the_previously_validated_work() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (a, _) = lab.edit(&lab.base, "a", "provider.mjs", "export const offset = 1;\n");
    let (b, _) = lab.edit(&lab.base, "b", "README", "independent work\n");
    let ca = lab.submit(&a, &profile, "a");
    let cb = lab.submit(&b, &profile, "b");
    lab.drain();
    lab.call(&["review", &a, "--reviewer", "reviewer"]);
    lab.call(&["review", &b, "--reviewer", "reviewer"]);
    lab.call(&["promote", ca["id"].as_str().unwrap()]);
    lab.refuses(
        &[
            "promote",
            cb["id"].as_str().unwrap(),
            "--expected",
            ca["id"].as_str().unwrap(),
        ],
        "does not contain",
    );
    let composed = lab.call(&[
        "compose",
        "--base",
        &a,
        "--change",
        &b,
        "--owner",
        "integrator",
    ]);
    let successor = lab.submit(composed["id"].as_str().unwrap(), &profile, "integrator");
    lab.drain();
    lab.call(&[
        "review",
        composed["id"].as_str().unwrap(),
        "--reviewer",
        "reviewer",
    ]);
    lab.call(&[
        "promote",
        successor["id"].as_str().unwrap(),
        "--expected",
        ca["id"].as_str().unwrap(),
    ]);
}

#[test]
fn failed_external_cleanup_holds_capacity_until_confirmed() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"resource","command":["sh","-c","true"],"slots":2,
        "cleanup":["sh","-c","test -f \"$PATCHWEFT_STATE/externals-drained\""],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
    lab.refuses(
        &["retry", candidate["id"].as_str().unwrap()],
        "still owns capacity",
    );
    let status = lab.call(&["status"]);
    let attempt = status["actions"][0]["attempt"].as_str().unwrap();
    fs::write(
        lab.state.join("externals-drained"),
        "confirmed by test operator",
    )
    .unwrap();
    lab.call(&["recover", "--confirm-drained", attempt]);
    lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
}

#[test]
fn timeout_retains_output_and_cleanup_evidence_across_retry() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"resource", "slots":2, "timeout_seconds":15,
        "command":["sh","-c",
            "printf 'stdout before completion\n'; printf 'stderr before completion\n' >&2; \
             printf '%s\n' \"$PATCHWEFT_ATTEMPT\" > \"$PATCHWEFT_STATE/external-resource\"; \
             if test -f \"$PATCHWEFT_STATE/allow-completion\"; then printf 'retry completed\n'; exit 0; fi; \
             exec sleep 60"],
        "cleanup":["sh","-c",
            "test \"$(cat \"$PATCHWEFT_STATE/external-resource\")\" = \"$PATCHWEFT_ATTEMPT\" && \
             rm \"$PATCHWEFT_STATE/external-resource\" && test ! -e \"$PATCHWEFT_STATE/external-resource\" && \
             printf '%s\n' \"$PATCHWEFT_ATTEMPT\" >> \"$PATCHWEFT_STATE/cleanup-records\""],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");

    let database = rusqlite::Connection::open(lab.state.join("state.sqlite")).unwrap();
    let (first_id, first_state, first_drained, first_body): (String, String, bool, String) =
        database
            .query_row("SELECT id,state,drained,body FROM attempts", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap();
    assert_eq!(first_state, "failed");
    assert!(first_drained);
    let first: Value = serde_json::from_str(&first_body).unwrap();
    assert_eq!(first["exit_code"], 124);
    assert_eq!(first["result"]["reason"], "timeout");
    assert_eq!(first["result"]["report"]["execution_failed"], true);
    let log_path = lab
        .state
        .join("attempts")
        .join(&first_id)
        .join("output.log");
    let log = fs::read(&log_path).unwrap();
    let output = String::from_utf8(log.clone()).unwrap();
    assert!(output.contains("stdout before completion\n"));
    assert!(output.contains("stderr before completion\n"));
    assert!(!output.contains("retry completed"));
    assert_eq!(
        first["result"]["log_sha256"],
        format!("{:x}", Sha256::digest(&log))
    );
    assert!(!lab.state.join("external-resource").exists());
    assert_eq!(
        fs::read_to_string(lab.state.join("cleanup-records")).unwrap(),
        format!("{first_id}\n")
    );

    // The retry consumes all available slots, so completion also checks release
    // of the failed attempt's reservation after its cleanup succeeds.
    fs::write(lab.state.join("allow-completion"), "ready").unwrap();
    lab.call(&["retry", candidate["id"].as_str().unwrap()]);
    lab.drain();
    assert_eq!(
        lab.state(&candidate),
        "passed",
        "{}",
        database
            .query_row(
                "SELECT body FROM attempts WHERE id != ?",
                [&first_id],
                |row| row.get::<_, String>(0)
            )
            .unwrap()
    );
    let (second_id, second_state, second_drained): (String, String, bool) = database
        .query_row(
            "SELECT id,state,drained FROM attempts WHERE id != ?",
            [&first_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_ne!(first_id, second_id);
    assert_eq!(second_state, "passed");
    assert!(second_drained);
    assert!(!lab.state.join("external-resource").exists());
    assert_eq!(
        fs::read_to_string(lab.state.join("cleanup-records")).unwrap(),
        format!("{first_id}\n{second_id}\n")
    );
    assert_eq!(fs::read(&log_path).unwrap(), log);
    let retained_body: String = database
        .query_row(
            "SELECT body FROM attempts WHERE id = ?",
            [&first_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_body, first_body);
    assert_eq!(
        database
            .query_row("SELECT COUNT(*) FROM attempts", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn external_cleanup_starts_outside_the_job_workspace() {
    let lab = Lab::new();
    let profile = lab.profile(json!([{
        "name":"resource","command":["sh","-c","chmod 000 ."],
        "cleanup":["sh","-c",
            "test \"$PWD\" = \"$PATCHWEFT_STATE/attempts/$PATCHWEFT_ATTEMPT\" && chmod 700 \"$PATCHWEFT_WORKSPACE\" && printf drained > \"$PATCHWEFT_STATE/cleanup-cwd\""],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    assert_eq!(
        fs::read_to_string(lab.state.join("cleanup-cwd")).unwrap(),
        "drained"
    );
}

#[test]
fn a_tracked_old_report_cannot_supply_fresh_passing_evidence() {
    let lab = Lab::new();
    let stale = json!({"success":true,"numTotalTests":1,"numFailedTests":0,"numFailedTestSuites":0,
                     "testResults":[{"assertionResults":[{"status":"passed"}]}]});
    let (revision, _) = lab.edit(&lab.base, "author", "old-report.json", &stale.to_string());
    let profile = lab.profile(json!([{
        "name":"missing-execution","command":["sh","-c","true"],
        "report":{"kind":"vitest","path":"old-report.json","minimum":1}
    }]));
    let candidate = lab.submit(&revision, &profile, "author");
    lab.drain();
    assert_eq!(lab.state(&candidate), "failed");
}

#[test]
fn a_shared_build_feeds_two_private_downstream_workspaces() {
    let lab = Lab::new();
    let profile=lab.profile(json!([
        {"name":"build","command":["sh","-c","mkdir dist; cp provider.mjs dist/provider.mjs"],
         "artifacts":["dist"],"report":{"kind":"command"}},
        {"name":"consumer-one","dependencies":["build"],
         "command":["sh","-c","test \"$(cat dist/provider.mjs)\" = 'export const offset = 0;'; echo private > dist/provider.mjs"],
         "report":{"kind":"command"}},
        {"name":"consumer-two","dependencies":["build"],
         "command":["sh","-c","sleep 0.2; test \"$(cat dist/provider.mjs)\" = 'export const offset = 0;'"],
         "report":{"kind":"command"}}
    ]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.started")
            .count(),
        3
    );
    let database = rusqlite::Connection::open(lab.state.join("state.sqlite")).unwrap();
    let mut statement = database.prepare("SELECT body FROM attempts").unwrap();
    let attempts: Vec<Value> = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str(&row.unwrap()).unwrap())
        .collect();
    assert_eq!(attempts.len(), 3);
    for attempt in attempts {
        let result = &attempt["result"];
        let preparation = &result["preparation"];
        let expected_bytes = if result["job"] == "build" {
            0
        } else {
            b"export const offset = 0;\n".len() as u64
        };
        assert_eq!(
            preparation["dependency_copy_bytes"].as_u64(),
            Some(expected_bytes)
        );
        assert_eq!(
            preparation["dependency_files"].as_u64(),
            Some(u64::from(expected_bytes > 0))
        );
        for field in [
            "source_seconds",
            "dependency_seconds",
            "dependency_copy_seconds",
        ] {
            assert!(preparation[field].as_f64().unwrap() >= 0.0);
        }
        assert!(
            preparation["dependency_copy_seconds"].as_f64().unwrap()
                <= preparation["dependency_seconds"].as_f64().unwrap()
        );
        assert!(result["command_seconds"].as_f64().unwrap() >= 0.0);
        assert!(result["artifact_capture_seconds"].as_f64().unwrap() >= 0.0);
    }
}

#[test]
fn a_new_request_does_not_join_an_attempt_already_being_canceled() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"slow","command":["node","-e",
            "const fs=require('fs'); const p=process.env.PATCHWEFT_STATE+'/started'; if(fs.existsSync(p)){process.exit(0)}; fs.writeFileSync(p,'first'); process.on('SIGTERM',()=>{}); setTimeout(()=>{},20000)"],
        "report":{"kind":"command"}
    }]));
    let first = lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "slow",
        "--owner",
        "first",
    ]);
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("started").exists() {
        assert!(start.elapsed().as_secs() < 10);
        thread::sleep(Duration::from_millis(10));
    }
    lab.call(&["cancel", first["id"].as_str().unwrap(), "--owner", "first"]);
    let start = Instant::now();
    loop {
        if lab
            .call(&["events"])
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "attempt.stopping")
        {
            break;
        }
        assert!(start.elapsed().as_secs() < 10);
        thread::sleep(Duration::from_millis(10));
    }
    let second = lab.call(&[
        "request",
        "--revision",
        &lab.base,
        "--profile",
        &profile,
        "--job",
        "slow",
        "--owner",
        "second",
    ]);
    assert_ne!(first["id"], second["id"]);
    parsed(coordinator.wait_with_output().unwrap());
    lab.drain();
    let status = lab.call(&["status"]);
    assert!(
        status["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["id"] == second["id"] && a["state"] == "passed")
    );
}

#[test]
fn supervisor_loss_is_reported_once_and_keeps_capacity_reserved() {
    let lab = Lab::new();
    let profile=lab.profile(json!([{
        "name":"live","slots":2,"command":["node","-e",
            "require('fs').writeFileSync(process.env.PATCHWEFT_STATE+'/child-pid',String(process.pid)); setTimeout(()=>{},20000)"],
        "report":{"kind":"command"}
    }]));
    let candidate = lab.submit(&lab.base, &profile, "agent");
    let coordinator = lab
        .command(&["worker", "--drain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !lab.state.join("child-pid").exists() {
        assert!(start.elapsed().as_secs() < 10);
        thread::sleep(Duration::from_millis(10));
    }
    let events = lab.call(&["events"]);
    let started = events
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "attempt.started")
        .unwrap();
    let supervisor = started["detail"]["pid"].as_i64().unwrap() as i32;
    let child = fs::read_to_string(lab.state.join("child-pid"))
        .unwrap()
        .parse::<i32>()
        .unwrap();
    unsafe {
        assert_eq!(libc::kill(supervisor, libc::SIGKILL), 0);
    }
    parsed(coordinator.wait_with_output().unwrap());
    lab.refuses(
        &["retry", candidate["id"].as_str().unwrap()],
        "still owns capacity",
    );
    lab.call(&["recover"]);
    lab.call(&["recover"]);
    let events = lab.call(&["events"]);
    assert_eq!(
        events
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "attempt.interrupted")
            .count(),
        1
    );
    unsafe {
        assert_eq!(libc::kill(child, libc::SIGKILL), 0);
    }
    lab.call(&[
        "recover",
        "--confirm-drained",
        started["subject"].as_str().unwrap(),
    ]);
}

#[test]
fn successive_checkpoints_and_reverts_preserve_author_history_without_rewriting_files() {
    let lab = Lab::new();
    let profile = lab.contract_profile();
    let (first, view) = lab.edit(&lab.base, "author", "README", "first checkpoint\n");
    let first_candidate = lab.submit(&first, &profile, "author");
    lab.drain();
    lab.call(&["review", &first, "--reviewer", "reviewer"]);
    lab.call(&["promote", first_candidate["id"].as_str().unwrap()]);
    let workspace = view["id"].as_str().unwrap();
    let path = Path::new(view["path"].as_str().unwrap());
    assert_eq!(
        lab.call(&["checkpoint", workspace, "--message", "no edits"])["id"],
        first
    );
    fs::write(path.join("README"), "second checkpoint\n").unwrap();
    let second = lab.call(&["checkpoint", workspace, "--message", "second"]);
    assert_eq!(second["parents"], json!([first]));
    let second_candidate = lab.submit(second["id"].as_str().unwrap(), &profile, "author");
    lab.drain();
    lab.call(&[
        "review",
        second["id"].as_str().unwrap(),
        "--reviewer",
        "reviewer",
    ]);
    lab.call(&[
        "promote",
        second_candidate["id"].as_str().unwrap(),
        "--expected",
        first_candidate["id"].as_str().unwrap(),
    ]);
    fs::write(path.join("README"), "initial\n").unwrap();
    let reverted = lab.call(&["checkpoint", workspace, "--message", "revert"]);
    assert_ne!(reverted["id"], lab.base);
    assert_eq!(reverted["parents"], json!([second["id"]]));
    assert_eq!(
        git(path, &["rev-parse", "HEAD"]),
        git(&lab.repo, &["rev-parse", "HEAD"])
    );
    assert_eq!(
        fs::read_to_string(path.join("README")).unwrap(),
        "initial\n"
    );
}

#[test]
fn dependency_artifacts_cannot_follow_source_symlinks_outside_the_workspace() {
    use std::os::unix::fs::symlink;
    for dangling in [false, true] {
        let lab = Lab::new();
        let outside = lab.state.join("outside");
        fs::create_dir(&outside).unwrap();
        let view = lab.call(&["workspace", "--revision", &lab.base, "--owner", "author"]);
        let root = Path::new(view["path"].as_str().unwrap());
        // Both a parent symlink and a dangling final symlink must be refused before writing.
        if dangling {
            fs::create_dir(root.join("dist")).unwrap();
            symlink(outside.join("payload"), root.join("dist/payload")).unwrap();
        } else {
            symlink(&outside, root.join("dist")).unwrap();
        }
        let revision = lab.call(&[
            "checkpoint",
            view["id"].as_str().unwrap(),
            "--message",
            "source link",
        ]);
        let profile=lab.profile(json!([
            {"name":"build","command":["sh","-c","rm -rf dist; mkdir dist; echo payload > dist/payload"],
             "artifacts":["dist"],"report":{"kind":"command"}},
            {"name":"consume","dependencies":["build"],"command":["sh","-c","true"],
             "report":{"kind":"command"}}
        ]));
        let candidate = lab.submit(revision["id"].as_str().unwrap(), &profile, "author");
        lab.drain();
        assert_eq!(lab.state(&candidate), "failed");
        assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
    }
}

#[test]
fn concurrent_checkpoints_of_one_workspace_publish_one_tip() {
    let lab = Lab::new();
    let view = lab.call(&["workspace", "--revision", &lab.base, "--owner", "author"]);
    fs::write(
        Path::new(view["path"].as_str().unwrap()).join("README"),
        "edited\n",
    )
    .unwrap();
    let mut children = Vec::new();
    for index in 0..4 {
        children.push(
            lab.command(&[
                "checkpoint",
                view["id"].as_str().unwrap(),
                "--message",
                &format!("concurrent requester {index}"),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        );
    }
    let revisions: Vec<Value> = children
        .into_iter()
        .map(|child| parsed(child.wait_with_output().unwrap()))
        .collect();
    assert!(
        revisions
            .iter()
            .all(|revision| revision["id"] == revisions[0]["id"])
    );
    assert_eq!(revisions[0]["parents"], json!([lab.base]));
    let workspaces = lab.call(&["list", "workspaces"]);
    assert_eq!(workspaces[0]["tip"], revisions[0]["id"]);
}

#[test]
fn multiple_repositories_keep_frontiers_separate_and_share_explicit_prerequisites() {
    let lab = Lab::new();
    let python_repo = lab.state.join("python-source");
    fs::create_dir(&python_repo).unwrap();
    git(&python_repo, &["init", "--quiet"]);
    fs::write(
        python_repo.join("contract.py"),
        "def total(values):\n    return sum(values)\n\nassert total([2, 3]) == 5\n",
    )
    .unwrap();
    git(&python_repo, &["add", "."]);
    git(&python_repo, &["commit", "--quiet", "-m", "Python tool"]);
    let python_revision = lab.call(&[
        "import",
        "--name",
        "python-tool",
        "--repo",
        python_repo.to_str().unwrap(),
        "--owner",
        "python-agent",
    ]);
    let profile_path = lab.state.join("python-profile.json");
    fs::write(
        &profile_path,
        serde_json::to_vec(&json!({
            "version":1,"name":"python-tool","certifying":true,
            "tools":{"python":"/usr/bin/python3"},
            "jobs":[{"name":"contract","command":["python","contract.py"],
                     "report":{"kind":"command"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    let python_profile = lab.call(&["register", profile_path.to_str().unwrap()]);
    let javascript_profile = lab.contract_profile();
    let (broken, _) = lab.edit(
        &lab.base,
        "javascript-agent",
        "provider.mjs",
        "export const offset = 3;\n",
    );
    let javascript = lab.submit(&broken, &javascript_profile, "javascript-agent");
    let independent = lab.submit(
        python_revision["id"].as_str().unwrap(),
        python_profile["id"].as_str().unwrap(),
        "python-agent",
    );
    let dependent = lab.call(&[
        "submit",
        "--revision",
        python_revision["id"].as_str().unwrap(),
        "--profile",
        python_profile["id"].as_str().unwrap(),
        "--owner",
        "release-agent",
        "--requires",
        javascript["id"].as_str().unwrap(),
    ]);
    lab.drain();
    assert_eq!(lab.state(&javascript), "failed");
    assert_eq!(lab.state(&independent), "passed");
    assert_eq!(lab.state(&dependent), "blocked");
    assert_ne!(
        javascript["required"]["contract"],
        independent["required"]["contract"]
    );
    lab.call(&[
        "review",
        python_revision["id"].as_str().unwrap(),
        "--reviewer",
        "python-reviewer",
    ]);
    lab.call(&["promote", independent["id"].as_str().unwrap()]);
    let projects = lab.call(&["list", "projects"]);
    assert_eq!(projects.as_array().unwrap().len(), 2);
    let python = projects
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "python-tool")
        .unwrap();
    let javascript_project = projects
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["name"] == "fixture")
        .unwrap();
    assert_eq!(python["validated_candidate"], independent["id"]);
    assert!(javascript_project["validated_candidate"].is_null());
    lab.refuses(
        &[
            "compose",
            "--base",
            &lab.base,
            "--change",
            python_revision["id"].as_str().unwrap(),
            "--owner",
            "integrator",
        ],
        "single project",
    );
}

#[test]
fn executable_symlinks_preserve_multicall_dispatch_names() {
    let lab = Lab::new();
    let tool = lab.state.join("cargo-fixture");
    std::os::unix::fs::symlink("/usr/bin/sh", &tool).unwrap();
    let path = lab.state.join("multicall-profile.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({
            "version":1,"name":"multicall","tools":{"cargo":tool},
            "jobs":[{"name":"dispatch","command":["cargo","-c",
                "case \"$0\" in */cargo-fixture) exit 0 ;; *) exit 3 ;; esac"],
                "report":{"kind":"command"}}]
        }))
        .unwrap(),
    )
    .unwrap();
    let profile = lab.call(&["register", path.to_str().unwrap()]);
    assert_eq!(
        profile["definition"]["tools"]["cargo"],
        tool.to_str().unwrap()
    );
    let candidate = lab.submit(&lab.base, profile["id"].as_str().unwrap(), "agent");
    lab.drain();
    assert_eq!(lab.state(&candidate), "passed");
}
