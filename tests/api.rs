use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use patchweft::{
    api::{self, Api, Command, Principal, Query},
    engine, hash, http, mcp, source,
    store::{self, Store},
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command as Process,
    sync::Arc,
};
use tower::ServiceExt;

// Public synthetic test strings; no deployment credential is loaded by these tests.
const ALICE: &str = "fixture-alice-credential-not-for-deployment";
const BOB: &str = "fixture-bob-credential-not-for-deployment";
struct Lab {
    _temp: tempfile::TempDir,
    root: PathBuf,
    repo: PathBuf,
    a: String,
    b: String,
    pa: String,
    pb: String,
    profile: String,
    api: Arc<Api>,
}
fn git(path: &Path, args: &[&str]) -> String {
    let output = Process::new("git")
        .arg("-C")
        .arg(path)
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
        let repo = temp.path().join("repo");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        fs::write(repo.join("README"), "initial\n").unwrap();
        git(&repo, &["add", "README"]);
        git(&repo, &["commit", "--quiet", "-m", "fixture"]);
        let root = temp.path().join("state");
        let mut store = Store::init(&root, 2, 2048).unwrap();
        let a = source::import(&mut store, "a", &repo, "HEAD", "fixture").unwrap();
        let b = source::import(&mut store, "b", &repo, "HEAD", "fixture").unwrap();
        let profile_path = temp.path().join("profile.json");
        fs::write(&profile_path,json!({"version":1,"name":"fixture","certifying":true,
            "tools":{"sh":"/usr/bin/sh"},"jobs":[
            {"name":"dep","command":["sh","-c","true"],"reuse":"never","report":{"kind":"command"}},
            {"name":"root","dependencies":["dep"],"command":["sh","-c","true"],"reuse":"never","report":{"kind":"command"}}
        ]}).to_string()).unwrap();
        let profile = engine::register(&mut store, &profile_path).unwrap().id;
        let api = Arc::new(Api::open(&root).unwrap());
        for (id, token, p) in [("alice", ALICE, &a.project), ("bob", BOB, &b.project)] {
            let principal = Principal {
                id: id.into(),
                projects: BTreeSet::from([p.clone()]),
                profiles: BTreeSet::from([profile.clone()]),
                roles: BTreeSet::from(["read".into(), "write".into()]),
                max_pending: 1024,
            };
            store
                .db
                .execute(
                    "INSERT INTO api_principals VALUES (?,?)",
                    rusqlite::params![id, serde_json::to_string(&principal).unwrap()],
                )
                .unwrap();
            store
                .db
                .execute(
                    "INSERT INTO api_credentials VALUES (?,?)",
                    rusqlite::params![hash(token.as_bytes()), id],
                )
                .unwrap();
        }
        Self {
            _temp: temp,
            root,
            repo,
            a: a.id,
            b: b.id,
            pa: a.project,
            pb: b.project,
            profile,
            api,
        }
    }
    fn command(&self) -> Command {
        Command::Request {
            revision: self.a.clone(),
            profile: self.profile.clone(),
            job: "root".into(),
        }
    }
    fn rows(&self, table: &str) -> i64 {
        Store::open(&self.root)
            .unwrap()
            .db
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
    fn router(&self) -> axum::Router {
        http::router(http::Service::new(self.api.clone(), BTreeSet::new()))
    }
}

#[test]
fn receipt_and_dependency_effects_are_atomic_and_replay_after_restart() {
    let lab = Lab::new();
    let before = lab.rows("events");
    let store = Store::open(&lab.root).unwrap();
    store
        .db
        .execute_batch(
            "CREATE TRIGGER fail_receipt BEFORE INSERT ON api_receipts BEGIN
        SELECT RAISE(ABORT,'fixture interruption before receipt'); END;",
        )
        .unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "one", &lab.command())
            .unwrap_err()
            .status,
        500
    );
    assert_eq!(lab.rows("actions"), 0);
    assert_eq!(lab.rows("interests"), 0);
    assert_eq!(lab.rows("api_receipts"), 0);
    assert_eq!(lab.rows("events"), before);
    store.db.execute_batch("DROP TRIGGER fail_receipt").unwrap();
    let first = lab.api.command(ALICE, "one", &lab.command()).unwrap();
    assert_eq!(first["status"], 200);
    assert_eq!(lab.rows("actions"), 2);
    let restarted = Api::open(&lab.root).unwrap();
    let replay = restarted.command(ALICE, "one", &lab.command()).unwrap();
    assert_eq!(replay["operation_id"], first["operation_id"]);
    assert_eq!(replay["result"], first["result"]);
    assert_eq!(replay["replayed"], true);
    assert_eq!(lab.rows("actions"), 2);
    assert_eq!(
        restarted
            .command(
                ALICE,
                "one",
                &Command::Cancel {
                    action: first["result"]["id"].as_str().unwrap().into()
                }
            )
            .unwrap_err()
            .status,
        409
    );
}

#[test]
fn concurrent_retry_of_one_key_creates_one_dependency_graph() {
    let lab = Lab::new();
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let mut threads = vec![];
    for _ in 0..16 {
        let api = lab.api.clone();
        let command = lab.command();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            api.command(ALICE, "concurrent", &command).unwrap()
        }));
    }
    let values: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(values.iter().all(|v| v["result"] == values[0]["result"]));
    assert_eq!(values.iter().filter(|v| v["replayed"] == false).count(), 1);
    assert_eq!(lab.rows("actions"), 2);
    assert_eq!(lab.rows("api_receipts"), 1);
}

#[test]
fn local_and_api_candidate_submission_share_one_transaction_order() {
    let lab = Lab::new();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let root = lab.root.clone();
    let revision = lab.a.clone();
    let profile = lab.profile.clone();
    let ready = barrier.clone();
    let local = std::thread::spawn(move || {
        ready.wait();
        engine::submit(
            &mut Store::open(&root).unwrap(),
            &revision,
            &profile,
            "local",
            vec![],
        )
        .unwrap()
    });
    barrier.wait();
    let remote = lab
        .api
        .command(
            ALICE,
            "submit",
            &Command::Submit {
                revision: lab.a.clone(),
                profile: lab.profile.clone(),
                requires: vec![],
            },
        )
        .unwrap();
    assert_eq!(remote["result"]["id"], local.join().unwrap().id);
    assert_eq!(lab.rows("actions"), 2);
    assert_eq!(lab.rows("candidates"), 1);
}

#[test]
fn principal_scope_covers_replays_dependencies_roles_and_cancellation() {
    let lab = Lab::new();
    let first = lab.api.command(ALICE, "first", &lab.command()).unwrap();
    let action = first["result"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        lab.api
            .query(
                BOB,
                &Query::Action {
                    action: action.clone()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        lab.api
            .command(BOB, "cancel", &Command::Cancel { action })
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        lab.api
            .query(
                BOB,
                &Query::Operation {
                    id: first["operation_id"].as_str().unwrap().into()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "review",
                &Command::Review {
                    revision: lab.a.clone()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    let prerequisite = engine::submit(
        &mut Store::open(&lab.root).unwrap(),
        &lab.b,
        &lab.profile,
        "b",
        vec![],
    )
    .unwrap();
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "cross",
                &Command::Submit {
                    revision: lab.a.clone(),
                    profile: lab.profile.clone(),
                    requires: vec![prerequisite.id]
                }
            )
            .unwrap_err()
            .status,
        403
    );
    let store = Store::open(&lab.root).unwrap();
    let mut principal: Principal = serde_json::from_str(
        &store
            .db
            .query_row(
                "SELECT body FROM api_principals WHERE id='alice'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    principal.roles.remove("write");
    store
        .db
        .execute(
            "UPDATE api_principals SET body=? WHERE id='alice'",
            [serde_json::to_string(&principal).unwrap()],
        )
        .unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "first", &lab.command())
            .unwrap_err()
            .status,
        403,
        "replay must recheck current scope"
    );
    api::revoke(&store, "alice").unwrap();
    assert_eq!(
        lab.api.query(ALICE, &Query::Profiles).unwrap_err().status,
        401
    );
}

#[test]
fn durable_domain_rejection_replays_without_creating_partial_work() {
    let lab = Lab::new();
    let command = Command::Request {
        revision: lab.a.clone(),
        profile: lab.profile.clone(),
        job: "absent".into(),
    };
    let first = lab.api.command(ALICE, "bad-job", &command).unwrap();
    assert_eq!(first["status"], 409);
    assert_eq!(lab.rows("actions"), 0);
    let second = lab.api.command(ALICE, "bad-job", &command).unwrap();
    assert_eq!(second["operation_id"], first["operation_id"]);
    assert_eq!(second["replayed"], true);
}

#[test]
fn paginated_events_cross_hidden_projects_without_losing_cursor_progress() {
    let lab = Lab::new();
    let mut store = Store::open(&lab.root).unwrap();
    let transaction = store.transaction().unwrap();
    for n in 0..1100 {
        store::event(
            &transaction,
            "revision.reviewed",
            if n % 2 == 0 { &lab.a } else { &lab.b },
            &json!({"reviewer":format!("fixture-{n}")}),
        )
        .unwrap();
    }
    transaction.commit().unwrap();
    let mut cursor = 0;
    let mut selected = vec![];
    loop {
        let page = lab
            .api
            .query(
                ALICE,
                &Query::Events {
                    after: cursor,
                    project: None,
                    limit: 1000,
                },
            )
            .unwrap();
        for row in page["events"].as_array().unwrap() {
            assert_ne!(row["subject"], lab.b);
            selected.push(row["seq"].as_i64().unwrap());
        }
        cursor = page["next_after"].as_i64().unwrap();
        if page["more"] == false {
            break;
        }
    }
    assert_eq!(
        selected.len(),
        551,
        "initial revision plus 550 allowed fixture events"
    );
    assert_eq!(
        selected.iter().collect::<BTreeSet<_>>().len(),
        selected.len()
    );
    assert_eq!(
        lab.api
            .query(
                ALICE,
                &Query::Events {
                    after: cursor + 1,
                    project: None,
                    limit: 10
                }
            )
            .unwrap_err()
            .code,
        "future_cursor"
    );
    assert_eq!(
        lab.api
            .query(
                ALICE,
                &Query::Events {
                    after: 0,
                    project: Some(lab.pb.clone()),
                    limit: 10
                }
            )
            .unwrap_err()
            .status,
        403
    );
}

#[test]
fn bundle_upload_hash_and_project_boundaries_precede_publication() {
    let lab = Lab::new();
    fs::write(lab.repo.join("README"), "new revision\n").unwrap();
    git(&lab.repo, &["add", "README"]);
    git(&lab.repo, &["commit", "--quiet", "-m", "second"]);
    let commit = git(&lab.repo, &["rev-parse", "HEAD"]);
    let bundle = lab._temp.path().join("source.bundle");
    git(
        &lab.repo,
        &["bundle", "create", bundle.to_str().unwrap(), "HEAD"],
    );
    let bytes = fs::read(&bundle).unwrap();
    let sha = hash(&bytes);
    assert_eq!(
        lab.api
            .upload(ALICE, &"0".repeat(64), &bytes)
            .unwrap_err()
            .status,
        422
    );
    lab.api.upload(ALICE, &sha, &bytes).unwrap();
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "wrong-project",
                &Command::Publish {
                    project: lab.pb.clone(),
                    sha256: sha.clone(),
                    commit: commit.clone(),
                    message: "publish".into()
                }
            )
            .unwrap_err()
            .status,
        403
    );
    let publish = Command::Publish {
        project: lab.pa.clone(),
        sha256: sha,
        commit: commit.clone(),
        message: "publish".into(),
    };
    let response = lab.api.command(ALICE, "publish", &publish).unwrap();
    assert_eq!(response["status"], 200, "{response}");
    assert_eq!(response["result"]["commit"], commit);
    assert_eq!(
        lab.api.command(ALICE, "publish", &publish).unwrap()["result"],
        response["result"]
    );
}

async fn send(
    router: axum::Router,
    uri: &str,
    body: Value,
    token: Option<&str>,
    extra: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut request = Request::post(uri).header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    for (key, value) in extra {
        request = request.header(*key, *value);
    }
    let response = router
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

#[tokio::test]
async fn unavailable_artifacts_are_a_scoped_state_error_and_failed_logs_remain_readable() {
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    let profile_path = lab.root.join("artifact-profile.json");
    fs::write(
        &profile_path,
        json!({"version":1,"name":"artifact-states","tools":{"sh":"/usr/bin/sh"},"jobs":[
            {"name":"fail","command":["sh","-c","printf 'failed fixture\\n'; printf 'not retained\\n' > report.txt; exit 1"],
             "artifacts":["report.txt"],"report":{"kind":"command"}},
            {"name":"pass","command":["sh","-c","printf 'artifact\\n' > report.txt"],
             "artifacts":["report.txt"],"report":{"kind":"command"}}
        ]}).to_string(),
    ).unwrap();
    let registered = Process::new(env!("CARGO_BIN_EXE_patchweft"))
        .arg("--state")
        .arg(&lab.root)
        .arg("register")
        .arg(&profile_path)
        .output()
        .unwrap();
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );
    let profile: Value = serde_json::from_slice(&registered.stdout).unwrap();
    let profile_id = profile["id"].as_str().unwrap();
    let request = |job: &str| {
        let output = Process::new(env!("CARGO_BIN_EXE_patchweft"))
            .arg("--state")
            .arg(&lab.root)
            .args([
                "request",
                "--revision",
                &lab.a,
                "--profile",
                profile_id,
                "--job",
                job,
                "--owner",
                "fixture",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<patchweft::model::Action>(&output.stdout).unwrap()
    };
    let failed = request("fail");
    let passed = request("pass");
    let digest = hash(b"artifact\n");
    for after_execution in [false, true] {
        if after_execution {
            let output = Process::new(env!("CARGO_BIN_EXE_patchweft"))
                .arg("--state")
                .arg(&lab.root)
                .args(["worker", "--drain"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                engine::action_state(&store.db, &failed.id).unwrap(),
                "failed"
            );
        }
        let query = json!({"op":"artifacts","action":failed.id});
        let (status, body) = send(lab.router(), "/v1/query", query.clone(), Some(ALICE), &[]).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "artifacts_unavailable");
        assert_eq!(
            send(lab.router(), "/v1/query", query, Some(BOB), &[])
                .await
                .0,
            StatusCode::FORBIDDEN
        );
        let response = lab
            .router()
            .oneshot(
                Request::get(format!("/v1/artifacts/{}/{digest}", failed.id))
                    .header("authorization", format!("Bearer {ALICE}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let error: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(error["error"]["code"], "artifacts_unavailable");
        assert_eq!(
            lab.api
                .artifact(BOB, &failed.id, &digest)
                .unwrap_err()
                .status,
            403
        );
    }
    let action = lab
        .api
        .query(
            ALICE,
            &Query::Action {
                action: failed.id.clone(),
            },
        )
        .unwrap();
    let log = lab
        .api
        .query(
            ALICE,
            &Query::Logs {
                attempt: action["attempt"].as_str().unwrap().into(),
                offset: 0,
                limit: 1024,
            },
        )
        .unwrap();
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(log["data"].as_str().unwrap())
        .unwrap();
    assert!(String::from_utf8(bytes).unwrap().contains("failed fixture"));
    let manifest = lab
        .api
        .query(
            ALICE,
            &Query::Artifacts {
                action: passed.id.clone(),
            },
        )
        .unwrap();
    assert_eq!(manifest["files"]["report.txt"]["sha256"], digest);
    use std::io::Read;
    let mut contents = String::new();
    lab.api
        .artifact(ALICE, &passed.id, &digest)
        .unwrap()
        .read_to_string(&mut contents)
        .unwrap();
    assert_eq!(contents, "artifact\n");
    assert_eq!(
        lab.api
            .query(
                ALICE,
                &Query::Artifacts {
                    action: "unknown-action".into()
                }
            )
            .unwrap_err()
            .status,
        403
    );
}

#[tokio::test]
async fn http_rejects_spoofed_identity_origin_and_missing_credentials() {
    let lab = Lab::new();
    let command = serde_json::to_value(lab.command()).unwrap();
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            command.clone(),
            None,
            &[("idempotency-key", "x")]
        )
        .await
        .0,
        401
    );
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            command.clone(),
            Some(ALICE),
            &[
                ("idempotency-key", "x"),
                ("origin", "https://unapproved.invalid")
            ]
        )
        .await
        .0,
        403
    );
    let mut spoof = command.clone();
    spoof["owner"] = json!("bob");
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            spoof,
            Some(ALICE),
            &[("idempotency-key", "x")]
        )
        .await
        .0,
        422
    );
    assert_eq!(
        send(
            lab.router(),
            "/v1/commands",
            command,
            Some(ALICE),
            &[("idempotency-key", "x")]
        )
        .await
        .0,
        200
    );
    assert_eq!(lab.rows("actions"), 2);
}

fn modern(id: i64, method: &str, params: Value) -> Value {
    let mut params = params;
    params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":mcp::MODERN,
        "io.modelcontextprotocol/clientCapabilities":{}});
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}
#[tokio::test]
async fn mcp_eras_share_effects_but_keep_request_envelopes_and_headers_distinct() {
    let lab = Lab::new();
    let headers = [
        ("accept", "application/json, text/event-stream"),
        ("mcp-protocol-version", mcp::MODERN),
        ("mcp-method", "tools/call"),
        ("mcp-name", "patchweft_command"),
    ];
    let params = json!({"name":"patchweft_command","arguments":{"idempotency_key":"mcp-same","command":lab.command()}});
    let (status, first) = send(
        lab.router(),
        "/mcp",
        modern(7, "tools/call", params.clone()),
        Some(ALICE),
        &headers,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(first["result"]["resultType"], "complete");
    let (_, second) = send(
        lab.router(),
        "/mcp",
        modern(8, "tools/call", params.clone()),
        Some(ALICE),
        &headers,
    )
    .await;
    assert_eq!(second["id"], 8);
    assert_eq!(second["result"]["structuredContent"]["replayed"], true);
    assert_eq!(lab.rows("actions"), 2);
    let (_, legacy) = send(
        lab.router(),
        "/mcp",
        json!({"jsonrpc":"2.0","id":9,"method":"tools/call","params":params}),
        Some(ALICE),
        &[
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", mcp::LEGACY),
        ],
    )
    .await;
    assert_eq!(legacy["id"], 9);
    assert!(legacy["result"].get("resultType").is_none());
    assert_eq!(
        legacy["result"]["structuredContent"]["operation_id"],
        first["result"]["structuredContent"]["operation_id"]
    );
    let (status, bad) = send(
        lab.router(),
        "/mcp",
        modern(10, "tools/list", json!({})),
        Some(ALICE),
        &headers,
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(bad["error"]["code"], -32020);
    let (status,notification)=send(lab.router(),"/mcp",json!({"jsonrpc":"2.0","method":"tools/call",
        "params":{"name":"patchweft_command","arguments":{"idempotency_key":"never-execute","command":lab.command()}}}),
        Some(ALICE),&headers).await;
    assert_eq!(status, 202);
    assert!(notification.is_null());
    assert_eq!(lab.rows("actions"), 2);
}

#[tokio::test]
async fn sse_disconnect_releases_stream_capacity_and_reconnect_replays() {
    let lab = Lab::new();
    let router = lab.router();
    let request = || {
        Request::get("/v1/events")
            .header("authorization", format!("Bearer {ALICE}"))
            .header("accept", "text/event-stream")
            .body(Body::empty())
            .unwrap()
    };
    let mut streams = vec![];
    for _ in 0..32 {
        let response = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(response.status(), 200);
        streams.push(response);
    }
    assert_eq!(
        router.clone().oneshot(request()).await.unwrap().status(),
        503
    );
    streams.clear();
    let mut response = router.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), 200);
    let frame = response
        .body_mut()
        .frame()
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&frame).contains("event: patchweft"));
    let first_seq = lab
        .api
        .query(
            ALICE,
            &Query::Events {
                after: 0,
                project: None,
                limit: 1000,
            },
        )
        .unwrap()["events"][0]["seq"]
        .as_i64()
        .unwrap();
    drop(response);
    let response = router
        .oneshot(
            Request::get("/v1/events")
                .header("authorization", format!("Bearer {ALICE}"))
                .header("last-event-id", first_seq.to_string())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        value["result"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["seq"].as_i64().unwrap() > first_seq)
    );
    assert_eq!(lab.rows("actions"), 0);
}

#[test]
fn shared_candidate_retry_uses_association_without_reviving_other_owners() {
    let lab = Lab::new();
    let mut store = Store::open(&lab.root).unwrap();
    let candidate =
        engine::submit(&mut store, &lab.a, &lab.profile, "local-first", vec![]).unwrap();
    let response = lab
        .api
        .command(
            ALICE,
            "attach",
            &Command::Submit {
                revision: lab.a.clone(),
                profile: lab.profile.clone(),
                requires: vec![],
            },
        )
        .unwrap();
    assert_eq!(response["result"]["id"], candidate.id);
    for action in candidate.required.values() {
        engine::cancel(&mut store, action, "local-first").unwrap();
        engine::cancel(&mut store, action, "api:alice").unwrap();
    }
    let response = lab
        .api
        .command(
            ALICE,
            "retry",
            &Command::Retry {
                candidate: candidate.id.clone(),
                jobs: vec![],
            },
        )
        .unwrap();
    assert_eq!(response["status"], 200);
    assert_eq!(response["result"]["retried"].as_array().unwrap().len(), 2);
    let active: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM interests WHERE owner='local-first' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 0);
    let active: i64 = store
        .db
        .query_row(
            "SELECT COUNT(*) FROM interests WHERE owner='api:alice' AND active=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(active, 2);
}

#[test]
fn admission_counts_actual_leaf_work_and_rolls_back_excess_dependencies() {
    let lab = Lab::new();
    let store = Store::open(&lab.root).unwrap();
    store
        .db
        .execute(
            "UPDATE api_principals SET body=json_set(body,'$.max_pending',1) WHERE id='alice'",
            [],
        )
        .unwrap();
    assert_eq!(
        lab.api
            .command(ALICE, "too-many", &lab.command())
            .unwrap_err()
            .status,
        429
    );
    assert_eq!(lab.rows("actions"), 0);
    assert_eq!(lab.rows("api_receipts"), 0);
    let leaf = Command::Request {
        revision: lab.a.clone(),
        profile: lab.profile.clone(),
        job: "dep".into(),
    };
    assert_eq!(
        lab.api.command(ALICE, "leaf", &leaf).unwrap()["status"],
        200
    );
    assert_eq!(lab.rows("actions"), 1);
}

#[test]
fn composition_conflicts_are_durable_inspectable_and_scoped() {
    let lab = Lab::new();
    let base = git(&lab.repo, &["rev-parse", "HEAD"]);
    let mut store = Store::open(&lab.root).unwrap();
    let mut changes = vec![];
    for text in ["first change\n", "second change\n"] {
        git(&lab.repo, &["checkout", "--quiet", "--detach", &base]);
        fs::write(lab.repo.join("README"), text).unwrap();
        git(&lab.repo, &["add", "README"]);
        git(
            &lab.repo,
            &["commit", "--quiet", "-m", "conflicting fixture"],
        );
        changes.push(
            source::import(&mut store, "a", &lab.repo, "HEAD", "fixture")
                .unwrap()
                .id,
        );
    }
    let command = Command::Compose {
        base: lab.a.clone(),
        changes,
        message: "conflict fixture".into(),
    };
    let response = lab.api.command(ALICE, "conflict", &command).unwrap();
    assert_eq!(response["status"], 200, "{response}");
    assert_eq!(response["result"]["state"], "conflict");
    let repair = response["result"]["repair"].as_str().unwrap().to_owned();
    assert_eq!(
        lab.api.command(ALICE, "conflict", &command).unwrap()["result"],
        response["result"]
    );
    assert_eq!(lab.rows("repairs"), 1);
    let query = Query::Repair {
        repair: repair.clone(),
    };
    let detail = lab.api.query(ALICE, &query).unwrap();
    assert!(
        detail["detail"]["conflicts"]
            .as_str()
            .unwrap()
            .contains("README")
    );
    assert_eq!(lab.api.query(BOB, &query).unwrap_err().status, 403);
    let page = lab
        .api
        .query(
            ALICE,
            &Query::Repairs {
                project: lab.pa.clone(),
                after: 0,
                limit: 10,
            },
        )
        .unwrap();
    assert_eq!(page["items"][0]["id"], repair);
    let events = lab
        .api
        .query(
            ALICE,
            &Query::Events {
                after: 0,
                project: None,
                limit: 1000,
            },
        )
        .unwrap();
    assert!(
        events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "composition.conflict" && e["subject"] == repair)
    );
    let cross = engine::submit(&mut store, &lab.b, &lab.profile, "b", vec![]).unwrap();
    assert_eq!(
        lab.api
            .command(
                ALICE,
                "bad-resolve",
                &Command::Resolve {
                    repair,
                    candidate: cross.id
                }
            )
            .unwrap_err()
            .status,
        403
    );
}

#[tokio::test]
async fn extractor_errors_and_mcp_schema_keep_the_wire_contract() {
    let lab = Lab::new();
    let request = Request::get("/v1/events?after=oops")
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::empty())
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 400);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["error"]["code"], "invalid_query");
    assert_eq!(value["api_version"], "1");
    let request = Request::put(format!("/v1/blobs/{}", "0".repeat(64)))
        .header("authorization", format!("Bearer {ALICE}"))
        .header("content-length", (api::MAX_BUNDLE + 1).to_string())
        .body(Body::empty())
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 413);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["error"]["code"], "invalid_body");
    assert_eq!(value["api_version"], "1");
    let (status, value) = send(
        lab.router(),
        "/mcp",
        modern(1, "tools/list", json!({})),
        Some(ALICE),
        &[
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", mcp::MODERN),
            ("mcp-method", "tools/list"),
        ],
    )
    .await;
    assert_eq!(status, 200);
    for tool in value["result"]["tools"].as_array().unwrap() {
        assert_eq!(tool["inputSchema"]["type"], "object");
    }
}

fn pending_uploads(lab: &Lab) -> usize {
    fs::read_dir(&lab.root)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".upload-")
        })
        .count()
}

async fn await_upload_cleanup(lab: &Lab) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pending_uploads(lab) != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn streamed_upload_exceeds_old_limit_without_length_and_retains_quota_guards() {
    use sha2::{Digest, Sha256};
    let lab = Lab::new();
    let chunk = axum::body::Bytes::from(vec![42u8; 65536]);
    let count = 65 * 1024 * 1024 / chunk.len();
    let mut digest = Sha256::new();
    for _ in 0..count {
        digest.update(&chunk);
    }
    let sha = format!("{:x}", digest.finalize());
    let stream = async_stream::stream! {
        for _ in 0..count {
            yield Ok::<_, std::io::Error>(chunk.clone());
        }
    };
    let request = Request::put(format!("/v1/blobs/{sha}"))
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::from_stream(stream))
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
    let value: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(value["result"]["bytes"], 65 * 1024 * 1024);
    let retained = lab.root.join("uploads").join(hash(b"alice"));
    assert_eq!(patchweft::file_hash(&retained.join(&sha)).unwrap(), sha);
    assert_eq!(pending_uploads(&lab), 0);

    // Sparse accounting fixture: quota includes retained content, never a bundle-sized allocation.
    let used = fs::metadata(retained.join(&sha)).unwrap().len();
    fs::File::create(retained.join("quota-fixture"))
        .unwrap()
        .set_len(api::MAX_RETAINED_UPLOADS - used)
        .unwrap();
    let error = lab.api.upload(ALICE, &hash(b"new"), b"new").unwrap_err();
    assert_eq!(error.status, 429);
    assert_eq!(error.code, "upload_quota");
    assert_eq!(pending_uploads(&lab), 0);
}

#[tokio::test]
async fn failed_streams_and_revoked_uploads_leave_no_staging_files() {
    let lab = Lab::new();
    let (started, ready) = tokio::sync::oneshot::channel();
    let stream = async_stream::stream! {
        yield Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"partial"));
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    };
    let request = Request::put(format!("/v1/blobs/{}", hash(b"partial")))
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::from_stream(stream))
        .unwrap();
    let pending = tokio::spawn(lab.router().oneshot(request));
    ready.await.unwrap();
    assert_eq!(pending_uploads(&lab), 1);
    pending.abort();
    let _ = pending.await;
    await_upload_cleanup(&lab).await;
    assert_eq!(pending_uploads(&lab), 0);

    let stream = async_stream::stream! {
        yield Ok(axum::body::Bytes::from_static(b"partial"));
        yield Err(std::io::Error::other("interrupted fixture"));
    };
    let request = Request::put(format!("/v1/blobs/{}", hash(b"partial")))
        .header("authorization", format!("Bearer {ALICE}"))
        .body(Body::from_stream(stream))
        .unwrap();
    let response = lab.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 400);
    await_upload_cleanup(&lab).await;
    assert_eq!(pending_uploads(&lab), 0);
    assert_eq!(
        lab.api
            .upload(ALICE, &hash(b"other"), b"partial")
            .unwrap_err()
            .status,
        422
    );
    assert_eq!(pending_uploads(&lab), 0);

    let oversized = lab.api.begin_upload(ALICE, &hash(b"large")).unwrap();
    oversized
        .as_file()
        .set_len(api::MAX_BUNDLE as u64 + 1)
        .unwrap();
    assert_eq!(
        lab.api
            .finish_upload(ALICE, &hash(b"large"), oversized)
            .unwrap_err()
            .status,
        413
    );
    assert_eq!(pending_uploads(&lab), 0);

    let mut pending = lab.api.begin_upload(ALICE, &hash(b"valid")).unwrap();
    std::io::Write::write_all(&mut pending, b"valid").unwrap();
    api::revoke(&Store::open(&lab.root).unwrap(), "alice").unwrap();
    assert_eq!(
        lab.api
            .finish_upload(ALICE, &hash(b"valid"), pending)
            .unwrap_err()
            .status,
        401
    );
    assert_eq!(pending_uploads(&lab), 0);
}
