//! Stateless tool adapter. An accepted durable action outlives its RPC.
use crate::api::{Api, Command, Query};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::io::{BufRead, Read, Write};

pub const MODERN: &str = "2026-07-28";
pub const LEGACY: &str = "2025-11-25";
type BackendResult = std::result::Result<Value, Value>;
pub trait Backend {
    fn query(&self, query: &Query) -> BackendResult;
    fn command(&self, key: &str, command: &Command) -> BackendResult;
}
struct Local<'a> {
    api: &'a Api,
    token: &'a str,
}
impl Backend for Local<'_> {
    fn query(&self, query: &Query) -> BackendResult {
        self.api.query(self.token, query).map_err(|e| e.body())
    }
    fn command(&self, key: &str, command: &Command) -> BackendResult {
        self.api
            .command(self.token, key, command)
            .map_err(|e| e.body())
    }
}
#[derive(Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct CommandArguments {
    /// A stable key for this logical operation. Preserve it across retries.
    idempotency_key: String,
    command: Command,
}
fn tools() -> Value {
    let mut query_schema =
        serde_json::to_value(schemars::schema_for!(Query)).expect("query schema");
    // MCP requires an explicitly object-typed root, including schemas with oneOf.
    query_schema["type"] = json!("object");
    json!([
        {"name":"patchweft_query","description":"Inspect permitted projects, profiles, revisions, actions, candidates, paginated events, logs, artifacts, a project's frontier certificate or a durable operation. Persist event cursors between calls.",
            "inputSchema":query_schema,"annotations":{"readOnlyHint":true}},
        {"name":"patchweft_command","description":"Publish or compose revisions, request shared computation, submit validation, detach your interest, retry owned candidates, or perform an authorized review/promotion. Submit's optional retain_from keeps passed obligations of a candidate for the same revision whose job identity is unchanged; a retained pass is the earlier observation, not a fresh test. Returns a durable handle promptly. Preserve idempotency_key when retrying a lost response. Closing this tool call never detaches accepted work.",
            "inputSchema":schemars::schema_for!(CommandArguments),"annotations":{"readOnlyHint":false,"idempotentHint":true}}
    ])
}
fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn unsupported(id: Value, requested: &str) -> Value {
    let mut value = error(id, -32022, "Unsupported protocol version");
    value["error"]["data"] = json!({"supported":[MODERN,LEGACY],"requested":requested});
    value
}

/// Legacy state is transport-local and only used by the stdio compatibility path.
/// Modern requests always carry and validate their own version and capabilities.
pub fn dispatch(
    backend: &impl Backend,
    message: Value,
    legacy_ready: bool,
) -> (u16, Option<Value>) {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    if !message.is_object()
        || message["jsonrpc"] != "2.0"
        || !message["method"].is_string()
        || message
            .get("id")
            .is_some_and(|id| !(id.is_string() || id.is_i64() || id.is_u64()))
    {
        return (400, Some(error(id, -32600, "Invalid request")));
    }
    let method = message["method"].as_str().expect("method");
    if message.get("id").is_none() {
        // Notifications, including cancellation, never invoke application mutations.
        return (202, None);
    }
    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return (400, Some(error(id, -32602, "Params must be an object")));
    }
    if method == "initialize" {
        if !params["protocolVersion"].is_string()
            || !params["capabilities"].is_object()
            || !params["clientInfo"].is_object()
        {
            return (
                400,
                Some(error(id, -32602, "Invalid initialization parameters")),
            );
        }
        return (
            200,
            Some(json!({"jsonrpc":"2.0","id":id,"result":{
                "protocolVersion":LEGACY,"capabilities":{"tools":{}},
                "serverInfo":{"name":"patchweft","version":env!("CARGO_PKG_VERSION")},
                "instructions":"Accepted operations are durable. Preserve idempotency keys; cancel work explicitly with patchweft_command."
            }})),
        );
    }
    let version = params["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str();
    let modern = if let Some(version) = version {
        if version != MODERN {
            return (400, Some(unsupported(id, version)));
        }
        if !params["_meta"]["io.modelcontextprotocol/clientCapabilities"].is_object() {
            return (
                400,
                Some(error(
                    id,
                    -32602,
                    "Modern requests require clientCapabilities metadata",
                )),
            );
        }
        true
    } else if legacy_ready {
        false
    } else {
        return (
            400,
            Some(error(
                id,
                -32602,
                "Request requires protocol metadata or legacy initialization",
            )),
        );
    };
    let result = match method {
        "server/discover" if modern => {
            json!({"supportedVersions":[MODERN,LEGACY],"capabilities":{"tools":{}},
            "ttlMs":0,"cacheScope":"private",
            "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"patchweft","version":env!("CARGO_PKG_VERSION")}},
            "instructions":"Use durable operation IDs and event cursors. Transport cancellation does not cancel an accepted job."})
        }
        "ping" => json!({}),
        "tools/list" => {
            if params.get("cursor").is_some_and(|v| !v.is_null()) {
                return (
                    400,
                    Some(error(id, -32602, "Tool listing has no additional pages")),
                );
            }
            let mut value = json!({"tools":tools()});
            if modern {
                value["ttlMs"] = json!(0);
                value["cacheScope"] = json!("private");
            }
            value
        }
        "tools/call" => {
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let result = match params["name"].as_str() {
                Some("patchweft_query") => match serde_json::from_value::<Query>(arguments) {
                    Ok(query) => backend.query(&query),
                    Err(_) => {
                        return (
                            400,
                            Some(error(id, -32602, "Invalid patchweft_query arguments")),
                        );
                    }
                },
                Some("patchweft_command") => {
                    match serde_json::from_value::<CommandArguments>(arguments) {
                        Ok(args) => backend.command(&args.idempotency_key, &args.command),
                        Err(_) => {
                            return (
                                400,
                                Some(error(id, -32602, "Invalid patchweft_command arguments")),
                            );
                        }
                    }
                }
                _ => return (400, Some(error(id, -32602, "Unknown tool"))),
            };
            let (value, failed) = match result {
                Ok(value) => {
                    let failed = value.get("error").is_some();
                    (value, failed)
                }
                Err(value) => (value, true),
            };
            json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value,"isError":failed})
        }
        _ => return (404, Some(error(id, -32601, "Method not found"))),
    };
    let mut result = result;
    if modern {
        result["resultType"] = json!("complete");
    }
    (200, Some(json!({"jsonrpc":"2.0","id":id,"result":result})))
}

pub fn http(api: &Api, token: &str, headers: &HeaderMap, message: Value) -> (u16, Option<Value>) {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    let get = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if !get("accept")
        .is_some_and(|v| v.contains("application/json") && v.contains("text/event-stream"))
    {
        return (
            406,
            Some(error(
                id,
                -32600,
                "Accept must include application/json and text/event-stream",
            )),
        );
    }
    let version = get("mcp-protocol-version");
    let body_version =
        message["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"].as_str();
    let legacy = body_version.is_none()
        && ((message["method"] == "initialize" && version.is_none_or(|v| v == LEGACY))
            || version == Some(LEGACY));
    if !legacy && message.get("id").is_some() {
        if let Some(v) = version
            && ![MODERN, LEGACY].contains(&v)
        {
            return (400, Some(unsupported(id, v)));
        }
        if version.is_none()
            || version != body_version
            || get("mcp-method") != message["method"].as_str()
        {
            return (
                400,
                Some(error(
                    id,
                    -32020,
                    "Required MCP headers are missing or differ from the body",
                )),
            );
        }
        if message["method"] == "tools/call" {
            use base64::Engine;
            let name = get("mcp-name").and_then(|name| {
                if let Some(encoded) = name
                    .strip_prefix("=?base64?")
                    .and_then(|s| s.strip_suffix("?="))
                {
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()
                        .and_then(|b| String::from_utf8(b).ok())
                } else {
                    Some(name.to_owned())
                }
            });
            if name.as_deref() != message["params"]["name"].as_str() {
                return (
                    400,
                    Some(error(id, -32020, "Mcp-Name differs from the tool name")),
                );
            }
        }
    }
    dispatch(&Local { api, token }, message, legacy)
}

pub fn stdio(backend: &impl Backend) -> anyhow::Result<()> {
    let mut input = std::io::stdin().lock();
    let mut output = std::io::stdout().lock();
    let mut initialized = false;
    let mut ready = false;
    loop {
        let mut line = Vec::new();
        let count = (&mut input)
            .take(2 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if count == 0 {
            break;
        }
        if line.len() > 2 * 1024 * 1024 {
            writeln!(
                output,
                "{}",
                error(Value::Null, -32600, "Message exceeds 2 MiB")
            )?;
            output.flush()?;
            break;
        }
        let message = match serde_json::from_slice::<Value>(&line) {
            Ok(v) => v,
            Err(_) => {
                writeln!(output, "{}", error(Value::Null, -32700, "Parse error"))?;
                output.flush()?;
                continue;
            }
        };
        if initialized && message["method"] == "notifications/initialized" {
            ready = true;
        }
        let is_initialize = message["method"] == "initialize";
        let (_, response) = dispatch(backend, message, ready);
        if let Some(response) = response {
            if is_initialize && response.get("result").is_some() {
                initialized = true;
            }
            writeln!(output, "{}", response)?;
            output.flush()?;
        }
    }
    Ok(())
}
