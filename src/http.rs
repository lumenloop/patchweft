use crate::api::{Api, ApiError, Command, MAX_BUNDLE, Query, TRANSFER_TIMEOUT_SECONDS};
use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Path, Query as UrlQuery, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post, put},
};
use http_body_util::BodyExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    convert::Infallible,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncReadExt, sync::Semaphore};

#[derive(Clone)]
pub struct Service {
    pub api: Arc<Api>,
    calls: Arc<Semaphore>,
    work: Arc<Semaphore>,
    ingress: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    transfers: Arc<Semaphore>,
    pub origins: BTreeSet<String>,
}
impl Service {
    pub fn new(api: Arc<Api>, origins: BTreeSet<String>) -> Self {
        Self {
            api,
            calls: Arc::new(Semaphore::new(64)),
            streams: Arc::new(Semaphore::new(32)),
            transfers: Arc::new(Semaphore::new(4)),
            work: Arc::new(Semaphore::new(64)),
            ingress: Arc::new(Semaphore::new(4)),
            origins,
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(self.body()),
        )
            .into_response()
    }
}
fn header<'a>(headers: &'a HeaderMap, name: &str) -> std::result::Result<&'a str, ApiError> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::new(
                400,
                "missing_header",
                &format!("Missing or invalid {name} header."),
            )
        })
}
pub fn token(headers: &HeaderMap) -> std::result::Result<String, ApiError> {
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| {
            ApiError::new(
                401,
                "unauthorized",
                "A scoped Bearer credential is required.",
            )
        })?;
    Ok(value.into())
}
fn busy() -> ApiError {
    ApiError::new(
        503,
        "busy",
        "Service concurrency budget is full; retry with backoff.",
    )
}

async fn boundary(State(service): State<Service>, request: Request, next: Next) -> Response {
    if let Some(origin) = request.headers().get("origin")
        && origin
            .to_str()
            .ok()
            .is_none_or(|o| !service.origins.contains(o))
    {
        return ApiError::new(403, "origin_forbidden", "Origin is not allowed.").into_response();
    }
    let token = match token(request.headers()) {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let permit = match service.calls.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return busy().into_response(),
    };
    let transfer = if request.uri().path().starts_with("/v1/blobs/") {
        match service.ingress.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => return busy().into_response(),
        }
    } else {
        None
    };
    let api = service.api.clone();
    if let Err(error) = blocking(service.work.clone(), move || api.authenticate(&token)).await {
        return error.into_response();
    }
    let transfer_request = request.uri().path().starts_with("/v1/blobs/")
        || (request.uri().path().starts_with("/v1/revisions/")
            && request.uri().path().ends_with("/bundle"));
    let deadline = if transfer_request {
        TRANSFER_TIMEOUT_SECONDS
    } else {
        35
    };
    let response =
        match tokio::time::timeout(Duration::from_secs(deadline), next.run(request)).await {
            Ok(response) => response,
            Err(_) => ApiError::new(
                504,
                "request_timeout",
                "Retry a mutation with its original idempotency key.",
            )
            .into_response(),
        };
    drop(transfer);
    drop(permit);
    response
}

pub fn router(service: Service) -> Router {
    Router::new()
        .route("/v1/commands", post(commands))
        .route("/v1/query", post(queries))
        .route("/v1/events", get(events))
        .route("/v1/blobs/{sha256}", put(upload))
        .route("/v1/revisions/{revision}/bundle", get(bundle))
        .route("/v1/artifacts/{action}/{sha256}", get(artifact))
        .route("/mcp", post(mcp))
        .fallback(|| async { ApiError::new(404, "not_found", "No such endpoint.") })
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(service.clone(), boundary))
        .with_state(service)
}
fn parse_json<T>(
    value: std::result::Result<Json<T>, JsonRejection>,
) -> std::result::Result<T, ApiError> {
    value.map(|Json(v)| v).map_err(|error| {
        ApiError::new(
            error.status().as_u16(),
            "invalid_json",
            "The JSON body does not match this endpoint's schema.",
        )
    })
}
async fn blocking<T: Send + 'static>(
    gate: Arc<Semaphore>,
    work: impl FnOnce() -> std::result::Result<T, ApiError> + Send + 'static,
) -> std::result::Result<T, ApiError> {
    start_blocking(gate, work)?
        .await
        .map_err(|_| ApiError::new(500, "internal", "Application worker failed."))?
}
fn start_blocking<T: Send + 'static>(
    gate: Arc<Semaphore>,
    work: impl FnOnce() -> std::result::Result<T, ApiError> + Send + 'static,
) -> std::result::Result<tokio::task::JoinHandle<std::result::Result<T, ApiError>>, ApiError> {
    let permit = gate.try_acquire_owned().map_err(|_| busy())?;
    // The closure owns capacity even if its HTTP caller disconnects or times out.
    Ok(tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    }))
}
async fn commands(
    State(service): State<Service>,
    headers: HeaderMap,
    body: std::result::Result<Json<Command>, JsonRejection>,
) -> Response {
    let result = async {
        let command = parse_json(body)?;
        let token = token(&headers)?;
        let key = header(&headers, "idempotency-key")?.to_owned();
        blocking(service.work.clone(), move || {
            service.api.command(&token, &key, &command)
        })
        .await
    }
    .await;
    match result {
        Ok(value) => (
            StatusCode::from_u16(value["status"].as_u64().unwrap_or(200) as u16)
                .unwrap_or(StatusCode::OK),
            Json(value),
        )
            .into_response(),
        Err(error) => error.into_response(),
    }
}
async fn queries(
    State(service): State<Service>,
    headers: HeaderMap,
    body: std::result::Result<Json<Query>, JsonRejection>,
) -> Response {
    let result = async {
        let query = parse_json(body)?;
        let token = token(&headers)?;
        blocking(service.work.clone(), move || {
            service.api.query(&token, &query)
        })
        .await
    }
    .await;
    match result {
        Ok(value) => Json(json!({"api_version":"1","result":value})).into_response(),
        Err(e) => e.into_response(),
    }
}

#[derive(Deserialize)]
struct EventParameters {
    after: Option<i64>,
    project: Option<String>,
    #[serde(default = "event_limit")]
    limit: usize,
}
fn event_limit() -> usize {
    1000
}
async fn events(
    State(service): State<Service>,
    headers: HeaderMap,
    parameters: std::result::Result<UrlQuery<EventParameters>, QueryRejection>,
) -> Response {
    let result=async {
        let token=token(&headers)?;
        let UrlQuery(parameters)=parameters.map_err(|e|ApiError::new(e.status().as_u16(),"invalid_query","Invalid event query parameters."))?;
        let header_cursor=headers.get("last-event-id").map(|h|h.to_str().ok().and_then(|s|s.parse::<i64>().ok())
            .ok_or_else(||ApiError::new(400,"invalid_cursor","Last-Event-ID must be an integer."))).transpose()?;
        if header_cursor.is_some() && parameters.after.is_some() && header_cursor!=parameters.after {
            return Err(ApiError::new(400,"cursor_conflict","Header and query cursors differ."));
        }
        let after=header_cursor.or(parameters.after).unwrap_or(0);
        let query=Query::Events {after,project:parameters.project.clone(),limit:parameters.limit};
        let api=service.api.clone(); let credential=token.clone();
        let first=blocking(service.work.clone(),move||api.query(&credential,&query)).await?;
        let wants_stream=headers.get("accept").and_then(|h|h.to_str().ok()).is_some_and(|h|h.contains("text/event-stream"));
        if !wants_stream { return Ok(Json(json!({"api_version":"1","result":first})).into_response()); }
        let permit=service.streams.clone().try_acquire_owned().map_err(|_|busy())?;
        let stream=async_stream::stream! {
            let _permit=permit;
            let mut cursor=after;
            let mut page=first;
            loop {
                let next=page["next_after"].as_i64().unwrap_or(cursor);
                if let Some(rows)=page["events"].as_array() {
                    for row in rows {
                        yield Ok::<_,Infallible>(Event::default().id(row["seq"].to_string()).event("patchweft")
                            .data(row.to_string()));
                    }
                }
                if next!=cursor {
                    // Advance even when rows were filtered by project permissions.
                    yield Ok(Event::default().id(next.to_string()).event("cursor")
                        .data(json!({"after":next}).to_string()));
                }
                cursor=next;
                if page["more"]!=true { tokio::time::sleep(Duration::from_millis(250)).await; }
                let api=service.api.clone(); let credential=token.clone();
                let query=Query::Events {after:cursor,project:parameters.project.clone(),limit:parameters.limit};
                match blocking(service.work.clone(),move||api.query(&credential,&query)).await {
                    Ok(value)=>page=value,
                    Err(error)=>{
                        yield Ok(Event::default().event("error").data(error.body().to_string()));
                        break;
                    }
                }
            }
        };
        Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(10))).into_response())
    }.await;
    result.unwrap_or_else(IntoResponse::into_response)
}
async fn upload(
    State(service): State<Service>,
    Path(sha256): Path<String>,
    request: Request,
) -> Response {
    let result = async {
        let token = token(request.headers())?;
        if request
            .headers()
            .get("content-length")
            .is_some_and(|value| {
                value
                    .to_str()
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|bytes| bytes > MAX_BUNDLE as u64)
            })
        {
            return Err(upload_body_error(413));
        }
        let transfer = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let api = service.api.clone();
        let credential = token.clone();
        let digest = sha256.clone();
        let mut temp = blocking(service.work.clone(), move || {
            api.begin_upload(&credential, &digest)
        })
        .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let writer = start_blocking(service.work.clone(), move || {
            let _transfer = transfer;
            receive_upload(receiver, temp.as_file_mut())?;
            service.api.finish_upload(&token, &sha256, temp)
        })?;
        let received = spool_upload(request.into_body(), sender, MAX_BUNDLE).await;
        if let Err(error) = received
            && error.code != "upload_writer_closed"
        {
            // Dropping the handle does not cancel the disk writer. It owns the
            // staged file and both capacity permits until its last write finishes.
            return Err(error);
        }
        writer
            .await
            .map_err(|_| ApiError::new(500, "internal", "Upload worker failed."))?
    }
    .await;
    match result {
        Ok(v) => Json(json!({"api_version":"1","result":v})).into_response(),
        Err(e) => e.into_response(),
    }
}
fn upload_body_error(status: u16) -> ApiError {
    ApiError::new(
        status,
        "invalid_body",
        "Upload could not be read within its size limit.",
    )
}
enum UploadPart {
    Data(Bytes),
    Complete,
}
fn receive_upload(
    mut receiver: tokio::sync::mpsc::Receiver<UploadPart>,
    file: &mut impl std::io::Write,
) -> std::result::Result<(), ApiError> {
    while let Some(part) = receiver.blocking_recv() {
        match part {
            UploadPart::Data(data) => file.write_all(&data).map_err(ApiError::internal)?,
            UploadPart::Complete => return Ok(()),
        }
    }
    Err(upload_body_error(400))
}
async fn spool_upload(
    mut body: Body,
    sender: tokio::sync::mpsc::Sender<UploadPart>,
    limit: usize,
) -> std::result::Result<(), ApiError> {
    // Direct Body consumption bypasses DefaultBodyLimit: count every data frame,
    // including uploads with no Content-Length, before writing it to disk.
    let mut bytes = 0usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| upload_body_error(400))?;
        if let Ok(data) = frame.into_data() {
            if data.len() > limit - bytes {
                return Err(upload_body_error(413));
            }
            bytes += data.len();
            // Bound queued disk work as well as the total upload size.
            for chunk in data.chunks(65536) {
                sender
                    .send(UploadPart::Data(Bytes::copy_from_slice(chunk)))
                    .await
                    .map_err(|_| upload_writer_closed())?;
            }
        }
    }
    sender
        .send(UploadPart::Complete)
        .await
        .map_err(|_| upload_writer_closed())
}
fn upload_writer_closed() -> ApiError {
    ApiError::new(500, "upload_writer_closed", "Upload writer stopped.")
}
async fn stream_file(
    file: std::fs::File,
    keep: Option<tempfile::TempDir>,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Response {
    let mut file = tokio::fs::File::from_std(file);
    let stream = async_stream::try_stream! {
        let _keep=keep; let _permit=permit;
        loop {
            let mut bytes=vec![0u8;65536];
            let count=file.read(&mut bytes).await?;
            if count==0 {break;}
            bytes.truncate(count);
            yield Bytes::from(bytes);
        }
    };
    let stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>,
    > = Box::pin(stream);
    (
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        Body::from_stream(stream),
    )
        .into_response()
}
async fn bundle(
    State(service): State<Service>,
    headers: HeaderMap,
    Path(revision): Path<String>,
) -> Response {
    let result = async {
        let permit = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let token = token(&headers)?;
        let (temp, file, sha256, permit) = blocking(service.work.clone(), move || {
            let (temp, path) = service.api.source_bundle(&token, &revision)?;
            let sha256 = crate::file_hash(&path)
                .map_err(|_| ApiError::new(500, "internal", "Bundle could not be hashed."))?;
            let file = std::fs::File::open(path)
                .map_err(|_| ApiError::new(500, "internal", "Bundle could not be opened."))?;
            Ok((temp, file, sha256, permit))
        })
        .await?;
        let mut response = stream_file(file, Some(temp), permit).await;
        response
            .headers_mut()
            .insert("x-content-sha256", sha256.parse().expect("digest header"));
        Ok::<_, ApiError>(response)
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}
async fn artifact(
    State(service): State<Service>,
    headers: HeaderMap,
    Path((action, sha256)): Path<(String, String)>,
) -> Response {
    let result = async {
        let permit = service
            .transfers
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let token = token(&headers)?;
        let (file, permit) = blocking(service.work.clone(), move || {
            Ok((service.api.artifact(&token, &action, &sha256)?, permit))
        })
        .await?;
        Ok::<_, ApiError>(stream_file(file, None, permit).await)
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}
async fn mcp(
    State(service): State<Service>,
    headers: HeaderMap,
    body: std::result::Result<Json<Value>, JsonRejection>,
) -> Response {
    let result = async {
        let message = parse_json(body)?;
        let token = token(&headers)?;
        blocking(service.work.clone(), move || {
            Ok(crate::mcp::http(&service.api, &token, &headers, message))
        })
        .await
    }
    .await;
    match result {
        Ok((status, Some(body))) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(body),
        )
            .into_response(),
        Ok((status, None)) => StatusCode::from_u16(status)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            .into_response(),
        Err(error) => error.into_response(),
    }
}

pub fn serve(
    root: &FsPath,
    listen: SocketAddr,
    certificate: Option<PathBuf>,
    key: Option<PathBuf>,
    ready: Option<PathBuf>,
    origins: BTreeSet<String>,
) -> Result<Value> {
    ensure!(
        certificate.is_some() == key.is_some(),
        "TLS requires both certificate and private key paths"
    );
    ensure!(
        listen.ip().is_loopback() || certificate.is_some(),
        "non-loopback listeners require TLS"
    );
    ensure!(!origins.contains("*"), "origins must be explicit");
    let api = Arc::new(Api::open(root)?);
    let listener = std::net::TcpListener::bind(listen)?;
    // Tokio requires nonblocking sockets; otherwise a later accept can stall shutdown.
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(64)
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let app = router(Service::new(api, origins));
        let handle = axum_server::Handle::new();
        let stop = handle.clone();
        tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal");
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
            stop.graceful_shutdown(Some(Duration::from_secs(5)));
        });
        let config = match (certificate, key) {
            (Some(cert), Some(key)) => {
                Some(axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?)
            }
            _ => None,
        };
        if let Some(path) = ready {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?;
            writeln!(
                file,
                "{}",
                json!({"address":address,"tls":config.is_some(),"pid":std::process::id()})
            )?;
        }
        eprintln!("Patchweft listening on {address}");
        if let Some(config) = config {
            axum_server::from_tcp_rustls(listener, config)?
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        } else {
            axum_server::from_tcp(listener)?
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        }
        Ok::<_, anyhow::Error>(json!({"stopped":true}))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn streamed_upload_counts_frames_and_rejects_interrupted_bodies() {
        async fn spool(body: Body) -> (std::result::Result<(), ApiError>, tempfile::NamedTempFile) {
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            let writer = tokio::task::spawn_blocking(move || {
                let mut temp = tempfile::NamedTempFile::new().unwrap();
                let _ = receive_upload(receiver, temp.as_file_mut());
                temp
            });
            let result = spool_upload(body, sender, 8).await;
            (result, writer.await.unwrap())
        }
        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
            yield Ok(Bytes::from_static(b"5678"));
        };
        let (result, temp) = spool(Body::from_stream(stream)).await;
        result.unwrap();
        assert_eq!(std::fs::read(temp.path()).unwrap(), b"12345678");

        let stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
            yield Ok(Bytes::from_static(b"56789"));
        };
        let (result, temp) = spool(Body::from_stream(stream)).await;
        assert_eq!(result.unwrap_err().status, 413);
        assert_eq!(std::fs::read(temp.path()).unwrap(), b"1234");

        let stream = async_stream::stream! {
            yield Ok(Bytes::from_static(b"1234"));
            yield Err(std::io::Error::other("interrupted fixture"));
        };
        let (result, _) = spool(Body::from_stream(stream)).await;
        assert_eq!(result.unwrap_err().status, 400);
    }

    #[tokio::test]
    async fn canceled_upload_holds_capacity_until_a_pending_disk_write_finishes() {
        struct HeldWrite {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl std::io::Write for HeldWrite {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.entered.take().unwrap().send(()).unwrap();
                self.release.recv().unwrap();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let gate = Arc::new(Semaphore::new(1));
        let transfers = Arc::new(Semaphore::new(1));
        let transfer = transfers.clone().acquire_owned().await.unwrap();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, hold) = std::sync::mpsc::channel();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let writer = start_blocking(gate.clone(), move || {
            let _transfer = transfer;
            receive_upload(
                receiver,
                &mut HeldWrite {
                    entered: Some(entered),
                    release: hold,
                },
            )
        })
        .unwrap();
        let request = tokio::spawn(async move {
            let stream = async_stream::stream! {
                yield Ok::<_, std::io::Error>(Bytes::from_static(b"held"));
                std::future::pending::<()>().await;
            };
            spool_upload(Body::from_stream(stream), sender, 8).await?;
            writer.await.unwrap()
        });
        ready.await.unwrap();
        request.abort();
        let _ = request.await;
        assert_eq!(gate.available_permits(), 0);
        assert_eq!(transfers.available_permits(), 0);
        release.send(()).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(5), transfers.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        let permit = tokio::time::timeout(Duration::from_secs(5), gate.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(transfers.available_permits(), 1);
        assert_eq!(gate.available_permits(), 1);
    }

    #[tokio::test]
    async fn abandoned_future_holds_capacity_until_its_blocking_work_finishes() {
        let gate = Arc::new(Semaphore::new(1));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, hold) = std::sync::mpsc::channel();
        let worker_gate = gate.clone();
        let task = tokio::spawn(async move {
            blocking(worker_gate, move || {
                started.send(()).unwrap();
                hold.recv().unwrap();
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        task.abort();
        let _ = task.await;
        assert_eq!(gate.available_permits(), 0);
        assert_eq!(
            blocking(gate.clone(), || Ok(())).await.unwrap_err().status,
            503
        );
        release.send(()).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(5), gate.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(permit);
        assert_eq!(gate.available_permits(), 1);
    }
}
