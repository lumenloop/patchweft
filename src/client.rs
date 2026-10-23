use crate::{
    api::{Command, MAX_BUNDLE, MAX_HAVES, Query, TRANSFER_TIMEOUT_SECONDS},
    mcp::Backend,
    source,
};
use anyhow::{Context, Result, bail, ensure};
use reqwest::blocking::{Client, Response};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{BufRead, Read, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    time::Duration,
};

pub struct Remote {
    client: Client,
    endpoint: String,
    credential: String,
}
impl Remote {
    pub fn new(endpoint: &str, credential_file: &Path, ca: Option<&Path>) -> Result<Self> {
        let url = reqwest::Url::parse(endpoint)?;
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && ["/", ""].contains(&url.path()),
            "endpoint must be an origin without credentials, query or path"
        );
        let local = url.host_str().is_some_and(|h| {
            h == "localhost"
                || h.parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        ensure!(
            url.scheme() == "https" || (url.scheme() == "http" && local),
            "HTTP is only allowed on loopback; remote endpoints require HTTPS"
        );
        ensure!(
            fs::metadata(credential_file)?.permissions().mode() & 0o077 == 0,
            "credential file must be private to its owner (mode 0600)"
        );
        let credential = fs::read_to_string(credential_file)?.trim().to_owned();
        ensure!(
            (16..=256).contains(&credential.len()),
            "invalid credential file"
        );
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(40));
        if let Some(path) = ca {
            builder =
                builder.add_root_certificate(reqwest::Certificate::from_pem(&fs::read(path)?)?);
        }
        Ok(Self {
            client: builder.build()?,
            endpoint: endpoint.trim_end_matches('/').into(),
            credential,
        })
    }
    fn json_response(response: Response) -> Result<Value> {
        let status = response.status();
        let mut bytes = Vec::new();
        response.take(8 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 8 * 1024 * 1024, "response exceeds 8 MiB");
        let value: Value = serde_json::from_slice(&bytes).context("response was not JSON")?;
        if !status.is_success() && value.get("error").is_none() {
            bail!("HTTP {status}");
        }
        Ok(value)
    }
    pub fn query_value(&self, query: &Query) -> Result<Value> {
        let value = Self::json_response(
            self.client
                .post(format!("{}/v1/query", self.endpoint))
                .bearer_auth(&self.credential)
                .json(query)
                .send()?,
        )?;
        if value.get("error").is_some() {
            bail!("{value}");
        }
        Ok(value["result"].clone())
    }
    pub fn command_value(&self, key: &str, command: &Command) -> Result<Value> {
        Self::json_response(
            self.client
                .post(format!("{}/v1/commands", self.endpoint))
                .bearer_auth(&self.credential)
                .header("idempotency-key", key)
                .json(command)
                .send()?,
        )
    }
    pub fn publish(
        &self,
        key: &str,
        project: &str,
        bundle: &Path,
        commit: &str,
        message: &str,
    ) -> Result<Value> {
        ensure!(
            fs::metadata(bundle)?.len() <= MAX_BUNDLE as u64,
            "bundle exceeds 1 GiB"
        );
        let sha256 = crate::file_hash(bundle)?;
        let value = Self::json_response(
            self.client
                .put(format!("{}/v1/blobs/{sha256}", self.endpoint))
                .timeout(Duration::from_secs(TRANSFER_TIMEOUT_SECONDS))
                .bearer_auth(&self.credential)
                .body(File::open(bundle)?)
                .send()?,
        )?;
        ensure!(value.get("error").is_none(), "upload refused: {value}");
        self.command_value(
            key,
            &Command::Publish {
                project: project.into(),
                sha256,
                commit: commit.into(),
                message: message.into(),
            },
        )
    }
    fn download(
        &self,
        path: &str,
        destination: &Path,
        expected: Option<&str>,
        max: u64,
    ) -> Result<String> {
        ensure!(!destination.exists(), "download requires a new destination");
        let parent = destination.parent().context("destination parent")?;
        fs::create_dir_all(parent)?;
        let mut response = self
            .client
            .get(format!("{}{path}", self.endpoint))
            .timeout(Duration::from_secs(TRANSFER_TIMEOUT_SECONDS))
            .bearer_auth(&self.credential)
            .send()?;
        ensure!(
            response.status().is_success(),
            "download refused with HTTP {}",
            response.status()
        );
        let digest = response
            .headers()
            .get("x-content-sha256")
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned);
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        let copied = std::io::copy(&mut response.by_ref().take(max + 1), &mut temp)?;
        ensure!(copied <= max, "download exceeds its declared limit");
        let actual = crate::file_hash(temp.path())?;
        if let Some(expected) = expected {
            ensure!(actual == expected, "downloaded artifact digest mismatch");
        }
        if let Some(expected) = digest {
            ensure!(actual == expected, "downloaded source digest mismatch");
        }
        temp.as_file().sync_all()?;
        temp.persist_noclobber(destination)?;
        Ok(actual)
    }
    pub fn bundle(&self, revision: &str, destination: &Path) -> Result<Value> {
        self.bundle_since(revision, &[], destination)
    }
    /// A bundle of the revision without the history of `haves`, commits the caller holds. It
    /// verifies only in a repository holding them, unless the service had to send everything.
    pub fn bundle_since(
        &self,
        revision: &str,
        haves: &[String],
        destination: &Path,
    ) -> Result<Value> {
        ensure!(crate::api::digest(revision, 64), "invalid revision ID");
        ensure!(
            haves.len() <= MAX_HAVES
                && haves
                    .iter()
                    .all(|h| crate::api::digest(h, 40) || crate::api::digest(h, 64)),
            "at most {MAX_HAVES} haves, each a full commit ID"
        );
        let mut path = format!("/v1/revisions/{revision}/bundle");
        if !haves.is_empty() {
            path = format!("{path}?have={}", haves.join(","));
        }
        let sha256 = self.download(&path, destination, None, MAX_BUNDLE as u64)?;
        Ok(json!({"revision":revision,"destination":destination,"sha256":sha256}))
    }
    pub fn deliver(&self, action: &str, destination: &Path) -> Result<Value> {
        ensure!(!destination.exists(), "delivery requires a new directory");
        let manifest = self.query_value(&Query::Artifacts {
            action: action.into(),
        })?;
        let files = manifest["files"].as_object().context("artifact manifest")?;
        let parent = destination.parent().context("destination parent")?;
        fs::create_dir_all(parent)?;
        let temp = tempfile::tempdir_in(parent)?;
        for (name, metadata) in files {
            let relative = source::relative_path(name)?;
            let sha256 = metadata["sha256"].as_str().context("artifact hash")?;
            ensure!(crate::api::digest(sha256, 64), "invalid artifact hash");
            let path = temp.path().join(relative);
            self.download(
                &format!("/v1/artifacts/{action}/{sha256}"),
                &path,
                Some(sha256),
                metadata["bytes"].as_u64().context("artifact length")?,
            )?;
            fs::set_permissions(
                &path,
                fs::Permissions::from_mode(
                    (metadata["mode"].as_u64().context("artifact mode")? & 0o777) as u32,
                ),
            )?;
        }
        fs::write(
            temp.path().join(".patchweft-receipt.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        // renameat2 NOREPLACE prevents a late-created directory from being overwritten.
        crate::worker::publish_directory(temp.path(), destination)?;
        Ok(manifest)
    }
    pub fn follow(&self, mut after: i64, project: Option<&str>) -> Result<()> {
        let output = std::io::stdout();
        let mut output = output.lock();
        let mut delay = Duration::ZERO;
        loop {
            std::thread::sleep(delay);
            delay = (delay * 2)
                .max(Duration::from_millis(250))
                .min(Duration::from_secs(5));
            let mut url = reqwest::Url::parse(&format!("{}/v1/events", self.endpoint))?;
            if let Some(project) = project {
                url.query_pairs_mut().append_pair("project", project);
            }
            let request = self
                .client
                .get(url)
                .bearer_auth(&self.credential)
                .header("accept", "text/event-stream")
                .header("last-event-id", after.to_string())
                .timeout(Duration::from_secs(3600));
            let response = match request.send() {
                Ok(response) => response,
                Err(_) => continue, // Keep the last flushed cursor across transport failures.
            };
            if response.status().is_server_error() || response.status().as_u16() == 429 {
                continue;
            }
            ensure!(
                response.status().is_success(),
                "event stream refused with HTTP {}",
                response.status()
            );
            let mut reader = std::io::BufReader::new(response);
            let mut id = None;
            let mut kind = String::new();
            let mut data = String::new();
            loop {
                let mut line = Vec::new();
                let count = match (&mut reader)
                    .take(2 * 1024 * 1024 + 1)
                    .read_until(b'\n', &mut line)
                {
                    Ok(count) => count,
                    Err(_) => break,
                };
                if count == 0 {
                    break;
                }
                ensure!(line.len() <= 2 * 1024 * 1024, "oversized event");
                let line = std::str::from_utf8(&line)?.trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    if !data.is_empty() {
                        let value: Value = serde_json::from_str(&data)?;
                        if kind == "error" {
                            if matches!(value["error"]["code"].as_str(), Some("busy" | "internal"))
                            {
                                break;
                            }
                            bail!("event stream refused: {value}");
                        }
                        writeln!(output, "{}", json!({"event":kind,"seq":id,"data":value}))?;
                        output.flush()?;
                        if let Some(cursor) = id {
                            after = cursor;
                        }
                        delay = Duration::from_millis(250);
                    }
                    id = None;
                    kind.clear();
                    data.clear();
                } else if let Some(v) = line.strip_prefix("id:") {
                    id = Some(v.trim().parse::<i64>()?);
                } else if let Some(v) = line.strip_prefix("event:") {
                    kind = v.trim().into();
                } else if let Some(v) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(v.strip_prefix(' ').unwrap_or(v));
                    ensure!(data.len() <= 2 * 1024 * 1024, "oversized event");
                }
            }
        }
    }
}
impl Backend for Remote {
    fn query(&self, query: &Query) -> std::result::Result<Value, Value> {
        self.query_value(query)
            .map_err(|e| json!({"error":{"code":"remote_error","message":e.to_string()}}))
    }
    fn command(&self, key: &str, command: &Command) -> std::result::Result<Value, Value> {
        self.command_value(key, command)
            .map_err(|e| json!({"error":{"code":"remote_error","message":e.to_string()}}))
    }
}
