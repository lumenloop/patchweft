use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub id: String,
    pub project: String,
    pub commit: String,
    pub tree: String,
    pub parents: Vec<String>,
    pub owner: String,
    pub message: String,
    pub created: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workspace {
    pub id: String,
    pub project: String,
    pub base: String,
    pub tip: String,
    pub owner: String,
    pub path: PathBuf,
}

fn default_slots() -> u32 {
    1
}
fn default_memory() -> u32 {
    1024
}
fn default_timeout() -> u64 {
    1800
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Reuse {
    /// Share outstanding requests; a completed observation is not a new observation.
    #[default]
    Inflight,
    /// Explicitly audited hermetic action, with the complete commit/history as input.
    Exact,
    /// Explicitly audited files-only action; every input must be listed.
    Files,
    /// Each request is physically independent.
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Report {
    /// "command", "vitest", "junit", or "node-tap".
    pub kind: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub minimum: u64,
    #[serde(default)]
    pub allow_skips: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub name: String,
    /// Exact argv. Shell expansion only happens when the profile explicitly invokes a shell.
    pub command: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default = "default_slots")]
    pub slots: u32,
    #[serde(default = "default_memory")]
    pub memory_mb: u32,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub reuse: Reuse,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default)]
    pub audit: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    /// Optional bounded finalizer for resources outside the process tree, such as owned containers.
    #[serde(default)]
    pub cleanup: Vec<String>,
    #[serde(default)]
    pub report: Report,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub certifying: bool,
    /// Absolute local executable paths, hashed when the profile is registered and before each run.
    pub tools: BTreeMap<String, PathBuf>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub jobs: Vec<Job>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredProfile {
    pub id: String,
    pub definition: Profile,
    pub tools: BTreeMap<String, String>,
    pub engine: String,
    pub platform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    Queued,
    Running,
    Passed,
    Failed,
    Interrupted,
    Canceled,
}

impl State {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::Canceled => "canceled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub id: String,
    pub key: String,
    pub revision: String,
    pub profile: String,
    pub job: String,
    pub dependencies: Vec<String>,
    pub created: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub id: String,
    pub revision: String,
    pub profile: String,
    pub required: BTreeMap<String, String>,
    pub prerequisites: Vec<String>,
    pub owner: String,
    pub created: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub id: String,
    pub action: String,
    pub worker_pid: u32,
    pub worker_start: String,
    pub supervisor_pid: Option<u32>,
    pub supervisor_start: Option<String>,
    pub slots: u32,
    pub memory_mb: u32,
    pub created: f64,
    pub started: Option<f64>,
    pub ended: Option<f64>,
    pub exit_code: Option<i32>,
    pub result: Option<serde_json::Value>,
}
