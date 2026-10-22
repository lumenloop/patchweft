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
fn yes() -> bool {
    true
}
fn is_true(value: &bool) -> bool {
    *value
}

/// Version tag of the frozen job recipe builder. A builder change needs a new tag.
pub const JOB_RECIPE: &str = "patchweft.job/1";

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
    /// False makes `slots` a pure admission reservation: no `PATCHWEFT_SLOTS`, not in identity.
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub export_slots: bool,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reservation {
    pub slots: u32,
    pub memory_mb: u32,
}

impl Reservation {
    pub fn max(self, other: Self) -> Self {
        Self {
            slots: self.slots.max(other.slots),
            memory_mb: self.memory_mb.max(other.memory_mb),
        }
    }
}

impl From<&Job> for Reservation {
    fn from(job: &Job) -> Self {
        Self {
            slots: job.slots,
            memory_mb: job.memory_mb,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionIdentity {
    pub version: String,
    pub recipe: String,
    pub class: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Origin {
    pub owner: String,
    pub context: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ActionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation: Option<Reservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_from: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub not_retained: BTreeMap<String, String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}
