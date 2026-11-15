//! Wire types shared by the control plane, the node agent and the CLI.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Lowercase hex BLAKE3 digest of a blob's content.
pub type Hash = String;

pub fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Whether `name` is usable as a relative path under a task's input directory.
pub fn safe_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.contains('\0')
        && name
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

// ---------------------------------------------------------------- resources

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct GpuInfo {
    pub index: u32,
    pub uuid: String,
    pub name: String,
    pub memory_mb: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct GpuStat {
    pub index: u32,
    pub memory_used_mb: u64,
    pub util_pct: u32,
}

/// What a device is: detected once at agent start.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct NodeInfo {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub cpus: u32,
    pub memory_mb: u64,
    pub gpus: Vec<GpuInfo>,
}

/// What a device is doing right now: refreshed on every heartbeat.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct NodeStats {
    pub load1: f32,
    pub mem_available_mb: u64,
    pub disk_free_bytes: u64,
    pub cache_bytes: u64,
    pub cache_blobs: u64,
    pub gpus: Vec<GpuStat>,
}

/// Per-task resource request. CPU and memory are admission-control accounting; GPUs are
/// allocated exclusively and enforced through `CUDA_VISIBLE_DEVICES`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Resources {
    pub cpus: u32,
    pub memory_mb: u64,
    pub gpus: u32,
    /// Minimum VRAM each allocated GPU must have (heterogeneous fleets).
    #[serde(default)]
    pub gpu_memory_mb: u64,
}

// ---------------------------------------------------------------- jobs

/// A content-addressed file made available to a task at `$DA_INPUTS_DIR/<name>`, or
/// produced by one from `$DA_OUTPUT_DIR/<name>`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Blob {
    pub name: String,
    pub hash: Hash,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobSpec {
    #[serde(default)]
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub resources: Resources,
    /// Gang size: all replicas are placed atomically or the job keeps waiting.
    pub replicas: u32,
    /// Files every replica gets.
    #[serde(default)]
    pub inputs: Vec<Blob>,
    /// Files dealt out across the gang: rank `r` gets every `replicas`-th one, starting at
    /// `r` (data shards, so each replica stages only its own slice).
    #[serde(default)]
    pub shards: Vec<Blob>,
    /// Node labels that must all match.
    #[serde(default)]
    pub selector: BTreeMap<String, String>,
    /// Re-runs allowed after a task exits non-zero. Node loss never consumes a retry.
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub priority: i32,
}

impl JobSpec {
    pub fn inputs_for_rank(&self, rank: u32) -> Vec<Blob> {
        let dealt = self
            .shards
            .iter()
            .skip(rank as usize)
            .step_by(self.replicas.max(1) as usize);
        self.inputs.iter().chain(dealt).cloned().collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Placed on a node, not yet started there.
    Assigned,
    /// Node is pulling missing inputs from peers.
    Fetching,
    Running,
    Succeeded,
    Failed,
    /// Stopped on request (cancel, or a sibling in the gang failed).
    Killed,
    /// Its node disappeared.
    Lost,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Killed | Self::Lost)
    }
}

// ---------------------------------------------------------------- agent <-> server

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterReq {
    pub name: String,
    pub info: NodeInfo,
    pub labels: BTreeMap<String, String>,
    /// Port of the agent's blob server. The host is the address the server sees the agent
    /// connect from, unless `advertise_host` overrides it.
    pub blob_port: u16,
    pub advertise_host: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterResp {
    pub heartbeat_ms: u64,
    /// Cluster TLS identity, used by the agent to serve blobs to its peers.
    pub cert_pem: String,
    pub key_pem: String,
}

/// Everything a node needs to run one replica of a job.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Assignment {
    pub task_id: String,
    pub job_id: String,
    pub job_name: String,
    pub rank: u32,
    pub world_size: u32,
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
    /// GPU indices on the target node.
    pub gpus: Vec<u32>,
    pub inputs: Vec<Blob>,
    /// Host of every rank's node, indexed by rank, for distributed-training rendezvous.
    pub peer_hosts: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskReport {
    pub task_id: String,
    pub state: TaskState,
    pub exit_code: Option<i32>,
    pub message: Option<String>,
    /// Byte offset in the agent's log file where `log` starts, and how many raw bytes it
    /// covers. The server appends only when the offset matches what it has acknowledged.
    pub log_offset: u64,
    pub log_len: u64,
    /// Size of the agent's log file, so the server knows when it has everything.
    pub log_total: u64,
    pub log: String,
    pub outputs: Vec<Blob>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatReq {
    pub stats: NodeStats,
    pub tasks: Vec<TaskReport>,
    /// Full cache inventory; sent when it changed or when the server asks for it.
    pub blobs: Option<Vec<(Hash, u64)>>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct HeartbeatResp {
    pub assign: Vec<Assignment>,
    pub kill: Vec<String>,
    /// Terminal tasks the server has recorded; the agent may delete their state.
    pub forget: Vec<String>,
    /// Acknowledged log offset per task.
    pub log_offsets: BTreeMap<String, u64>,
    pub want_blobs: bool,
}

// ---------------------------------------------------------------- views (server -> CLI)

#[derive(Serialize, Deserialize, Debug)]
pub struct NodeView {
    pub name: String,
    pub host: String,
    pub ready: bool,
    pub last_seen_secs: u64,
    pub info: NodeInfo,
    pub stats: NodeStats,
    pub labels: BTreeMap<String, String>,
    pub alloc_cpus: u32,
    pub alloc_memory_mb: u64,
    pub alloc_gpus: Vec<u32>,
    pub tasks: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskView {
    pub id: String,
    pub rank: u32,
    pub attempt: u32,
    pub node: String,
    pub gpus: Vec<u32>,
    pub state: TaskState,
    pub exit_code: Option<i32>,
    pub message: Option<String>,
    pub outputs: Vec<Blob>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct JobView {
    pub id: String,
    pub spec: JobSpec,
    pub state: JobState,
    pub attempt: u32,
    pub message: Option<String>,
    pub submitted_at: u64,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    /// Tasks of every attempt, oldest first.
    pub tasks: Vec<TaskView>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogChunk {
    pub data: String,
    pub next_offset: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlobSources {
    /// `host:port` of peers holding the blob, in the order to try them.
    pub peers: Vec<String>,
    /// The control plane holds a copy too (last resort, so it is not a bottleneck).
    pub origin: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PutBlobResp {
    pub hash: Hash,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ApiError {
    pub error: String,
}
