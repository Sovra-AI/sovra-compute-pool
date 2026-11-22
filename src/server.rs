//! Control plane: node registry, job queue, scheduler, log sink and origin blob store.
//!
//! Nodes always dial in (register, then heartbeat every couple of seconds), so devices
//! behind NAT or a firewall can join without opening anything for control traffic. All
//! decisions are made here under one lock; agents only report and obey.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request, State as AxState};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use crate::net::{self, Endpoint, Identity, Token, constant_time_eq};
use crate::proto::{
    ApiError, Assignment, Blob, BlobSources, Hash, HeartbeatReq, HeartbeatResp, JobSpec, JobState,
    JobView, LogChunk, NodeInfo, NodeStats, NodeView, PutBlobResp, RegisterReq, RegisterResp,
    TaskState, TaskView, is_hash, safe_name,
};
use crate::sched::{self, NodeFree};
use crate::store::Store;

const HEARTBEAT: Duration = Duration::from_secs(2);
/// A node that misses this many seconds of heartbeats is considered gone.
const NODE_TIMEOUT: Duration = Duration::from_secs(15);
const LOG_READ_MAX: usize = 1 << 20;

pub struct ServerOpts {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
}

// ---------------------------------------------------------------- state

#[derive(Serialize, Deserialize)]
struct Node {
    name: String,
    host: String,
    blob_port: u16,
    info: NodeInfo,
    labels: BTreeMap<String, String>,
    #[serde(skip)]
    stats: NodeStats,
    #[serde(skip)]
    blobs: Option<HashMap<Hash, u64>>,
    /// After a control-plane restart every known node gets a fresh grace period.
    #[serde(skip, default = "Instant::now")]
    last_seen: Instant,
}

impl Node {
    fn ready(&self) -> bool {
        self.last_seen.elapsed() < NODE_TIMEOUT
    }

    fn blob_addr(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.blob_port)
        } else {
            format!("{}:{}", self.host, self.blob_port)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Task {
    id: String,
    job_id: String,
    rank: u32,
    attempt: u32,
    node: String,
    gpus: Vec<u32>,
    state: TaskState,
    /// The agent has reported this task at least once.
    acked: bool,
    /// Its CPUs/memory/GPUs are free again. Lags `state` when the control plane has
    /// declared a task dead but the node has not yet confirmed the process is gone.
    released: bool,
    exit_code: Option<i32>,
    message: Option<String>,
    log_offset: u64,
    outputs: Vec<Blob>,
}

#[derive(Serialize, Deserialize)]
struct Job {
    id: String,
    seq: u64,
    spec: JobSpec,
    state: JobState,
    attempt: u32,
    retries_used: u32,
    message: Option<String>,
    submitted_at: u64,
    started_at: Option<u64>,
    finished_at: Option<u64>,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    nodes: BTreeMap<String, Node>,
    jobs: BTreeMap<String, Job>,
    tasks: BTreeMap<String, Task>,
    datasets: BTreeMap<String, Vec<Blob>>,
    next_job: u64,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl State {
    /// Capacity of every ready node minus what unreleased tasks hold.
    fn free_nodes(&self) -> Vec<NodeFree<'_>> {
        self.nodes
            .values()
            .filter(|node| node.ready())
            .map(|node| {
                let mut free = NodeFree {
                    name: &node.name,
                    cpus: node.info.cpus,
                    memory_mb: node.info.memory_mb,
                    gpus: node
                        .info
                        .gpus
                        .iter()
                        .map(|g| (g.index, g.memory_mb))
                        .collect(),
                    labels: &node.labels,
                    blobs: node.blobs.as_ref(),
                };
                for task in self.tasks.values() {
                    if task.node != node.name || task.released {
                        continue;
                    }
                    let held = &self.jobs[&task.job_id].spec.resources;
                    free.cpus = free.cpus.saturating_sub(held.cpus);
                    free.memory_mb = free.memory_mb.saturating_sub(held.memory_mb);
                    free.gpus.retain(|(index, _)| !task.gpus.contains(index));
                }
                free
            })
            .collect()
    }

    /// Places as many queued jobs as fit, highest priority first, oldest first within a
    /// priority. A job that does not fit does not block smaller ones behind it.
    fn schedule(&mut self) {
        let mut queue: Vec<&Job> = self
            .jobs
            .values()
            .filter(|job| job.state == JobState::Queued)
            .collect();
        queue.sort_by_key(|job| (std::cmp::Reverse(job.spec.priority), job.seq));

        let mut free = self.free_nodes();
        let decisions: Vec<_> = queue
            .iter()
            .map(|job| (job.id.clone(), sched::place(&job.spec, &mut free)))
            .collect();
        drop(free);

        for (job_id, decision) in decisions {
            let job = self.jobs.get_mut(&job_id).expect("job exists");
            match decision {
                Err(why) => job.message = Some(why),
                Ok(placements) => {
                    job.state = JobState::Running;
                    job.started_at = Some(now());
                    job.message = None;
                    for (rank, placement) in (0u32..).zip(placements) {
                        let id = format!("{}.{}.{rank}", job.id, job.attempt);
                        self.tasks.insert(
                            id.clone(),
                            Task {
                                id,
                                job_id: job.id.clone(),
                                rank,
                                attempt: job.attempt,
                                node: placement.node,
                                gpus: placement.gpus,
                                state: TaskState::Assigned,
                                acked: false,
                                released: false,
                                exit_code: None,
                                message: None,
                                log_offset: 0,
                                outputs: Vec::new(),
                            },
                        );
                    }
                }
            }
        }
    }

    /// Marks a live task dead on the control plane's authority. Its resources stay held
    /// until the node confirms, unless the node never knew about it or is itself gone.
    fn terminate(task: &mut Task, state: TaskState, why: &str, node_gone: bool) {
        if task.state.is_terminal() {
            return;
        }
        task.state = state;
        task.message = Some(why.to_owned());
        task.released = node_gone || !task.acked;
    }

    /// Advances a running job once its tasks have settled: success when every replica
    /// succeeded; otherwise stop the rest of the gang and requeue or fail.
    fn reconcile(&mut self, job_id: &str) {
        let Some(job) = self.jobs.get_mut(job_id) else {
            return;
        };
        if job.state != JobState::Running {
            return;
        }
        let current = |task: &&mut Task| task.job_id == job.id && task.attempt == job.attempt;
        let mut tasks: Vec<&mut Task> = self.tasks.values_mut().filter(current).collect();

        if tasks.iter().all(|t| t.state == TaskState::Succeeded) {
            job.state = JobState::Succeeded;
            job.finished_at = Some(now());
            job.message = None;
            return;
        }
        let culprit = tasks
            .iter()
            .find(|t| t.state == TaskState::Failed)
            .or_else(|| {
                tasks
                    .iter()
                    .find(|t| matches!(t.state, TaskState::Lost | TaskState::Killed))
            });
        let Some(culprit) = culprit else {
            return;
        };
        let crashed = culprit.state == TaskState::Failed;
        let what = match (&culprit.message, culprit.exit_code) {
            (Some(message), _) => format!("rank {} on {}: {message}", culprit.rank, culprit.node),
            (None, Some(code)) => format!(
                "rank {} on {} exited with code {code}",
                culprit.rank, culprit.node
            ),
            (None, None) => format!("rank {} on {} failed", culprit.rank, culprit.node),
        };
        for task in &mut tasks {
            Self::terminate(
                task,
                TaskState::Killed,
                "stopped: another replica of the job failed",
                false,
            );
        }

        if crashed && job.retries_used >= job.spec.max_retries {
            job.state = JobState::Failed;
            job.finished_at = Some(now());
            job.message = Some(what);
            return;
        }
        if crashed {
            job.retries_used += 1;
        }
        job.attempt += 1;
        job.state = JobState::Queued;
        job.started_at = None;
        job.message = Some(format!("requeued after {what}"));
    }

    fn cancel(&mut self, job_id: &str) -> bool {
        let Some(job) = self.jobs.get_mut(job_id) else {
            return false;
        };
        if !job.state.is_terminal() {
            job.state = JobState::Cancelled;
            job.finished_at = Some(now());
            job.message = None;
            for task in self.tasks.values_mut().filter(|t| t.job_id == job_id) {
                Self::terminate(task, TaskState::Killed, "cancelled", false);
            }
        }
        true
    }

    /// Declares the tasks of unresponsive nodes lost. Returns whether anything changed.
    fn sweep_lost_nodes(&mut self) -> bool {
        let gone: HashSet<&str> = self
            .nodes
            .values()
            .filter(|node| !node.ready())
            .map(|node| node.name.as_str())
            .collect();
        let mut affected = BTreeSet::new();
        for task in self.tasks.values_mut() {
            if gone.contains(task.node.as_str()) && !(task.state.is_terminal() && task.released) {
                let job = task.job_id.clone();
                Self::terminate(task, TaskState::Lost, "node stopped responding", true);
                task.released = true;
                affected.insert(job);
            }
        }
        for job in &affected {
            self.reconcile(job);
        }
        if !affected.is_empty() {
            self.schedule();
        }
        !affected.is_empty()
    }

    fn assignment(&self, task: &Task) -> Assignment {
        let job = &self.jobs[&task.job_id];
        let mut gang: Vec<&Task> = self
            .tasks
            .values()
            .filter(|t| t.job_id == task.job_id && t.attempt == task.attempt)
            .collect();
        gang.sort_by_key(|t| t.rank);
        Assignment {
            task_id: task.id.clone(),
            job_id: job.id.clone(),
            job_name: job.spec.name.clone(),
            rank: task.rank,
            world_size: job.spec.replicas,
            command: job.spec.command.clone(),
            env: job.spec.env.clone(),
            gpus: task.gpus.clone(),
            inputs: job.spec.inputs_for_rank(task.rank),
            peer_hosts: gang
                .iter()
                .map(|t| {
                    self.nodes
                        .get(&t.node)
                        .map(|n| n.host.clone())
                        .unwrap_or_default()
                })
                .collect(),
        }
    }

    /// Applies one node heartbeat. `None` means the node is unknown and must re-register.
    fn heartbeat(&mut self, name: &str, req: HeartbeatReq, logs: &FsPath) -> Option<HeartbeatResp> {
        let node = self.nodes.get_mut(name)?;
        node.last_seen = Instant::now();
        node.stats = req.stats;
        if let Some(blobs) = req.blobs {
            node.blobs = Some(blobs.into_iter().collect());
        }
        let mut resp = HeartbeatResp {
            want_blobs: node.blobs.is_none(),
            ..Default::default()
        };

        let mut reported = HashSet::new();
        let mut affected = BTreeSet::new();
        for report in req.tasks {
            reported.insert(report.task_id.clone());
            let known = self
                .tasks
                .get_mut(&report.task_id)
                .filter(|task| task.node == name);
            let Some(task) = known else {
                // Not ours (e.g. state was reset): make the node drop it.
                if report.state.is_terminal() {
                    resp.forget.push(report.task_id);
                } else {
                    resp.kill.push(report.task_id);
                }
                continue;
            };
            task.acked = true;

            if report.log_len > 0 && report.log_offset == task.log_offset {
                let appended = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(logs.join(format!("{}.log", task.id)))
                    .and_then(|mut file| file.write_all(report.log.as_bytes()));
                if appended.is_ok() {
                    task.log_offset += report.log_len;
                }
            }
            resp.log_offsets.insert(task.id.clone(), task.log_offset);
            let logs_complete = task.log_offset >= report.log_total;

            if !task.state.is_terminal() {
                task.state = report.state;
                task.exit_code = report.exit_code;
                task.message = report.message;
                if report.state.is_terminal() {
                    task.outputs = report.outputs;
                    affected.insert(task.job_id.clone());
                }
            } else if !report.state.is_terminal() {
                // We already gave up on it (cancel, gang failure, node flap): stop it.
                resp.kill.push(task.id.clone());
            }
            if report.state.is_terminal() {
                if !task.released {
                    task.released = true;
                    affected.insert(task.job_id.clone());
                }
                if logs_complete {
                    resp.forget.push(task.id.clone());
                }
            }
        }

        // Tasks the node acknowledged earlier but no longer knows: the agent restarted.
        for task in self.tasks.values_mut() {
            if task.node == name && task.acked && !task.released && !reported.contains(&task.id) {
                Self::terminate(task, TaskState::Lost, "agent restarted", true);
                task.released = true;
                affected.insert(task.job_id.clone());
            }
        }
        for job in &affected {
            self.reconcile(job);
        }
        self.schedule();

        // Deliver until acknowledged; the agent ignores duplicates.
        resp.assign = self
            .tasks
            .values()
            .filter(|t| t.node == name && t.state == TaskState::Assigned && !t.acked)
            .map(|t| self.assignment(t))
            .collect();
        Some(resp)
    }

    /// `host:port` of ready nodes caching `hash`, starting at a random one to spread load.
    fn holders(&self, hash: &str, exclude: Option<&str>) -> Vec<String> {
        let mut peers: Vec<String> = self
            .nodes
            .values()
            .filter(|n| n.ready() && Some(n.name.as_str()) != exclude)
            .filter(|n| n.blobs.as_ref().is_some_and(|b| b.contains_key(hash)))
            .map(Node::blob_addr)
            .collect();
        if !peers.is_empty() {
            let start = rand::random_range(0..peers.len());
            peers.rotate_left(start);
        }
        peers
    }

    fn node_view(&self, node: &Node) -> NodeView {
        let mut view = NodeView {
            name: node.name.clone(),
            host: node.host.clone(),
            ready: node.ready(),
            last_seen_secs: node.last_seen.elapsed().as_secs(),
            info: node.info.clone(),
            stats: node.stats.clone(),
            labels: node.labels.clone(),
            alloc_cpus: 0,
            alloc_memory_mb: 0,
            alloc_gpus: Vec::new(),
            tasks: Vec::new(),
        };
        for task in self.tasks.values() {
            if task.node == node.name && !task.released {
                let held = &self.jobs[&task.job_id].spec.resources;
                view.alloc_cpus += held.cpus;
                view.alloc_memory_mb += held.memory_mb;
                view.alloc_gpus.extend(&task.gpus);
                view.tasks.push(task.id.clone());
            }
        }
        view.alloc_gpus.sort_unstable();
        view
    }

    fn job_view(&self, job: &Job) -> JobView {
        let mut tasks: Vec<TaskView> = self
            .tasks
            .values()
            .filter(|t| t.job_id == job.id)
            .map(|t| TaskView {
                id: t.id.clone(),
                rank: t.rank,
                attempt: t.attempt,
                node: t.node.clone(),
                gpus: t.gpus.clone(),
                state: t.state,
                exit_code: t.exit_code,
                message: t.message.clone(),
                outputs: t.outputs.clone(),
            })
            .collect();
        tasks.sort_by_key(|t| (t.attempt, t.rank));
        JobView {
            id: job.id.clone(),
            spec: job.spec.clone(),
            state: job.state,
            attempt: job.attempt,
            message: job.message.clone(),
            submitted_at: job.submitted_at,
            started_at: job.started_at,
            finished_at: job.finished_at,
            tasks,
        }
    }
}

// ---------------------------------------------------------------- server

struct Server {
    state: Mutex<State>,
    dirty: AtomicBool,
    store: Store,
    identity: Identity,
    token: Token,
    logs: PathBuf,
    state_path: PathBuf,
}

type App = Arc<Server>;

impl Server {
    /// Runs `f` on the state and schedules a save.
    fn update<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        let result = f(&mut crate::lock(&self.state));
        self.dirty.store(true, Ordering::SeqCst);
        result
    }

    fn read<T>(&self, f: impl FnOnce(&State) -> T) -> T {
        f(&crate::lock(&self.state))
    }

    fn save(&self) -> std::io::Result<()> {
        if !self.dirty.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        let json = serde_json::to_vec(&*crate::lock(&self.state))?;
        let tmp = self.state_path.with_extension("json.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(tmp, &self.state_path)
    }
}

pub async fn run(opts: ServerOpts) -> Result<()> {
    let dir = &opts.data_dir;
    let logs = dir.join("logs");
    std::fs::create_dir_all(&logs).with_context(|| format!("creating {}", dir.display()))?;

    let identity = Identity::load_or_create(dir)?;
    let token_path = dir.join("token");
    let token = match std::fs::read_to_string(&token_path) {
        Ok(raw) => {
            Token::parse(&raw).with_context(|| format!("{} is corrupt", token_path.display()))?
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            // Never mint a new token over an unreadable one: every joined device would be
            // locked out.
            return Err(e).with_context(|| format!("reading {}", token_path.display()));
        }
        Err(_) => {
            let token = Token::generate(&identity.cert_pem);
            net::write_private(&token_path, &token.to_string())?;
            token
        }
    };

    let state_path = dir.join("state.json");
    let state: State = match std::fs::read(&state_path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("{} is corrupt", state_path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
        // Starting empty would overwrite the saved jobs and devices on the next save.
        Err(e) => return Err(e).with_context(|| format!("reading {}", state_path.display())),
    };
    tracing::info!(
        listen = %opts.listen,
        nodes = state.nodes.len(),
        jobs = state.jobs.len(),
        "dist-alloc control plane starting"
    );
    // `dist-alloc token` reads this to put the right port in invites.
    std::fs::write(dir.join("port"), opts.listen.port().to_string())
        .with_context(|| format!("writing {}", dir.join("port").display()))?;
    announce_join_command(&token, opts.listen.port());

    let app: App = Arc::new(Server {
        state: Mutex::new(state),
        dirty: AtomicBool::new(false),
        store: Store::open(dir)?,
        identity,
        token,
        logs,
        state_path,
    });

    // Housekeeping: detect dead nodes every second, persist state when it changed.
    let housekeeper = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            if crate::lock(&housekeeper.state).sweep_lost_nodes() {
                housekeeper.dirty.store(true, Ordering::SeqCst);
            }
            if let Err(e) = housekeeper.save() {
                tracing::warn!("saving state: {e}");
            }
        }
    });

    let authed = Router::new()
        .route("/v1/nodes", get(list_nodes).post(register))
        .route("/v1/nodes/{name}", delete(remove_node))
        .route("/v1/nodes/{name}/heartbeat", post(heartbeat))
        .route("/v1/jobs", get(list_jobs).post(submit_job))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
        .route("/v1/jobs/{id}/logs", get(job_logs))
        .route(
            "/v1/blobs",
            put(put_blob).layer(DefaultBodyLimit::disable()),
        )
        .route("/v1/blobs/{hash}", get(get_blob))
        .route("/v1/blobs/{hash}/sources", get(blob_sources))
        .route("/v1/datasets", get(list_datasets))
        .route("/v1/datasets/{name}", put(put_dataset))
        .layer(DefaultBodyLimit::max(64 << 20))
        .layer(middleware::from_fn_with_state(app.clone(), require_token));
    let router = Router::new()
        .route("/v1/ca", get(cluster_cert))
        .merge(authed)
        .with_state(app.clone());

    let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
        app.identity.cert_pem.clone().into_bytes(),
        app.identity.key_pem.clone().into_bytes(),
    )
    .await?;
    let serve = axum_server::bind_rustls(opts.listen, tls)
        .serve(router.into_make_service_with_connect_info::<SocketAddr>());
    tokio::select! {
        result = serve => result.with_context(|| format!("serving on {}", opts.listen))?,
        () = crate::shutdown_signal() => tracing::info!("shutting down"),
    }
    app.dirty.store(true, Ordering::SeqCst);
    app.save()?;
    Ok(())
}

/// The port the control plane in `data_dir` last listened on.
pub fn saved_port(data_dir: &FsPath) -> Option<u16> {
    std::fs::read_to_string(data_dir.join("port"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Tells the operator how to add devices. The command embeds the cluster-admin token, so it
/// is printed only to an interactive terminal, never into a service's log.
fn announce_join_command(token: &Token, port: u16) {
    use std::io::IsTerminal;
    let reachable = net::lan_addresses();
    if !std::io::stderr().is_terminal() {
        tracing::info!(
            "run `dist-alloc token` on this machine to print the join command for new devices"
        );
        return;
    }
    let host = reachable
        .first()
        .map_or_else(|| "<this-machine's-ip>".to_owned(), ToString::to_string);
    let mut out = std::io::stderr().lock();
    let _ = writeln!(out, "\nadd a device to the pool by running on it:\n");
    let _ = writeln!(
        out,
        "  dist-alloc agent --server {host}:{port} --token {token}\n"
    );
    if reachable.len() > 1 {
        let _ = writeln!(
            out,
            "(this machine is also reachable at {:?})",
            &reachable[1..]
        );
    }
}

// ---------------------------------------------------------------- http plumbing

pub struct Fail(pub StatusCode, pub String);

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        (self.0, Json(ApiError { error: self.1 })).into_response()
    }
}

impl From<std::io::Error> for Fail {
    fn from(e: std::io::Error) -> Self {
        let status = if e.kind() == std::io::ErrorKind::InvalidData {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        Fail(status, e.to_string())
    }
}

fn bad(message: impl Into<String>) -> Fail {
    Fail(StatusCode::BAD_REQUEST, message.into())
}

fn not_found(message: impl Into<String>) -> Fail {
    Fail(StatusCode::NOT_FOUND, message.into())
}

pub fn bearer(request: &Request) -> Option<&str> {
    request
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

async fn require_token(AxState(app): AxState<App>, request: Request, next: Next) -> Response {
    if bearer(&request).is_some_and(|t| constant_time_eq(t, &app.token.secret)) {
        next.run(request).await
    } else {
        Fail(StatusCode::UNAUTHORIZED, "missing or wrong token".into()).into_response()
    }
}

/// Streams a stored blob.
pub async fn file_response(path: &FsPath) -> Result<Response, Fail> {
    let file = tokio::fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    let body = Body::from_stream(tokio_util::io::ReaderStream::with_capacity(file, 1 << 16));
    Ok(([(header::CONTENT_LENGTH, len.to_string())], body).into_response())
}

// ---------------------------------------------------------------- handlers

async fn cluster_cert(AxState(app): AxState<App>) -> String {
    app.identity.cert_pem.clone()
}

async fn register(
    AxState(app): AxState<App>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    Json(req): Json<RegisterReq>,
) -> Result<Json<RegisterResp>, Fail> {
    let valid = !req.name.is_empty()
        && req.name.len() <= 63
        && req
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !valid {
        return Err(bad("node name must be 1-63 characters of [A-Za-z0-9._-]"));
    }
    let host = req
        .advertise_host
        .unwrap_or_else(|| remote.ip().to_canonical().to_string());
    tracing::info!(
        node = %req.name,
        %host,
        cpus = req.info.cpus,
        memory_mb = req.info.memory_mb,
        gpus = req.info.gpus.len(),
        "node joined"
    );
    app.update(|state| {
        state.nodes.insert(
            req.name.clone(),
            Node {
                name: req.name,
                host,
                blob_port: req.blob_port,
                info: req.info,
                labels: req.labels,
                stats: NodeStats::default(),
                blobs: None,
                last_seen: Instant::now(),
            },
        );
        state.schedule();
    });
    Ok(Json(RegisterResp {
        heartbeat_ms: HEARTBEAT.as_secs() * 1000 + u64::from(HEARTBEAT.subsec_millis()),
        cert_pem: app.identity.cert_pem.clone(),
        key_pem: app.identity.key_pem.clone(),
    }))
}

async fn heartbeat(
    AxState(app): AxState<App>,
    Path(name): Path<String>,
    Json(req): Json<HeartbeatReq>,
) -> Result<Json<HeartbeatResp>, Fail> {
    app.update(|state| state.heartbeat(&name, req, &app.logs))
        .map(Json)
        .ok_or_else(|| not_found(format!("node {name} is not registered")))
}

async fn list_nodes(AxState(app): AxState<App>) -> Json<Vec<NodeView>> {
    Json(app.read(|s| s.nodes.values().map(|n| s.node_view(n)).collect()))
}

async fn remove_node(
    AxState(app): AxState<App>,
    Path(name): Path<String>,
) -> Result<StatusCode, Fail> {
    app.update(|state| {
        state
            .nodes
            .remove(&name)
            .ok_or_else(|| not_found(format!("no node {name}")))?;
        let mut affected = BTreeSet::new();
        for task in state.tasks.values_mut().filter(|t| t.node == name) {
            if !task.released {
                State::terminate(task, TaskState::Lost, "node removed", true);
                task.released = true;
                affected.insert(task.job_id.clone());
            }
        }
        for job in &affected {
            state.reconcile(job);
        }
        state.schedule();
        Ok(StatusCode::NO_CONTENT)
    })
}

async fn submit_job(
    AxState(app): AxState<App>,
    Json(spec): Json<JobSpec>,
) -> Result<Json<JobView>, Fail> {
    if spec.command.is_empty() {
        return Err(bad("command is empty"));
    }
    if spec.replicas == 0 || spec.replicas > 4096 {
        return Err(bad("replicas must be between 1 and 4096"));
    }
    let mut names = HashSet::new();
    for input in spec.inputs.iter().chain(&spec.shards) {
        if !is_hash(&input.hash) {
            return Err(bad(format!("input {}: not a BLAKE3 hash", input.name)));
        }
        if !safe_name(&input.name) {
            return Err(bad(format!(
                "input name {:?} is not a safe relative path",
                input.name
            )));
        }
        if !names.insert(&input.name) {
            return Err(bad(format!("duplicate input name {:?}", input.name)));
        }
    }
    let missing = app.read(|state| {
        spec.inputs
            .iter()
            .chain(&spec.shards)
            .find(|input| {
                app.store.find(&input.hash).is_none()
                    && !state.nodes.values().any(|n| {
                        n.blobs
                            .as_ref()
                            .is_some_and(|b| b.contains_key(&input.hash))
                    })
            })
            .map(|input| input.name.clone())
    });
    if let Some(name) = missing {
        return Err(bad(format!(
            "input {name}: no node holds that blob; upload it first"
        )));
    }

    Ok(Json(app.update(|state| {
        state.next_job += 1;
        let job = Job {
            id: format!("j{}", state.next_job),
            seq: state.next_job,
            spec,
            state: JobState::Queued,
            attempt: 1,
            retries_used: 0,
            message: None,
            submitted_at: now(),
            started_at: None,
            finished_at: None,
        };
        let id = job.id.clone();
        state.jobs.insert(id.clone(), job);
        state.schedule();
        state.job_view(&state.jobs[&id])
    })))
}

async fn list_jobs(AxState(app): AxState<App>) -> Json<Vec<JobView>> {
    Json(app.read(|s| {
        let mut jobs: Vec<&Job> = s.jobs.values().collect();
        jobs.sort_by_key(|j| j.seq);
        jobs.into_iter().map(|j| s.job_view(j)).collect()
    }))
}

async fn get_job(
    AxState(app): AxState<App>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, Fail> {
    app.read(|s| s.jobs.get(&id).map(|j| Json(s.job_view(j))))
        .ok_or_else(|| not_found(format!("no job {id}")))
}

async fn cancel_job(
    AxState(app): AxState<App>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, Fail> {
    app.update(|state| {
        if !state.cancel(&id) {
            return Err(not_found(format!("no job {id}")));
        }
        state.schedule();
        Ok(Json(state.job_view(&state.jobs[&id])))
    })
}

#[derive(Deserialize)]
struct LogQuery {
    task: String,
    #[serde(default)]
    offset: u64,
}

async fn job_logs(
    AxState(app): AxState<App>,
    Path(id): Path<String>,
    Query(query): Query<LogQuery>,
) -> Result<Json<LogChunk>, Fail> {
    let owned = app.read(|s| s.tasks.get(&query.task).is_some_and(|t| t.job_id == id));
    if !owned {
        return Err(not_found(format!("job {id} has no task {}", query.task)));
    }
    let mut buf = Vec::new();
    if let Ok(mut file) = std::fs::File::open(app.logs.join(format!("{}.log", query.task))) {
        file.seek(SeekFrom::Start(query.offset))?;
        file.take(LOG_READ_MAX as u64).read_to_end(&mut buf)?;
    }
    // Never split a multi-byte character across two chunks.
    if let Err(e) = std::str::from_utf8(&buf) {
        buf.truncate(e.valid_up_to());
    }
    Ok(Json(LogChunk {
        next_offset: query.offset + buf.len() as u64,
        data: String::from_utf8(buf).unwrap_or_default(),
    }))
}

async fn put_blob(AxState(app): AxState<App>, body: Body) -> Result<Json<PutBlobResp>, Fail> {
    let mut writer = app.store.writer(true).await?;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| bad(format!("upload interrupted: {e}")))?;
        writer.write(&chunk).await?;
    }
    let (hash, size) = writer.finish(None).await?;
    Ok(Json(PutBlobResp { hash, size }))
}

/// Serves a blob from the origin store, or relays it from a node that has it (so the CLI
/// can fetch task outputs without talking to nodes directly).
async fn get_blob(AxState(app): AxState<App>, Path(hash): Path<String>) -> Result<Response, Fail> {
    if !is_hash(&hash) {
        return Err(bad("not a BLAKE3 hash"));
    }
    if let Some(path) = app.store.find(&hash) {
        return file_response(&path).await;
    }
    let peer_key = app.token.peer_key();
    for peer in app.read(|s| s.holders(&hash, None)) {
        let Ok(addr) = net::resolve(&peer).await else {
            continue;
        };
        let Ok(endpoint) = Endpoint::pinned(addr, &app.identity.cert_pem, &peer_key) else {
            continue;
        };
        if let Ok(upstream) = net::send(endpoint.get(&format!("/blobs/{hash}"))).await {
            // Relayed chunked; the receiver verifies the hash.
            return Ok(Body::from_stream(upstream.bytes_stream()).into_response());
        }
    }
    Err(not_found(format!("no reachable node holds blob {hash}")))
}

#[derive(Deserialize)]
struct SourcesQuery {
    exclude: Option<String>,
}

async fn blob_sources(
    AxState(app): AxState<App>,
    Path(hash): Path<String>,
    Query(query): Query<SourcesQuery>,
) -> Json<BlobSources> {
    Json(BlobSources {
        peers: app.read(|s| s.holders(&hash, query.exclude.as_deref())),
        origin: app.store.find(&hash).is_some(),
    })
}

async fn list_datasets(AxState(app): AxState<App>) -> Json<BTreeMap<String, Vec<Blob>>> {
    Json(app.read(|s| s.datasets.clone()))
}

async fn put_dataset(
    AxState(app): AxState<App>,
    Path(name): Path<String>,
    Json(files): Json<Vec<Blob>>,
) -> Result<StatusCode, Fail> {
    if !safe_name(&name) || name.contains('/') {
        return Err(bad("dataset name must be a single path component"));
    }
    if files.is_empty() {
        return Err(bad("a dataset needs at least one file"));
    }
    if let Some(file) = files
        .iter()
        .find(|f| !is_hash(&f.hash) || !safe_name(&f.name))
    {
        return Err(bad(format!(
            "dataset file {:?} has an invalid name or hash",
            file.name
        )));
    }
    app.update(|state| state.datasets.insert(name, files));
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{GpuInfo, Resources, TaskReport};

    fn spec(replicas: u32, gpus: u32, max_retries: u32) -> JobSpec {
        JobSpec {
            name: "t".into(),
            command: vec!["true".into()],
            env: BTreeMap::new(),
            resources: Resources {
                cpus: 1,
                memory_mb: 100,
                gpus,
                gpu_memory_mb: 0,
            },
            replicas,
            inputs: Vec::new(),
            shards: Vec::new(),
            selector: BTreeMap::new(),
            max_retries,
            priority: 0,
        }
    }

    fn cluster(nodes: &[&str]) -> (State, PathBuf) {
        // Tests run in parallel and task ids repeat across them, so each gets its own logs.
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let mut state = State::default();
        for name in nodes {
            state.nodes.insert(
                name.to_string(),
                Node {
                    name: name.to_string(),
                    host: "10.0.0.1".into(),
                    blob_port: 1,
                    info: NodeInfo {
                        cpus: 4,
                        memory_mb: 1000,
                        gpus: vec![GpuInfo {
                            index: 0,
                            uuid: "GPU-x".into(),
                            name: "T".into(),
                            memory_mb: 8000,
                        }],
                        ..Default::default()
                    },
                    labels: BTreeMap::new(),
                    stats: NodeStats::default(),
                    blobs: Some(HashMap::new()),
                    last_seen: Instant::now(),
                },
            );
        }
        let logs = std::env::temp_dir().join(format!(
            "da-server-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&logs).unwrap();
        (state, logs)
    }

    fn submit(state: &mut State, spec: JobSpec) -> String {
        state.next_job += 1;
        let id = format!("j{}", state.next_job);
        state.jobs.insert(
            id.clone(),
            Job {
                id: id.clone(),
                seq: state.next_job,
                spec,
                state: JobState::Queued,
                attempt: 1,
                retries_used: 0,
                message: None,
                submitted_at: 0,
                started_at: None,
                finished_at: None,
            },
        );
        state.schedule();
        id
    }

    fn beat(
        state: &mut State,
        logs: &FsPath,
        node: &str,
        tasks: &[(&str, TaskState, Option<i32>)],
    ) -> HeartbeatResp {
        let req = HeartbeatReq {
            stats: NodeStats::default(),
            blobs: None,
            tasks: tasks
                .iter()
                .map(|(id, state, exit_code)| TaskReport {
                    task_id: id.to_string(),
                    state: *state,
                    exit_code: *exit_code,
                    message: None,
                    log_offset: 0,
                    log_len: 0,
                    log_total: 0,
                    log: String::new(),
                    outputs: Vec::new(),
                })
                .collect(),
        };
        state
            .heartbeat(node, req, logs)
            .expect("node is registered")
    }

    #[test]
    fn job_runs_to_success_and_frees_its_gpu() {
        let (mut state, logs) = cluster(&["a"]);
        let job = submit(&mut state, spec(1, 1, 0));
        let waiting = submit(&mut state, spec(1, 1, 0));
        assert_eq!(state.jobs[&job].state, JobState::Running);
        assert_eq!(
            state.jobs[&waiting].state,
            JobState::Queued,
            "the only GPU is taken"
        );

        let resp = beat(&mut state, &logs, "a", &[]);
        assert_eq!(resp.assign.len(), 1);
        assert_eq!(resp.assign[0].gpus, [0]);
        let task = resp.assign[0].task_id.clone();

        let resp = beat(&mut state, &logs, "a", &[(&task, TaskState::Running, None)]);
        assert!(resp.assign.is_empty(), "not redelivered once acknowledged");

        let resp = beat(
            &mut state,
            &logs,
            "a",
            &[(&task, TaskState::Succeeded, Some(0))],
        );
        assert_eq!(resp.forget, [task]);
        assert_eq!(state.jobs[&job].state, JobState::Succeeded);
        assert_eq!(
            state.jobs[&waiting].state,
            JobState::Running,
            "freed GPU went to the next job"
        );
        assert_eq!(
            resp.assign.len(),
            1,
            "and it is delivered in the same heartbeat"
        );
    }

    #[test]
    fn failed_replica_stops_the_gang_then_retries_then_fails() {
        let (mut state, logs) = cluster(&["a", "b"]);
        let job = submit(&mut state, spec(2, 1, 1));
        let on_a = beat(&mut state, &logs, "a", &[]).assign[0].task_id.clone();
        let on_b = beat(&mut state, &logs, "b", &[]).assign[0].task_id.clone();
        beat(&mut state, &logs, "b", &[(&on_b, TaskState::Running, None)]);

        // Rank on `a` crashes: its sibling must be stopped before the GPU is reused.
        beat(
            &mut state,
            &logs,
            "a",
            &[(&on_a, TaskState::Failed, Some(3))],
        );
        assert_eq!(state.jobs[&job].state, JobState::Queued);
        assert_eq!(state.jobs[&job].attempt, 2);
        assert_eq!(state.tasks[&on_b].state, TaskState::Killed);
        let resp = beat(&mut state, &logs, "b", &[(&on_b, TaskState::Running, None)]);
        assert_eq!(resp.kill, std::slice::from_ref(&on_b));
        assert_eq!(
            state.jobs[&job].state,
            JobState::Queued,
            "b's GPU is still held by the dying task"
        );

        let resp = beat(&mut state, &logs, "b", &[(&on_b, TaskState::Killed, None)]);
        assert_eq!(
            state.jobs[&job].state,
            JobState::Running,
            "second attempt placed"
        );
        let retry_b = resp.assign[0].task_id.clone();
        let retry_a = beat(&mut state, &logs, "a", &[]).assign[0].task_id.clone();
        assert_ne!(retry_a, on_a);

        // Out of retries now.
        beat(
            &mut state,
            &logs,
            "a",
            &[(&retry_a, TaskState::Failed, Some(3))],
        );
        assert_eq!(state.jobs[&job].state, JobState::Failed);
        assert!(
            state.jobs[&job]
                .message
                .as_deref()
                .unwrap()
                .contains("exited with code 3")
        );
        assert_eq!(state.tasks[&retry_b].state, TaskState::Killed);
    }

    #[test]
    fn lost_node_requeues_without_spending_a_retry_and_cancel_kills() {
        let (mut state, logs) = cluster(&["a", "b"]);
        let job = submit(&mut state, spec(1, 0, 0));
        let node = state.tasks.values().next().unwrap().node.clone();
        let task = beat(&mut state, &logs, &node, &[]).assign[0]
            .task_id
            .clone();
        beat(
            &mut state,
            &logs,
            &node,
            &[(&task, TaskState::Running, None)],
        );

        state.nodes.get_mut(&node).unwrap().last_seen = Instant::now()
            .checked_sub(NODE_TIMEOUT + Duration::from_secs(1))
            .unwrap();
        assert!(state.sweep_lost_nodes());
        assert_eq!(state.tasks[&task].state, TaskState::Lost);
        let job_ref = &state.jobs[&job];
        assert_eq!(
            (job_ref.state, job_ref.attempt, job_ref.retries_used),
            (JobState::Running, 2, 0)
        );
        let survivor = if node == "a" { "b" } else { "a" };
        let moved = beat(&mut state, &logs, survivor, &[]).assign[0]
            .task_id
            .clone();

        // The lost node comes back still running the old task: it is told to stop it.
        state.nodes.get_mut(&node).unwrap().last_seen = Instant::now();
        let resp = beat(
            &mut state,
            &logs,
            &node,
            &[(&task, TaskState::Running, None)],
        );
        assert_eq!(resp.kill, [task]);

        beat(
            &mut state,
            &logs,
            survivor,
            &[(&moved, TaskState::Running, None)],
        );
        assert!(state.cancel(&job));
        assert_eq!(state.jobs[&job].state, JobState::Cancelled);
        let resp = beat(
            &mut state,
            &logs,
            survivor,
            &[(&moved, TaskState::Running, None)],
        );
        assert_eq!(resp.kill, [moved]);
    }

    #[test]
    fn higher_priority_runs_first_when_capacity_frees_up() {
        let (mut state, logs) = cluster(&["a"]);
        let busy = submit(&mut state, spec(1, 1, 0));
        let low = submit(&mut state, spec(1, 1, 0));
        let mut urgent = spec(1, 1, 0);
        urgent.priority = 5;
        let high = submit(&mut state, urgent);
        assert_eq!(state.jobs[&low].state, JobState::Queued);
        assert_eq!(state.jobs[&high].state, JobState::Queued);

        let task = format!("{busy}.1.0");
        beat(&mut state, &logs, "a", &[(&task, TaskState::Running, None)]);
        beat(
            &mut state,
            &logs,
            "a",
            &[(&task, TaskState::Succeeded, Some(0))],
        );
        assert_eq!(state.jobs[&busy].state, JobState::Succeeded);
        assert_eq!(
            state.jobs[&high].state,
            JobState::Running,
            "priority 5 goes first"
        );
        assert_eq!(state.jobs[&low].state, JobState::Queued);
    }

    #[test]
    fn a_job_that_cannot_fit_does_not_block_smaller_ones() {
        let (mut state, _logs) = cluster(&["a"]);
        let big = submit(&mut state, spec(2, 1, 0));
        let small = submit(&mut state, spec(1, 0, 0));
        assert_eq!(state.jobs[&big].state, JobState::Queued);
        let why = state.jobs[&big].message.clone().unwrap_or_default();
        assert!(why.contains("1 of 2 replicas fit"), "{why}");
        assert_eq!(state.jobs[&small].state, JobState::Running);
    }

    #[test]
    fn saved_state_round_trips_and_restarted_nodes_get_a_grace_period() {
        let (mut state, _logs) = cluster(&["a", "b"]);
        let job = submit(&mut state, spec(2, 1, 0));
        let json = serde_json::to_vec(&state).unwrap();
        let restored: State = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.jobs[&job].state, JobState::Running);
        assert_eq!(restored.tasks.len(), 2);
        assert_eq!(restored.next_job, state.next_job);
        assert!(restored.nodes.values().all(Node::ready));
    }

    #[test]
    fn ipv6_hosts_get_brackets_in_blob_addresses() {
        let (mut state, _logs) = cluster(&["a"]);
        let node = state.nodes.get_mut("a").unwrap();
        assert_eq!(node.blob_addr(), "10.0.0.1:1");
        node.host = "fd00::7".into();
        assert_eq!(node.blob_addr(), "[fd00::7]:1");
    }
}
