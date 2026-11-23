//! Node agent: turns a device into pool capacity.
//!
//! It detects what the machine has, dials the control plane, and from then on heartbeats:
//! reporting live stats, task progress, logs and its blob inventory, and receiving tasks to
//! start or stop. Inputs are pulled from peer nodes by content hash before a task starts,
//! so the task itself only ever reads local files.

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::IpAddr;
use std::path::{Path as FsPath, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::{StreamExt, TryStreamExt, stream};
use nix::sys::signal::Signal;

use crate::net::{self, ApiFailure, Endpoint, Token, constant_time_eq};
use crate::proto::{
    Assignment, Blob, BlobSources, GpuInfo, GpuStat, HeartbeatReq, HeartbeatResp, NodeInfo,
    NodeStats, RegisterReq, RegisterResp, TaskReport, TaskState, safe_name,
};
use crate::server::{Fail, bearer, file_response};
use crate::store::{self, GcPolicy, Store};

/// Most log bytes shipped per task per heartbeat.
pub const LOG_CHUNK_MAX: usize = 256 << 10;
const FETCH_PARALLELISM: usize = 4;
const FETCH_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const KILL_GRACE: Duration = Duration::from_secs(10);

pub struct AgentOpts {
    pub server: String,
    pub token: Token,
    pub name: Option<String>,
    pub data_dir: PathBuf,
    pub blob_port: u16,
    pub labels: BTreeMap<String, String>,
    pub advertise_host: Option<String>,
    pub cpus: Option<u32>,
    pub memory_mb: Option<u64>,
    pub no_gpus: bool,
    pub gc: GcPolicy,
}

// ---------------------------------------------------------------- resource detection

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
}

fn meminfo_mb(key: &str) -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            let line = text.lines().find(|l| l.starts_with(key))?;
            line.split_whitespace().nth(1)?.parse::<u64>().ok()
        })
        .map_or(0, |kb| kb / 1024)
}

/// NVIDIA GPUs via `nvidia-smi`; empty when there is no driver or no GPU.
fn probe_gpus() -> (Vec<GpuInfo>, Vec<GpuStat>) {
    let output = std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,uuid,name,memory.total,memory.used,utilization.gpu",
            "--format=csv,noheader,nounits",
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output();
    let Ok(output) = output else {
        return Default::default();
    };
    if !output.status.success() {
        return Default::default();
    }
    let mut infos = Vec::new();
    let mut stats = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        let [index, uuid, name, total, used, util] = fields.as_slice() else {
            continue;
        };
        let Ok(index) = index.parse::<u32>() else {
            continue;
        };
        infos.push(GpuInfo {
            index,
            uuid: (*uuid).to_owned(),
            name: (*name).to_owned(),
            memory_mb: total.parse().unwrap_or(0),
        });
        stats.push(GpuStat {
            index,
            memory_used_mb: used.parse().unwrap_or(0),
            util_pct: util.parse().unwrap_or(0),
        });
    }
    (infos, stats)
}

fn probe_node(opts: &AgentOpts, gpus: Vec<GpuInfo>) -> NodeInfo {
    let detected_cpus = std::thread::available_parallelism()
        .map_or(1, |n| u32::try_from(n.get()).unwrap_or(u32::MAX));
    let detected_mem = meminfo_mb("MemTotal:");
    NodeInfo {
        hostname: read_trimmed("/proc/sys/kernel/hostname").unwrap_or_else(|| "node".into()),
        os: format!(
            "{} {}",
            std::env::consts::OS,
            read_trimmed("/proc/sys/kernel/osrelease").unwrap_or_default()
        ),
        arch: std::env::consts::ARCH.to_owned(),
        // A device may offer less than it has, never more.
        cpus: opts
            .cpus
            .map_or(detected_cpus, |cap| cap.min(detected_cpus)),
        memory_mb: opts
            .memory_mb
            .map_or(detected_mem, |cap| cap.min(detected_mem)),
        gpus,
    }
}

// ---------------------------------------------------------------- agent state

struct Cancel {
    flag: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Cancel {
    fn fire(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }

    async fn wait(&self) {
        if !self.flag.load(Ordering::SeqCst) {
            self.notify.notified().await;
        }
    }
}

struct Local {
    state: TaskState,
    exit_code: Option<i32>,
    message: Option<String>,
    outputs: Vec<Blob>,
    /// Log bytes the control plane has confirmed.
    acked_log: u64,
    pgid: Option<i32>,
    cancel: Arc<Cancel>,
}

struct Agent {
    name: String,
    dir: PathBuf,
    server: Endpoint,
    cert_pem: String,
    peer_key: String,
    store: Store,
    gpus: Vec<GpuInfo>,
    gpu_stats: Mutex<Vec<GpuStat>>,
    tasks: Mutex<HashMap<String, Local>>,
    peers: Mutex<HashMap<String, Endpoint>>,
    fetch_slots: tokio::sync::Semaphore,
}

fn kill_group(pgid: i32, signal: Signal) {
    // A group that is already gone (ESRCH) is exactly the outcome we want.
    let _ = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pgid), signal);
}

impl Agent {
    async fn register(&self, registration: &RegisterReq) -> Result<RegisterResp> {
        net::send_json(self.server.post("/v1/nodes").json(registration)).await
    }

    fn task_dir(&self, task_id: &str) -> PathBuf {
        self.dir.join("tasks").join(task_id)
    }

    fn set_state(&self, task_id: &str, state: TaskState, pgid: Option<i32>) {
        if let Some(task) = crate::lock(&self.tasks).get_mut(task_id) {
            task.state = state;
            task.pgid = pgid;
        }
    }

    /// A node that registered through loopback is on the control plane's machine; other
    /// nodes must reach it at the control plane's address instead.
    fn routable(&self, ip: IpAddr) -> IpAddr {
        if ip.is_loopback() && !self.server.addr.ip().is_loopback() {
            self.server.addr.ip()
        } else {
            ip
        }
    }

    // ------------------------------------------------------------ blobs

    async fn download(&self, request: reqwest::RequestBuilder, hash: &str) -> Result<()> {
        // Allow large transfers as long as they make progress. A stalled peer must
        // eventually yield its slot or fall through to the next available source.
        let mut response = tokio::time::timeout(FETCH_IDLE_TIMEOUT, net::send(request))
            .await
            .context("blob source did not respond")??;
        let mut writer = self.store.writer(false).await?;
        while let Some(chunk) = tokio::time::timeout(FETCH_IDLE_TIMEOUT, response.chunk())
            .await
            .context("blob source stopped sending data")??
        {
            writer.write(&chunk).await?;
        }
        writer.finish(Some(hash)).await?;
        Ok(())
    }

    async fn peer(&self, peer: &str) -> Result<Endpoint> {
        if let Some(endpoint) = crate::lock(&self.peers).get(peer) {
            return Ok(endpoint.clone());
        }
        let mut addr = net::resolve(peer).await?;
        addr.set_ip(self.routable(addr.ip()));
        let endpoint = Endpoint::pinned(addr, &self.cert_pem, &self.peer_key)?;
        crate::lock(&self.peers).insert(peer.to_owned(), endpoint.clone());
        Ok(endpoint)
    }

    /// Makes a blob available locally: cache hit, else from a peer node, else from the
    /// control plane's origin copy. Every transfer is verified against the hash.
    async fn ensure_blob(&self, hash: &str) -> Result<PathBuf> {
        if let Some(path) = self.store.find(hash) {
            return Ok(path);
        }
        let _exclusive = self.store.fetch_lock(hash).await;
        if let Some(path) = self.store.find(hash) {
            return Ok(path);
        }
        // Cache hits and duplicate waiters do not consume a network slot.
        let _slot = self.fetch_slots.acquire().await?;
        let sources: BlobSources = net::send_json(
            self.server
                .get(&format!("/v1/blobs/{hash}/sources"))
                .timeout(FETCH_IDLE_TIMEOUT)
                .query(&[("exclude", &self.name)]),
        )
        .await
        .context("asking the control plane who has the blob")?;

        let mut errors = Vec::new();
        for peer in &sources.peers {
            let attempt = async {
                let endpoint = self.peer(peer).await?;
                self.download(endpoint.get(&format!("/blobs/{hash}")), hash)
                    .await
            };
            match attempt.await {
                Ok(()) => {
                    tracing::info!(blob = &hash[..12], %peer, "fetched blob from peer");
                    break;
                }
                Err(e) => errors.push(format!("{peer}: {e:#}")),
            }
        }
        if self.store.find(hash).is_none() && sources.origin {
            let request = self.server.get(&format!("/v1/blobs/{hash}"));
            match self.download(request, hash).await {
                Ok(()) => tracing::info!(blob = &hash[..12], "fetched blob from the control plane"),
                Err(e) => errors.push(format!("control plane: {e:#}")),
            }
        }
        self.store.find(hash).ok_or_else(|| {
            if errors.is_empty() {
                anyhow!("blob {hash} is not held by any reachable node")
            } else {
                anyhow!("could not fetch blob {hash}: {}", errors.join("; "))
            }
        })
    }

    // ------------------------------------------------------------ tasks

    async fn run_task(self: Arc<Self>, a: Assignment, cancel: Arc<Cancel>) {
        for input in &a.inputs {
            self.store.pin(&input.hash);
        }
        let dir = self.task_dir(&a.task_id);
        let outcome = self.execute(&a, &dir, &cancel).await;
        for input in &a.inputs {
            self.store.unpin(&input.hash);
        }
        let (state, exit_code, message, outputs) = outcome
            .unwrap_or_else(|e| (TaskState::Failed, None, Some(format!("{e:#}")), Vec::new()));
        if let Some(message) = &message
            && let Ok(mut log) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("log"))
        {
            let _ = writeln!(log, "[dist-alloc] {message}");
        }
        tracing::info!(task = %a.task_id, ?state, message = message.as_deref().unwrap_or(""), "task finished");
        if let Some(task) = crate::lock(&self.tasks).get_mut(&a.task_id) {
            task.state = state;
            task.exit_code = exit_code;
            task.message = message;
            task.outputs = outputs;
            task.pgid = None;
        }
    }

    /// Pulls every input onto local disk and links it under `inputs_dir`. Returns `false`
    /// when the task was cancelled first.
    async fn stage_inputs(
        &self,
        a: &Assignment,
        inputs_dir: &FsPath,
        cancel: &Cancel,
    ) -> Result<bool> {
        self.set_state(&a.task_id, TaskState::Fetching, None);
        let stage = stream::iter(&a.inputs).map(Ok).try_for_each_concurrent(
            FETCH_PARALLELISM,
            |input| async move {
                if !safe_name(&input.name) {
                    bail!("input name {:?} is not a safe relative path", input.name);
                }
                let path = self.ensure_blob(&input.hash).await?;
                let link = inputs_dir.join(&input.name);
                if let Some(parent) = link.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::symlink(path, link).await?;
                Ok(())
            },
        );
        tokio::select! {
            () = cancel.wait() => Ok(false),
            result = stage => result.map(|()| true),
        }
    }

    /// The task's process, not yet started: its own process group, `CUDA_VISIBLE_DEVICES`
    /// limited to the allocated GPUs (by UUID; a task that asked for none sees none), and
    /// the `DA_*` rendezvous variables.
    fn task_command(
        &self,
        a: &Assignment,
        inputs_dir: &FsPath,
        outputs_dir: &FsPath,
        work_dir: &FsPath,
        log: std::fs::File,
    ) -> Result<tokio::process::Command> {
        let visible: Vec<&str> = a
            .gpus
            .iter()
            .filter_map(|index| self.gpus.iter().find(|g| g.index == *index))
            .map(|g| g.uuid.as_str())
            .collect();
        let peer_hosts: Vec<String> = a
            .peer_hosts
            .iter()
            .map(|host| match host.parse::<IpAddr>() {
                Ok(ip) => self.routable(ip).to_string(),
                Err(_) => host.clone(),
            })
            .collect();
        let Some((program, args)) = a.command.split_first() else {
            bail!("the job has no command to run");
        };
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .current_dir(work_dir)
            // The cluster-admin token is the agent's, not the job's.
            .env_remove("DA_TOKEN")
            .envs(&a.env)
            .env("DA_JOB_ID", &a.job_id)
            .env("DA_JOB_NAME", &a.job_name)
            .env("DA_TASK_ID", &a.task_id)
            .env("DA_NODE", &self.name)
            .env("DA_RANK", a.rank.to_string())
            .env("DA_WORLD_SIZE", a.world_size.to_string())
            .env("DA_PEER_HOSTS", peer_hosts.join(","))
            .env(
                "DA_MASTER_ADDR",
                peer_hosts.first().map_or("", String::as_str),
            )
            .env("DA_INPUTS_DIR", inputs_dir)
            .env("DA_OUTPUT_DIR", outputs_dir)
            .env(
                "DA_GPUS",
                a.gpus
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .env("CUDA_VISIBLE_DEVICES", visible.join(","))
            .env(
                "NVIDIA_VISIBLE_DEVICES",
                if visible.is_empty() {
                    "void".into()
                } else {
                    visible.join(",")
                },
            )
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .process_group(0)
            .kill_on_drop(true);
        #[cfg(target_os = "linux")]
        #[expect(
            unsafe_code,
            reason = "pre_exec is the only way to set a child's death signal"
        )]
        // SAFETY: the closure runs between fork and exec, where only async-signal-safe calls
        // are allowed; it makes a single prctl(2) syscall and allocates nothing. It makes the
        // kernel kill the task if this agent dies, so a crashed agent cannot leave work
        // running on GPUs it no longer accounts for.
        unsafe {
            command.pre_exec(|| {
                nix::sys::prctl::set_pdeathsig(Signal::SIGKILL).map_err(std::io::Error::from)
            });
        }
        Ok(command)
    }

    async fn execute(
        &self,
        a: &Assignment,
        dir: &FsPath,
        cancel: &Cancel,
    ) -> Result<(TaskState, Option<i32>, Option<String>, Vec<Blob>)> {
        let killed = || Ok((TaskState::Killed, None, None, Vec::new()));
        let (inputs_dir, outputs_dir, work_dir) =
            (dir.join("inputs"), dir.join("outputs"), dir.join("work"));
        for d in [&inputs_dir, &outputs_dir, &work_dir] {
            tokio::fs::create_dir_all(d).await?;
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("log"))?;

        // 1. Stage inputs from the swarm onto local disk.
        if !self.stage_inputs(a, &inputs_dir, cancel).await? {
            return killed();
        }

        // 2. Run.
        let mut command = self.task_command(a, &inputs_dir, &outputs_dir, &work_dir, log)?;
        if cancel.flag.load(Ordering::SeqCst) {
            return killed();
        }
        let mut child = command
            .spawn()
            .with_context(|| format!("cannot start {:?}", a.command.first()))?;
        let pgid = child.id().and_then(|pid| i32::try_from(pid).ok());
        self.set_state(&a.task_id, TaskState::Running, pgid);
        tracing::info!(task = %a.task_id, command = %a.command.join(" "), "task running");

        let status = tokio::select! {
            status = child.wait() => Some(status?),
            () = cancel.wait() => None,
        };
        let Some(status) = status else {
            // Polite first, then firm.
            if let Some(pgid) = pgid {
                kill_group(pgid, Signal::SIGTERM);
            }
            if tokio::time::timeout(KILL_GRACE, child.wait())
                .await
                .is_err()
            {
                let _ = child.kill().await;
            }
            if let Some(pgid) = pgid {
                kill_group(pgid, Signal::SIGKILL);
            }
            return killed();
        };
        // Anything the command left running in the background goes with it.
        if let Some(pgid) = pgid {
            kill_group(pgid, Signal::SIGKILL);
        }

        // 3. Publish whatever the task wrote to $DA_OUTPUT_DIR as content-addressed blobs.
        let mut outputs = Vec::new();
        for (name, path) in walk_files(&outputs_dir)? {
            let (hash, size) = self.store.adopt(&path).await?;
            outputs.push(Blob { name, hash, size });
        }

        Ok(match status.code() {
            Some(0) => (TaskState::Succeeded, Some(0), None, outputs),
            Some(code) => (TaskState::Failed, Some(code), None, outputs),
            None => {
                use std::os::unix::process::ExitStatusExt;
                let signal = status.signal().unwrap_or(0);
                (
                    TaskState::Failed,
                    None,
                    Some(format!("killed by signal {signal}")),
                    outputs,
                )
            }
        })
    }

    // ------------------------------------------------------------ heartbeat

    fn stats(&self, cache_blobs: u64, cache_bytes: u64) -> NodeStats {
        NodeStats {
            load1: read_trimmed("/proc/loadavg")
                .and_then(|s| s.split_whitespace().next()?.parse().ok())
                .unwrap_or(0.0),
            mem_available_mb: meminfo_mb("MemAvailable:"),
            disk_free_bytes: store::free_bytes(&self.dir).unwrap_or(0),
            cache_bytes,
            cache_blobs,
            gpus: crate::lock(&self.gpu_stats).clone(),
        }
    }

    fn reports(&self) -> Vec<TaskReport> {
        let tasks = crate::lock(&self.tasks);
        tasks
            .iter()
            .map(|(id, task)| {
                let (log, log_len, log_total) =
                    read_log(&self.task_dir(id).join("log"), task.acked_log);
                TaskReport {
                    task_id: id.clone(),
                    state: task.state,
                    exit_code: task.exit_code,
                    message: task.message.clone(),
                    log_offset: task.acked_log,
                    log_len,
                    log_total,
                    log,
                    outputs: task.outputs.clone(),
                }
            })
            .collect()
    }

    fn apply(self: &Arc<Self>, resp: HeartbeatResp) {
        let mut tasks = crate::lock(&self.tasks);
        for (id, offset) in resp.log_offsets {
            if let Some(task) = tasks.get_mut(&id) {
                task.acked_log = offset;
            }
        }
        for assignment in resp.assign {
            if tasks.contains_key(&assignment.task_id) {
                continue;
            }
            let cancel = Arc::new(Cancel {
                flag: AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            });
            tasks.insert(
                assignment.task_id.clone(),
                Local {
                    state: TaskState::Assigned,
                    exit_code: None,
                    message: None,
                    outputs: Vec::new(),
                    acked_log: 0,
                    pgid: None,
                    cancel: cancel.clone(),
                },
            );
            tokio::spawn(self.clone().run_task(assignment, cancel));
        }
        for id in resp.kill {
            if let Some(task) = tasks.get(&id) {
                task.cancel.fire();
            }
        }
        for id in resp.forget {
            if tasks.get(&id).is_some_and(|t| t.state.is_terminal()) {
                tasks.remove(&id);
                let _ = std::fs::remove_dir_all(self.task_dir(&id));
            }
        }
    }
}

/// New log bytes from `offset`: `(text, raw bytes covered, total file size)`.
fn read_log(path: &FsPath, offset: u64) -> (String, u64, u64) {
    let Ok(mut file) = std::fs::File::open(path) else {
        return (String::new(), 0, 0);
    };
    let total = file.metadata().map_or(0, |m| m.len());
    let mut buf = Vec::new();
    if total > offset && file.seek(SeekFrom::Start(offset)).is_ok() {
        let _ = file.take(LOG_CHUNK_MAX as u64).read_to_end(&mut buf);
    }
    // Prefer to end a full chunk on a line boundary, and never inside a UTF-8 character.
    if buf.len() == LOG_CHUNK_MAX
        && let Some(newline) = buf.iter().rposition(|b| *b == b'\n')
    {
        buf.truncate(newline + 1);
    }
    if let Err(e) = std::str::from_utf8(&buf)
        && e.error_len().is_none()
        && e.valid_up_to() > 0
    {
        buf.truncate(e.valid_up_to());
    }
    let len = buf.len() as u64;
    (String::from_utf8_lossy(&buf).into_owned(), len, total)
}

/// Regular files under `root` as `(relative name, path)`, sorted by name.
fn walk_files(root: &FsPath) -> std::io::Result<Vec<(String, PathBuf)>> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file()
                && let Ok(relative) = entry.path().strip_prefix(root)
                && let Some(name) = relative.to_str()
            {
                found.push((name.to_owned(), entry.path()));
            }
        }
    }
    found.sort();
    Ok(found)
}

async fn serve_blob(
    State(agent): State<Arc<Agent>>,
    Path(hash): Path<String>,
    request: Request,
) -> Response {
    if !bearer(&request).is_some_and(|key| constant_time_eq(key, &agent.peer_key)) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match agent.store.find(&hash) {
        Some(path) => file_response(&path)
            .await
            .unwrap_or_else(Fail::into_response),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

// ---------------------------------------------------------------- main loop

/// Serves cached blobs to peer nodes, using the cluster identity handed out at registration.
async fn serve_blobs(
    agent: &Arc<Agent>,
    listener: std::net::TcpListener,
    joined: RegisterResp,
) -> Result<()> {
    let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
        joined.cert_pem.into_bytes(),
        joined.key_pem.into_bytes(),
    )
    .await?;
    let blob_router = Router::new()
        .route("/blobs/{hash}", get(serve_blob))
        .with_state(agent.clone());
    let blob_server = axum_server::from_tcp_rustls(listener, tls)?;
    tokio::spawn(async move {
        if let Err(e) = blob_server.serve(blob_router.into_make_service()).await {
            tracing::error!("blob server stopped: {e}");
        }
    });

    Ok(())
}

/// Background upkeep: refresh GPU utilisation, collect cache garbage.
fn spawn_maintenance(agent: &Arc<Agent>, policy: GcPolicy, watch_gpus: bool) {
    if watch_gpus && !agent.gpus.is_empty() {
        let agent = agent.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if let Ok((_, stats)) = tokio::task::spawn_blocking(probe_gpus).await {
                    *crate::lock(&agent.gpu_stats) = stats;
                }
            }
        });
    }
    {
        let agent = agent.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let worker = agent.clone();
                match tokio::task::spawn_blocking(move || worker.store.gc(&policy)).await {
                    Ok(Ok((count, bytes))) if count > 0 => {
                        tracing::info!(
                            count,
                            gb = format!("{:.1}", bytes as f64 / f64::from(1u32 << 30)),
                            "cache: evicted blobs"
                        );
                    }
                    Ok(Err(e)) => tracing::warn!("cache gc: {e}"),
                    _ => {}
                }
            }
        });
    }
}

/// Reports to the control plane and applies its answers until `shutdown` resolves.
async fn heartbeat_loop(
    agent: &Arc<Agent>,
    registration: &RegisterReq,
    heartbeat: Duration,
    shutdown: &mut std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
) {
    let mut sent_version = None;
    let mut want_blobs = true;
    let (mut cache_blobs, mut cache_bytes) = (0, 0);
    let mut connected = true;
    let path = format!("/v1/nodes/{}/heartbeat", registration.name);
    loop {
        let version = agent.store.version();
        let blobs = if want_blobs || sent_version != Some(version) {
            let worker = agent.clone();
            let snapshot = tokio::task::spawn_blocking(move || worker.store.list()).await;
            match snapshot {
                Ok(blobs) => {
                    cache_blobs = blobs.len() as u64;
                    cache_bytes = blobs.iter().map(|(_, size)| size).sum();
                    Some(blobs)
                }
                Err(e) => {
                    tracing::warn!("reading cache inventory: {e}");
                    None
                }
            }
        } else {
            None
        };
        let includes_blobs = blobs.is_some();
        let request = agent
            .server
            .post(&path)
            .timeout(Duration::from_secs(20))
            .json(&HeartbeatReq {
                stats: agent.stats(cache_blobs, cache_bytes),
                tasks: agent.reports(),
                blobs,
            });
        let beat = tokio::select! {
            () = &mut *shutdown => break,
            beat = net::send_json::<HeartbeatResp>(request) => beat,
        };
        match beat {
            Ok(resp) => {
                if !connected {
                    tracing::info!("reconnected to the control plane");
                    connected = true;
                }
                if includes_blobs {
                    sent_version = Some(version);
                }
                want_blobs = resp.want_blobs;
                agent.apply(resp);
            }
            Err(e)
                if e.downcast_ref::<ApiFailure>()
                    .is_some_and(|f| f.status == 404) =>
            {
                // The control plane lost track of us (state reset, or we were removed).
                want_blobs = true;
                match agent.register(registration).await {
                    Ok(_) => tracing::info!("re-registered with the control plane"),
                    Err(e) => tracing::warn!("re-registration failed: {e:#}"),
                }
            }
            Err(e) => {
                if connected {
                    tracing::warn!("lost contact with the control plane: {e:#} — retrying");
                    connected = false;
                }
            }
        }
        tokio::select! {
            () = &mut *shutdown => break,
            () = tokio::time::sleep(heartbeat) => {}
        }
    }
}

pub async fn run(opts: AgentOpts) -> Result<()> {
    let dir = opts.data_dir.clone();
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let store = Store::open(&dir)?;
    // Tasks do not survive an agent restart; the control plane reschedules them.
    let _ = std::fs::remove_dir_all(dir.join("tasks"));

    let listener = std::net::TcpListener::bind(("0.0.0.0", opts.blob_port))
        .with_context(|| format!("binding blob port {}", opts.blob_port))?;
    listener.set_nonblocking(true)?;
    let blob_port = listener.local_addr()?.port();

    let (gpus, gpu_stats) = if opts.no_gpus {
        Default::default()
    } else {
        tokio::task::spawn_blocking(probe_gpus).await?
    };
    let info = probe_node(&opts, gpus.clone());
    let name = opts.name.clone().unwrap_or_else(|| info.hostname.clone());
    tracing::info!(
        %name,
        cpus = info.cpus,
        memory_gb = format!("{:.1}", info.memory_mb as f64 / 1024.0),
        gpus = info.gpus.len(),
        "dist-alloc agent starting"
    );
    for gpu in &info.gpus {
        tracing::info!(index = gpu.index, model = %gpu.name, memory_gb = gpu.memory_mb / 1024, "gpu");
    }

    let shutdown = crate::shutdown_signal();
    tokio::pin!(shutdown);

    let (server, cert_pem) = loop {
        match Endpoint::bootstrap(&opts.server, &opts.token).await {
            Ok(connected) => break connected,
            Err(e) => tracing::warn!("cannot reach the control plane: {e:#} — retrying in 5s"),
        }
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            () = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    };

    let agent = Arc::new(Agent {
        name: name.clone(),
        dir,
        server,
        cert_pem,
        peer_key: opts.token.peer_key(),
        store,
        gpus,
        gpu_stats: Mutex::new(gpu_stats),
        tasks: Mutex::default(),
        peers: Mutex::default(),
        fetch_slots: tokio::sync::Semaphore::new(FETCH_PARALLELISM),
    });
    let registration = RegisterReq {
        name: name.clone(),
        info,
        labels: opts.labels.clone(),
        blob_port,
        advertise_host: opts.advertise_host.clone(),
    };
    let joined = agent
        .register(&registration)
        .await
        .context("registering with the control plane")?;
    tracing::info!(server = %agent.server.addr, %name, "joined the pool");
    let heartbeat = Duration::from_millis(joined.heartbeat_ms.max(200));

    serve_blobs(&agent, listener, joined).await?;
    spawn_maintenance(&agent, opts.gc, !opts.no_gpus);
    heartbeat_loop(&agent, &registration, heartbeat, &mut shutdown).await;

    tracing::info!("shutting down: stopping tasks");
    for task in crate::lock(&agent.tasks).values() {
        task.cancel.fire();
        if let Some(pgid) = task.pgid {
            kill_group(pgid, Signal::SIGKILL);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("da-agent-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn log_chunks_resume_at_the_offset_and_never_split_a_character() {
        let dir = scratch("log");
        let log = dir.join("log");
        assert_eq!(read_log(&log, 0), (String::new(), 0, 0), "no log yet");

        std::fs::write(&log, "line one\nline two\n").unwrap();
        let (text, len, total) = read_log(&log, 0);
        assert_eq!(
            (text.as_str(), len, total),
            ("line one\nline two\n", 18, 18)
        );
        assert_eq!(read_log(&log, 9).0, "line two\n");
        assert_eq!(read_log(&log, 18).1, 0, "nothing new");

        // A writer caught mid-character: hold back the partial byte until it is complete.
        std::fs::write(&log, b"caf\xc3").unwrap();
        assert_eq!(read_log(&log, 0), ("caf".to_owned(), 3, 4));

        // A full chunk ends on its last newline.
        let mut big = "x".repeat(LOG_CHUNK_MAX - 10);
        big.push('\n');
        big.push_str(&"y".repeat(100));
        std::fs::write(&log, &big).unwrap();
        let (text, len, _) = read_log(&log, 0);
        assert!(text.ends_with('\n'));
        assert_eq!(len, (LOG_CHUNK_MAX - 9) as u64);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn outputs_are_listed_recursively_by_relative_name() {
        let dir = scratch("walk");
        std::fs::create_dir_all(dir.join("ckpt/epoch-1")).unwrap();
        std::fs::write(dir.join("model.bin"), b"m").unwrap();
        std::fs::write(dir.join("ckpt/epoch-1/opt.bin"), b"o").unwrap();
        std::os::unix::fs::symlink("/etc/hostname", dir.join("link")).unwrap();
        let names: Vec<String> = walk_files(&dir)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            names,
            ["ckpt/epoch-1/opt.bin", "model.bin"],
            "symlinks are not outputs"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_device_can_offer_less_than_it_has_but_never_more() {
        let opts = AgentOpts {
            server: String::new(),
            token: Token::parse(&format!("da1.{}.s", "0".repeat(32))).unwrap(),
            name: None,
            data_dir: PathBuf::new(),
            blob_port: 0,
            labels: BTreeMap::new(),
            advertise_host: None,
            cpus: Some(u32::MAX),
            memory_mb: Some(1),
            no_gpus: true,
            gc: GcPolicy {
                max_bytes: None,
                min_free_bytes: 0,
                min_age: Duration::ZERO,
            },
        };
        let info = probe_node(&opts, Vec::new());
        let detected = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        assert_eq!(info.cpus as usize, detected);
        assert_eq!(info.memory_mb, 1);
    }
}
