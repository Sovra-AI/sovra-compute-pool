//! Operator commands: inspect the pool, submit work, move data.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::io::AsyncWriteExt;

use crate::net::{self, Endpoint, Token};
use crate::proto::{
    Blob, BlobSources, Hash, JobSpec, JobState, JobView, LogChunk, NodeView, PutBlobResp,
    Resources, TaskState, is_hash,
};
use crate::store::hash_file;

#[derive(clap::Args)]
pub struct SubmitArgs {
    /// Label shown in listings
    #[arg(long)]
    pub name: Option<String>,
    /// CPU cores per replica
    #[arg(long, default_value_t = 1)]
    pub cpus: u32,
    /// Memory per replica
    #[arg(long, default_value_t = 1024)]
    pub memory_mb: u64,
    /// GPUs per replica (allocated exclusively)
    #[arg(long, default_value_t = 0)]
    pub gpus: u32,
    /// Only use GPUs with at least this much VRAM
    #[arg(long, default_value_t = 0)]
    pub gpu_memory_mb: u64,
    /// Number of copies to run as a gang (all start together or none do)
    #[arg(long, default_value_t = 1)]
    pub replicas: u32,
    /// Re-run this many times if a replica exits non-zero
    #[arg(long, default_value_t = 0)]
    pub retries: u32,
    /// Higher runs first
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    pub priority: i32,
    /// Environment variable for the command (repeatable)
    #[arg(short, long = "env", value_name = "KEY=VALUE")]
    pub env: Vec<String>,
    /// Local file to ship to the nodes as $`DA_INPUTS_DIR/NAME` (repeatable)
    #[arg(short, long = "input", value_name = "[NAME=]PATH")]
    pub inputs: Vec<String>,
    /// Dataset to stage as $`DA_INPUTS_DIR`/<dataset>/<file> (repeatable)
    #[arg(long = "dataset", value_name = "NAME")]
    pub datasets: Vec<String>,
    /// Deal dataset files out across the replicas (rank r stages every replicas-th file)
    /// instead of giving every replica the whole dataset
    #[arg(long)]
    pub partition: bool,
    /// Only run on nodes carrying this label (repeatable)
    #[arg(long = "select", value_name = "LABEL=VALUE")]
    pub select: Vec<String>,
    /// The command to run, after `--`
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

pub fn data_home() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("dist-alloc")
}

/// Connects using `--server/--token`; else to the control plane on this machine; else to
/// the pool this machine's agent service joined.
pub async fn connect(server: Option<String>, token: Option<String>) -> Result<Endpoint> {
    let (server, token) = if let Some(token) = token {
        let server = server.unwrap_or_else(|| format!("127.0.0.1:{}", net::DEFAULT_PORT));
        (server, Token::parse(&token)?)
    } else {
        let local = data_home().join("server");
        if let Ok(raw) = std::fs::read_to_string(local.join("token")) {
            let port = crate::server::saved_port(&local).unwrap_or(net::DEFAULT_PORT);
            (
                server.unwrap_or_else(|| format!("127.0.0.1:{port}")),
                Token::parse(&raw)?,
            )
        } else if let Ok(text) = std::fs::read_to_string(crate::service::agent_env_path()?) {
            let (joined, token) = crate::service::parse_invite(&text)?;
            (server.unwrap_or(joined), token)
        } else {
            bail!("no token: pass --token, set DA_TOKEN, or run this on a computer in the pool")
        }
    };
    Ok(Endpoint::bootstrap(&server, &token).await?.0)
}

pub fn key_values(pairs: &[String], what: &str) -> Result<BTreeMap<String, String>> {
    pairs
        .iter()
        .map(|pair| {
            pair.split_once('=')
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .ok_or_else(|| anyhow!("{what} {pair:?} is not KEY=VALUE"))
        })
        .collect()
}

fn table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let print = |cells: &mut dyn Iterator<Item = &str>| {
        let line: Vec<String> = cells
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        println!("{}", line.join("  ").trim_end());
    };
    print(&mut headers.iter().copied());
    for row in rows {
        print(&mut row.iter().map(String::as_str));
    }
}

fn gib(bytes: u64) -> String {
    format!("{:.1}G", bytes as f64 / (1u64 << 30) as f64)
}

fn bytes(n: u64) -> String {
    match n {
        n if n >= 1 << 30 => gib(n),
        n if n >= 1 << 20 => format!("{:.1}M", n as f64 / (1u64 << 20) as f64),
        n if n >= 1 << 10 => format!("{:.1}K", n as f64 / 1024.0),
        n => format!("{n}B"),
    }
}

fn ago(unix: u64) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
        .saturating_sub(unix);
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86_400 => format!("{}h", s / 3600),
        s => format!("{}d", s / 86_400),
    }
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

// ---------------------------------------------------------------- pool

pub async fn nodes(ep: &Endpoint, json: bool) -> Result<()> {
    let nodes: Vec<NodeView> = net::send_json(ep.get("/v1/nodes")).await?;
    if json {
        return print_json(&nodes);
    }
    if nodes.is_empty() {
        println!("no devices have joined yet — run `dist-alloc token` for the join command");
        return Ok(());
    }
    let rows: Vec<Vec<String>> = nodes
        .iter()
        .map(|n| {
            let gpu = if n.info.gpus.is_empty() {
                "-".to_owned()
            } else {
                let mut models: Vec<String> = n
                    .info
                    .gpus
                    .iter()
                    .map(|g| {
                        let name = g.name.replace("NVIDIA ", "").replace("GeForce ", "");
                        format!("{name} {:.0}G", g.memory_mb as f64 / 1024.0)
                    })
                    .collect();
                models.dedup();
                let busy: u32 = n.stats.gpus.iter().map(|g| g.util_pct).max().unwrap_or(0);
                format!(
                    "{}/{} {} ({busy}% busy)",
                    n.alloc_gpus.len(),
                    n.info.gpus.len(),
                    models.join(", ")
                )
            };
            vec![
                n.name.clone(),
                if n.ready {
                    "ready".into()
                } else {
                    format!("lost {}s", n.last_seen_secs)
                },
                n.host.clone(),
                format!("{}/{}", n.alloc_cpus, n.info.cpus),
                format!(
                    "{:.1}/{:.1}G",
                    n.alloc_memory_mb as f64 / 1024.0,
                    n.info.memory_mb as f64 / 1024.0
                ),
                gpu,
                format!("{:.2}", n.stats.load1),
                gib(n.stats.disk_free_bytes),
                format!("{} ({})", n.stats.cache_blobs, bytes(n.stats.cache_bytes)),
                n.tasks.len().to_string(),
            ]
        })
        .collect();
    table(
        &[
            "NODE",
            "STATUS",
            "HOST",
            "CPU",
            "MEM",
            "GPU",
            "LOAD",
            "DISK FREE",
            "BLOBS",
            "TASKS",
        ],
        &rows,
    );
    let ready: Vec<&NodeView> = nodes.iter().filter(|n| n.ready).collect();
    println!(
        "\npool: {} of {} nodes ready — {} cpu, {:.0} GB RAM, {} gpu ({} allocated)",
        ready.len(),
        nodes.len(),
        ready.iter().map(|n| n.info.cpus).sum::<u32>(),
        ready.iter().map(|n| n.info.memory_mb).sum::<u64>() as f64 / 1024.0,
        ready.iter().map(|n| n.info.gpus.len()).sum::<usize>(),
        ready.iter().map(|n| n.alloc_gpus.len()).sum::<usize>(),
    );
    Ok(())
}

pub async fn remove_node(ep: &Endpoint, name: &str) -> Result<()> {
    net::send(ep.delete(&format!("/v1/nodes/{name}"))).await?;
    println!("removed {name}");
    Ok(())
}

// ---------------------------------------------------------------- data

/// Uploads a file to the control plane's origin store unless the pool already has it.
async fn upload(ep: &Endpoint, path: &Path) -> Result<(Hash, u64)> {
    let owned = path.to_owned();
    let (hash, size) = tokio::task::spawn_blocking(move || hash_file(&owned))
        .await?
        .with_context(|| format!("reading {}", path.display()))?;
    let sources: BlobSources = net::send_json(ep.get(&format!("/v1/blobs/{hash}/sources"))).await?;
    if sources.origin || !sources.peers.is_empty() {
        return Ok((hash, size));
    }
    let file = tokio::fs::File::open(path).await?;
    let body =
        reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::with_capacity(file, 1 << 16));
    let stored: PutBlobResp = net::send_json(
        ep.put("/v1/blobs")
            .header(reqwest::header::CONTENT_LENGTH, size)
            .body(body),
    )
    .await
    .with_context(|| format!("uploading {}", path.display()))?;
    if stored.hash != hash {
        bail!("{} changed while it was being uploaded", path.display());
    }
    Ok((hash, size))
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("{} has no usable file name", path.display()))
}

pub async fn blob_put(ep: &Endpoint, files: &[PathBuf]) -> Result<()> {
    for path in files {
        let (hash, size) = upload(ep, path).await?;
        println!("{hash}  {:>8}  {}", bytes(size), path.display());
    }
    Ok(())
}

pub async fn blob_get(ep: &Endpoint, hash: &str, output: &Path) -> Result<()> {
    if !is_hash(hash) {
        bail!("{hash:?} is not a BLAKE3 hash");
    }
    let mut response = net::send(ep.get(&format!("/v1/blobs/{hash}"))).await?;
    let partial = output.with_extension("partial");
    let mut file = tokio::fs::File::create(&partial).await?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    while let Some(chunk) = response.chunk().await? {
        hasher.update(&chunk);
        size += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    if hasher.finalize().to_hex().as_str() != hash {
        let _ = tokio::fs::remove_file(&partial).await;
        bail!("download did not match its hash; discarded");
    }
    tokio::fs::rename(&partial, output).await?;
    println!("{} ({})", output.display(), bytes(size));
    Ok(())
}

pub async fn dataset_create(ep: &Endpoint, name: &str, files: &[PathBuf]) -> Result<()> {
    let mut entries = Vec::new();
    for path in files {
        let (hash, size) = upload(ep, path).await?;
        eprintln!("  {}  {:>8}  {}", &hash[..12], bytes(size), path.display());
        entries.push(Blob {
            name: file_name(path)?,
            hash,
            size,
        });
    }
    net::send(ep.put(&format!("/v1/datasets/{name}")).json(&entries)).await?;
    println!(
        "dataset {name}: {} files, {}",
        entries.len(),
        bytes(entries.iter().map(|e| e.size).sum())
    );
    Ok(())
}

pub async fn dataset_list(ep: &Endpoint) -> Result<()> {
    let datasets: BTreeMap<String, Vec<Blob>> = net::send_json(ep.get("/v1/datasets")).await?;
    let rows: Vec<Vec<String>> = datasets
        .iter()
        .map(|(name, files)| {
            vec![
                name.clone(),
                files.len().to_string(),
                bytes(files.iter().map(|f| f.size).sum()),
            ]
        })
        .collect();
    table(&["DATASET", "FILES", "SIZE"], &rows);
    Ok(())
}

// ---------------------------------------------------------------- jobs

pub async fn submit(ep: &Endpoint, args: SubmitArgs) -> Result<JobView> {
    let (mut inputs, mut shards) = (Vec::new(), Vec::new());
    for raw in &args.inputs {
        let (name, path) = match raw.split_once('=') {
            Some((name, path)) => (name.to_owned(), PathBuf::from(path)),
            None => (file_name(Path::new(raw))?, PathBuf::from(raw)),
        };
        let (hash, size) = upload(ep, &path).await?;
        inputs.push(Blob { name, hash, size });
    }
    if !args.datasets.is_empty() {
        let known: BTreeMap<String, Vec<Blob>> = net::send_json(ep.get("/v1/datasets")).await?;
        for dataset in &args.datasets {
            let files = known.get(dataset).ok_or_else(|| {
                anyhow!("no dataset named {dataset:?} (see `dist-alloc dataset ls`)")
            })?;
            let target = if args.partition {
                &mut shards
            } else {
                &mut inputs
            };
            target.extend(files.iter().map(|f| Blob {
                name: format!("{dataset}/{}", f.name),
                ..f.clone()
            }));
        }
    }
    let spec = JobSpec {
        name: args.name.unwrap_or_else(|| {
            args.command[0]
                .rsplit('/')
                .next()
                .unwrap_or("job")
                .to_owned()
        }),
        command: args.command,
        env: key_values(&args.env, "--env")?,
        resources: Resources {
            cpus: args.cpus,
            memory_mb: args.memory_mb,
            gpus: args.gpus,
            gpu_memory_mb: args.gpu_memory_mb,
        },
        replicas: args.replicas,
        inputs,
        shards,
        selector: key_values(&args.select, "--select")?,
        max_retries: args.retries,
        priority: args.priority,
    };
    net::send_json(ep.post("/v1/jobs").json(&spec)).await
}

fn placement(job: &JobView) -> String {
    let mut nodes: Vec<&str> = job
        .tasks
        .iter()
        .filter(|t| t.attempt == job.attempt)
        .map(|t| t.node.as_str())
        .collect();
    nodes.dedup();
    if nodes.is_empty() {
        "-".into()
    } else {
        nodes.join(",")
    }
}

pub async fn jobs(ep: &Endpoint, json: bool) -> Result<()> {
    let jobs: Vec<JobView> = net::send_json(ep.get("/v1/jobs")).await?;
    if json {
        return print_json(&jobs);
    }
    let rows: Vec<Vec<String>> = jobs
        .iter()
        .map(|j| {
            let r = &j.spec.resources;
            vec![
                j.id.clone(),
                j.spec.name.clone(),
                format!("{:?}", j.state).to_lowercase(),
                format!("{}x {}c/{}g", j.spec.replicas, r.cpus, r.gpus),
                placement(j),
                ago(j.submitted_at),
                j.message.clone().unwrap_or_default(),
            ]
        })
        .collect();
    table(
        &["JOB", "NAME", "STATE", "SHAPE", "NODES", "AGE", "NOTE"],
        &rows,
    );
    Ok(())
}

pub async fn job(ep: &Endpoint, id: &str, json: bool) -> Result<()> {
    let job: JobView = net::send_json(ep.get(&format!("/v1/jobs/{id}"))).await?;
    if json {
        return print_json(&job);
    }
    let r = &job.spec.resources;
    println!("job      {} ({})", job.id, job.spec.name);
    println!(
        "state    {}{}",
        format!("{:?}", job.state).to_lowercase(),
        job.message
            .as_deref()
            .map(|m| format!(" — {m}"))
            .unwrap_or_default()
    );
    println!("command  {}", job.spec.command.join(" "));
    println!(
        "shape    {} replica(s), each {} cpu, {} MB, {} gpu",
        job.spec.replicas, r.cpus, r.memory_mb, r.gpus
    );
    let staged: Vec<&Blob> = job.spec.inputs.iter().chain(&job.spec.shards).collect();
    if !staged.is_empty() {
        println!(
            "inputs   {} files, {}{}",
            staged.len(),
            bytes(staged.iter().map(|i| i.size).sum()),
            if job.spec.shards.is_empty() {
                ""
            } else {
                " (dataset files dealt across replicas)"
            }
        );
    }
    let rows: Vec<Vec<String>> = job
        .tasks
        .iter()
        .map(|t| {
            vec![
                t.id.clone(),
                t.node.clone(),
                if t.gpus.is_empty() {
                    "-".into()
                } else {
                    format!("{:?}", t.gpus)
                },
                format!("{:?}", t.state).to_lowercase(),
                t.exit_code.map(|c| c.to_string()).unwrap_or_default(),
                t.message.clone().unwrap_or_default(),
            ]
        })
        .collect();
    println!();
    table(&["TASK", "NODE", "GPUS", "STATE", "EXIT", "NOTE"], &rows);
    let outputs: Vec<Vec<String>> = job
        .tasks
        .iter()
        .filter(|t| t.attempt == job.attempt)
        .flat_map(|t| {
            t.outputs.iter().map(move |o| {
                vec![
                    format!("r{}", t.rank),
                    o.name.clone(),
                    bytes(o.size),
                    o.hash.clone(),
                ]
            })
        })
        .collect();
    if !outputs.is_empty() {
        println!();
        table(
            &[
                "RANK",
                "OUTPUT",
                "SIZE",
                "HASH (dist-alloc blob get <hash> -o <file>)",
            ],
            &outputs,
        );
    }
    Ok(())
}

pub async fn cancel(ep: &Endpoint, id: &str) -> Result<()> {
    let job: JobView = net::send_json(ep.post(&format!("/v1/jobs/{id}/cancel"))).await?;
    println!("{} {}", job.id, format!("{:?}", job.state).to_lowercase());
    Ok(())
}

/// Prints a job's output; with `follow`, keeps going until the job finishes and returns the
/// exit code a shell would expect.
pub async fn logs(ep: &Endpoint, id: &str, follow: bool) -> Result<i32> {
    let mut offsets: HashMap<String, u64> = HashMap::new();
    let mut announced = String::new();
    let stdout = std::io::stdout();
    loop {
        let job: JobView = net::send_json(ep.get(&format!("/v1/jobs/{id}"))).await?;
        if follow {
            let status = match job.state {
                JobState::Queued => format!(
                    "{id} queued{}",
                    job.message
                        .as_deref()
                        .map(|m| format!(": {m}"))
                        .unwrap_or_default()
                ),
                JobState::Running => format!(
                    "{id} running on {} (attempt {})",
                    placement(&job),
                    job.attempt
                ),
                _ => String::new(),
            };
            if !status.is_empty() && status != announced {
                eprintln!("[{status}]");
                announced = status;
            }
        }
        let mut progressed = false;
        for task in &job.tasks {
            loop {
                let offset = offsets.get(&task.id).copied().unwrap_or(0);
                let chunk: LogChunk = net::send_json(
                    ep.get(&format!("/v1/jobs/{id}/logs"))
                        .query(&[("task", task.id.as_str()), ("offset", &offset.to_string())]),
                )
                .await?;
                if chunk.next_offset == offset {
                    break;
                }
                progressed = true;
                offsets.insert(task.id.clone(), chunk.next_offset);
                let mut out = stdout.lock();
                if job.spec.replicas > 1 {
                    for line in chunk.data.split_inclusive('\n') {
                        write!(out, "[r{}] {line}", task.rank)?;
                    }
                } else {
                    out.write_all(chunk.data.as_bytes())?;
                }
                out.flush()?;
            }
        }
        if job.state.is_terminal() && !progressed || !follow {
            if follow {
                eprintln!(
                    "[{id} {}{}]",
                    format!("{:?}", job.state).to_lowercase(),
                    job.message
                        .as_deref()
                        .map(|m| format!(": {m}"))
                        .unwrap_or_default()
                );
            }
            return Ok(match job.state {
                // Without --follow a job may still be going; its output so far printed fine.
                JobState::Succeeded | JobState::Queued | JobState::Running => 0,
                JobState::Failed => job
                    .tasks
                    .iter()
                    .filter(|t| t.attempt == job.attempt && t.state == TaskState::Failed)
                    .find_map(|t| t.exit_code)
                    .filter(|code| *code != 0)
                    .unwrap_or(1),
                JobState::Cancelled => 130,
            });
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_are_printed_in_the_largest_whole_unit() {
        assert_eq!(bytes(512), "512B");
        assert_eq!(bytes(1536), "1.5K");
        assert_eq!(bytes(5 << 20), "5.0M");
        assert_eq!(bytes(3 << 30), "3.0G");
    }

    #[test]
    fn labels_and_env_must_be_key_value_pairs() {
        let parsed = key_values(&["site=lab".into(), "url=a=b".into()], "--label").unwrap();
        assert_eq!(parsed["site"], "lab");
        assert_eq!(parsed["url"], "a=b", "only the first = splits");
        let err = key_values(&["oops".into()], "--label").unwrap_err();
        assert!(err.to_string().contains("--label \"oops\""), "{err}");
    }
}
