//! dist-alloc — pool the compute of ordinary networked machines.
//!
//! One binary, three roles:
//! * `dist-alloc server` — the control plane: knows every device, queues and places jobs;
//! * `dist-alloc agent`  — runs on each device, offering its CPUs, memory and GPUs;
//! * everything else     — the operator CLI.

mod agent;
mod cli;
mod net;
mod proto;
mod sched;
mod server;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "dist-alloc", version, about = "Pool CPUs and GPUs across machines and run jobs on them")]
struct Cli {
    /// Control plane address [default: this machine]
    #[arg(long, global = true, env = "DA_SERVER", value_name = "HOST[:PORT]")]
    server: Option<String>,
    /// Cluster token [default: read from the local control plane's data directory]
    #[arg(long, global = true, env = "DA_TOKEN", hide_env_values = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the control plane
    Server {
        #[arg(long, default_value = "0.0.0.0:7443")]
        listen: SocketAddr,
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Join this device to the pool (needs --server and --token)
    Agent {
        /// Node name [default: hostname]
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Port other nodes fetch cached blobs from (0 = pick a free one)
        #[arg(long, default_value_t = 7444)]
        blob_port: u16,
        /// Address other nodes should use to reach this one [default: as seen by the server]
        #[arg(long)]
        advertise_host: Option<String>,
        /// Label for job placement (repeatable)
        #[arg(long = "label", value_name = "KEY=VALUE")]
        labels: Vec<String>,
        /// Offer only this many CPU cores
        #[arg(long)]
        cpus: Option<u32>,
        /// Offer only this much memory
        #[arg(long)]
        memory_mb: Option<u64>,
        /// Do not offer this machine's GPUs
        #[arg(long)]
        no_gpus: bool,
        /// Cap on the evictable blob cache, in GB [default: unlimited]
        #[arg(long)]
        cache_max_gb: Option<f64>,
        /// Evict cached blobs when the disk has less than this free, in GB
        #[arg(long, default_value_t = 10.0)]
        min_free_gb: f64,
    },
    /// Print the join command for new devices (run on the control-plane machine)
    Token {
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Show every device and what it has
    Nodes {
        #[arg(long)]
        json: bool,
    },
    /// Forget a device (its running tasks are rescheduled)
    NodeRm { name: String },
    /// Queue a job and return immediately
    Submit(cli::SubmitArgs),
    /// Queue a job, stream its output, exit with its status
    Run(cli::SubmitArgs),
    /// List jobs
    Jobs {
        #[arg(long)]
        json: bool,
    },
    /// Show one job: tasks, placement, outputs
    Job {
        id: String,
        #[arg(long)]
        json: bool,
    },
    /// Print a job's output
    Logs {
        id: String,
        /// Keep streaming until the job finishes
        #[arg(short, long)]
        follow: bool,
    },
    /// Stop a job
    Cancel { id: String },
    /// Content-addressed files
    Blob {
        #[command(subcommand)]
        command: BlobCommand,
    },
    /// Named sets of files for jobs to consume
    Dataset {
        #[command(subcommand)]
        command: DatasetCommand,
    },
}

#[derive(Subcommand)]
enum BlobCommand {
    /// Upload files; prints their hashes
    Put {
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Download a blob (e.g. a job output) from wherever in the pool it lives
    Get {
        hash: String,
        #[arg(short, long)]
        output: PathBuf,
    },
}

#[derive(Subcommand)]
enum DatasetCommand {
    /// Upload files and register them under a name
    Create {
        name: String,
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// List datasets
    Ls,
}

/// Resolves on SIGINT or SIGTERM.
pub async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate()).expect("installing SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

fn gb(value: f64) -> u64 {
    (value.max(0.0) * (1u64 << 30) as f64) as u64
}

async fn run(cli: Cli) -> Result<i32> {
    let Cli { server, token, command } = cli;
    match command {
        Command::Server { listen, data_dir } => {
            let data_dir = data_dir.unwrap_or_else(|| cli::data_home().join("server"));
            server::run(server::ServerOpts { listen, data_dir }).await?;
        }
        Command::Agent {
            name,
            data_dir,
            blob_port,
            advertise_host,
            labels,
            cpus,
            memory_mb,
            no_gpus,
            cache_max_gb,
            min_free_gb,
        } => {
            let token = token.ok_or_else(|| anyhow!("--token is required (run `dist-alloc token` on the control-plane machine)"))?;
            agent::run(agent::AgentOpts {
                server: server.ok_or_else(|| anyhow!("--server is required"))?,
                token: net::Token::parse(&token)?,
                name,
                data_dir: data_dir.unwrap_or_else(|| cli::data_home().join("agent")),
                blob_port,
                labels: cli::key_values(&labels, "--label")?,
                advertise_host,
                cpus,
                memory_mb,
                no_gpus,
                gc: store::GcPolicy {
                    max_bytes: cache_max_gb.map(gb),
                    min_free_bytes: gb(min_free_gb),
                    min_age: Duration::from_secs(120),
                },
            })
            .await?;
        }
        Command::Token { data_dir } => {
            let path = data_dir.unwrap_or_else(|| cli::data_home().join("server")).join("token");
            let token = std::fs::read_to_string(&path)
                .with_context(|| format!("no control plane has run here yet ({} is missing)", path.display()))?;
            let host = net::lan_addresses()
                .first()
                .map(ToString::to_string)
                .unwrap_or_else(|| "<this-machine's-ip>".into());
            println!("dist-alloc agent --server {host}:{} --token {}", net::DEFAULT_PORT, token.trim());
        }
        command => {
            let ep = cli::connect(server, token).await?;
            match command {
                Command::Nodes { json } => cli::nodes(&ep, json).await?,
                Command::NodeRm { name } => cli::remove_node(&ep, &name).await?,
                Command::Submit(args) => {
                    let job = cli::submit(&ep, args).await?;
                    println!("{}", job.id);
                }
                Command::Run(args) => {
                    let job = cli::submit(&ep, args).await?;
                    return cli::logs(&ep, &job.id, true).await;
                }
                Command::Jobs { json } => cli::jobs(&ep, json).await?,
                Command::Job { id, json } => cli::job(&ep, &id, json).await?,
                Command::Logs { id, follow } => return cli::logs(&ep, &id, follow).await,
                Command::Cancel { id } => cli::cancel(&ep, &id).await?,
                Command::Blob { command } => match command {
                    BlobCommand::Put { files } => cli::blob_put(&ep, &files).await?,
                    BlobCommand::Get { hash, output } => cli::blob_get(&ep, &hash, &output).await?,
                },
                Command::Dataset { command } => match command {
                    DatasetCommand::Create { name, files } => cli::dataset_create(&ep, &name, &files).await?,
                    DatasetCommand::Ls => cli::dataset_list(&ep).await?,
                },
                Command::Server { .. } | Command::Agent { .. } | Command::Token { .. } => unreachable!(),
            }
        }
    }
    Ok(0)
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(code) => ExitCode::from(code.clamp(0, 255) as u8),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
