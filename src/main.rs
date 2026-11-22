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
mod service;
mod store;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "dist-alloc",
    version,
    about = "Pool CPUs and GPUs across machines and run jobs on them"
)]
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
        /// Write an invite file (owner-only) instead of printing the command. Carry it to
        /// the new device and run `dist-alloc service install agent --invite FILE` there.
        #[arg(long, value_name = "FILE")]
        write: Option<PathBuf>,
        /// Address new devices should dial [default: this machine's first LAN address]
        #[arg(long, value_name = "ADDRESS")]
        host: Option<String>,
    },
    /// Keep the control plane or this device's agent running in the background (a systemd
    /// user service that also starts after a reboot)
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
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
enum ServiceCommand {
    /// Install, enable and start the service
    Install {
        role: service::Role,
        /// Invite file from `dist-alloc token --write` (agent only; or use --server/--token)
        #[arg(long, value_name = "FILE")]
        invite: Option<PathBuf>,
        /// Extra options for `dist-alloc server` / `dist-alloc agent`, after `--`
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Stop and remove the service
    Uninstall { role: service::Role },
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

/// Locks `mutex`, recovering the data if a thread panicked while holding it: every critical
/// section here leaves the state consistent between statements, so a panic elsewhere must
/// not take the whole agent or control plane down with it.
pub fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "clamped to >= 0 first, and `as` saturates at u64::MAX"
)]
fn gb(value: f64) -> u64 {
    (value.max(0.0) * (1u64 << 30) as f64) as u64
}

/// `dist-alloc token`: the join command, or an invite file, for new devices.
fn print_join(
    data_dir: Option<PathBuf>,
    write: Option<PathBuf>,
    host: Option<String>,
) -> Result<()> {
    let dir = data_dir.unwrap_or_else(|| cli::data_home().join("server"));
    let path = dir.join("token");
    let raw = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "no control plane has run here yet ({} is missing)",
            path.display()
        )
    })?;
    let token = net::Token::parse(&raw)?;
    let port = server::saved_port(&dir).unwrap_or(net::DEFAULT_PORT);
    let host = host.unwrap_or_else(|| {
        net::lan_addresses()
            .first()
            .map_or_else(|| "<this-machine's-ip>".into(), ToString::to_string)
    });
    match write {
        Some(file) => {
            net::write_private(&file, &service::invite(&format!("{host}:{port}"), &token))
                .with_context(|| format!("writing {}", file.display()))?;
            println!(
                "wrote an invite to {} — it is a secret: anyone holding it controls the pool",
                file.display()
            );
        }
        None => println!("dist-alloc agent --server {host}:{port} --token {token}"),
    }
    Ok(())
}

/// How an agent on the control-plane machine reaches it, if a control plane has run here.
fn local_join() -> Option<(String, net::Token)> {
    let dir = cli::data_home().join("server");
    let token = net::Token::parse(&std::fs::read_to_string(dir.join("token")).ok()?).ok()?;
    let port = server::saved_port(&dir).unwrap_or(net::DEFAULT_PORT);
    Some((format!("127.0.0.1:{port}"), token))
}

/// `dist-alloc service`: install or remove the background service.
fn manage_service(
    command: ServiceCommand,
    server: Option<String>,
    token: Option<String>,
) -> Result<()> {
    match command {
        ServiceCommand::Install { role, invite, args } => {
            let join = match (invite, server, token) {
                (Some(file), _, _) => Some(service::parse_invite(
                    &std::fs::read_to_string(&file)
                        .with_context(|| format!("reading {}", file.display()))?,
                )?),
                (None, Some(server), Some(token)) => Some((server, net::Token::parse(&token)?)),
                // On the control-plane machine itself, offer its own capacity to its own pool.
                (None, None, None) => local_join(),
                (None, _, _) => None,
            };
            service::install(role, join, &args)?;
        }
        ServiceCommand::Uninstall { role } => service::uninstall(role)?,
    }
    Ok(())
}

async fn run(cli: Cli) -> Result<i32> {
    let Cli {
        server,
        token,
        command,
    } = cli;
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
            let token = token.ok_or_else(|| {
                anyhow!("--token is required (run `dist-alloc token` on the control-plane machine)")
            })?;
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
        Command::Token {
            data_dir,
            write,
            host,
        } => print_join(data_dir, write, host)?,
        Command::Service { command } => manage_service(command, server, token)?,
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
                    DatasetCommand::Create { name, files } => {
                        cli::dataset_create(&ep, &name, &files).await?;
                    }
                    DatasetCommand::Ls => cli::dataset_list(&ep).await?,
                },
                Command::Server { .. }
                | Command::Agent { .. }
                | Command::Token { .. }
                | Command::Service { .. } => {
                    unreachable!()
                }
            }
        }
    }
    Ok(0)
}

/// Daemon logs go to stderr, filtered by `RUST_LOG` (default `info`); CLI results go to stdout.
fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_target(false)
        .init();
}

/// Maps a job's exit status onto this process's, keeping failures non-zero.
fn exit_code(code: i32) -> ExitCode {
    match u8::try_from(code) {
        Ok(code) => ExitCode::from(code),
        Err(_) => ExitCode::FAILURE,
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    init_logging();
    match run(Cli::parse()).await {
        Ok(code) => exit_code(code),
        Err(e) => {
            #[expect(
                clippy::print_stderr,
                reason = "the CLI's one place to report a fatal error"
            )]
            {
                eprintln!("error: {e:#}");
            }
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }

    #[test]
    fn job_exit_codes_pass_through_and_odd_ones_still_fail() {
        assert_eq!(exit_code(0), ExitCode::SUCCESS);
        assert_eq!(exit_code(3), ExitCode::from(3));
        assert_eq!(exit_code(-1), ExitCode::FAILURE);
        assert_eq!(exit_code(300), ExitCode::FAILURE);
    }

    #[test]
    fn gb_converts_and_clamps_negative_sizes() {
        assert_eq!(gb(1.0), 1 << 30);
        assert_eq!(gb(0.5), 1 << 29);
        assert_eq!(gb(-3.0), 0);
    }
}
