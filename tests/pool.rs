//! End to end: a real control plane and agent on loopback, driven through the CLI.

#![expect(
    clippy::unwrap_used,
    reason = "test code: a panic is the failure report"
)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_dist-alloc");

/// Kills its process when the test ends, pass or fail.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("da-e2e-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn uninstall_cleans_leftover_agent_credentials_after_an_interrupted_install() {
    let config = std::env::temp_dir().join(format!("da-cleanup-{}", std::process::id()));
    let settings = config.join("dist-alloc");
    std::fs::create_dir(&config).unwrap();
    std::fs::create_dir(&settings).unwrap();
    let credentials = settings.join("agent.env");
    std::fs::write(&credentials, b"test settings").unwrap();
    let keep = settings.join("user-notes");
    std::fs::write(&keep, b"keep me").unwrap();
    for role in ["server", "agent", "agent"] {
        assert!(
            Command::new(BIN)
                .args(["service", "uninstall", role])
                .env("XDG_CONFIG_HOME", &config)
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(credentials.exists(), role == "server");
        assert!(keep.exists());
    }
    std::fs::remove_dir_all(config).unwrap();
}

fn daemon(args: &[&str], log: &Path) -> Daemon {
    let log = std::fs::File::create(log).unwrap();
    Daemon(
        Command::new(BIN)
            .args(args)
            .env_remove("DA_SERVER")
            .env_remove("DA_TOKEN")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    )
}

fn wait_for(what: &str, deadline: Duration, mut ready: impl FnMut() -> bool) {
    let start = Instant::now();
    while !ready() {
        assert!(start.elapsed() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn start_pool(dir: &Path) -> (Daemon, String, String) {
    let server = format!("127.0.0.1:{}", free_port());
    let server_dir = dir.join("server");
    let control = daemon(
        &[
            "server",
            "--listen",
            &server,
            "--data-dir",
            server_dir.to_str().unwrap(),
        ],
        &dir.join("server.log"),
    );
    let token_file = server_dir.join("token");
    wait_for("the control plane", Duration::from_secs(20), || {
        token_file.exists()
    });
    let token = std::fs::read_to_string(token_file).unwrap();
    (control, server, token)
}

fn start_agent(dir: &Path, server: &str, token: &str, name: &str) -> Daemon {
    daemon(
        &[
            "agent",
            "--server",
            server,
            "--token",
            token,
            "--name",
            name,
            "--no-gpus",
            "--cpus",
            "1",
            "--blob-port",
            "0",
            "--data-dir",
            dir.join(name).to_str().unwrap(),
        ],
        &dir.join(format!("{name}.log")),
    )
}

fn pool_cli(server: &str, token: &str, args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        .env("DA_SERVER", server)
        .env("DA_TOKEN", token)
        .output()
        .unwrap()
}

#[test]
fn a_job_runs_on_a_joined_device_and_its_output_comes_back() {
    let dir = scratch("run");
    let (_control, server, token) = start_pool(&dir);
    let _agent = start_agent(&dir, &server, &token, "box1");
    let cli = |args: &[&str]| pool_cli(&server, &token, args);
    wait_for("the device to join", Duration::from_secs(20), || {
        String::from_utf8_lossy(&cli(&["nodes"]).stdout).contains("box1  ready")
    });

    let input = dir.join("greeting.txt");
    std::fs::write(&input, "hello pool\n").unwrap();
    let run = cli(&[
        "run",
        "-i",
        input.to_str().unwrap(),
        "--",
        "sh",
        "-c",
        // The agent's admin token must not leak into the job.
        r#"cat "$DA_INPUTS_DIR/greeting.txt"; test -z "$DA_TOKEN" && echo done > "$DA_OUTPUT_DIR/result.txt""#,
    ]);
    assert!(run.status.success(), "{run:?}");
    assert_eq!(String::from_utf8_lossy(&run.stdout), "hello pool\n");

    let job = cli(&["job", "j1", "--json"]);
    let job: serde_json::Value = serde_json::from_slice(&job.stdout).unwrap();
    assert_eq!(job["state"], "succeeded");
    let hash = job["tasks"][0]["outputs"][0]["hash"]
        .as_str()
        .unwrap()
        .to_owned();
    let fetched = dir.join("result.txt");
    let get = cli(&["blob", "get", &hash, "-o", fetched.to_str().unwrap()]);
    assert!(get.status.success(), "{get:?}");
    assert_eq!(std::fs::read_to_string(&fetched).unwrap(), "done\n");

    let failing = cli(&["run", "--", "sh", "-c", "exit 7"]);
    assert_eq!(
        failing.status.code(),
        Some(7),
        "the job's exit code is passed through"
    );

    let wrong = Command::new(BIN)
        .args([
            "nodes",
            "--server",
            &server,
            "--token",
            &format!("da1.{}.x", "0".repeat(32)),
        ])
        .env_remove("DA_TOKEN")
        .output()
        .unwrap();
    assert!(!wrong.status.success());
    assert!(String::from_utf8_lossy(&wrong.stderr).contains("does not match this token"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn two_workers_stage_shared_inputs_and_only_their_own_dataset_shards() {
    let dir = scratch("shards");
    let (_control, server, token) = start_pool(&dir);
    let _first = start_agent(&dir, &server, &token, "box1");
    let _second = start_agent(&dir, &server, &token, "box2");
    let cli = |args: &[&str]| pool_cli(&server, &token, args);
    wait_for("both workers", Duration::from_secs(20), || {
        String::from_utf8_lossy(&cli(&["nodes"]).stdout).contains("2 of 2 nodes ready")
    });

    let shared = dir.join("common");
    std::fs::write(&shared, "shared").unwrap();
    let files: Vec<_> = (0..12)
        .map(|i| {
            let path = dir.join(format!("shard-{i}"));
            std::fs::write(&path, format!("shard {i}")).unwrap();
            path
        })
        .collect();
    let mut create = vec!["dataset", "create", "corpus"];
    create.extend(files.iter().map(|p| p.to_str().unwrap()));
    assert!(cli(&create).status.success());
    let run = cli(&[
        "run",
        "--replicas",
        "2",
        "--memory-mb",
        "1",
        "--dataset",
        "corpus",
        "--partition",
        "-i",
        shared.to_str().unwrap(),
        "--",
        "sh",
        "-ec",
        r#"
            test "$(cat "$DA_INPUTS_DIR/common")" = shared
            set -- "$DA_INPUTS_DIR"/corpus/*
            test "$#" = 6
            for path do
                i=${path##*-}
                test "$((i % 2))" = "$DA_RANK"
                test "$(cat "$path")" = "shard $i"
            done
            if [ "$DA_RANK" = 0 ]; then echo result > "$DA_OUTPUT_DIR/result.txt"; fi
            echo "$DA_NODE $DA_RANK $DA_WORLD_SIZE"
        "#,
    ]);
    assert!(run.status.success(), "{run:?}");
    let stdout = String::from_utf8(run.stdout).unwrap();
    assert!(
        stdout.contains("[r0] box1 0 2") && stdout.contains("[r1] box2 1 2"),
        "{stdout}"
    );
    let detail = cli(&["job", "j1"]);
    assert!(detail.status.success());
    let rows: Vec<_> = String::from_utf8(detail.stdout)
        .unwrap()
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    assert!(
        rows.iter()
            .any(|r| r == "TASK NODE GPUS STATE EXIT OUTPUTS NOTE")
    );
    assert!(rows.iter().any(|r| r == "j1.1.0 box1 - succeeded 0 1"));
    assert!(rows.iter().any(|r| r == "j1.1.1 box2 - succeeded 0 0"));
    wait_for(
        "both workers' cache and capacity reports",
        Duration::from_secs(10),
        || {
            let nodes: Vec<serde_json::Value> =
                serde_json::from_slice(&cli(&["nodes", "--json"]).stdout).unwrap();
            nodes.len() == 2
                && nodes.iter().all(|n| {
                    let count = if n["name"] == "box1" { 8 } else { 7 };
                    n["alloc_cpus"] == 0 && n["stats"]["cache_blobs"] == count
                })
        },
    );
    std::fs::remove_dir_all(dir).unwrap();
}
