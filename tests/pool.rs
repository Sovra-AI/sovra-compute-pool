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

fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("da-e2e-{}", std::process::id()));
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

#[test]
fn a_job_runs_on_a_joined_device_and_its_output_comes_back() {
    let dir = scratch();
    let server = format!("127.0.0.1:{}", free_port());
    let server_dir = dir.join("server");
    let _control = daemon(
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
    let token = std::fs::read_to_string(&token_file).unwrap();

    let agent_dir = dir.join("agent");
    let _agent = daemon(
        &[
            "agent",
            "--server",
            &server,
            "--token",
            &token,
            "--name",
            "box1",
            "--no-gpus",
            "--blob-port",
            "0",
            "--data-dir",
            agent_dir.to_str().unwrap(),
        ],
        &dir.join("agent.log"),
    );
    let cli = |args: &[&str]| -> Output {
        Command::new(BIN)
            .args(args)
            .env("DA_SERVER", &server)
            .env("DA_TOKEN", &token)
            .output()
            .unwrap()
    };
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
