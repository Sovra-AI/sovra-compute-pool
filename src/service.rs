//! Runs the control plane or the agent as a systemd user service, so the pool keeps going
//! after the terminal (or the assistant's command) that set it up has closed, and comes back
//! after a reboot.
//!
//! Secrets never go into the unit file or onto a command line, where `ps` and logs would
//! show them: an agent's server address and token live in an owner-only environment file
//! that the unit reads.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use crate::net::{self, Token};

/// Which half of the pool a service runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Role {
    Server,
    Agent,
}

impl Role {
    fn unit_name(self) -> &'static str {
        match self {
            Self::Server => "dist-alloc-server.service",
            Self::Agent => "dist-alloc-agent.service",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Server => "dist-alloc control plane (computer pool)",
            Self::Agent => "dist-alloc agent (offers this computer to the pool)",
        }
    }
}

/// `$XDG_CONFIG_HOME`, else `~/.config`.
fn config_home() -> Result<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .context("neither XDG_CONFIG_HOME nor HOME is set")
}

/// Owner-only file holding `DA_SERVER` and `DA_TOKEN` for this machine's agent. The CLI
/// reads it too, so every pool member can run `dist-alloc nodes` and friends.
pub fn agent_env_path() -> Result<PathBuf> {
    Ok(config_home()?.join("dist-alloc").join("agent.env"))
}

/// An invite: the two settings a device needs to join, in environment-file form.
pub fn invite(server: &str, token: &Token) -> String {
    format!("DA_SERVER={server}\nDA_TOKEN={token}\n")
}

/// Reads `DA_SERVER` and `DA_TOKEN` back out of an invite or agent environment file.
pub fn parse_invite(text: &str) -> Result<(String, Token)> {
    let mut server = None;
    let mut token = None;
    for line in text.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("DA_SERVER=") {
            server = Some(value.trim().to_owned());
        } else if let Some(value) = line.strip_prefix("DA_TOKEN=") {
            token = Some(Token::parse(value)?);
        }
    }
    match (server, token) {
        (Some(server), Some(token)) if !server.is_empty() => Ok((server, token)),
        _ => bail!("not a pool invite: it needs a DA_SERVER= line and a DA_TOKEN= line"),
    }
}

/// Quotes one argument for a systemd `ExecStart=` line.
fn quote(arg: &str) -> String {
    let plain = !arg.is_empty()
        && arg
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./:=@,+".contains(&b));
    if plain {
        arg.to_owned()
    } else {
        let escaped = arg
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$");
        format!("\"{escaped}\"")
    }
}

/// The unit file text. `env_file` is read by systemd, not baked in.
pub fn unit_file(role: Role, exe: &Path, args: &[String], env_file: Option<&Path>) -> String {
    let mut exec = quote(&exe.to_string_lossy());
    for arg in args {
        exec.push(' ');
        exec.push_str(&quote(arg));
    }
    let mut unit = String::new();
    let _ = writeln!(unit, "[Unit]");
    let _ = writeln!(unit, "Description={}", role.description());
    let _ = writeln!(
        unit,
        "Documentation=https://github.com/Sovra-AI/sovra-compute-pool"
    );
    let _ = writeln!(unit, "Wants=network-online.target");
    let _ = writeln!(unit, "After=network-online.target");
    let _ = writeln!(unit);
    let _ = writeln!(unit, "[Service]");
    if let Some(env_file) = env_file {
        let _ = writeln!(unit, "EnvironmentFile={}", env_file.display());
    }
    let _ = writeln!(unit, "ExecStart={exec}");
    let _ = writeln!(unit, "Restart=on-failure");
    let _ = writeln!(unit, "RestartSec=5");
    // Tasks are the agent's children: stopping the service must stop them too.
    let _ = writeln!(unit, "KillMode=control-group");
    let _ = writeln!(unit);
    let _ = writeln!(unit, "[Install]");
    let _ = writeln!(unit, "WantedBy=default.target");
    unit
}

fn systemctl(args: &[&str]) -> Result<()> {
    let status = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .context("running systemctl (is this a systemd-based Linux?)")?;
    if !status.success() {
        bail!("systemctl --user {} failed ({status})", args.join(" "));
    }
    Ok(())
}

fn unit_path(role: Role) -> Result<PathBuf> {
    Ok(config_home()?.join("systemd/user").join(role.unit_name()))
}

/// Installs, enables and starts the service. For the agent, `join` is where to connect.
pub fn install(role: Role, join: Option<(String, Token)>, args: &[String]) -> Result<()> {
    let exe = std::env::current_exe().context("finding this program's path")?;
    let mut command = vec![
        match role {
            Role::Server => "server",
            Role::Agent => "agent",
        }
        .to_owned(),
    ];
    command.extend(args.iter().cloned());
    let env_file = match (role, join) {
        (Role::Server, _) => None,
        (Role::Agent, Some((server, token))) => {
            let path = agent_env_path()?;
            std::fs::create_dir_all(path.parent().context("config path has a parent")?)?;
            net::write_private(&path, &invite(&server, &token))
                .with_context(|| format!("writing {}", path.display()))?;
            Some(path)
        }
        (Role::Agent, None) => bail!(
            "an agent needs to know which pool to join: pass --invite FILE (from `dist-alloc token \
             --write FILE` on the pool's main computer), or --server and --token"
        ),
    };
    let unit = unit_path(role)?;
    std::fs::create_dir_all(unit.parent().context("unit path has a parent")?)?;
    std::fs::write(&unit, unit_file(role, &exe, &command, env_file.as_deref()))
        .with_context(|| format!("writing {}", unit.display()))?;
    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", role.unit_name()])?;
    // `restart` starts a stopped unit, and makes a running one pick up new settings.
    systemctl(&["restart", role.unit_name()])?;
    println!(
        "{} is running and starts again after a reboot.",
        role.unit_name()
    );
    if !lingering() {
        println!(
            "note: it stops when you log out. To keep it running, run: loginctl enable-linger $USER"
        );
    }
    Ok(())
}

/// Stops, disables and removes the service. The agent's join settings go with it.
pub fn uninstall(role: Role) -> Result<()> {
    let unit = unit_path(role)?;
    let installed = unit
        .try_exists()
        .with_context(|| format!("checking {}", unit.display()))?;
    if installed {
        systemctl(&["disable", "--now", role.unit_name()])?;
    }
    // Install writes the agent credentials before writing its unit. An interrupted
    // install or an earlier partial uninstall may have left only the credentials.
    if role == Role::Agent {
        match std::fs::remove_file(agent_env_path()?) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
    }
    if installed {
        std::fs::remove_file(&unit).with_context(|| format!("removing {}", unit.display()))?;
        systemctl(&["daemon-reload"])?;
        println!("{} is stopped and removed.", role.unit_name());
    } else {
        println!("{} is not installed.", role.unit_name());
    }
    Ok(())
}

/// Whether this user's services keep running without a login session.
fn lingering() -> bool {
    let user = std::env::var("USER").unwrap_or_default();
    Command::new("loginctl")
        .args(["show-user", &user, "--property=Linger"])
        .output()
        .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).trim() == "Linger=yes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> Token {
        Token::parse(&format!("da1.{}.s3cret", "ab".repeat(16))).unwrap()
    }

    #[test]
    fn invites_round_trip_and_reject_anything_else() {
        let text = invite("10.0.0.2:7443", &token());
        let (server, parsed) = parse_invite(&text).unwrap();
        assert_eq!(server, "10.0.0.2:7443");
        assert_eq!(parsed.secret, "s3cret");
        assert!(parse_invite("DA_SERVER=10.0.0.2\n").is_err());
        assert!(parse_invite("DA_SERVER=\nDA_TOKEN=da1.x.y").is_err());
        assert!(parse_invite("hello").is_err());
    }

    #[test]
    fn unit_reads_secrets_from_the_env_file_and_quotes_odd_arguments() {
        let unit = unit_file(
            Role::Agent,
            Path::new("/home/me/.local/bin/dist-alloc"),
            &[
                "agent".into(),
                "--label".into(),
                "room=lab 2".into(),
                "--cpus".into(),
                "4".into(),
            ],
            Some(Path::new("/home/me/.config/dist-alloc/agent.env")),
        );
        assert!(unit.contains("EnvironmentFile=/home/me/.config/dist-alloc/agent.env\n"));
        assert!(unit.contains(
            "ExecStart=/home/me/.local/bin/dist-alloc agent --label \"room=lab 2\" --cpus 4\n"
        ));
        assert!(!unit.contains("s3cret") && !unit.contains("DA_TOKEN"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn systemd_specifiers_and_variables_are_escaped() {
        assert_eq!(quote("100%"), "\"100%%\"");
        assert_eq!(quote("$HOME"), "\"$$HOME\"");
        assert_eq!(quote("a\"b"), "\"a\\\"b\"");
        assert_eq!(quote(""), "\"\"");
        assert_eq!(quote("0.0.0.0:7443"), "0.0.0.0:7443");
    }
}
