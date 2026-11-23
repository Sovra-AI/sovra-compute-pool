# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

- The run-job skill preserves supplied input files and checks the recorded outputs
  for each rank before reporting results. The job detail table shows how many files
  each task produced.

- Workers reuse cache counts between inventory changes and share four concurrent input
  downloads across all jobs on a machine. A source that stops sending data for 30 seconds
  gives way to the next available copy.
- Idle heartbeats skip state-file writes and job-history scans. Job listings and gang
  placement do less work for pools with long histories or large shared datasets.
- Cancelling during delivery holds the reservation until the worker accounts for the
  task. Among equally local, equally packed nodes, GPU jobs prefer the smaller adequate
  cards.

- Every skill now ends with a plain message saying what happened and what to do next,
  instead of stopping on a command's output. Setting up a pool finishes with the pool's
  real size and three next steps (add a computer, run a job, check on it), and explains
  that Sovra's Training page is separate from the pool if asked.

- Add a cleanup guide for retiring one worker or a whole pool, including services,
  invitations, job results, network settings and the optional Kubernetes setup.
- Agent uninstall also removes saved join credentials left by an interrupted installation.

## [0.1.0] - 2026-11-22

First public release, as an official Sovra plugin.

### Added

- Sovra and Claude Code plugin: four skills — **Start a computer pool**, **Add a computer
  to my pool**, **Run a job on my computer pool** and **Check on my computer pool** — that
  set up and drive the pool through the assistant's approved terminal commands.
- `dist-alloc service install|uninstall server|agent`: run the control plane or an agent as
  a systemd user service that survives logout (with lingering) and reboots.
- `dist-alloc token --write FILE [--host ADDRESS]`: an owner-only invite file for joining a
  device without typing or pasting the token. `service install agent --invite FILE` uses it.
- The CLI finds the pool by itself on any computer whose agent was installed as a service.
- Release archives for x86_64 and arm64 Linux, each with a SHA-256 checksum.
- MIT licence, CI (format, lints, tests, cargo-deny, shellcheck, Sovra plugin check).

### Changed

- Logs use `tracing` (filter with `RUST_LOG`) and carry structured fields.
- The control plane prints the join command only to an interactive terminal, never into a
  service log; `dist-alloc token` shows it on demand.
- Job processes no longer inherit the agent's `DA_TOKEN`.
- Join addresses skip container and VM bridges (`docker0`, `virbr0`, …) and down links.

### Fixed

- An unreadable `state.json` or token file, or a half-present cluster identity, now stops
  the control plane with a clear error instead of silently replacing them (which lost
  saved jobs or locked every device out).
- Bare and bracketed IPv6 control-plane addresses (`::1`, `[fe80::1]`) resolve correctly.
- A job output copied across filesystems can no longer appear truncated after a crash.
- The agent's per-blob fetch locks no longer accumulate forever.
- A panic in one request no longer poisons shared state for the rest of the process.

[Unreleased]: https://github.com/Sovra-AI/sovra-compute-pool/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Sovra-AI/sovra-compute-pool/releases/tag/v0.1.0
