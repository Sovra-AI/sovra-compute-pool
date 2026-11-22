# Sovra Compute Pool

[![CI](https://github.com/Sovra-AI/sovra-compute-pool/actions/workflows/ci.yml/badge.svg)](https://github.com/Sovra-AI/sovra-compute-pool/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Pool the computers you already have — their processors, memory and graphics cards — and
run big jobs, such as training AI models, across all of them at once.**

Most homes and offices have several computers that sit idle most of the day: a desktop
with a good graphics card, an old laptop, a workstation under a desk. Sovra Compute Pool
joins them into one pool. You hand the pool a job ("train this model on two graphics
cards"), and it finds computers with room, copies the files they need, runs the job, and
brings the results back. When a computer switches off, its work moves to another one.

It is one small program, `dist-alloc`, that each computer runs. Everything stays on your
own network: computers talk to each other over encrypted connections that only members of
your pool can open, and no cloud service is involved.

## Use it from Sovra (recommended)

This repository is an official [Sovra](https://github.com/Sovra-AI/SovraHarness) plugin.
Open **Plugins** in Sovra and install it from the official plugins with one click (or
paste `Sovra-AI/sovra-compute-pool` into **Add from GitHub**). Then just ask, in your own words:

| Ask Sovra… | What happens |
| --- | --- |
| "Turn this computer into the main computer of a pool so my other computers can help with big jobs" | **Start a computer pool**: installs `dist-alloc`, starts the pool in the background, adds this computer |
| "Add my gaming PC to my computer pool so its graphics card can help" | **Add a computer to my pool**: makes an invite file here, joins the other computer with it |
| "Run my train.py on two of the pool's graphics cards and bring back the trained model" | **Run a job on my computer pool**: sends the files, runs the job, fetches the results |
| "How is my computer pool doing, and is my training job finished yet?" | **Check on my computer pool**: computers, jobs, logs and fixes for common problems |

Sovra runs each command through its terminal tool, so you see and approve every command
before it runs. The skills live in [`skills/`](skills); nothing in this repository runs
by itself when the plugin is installed.

The same skills work in Claude Code:

```text
/plugin marketplace add Sovra-AI/sovra-compute-pool
/plugin install sovra-compute-pool@sovra-compute-pool
```

## Requirements

* Linux (x86_64 or arm64) with systemd on every computer in the pool.
* For graphics cards: NVIDIA cards with their driver installed (`nvidia-smi` works).
* The computers can reach each other on TCP ports 7443 (main computer) and 7444 (every
  computer).
* Whatever your jobs need (Python, PyTorch, …) installed on the computers that run them:
  jobs run as ordinary programs, not containers.

## Install by hand

Download the archive for your computer from the
[latest release](https://github.com/Sovra-AI/sovra-compute-pool/releases/latest), check it,
and put the program on your `PATH`:

```sh
VERSION=0.1.0 TARGET=x86_64-unknown-linux-gnu   # or aarch64-unknown-linux-gnu
curl -fsSLO https://github.com/Sovra-AI/sovra-compute-pool/releases/download/v$VERSION/dist-alloc-$VERSION-$TARGET.tar.gz
curl -fsSLO https://github.com/Sovra-AI/sovra-compute-pool/releases/download/v$VERSION/dist-alloc-$VERSION-$TARGET.tar.gz.sha256
sha256sum -c dist-alloc-$VERSION-$TARGET.tar.gz.sha256
tar -xzf dist-alloc-$VERSION-$TARGET.tar.gz
mkdir -p ~/.local/bin && install -m 0755 dist-alloc-$VERSION-$TARGET/dist-alloc ~/.local/bin/
```

Or build it with Rust: `cargo install --locked --git https://github.com/Sovra-AI/sovra-compute-pool`.

Then, as yourself (no administrator rights needed):

```sh
# On the main computer: start the pool in the background and offer this computer to it.
dist-alloc service install server
dist-alloc service install agent
dist-alloc token --write ~/pool-invite.txt     # a secret: carry it to the next computer

# On every other computer, with the invite file copied over:
dist-alloc service install agent --invite ~/pool-invite.txt

# From any computer in the pool:
dist-alloc nodes
dist-alloc run --gpus 1 -- nvidia-smi
```

The services are systemd user services that start again after a reboot. Run
`loginctl enable-linger $USER` if they should keep running while you are logged out.
`dist-alloc service uninstall agent|server` removes them.

## Commands

One binary for every role:

| Command | Role |
| :-- | :-- |
| `dist-alloc server` | Control plane: registry of devices, job queue, scheduler, logs, origin copy of uploaded data |
| `dist-alloc agent`  | Runs on every device you want in the pool; offers its resources and runs what it is given |
| `dist-alloc <cmd>`  | Operator CLI (`nodes`, `run`, `submit`, `jobs`, `logs`, `dataset`, `blob`, ...) |
| `dist-alloc service` | Installs the server or agent as a systemd user service |
| `dist-alloc token`  | Prints the join command, or writes an invite file with `--write` |

The CLI finds its pool by itself on the main computer and on any computer whose agent was
installed with `dist-alloc service`; elsewhere pass `--server`/`--token` (or set
`DA_SERVER`/`DA_TOKEN`). `dist-alloc <command> --help` documents every option. Daemon logs
go to stderr (`journalctl --user -u dist-alloc-agent`); set `RUST_LOG=debug` for more.

Open TCP 7443 on the main computer (control) and 7444 on each device (peer data transfer).

## How it works

```text
            CLI ──────────────┐
                              ▼
                   ┌────────────────────┐   state.json, logs/, kept/ (origin blobs)
                   │   control plane    │
                   │ registry·scheduler │
                   └────────▲───────────┘
        register + heartbeat│(every 2 s, always dialled by the node)
      ┌─────────────────────┼─────────────────────┐
┌─────┴──────┐        ┌─────┴──────┐        ┌─────┴──────┐
│  agent A   │◄──────►│  agent B   │◄──────►│  agent C   │   blobs by BLAKE3 hash,
│ cache·tasks│        │ cache·tasks│        │ cache·tasks│   peer to peer
└────────────┘        └────────────┘        └────────────┘
```

* **Joining.** The agent detects CPUs, RAM and NVIDIA GPUs (`nvidia-smi`), registers, and
  heartbeats live load, GPU utilisation, free disk and its blob inventory. Nodes only make
  outbound connections for control, so NAT and firewalls on the device side do not matter.
  `--cpus`, `--memory-mb` and `--no-gpus` let a workstation offer only part of itself.
* **Scheduling.** Jobs ask for `cpus`, `memory`, `gpus` (optionally a minimum VRAM) and a
  number of `replicas`. Replicas are placed as a gang: all or none. Among nodes that fit, the
  scheduler prefers the one already caching the most input bytes, then packs GPUs best-fit
  and keeps CPU-only work off GPU nodes. Higher `--priority` first; small jobs may run ahead
  of a large one that does not fit yet.
* **Execution.** A task is a process in its own process group with `CUDA_VISIBLE_DEVICES`
  set to exactly the GPUs it was allocated (none, if it asked for none). It gets `DA_RANK`,
  `DA_WORLD_SIZE`, `DA_PEER_HOSTS`, `DA_MASTER_ADDR` for distributed-training rendezvous,
  `DA_INPUTS_DIR` with its staged inputs and `DA_OUTPUT_DIR`, whose files are published as
  job outputs when it exits.
* **Data.** Every file is a blob named by its BLAKE3 hash: immutable, deduplicated, verified
  on every transfer. Before a task starts, its node pulls missing inputs from other nodes
  that have them (the control plane's copy is the last resort), so the task's hot path is
  plain local files. Each node keeps an LRU cache (`--cache-max-gb`, `--min-free-gb`); blobs
  in use by a task and blobs the node itself produced are never evicted.
* **Failure.** A node silent for 15 s is marked lost and its jobs are requeued elsewhere
  (this does not consume `--retries`; non-zero exits do). If one replica of a gang fails the
  rest are stopped. A task's GPUs are not handed to another job until its node confirms the
  process is gone. Tasks die with their agent. The control plane persists its state and can
  be restarted under running agents.
* **Security.** The control plane generates a self-signed certificate; the join token is
  `da1.<certificate fingerprint>.<secret>`. Every connection (CLI, agent→server,
  agent→agent) is TLS pinned to that certificate, so the secret is never sent to anything
  but the real cluster. **The token is cluster-admin: it lets its holder run commands on
  every node.** Treat it like an SSH key. It is never printed into a service's log (only to
  an interactive terminal), never passed to jobs, and an agent service keeps it in an
  owner-only `~/.config/dist-alloc/agent.env`, not on its command line.

## Relation to the original proposal

[The original proposal](docs/original-proposal.md) was K3s + Cilium/WireGuard + KubeRay + PostgreSQL + Iroh,
with a custom cache agent. This project keeps its ideas — content-addressed shards, P2P
staging off the hot path, locality-aware gang scheduling, encrypted transport, metadata kept
out of the data path — and implements them directly instead of operating five systems:

| Proposal | Here |
| :-- | :-- |
| K3s + KubeRay | built-in registry and gang scheduler |
| Cilium + WireGuard | pinned TLS on every connection |
| Iroh / private IPFS | BLAKE3 blob store with peer-to-peer fetch |
| PostgreSQL metadata + cache inventory | control-plane state + heartbeat inventory |
| cache-gc DaemonSet | LRU eviction inside the agent, pins known first-hand |

## Limits

* Linux only; GPU detection is NVIDIA only.
* Tasks are host processes, not containers: the software a job needs must be installed on
  the nodes (or shipped as inputs). CPU and memory requests are admission control, not
  enforced limits; GPU isolation is by `CUDA_VISIBLE_DEVICES`.
* One control plane, no HA. If it is down, running tasks continue and report when it returns.
* Outputs are collected when a task exits and live on the node that produced them until
  fetched; a mid-run checkpoint is lost if that node dies.
* Blob transfers restart from zero if interrupted.

## Alternative: a Kubernetes pool (`deploy/`)

For people who want containers, a web dashboard and a private VPN between sites, `deploy/`
holds a separate setup that turns the computers into a K3s cluster instead. It needs
administrator rights on every computer and is not connected to `dist-alloc`.

```sh
# On the machine that hosts the control plane (it also offers its own CPUs/GPUs):
sudo deploy/node.sh server
#   -> prints the GUI address (https://<ip>/), a login token, and writes deploy/join-device.sh

# For every device to add (Linux + systemd; NVIDIA driver installed if it has a GPU):
sudo deploy/invite.sh <device-name>        # here: prints a one-line invite code (a secret)
git clone https://github.com/Sovra-AI/sovra-compute-pool && cd sovra-compute-pool       # on the device
sudo deploy/join.sh <invite-code>          # on the device: joins (and connects the pool VPN)
```

The invite code carries the cluster's join secret and, when the pool uses its VPN, that
device's VPN settings, so nothing secret has to live in the repo. Without git on the
device, copy `deploy/join-device.sh` (plus `deploy/vpn/<device>.conf` when using the VPN)
to it and run `sudo bash join-device.sh [<device>.conf]` instead.

After that one-time join, the device runs `k3s-agent` as a service: every time it boots it
registers with the control plane by itself, and its CPUs, memory and GPUs are schedulable.

```sh
export KUBECONFIG=~/.kube/config           # K3s's kubectl ignores this file unless told
kubectl get nodes -o custom-columns='NODE:.metadata.name,CPU:.status.allocatable.cpu,MEM:.status.allocatable.memory,GPU:.status.allocatable.nvidia\.com/gpu'
kubectl apply -f deploy/gpu-test.yaml      # runs nvidia-smi on a pooled GPU
kubectl logs job/gpu-test
```

### Pool VPN (optional)

`deploy/vpn.sh` creates a private WireGuard network hosted on the control-plane machine and
a settings file per device. Use it when devices are on other subnets, behind NAT or
off-site, or when the control plane's own address is not stable: every machine gets a fixed
pool address (`10.77.0.x`) and the cluster runs over it.

```sh
sudo deploy/vpn.sh init                      # before `node.sh server`; this machine becomes 10.77.0.1
sudo deploy/node.sh server                   # detects the VPN and builds the cluster on it
sudo deploy/invite.sh gpu-box                # creates gpu-box's VPN settings and prints its invite
sudo deploy/join.sh <invite-code>            # on the device: connects the VPN, then joins

sudo deploy/vpn.sh list                      # devices, addresses, last contact
sudo deploy/vpn.sh remove gpu-box            # revoke
```

* Devices dial the control plane at its LAN address on UDP 51821 by default. For off-site
  devices pass a public address — `vpn.sh init <host:port>` for all devices, or
  `vpn.sh add <device> <host:port>` for one — and forward that UDP port on your router.
* Devices reach each other through the control plane (hub and spoke), so its uplink carries
  device-to-device traffic.
* Pod traffic uses the VPN directly. `pool-vpn-sync.service` on the control plane tells
  WireGuard which device owns which pod range as nodes join and leave (within ~15 s).
  Nothing is encapsulated a second time, which also keeps it clear of host firewalls that
  drop kernel-generated tunnel packets to private addresses.
* Decide before installing the cluster: a cluster built on LAN addresses cannot be switched
  to the VPN without reinstalling its nodes.
* Each `.conf` holds that device's private key; `deploy/vpn/` is git-ignored.

What `deploy/node.sh` sets up:

| Piece | Choice |
| :-- | :-- |
| Kubernetes | K3s (single binary, `stable` channel); agents are pinned to the server's version |
| GUI | Headlamp at `https://<control-plane-ip>/` via the bundled Traefik ingress; log in with the printed token |
| Device networking | With the pool VPN: pods are routed straight over it (Flannel `host-gw`), no second tunnel. Without it: Flannel `wireguard-native` encrypts pod traffic between devices |
| GPUs | NVIDIA container toolkit + device plugin; a pod asks for `nvidia.com/gpu: 1` and gets one |
| Firewall | Opens the needed ports in `ufw`/`firewalld` if one is active |
| Disk pressure | Absolute eviction floors (5 GiB) instead of kubelet's 10%-free default |

Things to know:

* `deploy/join-device.sh` and the GUI token are admin secrets. The join file is git-ignored.
* The control plane's LAN address is baked into the join file and certificates. Give that
  machine a fixed address (DHCP reservation), or devices lose it when the address changes.
* GPU nodes run all containers under the NVIDIA runtime so GPU pods need no extra settings.
  The trade-off: an image that sets `NVIDIA_VISIBLE_DEVICES=all` itself can see GPUs it did
  not request.
* Undo: `sudo k3s-uninstall.sh` (control plane) or `sudo k3s-agent-uninstall.sh` (device).

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test                 # unit tests and an end-to-end pool on loopback
cargo deny check           # licences and advisories
shellcheck deploy/*.sh
```

CI runs all of these, plus Sovra's own plugin check (`check_repository` from
[SovraHarness](https://github.com/Sovra-AI/SovraHarness)) so the plugin always installs
with every skill. Pushing a `v*` tag builds the Linux release archives. See
[CHANGELOG.md](CHANGELOG.md) for what changed.

## License

[MIT](LICENSE) © 2026 Sovra-AI and contributors.
