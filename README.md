# dist-alloc

Pool the CPUs, memory and GPUs of ordinary networked machines and run jobs on them.

There are two ways to run the pool in this repo:

* **Kubernetes control plane (`deploy/`)** — K3s with a web GUI; devices register themselves
  and appear as schedulable nodes. Described first.
* **Standalone `dist-alloc` binary (`src/`)** — a self-contained control plane, agent and
  CLI with no Kubernetes. Described from "Standalone binary" down. It is not wired into the
  Kubernetes setup.

## Kubernetes control plane

```sh
# On the machine that hosts the control plane (it also offers its own CPUs/GPUs):
sudo deploy/node.sh server
#   -> prints the GUI address (https://<ip>/), a login token, and writes deploy/join-device.sh

# For every device to add (Linux + systemd; NVIDIA driver installed if it has a GPU):
sudo deploy/invite.sh <device-name>        # here: prints a one-line invite code (a secret)
git clone <this repo> && cd <repo>         # on the device
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

## Standalone binary

One static binary, three roles:

| Command | Role |
| :-- | :-- |
| `dist-alloc server` | Control plane: registry of devices, job queue, scheduler, logs, origin copy of uploaded data |
| `dist-alloc agent`  | Runs on every device you want in the pool; offers its resources and runs what it is given |
| `dist-alloc <cmd>`  | Operator CLI (`nodes`, `run`, `submit`, `jobs`, `logs`, `dataset`, `blob`, ...) |

## Quick start

```sh
cargo build --release            # binary: target/release/dist-alloc

# 1. On the machine that coordinates (it prints the join command):
dist-alloc server

# 2. On every device that should contribute (including the coordinator, if you want):
dist-alloc agent --server <coordinator-ip>:7443 --token da1.<...>

# 3. From the coordinator (or anywhere, with --server/--token or DA_SERVER/DA_TOKEN):
dist-alloc nodes                                   # what the pool has
dist-alloc run --gpus 1 -- nvidia-smi              # run on a GPU, stream output
dist-alloc dataset create corpus shards/*.bin      # content-addressed upload
dist-alloc run --replicas 4 --gpus 1 --dataset corpus --partition \
    -i train.py -- sh -c 'python3 $DA_INPUTS_DIR/train.py'
dist-alloc jobs ; dist-alloc job j3 ; dist-alloc logs -f j3 ; dist-alloc cancel j3
dist-alloc blob get <hash> -o model.bin            # pull a job output from wherever it is
```

Open TCP 7443 on the coordinator (control) and 7444 on each device (peer data transfer).
`dist-alloc token` reprints the join command.

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
  every node.** Treat it like an SSH key.

## Relation to `distml`

`distml` is the original proposal: K3s + Cilium/WireGuard + KubeRay + PostgreSQL + Iroh,
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
