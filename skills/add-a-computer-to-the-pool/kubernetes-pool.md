# Advanced: a Kubernetes (K3s) pool

This is a **different** pool from the one `dist-alloc` runs: the computers become a K3s
Kubernetes cluster with a web dashboard (Headlamp), NVIDIA GPU scheduling for containers,
and an optional private WireGuard VPN between sites. Offer it only to people who ask for
Kubernetes, containers or a VPN; everyone else is better served by `dist-alloc`.

What it needs, and what to tell the person before starting:

- Linux with systemd on every computer, **administrator rights** on each, and internet
  access to download K3s. The scripts change system services and firewall rules.
- A copy of the scripts on each computer:
  `git clone https://github.com/Sovra-AI/sovra-compute-pool ~/sovra-compute-pool`.
- Every administrator command shows an approval card and asks for the administrator
  password itself. Never ask for the password in chat.
- The printed GUI token, `deploy/join-device.sh` and every invite code are admin secrets.
  Do not repeat them in chat beyond showing the person where they are.

Run the scripts from that folder (`cd ~/sovra-compute-pool &&` in front of each command).
The reference below is the repository README's section on this setup.

## Reference

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

