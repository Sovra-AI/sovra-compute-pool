#!/usr/bin/env bash
# A private WireGuard network for the pool, hosted on the control-plane machine.
#
# Devices that are on another subnet, behind NAT, or off-site connect through it, and every
# machine gets a fixed pool address (10.77.0.x) that does not change when its real one does.
#
#   sudo deploy/vpn.sh init [host:port]      create the VPN here (run before `node.sh server`)
#   sudo deploy/vpn.sh add <device> [host:port]   create settings for one device -> deploy/vpn/<device>.conf
#   sudo deploy/vpn.sh list                  devices, their pool addresses, last contact
#   sudo deploy/vpn.sh remove <device>       revoke a device
#   sudo deploy/vpn.sh sync                  route each node's pods through its VPN peer (runs
#                                            by itself every few seconds as pool-vpn-sync.service)
#
# host:port is where devices reach this machine (default: its LAN address, port 51821). For
# off-site devices use your public address or DNS name and forward UDP 51821 to this machine.
#
# Optional environment: VPN_NET (first three octets, default 10.77.0), VPN_PORT (51821).
set -euo pipefail

IF=pool0
NET=${VPN_NET:-10.77.0}
PORT=${VPN_PORT:-51821}
# Pod addresses of the cluster. They travel over the VPN directly (no second tunnel), so
# every device routes this range into the VPN and the hub learns which device owns which part.
POD_CIDR=${POOL_POD_CIDR:-10.42.0.0/16}
WG_DIR=${WG_DIR:-/etc/wireguard}
CONF=$WG_DIR/$IF.conf
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
OUT=${VPN_OUT:-$HERE/vpn}
# VPN_APPLY=0 only writes files and touches nothing on the system (used to test this script).
APPLY=${VPN_APPLY:-1}

log() { printf '\033[1m==> %s\033[0m\n' "$*"; }
die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 1; }

need_root() { [[ $APPLY != 1 || $EUID -eq 0 ]] || die "must run as root (use sudo)"; }
need_pool() { [[ -e $CONF ]] || die "no pool VPN on this machine yet: run  sudo $0 init"; }

lan_ip() {
  local dev
  dev=$(ip -4 route show default table main | awk '{for (i = 1; i < NF; i++) if ($i == "dev") {print $(i + 1); exit}}')
  [[ -n $dev ]] || return 1
  ip -4 -o addr show dev "$dev" scope global | awk '{split($4, a, "/"); print a[1]; exit}'
}

ensure_tools() {
  command -v wg >/dev/null && command -v wg-quick >/dev/null && return
  [[ $APPLY == 1 ]] || die "wireguard-tools is not installed"
  log "installing wireguard-tools"
  if command -v apt-get >/dev/null; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y wireguard-tools
  elif command -v dnf >/dev/null; then
    dnf install -y wireguard-tools
  else
    die "install wireguard-tools for this distribution, then re-run"
  fi
}

# Push the config file's peers into the running interface without dropping connections.
reload() {
  [[ $APPLY == 1 ]] || return 0
  if ip link show "$IF" >/dev/null 2>&1; then
    wg syncconf "$IF" <(wg-quick strip "$IF")
    cmd_sync || true # syncconf reset the peers to what the file says; put pod routes back
  fi
}

# Each cluster node's pods live in their own slice of POD_CIDR, assigned by Kubernetes when
# the node joins. WireGuard only carries a packet to or from a peer if that peer is allowed
# the address, so tell it: device X = its VPN address + its pod slice. Idempotent; prints
# only what it changes.
cmd_sync() {
  [[ -e $CONF ]] || return 0
  ip link show "$IF" >/dev/null 2>&1 || return 0
  command -v k3s >/dev/null || return 0
  local nodes
  nodes=$(k3s kubectl get nodes \
    -o jsonpath='{range .items[*]}{.status.addresses[?(@.type=="InternalIP")].address}{" "}{.spec.podCIDR}{"\n"}{end}' 2>/dev/null) || return 0
  local live
  live=$(wg show "$IF" allowed-ips)
  awk '
    /^PublicKey = /  { key = $3 }
    /^AllowedIPs = / { split($3, a, "/"); print key, a[1] }
  ' "$CONF" | while read -r key addr; do
    local cidr want have
    cidr=$(awk -v a="$addr" '$1 == a && $2 ~ /^[0-9.]+\/[0-9]+$/ { print $2; exit }' <<<"$nodes")
    want="$addr/32${cidr:+,$cidr}"
    have=$(awk -v k="$key" '$1 == k { for (i = 2; i <= NF; i++) print $i }' <<<"$live" | sort | paste -sd, -)
    if [[ $(tr ',' '\n' <<<"$want" | sort | paste -sd, -) != "$have" ]]; then
      wg set "$IF" peer "$key" allowed-ips "$want"
      echo "pool vpn: $addr now carries ${cidr:-no pod range}"
    fi
  done
}

cmd_watch() {
  while true; do
    cmd_sync || true
    sleep 15
  done
}

# Runs `watch` as a service from a copy outside the repo, so moving the repo cannot break it.
cmd_install_sync() {
  need_root
  need_pool
  [[ $APPLY == 1 ]] || return 0
  install -m 755 "${BASH_SOURCE[0]}" /usr/local/sbin/pool-vpn
  cat > /etc/systemd/system/pool-vpn-sync.service <<UNIT
[Unit]
Description=Route each cluster node's pods through its pool VPN peer
After=wg-quick@$IF.service k3s.service

[Service]
ExecStart=/usr/local/sbin/pool-vpn watch
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable pool-vpn-sync.service >/dev/null 2>&1
  systemctl restart pool-vpn-sync.service
}

endpoint() { sed -n 's/^# endpoint: //p' "$CONF" | head -1; }

cmd_init() {
  need_root
  ensure_tools
  [[ ! -e $CONF ]] || die "the pool VPN already exists here ($CONF); use  $0 add <device>"
  if ip -4 route show | grep -q "^${NET//./\\.}\."; then
    die "$NET.0/24 is already routed on this machine; choose another range with VPN_NET=10.x.y"
  fi
  local ep=${1:-}
  if [[ -z $ep ]]; then
    ep="$(lan_ip || true):$PORT"
    [[ $ep != ":$PORT" ]] || die "could not detect this machine's address; pass it:  $0 init <host:port>"
  fi
  [[ $ep == *:* ]] || ep="$ep:$PORT"

  umask 077
  mkdir -p "$WG_DIR"
  {
    echo "# dist-alloc pool VPN (hub). Managed by deploy/vpn.sh; devices are appended below."
    echo "# endpoint: $ep"
    echo "[Interface]"
    echo "Address = $NET.1/24"
    echo "ListenPort = ${ep##*:}"
    echo "PrivateKey = $(wg genkey)"
  } > "$CONF"

  if [[ $APPLY == 1 ]]; then
    # Devices talk to each other through this machine, so it must forward between them.
    echo "net.ipv4.ip_forward = 1" > /etc/sysctl.d/99-dist-alloc-vpn.conf
    sysctl -q -w net.ipv4.ip_forward=1
    if command -v ufw >/dev/null && ufw status | grep -q '^Status: active'; then
      ufw allow "${ep##*:}/udp" comment 'pool vpn' >/dev/null
      ufw allow in on "$IF" comment 'pool vpn members' >/dev/null
      ufw route allow in on "$IF" out on "$IF" comment 'pool vpn device-to-device' >/dev/null
    elif command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
      firewall-cmd --permanent --add-port="${ep##*:}/udp" >/dev/null
      firewall-cmd --permanent --zone=trusted --add-interface="$IF" >/dev/null
      firewall-cmd --reload >/dev/null
    fi
    systemctl enable --now "wg-quick@$IF" >/dev/null
    cmd_install_sync
  fi
  log "pool VPN created: this machine is $NET.1, devices connect to $ep"
  echo "    next:  sudo $0 add <device-name>      (one per device)"
  echo "    then:  sudo $HERE/node.sh server      (the cluster will run over the VPN)"
}

cmd_add() {
  need_root
  need_pool
  local name=${1:-} ep=${2:-}
  [[ $name =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] || die "device name must be lowercase letters, digits and dashes"
  ! grep -qx "# peer: $name" "$CONF" || die "$name already has settings ($OUT/$name.conf); remove it first to reissue"
  [[ -n $ep ]] || ep=$(endpoint)
  [[ $ep == *:* ]] || ep="$ep:$PORT"

  local host
  for host in $(seq 2 254); do
    grep -q "^AllowedIPs = ${NET//./\\.}\.$host/32$" "$CONF" || break
    [[ $host -lt 254 ]] || die "the pool VPN is full (253 devices)"
  done

  umask 077
  local key psk
  key=$(wg genkey)
  psk=$(wg genpsk)
  {
    echo
    echo "# peer: $name"
    echo "[Peer]"
    echo "PublicKey = $(wg pubkey <<<"$key")"
    echo "PresharedKey = $psk"
    echo "AllowedIPs = $NET.$host/32"
  } >> "$CONF"

  mkdir -p "$OUT"
  {
    echo "# dist-alloc pool VPN settings for device '$name' ($NET.$host). Secret: one device only."
    echo "[Interface]"
    echo "Address = $NET.$host/24"
    echo "PrivateKey = $key"
    echo
    echo "[Peer]"
    echo "PublicKey = $(sed -n 's/^PrivateKey = //p' "$CONF" | wg pubkey)"
    echo "PresharedKey = $psk"
    echo "Endpoint = $ep"
    echo "AllowedIPs = $NET.0/24, $POD_CIDR"
    echo "PersistentKeepalive = 25"
  } > "$OUT/$name.conf"
  if [[ -n ${SUDO_USER:-} ]]; then chown -R "$SUDO_USER" "$OUT"; fi
  reload

  log "$name is $NET.$host — settings written to $OUT/$name.conf"
  echo "    join it to the pool (VPN + cluster in one step):"
  echo "        scp $HERE/join-device.sh $OUT/$name.conf <user>@<device>:"
  echo "        ssh -t <user>@<device> sudo bash join-device.sh $name.conf"
  echo "    or VPN only:  copy it to /etc/wireguard/$IF.conf on the device, then"
  echo "        sudo systemctl enable --now wg-quick@$IF"
}

cmd_remove() {
  need_root
  need_pool
  local name=${1:-}
  grep -qx "# peer: $name" "$CONF" || die "no device named '$name' (see  $0 list)"
  local tmp
  tmp=$(mktemp "$CONF.XXXXXX")
  awk -v name="$name" '
    $0 == "# peer: " name { skip = 1; next }
    skip && /^# peer: /   { skip = 0 }
    !skip                 { print }
  ' "$CONF" | cat -s > "$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$CONF"
  if [[ -e $OUT/$name.conf ]]; then mv "$OUT/$name.conf" "$OUT/$name.conf.revoked"; fi
  reload
  log "$name can no longer connect"
  echo "    if it was a cluster node, also run:  kubectl delete node <node-name>"
}

cmd_list() {
  need_pool
  local handshakes=""
  if [[ $APPLY == 1 && $EUID -eq 0 ]] && ip link show "$IF" >/dev/null 2>&1; then
    handshakes=$(wg show "$IF" latest-handshakes)
  fi
  printf '%-24s %-14s %s\n' DEVICE ADDRESS "LAST CONTACT"
  printf '%-24s %-14s %s\n' "(this machine)" "$NET.1" "-"
  awk '
    /^# peer: /      { name = substr($0, 9) }
    /^PublicKey = /  { key = $3 }
    /^AllowedIPs = / { split($3, a, "/"); print name, a[1], key }
  ' "$CONF" | while read -r name addr key; do
    local seen="unknown (run with sudo)" at
    if [[ -n $handshakes ]]; then
      at=$(awk -v k="$key" '$1 == k { print $2 }' <<<"$handshakes")
      if [[ -z $at || $at == 0 ]]; then seen="never"; else seen="$(($(date +%s) - at))s ago"; fi
    fi
    printf '%-24s %-14s %s\n' "$name" "$addr" "$seen"
  done
}

case ${1:-} in
  init) shift; cmd_init "$@" ;;
  add) shift; cmd_add "$@" ;;
  remove) shift; cmd_remove "$@" ;;
  list) cmd_list ;;
  sync) need_root; cmd_sync ;;
  watch) need_root; cmd_watch ;;
  install-sync) cmd_install_sync ;;
  *) usage ;;
esac
