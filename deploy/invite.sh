#!/usr/bin/env bash
# Create an invite code for one device. Run on the control-plane machine:
#
#   sudo deploy/invite.sh <device-name> [host:port]
#
# The code carries everything that device needs (cluster address and join secret, plus its
# own pool-VPN settings when the pool uses the VPN). On the device, in a clone of this repo:
#
#   sudo deploy/join.sh <code>
#
# Running it again for the same name reprints that device's invite. host:port overrides the
# address the device dials for the VPN (for devices outside this network).
set -euo pipefail
export PATH=$PATH:/usr/local/bin
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WG_DIR=${WG_DIR:-/etc/wireguard}
OUT=${VPN_OUT:-$HERE/vpn}
K3S_CONFIG=${K3S_CONFIG:-/etc/rancher/k3s/config.yaml}
TOKEN_FILE=${K3S_TOKEN_FILE:-/var/lib/rancher/k3s/server/node-token}
die() { echo "error: $*" >&2; exit 1; }

NAME=${1:-}
[[ $NAME =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] || die "usage: sudo $0 <device-name>   (lowercase letters, digits, dashes)"
[[ -r $TOKEN_FILE ]] || die "cannot read the cluster join secret: run with sudo, on the control-plane machine"
NODE_IP=$(awk '$1 == "node-ip:" {print $2}' "$K3S_CONFIG")
[[ -n $NODE_IP ]] || die "no control plane configured on this machine (run deploy/up.sh first)"
VERSION=${K3S_VERSION:-$(k3s --version | awk 'NR == 1 {print $3}')}

POOL_VPN=0
VPN_B64=""
if [[ -e $WG_DIR/pool0.conf ]]; then
  POOL_VPN=1
  if [[ ! -e $OUT/$NAME.conf ]]; then
    "$HERE/vpn.sh" add "$NAME" ${2:+"$2"} >/dev/null
  fi
  VPN_B64=$(base64 -w0 "$OUT/$NAME.conf")
fi

ENV_B64=$(printf '%s\n' \
  "K3S_URL=https://$NODE_IP:6443" \
  "K3S_TOKEN=$(cat "$TOKEN_FILE")" \
  "K3S_VERSION=$VERSION" \
  "POOL_VPN=$POOL_VPN" \
  "NODE_NAME=$NAME" | base64 -w0)

echo "Invite for '$NAME'. The code below is a secret: anyone who has it can join the pool as this device."
echo "On the device (Linux), in a clone of this repo, run:"
echo
# The trailing checksum lets join.sh reject a code that was cut short when copied.
CHECK=$(printf '%s' "$ENV_B64.$VPN_B64" | sha256sum | cut -c1-8)
echo "sudo deploy/join.sh pool1.$ENV_B64.$VPN_B64.$CHECK"
