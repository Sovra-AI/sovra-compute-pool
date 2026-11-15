#!/usr/bin/env bash
# Join this device to the pool with an invite code from the control plane
# (`sudo deploy/invite.sh <device-name>` there). Run here, in a clone of this repo:
#
#   sudo deploy/join.sh <code>
#
# Afterwards the device registers itself on every boot. Leave with: sudo k3s-agent-uninstall.sh
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
die() { echo "error: $*" >&2; exit 1; }

CODE=${1:-}
[[ $CODE == pool1.* ]] || die "usage: sudo $0 <invite-code>   (get one with  sudo deploy/invite.sh <device-name>  on the control plane)"
IFS=. read -r _ ENV_B64 VPN_B64 CHECK <<<"$CODE"
[[ -n $CHECK && $CHECK == "$(printf '%s' "$ENV_B64.$VPN_B64" | sha256sum | cut -c1-8)" ]] ||
  die "the invite code is incomplete or damaged (copy it again, in full, as one line)"
SETTINGS=$(base64 -d <<<"$ENV_B64" 2>/dev/null) || die "the invite code is damaged (copy it again, in full)"

# Only the expected settings are taken from the code; nothing in it is executed.
while IFS='=' read -r key value; do
  case $key in
    K3S_URL | K3S_TOKEN | K3S_VERSION | POOL_VPN | NODE_NAME) export "$key=$value" ;;
  esac
done <<<"$SETTINGS"
[[ -n ${K3S_URL:-} && -n ${K3S_TOKEN:-} ]] || die "the invite code is incomplete (copy it again, in full)"

VPN_CONF=""
if [[ -n $VPN_B64 ]]; then
  VPN_CONF=$(mktemp)
  trap 'rm -f "$VPN_CONF"' EXIT
  base64 -d <<<"$VPN_B64" > "$VPN_CONF" 2>/dev/null || die "the invite code is damaged (copy it again, in full)"
fi

if [[ ${JOIN_DRY_RUN:-0} == 1 ]]; then
  echo "would join $K3S_URL as node '${NODE_NAME:-}' (k3s ${K3S_VERSION:-latest}, vpn=${POOL_VPN:-0})"
  if [[ -n $VPN_CONF ]]; then grep -E '^(Address|Endpoint|AllowedIPs)' "$VPN_CONF"; fi
  exit 0
fi
"$HERE/node.sh" agent ${VPN_CONF:+"$VPN_CONF"}
