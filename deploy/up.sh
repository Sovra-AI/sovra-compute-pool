#!/usr/bin/env bash
# Bring the pool up on this machine in one step: the pool VPN, then the control plane.
#
#   sudo deploy/up.sh                 (or: pkexec /full/path/to/deploy/up.sh)
#
# Safe to re-run. POOL_VPN=0 skips the VPN; VPN_ENDPOINT=<host:port> sets the address
# devices dial (default: this machine's LAN address).
set -euo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# pkexec does not say who asked; node.sh uses SUDO_USER to hand files back to that person.
if [[ -z ${SUDO_USER:-} && -n ${PKEXEC_UID:-} ]]; then
  SUDO_USER=$(id -un "$PKEXEC_UID")
  export SUDO_USER
fi

if [[ ${POOL_VPN:-1} == 1 && ! -e /etc/wireguard/pool0.conf ]]; then
  "$HERE/vpn.sh" init ${VPN_ENDPOINT:+"$VPN_ENDPOINT"}
fi
"$HERE/node.sh" server
# Keep pod routing over the VPN in step with the nodes (also refreshes the installed copy).
if [[ -e /etc/wireguard/pool0.conf ]]; then
  "$HERE/vpn.sh" install-sync
  "$HERE/vpn.sh" sync
fi
