---
name: uninstall-plugin
description: Use when someone wants to take a computer out of a Sovra compute pool, take down the whole pool, or clean up after uninstalling Computer pool.
metadata:
  sovra-title: Take down a computer pool
  sovra-example: Help me stop the pool on my computers and keep the results of my jobs
---

Find out whether the person wants to remove one worker or the whole pool. List the affected computers and who owns each one. Run steps on the named computer with its owner's approval. Keep an offline computer on the list until its owner verifies cleanup there. If they only installed the Sovra plugin and never set up a pool, they can simply confirm Remove in Plugins.

1. Save work first. On a reachable pool, use `dist-alloc nodes` and `dist-alloc jobs` to identify workers and running jobs. Help the owner finish or cancel their jobs and fetch results they want to keep. Check which services were installed on each machine using `systemctl --user list-unit-files 'dist-alloc-*'`.

2. On each worker being removed, run `dist-alloc service uninstall agent` as the user who installed it. Use `~/.local/bin/dist-alloc` if needed. This stops the worker and its tasks and removes its unit and saved join settings. When the main computer will stay in use, run `dist-alloc node-rm NAME` there for the worker being retired. An old invitation still grants access. Check where copies went and remove only identified copies with their owners.

3. To take down the whole pool, remove the workers first, including the main computer's worker if present. On the main computer, run `dist-alloc service uninstall server`. After each uninstall, check that the relevant unit is absent from both `systemctl --user list-unit-files 'dist-alloc-*'` and `systemctl --user list-units --state=active 'dist-alloc-*'`. If a stop or removal failed, resolve it before deleting the program or credentials. Confirm that the worker's dist-alloc/agent.env under its XDG config directory is gone without reading its secret.

4. Review remaining files separately. The default data location is dist-alloc under XDG_DATA_HOME (normally ~/.local/share). Check the setup chat for custom server and worker directories. These may hold credentials, job data and uploaded files. Have the owner choose what to export, keep or delete. Also review the installation cache (~/.cache/dist-alloc-install), separately saved invites and any results downloaded elsewhere. Remove the executable only if it was installed for this pool and no remaining role uses it. Use the original installation method for a Cargo installation.

5. Review settings changed during setup. Restore firewall rules for TCP 7443 and 7444, router reservations, and login lingering only if added for this pool and no other service relies on them. Match individual rules to the setup record. Verify the desired final settings on each computer. Report which computers have been checked, what was intentionally kept, and what still needs attention.

If the owner used the optional Kubernetes setup, identify that cluster before changing it. Export needed workloads and volumes first. Removing a dedicated K3s worker uses its installed k3s-agent-uninstall.sh script; removing a dedicated control plane uses its k3s-uninstall.sh script. In a shared cluster, remove only the pool workloads and access grants. If the pool VPN was used, revoke retired peers with deploy/vpn.sh remove NAME. Review the pool-vpn-sync service, pool WireGuard interface and configuration, forwarding rules, router mapping, join files and dashboard credentials with the administrator of each affected machine. Restore only pool-specific changes and verify unrelated network access still works.

## Always finish with a plain message

Your last message must be words to the person, never a tool call. After the last command
has run, say in two or three plain sentences what you did and whether it worked, then list
what they can do next, one short line each. Never end the turn on a command's output: a
list of terminal cards with no words leaves people not knowing whether it worked or what
to do now.
