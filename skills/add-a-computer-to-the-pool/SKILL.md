---
name: add-a-computer-to-the-pool
description: Use when someone wants another of their computers to join their computer pool and lend its processors, memory or graphics card, or wants to stop a computer taking part.
metadata:
  sovra-title: Add a computer to my pool
  sovra-example: Add my gaming PC to my computer pool so its graphics card can help
---

# Add a computer to the pool

A computer joins with an **invite file** made on the pool's main computer. The invite holds
the pool's join token, which is a secret like a house key: whoever has it can run programs on
every computer in the pool. Keep it out of chat: never print it, `cat` it, or paste it.

Work out first which computer you are running on, by asking the person if it is not clear:

- **The pool's main computer** (it has `~/.local/share/dist-alloc/server/token`): make the
  invite (part A).
- **The computer that should join**: install and join (part B). Sovra with this plugin must
  be running on that computer, or the person can follow part B there themselves.

Do every step with the `run_terminal_command` tool; its approval card is how the person says
yes. Never paste commands into chat for them to run, and never start `dist-alloc agent`
directly (it runs forever): always use `dist-alloc service install agent`.

## A. On the main computer: make an invite

1. Run `~/.local/bin/dist-alloc token --write ~/pool-invite.txt`. It is saved so only this
   user can read it.
   - If the new computer reaches the main one at a different address than the one this
     computer would pick (another network, a VPN), add `--host ADDRESS`.
2. Tell the person to copy `pool-invite.txt` from their home folder to the new computer —
   on a USB stick, through a shared folder, or however they usually move files — and to
   delete the copies once the computer has joined. Do not offer to email or message it.

## B. On the computer that should join

1. **Check it can join.** Run `uname -sm` and `systemctl --user --version`. Only Linux
   (`x86_64` or `aarch64`) with systemd can join today; otherwise say so kindly and stop.
   For a graphics card to be shared it must be an NVIDIA card with its driver installed:
   `nvidia-smi` should list it.
2. **Install the pool program** if `~/.local/bin/dist-alloc --version` fails. Use exactly the
   steps in [install-dist-alloc.md](install-dist-alloc.md): download the release for this
   computer, check its fingerprint, install to `~/.local/bin`.
3. **Find the invite.** Ask where they put `pool-invite.txt` (or look for it with
   `ls ~/pool-invite.txt ~/Downloads/pool-invite.txt ~/Desktop/pool-invite.txt`). Do not
   open it.
4. **Join.** Run `~/.local/bin/dist-alloc service install agent --invite PATH`.
   - To share only part of the computer, pass limits after `--`:
     `--cpus 4`, `--memory-mb 16384`, `--no-gpus`. To label it for picking later:
     `--label room=office` (repeatable).
   - If it printed `note: it stops when you log out`, run `loginctl enable-linger "$USER"`.
5. **Confirm.** After a few seconds run `~/.local/bin/dist-alloc nodes` and find this
   computer's row marked `ready`. Say in plain words what it adds to the pool.
6. Remind them to delete the invite file copies (`rm PATH` once they agree). The computer
   keeps its own copy of the settings in `~/.config/dist-alloc/agent.env`.

If it does not appear, see what `journalctl --user -u dist-alloc-agent -n 40 --no-pager`
says. "cannot reach the control plane" usually means a firewall on the main computer
(ports 7443 and 7444), the computers being on different networks, or the main computer's
address having changed. "does not match this token" means the invite is from another pool.

## Take a computer out of the pool

On that computer: `~/.local/bin/dist-alloc service uninstall agent`. Its running work
stops and is moved to other computers. To also forget it on the main computer, run
`~/.local/bin/dist-alloc node-rm NAME` there.

## Advanced: a Kubernetes pool

For people who already use Kubernetes and want containers, a web dashboard and a private
VPN between sites, the repository also contains a separate K3s-based setup. It needs
administrator rights on every computer. See [kubernetes-pool.md](kubernetes-pool.md) and
offer it only if they ask for Kubernetes, containers or a VPN.
