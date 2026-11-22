---
name: set-up-a-computer-pool
description: Use when someone wants to start a computer pool — make this computer the one that shares out work, so their other computers can join and big jobs (such as training an AI model) can run across all of them.
metadata:
  sovra-title: Start a computer pool
  sovra-example: Turn this computer into the main computer of a pool so my other computers can help with big jobs
---

# Start a computer pool

This makes the current computer the pool's **main computer** (the "control plane"). It
keeps the list of computers, hands out jobs and stores uploaded files. It also offers its
own processors, memory and graphics cards to the pool.

Explain in plain words as you go: "I'm going to install the pool program, start it in the
background, and add this computer to it." Avoid jargon unless the person uses it first.

## How to work

- Do every step yourself with the `run_terminal_command` tool. Each command shows the person
  an approval card; that card is how they say yes. Never paste a command into the chat for
  them to copy, and never ask in chat whether you may run one.
- Never start `dist-alloc server` or `dist-alloc agent` directly, in the foreground or with
  `&`: they run forever, so the command would hang until its time limit. Always use
  `dist-alloc service install …`, which runs them in the background and brings them back
  after a restart.
- The pool's **join token** (it starts with `da1.`) is a secret, like a house key: whoever
  has it can run programs on every computer in the pool. Never print it, never put it in
  chat, and never run `dist-alloc token` without `--write`. Use invite files instead.

## 1. Check this computer can host a pool

Run `uname -sm` and `systemctl --user --version`.

- It must say `Linux` with `x86_64` or `aarch64`, and systemd must answer. On macOS,
  Windows or a Linux without systemd, stop and say kindly that the pool currently runs on
  Linux computers only.
- If `command -v dist-alloc` (or `~/.local/bin/dist-alloc --version`) already works, skip
  to step 3.

## 2. Install the pool program

Follow [install-dist-alloc.md](install-dist-alloc.md) exactly. It downloads the official
release, checks its fingerprint and installs it to `~/.local/bin/dist-alloc` without
needing an administrator password.

## 3. Start the main computer and join it to its own pool

1. `~/.local/bin/dist-alloc service install server`
2. `~/.local/bin/dist-alloc service install agent` — on the main computer this joins its own
   pool with no extra settings. Leave it out only if the person does not want this computer
   to do any of the work itself (then it only coordinates). Pass limits after `--` if they
   want to keep some of the computer for themselves, for example
   `… service install agent -- --cpus 4 --memory-mb 8192 --no-gpus`.
3. If either install printed `note: it stops when you log out`, run
   `loginctl enable-linger "$USER"` so the pool keeps running when nobody is logged in.
4. Wait a few seconds, then run `~/.local/bin/dist-alloc nodes`. Read the table back in plain
   words: how many processor cores, how much memory and which graphics cards the pool has.

If `nodes` cannot connect, run `systemctl --user status dist-alloc-server --no-pager` and
`journalctl --user -u dist-alloc-server -n 40 --no-pager`, and explain what they say.

## 4. Make sure other computers can reach it

Other computers connect to this one on **TCP port 7443**, and computers fetch files from each
other on **TCP port 7444**. Run `ip -4 -brief address` and tell the person this computer's
local network address (for example `192.168.1.20`).

- If a firewall is on (`ufw status` or `firewall-cmd --state`; either may need an
  administrator), ports 7443 and 7444 must be allowed. Opening them needs an administrator
  password: explain why, and run `sudo ufw allow 7443/tcp` and `sudo ufw allow 7444/tcp` (or
  the `firewall-cmd --add-port=…/tcp --permanent` equivalents) only after they agree.
- Suggest giving this computer a fixed address in their router (a "DHCP reservation"):
  invites contain the address, so computers lose the pool if it changes.

## 5. What next

Tell the person, in one or two sentences, that they can now:

- add another computer (the "Add a computer to my pool" skill makes an invite file for it),
- run work on the pool ("Run a job on my computer pool"),
- ask how the pool is doing ("Check on my computer pool").

To take the pool down later: `~/.local/bin/dist-alloc service uninstall agent` and
`~/.local/bin/dist-alloc service uninstall server`. Its saved state stays in
`~/.local/share/dist-alloc/` so it can be started again with the same devices.
