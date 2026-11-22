---
name: check-on-the-pool
description: Use when someone asks how their computer pool is doing — which computers are on, what is busy, how a job is going — or when something in the pool seems stuck or broken.
metadata:
  sovra-title: Check on my computer pool
  sovra-example: How is my computer pool doing, and is my training job finished yet?
---

# Check on the computer pool

Answer in plain words first ("All three computers are on. Your training job is halfway
through its second hour."), then offer details. Run every command yourself with the
`run_terminal_command` tool; never paste commands for the person to run. Use
`~/.local/bin/dist-alloc` unless `command -v dist-alloc` finds it. These commands work on
any computer in the pool.

Never show or repeat the pool's join token (it starts with `da1.`), and never read
`~/.config/dist-alloc/agent.env` or `~/.local/share/dist-alloc/server/token` aloud.

## What is in the pool

`dist-alloc nodes` lists every computer:

- **STATUS** `ready` means it is on and talking to the pool; `lost` means it has not been
  heard from for 15 seconds (turned off, asleep, or its connection dropped). Work on a lost
  computer is moved to others automatically.
- **CPU / MEM / GPU** show what is in use out of what it offers, for example `4/16` cores.
- **LOAD**, **DISK FREE** and **BLOBS** (cached files) show how busy and full it is.
- The last line sums up the whole pool.

## How jobs are doing

- `dist-alloc jobs` — every job with its state: `queued` (waiting; NOTE says why),
  `running`, `succeeded`, `failed` or `cancelled`.
- `dist-alloc job ID` — where each copy runs, exit codes, and its result files.
- `dist-alloc logs ID` — what it has printed so far. Summarise long logs; quote only the
  lines that matter, such as the latest progress line or the error.

To stop a job, confirm with the person, then run `dist-alloc cancel ID`.

## When something is wrong

Check the background services on the affected computer:
`systemctl --user status dist-alloc-agent --no-pager` (and `dist-alloc-server` on the main
computer), then `journalctl --user -u dist-alloc-agent -n 50 --no-pager`.

| Symptom | Likely cause and fix |
| --- | --- |
| `no token: pass --token…` | This computer is not in a pool yet: offer the "Start a computer pool" or "Add a computer to my pool" skill |
| `cannot reach the control plane` / `connecting to control plane` | The main computer is off, its service stopped (`systemctl --user restart dist-alloc-server` there), its address changed, or a firewall blocks TCP 7443 |
| `does not match this token` | The settings belong to a different pool, or the main computer's identity was reset: rejoin with a fresh invite |
| Computer shows `lost` | It is off or asleep, or its agent stopped: `systemctl --user restart dist-alloc-agent` on it |
| Job stays `queued` with `waiting for capacity` | It asks for more than any computer (or the gang) has free; lower `--gpus`/`--memory-mb`/`--replicas`, or wait for other jobs |
| `no ready node matches the selector` | No computer has the `--select` label it asked for |
| Job `failed` quickly | Read `dist-alloc logs ID`: usually a missing program or Python package on that computer |
| Fetching inputs fails between computers | TCP 7444 blocked between them; the pool falls back to the main computer's copy when it has one |
| Services stop when the person logs out | Run `loginctl enable-linger "$USER"` on that computer |

To restart everything on one computer: `systemctl --user restart dist-alloc-server` (main
computer only) and `systemctl --user restart dist-alloc-agent`. Running jobs on a
restarted agent are moved elsewhere and do not use up a retry.
