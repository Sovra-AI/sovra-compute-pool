---
name: run-a-job-on-the-pool
description: Use when someone wants to run a program, script or AI training on their computer pool — on one computer, on a graphics card, or spread across several computers at once — and get the results back.
metadata:
  sovra-title: Run a job on my computer pool
  sovra-example: Run my train.py on two of the pool's graphics cards and bring back the trained model
---

# Run a job on the computer pool

A **job** is one command the pool runs for the person. It can ask for processor cores,
memory and graphics cards, and run as several copies at once ("replicas") that start
together, for example to train one AI model across several computers.

Work from any computer in the pool, with the `run_terminal_command` tool; its approval card
is how the person says yes. Never paste commands into chat for them. Use the full path
`~/.local/bin/dist-alloc` unless `command -v dist-alloc` finds it.

## 1. Understand the work

Ask only what you cannot work out from their request or files:

- What should run? Look at their project folder to find the entry point (`train.py`, a
  script, a binary) and how it is normally started.
- How much does it need per copy: graphics cards, memory, cores? How many copies?
- Which files does it need (code, data), and what should come back (a model, results)?

Then run `~/.local/bin/dist-alloc nodes` to see what the pool has, and check the request
fits. Explain any mismatch in plain words ("the pool has one graphics card, so two copies
that each need one would wait forever").

**Important:** jobs run as ordinary programs on the pool's computers, not in containers.
Whatever the command needs (Python, PyTorch, a CUDA driver…) must already be installed on
the computers that will run it, or be shipped with `-i`. Check on this computer first
(`python3 -c 'import torch; print(torch.__version__, torch.cuda.is_available())'`) and say
plainly if other computers may lack it.

## 2. Send the files and start it

- **Small, short jobs** (under about a minute): `dist-alloc run …` waits, streams the
  output and returns the job's exit status.
- **Anything longer**: `dist-alloc submit …` prints a job id (like `j7`) straight away. Then
  follow along with `dist-alloc logs j7` and `dist-alloc job j7`, and tell the person they
  can ask "how is my job doing?" at any time. `run` would hit the command time limit.

Everything after `--` is the command. Common options (full list in
[job-reference.md](job-reference.md)):

| Option | Meaning |
| --- | --- |
| `--gpus N` | graphics cards per copy (`--gpu-memory-mb M` = at least M MB each) |
| `--cpus N`, `--memory-mb M` | cores and memory per copy (defaults 1 and 1024) |
| `--replicas N` | copies that all start together, on one or more computers |
| `-i PATH` or `-i NAME=PATH` | ship a file; it appears as `$DA_INPUTS_DIR/NAME` |
| `--dataset NAME` | stage a dataset made with `dist-alloc dataset create` |
| `--partition` | give each copy only its share of the dataset's files |
| `--retries N` | run again up to N times if it fails |
| `--select KEY=VALUE` | only computers with that label |
| `-e KEY=VALUE` | environment variable for the command |
| `--name LABEL` | a name to show in lists |

Inside the command, use `$DA_INPUTS_DIR` for inputs, write results to `$DA_OUTPUT_DIR`, and
run `sh -c '…'` when the command needs variables or several steps, for example:

```sh
dist-alloc submit --name train --gpus 1 -i train.py -- sh -c 'python3 "$DA_INPUTS_DIR/train.py" --out "$DA_OUTPUT_DIR/model.pt"'
```

For training across several copies, the pool provides `DA_RANK`, `DA_WORLD_SIZE`,
`DA_MASTER_ADDR` and `DA_PEER_HOSTS`; see the PyTorch example in
[job-reference.md](job-reference.md).

**Large datasets:** upload them once with `dist-alloc dataset create NAME FILES…` and use
`--dataset NAME`. Computers then fetch them from each other and keep a local copy, so later
jobs start faster.

## 3. Bring back the results

When `dist-alloc job ID` says `succeeded`, its OUTPUT table lists each result file with its
hash. Fetch each one where the person wants it:
`dist-alloc blob get HASH -o ~/results/model.pt`. Then say where the files are.

If it failed, read `dist-alloc logs ID` and the NOTE column of `dist-alloc job ID`, explain
the cause in plain words, and offer a fix. To stop a job: `dist-alloc cancel ID`.
