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

Treat supplied paths as existing inputs. Read or stage those files as supplied. If an
input is missing, identify the path and ask for it. Create or edit input files only when
the person requests that change.

Use `dist-alloc nodes --json` to check the fit. Each copy needs all of its cores, memory
and qualifying graphics cards on one ready computer matching the requested labels. For
example, two computers with one card each can run two one-card copies. A single two-card
copy needs a computer with both cards. Compare free resources with total capacity so the
person knows whether waiting for another job to finish would help.

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

For repeated runs, reuse a named dataset with `--dataset NAME`. Create it once with
`dist-alloc dataset create NAME FILES…`. Add `--partition` when each copy can work on its
own subset of files. A program that expects every copy to see the full dataset needs the
default shared layout.

## 3. Bring back the results

After the job finishes, run `dist-alloc job ID --json`. For each task whose `attempt`
matches the job's final `attempt`, pair its `rank` and `node` with the filenames in
that task's own `outputs` array. An empty array means that rank produced no files,
even when its exit code is zero. Report each requested file only for the ranks that
list it, and mention any missing results. When asked to download files, use their listed hashes with
`dist-alloc blob get HASH -o PATH`. Give same-named outputs from different ranks
distinct destination paths, and report the paths after the downloads succeed.

If it failed, read `dist-alloc logs ID` and the NOTE column of `dist-alloc job ID`, explain
the cause in plain words, and offer a fix. To stop a job: `dist-alloc cancel ID`.

## Always finish with a plain message

Your last message must be words to the person, never a tool call. After the last command
has run, say in two or three plain sentences what you did and whether it worked, then list
what they can do next, one short line each. Never end the turn on a command's output: a
list of terminal cards with no words leaves people not knowing whether it worked or what
to do now.
