# Job reference

## `dist-alloc run` / `dist-alloc submit` options

`run` queues the job, streams its output and exits with its status. `submit` queues it and
prints its id. Both take the same options; the command comes after `--`.

| Option | Default | Meaning |
| --- | --- | --- |
| `--name LABEL` | first word of the command | Name shown in `dist-alloc jobs` |
| `--cpus N` | 1 | Processor cores per replica (scheduling only; not a hard limit) |
| `--memory-mb M` | 1024 | Memory per replica, in MB (scheduling only; not a hard limit) |
| `--gpus N` | 0 | Graphics cards per replica, allocated exclusively |
| `--gpu-memory-mb M` | 0 | Only use cards with at least this much memory |
| `--replicas N` | 1 | Copies placed as a gang: all start together or none do |
| `--retries N` | 0 | Re-runs allowed when a replica exits non-zero (a lost computer does not use one) |
| `--priority N` | 0 | Higher runs first; smaller jobs may start ahead of one that does not fit yet |
| `-e KEY=VALUE` | | Environment variable (repeatable) |
| `-i [NAME=]PATH` | | Ship a local file as `$DA_INPUTS_DIR/NAME` (repeatable) |
| `--dataset NAME` | | Stage a dataset as `$DA_INPUTS_DIR/NAME/<file>` (repeatable) |
| `--partition` | off | Deal dataset files across replicas: rank r gets every replicas-th file from r |
| `--select KEY=VALUE` | | Only computers whose agent has that `--label` (repeatable) |

A job waits (state `queued`, with the reason in NOTE) until the whole gang fits. The
scheduler prefers computers that already hold the most input bytes, uses the smallest
graphics card that fits, and keeps CPU-only work off computers with graphics cards.

## What the command sees

| Variable | Value |
| --- | --- |
| `DA_INPUTS_DIR` | Folder with the staged inputs (read-only links into the local cache) |
| `DA_OUTPUT_DIR` | Folder whose files become the job's outputs when it exits |
| `DA_RANK`, `DA_WORLD_SIZE` | This replica's number (from 0) and the number of replicas |
| `DA_MASTER_ADDR` | Address of rank 0's computer, for distributed training |
| `DA_PEER_HOSTS` | Comma-separated addresses of every rank's computer, by rank |
| `DA_JOB_ID`, `DA_JOB_NAME`, `DA_TASK_ID`, `DA_NODE` | Identifiers |
| `CUDA_VISIBLE_DEVICES`, `DA_GPUS` | Exactly the allocated graphics cards (none if none were asked for) |

The working directory is an empty scratch folder. The process runs as the user that runs
the agent on that computer, in its own process group; everything it leaves running is
stopped when it exits. Output written to stdout and stderr is the job's log.

## Distributed PyTorch training

Each replica runs one `torchrun` node; rank 0's computer is the rendezvous point. Pick a
free port (29500 here) that the computers can reach each other on.

```sh
dist-alloc submit --name ddp --replicas 2 --gpus 1 --cpus 4 --memory-mb 16000 \
  -i train.py --dataset corpus --partition -- \
  sh -c 'torchrun --nnodes "$DA_WORLD_SIZE" --node-rank "$DA_RANK" --nproc-per-node 1 \
    --master-addr "$DA_MASTER_ADDR" --master-port 29500 \
    "$DA_INPUTS_DIR/train.py" --data "$DA_INPUTS_DIR/corpus" --out "$DA_OUTPUT_DIR"'
```

Write checkpoints and the final model to `$DA_OUTPUT_DIR` from rank 0 only, so there is one
copy. Outputs are collected when a replica exits; a checkpoint written mid-run is lost if
that computer goes down, so keep runs resumable.

## Data commands

| Command | What it does |
| --- | --- |
| `dist-alloc dataset create NAME FILE…` | Upload files (content-addressed, deduplicated) and name the set |
| `dist-alloc dataset ls` | List datasets |
| `dist-alloc blob put FILE…` | Upload files; prints their hashes |
| `dist-alloc blob get HASH -o FILE` | Download a blob, such as a job output, from wherever it is |

## Watching and stopping

| Command | What it does |
| --- | --- |
| `dist-alloc jobs` | Every job, newest last |
| `dist-alloc job ID` | One job: shape, placement, each task's state and exit code, outputs |
| `dist-alloc logs ID` / `logs -f ID` | Output so far / follow until it finishes (multi-replica lines are prefixed `[rN]`) |
| `dist-alloc cancel ID` | Stop it |

Add `--json` to `nodes`, `jobs` and `job` for machine-readable output.
