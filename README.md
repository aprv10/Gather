# Gather

Gather is a small private GPU compute pool. A coordinator keeps the queue in SQLite.
Workers advertise their NVIDIA GPUs and pull jobs over HTTP. The first scheduler
assigns each worker at most one job at a time, in queue order, when its reported
CPU, available RAM, and free GPU memory meet the request. A worker runs the command
as its own OS user, sends logs to the coordinator, and uploads declared output files.
The CLI can also send a small input directory with each job.

## Stage 1 concepts

- **Heartbeat:** A worker refreshes its resource report every five seconds. After
  60 seconds without a heartbeat, its running job becomes `lost`.
- **Claim:** The coordinator assigns a job in one SQLite transaction so two workers
  cannot claim it. Jobs move through `queued`, `assigned`, `running`, then
  `succeeded`, `failed`, `cancelled`, or `lost`.
- **Execution:** Workers initiate all connections. Jobs run without a shell, in a
  per-job directory. The assigned GPU is selected with `CUDA_VISIBLE_DEVICES`.
- **Trust:** Every member uses the same token in this first version. Commands are
  arbitrary programs with the worker user's permissions. Run Gather only among
  trusted friends over an encrypted private network, such as a VPN. Do not expose
  the coordinator's plain HTTP port directly to the public Internet.

## Stage 2: job inputs

`gather submit --input DIRECTORY` packages the directory as a ZIP. The coordinator
keeps the job in `uploading` until the complete input arrives, then queues it. The
worker unpacks it into the job directory before running the command. Unsafe archive
paths and symlinks are rejected. An input is limited to 50 MiB compressed, 200 MiB
unpacked, and 1000 files. Include only files you intend to send to every worker
that may claim the job; do not include secrets.

If the upload is interrupted, the CLI prints the job ID. Resume it with
`gather upload JOB_ID DIRECTORY`; the job remains out of the queue until its input
is complete. Restart both coordinator and workers after upgrading from Stage 1.

The requested executable and its dependencies must still be installed on the
worker. Jobs are not isolated, CPU/RAM/GPU memory limits are not enforced, lost
jobs are not retried, and a worker runs only one job at a time.

## Stage 3: job control

Use `gather cancel JOB_ID` to remove a queued job or ask a worker to stop a running
one. A running job briefly shows `cancelling`, then `cancelled` when the worker has
stopped its process tree. Submit with `--max-runtime SECONDS` to cancel a job that
runs too long; the timer starts when execution begins. A value of 0 means no limit.
If a worker cannot be reached, `cancelling` does not confirm its process has
stopped; the job becomes `lost` when the worker heartbeat expires.

Press Ctrl+C once in a worker terminal to let its current job finish and then take
the worker offline. To stop that job immediately, run `gather cancel JOB_ID` in
another terminal. Restart the coordinator and workers after upgrading; older
workers do not accept jobs from the Stage 3 coordinator.

## Install

Use Python 3.10 or newer on the coordinator, workers, and CLI computers. Workers
need an NVIDIA driver with `nvidia-smi` on `PATH` for GPU jobs. From this repository:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .
```

On Linux or macOS, activate with `source .venv/bin/activate`.

## Run

Choose a long random shared token and set the same value on every machine. The
examples below use PowerShell. Set `GATHER_URL` to the coordinator's private
network address on remote computers.

```powershell
$env:GATHER_TOKEN = "replace-with-a-long-random-secret"
uvicorn gather.coordinator:app --host 127.0.0.1 --port 8000
```

For friends on a Tailscale network, run `tailscale serve 8000` on the coordinator
in another terminal and use the private HTTPS URL it prints as `GATHER_URL`.
Alternatively, bind the coordinator to a private network interface that your
workers can reach.

In another terminal, on each worker:

```powershell
$env:GATHER_TOKEN = "replace-with-a-long-random-secret"
$env:GATHER_URL = "https://YOUR-COORDINATOR.YOUR-TAILNET.ts.net"
gather-worker --name my-pc
```

From a CLI computer, with the same environment variables:

```powershell
gather workers
gather submit --gpu-mb 1024 --output result.txt -- python -c "from pathlib import Path; print('hello from worker'); Path('result.txt').write_text('done')"
gather jobs
gather status JOB_ID
gather logs JOB_ID --follow
gather results JOB_ID --dir downloaded
```

For a time limit or manual cancellation:

```powershell
gather submit --max-runtime 300 -- python -c "import time; time.sleep(600)"
gather cancel JOB_ID
```

To submit a directory containing `train.py`, run:

```powershell
gather submit --gpu-mb 1024 --input .\project --output result.txt -- python train.py
```

Use `--gpu-mb 0` to submit a CPU-only command. The default request is one CPU,
no minimum available RAM, and at least 1 MiB of free NVIDIA GPU memory. Set
`--cpu` and `--ram-mb` when the job needs more. Output names are paths relative to
the job directory and each output must be at most 100 MiB.

State and logs survive coordinator restarts in `gather.db`. Runtime files are
kept in `gather.db`, `gather-data/`, and `worker-data/` by default. Uploaded inputs
and job directories stay there until you remove them.
