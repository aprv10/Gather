# Gather

Gather is a small private GPU compute pool. A coordinator keeps the queue in SQLite.
Workers advertise their NVIDIA GPUs and pull jobs over HTTP. The first scheduler
assigns each worker at most one job at a time, in queue order, when its reported
CPU, available RAM, and free GPU memory meet the request. A worker runs the command
as its own OS user, sends logs to the coordinator, and uploads declared output files.

## Stage 1 concepts

- **Heartbeat:** A worker refreshes its resource report every five seconds. After
  20 seconds without a heartbeat, its running job becomes `lost`.
- **Claim:** The coordinator assigns a job in one SQLite transaction so two workers
  cannot claim it. Jobs move through `queued`, `assigned`, `running`, then
  `succeeded`, `failed`, or `lost`.
- **Execution:** Workers initiate all connections. Jobs run without a shell, in a
  per-job directory. The assigned GPU is selected with `CUDA_VISIBLE_DEVICES`.
- **Trust:** Every member uses the same token in this first version. Commands are
  arbitrary programs with the worker user's permissions. Run Gather only among
  trusted friends over an encrypted private network, such as a VPN. Do not expose
  the coordinator's plain HTTP port directly to the public Internet.

Stage 1 assumes the requested program and its dependencies are already installed
on the worker. It does not transfer source files, isolate jobs, enforce resource
limits, retry lost jobs, or schedule multiple jobs on one worker.

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
uvicorn gather.coordinator:app --host 0.0.0.0 --port 8000
```

In another terminal, on each worker:

```powershell
$env:GATHER_TOKEN = "replace-with-a-long-random-secret"
$env:GATHER_URL = "http://PRIVATE_COORDINATOR_IP:8000"
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

Use `--gpu-mb 0` to submit a CPU-only command. The default request is one CPU,
no minimum available RAM, and at least 1 MiB of free NVIDIA GPU memory. Set
`--cpu` and `--ram-mb` when the job needs more. Output names are paths relative to
the job directory and each output must be at most 100 MiB.

State and logs survive coordinator restarts in `gather.db`. Runtime files are
kept in `gather.db`, `gather-data/`, and `worker-data/` by default.
