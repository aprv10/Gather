"""Outbound-polling worker. Jobs run as the current OS user."""

import argparse
import csv
import io
import os
import platform
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import psutil

from gather.http import call


def discover():
    gpus = []
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=uuid,name,memory.total,memory.free",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, check=True, timeout=5,
        )
        for row in csv.reader(io.StringIO(result.stdout)):
            if len(row) == 4:
                try:
                    gpus.append({"uuid": row[0].strip(), "name": row[1].strip(),
                                 "total_mb": int(row[2].strip()), "free_mb": int(row[3].strip())})
                except ValueError:
                    continue
    except (OSError, subprocess.SubprocessError):
        pass
    return {"cpu": os.cpu_count() or 1, "ram_mb": psutil.virtual_memory().available // (1024 * 1024),
            "gpus": gpus, "platform": platform.system()}


def run_job(job, state_dir, server, token, worker_id, session):
    job_id = job["id"]
    workspace = state_dir / "jobs" / job_id
    workspace.mkdir(parents=True, exist_ok=True)
    headers = {"X-Worker-Id": worker_id, "X-Worker-Session": session}
    call("POST", f"/jobs/{job_id}/start", {}, url=server, token=token, headers=headers)
    env = os.environ.copy()
    if job["gpu_uuid"]:
        env["CUDA_VISIBLE_DEVICES"] = job["gpu_uuid"]
    else:
        env["CUDA_VISIBLE_DEVICES"] = "-1"
    exit_code = 1
    error = None
    process = None
    try:
        process = subprocess.Popen(job["command"], cwd=workspace, env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   bufsize=0)
        with process.stdout:
            for line in iter(process.stdout.readline, b""):
                # Split long lines so each request remains under the coordinator's limit.
                for offset in range(0, len(line), 60000):
                    chunk = line[offset:offset + 60000].replace(b"\r\n", b"\n")
                    call("POST", f"/jobs/{job_id}/logs", chunk,
                         url=server, token=token, raw=True, headers=headers)
        exit_code = process.wait()
    except (OSError, RuntimeError) as exc:
        error = str(exc)
        if process and process.poll() is None:
            process.kill()
            process.wait()
        try:
            call("POST", f"/jobs/{job_id}/logs", (error + "\n").encode()[:60000],
                 url=server, token=token, raw=True, headers=headers)
        except RuntimeError:
            pass
    for name in job["outputs"]:
        path = workspace / name
        if path.is_file():
            try:
                if path.stat().st_size > 100 * 1024 * 1024:
                    raise RuntimeError(f"Output {name} exceeds 100 MiB")
                call("PUT", f"/jobs/{job_id}/output/{quote(name, safe='/')}", path.read_bytes(),
                     url=server, token=token, raw=True, headers=headers)
            except (OSError, RuntimeError) as exc:
                error = str(exc)
                exit_code = 1
        elif exit_code == 0:
            error = f"Declared output {name} was not created"
            exit_code = 1
    while True:
        try:
            call("POST", f"/jobs/{job_id}/finish", {"exit_code": exit_code, "error": error},
                 url=server, token=token, headers=headers)
            break
        except RuntimeError as exc:
            if "HTTP 409" in str(exc):
                print(f"{job_id}: coordinator no longer accepts this attempt", flush=True)
                return
            print(f"{job_id}: waiting to report result: {exc}", flush=True)
            time.sleep(5)
    print(f"{job_id}: exit {exit_code}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Run a Gather worker")
    parser.add_argument("--url", default=os.environ.get("GATHER_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--name", default=socket.gethostname())
    parser.add_argument("--state-dir", type=Path, default=Path("worker-data"))
    args = parser.parse_args()
    token = os.environ.get("GATHER_TOKEN")
    if not token:
        parser.error("Set GATHER_TOKEN first")
    args.state_dir.mkdir(parents=True, exist_ok=True)
    id_file = args.state_dir / "worker-id"
    if not id_file.exists():
        id_file.write_text(uuid.uuid4().hex)
    worker_id = id_file.read_text().strip()
    session = uuid.uuid4().hex
    registration = {"id": worker_id, "name": args.name, "session": session,
                    "resources": discover()}
    call("POST", "/workers/register", registration, url=args.url, token=token)
    print(f"Worker {args.name} ({worker_id}) connected", flush=True)
    stopped = threading.Event()

    def heartbeat():
        last_error = None
        while not stopped.wait(5):
            try:
                registration["resources"] = discover()
                call("POST", f"/workers/{worker_id}/heartbeat", registration,
                     url=args.url, token=token)
                last_error = None
            except RuntimeError as exc:
                if str(exc) != last_error:
                    print(f"Heartbeat: {exc}", flush=True)
                    last_error = str(exc)

    threading.Thread(target=heartbeat, daemon=True).start()
    try:
        while True:
            try:
                reply = call("POST", f"/workers/{worker_id}/claim", {},
                             url=args.url, token=token,
                             headers={"X-Worker-Session": session})
                if reply["job"]:
                    run_job(reply["job"], args.state_dir, args.url, token, worker_id, session)
                else:
                    time.sleep(2)
            except RuntimeError as exc:
                print(exc, flush=True)
                time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        stopped.set()


if __name__ == "__main__":
    main()
