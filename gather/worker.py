"""Outbound-polling worker. Jobs run as the current OS user."""

import argparse
import csv
import io
import os
import platform
import signal
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import psutil

from gather.bundle import extract_bundle
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
    return {"protocol": 3, "cpu": os.cpu_count() or 1,
            "ram_mb": psutil.virtual_memory().available // (1024 * 1024),
            "gpus": gpus, "platform": platform.system()}


def stop_process_tree(process):
    """Stop a job and children it launched, with a short POSIX grace period."""
    if os.name == "nt":
        try:
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.SubprocessError):
            pass
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
            time.sleep(2)
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.poll() is None:
        process.kill()
    process.wait()


def run_job(job, state_dir, server, token, worker_id, session, shutdown):
    job_id = job["id"]
    workspace = state_dir / "jobs" / job_id
    workspace.mkdir(parents=True, exist_ok=True)
    headers = {"X-Worker-Id": worker_id, "X-Worker-Session": session}

    def cancel_before_start():
        try:
            call("POST", f"/jobs/{job_id}/cancel", {"reason": "Worker stopped before execution"},
                 url=server, token=token)
        except RuntimeError:
            pass

    env = os.environ.copy()
    if job["gpu_uuid"]:
        env["CUDA_VISIBLE_DEVICES"] = job["gpu_uuid"]
    else:
        env["CUDA_VISIBLE_DEVICES"] = "-1"
    exit_code = 1
    error = None
    process = None
    cancelled = threading.Event()
    monitor_stop = threading.Event()
    cancel_reason = [None]
    monitor_thread = None
    try:
        if shutdown.is_set():
            cancel_before_start()
            return
        if job.get("has_input"):
            archive = call("GET", f"/jobs/{job_id}/input", url=server, token=token,
                           raw=True, timeout=300)
            extract_bundle(archive, workspace)
        while True:
            if shutdown.is_set():
                cancel_before_start()
                return
            try:
                call("POST", f"/jobs/{job_id}/start", {}, url=server, token=token, headers=headers)
                break
            except RuntimeError as exc:
                if "HTTP 409" in str(exc):
                    print(f"{job_id}: assignment expired or cancelled before start", flush=True)
                    return
                print(f"{job_id}: waiting to start: {exc}", flush=True)
                time.sleep(5)
        current = call("GET", f"/jobs/{job_id}", url=server, token=token)
        if current["state"] == "cancelling":
            cancelled.set()
            cancel_reason[0] = current["error"]
        else:
            options = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
                       if os.name == "nt" else {"start_new_session": True})
            process = subprocess.Popen(job["command"], cwd=workspace, env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       bufsize=0, **options)
            started = time.monotonic()

            def monitor():
                while not monitor_stop.is_set():
                    limit = job.get("max_runtime_s", 0)
                    if limit and time.monotonic() - started >= limit:
                        cancel_reason[0] = "Runtime limit exceeded"
                        cancelled.set()
                    if not cancelled.is_set():
                        try:
                            state = call("GET", f"/jobs/{job_id}", url=server,
                                         token=token, timeout=3)
                            if state["state"] in ("cancelling", "cancelled", "lost"):
                                cancel_reason[0] = state["error"] or "Job cancelled"
                                cancelled.set()
                        except RuntimeError:
                            pass
                    if cancelled.is_set():
                        stop_process_tree(process)
                        return
                    monitor_stop.wait(1)

            monitor_thread = threading.Thread(target=monitor, daemon=True)
            monitor_thread.start()
            with process.stdout:
                for line in iter(process.stdout.readline, b""):
                    for offset in range(0, len(line), 60000):
                        chunk = line[offset:offset + 60000].replace(b"\r\n", b"\n")
                        call("POST", f"/jobs/{job_id}/logs", chunk,
                             url=server, token=token, raw=True, headers=headers)
            exit_code = process.wait()
    except (OSError, RuntimeError, ValueError) as exc:
        error = str(exc)
        if process:
            stop_process_tree(process)
        try:
            call("POST", f"/jobs/{job_id}/logs", (error + "\n").encode()[:60000],
                 url=server, token=token, raw=True, headers=headers)
        except RuntimeError:
            pass
    finally:
        monitor_stop.set()
        if monitor_thread:
            monitor_thread.join(timeout=3)
    if cancelled.is_set():
        error = cancel_reason[0] or error or "Job cancelled"
    outputs = [] if cancelled.is_set() else job["outputs"]
    for name in outputs:
        path = workspace / name
        if path.is_file():
            try:
                if path.stat().st_size > 100 * 1024 * 1024:
                    raise RuntimeError(f"Output {name} exceeds 100 MiB")
                call("PUT", f"/jobs/{job_id}/output/{quote(name, safe='/')}", path.read_bytes(),
                     url=server, token=token, raw=True, headers=headers, timeout=300)
            except (OSError, RuntimeError) as exc:
                error = str(exc)
                exit_code = 1
        elif exit_code == 0:
            error = f"Declared output {name} was not created"
            exit_code = 1
    while True:
        try:
            call("POST", f"/jobs/{job_id}/finish", {"exit_code": exit_code, "error": error,
                                                    "cancelled": cancelled.is_set()},
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
    shutdown = threading.Event()

    def request_shutdown(_signum, _frame):
        if not shutdown.is_set():
            print("Stopping after the current job; use gather cancel to stop it now", flush=True)
            shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)

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
        while not shutdown.is_set():
            try:
                reply = call("POST", f"/workers/{worker_id}/claim", {},
                             url=args.url, token=token,
                             headers={"X-Worker-Session": session})
                if reply["job"]:
                    run_job(reply["job"], args.state_dir, args.url,
                            token, worker_id, session, shutdown)
                else:
                    shutdown.wait(2)
            except RuntimeError as exc:
                print(exc, flush=True)
                shutdown.wait(5)
    finally:
        stopped.set()
        try:
            call("POST", f"/workers/{worker_id}/stop", {}, url=args.url, token=token,
                 headers={"X-Worker-Session": session})
        except RuntimeError:
            pass


if __name__ == "__main__":
    main()
