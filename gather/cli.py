"""CLI for submitting jobs and reading their results."""

import argparse
import os
import sys
import time
from pathlib import Path
from urllib.parse import quote

from gather.bundle import pack_directory
from gather.http import call


DONE = {"succeeded", "failed", "lost"}


def main():
    parser = argparse.ArgumentParser(prog="gather")
    parser.add_argument("--url", default=os.environ.get("GATHER_URL", "http://127.0.0.1:8000"))
    commands = parser.add_subparsers(dest="action", required=True)
    submit = commands.add_parser("submit", help="Submit a command installed on workers")
    submit.add_argument("--cpu", type=int, default=1)
    submit.add_argument("--ram-mb", type=int, default=0)
    submit.add_argument("--gpu-mb", type=int, default=1, help="Minimum free GPU memory; 0 for CPU only")
    submit.add_argument("--output", action="append", default=[], help="Relative output file to retrieve")
    submit.add_argument("--input", type=Path, help="Small directory to copy into the job workspace")
    submit.add_argument("command", nargs=argparse.REMAINDER)
    upload = commands.add_parser("upload", help="Resume an interrupted input upload")
    upload.add_argument("id")
    upload.add_argument("directory", type=Path)
    commands.add_parser("workers")
    commands.add_parser("jobs")
    status = commands.add_parser("status")
    status.add_argument("id")
    logs = commands.add_parser("logs")
    logs.add_argument("id")
    logs.add_argument("--follow", action="store_true")
    results = commands.add_parser("results")
    results.add_argument("id")
    results.add_argument("--dir", type=Path, default=Path("."))
    args = parser.parse_args()

    def api(method, path, data=None, **kwargs):
        return call(method, path, data, url=args.url, **kwargs)

    try:
        if args.action == "submit":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            if not command:
                parser.error("submit needs a command after --")
            archive = pack_directory(args.input) if args.input else None
            if archive is not None and not api("GET", "/capabilities").get("input_bundle"):
                raise RuntimeError("Coordinator does not support input bundles")
            job = api("POST", "/jobs", {"command": command, "cpu": args.cpu,
                                        "ram_mb": args.ram_mb, "gpu_mb": args.gpu_mb,
                                        "outputs": args.output, "has_input": archive is not None})
            if archive is not None:
                try:
                    api("PUT", f"/jobs/{job['id']}/input", archive, raw=True, timeout=300)
                except RuntimeError as exc:
                    raise RuntimeError(f"Job {job['id']} is waiting for input; retry with "
                                       f"gather upload {job['id']} DIRECTORY: {exc}") from exc
            print(job["id"])
        elif args.action == "upload":
            archive = pack_directory(args.directory)
            api("PUT", f"/jobs/{args.id}/input", archive, raw=True, timeout=300)
            print(args.id)
        elif args.action == "workers":
            for worker in api("GET", "/workers"):
                resources = worker["resources"]
                gpus = ", ".join(f"{g['name']} ({g['free_mb']} MiB free)" for g in resources["gpus"])
                print(f"{worker['id'][:8]}  {worker['name']}  "
                      f"{'online' if worker['online'] else 'offline'}  "
                      f"{resources['cpu']} CPU  {resources['ram_mb']} MiB RAM  {gpus or 'no GPU'}")
        elif args.action == "jobs":
            for job in api("GET", "/jobs"):
                print(f"{job['id']}  {job['state']:<9}  {' '.join(job['command'])}")
        elif args.action == "status":
            job = api("GET", f"/jobs/{args.id}")
            for field in ("id", "state", "worker_id", "gpu_uuid", "exit_code", "error", "outputs"):
                print(f"{field}: {job[field]}")
        elif args.action == "logs":
            after = 0
            while True:
                entries = api("GET", f"/jobs/{args.id}/logs?after={after}")
                for entry in entries:
                    print(entry["text"], end="", flush=True)
                    after = entry["seq"]
                if not args.follow or (not entries and api("GET", f"/jobs/{args.id}")["state"] in DONE):
                    break
                time.sleep(1)
        elif args.action == "results":
            job = api("GET", f"/jobs/{args.id}")
            for name in job["outputs"]:
                content = api("GET", f"/jobs/{args.id}/output/{quote(name, safe='/')}",
                              raw=True, timeout=300)
                destination = args.dir / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(content)
                print(destination)
    except (RuntimeError, ValueError, OSError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
