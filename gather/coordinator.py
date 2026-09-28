"""Coordinator API, durable queue, and first-fit scheduler."""

import asyncio
import json
import os
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from urllib.parse import unquote

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from gather.bundle import MAX_ARCHIVE, validate_bundle


DB = Path(os.environ.get("GATHER_DB", "gather.db"))
DATA = Path(os.environ.get("GATHER_DATA_DIR", "gather-data"))
OFFLINE_AFTER = 60


@contextmanager
def connection():
    DB.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        with db:
            yield db
    finally:
        db.close()


def initialize():
    with connection() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, session TEXT NOT NULL,
                resources TEXT NOT NULL, last_seen REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, command TEXT NOT NULL, cpu INTEGER NOT NULL,
                ram_mb INTEGER NOT NULL, gpu_mb INTEGER NOT NULL,
                outputs TEXT NOT NULL, has_input INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL, worker_id TEXT,
                worker_session TEXT, gpu_uuid TEXT, exit_code INTEGER,
                error TEXT, created REAL NOT NULL, updated REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS logs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
                text TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS logs_job ON logs(job_id, seq);
        """)
        if "has_input" not in {row["name"] for row in db.execute("PRAGMA table_info(jobs)")}:
            db.execute("ALTER TABLE jobs ADD COLUMN has_input INTEGER NOT NULL DEFAULT 0")
    DATA.mkdir(parents=True, exist_ok=True)


def authenticate(request: Request):
    token = os.environ.get("GATHER_TOKEN")
    if not token:
        raise HTTPException(503, "Coordinator needs GATHER_TOKEN")
    supplied = request.headers.get("Authorization", "")
    if not secrets.compare_digest(supplied, "Bearer " + token):
        raise HTTPException(401, "Invalid token")


def valid_output(name):
    if (not name or name.startswith("/") or "\\" in name or ":" in name or
            "\x00" in name or any(p in ("", ".", "..") for p in name.split("/"))):
        raise HTTPException(400, "Outputs must be relative file paths")
    return name


def row_job(row):
    result = dict(row)
    result["command"] = json.loads(result["command"])
    result["outputs"] = json.loads(result["outputs"])
    result["has_input"] = bool(result["has_input"])
    return result


def assigned(db, job_id, request):
    job = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        raise HTTPException(404, "Job not found")
    if (job["worker_id"] != request.headers.get("X-Worker-Id") or
            job["worker_session"] != request.headers.get("X-Worker-Session") or
            job["state"] not in ("assigned", "running")):
        raise HTTPException(409, "Job is not assigned to this worker session")
    return job


def expire_workers():
    now = time.time()
    with connection() as db:
        db.execute("""
            UPDATE jobs SET state='lost', error='Worker heartbeat expired', updated=?
            WHERE state IN ('assigned','running') AND worker_id IN
                (SELECT id FROM workers WHERE last_seen < ?)
        """, (now, now - OFFLINE_AFTER))


@asynccontextmanager
async def lifespan(_app):
    initialize()
    async def reap():
        while True:
            await asyncio.sleep(5)
            expire_workers()
    task = asyncio.create_task(reap())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(title="Gather", lifespan=lifespan)


@app.get("/capabilities", dependencies=[Depends(authenticate)])
def capabilities():
    return {"input_bundle": True}


class Registration(BaseModel):
    id: str
    name: str
    session: str
    resources: dict


class JobSpec(BaseModel):
    command: list[str]
    cpu: int = Field(default=1, ge=1)
    ram_mb: int = Field(default=0, ge=0)
    gpu_mb: int = Field(default=1, ge=0)
    outputs: list[str] = Field(default_factory=list)
    has_input: bool = False


@app.post("/workers/register", dependencies=[Depends(authenticate)])
def register(worker: Registration):
    now = time.time()
    with connection() as db:
        old = db.execute("SELECT session FROM workers WHERE id=?", (worker.id,)).fetchone()
        if old and old["session"] != worker.session:
            db.execute("""UPDATE jobs SET state='lost', error='Worker restarted', updated=?
                          WHERE worker_id=? AND state IN ('assigned','running')""", (now, worker.id))
        db.execute("""INSERT INTO workers VALUES (?,?,?,?,?)
                      ON CONFLICT(id) DO UPDATE SET name=excluded.name, session=excluded.session,
                      resources=excluded.resources, last_seen=excluded.last_seen""",
                   (worker.id, worker.name, worker.session, json.dumps(worker.resources), now))
    return {"ok": True}


@app.post("/workers/{worker_id}/heartbeat", dependencies=[Depends(authenticate)])
def heartbeat(worker_id: str, worker: Registration):
    with connection() as db:
        changed = db.execute("""UPDATE workers SET resources=?, last_seen=?
            WHERE id=? AND session=?""", (json.dumps(worker.resources), time.time(), worker_id, worker.session))
        if not changed.rowcount:
            raise HTTPException(409, "Worker session changed")
    return {"ok": True}


@app.get("/workers", dependencies=[Depends(authenticate)])
def workers():
    with connection() as db:
        rows = db.execute("SELECT * FROM workers ORDER BY name").fetchall()
    return [{"id": r["id"], "name": r["name"], "online": r["last_seen"] >= time.time() - OFFLINE_AFTER,
             "resources": json.loads(r["resources"])} for r in rows]


@app.post("/jobs", dependencies=[Depends(authenticate)])
def submit(spec: JobSpec):
    if not spec.command or any(not arg for arg in spec.command):
        raise HTTPException(400, "Command must contain nonempty arguments")
    for name in spec.outputs:
        valid_output(name)
    job_id = uuid.uuid4().hex
    now = time.time()
    with connection() as db:
        db.execute("""INSERT INTO jobs
            (id,command,cpu,ram_mb,gpu_mb,outputs,has_input,state,created,updated)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (job_id, json.dumps(spec.command), spec.cpu, spec.ram_mb, spec.gpu_mb,
             json.dumps(spec.outputs), int(spec.has_input),
             "uploading" if spec.has_input else "queued", now, now))
    return {"id": job_id, "state": "uploading" if spec.has_input else "queued"}


@app.put("/jobs/{job_id}/input", dependencies=[Depends(authenticate)])
async def upload_input(job_id: str, request: Request):
    with connection() as db:
        current = db.execute("SELECT has_input,state FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not current or not current["has_input"]:
        raise HTTPException(404, "Input job not found")
    destination = DATA / job_id / "input.zip"
    if current["state"] == "queued" and destination.is_file():
        return {"ok": True}
    if current["state"] != "uploading":
        raise HTTPException(409, "Input can no longer be uploaded")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(uuid.uuid4().hex + ".tmp")
    size = 0
    try:
        with temporary.open("wb") as output:
            async for chunk in request.stream():
                size += len(chunk)
                if size > MAX_ARCHIVE:
                    raise HTTPException(413, "Input ZIP exceeds 50 MiB")
                output.write(chunk)
        try:
            validate_bundle(temporary)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        with connection() as db:
            current = db.execute("SELECT state FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not current or current["state"] != "uploading":
                raise HTTPException(409, "Input can no longer be uploaded")
            os.replace(temporary, destination)
            db.execute("UPDATE jobs SET state='queued', updated=? WHERE id=?", (time.time(), job_id))
    finally:
        temporary.unlink(missing_ok=True)
    return {"ok": True}


@app.get("/jobs/{job_id}/input", dependencies=[Depends(authenticate)])
def download_input(job_id: str):
    with connection() as db:
        current = db.execute("SELECT has_input,state FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not current or not current["has_input"] or current["state"] == "uploading":
        raise HTTPException(404, "Input not ready")
    path = DATA / job_id / "input.zip"
    if not path.is_file():
        raise HTTPException(404, "Input file missing")
    return FileResponse(path)


@app.get("/jobs", dependencies=[Depends(authenticate)])
def jobs():
    with connection() as db:
        rows = db.execute("SELECT * FROM jobs ORDER BY created DESC").fetchall()
    return [row_job(r) for r in rows]


@app.get("/jobs/{job_id}", dependencies=[Depends(authenticate)])
def job(job_id: str):
    with connection() as db:
        row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Job not found")
    return row_job(row)


@app.post("/workers/{worker_id}/claim", dependencies=[Depends(authenticate)])
def claim(worker_id: str, request: Request):
    session = request.headers.get("X-Worker-Session")
    with connection() as db:
        db.execute("BEGIN IMMEDIATE")
        worker = db.execute("SELECT * FROM workers WHERE id=? AND session=?", (worker_id, session)).fetchone()
        if not worker or worker["last_seen"] < time.time() - OFFLINE_AFTER:
            raise HTTPException(409, "Worker is not registered or heartbeat expired")
        busy = db.execute("""SELECT * FROM jobs WHERE worker_id=?
            AND state IN ('assigned','running')""", (worker_id,)).fetchone()
        if busy:
            # A lost claim response must not strand an assigned job.
            return {"job": row_job(busy) if busy["state"] == "assigned" and
                    busy["worker_session"] == session else None}
        resources = json.loads(worker["resources"])
        for candidate in db.execute("SELECT * FROM jobs WHERE state='queued' ORDER BY created, id"):
            if candidate["has_input"] and resources.get("protocol", 1) < 2:
                continue
            if candidate["cpu"] > resources.get("cpu", 0) or candidate["ram_mb"] > resources.get("ram_mb", 0):
                continue
            gpu = None
            if candidate["gpu_mb"]:
                gpu = next((g for g in resources.get("gpus", [])
                            if g["free_mb"] >= candidate["gpu_mb"]), None)
                if not gpu:
                    continue
            db.execute("""UPDATE jobs SET state='assigned', worker_id=?, worker_session=?, gpu_uuid=?, updated=?
                WHERE id=?""", (worker_id, session, gpu["uuid"] if gpu else None,
                                 time.time(), candidate["id"]))
            return {"job": row_job(db.execute("SELECT * FROM jobs WHERE id=?", (candidate["id"],)).fetchone())}
    return {"job": None}


@app.post("/jobs/{job_id}/start", dependencies=[Depends(authenticate)])
def start(job_id: str, request: Request):
    with connection() as db:
        assigned(db, job_id, request)
        db.execute("UPDATE jobs SET state='running', updated=? WHERE id=?", (time.time(), job_id))
    return {"ok": True}


@app.post("/jobs/{job_id}/logs", dependencies=[Depends(authenticate)])
async def append_logs(job_id: str, request: Request):
    content = await request.body()
    if len(content) > 65536:
        raise HTTPException(413, "Log chunk too large")
    with connection() as db:
        assigned(db, job_id, request)
        db.execute("INSERT INTO logs(job_id,text) VALUES (?,?)", (job_id, content.decode(errors="replace")))
    return {"ok": True}


@app.get("/jobs/{job_id}/logs", dependencies=[Depends(authenticate)])
def read_logs(job_id: str, after: int = 0):
    with connection() as db:
        if not db.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,)).fetchone():
            raise HTTPException(404, "Job not found")
        rows = db.execute("SELECT seq,text FROM logs WHERE job_id=? AND seq>? ORDER BY seq LIMIT 200",
                          (job_id, after)).fetchall()
    return [dict(r) for r in rows]


@app.put("/jobs/{job_id}/output/{name:path}", dependencies=[Depends(authenticate)])
async def upload_output(job_id: str, name: str, request: Request):
    name = valid_output(unquote(name))
    content = await request.body()
    if len(content) > 100 * 1024 * 1024:
        raise HTTPException(413, "Output exceeds 100 MiB")
    with connection() as db:
        current = assigned(db, job_id, request)
        if name not in json.loads(current["outputs"]):
            raise HTTPException(400, "Output was not declared")
    destination = DATA / job_id / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    return {"ok": True}


@app.get("/jobs/{job_id}/output/{name:path}", dependencies=[Depends(authenticate)])
def download_output(job_id: str, name: str):
    name = valid_output(unquote(name))
    with connection() as db:
        current = db.execute("SELECT outputs FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not current or name not in json.loads(current["outputs"]):
        raise HTTPException(404, "Output not found")
    path = DATA / job_id / name
    if not path.is_file():
        raise HTTPException(404, "Output not uploaded")
    return FileResponse(path)


@app.post("/jobs/{job_id}/finish", dependencies=[Depends(authenticate)])
async def finish(job_id: str, request: Request):
    result = await request.json()
    exit_code = result.get("exit_code")
    if not isinstance(exit_code, int):
        raise HTTPException(400, "exit_code must be an integer")
    with connection() as db:
        assigned(db, job_id, request)
        db.execute("""UPDATE jobs SET state=?, exit_code=?, error=?, updated=? WHERE id=?""",
                   ("succeeded" if exit_code == 0 else "failed", exit_code,
                    result.get("error"), time.time(), job_id))
    return {"ok": True}
