"""Small, bounded ZIP bundles for job inputs."""

import io
import shutil
import stat
import zipfile
from pathlib import Path


MAX_ARCHIVE = 50 * 1024 * 1024
MAX_UNPACKED = 200 * 1024 * 1024
MAX_FILES = 1000


def _members(archive):
    entries = archive.infolist()
    if not entries or len(entries) > MAX_FILES:
        raise ValueError(f"Input must contain 1-{MAX_FILES} files")
    total = 0
    seen = set()
    for entry in entries:
        name = entry.filename
        parts = name.split("/")
        mode = (entry.external_attr >> 16) & 0o170000
        if (entry.is_dir() or not name or "\\" in name or ":" in name or "\x00" in name or
                any(part in ("", ".", "..") for part in parts) or
                mode == stat.S_IFLNK or entry.flag_bits & 1):
            raise ValueError(f"Unsafe input path: {name!r}")
        folded = name.casefold()
        if folded in seen:
            raise ValueError(f"Duplicate input path: {name}")
        seen.add(folded)
        total += entry.file_size
        if total > MAX_UNPACKED:
            raise ValueError("Input expands beyond 200 MiB")
    return entries


def validate_bundle(source):
    try:
        with zipfile.ZipFile(source) as archive:
            _members(archive)
    except zipfile.BadZipFile as exc:
        raise ValueError("Input is not a valid ZIP archive") from exc


def pack_directory(directory: Path):
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError(f"Input directory does not exist: {directory}")
    buffer = io.BytesIO()
    count = total = 0
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(directory.rglob("*")):
            if path.is_symlink():
                raise ValueError(f"Input contains a symlink: {path}")
            if not path.is_file():
                continue
            count += 1
            total += path.stat().st_size
            if count > MAX_FILES or total > MAX_UNPACKED:
                raise ValueError("Input exceeds 1000 files or 200 MiB unpacked")
            archive.write(path, path.relative_to(directory).as_posix())
            if buffer.tell() > MAX_ARCHIVE:
                raise ValueError("Input ZIP exceeds 50 MiB")
    data = buffer.getvalue()
    if len(data) > MAX_ARCHIVE:
        raise ValueError("Input ZIP exceeds 50 MiB")
    validate_bundle(io.BytesIO(data))
    return data


def extract_bundle(data: bytes, workspace: Path):
    if len(data) > MAX_ARCHIVE:
        raise ValueError("Input ZIP exceeds 50 MiB")
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = _members(archive)
            root = workspace.resolve()
            total = 0
            for entry in entries:
                destination = (workspace / entry.filename).resolve()
                if root not in destination.parents:
                    raise ValueError(f"Unsafe input path: {entry.filename!r}")
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(entry) as source, destination.open("wb") as target:
                    while chunk := source.read(1024 * 1024):
                        total += len(chunk)
                        if total > MAX_UNPACKED:
                            raise ValueError("Input expands beyond 200 MiB")
                        target.write(chunk)
    except zipfile.BadZipFile as exc:
        raise ValueError("Input is not a valid ZIP archive") from exc
