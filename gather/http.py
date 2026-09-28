"""Small JSON client shared by the CLI and worker."""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def call(method, path, data=None, *, url=None, token=None, raw=False, headers=None, timeout=15):
    base = (url or os.environ.get("GATHER_URL", "http://127.0.0.1:8000")).rstrip("/")
    secret = token or os.environ.get("GATHER_TOKEN")
    if not secret:
        raise RuntimeError("Set GATHER_TOKEN first")
    body = data if raw else (json.dumps(data).encode() if data is not None else None)
    request = Request(base + path, data=body, method=method)
    request.add_header("Authorization", "Bearer " + secret)
    if body is not None and not raw:
        request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urlopen(request, timeout=timeout) as response:
            content = response.read()
            return content if raw else json.loads(content)
    except HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode(errors='replace')}") from exc
    except URLError as exc:
        raise RuntimeError(f"Coordinator unavailable: {exc.reason}") from exc
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"Coordinator request failed: {exc}") from exc
