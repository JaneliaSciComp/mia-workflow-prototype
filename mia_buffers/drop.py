"""The manual drop (spec §3): which file name a human gives each URL, the raw cache's key for a dropped file,
and the `files.json` manifest the harness reads instead of a HEAD request.

`cache_key` repeats the harness's `mia_harness.io._key` so a dropped file lands where `io.fetch(url)` looks.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

MANIFEST = "files.json"
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._()-]{0,120}")


def _file_url(url: str) -> str:
    """The URL without a `::member` suffix (the harness's zip-member syntax)."""
    path_start = url.find("/", url.find("://") + 3)
    member = url.find("::", path_start) if path_start >= 0 else -1
    return url if member < 0 else url[:member]


def drop_names(urls: list[str]) -> list[tuple[str, str]]:
    """(file URL, name under incoming/) per distinct file: the URL's last path segment when it looks like a plain
    unique file name (an extension, no query string), else `file-<index>`."""
    files = list(dict.fromkeys(_file_url(url) for url in urls))
    wanted = []
    for url in files:
        parts = urlsplit(url)
        name = unquote(parts.path.rsplit("/", 1)[-1]) if not parts.query else ""
        wanted.append(name if "." in name.strip(".") and _SAFE_NAME.fullmatch(name) else "")
    counts = {name: wanted.count(name) for name in wanted}
    return [
        (url, name if name and counts[name] == 1 else f"file-{i}")
        for i, (url, name) in enumerate(zip(files, wanted, strict=True))
    ]


def cache_key(url: str, validators: dict) -> str:
    return hashlib.sha256(f"{url}\n{json.dumps(validators, sort_keys=True)}".encode()).hexdigest()


def read_manifest(cache_dir: Path) -> dict:
    path = Path(cache_dir) / MANIFEST
    return json.loads(path.read_text()) if path.is_file() else {}


def merge_manifest(cache_dir: Path, entries: dict[str, dict]) -> None:
    """Add `entries` (url -> {validators, source, at}) to files.json, written atomically."""
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    merged = {**read_manifest(cache_dir), **entries}
    tmp = cache_dir / f".{MANIFEST}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(merged, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, cache_dir / MANIFEST)
