"""Small helpers for raw-cache asset rows: the states, the harness's files.json and the row both workers post."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from mia_core.fs import dir_bytes
from mia_core.untrusted import loads, read_untrusted, text

ASSET_STATES = ("fetching", "staging", "waiting-drop", "ready", "partial", "missing", "evicted")
MANIFEST = "files.json"


def read_files_json(cache_dir: Path) -> dict:
    """The harness's files.json (url -> {validators, source}); {} when missing or unreadable."""
    data = loads(read_untrusted(cache_dir, MANIFEST, 1 << 20))
    return data if isinstance(data, dict) else {}


def whole_file_url(url: str) -> str:
    """The file a url names: a zip member url `<zip>::<member>` is cached as its zip."""
    return url.split("::", 1)[0]


def _file_entry(entry: dict) -> dict:
    """The size and source of one files.json entry, reduced to bounded typed values."""
    validators = entry.get("validators")
    size = validators.get("size") if isinstance(validators, dict) else None
    return {
        "size": size if isinstance(size, int) and not isinstance(size, bool) else None,
        "source": text(entry.get("source"), 50) or None,
    }


def file_entries(manifest: dict) -> dict:
    """A files.json mapping reduced to bounded urls and typed entries, as raw rows carry it."""
    return {text(url, 500): _file_entry(entry) for url, entry in manifest.items() if isinstance(entry, dict)}


def raw_asset(cache_dir: Path, urls: Iterable[str]) -> dict:
    """The raw-cache asset row: path, bytes, files.json entries, and `ready` when every url of `urls` is cached."""
    manifest = read_files_json(cache_dir)
    wanted = {whole_file_url(url) for url in urls}
    ready = wanted <= manifest.keys() if wanted else bool(manifest)
    return {
        "path": str(cache_dir),
        "bytes": dir_bytes(cache_dir),
        "files": file_entries(manifest),
        "state": "ready" if ready else "partial",
    }
