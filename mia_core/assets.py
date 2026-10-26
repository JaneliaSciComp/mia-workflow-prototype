"""Small helpers for raw-cache asset rows: the states, the harness's files.json and the row both workers post."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from mia_core.fs import dir_bytes
from mia_core.untrusted import loads, read_untrusted

ASSET_STATES = ("fetching", "staging", "waiting-drop", "ready", "partial", "missing", "evicted")
MANIFEST = "files.json"


def read_files_json(cache_dir: Path) -> dict:
    """The harness's files.json (url -> {validators, source}); {} when missing or unreadable."""
    data = loads(read_untrusted(cache_dir, MANIFEST, 1 << 20))
    return data if isinstance(data, dict) else {}


def whole_file_url(url: str) -> str:
    """The file a url names: a zip member url `<zip>::<member>` is cached as its zip."""
    return url.split("::", 1)[0]


def raw_asset(cache_dir: Path, urls: Iterable[str]) -> dict:
    """The raw-cache asset row: path, bytes, files.json entries, and `ready` when every url of `urls` is cached."""
    manifest = read_files_json(cache_dir)
    wanted = {whole_file_url(url) for url in urls}
    ready = wanted <= manifest.keys() if wanted else bool(manifest)
    return {
        "path": str(cache_dir),
        "bytes": dir_bytes(cache_dir),
        "files": {
            str(url)[:500]: {"size": (entry.get("validators") or {}).get("size"), "source": entry.get("source")}
            for url, entry in manifest.items()
            if isinstance(entry, dict)
        },
        "state": "ready" if ready else "partial",
    }
