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


def cache_missing(manifest: dict, urls: Iterable[str], stream: Iterable[str] = ()) -> tuple[bool, list[str]]:
    """(ready, missing): the urls of `urls` that files.json lacks, leaving out the `stream` urls (the harness reads
    those in place, they are never cached). A `<zip>::<member or glob>` url counts only as itself: `harness fetch`
    lists it once its members are cached, while the zip's own entry says no more than that a HEAD recorded its
    validators. Ready when none is missing; with no urls at all, when files.json lists anything."""
    urls, streamed = list(urls), set(stream)
    wanted = dict.fromkeys(url for url in urls if url not in streamed)
    missing = [url for url in wanted if url not in manifest]
    return (not missing if urls else bool(manifest)), missing


def raw_asset(cache_dir: Path, urls: Iterable[str], stream: Iterable[str] = ()) -> dict:
    """The raw-cache asset row: path, bytes, files.json entries, and `ready` when every url of `urls` but the
    `stream` ones is cached."""
    manifest = read_files_json(cache_dir)
    ready, _ = cache_missing(manifest, urls, stream)
    return {
        "path": str(cache_dir),
        "bytes": dir_bytes(cache_dir),
        "files": file_entries(manifest),
        "state": "ready" if ready else "partial",
    }
