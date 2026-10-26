"""The syncs: which records have a prepared dataset folder under storage_root, and which raw caches and staged
artifacts exist under raw_root and staged_root."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from mia_core.assets import file_entries, read_files_json
from mia_core.fs import dir_bytes
from mia_core.untrusted import loads, read_untrusted, text
from mia_data.publish import prepared_row

log = logging.getLogger(__name__)


def scan_storage(storage_root: Path) -> dict[str, dict]:
    """Record id → its prepared row (without "record") for every immediate, non-dot folder that is prepared.
    A record prepared in two folders keeps the first by folder name."""
    found: dict[str, dict] = {}
    for folder in sorted(storage_root.iterdir()):
        if folder.name.startswith(".") or not folder.is_dir():
            continue
        row = prepared_row(folder)
        if row is None:
            continue
        record = row.pop("record")
        if record in found:
            log.warning(
                "record %s is prepared in %s and %s; keeping the first", record, found[record]["folder"], folder
            )
            continue
        found[record] = row
    return found


STAGE_JSON_LIMIT = 64 << 20  # as mia_buffers.activities.STAGE_JSON_LIMIT; activities is too heavy to import here
AssetKey = tuple[str, str]


def _children(root: Path | None) -> list[Path] | None:
    """The non-dot entries of `root`, sorted; None when `root` is unset, missing or has none (unmounted, wiped)."""
    if root is None:
        return None
    try:
        entries = sorted(path for path in root.iterdir() if not path.name.startswith("."))
    except OSError:
        return None
    return entries or None


def _bytes(path: Path, known: dict | None) -> int:
    """`path`'s size, re-summed only when its mtime is newer than the last scan of it."""
    # ponytail: a nested write does not bump the top mtime; the keeper's full re-sum every N passes is the upgrade
    try:
        unchanged = path.stat().st_mtime <= ((known or {}).get("scanned_at") or 0)
    except OSError:  # removed while we walked
        unchanged = False
    if unchanged and known and isinstance(known.get("bytes"), int):
        return known["bytes"]
    return dir_bytes(path)


def _artifact_fields(report: Any) -> dict:
    """The identity fields of a stage.json's `artifact` block, reduced to bounded typed values."""
    block = report.get("artifact") if isinstance(report, dict) else None
    if not isinstance(block, dict):
        return {}
    fields: dict[str, Any] = {
        name: text(block.get(name), 200) for name in ("binding_hash", "harness_commit", "env_digest")
    }
    voxel = block.get("voxel_size_nm")
    fields["voxel_size_nm"] = (
        {
            text(axis, 10): float(size)
            for axis, size in voxel.items()
            if isinstance(size, int | float) and not isinstance(size, bool)
        }
        if isinstance(voxel, dict)
        else {}
    )
    return fields


def scan_assets(
    raw_root: Path | None, staged_root: Path | None, known: dict[AssetKey, dict]
) -> tuple[dict[AssetKey, dict], list[Path], list[str]]:
    """(kind, key) → row for every raw cache `raw_root/<record>` and staged artifact
    `staged_root/<levels key>/<record>/<artifact>/stage.json`; the `.building-*` directories found; and the kinds
    whose root is unset, missing or empty, so a sync never marks a whole unmounted root missing."""
    now = time.time()
    found: dict[AssetKey, dict] = {}
    building: list[Path] = []
    skipped: list[str] = []
    raw_dirs = _children(raw_root)
    if raw_dirs is None:
        skipped.append("raw")
    for folder in raw_dirs or []:
        if folder.is_dir():
            row = {"path": str(folder), "bytes": _bytes(folder, known.get(("raw", folder.name)))}
            found[("raw", folder.name)] = {**row, "files": file_entries(read_files_json(folder)), "scanned_at": now}
    levels_dirs = _children(staged_root)
    if levels_dirs is None:
        skipped.append("staged")
    for levels_dir in levels_dirs or []:
        for record_dir in _children(levels_dir) or []:  # a stray file has no children
            if (record_dir / "stage.json").is_file():
                log.info("skipping %s: a pre-artifact stage.json directly under the record", record_dir)
                continue
            building += sorted(record_dir.glob(".building-*"))
            for artifact_dir in _children(record_dir) or []:
                if not (artifact_dir / "stage.json").is_file():
                    continue
                key = ("staged", artifact_dir.name)
                report = loads(read_untrusted(artifact_dir, "stage.json", STAGE_JSON_LIMIT))
                found[key] = {
                    "path": str(artifact_dir),
                    "bytes": _bytes(artifact_dir, known.get(key)),
                    "record": record_dir.name,
                    "levels_key": levels_dir.name,
                    "artifact": artifact_dir.name,
                    **_artifact_fields(report),
                    "scanned_at": now,
                }
    return found, building, skipped
