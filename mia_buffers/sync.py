"""The datasets sync: which records have a prepared dataset folder under storage_root."""

from __future__ import annotations

import logging
from pathlib import Path

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
