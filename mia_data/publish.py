"""Copying verified crops into the dataset folder under storage_root, and keeping its manifest.json current.

Publication is idempotent and safe against a second attempt running at the same time: each crop is staged
in a private temp directory, re-hashed, and renamed into place; a crop that already exists with the same
hashes counts as published, one with different hashes is a Conflict for a human.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import shutil
import time
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

from autodev.jobs import read_untrusted

CROP_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
MAX_FILE = 64 << 20  # a crop file is a 256x256 PNG or a small JSON


class Conflict(RuntimeError):
    """A crop exists with different checksums (or its source changed since verify): needs a human."""


def check_crop_id(crop_id: Any) -> str:
    if not (isinstance(crop_id, str) and CROP_ID_RE.fullmatch(crop_id)):
        raise ValueError(f"bad crop_id {crop_id!r}")
    return crop_id


def _hashes(directory: Path) -> dict[str, str]:
    """relative path -> sha256 of every file under `directory`."""
    return {path.relative_to(directory).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*") if path.is_file()}


def _assert_same(dest: Path, files: dict, crop_id: str) -> None:
    if _hashes(dest) != files:
        raise Conflict(f"{crop_id} is already published with different checksums")


def publish(dataset_dir: Path, src_dir: Path, verified: list[dict], withdrawn: dict[str, str], *,
            tmp_max_age_s: float) -> dict:
    """Copy the verified files of each crop from `src_dir` (a crop job's crops/) into `dataset_dir`.

    `verified` is [{"crop_id", "files": {rel: sha256}}] from read_verify; only those files are copied, and
    they must still hash the same. Temp directories older than `tmp_max_age_s` are left by dead attempts and
    are cleaned up. Returns {"published": [ids], "already": [ids], "total": count in the dataset}."""
    for crop in verified:
        check_crop_id(crop["crop_id"])
        if bad := [rel for rel in crop["files"]
                   if not rel or PurePosixPath(rel).is_absolute() or ".." in PurePosixPath(rel).parts]:
            raise ValueError(f"{crop['crop_id']}: bad file paths {bad}")
        if not {"crop.json", "raw.png"} <= set(crop["files"]):
            raise ValueError(f"{crop['crop_id']}: crop.json and raw.png must be verified")
    dataset_dir.mkdir(parents=True, exist_ok=True)
    now = time.time()
    for stale in dataset_dir.glob(".tmp-*"):  # younger ones may belong to a live attempt
        try:
            if now - stale.lstat().st_mtime > tmp_max_age_s:
                shutil.rmtree(stale) if stale.is_dir() else stale.unlink()
        except FileNotFoundError:  # another attempt got there first
            pass
    published, already = [], []
    try:
        for crop in verified:
            crop_id, files, dest = crop["crop_id"], crop["files"], dataset_dir / crop["crop_id"]
            if dest.exists():
                _assert_same(dest, files, crop_id)
                already.append(crop_id)
                continue
            tmp = dataset_dir / f".tmp-{crop_id}-{uuid.uuid4().hex}"
            for rel in files:
                data = read_untrusted(src_dir, f"{crop_id}/{rel}", MAX_FILE, binary=True)  # job output: no symlinks
                if data is None:
                    shutil.rmtree(tmp, ignore_errors=True)
                    raise Conflict(f"{crop_id}/{rel} is missing or unreadable in the crop job's output")
                (tmp / rel).parent.mkdir(parents=True, exist_ok=True)
                (tmp / rel).write_bytes(data)
            if _hashes(tmp) != files:
                shutil.rmtree(tmp)
                raise Conflict(f"{crop_id}: source files differ from the verified checksums")
            try:
                os.rename(tmp, dest)
                published.append(crop_id)
            except OSError as error:  # another attempt renamed first
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                shutil.rmtree(tmp)
                _assert_same(dest, files, crop_id)
                already.append(crop_id)
    finally:  # a Conflict leaves the manifest matching what is published
        total = reindex(dataset_dir, withdrawn)
    return {"published": published, "already": already, "total": total}


def reindex(dataset_dir: Path, withdrawn: dict[str, str]) -> int:
    """Rebuild manifest.json from every <crop>/crop.json (dot-dirs ignored). Returns the crop count.

    `withdrawn` maps snippet name -> reason; crops made with a withdrawn snippet are marked, not removed.
    Each manifest entry carries "files" (rel -> sha256), which `harness verify --existing` reads for its
    duplicate-checksum check."""
    crops = []
    for crop_dir in sorted(path for path in dataset_dir.iterdir() if path.is_dir() and not path.name.startswith(".")):
        if not (crop_dir / "crop.json").is_file():
            continue
        crop_json = json.loads((crop_dir / "crop.json").read_text())
        withdrawn_snippet = next((snippet["name"] for snippet in crop_json.get("snippets") or []
                                  if snippet.get("name") in withdrawn), None)
        crops.append({**crop_json, "crop_id": crop_dir.name, "files": _hashes(crop_dir),
                      "withdrawn": withdrawn_snippet is not None, "withdrawn_reason": withdrawn.get(withdrawn_snippet)})
    tmp = dataset_dir / f".tmp-manifest-{uuid.uuid4().hex}.json"  # per writer: a zombie attempt can't interleave
    tmp.write_text(json.dumps({"crops": crops, "count": len(crops)}, indent=1))
    os.replace(tmp, dataset_dir / "manifest.json")
    return len(crops)
