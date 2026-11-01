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
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from mia_core.untrusted import loads, read_untrusted

CROP_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
MAX_FILE = 64 << 20  # a crop file is a 256x256 PNG or a small JSON
MAX_INDEX_FILE = 1 << 20  # binding.yaml or manifest.json, read by the service's scan of storage_root


class Conflict(RuntimeError):
    """A crop exists with different checksums (or its source changed since verify): needs a human."""


def check_crop_id(crop_id: Any) -> str:
    if not (isinstance(crop_id, str) and CROP_ID_RE.fullmatch(crop_id)):
        raise ValueError(f"bad crop_id {crop_id!r}")
    return crop_id


PREVIEW_FILE = "overlay.png"  # beside a crop's verified files, outside its checksums: a picture for the web UI
MAX_PREVIEWS = 8  # crop ids a prepared row lists for the page's thumbnail


def _hashes(directory: Path) -> dict[str, str]:
    """relative path -> sha256 of every verified file under `directory` (not the preview, not a temp file)."""
    return {
        path.relative_to(directory).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*")
        if path.is_file() and path.name != PREVIEW_FILE and not path.name.startswith(".")
    }


def _place_preview(crop_dir: Path, crop_id: str, previews: Sequence[Path]) -> None:
    """Copy the crop's overlay.png from the first preview dir that has one; nothing when none does or it is there."""
    if (crop_dir / PREVIEW_FILE).exists():
        return
    for preview_dir in previews:
        if (data := read_untrusted(preview_dir, f"{crop_id}/{PREVIEW_FILE}", MAX_FILE, binary=True)) is not None:
            tmp = crop_dir / f".tmp-{PREVIEW_FILE}-{uuid.uuid4().hex}"
            tmp.write_bytes(data)
            os.replace(tmp, crop_dir / PREVIEW_FILE)
            return


def _assert_same(dest: Path, files: dict, crop_id: str) -> None:
    if _hashes(dest) != files:
        raise Conflict(f"{crop_id} is already published with different checksums")


def publish(
    dataset_dir: Path,
    src_dir: Path,
    verified: list[dict],
    withdrawn: dict[str, str],
    *,
    tmp_max_age_s: float,
    binding: Path | None = None,
    previews: Sequence[Path] = (),
) -> dict:
    """Copy the verified files of each crop from `src_dir` (a crop job's crops/) into `dataset_dir`, and
    `binding` (the approved binding YAML), when given, to `dataset_dir/binding.yaml`. `previews` are verify jobs'
    preview/ dirs: a crop's overlay.png found there goes beside its files (also for crops published earlier).

    `verified` is [{"crop_id", "files": {rel: sha256}}] from read_verify; only those files are copied, and
    they must still hash the same. Temp directories older than `tmp_max_age_s` are left by dead attempts and
    are cleaned up. Returns {"published": [ids], "already": [ids], "total": count in the dataset}."""
    for crop in verified:
        check_crop_id(crop["crop_id"])
        if bad := [
            rel
            for rel in crop["files"]
            if not rel or PurePosixPath(rel).is_absolute() or ".." in PurePosixPath(rel).parts
        ]:
            raise ValueError(f"{crop['crop_id']}: bad file paths {bad}")
        if not {"crop.json", "raw.png"} <= set(crop["files"]):
            raise ValueError(f"{crop['crop_id']}: crop.json and raw.png must be verified")
    dataset_dir.mkdir(parents=True, exist_ok=True)
    if binding is not None:
        tmp = dataset_dir / f".tmp-binding-{uuid.uuid4().hex}"
        shutil.copyfile(binding, tmp)
        os.replace(tmp, dataset_dir / "binding.yaml")
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
            _place_preview(tmp, crop_id, previews)
            try:
                os.rename(tmp, dest)
                published.append(crop_id)
            except OSError as error:  # another attempt renamed first
                if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                shutil.rmtree(tmp)
                _assert_same(dest, files, crop_id)
                already.append(crop_id)
        for crop_dir in dataset_dir.iterdir():  # crops of earlier rounds too, when a preview dir has theirs
            if crop_dir.is_dir() and not crop_dir.name.startswith("."):
                _place_preview(crop_dir, crop_dir.name, previews)
    finally:  # a Conflict leaves the manifest matching what is published
        total = reindex(dataset_dir, withdrawn)
    return {"published": published, "already": already, "total": total}


def reindex(dataset_dir: Path, withdrawn: dict[str, str]) -> int:
    """Rebuild manifest.json from every <crop>/crop.json (dot-dirs ignored) and record the published binding's
    sha256 (`"binding": {"sha256": ...}`, null when none is published). Returns the crop count.

    `withdrawn` maps snippet name -> reason; crops made with a withdrawn snippet are marked, not removed.
    Each manifest entry carries "files" (rel -> sha256), which `harness verify --existing` reads for its
    duplicate-checksum check."""
    crops = []
    for crop_dir in sorted(path for path in dataset_dir.iterdir() if path.is_dir() and not path.name.startswith(".")):
        if not (crop_dir / "crop.json").is_file():
            continue
        crop_json = json.loads((crop_dir / "crop.json").read_text())
        withdrawn_snippet = next(
            (snippet["name"] for snippet in crop_json.get("snippets") or [] if snippet.get("name") in withdrawn), None
        )
        crops.append(
            {
                **crop_json,
                "crop_id": crop_dir.name,
                "files": _hashes(crop_dir),
                "withdrawn": withdrawn_snippet is not None,
                "withdrawn_reason": withdrawn.get(withdrawn_snippet) if withdrawn_snippet is not None else None,
            }
        )
    tmp = dataset_dir / f".tmp-manifest-{uuid.uuid4().hex}.json"  # per writer: a zombie attempt can't interleave
    binding_file = dataset_dir / "binding.yaml"
    binding = {"sha256": hashlib.sha256(binding_file.read_bytes()).hexdigest()} if binding_file.is_file() else None
    tmp.write_text(json.dumps({"crops": crops, "count": len(crops), "binding": binding}, indent=1))
    os.replace(tmp, dataset_dir / "manifest.json")
    return len(crops)


def prepared_row(dataset_dir: Path) -> dict | None:
    """The registry row of a prepared dataset folder, or None: prepared means a binding.yaml naming its record and
    a manifest.json whose binding sha256 is that file's (a newer binding with crops not yet re-verified is not)."""
    binding_bytes = read_untrusted(dataset_dir, "binding.yaml", MAX_INDEX_FILE, binary=True)
    manifest = loads(read_untrusted(dataset_dir, "manifest.json", MAX_INDEX_FILE))
    if binding_bytes is None or not isinstance(manifest, dict):
        return None
    binding_hash = hashlib.sha256(binding_bytes).hexdigest()
    manifest_binding = manifest.get("binding")
    if not isinstance(manifest_binding, dict) or manifest_binding.get("sha256") != binding_hash:
        return None
    try:
        binding = yaml.safe_load(binding_bytes)
    except yaml.YAMLError:
        return None
    if not isinstance(binding, dict) or not isinstance(binding.get("record"), str):
        return None
    published = datetime.fromtimestamp((dataset_dir / "manifest.json").stat().st_mtime, UTC)
    previews: list[str] = []  # the first crops that have a preview; the page shows the first and counts the rest
    for crop in manifest.get("crops") or []:
        if len(previews) == MAX_PREVIEWS:
            break
        if isinstance(crop, dict) and isinstance(crop_id := crop.get("crop_id"), str) and CROP_ID_RE.fullmatch(crop_id):
            if (dataset_dir / crop_id / PREVIEW_FILE).is_file():
                previews.append(crop_id)
    return {
        "state": "prepared",
        "record": binding["record"],
        "folder": dataset_dir.name,
        "binding_hash": binding_hash,
        "binding_commit": binding.get("snippets_commit"),
        "crops": count if isinstance(count := manifest.get("count"), int) else 0,  # one odd folder must not stop a scan
        "previews": previews,
        "published_at": published.isoformat(),
    }
