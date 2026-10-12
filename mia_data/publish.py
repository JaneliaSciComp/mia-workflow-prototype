"""Idempotent publication of verified crops into the dataset folder (spec §6.3 Placement, §7.6, §5.6)."""

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

from autodev.jobs import read_untrusted

CROP_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")
MAX_FILE = 64 << 20  # a crop file is a 256x256 PNG or a small JSON


class Conflict(RuntimeError):
    """A crop exists with different checksums (or its source changed since verify): needs a human."""


def check_crop_id(cid) -> str:
    if not (isinstance(cid, str) and CROP_ID_RE.fullmatch(cid)):
        raise ValueError(f"bad crop_id {cid!r}")
    return cid


def _hashes(d: Path) -> dict[str, str]:
    return {p.relative_to(d).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in d.rglob("*") if p.is_file()}


def _same(dest: Path, files: dict, cid: str) -> None:
    if _hashes(dest) != files:
        raise Conflict(f"{cid} is already published with different checksums")


def publish(dataset_dir: Path, src_dir: Path, verified: list[dict], withdrawn: dict[str, str], *, tmp_max_age_s: float) -> dict:
    """verified = [{"crop_id", "files": {rel: sha256}}]. Copies only the verified files of each crop into a
    per-attempt `.tmp-<crop_id>-<uuid4>`, re-hashes, and renames it into place; an existing crop with the same
    hashes counts as already published. Then rebuilds manifest.json. Returns {"published", "already", "total"}."""
    for v in verified:
        check_crop_id(v["crop_id"])
        if bad := [r for r in v["files"] if not r or PurePosixPath(r).is_absolute() or ".." in PurePosixPath(r).parts]:
            raise ValueError(f"{v['crop_id']}: bad file paths {bad}")
        if not {"crop.json", "raw.png"} <= set(v["files"]):
            raise ValueError(f"{v['crop_id']}: crop.json and raw.png must be verified")
    dataset_dir.mkdir(parents=True, exist_ok=True)
    now = time.time()
    for t in dataset_dir.glob(".tmp-*"):  # younger ones may belong to a live attempt
        try:
            if now - t.lstat().st_mtime > tmp_max_age_s:
                shutil.rmtree(t) if t.is_dir() else t.unlink()
        except FileNotFoundError:  # another attempt got there first
            pass
    published, already = [], []
    try:
        for v in verified:
            cid, files, dest = v["crop_id"], v["files"], dataset_dir / v["crop_id"]
            if dest.exists():
                _same(dest, files, cid)
                already.append(cid)
                continue
            tmp = dataset_dir / f".tmp-{cid}-{uuid.uuid4().hex}"
            for rel in files:
                data = read_untrusted(src_dir, f"{cid}/{rel}", MAX_FILE, binary=True)  # job output: no symlinks
                if data is None:
                    shutil.rmtree(tmp, ignore_errors=True)
                    raise Conflict(f"{cid}/{rel} is missing or unreadable in the crop job's output")
                (tmp / rel).parent.mkdir(parents=True, exist_ok=True)
                (tmp / rel).write_bytes(data)
            if _hashes(tmp) != files:
                shutil.rmtree(tmp)
                raise Conflict(f"{cid}: source files differ from the verified checksums")
            try:
                os.rename(tmp, dest)
                published.append(cid)
            except OSError as e:  # another attempt renamed first
                if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                    raise
                shutil.rmtree(tmp)
                _same(dest, files, cid)
                already.append(cid)
    finally:  # a Conflict leaves the manifest matching what is published
        total = reindex(dataset_dir, withdrawn)
    return {"published": published, "already": already, "total": total}


def reindex(dataset_dir: Path, withdrawn: dict[str, str]) -> int:
    """Rebuild manifest.json from */crop.json (dot-dirs ignored), marking crops of withdrawn snippets.
    crop.json["snippets"] is [{"name", "commit"}]. Each entry carries "files" (rel -> sha256), which harness verify
    --existing reads for its duplicate-checksum check. Returns the crop count."""
    crops = []
    for d in sorted(p for p in dataset_dir.iterdir() if p.is_dir() and not p.name.startswith(".")):
        if not (d / "crop.json").is_file():
            continue
        cj = json.loads((d / "crop.json").read_text())
        hit = next((s["name"] for s in cj.get("snippets") or [] if s.get("name") in withdrawn), None)
        crops.append({**cj, "crop_id": d.name, "files": _hashes(d), "withdrawn": hit is not None,
                      "withdrawn_reason": withdrawn.get(hit)})
    tmp = dataset_dir / f".tmp-manifest-{uuid.uuid4().hex}.json"  # per writer: a zombie attempt can't interleave
    tmp.write_text(json.dumps({"crops": crops, "count": len(crops)}, indent=1))
    os.replace(tmp, dataset_dir / "manifest.json")
    return len(crops)
