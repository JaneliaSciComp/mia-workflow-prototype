"""Crops on the `galleries` branch of the data repo, embedded in comments (spec §7.4, §7.5)."""

from __future__ import annotations

import hashlib
from pathlib import Path

from autodev.jobs import read_untrusted

from .publish import MAX_FILE, check_crop_id


def _read(ws: Path, rel: str) -> bytes:
    if (data := read_untrusted(ws, rel, MAX_FILE, binary=True)) is None:
        raise ValueError(f"{rel} is missing or unreadable in {ws}")
    return data


def gallery_files(crop_ws: Path, verify_ws: Path, verified: list[dict]) -> dict[str, bytes]:
    """<crop_id>/raw.png (crop job's crops/) and <crop_id>/overlay.png (verify job's preview/) for verified crops
    only, plus the verify job's binding.yaml and verify.json. Job output: read bounded, no symlinks."""
    out = {}
    for v in verified:
        cid = check_crop_id(v["crop_id"])
        raw = _read(crop_ws, f"crops/{cid}/raw.png")
        if hashlib.sha256(raw).hexdigest() != v["files"]["raw.png"]:  # publish only what verify checked
            raise ValueError(f"{cid}/raw.png differs from the verified checksum")
        out[f"{cid}/raw.png"], out[f"{cid}/overlay.png"] = raw, _read(verify_ws, f"preview/{cid}/overlay.png")
    out["binding.yaml"], out["verify.json"] = _read(verify_ws, "binding.yaml"), _read(verify_ws, "verify.json")
    return out


async def publish_gallery(gh_data, branch: str, prefix: str, files: dict[str, bytes]) -> dict[str, str]:
    """ensure_branch once; upsert_file for each prefix/<rel> (existing files are skipped); returns
    rel -> https://github.com/<repo>/blob/<branch>/<prefix>/<rel>?raw=true."""
    # ponytail: PNGs on a branch are fine for a demo; move to real hosting (§15) before it reaches a few hundred MB
    await gh_data.ensure_branch(branch)
    for rel, data in files.items():
        await gh_data.upsert_file(f"{prefix}/{rel}", data, branch, f"mia-data: {prefix}")
    return {rel: f"https://github.com/{gh_data.repo}/blob/{branch}/{prefix}/{rel}?raw=true" for rel in files}
