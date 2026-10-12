"""Crops on the `galleries` branch of the data repo, embedded in comments (spec §7.4, §7.5)."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .publish import check_crop_id


def gallery_files(crops_dir: Path, preview_dir: Path, verified: list[dict], binding: Path, verify_json: Path) -> dict[str, bytes]:
    """<crop_id>/raw.png and <crop_id>/overlay.png for verified crops only, plus binding.yaml and verify.json."""
    out = {}
    for v in verified:
        cid = check_crop_id(v["crop_id"])
        raw = (crops_dir / cid / "raw.png").read_bytes()
        if hashlib.sha256(raw).hexdigest() != v["files"]["raw.png"]:  # publish only what verify checked
            raise ValueError(f"{cid}/raw.png differs from the verified checksum")
        out[f"{cid}/raw.png"], out[f"{cid}/overlay.png"] = raw, (preview_dir / cid / "overlay.png").read_bytes()
    out["binding.yaml"], out["verify.json"] = binding.read_bytes(), verify_json.read_bytes()
    return out


async def publish_gallery(gh_data, branch: str, prefix: str, files: dict[str, bytes]) -> dict[str, str]:
    """ensure_branch once; upsert_file for each prefix/<rel> (existing files are skipped); returns
    rel -> https://github.com/<repo>/blob/<branch>/<prefix>/<rel>?raw=true."""
    # ponytail: PNGs on a branch are fine for a demo; move to real hosting (§15) before it reaches a few hundred MB
    await gh_data.ensure_branch(branch)
    for rel, data in files.items():
        await gh_data.upsert_file(f"{prefix}/{rel}", data, branch, f"mia-data: {prefix}")
    return {rel: f"https://github.com/{gh_data.repo}/blob/{branch}/{prefix}/{rel}?raw=true" for rel in files}
