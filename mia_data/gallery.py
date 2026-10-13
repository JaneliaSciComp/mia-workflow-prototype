"""Publishing crop thumbnails to the `galleries` branch of the data repo, so issue comments can show them."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

from autodev.jobs import read_untrusted

from .publish import MAX_FILE, check_crop_id

if TYPE_CHECKING:
    from autodev.github import GitHub


def _read(workspace: Path, rel: str) -> bytes:
    if (data := read_untrusted(workspace, rel, MAX_FILE, binary=True)) is None:
        raise ValueError(f"{rel} is missing or unreadable in {workspace}")
    return data


def gallery_files(crop_workspace: Path, verify_workspace: Path, verified: list[dict]) -> dict[str, bytes]:
    """The files to publish, as relative path -> bytes: each verified crop's raw.png (from the crop job) and
    overlay.png (from the verify job's preview), plus the verify job's binding.yaml and verify.json.

    `verified` is read_verify's crop list; a raw.png is published only if it still matches the verified checksum."""
    out = {}
    for crop in verified:
        crop_id = check_crop_id(crop["crop_id"])
        raw = _read(crop_workspace, f"crops/{crop_id}/raw.png")
        if hashlib.sha256(raw).hexdigest() != crop["files"]["raw.png"]:
            raise ValueError(f"{crop_id}/raw.png differs from the verified checksum")
        out[f"{crop_id}/raw.png"] = raw
        out[f"{crop_id}/overlay.png"] = _read(verify_workspace, f"preview/{crop_id}/overlay.png")
    out["binding.yaml"], out["verify.json"] = (
        _read(verify_workspace, "binding.yaml"),
        _read(verify_workspace, "verify.json"),
    )
    return out


async def publish_gallery(gh_data: GitHub, branch: str, prefix: str, files: dict[str, bytes]) -> dict[str, str]:
    """Commit `files` under `prefix` on the gallery branch (files already there are left alone).

    Returns rel -> https://github.com/<repo>/blob/<branch>/<prefix>/<rel>?raw=true, the URLs markdown can embed."""
    # ponytail: PNGs on a branch are fine for a demo; move to real hosting before it reaches a few hundred MB
    await gh_data.ensure_branch(branch)
    for rel, data in files.items():
        await gh_data.upsert_file(f"{prefix}/{rel}", data, branch, f"mia-data: {prefix}")
    return {rel: f"https://github.com/{gh_data.repo}/blob/{branch}/{prefix}/{rel}?raw=true" for rel in files}
