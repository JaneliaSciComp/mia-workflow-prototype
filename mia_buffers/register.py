"""`mia-buffers register`: a plan plus staged volumes → one buffer in buffer-api (spec v0.5 §5, milestone 2).

The hand-registration path: the worker of milestone 3 will call the same functions after staging.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from urllib.parse import quote

import httpx

from .plan import split_counts
from .spec import Spec


def buffer_config(plan: dict, spec: Spec, *, tensorizer: str, split_sizes: dict[str, int]) -> dict:
    """The `PUT buffers/<id>` body from a plan and its spec."""
    patch = spec.patch_for("2D") or {}
    if patch.get("y") != patch.get("x"):
        raise ValueError(f"MuViT crops are square; the 2D patch is {patch}")
    normalize = spec.sampling.normalize
    return {
        "spec_name": plan["spec_name"],
        "spec_hash": plan["spec_hash"],
        "catalog_ref": plan.get("catalog_ref"),
        "levels": list(spec.sampling.levels),
        "patch": int(patch["y"]),
        "channels": spec.sampling.patch.c,
        "seed": spec.sampling.seed,
        "tensorizer": tensorizer,
        "tensorizer_params": {"normalize": normalize.method, "range": list(normalize.range), "clip": normalize.clip},
        "splits": dict(spec.splits.fractions),
        "split_sizes": dict(split_sizes),
    }


def _split_planes(volume_id: str, planes: int, spec: Spec) -> dict[str, list[int]]:
    """Seeded shuffle of the planes, cut by the spec's fractions (largest remainder); split keys are (volume, plane)."""
    rng = random.Random(f"{spec.sampling.seed}:{volume_id}")
    order = list(range(planes))
    rng.shuffle(order)
    fractions = spec.splits.fractions
    out: dict[str, list[int]] = {split: [] for split in fractions}
    if planes < len(fractions):  # too few planes to apportion: each plane picks its split by fraction, so a dataset
        for plane in order:  # of single-image volumes still splits by fraction overall (review C1)
            out[rng.choices(list(fractions), weights=list(fractions.values()))[0]].append(plane)
        return {split: sorted(chosen) for split, chosen in out.items()}
    for split, count in split_counts(planes, fractions).items():
        out[split], order = sorted(order[:count]), order[count:]
    return out


def volume_bodies(row: dict, staged: Path, spec: Spec) -> list[tuple[str, dict]]:
    """(volume id, `PUT volumes/<vol>` body) for every raw volume in the record's stage.json."""
    report = json.loads((staged / "stage.json").read_text())
    volumes = report.get("volumes") or []
    total_planes = sum(_planes(volume) for volume in volumes) or 1
    out = []
    for volume in volumes:
        volume_id = f"{row['id']}/{volume['path']}"
        planes = _planes(volume)
        out.append(
            (
                volume_id,
                {
                    "path": str(staged / volume["path"]),
                    "record": row["id"],
                    "axes": volume["axes"],
                    "shape": list(volume["shape"]),
                    "levels": [
                        {key: level[key] for key in ("factor", "shape", "scale_nm", "translation_nm")}
                        for level in volume["levels"]
                    ],
                    "split_planes": _split_planes(volume_id, planes, spec),
                    "labels": dict(volume.get("labels") or {}),
                    "weight": float(row["weight"]) * planes / total_planes,
                    "channels_stored": _channels(volume),
                    "recipe": row.get("channel_recipe"),
                    "voxel_size_nm": {k: v for k, v in (volume.get("voxel_size_nm") or {}).items() if v is not None},
                },
            )
        )
    return out


def _planes(volume: dict) -> int:
    axes, shape = volume["axes"], volume["shape"]
    return int(shape[axes.index("z")]) if "z" in axes else 1


def _channels(volume: dict) -> int:
    axes, shape = volume["axes"], volume["shape"]
    return int(shape[axes.index("c")]) if "c" in axes else 1


def register(
    url: str, buffer: str, plan: dict, spec: Spec, staged_root: Path, *, tensorizer: str, split_sizes: dict[str, int]
) -> dict:
    """Create the buffer and register every pooled, staged volume; returns registered / skipped / refused."""
    out: dict = {"registered": [], "skipped": {}, "refused": {}}
    with httpx.Client(base_url=url, timeout=30.0) as client:
        client.put(
            f"/buffers/{buffer}", json=buffer_config(plan, spec, tensorizer=tensorizer, split_sizes=split_sizes)
        ).raise_for_status()
        for row in plan["rows"]:
            if not (row.get("admitted") and (row.get("fit") or {}).get("ok")):
                continue
            staged = staged_root / row["id"]
            if not (staged / "stage.json").is_file():
                out["skipped"][row["id"]] = "not staged"
                continue
            for volume_id, body in volume_bodies(row, staged, spec):
                response = client.put(f"/buffers/{buffer}/volumes/{quote(volume_id, safe='')}", json=body)
                if response.status_code == 200:
                    out["registered"].append(volume_id)
                else:
                    out["refused"][volume_id] = response.json().get("error", response.text)
    return out
