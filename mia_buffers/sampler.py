"""Pure crop sampling over registered volumes: which volume (by weight), which plane and centre (uniform), per split.

Spec v0.5 §2 serving-time rules. No I/O and no clock: the service owns the rng and the ledger, this module only
turns (volumes, split, rng) into one crop descriptor without its sequence number.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Level:
    factor: int
    shape: tuple[int, ...]
    scale_nm: dict[str, float]
    translation_nm: dict[str, float]


@dataclass(frozen=True)
class Volume:
    """One staged pyramid as the service knows it: geometry, which planes belong to which split, weight, recipe."""

    id: str
    path: str
    record: str
    axes: str
    shape: tuple[int, ...]
    levels: tuple[Level, ...]
    split_planes: dict[str, tuple[int, ...]]
    labels: dict[str, str]
    weight: float
    channels_stored: int
    recipe: dict | None
    voxel_size_nm: dict[str, float]
    normalize: dict | None = None  # per-channel lo/hi bounds the tensorizer applies (scope volume|dataset)

    def extent(self, axis: str) -> int:
        return self.shape[self.axes.index(axis)] if axis in self.axes else 1

    def planes(self) -> int:
        return self.extent("z")


def footprint(patch: int, levels: list[int]) -> int:
    """Level-0 pixels the coarsest crop covers along one axis."""
    return patch * max(levels)


def fits(volume: Volume, patch: int, levels: list[int]) -> str | None:
    """None when the coarsest crop fits the volume's y and x, else the reason."""
    if "y" not in volume.axes or "x" not in volume.axes:
        return f"axes {volume.axes!r} lack y or x"
    size = footprint(patch, levels)
    extent_y, extent_x = volume.extent("y"), volume.extent("x")
    if size > extent_y or size > extent_x:
        return f"footprint {size} exceeds {extent_y}×{extent_x}"
    return None


def servable(volumes: list[Volume], split: str) -> list[Volume]:
    return [volume for volume in volumes if volume.split_planes.get(split)]


def draw(
    rng: random.Random, volumes: list[Volume], split: str, *, patch: int, levels: list[int], channels: int | str
) -> dict | None:
    """One crop descriptor (without seq) for the split, or None when no volume has planes in it."""
    candidates = servable(volumes, split)
    if not candidates:
        return None
    volume = rng.choices(candidates, weights=[max(v.weight, 0.0) for v in candidates])[0]
    plane = rng.choice(volume.split_planes[split])
    half = footprint(patch, levels) // 2
    level0 = volume.levels[0]
    centre = [
        rng.randint(half, volume.extent(axis) - half) * level0.scale_nm[axis] + level0.translation_nm.get(axis, 0.0)
        for axis in ("y", "x")
    ]
    return {
        "volume": volume.id,
        "path": volume.path,
        "plane": {"z": plane},
        "centre_nm": centre,
        "levels": list(levels),
        "patch": patch,
        "channels": volume.recipe if channels != "all" else None,
        "normalize": volume.normalize,
    }
