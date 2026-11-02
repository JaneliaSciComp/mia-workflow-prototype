"""Pure crop sampling over registered volumes: which volume (by weight), then which crop, per split.

Spec v0.5 §2 serving-time rules. A volume staged sparse (an OME-Zarr 0.6 artifact) carries its planned samples, the
only crops whose chunks exist on disk: a crop is one of them, verbatim. A volume without samples (a 0.5 artifact,
the whole pyramid on disk) gets a uniform plane and centre. No I/O and no clock: the service owns the rng and the
ledger, this module only turns (volumes, split, rng) into one crop descriptor without its sequence number.
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field


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
    artifact: str | None = None  # the staged artifact the volume lives in (its registry key); None before ids
    missing: bool = False  # its pyramid was not on disk at the last status check; never served while set
    # planned samples per split ({"plane", "centre_nm", "levels", "patch", "starts"}); empty for a 0.5 artifact,
    # whose levels describe the whole volume rather than the staged box
    samples: dict[str, tuple[dict, ...]] = field(default_factory=dict)

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
    """The volumes with something to serve in `split` (candidates when the volume has samples, else planes) whose
    pyramid is not missing."""
    return [
        volume
        for volume in volumes
        if not volume.missing and (volume.samples.get(split) if volume.samples else volume.split_planes.get(split))
    ]


def capacity(volumes: list[Volume], split: str) -> int | None:
    """How many crops `draw_frozen` yields for the split before it runs out: every candidate, or None (no end) when a
    volume without samples serves it."""
    served = servable(volumes, split)
    return None if any(not volume.samples for volume in served) else sum(len(v.samples[split]) for v in served)


def draw(
    rng: random.Random, volumes: list[Volume], split: str, *, patch: int, levels: list[int], channels: int | str
) -> dict | None:
    """One crop descriptor (without seq) for the split, with replacement, or None when no volume serves it."""
    choices = servable(volumes, split)
    if not choices:
        return None
    volume = rng.choices(choices, weights=[max(v.weight, 0.0) for v in choices])[0]
    index = rng.randrange(len(volume.samples[split])) if volume.samples else None
    return _crop(rng, volume, split, index, patch=patch, levels=levels, channels=channels)


def draw_frozen(
    rng: random.Random,
    volumes: list[Volume],
    split: str,
    used: set[tuple[str, int]],
    *,
    patch: int,
    levels: list[int],
    channels: int | str,
) -> dict | None:
    """One crop descriptor (without seq) whose (volume, candidate) is not in `used`, or None when nothing is left: a
    volume by weight among those with an unused candidate (a volume without samples always has one), then an unused
    candidate uniformly. Whether it returns None does not depend on `rng`."""
    served = servable(volumes, split)
    lengths = {volume.id: len(volume.samples[split]) for volume in served if volume.samples}
    taken = Counter(volume_id for volume_id, index in used if 0 <= index < lengths.get(volume_id, 0))
    choices = [volume for volume in served if not volume.samples or taken[volume.id] < lengths[volume.id]]
    if not choices:
        return None
    volume = rng.choices(choices, weights=[max(v.weight, 0.0) for v in choices])[0]
    index = None
    if volume.samples:
        index = rng.choice([i for i in range(lengths[volume.id]) if (volume.id, i) not in used])
    return _crop(rng, volume, split, index, patch=patch, levels=levels, channels=channels)


def _crop(
    rng: random.Random,
    volume: Volume,
    split: str,
    index: int | None,
    *,
    patch: int,
    levels: list[int],
    channels: int | str,
) -> dict:
    """The descriptor of the volume's candidate `index`, verbatim (its chunks are the only ones staged), or with
    None a uniform plane and centre over the whole volume (a 0.5 artifact)."""
    served = {
        "volume": volume.id,
        "path": volume.path,
        "channels": volume.recipe if channels != "all" else None,
        "normalize": volume.normalize,
    }
    if index is not None:
        return {**volume.samples[split][index], **served, "candidate": index}
    plane = rng.choice(volume.split_planes[split])
    half = footprint(patch, levels) // 2
    level0 = volume.levels[0]
    centre = [
        rng.randint(half, volume.extent(axis) - half) * level0.scale_nm[axis] + level0.translation_nm.get(axis, 0.0)
        for axis in ("y", "x")
    ]
    return {**served, "plane": {"z": plane}, "centre_nm": centre, "levels": list(levels), "patch": patch}
