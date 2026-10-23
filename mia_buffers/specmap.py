"""One catalog record → the facts the planner uses, with the spec's vocabulary mapped onto the record schema's.

Never raises on a missing or null field: a record the enricher has not inspected yields no arrays, and the planner
turns that into a rule, not a crash. Approximations the record schema forces (spec v0.4 §8.1) are named here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse

from mia_data.catalog import FAMILIES

SPEC_TYPES = {"point": "points", "bbox": "bounding-box"}  # spec annotation types → catalog enum
SPEC_SOURCES = {"auto-predict": "automatic"}  # spec annotation sources → catalog enum
PLANE_AXES = "tz"
CHANNEL_AXES = "cs"  # s = RGB samples


def family_of(modality: str) -> str:
    return next((family for family, members in FAMILIES.items() if modality in members), "other")


@dataclass(frozen=True)
class ArrayFacts:
    role: str
    axes: str
    shape: tuple[int, ...]
    shape_varies: bool
    extent_y: int | None
    extent_x: int | None
    extent_z: int | None
    channels: int
    planes: int
    voxels: int
    dtype: str | None
    format: str | None
    compression: str | None
    encoding: str | None


@dataclass(frozen=True)
class Facts:
    id: str
    path: str
    dims: str | None
    modalities: list[str]
    modality_group: str | None  # the first modality, the weighting group for `over: modality`
    family: str | None
    organisms: list[str]
    license_spdx: str | None
    annotations_present: bool
    annotation_types: list[str]
    annotation_source: str | None
    label_encodings: list[str]
    raw_arrays: list[ArrayFacts]
    voxel_size_nm: dict[str, float | None]
    n_items: int
    size_bytes: int | None
    download_method: str | None
    download_host: str | None
    doi: str | None
    all_arrays: list[ArrayFacts] = field(default_factory=list)

    @property
    def largest_raw(self) -> ArrayFacts | None:
        return max(self.raw_arrays, key=lambda array: array.voxels, default=None)

    def voxel_size_known(self, dims: str) -> bool:
        axes = ("x", "y", "z") if dims.startswith("3D") else ("x", "y")
        return all(isinstance(self.voxel_size_nm.get(axis), int | float) for axis in axes)


def _array(entry: dict) -> ArrayFacts | None:
    axes, shape = entry.get("axes"), entry.get("shape")
    if not axes or not shape or len(axes) != len(shape):
        return None
    extent = dict(zip(axes, shape, strict=True))
    channels = next((extent[axis] for axis in CHANNEL_AXES if axis in extent), 1)
    planes = 1
    for axis in PLANE_AXES:
        planes *= extent.get(axis, 1)
    voxels = 1
    for size in shape:
        voxels *= size
    return ArrayFacts(
        role=entry.get("role", "other"),
        axes=axes,
        shape=tuple(shape),
        shape_varies=bool(entry.get("shape_varies")),
        extent_y=extent.get("y"),
        extent_x=extent.get("x"),
        extent_z=extent.get("z"),
        channels=channels,
        planes=planes,
        voxels=voxels,
        dtype=entry.get("dtype"),
        format=entry.get("format"),
        compression=entry.get("compression"),
        encoding=entry.get("encoding"),
    )


def facts(path: str, record: dict) -> Facts:
    imaging = record.get("imaging") or {}
    data = record.get("data") or {}
    annotations = record.get("annotations") or {}
    arrays = [array for entry in (record.get("technical") or {}).get("arrays") or [] if (array := _array(entry))]
    modalities = list(imaging.get("modality") or [])
    url = data.get("download_url")
    return Facts(
        id=record.get("id") or path.rsplit("/", 1)[-1].removesuffix(".yaml"),
        path=path,
        dims=imaging.get("dimensionality"),
        modalities=modalities,
        modality_group=modalities[0] if modalities else None,
        family=family_of(modalities[0]) if modalities else None,
        organisms=list(imaging.get("organism") or []),
        license_spdx=(record.get("license") or {}).get("spdx"),
        annotations_present=bool(annotations.get("present")),
        annotation_types=list(annotations.get("types") or []),
        annotation_source=annotations.get("source"),
        label_encodings=sorted({array.encoding for array in arrays if array.role == "label" and array.encoding}),
        raw_arrays=[array for array in arrays if array.role == "raw"],
        voxel_size_nm=dict(imaging.get("voxel_size_nm") or {}),
        n_items=int(data.get("n_items") or 1),
        size_bytes=data.get("size_bytes"),
        download_method=data.get("download_method"),
        download_host=urlparse(url).hostname if url else None,
        doi=record.get("doi"),
        all_arrays=arrays,
    )
