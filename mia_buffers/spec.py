"""The buffer spec: the YAML a buffer issue carries, validated strictly (unknown keys refused).

The grammar is the catalog's examples/spec-microscopy-pretrain.yaml plus `sampling.levels`, `ready` and `sourcing`. Decisions the
architecture spec leaves open are fixed in notes/plans/2026-10-23-mia-buffers-milestone-0.md. The spec is untrusted
text: safe-loaded, every field typed, and refused with the validator's own message.
"""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, ValidationError, field_validator, model_validator


class SpecError(ValueError):
    """The spec file is not a valid buffer spec; the message is the validator's."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class InFilter(_Strict):
    in_: list[str] = Field(alias="in")


class ContainsFilter(_Strict):
    contains: str


class RangeFilter(_Strict):
    range: tuple[float, float]


Filter = Literal["any"] | InFilter | ContainsFilter | RangeFilter


class CatalogRef(_Strict):
    repo: str
    ref: str = Field(pattern=r"^[0-9a-f]{40}$")  # a commit, never a branch


class LicenseFilter(_Strict):
    in_: list[str] = Field(alias="in")
    allow_unknown: bool = False


class AnnotationsFilter(_Strict):
    required: bool = False
    types: Filter = "any"
    encoding: Filter = "any"
    source: Filter = "any"

    @field_validator("encoding")
    @classmethod
    def _encoding_unsupported(cls, value: Filter) -> Filter:
        if value != "any":
            raise ValueError("annotations.encoding is not in the catalog record schema yet (spec v0.4 §8.1)")
        return value


class Where(_Strict):
    dims: Filter = "any"
    modality: Filter = "any"
    organism: Filter = "any"
    sample_type: Filter = "any"
    license: LicenseFilter
    annotations: AnnotationsFilter = AnnotationsFilter()
    min_size_voxels: float = 0

    @field_validator("sample_type")
    @classmethod
    def _sample_type_unsupported(cls, value: Filter) -> Filter:
        if value != "any":
            raise ValueError("sample_type is not in the catalog record schema yet (spec v0.4 §8.1)")
        return value


class Exclude(_Strict):
    paths: list[str] = []
    ids: list[str] = []
    reasons: dict[str, str] = {}


class Dedup(_Strict):
    by: list[Literal["content_hash", "doi"]] = ["doi"]
    keep: Literal["first"] = "first"


class Select(_Strict):
    paths: list[str] = ["datasets/**"]
    where: Where
    exclude: Exclude = Exclude()
    dedup: Dedup = Dedup()


class PatchSize(_Strict):
    """Patch edge per axis in pixels; z and t only for the dimensionalities that have them."""

    y: PositiveInt
    x: PositiveInt
    z: PositiveInt | None = None
    t: PositiveInt | None = None


class Patch(_Strict):
    """Patch sizes per dimensionality; `c` is the channel policy (a channel count per crop, default 1, or all)."""

    two_d: PatchSize = Field(alias="2D")
    two_d_t: PatchSize | None = Field(default=None, alias="2D+t")
    three_d: PatchSize | None = Field(default=None, alias="3D")
    three_d_t: PatchSize | None = Field(default=None, alias="3D+t")
    c: Literal["all"] | int = 1  # one channel per crop (the lead's initial experiments); 3 = RGB; all = as stored

    @field_validator("c")
    @classmethod
    def _positive_channels(cls, value: Literal["all"] | int) -> Literal["all"] | int:
        if isinstance(value, int) and value < 1:
            raise ValueError("patch.c must be 'all' or a positive channel count")
        return value


class Resample(_Strict):
    target_voxel_size: dict[str, float] | None = None
    unit: Literal["nm", "um"] = "nm"
    image_interp: Literal["linear", "nearest", "cubic"] = "linear"
    label_interp: Literal["nearest"] = "nearest"


class Weighting(_Strict):
    scheme: Literal["uniform_over", "proportional", "temperature"] = "temperature"
    over: list[Literal["patch", "dataset", "organism", "modality", "dims"]] = ["dataset"]
    temperature: float = Field(default=1.0, ge=0, le=1)


class Bbox(_Strict):
    enabled: bool = True
    unit: Literal["nm", "um", "mm"] = "nm"


class Emit(_Strict):
    bbox: Bbox = Bbox()
    voxel_size: bool = True
    metadata: list[str] = []


class Normalize(_Strict):
    method: Literal["percentile", "zscore", "minmax", "none"] = "percentile"
    range: tuple[float, float] = (1, 99)
    scope: Literal["patch", "volume", "dataset"] = "patch"
    clip: bool = True


class Sampling(_Strict):
    patch: Patch
    levels: list[int] = [1]
    assume_voxel_size: bool = True  # fill missing voxel sizes from mia_buffers.assume (spec v0.5 §2)
    output_axes: str = "tczyx"
    missing_axes: Literal["singleton", "drop"] = "singleton"
    label_axis: str | None = None  # accepted and ignored, see Spec.warnings
    resample: Resample = Resample()
    weighting: Weighting = Weighting()
    emit: Emit = Emit()
    dtype: Literal["uint8", "uint16", "bfloat16", "float16", "float32"] = "float32"
    normalize: Normalize = Normalize()
    seed: int = 0

    @field_validator("levels")
    @classmethod
    def _levels_ascending_from_one(cls, value: list[int]) -> list[int]:
        if not value or value[0] != 1 or any(b <= a for a, b in zip(value, value[1:], strict=False)):
            raise ValueError(f"levels must be strictly ascending and start at 1, got {value}")
        return value


class Splits(_Strict):
    fractions: dict[str, float]
    stratify_by: list[str] = []
    holdout_ids: list[str] = []
    seed: int = 0

    @field_validator("fractions")
    @classmethod
    def _fractions_sum_to_one(cls, value: dict[str, float]) -> dict[str, float]:
        if not math.isclose(sum(value.values()), 1.0, abs_tol=1e-6):
            raise ValueError(f"fractions must sum to 1, got {sum(value.values())}")
        return value


class Cache(_Strict):
    dir: str
    format: str = "zarr"
    disk_gb: float = 100
    prefetch_patches: int = 128
    num_workers: int = 16


class Ready(_Strict):
    """When a sourcing buffer is announced ready: the servable share each weighting group must reach (spec §1)."""

    min_fraction: float = Field(default=1.0, gt=0, le=1)


class Sourcing(_Strict):
    """How long a weighting group may stay without a servable dataset before the issue needs a human (spec §1)."""

    dataset_timeout_days: float = Field(default=14, gt=0)
    unprepared: Literal["skip"] = "skip"  # what to do with a record nobody prepared; only skipping exists


class PlanOptions(_Strict):
    """What happens to a fresh plan: wait for a human's approval (default) or go ahead."""

    auto_approve: bool = False


class Idle(_Strict):
    """A buffer nobody draws from for this many hours is idle."""

    hours: float = Field(default=72, gt=0)


class Health(_Strict):
    """When a buffer counts as degraded."""

    max_empty_fraction: float = Field(default=0.05, ge=0, le=1)


class Spec(_Strict):
    schema_version: Literal[1]
    name: str
    catalog: CatalogRef
    select: Select
    sampling: Sampling
    splits: Splits
    cache: Cache | None = None
    ready: Ready = Ready()
    sourcing: Sourcing = Sourcing()
    plan: PlanOptions = PlanOptions()
    idle: Idle = Idle()
    retire_after_days: float = Field(default=14, gt=0)  # an idle buffer is retired after this many more days
    health: Health = Health()
    warnings: list[str] = []  # filled by the validator, not by the file

    @model_validator(mode="after")
    def _collect_warnings(self) -> Spec:
        if self.sampling.label_axis is not None:
            self.warnings.append("sampling.label_axis is ignored: labels are a separate tensor (spec v0.4 §8.5)")
        return self

    def patch_for(self, dims: str) -> dict[str, int] | None:
        """The patch edges for a dimensionality as {axis: pixels}, or None when the spec has none for it."""
        patch = self.sampling.patch
        block = {"2D": patch.two_d, "2D+t": patch.two_d_t, "3D": patch.three_d, "3D+t": patch.three_d_t}.get(dims)
        return None if block is None else block.model_dump(exclude_none=True)


def load_spec_text(text: str) -> Spec:
    """Validate spec YAML text (a file's, or a `POST /plan` body's)."""
    try:
        data = yaml.safe_load(text)
        if isinstance(data, dict) and "warnings" in data:
            raise SpecError("warnings is not a spec field")
        return Spec.model_validate(data)
    except (yaml.YAMLError, ValidationError) as error:
        raise SpecError(str(error)) from error


MAX_PREP_RECORDS = 200  # records one preparation request may name or select (ponytail: split a larger one)


class PrepSpec(_Strict):
    """A preparation request's YAML: the records to hand to mia-data, named or chosen by a buffer spec's `select`."""

    schema_version: Literal[1]
    kind: Literal["preparation"]
    catalog: CatalogRef
    records: list[str] = []
    select: Select | None = None
    timeout_days: float = Field(default=14, gt=0)  # after this long, the request needs a human
    # each record costs an agent run: more than this many need the number written into the request
    max_records: int = Field(default=25, ge=1, le=MAX_PREP_RECORDS)

    @model_validator(mode="after")
    def _records_or_select(self) -> PrepSpec:
        if bool(self.records) == (self.select is not None):
            raise ValueError("give exactly one of `records` (a non-empty list of record ids) or `select`")
        return self


def load_prep_text(text: str) -> PrepSpec:
    """Validate a preparation request's YAML text."""
    try:
        return PrepSpec.model_validate(yaml.safe_load(text))
    except (yaml.YAMLError, ValidationError) as error:
        raise SpecError(str(error)) from error


def load_spec(path: Path) -> Spec:
    return load_spec_text(path.read_text())


def spec_text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def spec_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
