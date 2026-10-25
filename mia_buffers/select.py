"""The data pool: `select`, `exclude` and `dedup` from the spec applied to record facts, one rule string per record.

Plain code, no agent (spec v0.4 §2). Rules fire in a fixed order so the plan comment is stable; dedup runs last and
only over admitted records, keeping the first in catalog path order.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from fnmatch import fnmatchcase

from .spec import ContainsFilter, Filter, InFilter, RangeFilter, Select, Where
from .specmap import SPEC_SOURCES, SPEC_TYPES, Facts


@dataclass(frozen=True)
class Decision:
    id: str
    admitted: bool
    rule: str


def matches(filter_: Filter, values: list[str]) -> bool:
    """Whether a record's values pass one `where` filter."""
    if filter_ == "any":
        return True
    if isinstance(filter_, InFilter):
        return bool(set(filter_.in_) & set(values))
    if isinstance(filter_, ContainsFilter):
        return any(filter_.contains in value for value in values)
    if isinstance(filter_, RangeFilter):
        low, high = filter_.range
        return any(low <= float(value) <= high for value in values)
    raise TypeError(f"unknown filter {filter_!r}")


def _allowed(filter_: Filter) -> str:
    return str(filter_.in_) if isinstance(filter_, InFilter) else repr(filter_)


def _mapped(filter_: Filter, table: dict[str, str]) -> Filter:
    """The spec's vocabulary for annotation types and sources, in the catalog's words."""
    if isinstance(filter_, InFilter):
        return InFilter(**{"in": [table.get(value, value) for value in filter_.in_]})
    return filter_


def _glob(patterns: list[str], path: str) -> bool:
    # ponytail: fnmatch lets `*` cross slashes, so `datasets/2D/**` and `datasets/**` both behave; real `**`
    # semantics only matter if a spec ever needs `datasets/*/confocal/*.yaml`.
    return any(fnmatchcase(path, pattern) for pattern in patterns)


def _where(where: Where, f: Facts) -> str | None:
    if not matches(where.dims, [f.dims or ""]):
        return f"dims {f.dims} not in {_allowed(where.dims)}"
    if not matches(where.modality, f.modalities):
        return f"modality {' '.join(f.modalities) or 'none'} not in {_allowed(where.modality)}"
    if not matches(where.organism, f.organisms):
        return f"organism {' '.join(f.organisms) or 'none'} not in {_allowed(where.organism)}"
    if f.license_spdx in (None, "unknown"):
        if not where.license.allow_unknown:
            return "license unknown and allow_unknown is false"
    elif f.license_spdx not in where.license.in_:
        return f"license {f.license_spdx} not in allowed"
    annotations = where.annotations
    if annotations.required and not f.annotations_present:
        return "annotations required but absent"
    if f.annotations_present:
        types = _mapped(annotations.types, SPEC_TYPES)
        if not matches(types, f.annotation_types):
            return f"annotation types {' '.join(f.annotation_types) or 'none'} not in {_allowed(types)}"
        source = _mapped(annotations.source, SPEC_SOURCES)
        if not matches(source, [f.annotation_source or "unknown"]):
            return f"annotation source {f.annotation_source or 'unknown'} not in {_allowed(source)}"
    largest = f.largest_raw
    if where.min_size_voxels and largest is not None and largest.voxels < where.min_size_voxels:
        return f"largest raw array has {largest.voxels} voxels < min_size_voxels {where.min_size_voxels}"
    return None


def decide_all(all_facts: list[Facts], select: Select) -> list[Decision]:
    """One decision per record, in the order given (catalog path order)."""
    checks: list[Callable[[Facts], str | None]] = [
        lambda f: None if _glob(select.paths, f.path) else "path not in select.paths",
        lambda f: "excluded by path" if _glob(select.exclude.paths, f.path) else None,
        lambda f: (
            f"excluded by id: {select.exclude.reasons.get(f.id, 'no reason given')}"
            if f.id in select.exclude.ids
            else None
        ),
        lambda f: _where(select.where, f),
        lambda f: (  # what mia-data will refuse at sourcing is out of the pool at planning
            None
            if f.folder
            else f"no dataset folder for mia-data: modality {' '.join(f.modalities) or 'none'} / "
            f"organism {' '.join(f.organisms) or 'none'} (one of each needed)"
        ),
    ]
    decisions: list[Decision] = []
    kept_by_doi: dict[str, str] = {}
    for f in all_facts:
        rule = next((reason for check in checks if (reason := check(f))), None)
        if rule is None and "doi" in select.dedup.by and f.doi:
            if (first := kept_by_doi.get(f.doi)) is not None:
                rule = f"dedup: doi {f.doi} already kept by {first}"
            else:
                kept_by_doi[f.doi] = f.id
        decisions.append(Decision(f.id, rule is None, rule or "admitted"))
    return decisions
