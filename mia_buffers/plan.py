"""The plan: fit, channel and dimensionality policy, weights and a split preview over the data pool, rendered as
JSON (plan.json) and markdown (the plan comment). Pure functions over Spec and Facts; the CLI and later the
`planning` activity call `build_plan`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass, field

from .select import Decision, decide_all
from .spec import Spec
from .specmap import Facts, facts

MANUAL_DROP_NOTE = "download_method other: needs a manual drop (spec v0.4 §3)"
CONTENT_HASH_WARNING = "dedup.by content_hash is not available; doi used"


@dataclass(frozen=True)
class Fit:
    ok: bool
    reason: str
    footprint: tuple[int, int]  # (y, x) pixels at level 0 covered by the coarsest level
    approximate: bool


@dataclass
class Row:
    id: str
    path: str
    dims: str | None
    modality_group: str | None
    family: str | None
    organism: str | None
    channels: int | None
    n_items: int
    admitted: bool
    rule: str
    fit: Fit | None = None
    positions: int = 0
    weight: float = 0.0
    splits: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @property
    def in_pool(self) -> bool:
        return self.admitted and self.fit is not None and self.fit.ok


@dataclass
class Plan:
    spec_name: str
    spec_hash: str
    catalog_ref: str | None
    rows: list[Row]
    errors: list[str]
    warnings: list[str]

    @property
    def valid(self) -> bool:
        return not self.errors

    @property
    def rows_by_id(self) -> dict[str, Row]:
        return {row.id: row for row in self.rows}

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def render_markdown(self) -> str:
        lines = [
            f"# Plan for `{self.spec_name}`",
            "",
            f"spec `{self.spec_hash[:12]}` · catalog `{self.catalog_ref or 'unpinned'}`",
            "",
        ]
        if self.errors:
            lines += ["## Errors", "", *(f"- {error}" for error in self.errors), ""]
        if self.warnings:
            lines += ["## Warnings", "", *(f"- {warning}" for warning in self.warnings), ""]
        pool = [row for row in self.rows if row.in_pool]
        lines += [f"## Records ({len(pool)} in the pool, {len(self.rows)} seen)", ""]
        lines += [
            "| id | decision | rule | dims | modality | channels | positions | weight | split preview |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for row in self.rows:
            rule = row.rule if row.fit is None else f"{row.rule}; {row.fit.reason}"
            if row.notes:
                rule += "; " + "; ".join(row.notes)
            splits = " / ".join(f"{name} {count}" for name, count in row.splits.items()) if row.in_pool else ""
            weight = f"{row.weight:.3f}" if row.in_pool else ""
            lines.append(
                f"| {row.id} | {'pool' if row.in_pool else 'out'} | {rule} | {row.dims or ''} | {row.modality_group or ''} "
                f"| {row.channels if row.channels is not None else ''} | {row.positions or ''} | {weight} | {splits} |"
            )
        return "\n".join(lines) + "\n"


def fit(f: Facts, spec: Spec) -> Fit:
    """Whether the spec's coarsest crop fits this record's largest raw array, and whether its box is in nm."""
    patch = spec.patch_for(f.dims or "")
    if patch is None:
        return Fit(False, f"no patch for dims {f.dims}", (0, 0), False)
    scale = max(spec.sampling.levels)
    footprint = (patch["y"] * scale, patch["x"] * scale)
    largest = f.largest_raw
    three_d = (f.dims or "").startswith("3D")
    if (
        largest is None
        or largest.extent_y is None
        or largest.extent_x is None
        or (three_d and largest.extent_z is None)
    ):
        return Fit(False, "no array shape", footprint, False)
    if footprint[0] > largest.extent_y or footprint[1] > largest.extent_x:
        return Fit(
            False,
            f"footprint {footprint[0]}×{footprint[1]} exceeds {largest.extent_y}×{largest.extent_x}",
            footprint,
            False,
        )
    if three_d and patch.get("z", 0) > (largest.extent_z or 0):
        return Fit(False, f"patch z {patch.get('z')} exceeds {largest.extent_z}", footprint, False)
    if not f.voxel_size_known(f.dims or ""):
        return Fit(False, "voxel size unknown", footprint, False)
    if largest.shape_varies:
        return Fit(True, "fits (shape varies: checked against the representative item)", footprint, True)
    return Fit(True, "fits", footprint, False)


def positions(f: Facts, spec: Spec) -> int:
    """How many non-overlapping coarsest crops the dataset holds: the weighting measure."""
    patch = spec.patch_for(f.dims or "")
    if patch is None:
        return 0
    scale = max(spec.sampling.levels)
    total = 0
    for array in f.raw_arrays:
        if array.extent_y and array.extent_x:
            total += f.n_items * (array.extent_y // (patch["y"] * scale)) * (array.extent_x // (patch["x"] * scale))
    return total


def _group_key(row: Row, level: str) -> str:
    return {"modality": row.modality_group, "organism": row.organism, "dims": row.dims}.get(level, row.id) or "none"


def weights(rows: list[Row], over: Sequence[str], temperature: float) -> dict[str, float]:
    """Dataset id → weight: at each level of `over`, siblings share their parent's weight ∝ positions ** T."""
    levels = [level for level in over if level != "patch"]
    if not levels or levels[-1] != "dataset":
        levels.append("dataset")
    result: dict[str, float] = {}

    def share(group: list[Row], depth: int, parent_weight: float) -> None:
        if depth == len(levels):
            for row in group:
                result[row.id] = result.get(row.id, 0.0) + parent_weight / len(group)
            return
        buckets: dict[str, list[Row]] = {}
        for row in group:
            buckets.setdefault(_group_key(row, levels[depth]), []).append(row)
        measures = {key: sum(row.positions for row in members) ** temperature for key, members in buckets.items()}
        total = sum(measures.values())
        for key, members in buckets.items():
            fraction = measures[key] / total if total else 1 / len(buckets)
            share(members, depth + 1, parent_weight * fraction)

    share(rows, 0, 1.0)
    return result


def split_counts(n: int, fractions: dict[str, float]) -> dict[str, int]:
    """Largest-remainder apportionment of `n` keys; ties go to the earlier split in the spec's order."""
    exact = {name: n * fraction for name, fraction in fractions.items()}
    counts = {name: int(value) for name, value in exact.items()}
    remainders = sorted(fractions, key=lambda name: -(exact[name] - counts[name]))
    for name in remainders[: n - sum(counts.values())]:
        counts[name] += 1
    return counts


def _policy_errors(pool: list[Row], spec: Spec) -> list[str]:
    errors: list[str] = []
    if not pool:
        return ["no dataset admitted"]
    dims = sorted({row.dims or "none" for row in pool})
    if len(dims) > 1:
        errors.append(f"one buffer serves one dimensionality; pool has {dims}: narrow select.where.dims")
    channel_policy = spec.sampling.patch.c
    if channel_policy == "all":
        by_count: dict[int, list[str]] = {}
        for row in pool:
            by_count.setdefault(row.channels or 1, []).append(row.id)
        if len(by_count) > 1:
            shown = {count: sorted(ids) for count, ids in sorted(by_count.items())}
            errors.append(f"patch.c: all needs one channel count; pool has {shown}")
    else:
        short = sorted(row.id for row in pool if (row.channels or 1) < channel_policy)
        if short:
            errors.append(f"patch.c: {channel_policy} needs at least {channel_policy} channels; {short} have fewer")
    return errors


def build_plan(spec: Spec, spec_hash: str, catalog_ref: str | None, records: list[tuple[str, dict]]) -> Plan:
    all_facts = sorted((facts(path, record) for path, record in records), key=lambda f: f.path)
    decisions: list[Decision] = decide_all(all_facts, spec.select)
    rows: list[Row] = []
    for f, decision in zip(all_facts, decisions, strict=True):
        largest = f.largest_raw
        row = Row(
            id=f.id,
            path=f.path,
            dims=f.dims,
            modality_group=f.modality_group,
            family=f.family,
            organism=f.organisms[0] if f.organisms else None,
            channels=largest.channels if largest else None,
            n_items=f.n_items,
            admitted=decision.admitted,
            rule=decision.rule,
        )
        if decision.admitted:
            row.fit = fit(f, spec)
            if row.fit.ok:
                row.positions = positions(f, spec)
            if f.download_method == "other":
                row.notes.append(MANUAL_DROP_NOTE)
        rows.append(row)
    pool = [row for row in rows if row.in_pool]
    errors = _policy_errors(pool, spec)
    if pool:
        weighting = spec.sampling.weighting
        temperature = {"uniform_over": 0.0, "proportional": 1.0}.get(weighting.scheme, weighting.temperature)
        for row_id, weight in weights(pool, weighting.over, temperature).items():
            rows_by_id = {row.id: row for row in pool}
            rows_by_id[row_id].weight = weight
        for row in pool:
            if row.id in spec.splits.holdout_ids:
                row.splits = {name: (row.n_items if name == "test" else 0) for name in spec.splits.fractions}
            else:
                row.splits = split_counts(row.n_items, spec.splits.fractions)
    warnings = list(spec.warnings)
    if "content_hash" in spec.select.dedup.by:
        warnings.append(CONTENT_HASH_WARNING)
    return Plan(spec.name, spec_hash, catalog_ref, rows, errors, warnings)
