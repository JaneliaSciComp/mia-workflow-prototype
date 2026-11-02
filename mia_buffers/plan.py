"""The plan: fit, channel and dimensionality policy, weights and a split preview over the data pool, rendered as
JSON (plan.json) and markdown (the plan comment). Pure functions over Spec and Facts; the CLI and later the
`planning` activity call `build_plan`.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .assume import channel_recipes, voxel_size
from .select import Decision, decide_all
from .spec import Spec
from .specmap import ArrayFacts, Facts, facts

MANUAL_DROP_NOTE = "download_method other: needs a manual drop (spec v0.4 §3)"
UNREADABLE = "_unreadable"  # set by cli.load_records on a file that is not a YAML mapping
NOT_PREPARED = "not prepared"  # the rule of a fitting row whose dataset is not prepared (server-side plans only)


def _cell(text: object) -> str:
    """Record and spec text is untrusted: no cell may add a column or a row to the table."""
    return str(text).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


CONTENT_HASH_WARNING = "dedup.by content_hash is not available; doi used"
UNKNOWN_SHAPE_NOTE = "unknown shape: planned counts assume the full request"
ITEMSIZE = {"uint8": 1, "int8": 1, "uint16": 2, "int16": 2, "float16": 2, "bfloat16": 2, "uint32": 4, "int32": 4}
ITEMSIZE |= {"float32": 4, "uint64": 8, "int64": 8, "float64": 8}
DEFAULT_ITEMSIZE = 2


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
    split: dict | None = None  # the policy registration cuts planes by: {policy, fractions, seed}; pool rows only
    notes: list[str] = field(default_factory=list)
    voxel_size_nm: dict[str, float | None] = field(default_factory=dict)
    assumed: list[str] = field(default_factory=list)  # axes whose voxel size is a guess (mia_buffers.assume)
    channel_recipe: dict | None = None  # how a crop gets the spec's channel count; None for `all`/first-c
    bytes: int | None = None  # the record's size_bytes: what fetching it adds to the raw cache
    # The record-local rule the harness derives this record's samples from, and its sha256; pool rows only.
    # Named plan_rule because `rule` is the selection rule above.
    plan_rule: dict | None = None
    plan_digest: str = ""
    planned: dict[str, int] = field(default_factory=dict)  # estimated samples per split over the raw arrays
    planned_bytes: int = 0  # upper bound on the staged chunk bytes the planned samples touch
    volumes: int = 0  # items × raw arrays: the volumes the request applies to, each capped on its own; pool rows only

    @property
    def in_pool(self) -> bool:
        return self.admitted and self.fit is not None and self.fit.ok


@dataclass
class Plan:
    spec_name: str
    spec_hash: str
    catalog_ref: str | None
    rows: list[Row]  # the records selection admitted (in the pool or not) and the unreadable ones
    errors: list[str]
    warnings: list[str]
    excluded: dict[str, int] = field(default_factory=dict)  # selection rule -> how many records it threw out

    @property
    def valid(self) -> bool:
        return not self.errors

    @property
    def n_seen(self) -> int:
        return len(self.rows) + sum(self.excluded.values())

    @property
    def rows_by_id(self) -> dict[str, Row]:
        return {row.id: row for row in self.rows}

    def to_dict(self, states: dict[str, str] | None = None) -> dict:
        """The plan as JSON; with `states` (record id → dataset state) each row also carries `prepared`."""
        data = dataclasses.asdict(self)
        data["n_seen"] = self.n_seen
        if states is not None:
            for row in data["rows"]:
                row["prepared"] = states.get(row["id"], "unprepared")
        return data

    def render_markdown(self) -> str:
        lines = [
            f"# Plan for `{self.spec_name}`",
            "",
            f"spec `{self.spec_hash[:12]}` · catalog `{self.catalog_ref or 'unpinned'}`",
            "",
        ]
        if self.errors:
            lines += ["## Errors", "", *(f"- {_cell(error)}" for error in self.errors), ""]
        if self.warnings:
            lines += ["## Warnings", "", *(f"- {_cell(warning)}" for warning in self.warnings), ""]
        pool = [row for row in self.rows if row.in_pool]
        lines += [f"## Records ({len(pool)} in the pool, {self.n_seen} seen)", ""]
        lines += [
            "| id | decision | rule | dims | modality | channels | voxel nm | positions | weight | split preview "
            "| planned | est. bytes |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for row in self.rows:
            rule = row.rule if row.fit is None else f"{row.rule}; {row.fit.reason}"
            if row.notes:
                rule += "; " + "; ".join(row.notes)
            splits = " / ".join(f"{name} {count}" for name, count in row.splits.items()) if row.in_pool else ""
            weight = f"{row.weight:.3f}" if row.in_pool else ""
            cells: list[object] = [
                row.id,
                "pool" if row.in_pool else "out",
                rule,
                row.dims or "",
                row.modality_group or "",
            ]
            cells += [
                row.channels if row.channels is not None else "",
                _voxel_cell(row),
                row.positions or "",
                weight,
                splits,
                _planned_cell(row),
                _human_bytes(row.planned_bytes) if row.in_pool else "",
            ]
            lines.append("| " + " | ".join(_cell(cell) for cell in cells) + " |")
        if pool:
            planned = sum(sum(row.planned.values()) for row in pool)
            requested = sum(sum((row.plan_rule or {}).get("samples", {}).values()) * row.volumes for row in pool)
            total_bytes = _human_bytes(sum(row.planned_bytes for row in pool))
            lines += [
                "",
                f"planned {planned} of requested {requested} samples; ~{total_bytes} uncompressed "
                "(planned is an expectation over each record's items)",
            ]
        if self.excluded:
            lines += [
                "",
                f"## Excluded by selection ({sum(self.excluded.values())})",
                "",
                "| rule | records |",
                "|---|---|",
            ]
            lines += [f"| {_cell(rule)} | {count} |" for rule, count in self.excluded.items()]
        not_prepared = [row.id for row in self.rows if row.rule == NOT_PREPARED]
        if not_prepared:
            lines += [
                "",
                f"## Not prepared ({len(not_prepared)})",
                "",
                *(f"- {_cell(row_id)}" for row_id in not_prepared),
            ]
        return "\n".join(lines) + "\n"


def _voxel_cell(row: Row) -> str:
    if not row.voxel_size_nm:
        return ""
    sizes = "×".join(f"{row.voxel_size_nm[a]:g}" for a in ("z", "y", "x") if row.voxel_size_nm.get(a) is not None)
    return f"{sizes} (assumed)" if row.assumed else sizes


def _planned_cell(row: Row) -> str:
    """`train 10 / 1000 · val 3 / 20 · …`: planned against requested samples per split, both over all its volumes."""
    if not row.in_pool or row.plan_rule is None:
        return ""
    samples = row.plan_rule["samples"]
    return " · ".join(f"{name} {row.planned.get(name, 0)} / {samples[name] * row.volumes}" for name in samples)


def _human_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1000:
            return f"{value:.0f} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{value:.1f} TB"


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
    _, assumed = voxel_size(f)
    if assumed and not spec.sampling.assume_voxel_size:
        return Fit(False, "voxel size unknown", footprint, False)
    notes = []
    if largest.shape_varies:
        notes.append("shape varies: checked against the representative item")
    if assumed:
        notes.append(f"voxel size assumed for {', '.join(assumed)}")
    return Fit(True, "fits" + (f" ({'; '.join(notes)})" if notes else ""), footprint, largest.shape_varies)


def positions(f: Facts, spec: Spec) -> int:
    """How many non-overlapping coarsest crops the dataset holds: the weighting measure."""
    patch = spec.patch_for(f.dims or "")
    if patch is None:
        return 0
    scale = max(spec.sampling.levels)
    total = 0
    for array in f.raw_arrays:
        if not (array.extent_y and array.extent_x):
            continue
        tiles = (array.extent_y // (patch["y"] * scale)) * (array.extent_x // (patch["x"] * scale))
        if "z" in patch:
            depth = (array.extent_z or 0) // patch["z"]  # 3D: how many patch-deep slabs the volume holds
        else:
            depth = _depth_2d(f, array)
        total += f.n_items * depth * tiles
    return total


def _depth_2d(f: Facts, array: ArrayFacts) -> int:
    """Planes per item of a 2D record's raw array."""
    # ponytail: a 2D record counts items or planes, whichever the enricher filled in (U937: 25 fields as
    # n_items and as t), never both; a record with both > 1 undercounts until the stage job reports planes.
    return max(f.n_items, array.planes) // f.n_items if array.planes > 1 else 1


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


def plan_hash(plan_json_text: str) -> str:
    """What a buffer's `plan_hash` is: the sha256 of the plan file's text."""
    return hashlib.sha256(plan_json_text.encode()).hexdigest()


def split_counts(n: int, fractions: dict[str, float]) -> dict[str, int]:
    """Largest-remainder apportionment of `n` keys; ties go to the earlier split in the spec's order."""
    exact = {name: n * fraction for name, fraction in fractions.items()}
    counts = {name: int(value) for name, value in exact.items()}
    remainders = sorted(fractions, key=lambda name: -(exact[name] - counts[name]))
    for name in remainders[: n - sum(counts.values())]:
        counts[name] += 1
    return counts


def plan_rule(row: Row, spec: Spec) -> dict:
    """The record-local rule the harness's `PlanRule` mirrors: same keys and value types, never a weight or total."""
    patch = spec.patch_for("2D") or {}  # as the levels key and the served config.patch: the square 2D crop
    return {
        "seed": spec.sampling.seed,
        "patch": int(patch.get("y", 0)),
        "levels": [int(level) for level in spec.sampling.levels],
        "samples": dict(spec.sampling.plan.samples),
        "split": row.split,
        "channel_recipe": row.channel_recipe,
    }


def plan_digest(rule: dict) -> str:
    """sha256 of the rule's canonical JSON; the harness's `PlanRule.digest()` hashes the same text."""
    return hashlib.sha256(json.dumps(rule, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _planned(row: Row, f: Facts, rule: dict) -> tuple[dict[str, int], int, bool]:
    """Expected samples per split over the record's items, each raw array of each item capped the way the harness
    caps a staged volume (`min(samples[split], capacity)`); the byte figure is an upper bound given those counts;
    the flag says an array had no shape (each of its items then counts the full request).

    A volume with at least one plane per split gets exactly `split_counts(planes, fractions)` planes per split (the
    harness's `split_planes`; only which planes is hashed), so 5 planes at .8/.1/.1 give test none; one with fewer
    puts each plane in one split with probability `fraction` (the registry's and the harness's draw), so the cap
    applies before the fraction. The planner counts t × z planes while the harness counts z only."""
    samples: dict[str, int] = rule["samples"]
    footprint = rule["patch"] * max(rule["levels"])
    split = row.split or {}
    fractions: dict[str, float] = split.get("fractions", {})
    held_out = split.get("policy") == "holdout"
    planned = dict.fromkeys(samples, 0)
    planned_bytes, unknown = 0, False
    for array in f.raw_arrays:
        if array.extent_y and array.extent_x:
            tiles = (array.extent_y // footprint) * (array.extent_x // footprint)
            planes = array.planes if (f.dims or "").startswith("3D") else _depth_2d(f, array)  # per item
            per_item: dict[str, float] = {}
            for name, wanted in samples.items():
                fraction = fractions.get(name, 0.0)
                if held_out:
                    per_item[name] = min(wanted, tiles * planes) if name == "test" else 0
                elif planes >= len(fractions):  # the harness's exact cut: split_counts planes go to this split
                    per_item[name] = min(wanted, tiles * split_counts(planes, fractions).get(name, 0))
                else:
                    per_item[name] = fraction * min(wanted, tiles * planes)
        else:
            per_item, unknown = dict(samples), True
        # 1e-9: 0.7 × 10 is 6.999…, an expectation of 7
        counts = {name: math.floor(f.n_items * value + 1e-9) for name, value in per_item.items()}
        itemsize = ITEMSIZE.get(array.dtype or "", DEFAULT_ITEMSIZE)
        # up to 4 chunks per sample per level, each patch² × channels × itemsize
        planned_bytes += (
            len(rule["levels"]) * sum(counts.values()) * 4 * rule["patch"] ** 2 * (row.channels or 1) * itemsize
        )
        for name, count in counts.items():
            planned[name] += count
    return planned, planned_bytes, unknown


def _some(ids: list[str], limit: int = 5) -> list[str]:
    """The first few ids with a count of the rest, so an error stays one readable line."""
    return ids if len(ids) <= limit else [*ids[:limit], f"… {len(ids) - limit} more"]


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
            shown = {count: _some(sorted(ids)) for count, ids in sorted(by_count.items())}
            errors.append(f"patch.c: all needs one channel count; pool has {shown}")
    elif channel_policy not in (1, 3):  # 1 and 3 always have a recipe (mia_buffers.assume)
        short = sorted(row.id for row in pool if (row.channels or 1) < channel_policy)
        if short:
            errors.append(
                f"patch.c: {channel_policy} needs at least {channel_policy} channels; {_some(short)} have fewer"
            )
    for name in spec.splits.fractions:
        if all(row.planned.get(name, 0) == 0 for row in pool):
            errors.append(
                f"split {name!r}: no record can supply it (planes); the buffer would serve an empty {name} set"
            )
    return errors


def _unreadable_row(path: str, record: dict, error: str) -> Row:
    raw_id = record.get("id")
    record_id = raw_id if isinstance(raw_id, str) else path.rsplit("/", 1)[-1].removesuffix(".yaml")
    return Row(record_id, path, None, None, None, None, None, 0, False, f"record unreadable: {error}")


def load_records(catalog_dir: Path) -> list[tuple[str, dict]]:
    """Every datasets/**/*.yaml under the checkout, path relative to it, sorted. Records are untrusted: safe_load only."""
    records = []
    for path in sorted((catalog_dir / "datasets").rglob("*.yaml")):
        try:
            record = yaml.safe_load(path.read_text())
        except yaml.YAMLError as error:
            record = {UNREADABLE: f"{type(error).__name__}: {str(error).splitlines()[0]}"}
        if not isinstance(record, dict):
            record = {UNREADABLE: f"not a mapping: {type(record).__name__}"}
        record.setdefault("id", path.stem)
        records.append((str(path.relative_to(catalog_dir)), record))
    return records


def build_plan(
    spec: Spec,
    spec_hash: str,
    catalog_ref: str | None,
    records: list[tuple[str, dict]],
    prepared: set[str] | None = None,
) -> Plan:
    """Fit, weigh and split the records the spec admits. With `prepared` (the ids of prepared datasets), a row
    that fits but is not prepared is out of the pool with the rule `not prepared`; None skips that rule."""
    all_facts: list[Facts] = []
    rows: list[Row] = []
    for path, record in sorted(records, key=lambda item: item[0]):
        if UNREADABLE in record:
            rows.append(_unreadable_row(path, record, str(record[UNREADABLE])))
            continue
        try:
            all_facts.append(facts(path, record))
        except Exception as error:  # one malformed record is a row with a reason, never a failed plan
            rows.append(_unreadable_row(path, record, f"{type(error).__name__}: {error}"))
    decisions: list[Decision] = decide_all(all_facts, spec.select)
    facts_by_id = {f.id: f for f in all_facts}
    excluded: Counter[str] = Counter()
    for f, decision in zip(all_facts, decisions, strict=True):
        if not decision.admitted:  # a catalog of thousands against a spec naming a few: a count per rule, not a row
            excluded[decision.rule] += 1
            continue
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
            bytes=f.size_bytes,
        )
        if decision.admitted:
            row.fit = fit(f, spec)
            row.voxel_size_nm, row.assumed = voxel_size(f)
            if largest is not None and spec.sampling.patch.c in (1, 3):
                recipes = channel_recipes(largest.channels, f.family, f.modality_group)
                row.channel_recipe = recipes["gray" if spec.sampling.patch.c == 1 else "rgb"]
            if row.fit.ok:
                row.positions = positions(f, spec)
                if prepared is not None and row.id not in prepared:
                    row.admitted, row.rule = False, NOT_PREPARED
            if f.download_method == "other":
                row.notes.append(MANUAL_DROP_NOTE)
        rows.append(row)
    rows.sort(key=lambda row: row.path)
    pool = [row for row in rows if row.in_pool]
    if pool:
        weighting = spec.sampling.weighting
        temperature = {"uniform_over": 0.0, "proportional": 1.0}.get(weighting.scheme, weighting.temperature)
        pool_by_id = {row.id: row for row in pool}
        for row_id, weight in weights(pool, weighting.over, temperature).items():
            pool_by_id[row_id].weight = weight
        for row in pool:
            held_out = row.id in spec.splits.holdout_ids
            row.split = {
                "policy": "holdout" if held_out else "fractions",
                "fractions": dict(spec.splits.fractions),
                "seed": spec.splits.seed,
            }
            if held_out:
                row.splits = {name: (row.n_items if name == "test" else 0) for name in spec.splits.fractions}
            else:
                row.splits = split_counts(row.n_items, spec.splits.fractions)
            row.plan_rule = plan_rule(row, spec)
            row.plan_digest = plan_digest(row.plan_rule)
            row.planned, row.planned_bytes, unknown = _planned(row, facts_by_id[row.id], row.plan_rule)
            row.volumes = row.n_items * len(facts_by_id[row.id].raw_arrays)
            if unknown:
                row.notes.append(UNKNOWN_SHAPE_NOTE)
    errors = _policy_errors(pool, spec)
    if spec.splits.stratify_by:
        errors.append("splits.stratify_by is not implemented; remove it")
    warnings = list(spec.warnings)
    if "content_hash" in spec.select.dedup.by:
        warnings.append(CONTENT_HASH_WARNING)
    return Plan(spec.name, spec_hash, catalog_ref, rows, errors, warnings, dict(excluded.most_common()))
