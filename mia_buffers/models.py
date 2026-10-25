"""Dataclasses passed between the MiaBuffer workflow and its activities.

Temporal serializes them as JSON, so they hold only strings, numbers, bools, lists and dicts. Activity results
are wire format: they live in recorded histories, so fields are added with defaults and never renamed.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field


@dataclass
class BufferPins:
    """What a round is pinned to at admission: the spec text, the catalog commit and the harness version."""

    spec_hash: str
    catalog_ref: str
    harness_commit: str
    env_digest: str


@dataclass
class DatasetPlan:
    """One record of the plan's data pool, as the workflow needs it: its weighting group and whether its files
    must be dropped by hand."""

    record: str
    group: str
    weight: float
    manual: bool
    voxel_size_nm: dict[str, float] | None = None  # nm per spatial axis for the stage job; z = 1.0 for a 2D stack


@dataclass
class PlanResult:
    """The `plan` activity's summary; the full plan.json and plan.md stay in the round directory."""

    valid: bool
    errors: list[str]
    pool: list[DatasetPlan]
    n_seen: int
    min_fraction: float = 1.0  # spec ready.min_fraction
    timeout_days: float = 14  # spec sourcing.dataset_timeout_days
    levels: list[int] = field(default_factory=list)
    patch: int = 0


@dataclass
class BindingInfo:
    """A dataset's approved binding as found under storage_root, with the hosts its stage job needs, or the
    files a human has to drop (`manual`: [url, incoming name] pairs) when the hosts are not reachable."""

    found: bool
    path: str | None = None
    hash: str | None = None
    hosts: list[str] = field(default_factory=list)
    manual: list[list[str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    staged: bool = False  # buffer-api's registry knows a pyramid staged from this very binding, still on disk


@dataclass
class StageJobArgs:
    """Arguments of the `submit` activity (not replayed). `out` is filled in by the activity from the config."""

    workspace: str
    round_id: str
    pins: BufferPins
    record: str
    hosts: list[str]
    levels: list[int]
    patch: int
    out: str = ""
    voxel_size_nm: dict[str, float] | None = None


@dataclass
class StageResult:
    """The stage job's stage.json, reduced."""

    ok: bool
    volumes: int = 0
    errors: list[str] = field(default_factory=list)
    bytes_fetched: int = 0
    host_not_allowed: str | None = None


@dataclass
class DropResult:
    """What `verify_drop` found under raw_root/<record>/incoming/."""

    ok: bool
    placed: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class RegisterResult:
    registered: list[str] = field(default_factory=list)
    refused: dict[str, str] = field(default_factory=dict)


@dataclass
class SourcingLimits:
    """Config values the workflow needs but cannot read."""

    parallel: int
    walltime_min: int
    service_url: str


@dataclass
class DatasetIssue:
    """What `nudge_dataset` found or did on the dataset's mia-data issue: its number, its `data:<stage>` (or
    `ineligible`, `new`), whether `data:go` was added now, and a note for the issue comment."""

    number: int
    stage: str
    nudged: bool
    note: str = ""
    folder: str = ""  # the dataset folder under storage_root, where the poller watches for binding.yaml


@dataclass
class DatasetStatus:
    """Where one dataset of the pool is in sourcing:
    pending | waiting-dataset | waiting-drop | staging | servable | failed | dropped.
    `waiting-dataset`: no approved binding yet, mia-data is working on `issue` (spec §3 step 1); `dropped`: left out
    of the plan (ineligible, or still waiting at the timeout) and of the ready denominator."""

    record: str
    group: str
    manual: bool
    status: str = "pending"
    volumes: int = 0
    reason: str = ""
    issue: int | None = None  # the dataset's issue number in the datasets repo, once looked up
    folder: str = ""  # <storage_root>/<folder>/binding.yaml is what the poller watches while waiting-dataset
    voxel_size_nm: dict[str, float] | None = None  # from the plan, handed to the stage job


@dataclass
class BufferState:
    """The durable state of one buffer issue's workflow, carried across continue_as_new."""

    issue: int
    schema: int = 1
    handled: list[str] = field(default_factory=list)  # go + approve event ids, newest last; keep <= 50
    round_id: str | None = None  # i<issue>-e<event id>; also the buffer id
    stage: str = "new"
    pins: BufferPins | None = None
    buffer_id: str | None = None
    planned_at: str | None = None  # the plan comment's time; an approve must be newer
    datasets: list[DatasetStatus] = field(default_factory=list)
    ready: bool = False
    min_fraction: float = 1.0
    timeout_days: float = 14
    levels: list[int] = field(default_factory=list)
    patch: int = 0


def job_key(issue: int, round_id: str, record: str) -> str:
    """csub job key of a dataset's stage job: unique per round and record, within csub's 120 characters."""
    digest = hashlib.sha256(record.encode()).hexdigest()[:8]
    return f"buf-{issue}-{round_id}-stage-{record[:40]}-{digest}"
