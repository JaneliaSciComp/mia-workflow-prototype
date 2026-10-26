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
    auto_approve: bool = False  # spec plan.auto_approve: sourcing starts without a review
    plan_hash: str = ""  # sha256 of the round's plan.json text, as the buffer config records it


@dataclass
class DegradedSignal:
    """The keeper's `degraded` signal: the records whose staged pyramids went missing from disk, and why."""

    records: list[str]
    reason: str


@dataclass
class LifecycleEvent:
    """A lifecycle signal the ready-state loop handles: idle, keep, retire or degraded, with its detail."""

    kind: str
    detail: str = ""


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
    artifact: str = ""  # the staged pyramid's artifact id (jobspec.artifact_id), set whenever the binding is found


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
    cache_writable: bool = True  # False: an offline stage after the fetch phase reads the raw cache only


@dataclass
class FetchJobArgs:
    """Arguments of the `submit_fetch` activity (not replayed): the fetch job fills the record's raw cache."""

    workspace: str
    round_id: str
    pins: BufferPins
    record: str
    hosts: list[str]
    attempt: int


@dataclass
class FetchResult:
    """The fetch job's fetch.json, reduced. `partial`: the job stopped before every file (byte budget, walltime);
    `stream`: urls the harness reads in place at stage time; `failed`: urls that could not be fetched."""

    ok: bool
    partial: bool = False
    bytes_fetched: int = 0
    stream: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    host_not_allowed: str | None = None


@dataclass
class QuotaCheck:
    """Whether the raw cache has room for a record: `expected` is its catalog size, `over_by` the bytes too many."""

    ok: bool
    over_by: int = 0
    expected: int = 0


@dataclass
class RawStatus:
    """Whether the record's raw cache holds every file of its binding; `missing` lists the whole-file urls not.
    `stream`: urls an earlier fetch found the harness reads in place, so a stage needs their hosts and a writable
    cache (at most 20 listed)."""

    ready: bool
    missing: list[str] = field(default_factory=list)
    stream: list[str] = field(default_factory=list)


@dataclass
class KeeperReport:
    """One pass of the cache keeper: the asset sync's counts, what it evicted, reset (`stale`, "<kind> <key>
    <state>") and removed (`.building-*` paths), the bytes each kind is still over quota by, the catalog it synced
    ("<ref> (<n> records)", "" when not due), the steps that failed, and the free assets spared because a fetch
    or stage holds their record's raw lock."""

    scanned: int = 0
    missing: int = 0
    new: int = 0
    evicted: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    removed_building: list[str] = field(default_factory=list)
    over_quota: dict[str, int] = field(default_factory=dict)
    catalog: str = ""
    errors: list[str] = field(default_factory=list)
    locked: list[str] = field(default_factory=list)  # "<kind> <key> (<lock owner>)": free, but a job holds its raw lock
    signalled: list[str] = field(default_factory=list)  # "<signal> <buffer>[ <records>]" sent to a buffer's workflow


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
    parallel_fetch: int = 2  # fetch jobs in flight per buffer
    fetch_walltime_min: int = 480
    fetch_attempts: int = 4
    poll_minutes: int = 2
    lock_ttl_s: int = 900  # a job lock lapses this long after its last heartbeat (await_job beats every poll)


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
    pending | waiting-dataset | waiting-drop | waiting-lock | fetching | blocked | staging | servable | failed |
    dropped.
    `waiting-dataset`: no approved binding yet, mia-data is working on `issue` (spec §3 step 1); `waiting-lock`:
    another job holds the record's raw-cache lock; `blocked`: the raw cache is over its quota, the fetch waits;
    `dropped`: left out of the plan (ineligible, or still waiting at the timeout) and of the ready denominator."""

    record: str
    group: str
    manual: bool
    status: str = "pending"
    volumes: int = 0
    reason: str = ""
    issue: int | None = None  # the dataset's issue number in the datasets repo, once looked up
    folder: str = ""  # <storage_root>/<folder>/binding.yaml is what the poller watches while waiting-dataset
    voxel_size_nm: dict[str, float] | None = None  # from the plan, handed to the stage job
    weight: float = 0.0  # the plan's weight, so the heaviest datasets are fetched first


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


def fetch_job_key(issue: int, round_id: str, record: str, attempt: int) -> str:
    """csub job key of a dataset's fetch job, one per attempt; otherwise as job_key."""
    digest = hashlib.sha256(record.encode()).hexdigest()[:8]
    return f"buf-{issue}-{round_id}-fetch{attempt}-{record[:40]}-{digest}"
