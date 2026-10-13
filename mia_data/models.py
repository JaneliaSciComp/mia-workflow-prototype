"""Dataclasses passed between the mia-data workflow and its activities.

Temporal serializes them as JSON, so they hold only strings, numbers, bools, lists and dicts. Job
reports are reduced to these small, typed results before they enter workflow history.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LabelEvent:
    """A human adding `data:go` or `data:approve` to a dataset issue, delivered to the workflow as a signal."""
    kind: str  # go | approve
    event_id: str
    actor: str
    at: str  # ISO-8601 from GitHub


@dataclass
class PrUpdate:
    """The current state of a snippet PR in mia-snippets, as the poller or the pr_state activity reads it."""
    pr: int
    head_sha: str
    state: str
    merge_sha: str | None
    snippet_issue: int
    needs_human: bool  # the snippets issue has `snippets:needs-human`
    url: str | None = None  # the PR's html_url


@dataclass
class Pins:
    """What a round is pinned to at admission: the record, the snippets commit and the harness version.

    A round never moves off its pins, so every job in it sees the same inputs."""
    record: str
    record_commit: str
    snippets_commit: str
    harness_commit: str
    env_digest: str


@dataclass
class JobDone:
    job_id: str
    state: str
    exit_code: int | None


@dataclass
class Test:
    """The test crops run against one snippets commit: whether it passed and where its gallery is."""
    __test__ = False  # not a pytest class
    sha: str
    test_id: str
    ok: bool | None = None
    finished_at: str | None = None
    gallery_prefix: str | None = None
    tree_sha: str | None = None


@dataclass
class DatasetState:
    """The durable state of one dataset issue's workflow, carried across continue_as_new.

    Most fields describe the current round and are reset when a new `go` starts one."""
    issue: int
    schema: int = 1
    handled: list[str] = field(default_factory=list)  # go + approve event ids, newest last; keep <= 50
    round_id: str | None = None  # i<issue>-e<event id>
    seed: str | None = None
    stage: str = "new"
    pins: Pins | None = None
    binding: str | None = None  # path under state_dir
    binding_hash: str | None = None
    snippet_name: str | None = None
    snippet_issue: int | None = None
    pr: int | None = None
    tests: list[Test] = field(default_factory=list)
    attempts: dict[str, int] = field(default_factory=dict)  # stage -> attempts used
    pending: list[PrUpdate] = field(default_factory=list)  # PR updates not yet acted on


def job_key(issue: int, round_id: str, kind: str, attempt: int) -> str:
    """csub job key, unique per round, stage and attempt (the workspace is <kind>-<attempt> under the round)."""
    return f"mia-{issue}-{round_id}-{kind}-{attempt}"


@dataclass
class JobArgs:
    """Arguments of the `submit` activity. Which fields matter depends on the job kind."""
    workspace: str  # from prepare_match / prepare_job
    round_id: str
    pins: Pins
    sha: str | None = None  # smoke/crop: mia-snippets commit to load; default pins.snippets_commit
    n_crops: int = 0  # crop: how many crops to make
    seed: str = ""
    batch: int = 0
    trusted: bool = False  # crop: reviewed code, dataset cache read-write
    existing: bool = False  # crop/verify: --existing <dataset folder>/manifest.json, when there is one
    crop_workspace: str | None = None  # verify: the crop job's workspace


@dataclass
class MatchResult:
    """What the match agent concluded about the dataset, after the worker checked its binding."""
    outcome: str  # matched | needs_snippet | ineligible | blocked (blocked = needs-human, with the reason)
    reason: str = ""
    binding_path: str | None = None  # matched/ineligible: the checked binding, under state_dir
    binding_hash: str | None = None
    shapes: dict | None = None  # ineligible
    draft: dict | None = None  # needs_snippet: draft_binding, what_fails, tried
    reasons: list[str] = field(default_factory=list)  # matched/ineligible: the binding's reasons


@dataclass
class SmokeResult:
    """The worker's own quick check of a binding: can the data be opened, and is it big enough to crop?"""
    ok: bool
    eligible: bool = False
    shapes: dict | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    bytes_fetched: int = 0
    host_not_allowed: str | None = None


@dataclass
class VerifyResult:
    """The verify job's report, reduced to the crops that passed."""
    ok_count: int
    n: int  # how many crops verify was asked to check (named `n` on the wire: it is in recorded histories)
    errors: list[str]
    crops: list[dict]  # verified crops only: [{"crop_id", "files": {rel: sha256}}]
    n_effective: int | None = None  # the crop job's n, lowered by capacity (its report.json); None if unknown
    bytes_fetched: int = 0  # the crop job's, from its report.json


@dataclass
class RoundLimits:
    """The config values a round runs with, read at admission (the workflow can't read config)."""
    test_n: int
    per_round: int
    walltime_min: int
    max_pr_revisions: int = 5  # PR heads tested per round (develop path)


@dataclass
class GalleryResult:
    prefix: str
    urls: dict[str, str]  # rel -> ?raw=true URL
    table: str  # the markdown table from render_crops_table
