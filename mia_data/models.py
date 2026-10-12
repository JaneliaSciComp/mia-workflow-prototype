"""Plain dataclasses, JSON-serialisable for Temporal."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LabelEvent:
    kind: str  # go | approve
    event_id: str
    actor: str
    at: str  # ISO-8601 from GitHub


@dataclass
class PrUpdate:
    pr: int
    head_sha: str
    state: str
    merge_sha: str | None
    snippet_issue: int
    needs_human: bool


@dataclass
class Pins:
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
    __test__ = False  # not a pytest class
    sha: str
    test_id: str
    ok: bool | None = None
    finished_at: str | None = None
    gallery_prefix: str | None = None
    tree_sha: str | None = None


@dataclass
class DatasetState:
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
    pending: list[PrUpdate] = field(default_factory=list)
