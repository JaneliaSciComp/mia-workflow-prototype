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


def job_key(issue: int, round_id: str, kind: str, attempt: int) -> str:
    """csub job key, unique per round, stage and attempt (the workspace is <kind>-<attempt> under the round)."""
    return f"mia-{issue}-{round_id}-{kind}-{attempt}"


@dataclass
class JobArgs:
    ws: str  # from prepare_match / prepare_job
    round_id: str
    pins: Pins
    sha: str | None = None  # smoke/crop: mia-snippets commit to load; default pins.snippets_commit
    n: int = 0  # crop
    seed: str = ""
    batch: int = 0
    trusted: bool = False  # crop: reviewed code, dataset cache read-write
    existing: bool = False  # crop/verify: --existing <dataset folder>/manifest.json, when there is one
    crop_ws: str | None = None  # verify: the crop job's workspace


@dataclass
class MatchResult:
    outcome: str  # matched | needs_snippet | ineligible | blocked (blocked = needs-human, with the reason)
    reason: str = ""
    binding_path: str | None = None  # matched/ineligible: the checked binding, under state_dir
    binding_hash: str | None = None
    shapes: dict | None = None  # ineligible
    draft: dict | None = None  # needs_snippet: draft_binding, what_fails, tried


@dataclass
class SmokeResult:
    ok: bool
    eligible: bool = False
    shapes: dict | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    bytes_fetched: int = 0
    host_not_allowed: str | None = None


@dataclass
class VerifyResult:
    ok_count: int
    n: int
    errors: list[str]
    crops: list[dict]  # verified crops only: [{"crop_id", "files": {rel: sha256}}]


@dataclass
class GalleryResult:
    prefix: str
    urls: dict[str, str]  # rel -> ?raw=true URL
    table: str  # render_crops_table
