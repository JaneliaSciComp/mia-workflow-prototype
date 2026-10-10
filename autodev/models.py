"""Dataclasses shared by workflows and activities (plain, so Temporal serializes them as JSON)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Feedback:
    id: str  # "review-<id>" | "comment-<id>"
    text: str


@dataclass
class IssueInput:
    issue: int
    max_revisions: int = 3
    poll_minutes: int = 2
    key: str = ""  # set on first execution; names this attempt's workspaces, job ids and refs
    round: int = 0
    pr: int | None = None
    revisions: int = 0
    handled: list[str] = field(default_factory=list)  # carried across continue_as_new


@dataclass
class StageInput:
    issue: int
    stage: str | None  # a key of config.STAGES, or None to remove every stage label


@dataclass
class CommentInput:
    number: int  # issue or PR
    body: str
    marker: str  # posted at most once per marker


@dataclass
class RoundInput:
    issue: int
    key: str
    round: int
    pr: int | None
    feedback: str | None


@dataclass
class Workspace:
    dir: str  # round directory: prompt.md, repo/, result.*, exit_code
    base_sha: str
    branch: str


@dataclass
class SubmitInput:
    issue: int
    key: str
    round: int
    workspace: Workspace


@dataclass
class JobRef:
    job_id: str
    walltime_min: int


@dataclass
class AwaitInput:
    job_id: str
    workspace: Workspace


@dataclass
class JobOutcome:
    job_id: str
    state: str  # csub/LSF: DONE | EXIT | UNKNOWN
    exit_code: int | None = None


@dataclass
class PublishInput:
    issue: int
    key: str
    round: int
    workspace: Workspace
    pr: int | None
    feedback_id: str | None = None


@dataclass
class PrRef:
    pr: int | None
    head_sha: str | None


@dataclass
class ObserveInput:
    pr: int


@dataclass
class PrSnapshot:
    pr: int
    state: str  # open | closed | merged
    head_sha: str
    approved: bool  # a qualifying human approval of head_sha exists
    checks: str  # success | pending | failure | none
    feedback: list[Feedback] = field(default_factory=list)  # not yet handled per GitHub markers


@dataclass
class MergeInput:
    issue: int
    pr: int
    sha: str


@dataclass
class MergeResult:
    merged: bool
    message: str


@dataclass
class FailureInput:
    issue: int
    key: str
    round: int
    error: str
    workspace: Workspace | None = None


@dataclass
class Status:
    issue: int
    state: str
    round: int
    pr: int | None
    job_id: str | None
    last_error: str | None
