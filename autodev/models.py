"""Dataclasses passed between the autodev workflow and its activities.

Temporal serializes activity arguments and results as JSON, so everything here is a plain dataclass of
strings, ints, bools and lists. The `*Input` classes are activity arguments; the rest are activity results
or state the workflow carries between executions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

KEY_RE = re.compile(r"[0-9a-f]{8}")  # the shape of IssueInput.key: the first 8 hex chars of a Temporal run id


class InvalidKeyError(ValueError):
    pass


def check_key(key: str) -> str:
    """Reject anything that is not an 8-hex-char workflow key.

    The key becomes part of workspace directory names and job-id file names, so a crafted value could
    escape those directories."""
    if not KEY_RE.fullmatch(key):
        raise InvalidKeyError(f"invalid workflow key {key!r}: want 8 hex chars")
    return key


@dataclass
class Feedback:
    """One reviewer request for changes, from a PR review or a `/revise` comment."""

    id: str  # "review-<id>" | "comment-<id>"
    text: str


@dataclass
class IssueInput:
    """The durable state of one issue's workflow.

    It is the workflow's start argument and is passed to continue_as_new, so every field that must
    survive a history rollover lives here."""

    issue: int
    max_revisions: int = 3
    poll_minutes: int = 2
    key: str = ""  # set on first execution; names this attempt's workspaces, job ids and refs
    round: int = 0
    pr: int | None = None
    revisions: int = 0
    handled: list[str] = field(default_factory=list)  # feedback ids already addressed by a round
    escalated: str | None = None  # decide.escalation() of the escalation already raised with a human


@dataclass
class StageInput:
    issue: int
    stage: str | None  # a key of config.STAGES, or None to remove every stage label


@dataclass
class CommentInput:
    number: int  # issue or PR number
    body: str
    marker: str  # the comment is posted at most once per marker


@dataclass
class RoundInput:
    issue: int
    key: str
    round: int
    pr: int | None
    feedback: str | None


@dataclass
class Workspace:
    """Where one agent round runs: the directory holding prompt.md, repo/, result.* and exit_code."""

    dir: str
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
    state: str  # csub/LSF state: DONE | EXIT | UNKNOWN
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
    """What to observe, plus the workflow's decision context: the activity fails (retryably) while the
    snapshot would decide WAIT, so one activity with a fixed-interval retry policy is the poll loop."""

    pr: int
    handled: list[str] = field(default_factory=list)
    revisions: int = 0
    max_revisions: int = 3
    escalated: str | None = None


@dataclass
class PrSnapshot:
    """What the PR looks like right now, as read by the observe_pr activity."""

    pr: int
    state: str  # open | closed | merged
    head_sha: str
    approved: bool  # a qualifying human approval of head_sha exists (or github.auto_merge with green required checks)
    checks: str  # success | pending | failure | none
    feedback: list[Feedback] = field(default_factory=list)  # requests not yet handled, per GitHub markers


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
    """The answer to the workflow's `status` query."""

    issue: int
    state: str
    round: int
    pr: int | None
    job_id: str | None
    last_error: str | None
