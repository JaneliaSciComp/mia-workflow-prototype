"""Dataclasses shared by workflows and activities (plain, so Temporal serializes them as JSON)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RunRequest:
    trigger: str  # "project" | "label" | "cli"
    requested_at: str


@dataclass
class Discovered:
    path: str
    fingerprint: str


@dataclass
class IssueInput:
    issue_number: int
    path: str
    runs: int = 0
    pending: list[RunRequest] = field(default_factory=list)  # carried across continue_as_new


@dataclass
class AckInput:
    issue: int
    run_id: str
    trigger: str


@dataclass
class SubmitInput:
    issue: int
    path: str
    run_id: str


@dataclass
class SubmitResult:
    job_id: str
    walltime_minutes: int


@dataclass
class AwaitInput:
    issue: int
    run_id: str
    job_id: str


@dataclass
class JobOutcome:
    job_id: str
    status: str  # cluster_api JobStatus value: done | failed | killed | ...
    exit_code: int | None = None
    max_mem: str | None = None
    runtime_s: float | None = None
    log_path: str = ""


@dataclass
class PublishInput:
    issue: int
    run_id: str
    path: str
    outcome: JobOutcome


@dataclass
class FinishInput:
    issue: int
    run_id: str


@dataclass
class FailureInput:
    issue: int
    run_id: str
    stage: str
    error: str
    outcome: JobOutcome | None = None


@dataclass
class WorkflowStatus:
    issue: int
    path: str
    state: str
    runs: int
    run_id: str | None
    job_id: str | None
    last_error: str | None
