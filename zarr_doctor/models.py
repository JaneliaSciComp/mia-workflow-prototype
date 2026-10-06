"""Dataclasses passed between flow tasks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class JobOutcome:
    job_id: str
    status: str  # cluster_api JobStatus value: done | failed | killed | ...
    exit_code: int | None = None
    max_mem: str | None = None
    runtime_s: float | None = None
    log_path: str = ""


@dataclass
class FailureInput:
    issue: int
    run_id: str
    stage: str
    error: str
    outcome: JobOutcome | None = None
