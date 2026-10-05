"""Temporal workflows. No I/O and no zarr/cluster_api/httpx imports here; activities are called by name."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from .models import (AckInput, AwaitInput, Discovered, FailureInput, FinishInput, IssueInput, JobOutcome,
                         PublishInput, RunRequest, SubmitInput, SubmitResult, WorkflowStatus)

RUN_STARTED_LOG = "issue #%d run %s started (trigger=%s)"
MAX_RUNS_PER_EXECUTION = 50
MAX_HISTORY_EVENTS = 5000


def workflow_id(issue: int) -> str:
    return f"zarr-doctor/issue-{issue}"


class _RunFailed(Exception):
    def __init__(self, msg: str, outcome: JobOutcome | None = None):
        super().__init__(msg)
        self.outcome = outcome


def _err(e: BaseException) -> str:
    while isinstance(e, ActivityError) and e.cause:
        e = e.cause
    return str(e) or type(e).__name__


@workflow.defn
class IssueWorkflow:
    @workflow.init
    def __init__(self, inp: IssueInput) -> None:
        self.issue = inp.issue_number
        self.path = inp.path
        self.runs = inp.runs
        self.pending: list[RunRequest] = list(inp.pending)
        self.state = "IDLE"
        self.run_id: str | None = None
        self.job_id: str | None = None
        self.last_error: str | None = None
        self.cancel_requested = False
        self._await_handle = None

    # --- handlers ---------------------------------------------------------------------------

    @workflow.signal(name="run")
    def on_run(self, req: RunRequest) -> None:
        if self.state != "IDLE" or self.pending:
            workflow.logger.info("issue #%d: ignoring run signal while %s", self.issue, self.state)
            return
        self.pending.append(req)

    @workflow.signal(name="cancel")
    def on_cancel(self) -> None:
        if self.state in ("ACKED", "SUBMITTED", "RUNNING"):
            self.cancel_requested = True

    @workflow.signal(name="discovered")
    def on_discovered(self, d: Discovered) -> None:
        self.path = d.path

    @workflow.query(name="status")
    def status(self) -> WorkflowStatus:
        return WorkflowStatus(self.issue, self.path, self.state, self.runs, self.run_id, self.job_id, self.last_error)

    # --- main loop --------------------------------------------------------------------------

    @workflow.run
    async def execute(self, inp: IssueInput) -> None:
        done_here = 0
        while True:
            await workflow.wait_condition(lambda: bool(self.pending))
            req = self.pending.pop(0)
            await self._run_once(req)
            done_here += 1
            info = workflow.info()
            if (done_here >= MAX_RUNS_PER_EXECUTION or info.is_continue_as_new_suggested()
                    or info.get_current_history_length() > MAX_HISTORY_EVENTS):
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(IssueInput(self.issue, self.path, self.runs, self.pending))

    async def _act(self, name: str, arg, seconds: int, attempts: int, result_type=None):
        return await workflow.execute_activity(
            name, arg, start_to_close_timeout=timedelta(seconds=seconds),
            retry_policy=RetryPolicy(maximum_attempts=attempts), result_type=result_type)

    async def _run_once(self, req: RunRequest) -> None:
        self.runs += 1
        self.run_id = run_id = f"r{self.runs}-{workflow.now():%Y%m%d%H%M%S}"
        self.job_id, self.last_error, self.cancel_requested, self._await_handle = None, None, False, None
        self.state, stage, outcome = "ACKED", "ack_run", None
        workflow.logger.info(RUN_STARTED_LOG, self.issue, run_id, req.trigger)
        try:
            await self._act("ack_run", AckInput(self.issue, run_id, req.trigger), 30, 5)
            stage = "submit"
            sub = await self._act("submit_histogram_job", SubmitInput(self.issue, self.path, run_id), 60, 3,
                                  result_type=SubmitResult)
            self.job_id, self.state = sub.job_id, "SUBMITTED"
            if self.cancel_requested:
                raise _RunFailed("cancelled")
            stage, self.state = "await_job", "RUNNING"
            self._await_handle = handle = workflow.start_activity(
                "await_job", AwaitInput(self.issue, run_id, sub.job_id), result_type=JobOutcome,
                task_queue=workflow.info().task_queue + "-await",
                start_to_close_timeout=timedelta(minutes=sub.walltime_minutes + 15),
                heartbeat_timeout=timedelta(minutes=2), retry_policy=RetryPolicy(maximum_attempts=3))
            await workflow.wait_condition(lambda: handle.done() or self.cancel_requested)
            if not handle.done():
                raise _RunFailed("cancelled")
            outcome = await handle
            if outcome.status != "done":
                raise _RunFailed(f"LSF job {outcome.job_id} {outcome.status} (exit {outcome.exit_code})", outcome)
            stage, self.state = "publish", "PUBLISHING"
            await self._act("publish_results", PublishInput(self.issue, run_id, self.path, outcome), 120, 5)
            stage = "finish_run"
            await self._act("finish_run", FinishInput(self.issue, run_id), 30, 5)
            self.state = "IDLE"
        except asyncio.CancelledError:  # the workflow itself is being cancelled: report, then honour it
            await self._fail(stage, "workflow cancelled", outcome, kill_job=True)
            raise
        except (ActivityError, _RunFailed) as e:
            await self._fail(stage, _err(e), outcome, kill_job=str(e) == "cancelled")

    async def _fail(self, stage: str, error: str, outcome: JobOutcome | None, kill_job: bool) -> None:
        self.state, self.last_error = "FAILED-REPORTED", error
        run_id, job_id, handle = self.run_id, self.job_id, self._await_handle

        async def cleanup() -> None:
            if handle and not handle.done():
                handle.cancel()
                try:
                    await handle
                except (ActivityError, asyncio.CancelledError):
                    pass
            if kill_job and job_id:
                try:
                    await self._act("cluster_cancel", job_id, 60, 3)
                except ActivityError as e:
                    workflow.logger.warning("issue #%d: cancelling job %s failed: %s", self.issue, job_id, _err(e))
            try:
                await self._act("report_failure", FailureInput(self.issue, run_id, stage, error, outcome), 60, 5)
            except ActivityError as e:
                workflow.logger.error("issue #%d: could not report failure of %s: %s", self.issue, run_id, _err(e))

        await asyncio.shield(asyncio.ensure_future(cleanup()))
        self.state = "IDLE"


@workflow.defn
class PollWorkflow:
    @workflow.run
    async def execute(self) -> list[int]:
        return await workflow.execute_activity(
            "poll_github", start_to_close_timeout=timedelta(seconds=90),
            retry_policy=RetryPolicy(maximum_attempts=1), result_type=list[int])
