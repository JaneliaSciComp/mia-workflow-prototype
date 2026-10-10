"""Temporal workflows. No I/O here; activities are called by name (see activities.py)."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from .decide import CLOSED, DONE, ESCALATE, MERGE, REVISE, decide
    from .models import (AwaitInput, CommentInput, FailureInput, Feedback, IssueInput, JobOutcome, JobRef,
                         MergeInput, MergeResult, ObserveInput, PrRef, PrSnapshot, PublishInput, RoundInput,
                         StageInput, Status, SubmitInput, Workspace)

ROUND_DONE_LOG = "issue #%d round %d: job %s ended %s"
MAX_POLLS_PER_EXECUTION = 500
RETRY_MAX_INTERVAL = timedelta(minutes=5)
QUEUE_SLACK = timedelta(hours=12)  # ponytail: covers LSF pending time; use schedule-to-start metrics if it bites


class RoundFailed(Exception):
    pass


def _err(e: BaseException) -> str:
    while isinstance(e, ActivityError) and e.cause:
        e = e.cause
    return str(e) or type(e).__name__


@workflow.defn
class IssueWorkflow:
    @workflow.init
    def __init__(self, inp: IssueInput) -> None:
        self.s = inp
        self.state = "STARTING"
        self.job_id: str | None = None
        self.last_error: str | None = None
        self.workspace: Workspace | None = None

    @workflow.query(name="status")
    def status(self) -> Status:
        return Status(self.s.issue, self.state, self.s.round, self.s.pr, self.job_id, self.last_error)

    async def _act(self, name: str, arg, seconds: int = 60, budget: timedelta = timedelta(hours=6), result_type=None):
        # retry by time budget; activities raise non-retryable ApplicationError for permanent failures
        return await workflow.execute_activity(
            name, arg, start_to_close_timeout=timedelta(seconds=seconds), schedule_to_close_timeout=budget,
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL), result_type=result_type)

    async def _comment(self, number: int, body: str, marker: str) -> None:
        await self._act("comment", CommentInput(number, body, marker))

    @workflow.run
    async def execute(self, inp: IssueInput) -> str:
        s = self.s
        s.key = s.key or workflow.info().run_id[:8]
        try:
            if s.pr is None:
                s.pr = (await self._act("find_pr", s.issue, result_type=PrRef)).pr
            if s.pr is None:
                await self._act("set_stage", StageInput(s.issue, "working"))
                res = await self._round(None)
                if res.pr is None:
                    raise RoundFailed("the agent made no commits; its summary is above")
                s.pr = res.pr
            await self._act("set_stage", StageInput(s.issue, "review"))
            return await self._watch()
        except (ActivityError, RoundFailed) as e:
            self.state, self.last_error = "FAILED", _err(e)
            await self._act("report_failure", FailureInput(s.issue, s.key, s.round, self.last_error, self.workspace),
                            budget=timedelta(hours=24))
            return "failed"

    async def _round(self, fb: Feedback | None) -> PrRef:
        s = self.s
        s.round += 1
        self.state, self.job_id = "PREPARING", None
        self.workspace = ws = await self._act(
            "prepare_round", RoundInput(s.issue, s.key, s.round, s.pr, fb.text if fb else None),
            seconds=600, result_type=Workspace)
        job = await self._act("submit_agent", SubmitInput(s.issue, s.key, s.round, ws),
                              result_type=JobRef)
        self.state, self.job_id = "RUNNING", job.job_id
        await self._comment(s.pr or s.issue, f"Round {s.round} started: agent job `{job.job_id}`.",
                            f"started {s.key}-{s.round}")
        out = await workflow.execute_activity(
            "await_agent", AwaitInput(job.job_id, ws), result_type=JobOutcome,
            task_queue=workflow.info().task_queue + "-await",
            start_to_close_timeout=timedelta(minutes=job.walltime_min) + QUEUE_SLACK,
            schedule_to_close_timeout=timedelta(minutes=job.walltime_min) + timedelta(hours=24),
            heartbeat_timeout=timedelta(minutes=3), retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL))
        workflow.logger.info(ROUND_DONE_LOG, s.issue, s.round, out.job_id, out.state)
        if out.state != "DONE" or out.exit_code not in (0, None):
            raise RoundFailed(f"agent job {out.job_id} ended {out.state} (exit {out.exit_code})")
        self.state = "PUBLISHING"
        return await self._act("publish_round", PublishInput(s.issue, s.key, s.round, ws, s.pr, fb.id if fb else None),
                               seconds=300, result_type=PrRef)

    async def _watch(self) -> str:
        s = self.s
        for _ in range(MAX_POLLS_PER_EXECUTION):
            self.state = "REVIEW"
            snap = await self._act("observe_pr", ObserveInput(s.pr), result_type=PrSnapshot)
            action, detail = decide(snap, s.handled, s.revisions, s.max_revisions)
            if action == DONE:
                await self._act("set_stage", StageInput(s.issue, "done"))
                return "merged"
            if action == CLOSED:
                await self._act("set_stage", StageInput(s.issue, None))
                return "closed"
            if action == MERGE:
                r = await self._act("merge_pr", MergeInput(s.issue, s.pr, snap.head_sha), result_type=MergeResult)
                if r.merged:
                    await self._act("set_stage", StageInput(s.issue, "done"))
                    return "merged"
                await self._comment(s.pr, f"Approved, but merging failed: {r.message}", f"merge-failed {snap.head_sha}")
            elif action == REVISE:
                s.handled.append(detail.id)
                s.revisions += 1
                await self._act("set_stage", StageInput(s.issue, "working"))
                await self._round(detail)
                await self._act("set_stage", StageInput(s.issue, "review"))
                continue
            elif action == ESCALATE:
                await self._comment(s.pr, f"Needs a human: {detail}.", f"escalate {detail} {snap.head_sha}")
            await workflow.sleep(timedelta(minutes=s.poll_minutes))
            if workflow.info().is_continue_as_new_suggested():
                break
        workflow.continue_as_new(s)


@workflow.defn
class PollWorkflow:
    @workflow.run
    async def execute(self) -> list[int]:
        return await workflow.execute_activity(
            "poll_issues", start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1), result_type=list[int])
