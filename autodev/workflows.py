"""The Temporal workflows that drive one GitHub issue from `go` label to merged PR.

Workflow code does no I/O of its own: every GitHub, git and cluster interaction is an activity, called by
name so this module never imports the activity implementations (see activities.py). The workflow keeps
its durable state in an `IssueInput` and hands that to continue_as_new when its history grows long.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from .decide import CLOSED, DONE, ESCALATE, MERGE, REVISE, decide
    from .models import (
        KEY_RE,
        AwaitInput,
        CommentInput,
        FailureInput,
        Feedback,
        IssueInput,
        JobOutcome,
        JobRef,
        MergeInput,
        MergeResult,
        ObserveInput,
        PrRef,
        PrSnapshot,
        PublishInput,
        RoundInput,
        StageInput,
        Status,
        SubmitInput,
        Workspace,
    )

ROUND_DONE_LOG = "issue #%d round %d: job %s ended %s"
MAX_POLLS_PER_EXECUTION = 500
RETRY_MAX_INTERVAL = timedelta(minutes=5)
QUEUE_SLACK = timedelta(hours=12)  # ponytail: covers LSF pending time; use schedule-to-start metrics if it bites


class RoundFailed(Exception):
    """An agent round ended without a usable result (job failed, or no commits were made)."""


def error_message(error: BaseException) -> str:
    """The message of the innermost cause of an activity failure, for posting to GitHub."""
    while isinstance(error, ActivityError) and error.cause:
        error = error.cause
    return str(error) or type(error).__name__


@workflow.defn
class IssueWorkflow:
    """One workflow per issue: prepare a workspace, run the agent, open a PR, then watch the PR until it
    is merged, closed, or needs a human. Reviewer change requests start another agent round."""

    @workflow.init
    def __init__(self, inp: IssueInput) -> None:
        self.state = inp  # durable: carried into continue_as_new
        self.phase = "STARTING"  # for the status query only
        self.job_id: str | None = None
        self.last_error: str | None = None
        self.workspace: Workspace | None = None

    @workflow.query(name="status")
    def status(self) -> Status:
        state = self.state
        return Status(state.issue, self.phase, state.round, state.pr, self.job_id, self.last_error)

    async def _act(
        self,
        name: str,
        arg: Any,
        seconds: int = 60,
        budget: timedelta = timedelta(hours=6),
        result_type: type | None = None,
    ) -> Any:
        """Run an activity with a per-attempt timeout and an overall retry budget.

        Transient failures retry until the budget runs out; activities raise a non-retryable
        ApplicationError for failures that a retry cannot fix."""
        return await workflow.execute_activity(
            name,
            arg,
            start_to_close_timeout=timedelta(seconds=seconds),
            schedule_to_close_timeout=budget,
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
            result_type=result_type,
        )

    async def _comment(self, number: int, body: str, marker: str) -> None:
        await self._act("comment", CommentInput(number, body, marker))

    @workflow.run
    async def execute(self, inp: IssueInput) -> str:
        state = self.state
        if not KEY_RE.fullmatch(state.key):  # unset, or a bad value from a hand-started run: it names directories
            state.key = workflow.info().run_id[:8]
        try:
            if state.pr is None:  # a restarted workflow may already have a PR from an earlier attempt
                state.pr = (await self._act("find_pr", state.issue, result_type=PrRef)).pr
            if state.pr is None:
                await self._act("set_stage", StageInput(state.issue, "working"))
                result = await self._run_round(None)
                if result.pr is None:
                    raise RoundFailed("the agent made no commits; its summary is above")
                state.pr = result.pr
            await self._act("set_stage", StageInput(state.issue, "review"))
            return await self._watch_pr()
        except (ActivityError, RoundFailed) as error:
            self.phase, self.last_error = "FAILED", error_message(error)
            await self._act(
                "report_failure",
                FailureInput(state.issue, state.key, state.round, self.last_error, self.workspace),
                budget=timedelta(hours=24),
            )
            return "failed"

    async def _run_round(self, feedback: Feedback | None) -> PrRef:
        """One agent round: prepare a workspace, submit the agent job, wait for it, publish its commits.

        With `feedback`, the round is a revision of the existing PR; otherwise it is the first attempt."""
        state = self.state
        state.round += 1
        self.phase, self.job_id = "PREPARING", None
        self.workspace = workspace = await self._act(
            "prepare_round",
            RoundInput(state.issue, state.key, state.round, state.pr, feedback.text if feedback else None),
            seconds=600,
            result_type=Workspace,
        )
        job = await self._act(
            "submit_agent", SubmitInput(state.issue, state.key, state.round, workspace), result_type=JobRef
        )
        self.phase, self.job_id = "RUNNING", job.job_id
        await self._comment(
            state.pr or state.issue,
            f"Round {state.round} started: agent job `{job.job_id}`.",
            f"started {state.key}-{state.round}",
        )
        # Waiting on the job runs on its own task queue so long waits don't starve the short activities.
        outcome = await workflow.execute_activity(
            "await_agent",
            AwaitInput(job.job_id, workspace),
            result_type=JobOutcome,
            task_queue=workflow.info().task_queue + "-await",
            start_to_close_timeout=timedelta(minutes=job.walltime_min) + QUEUE_SLACK,
            schedule_to_close_timeout=timedelta(minutes=job.walltime_min) + timedelta(hours=24),
            heartbeat_timeout=timedelta(minutes=3),
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
        )
        workflow.logger.info(ROUND_DONE_LOG, state.issue, state.round, outcome.job_id, outcome.state)
        if outcome.state != "DONE" or outcome.exit_code not in (0, None):
            raise RoundFailed(f"agent job {outcome.job_id} ended {outcome.state} (exit {outcome.exit_code})")
        self.phase = "PUBLISHING"
        result = await self._act(
            "publish_round",
            PublishInput(state.issue, state.key, state.round, workspace, state.pr, feedback.id if feedback else None),
            seconds=300,
            result_type=PrRef,
        )
        if result.head_sha:  # pushed: a later observe/merge failure must not attach this round's log tail
            self.workspace = None
        return result

    async def _watch_pr(self) -> str:
        """Poll the PR until it is merged or closed, running revision rounds and escalations as needed.

        Rolls the history over with continue_as_new when Temporal suggests it or the poll cap is hit."""
        state = self.state
        assert state.pr is not None  # a round opened the PR before _watch_pr runs
        for _ in range(MAX_POLLS_PER_EXECUTION):
            self.phase = "REVIEW"
            snapshot = await self._act("observe_pr", ObserveInput(state.pr), result_type=PrSnapshot)
            action, detail = decide(snapshot, state.handled, state.revisions, state.max_revisions)
            if action == DONE:
                await self._act("set_stage", StageInput(state.issue, "done"))
                return "merged"
            if action == CLOSED:
                await self._act("set_stage", StageInput(state.issue, None))
                return "closed"
            if action == MERGE:
                merge = await self._act(
                    "merge_pr", MergeInput(state.issue, state.pr, snapshot.head_sha), result_type=MergeResult
                )
                if merge.merged:
                    await self._act("set_stage", StageInput(state.issue, "done"))
                    return "merged"
                await self._comment(
                    state.pr, f"Approved, but merging failed: {merge.message}", f"merge-failed {snapshot.head_sha}"
                )
            elif action == REVISE:
                assert isinstance(detail, Feedback)  # decide() returns the Feedback to act on for REVISE
                state.handled.append(detail.id)
                state.revisions += 1
                await self._act("set_stage", StageInput(state.issue, "working"))
                await self._run_round(detail)
                await self._act("set_stage", StageInput(state.issue, "review"))
                continue
            elif action == ESCALATE:
                await self._comment(state.pr, f"Needs a human: {detail}.", f"escalate {detail} {snapshot.head_sha}")
            await workflow.sleep(timedelta(minutes=state.poll_minutes))
            if workflow.info().is_continue_as_new_suggested():
                break
        workflow.continue_as_new(state)


@workflow.defn
class PollWorkflow:
    """Run on a schedule: find issues labelled `go` and start an IssueWorkflow for each."""

    @workflow.run
    async def execute(self) -> list[int]:
        return await workflow.execute_activity(
            "poll_issues",
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
            result_type=list[int],
        )
