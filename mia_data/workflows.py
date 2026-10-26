"""The Temporal workflows that take a dataset issue from `go` label to published crops.

`MiaDataDataset` lives as long as the issue does. Each `go` starts a round: an agent matches the dataset to
a loader snippet, the worker smoke-tests that match, test crops are made and shown on the issue, a human
approves them, and the full set is cropped and published. When no snippet fits, the round instead opens
an issue in mia-snippets for autodev to write one and follows that PR until it merges.

Workflow code does no I/O: every GitHub, filesystem and cluster interaction is an activity called by name
(see activities.py). The durable state is a `DatasetState`, handed to continue_as_new at quiet points.
"""

from __future__ import annotations

import contextlib
import dataclasses
import math
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from autodev.workflows import QUEUE_SLACK, RETRY_MAX_INTERVAL, error_message

    from .crop_test import describe_failure, job_succeeded, judge
    from .models import (
        DatasetState,
        GalleryResult,
        JobArgs,
        JobDone,
        LabelEvent,
        LockTaken,
        MatchResult,
        Pins,
        PrUpdate,
        RoundLimits,
        SmokeResult,
        Test,
        VerifyResult,
        job_key,
    )

GO, APPROVE = "data:go", "data:approve"
BUSY = "busy, ignored: a round is already running. Add it again once the issue shows a final stage."
NOT_REVIEW = "`data:approve` counts only while the issue is in `data:review`, for the round under review."
MAX_ROUNDS_PER_EXECUTION = 20  # roll the history over between rounds at the latest after this many
DEVELOP = ("developing", "testing", "pr-ready")  # the stages spent following a snippet PR
RAW_LOCK_TTL_S = 900  # a job lock lapses this long after its last heartbeat (await_job beats every poll)


class RoundFailed(Exception):
    """A job ended in a state the round cannot continue from."""


def _rollover_due() -> bool:
    return workflow.info().is_continue_as_new_suggested()


def _parse_time(iso: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp as UTC; None if missing or malformed."""
    if iso is None:
        return None
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@workflow.defn(name="MiaDataDataset")
class MiaDataDataset:
    """One workflow per dataset issue, forever: each new `go` runs a round (match → test → review → approve →
    crop → publish) that ends in a dataset stage (done, needs-human, ineligible), then waits for the next `go`."""

    @workflow.init
    def __init__(self, state: DatasetState) -> None:
        self.state = state  # durable: carried into continue_as_new
        self.go_events: list[LabelEvent] = []  # signals not yet acted on
        self.approve_events: list[LabelEvent] = []
        self.limits: RoundLimits | None = None  # read from config at the start of a round

    @workflow.signal
    def go(self, event: LabelEvent) -> None:
        self.go_events.append(event)

    @workflow.signal
    def approve(self, event: LabelEvent) -> None:
        self.approve_events.append(event)

    @workflow.signal
    def pr_update(self, update: PrUpdate) -> None:
        """A wake-up for the develop path. The workflow re-reads the PR rather than trusting the signal."""
        if update not in self.state.pending:
            self.state.pending = (self.state.pending + [update])[-50:]

    @workflow.query
    def status(self) -> DatasetState:
        return self.state

    @workflow.run
    async def run(self, state: DatasetState) -> None:
        rounds = 0
        if state.stage == "review" and state.tests:  # rolled over while waiting for an approve
            await self._guard(self._from_review())
        elif state.stage in DEVELOP and state.snippet_issue:  # ... or while following the snippet PR
            await self._guard(self._follow_pr())
        while True:
            await workflow.wait_condition(lambda: bool(self.go_events or self.approve_events) or _rollover_due())
            if not (self.go_events or self.approve_events):
                workflow.continue_as_new(self.state)  # idle, nothing queued
            await self._refuse_queued(gos=False)  # idle: an approve has no round under review to apply to
            if not self.go_events:
                continue
            event = self.go_events.pop(0)
            if self._seen(event):  # redelivery
                continue
            rounds += 1
            await self._guard(self._round(event))
            if (rounds >= MAX_ROUNDS_PER_EXECUTION or _rollover_due()) and not (self.go_events or self.approve_events):
                workflow.continue_as_new(self.state)

    # --- helpers ---------------------------------------------------------------------------

    async def _act(
        self, name: str, *args: Any, seconds: int = 60, budget: timedelta = timedelta(hours=6), result_type: Any = None
    ) -> Any:
        """Run an activity with a per-attempt timeout and an overall retry budget.

        Transient failures retry until the budget runs out; activities raise a non-retryable
        ApplicationError for failures that a retry cannot fix."""
        return await workflow.execute_activity(
            name,
            args=list(args),
            start_to_close_timeout=timedelta(seconds=seconds),
            schedule_to_close_timeout=budget,
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
            result_type=result_type,
        )

    async def _limits(self) -> RoundLimits:
        """The round's config values. They are not part of the durable state, so a rollover re-reads them."""
        if self.limits is None:
            self.limits = await self._act("limits", result_type=RoundLimits)
        return self.limits

    async def _guard(self, step: Awaitable[None]) -> None:
        """Run a step of the round; any failure ends the round in needs-human instead of failing the workflow."""
        try:
            await step
        except (ActivityError, RoundFailed) as error:
            await self._finish("needs-human", error_message(error))

    def _seen(self, event: LabelEvent) -> bool:
        """True if this label event was handled before; otherwise record it as handled now."""
        if event.event_id in self.state.handled:
            return True
        self.state.handled = (self.state.handled + [event.event_id])[-50:]
        return False

    async def _stage(
        self, stage: str, detail: str = "", budget: timedelta = timedelta(hours=6), extra: str = ""
    ) -> None:
        """Move the issue to `stage`: swap its label and post the stage comment."""
        self.state.stage = stage
        await self._act("set_stage", self.state.issue, stage, self.state, detail, extra, budget=budget)

    async def _refuse_queued(self, gos: bool = True, approves: bool = True) -> None:
        """Every label queued now arrived while it couldn't count: answer each new one with refuse_label."""
        queued = []
        if approves:
            queued, self.approve_events = [(APPROVE, NOT_REVIEW, event) for event in self.approve_events], []
        if gos:
            queued, self.go_events = queued + [(GO, BUSY, event) for event in self.go_events], []
        for label, reason, event in queued:
            if not self._seen(event):
                await self._refuse(label, reason, event)

    async def _refuse(self, label: str, reason: str, event: LabelEvent) -> None:
        try:
            await self._act("refuse_label", self.state.issue, label, reason, event.event_id)
        except ActivityError as error:  # best effort: the event is handled either way
            workflow.logger.warning(
                "issue #%d: could not refuse %s %s: %s", self.state.issue, label, event.event_id, error_message(error)
            )

    async def _finish(self, stage: str, detail: str = "", extra: str = "") -> None:
        """End the round in a dataset stage, then refuse whatever queued up while it ran. Never raises."""
        try:
            await self._stage(stage, detail, budget=timedelta(hours=24), extra=extra)
        except ActivityError as error:
            workflow.logger.error("issue #%d: could not set %s: %s", self.state.issue, stage, error_message(error))
        await self._refuse_queued()

    async def _prepare(self, kind: str) -> str:
        """Make the workspace for a smoke/crop/verify job. Returns its path."""
        state = self.state
        return await self._act(
            "prepare_job", state.issue, state.round_id, kind, 1, state.binding, seconds=300, result_type=str
        )

    async def _job(
        self,
        kind: str,
        workspace: str,
        workspace_kind: str | None = None,
        lock: list[str] | None = None,
        **job_args: Any,
    ) -> JobDone:
        """Submit a job (at most once per key) and wait for it on the await queue.

        The wait is its own activity on a separate queue so a worker restart reattaches to the running job.
        `workspace_kind` distinguishes several jobs of one kind in a round (crop-<sha>); it defaults to `kind`.
        `lock` ([kind, key, owner], from _hold_raw) is heartbeated by the wait while the job runs."""
        state = self.state
        assert state.round_id and state.pins  # a round is admitted before any job runs
        job_id = await self._act(
            "submit",
            job_key(state.issue, state.round_id, workspace_kind or kind, 1),
            kind,
            JobArgs(workspace, state.round_id, state.pins, **job_args),
            seconds=600,
            result_type=str,
        )
        walltime = timedelta(minutes=(await self._limits()).walltime_min)
        return await workflow.execute_activity(
            "await_job",
            args=[job_id, workspace, *([lock] if lock else [])],  # two arguments as before when there is no lock
            result_type=JobDone,
            task_queue=workflow.info().task_queue + "-await",
            start_to_close_timeout=walltime + QUEUE_SLACK,
            schedule_to_close_timeout=walltime + timedelta(hours=24),
            heartbeat_timeout=timedelta(minutes=3),
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
        )

    async def _hold_raw(self, kind: str) -> list[str]:
        """Take the lock on the round record's raw cache for a `kind` (smoke, crop) job that writes it, waiting while
        another job holds it; the issue says so once per round. Returns the lock as [kind, key, owner]."""
        state = self.state
        assert state.round_id and state.pins  # a round is admitted before any job runs
        record, owner, commented, tries = state.pins.record, f"mia-data {state.round_id} {kind}", False, 0
        while not (
            taken := await self._act("take_lock", "raw", record, owner, RAW_LOCK_TTL_S, result_type=LockTaken)
        ).ok:
            if not commented:
                waiting = f"waiting for the raw cache of `{record}`: held by {taken.holder}"
                await self._act("comment", state.issue, waiting, f"lock {state.round_id}")
                commented = True
            # ponytail: poll_minutes, doubling each try up to 30 min (~20 history events an hour once backed off);
            # a wait of many days wants a wake-up signal from the releasing worker.
            poll_minutes = (await self._limits()).poll_minutes
            await workflow.sleep(timedelta(minutes=min(poll_minutes * 2**tries, 30)))
            tries += 1
        return ["raw", record, owner]

    async def _release_raw(self, lock: list[str] | None) -> None:
        """Release a lock _hold_raw took; never raises: a lock left behind lapses after its TTL."""
        if lock is None:
            return
        try:
            await self._act("release_lock", *lock, budget=timedelta(minutes=30))
        except ActivityError as error:
            workflow.logger.warning("issue #%d: could not release %s: %s", self.state.issue, lock, error_message(error))

    async def _crop(
        self, tag: str, sha: str, n_crops: int, seed: str, batch: int, trusted: bool = True
    ) -> tuple[str, str, VerifyResult, JobDone, JobDone]:
        """Run crop → verify (+ preview) → read_verify. Returns (crop workspace, verify workspace, verify
        result, crop job, verify job).

        The workspaces are crop-<tag> and verify-<tag>; `tag` is the tested SHA, or "production". The jobs are
        returned so the caller can judge their exit codes; a job whose state is uncertain fails the round.
        `trusted=False` (a PR head: unreviewed code) mounts the dataset cache read-only. A trusted crop job writes the
        cache, so it holds the record's raw lock from before the crop job until after read_verify."""
        assert self.state.pins  # a round is admitted before any job runs
        lock = await self._hold_raw("crop") if trusted and workflow.patched("c-raw-lock") else None
        try:
            crop_workspace = await self._prepare(f"crop-{tag}")
            crop_job = await self._job(
                "crop",
                crop_workspace,
                f"crop-{tag}",
                lock,
                sha=sha,
                n_crops=n_crops,
                seed=seed,
                batch=batch,
                trusted=trusted,
                existing=batch > 0,
            )
            if crop_job.state not in ("DONE", "EXIT"):
                raise RoundFailed(describe_failure("crop", crop_job))
            verify_workspace = await self._prepare(f"verify-{tag}")
            verify_job = await self._job(  # mounts no cache; the wait keeps the lock alive until it is released
                "verify", verify_workspace, f"verify-{tag}", lock, crop_workspace=crop_workspace, existing=batch > 0
            )
            if verify_job.state not in ("DONE", "EXIT"):
                raise RoundFailed(describe_failure("verify", verify_job))
            verify = await self._act(
                "read_verify",
                verify_workspace,
                crop_workspace,
                *([self.state.pins.record] if lock else []),  # with the lock: put the raw asset row
                result_type=VerifyResult,
            )
        finally:
            await self._release_raw(lock)
        return crop_workspace, verify_workspace, verify, crop_job, verify_job

    async def _gallery(
        self, tag: str, crop_workspace: str, verify_workspace: str, verify: VerifyResult
    ) -> GalleryResult:
        state = self.state
        return await self._act(
            "publish_gallery",
            state.issue,
            state.round_id,
            tag,
            crop_workspace,
            verify_workspace,
            verify.crops,
            seconds=300,
            result_type=GalleryResult,
        )

    async def _test(self, sha: str, seed: str, trusted: bool = True, notes: tuple = ()) -> tuple[Test, list[str], str]:
        """Make the test crops at `sha`, judge them and publish their gallery.

        Returns (the Test record, descriptions of failed jobs, the markdown report for the issue)."""
        state = self.state
        crop_workspace, verify_workspace, verify, crop_job, verify_job = await self._crop(
            sha, sha, (await self._limits()).test_n, seed, 0, trusted
        )
        passed, failed_jobs = judge(verify, [crop_job, verify_job])
        gallery = await self._gallery(sha, crop_workspace, verify_workspace, verify) if verify.crops else None
        test = Test(
            sha, f"{state.round_id}-{sha[:12]}", passed, workflow.now().isoformat(), gallery.prefix if gallery else None
        )
        lines = [
            f"Test crops at `{sha}`: **{'passed' if passed else 'failed'}**, {verify.ok_count} of "
            f"{verify.n} verified; the crop job fetched {verify.bytes_fetched} bytes.",
            *failed_jobs,
            *(f"- {error}" for error in verify.errors),
            *notes,
            "",
            gallery.table if gallery else "",
        ]
        return test, failed_jobs, "\n".join(lines)

    # --- the round -------------------------------------------------------------------------

    async def _round(self, event: LabelEvent) -> None:
        """One round from a `go`: admit, match, smoke, test crops, then wait for review.

        After a develop round ended in needs-human, the `go` resumes its snippet PR instead when that PR is still
        open (see _resume_pr); its snippets issue is remembered either way, so a retry never opens a second one."""
        state = self.state
        snippet_issue, snippet_pr = None, None  # what the previous round's develop path left behind, if anything
        if state.stage == "needs-human" and state.snippet_issue and workflow.patched("resume-snippet-pr"):
            snippet_issue, snippet_pr = state.snippet_issue, state.pr
        state.round_id, state.seed = f"i{state.issue}-e{event.event_id}", str(workflow.random().getrandbits(64))
        state.pins = state.binding = state.binding_hash = state.snippet_name = state.snippet_issue = state.pr = None
        state.tests, state.attempts, state.pending = [], {}, []
        state.pins = await self._act("admit", state.issue, event.event_id, seconds=300, result_type=Pins)
        self.limits = await self._act("limits", result_type=RoundLimits)
        if snippet_issue and snippet_pr and await self._resume_pr(snippet_issue, snippet_pr):
            return
        await self._stage("matching")

        # Match: the agent picks a snippet and writes a binding, then the worker smoke-tests that binding itself.
        workspace = await self._act(
            "prepare_match", state.issue, state.round_id, state.pins, 1, seconds=600, result_type=str
        )
        if not job_succeeded(job := await self._job("match", workspace)):
            return await self._finish("needs-human", describe_failure("match", job))
        match = await self._act("read_match", workspace, seconds=300, result_type=MatchResult)
        if match.outcome == "needs_snippet":
            return await self._develop(match, snippet_issue)
        if match.outcome not in ("matched", "ineligible"):
            return await self._finish("needs-human", f"match blocked: {match.reason}")
        state.binding, state.binding_hash = match.binding_path, match.binding_hash
        if workflow.patched("c-raw-lock"):  # smoke writes the record's raw cache: under its lock
            lock = await self._hold_raw("smoke")
            try:
                workspace = await self._prepare("smoke")
                job = await self._job("smoke", workspace, lock=lock)
                ran = job_succeeded(job)
                if ran:
                    smoke = await self._act("read_smoke", workspace, state.pins.record, result_type=SmokeResult)
            finally:
                await self._release_raw(lock)
            if not ran:
                return await self._finish("needs-human", describe_failure("smoke", job))
        else:
            workspace = await self._prepare("smoke")
            if not job_succeeded(job := await self._job("smoke", workspace)):
                return await self._finish("needs-human", describe_failure("smoke", job))
            smoke = await self._act("read_smoke", workspace, result_type=SmokeResult)
        if not smoke.ok:
            why = "; ".join(smoke.errors) or "no errors reported"
            return await self._finish(
                "needs-human",
                f"smoke failed: {why}"
                + (f" (host not allowed: {smoke.host_not_allowed})" if smoke.host_not_allowed else ""),
            )
        if smoke.eligible != (match.outcome == "matched"):
            return await self._finish(
                "needs-human",
                f"the match agent says {match.outcome}, but smoke found the data "
                f"{'eligible' if smoke.eligible else 'ineligible'}",
            )
        if not smoke.eligible:
            return await self._finish("ineligible", f"no raw volume is at least 256×256; shapes: {smoke.shapes}")

        # Test crops at the matched snippets commit, shown on the issue for review.
        sha = state.pins.snippets_commit
        await self._stage("testing")
        test, failed_jobs, report = await self._test(
            sha, state.seed + "-t", notes=("", "Binding reasons:", *(f"- {reason}" for reason in match.reasons))
        )
        state.tests.append(test)
        await self._act(
            "comment", state.issue, report, f"test {state.round_id} {sha}", "data", state.binding, state.binding_hash
        )
        if not test.ok:
            return await self._finish(
                "needs-human", "; ".join(["the test crops failed; nothing was produced", *failed_jobs])
            )
        await self._stage("review")
        await self._refuse_queued(approves=False)  # gos that came while the round ran; approves are judged below
        await self._from_review()

    async def _from_review(self) -> None:
        if await self._review():
            await self._produce()  # else a new go came during review: the next round starts from matching

    async def _produce(self) -> None:
        """Make and publish the full set of crops. Passing crops stay published even when the round ends in
        needs-human."""
        state = self.state
        assert state.pins and state.seed  # set at admission; carried in the durable state across rollovers
        sha = state.pins.snippets_commit
        await self._stage("cropping")
        n_crops = (await self._limits()).per_round
        crop_workspace, verify_workspace, verify, crop_job, _ = await self._crop(
            "production", sha, n_crops, state.seed + "-p", 1
        )
        # verify's exit is not checked: harness verify exits 1 whenever a crop fails; its report decides
        published = await self._act(
            "publish_crops",
            state.issue,
            state.round_id,
            crop_workspace,
            verify.crops,
            sha,
            state.binding,
            seconds=300,
            result_type=dict,
        )
        table = (
            (await self._gallery("production", crop_workspace, verify_workspace, verify)).table if verify.crops else ""
        )
        n_effective = n_crops if verify.n_effective is None else min(n_crops, verify.n_effective)
        needed = max(1, math.ceil(n_effective / 2))
        extra = (
            f"{table}\n**Totals:** {len(published['published'])} new crops published this round "
            f"({verify.ok_count} of {n_effective} passed); {published['total']} in the dataset\n"
        )
        if not job_succeeded(
            crop_job
        ):  # a limit or a crash: the surviving crops are published, the round needs a human
            return await self._finish(
                "needs-human",
                f"{describe_failure('crop', crop_job)}; the {verify.ok_count} crops that passed are published",
                extra,
            )
        if verify.ok_count >= needed:
            return await self._finish("done", extra=extra)
        await self._finish(
            "needs-human",
            f"only {verify.ok_count} of {n_effective} crops passed (at least {needed} "
            f"needed); the ones that passed are published",
            extra,
        )

    async def _review(self) -> bool:
        """Wait for an approve newer than the test result (True), or a new go (False). Stale approves are refused."""
        tested_at = _parse_time(self.state.tests[-1].finished_at) or workflow.now()  # unparseable: nothing older counts
        while True:
            await workflow.wait_condition(lambda: bool(self.approve_events or self.go_events) or _rollover_due())
            if not (self.approve_events or self.go_events):
                workflow.continue_as_new(self.state)  # stage review: the next run resumes here
            while self.approve_events:
                event = self.approve_events.pop(0)
                if self._seen(event):
                    continue
                if (approved_at := _parse_time(event.at)) and approved_at > tested_at:
                    return True
                await self._refuse(
                    APPROVE, "it is older than the round's test crops; add it again to approve them", event
                )
            self.go_events = [go for go in self.go_events if go.event_id not in self.state.handled]
            if self.go_events:
                return False

    # --- the development path ---------------------------------------------------------------

    async def _develop(self, match: MatchResult, snippet_issue: int | None = None) -> None:
        """No snippet fits: open the mia-snippets issue (autodev writes the snippet), then follow its PR.

        `snippet_issue` is the issue an earlier round opened for this dataset, when the workflow still knows it:
        the activity reuses it while it is open rather than asking autodev for the same snippet twice."""
        state = self.state
        state.snippet_issue = await self._act(
            "open_snippet_issue",
            state.issue,
            state.round_id,
            match.draft or {},
            *([snippet_issue] if snippet_issue else []),  # the extra argument only when it carries something
            seconds=300,
            result_type=int,
        )
        await self._stage(
            "developing",
            f"autodev is writing the snippet in mia-snippets issue #{state.snippet_issue}; "
            "each PR head gets test crops",
        )
        await self._follow_pr()

    async def _resume_pr(self, snippet_issue: int, pr_number: int) -> bool:
        """A `go` after a develop round ended in needs-human: pick its snippet PR up again if it is still open,
        and test the current head afresh under the new round and pins (spec §7.4, "comment and re-trigger").

        Returns False when the PR was merged or closed meanwhile, or is not this snippets issue's: the round
        then matches as usual (a merged snippet is on main now)."""
        state = self.state
        pr = await self._act("pr_state", pr_number, result_type=PrUpdate)
        if pr.state != "open" or pr.snippet_issue != snippet_issue:
            return False
        state.snippet_issue, state.pr = snippet_issue, pr_number
        await self._stage(
            "testing",
            f"resuming snippet PR {pr.url or f'#{pr.pr}'} from the previous round; its current head gets test crops again",
        )
        if not await self._on_pr(pr_number, pr):
            await self._follow_pr()
        return True

    async def _follow_pr(self) -> None:
        """Wake on pr_update until the round ends, refusing labels meanwhile (busy). Rolls over only here, idle."""
        state = self.state
        await self._limits()
        while True:
            await workflow.wait_condition(
                lambda: bool(state.pending or self.go_events or self.approve_events) or _rollover_due()
            )
            if not (state.pending or self.go_events or self.approve_events):
                workflow.continue_as_new(state)  # the develop state is all in `state`: the next run resumes here
            await self._refuse_queued()
            while state.pending:
                if await self._on_pr(state.pending.pop(0).pr):
                    return

    async def _on_pr(self, pr_number: int, pr: PrUpdate | None = None) -> bool:
        """Act on the PR as it is now (never on the signal's copy) until nothing is left to do.

        `pr` is the PR as the caller just read it; otherwise it is read here. Returns True when the round ended
        (merged, closed, needs-human), False when waiting for a new head."""
        state = self.state
        pr = pr or await self._act("pr_state", pr_number, result_type=PrUpdate)
        while pr.snippet_issue == state.snippet_issue:  # else another round's PR
            state.pr = pr.pr
            where = f"snippet PR {pr.url or f'#{pr.pr}'}"
            if pr.needs_human or pr.state == "closed":
                why = "its issue has `snippets:needs-human`" if pr.needs_human else "it was closed without merging"
                await self._finish("needs-human", f"{where}: {why}")
                return True
            if pr.state == "merged":
                await self._merged(pr)
                return True
            if any(test.sha == pr.head_sha for test in state.tests):
                return False  # tested: its status stands until a new head
            if len(state.tests) >= (await self._limits()).max_pr_revisions:
                await self._finish(
                    "needs-human", f"{where}: revision limit, {len(state.tests)} heads tested and none merged"
                )
                return True
            pr = await self._test_head(pr)
        return False

    async def _test_head(self, pr: PrUpdate) -> PrUpdate:
        """Test an untested PR head with the example binding the PR ships, posting the `mia-data/crops` status.

        Returns the PR as it is after the test: only a passing result for a head that is still current moves
        the stage to pr-ready."""
        state, head = self.state, pr.head_sha
        if state.stage != "testing":
            await self._stage("testing")
        await self._act("set_crop_status", head, "pending", None)
        try:
            binding = await self._act("adopt_pr_binding", state.round_id, pr.pr, head, seconds=600, result_type=dict)
            if binding["errors"]:
                test = Test(head, f"{state.round_id}-{head[:12]}", False, workflow.now().isoformat())
                report = "\n".join(
                    [
                        f"Test crops at `{head}`: **failed**, the PR's example binding was not adopted:",
                        *(f"- {error}" for error in binding["errors"]),
                    ]
                )
            else:
                state.binding, state.binding_hash, state.snippet_name = (
                    binding["path"],
                    binding["hash"],
                    binding["name"],
                )
                test, _, report = await self._test(head, f"{state.seed}-t{head[:7]}", trusted=False)
                test.tree_sha = await self._act("tree_sha", head, binding["name"])
        except (ActivityError, RoundFailed):
            with contextlib.suppress(ActivityError):  # never leave the required status pending
                await self._act("set_crop_status", head, "error", None, budget=timedelta(minutes=10))
            raise
        state.tests.append(test)
        marker = f"test {state.round_id} {head}"
        pr_url = await self._act("comment", pr.pr, report, marker, "snippets", state.binding, state.binding_hash)
        issue_url = await self._act("comment", state.issue, report, marker, "data", state.binding, state.binding_hash)
        url = pr_url or issue_url  # the commit status links the PR comment
        current = await self._act("pr_state", pr.pr, result_type=PrUpdate)
        await self._act(
            "set_crop_status", head, "success" if test.ok else "failure", url
        )  # the head's own, current or not
        if test.ok and current.head_sha == head and current.state == "open":
            await self._stage("pr-ready", extra=f"**Snippet PR:** {current.url or f'#{pr.pr}'}\n")
        return current

    async def _merged(self, pr: PrUpdate) -> None:
        """The snippet PR merged: produce crops pinned to the merge commit.

        The merged head's passing test carries over if the snippet tree is unchanged by the merge; otherwise
        the merge commit gets its own (trusted, status-less) test first. An older head's pass never counts."""
        state, merge_sha = self.state, pr.merge_sha
        if not merge_sha:
            raise RoundFailed(f"snippet PR #{pr.pr} is merged but GitHub reports no merge commit")
        assert state.pins and state.seed  # set at admission; carried in the durable state across rollovers
        tree = state.snippet_name and await self._act("tree_sha", merge_sha, state.snippet_name)
        binding = await self._act("adopt_pr_binding", state.round_id, pr.pr, merge_sha, seconds=600, result_type=dict)
        if binding["errors"]:
            return await self._finish(
                "needs-human", "; ".join(["the merged example binding was not adopted", *binding["errors"]])
            )
        state.binding, state.binding_hash, state.snippet_name = binding["path"], binding["hash"], binding["name"]
        if not (tree and any(test.ok and test.sha == pr.head_sha and test.tree_sha == tree for test in state.tests)):
            if state.stage != "testing":
                await self._stage("testing")
            test, failed_jobs, report = await self._test(merge_sha, f"{state.seed}-t{merge_sha[:7]}")
            test.tree_sha = await self._act("tree_sha", merge_sha, binding["name"])
            state.tests.append(test)
            await self._act(
                "comment",
                state.issue,
                report,
                f"test {state.round_id} {merge_sha}",
                "data",
                state.binding,
                state.binding_hash,
            )
            if not test.ok:
                return await self._finish(
                    "needs-human",
                    "; ".join(["the merge commit's test crops failed; nothing was produced", *failed_jobs]),
                )
        state.pins = dataclasses.replace(state.pins, snippets_commit=merge_sha)
        await self._produce()


@workflow.defn(name="MiaDataPoll")
class MiaDataPoll:
    """Run on a schedule: turn new labels and PR changes into signals for the dataset workflows."""

    @workflow.run
    async def run(self) -> list[str]:
        return await workflow.execute_activity(
            "poll",
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
            result_type=list[str],
        )
