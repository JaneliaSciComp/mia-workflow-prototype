"""The Temporal workflows that take a buffer issue from `buffer:go` to a served buffer (spec §1).

`MiaBuffer` lives as long as the issue does. Each `go` starts a round: the spec in the issue body is planned
against the catalog, a human approves the plan, then every dataset of the pool is sourced in parallel (a stage
job, or a manual drop first) and registered with buffer-api as it becomes servable; the issue is `ready` once
every weighting group reaches the spec's `ready.min_fraction`, and the rest keep joining. A dataset with no
approved binding is handed to mia-data (`data:go` on its dataset issue) and waits for the poller's
`dataset_update`; one that ends ineligible, or is still waiting at the timeout, is dropped from the plan.

Workflow code does no I/O: every GitHub, filesystem, cluster and service interaction is an activity called by
name (see activities.py). The durable state is a `BufferState`, handed to continue_as_new at quiet points.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from autodev.workflows import QUEUE_SLACK, RETRY_MAX_INTERVAL, error_message
    from mia_data.models import JobDone, LabelEvent

    from .models import (
        BindingInfo,
        BufferPins,
        BufferState,
        DatasetIssue,
        DatasetStatus,
        DropResult,
        PlanResult,
        RegisterResult,
        SourcingLimits,
        StageJobArgs,
        StageResult,
        job_key,
    )
    from .status import render_ready

GO, APPROVE = "buffer:go", "buffer:approve"
BUSY = "busy, ignored: a round is already running. Add it again once the issue shows a final stage."
NOT_REVIEW = "`buffer:approve` counts only while the issue is in `buffer:plan-review`, for the plan under review."
MAX_ROUNDS_PER_EXECUTION = 20  # roll the history over between rounds at the latest after this many
WAITING = ("waiting-dataset", "waiting-drop")  # dataset statuses that wait on a human or on mia-data


class RoundFailed(Exception):
    """A step of the round that ends it in needs-human (raised by workflow code, never by an activity)."""


def _rollover_due() -> bool:
    return workflow.info().is_continue_as_new_suggested()


def _parse_time(iso: str | None) -> datetime | None:
    if not iso:
        return None
    try:
        parsed = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@workflow.defn(name="MiaBuffer")
class MiaBuffer:
    """One buffer issue: go → planning → plan-review → (approve) → sourcing → ready, or needs-human."""

    @workflow.init
    def __init__(self, state: BufferState) -> None:
        self.state = state
        self.go_events: list[LabelEvent] = []
        self.approve_events: list[LabelEvent] = []
        self.drops: set[str] = set()  # records whose files a human dropped since we last looked
        self.dataset_updates: dict[str, str] = {}  # record -> "binding" | "ineligible", from the poller
        self.limits: SourcingLimits | None = None

    @workflow.signal
    def go(self, event: LabelEvent) -> None:
        self.go_events.append(event)

    @workflow.signal
    def approve(self, event: LabelEvent) -> None:
        self.approve_events.append(event)

    @workflow.signal
    def dropped(self, record: str) -> None:
        self.drops.add(record)

    @workflow.signal
    def dataset_update(self, record: str, outcome: str) -> None:
        """The poller saw the dataset's binding published (`binding`) or its issue go `data:ineligible`."""
        self.dataset_updates[record] = outcome

    @workflow.query
    def status(self) -> BufferState:
        return self.state

    @workflow.run
    async def run(self, state: BufferState) -> None:
        rounds = 0
        if state.stage == "plan-review":  # rolled over while waiting for an approve
            await self._guard(self._from_review())
        while True:
            await workflow.wait_condition(lambda: bool(self.go_events or self.approve_events) or _rollover_due())
            if not (self.go_events or self.approve_events):
                workflow.continue_as_new(self.state)  # idle, nothing queued
            await self._refuse_queued(gos=False)  # idle: an approve has no plan under review to apply to
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
        """Run an activity with a per-attempt timeout and an overall retry budget."""
        return await workflow.execute_activity(
            name,
            args=list(args),
            start_to_close_timeout=timedelta(seconds=seconds),
            schedule_to_close_timeout=budget,
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
            result_type=result_type,
        )

    async def _limits(self) -> SourcingLimits:
        if self.limits is None:
            self.limits = await self._act("limits", result_type=SourcingLimits)
        return self.limits

    async def _guard(self, step: Awaitable[None]) -> None:
        """Run a step of the round; any failure ends the round in needs-human instead of failing the workflow."""
        try:
            await step
        except (ActivityError, RoundFailed) as error:
            await self._finish("needs-human", error_message(error))

    def _seen(self, event: LabelEvent) -> bool:
        if event.event_id in self.state.handled:
            return True
        self.state.handled = (self.state.handled + [event.event_id])[-50:]
        return False

    async def _stage(
        self, stage: str, detail: str = "", budget: timedelta = timedelta(hours=6), extra: str = ""
    ) -> None:
        self.state.stage = stage
        await self._act("set_stage", self.state.issue, stage, self.state, detail, extra, budget=budget)

    async def _refuse_queued(self, gos: bool = True, approves: bool = True) -> None:
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
        """End the round in a buffer stage, then refuse whatever queued up while it ran. Never raises."""
        try:
            await self._stage(stage, detail, budget=timedelta(hours=24), extra=extra)
        except ActivityError as error:
            workflow.logger.error("issue #%d: could not set %s: %s", self.state.issue, stage, error_message(error))
        await self._refuse_queued()

    async def _comment(self, body: str, marker: str) -> None:
        await self._act("comment", self.state.issue, body, marker)

    # --- the round -------------------------------------------------------------------------

    async def _round(self, event: LabelEvent) -> None:
        """Admit, plan, post the plan, wait for review; an approve continues into sourcing."""
        state = self.state
        state.round_id = f"i{state.issue}-e{event.event_id}"
        state.pins = state.buffer_id = state.planned_at = None
        state.datasets, state.ready = [], False
        state.pins = await self._act("admit", state.issue, event.event_id, seconds=600, result_type=BufferPins)
        await self._stage("planning")
        plan = await self._act("plan", state.issue, state.round_id, state.pins, seconds=1800, result_type=PlanResult)
        await self._act("comment_plan", state.issue, state.round_id, seconds=300)
        if not plan.valid:
            return await self._finish("needs-human", "the plan is invalid: " + "; ".join(plan.errors))
        state.datasets = [
            DatasetStatus(dataset.record, dataset.group, dataset.manual, voxel_size_nm=dataset.voxel_size_nm)
            for dataset in plan.pool
        ]
        state.min_fraction, state.timeout_days = plan.min_fraction, plan.timeout_days
        state.levels, state.patch = list(plan.levels), plan.patch
        state.planned_at = workflow.now().isoformat()
        await self._stage(
            "plan-review", "approve the plan with `buffer:approve`, or edit the spec and add `buffer:go` again"
        )
        await self._refuse_queued(approves=False)  # gos that came while planning; approves are judged below
        await self._from_review()

    async def _from_review(self) -> None:
        if await self._review():
            await self._sourcing()  # else a new go came during review: the next round plans again

    async def _review(self) -> bool:
        """Wait for an approve newer than the plan (True), or a new go (False). Stale approves are refused."""
        planned_at = _parse_time(self.state.planned_at) or workflow.now()
        while True:
            await workflow.wait_condition(lambda: bool(self.approve_events or self.go_events) or _rollover_due())
            if not (self.approve_events or self.go_events):
                workflow.continue_as_new(self.state)  # stage plan-review: the next run resumes here
            while self.approve_events:
                event = self.approve_events.pop(0)
                if self._seen(event):
                    continue
                if (approved_at := _parse_time(event.at)) and approved_at > planned_at:
                    return True
                await self._refuse(APPROVE, "it is older than the plan under review; add it again to approve it", event)
            self.go_events = [go for go in self.go_events if go.event_id not in self.state.handled]
            if self.go_events:
                return False

    # --- sourcing --------------------------------------------------------------------------

    def _groups(self) -> dict[str, tuple[int, int]]:
        """group -> (servable, total) over the pool; dropped datasets are out of the denominator."""
        groups: dict[str, tuple[int, int]] = {}
        for dataset in self.state.datasets:
            if dataset.status == "dropped":
                continue
            servable, total = groups.get(dataset.group, (0, 0))
            groups[dataset.group] = (servable + (dataset.status == "servable"), total + 1)
        return groups

    def _ready_now(self) -> bool:
        groups = self._groups()
        return bool(groups) and all(servable / total >= self.state.min_fraction for servable, total in groups.values())

    def _below(self) -> list[str]:
        """The weighting groups under ready.min_fraction, sorted."""
        groups = self._groups()
        return sorted(
            group for group, (servable, total) in groups.items() if servable / total < self.state.min_fraction
        )

    async def _sourcing(self) -> None:
        """Create the buffer, source every dataset in parallel, announce ready when the groups allow it."""
        state = self.state
        assert state.round_id
        await self._stage("sourcing")
        state.buffer_id = state.round_id
        limits = await self._limits()
        await self._act("create_buffer", state.round_id, state.buffer_id, seconds=300)
        semaphore = asyncio.Semaphore(max(1, limits.parallel))
        # ponytail: no continue-as-new during sourcing (the tasks live in this run); fine below ~2,000 datasets per
        # buffer, beyond that persist per-dataset progress and resume from it
        tasks = [asyncio.create_task(self._source(dataset, semaphore)) for dataset in state.datasets]
        try:
            await workflow.wait_condition(
                lambda: self._ready_now() or all(task.done() for task in tasks),
                timeout=timedelta(days=state.timeout_days),
            )
        except TimeoutError:
            if not self._ready_now():
                below = self._below()  # before the drops: the message names what the human waited for
                await self._drop_waiting(tasks, f"still {{status}} after {state.timeout_days:g} days")
                # the waiters are gone; what is left is a stage job or a registration, bounded by the walltime
                await workflow.wait_condition(lambda: self._ready_now() or all(task.done() for task in tasks))
                if not self._ready_now():
                    return await self._finish(
                        "needs-human",
                        f"after {state.timeout_days:g} days these weighting groups are below ready.min_fraction "
                        f"{state.min_fraction:g}: {', '.join(below)}",
                    )
        if self._ready_now():
            await self._announce_ready(limits)
            if not await self._await_stragglers(tasks):
                return  # a new go: the main loop plans again
        await asyncio.gather(*tasks, return_exceptions=True)  # every task handles its own failure (_source)
        if not state.ready:
            reasons = "; ".join(f"{d.record}: {d.reason or d.status}" for d in state.datasets if d.status != "servable")
            return await self._finish("needs-human", f"no weighting group reached ready.min_fraction: {reasons}")
        await self._refuse_queued()  # labels that arrived while sourcing ran were busy, as in mia-data

    async def _await_stragglers(self, tasks: list[asyncio.Task]) -> bool:
        """After ready: wait for the remaining datasets, answering labels meanwhile. An approve is refused (nothing
        is under review); a go abandons the stragglers that are waiting for a drop and ends the round (False) so it
        can be planned again, but only once no stage job is in flight (a new round would write the same staged
        directories)."""

        pairs = list(zip(tasks, self.state.datasets, strict=True))

        def abandonable() -> bool:
            """Every unfinished task is only waiting on a human or mia-data (no job, no registration in flight)."""
            pending = [dataset for task, dataset in pairs if not task.done()]
            return bool(pending) and all(dataset.status in WAITING for dataset in pending)

        while not all(task.done() for task in tasks):
            await workflow.wait_condition(
                lambda: (
                    all(task.done() for task in tasks)
                    or bool(self.approve_events)
                    or bool(self.go_events and abandonable())
                )
            )
            await self._refuse_queued(gos=False)
            self.go_events = [go for go in self.go_events if go.event_id not in self.state.handled]
            if self.go_events and abandonable():
                for task, dataset in pairs:
                    if not task.done():
                        task.cancel()
                        dataset.status, dataset.reason = "failed", "abandoned: a new buffer:go started another round"
                return False
        return True

    async def _announce_ready(self, limits: SourcingLimits) -> None:
        state = self.state
        state.ready = True
        servable = [dataset.record for dataset in state.datasets if dataset.status == "servable"]
        await self._stage("ready", extra=render_ready(state.buffer_id or "", limits.service_url, servable))

    async def _source(self, dataset: DatasetStatus, semaphore: asyncio.Semaphore) -> None:
        """Binding → (drop) → stage job → register; the dataset's status records the outcome. An activity that
        gives up fails this dataset alone, never the round."""
        try:
            await self._source_steps(dataset, semaphore)
        except ActivityError as error:
            if dataset.status == "servable":  # only the "servable" comment failed; the volumes are registered
                workflow.logger.warning("issue #%d: %s: %s", self.state.issue, dataset.record, error_message(error))
                return
            await self._fail(dataset, error_message(error))

    async def _source_steps(self, dataset: DatasetStatus, semaphore: asyncio.Semaphore) -> None:
        state = self.state
        assert state.round_id and state.pins and state.buffer_id
        round_id, record = state.round_id, dataset.record
        binding = await self._act("read_binding", round_id, record, seconds=300, result_type=BindingInfo)
        if not binding.found:
            if not await self._await_dataset(dataset):
                return  # dropped
            binding = await self._act("read_binding", round_id, record, seconds=300, result_type=BindingInfo)
            if not binding.found:
                return await self._fail(dataset, "; ".join(binding.errors) or "binding not found")
        if binding.manual:
            await self._await_drop(dataset)
        async with semaphore:
            dataset.status = "staging"
            if not binding.staged:  # else the registry holds this binding's pyramid from an earlier round: reuse it
                workspace = await self._act(
                    "prepare_stage", state.issue, round_id, record, seconds=300, result_type=str
                )
                job = await self._job(record, workspace, binding.hosts, dataset.voxel_size_nm)
                if job.state not in ("DONE", "EXIT"):
                    return await self._fail(dataset, f"stage job {job.job_id} ended in state {job.state}")
                if job.state == "EXIT" and job.exit_code not in (0, None):
                    return await self._fail(dataset, f"stage job {job.job_id} exited with code {job.exit_code}")
            staged = await self._act("read_stage", round_id, record, seconds=300, result_type=StageResult)
        if not staged.ok:
            why = "; ".join(staged.errors) or "no errors reported"
            if staged.host_not_allowed:
                why += f" (host not allowed: {staged.host_not_allowed})"
            return await self._fail(dataset, f"stage failed: {why}")
        registered = await self._act(
            "register_dataset", round_id, record, state.buffer_id, seconds=300, result_type=RegisterResult
        )
        if not registered.registered:
            refused = "; ".join(f"{volume}: {why}" for volume, why in registered.refused.items())
            return await self._fail(dataset, f"buffer-api refused every volume: {refused or 'none staged'}")
        dataset.status, dataset.volumes, dataset.reason = "servable", len(registered.registered), ""
        reused = " The pyramid was staged earlier for this binding and reused." if binding.staged else ""
        await self._comment(
            f"`{record}` is servable: {dataset.volumes} volume(s) registered with buffer `{state.buffer_id}`.{reused}",
            f"dataset {round_id} {record} servable",
        )

    async def _await_dataset(self, dataset: DatasetStatus) -> bool:
        """No approved binding: hand the dataset to mia-data (spec §3 step 1) and wait for the poller's
        `dataset_update`. True once a binding was published; False when the dataset was dropped."""
        state = self.state
        assert state.round_id
        info = await self._act("nudge_dataset", state.round_id, dataset.record, seconds=300, result_type=DatasetIssue)
        dataset.issue, dataset.folder = info.number, info.folder
        where = f"the dataset issue #{info.number}"
        if info.stage == "ineligible":
            await self._drop(dataset, f"data:ineligible on {where}")
            return False
        dataset.status = "waiting-dataset"
        what = (
            f"`data:go` added on {where}"
            if info.nudged
            else f"{where} is already `data:{info.stage}`" + (f": {info.note}" if info.note else "")
        )
        await self._comment(
            f"`{dataset.record}` has no approved binding yet: {what}. Waiting for mia-data to publish it.",
            f"dataset {state.round_id} {dataset.record} waiting",
        )
        await workflow.wait_condition(lambda: dataset.record in self.dataset_updates)
        if self.dataset_updates.pop(dataset.record) == "ineligible":
            await self._drop(dataset, f"data:ineligible on {where}")
            return False
        return True

    async def _drop(self, dataset: DatasetStatus, reason: str) -> None:
        """Take the dataset out of the plan and the ready denominator; say so and re-post the plan."""
        state = self.state
        assert state.round_id
        dataset.status, dataset.reason = "dropped", reason
        await self._comment(
            f"`{dataset.record}` dropped from the plan: {reason}", f"dataset {state.round_id} {dataset.record} dropped"
        )
        dropped = {d.record: d.reason for d in state.datasets if d.status == "dropped"}
        await self._act("comment_plan", state.issue, state.round_id, dropped, seconds=300)

    async def _drop_waiting(self, tasks: list[asyncio.Task], reason: str) -> None:
        """At the timeout: cancel the datasets still waiting on a human or mia-data and drop them; `reason` may
        name `{status}`. One plan re-post for all of them."""
        state = self.state
        assert state.round_id
        dropped = {}
        for task, dataset in zip(tasks, state.datasets, strict=True):
            if not task.done() and dataset.status in WAITING:
                task.cancel()
                dataset.status, dataset.reason = "dropped", reason.format(status=dataset.status)
                dropped[dataset.record] = dataset.reason
        if not dropped:
            return
        for record, why in dropped.items():
            await self._comment(
                f"`{record}` dropped from the plan: {why}", f"dataset {state.round_id} {record} dropped"
            )
        await self._act(
            "comment_plan",
            state.issue,
            state.round_id,
            {d.record: d.reason for d in state.datasets if d.status == "dropped"},
            seconds=300,
        )

    async def _await_drop(self, dataset: DatasetStatus) -> None:
        """Ask for the files and wait for the poller's `dropped` signal until the drop verifies."""
        state = self.state
        assert state.round_id
        dataset.status = "waiting-drop"
        await self._act("comment_drop", state.issue, state.round_id, dataset.record, [], seconds=300)
        attempt = 0
        while True:
            await workflow.wait_condition(lambda: dataset.record in self.drops)
            self.drops.discard(dataset.record)
            result = await self._act(
                "verify_drop", state.round_id, dataset.record, seconds=1800, result_type=DropResult
            )
            if result.ok:
                return
            attempt += 1
            await self._act("comment_drop", state.issue, state.round_id, dataset.record, result.missing, seconds=300)

    async def _fail(self, dataset: DatasetStatus, reason: str) -> None:
        state = self.state
        dataset.status, dataset.reason = "failed", reason
        try:
            await self._comment(
                f"`{dataset.record}` failed: {reason}", f"dataset {state.round_id} {dataset.record} failed"
            )
        except ActivityError as error:  # best effort: the status carries the reason either way
            workflow.logger.warning(
                "issue #%d: could not post %s's failure: %s", state.issue, dataset.record, error_message(error)
            )

    async def _job(
        self, record: str, workspace: str, hosts: list[str], voxel_size_nm: dict[str, float] | None
    ) -> JobDone:
        """Submit the stage job (at most once per key) and wait for it on the await queue."""
        state = self.state
        assert state.round_id and state.pins
        args = StageJobArgs(
            workspace,
            state.round_id,
            state.pins,
            record,
            list(hosts),
            list(state.levels),
            state.patch,
            voxel_size_nm=dict(voxel_size_nm) if voxel_size_nm else None,
        )
        job_id = await self._act(
            "submit", job_key(state.issue, state.round_id, record), args, seconds=600, result_type=str
        )
        walltime = timedelta(minutes=(await self._limits()).walltime_min)
        return await workflow.execute_activity(
            "await_job",
            args=[job_id, workspace],
            result_type=JobDone,
            task_queue=workflow.info().task_queue + "-await",
            start_to_close_timeout=walltime + QUEUE_SLACK,
            schedule_to_close_timeout=walltime + timedelta(hours=24),
            heartbeat_timeout=timedelta(minutes=3),
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
        )


@workflow.defn(name="MiaBufferPoll")
class MiaBufferPoll:
    """Run on a schedule: turn new labels and dropped files into signals for the buffer workflows."""

    @workflow.run
    async def run(self) -> list[str]:
        return await workflow.execute_activity(
            "poll",
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
            result_type=list[str],
        )
