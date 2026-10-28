"""The Temporal workflows that take a buffer issue from `buffer:go` to a served buffer (spec §1).

`MiaBuffer` lives as long as the issue does. Each `go` starts a round: the spec in the issue body is planned
against the catalog, a human approves the plan, then every dataset of the pool is sourced in parallel (a stage
job, or a manual drop first) and registered with buffer-api as it becomes servable; the issue is `ready` once
every weighting group reaches the spec's `ready.min_fraction`, and the rest keep joining. A dataset with no
approved binding fails: `MiaPrep` follows a `prep:go` preparation request, which hands its records to
mia-data (`data:go` on their dataset issues) and polls the registry until each is prepared. The path from before
`e-prep`, where the buffer itself waited for mia-data, is gone (spec §12); its marker is kept deprecated so a
history that carries it still replays.

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
from temporalio.exceptions import ActivityError, CancelledError

with workflow.unsafe.imports_passed_through():
    from autodev.workflows import QUEUE_SLACK, RETRY_MAX_INTERVAL, error_message
    from mia_data.models import JobDone, LabelEvent, LockTaken

    from .models import (
        MAX_POLL_MINUTES,
        BindingInfo,
        BufferPins,
        BufferState,
        DatasetStatus,
        DegradedSignal,
        DropResult,
        FetchJobArgs,
        FetchResult,
        KeeperReport,
        PlanResult,
        PrepOutcome,
        PrepPins,
        PrepState,
        QuotaCheck,
        RawStatus,
        RegisterResult,
        SourcingLimits,
        StageJobArgs,
        StageResult,
        fetch_job_key,
        job_key,
    )
    from .status import render_ready

GO, APPROVE = "buffer:go", "buffer:approve"
BUSY = "busy, ignored: a round is already running. Add it again once the issue shows a final stage."
NOT_REVIEW = "`buffer:approve` counts only while the issue is in `buffer:plan-review`, for the plan under review."
MAX_ROUNDS_PER_EXECUTION = 20  # roll the history over between rounds at the latest after this many
# dataset statuses that wait on a human, on mia-data or on quota: dropped at the timeout, abandoned by a new go
WAITING = ("waiting-dataset", "waiting-drop", "blocked")
ABANDONED = "abandoned: a new buffer:go started another round"
RETIRED_WHILE = "abandoned: the buffer was retired"
NO_BUFFER = "this issue has no ready or idle buffer"
SOURCING = "{count} datasets still sourcing; add `{label}` again once they settle"
NOT_IDLE = "the buffer is ready, not idle"
AUTO_APPROVE = "plan.auto_approve: sourcing starts without review"
IDLE = (
    "No consumer for {hours:g} hours: idle. `buffer:keep` returns it to ready; `buffer:retire` or {days:g} days of "
    "idleness retires it."
)
RETIRED = "Retired: refs released; draws archived to {path}"
RAW_GONE = "pyramid missing and raw cache gone"
NOT_PREPARED = "not prepared: request preparation with a prep:go issue"
PREP_GO = "prep:go"
PREP_SETTLED = ("prepared", "needs-human")  # registry states a preparation request stops following (spec §12)
# nudge outcomes that settle a record as needs-human: done but unpublished needs a human run, a failed nudge a look
PREP_HUMAN = ("done-unpublished", "unknown")


class RoundFailed(Exception):
    """A step of the round that ends it in needs-human (raised by workflow code, never by an activity)."""


class LockChanged(Exception):
    """A re-take of a record's lock had to wait for another owner: what was decided before is stale (c-cache)."""

    def __init__(self, holder: str) -> None:
        super().__init__(holder)
        self.holder = holder


def _backoff(poll_minutes: int, tries: int) -> timedelta:
    """The sleep before the next try of a poll: `poll_minutes`, doubling with each try, at most MAX_POLL_MINUTES."""
    return timedelta(minutes=min(poll_minutes * 2**tries, MAX_POLL_MINUTES))


def _label(event: Any) -> LabelEvent | None:
    """A label event given to a two-parameter signal with one argument arrives as a dict (the SDK converts by
    type hint only when every parameter has an argument)."""
    return LabelEvent(**event) if isinstance(event, dict) else event


def _abandoned(dataset: DatasetStatus) -> bool:
    """A new go or a retire cancelled the dataset's task: its reason says so."""
    return dataset.reason.startswith("abandoned:")


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
        self.limits: SourcingLimits | None = None
        self.timed_out = False  # the round's day limit fired: a dataset that turns blocked now is dropped (c-cache)
        self.held: set[str] = set()  # owners of the raw-cache locks this run holds (c-cache)
        self.lifecycle: list[tuple[str, Any, str]] = []  # (idle|keep|retire|degraded, event or signal, buffer)
        self.lifecycle_on = False  # this run takes the d-lifecycle path, decided once at its start
        self.board: list[asyncio.Task] = []  # fire-and-forget status board writes, awaited before the round goes on
        self.progress = 0  # bumped by every dataset status change; the ticker backs off while it stays put
        self.tick = 0  # the shared ticker's count; lock and quota waiters wake on its next value (d-tick)
        self.wanted = 0  # the tick a waiter waits for; the ticker starts no timer while it has been reached
        self._ticker: asyncio.Task | None = None
        self.rebuild: list[tuple[asyncio.Task, DatasetStatus]] = []  # degraded datasets staging again (d-lifecycle)
        self.rebuild_slots: tuple[asyncio.Semaphore, asyncio.Semaphore] | None = None  # their stage and fetch slots
        self.degraded_records: set[str] = set()  # records of the degraded signals the running rebuild answers
        self.degraded_reason = ""

    @workflow.signal
    def go(self, event: LabelEvent) -> None:
        self.go_events.append(event)

    @workflow.signal
    def approve(self, event: LabelEvent) -> None:
        self.approve_events.append(event)

    @workflow.signal
    def dropped(self, record: str) -> None:
        self.drops.add(record)

    # lifecycle signals queue as (kind, label event or DegradedSignal, the buffer the keeper meant or "")

    @workflow.signal
    def degraded(self, signal: DegradedSignal) -> None:
        """The keeper found staged pyramids of these records of `signal.buffer` gone from disk."""
        self.lifecycle.append(("degraded", signal, signal.buffer))

    @workflow.signal
    def idle(self, event: LabelEvent | None = None, buffer: str = "") -> None:
        """The keeper saw no consumer of `buffer` for the spec's idle.hours."""
        self.lifecycle.append(("idle", _label(event), buffer))

    @workflow.signal
    def keep(self, event: LabelEvent) -> None:
        """`buffer:keep`: an idle buffer goes back to ready."""
        self.lifecycle.append(("keep", event, ""))

    @workflow.signal
    def retire(self, event: LabelEvent | None = None, buffer: str = "") -> None:
        """`buffer:retire` (an event), or the keeper (`buffer`) after retire_after_days of idleness."""
        self.lifecycle.append(("retire", _label(event), buffer))

    @workflow.query
    def status(self) -> BufferState:
        return self.state

    @workflow.run
    async def run(self, state: BufferState) -> None:
        rounds = 0
        self.lifecycle_on = workflow.patched("d-lifecycle")
        if state.stage == "plan-review":  # rolled over while waiting for an approve
            await self._guard(self._from_review())
        elif self.lifecycle_on and state.stage in ("ready", "idle") and state.buffer_id:  # rolled over serving
            await self._guard(self._steady())
        while True:
            await workflow.wait_condition(
                lambda: bool(self.go_events or self.approve_events or self._lifecycle()) or _rollover_due()
            )
            if not (self.go_events or self.approve_events or self._lifecycle()):
                workflow.continue_as_new(self.state)  # idle, nothing queued
            await self._refuse_queued(gos=False)  # idle: an approve has no plan under review to apply to
            await self._refuse_lifecycle()  # nor a lifecycle signal a buffer to act on
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
        await self._refuse_lifecycle()

    def _lifecycle(self) -> bool:
        """Lifecycle signals are queued and this run acts on them (an old run ignores them, as it always did)."""
        return self.lifecycle_on and bool(self.lifecycle)

    async def _refuse_lifecycle(self, reason: str = NO_BUFFER, count: int = 0) -> None:
        """d-lifecycle: refuse the queued lifecycle signals, for there is no ready or idle buffer to apply them to
        (`reason` may name `{label}` and `{count}`). A label is refused with a comment; the keeper's own signals
        are only logged, but for a retire of a superseded buffer."""
        if not self._lifecycle():
            return
        queued, self.lifecycle = self.lifecycle, []
        for kind, payload, buffer in queued:
            why = reason.format(label=f"buffer:{kind}", count=count)
            if isinstance(payload, LabelEvent):
                if not self._seen(payload):
                    await self._refuse(f"buffer:{kind}", why, payload)
            elif not await self._superseded(kind, buffer):
                workflow.logger.info("issue #%d: %s ignored: %s", self.state.issue, kind, why)

    async def _superseded(self, kind: str, buffer: str) -> bool:
        """A keeper signal about a buffer a go left serving (`state.superseded`): its idle writes the `idle` event on
        that buffer (so the keeper's retire rule runs from it), its retire releases it; anything else is logged.
        False: not a superseded buffer."""
        if not buffer or buffer not in self.state.superseded:
            return False
        if kind == "idle":  # best effort: the keeper says it again on its next pass until the event is there
            try:
                await self._act("buffer_event", buffer, "idle", "superseded", budget=timedelta(minutes=10))
            except ActivityError as error:
                workflow.logger.warning("issue #%d: idle of %s: %s", self.state.issue, buffer, error_message(error))
            return True
        if kind != "retire":
            workflow.logger.info("issue #%d: %s of superseded buffer %s ignored", self.state.issue, kind, buffer)
            return True
        try:
            await self._act("retire_buffer", buffer, buffer, seconds=900, result_type=dict)
        except ActivityError as error:  # the keeper signals it again on its next pass
            workflow.logger.warning(
                "issue #%d: retire of superseded buffer %s: %s", self.state.issue, buffer, error_message(error)
            )
            return True
        self.state.superseded.remove(buffer)
        try:
            await self._comment(f"superseded buffer `{buffer}` retired: refs released", f"retired {buffer}")
        except ActivityError as error:  # best effort: the retire is done
            workflow.logger.warning("issue #%d: %s", self.state.issue, error_message(error))
        return True

    async def _comment(self, body: str, marker: str) -> None:
        await self._act("comment", self.state.issue, body, marker)

    # --- the round -------------------------------------------------------------------------

    async def _round(self, event: LabelEvent) -> None:
        """Admit, plan, post the plan, wait for review; an approve continues into sourcing."""
        state = self.state
        failed = state.buffer_id if state.stage == "needs-human" and state.buffer_id and not state.retired else None
        state.round_id = f"i{state.issue}-e{event.event_id}"
        state.pins = state.buffer_id = state.planned_at = None
        state.datasets, state.ready = [], False
        state.idle_since, state.retired = None, False
        if failed and workflow.patched("retire-failed-round"):
            await self._retire_failed(failed)
        state.pins = await self._act("admit", state.issue, event.event_id, seconds=600, result_type=BufferPins)
        await self._stage("planning")
        plan = await self._act("plan", state.issue, state.round_id, state.pins, seconds=1800, result_type=PlanResult)
        await self._act("comment_plan", state.issue, state.round_id, seconds=300)
        if not plan.valid:
            return await self._finish("needs-human", "the plan is invalid: " + "; ".join(plan.errors))
        state.datasets = [
            DatasetStatus(
                dataset.record,
                dataset.group,
                dataset.manual,
                voxel_size_nm=dataset.voxel_size_nm,
                weight=dataset.weight,
            )
            for dataset in plan.pool
        ]
        state.min_fraction, state.timeout_days = plan.min_fraction, plan.timeout_days
        state.levels, state.patch = list(plan.levels), plan.patch
        state.idle_hours, state.retire_after_days = plan.idle_hours, plan.retire_after_days
        state.planned_at = workflow.now().isoformat()
        if plan.auto_approve and self.lifecycle_on:
            await self._comment(AUTO_APPROVE, f"auto-approve {state.round_id}")
            await self._refuse_queued()  # gos were busy, and an approve has no plan under review
            return await self._serve()
        await self._stage(
            "plan-review", "approve the plan with `buffer:approve`, or edit the spec and add `buffer:go` again"
        )
        await self._refuse_queued(approves=False)  # gos that came while planning; approves are judged below
        await self._from_review()

    async def _retire_failed(self, buffer: str) -> None:
        """A new go supersedes the buffer of a round that ended needs-human: nothing serves it, so it is retired now
        (refs released, `retired` event) and the page shows the new round as the issue's one live buffer. Best
        effort: a retire buffer-api refuses is logged, and the keeper's rules still reach the buffer later."""
        try:
            await self._act("retire_buffer", buffer, buffer, seconds=900, result_type=dict)
            await self._comment(f"failed round's buffer `{buffer}` retired: refs released", f"retired {buffer}")
        except ActivityError as error:
            workflow.logger.warning("issue #%d: retire of %s: %s", self.state.issue, buffer, error_message(error))

    async def _from_review(self) -> None:
        if await self._review():
            await self._serve()  # else a new go came during review: the next round plans again

    async def _serve(self) -> None:
        """Source the buffer; once it is ready and every dataset settled, serve it (d-lifecycle)."""
        if await self._sourcing() and self.lifecycle_on:
            await self._steady()

    async def _review(self) -> bool:
        """Wait for an approve newer than the plan (True), or a new go (False). Stale approves are refused."""
        planned_at = _parse_time(self.state.planned_at) or workflow.now()
        while True:
            await workflow.wait_condition(
                lambda: bool(self.approve_events or self.go_events or self._lifecycle()) or _rollover_due()
            )
            await self._refuse_lifecycle()  # refused now: a rollover would lose them
            if not (self.approve_events or self.go_events):
                if not _rollover_due():
                    continue
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

    async def _sourcing(self) -> bool:
        """Create the buffer, source every dataset in parallel, announce ready when the groups allow it. True when
        the buffer is ready and every dataset settled."""
        state = self.state
        assert state.round_id
        self.timed_out = False
        await self._stage("sourcing")
        state.buffer_id = state.round_id
        limits = await self._limits()
        await self._act("create_buffer", state.round_id, state.buffer_id, seconds=300)
        for dataset in state.datasets:  # every row is on the board from the start
            self._post(dataset)
        self._start_ticker(limits)
        try:
            return await self._source_all(limits)
        finally:
            await self._stop_ticker()
            await self._flush_board()

    async def _source_all(self, limits: SourcingLimits) -> bool:
        state = self.state
        semaphore = asyncio.Semaphore(max(1, limits.parallel))
        fetch_slots = asyncio.Semaphore(max(1, limits.parallel_fetch))
        # ponytail: no continue-as-new during sourcing (the tasks live in this run); fine below ~2,000 datasets per
        # buffer, beyond that persist per-dataset progress and resume from it
        datasets = state.datasets
        starts = list(range(len(datasets)))
        if workflow.patched("c-cache"):  # the heaviest first, so they take the first fetch and stage slots
            starts = sorted(starts, key=lambda index: (-datasets[index].weight, datasets[index].record))
        created = {
            index: asyncio.create_task(self._source(datasets[index], semaphore, fetch_slots)) for index in starts
        }
        tasks = [created[index] for index in range(len(datasets))]  # in state.datasets order, as the zips below need
        deadline = workflow.now() + timedelta(days=state.timeout_days)
        try:
            while True:
                await workflow.wait_condition(
                    lambda: self._ready_now() or all(task.done() for task in tasks) or self._lifecycle(),
                    timeout=deadline - workflow.now(),
                )
                if self._ready_now() or all(task.done() for task in tasks):
                    break
                # d-lifecycle: a keep or retire label while sourcing is refused now, not once the buffer is ready
                await self._refuse_lifecycle(SOURCING, sum(not task.done() for task in tasks))
        except TimeoutError:
            self.timed_out = True
            if not self._ready_now():
                below = self._below()  # before the drops: the message names what the human waited for
                await self._drop_waiting(tasks, f"still {{status}} after {state.timeout_days:g} days")
                # the waiters are gone; what is left is a stage job or a registration, bounded by the walltime
                await workflow.wait_condition(lambda: self._ready_now() or all(task.done() for task in tasks))
                if not self._ready_now():
                    await self._finish(
                        "needs-human",
                        f"after {state.timeout_days:g} days these weighting groups are below ready.min_fraction "
                        f"{state.min_fraction:g}: {', '.join(below)}",
                    )
                    return False
        if self._ready_now():
            await self._refuse_lifecycle()  # they came while there was no ready buffer
            await self._announce_ready(limits)
            if not await self._await_stragglers(tasks):
                return False  # a new go: the main loop plans again
        await asyncio.gather(*tasks, return_exceptions=True)  # every task handles its own failure (_source)
        if not state.ready:
            reasons = "; ".join(f"{d.record}: {d.reason or d.status}" for d in state.datasets if d.status != "servable")
            groups = ", ".join(self._below())
            await self._finish(
                "needs-human", f"weighting groups below ready.min_fraction {state.min_fraction:g}: {groups}; {reasons}"
            )
            return False
        await self._refuse_queued()  # labels that arrived while sourcing ran were busy, as in mia-data
        return True

    # --- serving: the ready-state loop (d-lifecycle) ---------------------------------------------

    async def _steady(self) -> None:
        """The buffer is ready or idle: act on lifecycle signals until a go (the main loop plans again; this buffer
        stays registered), a retire, or a rebuild that leaves a group below the floor (needs-human). A rebuild runs
        alongside: a go or a retire abandons it. An approve is refused; a quiet history rolls over (never while a
        rebuild runs) and the next run comes back here."""
        while True:
            await workflow.wait_condition(
                lambda: (
                    bool(self.lifecycle or self.go_events or self.approve_events)
                    or self._rebuilt()
                    or (_rollover_due() and not self.rebuild)
                )
            )
            if self._rebuilt():
                if not await self._end_rebuild():
                    return
                continue
            if not (self.lifecycle or self.go_events or self.approve_events):
                workflow.continue_as_new(self.state)  # stage ready or idle: run() comes back here
            await self._refuse_queued(gos=False)
            self.go_events = [go for go in self.go_events if go.event_id not in self.state.handled]
            if self.go_events:
                await self._cancel_rebuild(ABANDONED)
                if self.state.buffer_id and not self.state.retired:  # it stays registered: its retire comes later
                    self.state.superseded = (self.state.superseded + [self.state.buffer_id])[-20:]
                return
            while self.lifecycle:
                kind, payload, buffer = self.lifecycle.pop(0)
                if isinstance(payload, LabelEvent) and self._seen(payload):
                    continue
                if buffer and buffer != self.state.buffer_id:  # about a buffer an earlier round left registered
                    if not await self._superseded(kind, buffer):
                        workflow.logger.info("issue #%d: %s of buffer %s ignored", self.state.issue, kind, buffer)
                    continue
                if not await self._on_lifecycle(kind, payload):
                    return

    async def _on_lifecycle(self, kind: str, payload: Any) -> bool:
        """One lifecycle signal while ready or idle; False when the round is over (retired, needs-human)."""
        state = self.state
        assert state.round_id and state.buffer_id
        if kind == "idle" and state.stage == "ready":
            state.idle_since = workflow.now().isoformat()
            await self._stage("idle", IDLE.format(hours=state.idle_hours, days=state.retire_after_days))
        elif kind == "keep":
            if state.stage != "idle":
                await self._refuse("buffer:keep", NOT_IDLE, payload)
                return True
            state.idle_since = None
            await self._stage("ready")
            await self._comment("kept", f"keep {state.round_id} {payload.event_id}")
        elif kind == "retire":
            if payload is None and state.stage != "idle":  # the keeper's, sent before a keep made it ready again
                workflow.logger.info("issue #%d: the keeper's retire ignored: the buffer is ready", state.issue)
                return True
            await self._cancel_rebuild(RETIRED_WHILE)
            retired = await self._act("retire_buffer", state.round_id, state.buffer_id, seconds=900, result_type=dict)
            state.retired, state.idle_since = True, None
            await self._stage("retired", RETIRED.format(path=retired.get("archive") or "none"))
            return False
        elif kind == "degraded":
            return await self._degraded(payload)
        return True  # an idle while idle: the keeper says it again each pass

    async def _degraded(self, signal: DegradedSignal) -> bool:
        """Start staging again, from the raw cache, the named servable datasets whose volumes buffer-api finds
        missing now (a dataset whose raw cache is gone fails). The keeper re-sends while a pyramid stays missing:
        a record rebuilt since, being rebuilt or failed is left alone. False: the round is over."""
        state = self.state
        assert state.round_id and state.buffer_id
        missing = set(await self._act("missing_records", state.buffer_id, seconds=300, result_type=list[str]))
        named = set(signal.records) & missing
        hit = [dataset for dataset in state.datasets if dataset.record in named and dataset.status == "servable"]
        if not hit:
            return True
        limits = await self._limits()
        self.degraded_reason = signal.reason
        for dataset in hit:
            self.degraded_records.add(dataset.record)
            if not await self._act("raw_ready", state.round_id, dataset.record, seconds=300, result_type=bool):
                await self._fail(dataset, RAW_GONE)
                continue
            dataset.rebuilds += 1
            self._set(dataset, "pending")
            if self.rebuild_slots is None:  # the first of this rebuild: its slots and its ticker
                self.timed_out = False  # the round's day limit is long past; a quota block waits, not drops
                self.rebuild_slots = (
                    asyncio.Semaphore(max(1, limits.parallel)),
                    asyncio.Semaphore(max(1, limits.parallel_fetch)),
                )
                self._start_ticker(limits)
            self.rebuild.append((asyncio.create_task(self._source(dataset, *self.rebuild_slots)), dataset))
        return True if self.rebuild else await self._end_rebuild()

    def _rebuilt(self) -> bool:
        return bool(self.rebuild) and all(task.done() for task, _ in self.rebuild)

    async def _close_rebuild(self) -> None:
        """The rebuild's tasks (done or cancelled), ticker and board writes, all ended."""
        tasks = [task for task, _ in self.rebuild]
        self.rebuild, self.rebuild_slots = [], None
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._stop_ticker()
        await self._flush_board()

    async def _cancel_rebuild(self, reason: str) -> None:
        """A go or a retire: abandon the datasets still staging again."""
        for task, dataset in self.rebuild:
            if not task.done():
                task.cancel()
                self._set(dataset, "failed", reason)
        await self._close_rebuild()
        self.degraded_records = set()

    async def _end_rebuild(self) -> bool:
        """Every degraded dataset is servable again or failed: stay while every group is at its floor (say which
        were rebuilt), else needs-human naming the records. False: the round is over."""
        state = self.state
        assert state.round_id
        await self._close_rebuild()
        degraded, self.degraded_records = self.degraded_records, set()
        datasets = [dataset for dataset in state.datasets if dataset.record in degraded]
        if not self._ready_now():
            gone = [d for d in state.datasets if d.status != "servable" and (d in datasets or d.reason == RAW_GONE)]
            records = ", ".join(sorted(d.record for d in gone))
            await self._finish(
                "needs-human",
                f"pyramids missing ({self.degraded_reason}): {records}; these weighting groups are now below "
                f"ready.min_fraction {state.min_fraction:g}: {', '.join(self._below())}",
            )
            return False
        if rebuilt := sorted(d.record for d in datasets if d.status == "servable"):
            await self._comment(
                f"rebuilt {', '.join(rebuilt)}", f"rebuilt {state.round_id} {' '.join(rebuilt)} {self.progress}"
            )
        return True

    # --- the status board and the shared ticker ------------------------------------------------

    def _set(self, dataset: DatasetStatus, status: str, reason: str = "") -> None:
        """Set a dataset's status; a change bumps `progress` and goes to the status board (d-lifecycle)."""
        changed = dataset.status != status
        dataset.status, dataset.reason = status, reason
        if changed:
            self.progress += 1
            self._post(dataset)

    def _post(self, dataset: DatasetStatus) -> None:
        """Write the dataset's row on the buffer's status board without waiting: the board is best effort."""
        if self.lifecycle_on and self.state.buffer_id:
            args = (self.state.buffer_id, dataset.record, dataset.status, dataset.reason)
            self.board.append(asyncio.create_task(self._mark(*args)))

    async def _mark(self, buffer_id: str, record: str, status: str, reason: str) -> None:
        try:
            await self._act("mark_dataset", buffer_id, record, status, reason, budget=timedelta(minutes=10))
        except ActivityError as error:
            workflow.logger.warning(
                "issue #%d: board %s %s: %s", self.state.issue, record, status, error_message(error)
            )

    async def _flush_board(self) -> None:
        """Wait for the board writes in flight, so none outlives the round."""
        board, self.board = self.board, []
        await asyncio.gather(*board, return_exceptions=True)

    def _start_ticker(self, limits: SourcingLimits) -> None:
        """d-tick: one ticker for the sourcing pass, which every lock and quota waiter shares."""
        if workflow.patched("d-tick"):
            self._ticker = asyncio.create_task(self._tick_forever(limits))

    async def _stop_ticker(self) -> None:
        ticker, self._ticker = self._ticker, None
        if ticker is not None:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)

    async def _tick_forever(self, limits: SourcingLimits) -> None:
        """Tick while someone waits: poll_minutes, doubling while no dataset changed status, up to
        max_tick_minutes; a change resets it. One timer per tick for the whole round, not one per waiter."""
        quiet, seen = 0, self.progress
        while True:
            await workflow.wait_condition(lambda: self.wanted > self.tick)
            if self.progress != seen:  # a change while nobody waited
                quiet, seen = 0, self.progress
            await workflow.sleep(timedelta(minutes=min(limits.poll_minutes * 2**quiet, limits.max_tick_minutes)))
            self.tick += 1
            if self.progress != seen:
                quiet = 0
            elif limits.poll_minutes * 2**quiet < limits.max_tick_minutes:  # no further once at the cap
                quiet += 1
            seen = self.progress

    async def _next_tick(self) -> None:
        seen = self.tick
        self.wanted = seen + 1
        await workflow.wait_condition(lambda: self.tick > seen)

    async def _poll_wait(self, limits: SourcingLimits, tries: int) -> None:
        """A lock or quota waiter's wait for its next try: the round's ticker (d-tick), else its own backoff."""
        if self._ticker is not None:
            await self._next_tick()
        else:
            await workflow.sleep(_backoff(limits.poll_minutes, tries))

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
                    or self._lifecycle()
                )
            )
            await self._refuse_queued(gos=False)
            # the buffer is ready, but its datasets are still sourcing
            await self._refuse_lifecycle(SOURCING, sum(not task.done() for task in tasks))
            self.go_events = [go for go in self.go_events if go.event_id not in self.state.handled]
            if self.go_events and abandonable():
                for task, dataset in pairs:
                    if not task.done():
                        task.cancel()
                        self._set(dataset, "failed", ABANDONED)
                return False
        return True

    async def _announce_ready(self, limits: SourcingLimits) -> None:
        state = self.state
        state.ready = True
        servable = [dataset.record for dataset in state.datasets if dataset.status == "servable"]
        await self._stage("ready", extra=render_ready(state.buffer_id or "", limits.service_url, servable))

    async def _source(
        self, dataset: DatasetStatus, semaphore: asyncio.Semaphore, fetch_slots: asyncio.Semaphore
    ) -> None:
        """Binding → (drop) → stage job → register; the dataset's status records the outcome. An activity that
        gives up fails this dataset alone, never the round."""
        try:
            await self._source_steps(dataset, semaphore, fetch_slots)
        except ActivityError as error:
            if dataset.status == "servable":  # only the "servable" comment failed; the volumes are registered
                workflow.logger.warning("issue #%d: %s: %s", self.state.issue, dataset.record, error_message(error))
                return
            if (dataset.status == "dropped" or _abandoned(dataset)) and workflow.patched("c-cache"):
                return  # a cancel that landed inside an activity: the drop or abandon already says why
            await self._fail(dataset, error_message(error))

    async def _source_steps(
        self, dataset: DatasetStatus, semaphore: asyncio.Semaphore, fetch_slots: asyncio.Semaphore
    ) -> None:
        if workflow.patched("c-cache"):
            return await self._source_cached(dataset, semaphore, fetch_slots)
        state = self.state
        assert state.round_id and state.pins and state.buffer_id
        round_id, record = state.round_id, dataset.record
        binding = await self._act("read_binding", round_id, record, seconds=300, result_type=BindingInfo)
        if not binding.found:
            workflow.deprecate_patch("e-prep")  # a `prep:go` issue prepares it; the buffer never waits on mia-data
            return await self._fail(dataset, NOT_PREPARED)
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

    async def _source_cached(
        self, dataset: DatasetStatus, semaphore: asyncio.Semaphore, fetch_slots: asyncio.Semaphore
    ) -> None:
        """`c-cache`: binding → (drop) → the record's raw-cache lock → fetch phase → stage → register → release.
        The lock is held from before the fetch until the volumes are registered, so a second buffer of the same
        record waits and then reuses the pyramid the first one staged. The stage is offline (no hosts, read-only
        cache) unless some url streams."""
        state = self.state
        assert state.round_id and state.pins and state.buffer_id
        round_id, record = state.round_id, dataset.record
        binding = await self._act("read_binding", round_id, record, seconds=300, result_type=BindingInfo)
        if not binding.found:
            workflow.deprecate_patch("e-prep")  # a `prep:go` issue prepares it; the buffer never waits on mia-data
            return await self._fail(dataset, NOT_PREPARED)
        if binding.manual and not dataset.rebuilds:  # a rebuild found the dropped files in the raw cache
            await self._await_drop(dataset, mark=True)
            self._set(dataset, "pending")  # no longer waiting on a human: not dropped at the timeout, nor abandoned
        if binding.staged and not dataset.rebuilds:  # the registry holds this binding's pyramid: reuse it, no lock
            return await self._reuse(dataset, semaphore, " The pyramid was staged earlier for this binding and reused.")
        owner = f"mia-buffers {round_id} {record}"
        lock = ["raw", record, owner]
        # ponytail: between a take and the next await_job poll the lock is not heartbeated (prepare + submit retries
        # could outlast lock_ttl_s); it is re-taken after each slot wait and after a quota block. A heartbeat in
        # submit is the upgrade.
        holder = await self._lock(dataset, owner)
        try:
            while True:
                try:
                    return await self._source_locked(dataset, semaphore, fetch_slots, owner, holder)
                except LockChanged as changed:  # another owner had the cache meanwhile: look at it again
                    holder = changed.holder
        finally:
            await self._release(lock)

    async def _source_locked(
        self,
        dataset: DatasetStatus,
        semaphore: asyncio.Semaphore,
        fetch_slots: asyncio.Semaphore,
        owner: str,
        holder: str,
    ) -> None:
        """Under the record's lock: re-read the binding (`holder` may have staged it meanwhile) and the raw cache,
        fetch what is missing, stage, register. LockChanged from a re-take means: decide again."""
        state = self.state
        assert state.round_id and state.buffer_id
        round_id, record = state.round_id, dataset.record
        binding = await self._act("read_binding", round_id, record, seconds=300, result_type=BindingInfo)
        if binding.staged and not dataset.rebuilds:  # the lock's previous holder staged it while we waited
            built = f" The pyramid was staged by {holder} while this buffer waited and reused." if holder else ""
            return await self._reuse(
                dataset, semaphore, built or " The pyramid was staged earlier for this binding and reused."
            )
        if dataset.rebuilds:  # d-lifecycle: another buffer of this artifact may have rebuilt it while we waited
            missing = await self._act("missing_records", state.buffer_id, seconds=300, result_type=list[str])
            if record not in missing:
                again = f" The pyramid was rebuilt by {holder} while this buffer waited and reused." if holder else ""
                return await self._reuse(dataset, semaphore, again or " The pyramid is back on disk and reused.")
        raw = await self._act("raw_status", round_id, record, seconds=300, result_type=RawStatus)
        stream = list(raw.stream)
        if raw.missing and not binding.manual:
            fetched = await self._fetch(dataset, binding, owner, fetch_slots)
            if fetched is None:
                return  # failed
            stream += fetched
        async with semaphore:
            await self._retake(dataset, owner)  # refreshed after the wait for a stage slot
            self._set(dataset, "staging")
            rebuild = [dataset.rebuilds] if dataset.rebuilds else []  # a rebuild's own workspace, job and build dir
            workspace = await self._act(
                "prepare_stage", state.issue, round_id, record, *rebuild, seconds=300, result_type=str
            )
            hosts = binding.hosts if stream else []
            job = await self._job(
                record,
                workspace,
                hosts,
                dataset.voxel_size_nm,
                cache_writable=bool(stream),
                lock=["raw", record, owner],
                rebuild=dataset.rebuilds,
            )
            if job.state not in ("DONE", "EXIT"):
                return await self._fail(dataset, f"stage job {job.job_id} ended in state {job.state}")
            exited = job.state == "EXIT" and job.exit_code not in (0, None)
            why = f"stage job {job.job_id} exited with code {job.exit_code}"
            if exited and not workflow.patched("stage-errors"):
                return await self._fail(dataset, why)
            staged = await self._act("read_stage", round_id, record, *rebuild, seconds=300, result_type=StageResult)
            if exited:  # stage-errors: the harness exits 1 with its reasons in stage.json; the comment says them
                return await self._fail(dataset, f"{why}: {'; '.join(staged.errors) or 'no errors in stage.json'}")
        await self._register(dataset, staged, "")

    async def _reuse(self, dataset: DatasetStatus, semaphore: asyncio.Semaphore, reused: str) -> None:
        """A pyramid staged from this very binding is on disk: re-read its report and register it, no job."""
        assert self.state.round_id
        async with semaphore:
            self._set(dataset, "staging")
            staged = await self._act(
                "read_stage",
                self.state.round_id,
                dataset.record,
                *([dataset.rebuilds] if dataset.rebuilds else []),
                seconds=300,
                result_type=StageResult,
            )
        await self._register(dataset, staged, reused)

    async def _register(self, dataset: DatasetStatus, staged: StageResult, reused: str) -> None:
        """Register a staged pyramid's volumes with the buffer and say so; a failed stage or a refusal fails it."""
        state = self.state
        assert state.round_id and state.buffer_id
        round_id, record = state.round_id, dataset.record
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
        dataset.volumes = len(registered.registered)
        self._set(dataset, "servable")
        await self._comment(
            f"`{record}` is servable: {dataset.volumes} volume(s) registered with buffer `{state.buffer_id}`.{reused}",
            f"dataset {round_id} {record} servable",
        )

    async def _lock(self, dataset: DatasetStatus, owner: str) -> str:
        """Take the record's raw-cache lock, waiting while another owner holds it; a re-take by the same owner
        succeeds at once and refreshes it. Returns the last other holder seen ("" when it was free)."""
        limits = await self._limits()
        holder, tries = "", 0
        while not (
            taken := await self._act(
                "take_lock", "raw", dataset.record, owner, limits.lock_ttl_s, result_type=LockTaken
            )
        ).ok:
            holder = taken.holder
            self._set(dataset, "waiting-lock", f"raw cache held by {holder}")
            # ponytail: ~10 history events per try, backing off to one try per 30 min (~20 events an hour per
            # waiter); 100 datasets waiting a day fill the 51,200-event history. Upgrade: a wake-up signal from the
            # releasing worker, or continue-as-new during sourcing.
            await self._poll_wait(limits, tries)
            tries += 1
        self.held.add(owner)
        if holder:  # our own lock is not "held by" anyone on the page
            self._set(dataset, "pending")
        return holder

    async def _retake(self, dataset: DatasetStatus, owner: str) -> None:
        """Take the record's lock again after a wait (a slot, a quota block). LockChanged when another owner held
        it meanwhile: it may have staged the binding or the keeper evicted the cache, so the caller decides again."""
        if holder := await self._lock(dataset, owner):
            raise LockChanged(holder)

    async def _release(self, lock: list[str]) -> None:
        """Release a lock _lock took, if this run still holds it. A failed release is logged (a lock left behind
        lapses after its TTL); a cancel that lands in it is re-raised after one more try, so a dropped or abandoned
        dataset stops instead of polling on."""
        if lock[2] not in self.held:
            return
        self.held.discard(lock[2])
        try:
            await self._act("release_lock", *lock, budget=timedelta(minutes=30))
        except ActivityError as error:
            if isinstance(error.cause, CancelledError):
                try:  # the cancel is delivered: this try runs, and the next round need not wait out the TTL
                    await self._act("release_lock", *lock, budget=timedelta(minutes=30))
                except ActivityError:
                    pass
                raise
            workflow.logger.warning("issue #%d: could not release %s: %s", self.state.issue, lock, error_message(error))

    async def _quota(self, dataset: DatasetStatus, owner: str) -> bool:
        """Wait until the raw cache has room for the record (`blocked` meanwhile, said once on the issue), with the
        record's lock released while blocked (spec §5.2) and taken again after. False: the dataset was dropped
        (the round's day limit had fired, so the block dropped it) or abandoned while blocked."""
        state = self.state
        assert state.round_id
        limits = await self._limits()
        said, tries = False, 0
        while not (quota := await self._act("check_quota", state.round_id, dataset.record, result_type=QuotaCheck)).ok:
            if self.timed_out:  # _drop_waiting has run: nothing would drop this one later
                await self._drop(dataset, f"still blocked after {state.timeout_days:g} days")
                return False
            self._set(dataset, "blocked", f"raw quota over by {quota.over_by} bytes")
            await self._release(["raw", dataset.record, owner])
            if dataset.status == "dropped" or _abandoned(dataset):  # cancelled meanwhile
                return False
            if not said:
                await self._comment(
                    f"`{dataset.record}` is blocked: {dataset.reason}. Its fetch waits until the keeper frees space "
                    "or the quota is raised.",
                    f"dataset {state.round_id} {dataset.record} blocked",
                )
                said = True
            # ponytail: as _lock, backing off to one try per 30 min; a wake-up from the keeper is the upgrade
            await self._poll_wait(limits, tries)
            tries += 1
        if owner not in self.held:
            self._set(dataset, "pending")
            await self._retake(dataset, owner)
        return True

    async def _fetch(
        self, dataset: DatasetStatus, binding: BindingInfo, owner: str, fetch_slots: asyncio.Semaphore
    ) -> list[str] | None:
        """The fetch phase under the record's lock: wait for quota (holding no fetch slot), then take a fetch slot;
        after a wait for it the lock is refreshed and the quota checked again (other fetches ran meanwhile; over
        again: back to waiting without the slot). Returns the urls the stage must stream, or None (failed or
        dropped)."""
        state = self.state
        assert state.round_id
        while True:
            if not await self._quota(dataset, owner):
                return None  # dropped
            waited = fetch_slots.locked()
            async with fetch_slots:
                if waited:
                    await self._retake(dataset, owner)  # refreshed after the wait for a fetch slot
                    if not (await self._act("check_quota", state.round_id, dataset.record, result_type=QuotaCheck)).ok:
                        continue
                self._set(dataset, "fetching")
                return await self._fetch_jobs(dataset, binding, owner)

    async def _fetch_jobs(self, dataset: DatasetStatus, binding: BindingInfo, owner: str) -> list[str] | None:
        """Up to fetch_attempts fetch jobs, each resuming from the cache; the stream urls, or None (failed)."""
        state = self.state
        assert state.round_id and state.pins
        round_id, record = state.round_id, dataset.record
        limits = await self._limits()
        last = ""
        first = dataset.rebuilds * limits.fetch_attempts  # a rebuild's fetch jobs get keys of their own
        for attempt in range(first + 1, first + limits.fetch_attempts + 1):
            workspace = await self._act(
                "prepare_fetch", state.issue, round_id, record, attempt, seconds=300, result_type=str
            )
            args = FetchJobArgs(workspace, round_id, state.pins, record, list(binding.hosts), attempt)
            key = fetch_job_key(state.issue, round_id, record, attempt)
            job_id = await self._act("submit_fetch", key, args, seconds=600, result_type=str)
            job = await self._await_job(job_id, workspace, limits.fetch_walltime_min, ["raw", record, owner])
            last = f"last job {job.job_id} {job.state} exit {job.exit_code}"
            fetched = await self._act("read_fetch", round_id, record, workspace, seconds=300, result_type=FetchResult)
            if fetched.host_not_allowed or not fetched.ok:
                why = "; ".join(fetched.errors + [f"{url} failed" for url in fetched.failed]) or "no errors reported"
                if fetched.host_not_allowed:
                    why += f" (host not allowed: {fetched.host_not_allowed})"
                await self._fail(dataset, f"fetch failed: {why}")
                return None
            if not fetched.partial:
                return list(fetched.stream)
        await self._fail(dataset, f"fetch still partial after {limits.fetch_attempts} attempts ({last})")
        return None

    async def _drop(self, dataset: DatasetStatus, reason: str) -> None:
        """Take the dataset out of the plan and the ready denominator; say so and re-post the plan."""
        state = self.state
        assert state.round_id
        self._set(dataset, "dropped", reason)
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
                self._set(dataset, "dropped", reason.format(status=dataset.status))
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

    async def _await_drop(self, dataset: DatasetStatus, mark: bool = False) -> None:
        """Ask for the files and wait for the poller's `dropped` signal until the drop verifies. `mark` (c-cache):
        the raw asset row says `waiting-drop` first."""
        state = self.state
        assert state.round_id
        self._set(dataset, "waiting-drop")
        if mark:
            await self._act("mark_asset", "raw", dataset.record, {"state": "waiting-drop"})
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
        self._set(dataset, "failed", reason)
        try:
            await self._comment(
                f"`{dataset.record}` failed: {reason}", f"dataset {state.round_id} {dataset.record} failed"
            )
        except ActivityError as error:  # best effort: the status carries the reason either way
            workflow.logger.warning(
                "issue #%d: could not post %s's failure: %s", state.issue, dataset.record, error_message(error)
            )

    async def _job(
        self,
        record: str,
        workspace: str,
        hosts: list[str],
        voxel_size_nm: dict[str, float] | None,
        cache_writable: bool = True,
        lock: list[str] | None = None,
        rebuild: int = 0,
    ) -> JobDone:
        """Submit the stage job (at most once per key) and wait for it on the await queue; the wait keeps `lock`
        ([kind, key, owner]) alive."""
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
            cache_writable=cache_writable,
            rebuild=rebuild,
        )
        job_id = await self._act(
            "submit", job_key(state.issue, state.round_id, record, rebuild), args, seconds=600, result_type=str
        )
        return await self._await_job(job_id, workspace, (await self._limits()).walltime_min, lock)

    async def _await_job(
        self, job_id: str, workspace: str, walltime_min: int, lock: list[str] | None = None
    ) -> JobDone:
        """Wait for a job on the await queue (a worker restart reattaches to it), heartbeating `lock` if given."""
        walltime = timedelta(minutes=walltime_min)
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


@workflow.defn(name="MiaPrep")
class MiaPrep:
    """One preparation request issue: each `prep:go` hands the records it names to mia-data and follows them in
    the registry to `prep:done` (each prepared) or `prep:needs-human`. One round at a time."""

    @workflow.init
    def __init__(self, state: PrepState) -> None:
        self.state = state
        self.go_events: list[LabelEvent] = []

    @workflow.signal
    def go(self, event: LabelEvent) -> None:
        self.go_events.append(event)

    @workflow.query
    def status(self) -> PrepState:
        return self.state

    @workflow.run
    async def run(self, state: PrepState) -> None:
        rounds = 0
        while True:
            await workflow.wait_condition(lambda: bool(self.go_events) or _rollover_due())
            if not self.go_events:
                workflow.continue_as_new(self.state)  # idle, nothing queued
            event = self.go_events.pop(0)
            if self._seen(event):  # redelivery
                continue
            rounds += 1
            try:
                await self._round(event)
            except ActivityError as error:  # an activity gave up (a bad request, the registry down too long)
                if isinstance(error.cause, CancelledError):
                    raise
                await self._stage("needs-human", error_message(error))
            await self._refuse_queued()
            if (rounds >= MAX_ROUNDS_PER_EXECUTION or _rollover_due()) and not self.go_events:
                workflow.continue_as_new(self.state)

    async def _act(self, name: str, *args: Any, seconds: int = 300, result_type: Any = None) -> Any:
        """Run an activity with a per-attempt timeout and a six-hour retry budget."""
        return await workflow.execute_activity(
            name,
            args=list(args),
            start_to_close_timeout=timedelta(seconds=seconds),
            schedule_to_close_timeout=timedelta(hours=6),
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL),
            result_type=result_type,
        )

    def _seen(self, event: LabelEvent) -> bool:
        if event.event_id in self.state.handled:
            return True
        self.state.handled = (self.state.handled + [event.event_id])[-50:]
        return False

    async def _stage(self, stage: str, detail: str = "") -> None:
        """Swap the `prep:<stage>` label; the round's last one is best effort (the state says it either way)."""
        self.state.stage = stage
        try:
            await self._act("set_prep_stage", self.state.issue, stage, self.state, detail)
        except ActivityError as error:
            if stage == "requested":
                raise
            workflow.logger.error("issue #%d: could not set %s: %s", self.state.issue, stage, error_message(error))

    async def _refuse_queued(self) -> None:
        """A go while a round runs is busy; one this run handled already is a redelivery."""
        queued, self.go_events = self.go_events, []
        for event in queued:
            if self._seen(event):
                continue
            try:
                await self._act("refuse_label", self.state.issue, PREP_GO, BUSY, event.event_id)
            except ActivityError as error:  # best effort: the event is handled either way
                workflow.logger.warning(
                    "issue #%d: could not refuse %s: %s", self.state.issue, event.event_id, error_message(error)
                )

    async def _round(self, event: LabelEvent) -> None:
        """Admit, nudge each record, then poll the registry until every record settles or the request times out."""
        state = self.state
        state.round_id = f"p{state.issue}-e{event.event_id}"
        state.pins, state.outcomes, state.settled = None, [], {}
        started = workflow.now()
        # a pre-H registry had a third settled state; a round recorded then (and the fixture) still treats it as one
        legacy = () if workflow.patched("h-no-ineligible") else ("ineligible",)
        pins = state.pins = await self._act(
            "prep_admit", state.issue, event.event_id, seconds=600, result_type=PrepPins
        )
        await self._stage("requested")
        why: dict[str, str] = {}  # records settled as needs-human at the nudge -> what the nudge said
        for record in pins.records:  # one at a time: each may open an issue in the datasets repo
            try:
                outcome = await self._act("prep_nudge", state.round_id, record, result_type=PrepOutcome)
            except ActivityError as error:  # this record alone: an unclear catalog record, a refused issue
                if isinstance(error.cause, CancelledError):  # the request was cancelled: stop opening issues
                    raise
                outcome = PrepOutcome(record, "unknown", note=error_message(error))
            state.outcomes.append(outcome)
            if outcome.outcome in (*PREP_SETTLED, *legacy):
                state.settled[record] = outcome.outcome
            elif outcome.outcome in PREP_HUMAN:  # mia-data will not get to it without a human
                state.settled[record] = "needs-human"
                why[record] = outcome.outcome + (f": {outcome.note}" if outcome.note else "")
        try:  # best effort: the outcomes stay in the state, and the final stage names what needs a human
            await self._act("comment_prep", state.issue, state.round_id, state.outcomes)
        except ActivityError as error:
            workflow.logger.error("issue #%d: could not post the outcomes: %s", state.issue, error_message(error))
        poll_minutes = (await self._act("limits", result_type=SourcingLimits)).poll_minutes
        deadline, tries = started + timedelta(days=pins.timeout_days), 0
        last: dict[str, str] = {}  # the latest registry state of each unsettled record
        while (unsettled := [record for record in pins.records if record not in state.settled]) and (
            workflow.now() < deadline
        ):
            await workflow.sleep(min(_backoff(poll_minutes, tries), deadline - workflow.now()))
            tries += 1
            await self._refuse_queued()  # ponytail: a busy go waits for the next poll (MAX_POLL_MINUTES at most)
            last = await self._act("prep_status", unsettled, result_type=dict[str, str])
            state.settled |= {record: value for record, value in last.items() if value in (*PREP_SETTLED, *legacy)}
        prepared = [record for record in pins.records if state.settled.get(record) == "prepared"]
        stuck = [record for record in pins.records if state.settled.get(record) not in ("prepared", *legacy)]
        if not stuck:
            return await self._stage("done", "prepared: " + (", ".join(f"`{r}`" for r in prepared) or "none"))
        humans = [
            f"`{record}`" + (f" ({why[record]})" if record in why else "")
            for record in stuck
            if state.settled.get(record) == "needs-human"
        ]
        waiting = [f"`{record}` ({last.get(record, 'unprepared')})" for record in stuck if record not in state.settled]
        detail = "; ".join(
            ([f"needs a human on its dataset issue: {', '.join(humans)}"] if humans else [])
            + ([f"not prepared after {pins.timeout_days:g} days: {', '.join(waiting)}"] if waiting else [])
        )
        await self._stage("needs-human", detail)


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


@workflow.defn(name="MiaCacheKeeper")
class MiaCacheKeeper:
    """Run on a schedule: the `keep` activity's housekeeping of the raw and staged roots."""

    @workflow.run
    async def run(self) -> KeeperReport:
        return await workflow.execute_activity(
            "keep",
            start_to_close_timeout=timedelta(minutes=25),
            heartbeat_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1),  # the next scheduled pass is the retry
            result_type=KeeperReport,
        )
