"""MiaBuffer on Temporal's time-skipping server with every activity mocked, the poller, and a replay test."""

import asyncio
import contextlib
import json
import os
import uuid
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_buffers_config import make_cfg

from mia_buffers import workflows
from mia_buffers.activities import Activities
from mia_buffers.models import (
    BindingInfo,
    BufferPins,
    BufferState,
    DatasetIssue,
    DatasetPlan,
    DegradedSignal,
    DropResult,
    FetchJobArgs,
    FetchResult,
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
from mia_buffers.poll import run_poll
from mia_buffers.workflows import MiaBuffer
from mia_data.models import JobDone, LabelEvent, LockTaken
from mia_data.publish import reindex

PINS = BufferPins("s" * 64, "c" * 40, "h" * 40, "d" * 64)
PLAN = PlanResult(
    True,
    [],
    [
        DatasetPlan("u937", "confocal", 0.6, False, {"z": 1.0, "y": 45.0718, "x": 45.0718}),
        DatasetPlan("bcss", "histology-WSI", 0.4, True),
    ],
    10,
    levels=[1, 2, 4],
    patch=256,
)
ONE_GROUP = PlanResult(
    True,
    [],
    [DatasetPlan("a", "TEM", 0.5, False), DatasetPlan("b", "TEM", 0.5, True)],
    10,
    min_fraction=0.5,
    levels=[1, 2, 4],
    patch=256,
)


@pytest.fixture(scope="session")
async def env():
    e = await WorkflowEnvironment.start_time_skipping()
    yield e
    await e.shutdown()


def ev(kind, eid, at="2026-01-01T00:00:00Z"):
    return LabelEvent(kind, eid, "alice", at)


def shift(iso, seconds):
    return (datetime.fromisoformat(iso) + timedelta(seconds=seconds)).isoformat()


class Mocks:
    def __init__(
        self,
        plan=PLAN,
        bindings=None,
        stage=None,
        drops=None,
        states=None,
        refuse=None,
        gate=None,
        gate_on="",
        unbound=(),
        nudges=None,
        locks=None,
        raw=None,
        fetches=None,
        quotas=None,
        limits=None,
        release_gates=None,
        raw_ready=None,
        quota_after=None,
        missing=None,
    ):
        self.calls: list[tuple[str, tuple]] = []
        self.plan, self.bindings, self.stage = plan, bindings or {}, stage or {}
        self.drops, self.states, self.refuse = drops or {}, states or {}, refuse or {}
        self.gate, self.gate_on = gate, gate_on
        self.unbound, self.bound = set(unbound), set()  # records without a binding until added to `bound`
        self.nudges = nudges or {}  # record -> DatasetIssue
        # record -> answers in turn (the last one repeats): take_lock, read_fetch, check_quota; raw_status as is
        self.locks, self.fetches, self.quotas = locks or {}, fetches or {}, quotas or {}
        self.raw = raw or {}  # record -> RawStatus; default: the raw cache is complete, nothing to fetch
        self.limits = limits or SourcingLimits(2, 120, "http://svc:8765")
        self.release_gates = release_gates or {}  # record -> asyncio.Event its release_lock waits for
        self.raw_ready = raw_ready or {}  # record -> raw_ready's answer; default True
        self.board: list[tuple[str, str]] = []  # (record, status) as mark_dataset received them
        self.quota_after = quota_after or {}  # record -> the record whose fetch must be submitted before its quota
        self.missing = missing  # missing_records' answers in turn (the last repeats); default: every pool record

    def acts(self):
        rec = lambda name, *a: self.calls.append((name, a))  # noqa: E731

        @activity.defn(name="admit")
        async def admit(issue: int, event_id: str) -> BufferPins:
            rec("admit", event_id)
            return PINS

        @activity.defn(name="plan")
        async def plan(issue: int, round_id: str, pins: BufferPins) -> PlanResult:
            rec("plan", round_id)
            return self.plan

        @activity.defn(name="limits")
        async def limits() -> SourcingLimits:
            rec("limits")
            return self.limits

        @activity.defn(name="set_stage")
        async def set_stage(
            issue: int, stage: str, state: BufferState | None = None, detail: str = "", extra: str = ""
        ) -> None:
            rec("set_stage", stage, detail, extra)

        @activity.defn(name="comment")
        async def comment(issue: int, body: str, marker: str) -> str | None:
            rec("comment", body, marker)
            return None

        @activity.defn(name="comment_plan")
        async def comment_plan(issue: int, round_id: str, dropped: dict[str, str] | None = None) -> None:
            rec("comment_plan", round_id, dropped)

        @activity.defn(name="nudge_dataset")
        async def nudge_dataset(round_id: str, record: str) -> DatasetIssue:
            rec("nudge_dataset", record)
            return self.nudges.get(record, DatasetIssue(5, "new", True, folder=f"lm-{record}"))

        @activity.defn(name="comment_drop")
        async def comment_drop(issue: int, round_id: str, record: str, missing: list[str]) -> None:
            rec("comment_drop", record, missing)

        @activity.defn(name="refuse_label")
        async def refuse_label(issue: int, label: str, reason: str, event_id: str) -> None:
            rec("refuse_label", label, reason, event_id)

        @activity.defn(name="read_binding")
        async def read_binding(round_id: str, record: str) -> BindingInfo:
            rec("read_binding", record)
            if record in self.unbound and record not in self.bound:
                return BindingInfo(False, errors=[f"no prepared dataset at /s/{record}: the dataset is not data:done"])
            manual = next(d.manual for d in self.plan.pool if d.record == record)
            default = (
                BindingInfo(True, f"/s/{record}.yaml", "bh", [], [[f"manual://{record}/a.zip", "a.zip"]])
                if manual
                else BindingInfo(True, f"/s/{record}.yaml", "bh", ["zenodo.org"])
            )
            return turn(self.bindings, record, default)

        @activity.defn(name="prepare_stage")
        async def prepare_stage(issue: int, round_id: str, record: str, rebuild: int = 0) -> str:
            rec("prepare_stage", record, *([rebuild] if rebuild else []))
            return f"/w/{round_id}/stage-{record}" + (f"-r{rebuild}" if rebuild else "")

        @activity.defn(name="submit")
        async def submit(key: str, args: StageJobArgs) -> str:
            rec("submit", key, args)
            return f"job-{key}"

        @activity.defn(name="read_stage")
        async def read_stage(round_id: str, record: str, rebuild: int = 0) -> StageResult:
            rec("read_stage", record, *([rebuild] if rebuild else []))
            return self.stage.get(record, StageResult(True, 1))

        @activity.defn(name="verify_drop")
        async def verify_drop(round_id: str, record: str) -> DropResult:
            rec("verify_drop", record)
            queue = self.drops.get(record)
            return queue.pop(0) if queue else DropResult(True, ["a.zip"])

        @activity.defn(name="create_buffer")
        async def create_buffer(round_id: str, buffer_id: str) -> None:
            rec("create_buffer", buffer_id)

        @activity.defn(name="register_dataset")
        async def register_dataset(round_id: str, record: str, buffer_id: str) -> RegisterResult:
            rec("register_dataset", record, buffer_id)
            if self.refuse.get(record) == "__raise__":
                raise ApplicationError("boom: buffer-api answered 502 with an HTML page", non_retryable=True)
            if record in self.refuse:
                return RegisterResult(refused={f"{record}/v0.zarr": self.refuse[record]})
            return RegisterResult([f"{record}/v0.zarr"])

        return [
            admit,
            plan,
            limits,
            set_stage,
            comment,
            comment_plan,
            nudge_dataset,
            comment_drop,
            refuse_label,
            read_binding,
            prepare_stage,
            submit,
            read_stage,
            verify_drop,
            create_buffer,
            register_dataset,
            *self.cache_acts(),
        ]

    def cache_acts(self):
        rec = lambda name, *a: self.calls.append((name, a))  # noqa: E731

        @activity.defn(name="take_lock")
        async def take_lock(kind: str, key: str, owner: str, ttl_s: int) -> LockTaken:
            rec("take_lock", kind, key, owner, ttl_s)
            return turn(self.locks, key, LockTaken(True))

        @activity.defn(name="release_lock")
        async def release_lock(kind: str, key: str, owner: str) -> None:
            rec("release_lock", kind, key, owner)
            if gate := self.release_gates.get(key):
                await gate.wait()

        @activity.defn(name="raw_status")
        async def raw_status(round_id: str, record: str) -> RawStatus:
            rec("raw_status", record)
            return self.raw.get(record, RawStatus(True))

        @activity.defn(name="check_quota")
        async def check_quota(round_id: str, record: str) -> QuotaCheck:
            if other := self.quota_after.get(record):  # the activities of two datasets race: fix their order
                while not any(args[1].record == other for args in self.of("submit_fetch")):
                    await asyncio.sleep(0.01)
            rec("check_quota", record)
            return turn(self.quotas, record, QuotaCheck(True))

        @activity.defn(name="prepare_fetch")
        async def prepare_fetch(issue: int, round_id: str, record: str, attempt: int) -> str:
            rec("prepare_fetch", record, attempt)
            return f"/w/{round_id}/fetch-{record}-{attempt}"

        @activity.defn(name="submit_fetch")
        async def submit_fetch(key: str, args: FetchJobArgs) -> str:
            rec("submit_fetch", key, args)
            return f"job-{key}"

        @activity.defn(name="read_fetch")
        async def read_fetch(round_id: str, record: str, workspace: str) -> FetchResult:
            rec("read_fetch", record, workspace)
            return turn(self.fetches, record, FetchResult(True))

        @activity.defn(name="mark_asset")
        async def mark_asset(kind: str, key: str, fields: dict) -> None:
            rec("mark_asset", kind, key, fields)

        @activity.defn(name="mark_dataset")
        async def mark_dataset(buffer: str, record: str, status: str, reason: str = "", job: str | None = None) -> None:
            rec("mark_dataset", buffer, record, status, reason)
            self.board.append((record, status))

        @activity.defn(name="buffer_event")
        async def buffer_event(buffer_id: str, kind: str, detail: str = "") -> None:
            rec("buffer_event", buffer_id, kind, detail)

        @activity.defn(name="retire_buffer")
        async def retire_buffer(round_id: str, buffer_id: str) -> dict:
            rec("retire_buffer", buffer_id)
            return {"refs": 2, "draws": 5, "archive": f"/state/archive/{buffer_id}.draws.jsonl.gz"}

        @activity.defn(name="missing_records")
        async def missing_records(buffer_id: str) -> list[str]:
            rec("missing_records", buffer_id)
            if self.missing is None:
                return [d.record for d in self.plan.pool]
            return self.missing.pop(0) if len(self.missing) > 1 else self.missing[0]

        @activity.defn(name="raw_ready")
        async def raw_ready(round_id: str, record: str) -> bool:
            rec("raw_ready", record)
            return self.raw_ready.get(record, True)

        return [
            take_lock,
            release_lock,
            raw_status,
            check_quota,
            prepare_fetch,
            submit_fetch,
            read_fetch,
            mark_asset,
            mark_dataset,
            buffer_event,
            retire_buffer,
            raw_ready,
            missing_records,
        ]

    def await_act(self):
        @activity.defn(name="await_job")
        async def await_job(job_id: str, ws: str, lock: list[str] | None = None) -> JobDone:
            self.calls.append(("await", (job_id, ws, lock)))
            if self.gate and self.gate_on in ws:
                await self.gate.wait()
            state, code = self.states.get(ws.rsplit("/", 1)[1], ("DONE", 0))  # by stage-<record>
            return JobDone(job_id, state, code)

        return [await_job]

    def names(self):
        return [c[0] for c in self.calls]

    def of(self, name):
        return [c[1] for c in self.calls if c[0] == name]

    @property
    def stages(self):
        return [a[0] for a in self.of("set_stage")]


def turn(answers, record, default):
    """The next of a record's queued answers (a list; its last one repeats), the one answer, or `default`."""
    queued = answers.get(record, default)
    if not isinstance(queued, list):
        return queued
    return queued.pop(0) if len(queued) > 1 else queued[0]


async def until(cond, timeout=10.0, m=None):
    for _ in range(int(timeout / 0.02)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out; calls so far: {m.calls if m else '?'}")


async def idle(h, m, stage):
    """Wait until set_stage(stage) ran and the workflow is blocked again."""
    await until(lambda: m.stages[-1:] == [stage])
    for _ in range(500):
        if (await h.fetch_history()).events[-1].event_type in IDLE_EVENTS:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("workflow never went idle")


# a completed workflow task ends the history, or a command of it that needs no answer: the search attribute upsert
# of a patch marker, a timer cancelled (the round's ticker)
IDLE_EVENTS = (
    EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
    EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES,
    EventType.EVENT_TYPE_TIMER_CANCELED,
)


@contextlib.asynccontextmanager
async def started(env, m, first=None, runner=None, signal="go"):
    tq = f"tq-{uuid.uuid4()}"
    kw = {"workflow_runner": runner} if runner else {}
    async with (
        Worker(env.client, task_queue=tq, workflows=[MiaBuffer], activities=m.acts(), **kw),
        Worker(env.client, task_queue=tq + "-await", activities=m.await_act()),
    ):
        h0 = await env.client.start_workflow(
            MiaBuffer.run,
            BufferState(7),
            id=f"wf-{uuid.uuid4()}",
            task_queue=tq,
            start_signal=signal,
            start_signal_args=[first or ev("go", "11")],
        )
        h = env.client.get_workflow_handle(h0.id)  # follows continue-as-new
        h.first_run_id = h0.run_id
        try:
            yield h
        finally:
            await h.terminate()


async def approve(h, m, eid="12", dt=1):
    await idle(h, m, "plan-review")
    planned_at = (await h.query(MiaBuffer.status)).planned_at
    await h.signal(MiaBuffer.approve, ev("approve", eid, shift(planned_at, dt)))


def dataset(state, record):
    return next(d for d in state.datasets if d.record == record)


async def test_go_plan_approve_sourcing_ready(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("register_dataset") and m.of("comment_drop"))
        state = await h.query(MiaBuffer.status)
        assert dataset(state, "u937").status == "servable" and dataset(state, "bcss").status == "waiting-drop"
        assert m.stages == ["planning", "plan-review", "sourcing"]  # bcss's group is empty: not ready yet
        await h.signal(MiaBuffer.dropped, "bcss")
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    assert m.stages == ["planning", "plan-review", "sourcing", "ready"]
    assert m.of("create_buffer") == [("i7-e11",)] and state.buffer_id == "i7-e11" and state.ready
    assert sorted(a[0] for a in m.of("register_dataset")) == ["bcss", "u937"]
    assert [a[0] for a in m.of("comment_drop")] == ["bcss"] and m.of("verify_drop") == [("bcss",)]
    keys = sorted(a[0] for a in m.of("submit"))
    assert len(keys) == 2 and all(k.startswith("buf-7-i7-e11-stage-") for k in keys)
    args = m.of("submit")[0][1]
    assert (args.levels, args.patch, args.pins) == ([1, 2, 4], 256, PINS) and args.out == ""
    assert {a[1].record: a[1].voxel_size_nm for a in m.of("submit")} == {
        "u937": {"z": 1.0, "y": 45.0718, "x": 45.0718},
        "bcss": None,
    }
    assert {d.record: d.status for d in state.datasets} == {"u937": "servable", "bcss": "servable"}
    ready = m.of("set_stage")[-1]
    assert "BufferDataset(" in ready[2] and "i7-e11" in ready[2] and "http://svc:8765" in ready[2]
    assert not m.of("refuse_label")


async def test_progressive_ready_with_lowered_fraction(env):
    m = Mocks(plan=ONE_GROUP, gate=asyncio.Event(), gate_on="stage-b")
    async with started(env, m) as h:
        await approve(h, m)
        await h.signal(MiaBuffer.dropped, "b")
        await idle(h, m, "ready")  # a alone is half the group: ready
        state = await h.query(MiaBuffer.status)
        assert dataset(state, "b").status == "staging" and dataset(state, "a").status == "servable"
        m.gate.set()
        await until(lambda: len(m.of("register_dataset")) == 2)
        await until(lambda: any("`b` is servable" in a[0] for a in m.of("comment")))
        state = await h.query(MiaBuffer.status)
    assert m.stages == ["planning", "plan-review", "sourcing", "ready"]
    assert dataset(state, "b").status == "servable"


async def test_invalid_plan_is_needs_human(env):
    m = Mocks(plan=PlanResult(False, ["patch.c all: channel counts differ"], [], 10))
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert m.stages == ["planning", "needs-human"] and m.of("comment_plan") == [("i7-e11", None)]
    assert "channel counts differ" in m.of("set_stage")[-1][1] and not m.of("create_buffer")


async def test_redelivered_go_is_ignored(env):
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "plan-review")
        await h.signal(MiaBuffer.go, ev("go", "11"))
        await h.signal(MiaBuffer.go, ev("go", "12"))  # a new go: the spec was edited; plan again
        await until(lambda: len(m.of("admit")) == 2)
        await idle(h, m, "plan-review")
    assert m.of("admit") == [("11",), ("12",)] and not m.of("refuse_label")


async def test_stale_approve_is_refused(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve(h, m, eid="12", dt=-60)
        await until(lambda: m.of("refuse_label"))
        await idle(h, m, "plan-review")
    ((label, reason, eid),) = m.of("refuse_label")
    assert (label, eid) == ("buffer:approve", "12") and "older" in reason and m.stages[-1] == "plan-review"


async def test_approve_outside_plan_review_is_refused(env):
    m = Mocks(plan=PlanResult(False, ["bad"], [], 1))
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
        await h.signal(MiaBuffer.approve, ev("approve", "12"))
        await until(lambda: m.of("refuse_label"))
    assert "plan-review" in m.of("refuse_label")[0][1]


async def test_go_while_sourcing_is_refused_busy(env):
    m = Mocks(plan=ONE_GROUP, gate=asyncio.Event(), gate_on="stage-a")
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("await"))
        await h.signal(MiaBuffer.go, ev("go", "13"))  # "12" is the approve's event id: a reused id is a redelivery
        m.gate.set()
        await h.signal(MiaBuffer.dropped, "b")  # both datasets finish: the round ends and the go was busy
        await until(lambda: m.of("refuse_label"), m=m)
    ((label, reason, eid),) = m.of("refuse_label")
    assert (label, eid) == ("buffer:go", "13") and "busy" in reason and len(m.of("admit")) == 1


async def test_stage_report_not_ok_fails_dataset_without_registering(env):
    only = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
    m = Mocks(plan=only, stage={"u937": StageResult(False, 0, ["raw 0: voxel size unknown"])})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert not m.of("register_dataset") and dataset(state, "u937").status == "failed"
    assert "voxel size unknown" in dataset(state, "u937").reason
    assert "groups below ready.min_fraction" in m.of("set_stage")[-1][1] and "u937" in m.of("set_stage")[-1][1]


async def test_job_exit_failure_fails_dataset_with_the_reports_errors(env):
    only = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
    report = StageResult(False, 0, ["host 'zenodo.org' is not allowed (requests: https://zenodo.org/a.zip)"])
    m = Mocks(plan=only, states={"stage-u937": ("EXIT", 1)}, stage={"u937": report})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    reason = dataset(state, "u937").reason
    assert dataset(state, "u937").status == "failed" and "exited with code 1" in reason and "zenodo.org" in reason
    assert len(m.of("read_stage")) == 1 and not m.of("register_dataset")


async def test_drop_with_missing_files_keeps_waiting(env):
    only = PlanResult(True, [], [DatasetPlan("bcss", "histology-WSI", 1.0, True)], 1, levels=[1], patch=64)
    m = Mocks(plan=only, drops={"bcss": [DropResult(False, [], ["a.zip"]), DropResult(True, ["a.zip"])]})
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop"))
        await h.signal(MiaBuffer.dropped, "bcss")
        await until(lambda: len(m.of("comment_drop")) == 2)
        state = await h.query(MiaBuffer.status)
        assert dataset(state, "bcss").status == "waiting-drop" and m.of("comment_drop")[1] == ("bcss", ["a.zip"])
        await h.signal(MiaBuffer.dropped, "bcss")
        await idle(h, m, "ready")
    assert m.of("verify_drop") == [("bcss",), ("bcss",)] and m.stages[-1] == "ready"


# a real-time day limit of ~5 s (a long env.sleep on the shared time-skipping server can hang it)
SHORT_DAYS = 6e-5


async def test_timeout_without_ready_is_needs_human(env):
    only = PlanResult(
        True, [], [DatasetPlan("bcss", "histology-WSI", 1.0, True)], 1, levels=[1], patch=64, timeout_days=SHORT_DAYS
    )
    m = Mocks(plan=only)
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop"))
        await idle(h, m, "needs-human")
    detail = m.of("set_stage")[-1][1]
    assert "after 6e-05 days" in detail and "histology-WSI" in detail and not m.of("verify_drop")


async def test_timeout_after_ready_changes_nothing(env):
    m = Mocks(plan=replace(ONE_GROUP, timeout_days=SHORT_DAYS))  # a and b share one group with min_fraction 0.5:
    async with started(env, m) as h:  # a alone makes the buffer ready
        await approve(h, m)
        await idle(h, m, "ready")
        await asyncio.sleep(SHORT_DAYS * 86400 + 1)  # b is still waiting for its drop past the day limit
        state = await h.query(MiaBuffer.status)
    assert m.stages == ["planning", "plan-review", "sourcing", "ready"] and state.ready
    assert dataset(state, "b").status == "waiting-drop" and not m.of("verify_drop")


async def test_rollover_in_plan_review_then_approve(env, monkeypatch):
    from temporalio import workflow

    monkeypatch.setattr(workflows, "_rollover_due", lambda: workflow.info().continued_run_id is None)  # once
    only = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
    m = Mocks(plan=only)
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await until(lambda: m.stages[-1:] == ["plan-review"])
        for _ in range(500):
            if (await h.describe()).run_id != h.first_run_id:
                break
            await asyncio.sleep(0.02)
        await approve(h, m)
        await idle(h, m, "ready")
    assert (await h.describe()).run_id != h.first_run_id
    assert m.stages == ["planning", "plan-review", "sourcing", "ready"] and len(m.of("admit")) == 1


# --- the poller -------------------------------------------------------------------------------


class PollGH:
    def __init__(self, issues, events=None, perms=None):
        self.issues, self.events, self.perms = issues, events or {}, perms or {}
        self.comments, self.removed = {}, []

    def tokens(self):
        return "tok"

    async def list_issues(self, labels=None, state=None, **_):
        return [
            i
            for i in self.issues.values()
            if (not labels or labels in {x["name"] for x in i["labels"]})
            and (state is None or i.get("state", "open") == state)
        ]

    async def add_labels(self, n, names):
        self.issues[n]["labels"] += [{"name": x} for x in names]

    async def label_event(self, n, label):
        return self.events[(n, label)]

    async def permission(self, login):
        return self.perms.get(login, "none")

    async def remove_label(self, n, name):
        self.removed.append((n, name))
        self.issues[n]["labels"] = [x for x in self.issues[n]["labels"] if x["name"] != name]

    async def issue_comments(self, n):
        return self.comments.get(n, [])

    async def comment(self, n, body):
        self.comments.setdefault(n, []).append({"body": body, "user": {"login": "bot"}, "html_url": "u"})
        return self.comments[n][-1]

    async def get_issue(self, n):
        return self.issues[n]


class FakeClient:
    def __init__(self, states):
        self.states, self.started, self.signals, self.terminated = states, [], [], []

    async def start_workflow(self, wf, arg, **kw):
        self.started.append((wf, arg, kw))

    def get_workflow_handle(self, wid):
        client = self

        class Handle:
            async def signal(self, name, arg=None, *, args=None):
                client.signals.append((wid, name, tuple(args) if args is not None else arg))

            async def query(self, name, result_type=None):
                return client.states[wid]

            async def terminate(self, reason=None):
                if wid not in client.states:
                    raise RPCError("workflow execution already completed", None, None)
                client.terminated.append((wid, reason))

        return Handle()


def issue(n, *labels):
    return {"number": n, "body": "", "labels": [{"name": x} for x in labels]}


async def test_poll_delivers_labels_and_drops(tmp_path):
    cfg = make_cfg(tmp_path)
    at = "2026-10-24T10:00:00Z"
    gh = PollGH(
        issues={
            1: issue(1, "buffer:new", "buffer:go"),
            2: issue(2, "buffer:sourcing"),
            3: issue(3, "buffer:ready"),
            4: issue(4, "buffer:staged"),  # at the gate: a waiting dataset's drop is still delivered (spec §15)
        },
        events={(1, "buffer:go"): ("alice", "501", at)},
        perms={"alice": "write"},
    )
    waiting = BufferState(2, round_id="i2-e9", stage="sourcing")
    waiting.datasets = [
        workflows.DatasetStatus("bcss", "histology-WSI", True, "waiting-drop"),
        workflows.DatasetStatus("u937", "confocal", False, "servable"),
    ]
    ready = BufferState(
        3, round_id="i3-e1", stage="ready", datasets=[workflows.DatasetStatus("x", "g", True, "waiting-drop")]
    )
    staged = BufferState(
        4, round_id="i4-e2", stage="staged", datasets=[workflows.DatasetStatus("y", "g", True, "waiting-drop")]
    )
    client = FakeClient({cfg.workflow_id(2): waiting, cfg.workflow_id(3): ready, cfg.workflow_id(4): staged})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [])
    (cfg.paths.raw_root / "bcss" / "incoming").mkdir(parents=True)
    (cfg.paths.raw_root / "bcss" / "incoming" / "a.zip").write_bytes(b"z")
    (cfg.paths.raw_root / "x" / "incoming").mkdir(parents=True)  # empty: nothing to signal
    (cfg.paths.raw_root / "y" / "incoming").mkdir(parents=True)
    (cfg.paths.raw_root / "y" / "incoming" / "b.tif").write_bytes(b"t")
    assert await run_poll(acts) == ["go #1"]  # the drop is not signalled on first sight (a copy may be running)
    assert set(await run_poll(acts)) == {"dropped #2 bcss", "dropped #4 y"}
    ((wf, state, kw),) = client.started
    assert wf == "MiaBuffer" and state == BufferState(1) and kw["start_signal"] == "go"
    assert kw["start_signal_args"] == [LabelEvent("go", "501", "alice", at)] and kw["id"] == cfg.workflow_id(1)
    assert gh.removed == [(1, "buffer:go")]
    assert set(client.signals) == {(cfg.workflow_id(2), "dropped", "bcss"), (cfg.workflow_id(4), "dropped", "y")}
    assert await run_poll(acts) == []  # nothing new: the drop was signalled once
    (cfg.paths.raw_root / "bcss" / "incoming" / "b.zip").write_bytes(b"z")
    assert await run_poll(acts) == []  # a new file: seen, not yet stable
    assert await run_poll(acts) == ["dropped #2 bcss"]  # once more


async def test_poll_retires_a_closed_issue(tmp_path):
    """Closing the issue is the retire: the workflow is terminated (frozen or not), the buffer released, the
    label swapped, once; a closed issue with no workflow and no buffer just gets its label."""
    cfg = make_cfg(tmp_path)
    gh = PollGH(issues={2: issue(2, "buffer:sourcing"), 3: issue(3, "buffer:plan-review"), 4: issue(4, "buffer:ready")})
    gh.issues[2]["state"] = gh.issues[3]["state"] = "closed"
    states = {
        cfg.workflow_id(n): BufferState(n, round_id=f"i{n}-e1", stage=s) for n, s in ((2, "sourcing"), (4, "ready"))
    }
    client = FakeClient(states)
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [])

    class Reg:
        retired, events = [], []

        def buffers(self):
            return [{"id": "i2-e9", "latest_event": {"kind": "approved"}}, {"id": "i4-e1", "latest_event": None}]

        def retire(self, buffer):
            self.retired.append(buffer)
            return {"refs": 1, "draws": 0, "archive": None}

        def event(self, buffer, kind, **kw):
            self.events.append((buffer, kind))

    acts.registry = Reg()
    expect = {f"terminated {cfg.workflow_id(2)}", "retired i2-e9", "closed #2", "closed #3"}
    assert set(await run_poll(acts)) == expect
    assert client.terminated == [(cfg.workflow_id(2), "issue closed")]
    assert acts.registry.retired == ["i2-e9"] and acts.registry.events == [("i2-e9", "retired")]
    assert {x["name"] for x in gh.issues[2]["labels"]} == {"buffer:retired"}
    assert {x["name"] for x in gh.issues[3]["labels"]} == {"buffer:retired"}
    assert {x["name"] for x in gh.issues[4]["labels"]} == {"buffer:ready"}  # open: untouched
    assert "workflow stopped; retired `i2-e9`" in gh.comments[2][0]["body"]
    assert "no workflow running" in gh.comments[3][0]["body"]
    assert await run_poll(acts) == [] and len(gh.comments[2]) == 1  # the label is gone: nothing to do again


async def test_poll_refuses_go_from_a_reader(tmp_path):
    cfg = make_cfg(tmp_path)
    gh = PollGH(issues={1: issue(1, "buffer:go")}, events={(1, "buffer:go"): ("eve", "7", "2026-10-24T10:00:00Z")})
    client = FakeClient({})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [])
    assert await run_poll(acts) == [] and not client.started and gh.removed == [(1, "buffer:go")]
    assert "does not have write access" in gh.comments[1][0]["body"]


async def test_a_pyramid_staged_earlier_is_registered_without_a_stage_job(env):
    """read_binding found this binding's pyramid in the registry (an earlier round or buffer staged it): no
    workspace, no job; read_stage re-reads it and registration proceeds."""
    m = Mocks(bindings={"u937": BindingInfo(True, "/s/u937.yaml", "bh", ["zenodo.org"], staged=True)})
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop"))
        await h.signal(MiaBuffer.dropped, "bcss")
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    assert [a[0] for a in m.of("prepare_stage")] == ["bcss"] and [a[1].record for a in m.of("submit")] == ["bcss"]
    assert sorted(a[0] for a in m.of("read_stage")) == ["bcss", "u937"]
    assert dataset(state, "u937").status == "servable" and dataset(state, "u937").volumes == 1
    servable = [c[0] for c in m.of("comment") if c[1].endswith("u937 servable")]
    assert len(servable) == 1 and "staged earlier" in servable[0]


# --- replay -------------------------------------------------------------------------------------

HISTORY = Path(__file__).parent / "fixtures" / "buffers" / "buffer_history.json"
CACHE_HISTORY = HISTORY.with_name("buffer_history_c.json")
LIFECYCLE_HISTORY = HISTORY.with_name("buffer_history_d.json")
REBUILD_HISTORY = HISTORY.with_name("buffer_history_d_rebuild.json")


def _scrub(obj):
    if isinstance(obj, dict):
        return {k: "test" if k == "identity" else _scrub(v) for k, v in obj.items()}
    return [_scrub(v) for v in obj] if isinstance(obj, list) else obj


async def test_replay_buffer_history(env):
    """The recorded go → plan → approve → sourcing → ready run (with a manual drop) must replay on today's
    MiaBuffer. Re-record after an intended change guarded by workflow.patched():
    MIA_BUFFERS_RECORD_HISTORY=1 pytest tests/test_buffers_workflows.py -k replay"""
    if os.environ.get("MIA_BUFFERS_RECORD_HISTORY"):
        m = Mocks()
        async with started(env, m) as h:
            await approve(h, m)
            await until(lambda: m.of("comment_drop"))
            await h.signal(MiaBuffer.dropped, "bcss")
            await idle(h, m, "ready")
            HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("buffers-recorded", HISTORY.read_text())
    await Replayer(workflows=[MiaBuffer]).replay_workflow(history)


async def test_replay_buffer_history_c(env):
    """The go → ready run under the `c-cache` patch: u937 waits once for its raw lock, fetches and stages offline,
    bcss is dropped by hand; as test_replay_buffer_history. Re-record this one alone, so the older fixture keeps
    the unpatched path: MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay_buffer_history_c"""
    if os.environ.get("MIA_BUFFERS_RECORD_HISTORY"):
        m = Mocks(locks={"u937": [HELD, LockTaken(True)]}, raw={"u937": RawStatus(False, [U937_URL])})
        async with started(env, m) as h:
            await approve(h, m)
            await waits_on_lock(h, "u937")
            await env.sleep(timedelta(minutes=3))
            await until(lambda: m.of("comment_drop"), m=m)
            await h.signal(MiaBuffer.dropped, "bcss")
            await idle(h, m, "ready")
            CACHE_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("buffers-c-cache", CACHE_HISTORY.read_text())
    await Replayer(workflows=[MiaBuffer]).replay_workflow(history)


# --- review fix pass -----------------------------------------------------------------------------


async def test_activity_failure_fails_only_that_dataset(env):
    m = Mocks(plan=ONE_GROUP, refuse={"a": "__raise__"})  # register_dataset raises a non-retryable error for a
    async with started(env, m) as h:
        await approve(h, m)
        await h.signal(MiaBuffer.dropped, "b")
        await idle(h, m, "ready")  # b alone is half the group
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "a").status == "failed" and "boom" in dataset(state, "a").reason
    assert dataset(state, "b").status == "servable" and "needs-human" not in m.stages


async def test_go_after_ready_with_a_straggler_starts_a_new_round(env):
    m = Mocks(plan=ONE_GROUP)
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")  # a servable, b waiting for its drop
        await h.signal(MiaBuffer.approve, ev("approve", "14"))
        await until(lambda: m.of("refuse_label"), m=m)
        await h.signal(MiaBuffer.go, ev("go", "15"))
        await until(lambda: len(m.of("admit")) == 2, m=m)
        await idle(h, m, "plan-review")
        state = await h.query(MiaBuffer.status)
    assert m.of("refuse_label")[0][0] == "buffer:approve" and "plan-review" in m.of("refuse_label")[0][1]
    assert m.stages[-3:] == ["ready", "planning", "plan-review"] and state.round_id == "i7-e15"
    assert not m.of("verify_drop")  # the straggler was abandoned, not dropped


async def test_poll_signals_only_a_stable_listing(tmp_path):
    cfg = make_cfg(tmp_path)
    gh = PollGH(issues={2: issue(2, "buffer:sourcing")})
    waiting = BufferState(2, round_id="i2-e9", stage="sourcing")
    waiting.datasets = [workflows.DatasetStatus("bcss", "histology-WSI", True, "waiting-drop")]
    client = FakeClient({cfg.workflow_id(2): waiting})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [])
    incoming = cfg.paths.raw_root / "bcss" / "incoming"
    incoming.mkdir(parents=True)
    (incoming / "a.zip").write_bytes(b"part")
    assert await run_poll(acts) == []  # first sight: the file may still be copying
    (incoming / "a.zip").write_bytes(b"part two")  # grew: still copying
    assert await run_poll(acts) == []
    assert await run_poll(acts) == ["dropped #2 bcss"]  # unchanged for a whole pass: stable
    assert await run_poll(acts) == []


# --- milestone 4: datasets without a binding -----------------------------------------------------


async def test_timeout_drops_waiting_datasets_and_may_reach_ready(env):
    m = Mocks(plan=replace(PLAN, timeout_days=SHORT_DAYS))  # bcss waits for a manual drop that never comes
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("register_dataset") and m.of("comment_drop"), m=m)
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    bcss = dataset(state, "bcss")
    assert bcss.status == "dropped" and "after 6e-05 days" in bcss.reason and "waiting-drop" in bcss.reason
    assert m.of("comment_plan")[-1][1] == {"bcss": bcss.reason} and "needs-human" not in m.stages
    assert state.ready and dataset(state, "u937").status == "servable"


async def test_poll_no_longer_watches_bindings(tmp_path):
    """Milestone E: a buffer never waits on mia-data, so the poller sends no `dataset_update` for a pre-E round
    still waiting: a published binding and an ineligible dataset issue both go unsignalled."""
    cfg = make_cfg(tmp_path)
    gh = PollGH(issues={2: issue(2, "buffer:sourcing")})
    datasets = PollGH(issues={5: issue(5, "data:ineligible", "licence-excluded")})
    waiting = BufferState(2, round_id="i2-e9", stage="sourcing")
    waiting.datasets = [
        workflows.DatasetStatus("u937", "confocal", False, "waiting-dataset", issue=4, folder="lm-u937"),
        workflows.DatasetStatus("bcss", "histology-WSI", True, "waiting-dataset", issue=5, folder="lm-bcss"),
    ]
    client = FakeClient({cfg.workflow_id(2): waiting})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [], gh_datasets=datasets)
    (cfg.paths.storage_root / "lm-u937").mkdir(parents=True)
    (cfg.paths.storage_root / "lm-u937" / "binding.yaml").write_text("record: u937\n")
    reindex(cfg.paths.storage_root / "lm-u937", {})
    assert await run_poll(acts) == [] and client.signals == []


async def test_poll_starts_a_prep_workflow_on_prep_go(tmp_path):
    cfg = make_cfg(tmp_path)
    at = "2026-10-26T10:00:00Z"
    gh = PollGH(
        issues={8: issue(8, "prep:go"), 9: issue(9, "prep:go")},
        events={(8, "prep:go"): ("alice", "71", at), (9, "prep:go"): ("eve", "72", at)},
        perms={"alice": "write"},
    )
    client = FakeClient({})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [])
    assert await run_poll(acts) == ["prep:go #8"]
    ((wf, state, kw),) = client.started  # eve has no write access: her label is removed, nothing starts
    assert (
        wf == "MiaPrep"
        and state == PrepState(8)
        and kw["id"] == cfg.prep_workflow_id(8) == ("mia-buffers/o/buffers/prep-8")
    )
    assert kw["start_signal"] == "go" and kw["start_signal_args"] == [LabelEvent("go", "71", "alice", at)]
    assert gh.removed == [(8, "prep:go"), (9, "prep:go")] and "does not have write access" in gh.comments[9][0]["body"]


async def test_timeout_lets_a_staging_job_finish_then_announces_ready(env):
    """Review Focus 5: at the timeout the waiters are dropped, a stage job still running finishes, and ready
    follows from the groups that remain."""
    # a real-time timeout of ~5 s: the test server cannot skip time while await_job is blocked on the gate
    short = PlanResult(True, [], PLAN.pool, 10, levels=[1, 2, 4], patch=256, timeout_days=6e-5)
    m = Mocks(plan=short, gate=asyncio.Event(), gate_on="stage-u937")
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop") and m.of("await"), m=m)  # bcss waits on a drop, u937's job runs
        for _ in range(400):
            state = await h.query(MiaBuffer.status)
            if dataset(state, "bcss").status == "dropped":
                break
            await asyncio.sleep(0.05)
        assert dataset(state, "bcss").status == "dropped" and dataset(state, "u937").status == "staging"
        assert "needs-human" not in m.stages
        m.gate.set()
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "servable" and state.ready and m.stages[-1] == "ready"


# --- milestone C: the raw-cache lock, the fetch phase, the offline stage ------------------------------

ONLY_U937 = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
U937_URL = "https://zenodo.org/u937.zip"
OWNER = "mia-buffers i7-e11 u937"
HELD = LockTaken(False, "mia-buffers i9-e9 rec")


async def waits_on_lock(h, record, status="waiting-lock"):
    """Poll the status query until `record` shows `status`; returns that dataset."""
    for _ in range(400):
        found = dataset(await h.query(MiaBuffer.status), record)
        if found.status == status:
            return found
        await asyncio.sleep(0.05)
    raise AssertionError(f"{record} never showed {status}")


def order(m, *names, record=None):
    """The calls among `names`, in the order they ran; only those naming `record` among their arguments if given."""
    return [name for name, args in m.calls if name in names and (record is None or record in args)]


async def poll_timers(h) -> list[float]:
    """The minutes of every timer the run started, but the day limit's."""
    events = (await h.fetch_history()).events
    seconds = [
        e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta().total_seconds()
        for e in events
        if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED
    ]
    return [value / 60 for value in seconds if value < 3600]


async def scheduled(h, name) -> list[list]:
    """The arguments of every `name` activity the run scheduled, in history order."""
    return [
        [json.loads(payload.data) for payload in e.activity_task_scheduled_event_attributes.input.payloads]
        for e in (await h.fetch_history()).events
        if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
        and e.activity_task_scheduled_event_attributes.activity_type.name == name
    ]


async def test_two_buffers_one_record_second_waits_then_reuses(env):
    """Another buffer holds u937's raw lock and stages it meanwhile: this one waits, then registers that pyramid."""
    staged_after = BindingInfo(True, "/s/u937.yaml", "bh", ["zenodo.org"], staged=True)
    m = Mocks(
        plan=ONLY_U937,
        locks={"u937": [HELD, HELD, LockTaken(True)]},
        bindings={"u937": [BindingInfo(True, "/s/u937.yaml", "bh", ["zenodo.org"]), staged_after]},
    )
    async with started(env, m) as h:
        await approve(h, m)
        waiting = await waits_on_lock(h, "u937")
        assert "raw cache held by mia-buffers i9-e9 rec" in waiting.reason
        await env.sleep(timedelta(minutes=3))
        await until(lambda: len(m.of("take_lock")) == 2, m=m)
        await env.sleep(timedelta(minutes=5))
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
        assert await poll_timers(h) == [2, 4]  # poll_minutes, doubling
    assert m.of("take_lock") == [("raw", "u937", OWNER, 900)] * 3
    assert not m.of("submit_fetch") and not m.of("submit") and not m.of("prepare_stage")
    assert order(m, "take_lock", "read_binding", "read_stage", "register_dataset", "release_lock") == [
        "read_binding",
        "take_lock",
        "take_lock",
        "take_lock",
        "read_binding",  # re-read once the lock is ours: the holder staged it
        "read_stage",  # the reuse
        "register_dataset",
        "release_lock",
    ]
    assert m.of("release_lock") == [("raw", "u937", OWNER)] and dataset(state, "u937").status == "servable"
    (servable,) = [c[0] for c in m.of("comment") if c[1].endswith("u937 servable")]
    assert "staged by mia-buffers i9-e9 rec while this buffer waited and reused" in servable


async def test_fetch_then_offline_stage(env):
    m = Mocks(
        plan=ONLY_U937,
        raw={"u937": RawStatus(False, [U937_URL])},
        fetches={"u937": [FetchResult(True, partial=True), FetchResult(True, bytes_fetched=9)]},
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
    assert [a[0] for a in m.of("submit_fetch")] == [fetch_job_key(7, "i7-e11", "u937", n) for n in (1, 2)]
    assert [a[1].attempt for a in m.of("submit_fetch")] == [1, 2] and m.of("submit_fetch")[0][1].hosts == ["zenodo.org"]
    assert fetch_job_key(7, "i7-e11", "u937", 1).startswith("buf-7-i7-e11-fetch1-u937-")
    assert [a[2] for a in m.of("await")] == [["raw", "u937", OWNER]] * 3  # two fetch jobs, then the stage job
    ((_, stage),) = m.of("submit")
    assert stage.cache_writable is False and stage.hosts == []
    assert order(m, "take_lock", "submit_fetch", "submit", "register_dataset", "release_lock") == [
        "take_lock",  # the fetch slot was free: no re-take before the fetch
        "submit_fetch",
        "submit_fetch",
        "take_lock",  # and once the stage slot is
        "submit",
        "register_dataset",
        "release_lock",
    ]


async def test_stream_url_keeps_the_stage_online(env):
    m = Mocks(
        plan=ONLY_U937,
        raw={"u937": RawStatus(False, [U937_URL])},
        fetches={"u937": FetchResult(True, stream=["https://h/x.zarr/"])},
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
    ((_, stage),) = m.of("submit")
    assert stage.cache_writable is True and stage.hosts == ["zenodo.org"] and len(m.of("submit_fetch")) == 1


async def test_fetch_still_partial_fails_and_releases(env):
    m = Mocks(
        plan=ONLY_U937,
        raw={"u937": RawStatus(False, [U937_URL])},
        fetches={"u937": FetchResult(True, partial=True)},
        states={"fetch-u937-4": ("EXIT", 137)},  # the last attempt was killed
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    last = f"job-{fetch_job_key(7, 'i7-e11', 'u937', 4)}"
    assert dataset(state, "u937").reason == f"fetch still partial after 4 attempts (last job {last} EXIT exit 137)"
    assert not m.of("submit")
    assert len(m.of("submit_fetch")) == 4 and order(m, "read_fetch", "release_lock")[-2:] == [
        "read_fetch",
        "release_lock",
    ]


async def test_fetch_of_a_host_not_allowed_fails_without_retrying(env):
    refused = FetchResult(False, errors=["egress refused"], host_not_allowed="zenodo.org")
    m = Mocks(plan=ONLY_U937, raw={"u937": RawStatus(False, [U937_URL])}, fetches={"u937": refused})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    reason = dataset(state, "u937").reason
    assert reason == "fetch failed: egress refused (host not allowed: zenodo.org)" and len(m.of("submit_fetch")) == 1
    assert not m.of("submit") and m.of("release_lock") == [("raw", "u937", OWNER)]


async def test_blocked_waits_for_quota_then_fetches(env):
    m = Mocks(
        plan=ONLY_U937,
        raw={"u937": RawStatus(False, [U937_URL])},
        quotas={"u937": [QuotaCheck(False, over_by=5)] * 3 + [QuotaCheck(True)]},
    )
    async with started(env, m) as h:
        await approve(h, m)
        blocked = await waits_on_lock(h, "u937", "blocked")
        assert "over by 5 bytes" in blocked.reason and not m.of("submit_fetch")
        await env.sleep(timedelta(minutes=15))  # the polls back off: 2, 4, 8 minutes
        await idle(h, m, "ready")
        assert await poll_timers(h) == [2, 4, 8]
    assert len([c for c in m.of("comment") if c[1] == "dataset i7-e11 u937 blocked"]) == 1
    assert len(m.of("check_quota")) == 4 and len(m.of("submit_fetch")) == 1
    assert order(m, "check_quota", "take_lock", "submit_fetch", "release_lock") == [
        "take_lock",
        "check_quota",  # before the fetch slot: a blocked dataset holds none
        "release_lock",  # released while blocked (spec §5.2)
        "check_quota",
        "check_quota",
        "check_quota",
        "take_lock",  # taken again before the fetch
        "submit_fetch",
        "take_lock",  # refreshed once the stage slot is free
        "release_lock",
    ]


async def test_dropped_files_mark_the_raw_row_and_stage_offline(env):
    only = PlanResult(True, [], [DatasetPlan("bcss", "histology-WSI", 1.0, True)], 1, levels=[1], patch=64)
    m = Mocks(plan=only, raw={"bcss": RawStatus(False, ["manual://bcss/a.zip"])})
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop"), m=m)
        await h.signal(MiaBuffer.dropped, "bcss")
        await idle(h, m, "ready")
    assert m.of("mark_asset") == [("raw", "bcss", {"state": "waiting-drop"})]
    assert m.names().index("mark_asset") < m.names().index("comment_drop") and not m.of("submit_fetch")
    ((_, stage),) = m.of("submit")
    assert stage.cache_writable is False and stage.hosts == []


async def test_heaviest_dataset_is_sourced_first(env):
    pool = [DatasetPlan("light", "TEM", 0.2, False), DatasetPlan("heavy", "TEM", 0.8, False)]
    m = Mocks(plan=PlanResult(True, [], pool, 2, levels=[1], patch=64))
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
        bindings = await scheduled(h, "read_binding")
    assert [args[1] for args in bindings[:2]] == ["heavy", "light"]
    assert [(d.record, d.weight) for d in state.datasets] == [("light", 0.2), ("heavy", 0.8)]


async def test_timeout_drops_a_blocked_dataset_and_releases_its_lock(env):
    """`blocked` is waiting: the timeout cancels the dataset inside its lock, and the lock is released."""
    # a real-time timeout of ~5 s, as test_timeout_lets_a_staging_job_finish_then_announces_ready
    short = PlanResult(True, [], ONLY_U937.pool, 1, levels=[1], patch=64, timeout_days=6e-5)
    m = Mocks(plan=short, raw={"u937": RawStatus(False, [U937_URL])}, quotas={"u937": QuotaCheck(False, over_by=5)})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        await until(lambda: m.of("release_lock"), m=m)
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "dropped" and "still blocked" in dataset(state, "u937").reason
    assert m.of("release_lock") == [("raw", "u937", OWNER)] and not m.of("submit_fetch")


async def test_a_dataset_blocked_after_the_timeout_is_dropped_too(env):
    """Review fix: with one fetch slot, `second` waits for it (behind `first`'s fetch job) past the timeout; the
    quota re-checked in the slot is then over, so it is dropped rather than blocked forever, and the round ends."""
    two = [DatasetPlan("first", "TEM", 0.6, False), DatasetPlan("second", "TEM", 0.4, False)]
    short = PlanResult(True, [], two, 1, levels=[1], patch=64, timeout_days=SHORT_DAYS)
    m = Mocks(
        plan=short,
        raw={record: RawStatus(False, [U937_URL]) for record in ("first", "second")},
        quotas={"second": [QuotaCheck(True), QuotaCheck(False, over_by=5)]},  # room, until first's fetch filled it
        limits=SourcingLimits(2, 120, "http://svc:8765", parallel_fetch=1),
        gate=asyncio.Event(),
        gate_on="fetch-first",
        quota_after={"second": "first"},
    )
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("await") and len(m.of("check_quota")) == 2, m=m)  # second waits for the slot
        for _ in range(400):  # until the day limit fired
            if any(e.event_type == EventType.EVENT_TYPE_TIMER_FIRED for e in (await h.fetch_history()).events):
                break
            await asyncio.sleep(0.05)
        m.gate.set()
        await idle(h, m, "ready")
        await until(lambda: len(m.of("release_lock")) == 2, m=m)
        state = await h.query(MiaBuffer.status)
    assert {d.record: (d.status, d.reason) for d in state.datasets} == {
        "first": ("servable", ""),
        "second": ("dropped", "still blocked after 6e-05 days"),
    }
    assert order(m, "take_lock", "check_quota", "release_lock", record="second") == [
        "take_lock",
        "check_quota",  # room: on to the fetch slot, which first holds
        "take_lock",  # refreshed after the slot wait
        "check_quota",  # re-checked in the slot: over
        "check_quota",  # back outside the slot: over, and the day limit has fired
        "release_lock",
    ]
    assert [args[1].record for args in m.of("submit_fetch")] == ["first"]


async def test_a_blocked_dataset_leaves_its_fetch_slot_to_others(env):
    """Review fix: the quota wait comes before the fetch slot, so with one slot a blocked dataset does not hold up
    another dataset's fetch."""
    two = [DatasetPlan("first", "TEM", 0.6, False), DatasetPlan("second", "TEM", 0.4, False)]
    m = Mocks(
        plan=PlanResult(True, [], two, 1, levels=[1], patch=64),
        raw={record: RawStatus(False, [U937_URL]) for record in ("first", "second")},
        quotas={"first": [QuotaCheck(False, over_by=5), QuotaCheck(True)]},
        limits=SourcingLimits(2, 120, "http://svc:8765", parallel_fetch=1),
    )
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("submit_fetch"), m=m)
        assert [args[1].record for args in m.of("submit_fetch")] == ["second"]  # while first is blocked
        assert dataset(await h.query(MiaBuffer.status), "first").status == "blocked"
        await env.sleep(timedelta(minutes=3))
        await idle(h, m, "ready")
    assert [args[1].record for args in m.of("submit_fetch")] == ["second", "first"]


async def test_a_retake_that_waited_decides_again(env):
    """Review fix: the lock was taken, then another owner held it at the stage slot's re-take and staged the binding
    meanwhile: the binding is read again and its pyramid reused, no stage job."""
    unstaged = BindingInfo(True, "/s/u937.yaml", "bh", ["zenodo.org"])
    m = Mocks(
        plan=ONLY_U937,
        locks={"u937": [LockTaken(True), HELD, LockTaken(True)]},
        bindings={"u937": [unstaged, unstaged, replace(unstaged, staged=True)]},
    )
    async with started(env, m) as h:
        await approve(h, m)
        await waits_on_lock(h, "u937")
        await env.sleep(timedelta(minutes=3))
        await idle(h, m, "ready")
    assert order(m, "take_lock", "read_binding", "raw_status", "read_stage", "release_lock") == [
        "read_binding",
        "take_lock",
        "read_binding",
        "raw_status",
        "take_lock",  # the stage slot's re-take: held by another owner
        "take_lock",
        "read_binding",  # decided again: staged meanwhile
        "read_stage",
        "release_lock",
    ]
    assert not m.of("submit") and not m.of("prepare_stage")
    (servable,) = [c[0] for c in m.of("comment") if c[1].endswith("u937 servable")]
    assert "staged by mia-buffers i9-e9 rec while this buffer waited" in servable


async def test_a_go_while_blocked_stops_the_blocked_dataset(env):
    """Review fix: a new go abandons a blocked straggler; the cancel lands in the release of its lock, which must
    end the task (before, the release swallowed it and the task polled on, re-took the lock and fetched)."""
    two = [DatasetPlan("a", "TEM", 0.5, False), DatasetPlan("b", "TEM", 0.5, False)]
    gate = asyncio.Event()
    m = Mocks(
        plan=PlanResult(True, [], two, 1, min_fraction=0.5, levels=[1], patch=64),
        raw={"b": RawStatus(False, [U937_URL])},
        quotas={"b": [QuotaCheck(False, over_by=5), QuotaCheck(True)]},
        release_gates={"b": gate},
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")  # a servable; b blocked, inside its release
        await until(lambda: ("release_lock", ("raw", "b", "mia-buffers i7-e11 b")) in m.calls, m=m)
        await h.signal(MiaBuffer.go, ev("go", "15"))
        await idle(h, m, "plan-review")
        before = order(m, "check_quota", "take_lock", "submit_fetch", record="b")
        gate.set()
        await env.sleep(timedelta(minutes=10))
        await asyncio.sleep(0.5)
        state = await h.query(MiaBuffer.status)
    assert order(m, "check_quota", "take_lock", "submit_fetch", record="b") == before
    assert not m.of("submit_fetch") and state.round_id == "i7-e15"
    assert not [c for c in m.of("comment") if c[1] == "dataset i7-e11 b blocked"]  # it never went on to say so


async def test_cache_keeper_runs_one_keep_pass(env):
    from mia_buffers.models import KeeperReport
    from mia_buffers.workflows import MiaCacheKeeper

    passes = []

    @activity.defn(name="keep")
    async def keep(dry_run: bool = False) -> KeeperReport:
        passes.append(dry_run)
        return KeeperReport(scanned=2, evicted=["staged a"], over_quota={"raw": 5})

    tq = f"tq-{uuid.uuid4()}"
    async with Worker(env.client, task_queue=tq, workflows=[MiaCacheKeeper], activities=[keep]):
        report = await env.client.execute_workflow(MiaCacheKeeper.run, id=f"wf-{uuid.uuid4()}", task_queue=tq)
    assert report == KeeperReport(scanned=2, evicted=["staged a"], over_quota={"raw": 5}) and passes == [False]


# --- milestone D: auto-approve, the status board, the shared ticker, the ready-state loop -----------------------

TWO = PlanResult(
    True,
    [],
    [DatasetPlan("a", "TEM", 0.5, False), DatasetPlan("b", "TEM", 0.5, False)],
    1,
    min_fraction=0.5,
    levels=[1],
    patch=64,
)


def ticker_timers_pending(events) -> list[int]:
    """The ids of the poll timers (under an hour) started and neither fired nor cancelled."""
    started_ids = {
        e.event_id
        for e in events
        if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED
        and e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta() < timedelta(hours=1)
    }
    for e in events:
        if e.event_type == EventType.EVENT_TYPE_TIMER_FIRED:
            started_ids.discard(e.timer_fired_event_attributes.started_event_id)
        elif e.event_type == EventType.EVENT_TYPE_TIMER_CANCELED:
            started_ids.discard(e.timer_canceled_event_attributes.started_event_id)
    return sorted(started_ids)


async def test_auto_approve_skips_review(env):
    m = Mocks(plan=replace(ONLY_U937, auto_approve=True))
    async with started(env, m) as h:
        await idle(h, m, "ready")
    assert m.stages == ["planning", "sourcing", "ready"] and not m.of("refuse_label")
    (auto,) = [c for c in m.of("comment") if c[1] == "auto-approve i7-e11"]
    assert auto[0] == "plan.auto_approve: sourcing starts without review"


async def test_board_is_written_on_every_status_change(env):
    m = Mocks(plan=ONLY_U937, raw={"u937": RawStatus(False, [U937_URL])})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await until(lambda: len(m.board) == 4, m=m)
    assert m.board == [("u937", "pending"), ("u937", "fetching"), ("u937", "staging"), ("u937", "servable")]
    assert {args[0] for args in m.of("mark_dataset")} == {"i7-e11"}


async def test_idle_keep_and_retire(env):
    m = Mocks(plan=ONLY_U937)
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.idle)
        await idle(h, m, "idle")
        assert (await h.query(MiaBuffer.status)).idle_since
        await h.signal(MiaBuffer.idle)  # the keeper says it again: nothing changes
        await h.signal(MiaBuffer.keep, ev("keep", "31"))
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
        assert state.idle_since is None and state.stage == "ready"
        await h.signal(MiaBuffer.retire, ev("retire", "32"))
        await idle(h, m, "retired")
        state = await h.query(MiaBuffer.status)
    assert m.stages == ["planning", "plan-review", "sourcing", "ready", "idle", "ready", "retired"]
    assert m.of("retire_buffer") == [("i7-e11",)] and state.retired and state.stage == "retired"
    idle_detail = m.of("set_stage")[4][1]
    assert idle_detail.startswith("No consumer for 72 hours: idle.") and "14 days of idleness" in idle_detail
    assert ("kept", "keep i7-e11 31") in m.of("comment")
    assert m.of("set_stage")[-1][1] == "Retired: refs released; draws archived to /state/archive/i7-e11.draws.jsonl.gz"
    assert not m.of("refuse_label")


async def test_a_lifecycle_label_without_a_ready_buffer_is_refused(env):
    """`buffer:retire` on an issue whose workflow had finished starts a fresh run with that signal: refused."""
    m = Mocks()
    async with started(env, m, first=ev("retire", "21"), signal="retire") as h:
        await until(lambda: m.of("refuse_label"), m=m)
        await h.signal(MiaBuffer.keep, ev("keep", "22"))
        await until(lambda: len(m.of("refuse_label")) == 2, m=m)
        assert (await h.describe()).status.name == "RUNNING"
    assert m.of("refuse_label") == [
        ("buffer:retire", "this issue has no ready or idle buffer", "21"),
        ("buffer:keep", "this issue has no ready or idle buffer", "22"),
    ]
    assert not m.of("admit") and not m.of("retire_buffer")


async def test_degraded_rebuilds_from_the_raw_cache(env):
    """The zarr is gone but stage.json is still there, so the registry says the binding is staged: the rebuild must
    stage anyway (no reuse of the broken paths), with its own key, workspace and build dir (read_stage replaces)."""
    unstaged = BindingInfo(True, "/s/u937.yaml", "bh", ["zenodo.org"])
    m = Mocks(
        plan=ONLY_U937,
        bindings={"u937": [unstaged, unstaged, replace(unstaged, staged=True)]},  # sourcing reads it twice
        missing=[["u937"], ["u937"], []],  # asked again under the lock; the re-send comes after the rebuild
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e11"))
        await until(lambda: any(c[0] == "rebuilt u937" for c in m.of("comment")), m=m)
        await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e11"))
        await until(lambda: len(m.of("missing_records")) == 3, m=m)
        await asyncio.sleep(0.3)
        state = await h.query(MiaBuffer.status)
    assert len(m.of("read_binding")) == 4  # staged=True on both of the rebuild's reads, and staged anyway
    u937 = dataset(state, "u937")
    assert (u937.status, u937.rebuilds) == ("servable", 1) and state.stage == "ready"
    assert m.stages == ["planning", "plan-review", "sourcing", "ready"]
    assert m.of("raw_ready") == [("u937",)] and m.of("prepare_stage") == [("u937",), ("u937", 1)]
    assert [args[0] for args in m.of("submit")] == [job_key(7, "i7-e11", "u937"), job_key(7, "i7-e11", "u937", 1)]
    assert m.of("submit")[1][1].rebuild == 1 and m.of("read_stage") == [("u937",), ("u937", 1)]
    assert len(m.of("register_dataset")) == 2
    assert m.board[-4:] == [("u937", "servable"), ("u937", "pending"), ("u937", "staging"), ("u937", "servable")]


async def test_degraded_manual_dataset_rebuilds_without_a_new_drop(env):
    """The dropped files are in the raw cache (raw_ready): the rebuild stages from them, it asks no human again."""
    only = PlanResult(True, [], [DatasetPlan("bcss", "histology-WSI", 1.0, True)], 1, levels=[1], patch=64)
    m = Mocks(plan=only)
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop"), m=m)
        await h.signal(MiaBuffer.dropped, "bcss")
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.degraded, DegradedSignal(["bcss"], "pyramid missing: bcss/v0.zarr"))
        await until(lambda: any(c[0] == "rebuilt bcss" for c in m.of("comment")), m=m)
        state = await h.query(MiaBuffer.status)
    assert len(m.of("comment_drop")) == 1 and m.of("verify_drop") == [("bcss",)]
    assert dataset(state, "bcss").rebuilds == 1 and m.of("prepare_stage") == [("bcss",), ("bcss", 1)]


async def test_degraded_without_raw_is_needs_human_only_below_the_floor(env):
    m = Mocks(plan=TWO, raw_ready={"a": False, "b": False})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.degraded, DegradedSignal(["a"], "pyramid missing: a/v0.zarr"))
        await until(lambda: any("`a` failed" in c[0] for c in m.of("comment")), m=m)
        await h.signal(MiaBuffer.degraded, DegradedSignal(["a"], "pyramid missing: a/v0.zarr"))  # the next keeper pass
        await asyncio.sleep(0.5)
        state = await h.query(MiaBuffer.status)
        assert (
            dataset(state, "a").status == "failed"
            and dataset(state, "a").reason == "pyramid missing and raw cache gone"
        )
        assert state.stage == "ready" and m.of("raw_ready") == [("a",)]
        await h.signal(MiaBuffer.degraded, DegradedSignal(["a", "b"], "pyramid missing: a/v0.zarr, b/v0.zarr"))
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert m.of("raw_ready") == [("a",), ("b",)] and dataset(state, "b").status == "failed"
    detail = m.of("set_stage")[-1][1]
    assert "a, b" in detail and "TEM" in detail and not m.of("prepare_fetch")


async def test_waiters_share_one_ticker(env):
    """Three datasets wait for their raw locks: one timer per tick, doubling while no status changes; a change (w's
    drop staged meanwhile) resets it to poll_minutes."""
    pool = [DatasetPlan(record, "TEM", 0.2, False) for record in ("x", "y", "z")] + [DatasetPlan("w", "TEM", 0.4, True)]
    m = Mocks(plan=PlanResult(True, [], pool, 4, levels=[1], patch=64), locks={r: HELD for r in ("x", "y", "z")})
    async with started(env, m) as h:
        await approve(h, m)
        for record in ("x", "y", "z"):
            await waits_on_lock(h, record)
        await until(lambda: m.of("comment_drop"), m=m)
        for _ in range(40):  # quiet: the ticks back off until the 16-minute timer
            if (await poll_timers(h))[-1:] == [16]:
                break
            await env.sleep(timedelta(minutes=1))
        await h.signal(MiaBuffer.dropped, "w")
        for _ in range(400):
            if dataset(await h.query(MiaBuffer.status), "w").status == "servable":
                break
            await asyncio.sleep(0.05)
        await env.sleep(timedelta(minutes=17))  # the 16 fires; w changed status meanwhile
        for _ in range(200):  # until the timer after the 16 started
            if (timers := await poll_timers(h)).index(16) < len(timers) - 1:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.3)
        events = (await h.fetch_history()).events
    timers = [
        e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta() / timedelta(minutes=1)
        for e in events
        if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED
        and e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta() < timedelta(hours=1)
    ]
    # one leading 2 more when a status changed during the first tick (the four datasets settle in any order)
    assert timers[: timers.index(16) + 2] in ([2, 4, 8, 16, 2], [2, 2, 4, 8, 16, 2])
    assert len(ticker_timers_pending(events)) <= 1
    fired = len(timers) - len(ticker_timers_pending(events))
    takes = [args[1] for args in m.of("take_lock")]
    assert [takes.count(record) for record in ("x", "y", "z")] == [fired + 1] * 3  # each tries once per tick


async def test_ticker_stops_with_the_round(env):
    """b is blocked on quota (it waits on the ticker) when a go abandons it: the ticker is cancelled with the
    round and no further timers fire."""
    m = Mocks(plan=TWO, raw={"b": RawStatus(False, [U937_URL])}, quotas={"b": QuotaCheck(False, over_by=5)})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await waits_on_lock(h, "b", "blocked")
        await env.sleep(timedelta(minutes=3))
        await until(lambda: len(m.of("check_quota")) >= 2, m=m)
        await h.signal(MiaBuffer.go, ev("go", "15"))
        await idle(h, m, "plan-review")
        events = (await h.fetch_history()).events
        timers, checks = await poll_timers(h), len(m.of("check_quota"))
        await env.sleep(timedelta(hours=1))
        await asyncio.sleep(0.3)
        assert await poll_timers(h) == timers and len(m.of("check_quota")) == checks
    assert len(timers) >= 2 and ticker_timers_pending(events) == []


async def test_replay_buffer_history_d(env):
    """The run under `d-lifecycle` and `d-tick`: the plan auto-approves, u937 waits one tick for its raw lock, the
    buffer goes ready, idle, back to ready and is retired; as test_replay_buffer_history. Re-record this one alone:
    MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k "replay_buffer_history_d and not rebuild"
    """
    if os.environ.get("MIA_BUFFERS_RECORD_HISTORY"):
        m = Mocks(plan=replace(ONLY_U937, auto_approve=True), locks={"u937": [HELD, LockTaken(True)]})
        async with started(env, m) as h:
            await until(lambda: m.of("take_lock"), m=m)
            await waits_on_lock(h, "u937")
            await env.sleep(timedelta(minutes=3))
            await idle(h, m, "ready")
            await h.signal(MiaBuffer.idle, args=[None, "i7-e11"])  # as the keeper sends it
            await idle(h, m, "idle")
            await h.signal(MiaBuffer.keep, ev("keep", "31"))
            await idle(h, m, "ready")
            await h.signal(MiaBuffer.retire, ev("retire", "32"))
            await idle(h, m, "retired")
            LIFECYCLE_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("buffers-d-lifecycle", LIFECYCLE_HISTORY.read_text())
    await Replayer(workflows=[MiaBuffer]).replay_workflow(history)


async def test_a_keeper_signal_about_another_buffer_is_ignored(env):
    """After a go from the ready loop the earlier buffer stays registered and ready; the keeper's signals about it
    carry its id and must not idle or retire this round's buffer."""
    m = Mocks(plan=ONLY_U937)
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.idle, args=[None, "i7-e3"])
        await h.signal(MiaBuffer.retire, args=[None, "i7-e3"])
        await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e3"))
        await h.signal(MiaBuffer.idle, args=[None, "i7-e11"])  # about this round's buffer
        await idle(h, m, "idle")
    assert m.stages[-2:] == ["ready", "idle"] and not m.of("retire_buffer") and not m.of("missing_records")


async def test_retire_abandons_a_rebuild_that_waits(env):
    """A rebuild whose raw lock is held elsewhere waits on the ticker; a retire does not wait for it."""
    m = Mocks(plan=ONLY_U937, locks={"u937": [LockTaken(True), LockTaken(True), HELD]})  # sourcing takes it twice
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e11"))
        await waits_on_lock(h, "u937")
        await h.signal(MiaBuffer.retire, ev("retire", "32"))
        await idle(h, m, "retired")
        takes = len(m.of("take_lock"))
        await env.sleep(timedelta(hours=1))
        await asyncio.sleep(0.3)
        state = await h.query(MiaBuffer.status)
        events = (await h.fetch_history()).events
    assert m.of("retire_buffer") == [("i7-e11",)] and len(m.of("take_lock")) == takes
    assert dataset(state, "u937").reason == "abandoned: the buffer was retired" and ticker_timers_pending(events) == []


async def test_go_abandons_a_rebuild_that_waits(env):
    m = Mocks(plan=ONLY_U937, locks={"u937": [LockTaken(True), LockTaken(True), HELD]})  # sourcing takes it twice
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e11"))
        await waits_on_lock(h, "u937")
        await h.signal(MiaBuffer.go, ev("go", "15"))
        await idle(h, m, "plan-review")
        state = await h.query(MiaBuffer.status)
    assert state.round_id == "i7-e15" and m.stages[-3:] == ["ready", "planning", "plan-review"]


async def test_lifecycle_labels_outside_the_ready_loop_are_refused_at_once(env):
    """In plan-review (where a rollover would lose them) and while stragglers still source after ready."""
    m = Mocks(plan=ONE_GROUP)
    async with started(env, m) as h:
        await idle(h, m, "plan-review")
        await h.signal(MiaBuffer.retire, ev("retire", "21"))
        await until(lambda: m.of("refuse_label"), m=m)
        await approve(h, m)
        await idle(h, m, "ready")  # a servable, b waits for its drop
        await h.signal(MiaBuffer.keep, ev("keep", "22"))
        await until(lambda: len(m.of("refuse_label")) == 2, m=m)
    assert [(a[0], a[2]) for a in m.of("refuse_label")] == [("buffer:retire", "21"), ("buffer:keep", "22")]
    assert not m.of("retire_buffer")


# --- final fix pass ------------------------------------------------------------------------------


async def test_a_rebuild_another_buffer_finished_while_this_one_waited_is_reused(env):
    """Two buffers share the artifact: the other one holds the raw lock while rebuilding it. Once the lock is ours
    the record is no longer missing, so the pyramid is registered as it is: no second stage job."""
    m = Mocks(
        plan=ONLY_U937,
        locks={"u937": [LockTaken(True), LockTaken(True), HELD, LockTaken(True)]},  # sourcing takes it twice
        missing=[["u937"], []],  # missing when the keeper says so, back on disk once the lock is ours
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e11"))
        await waits_on_lock(h, "u937")
        await env.sleep(timedelta(minutes=3))
        await until(lambda: any(c[0] == "rebuilt u937" for c in m.of("comment")), m=m)
        state = await h.query(MiaBuffer.status)
    assert len(m.of("submit")) == 1 and m.of("prepare_stage") == [("u937",)] and not m.of("raw_status")[1:]
    assert m.of("read_stage") == [("u937",), ("u937", 1)] and len(m.of("register_dataset")) == 2
    servable = [c[0] for c in m.of("comment") if c[1] == "dataset i7-e11 u937 servable"]
    assert servable[-1].endswith("rebuilt by mia-buffers i9-e9 rec while this buffer waited and reused.")
    assert dataset(state, "u937").status == "servable" and state.stage == "ready"


async def test_the_keepers_retire_of_a_ready_buffer_is_ignored(env):
    """The keeper sent retire for the idle buffer, then a keep made it ready: the keeper's retire is stale. A
    `buffer:retire` label still retires a ready buffer."""
    m = Mocks(plan=ONLY_U937)
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.retire, args=[None, "i7-e11"])
        await h.signal(MiaBuffer.idle, args=[None, "i7-e11"])
        await idle(h, m, "idle")
        assert not m.of("retire_buffer")
        await h.signal(MiaBuffer.retire, args=[None, "i7-e11"])
        await idle(h, m, "retired")
    assert m.of("retire_buffer") == [("i7-e11",)] and m.stages[-3:] == ["ready", "idle", "retired"]


async def test_a_go_after_needs_human_retires_the_failed_rounds_buffer(env):
    """A round that ended needs-human leaves its buffer registered; the next go retires it before planning, with a
    comment, so an issue has one live buffer. A round that never created a buffer has nothing to retire."""
    m = Mocks(plan=ONLY_U937, stage={"u937": StageResult(False, 0, ["raw 0: voxel size unknown"])})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        assert (await h.query(MiaBuffer.status)).buffer_id == "i7-e11"
        await h.signal(MiaBuffer.go, ev("go", "15"))
        await idle(h, m, "plan-review")
        state = await h.query(MiaBuffer.status)
    assert m.of("retire_buffer") == [("i7-e11",)] and state.round_id == "i7-e15" and state.buffer_id is None
    assert ("failed round's buffer `i7-e11` retired: refs released", "retired i7-e11") in m.of("comment")
    assert m.stages[-3:] == ["needs-human", "planning", "plan-review"]


async def test_a_superseded_buffer_is_retired_by_the_keeper(env):
    """A go from the ready loop leaves the buffer registered: the keeper's idle about it writes the `idle` event on
    it (so the keeper's retire rule can fire), its retire releases it and says so; the new round is untouched."""
    m = Mocks(plan=ONLY_U937)
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await h.signal(MiaBuffer.go, ev("go", "15"))
        await idle(h, m, "plan-review")
        assert (await h.query(MiaBuffer.status)).superseded == ["i7-e11"]
        await h.signal(MiaBuffer.idle, args=[None, "i7-e11"])
        await until(lambda: m.of("buffer_event"), m=m)
        assert m.of("buffer_event") == [("i7-e11", "idle", "superseded")] and not m.of("retire_buffer")
        await h.signal(MiaBuffer.retire, args=[None, "i7-e11"])  # retire_after_days later
        await until(lambda: m.of("retire_buffer"), m=m)
        await h.signal(MiaBuffer.retire, args=[None, "i7-e11"])  # the next keeper pass, before it saw the event
        await until(lambda: any(c[1] == "retired i7-e11" for c in m.of("comment")), m=m)
        await asyncio.sleep(0.3)
        state = await h.query(MiaBuffer.status)
    assert m.of("retire_buffer") == [("i7-e11",)] and state.superseded == [] and state.round_id == "i7-e15"
    assert ("superseded buffer `i7-e11` retired: refs released", "retired i7-e11") in m.of("comment")
    assert m.stages[-2:] == ["planning", "plan-review"] and not m.of("refuse_label")


async def test_a_lifecycle_label_while_sourcing_is_refused_at_once(env):
    m = Mocks(plan=ONLY_U937, gate=asyncio.Event(), gate_on="stage-u937")
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("await"), m=m)  # the stage job runs: sourcing, not ready
        await h.signal(MiaBuffer.keep, ev("keep", "31"))
        await until(lambda: m.of("refuse_label"), m=m)
        m.gate.set()
        await idle(h, m, "ready")
    assert m.of("refuse_label") == [
        ("buffer:keep", "1 datasets still sourcing; add `buffer:keep` again once they settle", "31")
    ]


async def test_replay_buffer_history_d_rebuild(env):
    """The degraded path under `d-lifecycle` and `d-tick`: ready, the keeper finds u937's pyramid gone, it is still
    missing when asked again under the raw lock, so it is staged again from the raw cache with a job of its own and
    the buffer stays ready; as test_replay_buffer_history. Re-record this one alone:
    MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay_buffer_history_d_rebuild"""
    if os.environ.get("MIA_BUFFERS_RECORD_HISTORY"):
        m = Mocks(plan=ONLY_U937, missing=[["u937"]])
        async with started(env, m) as h:
            await approve(h, m)
            await idle(h, m, "ready")
            await h.signal(MiaBuffer.degraded, DegradedSignal(["u937"], "pyramid missing: u937/v0.zarr", "i7-e11"))
            await until(lambda: any(c[0] == "rebuilt u937" for c in m.of("comment")), m=m)
            await idle(h, m, "ready")
            assert len(m.of("submit")) == 2 and len(m.of("missing_records")) == 2
            REBUILD_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("buffers-d-rebuild", REBUILD_HISTORY.read_text())
    await Replayer(workflows=[MiaBuffer]).replay_workflow(history)


# --- milestone E: preparation requests, and a buffer no longer waits on mia-data ------------------

PREP_HISTORY = HISTORY.with_name("prep_history.json")
PREP_E_HISTORY = HISTORY.with_name("buffer_history_e.json")
NOT_PREPARED = "not prepared: request preparation with a prep:go issue"


class PrepMocks:
    """The activities MiaPrep calls, recorded; `statuses` gives each record's prep_status answers in turn (the last
    repeats; default `unprepared`)."""

    def __init__(
        self,
        records=("a", "b", "c"),
        outcomes=None,
        statuses=None,
        admit_error="",
        status_error="",
        days=1,
        nudge_errors=None,
        comment_error="",
        slow=(),
    ):
        self.slow, self.gate = set(slow), asyncio.Event()  # records whose nudge waits for the gate
        self.calls: list[tuple[str, tuple]] = []
        self.days = days  # the request's timeout_days
        self.records, self.outcomes, self.statuses = list(records), outcomes or {}, statuses or {}
        self.admit_error, self.status_error = admit_error, status_error
        self.nudge_errors, self.comment_error = nudge_errors or {}, comment_error  # record -> its nudge's error

    def acts(self):
        rec = lambda name, *a: self.calls.append((name, a))  # noqa: E731

        @activity.defn(name="prep_admit")
        async def prep_admit(issue: int, event_id: str) -> PrepPins:
            rec("prep_admit", event_id)
            if self.admit_error:
                raise ApplicationError(self.admit_error, non_retryable=True)
            return PrepPins(f"p{issue}-e{event_id}", "c" * 40, self.records, self.days, issue)

        @activity.defn(name="limits")
        async def limits() -> SourcingLimits:
            return SourcingLimits(2, 120, "http://svc:8765")

        @activity.defn(name="set_prep_stage")
        async def set_prep_stage(issue: int, stage: str, state: PrepState | None = None, detail: str = "") -> None:
            rec("set_prep_stage", stage, detail)

        @activity.defn(name="prep_nudge")
        async def prep_nudge(round_id: str, record: str) -> PrepOutcome:
            rec("prep_nudge", round_id, record)
            if record in self.slow:
                await self.gate.wait()
            if record in self.nudge_errors:
                raise ApplicationError(self.nudge_errors[record], non_retryable=True)
            return self.outcomes.get(record, PrepOutcome(record, "nudged", 5))

        @activity.defn(name="comment_prep")
        async def comment_prep(issue: int, round_id: str, outcomes: list[PrepOutcome]) -> None:
            rec("comment_prep", round_id, outcomes)
            if self.comment_error:
                raise ApplicationError(self.comment_error, non_retryable=True)

        @activity.defn(name="prep_status")
        async def prep_status(records: list[str]) -> dict[str, str]:
            rec("prep_status", list(records))
            if self.status_error:
                raise ApplicationError(self.status_error, non_retryable=True)
            return {record: turn(self.statuses, record, "unprepared") for record in records}

        @activity.defn(name="refuse_label")
        async def refuse_label(issue: int, label: str, reason: str, event_id: str) -> None:
            rec("refuse_label", label, reason, event_id)

        return [prep_admit, limits, set_prep_stage, prep_nudge, comment_prep, prep_status, refuse_label]

    def of(self, name):
        return [c[1] for c in self.calls if c[0] == name]

    @property
    def stages(self):
        return [a[0] for a in self.of("set_prep_stage")]


@contextlib.asynccontextmanager
async def prep_started(env, m):
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(env.client, task_queue=tq, workflows=[workflows.MiaPrep], activities=m.acts()):
        h = await env.client.start_workflow(
            workflows.MiaPrep.run,
            PrepState(8),
            id=f"wf-{uuid.uuid4()}",
            task_queue=tq,
            start_signal="go",
            start_signal_args=[ev("go", "21")],
        )
        try:
            yield h
        finally:
            with contextlib.suppress(RPCError):  # a test may have cancelled it already
                await h.terminate()


async def prep_settled(h, m, stage):
    """Wait until set_prep_stage(stage) ran and the workflow is blocked again."""
    await until(lambda: m.stages[-1:] == [stage], m=m)
    for _ in range(500):
        if (await h.fetch_history()).events[-1].event_type in IDLE_EVENTS:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("workflow never went idle")


async def polled(env, m, count):
    """Skip time until prep_status ran `count` times (the poll's timer may start after a skip, so skip again)."""
    for _ in range(50):
        if len(m.of("prep_status")) >= count:
            return
        await env.sleep(timedelta(minutes=5))
        await asyncio.sleep(0.05)
    raise AssertionError(f"prep_status never ran {count} times; calls so far: {m.calls}")


def prep_round():
    """a opened, b already prepared, c in progress; a and c turn prepared on the second poll."""
    return PrepMocks(
        outcomes={
            "a": PrepOutcome("a", "opened", 4),
            "b": PrepOutcome("b", "prepared"),
            "c": PrepOutcome("c", "in-progress", 6),
        },
        statuses={"a": ["unprepared", "prepared"], "c": ["in-progress", "prepared"]},
    )


async def test_prep_round_opens_nudges_and_closes_done(env):
    m = prep_round()
    async with prep_started(env, m) as h:
        await until(lambda: m.of("comment_prep"), m=m)
        await h.signal(workflows.MiaPrep.go, ev("go", "22"))  # a round is running: busy
        await h.signal(workflows.MiaPrep.go, ev("go", "21"))  # a redelivery of the round's own go: ignored
        await polled(env, m, 2)
        await prep_settled(h, m, "done")
        state = await h.query(workflows.MiaPrep.status)
    assert m.of("prep_admit") == [("21",)] and m.stages == ["requested", "done"]
    assert [a[1] for a in m.of("prep_nudge")] == ["a", "b", "c"] and m.of("prep_nudge")[0][0] == "p8-e21"
    ((round_id, outcomes),) = m.of("comment_prep")
    assert round_id == "p8-e21" and [o.outcome for o in outcomes] == ["opened", "prepared", "in-progress"]
    assert m.of("prep_status") == [(["a", "c"],), (["a", "c"],)]  # b settled at the nudge
    assert "`a`" in m.of("set_prep_stage")[-1][1] and "`b`" in m.of("set_prep_stage")[-1][1]
    assert m.of("refuse_label") == [("prep:go", workflows.BUSY, "22")]
    assert state.settled == {"a": "prepared", "b": "prepared", "c": "prepared"} and state.stage == "done"


async def test_prep_needs_human_on_timeout(env):
    m = PrepMocks(
        outcomes={"c": PrepOutcome("c", "needs-human", 6)},
        statuses={"a": "prepared", "b": "in-progress"},
        days=SHORT_DAYS,
    )  # b never settles: still preparing at the day limit
    async with prep_started(env, m) as h:
        await prep_settled(h, m, "needs-human")
        state = await h.query(workflows.MiaPrep.status)
    detail = m.of("set_prep_stage")[-1][1]
    assert m.stages == ["requested", "needs-human"] and "`b`" in detail and "`c`" in detail and "`a`" not in detail
    assert "in-progress" in detail and state.settled == {"a": "prepared", "c": "needs-human"}


async def test_prep_registry_outage_ends_needs_human(env):
    m = PrepMocks(status_error="RegistryError: buffer-api unreachable")
    async with prep_started(env, m) as h:
        await polled(env, m, 1)
        await prep_settled(h, m, "needs-human")
    assert m.stages == ["requested", "needs-human"] and "buffer-api unreachable" in m.of("set_prep_stage")[-1][1]


async def test_prep_admit_error_is_needs_human(env):
    m = PrepMocks(admit_error="ValueError: records not in the catalog at cccc: nope")
    async with prep_started(env, m) as h:
        await prep_settled(h, m, "needs-human")
    assert m.stages == ["needs-human"] and "records not in the catalog" in m.of("set_prep_stage")[0][1]
    assert not m.of("prep_nudge") and not m.of("comment_prep")


async def test_prep_a_failed_nudge_settles_that_record_alone(env):
    """b's nudge fails and c is done but unpublished: both need a human, while a is still followed to prepared."""
    m = PrepMocks(
        outcomes={"c": PrepOutcome("c", "done-unpublished", 6, "no binding.yaml")},
        statuses={"a": "prepared"},
        nudge_errors={"b": "ValueError: record b: unclear modality"},
    )
    async with prep_started(env, m) as h:
        await polled(env, m, 1)
        await prep_settled(h, m, "needs-human")
        state = await h.query(workflows.MiaPrep.status)
    ((_, outcomes),) = m.of("comment_prep")
    assert [(o.record, o.outcome) for o in outcomes] == [("a", "nudged"), ("b", "unknown"), ("c", "done-unpublished")]
    assert "unclear modality" in outcomes[1].note and m.of("prep_status") == [(["a"],)]
    detail = m.of("set_prep_stage")[-1][1]
    assert "`b` (unknown: ValueError: record b: unclear modality)" in detail and "`c` (done-unpublished" in detail
    assert "`a`" not in detail and state.settled == {"a": "prepared", "b": "needs-human", "c": "needs-human"}


async def test_prep_cancel_stops_opening_issues(env):
    """A cancel during the nudges ends the workflow cancelled: no further record is nudged, no stage is set."""
    m = PrepMocks(slow={"b"})
    # its own server: a cancelled workflow leaves the shared time-skipping server unable to skip time afterwards
    async with await WorkflowEnvironment.start_time_skipping() as own, prep_started(own, m) as h:
        await until(lambda: len(m.of("prep_nudge")) == 2, m=m)
        await h.cancel()
        with pytest.raises(WorkflowFailureError) as error:
            await h.result()
        m.gate.set()  # the nudge in flight completes against the finished workflow
    assert isinstance(error.value.cause, CancelledError)
    assert [a[1] for a in m.of("prep_nudge")] == ["a", "b"] and m.stages == ["requested"] and not m.of("comment_prep")


async def test_prep_a_failed_outcomes_comment_keeps_polling(env):
    m = PrepMocks(records=["a"], statuses={"a": "prepared"}, comment_error="GitHubError: 502")
    async with prep_started(env, m) as h:
        await polled(env, m, 1)
        await prep_settled(h, m, "done")
        state = await h.query(workflows.MiaPrep.status)
    assert m.stages == ["requested", "done"] and [o.outcome for o in state.outcomes] == ["nudged"]


async def test_buffer_fails_an_unprepared_record_instead_of_waiting(env):
    """e-prep: a record without a binding fails at once; mia-data is not nudged and nothing waits on it."""
    m = Mocks(plan=ONE_GROUP, unbound={"b"})  # a alone is half the group: ready
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "b").status == "failed" and dataset(state, "b").reason == NOT_PREPARED
    assert not m.of("nudge_dataset") and not m.of("comment_drop") and state.ready
    assert any(a[0] == f"`b` failed: {NOT_PREPARED}" for a in m.of("comment"))


async def test_replay_prep_history(env):
    """The recorded prep:go → requested → polls → done run must replay on today's MiaPrep. Re-record this one alone:
    MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay_prep_history"""
    if os.environ.get("MIA_BUFFERS_RECORD_HISTORY"):
        m = prep_round()
        async with prep_started(env, m) as h:
            await polled(env, m, 2)
            await prep_settled(h, m, "done")
            PREP_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("prep-recorded", PREP_HISTORY.read_text())
    await Replayer(workflows=[workflows.MiaPrep]).replay_workflow(history)


async def test_replay_buffer_history_e(env):
    """The run under `e-prep`: b has no binding and fails at once, a alone makes the buffer ready; as
    test_replay_buffer_history. Re-record this one alone:
    MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay_buffer_history_e"""
    if os.environ.get("MIA_BUFFERS_RECORD_HISTORY"):
        m = Mocks(plan=ONE_GROUP, unbound={"b"})
        async with started(env, m) as h:
            await approve(h, m)
            await idle(h, m, "ready")
            PREP_E_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("buffers-e-prep", PREP_E_HISTORY.read_text())
    await Replayer(workflows=[MiaBuffer]).replay_workflow(history)
