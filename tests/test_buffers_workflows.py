"""MiaBuffer on Temporal's time-skipping server with every activity mocked, the poller, and a replay test."""

import asyncio
import contextlib
import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
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
    DropResult,
    FetchJobArgs,
    FetchResult,
    PlanResult,
    QuotaCheck,
    RawStatus,
    RegisterResult,
    SourcingLimits,
    StageJobArgs,
    StageResult,
    fetch_job_key,
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
        async def prepare_stage(issue: int, round_id: str, record: str) -> str:
            rec("prepare_stage", record)
            return f"/w/{round_id}/stage-{record}"

        @activity.defn(name="submit")
        async def submit(key: str, args: StageJobArgs) -> str:
            rec("submit", key, args)
            return f"job-{key}"

        @activity.defn(name="read_stage")
        async def read_stage(round_id: str, record: str) -> StageResult:
            rec("read_stage", record)
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

        @activity.defn(name="raw_status")
        async def raw_status(round_id: str, record: str) -> RawStatus:
            rec("raw_status", record)
            return self.raw.get(record, RawStatus(True))

        @activity.defn(name="check_quota")
        async def check_quota(round_id: str, record: str) -> QuotaCheck:
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

        return [take_lock, release_lock, raw_status, check_quota, prepare_fetch, submit_fetch, read_fetch, mark_asset]

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
        if (await h.fetch_history()).events[-1].event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("workflow never went idle")


@contextlib.asynccontextmanager
async def started(env, m, first=None, runner=None):
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
            start_signal="go",
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
    assert "no weighting group reached" in m.of("set_stage")[-1][1] and "u937" in m.of("set_stage")[-1][1]


async def test_job_exit_failure_fails_dataset(env):
    only = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
    m = Mocks(plan=only, states={"stage-u937": ("EXIT", 1)})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "failed" and "exit" in dataset(state, "u937").reason.lower()
    assert not m.of("read_stage")


async def test_binding_still_missing_after_update_fails_dataset(env):
    """A `binding` update whose file then cannot be read (gone again, or naming another record) fails the dataset
    rather than waiting forever; a plain missing binding goes to mia-data instead (milestone 4 tests below)."""
    only = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
    m = Mocks(plan=only, unbound={"u937"})
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("nudge_dataset"), m=m)
        await h.signal(MiaBuffer.dataset_update, args=["u937", "binding"])  # the mock still has no binding
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "failed" and "not data:done" in dataset(state, "u937").reason
    assert [a[0] for a in m.of("read_binding")] == ["u937", "u937"] and not m.of("prepare_stage")
    assert len([a for a in m.of("comment") if "u937" in a[0] and "failed" in a[0]]) == 1


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


async def test_timeout_without_ready_is_needs_human(env):
    only = PlanResult(
        True, [], [DatasetPlan("bcss", "histology-WSI", 1.0, True)], 1, levels=[1], patch=64, timeout_days=14
    )
    m = Mocks(plan=only)
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("comment_drop"))
        await env.sleep(timedelta(days=15))  # the test server skips time only when asked
        await idle(h, m, "needs-human")
    detail = m.of("set_stage")[-1][1]
    assert "14" in detail and "histology-WSI" in detail and not m.of("verify_drop")


async def test_timeout_after_ready_changes_nothing(env):
    m = Mocks(plan=ONE_GROUP)  # a and b share one group with min_fraction 0.5: a alone makes the buffer ready
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        await env.sleep(timedelta(days=15))  # b is still waiting for its drop when the timer fires
        await asyncio.sleep(0.5)
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
        return [i for i in self.issues.values() if not labels or labels in {x["name"] for x in i["labels"]}]

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
        self.states, self.started, self.signals = states, [], []

    async def start_workflow(self, wf, arg, **kw):
        self.started.append((wf, arg, kw))

    def get_workflow_handle(self, wid):
        client = self

        class Handle:
            async def signal(self, name, arg=None, *, args=None):
                client.signals.append((wid, name, tuple(args) if args is not None else arg))

            async def query(self, name, result_type=None):
                return client.states[wid]

        return Handle()


def issue(n, *labels):
    return {"number": n, "body": "", "labels": [{"name": x} for x in labels]}


async def test_poll_delivers_labels_and_drops(tmp_path):
    cfg = make_cfg(tmp_path)
    at = "2026-10-24T10:00:00Z"
    gh = PollGH(
        issues={1: issue(1, "buffer:new", "buffer:go"), 2: issue(2, "buffer:sourcing"), 3: issue(3, "buffer:ready")},
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
    client = FakeClient({cfg.workflow_id(2): waiting, cfg.workflow_id(3): ready})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [])
    (cfg.paths.raw_root / "bcss" / "incoming").mkdir(parents=True)
    (cfg.paths.raw_root / "bcss" / "incoming" / "a.zip").write_bytes(b"z")
    (cfg.paths.raw_root / "x" / "incoming").mkdir(parents=True)  # empty: nothing to signal
    assert await run_poll(acts) == ["go #1"]  # the drop is not signalled on first sight (a copy may be running)
    assert await run_poll(acts) == ["dropped #2 bcss"]
    ((wf, state, kw),) = client.started
    assert wf == "MiaBuffer" and state == BufferState(1) and kw["start_signal"] == "go"
    assert kw["start_signal_args"] == [LabelEvent("go", "501", "alice", at)] and kw["id"] == cfg.workflow_id(1)
    assert gh.removed == [(1, "buffer:go")] and client.signals == [(cfg.workflow_id(2), "dropped", "bcss")]
    assert await run_poll(acts) == []  # nothing new: the drop was signalled once
    (cfg.paths.raw_root / "bcss" / "incoming" / "b.zip").write_bytes(b"z")
    assert await run_poll(acts) == []  # a new file: seen, not yet stable
    assert await run_poll(acts) == ["dropped #2 bcss"]  # once more


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


async def test_unmatched_dataset_waits_for_binding_then_stages(env):
    m = Mocks(unbound={"bcss"})
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("nudge_dataset") and m.of("register_dataset"), m=m)
        await until(lambda: any("data:go" in a[0] for a in m.of("comment")), m=m)
        state = await h.query(MiaBuffer.status)
        bcss = dataset(state, "bcss")
        assert (bcss.status, bcss.issue, bcss.folder) == ("waiting-dataset", 5, "lm-bcss")
        assert dataset(state, "u937").status == "servable" and m.stages[-1] == "sourcing"
        m.bound.add("bcss")
        await h.signal(MiaBuffer.dataset_update, args=["bcss", "binding"])
        await until(lambda: m.of("comment_drop"), m=m)  # bcss is manual: the binding found, the drop path follows
        await h.signal(MiaBuffer.dropped, "bcss")
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    # unbound, after the update, and again under the raw-cache lock (c-cache)
    assert m.of("nudge_dataset") == [("bcss",)] and [a[0] for a in m.of("read_binding")].count("bcss") == 3
    assert dataset(state, "bcss").status == "servable" and state.ready


async def test_ineligible_dataset_is_dropped_and_plan_reposted(env):
    m = Mocks(unbound={"bcss"}, nudges={"bcss": DatasetIssue(5, "ineligible", False, folder="lm-bcss")})
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")  # u937 alone: bcss's group is gone from the denominator
        state = await h.query(MiaBuffer.status)
    bcss = dataset(state, "bcss")
    assert bcss.status == "dropped" and "ineligible" in bcss.reason and "#5" in bcss.reason
    assert [a[1] for a in m.of("comment_plan")] == [None, {"bcss": bcss.reason}]
    assert any("dropped" in a[0] and "bcss" in a[0] for a in m.of("comment"))
    assert not m.of("comment_drop") and not m.of("verify_drop") and state.ready


async def test_late_dataset_update_is_ignored(env):
    only = PlanResult(True, [], [DatasetPlan("u937", "confocal", 1.0, False)], 1, levels=[1], patch=64)
    m = Mocks(plan=only)
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "ready")
        calls = len(m.calls)
        await h.signal(MiaBuffer.dataset_update, args=["u937", "binding"])
        await h.signal(MiaBuffer.dataset_update, args=["nobody", "ineligible"])
        await asyncio.sleep(0.5)
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").status == "servable" and len(m.calls) == calls and state.ready


async def test_timeout_drops_waiting_datasets_and_may_reach_ready(env):
    m = Mocks(unbound={"bcss"})  # u937 becomes servable, bcss waits for mia-data forever
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("register_dataset") and m.of("nudge_dataset"), m=m)
        await env.sleep(timedelta(days=15))
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    bcss = dataset(state, "bcss")
    assert bcss.status == "dropped" and "14" in bcss.reason and "waiting-dataset" in bcss.reason
    assert m.of("comment_plan")[-1][1] == {"bcss": bcss.reason} and "needs-human" not in m.stages
    assert state.ready and dataset(state, "u937").status == "servable"


async def test_poll_signals_dataset_updates(tmp_path):
    cfg = make_cfg(tmp_path)
    gh = PollGH(issues={2: issue(2, "buffer:sourcing")})
    datasets = PollGH(issues={4: issue(4, "data:testing"), 5: issue(5, "data:ineligible", "licence-excluded")})
    waiting = BufferState(2, round_id="i2-e9", stage="sourcing")
    waiting.datasets = [
        workflows.DatasetStatus("u937", "confocal", False, "waiting-dataset", issue=4, folder="lm-u937"),
        workflows.DatasetStatus("bcss", "histology-WSI", True, "waiting-dataset", issue=5, folder="lm-bcss"),
        workflows.DatasetStatus("other", "TEM", False, "servable", issue=6, folder="lm-other"),
    ]
    client = FakeClient({cfg.workflow_id(2): waiting})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [], gh_datasets=datasets)
    assert await run_poll(acts) == ["dataset #2 bcss ineligible"]  # u937's issue is still in progress
    (cfg.paths.storage_root / "lm-u937").mkdir(parents=True)
    (cfg.paths.storage_root / "lm-u937" / "binding.yaml").write_text("record: u937\n")
    assert await run_poll(acts) == []  # a bare binding.yaml is not prepared (spec §4.2)
    reindex(cfg.paths.storage_root / "lm-u937", {})  # mia-data's publish writes the manifest for that binding
    assert await run_poll(acts) == ["dataset #2 u937 binding"]
    assert client.signals == [
        (cfg.workflow_id(2), "dataset_update", ("bcss", "ineligible")),
        (cfg.workflow_id(2), "dataset_update", ("u937", "binding")),
    ]
    assert await run_poll(acts) == []  # each update is signalled once


async def test_poll_checks_ineligible_issues_with_list_calls_not_one_get_per_dataset(tmp_path):
    """Review (milestone 4): a 400-dataset pool polled every 2 minutes must not spend one GitHub request per
    dataset per pass; the ineligible issues come from labelled list calls, at most two per pass."""

    class CountingGH(PollGH):
        def __init__(self, issues):
            super().__init__(issues)
            self.lists, self.gets = [], []

        async def list_issues(self, labels=None, state=None, **_):
            self.lists.append(labels)
            return await super().list_issues(labels=labels, state=state)

        async def get_issue(self, n):
            self.gets.append(n)
            return await super().get_issue(n)

    cfg = make_cfg(tmp_path)
    gh = PollGH(issues={2: issue(2, "buffer:sourcing")})
    datasets = CountingGH({n: issue(n, "data:testing") for n in range(10, 40)} | {40: issue(40, "data:ineligible")})
    waiting = BufferState(2, round_id="i2-e9", stage="sourcing")
    waiting.datasets = [
        workflows.DatasetStatus(f"r{n}", "g", False, "waiting-dataset", issue=n, folder=f"lm-r{n}")
        for n in range(10, 41)
    ]
    client = FakeClient({cfg.workflow_id(2): waiting})
    acts = Activities(cfg, gh, None, None, None, bot="bot", client=client, probe=lambda: [], gh_datasets=datasets)
    assert await run_poll(acts) == ["dataset #2 r40 ineligible"]
    assert datasets.gets == [] and len(datasets.lists) <= 2


async def test_timeout_lets_a_staging_job_finish_then_announces_ready(env):
    """Review Focus 5: at the timeout the waiters are dropped, a stage job still running finishes, and ready
    follows from the groups that remain."""
    # a real-time timeout of ~5 s: the test server cannot skip time while await_job is blocked on the gate
    short = PlanResult(True, [], PLAN.pool, 10, levels=[1, 2, 4], patch=256, timeout_days=6e-5)
    m = Mocks(plan=short, unbound={"bcss"}, gate=asyncio.Event(), gate_on="stage-u937")
    async with started(env, m) as h:
        await approve(h, m)
        await until(lambda: m.of("nudge_dataset") and m.of("await"), m=m)  # bcss waits on mia-data, u937's job runs
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


def order(m, *names):
    """The calls among `names`, in the order they ran."""
    return [name for name in m.names() if name in names]


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
        await env.sleep(timedelta(minutes=3))
        await idle(h, m, "ready")
        state = await h.query(MiaBuffer.status)
    assert m.of("take_lock") == [("raw", "u937", OWNER, 900)] * 3
    assert not m.of("submit_fetch") and not m.of("submit") and not m.of("prepare_stage")
    assert order(m, "take_lock", "read_binding", "register_dataset", "release_lock")[-4:] == [
        "take_lock",
        "read_binding",
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
        "take_lock",
        "take_lock",  # re-taken once the fetch slot is free
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
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        state = await h.query(MiaBuffer.status)
    assert dataset(state, "u937").reason == "fetch still partial after 4 attempts" and not m.of("submit")
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
        quotas={"u937": [QuotaCheck(False, over_by=5), QuotaCheck(True)]},
    )
    async with started(env, m) as h:
        await approve(h, m)
        blocked = await waits_on_lock(h, "u937", "blocked")
        assert "over by 5 bytes" in blocked.reason and not m.of("submit_fetch")
        await env.sleep(timedelta(minutes=3))
        await idle(h, m, "ready")
    assert len([c for c in m.of("comment") if c[1] == "dataset i7-e11 u937 blocked"]) == 1
    assert len(m.of("check_quota")) == 2 and len(m.of("submit_fetch")) == 1
    assert order(m, "check_quota", "take_lock", "submit_fetch", "release_lock") == [
        "take_lock",
        "take_lock",  # once the fetch slot is free
        "check_quota",
        "release_lock",  # released while blocked (spec §5.2)
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
    assert m.of("read_binding")[0] == ("heavy",)
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
    """Review fix: with one fetch slot, `second` waits for it past the timeout; when it then finds the quota over
    it is dropped rather than blocked forever, and the round ends. A blocked dataset holds no lock while it sleeps."""
    short = PlanResult(
        True,
        [],
        [DatasetPlan("first", "TEM", 0.6, False), DatasetPlan("second", "TEM", 0.4, False)],
        1,
        levels=[1],
        patch=64,
        timeout_days=6e-5,  # ~5 s of real time, as test_timeout_lets_a_staging_job_finish_then_announces_ready
    )
    over = QuotaCheck(False, over_by=5)
    m = Mocks(
        plan=short,
        raw={record: RawStatus(False, [U937_URL]) for record in ("first", "second")},
        quotas={"first": over, "second": over},
        limits=SourcingLimits(2, 120, "http://svc:8765", parallel_fetch=1),
    )
    async with started(env, m) as h:
        await approve(h, m)
        await idle(h, m, "needs-human")
        await until(lambda: len(m.of("release_lock")) == 2, m=m)
        state = await h.query(MiaBuffer.status)
        history = await h.fetch_history()
    assert {d.record: (d.status, d.reason) for d in state.datasets} == {
        "first": ("dropped", "still blocked after 6e-05 days"),
        "second": ("dropped", "still blocked after 6e-05 days"),
    }
    assert sorted(m.of("release_lock")) == [("raw", r, f"mia-buffers i7-e11 {r}") for r in ("first", "second")]
    assert not m.of("submit_fetch") and "below ready.min_fraction" in m.of("set_stage")[-1][1]
    # first: check_quota → release_lock → its poll timer (the round's day-limit timer started before any of it)
    names = []
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            names.append(event.activity_task_scheduled_event_attributes.activity_type.name)
        elif event.event_type == EventType.EVENT_TYPE_TIMER_STARTED:
            names.append("timer")
    after = names[names.index("check_quota") :]
    assert [n for n in after if n in ("check_quota", "release_lock", "timer")][:3] == [
        "check_quota",
        "release_lock",
        "timer",
    ]


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
