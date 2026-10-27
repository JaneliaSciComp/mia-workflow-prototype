"""MiaDataDataset on Temporal's time-skipping server with every activity mocked, and the poll activity."""

import asyncio
import contextlib
import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from mia_helpers import FakeGH, make_cfg
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from mia_data import workflows
from mia_data.activities import Activities
from mia_data.models import (
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
    VerifyResult,
)
from mia_data.workflows import MiaDataDataset

SHA = "a" * 40
PINS = Pins("rec-1", "c" * 40, SHA, "h" * 40, "sha256:x")


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
        outcome="matched",
        smoke=None,
        states=None,
        test_ok=16,
        set_ok=16,
        n_effective=16,
        await_failures=0,
        gate=None,
        gate_on="",
        test_ok_by=None,
        pr_now=None,
        trees=None,
        revisions=5,
        lock_busy=0,
        slow_stages=(),
    ):
        self.slow_stages, self.stage_gate = set(slow_stages), asyncio.Event()  # set_stage waits for the gate there
        self.calls: list[tuple[str, tuple]] = []
        self.outcome, self.smoke, self.states = outcome, smoke or SmokeResult(True, True), states or {}
        self.test_ok, self.set_ok, self.n_effective = test_ok, set_ok, n_effective
        self.crop_jobs: dict[str, JobArgs] = {}  # crop workspace -> its args (trusted = the dataset's set)
        self.await_failures, self.gate, self.gate_on = await_failures, gate, gate_on
        self.test_ok_by, self.pr_now, self.trees, self.revisions = test_ok_by or {}, pr_now, trees or {}, revisions
        self.lock_busy = lock_busy  # how many take_lock calls find the raw cache held by someone else

    def acts(self):
        rec = lambda name, *a: self.calls.append((name, a))  # noqa: E731

        @activity.defn(name="admit")
        async def admit(issue: int, event_id: str) -> Pins:
            rec("admit", event_id)
            return PINS

        @activity.defn(name="limits")
        async def limits() -> RoundLimits:
            rec("limits")
            return RoundLimits(4, 16, 120, self.revisions)

        @activity.defn(name="set_stage")
        async def set_stage(
            issue: int, stage: str, state: DatasetState | None = None, detail: str = "", extra: str = ""
        ) -> None:
            if stage in self.slow_stages:
                await self.stage_gate.wait()
            rec("set_stage", stage, detail, extra)

        @activity.defn(name="comment")
        async def comment(
            issue: int,
            body: str,
            marker: str,
            repo: str = "data",
            binding_path: str | None = None,
            binding_hash: str | None = None,
        ) -> str | None:
            rec("comment", body, marker, repo, binding_path)
            return f"https://github.com/o/{repo}/issues/{issue}#{marker}" if repo == "snippets" else None

        @activity.defn(name="refuse_label")
        async def refuse_label(issue: int, label: str, reason: str, event_id: str) -> None:
            rec("refuse_label", label, reason, event_id)

        @activity.defn(name="prepare_match")
        async def prepare_match(issue: int, round_id: str, pins: Pins, attempt: int = 1) -> str:
            rec("prepare_match", round_id)
            return f"/w/{round_id}/match-{attempt}"

        @activity.defn(name="prepare_job")
        async def prepare_job(issue: int, round_id: str, kind: str, attempt: int, binding: str | None) -> str:
            rec("prepare_job", kind, binding)
            return f"/w/{round_id}/{kind}-{attempt}"

        @activity.defn(name="submit")
        async def submit(key: str, kind: str, a: JobArgs) -> str:
            rec("submit", key, kind, a)
            if kind == "crop":
                self.crop_jobs[a.workspace] = a
            return f"job-{key}"

        @activity.defn(name="read_match")
        async def read_match(ws: str) -> MatchResult:
            rec("read_match")
            return MatchResult(
                self.outcome,
                reason="label class undecidable",
                binding_path="/s/binding-1.yaml",
                binding_hash="bh",
                reasons=["axes from conversion.json"],
            )

        @activity.defn(name="read_smoke")
        async def read_smoke(ws: str, record: str | None = None) -> SmokeResult:
            rec("read_smoke", ws, record)
            return self.smoke

        @activity.defn(name="take_lock")
        async def take_lock(kind: str, key: str, owner: str, ttl_s: int) -> LockTaken:
            rec("take_lock", kind, key, owner, ttl_s)
            if self.lock_busy:
                self.lock_busy -= 1
                return LockTaken(False, "mia-buffers b1 stage")
            return LockTaken(True)

        @activity.defn(name="release_lock")
        async def release_lock(kind: str, key: str, owner: str) -> None:
            rec("release_lock", kind, key, owner)

        @activity.defn(name="read_verify")
        async def read_verify(ws: str, crop_ws: str | None = None, record: str | None = None) -> VerifyResult:
            rec("read_verify", ws, crop_ws, *([record] if record else []))
            job = self.crop_jobs.get(crop_ws or "")
            trusted = bool(job and job.trusted)  # a matched binding's or the merge commit's set; else a PR head's
            by_sha = next((v for s, v in self.test_ok_by.items() if s in ws), None)  # by tested SHA
            k = by_sha if by_sha is not None else (self.set_ok if trusted else self.test_ok)
            n = job.n_crops if job else 16
            return VerifyResult(
                k,
                n,
                [] if k == n else ["c9: overlaps c1"],
                [{"crop_id": f"c{i}", "files": {}} for i in range(k)],
                self.n_effective if trusted else n,
                bytes_fetched=1234,
            )

        @activity.defn(name="publish_gallery")
        async def publish_gallery(
            issue: int, round_id: str, kind: str, crop_ws: str, verify_ws: str, verified: list[dict]
        ) -> GalleryResult:
            rec("publish_gallery", kind, len(verified))
            return GalleryResult(f"galleries/rec-1/{round_id}/{kind}", {}, f"table {kind}")

        @activity.defn(name="publish_crops")
        async def publish_crops(
            issue: int, round_id: str, crop_ws: str, verified: list[dict], sha: str, binding: str | None = None
        ) -> dict:
            rec("publish_crops", crop_ws, len(verified), sha, binding)
            return {"published": [c["crop_id"] for c in verified], "already": [], "total": len(verified)}

        @activity.defn(name="open_snippet_issue")
        async def open_snippet_issue(
            issue: int, round_id: str, draft: dict, snippet_issue: int | None = None, binding: str | None = None
        ) -> int:
            rec("open_snippet_issue", round_id, snippet_issue, *([binding, draft] if binding else []))
            return 100

        @activity.defn(name="read_feedback")
        async def read_feedback(issue: int, since: str | None, until: str) -> str:
            rec("read_feedback", issue, since, until)
            return "alice wrote:\nthe masks are shifted by one pixel"

        @activity.defn(name="pr_state")
        async def pr_state(pr: int) -> PrUpdate:
            rec("pr_state", pr)
            return self.pr_now

        @activity.defn(name="set_crop_status")
        async def set_crop_status(sha: str, state: str, url: str | None, description: str = "") -> None:
            rec("set_crop_status", sha, state, url)

        @activity.defn(name="tree_sha")
        async def tree_sha(sha: str, name: str) -> str | None:
            rec("tree_sha", sha, name)
            return self.trees.get(sha, "T1")

        @activity.defn(name="adopt_pr_binding")
        async def adopt_pr_binding(round_id: str, pr: int, sha: str) -> dict:
            rec("adopt_pr_binding", pr, sha)
            return {"path": f"/s/binding-pr-{sha[:7]}.yaml", "hash": "h" + sha[:7], "name": "new-snip", "errors": []}

        return [
            admit,
            limits,
            set_stage,
            comment,
            refuse_label,
            prepare_match,
            prepare_job,
            submit,
            read_match,
            read_smoke,
            take_lock,
            release_lock,
            read_verify,
            publish_gallery,
            publish_crops,
            open_snippet_issue,
            read_feedback,
            pr_state,
            set_crop_status,
            tree_sha,
            adopt_pr_binding,
        ]

    def await_act(self):
        @activity.defn(name="await_job")
        async def await_job(job_id: str, ws: str, lock: list[str] | None = None) -> JobDone:
            self.calls.append(("await", (job_id, ws, lock)))
            if self.gate and self.gate_on in ws:
                await self.gate.wait()
            if self.await_failures:
                self.await_failures -= 1
                raise ApplicationError("worker died mid-await")
            state, code = self.states.get(ws.rsplit("/", 1)[1].rsplit("-", 1)[0], ("DONE", 0))  # by kind
            return JobDone(job_id, state, code)

        return [await_job]

    def names(self):
        return [c[0] for c in self.calls]

    def of(self, name):
        return [c[1] for c in self.calls if c[0] == name]

    @property
    def stages(self):
        return [a[0] for a in self.of("set_stage")]

    def jobs(self):
        return [a[2] for a in self.of("submit")]


async def until(cond, timeout=10.0):
    for _ in range(int(timeout / 0.02)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("timed out")


async def idle(h, m, stage):
    """Wait until set_stage(stage) ran and the workflow is blocked again (its last history event is a completed
    workflow task, not a scheduled activity), so the next signal finds it waiting rather than busy."""
    await until(lambda: m.stages[-1:] == [stage])
    trailing = {EventType.EVENT_TYPE_MARKER_RECORDED, EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES}
    for _ in range(500):
        events = [e for e in (await h.fetch_history()).events if e.event_type not in trailing]  # a patch marker
        if events[-1].event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("workflow never went idle")


@contextlib.asynccontextmanager
async def started(env, m, first=None, runner=None):
    tq = f"tq-{uuid.uuid4()}"
    kw = {"workflow_runner": runner} if runner else {}
    async with (
        Worker(env.client, task_queue=tq, workflows=[MiaDataDataset], activities=m.acts(), **kw),
        Worker(env.client, task_queue=tq + "-await", activities=m.await_act()),
    ):
        h0 = await env.client.start_workflow(
            MiaDataDataset.run,
            DatasetState(7),
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


async def approve_review(h, m, eid="12", dt=1, kind="approve"):
    await idle(h, m, "review")
    fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
    await h.signal(getattr(MiaDataDataset, kind), ev(kind, eid, shift(fin, dt)))


async def reject_review(h, m, eid="12", dt=1):
    await approve_review(h, m, eid, dt, kind="reject")


async def test_a_stage_write_does_not_hold_the_round(env):
    """stage-write-async: the match job is submitted while GitHub still owes the `matching` label; a final stage
    (needs-human here, from the failed match) waits for its write."""
    m = Mocks(outcome="blocked", slow_stages={"matching", "needs-human"})
    async with started(env, m) as h:
        await until(lambda: m.of("submit"))
        assert m.stages == [] and m.of("submit")[0][1] == "match"
        m.stage_gate.set()
        await idle(h, m, "needs-human")
    assert m.stages == ["matching", "needs-human"]


async def test_match_crops_review_approve_publishes_that_set(env):
    """spec §13: one set per round; the crops a human validated are the crops published, no second job."""
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
        assert "publish_crops" not in m.names()  # nothing is published before the approve
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.stages == ["matching", "testing", "review", "done"]
    assert [a[1] for a in m.of("submit")] == ["match", "smoke", "crop", "verify"]
    (the_set,) = [a for a in m.jobs() if a.n_crops]
    assert (the_set.n_crops, the_set.batch, the_set.trusted, the_set.sha, the_set.existing) == (16, 0, True, SHA, True)
    assert [a[0] for a in m.of("publish_gallery")] == [SHA]
    assert m.of("publish_crops") == [(f"/w/i7-e11/crop-{SHA}-1", 16, SHA, "/s/binding-1.yaml")]
    seq = [n for n in m.names() if n in ("read_verify", "publish_gallery", "publish_crops", "comment")]
    assert seq == ["read_verify", "publish_gallery", "comment", "read_verify", "publish_crops"]
    assert m.of("read_verify")[1] == (
        f"/w/i7-e11/verify-{SHA}-1",
        f"/w/i7-e11/crop-{SHA}-1",
    )  # read back for publishing
    assert "16 crops published" in m.of("set_stage")[-1][2]
    owners = ["mia-data i7-e11 smoke", "mia-data i7-e11 crop"]
    assert [a[2] for a in m.of("take_lock")] == owners and [a[2] for a in m.of("release_lock")] == owners
    smoke_lock, crop_lock = ["raw", "rec-1", owners[0]], ["raw", "rec-1", owners[1]]
    assert [a[2] for a in m.of("await")] == [None, smoke_lock, *[crop_lock] * 2]  # match: none; crop and verify jobs
    report = m.of("comment")[0][0]
    assert "table" in report and "axes from conversion.json" in report and "1234 bytes" in report
    done = m.of("set_stage")[-1]
    assert done[0] == "done" and "**Totals:** 16 crops published" in done[2]  # the gallery table went up at review
    assert m.of("comment")[0][3] == "/s/binding-1.yaml"  # the test report carries the matched binding
    assert not m.of("refuse_label")


async def test_seed_is_recorded(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert st.round_id == "i7-e11" and st.handled == ["11", "12"] and int(st.seed) >= 0
    assert [a.seed for a in m.jobs() if a.n_crops] == [st.seed + "-t"]


async def test_approve_outside_review_is_refused(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
        await h.signal(MiaDataDataset.approve, ev("approve", "12", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.of("refuse_label")[0][0] == "data:approve" and m.of("refuse_label")[0][2] == "12"
    assert "cropping" not in m.stages and not [a for a in m.jobs() if a.n_crops]


async def test_stale_approve_is_refused(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m, "12", dt=-3600)
        await until(lambda: m.of("refuse_label"))
        assert m.stages[-1] == "review"
        await approve_review(h, m, "13")
        await idle(h, m, "done")
    ((label, reason, eid),) = m.of("refuse_label")
    assert (label, eid) == ("data:approve", "12") and "older" in reason


async def test_redelivered_go_is_ignored(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
        await h.signal(MiaDataDataset.go, ev("go", "11"))
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: len(m.stages) == 4)
        await idle(h, m, "needs-human")
    assert m.of("admit") == [("11",), ("12",)] and not m.of("refuse_label")


async def test_go_while_busy_is_rejected_with_comment(env):
    m = Mocks(outcome="blocked", gate=asyncio.Event())
    async with started(env, m) as h:
        await until(lambda: m.of("await"))
        await h.signal(MiaDataDataset.go, ev("go", "12"))  # in history before the job ends
        m.gate.set()
        await until(lambda: m.of("refuse_label"))
        await idle(h, m, "needs-human")
        await h.signal(MiaDataDataset.go, ev("go", "13"))  # arrives while idle: a new round
        await until(lambda: len(m.of("admit")) == 2)
        await until(lambda: m.stages.count("needs-human") == 2)  # no activity in flight at shutdown: one would hold
        await idle(h, m, "needs-human")  # up the test server's time skipping (env.sleep) in later tests
    ((label, reason, eid),) = m.of("refuse_label")
    assert (label, eid) == ("data:go", "12") and "busy" in reason
    assert m.of("admit") == [("11",), ("13",)]


async def test_a_set_with_fewer_than_half_verified_never_reaches_review(env):
    m = Mocks(set_ok=5)
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert m.stages == ["matching", "testing", "needs-human"] and "publish_crops" not in m.names()
    assert "5 of 16 verified (at least 8 needed)" in m.of("comment")[0][0]


async def test_a_set_with_at_least_half_verified_is_reviewed_and_published(env):
    """harness verify exits 1 whenever a crop fails; the set is judged by its report, not that exit."""
    m = Mocks(states={f"verify-{SHA}": ("EXIT", 1)}, set_ok=12)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.of("publish_crops")[0][1] == 12 and "12 crops published" in m.of("set_stage")[-1][2]


async def test_a_small_raw_is_cropped_not_judged(env):
    """Size is the buffer plan's decision (spec §12): smoke's old `eligible` flag changes nothing in the round."""
    m = Mocks(smoke=SmokeResult(True, False, shapes={"raw": [[10, 10]]}))
    async with started(env, m) as h:
        await idle(h, m, "review")
    assert "ineligible" not in m.stages and [a for a in m.jobs() if a.n_crops]


async def test_an_ineligible_outcome_is_an_unknown_one_now(env):
    m = Mocks(outcome="ineligible", smoke=SmokeResult(True, False))
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert m.stages == ["matching", "needs-human"] and not [a for a in m.jobs() if a.n_crops]


async def test_blocked_goes_needs_human(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert m.stages == ["matching", "needs-human"] and "label class undecidable" in m.of("set_stage")[-1][1]
    assert [a[1] for a in m.of("submit")] == ["match"]


@pytest.mark.parametrize(
    "states, stages, published",
    [
        ({"match": ("EXIT", 1)}, ["matching", "needs-human"], []),
        ({"smoke": ("EXIT", 1)}, ["matching", "needs-human"], []),  # even though smoke.json says ok
        ({f"crop-{SHA}": ("EXIT", 1)}, ["matching", "testing", "needs-human"], []),
        ({f"verify-{SHA}": ("UNKNOWN", None)}, ["matching", "testing", "needs-human"], []),
    ],
)
async def test_failed_job_needs_human_no_retry(env, states, stages, published):
    m = Mocks(states=states)
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    keys = [a[0] for a in m.of("submit")]
    assert m.stages == stages and len(keys) == len(set(keys))
    assert [a[1] for a in m.of("publish_crops")] == published  # nothing is published before a review
    assert ("read_match" in m.names()) == ("match" not in states)


async def test_rollover_in_review_then_approve(env, monkeypatch):
    from temporalio import workflow

    monkeypatch.setattr(workflows, "_rollover_due", lambda: workflow.info().continued_run_id is None)  # once
    m = Mocks()
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await until(lambda: m.stages[-1:] == ["review"])
        for _ in range(500):
            if (await h.describe()).run_id != h.first_run_id:
                break
            await asyncio.sleep(0.02)
        await approve_review(h, m)
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert (await h.describe()).run_id != h.first_run_id
    assert m.stages == ["matching", "testing", "review", "done"] and len(m.of("admit")) == 1
    assert [a.seed for a in m.jobs() if a.n_crops] == [st.seed + "-t"]
    assert "16 crops published" in m.of("set_stage")[-1][2]  # the workspaces outlive the rollover


async def test_test_crop_failure_never_produces(env):
    m = Mocks(set_ok=0)
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
        await h.signal(MiaDataDataset.approve, ev("approve", "12", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.stages == ["matching", "testing", "needs-human"]
    assert [a.batch for a in m.jobs() if a.n_crops] == [0] and "publish_crops" not in m.names()


async def test_worker_death_mid_await_reattaches(env):
    m = Mocks(await_failures=1)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    keys = [a[0] for a in m.of("submit")]
    assert len(keys) == len(set(keys)) == 4 and len(m.of("await")) == 5  # one await retried, no job resubmitted


async def test_continue_as_new_keeps_handled(env, monkeypatch):
    monkeypatch.setattr(workflows, "MAX_ROUNDS_PER_EXECUTION", 1)
    m = Mocks(outcome="blocked")
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await idle(h, m, "needs-human")
        assert (await h.describe()).run_id != h.first_run_id  # rolled over
        await h.signal(MiaDataDataset.go, ev("go", "11"))  # redelivered into the new run
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: len(m.of("admit")) == 2)
        await idle(h, m, "needs-human")
        assert (await h.query(MiaDataDataset.status)).handled == ["11", "12"]
    assert m.of("admit") == [("11",), ("12",)]


async def test_smoke_waits_for_the_raw_lock_and_releases_it_after_a_failed_smoke(env):
    m = Mocks(smoke=SmokeResult(False, errors=["no data"]), lock_busy=2)
    async with started(env, m) as h:
        for tries in (1, 2):  # a refused take sleeps poll_minutes (2), doubling with each try
            await until(lambda tries=tries: len(m.of("take_lock")) == tries)
            await env.sleep(timedelta(minutes=2 * tries + 1))
        await idle(h, m, "needs-human")
        timers = [e for e in (await h.fetch_history()).events if e.event_type == EventType.EVENT_TYPE_TIMER_STARTED]
        assert [e.timer_started_event_attributes.start_to_fire_timeout.seconds for e in timers] == [120, 240]
    owner = "mia-data i7-e11 smoke"
    assert m.of("take_lock") == [("raw", "rec-1", owner, 900)] * 3
    waiting = [a for a in m.of("comment") if a[1] == "lock i7-e11"]
    assert waiting == [
        ("waiting for the raw cache of `rec-1`: held by mia-buffers b1 stage", "lock i7-e11", "data", None)
    ]
    names = [(name, args[1] if name == "submit" else None) for name, args in m.calls]
    last_take = max(i for i, (name, _) in enumerate(names) if name == "take_lock")
    assert names.index(("submit", "smoke")) > last_take > names.index(("comment", None))
    assert [a for a in m.of("await") if "smoke" in a[1]] == [
        ("job-mia-7-i7-e11-smoke-1", "/w/i7-e11/smoke-1", ["raw", "rec-1", owner])
    ]
    assert m.of("read_smoke") == [("/w/i7-e11/smoke-1", "rec-1")]
    assert m.of("release_lock") == [("raw", "rec-1", owner)]
    assert names.index(("read_smoke", None)) < names.index(("release_lock", None))
    assert m.stages == ["matching", "needs-human"] and "no data" in m.of("set_stage")[-1][1]


async def test_a_failed_smoke_job_still_releases_the_raw_lock(env):
    m = Mocks(states={"smoke": ("EXIT", 2)})
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert "read_smoke" not in m.names() and m.of("release_lock") == [("raw", "rec-1", "mia-data i7-e11 smoke")]


# --- the development path (§7.3) ------------------------------------------------------------

H1, H2, H3, MC = "1" * 40, "2" * 40, "3" * 40, "e" * 40
PR_URL = "https://github.com/o/snippets/pull/5"


def head(sha, state="open", merge=None, needs_human=False, issue=100):
    return PrUpdate(5, sha, state, merge, issue, needs_human, PR_URL)


def statuses(m):
    return [(sha, state) for sha, state, _ in m.of("set_crop_status")]


async def wake(h, m, now):
    """The PR is now `now`; the poller's signal is a wake-up only (its fields are deliberately stale)."""
    m.pr_now = now
    await h.signal(MiaDataDataset.pr_update, head("0" * 40))


async def to_review(h, m):
    """needs_snippet → snippets issue → h1's set made and shown → review."""
    await idle(h, m, "developing")
    await wake(h, m, head(H1))
    await idle(h, m, "review")


def pr_comments(m):
    return [a[0] for a in m.of("comment") if a[2] == "snippets"]


async def happy(h, m):
    """h1's set approved → `/approve h1` → autodev merges → h1's crops are published under the merge commit."""
    await to_review(h, m)
    await approve_review(h, m)
    await idle(h, m, "merging")
    await wake(h, m, head(H1, "merged", MC))
    await idle(h, m, "done")


async def test_develop_happy_path(env):
    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await happy(h, m)
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "review", "merging", "done"]
    assert statuses(m) == [(H1, "pending"), (H1, "success")]
    assert m.of("set_crop_status")[1][2] == f"https://github.com/o/snippets/issues/5#test i7-e11 {H1}"  # the PR comment
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "verify"]  # no smoke: tested; no second set
    assert not m.of("take_lock")  # the PR head's crop job writes a private cache, not the record's
    (the_set,) = [a for a in m.jobs() if a.n_crops]
    assert (the_set.sha, the_set.trusted, the_set.n_crops, the_set.batch) == (H1, False, 16, 0)  # the dataset's set
    assert the_set.existing  # dedups against a published manifest like any set
    assert m.of("adopt_pr_binding") == [(5, H1), (5, MC)]  # the merge commit's binding is what is published
    assert pr_comments(m)[-1] == f"/approve {H1}"  # autodev merges on the worker's word
    assert m.of("publish_crops")[0] == (f"/w/i7-e11/crop-{H1}-1", 16, MC, f"/s/binding-pr-{MC[:7]}.yaml")
    assert m.of("publish_gallery") == [(H1, 16)]
    assert st.snippet_issue == 100 and st.pr is None and st.pins.snippets_commit == MC and st.snippet_name == "new-snip"
    assert [(t.sha, t.ok, t.tree_sha, bool(t.approved_at)) for t in st.tests] == [(H1, True, "T", True)]
    assert [a[2] for a in m.of("set_stage") if a[0] == "review"] == [f"**Snippet PR:** {PR_URL}\n"]
    assert "16 crops published" in m.of("set_stage")[-1][2]


async def test_revise_new_head_retests(env):
    """A head whose set fails goes back to autodev as `/revise` with the report; the stage leaves review."""
    m = Mocks(outcome="needs_snippet", test_ok_by={H2: 0})
    async with started(env, m) as h:
        await to_review(h, m)
        await wake(h, m, head(H2))
        await until(lambda: (H2, "failure") in statuses(m))
        await idle(h, m, "testing")
    assert m.stages[-3:] == ["testing", "review", "testing"]  # h1's success does not make h2 ready
    assert statuses(m) == [(H1, "pending"), (H1, "success"), (H2, "pending"), (H2, "failure")]
    assert [a.sha for a in m.jobs() if a.n_crops] == [H1, H2] and "cropping" not in m.stages
    h1_report, h2_report = pr_comments(m)
    assert h1_report.startswith("Crops at") and h2_report.startswith(f"/revise the crops at `{H2}` failed")
    assert "0 of 16 verified (at least 8 needed)" in h2_report


async def test_reject_in_develop_review_sends_the_comments_to_the_pr(env):
    """data:reject at a PR head's review: the reviewer's comments since the set's report go to the PR as
    `/revise`; the next head's set comes back to review, and its approve merges (spec §14)."""
    m = Mocks(outcome="needs_snippet", trees={H2: "T2", MC: "T2"})
    async with started(env, m) as h:
        await to_review(h, m)
        await reject_review(h, m, eid="12")
        await idle(h, m, "developing")
        fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
        await wake(h, m, head(H2))
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await wake(h, m, head(H2, "merged", MC))
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == [
        "matching",
        "developing",
        "testing",
        "review",
        "developing",
        "testing",
        "review",
        "merging",
        "done",
    ]
    assert m.of("read_feedback") == [(7, fin, shift(fin, 1))]  # the comments between the report and the reject
    assert pr_comments(m)[1] == "/revise alice wrote:\nthe masks are shifted by one pixel"
    assert pr_comments(m)[-1] == f"/approve {H2}" and not m.of("refuse_label")
    assert [(t.sha, bool(t.approved_at)) for t in st.tests] == [(H1, False), (H2, True)]
    assert m.of("publish_crops")[0][0] == f"/w/i7-e11/crop-{H2}-1"


async def test_reject_in_matched_review_asks_for_a_superseding_snippet(env):
    """data:reject on a matched binding's crops: a snippets issue asks autodev to supersede the binding's
    snippet, with the comments; the round goes on as a develop round and ends like one (spec §14)."""
    m = Mocks(trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await reject_review(h, m)
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "testing", "review", "developing", "testing", "review", "merging", "done"]
    assert m.of("open_snippet_issue") == [
        ("i7-e11", None, "/s/binding-1.yaml", {"what_fails": "alice wrote:\nthe masks are shifted by one pixel"})
    ]
    assert "superseding" in m.of("set_stage")[3][1]
    assert [a[1] for a in m.of("submit")] == ["match", "smoke", "crop", "verify", "crop", "verify"]
    assert [(a.sha, a.trusted) for a in m.jobs() if a.n_crops] == [(PINS.snippets_commit, True), (H1, False)]
    assert m.of("publish_crops")[0][2] == MC and st.pins.snippets_commit == MC and st.snippet_issue == 100


async def test_stale_and_misplaced_rejects_are_refused(env):
    m = Mocks(outcome="needs_snippet")
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await h.signal(MiaDataDataset.reject, ev("reject", "12"))  # not in review
        await until(lambda: m.of("refuse_label"))
        await wake(h, m, head(H1))
        await idle(h, m, "review")
        await h.signal(MiaDataDataset.reject, ev("reject", "13"))  # older than the set
        await until(lambda: len(m.of("refuse_label")) == 2)
        await idle(h, m, "review")
    assert [(a[0], a[2]) for a in m.of("refuse_label")] == [("data:reject", "12"), ("data:reject", "13")]
    assert "data:review" in m.of("refuse_label")[0][1] and "older" in m.of("refuse_label")[1][1]
    assert not m.of("read_feedback") and m.stages[-1] == "review"


async def test_rollover_at_a_pr_heads_review_keeps_following_the_pr(env, monkeypatch):
    from temporalio import workflow

    m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
    m.roll = False  # the rollover is due once h1's set is at review, and once
    monkeypatch.setattr(workflows, "_rollover_due", lambda: m.roll and workflow.info().continued_run_id is None)
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await idle(h, m, "developing")
        m.roll = True
        await wake(h, m, head(H1))
        await until(lambda: m.stages[-1:] == ["review"])
        for _ in range(500):
            if (await h.describe()).run_id != h.first_run_id:
                break
            await asyncio.sleep(0.02)
        await approve_review(h, m)
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
    assert (await h.describe()).run_id != h.first_run_id
    assert m.stages == ["matching", "developing", "testing", "review", "merging", "done"]
    assert pr_comments(m)[-1] == f"/approve {H1}"


async def test_stale_result_ignored(env):
    m = Mocks(outcome="needs_snippet", gate=asyncio.Event(), gate_on=H1, test_ok_by={H2: 0})
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: any(H1 in a[1] for a in m.of("await")))
        await wake(h, m, head(H2))  # h1 is still testing (and will pass)
        m.gate.set()
        await until(lambda: (H2, "failure") in statuses(m))
        await idle(h, m, "testing")
        st = await h.query(MiaDataDataset.status)
    assert "review" not in m.stages and [(t.sha, t.ok) for t in st.tests] == [(H1, True), (H2, False)]
    assert statuses(m)[-2:] == [(H2, "pending"), (H2, "failure")]  # h1's late success is h1's, not h2's


@pytest.mark.parametrize("merge_ok", [True, False])
async def test_merge_with_different_tree_makes_the_merge_commits_set_for_review(env, merge_ok):
    """The merged snippet is not the approved head's (someone pushed more): its own set, reviewed again."""
    m = Mocks(outcome="needs_snippet", trees={H1: "T1", MC: "T2"}, test_ok_by={} if merge_ok else {MC: 0})
    async with started(env, m) as h:
        await to_review(h, m)
        await approve_review(h, m)
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        if merge_ok:
            await approve_review(h, m, eid="13")
        await idle(h, m, "done" if merge_ok else "needs-human")
        st = await h.query(MiaDataDataset.status)
    tests = [a for a in m.jobs() if a.n_crops and not a.batch]
    assert [(a.sha, a.trusted) for a in tests] == [(H1, False), (MC, True)]  # the merge commit: a trusted job
    assert statuses(m) == [(H1, "pending"), (H1, "success")]  # no status on the merge commit
    assert bool(m.of("publish_crops")) == merge_ok and (m.stages[-1] == "done") == merge_ok
    assert m.stages[3:6] == ["review", "merging", "testing"] and "not the reviewed head" in m.of("set_stage")[5][1]
    if merge_ok:
        assert m.of("publish_crops")[0][0] == f"/w/i7-e11/crop-{MC}-1" and st.tests[-1].approved_at


async def test_head_test_that_cannot_run_posts_error(env):
    m = Mocks(outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)})
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await idle(h, m, "needs-human")
    assert statuses(m) == [(H1, "pending"), (H1, "error")] and "UNKNOWN" in m.of("set_stage")[-1][1]


@pytest.mark.parametrize("now, why", [(head(H1, "closed"), "closed"), (head(H1, needs_human=True), "needs-human")])
async def test_closed_pr_needs_human(env, now, why):
    m = Mocks(outcome="needs_snippet")
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await h.signal(MiaDataDataset.go, ev("go", "12"))  # busy: a round is developing
        await until(lambda: m.of("refuse_label"))
        await wake(h, m, now)
        await idle(h, m, "needs-human")
    assert m.of("refuse_label")[0][0] == "data:go" and "busy" in m.of("refuse_label")[0][1]
    detail = m.of("set_stage")[-1][1]
    assert why in detail and PR_URL in detail and not [a for a in m.jobs() if a.n_crops]


async def test_revision_limit(env):
    m = Mocks(outcome="needs_snippet", revisions=2, test_ok=0)
    async with started(env, m) as h:
        await idle(h, m, "developing")
        for sha in (H1, H2):
            await wake(h, m, head(sha))
            await until(lambda sha=sha: (sha, "failure") in statuses(m))
            await idle(h, m, "testing")
        await wake(h, m, head(H3))
        await idle(h, m, "needs-human")
    assert "revision limit" in m.of("set_stage")[-1][1] and [a.sha for a in m.jobs() if a.n_crops] == [H1, H2]


async def test_merge_signal_before_test_finishes(env):
    """A human merged the PR by hand while h1's set was still being made: nobody approved h1's crops, so the
    merge commit's set is made and reviewed like a matched binding's."""
    m = Mocks(outcome="needs_snippet", gate=asyncio.Event(), gate_on=H1, trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await idle(h, m, "developing")
        await wake(h, m, head(H1))
        await until(lambda: any(H1 in a[1] for a in m.of("await")))
        await wake(h, m, head(H1, "merged", MC))
        m.gate.set()
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.stages == ["matching", "developing", "testing", "testing", "review", "done"]
    assert statuses(m) == [(H1, "pending"), (H1, "success")]
    assert [(a.sha, a.trusted) for a in m.jobs() if a.n_crops] == [(H1, False), (MC, True)]
    assert m.of("publish_crops")[0][0] == f"/w/i7-e11/crop-{MC}-1"


async def to_needs_human_in_develop(h, m):
    """needs_snippet → snippets issue → h1's crop job is lost (an infrastructure failure) → needs-human."""
    await idle(h, m, "developing")
    await wake(h, m, head(H1))
    await idle(h, m, "needs-human")


async def test_go_after_needs_human_resumes_the_snippet_pr(env):
    """A develop round that failed for an infrastructure reason: `go` re-tests the same PR under a new round and
    pins, and opens no second snippets issue (spec §7.4, comment and re-trigger)."""
    m = Mocks(outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)}, trees={H1: "T", MC: "T"})
    async with started(env, m) as h:
        await to_needs_human_in_develop(h, m)
        m.states = {}  # the infrastructure is fixed
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await approve_review(h, m, eid="13")
        await idle(h, m, "merging")
        await wake(h, m, head(H1, "merged", MC))
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "needs-human", "testing", "review", "merging", "done"]
    assert m.of("admit") == [("11",), ("12",)] and m.of("open_snippet_issue") == [("i7-e11", None)]
    assert [a[1] for a in m.of("submit")] == ["match", "crop", "crop", "verify"]  # no second match
    keys = [a[0] for a in m.of("submit")]
    assert len(keys) == len(set(keys)) and keys[2].startswith("mia-7-i7-e12-crop-")  # the retest is the new round's
    assert statuses(m) == [(H1, "pending"), (H1, "error"), (H1, "pending"), (H1, "success")]
    resumed = m.of("set_stage")[4]
    assert resumed[0] == "testing" and "resuming" in resumed[1] and PR_URL in resumed[1]
    assert (st.round_id, st.snippet_issue, st.pr) == ("i7-e12", 100, None) and not m.of("refuse_label")
    assert [t.test_id for t in st.tests] == [f"i7-e12-{H1[:12]}"]


@pytest.mark.parametrize("now", [head(H1, "closed"), head(H1, "merged", MC)], ids=["closed", "merged"])
async def test_go_after_needs_human_with_a_finished_pr_matches_again(env, now):
    """The PR is gone by the time `go` arrives: a new match, which hands the snippets issue back for reuse."""
    m = Mocks(outcome="needs_snippet", states={f"crop-{H1}": ("UNKNOWN", None)})
    async with started(env, m) as h:
        await to_needs_human_in_develop(h, m)
        m.pr_now = now
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await idle(h, m, "developing")
        st = await h.query(MiaDataDataset.status)
    assert m.stages == ["matching", "developing", "testing", "needs-human", "matching", "developing"]
    assert m.of("pr_state") == [(5,), (5,)] and [a[1] for a in m.of("submit")] == ["match", "crop", "match"]
    assert m.of("open_snippet_issue") == [("i7-e11", None), ("i7-e12", 100)]
    assert (st.round_id, st.snippet_issue, st.pr) == ("i7-e12", 100, None) and not m.of("refuse_label")


# --- the poll activity -------------------------------------------------------------------------


class PollGH(FakeGH):
    def __init__(self, repo, issues=None, events=None, perms=None, prs=None, changed=None, status=None):
        super().__init__(repo, issues=issues)
        self.events, self.perms, self.prs = events or {}, perms or {}, prs or []
        self.changed, self.status, self.set = changed or {}, status or {}, []

    async def list_issues(self, labels=None, state=None, **_):
        return [i for i in self.issues.values() if not labels or labels in {x["name"] for x in i["labels"]}]

    async def label_event(self, n, label):
        return self.events[(n, label)]

    async def permission(self, login):
        return self.perms.get(login, "none")

    async def list_prs(self, state="all", per_page=50):
        return self.prs

    async def pr_files(self, n):
        return self.changed[n]

    async def statuses(self, sha):
        return self.status.get(sha, [])

    async def set_status(self, sha, context, state, description, target_url):
        self.set.append((sha, context, state, description))


class FakeClient:
    def __init__(self):
        self.started, self.signals = [], []

    async def start_workflow(self, wf, arg, **kw):
        self.started.append((wf, arg, kw))

    def get_workflow_handle(self, wid):
        signals = self.signals

        class H:
            async def signal(self, name, arg):
                signals.append((wid, name, arg))

        return H()


def issue(n, *labels, body="", user="bot"):
    return {"number": n, "body": body, "labels": [{"name": x} for x in labels], "user": {"login": user}}


def pr(n, ref, sha, state="open", repo="o/snippets"):
    return {
        "number": n,
        "state": state,
        "updated_at": "2099-01-01T00:00:00Z",
        "merged_at": None,
        "merge_commit_sha": None,
        "head": {"ref": ref, "sha": sha, "repo": {"full_name": repo}},
    }


def _poll(tmp_path, data, snip):
    fc = FakeClient()
    return Activities(make_cfg(tmp_path), data, snip, None, None, bot="bot", client=fc), fc


async def test_poll_starts_a_round_per_go_and_refuses_an_unprivileged_label(tmp_path):
    at = "2026-10-12T10:00:00Z"
    data = PollGH(
        "o/data",
        issues={1: issue(1, "data:go", "licence-excluded"), 2: issue(2, "data:go"), 3: issue(3, "data:approve")},
        events={
            (1, "data:go"): ("alice", "51", at),
            (2, "data:go"): ("alice", "52", at),
            (3, "data:approve"): ("mallory", "53", at),
        },
        perms={"alice": "write"},
    )
    a, fc = _poll(tmp_path, data, PollGH("o/snippets"))
    await a.poll()
    started_ids = sorted(kw["id"] for _, _, kw in fc.started)  # a stale `licence-excluded` label refuses nothing
    assert started_ids == ["mia-data/o/data/issue-1", "mia-data/o/data/issue-2"]
    (_, arg, kw) = next(s for s in fc.started if s[2]["id"].endswith("issue-2"))
    assert (arg, kw["task_queue"], kw["start_signal"]) == (DatasetState(2), "mia-data", "go")
    assert kw["start_signal_args"] == [LabelEvent("go", "52", "alice", at)]
    assert sorted(data.removed) == [(1, "data:go"), (2, "data:go"), (3, "data:approve")]
    assert "mallory" in data.comments[3][0]["body"] and 1 not in data.comments and 2 not in data.comments


async def test_poll_sets_status_for_non_snippet_pr(tmp_path):
    h = ["1" * 40, "2" * 40, "3" * 40, "4" * 40]
    snip = PollGH(
        "o/snippets",
        issues={
            100: issue(100, body="<!-- mia-data: issue=1 round=i1-e99 -->\n\nNew snippet"),
            101: issue(101, body="<!-- mia-data: issue=2 round=i2-e9 -->", user="mallory"),
        },
        prs=[
            pr(5, "docs", h[0]),
            pr(6, "feature", h[1]),
            pr(7, "docs2", h[2]),
            pr(8, "snippets/issue-100", h[3]),
            pr(9, "snippets/issue-101", h[3]),
        ],
        changed={
            5: ["README.md"],
            6: ["snippets/x/snippet.py"],
            7: ["SPEC.md"],
            8: ["snippets/x/snippet.py"],
            9: ["snippets/y/snippet.py"],
        },
        status={h[2]: [{"context": "mia-data/crops", "state": "success"}]},
    )
    a, fc = _poll(tmp_path, PollGH("o/data"), snip)
    await a.poll()
    assert snip.set == [(h[0], "mia-data/crops", "success", "no snippet in this PR")]
    assert fc.signals == [("mia-data/o/data/issue-1", "pr_update", PrUpdate(8, h[3], "open", None, 100, False))]


async def test_poll_signals_a_pr_only_when_it_changes(tmp_path):
    k = issue(100, body="<!-- mia-data: issue=1 round=i1-e99 -->")
    snip = PollGH(
        "o/snippets",
        issues={100: k},
        prs=[pr(8, "snippets/issue-100", H1)],
        changed={8: ["snippets/x/a.py"]},
        status={H1: [{"context": "mia-data/crops", "state": "pending"}]},
    )
    a, fc = _poll(tmp_path, PollGH("o/data"), snip)
    await a.poll()
    await a.poll()  # nothing changed: no signal
    snip.prs[0]["head"]["sha"] = H2
    snip.status[H2] = snip.status[H1]
    await a.poll()
    k["labels"] = [{"name": "snippets:needs-human"}]
    await a.poll()
    assert [(u.head_sha, u.needs_human) for _, _, u in fc.signals] == [(H1, False), (H2, False), (H2, True)]


HISTORY = Path(__file__).parent / "fixtures" / "mia" / "dataset_history.json"
DEVELOP_HISTORY = HISTORY.with_name("develop_history.json")
LOCK_HISTORY = HISTORY.with_name("dataset_history_c.json")


def _scrub(obj):
    """Drop worker/client identities (pid@hostname) from a recorded history."""
    if isinstance(obj, dict):
        return {k: "test" if k == "identity" else _scrub(v) for k, v in obj.items()}
    return [_scrub(v) for v in obj] if isinstance(obj, list) else obj


async def test_replay_dataset_history(env):
    """A recorded match/test/review/approve/crop run must replay on today's MiaDataDataset. This fails when a change
    to workflow code alters the commands it issues, which breaks in-flight runs; guard an intended change with
    workflow.patched() and re-record: MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks()
        async with started(env, m) as h:
            await approve_review(h, m)
            await idle(h, m, "done")
            HISTORY.parent.mkdir(exist_ok=True)
            HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-recorded", HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_develop_history(env):
    """The develop path's recorded run (test_develop_happy_path), as test_replay_dataset_history; same switch."""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(outcome="needs_snippet", trees={H1: "T", MC: "T"})
        async with started(env, m) as h:
            await happy(h, m)
            DEVELOP_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-develop", DEVELOP_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_replay_dataset_history_c(env):
    """The match → done run under the `c-raw-lock` patch (smoke waits once for the raw lock), as
    test_replay_dataset_history. Re-record this one alone, so the older fixtures keep the unpatched path:
    MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay_dataset_history_c"""
    if os.environ.get("MIA_DATA_RECORD_HISTORY"):
        m = Mocks(lock_busy=1)
        async with started(env, m) as h:
            await until(lambda: m.of("take_lock"))
            await env.sleep(timedelta(minutes=3))
            await approve_review(h, m)
            await idle(h, m, "done")
            LOCK_HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("mia-raw-lock", LOCK_HISTORY.read_text())
    await Replayer(workflows=[MiaDataDataset]).replay_workflow(history)


async def test_poll_accepts_go_only_from_listed_bot(tmp_path):
    at = "2026-10-12T10:00:00Z"
    data = PollGH(
        "o/data",
        issues={1: issue(1, "data:go"), 2: issue(2, "data:go")},
        events={(1, "data:go"): ("mia-bot[bot]", "61", at), (2, "data:go"): ("other[bot]", "62", at)},
    )
    fc = FakeClient()
    cfg = make_cfg(tmp_path, github={"trusted_bots": ["mia-bot[bot]"]})
    a = Activities(cfg, data, PollGH("o/snippets"), None, None, bot="bot", client=fc)
    await a.poll()
    ((wf, arg, kw),) = fc.started
    assert (arg, kw["start_signal_args"]) == (DatasetState(1), [LabelEvent("go", "61", "mia-bot[bot]", at)])
    assert sorted(data.removed) == [(1, "data:go"), (2, "data:go")]
    assert 1 not in data.comments and "does not have write access" in data.comments[2][0]["body"]
