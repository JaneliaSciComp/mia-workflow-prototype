"""MiaDataDataset on Temporal's time-skipping server with every activity mocked, and the poll activity."""

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
from temporalio.exceptions import ApplicationError
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from mia_data import workflows
from mia_data.activities import Activities
from mia_data.models import (DatasetState, GalleryResult, JobArgs, JobDone, LabelEvent, MatchResult, Pins, PrUpdate,
                             RoundLimits, SmokeResult, VerifyResult)
from mia_data.workflows import MiaDataDataset
from mia_helpers import FakeGH, make_cfg

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
    def __init__(self, outcome="matched", smoke=SmokeResult(True, True), states=None, test_ok=4, prod_ok=16,
                 n_effective=16, await_failures=0, gate=None):
        self.calls: list[tuple[str, tuple]] = []
        self.outcome, self.smoke, self.states = outcome, smoke, states or {}
        self.test_ok, self.prod_ok, self.n_effective = test_ok, prod_ok, n_effective
        self.await_failures, self.gate = await_failures, gate

    def acts(self):
        rec = lambda name, *a: self.calls.append((name, a))  # noqa: E731

        @activity.defn(name="admit")
        async def admit(issue: int, event_id: str) -> Pins:
            rec("admit", event_id)
            return PINS

        @activity.defn(name="limits")
        async def limits() -> RoundLimits:
            rec("limits")
            return RoundLimits(4, 16, 120)

        @activity.defn(name="set_stage")
        async def set_stage(issue: int, stage: str, state: DatasetState | None = None, links: dict | None = None,
                            detail: str = "") -> None:
            rec("set_stage", stage, detail, links)

        @activity.defn(name="comment")
        async def comment(issue: int, body: str, marker: str, repo: str = "data") -> None:
            rec("comment", body, marker)

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
            return f"job-{key}"

        @activity.defn(name="read_match")
        async def read_match(ws: str) -> MatchResult:
            rec("read_match")
            return MatchResult(self.outcome, reason="label class undecidable", binding_path="/s/binding-1.yaml",
                               binding_hash="bh", reasons=["axes from conversion.json"])

        @activity.defn(name="read_smoke")
        async def read_smoke(ws: str) -> SmokeResult:
            rec("read_smoke")
            return self.smoke

        @activity.defn(name="read_verify")
        async def read_verify(ws: str, crop_ws: str | None = None) -> VerifyResult:
            rec("read_verify", ws, crop_ws)
            prod = "production" in ws
            k, n = (self.prod_ok, 16) if prod else (self.test_ok, 4)
            return VerifyResult(k, n, [] if k == n else ["c9: overlaps c1"],
                                [{"crop_id": f"c{i}", "files": {}} for i in range(k)], self.n_effective if prod else 4,
                                bytes_fetched=1234)

        @activity.defn(name="publish_gallery")
        async def publish_gallery(issue: int, round_id: str, kind: str, crop_ws: str, verify_ws: str,
                                  verified: list[dict]) -> GalleryResult:
            rec("publish_gallery", kind, len(verified))
            return GalleryResult(f"galleries/rec-1/{round_id}/{kind}", {}, f"table {kind}")

        @activity.defn(name="publish_crops")
        async def publish_crops(issue: int, round_id: str, crop_ws: str, verified: list[dict], sha: str) -> dict:
            rec("publish_crops", crop_ws, len(verified), sha)
            return {"published": [c["crop_id"] for c in verified], "already": [], "total": len(verified)}

        return [admit, limits, set_stage, comment, refuse_label, prepare_match, prepare_job, submit, read_match,
                read_smoke, read_verify, publish_gallery, publish_crops]

    def await_act(self):
        @activity.defn(name="await_job")
        async def await_job(job_id: str, ws: str) -> JobDone:
            self.calls.append(("await", (job_id, ws)))
            if self.gate:
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
    for _ in range(500):
        if (await h.fetch_history()).events[-1].event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("workflow never went idle")


@contextlib.asynccontextmanager
async def started(env, m, first=None, runner=None):
    tq = f"tq-{uuid.uuid4()}"
    kw = {"workflow_runner": runner} if runner else {}
    async with Worker(env.client, task_queue=tq, workflows=[MiaDataDataset], activities=m.acts(), **kw), \
            Worker(env.client, task_queue=tq + "-await", activities=m.await_act()):
        h0 = await env.client.start_workflow(MiaDataDataset.run, DatasetState(7), id=f"wf-{uuid.uuid4()}",
                                             task_queue=tq, start_signal="go", start_signal_args=[first or ev("go", "11")])
        h = env.client.get_workflow_handle(h0.id)  # follows continue-as-new
        h.first_run_id = h0.run_id
        try:
            yield h
        finally:
            await h.terminate()


async def approve_review(h, m, eid="12", dt=1):
    await idle(h, m, "review")
    fin = (await h.query(MiaDataDataset.status)).tests[-1].finished_at
    await h.signal(MiaDataDataset.approve, ev("approve", eid, shift(fin, dt)))


async def test_match_test_review_approve_crop_done(env):
    m = Mocks()
    async with started(env, m) as h:
        await idle(h, m, "review")
        assert [a.batch for a in m.jobs() if a.n] == [0]  # no production crop before approve
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.stages == ["matching", "testing", "review", "cropping", "done"]
    assert [a[1] for a in m.of("submit")] == ["match", "smoke", "crop", "verify", "crop", "verify"]
    test, prod = [a for a in m.jobs() if a.n]
    assert (test.n, test.batch, test.trusted, test.sha, prod.n, prod.batch, prod.trusted, prod.existing) == \
        (4, 0, True, SHA, 16, 1, True, True)
    assert [a[0] for a in m.of("publish_gallery")] == [SHA, "production"]
    assert m.of("publish_crops") == [("/w/i7-e11/crop-production-1", 16, SHA)]
    seq = [n for n in m.names() if n in ("read_verify", "publish_gallery", "publish_crops", "comment")]
    assert seq == ["read_verify", "publish_gallery", "comment", "read_verify", "publish_crops", "publish_gallery"]
    assert m.of("read_verify")[1] == ("/w/i7-e11/verify-production-1", "/w/i7-e11/crop-production-1")
    report = m.of("comment")[0][0]
    assert "table" in report and "axes from conversion.json" in report and "1234 bytes" in report
    assert m.of("set_stage")[-1][2]["totals"].startswith("16 new")
    assert not m.of("refuse_label")


async def test_seed_is_recorded(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
        st = await h.query(MiaDataDataset.status)
    assert st.round_id == "i7-e11" and st.handled == ["11", "12"] and int(st.seed) >= 0
    assert [a.seed for a in m.jobs() if a.n] == [st.seed + "-t", st.seed + "-p"]


async def test_approve_outside_review_is_refused(env):
    m = Mocks(outcome="ineligible", smoke=SmokeResult(True, False))
    async with started(env, m) as h:
        await idle(h, m, "ineligible")
        await h.signal(MiaDataDataset.approve, ev("approve", "12", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.of("refuse_label")[0][0] == "data:approve" and m.of("refuse_label")[0][2] == "12"
    assert "cropping" not in m.stages and not [a for a in m.jobs() if a.n]


async def test_stale_approve_is_refused(env):
    m = Mocks()
    async with started(env, m) as h:
        await approve_review(h, m, "12", dt=-3600)
        await until(lambda: m.of("refuse_label"))
        assert m.stages[-1] == "review"
        await approve_review(h, m, "13")
        await idle(h, m, "done")
    (label, reason, eid), = m.of("refuse_label")
    assert (label, eid) == ("data:approve", "12") and "older" in reason


async def test_redelivered_go_is_ignored(env):
    m = Mocks(outcome="ineligible", smoke=SmokeResult(True, False))
    async with started(env, m) as h:
        await idle(h, m, "ineligible")
        await h.signal(MiaDataDataset.go, ev("go", "11"))
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: len(m.stages) == 4)
        await idle(h, m, "ineligible")
    assert m.of("admit") == [("11",), ("12",)] and not m.of("refuse_label")


async def test_go_while_busy_is_rejected_with_comment(env):
    m = Mocks(outcome="ineligible", smoke=SmokeResult(True, False), gate=asyncio.Event())
    async with started(env, m) as h:
        await until(lambda: m.of("await"))
        await h.signal(MiaDataDataset.go, ev("go", "12"))  # in history before the job ends
        m.gate.set()
        await until(lambda: m.of("refuse_label"))
        await idle(h, m, "ineligible")
        await h.signal(MiaDataDataset.go, ev("go", "13"))  # arrives while idle: a new round
        await until(lambda: len(m.of("admit")) == 2)
    (label, reason, eid), = m.of("refuse_label")
    assert (label, eid) == ("data:go", "12") and "busy" in reason
    assert m.of("admit") == [("11",), ("13",)]


async def test_partial_production(env):
    m = Mocks(prod_ok=5)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "needs-human")
    assert m.of("publish_crops")[0][1] == 5 and m.stages[-2:] == ["cropping", "needs-human"]
    assert "5 of 16" in m.of("set_stage")[-1][1] and m.of("publish_gallery")[-1] == ("production", 5)


async def test_ineligible(env):
    m = Mocks(outcome="ineligible", smoke=SmokeResult(True, False, shapes={"raw": [[10, 10]]}))
    async with started(env, m) as h:
        await idle(h, m, "ineligible")
    assert m.stages == ["matching", "ineligible"] and m.jobs() and not [a for a in m.jobs() if a.n]


async def test_blocked_goes_needs_human(env):
    m = Mocks(outcome="blocked")
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
    assert m.stages == ["matching", "needs-human"] and "label class undecidable" in m.of("set_stage")[-1][1]
    assert [a[1] for a in m.of("submit")] == ["match"]


@pytest.mark.parametrize("states, stages, published", [
    ({"match": ("EXIT", 1)}, ["matching", "needs-human"], []),
    ({"smoke": ("EXIT", 1)}, ["matching", "needs-human"], []),  # even though smoke.json says ok
    ({f"verify-{SHA}": ("EXIT", 1)}, ["matching", "testing", "needs-human"], []),
    ({"crop-production": ("EXIT", 1)}, ["matching", "testing", "review", "cropping", "needs-human"], [16]),
])
async def test_failed_job_needs_human_no_retry(env, states, stages, published):
    m = Mocks(states=states)
    async with started(env, m) as h:
        if "review" in stages:
            await approve_review(h, m)
        await idle(h, m, "needs-human")
    keys = [a[0] for a in m.of("submit")]
    assert m.stages == stages and len(keys) == len(set(keys)) and "EXIT" in m.of("set_stage")[-1][1]
    assert [a[1] for a in m.of("publish_crops")] == published  # what passed stays published (§7.5)
    assert ("read_match" in m.names()) == ("match" not in states)


async def test_production_verify_exit_is_judged_by_its_report(env):
    """harness verify exits 1 whenever a crop fails; production still needs only half of n (§7.5)."""
    m = Mocks(states={"verify-production": ("EXIT", 1)}, prod_ok=12)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    assert m.of("publish_crops")[0][1] == 12


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
    assert m.stages == ["matching", "testing", "review", "cropping", "done"] and len(m.of("admit")) == 1
    assert [a.seed for a in m.jobs() if a.n] == [st.seed + "-t", st.seed + "-p"] and st.links["totals"]


async def test_test_crop_failure_never_produces(env):
    m = Mocks(test_ok=0)
    async with started(env, m) as h:
        await idle(h, m, "needs-human")
        await h.signal(MiaDataDataset.approve, ev("approve", "12", "2030-01-01T00:00:00Z"))
        await until(lambda: m.of("refuse_label"))
    assert m.stages == ["matching", "testing", "needs-human"]
    assert [a.batch for a in m.jobs() if a.n] == [0] and "publish_crops" not in m.names()


async def test_worker_death_mid_await_reattaches(env):
    m = Mocks(await_failures=1)
    async with started(env, m) as h:
        await approve_review(h, m)
        await idle(h, m, "done")
    keys = [a[0] for a in m.of("submit")]
    assert len(keys) == len(set(keys)) == 6 and len(m.of("await")) == 7


async def test_continue_as_new_keeps_handled(env, monkeypatch):
    monkeypatch.setattr(workflows, "MAX_ROUNDS_PER_EXECUTION", 1)
    m = Mocks(outcome="ineligible", smoke=SmokeResult(True, False))
    async with started(env, m, runner=UnsandboxedWorkflowRunner()) as h:
        await idle(h, m, "ineligible")
        assert (await h.describe()).run_id != h.first_run_id  # rolled over
        await h.signal(MiaDataDataset.go, ev("go", "11"))  # redelivered into the new run
        await h.signal(MiaDataDataset.go, ev("go", "12"))
        await until(lambda: len(m.of("admit")) == 2)
        await idle(h, m, "ineligible")
        assert (await h.query(MiaDataDataset.status)).handled == ["11", "12"]
    assert m.of("admit") == [("11",), ("12",)]


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
    return {"number": n, "state": state, "updated_at": "2099-01-01T00:00:00Z", "merged_at": None,
            "merge_commit_sha": None, "head": {"ref": ref, "sha": sha, "repo": {"full_name": repo}}}


def _poll(tmp_path, data, snip):
    fc = FakeClient()
    return Activities(make_cfg(tmp_path), data, snip, None, None, bot="bot", client=fc), fc


async def test_poll_refuses_go_on_licence_excluded(tmp_path):
    at = "2026-10-12T10:00:00Z"
    data = PollGH("o/data", issues={1: issue(1, "data:go", "licence-excluded"), 2: issue(2, "data:go"),
                                    3: issue(3, "data:approve")},
                  events={(1, "data:go"): ("alice", "51", at), (2, "data:go"): ("alice", "52", at),
                          (3, "data:approve"): ("mallory", "53", at)}, perms={"alice": "write"})
    a, fc = _poll(tmp_path, data, PollGH("o/snippets"))
    await a.poll()
    (wf, arg, kw), = fc.started
    assert (wf, arg, kw["id"], kw["task_queue"], kw["start_signal"]) == \
        ("MiaDataDataset", DatasetState(2), "mia-data/o/data/issue-2", "mia-data", "go")
    assert kw["start_signal_args"] == [LabelEvent("go", "52", "alice", at)]
    assert sorted(data.removed) == [(1, "data:go"), (2, "data:go"), (3, "data:approve")]
    assert "licence" in data.comments[1][0]["body"] and "mallory" in data.comments[3][0]["body"]
    assert 2 not in data.comments


async def test_poll_sets_status_for_non_snippet_pr(tmp_path):
    h = ["1" * 40, "2" * 40, "3" * 40, "4" * 40]
    snip = PollGH("o/snippets",
                  issues={100: issue(100, body="<!-- mia-data: issue=1 round=i1-e99 -->\n\nNew snippet"),
                          101: issue(101, body="<!-- mia-data: issue=2 round=i2-e9 -->", user="mallory")},
                  prs=[pr(5, "docs", h[0]), pr(6, "feature", h[1]), pr(7, "docs2", h[2]),
                       pr(8, "snippets/issue-100", h[3]), pr(9, "snippets/issue-101", h[3])],
                  changed={5: ["README.md"], 6: ["snippets/x/snippet.py"], 7: ["SPEC.md"],
                           8: ["snippets/x/snippet.py"], 9: ["snippets/y/snippet.py"]},
                  status={h[2]: [{"context": "mia-data/crops", "state": "success"}]})
    a, fc = _poll(tmp_path, PollGH("o/data"), snip)
    await a.poll()
    assert snip.set == [(h[0], "mia-data/crops", "success", "no snippet in this PR")]
    assert fc.signals == [("mia-data/o/data/issue-1", "pr_update", PrUpdate(8, h[3], "open", None, 100, False))]


HISTORY = Path(__file__).parent / "fixtures" / "mia" / "dataset_history.json"


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
