"""IssueWorkflow on Temporal's time-skipping server, with every activity mocked."""

import json
import os
import re
import uuid
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from autodev import workflows
from autodev.models import (
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
    SubmitInput,
    Workspace,
)
from autodev.workflows import IssueWorkflow


@pytest.fixture(scope="session")
async def env():
    e = await WorkflowEnvironment.start_time_skipping()
    yield e
    await e.shutdown()


def snap(state="open", sha="h1", approved=False, checks="none", feedback=()):
    return PrSnapshot(5, state, sha, approved, checks, list(feedback))


class Mocks:
    def __init__(self, snaps, existing_pr=None, job_state="DONE", exit_code=0, publish_pr=5, await_failures=0,
                 merge_ok=True, observe_failures=0, report_failures=0, observe_fatal=False):
        self.observe_failures, self.report_failures, self.reported = observe_failures, report_failures, 0
        self.observe_fatal = observe_fatal
        self.calls: list[tuple[str, object]] = []
        self.snaps = list(snaps)
        self.existing_pr, self.job_state, self.exit_code = existing_pr, job_state, exit_code
        self.publish_pr, self.await_failures, self.merge_ok = publish_pr, await_failures, merge_ok

    def acts(self):
        @activity.defn(name="set_stage")
        async def set_stage(a: StageInput) -> None:
            self.calls.append(("set_stage", a.stage))

        @activity.defn(name="comment")
        async def comment(a: CommentInput) -> None:
            self.calls.append(("comment", a))

        @activity.defn(name="find_pr")
        async def find_pr(issue: int) -> PrRef:
            self.calls.append(("find_pr", issue))
            return PrRef(self.existing_pr, "h0" if self.existing_pr else None)

        @activity.defn(name="prepare_round")
        async def prepare(a: RoundInput) -> Workspace:
            self.calls.append(("prepare", a))
            return Workspace(f"/w/round-{a.round}", "base", "autodev/issue-7")

        @activity.defn(name="submit_agent")
        async def submit(a: SubmitInput) -> JobRef:
            self.calls.append(("submit", a))
            return JobRef(f"job-{a.round}", 60)

        @activity.defn(name="publish_round")
        async def publish(a: PublishInput) -> PrRef:
            self.calls.append(("publish", a))
            if a.pr is None:
                return PrRef(self.publish_pr, "h1" if self.publish_pr else None)
            return PrRef(a.pr, f"h{a.round}")

        @activity.defn(name="observe_pr")
        async def observe(a: ObserveInput) -> PrSnapshot:
            self.calls.append(("observe", a.pr))
            if self.observe_fatal:
                raise ApplicationError("PR is gone", non_retryable=True)
            if self.observe_failures:
                self.observe_failures -= 1
                raise ApplicationError("github 502")
            return self.snaps.pop(0) if len(self.snaps) > 1 else self.snaps[0]

        @activity.defn(name="merge_pr")
        async def merge(a: MergeInput) -> MergeResult:
            self.calls.append(("merge", a))
            return MergeResult(self.merge_ok, "merged" if self.merge_ok else "405 not mergeable")

        @activity.defn(name="report_failure")
        async def report(a: FailureInput) -> None:
            self.calls.append(("report_failure", a))
            if self.report_failures:
                self.report_failures -= 1
                raise ApplicationError("github 502")
            self.reported += 1

        return [set_stage, comment, find_pr, prepare, submit, publish, observe, merge, report]

    def await_act(self):
        @activity.defn(name="await_agent")
        async def await_agent(a: AwaitInput) -> JobOutcome:
            self.calls.append(("await", a.job_id))
            if self.await_failures:
                self.await_failures -= 1
                raise ApplicationError("worker died mid-await")
            return JobOutcome(a.job_id, self.job_state, self.exit_code)

        return [await_agent]

    def names(self):
        return [c[0] for c in self.calls]

    def of(self, name):
        return [c[1] for c in self.calls if c[0] == name]


async def start(env, m, inp=None, runner=None):
    tq = f"tq-{uuid.uuid4()}"
    kw = {"workflow_runner": runner} if runner else {}
    main = Worker(env.client, task_queue=tq, workflows=[IssueWorkflow], activities=m.acts(), **kw)
    waits = Worker(env.client, task_queue=tq + "-await", activities=m.await_act())
    async with main, waits:
        h = await env.client.start_workflow(IssueWorkflow.execute, inp or IssueInput(7), id=f"wf-{uuid.uuid4()}",
                                            task_queue=tq)
        return await h.result(), h


async def run(env, m, inp=None, runner=None):
    return (await start(env, m, inp, runner))[0]


async def test_issue_to_merge(env):
    m = Mocks([snap(), snap(approved=True, checks="success")])
    assert await run(env, m) == "merged"
    assert m.of("set_stage") == ["working", "review", "done"]
    assert [r.round for r in m.of("prepare")] == [1] and m.of("prepare")[0].pr is None
    assert m.of("merge")[0].sha == "h1" and m.names().count("observe") == 2
    assert any("job-1" in c.body and c.number == 7 for c in m.of("comment"))


async def test_existing_pr_is_resumed_not_redone(env):
    m = Mocks([snap(approved=True)], existing_pr=9)
    assert await run(env, m) == "merged"
    assert "prepare" not in m.names() and m.of("observe") == [9]


async def test_no_commits_reports_failure(env):
    m = Mocks([snap()], publish_pr=None)
    assert await run(env, m) == "failed"
    f = m.of("report_failure")[0]
    assert "no commits" in f.error and f.workspace.dir == "/w/round-1" and "observe" not in m.names()


async def test_failed_job_reports_and_does_not_publish(env):
    m = Mocks([snap()], job_state="EXIT", exit_code=1)
    assert await run(env, m) == "failed"
    f = m.of("report_failure")[0]
    assert "job-1" in f.error and "EXIT" in f.error and f.workspace.dir == "/w/round-1"
    assert "publish" not in m.names()


async def test_worker_death_mid_await_does_not_resubmit(env):
    m = Mocks([snap(state="merged")], await_failures=1)
    assert await run(env, m) == "merged"
    assert m.names().count("submit") == 1 and m.names().count("await") == 2


async def test_feedback_revised_once(env):
    fb = Feedback("review-1", "rename foo")
    m = Mocks([snap(feedback=[fb]), snap(sha="h2", feedback=[fb]), snap(sha="h2", approved=True)])
    assert await run(env, m) == "merged"
    rounds = m.of("prepare")
    assert [r.round for r in rounds] == [1, 2]
    assert rounds[1].feedback == "rename foo" and rounds[1].pr == 5
    assert m.of("publish")[1].feedback_id == "review-1" and m.of("merge")[0].sha == "h2"


async def test_revision_limit_escalates_but_approval_still_merges(env):
    f1, f2 = Feedback("review-1", "a"), Feedback("comment-2", "b")
    m = Mocks([snap(feedback=[f1]), snap(feedback=[f1, f2]), snap(feedback=[f1, f2], approved=True)])
    assert await run(env, m, IssueInput(7, max_revisions=1)) == "merged"
    assert len(m.of("prepare")) == 2
    assert any("revision limit" in c.body for c in m.of("comment"))


async def test_closed_pr_stops(env):
    m = Mocks([snap(state="closed")])
    assert await run(env, m) == "closed"
    assert m.of("set_stage")[-1] is None and "merge" not in m.names()


async def test_merge_failure_comments_then_external_merge_finishes(env):
    m = Mocks([snap(approved=True), snap(state="merged")], merge_ok=False)
    assert await run(env, m) == "merged"
    assert any("merging failed" in c.body for c in m.of("comment"))


async def test_continue_as_new_keeps_handled_feedback(env, monkeypatch):
    monkeypatch.setattr(workflows, "MAX_POLLS_PER_EXECUTION", 2)
    fb = Feedback("review-1", "x")
    m = Mocks([snap(feedback=[fb])] + [snap(sha="h2", feedback=[fb])] * 3 + [snap(sha="h2", approved=True)])
    assert await run(env, m, runner=UnsandboxedWorkflowRunner()) == "merged"
    assert len(m.of("prepare")) == 2


async def test_replay_after_log_change(env, monkeypatch):
    _, h = await start(env, Mocks([snap(approved=True)]))
    history = await h.fetch_history()
    replayer = Replayer(workflows=[IssueWorkflow], workflow_runner=UnsandboxedWorkflowRunner())
    await replayer.replay_workflow(history)
    monkeypatch.setattr(workflows, "ROUND_DONE_LOG", "issue #%d, round %d: %s -> %s")
    assert (await replayer.replay_workflow(history, raise_on_replay_failure=False)).replay_failure is None


HISTORY = Path(__file__).parent / "fixtures" / "autodev_issue_history.json"


def _scrub(obj):
    """Drop worker/client identities (pid@hostname) from a recorded history."""
    if isinstance(obj, dict):
        return {k: "test" if k == "identity" else _scrub(v) for k, v in obj.items()}
    return [_scrub(v) for v in obj] if isinstance(obj, list) else obj


async def test_replay_recorded_history(env):
    """A run recorded with an earlier IssueWorkflow (round 1, a revision round, merge) must replay on today's.
    This fails when a change to workflow code alters the commands it issues, which breaks in-flight runs. If the
    change is intentional, guard it with workflow.patched() (or Worker Versioning) and re-record the fixture:
    AUTODEV_RECORD_HISTORY=1 pixi run pytest tests/test_autodev_workflows.py -k recorded_history"""
    if os.environ.get("AUTODEV_RECORD_HISTORY"):
        fb = Feedback("review-1", "rename foo")
        _, h = await start(env, Mocks([snap(feedback=[fb]), snap(sha="h2", feedback=[fb]),
                                       snap(sha="h2", approved=True, checks="success")]))
        HISTORY.parent.mkdir(exist_ok=True)
        HISTORY.write_text(json.dumps(_scrub((await h.fetch_history()).to_json_dict()), indent=1) + "\n")
    history = WorkflowHistory.from_json("autodev-recorded", HISTORY.read_text())
    await Replayer(workflows=[IssueWorkflow]).replay_workflow(history)


async def test_transient_observe_failures_do_not_fail_the_workflow(env):
    m = Mocks([snap(approved=True)], observe_failures=8)
    assert await run(env, m) == "merged"
    assert m.names().count("observe") == 9


async def test_report_failure_survives_transient_errors(env):
    m = Mocks([snap()], job_state="EXIT", exit_code=1, report_failures=7)
    assert await run(env, m) == "failed"
    assert m.names().count("report_failure") == 8 and m.reported == 1


async def test_failure_after_publish_has_no_stale_log_tail(env):
    """The round's logs belong to the round; a later observe/merge failure must not attach them."""
    m = Mocks([snap()], observe_fatal=True)
    assert await run(env, m) == "failed"
    f = m.of("report_failure")[0]
    assert "PR is gone" in f.error and f.workspace is None


@pytest.mark.parametrize("key", ["../../etc", "C0FFEE01", "c0ffee0"])
async def test_bad_key_is_replaced_by_run_id(env, key):
    """The key names directories; one sent by a hand-started workflow must not reach the activities."""
    m = Mocks([snap(state="merged")])
    assert await run(env, m, IssueInput(7, key=key)) == "merged"
    used = m.of("prepare")[0].key
    assert re.fullmatch(r"[0-9a-f]{8}", used) and m.of("submit")[0].key == used
