"""IssueWorkflow against Temporal's time-skipping test server, with all activities mocked."""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from zarr_doctor.models import (AckInput, AwaitInput, FailureInput, FinishInput, IssueInput, JobOutcome,
                                PublishInput, RunRequest, SubmitInput, SubmitResult)
from zarr_doctor.workflows import IssueWorkflow, PollWorkflow


@pytest.fixture(scope="session")
async def env():
    e = await WorkflowEnvironment.start_time_skipping()
    yield e
    await e.shutdown()


class Mocks:
    def __init__(self, job_status="done", await_failures=0):
        self.calls: list[tuple[str, object]] = []
        self.job_status, self.await_failures = job_status, await_failures
        self.release = asyncio.Event()
        self.block = False

    def acts(self):
        @activity.defn(name="ack_run")
        async def ack_run(a: AckInput) -> None:
            self.calls.append(("ack_run", a))

        @activity.defn(name="submit_histogram_job")
        async def submit(a: SubmitInput) -> SubmitResult:
            self.calls.append(("submit", a))
            return SubmitResult("4242", 60)

        @activity.defn(name="publish_results")
        async def publish(a: PublishInput) -> None:
            self.calls.append(("publish", a))

        @activity.defn(name="finish_run")
        async def finish(a: FinishInput) -> None:
            self.calls.append(("finish", a))

        @activity.defn(name="report_failure")
        async def report(a: FailureInput) -> None:
            self.calls.append(("report_failure", a))

        @activity.defn(name="cluster_cancel")
        async def cancel(job_id: str) -> None:
            self.calls.append(("cluster_cancel", job_id))

        return [ack_run, submit, publish, finish, report, cancel]

    def await_act(self):
        @activity.defn(name="await_job")
        async def await_job(a: AwaitInput) -> JobOutcome:
            self.calls.append(("await", a))
            if self.await_failures:
                self.await_failures -= 1
                raise ApplicationError("worker died mid-await")
            while self.block and not self.release.is_set():
                activity.heartbeat("running")
                await asyncio.sleep(0.05)
            return JobOutcome(a.job_id, self.job_status, 0 if self.job_status == "done" else 137, "1 GB", 12.0, "/x/lsf.err")

        return [await_job]

    def names(self):
        return [c[0] for c in self.calls]


async def run_scenario(env, mocks, body):
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(env.client, task_queue=tq, workflows=[IssueWorkflow, PollWorkflow], activities=mocks.acts()), \
            Worker(env.client, task_queue=tq + "-await", activities=mocks.await_act()):
        handle = await env.client.start_workflow(
            IssueWorkflow.execute, IssueInput(7, "data/a/crop-001.zarr"), id=f"zarr-doctor/issue-7-{uuid.uuid4()}",
            task_queue=tq, start_signal="run", start_signal_args=[RunRequest("cli", datetime.now(timezone.utc).isoformat())])
        await body(handle)
        return handle


async def wait_for(handle, pred, timeout=15.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        st = await handle.query(IssueWorkflow.status)
        if pred(st):
            return st
        assert asyncio.get_running_loop().time() < deadline, f"timed out; last status {st}"
        await asyncio.sleep(0.05)


async def test_run_to_done(env):
    m = Mocks()

    async def body(h):
        st = await wait_for(h, lambda s: s.runs == 1 and s.state == "IDLE" and m.names()[-1:] == ["finish"])
        assert st.job_id == "4242" and st.last_error is None

    await run_scenario(env, m, body)
    assert m.names() == ["ack_run", "submit", "await", "publish", "finish"]
    assert m.calls[3][1].outcome.status == "done"


async def test_job_failure_reports(env):
    m = Mocks(job_status="failed")

    async def body(h):
        await wait_for(h, lambda s: "report_failure" in m.names() and s.state == "IDLE")

    await run_scenario(env, m, body)
    assert m.names() == ["ack_run", "submit", "await", "report_failure"]
    rep = m.calls[-1][1]
    assert rep.stage == "await_job" and rep.outcome.exit_code == 137 and "failed" in rep.error


async def test_restart_between_submit_and_await_does_not_resubmit(env):
    m = Mocks(await_failures=1)

    async def body(h):
        await wait_for(h, lambda s: "finish" in m.names())

    await run_scenario(env, m, body)
    assert m.names().count("submit") == 1 and m.names().count("await") == 2


async def test_duplicate_run_ignored_while_running(env):
    m = Mocks()
    m.block = True

    async def body(h):
        await wait_for(h, lambda s: s.state == "RUNNING")
        await h.signal("run", RunRequest("project", "now"))
        await h.signal("run", RunRequest("project", "now"))
        await asyncio.sleep(0.3)
        m.release.set()
        st = await wait_for(h, lambda s: "finish" in m.names() and s.state == "IDLE")
        assert st.runs == 1
        # a run signal after completion starts a fresh run
        await h.signal("run", RunRequest("project", "later"))
        st = await wait_for(h, lambda s: s.runs == 2 and s.state == "IDLE" and m.names().count("finish") == 2)
        assert st.run_id.startswith("r2-")

    await run_scenario(env, m, body)
    assert m.names().count("submit") == 2


async def test_cancel_during_running(env):
    m = Mocks()
    m.block = True

    async def body(h):
        await wait_for(h, lambda s: s.state == "RUNNING")
        await h.signal("cancel")
        st = await wait_for(h, lambda s: "report_failure" in m.names() and s.state == "IDLE")
        assert st.last_error == "cancelled"

    await run_scenario(env, m, body)
    assert ("cluster_cancel", "4242") in m.calls
    assert "publish" not in m.names()
    assert m.calls[-1][1].error == "cancelled" and m.calls[-1][1].outcome.job_id == "4242"


async def test_workflow_cancellation_still_reports(env):
    m = Mocks()
    m.block = True

    async def body(h):
        await wait_for(h, lambda s: s.state == "RUNNING")
        await h.cancel()
        for _ in range(300):
            if (await h.describe()).status == WorkflowExecutionStatus.CANCELED:
                break
            await asyncio.sleep(0.05)
        assert (await h.describe()).status == WorkflowExecutionStatus.CANCELED

    await run_scenario(env, m, body)
    assert ("cluster_cancel", "4242") in m.calls
    assert m.calls[-1][0] == "report_failure" and m.calls[-1][1].error == "workflow cancelled"
