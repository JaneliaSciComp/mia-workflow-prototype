"""Record a history, change a log line in the workflow, replay: must not be nondeterministic."""

import uuid
from datetime import datetime, timezone

from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from zarr_doctor import workflows
from zarr_doctor.models import IssueInput, RunRequest
from zarr_doctor.workflows import IssueWorkflow

from test_workflows import Mocks, env, wait_for  # noqa: F401  (env is a fixture)


async def test_replay_after_log_change(env, monkeypatch):
    m = Mocks()
    tq = f"tq-{uuid.uuid4()}"
    async with Worker(env.client, task_queue=tq, workflows=[IssueWorkflow], activities=m.acts()), \
            Worker(env.client, task_queue=tq + "-await", activities=m.await_act()):
        h = await env.client.start_workflow(
            IssueWorkflow.execute, IssueInput(9, "data/a/c.zarr"), id=f"replay-{uuid.uuid4()}", task_queue=tq,
            start_signal="run", start_signal_args=[RunRequest("cli", datetime.now(timezone.utc).isoformat())])
        await wait_for(h, lambda s: "finish" in m.names() and s.state == "IDLE")
        history = await h.fetch_history()

    replayer = Replayer(workflows=[IssueWorkflow], workflow_runner=UnsandboxedWorkflowRunner())
    await replayer.replay_workflow(history)  # baseline

    monkeypatch.setattr(workflows, "RUN_STARTED_LOG", "issue #%d: run %s kicked off via %s")
    result = await replayer.replay_workflow(history, raise_on_replay_failure=False)
    assert result.replay_failure is None
