"""Prefect flows against a temporary local Prefect server (prefect_test_harness), with fake GitHub + cluster."""

import uuid
from types import SimpleNamespace

import pytest
from prefect.client.orchestration import get_client
from prefect.client.schemas.objects import StateType
from prefect.states import Crashed, Running
from prefect.testing.utilities import prefect_test_harness

from zarr_doctor import flows
from zarr_doctor.config import Config
from zarr_doctor.jobs.histogram import run as run_histogram
from zarr_doctor.models import FailureInput, JobOutcome
from zarr_doctor.state import State

VOL = "data/em-mouse-foo/crop-001_cortex.zarr"


@pytest.fixture(scope="session", autouse=True)
def prefect_server():
    with prefect_test_harness(server_startup_timeout=120):
        yield


class FakeGH:
    def __init__(self):
        self.comments, self.files, self.labels = [], {}, {}

    async def comment_once(self, n, body, marker):
        if any(marker in b for _, b in self.comments):
            return False
        self.comments.append((n, body))
        return True

    async def ensure_branch(self, b):
        return False

    async def upsert_file(self, path, content, branch, msg):
        if path in self.files:
            return False
        self.files[path] = content
        return True

    async def add_labels(self, n, labels):
        self.labels.setdefault(n, set()).update(labels)

    async def remove_label(self, n, label):
        self.labels.setdefault(n, set()).discard(label)

    def kinds(self, n):
        return [k for k in ("started", "result", "failed") for i, b in self.comments if i == n and f" {k} -->" in b]


class FakeCluster:
    """Submit 'runs' the real histogram job synchronously into the run dir; wait() can be told to fail."""

    def __init__(self, root, lmd_root, job_status="done", wait_failures=0):
        self.root, self.lmd_root = root, lmd_root
        self.job_status, self.wait_failures = job_status, wait_failures
        self.submits, self.waits, self.cancelled = [], 0, []

    def run_dir(self, issue, run_id):
        return self.root / str(issue) / run_id

    async def submit(self, issue, path, run_id):
        rd = self.run_dir(issue, run_id)
        if (rd / "job_id").exists():  # same idempotency contract as the real Cluster.submit
            return (rd / "job_id").read_text()
        run_histogram(self.lmd_root / path, rd, max_chunks=4)
        (rd / "job_id").write_text(f"job-{issue}")
        self.submits.append(run_id)
        return f"job-{issue}"

    async def wait(self, issue, run_id, job_id, on_poll, interval=30.0):
        self.waits += 1
        on_poll("running")
        if self.wait_failures:
            self.wait_failures -= 1
            raise RuntimeError("worker died mid-await")
        return JobOutcome(job_id, self.job_status, 0 if self.job_status == "done" else 137, "1 GB", 12.0)

    async def cancel(self, job_id):
        self.cancelled.append(job_id)

    def log_tail(self, issue, run_id):
        return "Traceback...\nMemoryError"


@pytest.fixture
def env(tmp_path, lmd_root, monkeypatch):
    cfg = Config(lmd_root=lmd_root, results_dir=tmp_path / "results", state_dir=tmp_path / "state",
                 github={"repo": "o/r"}, trigger={"mode": "label"})
    d = flows.Deps(cfg, FakeGH(), State(cfg.state_dir), FakeCluster(cfg.results_dir, lmd_root))
    flows.set_deps(d)
    # keep retries but drop the waits between them
    for name in ("ack_run", "submit_histogram_job", "await_job", "publish_results", "finish_run", "report_failure"):
        monkeypatch.setattr(flows, name, getattr(flows, name).with_options(retry_delay_seconds=0))
    yield d
    flows.set_deps(None)


async def test_run_to_done(env):
    run_id = await flows.histogram_run(5, VOL, "label")
    assert run_id.startswith("p-") and env.gh.kinds(5) == ["started", "result"]
    assert env.gh.labels[5] == {"zd:done"}
    assert sorted(env.gh.files) == [f"results/5/{run_id}/{n}" for n in ("histogram.json", "histogram.png", "stats.json")]
    assert env.cluster.submits == [run_id]


async def test_job_failure_reports_and_fails_flow(env):
    env.cluster.job_status = "failed"
    state = await flows.histogram_run(6, VOL, "label", return_state=True)
    assert state.is_failed()
    assert env.gh.kinds(6) == ["started", "failed"] and env.gh.labels[6] == {"zd:failed"}
    body = env.gh.comments[-1][1]
    assert "**Stage:** await_job" in body and "exit:** 137" in body and "MemoryError" in body


async def test_task_retry_does_not_resubmit(env):
    env.cluster.wait_failures = 1
    await flows.histogram_run(7, VOL, "cli")
    assert len(env.cluster.submits) == 1 and env.cluster.waits == 2 and env.gh.kinds(7) == ["started", "result"]


async def test_retries_exhausted_reports_failure(env):
    env.cluster.wait_failures = 99  # await_job has retries=2 -> 3 attempts
    state = await flows.histogram_run(8, VOL, "cli", return_state=True)
    assert state.is_failed() and env.cluster.waits == 3
    assert "worker died mid-await" in env.gh.comments[-1][1]


async def test_rerun_of_same_run_is_idempotent(env):
    """A crashed flow run is rescheduled with the same id -> same run_id; every step must be a no-op the 2nd time."""
    rid = "p-deadbeef"
    for _ in range(2):
        await flows.ack_run.fn(9, rid, "cli")
        job = await flows.submit_histogram_job.fn(9, VOL, rid)
        await flows.publish_results.fn(9, rid, JobOutcome(job, "done", 0))
    assert len(env.cluster.submits) == 1 and env.gh.kinds(9) == ["started", "result"] and len(env.gh.files) == 3


async def test_cancellation_hook_kills_job_and_reports(env):
    fr = SimpleNamespace(id=uuid.uuid4(), parameters={"issue": 10, "path": VOL})
    rid = flows.run_id_for(fr.id)
    await env.cluster.submit(10, VOL, rid)
    await flows.on_cancelled(None, fr, None)
    assert env.cluster.cancelled == ["job-10"]
    assert env.gh.kinds(10) == ["failed"] and "**Error:** cancelled" in env.gh.comments[-1][1]


async def test_start_run_skips_issue_with_active_run(env, monkeypatch):
    started = []

    async def fake_run_deployment(name, **kw):
        started.append(kw["parameters"]["issue"])
        return SimpleNamespace(id=uuid.uuid4(), name="x")

    monkeypatch.setattr(flows, "run_deployment", fake_run_deployment)
    async with get_client() as client:
        await client.create_flow_run(flows.histogram_run, parameters={"issue": 11, "path": VOL},
                                     tags=[flows.issue_tag(11)], state=Running())
        assert await flows.start_run(client, 11, VOL, "label") is None  # duplicate: ignored
        assert await flows.start_run(client, 12, VOL, "label") is not None
    assert started == [12]


async def test_recover_crashed_reschedules_then_gives_up(env, monkeypatch):
    async with get_client() as client:
        fr = await client.create_flow_run(flows.histogram_run, parameters={"issue": 13, "path": VOL},
                                          tags=[flows.issue_tag(13)], state=Crashed(message="node died"))
        assert await flows.recover_crashed(client, env) == [f"rescheduled {fr.name} ({fr.id})"]
        assert (await client.read_flow_run(fr.id)).state.type == StateType.SCHEDULED

        await client.set_flow_run_state(fr.id, Crashed(message="node died again"), force=True)
        monkeypatch.setattr(flows, "MAX_ATTEMPTS", 0)
        assert await flows.recover_crashed(client, env) == [f"reported {fr.name} ({fr.id})"]
        assert flows.REPORTED_TAG in (await client.read_flow_run(fr.id)).tags
        assert await flows.recover_crashed(client, env) == []  # reported once only
    assert env.gh.kinds(13) == ["failed"] and "node died again" in env.gh.comments[-1][1]


def test_render_failure_without_outcome():
    cfg = Config(lmd_root="/x", results_dir="/y", trigger={"mode": "project", "project_number": 1})
    body = flows.render_failure(cfg, FailureInput(1, "p-1", "crashed", "boom"), "tail")
    assert "**Stage:** crashed\n" in body and "Move the issue back to **Run**" in body


def test_ascii_histogram():
    lines = flows.ascii_histogram(list(range(33)), [1] * 16 + [3] * 16).splitlines()
    assert len(lines) == 16 and lines[-1].endswith("#" * 40) and lines[0].count("#") == round(40 * 2 / 6)
