import json
import os
import subprocess
import threading
from types import SimpleNamespace

import pytest
from csub.protocol import CsubError, JobSpec, ProtocolError
from csub.transport.base import TransportError
from temporalio.exceptions import ApplicationError

from autodev.activities import Activities
from autodev.agent import AgentJobs, csub_client, job_script, log_tail, read_result, render_prompt
from autodev.gitops import Mirror
from autodev.models import SubmitInput, Workspace

from autodev_helpers import K, make_cfg, make_origin, run_git

ISSUE = {"number": 7, "title": "Add feature", "body": "Please add feature.txt"}


def test_prompt_has_issue_and_human_discussion_only(tmp_path):
    cfg = make_cfg(tmp_path)
    comments = [{"user": {"login": "alice"}, "body": "use lowercase"},
                {"user": {"login": "bot"}, "body": "Started <!-- autodev: started k-1 -->"}]
    p = render_prompt(cfg, ISSUE, comments, "autodev/issue-7", "abc", None, None)
    assert "#7 in o/r" in p and "Add feature" in p and "Please add feature.txt" in p
    assert "use lowercase" in p and "Started" not in p and "Requested changes" not in p
    p2 = render_prompt(cfg, ISSUE, [], "autodev/issue-7", "abc", 5, "rename it")
    assert "Pull request #5" in p2 and "rename it" in p2


def test_job_script_runs_claude_and_bundles(tmp_path):
    """Run the real script with a fake `claude` on PATH, against a real clone."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "claude"
    fake.write_text('#!/bin/sh\necho x > feature.txt\ngit add feature.txt\ngit commit -qm "add feature"\n'
                    'echo \'{"type":"result","result":"Added feature.txt","total_cost_usd":0.12}\'\n')
    fake.chmod(0o755)
    cfg = make_cfg(tmp_path, agent={"path_prepend": [str(bin_dir)], "model": "it's-quoted"})
    m = Mirror(tmp_path / "mirror.git", str(make_origin(tmp_path)))
    m.sync()
    d = tmp_path / "round-1"
    base = m.workspace(d, "main", "autodev/issue-7", "https://example.invalid/r.git")
    (d / "prompt.md").write_text("do it")
    script = job_script(cfg, base)
    r = subprocess.run(["bash", "-c", script], cwd=d)
    assert r.returncode == 0
    assert (d / "exit_code").read_text().strip() == "0"
    assert read_result(d) == ("Added feature.txt", 0.12)
    assert run_git("bundle", "list-heads", str(d / "result.bundle"), cwd=d)


def test_job_script_budget_flag(tmp_path):
    assert "--max-budget-usd 5.0" in job_script(make_cfg(tmp_path, agent={"max_budget_usd": 5}), "abc")
    assert "--max-budget-usd" not in job_script(make_cfg(tmp_path), "abc")


def test_read_result_tolerates_garbage(tmp_path):
    assert read_result(tmp_path)[0].startswith("(the agent left no readable result.json")
    (tmp_path / "result.json").write_text("[1, 2]")
    assert read_result(tmp_path)[1] is None
    (tmp_path / "result.json").write_text(json.dumps({"result": "x" * 50000, "total_cost_usd": "free"}))
    text, cost = read_result(tmp_path)
    assert len(text) < 20100 and text.endswith("(truncated)") and cost is None


def test_log_tail_reads_claude_and_job_stderr(tmp_path):
    (tmp_path / "claude.err").write_text("\n".join(f"line {i}" for i in range(100)))
    (tmp_path / ".csub" / "jobs" / "9").mkdir(parents=True)
    (tmp_path / ".csub" / "jobs" / "9" / "stderr").write_text("bwrap: oops\n")
    t = log_tail(tmp_path, n=5)
    assert "line 99" in t and "line 90" not in t and "bwrap: oops" in t


class FakeCsub:
    def __init__(self, states=("DONE",), existing=(), error=None):
        self.submitted, self.states, self.existing, self.error = [], list(states), list(existing), error

    def submit(self, spec):
        if self.error:
            raise self.error
        JobSpec.from_dict(spec)
        self.submitted.append(spec)
        return SimpleNamespace(job_id="777", walltime_min=spec["walltime_min"])

    def status(self, ids=None):
        if ids is None:
            return self.existing
        st = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return [SimpleNamespace(job_id=ids[0], state=st, exit_code=0 if st == "DONE" else None, name=None,
                                terminal=st in ("DONE", "EXIT", "UNKNOWN"))]


def _jobs(tmp_path, fake, **agent):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)

    j = AgentJobs(make_cfg(tmp_path, agent=agent), client_factory=lambda: fake, sleep=sleep)
    j.sleeps = sleeps
    return j


async def test_submit_is_idempotent_and_scoped(tmp_path):
    fake = FakeCsub()
    j = _jobs(tmp_path, fake, extra_mounts=["/tools:ro"], allow_hosts=["pypi.org"])
    ws = Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7")
    ref = await j.submit(7, K, 1, ws)
    assert ref.job_id == "777" and await j.submit(7, K, 1, ws) == ref
    assert len(fake.submitted) == 1
    spec = fake.submitted[0]
    assert spec["claude"] is True and spec["shell"] is True and spec["cwd"] == ws.dir
    assert spec["mounts"] == [{"path": ws.dir, "mode": "rw"}, {"path": "/tools", "mode": "ro"}]
    assert spec["allow_hosts"] == ["pypi.org"] and spec["name"] == "autodev-7-r1" and "env" not in spec


async def test_submit_reuses_job_found_by_name(tmp_path):
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", name="autodev-7-r1", cwd=str(tmp_path), state="RUN")])
    j = _jobs(tmp_path, fake)
    ref = await j.submit(7, K, 1, Workspace(str(tmp_path), "abc", "b"))
    assert ref.job_id == "555" and fake.submitted == []


async def test_submit_ignores_same_name_job_from_another_round_dir(tmp_path):
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", name="autodev-7-r1", cwd="/old/run", state="DONE")])
    ref = await _jobs(tmp_path, fake).submit(7, "c0ffee02", 1, Workspace(str(tmp_path), "abc", "b"))
    assert ref.job_id == "777" and len(fake.submitted) == 1


async def test_api_key_mode_passes_key_in_job_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SVC_KEY", "sk-1")
    fake = FakeCsub()
    await _jobs(tmp_path, fake, api_key_env="SVC_KEY").submit(7, K, 1, Workspace(str(tmp_path), "a", "b"))
    assert fake.submitted[0]["env"] == {"ANTHROPIC_API_KEY": "sk-1"}


async def test_wait_heartbeats_until_terminal(tmp_path):
    fake = FakeCsub(states=["PEND", "RUN", "DONE"])
    j = _jobs(tmp_path, fake)
    beats = []
    out = await j.wait("777", Workspace(str(tmp_path), "a", "b"), beats.append)
    assert (out.state, out.exit_code) == ("DONE", 0)
    assert beats == ["PEND", "RUN", "DONE"] and j.sleeps == [60.0, 60.0]


async def test_unknown_state_uses_exit_code_file(tmp_path):
    (tmp_path / "exit_code").write_text("0\n")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(tmp_path), "a", "b"),
                                                                    lambda s: None)
    assert (out.state, out.exit_code) == ("DONE", 0)


def test_log_tail_ignores_symlinks(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("TOKEN-123")
    d = tmp_path / "round"
    (d / ".csub").mkdir(parents=True)
    (d / "claude.err").symlink_to(secret)
    (d / ".csub" / "jobs").symlink_to(tmp_path / "elsewhere")
    (tmp_path / "elsewhere" / "9").mkdir(parents=True)
    (tmp_path / "elsewhere" / "9" / "stderr").write_text("TOKEN-456")
    assert "TOKEN" not in log_tail(d)


def test_read_result_fifo_does_not_block(tmp_path):
    os.mkfifo(tmp_path / "result.json")
    out = []
    t = threading.Thread(target=lambda: out.append(read_result(tmp_path)), daemon=True)
    t.start()
    t.join(5)
    assert out and out[0][0].startswith("(the agent left no readable result.json")


def test_read_result_deeply_nested_json(tmp_path):
    (tmp_path / "result.json").write_text("[" * 100000)
    assert read_result(tmp_path)[0].startswith("(the agent left no readable")


async def test_exit_code_symlink_and_garbage_ignored(tmp_path):
    secret = tmp_path / "secret"
    secret.write_text("0")
    d = tmp_path / "r"
    d.mkdir()
    (d / "exit_code").symlink_to(secret)
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(d), "a", "b"), lambda s: None)
    assert (out.state, out.exit_code) == ("UNKNOWN", None)
    (d / "exit_code").unlink()
    (d / "exit_code").write_text("\u00b2")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(d), "a", "b"), lambda s: None)
    assert out.exit_code is None


def test_csub_client_uses_deployment_session(tmp_path, monkeypatch):
    monkeypatch.setenv("CSUB_TRANSPORT", "local")
    c = csub_client(make_cfg(tmp_path, deployment="mydep"))
    assert c.config.session == "mydep"


async def test_symlink_loops_are_survived(tmp_path):
    d = tmp_path / "r"
    (d / ".csub" / "jobs" / "9").mkdir(parents=True)
    for rel in ("claude.err", "result.json", "exit_code", ".csub/jobs/9/stderr"):
        (d / rel).symlink_to(d / rel)
    assert log_tail(d) == ""
    assert read_result(d)[0].startswith("(the agent left no readable result.json")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(d), "a", "b"), lambda s: None)
    assert out.exit_code is None


def test_log_tail_reads_only_newest_job_dirs(tmp_path):
    for i in range(10):
        (tmp_path / ".csub" / "jobs" / str(i)).mkdir(parents=True)
        (tmp_path / ".csub" / "jobs" / str(i) / "stderr").write_text(f"err{i}")
    t = log_tail(tmp_path)
    assert "err9" in t and "err7" in t and "err6" not in t


async def test_submit_reuses_job_when_work_root_is_symlinked(tmp_path):
    """Config canonicalizes work_root, so round dirs (and the csub cwd/mount) match what the broker records."""
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real)
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", name="x", cwd=str(real.resolve() / "r1"), state="RUN"),
                              SimpleNamespace(job_id="556", name="y", state="RUN")])
    cfg = make_cfg(tmp_path, paths={"state_dir": str(tmp_path / "state"), "work_root": str(tmp_path / "link")})
    j = AgentJobs(cfg, client_factory=lambda: fake)
    ws = Workspace(str(j.cfg.paths.work_root / "r1"), "abc", "b")
    assert ws.dir == str(real.resolve() / "r1")
    ref = await j.submit(7, K, 1, ws)
    assert ref.job_id == "555" and fake.submitted == []


@pytest.mark.parametrize("err,permanent", [
    (CsubError("policy_violation", "mount not under allowed_roots"), True),
    (CsubError("invalid_request", "bad spec"), True),
    (ProtocolError("response: not valid JSON"), True),
    (TransportError("cannot reach broker socket"), False),
    (CsubError("lsf_error", "bsub timed out"), False),
])
async def test_submit_rejection_is_permanent_transport_error_is_not(tmp_path, err, permanent):
    j = _jobs(tmp_path, FakeCsub(error=err))
    a = Activities(j.cfg, None, None, j, "bot")
    with pytest.raises(Exception) as e:
        await a.submit_agent(SubmitInput(7, K, 1, Workspace(str(tmp_path), "abc", "b")))
    if permanent:
        assert isinstance(e.value, ApplicationError) and e.value.non_retryable and err.code in str(e.value)
    else:
        assert e.value is err


@pytest.mark.parametrize("key", ["", "../../tmp", "C0FFEE01", "c0ffee0", "c0ffee01/.."])
async def test_submit_refuses_a_key_that_is_not_8_hex(tmp_path, key):
    j = _jobs(tmp_path, FakeCsub())
    with pytest.raises(ApplicationError) as e:
        await Activities(j.cfg, None, None, j, "bot").submit_agent(SubmitInput(7, key, 1, Workspace(str(tmp_path), "a", "b")))
    assert e.value.non_retryable and not (tmp_path / "state" / "jobs").exists()


async def test_job_name_leaves_out_the_deployment(tmp_path):
    # csub rejects job names containing the submitter's username, and personal deployments are autodev-<user>
    fake = FakeCsub()
    j = AgentJobs(make_cfg(tmp_path, deployment="autodev-someuser"), client_factory=lambda: fake)
    await j.submit(7, K, 1, Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev-someuser/issue-7"))
    assert fake.submitted[0]["name"] == "autodev-7-r1" and fake.submitted[0]["session"] == "autodev-someuser"
