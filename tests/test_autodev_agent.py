import json
import subprocess
from types import SimpleNamespace

from autodev.agent import AgentJobs, job_script, log_tail, read_result, render_prompt
from autodev.gitops import Mirror
from autodev.models import Workspace

from autodev_helpers import make_cfg, make_origin, run_git

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
    def __init__(self, states=("DONE",), existing=()):
        self.submitted, self.states, self.existing = [], list(states), list(existing)

    def submit(self, spec):
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
    ref = await j.submit(7, "k", 1, ws)
    assert ref.job_id == "777" and await j.submit(7, "k", 1, ws) == ref
    assert len(fake.submitted) == 1
    spec = fake.submitted[0]
    assert spec["claude"] is True and spec["shell"] is True and spec["cwd"] == ws.dir
    assert spec["mounts"] == [{"path": ws.dir, "mode": "rw"}, {"path": "/tools", "mode": "ro"}]
    assert spec["allow_hosts"] == ["pypi.org"] and spec["name"] == "autodev-7-r1" and "env" not in spec


async def test_submit_reuses_job_found_by_name(tmp_path):
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", name="autodev-7-r1", state="RUN")])
    j = _jobs(tmp_path, fake)
    ref = await j.submit(7, "k", 1, Workspace(str(tmp_path), "abc", "b"))
    assert ref.job_id == "555" and fake.submitted == []


async def test_api_key_mode_passes_key_in_job_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SVC_KEY", "sk-1")
    fake = FakeCsub()
    await _jobs(tmp_path, fake, api_key_env="SVC_KEY").submit(7, "k", 1, Workspace(str(tmp_path), "a", "b"))
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
