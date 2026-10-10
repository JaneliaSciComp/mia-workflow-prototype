import subprocess
from argparse import Namespace
from types import SimpleNamespace

import pytest

from autodev import ops
from autodev.cli import main


def cfg(tmp_path):
    return SimpleNamespace(deployment="autodev-me", paths=SimpleNamespace(state_dir=tmp_path / "state"))


@pytest.fixture
def tmux(monkeypatch):
    calls, state = [], {"running": True}

    def run(argv, **kw):
        calls.append(argv)
        rc = 1 if argv[:2] == ["tmux", "has-session"] and not state["running"] else 0
        return subprocess.CompletedProcess(argv, rc, stdout="  worker: alive\n", stderr="")

    monkeypatch.setattr(ops.subprocess, "run", run)
    return SimpleNamespace(calls=calls, state=state)


def test_log_files_put_host_logs_beside_state(tmp_path):
    f = ops.log_files(cfg(tmp_path))
    assert f["worker"] == tmp_path / "state/logs/worker.log"
    assert f["temporal"] == tmp_path / "state-host/logs/temporal.log"


def test_down_kills_exact_session(tmp_path, tmux):
    assert ops.cmd_down(cfg(tmp_path), Namespace()) == 0
    assert ["tmux", "kill-session", "-t", "=autodev-me"] in tmux.calls


def test_down_when_stopped_is_noop(tmp_path, tmux, capsys):
    tmux.state["running"] = False
    assert ops.cmd_down(cfg(tmp_path), Namespace()) == 0
    assert not any(c[1] == "kill-session" for c in tmux.calls)
    assert "not running" in capsys.readouterr().out


def test_ps_reports_status(tmp_path, tmux, capsys):
    assert ops.cmd_ps(cfg(tmp_path), Namespace()) == 0
    assert "autodev-me: running" in capsys.readouterr().out
    tmux.state["running"] = False
    assert ops.cmd_ps(cfg(tmp_path), Namespace()) == 1


def test_logs_tails_file(tmp_path, tmux):
    c = cfg(tmp_path)
    log = ops.log_files(c)["worker"]
    log.parent.mkdir(parents=True)
    log.write_text("x\n")
    ops.cmd_logs(c, Namespace(name="worker", lines=5, follow=True))
    assert tmux.calls[-1] == ["tail", "-n", "5", "-F", str(log)]
    ops.cmd_logs(c, Namespace(name="worker", lines=5, follow=False))
    assert tmux.calls[-1] == ["tail", "-n", "5", str(log)]


def test_logs_missing_file_exits(tmp_path):
    with pytest.raises(SystemExit):
        ops.cmd_logs(cfg(tmp_path), Namespace(name="broker", lines=5, follow=True))


def test_cli_reads_deployment_from_config(tmp_path, tmux):
    y = tmp_path / "a.yaml"
    y.write_text(f"deployment: autodev-zed\ngithub: {{repo: o/r}}\npaths: {{work_root: {tmp_path}/w, state_dir: {tmp_path}/s}}\n")
    assert main(["--config", str(y), "down"]) == 0
    assert ["tmux", "kill-session", "-t", "=autodev-zed"] in tmux.calls
