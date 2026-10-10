import pytest

from autodev.cli import main, proxy_target


def test_proxy_target_from_sandbox_env():
    assert proxy_target({"https_proxy": "http://127.0.0.1:41234"}) == "127.0.0.1:41234"
    assert proxy_target({"HTTPS_PROXY": "http://127.0.0.1:9"}) == "127.0.0.1:9"
    assert proxy_target({}) is None


def test_help_lists_commands(capsys):
    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for cmd in ("check", "init-labels", "worker", "schedule"):
        assert cmd in out


def test_csub_problems_roots_are_path_prefixes():
    from pathlib import Path
    from types import SimpleNamespace

    from autodev.cli import _csub_problems

    cfg = SimpleNamespace(agent=SimpleNamespace(allow_hosts=[]), paths=SimpleNamespace(work_root=Path("/x/work2")))
    hosts = ["api.anthropic.com", "claude.ai", "platform.claude.com"]
    probe = SimpleNamespace(allow_claude=True, allowed_hosts=hosts, allowed_roots=["/x/work"])
    assert any("allowed_roots" in m for m in _csub_problems(cfg, probe))
    probe.allowed_roots = ["/x"]
    assert _csub_problems(cfg, probe) == []
