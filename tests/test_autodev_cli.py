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
