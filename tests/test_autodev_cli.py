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
    assert "restart" not in out and "attach" not in out


def test_csub_problems_roots_are_path_prefixes():
    from pathlib import Path
    from types import SimpleNamespace

    from autodev.cli import _csub_problems

    cfg = SimpleNamespace(agent=SimpleNamespace(allow_hosts=[]), paths=SimpleNamespace(work_root=Path("/x/work2")))
    hosts = ["api.anthropic.com", "claude.ai", "platform.claude.com"]
    probe = SimpleNamespace(agents=["claude"], allowed_hosts=hosts, allowed_roots=["/x/work"])
    assert any("allowed_roots" in m for m in _csub_problems(cfg, probe))
    probe.allowed_roots = ["/x"]
    assert _csub_problems(cfg, probe) == []


async def test_apply_schedule_updates_when_the_server_says_already_registered():
    """A plain ALREADY_EXISTS (no WorkflowExecutionAlreadyStartedFailure detail, as newer servers and the proxy
    answer) must lead to an update, like the SDK's ScheduleAlreadyRunningError does."""
    from temporalio.client import ScheduleAlreadyRunningError
    from temporalio.service import RPCError, RPCStatusCode

    from autodev.cli import apply_schedule

    class Handle:
        updates: list = []

        async def update(self, fn):
            self.updates.append(fn)

    class Client:
        def __init__(self, error):
            self.error, self.created = error, []

        async def create_schedule(self, schedule_id, schedule):
            if self.error:
                raise self.error
            self.created.append(schedule_id)

        def get_workflow_handle(self, *_):
            raise AssertionError

        def get_schedule_handle(self, schedule_id):
            return Handle()

    plain = RPCError('schedule "autodev-x-poll" is already registered', RPCStatusCode.ALREADY_EXISTS, b"")
    assert await apply_schedule(Client(plain), "autodev-x-poll", object()) == "updated"
    assert await apply_schedule(Client(ScheduleAlreadyRunningError()), "autodev-x-poll", object()) == "updated"
    assert len(Handle.updates) == 2
    fresh = Client(None)
    assert await apply_schedule(fresh, "autodev-x-poll", object()) == "created" and fresh.created == ["autodev-x-poll"]
    with pytest.raises(RPCError):  # any other failure still surfaces
        await apply_schedule(Client(RPCError("down", RPCStatusCode.UNAVAILABLE, b"")), "autodev-x-poll", object())
