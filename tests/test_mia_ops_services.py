"""mia_ops.services against a fake tmux / temporal CLI: subprocess.run is replaced by a recorder."""

import subprocess
from types import SimpleNamespace

import pytest

from mia_ops import services
from mia_ops.manifest import Manifest
from mia_ops.services import Stack, Temporal


def write_configs(tmp_path, address="localhost:7233"):
    (tmp_path / "token").write_text("t")
    (tmp_path / "harness").mkdir(exist_ok=True)
    (tmp_path / "autodev.yaml").write_text(
        f"deployment: autodev\ngithub: {{repo: o/r, token_file: {tmp_path}/token}}\n"
        f"temporal: {{address: {address}, namespace: autodev}}\n"
        f"paths: {{state_dir: {tmp_path}/ad-state, work_root: {tmp_path}/ad-work}}\n"
    )
    (tmp_path / "mia_data.yaml").write_text(
        f"github: {{data_repo: o/d, snippets_repo: o/s, catalog_repo: o/c, token_file: {tmp_path}/token}}\n"
        f"temporal: {{address: {address}, namespace: mia-data}}\n"
        f"paths: {{state_dir: {tmp_path}/md-state, work_root: {tmp_path}/md-work, storage_root: {tmp_path}/store}}\n"
        f"harness: {{dir: {tmp_path}/harness}}\n"
    )


def make_stack(tmp_path, managed=True, address="localhost:7233"):
    write_configs(tmp_path, address)
    return Stack(
        Manifest(
            sandbox_dir=tmp_path / "sandbox",
            temporal={"managed": managed, "state_dir": tmp_path / "temporal"},
            services={
                "autodev": {"app": "autodev", "config": tmp_path / "autodev.yaml"},
                "mia-data": {"app": "mia-data", "config": tmp_path / "mia_data.yaml"},
            },
        )
    )


@pytest.fixture
def fake(monkeypatch, tmp_path):
    """Records every subprocess.run; tmux sessions and Temporal health are simulated."""
    state = SimpleNamespace(calls=[], running=set(), healthy=False, namespace_rc=0)
    for name in ("python", "csub-broker"):
        (tmp_path / name).write_text("#!/bin/sh\n")
        (tmp_path / name).chmod(0o755)
    (tmp_path / "sandbox").mkdir()
    (tmp_path / "sandbox/sandbox-run.sh").write_text("#!/bin/sh\n")
    (tmp_path / "sandbox/sandbox-run.sh").chmod(0o755)
    monkeypatch.setattr(services, "PY", tmp_path / "python")
    monkeypatch.setattr(services, "BROKER", tmp_path / "csub-broker")
    monkeypatch.setattr(services.shutil, "which", lambda name: "/usr/bin/temporal")
    monkeypatch.setattr(services.time, "sleep", lambda s: None)
    monkeypatch.setenv("USER", "me")
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)

    def run(argv, **kw):
        argv = [str(a) for a in argv] if isinstance(argv, list) else argv
        state.calls.append((argv, kw))
        rc = 0
        if isinstance(argv, list) and argv[0] == "tmux":
            if argv[1] == "has-session":
                rc = 0 if argv[3].lstrip("=") in state.running else 1
            elif argv[1] == "new-session":
                state.running.add(argv[argv.index("-s") + 1])
                if argv[argv.index("-s") + 1] == "temporal":
                    state.healthy = True
            elif argv[1] == "kill-session":
                state.running.discard(argv[3].lstrip("="))
                if argv[3] == "=temporal":
                    state.healthy = False
        elif isinstance(argv, list) and argv[0] == "/usr/bin/temporal":
            if argv[1:4] == ["operator", "cluster", "health"]:
                rc = 0 if state.healthy else 1
            elif argv[1:4] == ["operator", "namespace", "describe"]:
                rc = 1  # never exists, so create always runs
            elif argv[1:4] == ["operator", "namespace", "create"]:
                rc = state.namespace_rc
        if kw.get("check") and rc:
            raise subprocess.CalledProcessError(rc, argv)
        return subprocess.CompletedProcess(argv, rc, stdout="  worker: alive\n", stderr="")

    monkeypatch.setattr(services.subprocess, "run", run)
    return state


def tmux_calls(fake, verb):
    return [c for c, _ in fake.calls if c[:2] == ["tmux", verb]]


# ---- Stack


def test_stack_loads_each_service_with_its_app(tmp_path, fake):
    s = make_stack(tmp_path)
    assert list(s.services) == ["autodev", "mia-data"] and s.temporal_address == "localhost:7233"
    assert s.services["mia-data"].cfg.harness.dir == tmp_path / "harness"
    assert s.services["autodev"].secret == tmp_path / "token"
    assert s.services["autodev"].run == services.Path("/scratch/me/autodev")


def test_stack_rejects_disagreeing_temporal_addresses(tmp_path, fake):
    write_configs(tmp_path)
    (tmp_path / "mia_data.yaml").write_text(
        (tmp_path / "mia_data.yaml").read_text().replace("localhost:7233", "other:1")
    )
    with pytest.raises(SystemExit, match="disagree"):
        Stack(
            Manifest(
                services={
                    "autodev": {"app": "autodev", "config": tmp_path / "autodev.yaml"},
                    "mia-data": {"app": "mia-data", "config": tmp_path / "mia_data.yaml"},
                }
            )
        )


def test_select(tmp_path, fake):
    s = make_stack(tmp_path)
    want_t, picked = s.select([])
    assert want_t and [p.name for p in picked] == ["autodev", "mia-data"]
    want_t, picked = s.select(["mia-data"])
    assert not want_t and [p.name for p in picked] == ["mia-data"]
    assert s.select(["temporal"]) == (True, [])
    with pytest.raises(SystemExit, match="unknown service"):
        s.select(["nope"])


# ---- Temporal


def test_temporal_host_from_address(tmp_path, fake):
    assert Temporal(make_stack(tmp_path, address="temporal.k8s.svc:7233")).host == "temporal.k8s.svc"
    assert Temporal(make_stack(tmp_path, address="temporal.k8s.svc")).host == "temporal.k8s.svc"


def test_managed_up_starts_dev_server_and_waits(tmp_path, fake):
    t = Temporal(make_stack(tmp_path))
    t.up()
    (session,) = tmux_calls(fake, "new-session")
    assert session[session.index("-s") + 1] == "temporal" and "start-dev" in session[-1]
    assert str(tmp_path / "temporal/temporal.db") in session[-1]
    assert t.status() == "running"
    t.up()  # idempotent: healthy, so no second session
    assert len(tmux_calls(fake, "new-session")) == 1


def test_managed_down_and_external_status(tmp_path, fake):
    t = Temporal(make_stack(tmp_path))
    assert t.status() == "stopped"
    fake.healthy = True  # started by hand or by the old scripts
    assert t.status() == "external"
    t.up()
    assert not tmux_calls(fake, "new-session")
    assert t.down(explicit=False) == 0 and not tmux_calls(fake, "kill-session")


def test_unmanaged_never_touches_temporal(tmp_path, fake, capsys):
    t = Temporal(make_stack(tmp_path, managed=False))
    assert t.status() == "external (unreachable)"
    with pytest.raises(SystemExit, match="unreachable"):
        t.up()
    assert not tmux_calls(fake, "new-session")
    fake.healthy = True
    assert t.status() == "external (healthy)"
    t.up()
    assert t.down(explicit=False) == 0 and "left alone" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="external"):
        t.down(explicit=True)
    assert not tmux_calls(fake, "kill-session")


def test_unmanaged_needs_cli_on_path(tmp_path, fake, monkeypatch):
    monkeypatch.setattr(services.shutil, "which", lambda name: None)
    with pytest.raises(SystemExit, match="PATH"):
        Temporal(make_stack(tmp_path, managed=False)).cli()


def test_ensure_namespace_creates_then_waits(tmp_path, fake):
    fake.healthy = True
    Temporal(make_stack(tmp_path)).ensure_namespace("autodev")
    verbs = [c[1:4] for c, _ in fake.calls if c[0] == "/usr/bin/temporal"]
    assert ["operator", "namespace", "create"] in verbs and ["workflow", "list", "-n"] in verbs


def test_ensure_namespace_failure_is_one_line(tmp_path, fake):
    fake.healthy, fake.namespace_rc = True, 1
    with pytest.raises(SystemExit, match="could not create namespace"):
        Temporal(make_stack(tmp_path)).ensure_namespace("autodev")
