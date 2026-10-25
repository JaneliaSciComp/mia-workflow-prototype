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


def write_mia_yaml(tmp_path, address="localhost:7233", token_mode=0o600, local_root=None):
    """mia.yaml in the new shape; `local_root=""` leaves the key out."""
    (tmp_path / "token").write_text("t" * 64)
    (tmp_path / "token").chmod(token_mode)
    (tmp_path / "harness").mkdir(exist_ok=True)
    local = tmp_path / "local" if local_root is None else local_root
    paths = (
        f"state_root: {tmp_path}/state, work_root: {tmp_path}/w, storage_root: {tmp_path}/store, "
        f"staged_root: {tmp_path}/staged" + (f", local_root: {local}" if local else "")
    )
    (tmp_path / "mia.yaml").write_text(
        f"temporal: {{managed: true, state_dir: {tmp_path}/temporal, address: {address}}}\n"
        f"github: {{auth: token, token_file: {tmp_path}/token, catalog_repo: o/c, snippets_repo: o/s}}\n"
        f"paths: {{{paths}}}\nharness: {{dir: {tmp_path}/harness}}\n"
        f"service: {{url: http://buffer-host.example.org:8765, token_file: {tmp_path}/token}}\n"
        "services:\n"
        "  autodev: {app: autodev, github: {repo: o/r}, temporal: {namespace: autodev}}\n"
        "  snippets: {app: autodev, github: {repo: o/s2}}\n"
        "  mia-data: {app: mia-data, github: {data_repo: o/d}, temporal: {namespace: mia-data}}\n"
        "  mia-buffers: {app: mia-buffers, github: {buffers_repo: o/b, datasets_repo: o/d}, "
        "temporal: {namespace: mia-buffers}}\n"
    )
    return tmp_path / "mia.yaml"


def make_stack_v2(tmp_path, **kw):
    from mia_ops.manifest import load_manifest

    manifest = load_manifest(write_mia_yaml(tmp_path, **kw))
    manifest.sandbox_dir = tmp_path / "sandbox"
    return Stack(manifest)


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
    monkeypatch.setattr(services, "SCRATCH", tmp_path / "scratch")
    monkeypatch.setattr(services.shutil, "which", lambda name: "/usr/bin/temporal")
    monkeypatch.setattr(services.time, "sleep", lambda s: None)
    monkeypatch.setenv("USER", "me")
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    monkeypatch.setattr(services, "require_local_dir", lambda p: p.mkdir(parents=True, exist_ok=True))

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
    assert s.services["autodev"].run == tmp_path / "scratch/me/autodev"


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


# ---- service_up


def worker_command(fake, session):
    (call,) = [
        c
        for c, _ in fake.calls
        if c[:2] == ["tmux", "new-window"]
        and c[c.index("-t") + 1] == f"={session}"
        and c[c.index("-n") + 1] == "worker"
    ]
    return call[-1]


def schedule_calls(fake):
    return [
        (c, kw)
        for c, kw in fake.calls
        if c[1] == "-m"
        and c[2] in ("autodev.cli", "mia_data.cli", "mia_buffers.cli")
        and c[3:5] == ["schedule", "apply"]
    ]


def test_up_starts_temporal_then_broker_and_worker(tmp_path, fake):
    s = make_stack(tmp_path)
    t = Temporal(s)
    assert services.service_up(s.services["autodev"], s, t) == 0
    sessions = [c[c.index("-s") + 1] for c in tmux_calls(fake, "new-session")]
    assert sessions == ["temporal", "autodev"]
    broker = tmux_calls(fake, "new-session")[1][-1]
    assert "csub-broker" in broker and f"{tmp_path}/scratch/me/autodev/csub.sock" in broker and "broker.log" in broker
    worker = worker_command(fake, "autodev")
    for piece in (
        f"--ro {services.PROJECT_DIR}",
        f"--ro {tmp_path}/autodev.yaml",
        f"--ro {tmp_path}/token",
        f"--rw {tmp_path}/ad-work",
        f"--rw {tmp_path}/scratch/me/autodev",
        "--allow github.com --allow api.github.com --allow localhost",
        f"AUTODEV_CONFIG={tmp_path}/autodev.yaml",
        f"CSUB_TRANSPORT=unix CSUB_SOCKET={tmp_path}/scratch/me/autodev/csub.sock AUTODEV_TEMPORAL_VIA_PROXY=1",
        "-m autodev.cli worker",
    ):
        assert piece in worker, piece
    assert worker.startswith(f"cd {tmp_path}/ad-state &&")
    assert "harness" not in worker
    ((argv, kw),) = schedule_calls(fake)
    assert kw["env"]["AUTODEV_CONFIG"] == f"{tmp_path}/autodev.yaml"
    assert (tmp_path / "ad-state/logs").is_dir() and (tmp_path / "ad-state-host/logs").is_dir()


def test_up_mia_data_mounts_harness_and_storage(tmp_path, fake):
    s = make_stack(tmp_path)
    services.service_up(s.services["mia-data"], s, Temporal(s))
    worker = worker_command(fake, "mia-data")
    assert f"--ro {tmp_path}/harness" in worker and f"--rw {tmp_path}/store" in worker
    assert f"MIA_DATA_CONFIG={tmp_path}/mia_data.yaml" in worker and "MIA_DATA_TEMPORAL_VIA_PROXY=1" in worker
    assert "-m mia_data.cli worker" in worker and (tmp_path / "store").is_dir()


def test_up_allows_remote_temporal_host(tmp_path, fake):
    s = make_stack(tmp_path, managed=False, address="temporal.k8s.svc:7233")
    fake.healthy = True
    services.service_up(s.services["autodev"], s, Temporal(s))
    assert "--allow temporal.k8s.svc" in worker_command(fake, "autodev")
    assert [c[c.index("-s") + 1] for c in tmux_calls(fake, "new-session")] == ["autodev"]


def test_up_with_external_temporal_down_starts_nothing(tmp_path, fake):
    s = make_stack(tmp_path, managed=False)
    with pytest.raises(SystemExit, match="unreachable"):
        services.service_up(s.services["autodev"], s, Temporal(s))
    assert not tmux_calls(fake, "new-session")


def test_up_when_running_only_reapplies_schedule(tmp_path, fake, capsys):
    s = make_stack(tmp_path)
    fake.running.add("autodev")
    fake.healthy = True
    assert services.service_up(s.services["autodev"], s, Temporal(s)) == 0
    assert not tmux_calls(fake, "new-session") and len(schedule_calls(fake)) == 1
    assert "already running" in capsys.readouterr().out


@pytest.mark.parametrize("missing", ["python", "csub-broker", "sandbox/sandbox-run.sh"])
def test_up_refuses_without_prerequisites(tmp_path, fake, missing):
    s = make_stack(tmp_path)
    (tmp_path / missing).unlink()
    with pytest.raises(SystemExit, match=missing.split("/")[-1]):
        services.service_up(s.services["autodev"], s, Temporal(s))
    assert not tmux_calls(fake, "new-session")


def test_up_refuses_api_key_env_and_missing_secret(tmp_path, fake):
    s = make_stack(tmp_path)
    svc = s.services["autodev"]
    svc.cfg.agent.api_key_env = "KEY"
    with pytest.raises(SystemExit, match="api_key_env"):
        services.service_up(svc, s, Temporal(s))
    svc.cfg.agent.api_key_env = None
    svc.cfg.github.token_file = None
    with pytest.raises(SystemExit, match="token_file"):
        services.service_up(svc, s, Temporal(s))


# ---- final review fixes


def test_status_never_downloads_the_cli(tmp_path, fake, monkeypatch):
    monkeypatch.setattr(services.shutil, "which", lambda name: None)
    assert Temporal(make_stack(tmp_path)).status() == "stopped"
    assert not [c for c, kw in fake.calls if kw.get("shell")], "a status check must not run curl"
    assert not (tmp_path / "temporal/bin/temporal").exists()


def test_stack_rejects_services_sharing_a_state_dir(tmp_path, fake):
    write_configs(tmp_path)
    (tmp_path / "autodev2.yaml").write_text(
        (tmp_path / "autodev.yaml").read_text().replace("deployment: autodev", "deployment: other")
    )
    with pytest.raises(SystemExit, match="share paths.state_dir"):
        Stack(
            Manifest(
                services={
                    "autodev": {"app": "autodev", "config": tmp_path / "autodev.yaml"},
                    "other": {"app": "autodev", "config": tmp_path / "autodev2.yaml"},
                }
            )
        )


def test_stack_rejects_services_sharing_a_deployment(tmp_path, fake):
    write_configs(tmp_path)
    (tmp_path / "autodev2.yaml").write_text((tmp_path / "autodev.yaml").read_text().replace("ad-state", "ad2-state"))
    with pytest.raises(SystemExit, match="share deployment"):
        Stack(
            Manifest(
                services={
                    "autodev": {"app": "autodev", "config": tmp_path / "autodev.yaml"},
                    "other": {"app": "autodev", "config": tmp_path / "autodev2.yaml"},
                }
            )
        )


def test_mia_buffers_app_mounts(tmp_path):
    from test_buffers_config import make_cfg

    cfg = make_cfg(tmp_path)
    app = services.APPS["mia-buffers"]
    assert app.module == "mia_buffers.cli" and app.config_env == "MIA_BUFFERS_CONFIG"
    assert app.extra_mounts(cfg) == [
        ("--ro", cfg.harness.dir),
        ("--ro", cfg.paths.storage_root),
        ("--rw", cfg.paths.raw_root),
        ("--rw", cfg.paths.staged_root),
    ]


def make_buffers_stack(tmp_path, url="http://buffer-host.example.org:8765"):
    write_configs(tmp_path)
    (tmp_path / "mia_buffers.yaml").write_text(
        f"github: {{buffers_repo: o/b, datasets_repo: o/d, snippets_repo: o/s, catalog_repo: o/c, "
        f"token_file: {tmp_path}/token}}\n"
        "temporal: {address: localhost:7233, namespace: mia-buffers}\n"
        f"paths: {{state_dir: {tmp_path}/mb-state, work_root: {tmp_path}/mb-work, storage_root: {tmp_path}/store, "
        f"raw_root: {tmp_path}/raw, staged_root: {tmp_path}/staged}}\n"
        f"service: {{url: {url}}}\n"
        f"harness: {{dir: {tmp_path}/harness}}\n"
    )
    return Stack(
        Manifest(
            sandbox_dir=tmp_path / "sandbox",
            temporal={"managed": True, "state_dir": tmp_path / "temporal"},
            services={"mia-buffers": {"app": "mia-buffers", "config": tmp_path / "mia_buffers.yaml"}},
        )
    )


def test_up_mia_buffers_starts_buffer_api_on_the_host_and_lets_the_worker_reach_it(tmp_path, fake):
    s = make_buffers_stack(tmp_path)
    services.service_up(s.services["mia-buffers"], s, Temporal(s))
    windows_ = [(c[c.index("-n") + 1], c[-1]) for c in tmux_calls(fake, "new-window")]
    assert [name for name, _ in windows_] == ["api", "worker"]
    api = dict(windows_)["api"]
    assert "sandbox-run.sh" not in api
    assert f"-m mia_buffers.cli serve --state-dir {tmp_path}/mb-state --host buffer-host.example.org --port 8765" in api
    assert f"{tmp_path}/mb-state-host/logs/api.log" in api
    assert "--allow localhost --allow buffer-host.example.org" in worker_command(fake, "mia-buffers")
    assert s.services["mia-buffers"].logs()["api"] == tmp_path / "mb-state-host/logs/api.log"


def test_only_mia_buffers_has_an_api_window(tmp_path, fake):
    s = make_stack(tmp_path)
    assert "api" not in s.services["autodev"].logs() and "api" not in s.services["mia-data"].logs()


@pytest.mark.parametrize("url", ["http://localhost:8765", "http://127.0.0.1:8765"])
def test_up_mia_buffers_refuses_a_loopback_service_url(tmp_path, fake, url):
    """Inside the sandbox localhost is the sandbox's own loopback (the proxy is bypassed for it), so a worker could
    never reach buffer-api there; refuse before anything starts."""
    s = make_buffers_stack(tmp_path, url)
    with pytest.raises(SystemExit, match="service.url"):
        services.service_up(s.services["mia-buffers"], s, Temporal(s))
    assert not tmux_calls(fake, "new-session")


# ---- mia.yaml as the configuration


def test_stack_builds_each_service_from_its_block(tmp_path, fake):
    s = make_stack_v2(tmp_path)
    assert [svc.legacy for svc in s.services.values()] == [False] * 4
    assert s.services["mia-data"].cfg.github.data_repo == "o/d"
    assert s.services["mia-data"].state == tmp_path / "state/mia-data"
    assert s.services["snippets"].env() == {"MIA_SERVICE": "snippets", "MIA_CONFIG": str(tmp_path / "mia.yaml")}


def test_legacy_config_files_still_load_with_a_deprecation_line(tmp_path, fake, capsys):
    s = make_stack(tmp_path)
    assert s.services["mia-data"].legacy
    assert s.services["mia-data"].env() == {"MIA_DATA_CONFIG": f"{tmp_path}/mia_data.yaml"}
    assert "services.mia-data.config is deprecated" in capsys.readouterr().out


def test_up_passes_the_service_name_and_mounts_locks_and_token(tmp_path, fake):
    s = make_stack_v2(tmp_path)
    services.service_up(s.services["mia-data"], s, Temporal(s))
    worker = worker_command(fake, "mia-data")
    for piece in (
        f"MIA_SERVICE=mia-data MIA_CONFIG={tmp_path}/mia.yaml",
        f"--ro {tmp_path}/mia.yaml",
        f"--rw {tmp_path}/local/locks",
        f"--ro {tmp_path}/token",
        "-m mia_data.cli worker",
    ):
        assert piece in worker, piece
    assert "MIA_DATA_CONFIG" not in worker
    assert worker.count(f"--ro {tmp_path}/token") == 1  # the GitHub secret and the worker token are one file here
    assert (tmp_path / "local/locks").is_dir() and (tmp_path / "local/service").is_dir()
    ((argv, kw),) = schedule_calls(fake)
    assert kw["env"]["MIA_SERVICE"] == "mia-data"


def test_up_refuses_a_network_local_root(tmp_path, fake, monkeypatch):
    s = make_stack_v2(tmp_path)

    def on_nfs(path):
        raise RuntimeError(f"{path} is on nfs, a network filesystem; it must be host-local disk")

    monkeypatch.setattr(services, "require_local_dir", on_nfs)
    with pytest.raises(SystemExit, match="network filesystem"):
        services.service_up(s.services["mia-buffers"], s, Temporal(s))
    assert not tmux_calls(fake, "new-session")


def test_up_refuses_a_missing_local_root_for_data_and_buffers(tmp_path, fake):
    s = make_stack_v2(tmp_path, local_root="")  # the builder omits the key when empty
    with pytest.raises(SystemExit, match="paths.local_root"):
        services.service_up(s.services["mia-data"], s, Temporal(s))
    services.service_up(s.services["autodev"], s, Temporal(s))  # autodev needs none


def test_up_refuses_a_loose_token_file(tmp_path, fake):
    s = make_stack_v2(tmp_path, token_mode=0o644)
    with pytest.raises(SystemExit, match="mode 600"):
        services.service_up(s.services["mia-buffers"], s, Temporal(s))


def test_mia_buffers_app_mounts_include_the_lock_directory(tmp_path, fake):
    s = make_stack_v2(tmp_path)
    cfg = s.services["mia-buffers"].cfg
    assert ("--rw", cfg.paths.local_root / "locks") in services.APPS["mia-buffers"].extra_mounts(cfg)
