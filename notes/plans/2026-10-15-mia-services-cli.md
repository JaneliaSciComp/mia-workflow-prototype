# `mia` services CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One `pixi run mia up|down|restart|ps|logs|attach|exec [SERVICE...]` command that manages every service on a host, replacing the two bash up-scripts and the lifecycle subcommands of `autodev` and `mia-data`.

**Architecture:** A new package `mia_ops/` reads a gitignored `mia.yaml` manifest (services -> app + config, Temporal managed or external), loads each service's config with that app's own `load_config`, and drives tmux the way the bash scripts did: one session per service with a `broker` and a `worker` window, plus a `temporal` session when `mia` manages the dev server. Per-app differences are a three-field table, not two scripts.

**Tech Stack:** Python 3.12, pydantic v2, PyYAML, argparse, subprocess (tmux, the `temporal` CLI, `tail`), pytest with a monkeypatched `subprocess.run`.

**Spec:** `notes/specs/2026-10-14-mia-services-cli.md`

## Global Constraints

- Python 3.12, ruff line length 120, `pixi run lint` and `pixi run typecheck` (mypy, `check_untyped_defs`) must pass after every task.
- Tracked files hold placeholder paths only: never `rokickik`, `/nrs/...` or a real home directory (see `autodev.yaml.example` for the style).
- Service names: `[a-z][a-z0-9-]{1,30}`, the same rule `autodev.config` applies to `deployment`.
- The service name is the tmux session name; a tmux target is always written `=<name>` (exact match).
- Per-app table (from the spec): autodev = module `autodev.cli`, env `AUTODEV_CONFIG`, proxy flag `AUTODEV_TEMPORAL_VIA_PROXY`, no extra mounts; mia-data = module `mia_data.cli`, env `MIA_DATA_CONFIG`, proxy flag `MIA_DATA_TEMPORAL_VIA_PROXY`, extra mounts `--ro harness.dir`, `--rw paths.storage_root`.
- Shared mounts and env for every worker: `--ro <repo> --ro <config> --ro <secret> --rw <work_root> --rw /scratch/$USER/<service>`, `--allow github.com --allow api.github.com --allow <host of temporal.address>`, `CSUB_TRANSPORT=unix`, `CSUB_SOCKET=<run>/csub.sock`.
- `temporal.managed` defaults to `false`; omitting the `temporal` section means an external Temporal that `mia` never starts, stops, downloads or writes anything for. Dev hosts set `managed: true` explicitly.
- Deviation from the spec, on purpose: the "no single quotes in paths" refusal is dropped. Every tmux command line is built with `shlex.quote`, which is correct for any path; the refusal only existed because the bash scripts hand-quoted.
- Commit after every task with a message in the repo's style (imperative, one line, optional body) ending in `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

Inputs the spec implies but does not spell out. Each has a test pinned to the task that owns the code.

1. **Two services pointing at the same config file** would start two tmux sessions over one state dir and one broker socket name clash. Expect the manifest to reject it. (Task 1)
2. **A Temporal address with no port or with a scheme** (`temporal.k8s.svc`, `grpc://host:7233`) must still yield a sane `--allow` host. Expect `host:port` -> `host`, bare host -> itself. (Task 2)
3. **Namespace creation failing on an external cluster** (no permission) must be a one-line error, not a traceback. (Task 2)
4. **`mia up` when the sandbox script, pixi env or broker binary is missing** must stop before any tmux session exists, naming the missing path. (Task 3)
5. **`mia logs <service> temporal`**, a habit from the old `ad logs temporal`, must say that Temporal's log is `mia logs temporal`, not crash on a KeyError. (Task 4, via argparse `choices`)

---

### Task 1: Manifest model and `mia.yaml.example`

**Files:**
- Create: `mia_ops/__init__.py` (empty)
- Create: `mia_ops/manifest.py`
- Create: `mia.yaml.example`
- Modify: `.gitignore`
- Modify: `pyproject.toml` (hatch packages, typecheck task)
- Test: `tests/test_mia_ops_manifest.py`

**Interfaces:**
- Produces: `Manifest` (pydantic) with `sandbox_dir: Path` (absolute), `temporal: TemporalCfg(managed: bool, state_dir: Path)` (absolute), `services: dict[str, ServiceCfg(app: Literal["autodev", "mia-data"], config: Path)]` (absolute, in file order); `load_manifest(path: str | os.PathLike | None = None) -> Manifest` which `sys.exit`s with a readable message when the file is missing or invalid.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_mia_ops_manifest.py
import pytest
from pydantic import ValidationError

from autodev.config import PROJECT_DIR
from mia_ops.manifest import Manifest, load_manifest

SERVICES = {"autodev": {"app": "autodev", "config": "autodev.yaml"}, "mia-data": {"app": "mia-data", "config": "mia_data.yaml"}}


def test_example_manifest_loads():
    m = load_manifest(PROJECT_DIR / "mia.yaml.example")
    assert list(m.services) == ["autodev", "snippets", "mia-data"] and m.temporal.managed is True
    assert m.services["snippets"].app == "autodev"


def test_omitted_temporal_section_means_external():
    assert Manifest(services=SERVICES).temporal.managed is False


def test_paths_resolve_against_the_repo():
    m = Manifest(services=SERVICES)
    assert m.services["autodev"].config == PROJECT_DIR / "autodev.yaml"
    assert m.sandbox_dir == (PROJECT_DIR / "../agentic-sandbox/scripts").resolve()
    assert m.temporal.state_dir.is_absolute() and "~" not in str(m.temporal.state_dir)


@pytest.mark.parametrize(
    "services, message",
    [
        ({}, "at least one"),
        ({"temporal": {"app": "autodev", "config": "a.yaml"}}, "implicit"),
        ({"Bad.Name": {"app": "autodev", "config": "a.yaml"}}, "Bad.Name"),
        ({"a": {"app": "nope", "config": "a.yaml"}}, "nope"),
        ({"a": {"app": "autodev", "config": "x.yaml"}, "b": {"app": "autodev", "config": "x.yaml"}}, "same config"),
    ],
)
def test_rejects(services, message):
    with pytest.raises(ValidationError, match=message):
        Manifest(services=services)


def test_load_manifest_missing_file_exits(tmp_path):
    with pytest.raises(SystemExit, match="not found"):
        load_manifest(tmp_path / "mia.yaml")


def test_load_manifest_invalid_file_exits(tmp_path):
    (tmp_path / "mia.yaml").write_text("services: {}\n")
    with pytest.raises(SystemExit, match="at least one"):
        load_manifest(tmp_path / "mia.yaml")


def test_load_manifest_env_default(tmp_path, monkeypatch):
    p = tmp_path / "m.yaml"
    p.write_text("services:\n  autodev: {app: autodev, config: autodev.yaml}\n")
    monkeypatch.setenv("MIA_MANIFEST", str(p))
    assert list(load_manifest().services) == ["autodev"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_mia_ops_manifest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mia_ops'`

- [ ] **Step 3: Write the manifest module**

```python
# mia_ops/__init__.py
```
(empty file)

```python
# mia_ops/manifest.py
"""mia.yaml: the services on this host (name -> app + config), where agentic-sandbox is, and whether `mia`
runs the Temporal dev server itself or depends on an external one (production: Temporal in k8s)."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ValidationError, model_validator

from autodev.config import PROJECT_DIR

NAME_RE = re.compile(r"[a-z][a-z0-9-]{1,30}")


class TemporalCfg(BaseModel):
    managed: bool = False  # True: `mia` runs a local dev server (dev hosts); False: something else does (k8s)
    state_dir: Path = Path("~/.local/state/temporal")  # managed only: temporal.db, logs/, bin/temporal


class ServiceCfg(BaseModel):
    app: Literal["autodev", "mia-data"]
    config: Path  # relative to the repo


class Manifest(BaseModel):
    sandbox_dir: Path = Path("../agentic-sandbox/scripts")  # relative to the repo
    temporal: TemporalCfg = TemporalCfg()
    services: dict[str, ServiceCfg]

    @model_validator(mode="after")
    def _finish(self) -> Manifest:
        if not self.services:
            raise ValueError("services: list at least one")
        if "temporal" in self.services:
            raise ValueError("services: 'temporal' is implicit; do not list it")
        for name in self.services:
            if not NAME_RE.fullmatch(name):
                raise ValueError(f"services: {name!r}: use 2-31 chars of a-z, 0-9, '-'")
        self.sandbox_dir = (PROJECT_DIR / self.sandbox_dir).resolve()
        self.temporal.state_dir = self.temporal.state_dir.expanduser().resolve()
        seen: dict[Path, str] = {}
        for name, service in self.services.items():
            service.config = (PROJECT_DIR / service.config).resolve()
            if service.config in seen:
                raise ValueError(f"services: {name!r} and {seen[service.config]!r} use the same config file")
            seen[service.config] = name
        return self


def load_manifest(path: str | os.PathLike | None = None) -> Manifest:
    """Load and validate mia.yaml, exiting with a readable message if it is missing or invalid."""
    path = Path(path or os.environ.get("MIA_MANIFEST") or PROJECT_DIR / "mia.yaml")
    if not path.is_file():
        sys.exit(f"mia: manifest not found at {path} (copy mia.yaml.example, or set $MIA_MANIFEST)")
    try:
        return Manifest(**(yaml.safe_load(path.read_text()) or {}))
    except ValidationError as e:
        sys.exit(f"mia: {path}: {e}")
```

- [ ] **Step 4: Write `mia.yaml.example` and wire the package in**

```yaml
# Copy to mia.yaml (git-ignored). Lists the services `pixi run mia` manages on this host.
# Each service is one tmux session named after it: a csub broker and a sandboxed worker.
sandbox_dir: ../agentic-sandbox/scripts   # agentic-sandbox's scripts dir, relative to this repo
temporal:                                 # omit this section entirely for an external Temporal (production: k8s)
  managed: true                           # dev hosts: mia runs a local dev server (the default is false)
  state_dir: ~/.local/state/temporal      # managed only: temporal.db, logs/, and bin/temporal if downloaded
services:                                 # name -> app (autodev | mia-data) + its config file
  autodev:  {app: autodev,  config: autodev.yaml}
  snippets: {app: autodev,  config: autodev-snippets.yaml}
  mia-data: {app: mia-data, config: mia_data.yaml}
```

Append to `.gitignore`:

```
mia.yaml
```

In `pyproject.toml`:

```toml
[tool.hatch.build.targets.wheel]
packages = ["autodev", "mia_data", "mia_ops"]
```

and

```toml
typecheck = "mypy autodev mia_data mia_ops"
```

- [ ] **Step 5: Run the tests, lint and typecheck**

Run: `pixi run pytest tests/test_mia_ops_manifest.py -v && pixi run lint && pixi run typecheck`
Expected: all PASS (`pixi install` may be needed once so hatch picks up the new package).

- [ ] **Step 6: Commit**

```bash
git add mia_ops/__init__.py mia_ops/manifest.py mia.yaml.example .gitignore pyproject.toml tests/test_mia_ops_manifest.py
git commit -m "mia: manifest model for mia.yaml" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Stack, tmux helpers and the Temporal service

**Files:**
- Create: `mia_ops/services.py`
- Test: `tests/test_mia_ops_services.py` (fixture used by Tasks 2 and 3)

**Interfaces:**
- Consumes: `Manifest`, `load_manifest` from Task 1.
- Produces, all in `mia_ops/services.py`:
  - `PY: Path`, `BROKER: Path` (module constants; tests monkeypatch them)
  - `App(package: str, config_env: str, proxy_env: str)` with `module -> str` property (`f"{package}.cli"`), `load_config(path: Path) -> Any`, `extra_mounts(cfg) -> list[tuple[str, Path]]`; `APPS: dict[str, App]` keyed `"autodev"`, `"mia-data"`
  - `Service(name: str, app: App, config_path: Path, cfg: Any)` dataclass with properties `state`, `host`, `run`, `secret: Path | None`, and `logs() -> dict[str, Path]` (`worker`, `broker`)
  - `Stack(manifest: Manifest)` with `.manifest`, `.services: dict[str, Service]`, `.temporal_address: str`, `.service(name) -> Service` (exits on unknown), `.select(names: list[str]) -> tuple[bool, list[Service]]` (temporal selected?, app services in manifest order; empty names = everything)
  - `Temporal(stack)` with `.managed`, `.address`, `.host`, `.state`, `.log: Path`, `SESSION = "temporal"`, `cli() -> Path`, `healthy() -> bool`, `status() -> str`, `up() -> None`, `down(explicit: bool) -> int`, `ensure_namespace(ns: str) -> None`
  - tmux helpers `is_running(session) -> bool`, `kill(session) -> int`, `windows(session) -> str`, `attach(session) -> int`, `shell(*parts) -> str` (shlex-quoted command line)

- [ ] **Step 1: Write the fixture and the Temporal tests**

```python
# tests/test_mia_ops_services.py
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
    (tmp_path / "mia_data.yaml").write_text((tmp_path / "mia_data.yaml").read_text().replace("localhost:7233", "other:1"))
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_mia_ops_services.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mia_ops.services'`

- [ ] **Step 3: Write `mia_ops/services.py` (Stack, tmux helpers, Temporal; `service_up` comes in Task 3)**

```python
# mia_ops/services.py
"""The services in mia.yaml and how to run them: a Temporal dev server (or an external one), and per app
service one tmux session holding a csub broker and a worker inside agentic-sandbox."""

from __future__ import annotations

import getpass
import importlib
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from autodev.config import PROJECT_DIR

from .manifest import Manifest

PY = PROJECT_DIR / ".pixi/envs/default/bin/python"
BROKER = PROJECT_DIR / ".pixi/envs/default/bin/csub-broker"
TEMPORAL_CLI_URL = "https://temporal.download/cli/archive/latest?platform=linux&arch=amd64"


# ---- tmux


def _tmux(*args: str, **kwargs: Any) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["tmux", *args], text=True, **kwargs)
    except FileNotFoundError:
        sys.exit("tmux not found on PATH")


def is_running(session: str) -> bool:
    return _tmux("has-session", "-t", f"={session}", capture_output=True).returncode == 0


def kill(session: str) -> int:
    if not is_running(session):
        print(f"{session}: not running")
        return 0
    rc = _tmux("kill-session", "-t", f"={session}").returncode
    if rc == 0:
        print(f"{session}: stopped")
    return rc


def windows(session: str) -> str:
    fmt = "  #{window_name}: #{?pane_dead,dead,alive}"
    return _tmux("list-windows", "-t", f"={session}", "-F", fmt, capture_output=True).stdout


def attach(session: str) -> int:
    if not is_running(session):
        sys.exit(f"{session} is not running (run `mia up {session}` first)")
    return _tmux("attach", "-t", f"={session}").returncode


def new_session(session: str, window: str, command: str) -> None:
    _tmux("new-session", "-d", "-s", session, "-n", window, command, check=True)


def new_window(session: str, window: str, command: str) -> None:
    _tmux("new-window", "-t", f"={session}", "-n", window, command, check=True)


def shell(*parts: object) -> str:
    """One shell command line for tmux from argv parts; quoting handles any path, quotes included."""
    return " ".join(shlex.quote(str(p)) for p in parts)


# ---- apps and services


@dataclass(frozen=True)
class App:
    package: str  # `python -m <package>.cli`; `<package>.config.load_config`
    config_env: str
    proxy_env: str

    @property
    def module(self) -> str:
        return f"{self.package}.cli"

    def load_config(self, path: Path) -> Any:
        return importlib.import_module(f"{self.package}.config").load_config(path)

    def extra_mounts(self, cfg: Any) -> list[tuple[str, Path]]:
        if self.package == "mia_data":
            return [("--ro", cfg.harness.dir), ("--rw", cfg.paths.storage_root)]
        return []


APPS = {
    "autodev": App("autodev", "AUTODEV_CONFIG", "AUTODEV_TEMPORAL_VIA_PROXY"),
    "mia-data": App("mia_data", "MIA_DATA_CONFIG", "MIA_DATA_TEMPORAL_VIA_PROXY"),
}


@dataclass
class Service:
    name: str  # also the tmux session
    app: App
    config_path: Path
    cfg: Any  # the app's validated Config

    @property
    def state(self) -> Path:
        return self.cfg.paths.state_dir

    @property
    def host(self) -> Path:  # host-only files beside the state dir, which the sandbox binds read-write
        return self.state.with_name(self.state.name + "-host")

    @property
    def run(self) -> Path:  # node-local: the csub broker's unix socket
        return Path(f"/scratch/{getpass.getuser()}/{self.name}")

    @property
    def secret(self) -> Path | None:
        g = self.cfg.github
        path = g.private_key_path if g.auth == "app" else g.token_file
        return Path(path).expanduser().resolve() if path else None

    def logs(self) -> dict[str, Path]:
        return {"worker": self.state / "logs/worker.log", "broker": self.host / "logs/broker.log"}


class Stack:
    """mia.yaml loaded: every service with its config, and the one Temporal address they share."""

    def __init__(self, manifest: Manifest):
        self.manifest = manifest
        self.services = {
            name: Service(name, APPS[s.app], s.config, APPS[s.app].load_config(s.config))
            for name, s in manifest.services.items()
        }
        addresses = {s.cfg.temporal.address for s in self.services.values()}
        if len(addresses) != 1:
            sys.exit(f"mia: services disagree on temporal.address: {sorted(addresses)}")
        self.temporal_address: str = addresses.pop()

    def service(self, name: str) -> Service:
        if name not in self.services:
            sys.exit(f"mia: unknown service {name!r}; mia.yaml lists {list(self.services)} (and temporal)")
        return self.services[name]

    def select(self, names: list[str]) -> tuple[bool, list[Service]]:
        """(temporal selected?, app services in manifest order). No names means everything."""
        if not names:
            return True, list(self.services.values())
        for name in names:
            if name != "temporal":
                self.service(name)
        return "temporal" in names, [s for n, s in self.services.items() if n in names]


# ---- temporal


class Temporal:
    SESSION = "temporal"

    def __init__(self, stack: Stack):
        self.managed = stack.manifest.temporal.managed
        self.state = stack.manifest.temporal.state_dir
        self.address = stack.temporal_address
        self.log = self.state / "logs/temporal.log"

    @property
    def host(self) -> str:
        return self.address.rsplit(":", 1)[0] if ":" in self.address else self.address

    def cli(self) -> Path:
        if found := shutil.which("temporal"):
            return Path(found)
        if not self.managed:
            sys.exit("mia: the `temporal` CLI must be on PATH when temporal.managed is false")
        target = self.state / "bin/temporal"
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            print(f"downloading the temporal CLI to {target}")
            cmd = f"curl -sSL {shlex.quote(TEMPORAL_CLI_URL)} | tar xz -C {shlex.quote(str(target.parent))} temporal"
            subprocess.run(cmd, shell=True, check=True)
        return target

    def _run(self, *args: str, check: bool = False) -> int:
        return subprocess.run([str(self.cli()), *args, "--address", self.address], capture_output=True, check=check).returncode

    def healthy(self) -> bool:
        return self._run("operator", "cluster", "health") == 0

    def status(self) -> str:
        if self.managed:
            return "running" if is_running(self.SESSION) else "external" if self.healthy() else "stopped"
        return "external (healthy)" if self.healthy() else "external (unreachable)"

    def up(self) -> None:
        if self.healthy():
            print(f"temporal: {self.status()} at {self.address}")
            return
        if not self.managed:
            sys.exit(f"temporal: unreachable at {self.address} (temporal.managed is false; see your cluster)")
        self.log.parent.mkdir(parents=True, exist_ok=True)
        server = shell(self.cli(), "server", "start-dev", "--db-filename", self.state / "temporal.db")
        new_session(self.SESSION, "temporal", f"{server} 2>&1 | tee -a {shell(self.log)}")
        for _ in range(60):
            if self.healthy():
                print(f"temporal: started at {self.address}")
                return
            time.sleep(1)
        sys.exit(f"temporal: did not come up; see {self.log}")

    def down(self, explicit: bool) -> int:
        if not self.managed:
            if explicit:
                sys.exit("temporal is external (temporal.managed is false); see your cluster")
            print("temporal: external, left alone")
            return 0
        return kill(self.SESSION)

    def ensure_namespace(self, ns: str) -> None:
        if self._run("operator", "namespace", "describe", "-n", ns) == 0:
            return
        if self._run("operator", "namespace", "create", "-n", ns) != 0:
            sys.exit(f"temporal: could not create namespace {ns!r} at {self.address}")
        for _ in range(30):  # registration is async: the frontend's namespace cache refreshes every ~10 s
            if self._run("workflow", "list", "-n", ns, "--limit", "1") == 0:
                return
            time.sleep(1)
```

- [ ] **Step 4: Run the tests, lint and typecheck**

Run: `pixi run pytest tests/test_mia_ops_services.py -v && pixi run lint && pixi run typecheck`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add mia_ops/services.py tests/test_mia_ops_services.py
git commit -m "mia: Stack, tmux helpers and the Temporal service (managed or external)" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: App service start (`service_up`)

**Files:**
- Modify: `mia_ops/services.py` (append)
- Test: `tests/test_mia_ops_services.py` (append)

**Interfaces:**
- Consumes: `Service`, `Stack`, `Temporal`, tmux helpers, `PY`, `BROKER` from Task 2.
- Produces: `service_up(svc: Service, stack: Stack, temporal: Temporal) -> int`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_mia_ops_services.py`:

```python
# ---- service_up


def worker_command(fake, session):
    (call,) = [c for c, _ in fake.calls if c[:2] == ["tmux", "new-window"] and c[c.index("-t") + 1] == f"={session}"]
    return call[-1]


def schedule_calls(fake):
    return [(c, kw) for c, kw in fake.calls if c[1:5] == ["-m", "autodev.cli", "schedule", "apply"] or c[1:5] == ["-m", "mia_data.cli", "schedule", "apply"]]


def test_up_starts_temporal_then_broker_and_worker(tmp_path, fake):
    s = make_stack(tmp_path)
    t = Temporal(s)
    assert services.service_up(s.services["autodev"], s, t) == 0
    sessions = [c[c.index("-s") + 1] for c in tmux_calls(fake, "new-session")]
    assert sessions == ["temporal", "autodev"]
    broker = tmux_calls(fake, "new-session")[1][-1]
    assert "csub-broker" in broker and "/scratch/me/autodev/csub.sock" in broker and "broker.log" in broker
    worker = worker_command(fake, "autodev")
    for piece in (
        f"--ro {services.PROJECT_DIR}",
        f"--ro {tmp_path}/autodev.yaml",
        f"--ro {tmp_path}/token",
        f"--rw {tmp_path}/ad-work",
        "--rw /scratch/me/autodev",
        "--allow github.com --allow api.github.com --allow localhost",
        f"AUTODEV_CONFIG={tmp_path}/autodev.yaml",
        "CSUB_TRANSPORT=unix CSUB_SOCKET=/scratch/me/autodev/csub.sock AUTODEV_TEMPORAL_VIA_PROXY=1",
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_mia_ops_services.py -k "up_" -v`
Expected: FAIL with `AttributeError: module 'mia_ops.services' has no attribute 'service_up'`

- [ ] **Step 3: Append `service_up`**

Append to `mia_ops/services.py`:

```python
# ---- app services


def service_up(svc: Service, stack: Stack, temporal: Temporal) -> int:
    """Start one app service: broker + sandboxed worker in tmux, then apply its poll schedule.
    Temporal is started (managed) or verified (external) first. Re-running on a live session only
    re-applies the schedule, so a run that failed partway is repaired by running it again."""
    cfg = svc.cfg
    if cfg.agent.api_key_env:
        sys.exit(f"{svc.name}: agent.api_key_env is not supported by `mia up` yet; give the account a Claude login")
    if not svc.secret:  # the sandbox masks .env and tmux passes no env, so the GitHub secret must be a file
        sys.exit(f"{svc.name}: set github.token_file (auth: token) or github.private_key_path (auth: app)")
    sandbox = stack.manifest.sandbox_dir / "sandbox-run.sh"
    for path, hint in ((PY, "run `pixi install`"), (BROKER, "run `pixi install`"), (sandbox, "set sandbox_dir in mia.yaml")):
        if not os.access(path, os.X_OK):
            sys.exit(f"{svc.name}: {path} not found ({hint})")
    extra = svc.app.extra_mounts(cfg)
    for d in (svc.state / "logs", svc.host / "logs", cfg.paths.work_root, svc.run, *(p for flag, p in extra if flag == "--rw")):
        d.mkdir(parents=True, exist_ok=True)

    temporal.up()
    temporal.ensure_namespace(cfg.temporal.namespace)

    schedule = [str(PY), "-m", svc.app.module, "schedule", "apply"]
    env = {**os.environ, svc.app.config_env: str(svc.config_path)}
    if is_running(svc.name):
        subprocess.run(schedule, env=env, check=True)
        print(f"{svc.name}: already running (tmux attach -t {svc.name})")
        return 0

    sock = svc.run / "csub.sock"
    broker_log = svc.host / "logs/broker.log"
    new_session(svc.name, "broker", f"{shell(BROKER, '--serve', sock)} 2>&1 | tee -a {shell(broker_log)}")
    # cwd = the state dir, which sandbox-run.sh binds read-write; the repo (code + pixi env), config and secret
    # are read-only. The worker reaches Temporal through the sandbox's allowlist proxy.
    mounts: list[object] = ["--ro", PROJECT_DIR, "--ro", svc.config_path, "--ro", svc.secret]
    for flag, path in extra:
        mounts += [flag, path]
    mounts += ["--rw", cfg.paths.work_root, "--rw", svc.run]
    worker = shell(
        sandbox, *mounts, "--allow", "github.com", "--allow", "api.github.com", "--allow", temporal.host, "--",
        "env", f"{svc.app.config_env}={svc.config_path}", "CSUB_TRANSPORT=unix", f"CSUB_SOCKET={sock}",
        f"{svc.app.proxy_env}=1", PY, "-m", svc.app.module, "worker",
    )
    new_window(svc.name, "worker", f"cd {shell(svc.state)} && {worker}; read -p 'worker exited; press enter'")
    subprocess.run(schedule, env=env, check=True)
    print(f"{svc.name}: started (attach: mia attach {svc.name}; logs: mia logs {svc.name} -f)")
    return 0
```

- [ ] **Step 4: Run the tests, lint and typecheck**

Run: `pixi run pytest tests/test_mia_ops_services.py -v && pixi run lint && pixi run typecheck`
Expected: all PASS. If ruff reformats the long `shell(...)` call, accept its formatting.

- [ ] **Step 5: Commit**

```bash
git add mia_ops/services.py tests/test_mia_ops_services.py
git commit -m "mia: start an app service (broker + sandboxed worker) from its config" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: The `mia` command line

**Files:**
- Create: `mia_ops/cli.py`
- Modify: `pyproject.toml` (`[project.scripts]`, `[tool.pixi.tasks]`)
- Test: `tests/test_mia_ops_cli.py`

**Interfaces:**
- Consumes: everything in `mia_ops.services` and `load_manifest`.
- Produces: `mia_ops.cli.main(argv: list[str] | None = None) -> int`; console script `mia`; pixi task `mia` (the `ad` and `md` tasks are removed).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_mia_ops_cli.py
import pytest
from test_mia_ops_services import fake, make_stack, tmux_calls  # noqa: F401  (fixture re-export)

from mia_ops import cli, services


@pytest.fixture
def stack(tmp_path, fake, monkeypatch):  # noqa: F811
    s = make_stack(tmp_path)
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    return s


def new_sessions(fake):  # noqa: F811
    return [c[c.index("-s") + 1] for c in tmux_calls(fake, "new-session")]


def killed(fake):  # noqa: F811
    return [c[3].lstrip("=") for c in tmux_calls(fake, "kill-session")]


def test_help_lists_commands(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for cmd in ("up", "down", "restart", "ps", "logs", "attach", "exec"):
        assert cmd in out


def test_up_all_starts_temporal_first(stack, fake):  # noqa: F811
    assert cli.main(["up"]) == 0
    assert new_sessions(fake) == ["temporal", "autodev", "mia-data"]


def test_up_one_service(stack, fake):  # noqa: F811
    assert cli.main(["up", "mia-data"]) == 0
    assert new_sessions(fake) == ["temporal", "mia-data"]


def test_up_temporal_alone(stack, fake):  # noqa: F811
    assert cli.main(["up", "temporal"]) == 0
    assert new_sessions(fake) == ["temporal"]


def test_down_all_stops_temporal_last(stack, fake):  # noqa: F811
    cli.main(["up"])
    assert cli.main(["down"]) == 0
    assert killed(fake) == ["mia-data", "autodev", "temporal"]


def test_down_one_leaves_temporal(stack, fake):  # noqa: F811
    cli.main(["up"])
    assert cli.main(["down", "autodev"]) == 0
    assert killed(fake) == ["autodev"] and fake.running == {"temporal", "mia-data"}


def test_restart_one(stack, fake):  # noqa: F811
    cli.main(["up"])
    assert cli.main(["restart", "autodev"]) == 0
    assert killed(fake) == ["autodev"] and new_sessions(fake) == ["temporal", "autodev", "mia-data", "autodev"]


def test_unknown_service_exits(stack, fake):  # noqa: F811
    with pytest.raises(SystemExit, match="unknown service"):
        cli.main(["up", "nope"])


def test_ps(stack, fake, capsys):  # noqa: F811
    assert cli.main(["ps"]) == 1
    out = capsys.readouterr().out
    assert "temporal: stopped" in out and "autodev: stopped" in out
    cli.main(["up"])
    capsys.readouterr()
    assert cli.main(["ps"]) == 0
    out = capsys.readouterr().out
    assert "temporal: running" in out and "autodev: running" in out and "worker: alive" in out


def test_logs(stack, fake, tmp_path):  # noqa: F811
    log = tmp_path / "ad-state/logs/worker.log"
    log.parent.mkdir(parents=True)
    log.write_text("x\n")
    assert cli.main(["logs", "autodev", "-n", "3", "-f"]) == 0
    assert fake.calls[-1][0] == ["tail", "-n", "3", "-F", str(log)]
    with pytest.raises(SystemExit, match="no log"):
        cli.main(["logs", "autodev", "broker"])
    with pytest.raises(SystemExit):  # argparse: `temporal` is not a window name; its log is `mia logs temporal`
        cli.main(["logs", "autodev", "temporal"])
    tlog = tmp_path / "temporal/logs/temporal.log"
    tlog.parent.mkdir(parents=True)
    tlog.write_text("x\n")
    assert cli.main(["logs", "temporal"]) == 0 and fake.calls[-1][0][-1] == str(tlog)


def test_attach(stack, fake):  # noqa: F811
    with pytest.raises(SystemExit, match="not running"):
        cli.main(["attach", "autodev"])
    cli.main(["up", "autodev"])
    assert cli.main(["attach", "autodev"]) == 0
    assert fake.calls[-1][0] == ["tmux", "attach", "-t", "=autodev"]


def test_exec_runs_the_app_cli_with_its_config(stack, fake, tmp_path):  # noqa: F811
    assert cli.main(["exec", "mia-data", "seed", "--dry-run"]) == 0
    argv, kw = fake.calls[-1]
    assert argv == [str(services.PY), "-m", "mia_data.cli", "seed", "--dry-run"]
    assert kw["env"]["MIA_DATA_CONFIG"] == f"{tmp_path}/mia_data.yaml"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_mia_ops_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mia_ops.cli'`

- [ ] **Step 3: Write the CLI**

```python
# mia_ops/cli.py
"""The `mia` command line: start, stop and inspect every service in mia.yaml, like docker compose for the
tmux sessions. `mia up` with no service acts on all of them (Temporal first); `mia down` stops them all
(Temporal last); naming services restricts each command to those, and never touches Temporal unless named."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys

from .manifest import load_manifest
from .services import PY, Stack, Temporal, attach, is_running, kill, service_up, windows


def cmd_up(stack: Stack, args: argparse.Namespace) -> int:
    temporal = Temporal(stack)
    want_temporal, selected = stack.select(args.services)
    if want_temporal and not selected:  # `mia up temporal`
        temporal.up()
    for svc in selected:  # each call starts or verifies Temporal first; after the first that is a health check
        service_up(svc, stack, temporal)
    return 0


def cmd_down(stack: Stack, args: argparse.Namespace) -> int:
    want_temporal, selected = stack.select(args.services)
    rc = 0
    for svc in reversed(selected):
        rc |= kill(svc.name)
    if want_temporal:
        rc |= Temporal(stack).down(explicit="temporal" in args.services)
    return rc


def cmd_restart(stack: Stack, args: argparse.Namespace) -> int:
    return cmd_down(stack, args) or cmd_up(stack, args)


def cmd_ps(stack: Stack, args: argparse.Namespace) -> int:
    want_temporal, selected = stack.select(args.services)
    rc = 0
    if want_temporal:
        temporal = Temporal(stack)
        status = temporal.status()
        print(f"temporal: {status} ({temporal.address})")
        rc |= "stopped" in status or "unreachable" in status
    for svc in selected:
        if is_running(svc.name):
            print(f"{svc.name}: running")
            print(windows(svc.name), end="")
        else:
            print(f"{svc.name}: stopped")
            rc = 1
    return int(rc)


def cmd_logs(stack: Stack, args: argparse.Namespace) -> int:
    if args.service == "temporal":
        temporal = Temporal(stack)
        if not temporal.managed:
            sys.exit("temporal is external (temporal.managed is false); see your cluster")
        path = temporal.log
    else:
        path = stack.service(args.service).logs()[args.window]
    if not path.is_file():
        sys.exit(f"no log at {path}")
    cmd = ["tail", "-n", str(args.lines), *(["-F"] if args.follow else []), str(path)]
    try:
        return subprocess.run(cmd).returncode
    except KeyboardInterrupt:
        return 0


def cmd_attach(stack: Stack, args: argparse.Namespace) -> int:
    if args.service == "temporal":
        if not Temporal(stack).managed:
            sys.exit("temporal is external (temporal.managed is false); see your cluster")
        return attach(Temporal.SESSION)
    return attach(stack.service(args.service).name)


def cmd_exec(stack: Stack, args: argparse.Namespace) -> int:
    svc = stack.service(args.service)
    env = {**os.environ, svc.app.config_env: str(svc.config_path)}
    return subprocess.run([str(PY), "-m", svc.app.module, *args.args], env=env).returncode


COMMANDS = {
    "up": cmd_up, "down": cmd_down, "restart": cmd_restart, "ps": cmd_ps,
    "logs": cmd_logs, "attach": cmd_attach, "exec": cmd_exec,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mia", description=__doc__)
    parser.add_argument("--manifest", help="path to mia.yaml (default: $MIA_MANIFEST or ./mia.yaml)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for cmd, help_text in (
        ("up", "start all services (Temporal first), or the named ones"),
        ("down", "stop all services (Temporal last), or the named ones"),
        ("restart", "down, then up, same selection"),
        ("ps", "show which services are running"),
    ):
        sub.add_parser(cmd, help=help_text).add_argument("services", nargs="*", metavar="SERVICE")
    logs = sub.add_parser("logs", help="tail a service's log (default window: worker; `mia logs temporal` for Temporal)")
    logs.add_argument("service")
    logs.add_argument("window", nargs="?", default="worker", choices=["worker", "broker"])
    logs.add_argument("-n", "--lines", type=int, default=50)
    logs.add_argument("-f", "--follow", action="store_true", help="keep following (default: print the last lines)")
    sub.add_parser("attach", help="attach to a service's tmux session").add_argument("service")
    ex = sub.add_parser("exec", help="run a service's app CLI with its config, e.g. `mia exec snippets check`")
    ex.add_argument("service")
    ex.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    stack = Stack(load_manifest(args.manifest))
    return COMMANDS[args.cmd](stack, args)


if __name__ == "__main__":
    sys.exit(main())
```

In `pyproject.toml`:

```toml
[project.scripts]
autodev = "autodev.cli:main"
mia-data = "mia_data.cli:main"
mia = "mia_ops.cli:main"
```

and under `[tool.pixi.tasks]`, replace the two lines `ad = "autodev"` and `md = "mia-data"` with:

```toml
mia = "mia"
```

(`pixi run mia exec <service> ...` replaces `pixi run ad ...` and `pixi run md ...`; the console scripts stay for the worker.)

- [ ] **Step 4: Run the tests, lint, typecheck, and the real entry point**

Run: `pixi install && pixi run pytest tests/test_mia_ops_cli.py -v && pixi run lint && pixi run typecheck && pixi run mia --help`
Expected: tests PASS; `--help` prints the seven subcommands; `pixi run ad` and `pixi run md` now fail with "task not found".

- [ ] **Step 5: Commit**

```bash
git add mia_ops/cli.py pyproject.toml pixi.lock tests/test_mia_ops_cli.py
git commit -m "mia: the command line (up/down/restart/ps/logs/attach/exec)" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Remove the old lifecycle code

**Files:**
- Delete: `scripts/autodev-up.sh`, `scripts/mia-data-up.sh`, `autodev/ops.py`, `tests/test_autodev_ops.py`
- Modify: `autodev/cli.py:1-35` (docstring, `from . import ops`, `OPS`), `autodev/cli.py:253-270` (six subparsers and the `OPS` dispatch)
- Modify: `mia_data/cli.py:1-4` (docstring), `mia_data/cli.py:27` (import), `mia_data/cli.py:447-463` (six subparsers and the `OPS` dispatch)
- Modify: `tests/test_autodev_cli.py:17`, `tests/test_mia_cli.py:173-206`

**Interfaces:**
- Consumes: nothing new. After this task `autodev.cli` and `mia_data.cli` export no `OPS`.

- [ ] **Step 1: Update the tests first**

In `tests/test_autodev_cli.py` change the help assertion to:

```python
    for cmd in ("check", "init-labels", "worker", "schedule"):
        assert cmd in out
    assert "restart" not in out and "attach" not in out
```

In `tests/test_mia_cli.py` replace `test_help_lists_devops_commands` with:

```python
def test_help_lists_app_commands(capsys):
    import pytest

    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for cmd in ("check", "seed", "worker", "test-pr", "schedule"):
        assert cmd in out
    assert "restart" not in out and "attach" not in out
```

and delete `test_devops_commands_drive_the_mia_session` entirely. Delete `tests/test_autodev_ops.py`.

- [ ] **Step 2: Run the two test files to verify the new assertions fail**

Run: `pixi run pytest tests/test_autodev_cli.py tests/test_mia_cli.py -k help -v`
Expected: both help tests FAIL (the old subcommands are still listed).

- [ ] **Step 3: Remove the lifecycle code**

`git rm scripts/autodev-up.sh scripts/mia-data-up.sh autodev/ops.py tests/test_autodev_ops.py`. The `scripts/` directory then holds only `__pycache__`; remove it too.

In `autodev/cli.py`:
- Docstring becomes: `"""The `autodev` command line: health checks, label setup, the worker and the poll schedule. Heavy imports happen inside the commands that need them so `--help` stays fast. Starting and stopping deployments is `pixi run mia` (mia_ops)."""`
- Delete `from . import ops` and the whole `OPS = {...}` block.
- Delete the six `sub.add_parser(...)` lines for `up`, `down`, `restart`, `ps`, `attach`, `logs` and the three `logs.add_argument(...)` lines.
- Delete the `if args.cmd in OPS: ... return OPS[args.cmd](cfg, args)` block. `os` and `Path` may become unused imports; ruff will say so, remove them if it does.

In `mia_data/cli.py`:
- Docstring: replace `and deployment management (shared with autodev through autodev.ops)` with `(deployments start and stop with `pixi run mia`)`.
- Change `from autodev.cli import OPS, _client, _setup_logging` to `from autodev.cli import _client, _setup_logging`.
- Delete the six `sub.add_parser(...)` lines for `up`, `down`, `restart`, `ps`, `attach`, `logs`, the three `logs.add_argument(...)` lines, and the `if args.cmd in OPS: ... return OPS[args.cmd](cfg, args)` block (including the `args.up_script, args.config_env = ...` line). Remove imports ruff reports unused.

- [ ] **Step 4: Run everything**

Run: `pixi run test && pixi run lint && pixi run typecheck && grep -rn "ops\.\|up_script\|autodev-up\|mia-data-up" autodev mia_data tests; true`
Expected: tests, lint and typecheck PASS; the grep prints nothing.

- [ ] **Step 5: Commit**

```bash
git add -A autodev mia_data tests scripts
git commit -m "Remove the up-scripts and the per-app up/down/ps/logs commands; mia owns the lifecycle" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Docs

**Files:**
- Modify: `README.md:74-77`, `docs/autodev-local-setup.md:7-25,76-123`, `docs/github-app.md:85,92,96,106,128`, `docs/mia-data-workflow.md:81,158-166`, `notes/autodev-runbook.md:39-47,66,75-76,100,108-120`, `notes/mia-data-handoff.md:21,48-60,118`, `autodev.yaml.example:9`
- Rule for every file: `pixi run autodev X`, `pixi run ad X` and `pixi run md X` become `pixi run mia exec <service> X` (`autodev` or `mia-data` as the service, matching `mia.yaml.example`); a bare `md check` in prose becomes `mia exec mia-data check`.
- Not tracked but on your host: the header comment of `autodev-snippets.yaml` (line 2) says `AUTODEV_CONFIG=autodev-snippets.yaml pixi run ad up`; change it to `pixi run mia up snippets` locally.

**Interfaces:** none.

- [ ] **Step 1: README layout table**

Replace the `mia_data/` and `scripts/` rows:

```markdown
| `mia_data/` | mia-data: workflows, activities, harness job specs, CLI (`pixi run mia exec mia-data ...`) |
| `mia_ops/` | `pixi run mia`: starts, stops and inspects every service in `mia.yaml` (Temporal, autodev, snippets, mia-data), and `mia exec <service> <cmd>` runs an app command with that service's config |
```

(delete the `scripts/` row).

- [ ] **Step 2: `docs/autodev-local-setup.md`**

Replace "What you're starting" with:

```markdown
## What you're starting

`pixi run mia up` reads `mia.yaml` (copy `mia.yaml.example`) and starts, in tmux:

| tmux session | window | runs | where |
|---|---|---|---|
| `temporal` | `temporal` | Temporal dev server (gRPC `localhost:7233`, UI `:8233`); shared by every service | host |
| `<service>` | `broker` | `csub-broker`, applying your `~/.config/csub/broker.toml` policy | host |
| `<service>` | `worker` | `autodev worker`, wrapped in `agentic-sandbox` | sandbox |

For a personal instance `mia.yaml` lists one service, your `autodev.yaml`. The worker polls GitHub, drives the
workflows, and asks the broker to submit Claude Code jobs to LSF.
```

Prerequisite 2 becomes: `**agentic-sandbox** checked out next to this repo (`../agentic-sandbox`), or set `sandbox_dir` in `mia.yaml` to its `scripts` directory. It needs rootless podman or bwrap set up, per its README.`

In section 4 replace the four commands with:

```bash
cp mia.yaml.example mia.yaml      # keep only the `autodev` service, pointing at your autodev.yaml; keep temporal.managed: true
pixi run mia exec autodev check         # the `temporal` row is BAD until `mia up` starts the dev server; fix any other BAD first
pixi run mia exec autodev init-labels   # creates the autodev-<you>:* labels (idempotent)
pixi run mia up                         # tmux sessions temporal and autodev; applies the poll schedule
pixi run mia exec autodev check         # now every row should say ok
```

In section 5: `pixi run mia attach autodev    # windows: broker, worker (Temporal: mia attach temporal)`.

"Day to day" becomes:

```bash
pixi run mia ps                     # every service, or: mia ps autodev
pixi run mia logs autodev [worker|broker] [-n N] [-f]
pixi run mia logs temporal
pixi run mia attach autodev
pixi run mia restart autodev        # after changing code or config; leaves Temporal and other services alone
pixi run mia restart                # everything, Temporal included
pixi run mia down                   # everything, Temporal last
pixi run mia exec autodev check     # any app command, with this service's config
pixi run test                       # unit tests; no GitHub, LSF or Temporal server needed
```

Gotcha line: `**`agent.api_key_env` is refused** by `mia up` for now. Use a Claude login.`

- [ ] **Step 3: `docs/github-app.md`, `docs/mia-data-workflow.md`, `autodev.yaml.example`**

github-app.md line 85: `scripts/autodev-up.sh` -> `` `mia up` ``. Lines 92 and 96: `pixi run autodev check` / `init-labels` -> `pixi run mia exec autodev check` / `init-labels`. Lines 106 and 128: `pixi run ad restart` -> `pixi run mia restart autodev` (or the service name the App runs under).

mia-data-workflow.md "Operating it":

```
pixi run mia exec mia-data check    # repos, Temporal, csub policy, directories, harness pin
pixi run mia up                     # Temporal, then every service in mia.yaml; or: mia up mia-data
pixi run mia ps | logs mia-data -f | attach mia-data | down mia-data | restart mia-data
pixi run mia exec mia-data seed --dry-run
```

mia-data-workflow.md line 81: `pixi run md seed` -> `pixi run mia exec mia-data seed`.

autodev.yaml.example line 9: `scripts/autodev-up.sh needs it` -> `` `mia up` needs it ``.

- [ ] **Step 4: `notes/autodev-runbook.md`**

Lines 39 and 43: `pixi run autodev check` -> `pixi run mia exec autodev check`. Line 41: `pixi run autodev init-labels` -> `pixi run mia exec autodev init-labels`, and `then `scripts/autodev-up.sh`. It starts, in tmux, a Temporal dev server and` -> `then `pixi run mia up`. It starts, in tmux, a Temporal dev server (session `temporal`) and`.
Line 44: `watch `tmux attach -t autodev-<you>`` -> `watch `pixi run mia attach <service>``.
Lines 46-47 become: `Manage the services listed in `mia.yaml`: `pixi run mia ps`, `mia logs <service> [worker|broker]` (`-n N`, `-f` to follow), `mia logs temporal`, `mia attach <service>`, `mia down [<service>...]`, `mia restart [<service>...]`, `mia up [<service>...]`. Without a service name each command acts on everything, Temporal first on the way up and last on the way down.`
Line 66: `autodev-up.sh` -> `mia up`.
Lines 75-76: `pixi run autodev init-labels`, `scripts/autodev-up.sh`, then `pixi run autodev check` (the `temporal` row needs the dev server that `autodev-up.sh` starts)` -> `` `pixi run mia exec autodev init-labels`, `pixi run mia up`, then `pixi run mia exec autodev check` ``.
Line 100: `md check` -> `mia exec mia-data check`. Lines 108-110: `pixi run md check` -> `pixi run mia exec mia-data check` (twice), `md check` and `md test-pr` -> `mia exec mia-data check` and `mia exec mia-data test-pr`. Lines 119-120: `pixi run md init-labels` / `seed` / `test-pr` -> `pixi run mia exec mia-data init-labels` / `seed` / `test-pr`.
Lines 111-118 become:

```
3. `pixi run mia up` starts Temporal (unless `mia.yaml` says `temporal.managed: false`, in which case it only
   checks that the configured cluster answers), then each service: it ensures the service's namespace, starts
   its broker and sandboxed worker, and applies the poll schedule. Re-running it while a session exists only
   re-applies that service's schedule, so a run that failed partway is repaired by running it again. Uses
   `temporal` from PATH; a managed server downloads one into `temporal.state_dir/bin` otherwise. Manage with
   `mia ps`, `mia logs mia-data [worker|broker]` (`-n N`, `-f`), `mia attach mia-data`, `mia down mia-data`,
   `mia restart mia-data` (needed after a config change; wait for running rounds to finish first). `mia down`
   with no service stops everything, Temporal last; naming a service never touches Temporal.
```

- [ ] **Step 5: `notes/mia-data-handoff.md`**

Line 21: `` CLI (`pixi run md …`) `` -> `` CLI (`pixi run mia exec mia-data …`) ``. Lines 48, 52 and 118: `pixi run md check` / `md check` -> `pixi run mia exec mia-data check` / `mia exec mia-data check`. Lines 54-57 become: `5. **Bring up.** `pixi run mia up` (Temporal if `mia.yaml` manages it, then every service: broker, sandboxed worker, poll schedule; without that schedule labels are never read). Then `mia ps`, `mia logs mia-data -f`, `mia down mia-data`, `mia restart mia-data`. `pixi run mia exec mia-data init-labels`; `pixi run mia exec mia-data seed --dry-run`; `pixi run mia exec mia-data seed`.` Line 60: `pixi run md test-pr` -> `pixi run mia exec mia-data test-pr`.

- [ ] **Step 6: Check and commit**

Run: `grep -rn "up\.sh\|pixi run ad\b\|pixi run md\b\|pixi run autodev\|\`ad \|\`md \|\$SANDBOX" README.md docs notes/autodev-runbook.md notes/mia-data-handoff.md autodev.yaml.example mia_data.yaml.example; true`
Expected: no output. Then:

```bash
git add README.md docs notes/autodev-runbook.md notes/mia-data-handoff.md autodev.yaml.example
git commit -m "Docs: pixi run mia replaces the up-scripts and the ad/md tasks" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Manual smoke on a submit host (not automatable here)

**Files:** none.

- [ ] **Step 1: Migrate the Temporal database** so running workflows survive: `mkdir -p ~/.local/state/temporal && cp <autodev state_dir>-host/temporal.db ~/.local/state/temporal/` while the old session is down (`tmux kill-session -t <deployment>` for each old session, or point `temporal.state_dir` in `mia.yaml` at the old `-host` directory instead).
- [ ] **Step 2:** `cp mia.yaml.example mia.yaml`, edit services to the configs on this host, then `pixi run mia up`, `pixi run mia ps`, `pixi run mia logs autodev -n 5`, `pixi run mia restart snippets`, `pixi run mia ps`, `pixi run mia down`. Confirm `pixi run mia ps` then exits 1 and reports everything stopped.
- [ ] **Step 3:** `pixi run mia exec snippets check` should print the same rows `AUTODEV_CONFIG=autodev-snippets.yaml pixi run ad check` used to, and `pixi run mia exec mia-data seed --dry-run` the same as `pixi run md seed --dry-run` did.
