"""The services in mia.yaml and how to run them: a Temporal dev server (or an external one), and per app
service one tmux session holding a csub broker and a worker inside agentic-sandbox (mia-buffers adds buffer-api,
which runs on the host like the broker)."""

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
from urllib.parse import urlparse

from autodev.config import PROJECT_DIR
from mia_core.fs import require_local_dir, require_secret_file

from .manifest import Manifest

PY = PROJECT_DIR / ".pixi/envs/default/bin/python"
BROKER = PROJECT_DIR / ".pixi/envs/default/bin/csub-broker"
SCRATCH = Path("/scratch")  # node-local; the csub broker's unix socket lives under it
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
    config_env: str  # names the deprecated per-app config file
    proxy_env: str

    @property
    def module(self) -> str:
        return f"{self.package}.cli"

    @property
    def has_local_root(self) -> bool:  # host-local disk for the lock directory and the service state
        return self.package in ("mia_data", "mia_buffers")

    def load_config(self, path: Path, service: str | None = None) -> Any:
        """The app's Config: service `service` of mia.yaml at `path`, or the legacy per-app file when None."""
        return importlib.import_module(f"{self.package}.config").load_config(path, service=service)

    def extra_mounts(self, cfg: Any) -> list[tuple[str, Path]]:
        mounts: list[tuple[str, Path]] = []
        if self.package == "mia_data":
            mounts = [("--ro", cfg.harness.dir), ("--rw", cfg.paths.storage_root)]
        if self.package == "mia_buffers":  # bindings are read from mia-data's storage_root; the caches are written
            mounts = [
                ("--ro", cfg.harness.dir),
                ("--ro", cfg.paths.storage_root),
                ("--rw", cfg.paths.raw_root),
                ("--rw", cfg.paths.staged_root),
            ]
        if self.has_local_root and cfg.paths.local_root:
            mounts.append(("--rw", cfg.paths.local_root / "locks"))
        return mounts

    def api_endpoint(self, cfg: Any) -> tuple[str, int] | None:
        """(host, port) of buffer-api from `service.url`: `mia up` runs it beside the broker and lets the worker
        reach that host through the sandbox proxy. None for the apps without a service."""
        if self.package != "mia_buffers":
            return None
        url = urlparse(cfg.service.url)
        return url.hostname or "localhost", url.port


APPS = {
    "autodev": App("autodev", "AUTODEV_CONFIG", "AUTODEV_TEMPORAL_VIA_PROXY"),
    "mia-data": App("mia_data", "MIA_DATA_CONFIG", "MIA_DATA_TEMPORAL_VIA_PROXY"),
    "mia-buffers": App("mia_buffers", "MIA_BUFFERS_CONFIG", "MIA_BUFFERS_TEMPORAL_VIA_PROXY"),
}


@dataclass
class Service:
    name: str  # also the tmux session
    app: App
    config_path: Path  # the file the worker reads: mia.yaml, or the legacy per-app file
    cfg: Any  # the app's validated Config
    legacy: bool = False  # mia.yaml named a per-app config file (`services.<name>.config`)

    def env(self) -> dict[str, str]:
        """The variables that point the app's CLI at its config."""
        if self.legacy:
            return {self.app.config_env: str(self.config_path)}
        return {"MIA_SERVICE": self.name, "MIA_CONFIG": str(self.config_path)}

    @property
    def state(self) -> Path:
        return self.cfg.paths.state_dir

    @property
    def host(self) -> Path:  # host-only files beside the state dir, which the sandbox binds read-write
        return self.state.with_name(self.state.name + "-host")

    @property
    def run(self) -> Path:  # the csub broker's unix socket dir
        return SCRATCH / (os.environ.get("USER") or getpass.getuser()) / self.name

    @property
    def secret(self) -> Path | None:
        g = self.cfg.github
        path = g.private_key_path if g.auth == "app" else g.token_file
        return Path(path).expanduser().resolve() if path else None

    @property
    def token_file(self) -> Path | None:  # the buffer-api worker token, for apps that have a `service` section
        path = getattr(getattr(self.cfg, "service", None), "token_file", None)
        return Path(path).expanduser().resolve() if path else None

    def logs(self) -> dict[str, Path]:
        logs = {"worker": self.state / "logs/worker.log", "broker": self.host / "logs/broker.log"}
        if self.app.api_endpoint(self.cfg):
            logs["api"] = self.host / "logs/api.log"
        return logs


class Stack:
    """mia.yaml loaded: every service with its config, and the one Temporal address they share."""

    def __init__(self, manifest: Manifest):
        self.manifest = manifest
        self.services: dict[str, Service] = {}
        for name, block in manifest.services.items():
            app = APPS[block.app]
            if block.config:
                print(f"{name}: services.{name}.config is deprecated; move its sections into mia.yaml", file=sys.stderr)
                self.services[name] = Service(name, app, block.config, app.load_config(block.config), legacy=True)
            elif manifest.path is None:
                sys.exit(f"mia: service {name!r} has no config: file and the manifest was not loaded from a file")
            else:
                self.services[name] = Service(name, app, manifest.path, app.load_config(manifest.path, service=name))
        addresses = {s.cfg.temporal.address for s in self.services.values()}
        if len(addresses) != 1:
            sys.exit(f"mia: services disagree on temporal.address: {sorted(addresses)}")
        self.temporal_address: str = addresses.pop()
        # Two workers binding one state dir read-write, or two deployments with one label prefix, would corrupt
        # each other: the usual slip is copying a config and forgetting to change these.
        seen: dict[tuple[str, object], str] = {}
        for name, svc in self.services.items():
            for key, value in (("paths.state_dir", svc.state), ("deployment", svc.cfg.deployment)):
                if (key, value) in seen:
                    sys.exit(f"mia: services {seen[key, value]!r} and {name!r} share {key} {value}")
                seen[key, value] = name

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
        self._reported = False

    @property
    def host(self) -> str:
        return self.address.rsplit(":", 1)[0] if ":" in self.address else self.address

    def cli_if_present(self) -> Path | None:
        """The `temporal` CLI on PATH or already downloaded; None if a managed server would have to download it."""
        if found := shutil.which("temporal"):
            return Path(found)
        if not self.managed:
            sys.exit("mia: the `temporal` CLI must be on PATH when temporal.managed is false")
        target = self.state / "bin/temporal"
        return target if target.exists() else None

    def cli(self) -> Path:
        if present := self.cli_if_present():
            return present
        target = self.state / "bin/temporal"
        target.parent.mkdir(parents=True, exist_ok=True)
        print(f"downloading the temporal CLI to {target}")
        cmd = f"curl -sSL {shlex.quote(TEMPORAL_CLI_URL)} | tar xz -C {shlex.quote(str(target.parent))} temporal"
        subprocess.run(cmd, shell=True, check=True)
        return target

    def _run(self, *args: str, download: bool = True) -> int:
        cli = self.cli() if download else self.cli_if_present()
        if cli is None:  # no CLI and not allowed to fetch one: treat the server as unreachable
            return 1
        return subprocess.run([str(cli), *args, "--address", self.address], capture_output=True).returncode

    def healthy(self, download: bool = True) -> bool:
        return self._run("operator", "cluster", "health", download=download) == 0

    def status(self) -> str:
        """Read-only: never downloads the CLI or writes anything."""
        if self.managed:
            return "running" if is_running(self.SESSION) else "external" if self.healthy(download=False) else "stopped"
        return "external (healthy)" if self.healthy() else "external (unreachable)"

    def up(self) -> None:
        if self.healthy():
            if not self._reported:  # every service_up checks Temporal; report the result once per command
                print(f"temporal: {self.status()} at {self.address}")
            self._reported = True
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
    prerequisites = (
        (PY, "run `pixi install`"),
        (BROKER, "run `pixi install`"),
        (sandbox, "set sandbox_dir in mia.yaml"),
    )
    for path, hint in prerequisites:
        if not os.access(path, os.X_OK):
            sys.exit(f"{svc.name}: {path} not found ({hint})")
    endpoint = svc.app.api_endpoint(cfg)
    if endpoint and endpoint[0] in ("localhost", "127.0.0.1", "::1"):  # the sandbox bypasses its proxy for these
        sys.exit(f"{svc.name}: service.url must name this host, not {endpoint[0]}: inside the sandbox that is the "
                 "sandbox's own loopback, where nothing listens")  # fmt: skip
    if svc.app.has_local_root:
        local_root = cfg.paths.local_root
        # ponytail: a legacy per-app file may omit local_root (no lock dir); drop the exemption with `config:`
        if local_root is None and not svc.legacy:
            sys.exit(f"{svc.name}: set paths.local_root in mia.yaml (host-local disk for the lock directory and the "
                     "service state)")  # fmt: skip
        if local_root is not None:
            try:
                require_local_dir(local_root)
            except RuntimeError as error:
                sys.exit(f"{svc.name}: {error}")
            for d in (local_root / "locks", local_root / "service"):
                d.mkdir(parents=True, exist_ok=True)
    token_file = svc.token_file
    if svc.app.package == "mia_buffers" and token_file is None and not svc.legacy:
        sys.exit(f"{svc.name}: set service.token_file in mia.yaml and run `mia init-token`")
    if token_file is not None:
        try:
            require_secret_file(token_file)
        except RuntimeError as error:
            sys.exit(f"{svc.name}: {error}")
    extra = svc.app.extra_mounts(cfg)
    rw_extra = (p for flag, p in extra if flag == "--rw")
    for d in (svc.state / "logs", svc.host / "logs", cfg.paths.work_root, svc.run, *rw_extra):
        d.mkdir(parents=True, exist_ok=True)

    temporal.up()
    temporal.ensure_namespace(cfg.temporal.namespace)

    schedule = [str(PY), "-m", svc.app.module, "schedule", "apply"]
    env = {**os.environ, **svc.env()}
    if is_running(svc.name):
        subprocess.run(schedule, env=env, check=True)
        print(f"{svc.name}: already running (tmux attach -t {svc.name})")
        return 0

    sock = svc.run / "csub.sock"
    broker_log = svc.host / "logs/broker.log"
    new_session(svc.name, "broker", f"{shell(BROKER, '--serve', sock)} 2>&1 | tee -a {shell(broker_log)}")
    allow = ["github.com", "api.github.com", temporal.host]
    if endpoint:  # buffer-api: on the host, unsandboxed like the broker
        allow.append(endpoint[0])
        env_pairs = [f"{name}={value}" for name, value in svc.env().items()]
        api = shell("env", *env_pairs, PY, "-m", svc.app.module, "serve")
        new_window(svc.name, "api", f"{api} 2>&1 | tee -a {shell(svc.host / 'logs/api.log')}")
    # cwd = the state dir, which sandbox-run.sh binds read-write; the repo (code + pixi env), config and secret
    # are read-only. The worker reaches Temporal through the sandbox's allowlist proxy.
    mounts: list[object] = ["--ro", PROJECT_DIR, "--ro", svc.config_path, "--ro", svc.secret]
    if token_file is not None and token_file != svc.secret:
        mounts += ["--ro", token_file]
    for flag, path in extra:
        mounts += [flag, path]
    mounts += ["--rw", cfg.paths.work_root, "--rw", svc.run]
    worker = shell(
        sandbox, *mounts, *(a for h in allow for a in ("--allow", h)), "--",
        "env", *(f"{key}={value}" for key, value in svc.env().items()), "CSUB_TRANSPORT=unix", f"CSUB_SOCKET={sock}",
        f"{svc.app.proxy_env}=1", PY, "-m", svc.app.module, "worker",
    )  # fmt: skip
    new_window(svc.name, "worker", f"cd {shell(svc.state)} && {worker}; read -p 'worker exited; press enter'")
    subprocess.run(schedule, env=env, check=True)
    print(f"{svc.name}: started (attach: mia attach {svc.name}; logs: mia logs {svc.name} -f)")
    return 0
