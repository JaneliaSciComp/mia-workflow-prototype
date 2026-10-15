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
        return Path(f"/scratch/{os.environ.get('USER') or getpass.getuser()}/{self.name}")

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

    def _run(self, *args: str) -> int:
        return subprocess.run([str(self.cli()), *args, "--address", self.address], capture_output=True).returncode

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
