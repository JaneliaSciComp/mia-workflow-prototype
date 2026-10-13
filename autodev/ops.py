"""Deployment management shared by the autodev and mia-data CLIs: start, stop, status and logs.

A deployment runs inside a tmux session named after `cfg.deployment`, started by an up-script. The CLI
puts the script path and config variable on `args` (args.up_script, args.config_env, args.config_path)
before calling these commands; the defaults are autodev's.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from .config import PROJECT_DIR

if TYPE_CHECKING:
    from .config import Deployment

UP_SCRIPT = PROJECT_DIR / "scripts" / "autodev-up.sh"


def log_files(cfg: Deployment) -> dict[str, Path]:
    """Log name -> path. Worker logs are in the state dir; temporal/broker logs are host-only, beside it."""
    state = cfg.paths.state_dir
    host = state.with_name(state.name + "-host") / "logs"
    return {"worker": state / "logs" / "worker.log", "temporal": host / "temporal.log", "broker": host / "broker.log"}


def _tmux(*args: str, **kwargs) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["tmux", *args], text=True, **kwargs)
    except FileNotFoundError:
        sys.exit("tmux not found on PATH")


def is_running(cfg: Deployment) -> bool:
    return _tmux("has-session", "-t", f"={cfg.deployment}", capture_output=True).returncode == 0


def cmd_up(cfg: Deployment, args: argparse.Namespace) -> int:
    script = getattr(args, "up_script", UP_SCRIPT)
    config_env = getattr(args, "config_env", "AUTODEV_CONFIG")
    return subprocess.run([str(script)], env={**os.environ, config_env: str(args.config_path)}).returncode


def cmd_down(cfg: Deployment, args: argparse.Namespace) -> int:
    if not is_running(cfg):
        print(f"{cfg.deployment}: not running")
        return 0
    returncode = _tmux("kill-session", "-t", f"={cfg.deployment}").returncode
    if returncode == 0:
        print(f"{cfg.deployment}: stopped")
    return returncode


def cmd_restart(cfg: Deployment, args: argparse.Namespace) -> int:
    return cmd_down(cfg, args) or cmd_up(cfg, args)


def cmd_ps(cfg: Deployment, args: argparse.Namespace) -> int:
    """Print whether the session is up, its windows, and where the logs are."""
    if not is_running(cfg):
        print(f"{cfg.deployment}: not running")
        return 1
    print(f"{cfg.deployment}: running (tmux session)")
    windows = _tmux(
        "list-windows",
        "-t",
        f"={cfg.deployment}",
        "-F",
        "  #{window_name}: #{?pane_dead,dead,alive}",
        capture_output=True,
    )
    print(windows.stdout, end="")
    for name, path in log_files(cfg).items():
        if path.is_file():
            print(f"  log {name}: {path}")
    return 0


def cmd_attach(cfg: Deployment, args: argparse.Namespace) -> int:
    if not is_running(cfg):
        sys.exit(f"{cfg.deployment} is not running (run `up` first)")
    return _tmux("attach", "-t", f"={cfg.deployment}").returncode


def cmd_logs(cfg: Deployment, args: argparse.Namespace) -> int:
    path = log_files(cfg)[args.name]
    if not path.is_file():
        sys.exit(f"no log at {path}")
    cmd = ["tail", "-n", str(args.lines), *(["-F"] if args.follow else []), str(path)]
    try:
        return subprocess.run(cmd).returncode
    except KeyboardInterrupt:
        return 0
