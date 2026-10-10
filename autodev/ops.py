"""Deployment management: start, stop, status and logs for this deployment's tmux session (name from autodev.yaml)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from .config import PROJECT_DIR

UP_SCRIPT = PROJECT_DIR / "scripts" / "autodev-up.sh"


def log_files(cfg) -> dict[str, Path]:
    """Log name -> path. Worker logs are in the state dir; temporal/broker logs are host-only, beside it."""
    state = cfg.paths.state_dir
    host = state.with_name(state.name + "-host") / "logs"
    return {"worker": state / "logs" / "worker.log", "temporal": host / "temporal.log", "broker": host / "broker.log"}


def _tmux(*args: str, **kw) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["tmux", *args], text=True, **kw)
    except FileNotFoundError:
        sys.exit("autodev: tmux not found on PATH")


def is_running(cfg) -> bool:
    return _tmux("has-session", "-t", f"={cfg.deployment}", capture_output=True).returncode == 0


def cmd_up(cfg, args) -> int:
    return subprocess.run([str(UP_SCRIPT)], env={**os.environ, "AUTODEV_CONFIG": str(args.config_path)}).returncode


def cmd_down(cfg, args) -> int:
    if not is_running(cfg):
        print(f"{cfg.deployment}: not running")
        return 0
    rc = _tmux("kill-session", "-t", f"={cfg.deployment}").returncode
    if rc == 0:
        print(f"{cfg.deployment}: stopped")
    return rc


def cmd_restart(cfg, args) -> int:
    return cmd_down(cfg, args) or cmd_up(cfg, args)


def cmd_ps(cfg, args) -> int:
    if not is_running(cfg):
        print(f"{cfg.deployment}: not running")
        return 1
    print(f"{cfg.deployment}: running (tmux session)")
    r = _tmux("list-windows", "-t", f"={cfg.deployment}", "-F", "  #{window_name}: #{?pane_dead,dead,alive}",
              capture_output=True)
    print(r.stdout, end="")
    for name, path in log_files(cfg).items():
        print(f"  log {name}: {path}")
    return 0


def cmd_attach(cfg, args) -> int:
    if not is_running(cfg):
        sys.exit(f"autodev: {cfg.deployment} is not running (autodev up)")
    return _tmux("attach", "-t", f"={cfg.deployment}").returncode


def cmd_logs(cfg, args) -> int:
    path = log_files(cfg)[args.name]
    if not path.is_file():
        sys.exit(f"autodev: no log at {path}")
    cmd = ["tail", "-n", str(args.lines), *([] if args.no_follow else ["-F"]), str(path)]
    try:
        return subprocess.run(cmd).returncode
    except KeyboardInterrupt:
        return 0
