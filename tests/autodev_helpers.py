"""Shared helpers for autodev tests."""

from __future__ import annotations

from pathlib import Path

from autodev.config import Config

API = "https://api.github.com"
R = "/repos/o/r"


def make_cfg(tmp_path: Path, github: dict | None = None, agent: dict | None = None, **top) -> Config:
    return Config(**{
        "deployment": "autodev",
        "github": {"repo": "o/r", **(github or {})},
        "paths": {"state_dir": str(tmp_path / "state"), "work_root": str(tmp_path / "work")},
        "agent": agent or {},
        **top,
    })
