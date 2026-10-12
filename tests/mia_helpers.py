"""Shared helpers for mia_data tests."""

from __future__ import annotations

from pathlib import Path

from mia_data.config import Config


def make_cfg(tmp_path: Path, **overrides) -> Config:
    harness = tmp_path / "harness"
    harness.mkdir(exist_ok=True)
    data = {
        "github": {"data_repo": "o/data", "snippets_repo": "o/snippets", "catalog_repo": "o/catalog"},
        "paths": {"state_dir": str(tmp_path / "state"), "work_root": str(tmp_path / "work")},
        "storage": {"root": str(tmp_path / "storage")},
        "harness": {"dir": str(harness)},
    }
    for k, v in overrides.items():
        data[k] = {**data[k], **v} if isinstance(v, dict) and isinstance(data.get(k), dict) else v
    return Config(**data)
