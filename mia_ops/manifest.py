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
    app: Literal["autodev", "mia-data", "mia-buffers"]
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
