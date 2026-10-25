"""mia.yaml as `mia` reads it: the services on this host (name -> app + its sections), where agentic-sandbox is,
and whether `mia` runs the Temporal dev server itself or depends on an external one (production: Temporal in k8s).
The applications validate the rest of the file themselves (`mia_core.config.service_config`)."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from autodev.config import PROJECT_DIR
from mia_core.config import manifest_path, read_manifest

NAME_RE = re.compile(r"[a-z][a-z0-9-]{1,30}")


class TemporalCfg(BaseModel):
    model_config = ConfigDict(extra="allow")  # address, poll_minutes: shared with the applications, not read here

    managed: bool = False  # True: `mia` runs a local dev server (dev hosts); False: something else does (k8s)
    state_dir: Path = Path("~/.local/state/temporal")  # managed only: temporal.db, logs/, bin/temporal


class ServiceCfg(BaseModel):
    model_config = ConfigDict(extra="allow")  # the service's own sections, validated by its application

    app: Literal["autodev", "mia-data", "mia-buffers"]
    config: Path | None = None  # deprecated: a separate per-app config file, relative to the repo


class Manifest(BaseModel):
    sandbox_dir: Path = Path("../agentic-sandbox/scripts")  # relative to the repo
    temporal: TemporalCfg = TemporalCfg()
    services: dict[str, ServiceCfg]
    path: Path | None = None  # the file this was loaded from (set by load_manifest)

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
            if service.config is None:
                continue
            service.config = (PROJECT_DIR / service.config).resolve()
            if service.config in seen:
                raise ValueError(f"services: {name!r} and {seen[service.config]!r} use the same config file")
            seen[service.config] = name
        return self


def load_manifest(path: str | os.PathLike | None = None) -> Manifest:
    """Load and validate mia.yaml ($MIA_CONFIG; $MIA_MANIFEST is the deprecated name), exiting with a readable
    message if it is missing or invalid."""
    # absolute: the worker runs from its state dir and mounts this file
    path = manifest_path(path or os.environ.get("MIA_CONFIG") or os.environ.get("MIA_MANIFEST")).expanduser().resolve()
    data = read_manifest(path)
    try:
        return Manifest(**{**data, "path": path})
    except ValidationError as e:
        sys.exit(f"mia: {path}: {e}")
