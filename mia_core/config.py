"""Configuration shared by every MIA application: the common models, .env loading and the mia.yaml merge.

`mia.yaml` holds the sections every service shares plus a `services` map; `service_config` turns it into the
one-service dict an application's own config model validates.
"""

from __future__ import annotations

import copy
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, Field, field_validator

PROJECT_DIR = Path(__file__).resolve().parent.parent

MANIFEST_ONLY = frozenset({"sandbox_dir", "services"})  # top-level keys no service config receives
TEMPORAL_MANIFEST_ONLY = frozenset({"managed", "state_dir"})  # temporal keys read by the ops CLI, not by services


class TemporalCfg(BaseModel):
    address: str = "localhost:7233"
    namespace: str = "default"
    via_https_proxy: bool = False  # inside agentic-sandbox: reach Temporal through the allowlist proxy
    poll_minutes: int = 2


class ServiceCfg(BaseModel):
    url: str  # buffer-api base URL, reachable from the worker
    token_file: Path | None = None  # the worker token the service checks

    @field_validator("url")
    @classmethod
    def _has_port(cls, url: str) -> str:
        """An explicit port, so `serve` and `mia up` read the same one from the URL."""
        if urlparse(url).port is None:
            raise ValueError("service.url needs an explicit port, e.g. http://host.example.org:8765")
        return url


class KeeperCfg(BaseModel):
    minutes: int = Field(default=30, ge=1)  # SQLite backup cadence of buffer-api
    catalog_sync_hours: int = Field(default=24, ge=1)


def load_dotenv(path: Path = PROJECT_DIR / ".env") -> None:
    """Minimal .env loader: KEY=VALUE lines; variables already in the environment win."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def manifest_path(path: str | os.PathLike | None = None) -> Path:
    """The manifest to read: `path`, else $MIA_CONFIG, else mia.yaml in the project directory."""
    return Path(path or os.environ.get("MIA_CONFIG") or PROJECT_DIR / "mia.yaml")


def read_manifest(path: Path) -> dict:
    """Parse the manifest, exiting with a one-line message if it is missing, not YAML, or not a mapping."""
    if not path.is_file():
        sys.exit(f"mia: config not found at {path} (copy mia.yaml.example, or set $MIA_CONFIG)")
    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as err:
        sys.exit(f"mia: invalid config {path}: {err}")
    if not isinstance(data, dict):
        sys.exit(f"mia: invalid config {path}: expected a mapping at the top level")
    return data


def service_config(manifest: dict, name: str) -> dict:
    """Merge the shared sections with service `name`'s block (one level deep); the input is not modified."""
    services = manifest.get("services")
    if not isinstance(services, dict) or name not in services:
        known = sorted(services) if isinstance(services, dict) else []
        raise ValueError(f"no service {name!r} in mia.yaml; services: {known}")
    block = services[name]
    if not isinstance(block, dict) or not isinstance(block.get("app"), str):
        raise ValueError(f"services.{name}: needs app: autodev | mia-data | mia-buffers")
    result = copy.deepcopy({key: value for key, value in manifest.items() if key not in MANIFEST_ONLY})
    if isinstance(result.get("temporal"), dict):
        result["temporal"] = {k: v for k, v in result["temporal"].items() if k not in TEMPORAL_MANIFEST_ONLY}
    for key, value in block.items():
        if key in ("app", "config"):  # `config` is the legacy per-service file pointer
            continue
        if not isinstance(value, dict):
            raise ValueError(f"services.{name}.{key}: a service block holds sections; {key!r} is not a mapping")
        shared = result.get(key) or {}
        if not isinstance(shared, dict):
            raise ValueError(f"{key}: the shared section must be a mapping")
        result[key] = {**shared, **copy.deepcopy(value)}
    result["deployment"] = name
    paths = result.get("paths")
    if isinstance(paths, dict) and "state_root" in paths:
        state_root = paths.pop("state_root")
        if not isinstance(state_root, str):
            raise ValueError("paths.state_root must be a path string")
        paths.setdefault("state_dir", str(Path(state_root) / name))
    return result


def load_service_config(name: str, path: str | os.PathLike | None = None) -> dict:
    """`service_config` of the manifest on disk, exiting with a readable message instead of raising."""
    try:
        return service_config(read_manifest(manifest_path(path)), name)
    except ValueError as err:
        sys.exit(f"mia: {err}")
