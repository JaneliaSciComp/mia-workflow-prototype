"""Config: zarr_doctor.yaml + environment overrides. Secrets only come from env (or ./.env)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ValidationError, model_validator

PROJECT_DIR = Path(__file__).resolve().parent.parent

# Env vars that override YAML keys.
_ENV_OVERRIDES = {
    "LMD_ROOT": ("lmd_root",),
    "RESULTS_DIR": ("results_dir",),
    "STATE_DIR": ("state_dir",),
    "TEMPORAL_ADDRESS": ("temporal", "address"),
    "LSF_BILLING_GROUP": ("cluster", "project"),
}


class GitHubCfg(BaseModel):
    repo: str = "AI-HHMI/zarr-doctor-poc"
    token_env: str = "GITHUB_TOKEN"
    results_branch: str = "zarr-doctor-results"
    api_url: str = "https://api.github.com"


class TriggerCfg(BaseModel):
    mode: Literal["project", "label"] = "project"
    project_number: int | None = None
    status_field: str = "Status"
    statuses: dict[str, str] = {
        "discovered": "Discovered", "run": "Run", "running": "Running", "done": "Done", "failed": "Failed",
    }


class TemporalCfg(BaseModel):
    address: str = "localhost:7233"
    namespace: str = "default"
    task_queue: str = "zarr-doctor"
    poll_every_minutes: int = 2


class ClusterCfg(BaseModel):
    config_path: str = "./cluster_api.yaml"
    profile: str = "janelia_lsf"
    job_name_prefix: str = "zd"
    # No queue: on the Janelia cluster, -W <= 60 auto-routes to `short` (best turnaround).
    resources: dict = {"cpus": 4, "memory": "16 GB", "walltime": "01:00"}
    project: str | None = None  # LSF billing project (bsub -P); required for scicompsoft members


class HistogramCfg(BaseModel):
    level: str = "s0"
    max_chunks: int = 256


class DiscoveryCfg(BaseModel):
    max_new_default: int = 10
    creates_per_second: float = 1.0


class Config(BaseModel):
    lmd_root: Path
    results_dir: Path
    state_dir: Path = Path("~/.local/state/zarr-doctor")
    github: GitHubCfg = GitHubCfg()
    trigger: TriggerCfg = TriggerCfg()
    temporal: TemporalCfg = TemporalCfg()
    cluster: ClusterCfg = ClusterCfg()
    histogram: HistogramCfg = HistogramCfg()
    discovery: DiscoveryCfg = DiscoveryCfg()

    @model_validator(mode="after")
    def _finish(self) -> "Config":
        self.lmd_root = self.lmd_root.expanduser()
        self.results_dir = self.results_dir.expanduser()
        self.state_dir = self.state_dir.expanduser()
        if self.trigger.mode == "project" and self.trigger.project_number is None:
            raise ValueError("trigger.project_number is required when trigger.mode is 'project'")
        return self

    @property
    def owner(self) -> str:
        return self.github.repo.split("/")[0]

    @property
    def repo_name(self) -> str:
        return self.github.repo.split("/")[1]

    @property
    def walltime_minutes(self) -> int:
        h, _, m = str(self.cluster.resources.get("walltime", "01:00")).partition(":")
        return int(h) * 60 + int(m or 0)


def load_dotenv(path: Path = PROJECT_DIR / ".env") -> None:
    """Minimal .env loader: KEY=VALUE lines; real env vars win."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load config; exits with a readable message if it is missing or invalid."""
    load_dotenv()
    path = Path(path or os.environ.get("ZARR_DOCTOR_CONFIG") or PROJECT_DIR / "zarr_doctor.yaml")
    if not path.is_file():
        sys.exit(f"zarr-doctor: config not found at {path} (copy zarr_doctor.yaml.example, or set --config / $ZARR_DOCTOR_CONFIG)")
    data = yaml.safe_load(path.read_text()) or {}
    for env, keys in _ENV_OVERRIDES.items():
        if os.environ.get(env):
            d = data
            for k in keys[:-1]:
                d = d.setdefault(k, {})
            d[keys[-1]] = os.environ[env]
    try:
        return Config(**data)
    except ValidationError as e:
        sys.exit(f"zarr-doctor: invalid config {path}:\n{e}")
