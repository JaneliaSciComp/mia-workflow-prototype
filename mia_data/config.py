"""Config: mia_data.yaml + environment overrides. Secrets come only from env / ./.env and key files."""

from __future__ import annotations

import math
import os
import re
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ValidationError, model_validator

from autodev.config import PROJECT_DIR, AgentCfg, TemporalCfg, is_github_host, load_dotenv

_ENV_OVERRIDES = {"TEMPORAL_ADDRESS": ("temporal", "address"), "MIA_DATA_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy")}

# stage -> (label color, description); labels are `data:<stage>`
STAGES = {
    "new": ("ededed", "mia-data: dataset issue seen"),
    "matching": ("fbca04", "mia-data: matching the dataset to a snippet"),
    "developing": ("fbca04", "mia-data: agent writing a snippet"),
    "testing": ("fbca04", "mia-data: testing the snippet"),
    "pr-ready": ("1d76db", "mia-data: snippet PR ready for review"),
    "review": ("1d76db", "mia-data: waiting for review"),
    "cropping": ("fbca04", "mia-data: producing crops"),
    "done": ("5319e7", "mia-data: crops published"),
    "needs-human": ("d93f0b", "mia-data: stopped, needs a human"),
    "ineligible": ("cccccc", "mia-data: dataset not eligible"),
}
HUMAN_LABELS = {  # set by humans
    "go": ("0e8a16", "Start (or restart) a mia-data round"),
    "approve": ("0e8a16", "Approve the test crops; produce the full set"),
}
EXTRA_LABELS = {  # plain labels; modality:<family> labels are generated per family at seed time
    "has-labels": ("c5def5", "Dataset ships labels or annotations"),
    "maybe-small": ("c5def5", "Dataset may be too small for crops"),
    "licence-excluded": ("cccccc", "Licence not in crops.licences"),
}


class GitHubCfg(BaseModel):
    data_repo: str
    snippets_repo: str
    catalog_repo: str
    snippets_git_url: str | None = None  # clone URL override for the snippet mirror (default: GitHub https)
    auth: Literal["token", "app"] = "token"
    token_env: str = "GITHUB_TOKEN"
    token_file: Path | None = None
    app_id: int | None = None
    installation_id: int | None = None
    private_key_path: Path | None = None
    api_url: str = "https://api.github.com"


class PathsCfg(BaseModel):
    state_dir: Path = Path("~/.local/state/mia-data")
    work_root: Path


class StorageCfg(BaseModel):
    root: Path


class GalleryCfg(BaseModel):
    branch: str = "galleries"


class CropsCfg(BaseModel):
    size: int = 256
    per_round: int = 16
    test_per_round: int = 4
    per_plane: int = 4
    per_file: int = 8
    licences: list[str] = ["CC0-1.0", "CC-BY-4.0", "CC-BY-3.0", "MIT"]


class LimitsCfg(BaseModel):
    job_bytes: float = 5e9
    job_walltime_min: int = 120
    agent_max_budget_usd: float = 5
    max_stage_attempts: int = 1
    max_pr_revisions: int = 5


class HarnessCfg(BaseModel):
    dir: Path


class NetworkCfg(BaseModel):
    package_hosts: list[str] = ["pypi.org", "files.pythonhosted.org", "conda.anaconda.org", "repo.prefix.dev"]
    redirect_hosts: dict[str, list[str]] = {}  # exact hosts, no patterns


class Config(BaseModel):
    deployment: str = "mia-data"
    github: GitHubCfg
    temporal: TemporalCfg = TemporalCfg(namespace="mia-data")
    paths: PathsCfg
    storage: StorageCfg
    gallery: GalleryCfg = GalleryCfg()
    crops: CropsCfg = CropsCfg()
    limits: LimitsCfg = LimitsCfg()
    agent: AgentCfg = AgentCfg()
    harness: HarnessCfg
    network: NetworkCfg = NetworkCfg()

    @model_validator(mode="after")
    def _finish(self) -> "Config":
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", self.deployment):
            raise ValueError(f"deployment {self.deployment!r}: use 2-31 chars of a-z, 0-9, '-'")
        g = self.github
        if g.auth == "app" and not (g.app_id and g.installation_id and g.private_key_path):
            raise ValueError("github.auth=app needs github.app_id, installation_id and private_key_path")
        for attr in ("private_key_path", "token_file"):
            if getattr(g, attr):
                setattr(g, attr, getattr(g, attr).expanduser())
        p = self.paths
        for o in (p, self.storage, self.harness):
            for k, v in vars(o).items():
                if isinstance(v, Path):
                    setattr(o, k, v.expanduser().resolve())
        for other in (p.work_root, self.storage.root):
            if p.state_dir == other or p.state_dir in other.parents or other in p.state_dir.parents:
                raise ValueError(f"paths.state_dir {p.state_dir} overlaps {other}")
        for k, v in vars(self.limits).items():
            if not (math.isfinite(v) and v > 0):
                raise ValueError(f"limits.{k} must be finite and positive")
        if self.limits.max_stage_attempts != 1:
            raise ValueError("limits.max_stage_attempts: only 1 is supported (a failed job ends the round)")
        if not self.crops.licences:
            raise ValueError("crops.licences must not be empty")
        n = self.network
        for h in n.package_hosts + list(n.redirect_hosts) + [x for v in n.redirect_hosts.values() for x in v]:
            if is_github_host(h):
                raise ValueError(f"network: {h!r} is a GitHub host")
        if bad := [x for v in n.redirect_hosts.values() for x in v if "*" in x]:
            raise ValueError(f"network.redirect_hosts: {bad} must be exact hosts, no '*'")
        return self

    @property
    def task_queue(self) -> str:
        return self.deployment

    @property
    def await_queue(self) -> str:
        return self.deployment + "-await"

    def label(self, stage: str) -> str:
        return f"data:{stage}"

    def workflow_id(self, issue: int) -> str:
        return f"mia-data/{self.github.data_repo}/issue-{issue}"

    def marker(self, text: str) -> str:
        return f"<!-- mia-data: {text} -->"


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load config; exits with a readable message if it is missing or invalid."""
    load_dotenv()
    path = Path(path or os.environ.get("MIA_DATA_CONFIG") or PROJECT_DIR / "mia_data.yaml")
    if not path.is_file():
        sys.exit(f"mia-data: config not found at {path} (copy mia_data.yaml.example, or set $MIA_DATA_CONFIG)")
    data = yaml.safe_load(path.read_text()) or {}
    for env, keys in _ENV_OVERRIDES.items():
        if os.environ.get(env):
            d = data
            for k in keys[:-1]:
                if d.get(k) is None:
                    d[k] = {}
                d = d[k]
            d[keys[-1]] = os.environ[env]
    try:
        return Config(**data)
    except ValidationError as e:
        sys.exit(f"mia-data: invalid config {path}:\n{e}")
