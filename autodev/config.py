"""Config: autodev.yaml + environment overrides. Secrets come only from env / ./.env and key files."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ValidationError, model_validator

from zarr_doctor.config import PROJECT_DIR, load_dotenv

__all__ = ["PROJECT_DIR", "STAGES", "Config", "load_config"]

_ENV_OVERRIDES = {
    "AUTODEV_DEPLOYMENT": ("deployment",),
    "TEMPORAL_ADDRESS": ("temporal", "address"),
    "AUTODEV_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy"),
}

# stage -> (label color, description). `go` is set by humans; the rest by autodev.
STAGES = {
    "go": ("0e8a16", "Ask autodev to implement this issue"),
    "working": ("fbca04", "autodev: agent job running"),
    "review": ("1d76db", "autodev: PR open, waiting for review"),
    "needs-human": ("d93f0b", "autodev: stopped, needs a human"),
    "done": ("5319e7", "autodev: merged"),
}


class GitHubCfg(BaseModel):
    repo: str
    base_branch: str = "main"
    auth: Literal["token", "app"] = "token"  # token: run as yourself; app: GitHub App (service)
    token_env: str = "GITHUB_TOKEN"
    token_file: Path | None = None  # used when $token_env is unset; needed inside the sandbox, which masks .env
    app_id: int | None = None
    installation_id: int | None = None
    private_key_path: Path | None = None
    api_url: str = "https://api.github.com"
    git_url: str | None = None  # default https://github.com/<repo>.git; tests use a local path
    protected_paths: list[str] = [".github/"]


class TemporalCfg(BaseModel):
    address: str = "localhost:7233"
    namespace: str = "default"
    via_https_proxy: bool = False  # inside agentic-sandbox: reach Temporal through the allowlist proxy
    poll_minutes: int = 2


class PathsCfg(BaseModel):
    state_dir: Path = Path("~/.local/state/autodev")  # trusted: mirror, job ids, logs; never mounted into jobs
    work_root: Path  # per-round workspaces; must be under the csub policy's allowed_roots


class AgentCfg(BaseModel):
    model: str = "sonnet"
    walltime_min: int = 60
    cpus: int = 1
    mem_mb: int = 4000
    allow_hosts: list[str] = []  # never GitHub: the agent must not be able to push
    extra_mounts: list[str] = []  # "PATH:ro" only (read-write mounts are not allowed)
    path_prepend: list[str] = []  # dirs put first on PATH inside the job (e.g. where pixi lives)
    max_revisions: int = 3
    api_key_env: str | None = None  # bill the API with this env var's key instead of the broker account's login


class Config(BaseModel):
    deployment: str = "autodev"
    github: GitHubCfg
    temporal: TemporalCfg = TemporalCfg()
    paths: PathsCfg
    agent: AgentCfg = AgentCfg()

    @model_validator(mode="after")
    def _finish(self) -> "Config":
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", self.deployment):
            raise ValueError(f"deployment {self.deployment!r}: use 2-31 chars of a-z, 0-9, '-'")
        g = self.github
        if g.auth == "app" and not (g.app_id and g.installation_id and g.private_key_path):
            raise ValueError("github.auth=app needs github.app_id, installation_id and private_key_path")
        # canonical: csub rejects non-canonical mounts, and round dirs (under work_root) are mounts
        self.paths.state_dir = self.paths.state_dir.expanduser().resolve()
        self.paths.work_root = self.paths.work_root.expanduser().resolve()
        for attr in ("private_key_path", "token_file"):
            if getattr(g, attr):
                setattr(g, attr, getattr(g, attr).expanduser())
        for h in self.agent.allow_hosts:
            h = h.strip().lower().rstrip(".")
            if any(h == b or h.endswith("." + b) for b in ("github.com", "githubusercontent.com")):
                raise ValueError(f"agent.allow_hosts: {h!r} would let the agent reach GitHub")
        secrets = [self.paths.state_dir, g.token_file, g.private_key_path]
        for i, m in enumerate(self.agent.extra_mounts):
            if not m.endswith(":ro"):
                raise ValueError(f"agent.extra_mounts: {m!r} must end with ':ro'")
            mp = Path(m.removesuffix(":ro"))
            if not mp.is_absolute():
                raise ValueError(f"agent.extra_mounts: {m!r} must be an absolute path")
            mp = mp.resolve()
            self.agent.extra_mounts[i] = f"{mp}:ro"
            for sec in filter(None, secrets):
                sec = Path(sec).resolve()
                if mp == sec or mp in sec.parents or sec in mp.parents:
                    raise ValueError(f"agent.extra_mounts: {m!r} overlaps {sec}, which the agent must not see")
        return self

    @property
    def task_queue(self) -> str:
        return self.deployment

    def label(self, stage: str) -> str:
        return f"{self.deployment}:{stage}"

    def branch(self, issue: int) -> str:
        return f"{self.deployment}/issue-{issue}"

    def workflow_id(self, issue: int) -> str:
        return f"{self.deployment}/{self.github.repo}/issue-{issue}"

    @property
    def marker_prefix(self) -> str:
        return f"<!-- {self.deployment}:"

    def marker(self, text: str) -> str:
        return f"<!-- {self.deployment}: {text} -->"

    @property
    def git_url(self) -> str:
        return self.github.git_url or f"https://github.com/{self.github.repo}.git"

    @property
    def repo_slug(self) -> str:
        return self.github.repo.replace("/", "__")


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load config; exits with a readable message if it is missing or invalid."""
    load_dotenv()
    path = Path(path or os.environ.get("AUTODEV_CONFIG") or PROJECT_DIR / "autodev.yaml")
    if not path.is_file():
        sys.exit(f"autodev: config not found at {path} (copy autodev.yaml.example, or set $AUTODEV_CONFIG)")
    data = yaml.safe_load(path.read_text()) or {}
    for env, keys in _ENV_OVERRIDES.items():
        if os.environ.get(env):
            d = data
            for k in keys[:-1]:
                if d.get(k) is None:  # a bare `temporal:` in YAML is None
                    d[k] = {}
                d = d[k]
            d[keys[-1]] = os.environ[env]
    try:
        return Config(**data)
    except ValidationError as e:
        sys.exit(f"autodev: invalid config {path}:\n{e}")
