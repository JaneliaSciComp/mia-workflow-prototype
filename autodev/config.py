"""autodev configuration: autodev.yaml, with a few environment-variable overrides.

Secrets are never in the YAML file. They come from the environment, from ./.env, or from key files whose
paths the YAML names. The pydantic models validate the file and normalize paths on load, so the rest of the
code can trust what it reads from a `Config`.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Literal, Protocol

import yaml
from pydantic import BaseModel, ValidationError, model_validator

from mia_core.config import PROJECT_DIR as PROJECT_DIR
from mia_core.config import TemporalCfg as TemporalCfg
from mia_core.config import load_dotenv as load_dotenv

# environment variable -> the YAML key path it overrides
_ENV_OVERRIDES = {
    "AUTODEV_DEPLOYMENT": ("deployment",),
    "TEMPORAL_ADDRESS": ("temporal", "address"),
    "AUTODEV_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy"),
}

# stage -> (label color, description). Humans add `go`; autodev sets the rest.
STAGES = {
    "go": ("0e8a16", "Ask autodev to implement this issue"),
    "working": ("fbca04", "autodev: agent job running"),
    "review": ("1d76db", "autodev: PR open, waiting for review"),
    "needs-human": ("d93f0b", "autodev: stopped, needs a human"),
    "done": ("5319e7", "autodev: merged"),
}


def is_github_host(host: str) -> bool:
    """True for github.com, githubusercontent.com and any of their subdomains."""
    host = host.strip().lower().rstrip(".")
    return any(host == base or host.endswith("." + base) for base in ("github.com", "githubusercontent.com"))


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
    trusted_bots: list[str] = []  # GitHub App slugs whose `go` label counts without a permission lookup
    required_checks: list[str] = []  # status contexts / check-run names that must be present and green before merge


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
    max_budget_usd: float | None = None  # passed to `claude -p --max-budget-usd`
    api_key_env: str | None = None  # bill the API with this env var's key instead of the broker account's login


class GitHubAuth(Protocol):
    """The GitHub auth fields both deployments' configs have (see auth.py)."""

    @property
    def auth(self) -> str: ...
    @property
    def token_env(self) -> str: ...
    @property
    def token_file(self) -> Path | None: ...
    @property
    def app_id(self) -> int | None: ...
    @property
    def installation_id(self) -> int | None: ...
    @property
    def private_key_path(self) -> Path | None: ...
    @property
    def api_url(self) -> str: ...


class StatePaths(Protocol):
    @property
    def state_dir(self) -> Path: ...


class Deployment(Protocol):
    """What autodev's shared helpers (auth, the Temporal connection, logging, ops) need from a config.

    Both autodev's and mia-data's Config satisfy it, so mia-data reuses those helpers with its own config."""

    @property
    def deployment(self) -> str: ...
    @property
    def github(self) -> GitHubAuth: ...
    @property
    def temporal(self) -> TemporalCfg: ...
    @property
    def paths(self) -> StatePaths: ...


class Config(BaseModel):
    """The whole autodev.yaml, validated, plus the derived names (labels, branches, markers) built from it."""

    deployment: str = "autodev"
    github: GitHubCfg
    temporal: TemporalCfg = TemporalCfg()
    paths: PathsCfg
    agent: AgentCfg = AgentCfg()

    @model_validator(mode="after")
    def _finish(self) -> Config:
        """Cross-field checks and path normalization that pydantic's per-field validation cannot do."""
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", self.deployment):
            raise ValueError(f"deployment {self.deployment!r}: use 2-31 chars of a-z, 0-9, '-'")
        self._check_github()
        self._resolve_paths()
        self._check_agent()
        self._check_mounts()
        return self

    def _check_github(self) -> None:
        github = self.github
        if github.auth == "app" and not (github.app_id and github.installation_id and github.private_key_path):
            raise ValueError("github.auth=app needs github.app_id, installation_id and private_key_path")
        # the slug as GitHub shows the App, or the login it acts under ("<slug>[bot]"); stored as the login
        slugs = [login.removesuffix("[bot]") for login in github.trusted_bots]
        if bad := [slug for slug in slugs if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", slug)]:
            raise ValueError(f"github.trusted_bots: {bad} must be GitHub App slugs")
        github.trusted_bots = [f"{slug}[bot]" for slug in slugs]
        for attr in ("private_key_path", "token_file"):
            if getattr(github, attr):
                setattr(github, attr, getattr(github, attr).expanduser())

    def _resolve_paths(self) -> None:
        """Paths are made canonical because csub rejects non-canonical mounts, and round dirs (under work_root)
        are mounted into jobs."""
        self.paths.state_dir = self.paths.state_dir.expanduser().resolve()
        self.paths.work_root = self.paths.work_root.expanduser().resolve()

    def _check_agent(self) -> None:
        if self.agent.max_budget_usd is not None and self.agent.max_budget_usd <= 0:
            raise ValueError("agent.max_budget_usd must be positive")
        for host in self.agent.allow_hosts:
            if is_github_host(host):
                raise ValueError(f"agent.allow_hosts: {host.strip()!r} would let the agent reach GitHub")

    def _check_mounts(self) -> None:
        """Extra mounts are read-only, absolute, and must not expose anything that holds credentials or trusted
        state."""
        secrets = [self.paths.state_dir, self.github.token_file, self.github.private_key_path]
        for index, mount in enumerate(self.agent.extra_mounts):
            if not mount.endswith(":ro"):
                raise ValueError(f"agent.extra_mounts: {mount!r} must end with ':ro'")
            mount_path = Path(mount.removesuffix(":ro"))
            if not mount_path.is_absolute():
                raise ValueError(f"agent.extra_mounts: {mount!r} must be an absolute path")
            mount_path = mount_path.resolve()
            self.agent.extra_mounts[index] = f"{mount_path}:ro"
            for secret in filter(None, secrets):
                secret = Path(secret).resolve()
                if mount_path == secret or mount_path in secret.parents or secret in mount_path.parents:
                    raise ValueError(f"agent.extra_mounts: {mount!r} overlaps {secret}, which the agent must not see")

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
        """The start of every HTML-comment marker autodev hides in its own GitHub comments."""
        return f"<!-- {self.deployment}:"

    def marker(self, text: str) -> str:
        return f"<!-- {self.deployment}: {text} -->"

    @property
    def git_url(self) -> str:
        return self.github.git_url or f"https://github.com/{self.github.repo}.git"

    @property
    def repo_slug(self) -> str:
        """The repo name made safe for use as a directory name ("owner__repo")."""
        return self.github.repo.replace("/", "__")


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load and validate the config file, exiting with a readable message if it is missing or invalid."""
    load_dotenv()
    path = Path(path or os.environ.get("AUTODEV_CONFIG") or PROJECT_DIR / "autodev.yaml")
    if not path.is_file():
        sys.exit(f"autodev: config not found at {path} (copy autodev.yaml.example, or set $AUTODEV_CONFIG)")
    data = yaml.safe_load(path.read_text()) or {}
    for env_var, key_path in _ENV_OVERRIDES.items():
        if os.environ.get(env_var):
            node = data
            for key in key_path[:-1]:
                if node.get(key) is None:  # a bare `temporal:` in YAML parses as None
                    node[key] = {}
                node = node[key]
            node[key_path[-1]] = os.environ[env_var]
    try:
        return Config(**data)
    except ValidationError as err:
        sys.exit(f"autodev: invalid config {path}:\n{err}")
