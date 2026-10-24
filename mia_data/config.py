"""mia-data configuration: mia_data.yaml, with a few environment-variable overrides.

mia-data works with three GitHub repositories: the data repo (one issue per dataset, where humans label
and the galleries branch lives), the snippets repo (mia-snippets, the loader code) and the catalog repo
(mia-agentic-search, the dataset records and download state). Secrets come only from the environment,
./.env and key files, never from the YAML.
"""

from __future__ import annotations

import math
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import ClassVar, Literal
from urllib.parse import quote

import yaml
from pydantic import BaseModel, ValidationError, model_validator

from autodev.config import PROJECT_DIR, AgentCfg, TemporalCfg, is_github_host, load_dotenv

_ENV_OVERRIDES = {
    "TEMPORAL_ADDRESS": ("temporal", "address"),
    "MIA_DATA_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy"),
}

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
    """`work_root` and `storage_root` take a path, or `{path, url}` where `url` is where a browser (Fileglancer)
    shows that folder; issue comments then link the folders they mention."""

    ROOTS: ClassVar[tuple[str, ...]] = ("work_root", "storage_root")  # the roots that take `{path, url}`

    state_dir: Path = Path("~/.local/state/mia-data")
    work_root: Path
    work_root_url: str | None = None
    storage_root: Path
    storage_root_url: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data):
        """Accept `{path, url}` for a root and split it into the `<root>` and `<root>_url` fields."""
        if isinstance(data, dict):
            data = dict(data)
            for key in cls.ROOTS:
                if isinstance(value := data.get(key), dict):
                    data[key], data[f"{key}_url"] = value.get("path"), value.get("url")
        return data

    def link(self, path: Path) -> str | None:
        """The browse URL of `path`, from the url of the root it is under; None when that root has none."""
        for name in self.ROOTS:
            root, url = getattr(self, name), getattr(self, f"{name}_url")
            if url and path.is_relative_to(root):
                rel = path.relative_to(root)
                return url.rstrip("/") + ("" if rel == Path() else "/" + quote(rel.as_posix()))
        return None


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
    dir: Path  # a checkout of the harness repo with its pixi environment installed


class NetworkCfg(BaseModel):
    package_hosts: list[str] = ["pypi.org", "files.pythonhosted.org", "conda.anaconda.org", "repo.prefix.dev"]
    redirect_hosts: dict[str, list[str]] = {}  # data host -> hosts it redirects to; exact names, no patterns


class Config(BaseModel):
    """The whole mia_data.yaml, validated, plus the derived names (labels, markers, queues) built from it."""

    deployment: str = "mia-data"
    github: GitHubCfg
    temporal: TemporalCfg = TemporalCfg(namespace="mia-data")
    paths: PathsCfg
    gallery: GalleryCfg = GalleryCfg()
    crops: CropsCfg = CropsCfg()
    limits: LimitsCfg = LimitsCfg()
    agent: AgentCfg = AgentCfg()
    harness: HarnessCfg
    network: NetworkCfg = NetworkCfg()

    @model_validator(mode="after")
    def _finish(self) -> Config:
        """Cross-field checks and path normalization that pydantic's per-field validation cannot do."""
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", self.deployment):
            raise ValueError(f"deployment {self.deployment!r}: use 2-31 chars of a-z, 0-9, '-'")
        self._check_github()
        self._resolve_paths()
        self._check_limits()
        self._check_agent()
        if not self.crops.licences:
            raise ValueError("crops.licences must not be empty")
        self._check_network()
        return self

    def _check_github(self) -> None:
        github = self.github
        if github.auth == "app" and not (github.app_id and github.installation_id and github.private_key_path):
            raise ValueError("github.auth=app needs github.app_id, installation_id and private_key_path")
        for attr in ("private_key_path", "token_file"):
            if getattr(github, attr):
                setattr(github, attr, getattr(github, attr).expanduser())

    def _resolve_paths(self) -> None:
        """Make every path absolute, and keep state_dir (secrets, never mounted) apart from the mounted roots."""
        paths = self.paths
        for section in (paths, self.harness):
            for name, value in vars(section).items():
                if isinstance(value, Path):
                    setattr(section, name, value.expanduser().resolve())
        for other in (paths.work_root, paths.storage_root):
            if paths.state_dir == other or paths.state_dir in other.parents or other in paths.state_dir.parents:
                raise ValueError(f"paths.state_dir {paths.state_dir} overlaps {other}")

    def _check_limits(self) -> None:
        for name, value in vars(self.limits).items():
            if not (math.isfinite(value) and value > 0):
                raise ValueError(f"limits.{name} must be finite and positive")
        if self.limits.max_stage_attempts != 1:
            raise ValueError("limits.max_stage_attempts: only 1 is supported (a failed job ends the round)")

    def _check_agent(self) -> None:
        """The agent section is autodev's; mia-data's jobs never read these fields, so setting them is a mistake."""
        for name in ("allow_hosts", "extra_mounts", "api_key_env"):
            if getattr(self.agent, name):
                raise ValueError(
                    f"agent.{name} is not used by mia-data (job hosts come from the binding and network.*, "
                    "mounts from the job builders); remove it"
                )

    def _check_network(self) -> None:
        """No GitHub host may be allowed to a job, and redirect targets must be exact hosts."""
        network = self.network
        redirect_targets = [target for targets in network.redirect_hosts.values() for target in targets]
        for host in network.package_hosts + list(network.redirect_hosts) + redirect_targets:
            if is_github_host(host):
                raise ValueError(f"network: {host!r} is a GitHub host")
        if bad := [target for target in redirect_targets if "*" in target]:
            raise ValueError(f"network.redirect_hosts: {bad} must be exact hosts, no '*'")

    @property
    def task_queue(self) -> str:
        return self.deployment

    @property
    def await_queue(self) -> str:
        """The task queue for the long-running job waits, kept apart from the short activities."""
        return self.deployment + "-await"

    def label(self, stage: str) -> str:
        return f"data:{stage}"

    def workflow_id(self, issue: int) -> str:
        return f"mia-data/{self.github.data_repo}/issue-{issue}"

    def marker(self, text: str) -> str:
        """The HTML comment hidden in our own GitHub comments so we can find them again."""
        return f"<!-- mia-data: {text} -->"


def load_config(
    path: str | os.PathLike | None = None,
    *,
    cls: type[Config] = Config,
    env_var: str = "MIA_DATA_CONFIG",
    default_name: str = "mia_data.yaml",
    prog: str = "mia-data",
    overrides: Mapping[str, tuple[str, ...]] = _ENV_OVERRIDES,
) -> Config:
    """Load and validate the config file, exiting with a readable message if it is missing or invalid.

    mia_buffers loads its `Config` subclass through the same function (`cls`, its own env var and file name)."""
    load_dotenv()
    path = Path(path or os.environ.get(env_var) or PROJECT_DIR / default_name)
    if not path.is_file():
        sys.exit(f"{prog}: config not found at {path} (copy {default_name}.example, or set ${env_var})")
    data = yaml.safe_load(path.read_text()) or {}
    for env_name, key_path in overrides.items():
        if os.environ.get(env_name):
            node = data
            for key in key_path[:-1]:
                if node.get(key) is None:  # a bare `temporal:` in YAML parses as None
                    node[key] = {}
                node = node[key]
            node[key_path[-1]] = os.environ[env_name]
    try:
        return cls(**data)
    except ValidationError as err:
        sys.exit(f"{prog}: invalid config {path}:\n{err}")
