"""mia-buffers configuration: mia_buffers.yaml, mia-data's sections plus the buffer roots and the service.

The buffer worker reuses mia-data's job machinery, so its config is mia-data's `Config` with a different issue
repo (`github.buffers_repo`), two more roots (`raw_root` for the shared raw cache, `staged_root` for the pyramids),
the buffer-api address and the sourcing knobs. `storage_root` is mia-data's: the approved bindings are read from
`<storage_root>/<dataset folder>/binding.yaml` (spec §3, §6.1).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, Field, model_validator

from autodev.config import TemporalCfg
from mia_core.config import KeeperCfg, QuotaCfg, ServiceCfg
from mia_data import config as mia_data_config

# stage -> (label color, description); labels are `buffer:<stage>` (spec §1)
STAGES = {
    "new": ("ededed", "mia-buffers: buffer issue seen"),
    "planning": ("fbca04", "mia-buffers: resolving the spec against the catalog"),
    "plan-review": ("1d76db", "mia-buffers: plan posted, waiting for buffer:approve"),
    "sourcing": ("fbca04", "mia-buffers: staging datasets"),
    "ready": ("5319e7", "mia-buffers: the service is serving this buffer"),
    "idle": ("cccccc", "mia-buffers: no consumer for a while"),
    "retired": ("cccccc", "mia-buffers: staged data released"),
    "needs-human": ("d93f0b", "mia-buffers: stopped, needs a human"),
}
HUMAN_LABELS = {  # set by humans; `keep` and `retire` arrive with milestone 5
    "go": ("0e8a16", "Plan this buffer (or plan it again after editing the spec)"),
    "approve": ("0e8a16", "Approve the plan; start sourcing"),
}
_ENV_OVERRIDES = {
    "TEMPORAL_ADDRESS": ("temporal", "address"),
    "MIA_BUFFERS_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy"),
}


class GitHubCfg(mia_data_config.GitHubCfg):
    """mia-data's GitHub section with the issue repo named `buffers_repo` in the YAML (it stays `data_repo` on
    the object so mia-data's helpers, which post to `data_repo`, work unchanged) and mia-data's own issue repo as
    `datasets_repo`, where a dataset without a binding gets its `data:go` (spec §3 step 1)."""

    data_repo: str = Field(alias="buffers_repo")
    datasets_repo: str  # mia-data's issue repo (mia_data.yaml `data_repo`)
    catalog_git_url: str | None = None  # clone URL override for the catalog mirror (default: GitHub https)

    @property
    def buffers_repo(self) -> str:
        return self.data_repo


class PathsCfg(mia_data_config.PathsCfg):
    """mia-data's roots plus the raw cache and the staged pyramids; each takes a path or `{path, url}`."""

    ROOTS: ClassVar[tuple[str, ...]] = ("work_root", "storage_root", "raw_root", "staged_root")

    raw_root: Path  # unset: mia-data's download cache, <work_root>/_cache, so a dataset is fetched once for both
    raw_root_url: str | None = None
    staged_root: Path
    staged_root_url: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _default_raw_root(cls, data):
        if isinstance(data, dict) and not data.get("raw_root") and data.get("work_root"):
            work = data["work_root"]
            work = work.get("path") if isinstance(work, dict) else work
            if isinstance(work, str):
                data = {**data, "raw_root": str(Path(work) / "_cache")}
        return data


class SourcingCfg(BaseModel):
    parallel: int = Field(default=4, ge=1)  # stage jobs in flight per buffer
    tensorizer: str = "muvit-2d"  # must exist under <harness.dir>/tensorizers/
    val_size: int = Field(default=256, ge=1)  # frozen list lengths
    test_size: int = Field(default=256, ge=1)


class Config(mia_data_config.Config):
    """The whole mia_buffers.yaml, validated."""

    deployment: str = "mia-buffers"
    github: GitHubCfg
    temporal: TemporalCfg = TemporalCfg(namespace="mia-buffers")
    paths: PathsCfg
    service: ServiceCfg
    quota: QuotaCfg = QuotaCfg()
    keeper: KeeperCfg = KeeperCfg()
    sourcing: SourcingCfg = SourcingCfg()

    def label(self, stage: str) -> str:
        return f"buffer:{stage}"

    def workflow_id(self, issue: int) -> str:
        return f"mia-buffers/{self.github.buffers_repo}/issue-{issue}"

    def marker(self, text: str) -> str:
        return f"<!-- mia-buffers: {text} -->"


def load_config(path: str | os.PathLike | None = None, *, service: str | None = None) -> Config:
    """Load and validate mia_buffers.yaml ($MIA_BUFFERS_CONFIG), exiting with a readable message on failure."""
    cfg = mia_data_config.load_config(
        path,
        cls=Config,
        env_var="MIA_BUFFERS_CONFIG",
        default_name="mia_buffers.yaml",
        prog="mia-buffers",
        overrides=_ENV_OVERRIDES,
        service=service,
    )
    assert isinstance(cfg, Config)  # `cls` built it
    return cfg
