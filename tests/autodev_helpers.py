"""Shared helpers for autodev tests."""

from __future__ import annotations

import subprocess
from pathlib import Path

from autodev.config import Config

API = "https://api.github.com"
R = "/repos/o/r"


def make_cfg(tmp_path: Path, github: dict | None = None, agent: dict | None = None, **top) -> Config:
    return Config(**{
        "deployment": "autodev",
        "github": {"repo": "o/r", **(github or {})},
        "paths": {"state_dir": str(tmp_path / "state"), "work_root": str(tmp_path / "work")},
        "agent": agent or {},
        **top,
    })

GIT_ID = ["-c", "user.name=t", "-c", "user.email=t@t"]


def run_git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def make_origin(tmp_path: Path) -> Path:
    """A bare 'GitHub' repo with main = README.md + .github/workflows/ci.yml."""
    src = tmp_path / "src"
    (src / ".github" / "workflows").mkdir(parents=True)
    (src / "README.md").write_text("hi\n")
    (src / ".github" / "workflows" / "ci.yml").write_text("on: push\n")
    run_git("init", "-q", "-b", "main", cwd=src)
    run_git("add", "-A", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "init", cwd=src)
    origin = tmp_path / "origin.git"
    run_git("clone", "-q", "--bare", str(src), str(origin), cwd=tmp_path)
    return origin


def agent_commit(repo: Path, path: str, text: str, base_sha: str) -> str:
    """What an agent job does: commit, then bundle base..HEAD next to the repo. Returns the new HEAD."""
    p = repo / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    run_git("add", "-A", cwd=repo)
    run_git("commit", "-qm", f"edit {path}", cwd=repo)
    run_git("bundle", "create", "-q", str(repo.parent / "result.bundle"), f"{base_sha}..HEAD", cwd=repo)
    return run_git("rev-parse", "HEAD", cwd=repo)
