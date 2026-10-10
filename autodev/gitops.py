"""Git on the trusted side. The worker owns a bare mirror that no job ever sees. Each agent round gets a fresh
clone of it (no hardlinks back into the mirror) and hands back a bundle, which is only ever read as data."""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
from pathlib import Path
from typing import Callable


class GitError(RuntimeError):
    pass


class ProtectedPathError(GitError):
    pass


def _env(token: str | None) -> dict[str, str]:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    if token:
        cred = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        # Via env only: never in argv (visible in ps) or a config file (left on disk).
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                   GIT_CONFIG_VALUE_0=f"AUTHORIZATION: basic {cred}")
    return env


def git(*args: str, cwd: Path | None = None, token: str | None = None) -> str:
    r = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", *args], cwd=cwd, env=_env(token),
                       capture_output=True, text=True)
    if r.returncode:
        raise GitError(f"git {' '.join(args)}: {r.stderr.strip()[-2000:]}")
    return r.stdout.strip()


class Mirror:
    def __init__(self, path: Path, url: str, tokens: Callable[[], str | None] = lambda: None):
        self.path, self.url, self.tokens = Path(path), url, tokens

    def sync(self) -> None:
        if not (self.path / "HEAD").exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            git("clone", "-q", "--bare", self.url, str(self.path), token=self.tokens())
        git("fetch", "-q", "--prune", "origin", "+refs/heads/*:refs/heads/*", cwd=self.path, token=self.tokens())

    def workspace(self, dest: Path, start: str, branch: str, agent_url: str) -> str:
        """Fresh clone of branch `start` at dest/repo, checked out as `branch`. Returns the base sha."""
        repo = Path(dest) / "repo"
        if repo.exists():  # a retried prepare; nothing has run in it yet
            shutil.rmtree(repo)
        repo.parent.mkdir(parents=True, exist_ok=True)
        git("clone", "-q", "--no-hardlinks", "--branch", start, str(self.path), str(repo))
        git("checkout", "-q", "-B", branch, cwd=repo)
        git("remote", "set-url", "origin", agent_url, cwd=repo)
        git("config", "user.name", "autodev", cwd=repo)
        git("config", "user.email", "autodev@localhost", cwd=repo)
        return git("rev-parse", "HEAD", cwd=repo)

    def publish(self, bundle: Path, base_sha: str, branch: str, ref: str, protected: list[str],
                force: bool) -> str | None:
        """Import the agent's bundle and push it to `branch`. Returns the new head, or None if no commits."""
        if not Path(bundle).is_file():
            return None
        git("fetch", "-q", str(bundle), f"+HEAD:{ref}", cwd=self.path)
        new = git("rev-parse", ref, cwd=self.path)
        if new == base_sha:
            return None
        try:
            git("merge-base", "--is-ancestor", base_sha, new, cwd=self.path)
        except GitError:
            raise GitError(f"agent commits do not descend from {base_sha[:12]}") from None
        changed = git("diff", "--name-only", base_sha, new, cwd=self.path).splitlines()
        if bad := [p for p in changed if any(p.startswith(pre) for pre in protected)]:
            raise ProtectedPathError(f"agent changed protected paths: {', '.join(bad[:10])}")
        git("push", "-q", "origin", f"{'+' if force else ''}{new}:refs/heads/{branch}", cwd=self.path,
            token=self.tokens())
        git("update-ref", f"refs/heads/{branch}", new, cwd=self.path)
        return new
