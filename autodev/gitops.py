"""Git operations on the trusted (worker) side.

The worker owns a bare mirror of the repository that no sandboxed job ever sees. Each agent round gets a
fresh clone of that mirror (with no hardlinks back into it) and hands its commits back as a git bundle.
The bundle is agent-controlled, so it is only ever read as data: validated, fetched into a temporary ref,
checked against the protected paths, and only then pushed.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path


class GitError(RuntimeError):
    pass


class RejectedBundleError(GitError):
    """The agent's result is invalid (permanent); other GitErrors may be transient (push, lock contention)."""


class ProtectedPathError(RejectedBundleError):
    """The agent changed a path the config lists as protected."""


def _git_env(token: str | None) -> dict[str, str]:
    """The environment for a git subprocess, with the GitHub token as an HTTP header if one is given."""
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    if token:
        credential = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        # Via env only: never in argv (visible in ps) or a config file (left on disk).
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                   GIT_CONFIG_VALUE_0=f"AUTHORIZATION: basic {credential}")
    return env


def git(*args: str, cwd: Path | None = None, token: str | None = None) -> str:
    """Run one git command (hooks disabled) and return its stdout, raising GitError on failure."""
    proc = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", *args], cwd=cwd, env=_git_env(token),
                          capture_output=True, text=True)
    if proc.returncode:
        raise GitError(f"git {' '.join(args)}: {proc.stderr.strip()[-2000:]}")
    return proc.stdout.strip()


class Mirror:
    """A bare clone of the remote at `path`, kept in sync, from which workspaces are cloned and to which
    agent bundles are imported before being pushed. `author` is the (name, email) agent commits are made as."""

    def __init__(self, path: Path, url: str, tokens: Callable[[], str | None] = lambda: None,
                 author: tuple[str, str] = ("autodev", "autodev@localhost")):
        self.path, self.url, self.tokens, self.author = Path(path), url, tokens, author

    def sync(self) -> None:
        """Clone the mirror if it does not exist yet, then fetch every branch from the remote."""
        if not (self.path / "HEAD").exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            git("clone", "-q", "--bare", self.url, str(self.path), token=self.tokens())
        git("fetch", "-q", "--prune", "origin", "+refs/heads/*:refs/heads/*", cwd=self.path, token=self.tokens())

    def workspace(self, dest: Path, start: str, branch: str, agent_url: str) -> str:
        """Fresh clone of branch `start` at dest/repo, checked out as `branch`. Returns the base sha.

        The clone's origin is pointed at `agent_url` so nothing in it refers to the mirror on disk."""
        repo = Path(dest) / "repo"
        if repo.exists():  # a retried prepare; nothing has run in it yet
            shutil.rmtree(repo)
        repo.parent.mkdir(parents=True, exist_ok=True)
        git("clone", "-q", "--no-hardlinks", "--branch", start, str(self.path), str(repo))
        git("checkout", "-q", "-B", branch, cwd=repo)
        git("remote", "set-url", "origin", agent_url, cwd=repo)
        git("config", "user.name", self.author[0], cwd=repo)
        git("config", "user.email", self.author[1], cwd=repo)
        return git("rev-parse", "HEAD", cwd=repo)

    def publish(self, bundle: Path, base_sha: str, branch: str, ref: str, protected: list[str],
                force: bool) -> str | None:
        """Import the agent's bundle and push it to `branch`. Returns the new head, or None if no commits.

        The commits must descend from `base_sha` and must not touch `protected` paths; `force` allows a
        non-fast-forward push (only for a round that owns the branch)."""
        bundle = Path(bundle)
        if not bundle.exists() and not bundle.is_symlink():
            return None
        # Only a real bundle file is fetched, and only from a worker-owned copy: a gitfile or symlink would
        # make git run upload-pack inside the agent's repo.
        if bundle.is_symlink() or not bundle.is_file():
            raise RejectedBundleError("agent result is not a regular file")
        with bundle.open("rb") as bundle_file:
            header = bundle_file.readline(64).rstrip(b"\n")  # bounded: the file may be one huge line
            if header not in (b"# v2 git bundle", b"# v3 git bundle"):
                raise RejectedBundleError("agent result is not a git bundle")
            bundle_file.seek(0)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=self.path.parent, suffix=".bundle") as copy:
                shutil.copyfileobj(bundle_file, copy)
                copy.flush()
                try:
                    git("-c", "transfer.fsckObjects=true", "fetch", "-q", copy.name, f"+HEAD:{ref}", cwd=self.path)
                except GitError as error:  # a local fetch from our own copy: the bundle's content is at fault
                    raise RejectedBundleError(f"agent bundle is unusable: {error}") from error
        try:
            new_sha = git("rev-parse", ref, cwd=self.path)
            if new_sha == base_sha:
                git("update-ref", "-d", ref, cwd=self.path)
                return None
            try:
                git("merge-base", "--is-ancestor", base_sha, new_sha, cwd=self.path)
            except GitError:
                raise RejectedBundleError(f"agent commits do not descend from {base_sha[:12]}") from None
            # Plumbing, NUL-separated, no rename detection: names are never quoted and both sides of a rename show.
            diff = git("diff-tree", "-r", "-z", "--no-renames", "--name-only", base_sha, new_sha, cwd=self.path)
            changed = [path for path in diff.split("\0") if path]
            if bad := [path for path in changed
                       if any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in protected)]:
                raise ProtectedPathError(f"agent changed protected paths: {', '.join(bad[:10])!r}")
        except GitError:
            git("update-ref", "-d", ref, cwd=self.path)
            raise
        git("push", "-q", "origin", f"{'+' if force else ''}{new_sha}:refs/heads/{branch}", cwd=self.path,
            token=self.tokens())
        git("update-ref", f"refs/heads/{branch}", new_sha, cwd=self.path)
        return new_sha
