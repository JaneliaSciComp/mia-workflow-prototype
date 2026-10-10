import pytest

from autodev.gitops import GitError, Mirror, ProtectedPathError

from autodev_helpers import agent_commit, make_origin, run_git

URL = "https://github.com/o/r.git"


@pytest.fixture
def setup(tmp_path):
    origin = make_origin(tmp_path)
    m = Mirror(tmp_path / "state" / "mirror.git", str(origin))
    m.sync()
    ws = tmp_path / "work" / "round-1"
    base = m.workspace(ws, "main", "autodev/issue-7", URL)
    return origin, m, ws, base


def test_round_trip_pushes_agent_commits(setup):
    origin, m, ws, base = setup
    assert run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=ws / "repo") == "autodev/issue-7"
    assert run_git("remote", "get-url", "origin", cwd=ws / "repo") == URL
    head = agent_commit(ws / "repo", "feature.txt", "x\n", base)
    got = m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [".github/"], force=True)
    assert got == head
    assert run_git("rev-parse", "refs/heads/autodev/issue-7", cwd=origin) == head


def test_no_bundle_means_no_commits(setup):
    origin, m, ws, base = setup
    assert m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [], force=True) is None


def test_protected_path_refused_and_nothing_pushed(setup):
    origin, m, ws, base = setup
    agent_commit(ws / "repo", ".github/workflows/ci.yml", "on: [push, pull_request]\n", base)
    with pytest.raises(ProtectedPathError, match="ci.yml"):
        m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [".github/"], force=True)
    with pytest.raises(Exception):
        run_git("rev-parse", "--verify", "refs/heads/autodev/issue-7", cwd=origin)


def test_commits_must_descend_from_base(setup):
    origin, m, ws, base = setup
    repo = ws / "repo"
    run_git("checkout", "-q", "--orphan", "evil", cwd=repo)
    (repo / "x.txt").write_text("x")
    run_git("add", "-A", cwd=repo)
    run_git("commit", "-qm", "orphan", cwd=repo)
    run_git("bundle", "create", "-q", str(ws / "result.bundle"), "HEAD", cwd=repo)
    with pytest.raises(GitError, match="descend"):
        m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [], force=True)


def test_workspace_is_independent_of_mirror(setup):
    _, m, ws, _ = setup
    objects = [p for p in (ws / "repo" / ".git" / "objects").rglob("*") if p.is_file()]
    assert objects and all(p.stat().st_nlink == 1 for p in objects)
    assert not (ws / "repo" / ".git" / "objects" / "info" / "alternates").exists()


def test_workspace_hooks_never_run(setup, tmp_path):
    origin, m, ws, base = setup
    sentinel = tmp_path / "pwned"
    hook = f"#!/bin/sh\ntouch {sentinel}\n"
    for name in ("pre-push", "post-checkout", "reference-transaction", "post-merge"):
        h = ws / "repo" / ".git" / "hooks" / name
        h.write_text(hook)
        h.chmod(0o755)
    run_git("config", "core.hooksPath", str(ws / "repo" / ".git" / "hooks"), cwd=ws / "repo")
    agent_commit(ws / "repo", "f.txt", "y", base)
    sentinel.unlink(missing_ok=True)  # agent_commit itself ran the agent's own hooks inside the "job"
    m.sync()
    m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [], force=True)
    assert not sentinel.exists()
