import subprocess
from types import SimpleNamespace

import pytest

from autodev import gitops
from autodev.gitops import GitError, Mirror, ProtectedPathError, RejectedBundleError

from autodev_helpers import agent_commit, make_origin, run_git

URL = "https://github.com/o/r.git"


@pytest.fixture(autouse=True)
def hermetic_git(monkeypatch):
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/dev/null")
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


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


PUB = ("autodev/issue-7", "refs/autodev/7/k-r1")


def _publish(m, ws, base, protected=(".github/",)):
    return m.publish(ws / "result.bundle", base, PUB[0], PUB[1], list(protected), force=True)


def _assert_refused(origin, m, ws, base, exc=ProtectedPathError):
    with pytest.raises(exc):
        _publish(m, ws, base)
    with pytest.raises(Exception):
        run_git("rev-parse", "--verify", "refs/heads/autodev/issue-7", cwd=origin)
    with pytest.raises(Exception):
        run_git("rev-parse", "--verify", PUB[1], cwd=m.path)


def test_rename_out_of_protected_dir_refused(setup):
    origin, m, ws, base = setup
    repo = ws / "repo"
    run_git("mv", ".github/workflows/ci.yml", "ci.yml", cwd=repo)
    run_git("commit", "-qm", "mv", cwd=repo)
    run_git("bundle", "create", "-q", str(ws / "result.bundle"), f"{base}..HEAD", cwd=repo)
    _assert_refused(origin, m, ws, base)


@pytest.mark.parametrize("name", ['a"b.yml', "a\tb.yml", "caf\u00e9.yml"])
def test_odd_protected_names_refused(setup, name):
    origin, m, ws, base = setup
    agent_commit(ws / "repo", f".github/workflows/{name}", "x\n", base)
    _assert_refused(origin, m, ws, base)


def test_symlink_bundle_refused(setup, tmp_path):
    origin, m, ws, base = setup
    agent_commit(ws / "repo", "f.txt", "y", base)
    real = tmp_path / "real.bundle"
    (ws / "result.bundle").rename(real)
    (ws / "result.bundle").symlink_to(real)
    _assert_refused(origin, m, ws, base, GitError)


def test_gitfile_bundle_refused(setup):
    origin, m, ws, base = setup
    (ws / "result.bundle").write_text(f"gitdir: {ws / 'repo' / '.git'}\n")
    _assert_refused(origin, m, ws, base, GitError)


def test_corrupt_bundle_with_valid_header_is_rejected(setup):
    """Not a transient git failure: retrying cannot fix the agent's bundle."""
    origin, m, ws, base = setup
    (ws / "result.bundle").write_bytes(b"# v2 git bundle\n" + b"\x00garbage" * 100)
    _assert_refused(origin, m, ws, base, RejectedBundleError)


def test_every_worker_git_call_disables_hooks(setup, monkeypatch):
    """The workspace's .git/hooks belong to the agent: no git command of ours may run them."""
    origin, m, ws, base = setup
    calls = []

    def spy(argv, *a, **kw):
        calls.append(argv)
        return subprocess.run(argv, *a, **kw)

    monkeypatch.setattr(gitops, "subprocess", SimpleNamespace(run=spy))  # gitops' calls only, not the test's
    m.sync()
    base = m.workspace(ws, "main", "autodev/issue-7", URL)
    agent_commit(ws / "repo", "f.txt", "x", base)
    _publish(m, ws, base)
    assert len(calls) > 10 and all(c[:3] == ["git", "-c", "core.hooksPath=/dev/null"] for c in calls)


def test_token_never_on_disk_or_in_errors(tmp_path):
    origin = make_origin(tmp_path)
    m = Mirror(tmp_path / "state" / "mirror.git", str(origin), tokens=lambda: "sekrit-token")
    m.sync()
    assert "sekrit-token" not in (m.path / "config").read_text()
    bad = Mirror(tmp_path / "state2" / "mirror.git", str(tmp_path / "nope.git"), tokens=lambda: "sekrit-token")
    with pytest.raises(GitError) as e:
        bad.sync()
    assert "sekrit-token" not in str(e.value)
