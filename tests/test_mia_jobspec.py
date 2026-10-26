import stat
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import yaml
from autodev_helpers import GIT_ID, make_origin, run_git
from mia_helpers import make_cfg

from autodev.gitops import GitError
from mia_data import jobspec
from mia_data.jobspec import (
    HostsNotAllowed,
    PinMoved,
    assert_pin,
    check_hosts_allowed,
    crop_job,
    harness_pin,
    harness_snapshot,
    match_job,
    match_prompt,
    smoke_job,
    snapshot,
    verify_job,
)
from mia_data.models import JobArgs, Pins

RECORD = {"data": {"download_url": "https://data.example.org/a.zip"}}
BINDING = {
    "record": "rec-1",
    "record_commit": "c" * 40,
    "raw": [{"snippet": "tiff-zip-member", "urls": ["https://data.example.org/a.zip::m.tif"], "params": {}}],
}


def _cfg(tmp_path):
    cfg = make_cfg(
        tmp_path, limits={"job_bytes": 1e6}, network={"redirect_hosts": {"data.example.org": ["cdn.example.net"]}}
    )
    h = cfg.harness.dir
    (h / "harness").mkdir()
    (h / "harness" / "pixi.lock").write_text("lock v1\n")
    (h / "harness" / "src" / "mia_harness").mkdir(parents=True)
    (h / "harness" / "src" / "mia_harness" / "cli.py").write_text("def main(): return 0\n")
    run_git("init", "-q", "-b", "main", cwd=h)
    run_git("add", "-A", cwd=h)
    run_git(*GIT_ID, "commit", "-qm", "harness", cwd=h)
    return cfg


def _pins(cfg, record="rec-1"):
    return Pins(record, "c" * 40, "a" * 40, *harness_pin(cfg))


def _args(ws, pins, **job_args):
    """A submit activity's JobArgs for workspace `ws` in round i1-e2."""
    return JobArgs(str(ws), "i1-e2", pins, **job_args)


def _ws(tmp_path):
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "record.yaml").write_text(yaml.safe_dump(RECORD))
    (ws / "binding.yaml").write_text(yaml.safe_dump(BINDING))
    return ws


def _origin(tmp_path):
    origin, src = make_origin(tmp_path), tmp_path / "src"
    (src / "snippets" / "npy").mkdir(parents=True)
    (src / "snippets" / "npy" / "snippet.yaml").write_text("name: npy\n")
    (src / "WITHDRAWN.yaml").write_text("[]\n")
    (src / "SPEC.md").write_text("spec\n")
    run_git("add", "-A", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "snippets", cwd=src)
    run_git("push", "-q", str(origin), "main", cwd=src)
    return origin, src


def _readonly(p: Path) -> bool:
    return not stat.S_IMODE(p.stat().st_mode) & 0o222


def test_snapshot_extracts_once_read_only(tmp_path, monkeypatch):
    origin, src = _origin(tmp_path)
    cfg = make_cfg(tmp_path, github={"snippets_git_url": str(origin)})
    sha = run_git("rev-parse", "HEAD", cwd=src)
    archives = []
    real = subprocess.run
    monkeypatch.setattr(
        jobspec.subprocess, "run", lambda a, **k: (archives.append(a) if "archive" in a else None, real(a, **k))[1]
    )
    d = snapshot(cfg, sha)
    assert snapshot(cfg, sha) == d == cfg.paths.work_root / "_snapshots" / sha and len(archives) == 1
    assert sorted(p.name for p in d.iterdir()) == ["SPEC.md", "WITHDRAWN.yaml", "snippets"]
    assert all(
        _readonly(p) for p in [d, d / "SPEC.md", d / "snippets" / "npy", d / "snippets" / "npy" / "snippet.yaml"]
    )
    assert (cfg.paths.state_dir / "mirror" / "mia-snippets.git" / "HEAD").exists()
    # a commit pushed after the mirror was made is fetched on demand
    (src / "snippets" / "npy" / "README.md").write_text("new\n")
    run_git("add", "-A", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "more", cwd=src)
    run_git("push", "-q", str(origin), "main", cwd=src)
    assert (snapshot(cfg, run_git("rev-parse", "HEAD", cwd=src)) / "snippets" / "npy" / "README.md").exists()


def test_snapshot_concurrent_calls_extract_once(tmp_path, monkeypatch):
    origin, src = _origin(tmp_path)
    cfg = make_cfg(tmp_path, github={"snippets_git_url": str(origin)})
    sha = run_git("rev-parse", "HEAD", cwd=src)
    archives, real = [], subprocess.run

    def run(a, **k):
        if "archive" in a:
            archives.append(a)
            time.sleep(0.2)  # widen the race
        return real(a, **k)

    monkeypatch.setattr(jobspec.subprocess, "run", run)
    with ThreadPoolExecutor(2) as ex:
        a, b = ex.map(lambda _: snapshot(cfg, sha), range(2))
    assert a == b and len(archives) == 1 and (a / "SPEC.md").read_text() == "spec\n" and _readonly(a)


def test_snapshot_archive_error_names_stderr(tmp_path):
    origin, src = _origin(tmp_path)
    run_git("rm", "-q", "SPEC.md", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "no spec", cwd=src)
    run_git("push", "-q", str(origin), "main", cwd=src)
    cfg = make_cfg(tmp_path, github={"snippets_git_url": str(origin)})
    with pytest.raises(GitError, match="SPEC.md"):
        snapshot(cfg, run_git("rev-parse", "HEAD", cwd=src))


@pytest.mark.parametrize("sha", ["main", "A" * 40, "a" * 39, "../" + "a" * 37])
def test_snapshot_rejects_non_sha(tmp_path, sha):
    with pytest.raises(ValueError):
        snapshot(make_cfg(tmp_path), sha)


def test_a_new_harness_commit_does_not_touch_an_admitted_round(tmp_path):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    pins = _pins(cfg)
    snap = harness_snapshot(cfg, pins.harness_commit)
    assert snap == cfg.paths.work_root / "_harness" / pins.harness_commit
    assert (snap / "src" / "mia_harness" / "cli.py").read_text() == "def main(): return 0\n" and _readonly(snap)
    (cfg.harness.dir / "harness" / "src" / "mia_harness" / "cli.py").write_text("def main(): return 1\n")
    run_git("add", "-A", cwd=cfg.harness.dir)
    run_git(*GIT_ID, "commit", "-qm", "moved", cwd=cfg.harness.dir)
    assert_pin(cfg, pins)  # HEAD moved, env didn't
    s = smoke_job(cfg, _args(ws, pins), tmp_path / "snap")
    assert f"PYTHONPATH={snap}/src" in s.command and str(snap) in s.ro_mounts
    assert (snap / "src" / "mia_harness" / "cli.py").read_text() == "def main(): return 0\n"  # still the admitted one
    assert harness_snapshot(cfg, harness_pin(cfg)[0]) != snap  # the new commit gets its own


def test_harness_snapshot_rejects_unknown_commit_and_non_sha(tmp_path):
    cfg = _cfg(tmp_path)
    with pytest.raises(GitError, match="archive"):
        harness_snapshot(cfg, "e" * 40)
    with pytest.raises(ValueError):
        harness_snapshot(cfg, "main")


def test_two_threads_share_one_harness_extraction_under_a_local_lock_file(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    commit = harness_pin(cfg)[0]
    lock_file = cfg.paths.local_root / "locks" / "snapshot" / commit
    real_run, extractions, lock_seen = subprocess.run, [], []
    first_extracting, second_started = threading.Event(), threading.Event()

    def spying_run(cmd, *args, **kwargs):
        if cmd[0] == "tar":
            extractions.append(cmd)
            lock_seen.append(lock_file.exists())
            first_extracting.set()
            second_started.wait(5)  # the other thread is by now waiting on the lock
            time.sleep(0.3)  # it polls every 2 s; it cannot extract while we hold the lock
        return real_run(cmd, *args, **kwargs)

    def call(index):
        if index:
            first_extracting.wait(5)
            second_started.set()
        return harness_snapshot(cfg, commit)

    monkeypatch.setattr(jobspec.subprocess, "run", spying_run)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(call, range(2)))
    assert results[0] == results[1] and len(extractions) == 1 and lock_seen == [True] and not lock_file.exists()
    assert not list(cfg.paths.work_root.glob("_harness/.*.lock"))  # no lock file beside the destination


def test_snapshots_need_a_local_root(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.paths.local_root = None
    with pytest.raises(RuntimeError, match="paths.local_root"):
        harness_snapshot(cfg, harness_pin(cfg)[0])


def test_assert_pin_raises_when_lock_changes(tmp_path):
    cfg = _cfg(tmp_path)
    pins = _pins(cfg)
    (cfg.harness.dir / "harness" / "pixi.lock").write_text("lock v2\n")
    with pytest.raises(PinMoved):
        assert_pin(cfg, pins)


def test_match_job(tmp_path):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    pins = _pins(cfg)
    s = match_job(cfg, _args(ws, pins))
    assert s.allow_hosts == sorted(["data.example.org", "cdn.example.net", *cfg.network.package_hosts])
    assert not any("github" in h or "anthropic" in h or "claude" in h for h in s.allow_hosts)
    assert s.agent == "claude" and f"--max-budget-usd {cfg.limits.agent_max_budget_usd}" in s.command
    assert '--allowedTools "Bash(harness smoke:*),Read,Write,Glob,Grep"' in s.command and "> claude.json" in s.command
    cache = str(cfg.paths.work_root / "_cache" / "rec-1")
    assert cache in s.ro_mounts and s.rw_mounts == [] and s.env["MIA_HARNESS_CACHE_WRITABLE"] == "0"
    snap = str(cfg.paths.work_root / "_snapshots" / pins.snippets_commit)
    hsnap = str(cfg.paths.work_root / "_harness" / pins.harness_commit)
    assert sorted(s.ro_mounts) == sorted([str(cfg.harness.dir), hsnap, snap, cache]) and snap in s.command
    assert (s.env["MIA_HARNESS_COMMIT"], s.env["MIA_HARNESS_ENV_DIGEST"]) == (pins.harness_commit, pins.env_digest)


@pytest.mark.parametrize("trusted", [False, True])
def test_crop_job_cache_mode(tmp_path, trusted):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    s = crop_job(cfg, _args(ws, _pins(cfg), n_crops=4, seed="s-t", batch=0, trusted=trusted), tmp_path / "snap", None)
    cache = str(cfg.paths.work_root / "_cache" / "rec-1")
    assert (cache in s.rw_mounts, cache in s.ro_mounts) == (trusted, not trusted)
    assert s.env["MIA_HARNESS_CACHE_WRITABLE"] == ("1" if trusted else "0") and Path(cache).is_dir()
    assert s.allow_hosts == sorted(["data.example.org", "cdn.example.net", *cfg.network.package_hosts])
    assert "--n 4 --seed s-t --round i1-e2 --batch 0 --out crops" in s.command and s.agent is None


def test_crop_job_mounts_the_existing_manifests_dir(tmp_path):
    cfg, ws, m = _cfg(tmp_path), _ws(tmp_path), tmp_path / "ds" / "manifest.json"
    s = crop_job(cfg, _args(ws, _pins(cfg), n_crops=4, seed="s", batch=1, trusted=True), tmp_path / "snap", m)
    body = next(line for line in s.command.split("\n") if line.startswith("harness crop"))
    assert body.endswith(f"--existing {m}")
    assert str(m.parent) in s.ro_mounts and str(m) not in s.ro_mounts  # csub mounts directories only


def test_verify_job(tmp_path):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    crops = tmp_path / "crop-ws" / "crops"
    m = tmp_path / "ds" / "manifest.json"
    s = verify_job(cfg, _args(ws, _pins(cfg)), crops, m)
    assert s.allow_hosts == [] and str(crops) in s.ro_mounts and f"--existing {m} " in s.command
    assert str(m.parent) in s.ro_mounts and str(m) not in s.ro_mounts  # csub mounts directories only
    assert s.rw_mounts == [] and s.env["MIA_HARNESS_CACHE_DIR"] == "" and "harness preview" in s.command
    assert "verify.json; v=$?" in s.command  # preview runs even when a crop fails verification


def test_check_hosts_allowed(tmp_path):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    s = smoke_job(cfg, _args(ws, _pins(cfg)), tmp_path / "snap")
    check_hosts_allowed(s, ["*.example.org", "cdn.example.net", *cfg.network.package_hosts])
    with pytest.raises(HostsNotAllowed, match="data.example.org") as e:
        check_hosts_allowed(s, ["cdn.example.net", "*.pypi.org", *cfg.network.package_hosts])
    assert e.value.missing == ["data.example.org"]


def _builders(cfg, ws, pins, tmp_path):
    snap = tmp_path / "snap"
    return [
        lambda: match_job(cfg, _args(ws, pins)),
        lambda: smoke_job(cfg, _args(ws, pins), snap),
        lambda: crop_job(cfg, _args(ws, pins, n_crops=4, seed="s", batch=1, trusted=True), snap, tmp_path / "m.json"),
        lambda: verify_job(cfg, _args(ws, pins), tmp_path / "crops", None),
    ]


def test_every_builder_sets_harness_env(tmp_path):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    pins = _pins(cfg)
    for s in (b() for b in _builders(cfg, ws, pins, tmp_path)):
        assert s.env["MIA_HARNESS_JOB_BYTES"] == "1000000" and sorted(s.env) == sorted(jobspec.HARNESS_ENV)
        assert s.env["MIA_HARNESS_ALLOWED_HOSTS"] == ",".join(s.allow_hosts)
        assert (s.env["MIA_HARNESS_COMMIT"], s.env["MIA_HARNESS_ENV_DIGEST"]) == (pins.harness_commit, pins.env_digest)
        assert str(cfg.harness.dir) in s.ro_mounts and s.workspace == str(ws)
        assert s.command.rstrip().endswith('echo "$rc" > exit_code\nexit "$rc"')
        assert s.walltime_min == cfg.limits.job_walltime_min and not set(s.ro_mounts) & set(s.rw_mounts)
        snap = cfg.paths.work_root / "_harness" / pins.harness_commit
        assert f"{cfg.harness.dir}/harness/.pixi/envs/default/bin/python -P -c" in s.command
        assert "pixi run" not in s.command
        assert f"PYTHONPATH={snap}/src" in s.command and str(snap) in s.ro_mounts
        assert '> bin/harness && chmod +x bin/harness\nexport PATH="$PWD/bin":"$PATH"\n' in s.command


def test_every_builder_refuses_a_moved_harness(tmp_path):
    cfg, ws = _cfg(tmp_path), _ws(tmp_path)
    pins = _pins(cfg)
    (cfg.harness.dir / "harness" / "pixi.lock").write_text("lock v2\n")
    for b in _builders(cfg, ws, pins, tmp_path):
        with pytest.raises(PinMoved):
            b()


def test_match_prompt_names_pins_and_outcomes(tmp_path):
    p = match_prompt(_pins(_cfg(tmp_path)))
    assert "rec-1" in p and "c" * 40 in p and "$" not in p
    for o in ("matched", "needs_snippet", "ineligible", "blocked", "host_not_allowed", "outcome.json"):
        assert o in p
