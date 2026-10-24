"""mia_buffers.jobspec: the staged layout key, the catalog snapshot and the one job kind (stage)."""

import stat

import yaml
from autodev_helpers import GIT_ID, run_git
from test_buffers_config import make_cfg

from autodev.jobs import JOB_KEY_RE
from mia_buffers.jobspec import catalog_snapshot, levels_key, stage_job
from mia_buffers.models import BufferPins, StageJobArgs, job_key
from mia_data.jobspec import harness_pin


def _cfg(tmp_path, **overrides):
    cfg = make_cfg(tmp_path, limits={"job_bytes": 1e6}, **overrides)
    harness = cfg.harness.dir
    (harness / "harness").mkdir()
    (harness / "harness" / "pixi.lock").write_text("lock v1\n")
    (harness / "harness" / "src" / "mia_harness").mkdir(parents=True)
    (harness / "harness" / "src" / "mia_harness" / "cli.py").write_text("def main(): return 0\n")
    run_git("init", "-q", "-b", "main", cwd=harness)
    run_git("add", "-A", cwd=harness)
    run_git(*GIT_ID, "commit", "-qm", "harness", cwd=harness)
    return cfg


def _pins(cfg):
    return BufferPins("s" * 64, "c" * 40, *harness_pin(cfg))


def _args(cfg, workspace, hosts, out=""):
    workspace.mkdir(parents=True)
    (workspace / "binding.yaml").write_text(yaml.safe_dump({"record": "rec", "snippets_commit": "a" * 40}))
    (workspace / "record.yaml").write_text("id: rec\n")
    return StageJobArgs(str(workspace), "i1-e2", _pins(cfg), "rec", hosts, [1, 2, 4], 256, out, {"y": 45.0, "x": 45.0})


def test_levels_key():
    assert levels_key(256, [1, 2, 4]) == "p256-l1-2-4" and levels_key(64, [1]) == "p64-l1"


def test_stage_job_mounts_cache_and_out_rw_and_allows_hosts(tmp_path):
    cfg = _cfg(tmp_path)
    out = cfg.paths.staged_root / "p256-l1-2-4" / "rec"
    spec = stage_job(
        cfg,
        _args(cfg, cfg.paths.work_root / "issue-1" / "i1-e2" / "stage-rec", ["zenodo.org"], str(out)),
        tmp_path / "snap",
    )
    assert "harness stage binding.yaml" in spec.command and "--levels 1,2,4 --patch 256" in spec.command
    assert f"--out {out}" in spec.command and "--voxel-size-nm 45.0,45.0" in spec.command
    assert spec.allow_hosts == ["zenodo.org"] and spec.name == "mia-buffers-stage"
    assert str(cfg.paths.raw_root / "rec") in spec.rw_mounts and str(out) in spec.rw_mounts
    assert str(tmp_path / "snap") in spec.ro_mounts
    assert spec.env["MIA_HARNESS_CACHE_DIR"] == str(cfg.paths.raw_root / "rec")
    assert spec.env["MIA_HARNESS_CACHE_WRITABLE"] == "1" and spec.env["MIA_HARNESS_ALLOWED_HOSTS"] == "zenodo.org"


def test_stage_job_without_hosts_is_offline(tmp_path):
    cfg = _cfg(tmp_path)
    spec = stage_job(cfg, _args(cfg, tmp_path / "ws", [], str(tmp_path / "out")), tmp_path / "snap")
    assert spec.allow_hosts == [] and spec.env["MIA_HARNESS_ALLOWED_HOSTS"] == ""


def test_stage_job_needs_an_out_dir(tmp_path):
    cfg = _cfg(tmp_path)
    import pytest

    with pytest.raises(ValueError, match="out"):
        stage_job(cfg, _args(cfg, tmp_path / "ws", [], ""), tmp_path / "snap")


def test_catalog_snapshot_archives_datasets_at_ref(tmp_path):
    src = tmp_path / "catalog-src"
    (src / "datasets" / "em").mkdir(parents=True)
    (src / "datasets" / "em" / "a.yaml").write_text("id: a\n")
    (src / "README.md").write_text("not archived\n")
    run_git("init", "-q", "-b", "main", cwd=src)
    run_git("add", "-A", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "one", cwd=src)
    first = run_git("rev-parse", "HEAD", cwd=src)
    (src / "datasets" / "em" / "b.yaml").write_text("id: b\n")
    run_git("add", "-A", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "two", cwd=src)
    origin = tmp_path / "catalog.git"
    run_git("clone", "-q", "--bare", str(src), str(origin), cwd=tmp_path)
    cfg = make_cfg(tmp_path, github={"catalog_git_url": str(origin)})
    snap = catalog_snapshot(cfg, first)
    assert snap == cfg.paths.work_root / "_catalog" / first
    assert (
        sorted(p.name for p in (snap / "datasets" / "em").iterdir()) == ["a.yaml"] and not (snap / "README.md").exists()
    )
    assert not (snap / "datasets" / "em" / "a.yaml").stat().st_mode & stat.S_IWUSR
    assert catalog_snapshot(cfg, run_git("rev-parse", "HEAD", cwd=src)) / "datasets" / "em" / "b.yaml"


def test_job_key_is_valid_and_distinct():
    long_record = "a" * 121
    assert JOB_KEY_RE.fullmatch(job_key(7, "i7-e1", long_record))
    assert job_key(7, "i7-e1", "x") != job_key(7, "i7-e1", "y")
    assert job_key(7, "i7-e1", "a" * 121) != job_key(7, "i7-e1", "a" * 120 + "b")
