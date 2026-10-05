"""py-cluster-api local executor end to end: submit -> await, plus idempotent resubmit."""

from types import SimpleNamespace

from zarr_doctor.cluster import Cluster
from zarr_doctor.config import ClusterCfg, HistogramCfg


def _cfg(lmd_root, tmp_path):
    return SimpleNamespace(lmd_root=lmd_root, results_dir=tmp_path / "results",
                           cluster=ClusterCfg(profile="local", resources={"cpus": 1}),
                           histogram=HistogramCfg(max_chunks=8))


async def test_submit_and_await(lmd_root, tmp_path):
    c = Cluster(_cfg(lmd_root, tmp_path))
    try:
        job = await c.submit(7, "data/em-mouse-foo/crop-001_cortex.zarr", "r1")
        assert await c.submit(7, "data/em-mouse-foo/crop-001_cortex.zarr", "r1") == job  # no second submission
        polls = []
        out = await c.wait(7, "r1", job, polls.append, interval=1)
        assert out.status == "done", c.log_tail(7, "r1")
        assert out.exit_code == 0 and polls
        rd = c.run_dir(7, "r1")
        assert {"stats.json", "histogram.png", "histogram.json", "lsf.out", "lsf.err", "job_id"} <= {p.name for p in rd.iterdir()}
    finally:
        await c.close()


async def test_failed_job_reports_log_tail(lmd_root, tmp_path):
    c = Cluster(_cfg(lmd_root, tmp_path))
    try:
        job = await c.submit(8, "data/does-not/exist.zarr", "r1")
        out = await c.wait(8, "r1", job, lambda s: None, interval=1)
        assert out.status == "failed" and out.exit_code == 1
        assert "histogram failed" in c.log_tail(8, "r1")
    finally:
        await c.close()
