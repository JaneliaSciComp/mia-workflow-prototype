import json
import subprocess
import sys

import numpy as np
import pytest

from zarr_doctor.jobs.histogram import _percentile_from_hist, sample_chunks


@pytest.mark.parametrize("crop,dtype", [
    ("em-mouse-foo/crop-001_cortex.zarr", "uint8"),
    ("xray-fly-bar/crop-002_vnc.zarr", "uint16"),
    ("em-mouse-foo/crop-003_float.zarr", "float32"),
])
def test_job_script(lmd_root, tmp_path, crop, dtype):
    out = tmp_path / "out"
    r = subprocess.run([sys.executable, "-m", "zarr_doctor.jobs.histogram", "--zarr", str(lmd_root / "data" / crop),
                        "--out", str(out), "--max-chunks", "10"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    stats = json.loads((out / "stats.json").read_text())
    hist = json.loads((out / "histogram.json").read_text())
    assert stats["dtype"] == dtype and stats["chunks_read"] == min(10, stats["total_chunks"])
    assert len(hist["edges"]) == len(hist["counts"]) + 1
    assert sum(hist["counts"]) + stats["outliers_low"] + stats["outliers_high"] == stats["voxels_counted"]
    assert stats["min"] <= stats["p01"] <= stats["p50"] <= stats["p99"] <= stats["max"]
    assert 0 < stats["zero_fraction"] < 1
    assert (out / "histogram.png").stat().st_size > 10_000
    assert sorted(p.name for p in out.iterdir()) == ["histogram.json", "histogram.png", "stats.json"]


def test_uint8_exact(lmd_root, tmp_path):
    from zarr_doctor.jobs.histogram import run
    import zarr

    path = lmd_root / "data/em-mouse-foo/crop-001_cortex.zarr"
    stats = run(path, tmp_path, max_chunks=10_000)  # all chunks
    data = zarr.open_array(str(path / "raw/s0"))[:]
    assert stats["sampled_fraction"] == 1
    assert stats["voxels_counted"] == data.size
    assert stats["mean"] == pytest.approx(data.mean())
    assert stats["zero_fraction"] == pytest.approx((data == 0).mean())
    assert json.loads((tmp_path / "histogram.json").read_text())["counts"] == np.bincount(data.ravel(), minlength=256).tolist()


def test_broken_path_fails_cleanly(tmp_path):
    r = subprocess.run([sys.executable, "-m", "zarr_doctor.jobs.histogram", "--zarr", str(tmp_path / "nope.zarr"),
                        "--out", str(tmp_path / "out")], capture_output=True, text=True)
    assert r.returncode == 1 and r.stderr.startswith("histogram failed:")
    assert not (tmp_path / "out").exists()


def test_sampling_deterministic():
    a = sample_chunks((10, 10, 10), 50, seed=0)
    assert a == sample_chunks((10, 10, 10), 50, seed=0) and len(set(a)) == 50 and max(a) < 1000
    assert sample_chunks((2, 2), 50, 0) == [0, 1, 2, 3]


def test_percentile_from_hist():
    edges, counts = np.arange(11.0), np.ones(10)
    assert _percentile_from_hist(edges, counts, [50])[0] == pytest.approx(5.0)
