"""Real-cluster checks (pytest -m integration). Need bsub and read access to the LMD store."""

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from zarr_doctor.cluster import Cluster
from zarr_doctor.config import PROJECT_DIR, ClusterCfg, HistogramCfg, load_dotenv

pytestmark = pytest.mark.integration
load_dotenv()  # LSF_BILLING_GROUP, RESULTS_DIR

LMD = Path(os.environ.get("LMD_ROOT", "/groups/miaai/miaai/lmd-v0.0.1"))
VOLUMES = [
    "data/em-celegans-MitoVerse-conrad23-whole-organism/crop-001_c_elegans.zarr",  # v3 uint8, sharded
    "data/exm-drosophila-flyliconn-FlyID10-20250923_2ndGel_PBS_MES_B1_Atto488_40XW_002/crop-001_normalized.zarr",  # v2 >u2
]


@pytest.mark.parametrize("rel", VOLUMES)
async def test_lsf_histogram(rel, tmp_path):
    results = Path(os.environ["RESULTS_DIR"]) / "integration-tests" if os.environ.get("RESULTS_DIR") else PROJECT_DIR / ".it-results"
    cfg = SimpleNamespace(lmd_root=LMD, results_dir=results, histogram=HistogramCfg(max_chunks=32),
                          cluster=ClusterCfg(profile="janelia_lsf",
                                             resources={"cpus": 1, "memory": "4 GB", "walltime": "00:20"},
                                             project=os.environ.get("LSF_BILLING_GROUP")))
    c = Cluster(cfg)
    run_id = f"it-{tmp_path.name[-12:].strip('_')}"
    try:
        job = await c.submit(1, rel, run_id)
        assert await c.submit(1, rel, run_id) == job
        out = await c.wait(1, run_id, job, lambda s: print("status", s), interval=15)
        assert out.status == "done", c.log_tail(1, run_id)
        stats = json.loads((c.run_dir(1, run_id) / "stats.json").read_text())
        print(out, {k: stats[k] for k in ("dtype", "chunks_read", "total_chunks", "mean", "elapsed_s")})
        assert stats["chunks_read"] == min(32, stats["total_chunks"])
    finally:
        await c.close()
