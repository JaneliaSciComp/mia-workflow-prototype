"""Tiny synthetic LMD store: zarr v2 + v3 volumes with uint8, uint16 and float32 raw."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr


def _multiscales(paths, voxel=(8.0, 8.0, 8.0)):
    datasets = [{"path": p, "coordinateTransformations": [{"type": "scale", "scale": [v * 2**i for v in voxel]}]}
                for i, p in enumerate(paths)]
    axes = [{"name": a, "type": "space", "unit": "nanometer"} for a in "zyx"]
    return [{"version": "0.4", "axes": axes, "datasets": datasets}]


def make_volume(root: Path, dataset: str, crop: str, *, fmt: int, dtype: str, shape=(40, 40, 40),
                chunks=(16, 16, 16), labels: dict[str, str] | None = None, voxel=True, seed=0) -> Path:
    path = root / "data" / dataset / crop
    rng = np.random.default_rng(seed)
    g = zarr.open_group(str(path), mode="w", zarr_format=fmt)
    raw = g.create_group("raw")
    ms = _multiscales(["s0", "s1"])
    if not voxel:
        for d in ms[0]["datasets"]:
            d.pop("coordinateTransformations")
    raw.attrs.update({"ome": {"version": "0.5", "multiscales": ms}} if fmt == 3 else {"multiscales": ms})
    if np.dtype(dtype).kind == "f":
        data = rng.normal(100, 20, shape).astype(dtype)
    else:
        data = rng.integers(0, min(np.iinfo(dtype).max, 4000), shape).astype(dtype)
    data[:8] = 0  # some exact zeros
    for i, lvl in enumerate(["s0", "s1"]):
        d = data[:: 2**i, :: 2**i, :: 2**i]
        a = raw.create_array(lvl, shape=d.shape, dtype=dtype, chunks=chunks)
        a[:] = d
    for name, ldt in (labels or {}).items():
        lg = g.require_group("labels").create_group(name)
        lg.attrs.update({"class": "cell", "segmentation_type": "instance", "provenance": "manual",
                         "proofreading_status": "proofread", "coverage": "dense"} if "manual" in name else {})
        a = lg.create_array("s0", shape=shape, dtype=ldt, chunks=chunks)
        a[:] = 1
    return path


@pytest.fixture(scope="session")
def lmd_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("lmd")
    make_volume(root, "em-mouse-foo", "crop-001_cortex.zarr", fmt=3, dtype="uint8",
                labels={"manual_gt-cell-dense": "uint64", "auto_pred-mito-v2": "int32"})
    make_volume(root, "xray-fly-bar", "crop-002_vnc.zarr", fmt=2, dtype="uint16", chunks=(20, 20, 20), voxel=False)
    make_volume(root, "em-mouse-foo", "crop-003_float.zarr", fmt=3, dtype="float32", chunks=(8, 8, 8))
    (root / "data" / "em-mouse-foo" / "notes.txt").write_text("not a volume")
    (root / "data" / "em-mouse-foo" / "crop-004_empty.zarr").mkdir()
    return root
