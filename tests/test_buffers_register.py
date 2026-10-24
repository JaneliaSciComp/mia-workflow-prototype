"""`mia-buffers register`: plan rows + stage.json files → buffer-api registrations."""

import json
import threading

import httpx
import pytest

from mia_buffers.api import BufferService, make_server
from mia_buffers.ledger import Ledger
from mia_buffers.register import buffer_config, register, volume_bodies
from tests.test_buffers_plan import spec, spec_2d, spec_2d_mixed  # noqa: F401  (fixtures)


def write_stage_json(staged, *, planes=25, extent=2048, labels=None, channels=3):
    staged.mkdir(parents=True)
    levels = [
        {
            "path": f"s{i}",
            "factor": f,
            "shape": [channels, planes, extent // f, extent // f],
            "scale_nm": {"z": 1.0, "y": 45.0 * f, "x": 45.0 * f},
            "translation_nm": {"z": 0.0, "y": 22.5 * (f - 1), "x": 22.5 * (f - 1)},
        }
        for i, f in enumerate((1, 2, 4))
    ]
    volume = {
        "path": "v0.zarr",
        "axes": "czyx",
        "shape": [channels, planes, extent, extent],
        "dtype": "uint16",
        "access": "plane",
        "voxel_size_nm": {"z": 1.0, "y": 45.0, "x": 45.0},
        "labels": labels or {},
        "levels": levels,
    }
    (staged / "stage.json").write_text(json.dumps({"ok": True, "volumes": [volume], "errors": []}))
    return staged


ROW = {
    "id": "u937",
    "weight": 0.5,
    "channel_recipe": {"op": "mean"},
    "splits": {"train": 20, "val": 3, "test": 2},
    "n_items": 25,
    "admitted": True,
    "fit": {"ok": True},
}


def test_volume_bodies_split_planes_by_fraction(tmp_path, spec_2d):  # noqa: F811
    staged = write_stage_json(tmp_path / "u937", labels={"nuclei": "labels/nuclei"})
    [(vid, vb)] = volume_bodies(ROW, staged, spec_2d)
    assert vid == "u937/v0.zarr" and vb["path"] == str(staged / "v0.zarr") and vb["weight"] == 0.5
    assert vb["recipe"] == {"op": "mean"} and vb["labels"] == {"nuclei": "labels/nuclei"} and vb["channels_stored"] == 3
    assert {k: len(v) for k, v in vb["split_planes"].items()} == {"train": 20, "val": 3, "test": 2}
    assert sorted(p for ps in vb["split_planes"].values() for p in ps) == list(range(25))
    assert vb["split_planes"]["train"] != list(range(20))  # shuffled, not the first twenty
    assert volume_bodies(ROW, staged, spec_2d) == [(vid, vb)]  # deterministic
    one = write_stage_json(tmp_path / "one", planes=1)
    [(_, single)] = volume_bodies({**ROW, "id": "one"}, one, spec_2d)
    assert sum(len(v) for v in single["split_planes"].values()) == 1


def test_buffer_config_from_plan(spec_2d):  # noqa: F811
    plan = {"spec_name": "s", "spec_hash": "abc", "catalog_ref": "r", "rows": []}
    cfg = buffer_config(plan, spec_2d, tensorizer="muvit-2d", split_sizes={"val": 8, "test": 8})
    assert cfg["levels"] == [1, 2, 4] and cfg["patch"] == 256 and cfg["channels"] == "all" and cfg["seed"] == 0
    assert cfg["splits"] == {"train": 0.8, "val": 0.1, "test": 0.1} and cfg["split_sizes"] == {"val": 8, "test": 8}
    assert cfg["tensorizer_params"] == {"normalize": "percentile", "range": [1, 99], "clip": True}


@pytest.fixture
def server(tmp_path):
    srv = make_server(BufferService(Ledger(tmp_path / "l.sqlite")), "127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def test_register_end_to_end(tmp_path, spec_2d, server):  # noqa: F811
    root = tmp_path / "staged"
    write_stage_json(root / "u937")
    write_stage_json(root / "tiny", extent=512)
    plan = {
        "spec_name": "s",
        "spec_hash": "abc",
        "catalog_ref": None,
        "rows": [
            ROW,
            {**ROW, "id": "bcss"},
            {**ROW, "id": "tiny"},
            {**ROW, "id": "out", "admitted": False},
            {**ROW, "id": "unfit", "fit": {"ok": False}},
        ],
    }
    out = register(server, "demo", plan, spec_2d, root, tensorizer="muvit-2d", split_sizes={"val": 4, "test": 4})
    assert out["registered"] == ["u937/v0.zarr"] and out["skipped"] == {"bcss": "not staged"}
    assert out["refused"] == {"tiny/v0.zarr": "footprint 1024 exceeds 512×512"}
    status = httpx.get(f"{server}/buffers/demo/status").json()
    assert status["servable"] == {"test": 1, "train": 1, "val": 1} and status["config"]["split_sizes"] == {
        "val": 4,
        "test": 4,
    }


def test_single_plane_volumes_split_by_fraction_across_a_dataset(tmp_path, spec_2d):  # noqa: F811
    seen = {"train": 0, "val": 0, "test": 0}
    for i in range(40):
        staged = write_stage_json(tmp_path / f"one{i}", planes=1)
        [(_, vb)] = volume_bodies({**ROW, "id": f"one{i}"}, staged, spec_2d)
        for split, planes in vb["split_planes"].items():
            seen[split] += len(planes)
    assert sum(seen.values()) == 40 and seen["val"] > 0 and seen["test"] > 0 and seen["train"] > 20


def test_volume_bodies_accepts_a_report(tmp_path, spec_2d):  # noqa: F811
    staged = write_stage_json(tmp_path / "staged" / "rec")
    row = {"id": "rec", "weight": 1.0, "channel_recipe": None}
    from_file = volume_bodies(row, staged, spec_2d)
    report = json.loads((staged / "stage.json").read_text())
    (staged / "stage.json").unlink()
    assert volume_bodies(row, staged, spec_2d, report=report) == from_file
