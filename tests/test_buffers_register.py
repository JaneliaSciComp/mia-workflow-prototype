"""`mia-buffers register`: plan rows + stage.json files → buffer-api registrations."""

import json
import threading

import httpx
import pytest

from mia_buffers.api import BufferService, make_server
from mia_buffers.ledger import Ledger
from mia_buffers.register import buffer_config, normalize_bounds, recipe_for, register, volume_bodies
from tests.test_buffers_plan import spec, spec_2d, spec_2d_mixed, spec_with  # noqa: F401  (fixtures)


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


PINNED = {"recipe": {"tensorizer": "muvit-2d"}, "plan_hash": "ph"}  # buffer_config requires both
ROW = {
    "id": "u937",
    "weight": 0.5,
    "channel_recipe": {"op": "mean"},
    "splits": {"train": 20, "val": 3, "test": 2},
    "split": {"policy": "fractions", "fractions": {"train": 0.8, "val": 0.1, "test": 0.1}, "seed": 0},
    "n_items": 25,
    "admitted": True,
    "fit": {"ok": True},
}


RECIPE = {"tensorizer": "muvit-2d", "params": {}, "harness_commit": "a" * 40, "env_digest": "d"}


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


def test_volume_bodies_carry_the_artifact(tmp_path, spec_2d):  # noqa: F811
    staged = write_stage_json(tmp_path / "u937")
    [(_, plain)] = volume_bodies(ROW, staged, spec_2d)
    [(_, keyed)] = volume_bodies(ROW, staged, spec_2d, artifact="0123456789abcdef")
    assert plain["artifact"] == "u937" and keyed["artifact"] == "0123456789abcdef"


def test_buffer_config_from_plan(spec_2d):  # noqa: F811
    plan = {"spec_name": "s", "spec_hash": "abc", "catalog_ref": "r", "rows": []}
    cfg = buffer_config(plan, spec_2d, tensorizer="muvit-2d", split_sizes={"val": 8, "test": 8}, **PINNED)
    assert cfg["levels"] == [1, 2, 4] and cfg["patch"] == 256 and cfg["channels"] == "all" and cfg["seed"] == 0
    assert cfg["splits"] == {"train": 0.8, "val": 0.1, "test": 0.1} and cfg["split_sizes"] == {"val": 8, "test": 8}
    assert cfg["tensorizer_params"] == {"normalize": "percentile", "range": [1, 99], "clip": True, "scope": "patch"}


@pytest.fixture
def server(tmp_path):
    srv = make_server(BufferService(Ledger(tmp_path / "l.sqlite")), "127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.fixture
def token_server(tmp_path):
    """A buffer-api that wants the worker token `t` on its worker routes."""
    srv = make_server(BufferService(Ledger(tmp_path / "l.sqlite"), token="t"), "127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def test_register_end_to_end(tmp_path, spec_2d, token_server):  # noqa: F811
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
    sizes = {"val": 4, "test": 4}
    with pytest.raises(httpx.HTTPStatusError):  # worker routes need the token
        register(
            token_server,
            "demo",
            plan,
            spec_2d,
            root,
            tensorizer="muvit-2d",
            split_sizes=sizes,
            recipe=RECIPE,
            plan_hash="h",
        )
    out = register(
        token_server,
        "demo",
        plan,
        spec_2d,
        root,
        tensorizer="muvit-2d",
        split_sizes=sizes,
        recipe=RECIPE,
        plan_hash="h",
        token="t",
    )
    assert out["registered"] == ["u937/v0.zarr"] and out["skipped"] == {"bcss": "not staged"}
    assert out["refused"] == {"tiny/v0.zarr": "footprint 1024 exceeds 512×512"}
    status = httpx.get(f"{token_server}/buffers/demo/status").json()
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
    row = {**ROW, "id": "rec", "weight": 1.0, "channel_recipe": None}
    from_file = volume_bodies(row, staged, spec_2d)
    report = json.loads((staged / "stage.json").read_text())
    (staged / "stage.json").unlink()
    assert volume_bodies(row, staged, spec_2d, report=report) == from_file


# --- normalisation statistics (milestone 4) ---------------------------------------------------

SAMPLE_V0 = [[float(v) for v in range(0, 1010, 10)]]  # one channel, 101 values 0..1000
SAMPLE_V1 = [[float(v) for v in range(500, 1510, 10)]]


def write_two_volumes(staged, samples):
    """stage.json with v0.zarr and v1.zarr (one channel, 25 planes); `samples` maps path -> intensity_sample
    (omitted when None)."""
    report = json.loads((write_stage_json(staged, channels=1) / "stage.json").read_text())
    base = report["volumes"][0]
    report["volumes"] = []
    for path, sample in samples.items():
        volume = {**base, "path": path}
        if sample is not None:
            volume["intensity_sample"] = sample
        report["volumes"].append(volume)
    (staged / "stage.json").write_text(json.dumps(report))
    return staged


def scoped(spec_2d, scope):  # noqa: F811
    return spec_with(spec_2d, **{"sampling.normalize.scope": scope})


def test_normalize_bounds_nearest_rank():
    assert normalize_bounds([[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]], (10, 90)) == {"lo": [10], "hi": [90]}
    assert normalize_bounds([[5, 5, 5], [1, 2, 3]], (0, 100)) == {"lo": [5, 1], "hi": [5, 3]}


def test_volume_scope_bounds_from_sample(tmp_path, spec_2d):  # noqa: F811
    staged = write_two_volumes(tmp_path / "u937", {"v0.zarr": SAMPLE_V0, "v1.zarr": SAMPLE_V1})
    bodies = dict(volume_bodies(ROW, staged, scoped(spec_2d, "volume")))
    assert bodies["u937/v0.zarr"]["normalize"] == normalize_bounds(SAMPLE_V0, (1, 99)) == {"lo": [10.0], "hi": [990.0]}
    assert bodies["u937/v1.zarr"]["normalize"] == normalize_bounds(SAMPLE_V1, (1, 99))


def test_dataset_scope_pools_volumes(tmp_path, spec_2d):  # noqa: F811
    staged = write_two_volumes(tmp_path / "u937", {"v0.zarr": SAMPLE_V0, "v1.zarr": SAMPLE_V1})
    bodies = dict(volume_bodies(ROW, staged, scoped(spec_2d, "dataset")))
    pooled = normalize_bounds([sorted(SAMPLE_V0[0] + SAMPLE_V1[0])], (1, 99))
    assert pooled != normalize_bounds(SAMPLE_V0, (1, 99))
    assert all(body["normalize"] == pooled for body in bodies.values()) and len(bodies) == 2


def test_patch_scope_has_no_bounds(tmp_path, spec_2d):  # noqa: F811
    staged = write_two_volumes(tmp_path / "u937", {"v0.zarr": SAMPLE_V0, "v1.zarr": None})
    bodies = dict(volume_bodies(ROW, staged, spec_2d))
    assert len(bodies) == 2 and all(body["normalize"] is None for body in bodies.values())


def test_scope_without_sample_is_refused(tmp_path, spec_2d):  # noqa: F811
    staged = write_two_volumes(tmp_path / "u937", {"v0.zarr": None, "v1.zarr": SAMPLE_V1})
    refused = {}
    bodies = dict(volume_bodies(ROW, staged, scoped(spec_2d, "volume"), refused=refused))
    assert list(bodies) == ["u937/v1.zarr"] and "re-stage" in refused["u937/v0.zarr"]
    with pytest.raises(ValueError, match="re-stage"):
        volume_bodies(ROW, staged, scoped(spec_2d, "volume"))


@pytest.mark.parametrize("sample", [[["a", "b"]], [[1, 2], [3]], [[]], "nope", [[1, None]]])
def test_malformed_sample_is_refused(tmp_path, spec_2d, sample):  # noqa: F811
    staged = write_two_volumes(tmp_path / "u937", {"v0.zarr": sample})
    refused = {}
    assert volume_bodies(ROW, staged, scoped(spec_2d, "dataset"), refused=refused) == []
    assert "re-stage" in refused["u937/v0.zarr"]


def test_buffer_config_carries_scope(spec_2d):  # noqa: F811
    plan = {"spec_name": "s", "spec_hash": "h", "catalog_ref": None}
    sizes = {"val": 1, "test": 1}
    params = buffer_config(plan, scoped(spec_2d, "dataset"), tensorizer="muvit-2d", split_sizes=sizes, **PINNED)
    assert params["tensorizer_params"]["scope"] == "dataset"
    patch_params = buffer_config(plan, spec_2d, tensorizer="muvit-2d", split_sizes=sizes, **PINNED)
    assert patch_params["tensorizer_params"]["scope"] == "patch"


def test_dataset_scope_refuses_volumes_with_other_channel_counts(tmp_path, spec_2d):  # noqa: F811
    """Review (milestone 4): pooling across volumes needs one channel count; a volume with another count is refused
    with a reason, not an IndexError that fails the activity."""
    staged = write_two_volumes(tmp_path / "u937", {"v0.zarr": SAMPLE_V0, "v1.zarr": SAMPLE_V1})
    report = json.loads((staged / "stage.json").read_text())
    rgb = report["volumes"][1]
    rgb["shape"], rgb["intensity_sample"] = [3, *rgb["shape"][1:]], SAMPLE_V1 * 3
    for level in rgb["levels"]:
        level["shape"] = [3, *level["shape"][1:]]
    (staged / "stage.json").write_text(json.dumps(report))
    refused = {}
    bodies = dict(volume_bodies(ROW, staged, scoped(spec_2d, "dataset"), refused=refused))
    assert list(bodies) == ["u937/v0.zarr"] and bodies["u937/v0.zarr"]["normalize"] == normalize_bounds(
        SAMPLE_V0, (1, 99)
    )
    assert "channel" in refused["u937/v1.zarr"]


def test_holdout_volume_registers_all_test(tmp_path, spec_2d):  # noqa: F811
    staged = write_stage_json(tmp_path / "u937")
    row = {**ROW, "split": {**ROW["split"], "policy": "holdout"}}
    [(_, vb)] = volume_bodies(row, staged, spec_2d)
    assert vb["split_planes"] == {"train": [], "val": [], "test": list(range(25))}


def test_split_seed_comes_from_the_row_not_the_spec(tmp_path, spec_2d):  # noqa: F811
    staged = write_stage_json(tmp_path / "u937")

    def planes(row, spec_):
        return volume_bodies(row, staged, spec_)[0][1]["split_planes"]

    other = {**ROW, "split": {**ROW["split"], "seed": 7}}
    assert planes(ROW, spec_2d) != planes(other, spec_2d)
    assert planes(ROW, spec_2d) == planes(ROW, spec_with(spec_2d, **{"sampling.seed": 99}))


def test_a_row_planned_before_d_falls_back_to_the_spec_fractions(tmp_path, spec_2d, caplog):  # noqa: F811
    """A plan.json written before milestone D has no `split` on its rows: registration uses the spec's fractions
    and seed, and says so once, rather than failing a live round."""
    staged = write_stage_json(tmp_path / "u937")
    fallback = {"policy": "fractions", "fractions": dict(spec_2d.splits.fractions), "seed": spec_2d.splits.seed}
    with caplog.at_level("WARNING", logger="mia_buffers.register"):
        [(_, old)] = volume_bodies({**ROW, "split": None}, staged, spec_2d)
    [(_, new)] = volume_bodies({**ROW, "split": fallback}, staged, spec_2d)
    assert old["split_planes"] == new["split_planes"]
    assert [record.getMessage() for record in caplog.records] == [
        "u937: plan row has no split policy (planned before D): using the spec's fractions"
    ]


def test_buffer_config_carries_recipe_plan_hash_and_lifecycle(spec_2d):  # noqa: F811
    plan = {"spec_name": "s", "spec_hash": "abc", "catalog_ref": "r", "rows": []}
    recipe = recipe_for(spec_2d, tensorizer="muvit-2d", harness_commit="a" * 40, env_digest="d")
    cfg = buffer_config(plan, spec_2d, tensorizer="muvit-2d", split_sizes={}, recipe=recipe, plan_hash="ph")
    assert recipe["params"] == cfg["tensorizer_params"] and cfg["recipe"] == recipe and cfg["plan_hash"] == "ph"
    assert (cfg["idle_hours"], cfg["retire_after_days"], cfg["auto_approve"]) == (72, 14, False)
