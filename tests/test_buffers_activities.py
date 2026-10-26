"""mia_buffers.activities against fake GitHub clients, the fixture catalog as the catalog snapshot, and a live
buffer-api for registration."""

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
import yaml
from mia_helpers import FakeGH
from temporalio.exceptions import ApplicationError
from test_buffers_config import make_cfg
from test_buffers_plan import BCSS, CATALOG, ONE_CH, U937, spec_with
from test_buffers_register import token_server, write_stage_json  # noqa: F401  (fixture)

from mia_buffers import activities as acts_module
from mia_buffers.activities import Activities
from mia_buffers.drop import cache_key, read_manifest
from mia_buffers.jobspec import artifact_id
from mia_buffers.models import BufferPins, BufferState
from mia_buffers.spec import load_spec
from mia_buffers.status import render_plan_comment
from mia_core.registry import Registry, RegistryError
from mia_data.catalog import dataset_folder

FIXTURES = Path(__file__).parent / "fixtures" / "buffers"
REF = "c" * 40
ROUND = "i7-e11"


def spec_text() -> str:
    spec = spec_with(
        load_spec(FIXTURES / "spec.yaml"),
        **{
            "select.where.dims": {"in": ["2D"]},
            "select.where.annotations.source": "any",
            "select.exclude.ids": [ONE_CH],
            "select.exclude.reasons": {ONE_CH: "one channel"},
            "catalog.ref": REF,
        },
    )
    return yaml.safe_dump(spec.model_dump(by_alias=True, exclude={"warnings"}), sort_keys=False)


def issue_body(spec: str | None = None, fences: int = 1) -> str:
    block = f"```yaml\n{spec or spec_text()}```\n"
    return "A buffer for the demo.\n\n" + block * fences + "\nNotes below.\n"


def record_of(record_id: str) -> dict:
    return yaml.safe_load(next(CATALOG.glob(f"datasets/**/{record_id}.yaml")).read_text())


@pytest.fixture
def acts(tmp_path, monkeypatch):
    cfg = make_cfg(tmp_path)
    for directory in (cfg.paths.state_dir, cfg.paths.work_root, cfg.paths.storage_root, cfg.paths.raw_root):
        directory.mkdir(parents=True, exist_ok=True)
    (cfg.harness.dir / "tensorizers" / "muvit-2d").mkdir(parents=True)
    (cfg.harness.dir / "tensorizers" / "muvit-2d" / "tensorizer.yaml").write_text("name: muvit-2d\n")

    def fake_catalog_snapshot(cfg_, ref, tokens=None):
        dest = cfg_.paths.work_root / "_catalog" / ref
        if not dest.exists():
            shutil.copytree(CATALOG, dest)
        return dest

    monkeypatch.setattr(acts_module, "catalog_snapshot", fake_catalog_snapshot)
    monkeypatch.setattr(acts_module, "harness_pin", lambda cfg_: ("h" * 40, "d" * 64))
    monkeypatch.setattr(acts_module, "harness_snapshot", lambda cfg_, commit: tmp_path / "hsnap")
    gh = FakeGH("o/buffers", issues={7: {"number": 7, "body": issue_body(), "labels": [{"name": "buffer:planning"}]}})
    activities = Activities(
        cfg,
        gh,
        FakeGH("o/snippets"),
        FakeGH("o/catalog"),
        None,
        bot="bot",
        probe=lambda: ["zenodo.org", "*.zenodo.org"],
    )
    activities.gh = gh
    return activities


@pytest.fixture
def server(acts, token_server):  # noqa: F811
    """A buffer-api that wants the worker token `t`, and the activities' registry client holding it."""
    acts.registry = Registry(token_server, "t")
    yield token_server
    acts.registry.close()


async def admitted(acts) -> BufferPins:
    return await acts.admit(7, "11")


def write_binding(acts, record_id: str, urls: list[str], snippet: str = "tiff-zip-member") -> Path:
    folder = acts.cfg.paths.storage_root / dataset_folder(record_of(record_id), record_id)
    folder.mkdir(parents=True, exist_ok=True)
    binding = {
        "record": record_id,
        "record_commit": "c" * 40,
        "snippets_commit": "a" * 40,
        "raw": [{"snippet": snippet, "urls": urls, "params": {"axes": "zcyx"}}],
        "labels": [],
        "reasons": ["test"],
    }
    (folder / "binding.yaml").write_text(yaml.safe_dump(binding))
    return folder / "binding.yaml"


# -- admit and plan


async def test_admit_writes_spec_and_pins(acts):
    pins = await admitted(acts)
    assert pins.catalog_ref == REF and pins.harness_commit == "h" * 40 and len(pins.spec_hash) == 64
    round_dir = acts.cfg.paths.state_dir / "rounds" / ROUND
    assert (round_dir / "spec.yaml").read_text() == spec_text() and (round_dir / "pins.json").is_file()
    assert await acts.admit(7, "11") == pins


@pytest.mark.parametrize("fences", [0, 2])
async def test_admit_refuses_issue_without_one_yaml_fence(acts, fences):
    acts.gh.issues[7]["body"] = issue_body(fences=fences)
    with pytest.raises(ApplicationError, match="exactly one ```yaml") as error:
        await admitted(acts)
    assert error.value.non_retryable


async def test_admit_refuses_invalid_spec(acts):
    acts.gh.issues[7]["body"] = issue_body(spec_text().replace("schema_version: 1", "schema_version: 2"))
    with pytest.raises(ApplicationError, match="spec refused") as error:
        await admitted(acts)
    assert error.value.non_retryable


async def test_plan_writes_files_and_summarises(acts):
    pins = await admitted(acts)
    result = await acts.plan(7, ROUND, pins)
    assert result.valid and result.errors == [] and result.n_seen == 6
    assert sorted(d.record for d in result.pool) == sorted([BCSS, U937])
    by_record = {d.record: d for d in result.pool}
    assert by_record[U937].group == "confocal" and by_record[BCSS].group == "histology-WSI"
    assert by_record[BCSS].manual and not by_record[U937].manual
    # the record's voxel size reaches the stage job; a 2D stack's plane axis (fields of view) gets 1 nm, which
    # the harness would otherwise report as "voxel size unknown" after downloading the whole dataset
    assert by_record[U937].voxel_size_nm == {"z": 1.0, "y": 45.0718, "x": 45.0718}
    assert (result.min_fraction, result.timeout_days, result.levels, result.patch) == (1.0, 14, [1, 2, 4], 256)
    round_dir, workspace = acts.cfg.paths.state_dir / "rounds" / ROUND, acts.cfg.paths.work_root / "issue-7" / ROUND
    assert json.loads((round_dir / "plan.json").read_text())["spec_hash"] == pins.spec_hash
    assert (workspace / "plan.md").read_text() == (round_dir / "plan.md").read_text()


async def test_plan_without_tensorizer_is_invalid(acts):
    shutil.rmtree(acts.cfg.harness.dir / "tensorizers")
    result = await acts.plan(7, ROUND, await admitted(acts))
    assert not result.valid and result.errors == ["no tensorizer muvit-2d in the harness checkout"]


async def test_comment_plan_posts_once(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    await acts.comment_plan(7, ROUND)
    await acts.comment_plan(7, ROUND)
    bodies = [c["body"] for c in acts.gh.comments[7]]
    assert (
        len(bodies) == 1
        and "2 in the pool" in bodies[0]
        and U937 in bodies[0]
        and "<!-- mia-buffers: plan i7-e11 -->" in bodies[0]
    )


def test_render_plan_comment_truncates():
    rows = [
        {
            "id": f"rec-{i}",
            "in_pool": True,
            "modality_group": "TEM",
            "voxel_size_nm": {"y": 4, "x": 4},
            "assumed": [],
            "weight": 0.001,
            "splits": {"train": 1},
            "notes": [],
            "admitted": True,
            "fit": {"ok": True},
        }
        for i in range(3000)
    ]
    body = render_plan_comment(
        {"spec_name": "s", "spec_hash": "h" * 64, "catalog_ref": REF, "errors": [], "warnings": [], "rows": rows},
        "https://b/w",
    )
    assert len(body) <= 61_000 and "truncated" in body and "https://b/w" in body


def test_plan_comment_lists_dropped_records():
    rows = [
        {
            "id": rid,
            "modality_group": "TEM",
            "weight": 0.5,
            "splits": {"train": 1},
            "notes": [],
            "admitted": True,
            "fit": {"ok": True},
        }
        for rid in ("u937", "bcss")
    ]
    plan = {"spec_name": "s", "spec_hash": "h" * 64, "catalog_ref": REF, "errors": [], "warnings": [], "rows": rows}
    body = render_plan_comment(plan, None, dropped={"bcss": "data:ineligible on o/data#5"})
    assert "| `bcss` |" not in body and "| `u937` |" in body  # the table keeps only the remaining pool
    assert "**Dropped**" in body and "- `bcss`: data:ineligible on o/data#5" in body
    assert "1 in the pool of 2 seen" in body
    assert "**Dropped**" not in render_plan_comment(plan, None)


# -- the dataset hand-off (milestone 4)


def with_datasets(acts, issues=None):
    """Give the activities a datasets-repo client holding `issues` (number -> labels)."""
    gh = FakeGH(
        "o/data",
        issues={
            n: {"number": n, "body": f"<!-- mia-data:record={rid} -->\n", "labels": [{"name": x} for x in labels]}
            for n, (rid, labels) in (issues or {}).items()
        },
    )
    acts.gh_datasets = gh
    return gh


async def test_nudge_dataset_opens_the_issue_and_adds_go(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    gh = with_datasets(acts)
    info = await acts.nudge_dataset(ROUND, U937)
    assert (info.number, info.stage, info.nudged) == (100, "new", True) and info.folder == "lm-human-u937-edu-h2ax-dapi"
    ((title, body, labels),) = gh.created
    assert body.startswith(f"<!-- mia-data:record={U937} -->") and title.startswith(U937) and "data:new" in labels
    assert "Downloaded by: none" in body and "data:go" in {x["name"] for x in gh.issues[100]["labels"]}
    again = await acts.nudge_dataset(ROUND, U937)  # a retry: the issue exists and already carries data:go
    assert (again.number, again.nudged) == (100, False) and len(gh.created) == 1


async def test_nudge_dataset_in_progress_does_not_relabel(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    gh = with_datasets(acts, {5: (U937, ["data:testing"])})
    info = await acts.nudge_dataset(ROUND, U937)
    assert (info.number, info.stage, info.nudged) == (5, "testing", False)
    assert {x["name"] for x in gh.issues[5]["labels"]} == {"data:testing"}


async def test_nudge_dataset_ineligible(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    gh = with_datasets(acts, {5: (U937, ["data:ineligible", "licence-excluded"])})
    info = await acts.nudge_dataset(ROUND, U937)
    assert (info.number, info.stage, info.nudged) == (5, "ineligible", False)
    assert {x["name"] for x in gh.issues[5]["labels"]} == {"data:ineligible", "licence-excluded"}


async def test_nudge_dataset_done_without_binding_reports_it(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    gh = with_datasets(acts, {5: (U937, ["data:done"])})
    info = await acts.nudge_dataset(ROUND, U937)
    assert (info.number, info.stage, info.nudged) == (5, "done", False)
    assert "binding.yaml" in info.note and "data:go" in info.note
    assert {x["name"] for x in gh.issues[5]["labels"]} == {"data:done"}


async def test_nudge_dataset_needs_human_gets_go_again(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    gh = with_datasets(acts, {5: (U937, ["data:needs-human"])})
    info = await acts.nudge_dataset(ROUND, U937)
    assert (info.stage, info.nudged) == ("needs-human", True)
    assert "data:go" in {x["name"] for x in gh.issues[5]["labels"]}


async def test_comment_plan_reposts_once_per_dropped_set(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    await acts.comment_plan(7, ROUND)
    await acts.comment_plan(7, ROUND, {BCSS: "data:ineligible on o/data#5"})
    await acts.comment_plan(7, ROUND, {BCSS: "data:ineligible on o/data#5"})
    bodies = [c["body"] for c in acts.gh.comments[7]]
    assert len(bodies) == 2 and "**Dropped**" in bodies[1] and f"- `{BCSS}`: data:ineligible" in bodies[1]
    assert "1 in the pool" in bodies[1] and f"| `{BCSS}` |" not in bodies[1]
    await acts.comment_plan(7, ROUND, {BCSS: "x", U937: "y"})
    assert len(acts.gh.comments[7]) == 3


# -- bindings and the manual drop


async def test_read_binding_found_hosts_and_copy(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, U937, ["https://zenodo.org/api/records/1/files/a.tif/content"])
    info = await acts.read_binding(ROUND, U937)
    assert info.found and info.errors == [] and info.manual == []
    assert info.hosts == ["conda.anaconda.org", "files.pythonhosted.org", "pypi.org", "repo.prefix.dev", "zenodo.org"]
    copied = acts.cfg.paths.state_dir / "rounds" / ROUND / "bindings" / f"{U937}.yaml"
    assert Path(info.path) == copied and info.hash == hashlib.sha256(copied.read_bytes()).hexdigest()


async def test_read_binding_manual_when_host_not_allowed(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    urls = ["https://drive.google.com/uc?export=download&id=1", "manual://x/b.zip::m.png"]
    write_binding(acts, BCSS, urls, snippet="png-pairs")
    info = await acts.read_binding(ROUND, BCSS)
    assert info.found and info.hosts == []
    assert info.manual == [
        ["https://drive.google.com/uc?export=download&id=1", "file-0"],
        ["manual://x/b.zip", "b.zip"],
    ]


async def test_read_binding_missing_or_wrong_record(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    info = await acts.read_binding(ROUND, U937)
    assert not info.found and info.errors and "no binding at" in info.errors[0]
    path = write_binding(acts, U937, ["https://zenodo.org/a.tif"])
    path.write_text(path.read_text().replace(f"record: {U937}", "record: other"))
    info = await acts.read_binding(ROUND, U937)
    assert not info.found and "names record 'other'" in info.errors[0]


async def test_verify_drop_moves_files_and_writes_manifest(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, BCSS, ["manual://x/b.zip::m.png", "manual://x/b.zip::n.png"], snippet="png-pairs")
    await acts.read_binding(ROUND, BCSS)
    cache = acts.cfg.paths.raw_root / BCSS
    (cache / "incoming").mkdir(parents=True)
    (cache / "incoming" / "b.zip").write_bytes(b"zipbytes")
    result = await acts.verify_drop(ROUND, BCSS)
    validators = {"size": 8, "sha256": hashlib.sha256(b"zipbytes").hexdigest()}
    assert result.ok and result.placed == ["b.zip"] and result.missing == []
    assert (cache / cache_key("manual://x/b.zip", validators)).read_bytes() == b"zipbytes"
    assert not (cache / "incoming" / "b.zip").exists()
    entry = read_manifest(cache)["manual://x/b.zip"]
    assert entry["validators"] == validators and entry["source"] == "manual"
    assert (await acts.verify_drop(ROUND, BCSS)).ok  # idempotent: the manifest already has it


async def test_verify_drop_rejects_symlink_and_empty_file(acts):
    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, BCSS, ["manual://x/a.zip", "manual://x/b.zip", "manual://x/c.zip"], snippet="png-pairs")
    await acts.read_binding(ROUND, BCSS)
    cache = acts.cfg.paths.raw_root / BCSS
    (cache / "incoming").mkdir(parents=True)
    (cache / "incoming" / "a.zip").write_bytes(b"")
    os.symlink("/etc/hostname", cache / "incoming" / "b.zip")
    result = await acts.verify_drop(ROUND, BCSS)
    assert not result.ok and result.placed == [] and result.missing == ["a.zip", "b.zip", "c.zip"]
    assert not (cache / "files.json").exists() and (cache / "incoming" / "a.zip").exists()


# -- the stage job's output and registration


def staged_dir(acts, record_id):
    """<staged_root>/<levels key>/<record>/<artifact>: the id from the binding read_binding copied into the round,
    the plan row's voxel size and the round's pins."""
    round_dir = acts.cfg.paths.state_dir / "rounds" / ROUND
    binding_hash = hashlib.sha256((round_dir / "bindings" / f"{record_id}.yaml").read_bytes()).hexdigest()
    row = next(row for row in json.loads((round_dir / "plan.json").read_text())["rows"] if row["id"] == record_id)
    voxel = {axis: size for axis, size in row["voxel_size_nm"].items() if size is not None}
    pins = json.loads((round_dir / "pins.json").read_text())
    artifact = artifact_id(binding_hash, "p256-l1-2-4", voxel, pins["harness_commit"], pins["env_digest"])
    return acts.cfg.paths.staged_root / "p256-l1-2-4" / record_id / artifact


async def planned_and_bound(acts, record_id: str = U937) -> None:
    """Admit and plan the round and copy the record's binding in: the staged directory's id is built from it."""
    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, record_id, ["https://zenodo.org/a.tif"])
    await acts.read_binding(ROUND, record_id)


async def test_prepare_stage_makes_workspace_and_dirs(acts):
    await planned_and_bound(acts)
    workspace = Path(await acts.prepare_stage(7, ROUND, U937))
    assert workspace == acts.cfg.paths.work_root / "issue-7" / ROUND / f"stage-{U937}"
    assert yaml.safe_load((workspace / "record.yaml").read_text())["id"] == U937
    assert yaml.safe_load((workspace / "binding.yaml").read_text())["record"] == U937
    assert (acts.cfg.paths.raw_root / U937).is_dir() and staged_dir(acts, U937).is_dir()


async def test_read_stage_reduces_report(acts, server):  # read_stage registers what it read
    await planned_and_bound(acts)
    assert (await acts.read_stage(ROUND, U937)).errors == ["stage.json is missing, unreadable or not a JSON object"]
    write_stage_json(staged_dir(acts, U937))
    result = await acts.read_stage(ROUND, U937)
    assert result.ok and result.volumes == 1 and result.errors == []
    (staged_dir(acts, U937) / "stage.json").write_text(
        json.dumps(
            {"ok": False, "volumes": [], "errors": ["boom"], "host_not_allowed": {"host": "h.example", "chain": []}}
        )
    )
    result = await acts.read_stage(ROUND, U937)
    assert not result.ok and result.errors == ["boom"] and result.host_not_allowed == "h.example"


async def test_register_dataset_against_live_service(acts, server):
    await planned_and_bound(acts)
    write_stage_json(staged_dir(acts, U937))
    await acts.create_buffer(ROUND, ROUND)
    result = await acts.register_dataset(ROUND, U937, ROUND)
    assert result.registered == [f"{U937}/v0.zarr"] and result.refused == {}
    import httpx

    status = httpx.get(f"{server}/buffers/{ROUND}/status").json()
    assert status["config"]["spec_name"] == "microscopy-foundation-pretrain" and status["volumes"] == 1
    artifact = staged_dir(acts, U937).name  # the volume references the artifact, not the directory
    assert httpx.get(f"{server}/assets/staged/{artifact}").json()["refs"] == [ROUND]


async def test_register_dataset_raises_when_buffer_api_is_down(acts, server):
    """An outage is retried by Temporal, not turned into refused volumes (which would fail the dataset)."""
    await planned_and_bound(acts)
    write_stage_json(staged_dir(acts, U937))
    await acts.create_buffer(ROUND, ROUND)
    acts.registry = Registry("http://127.0.0.1:9")
    with pytest.raises(RegistryError) as error:
        await acts.register_dataset(ROUND, U937, ROUND)
    assert error.value.status is None


async def test_large_stage_json_with_many_sampled_volumes_is_read_and_registered(acts, server):
    """Review (milestone 4): 30 three-channel volumes with 4,096-value samples make stage.json ~5 MB, past the
    1 MiB cap the generic job-JSON reader uses; read_stage and register_dataset must still read it."""
    await planned_and_bound(acts)
    staged = write_stage_json(staged_dir(acts, U937))
    report = json.loads((staged / "stage.json").read_text())
    base = report["volumes"][0]
    sample = [[float(v) for v in range(4096)] for _ in range(3)]
    report["volumes"] = [{**base, "path": f"v{i}.zarr", "intensity_sample": sample} for i in range(30)]
    (staged / "stage.json").write_text(json.dumps(report, indent=2))
    assert (staged / "stage.json").stat().st_size > 1 << 20
    result = await acts.read_stage(ROUND, U937)
    assert result.ok and result.volumes == 30 and result.errors == []
    await acts.create_buffer(ROUND, ROUND)
    registered = await acts.register_dataset(ROUND, U937, ROUND)
    assert len(registered.registered) == 30 and registered.refused == {}


# -- labels and comments


async def test_set_stage_swaps_label_and_comments_once(acts):
    state = BufferState(7, round_id=ROUND, stage="plan-review")
    await acts.set_stage(7, "plan-review", state, "approve the plan")
    await acts.set_stage(7, "plan-review", state, "approve the plan")
    assert [x["name"] for x in acts.gh.issues[7]["labels"]] == ["buffer:plan-review"]
    bodies = [c["body"] for c in acts.gh.comments[7]]
    assert len(bodies) == 1 and "`buffer:plan-review`" in bodies[0] and "approve the plan" in bodies[0]


async def test_refuse_label_removes_and_explains(acts):
    acts.gh.issues[7]["labels"].append({"name": "buffer:approve"})
    await acts.refuse_label(7, "buffer:approve", "it is older than the plan", "12")
    assert (7, "buffer:approve") in acts.gh.removed and "older than the plan" in acts.gh.comments[7][0]["body"]
    with pytest.raises(ApplicationError):
        await acts.refuse_label(7, "data:go", "x", "12")


async def test_limits_come_from_config(acts):
    limits = await acts.limits()
    assert (limits.parallel, limits.walltime_min, limits.service_url) == (4, 120, "http://127.0.0.1:8765")


async def test_admit_accepts_crlf_bodies(acts):
    acts.gh.issues[7]["body"] = issue_body().replace("\n", "\r\n")  # GitHub's web editor saves CRLF
    pins = await admitted(acts)
    assert (acts.cfg.paths.state_dir / "rounds" / ROUND / "spec.yaml").read_text() == spec_text()
    assert pins.catalog_ref == REF


async def test_register_dataset_refuses_malformed_volumes(acts, server):
    await planned_and_bound(acts)
    staged = write_stage_json(staged_dir(acts, U937))
    report = json.loads((staged / "stage.json").read_text())
    good = report["volumes"][0]
    escaped = {**good, "path": "../../elsewhere.zarr"}
    broken = {"path": "v2.zarr", "axes": "czyx"}  # no shape, no levels
    (staged / "stage.json").write_text(json.dumps({**report, "volumes": [escaped, broken, good]}))
    await acts.create_buffer(ROUND, ROUND)
    result = await acts.register_dataset(ROUND, U937, ROUND)
    assert result.registered == [f"{U937}/v0.zarr"]
    assert set(result.refused) == {f"{U937}/../../elsewhere.zarr", f"{U937}/v2.zarr"}
    assert "path" in result.refused[f"{U937}/../../elsewhere.zarr"]


async def test_read_stage_records_the_staged_and_raw_assets(acts, server):
    """What the stage job left on disk is registered with buffer-api: the staged artifact with every input its id
    hashes, and the record's raw cache from files.json, so later rounds and a retire can find them."""
    from mia_buffers.drop import merge_manifest

    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, U937, ["https://zenodo.org/api/records/1/files/a.tif/content"])
    info = await acts.read_binding(ROUND, U937)
    url = "https://zenodo.org/api/records/1/files/a.tif/content"
    merge_manifest(acts.cfg.paths.raw_root / U937, {url: {"validators": {"size": 12}, "source": "download"}})
    write_stage_json(staged_dir(acts, U937))
    assert (await acts.read_stage(ROUND, U937)).ok
    artifact = staged_dir(acts, U937).name
    staged = acts.registry.asset("staged", artifact)
    assert {key: staged[key] for key in ("record", "levels_key", "path", "artifact", "ok")} == {
        "record": U937,
        "levels_key": "p256-l1-2-4",
        "path": str(staged_dir(acts, U937)),
        "artifact": artifact,
        "ok": True,
    }
    assert (staged["binding_hash"], staged["harness_commit"], staged["env_digest"]) == (info.hash, "h" * 40, "d" * 64)
    raw = acts.registry.asset("raw", U937)
    assert raw == {"path": str(acts.cfg.paths.raw_root / U937), "files": {url: {"size": 12, "source": "download"}}}
    acts.cfg.service.url = server  # the service's worker routes refuse a client without the token
    with pytest.raises(RegistryError) as error:
        Registry.from_config(acts.cfg.service).put_asset("staged", artifact, {})
    assert error.value.status == 401


async def test_read_binding_reports_a_pyramid_staged_earlier_for_the_same_binding(acts, server):
    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, U937, ["https://zenodo.org/api/records/1/files/a.tif/content"])
    assert not (await acts.read_binding(ROUND, U937)).staged  # nothing registered yet
    artifact = staged_dir(acts, U937).name
    write_stage_json(staged_dir(acts, U937))
    await acts.read_stage(ROUND, U937)
    info = await acts.read_binding(ROUND, U937)
    assert info.staged and info.artifact == artifact
    (staged_dir(acts, U937) / "stage.json").unlink()  # registered but gone from disk: stage again
    assert not (await acts.read_binding(ROUND, U937)).staged
    write_stage_json(staged_dir(acts, U937).parent / "tmp")
    (staged_dir(acts, U937).parent / "tmp" / "stage.json").rename(staged_dir(acts, U937) / "stage.json")
    assert (await acts.read_binding(ROUND, U937)).staged
    write_binding(acts, U937, ["https://zenodo.org/api/records/1/files/b.tif/content"])  # a new binding
    info = await acts.read_binding(ROUND, U937)
    assert not info.staged and info.artifact not in ("", artifact)
    acts.registry = Registry("http://127.0.0.1:9")  # buffer-api down: the binding is still read, nothing reused
    info = await acts.read_binding(ROUND, U937)
    assert info.found and not info.staged
