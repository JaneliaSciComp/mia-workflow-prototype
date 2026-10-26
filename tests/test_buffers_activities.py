"""mia_buffers.activities against fake GitHub clients, the fixture catalog as the catalog snapshot, and a live
buffer-api for registration."""

import hashlib
import json
import os
import shutil
import time
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
from mia_buffers.models import (
    BufferPins,
    BufferState,
    DegradedSignal,
    FetchResult,
    KeeperReport,
    PrepOutcome,
    PrepPins,
    PrepState,
    QuotaCheck,
    RawStatus,
    job_key,
)
from mia_buffers.plan import load_records, plan_hash
from mia_buffers.spec import load_spec
from mia_buffers.status import render_plan_comment
from mia_core.registry import Registry, RegistryError
from mia_data.catalog import dataset_folder
from mia_data.models import LockTaken
from mia_data.publish import reindex

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
def acts(tmp_path, monkeypatch, token_server):  # noqa: F811
    """The activities over the fixture catalog, with a live buffer-api (worker token `t`) that knows every catalog
    record as prepared; the plan syncs the round's ref into it on first use."""
    from mia_buffers import cli

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
    monkeypatch.setattr(cli, "catalog_snapshot", fake_catalog_snapshot)  # the plan's on-demand `catalog sync`
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
    registry = activities.registry = Registry(token_server, "t")
    for _, record in load_records(CATALOG):
        registry.put_dataset(record["id"], state="prepared")
    yield activities
    registry.close()


@pytest.fixture
def server(acts, token_server):  # noqa: F811
    """The buffer-api the activities' registry client talks to (the `acts` fixture starts it)."""
    return token_server


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
    reindex(folder, {})  # as mia-data's publish does: manifest.json records the binding it was verified with
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


async def test_plan_reads_the_registry_and_syncs_the_ref_on_demand(acts, server, monkeypatch):
    from mia_buffers import cli

    synced, real_sync = [], cli.catalog_sync

    async def counting_sync(cfg_, gh, registry, ref=None):
        synced.append(ref)
        return await real_sync(cfg_, gh, registry, ref)

    monkeypatch.setattr(cli, "catalog_sync", counting_sync)
    acts.registry.put_dataset(U937, state="in-progress")  # mia-data is still working on it: not prepared
    pins = await admitted(acts)
    spec_path = acts.cfg.paths.state_dir / "rounds" / ROUND / "spec.yaml"
    spec_path.write_text(spec_path.read_text().replace("auto_approve: false", "auto_approve: true"))
    assert acts.registry.catalogs() == []
    result = await acts.plan(7, ROUND, pins)
    assert synced == [REF] and [catalog["ref"] for catalog in acts.registry.catalogs()] == [REF]
    assert [dataset.record for dataset in result.pool] == [BCSS]
    plan_text = (acts.cfg.paths.state_dir / "rounds" / ROUND / "plan.json").read_text()
    row = next(row for row in json.loads(plan_text)["rows"] if row["id"] == U937)
    assert row["rule"] == "not prepared" and not row["admitted"]
    assert result.plan_hash == plan_hash(plan_text) and result.auto_approve is True
    [planned] = acts.registry.request("GET", f"/buffers/{ROUND}/events")["events"]  # no plan-review stage writes it
    assert (planned["kind"], planned["issue"], planned["detail"]) == ("planned", 7, "auto-approved")
    await acts.plan(7, ROUND, pins)
    assert synced == [REF]  # synced once: the ref is in the registry now
    acts.registry = Registry("http://127.0.0.1:9")
    with pytest.raises(RegistryError):  # buffer-api down: the activity retries, it never plans without it
        await acts.plan(7, ROUND, pins)


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
    assert "of 3000 seen" in body  # an older plan.json, every record a row
    plan = {"spec_name": "s", "spec_hash": "h", "catalog_ref": REF, "errors": [], "rows": rows[:2], "n_seen": 1940}
    assert "2 in the pool of 1940 seen" in render_plan_comment(plan, None)


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
    assert not info.found and info.errors and "no prepared dataset at" in info.errors[0]
    path = write_binding(acts, U937, ["https://zenodo.org/a.tif"])
    path.write_text(path.read_text().replace(f"record: {U937}", "record: other"))
    reindex(path.parent, {})
    info = await acts.read_binding(ROUND, U937)
    assert not info.found and "names record 'other'" in info.errors[0]


async def test_read_binding_needs_the_manifest_mia_data_published_for_it(acts):
    """Spec §4.2: a binding.yaml alone (placed by hand, or edited after the crops were verified) is not prepared."""
    await acts.plan(7, ROUND, await admitted(acts))
    path = write_binding(acts, U937, ["https://zenodo.org/a.tif"])
    (path.parent / "manifest.json").unlink()
    info = await acts.read_binding(ROUND, U937)
    assert not info.found and "matching manifest.json" in info.errors[0]
    reindex(path.parent, {})
    assert (await acts.read_binding(ROUND, U937)).found
    path.write_text(path.read_text() + "# edited after verification\n")
    assert not (await acts.read_binding(ROUND, U937)).found


async def test_verify_drop_moves_files_and_writes_manifest(acts, server):
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
    assert acts.registry.asset("raw", BCSS) == {"state": "ready"}  # no longer waiting-drop
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
    assert (acts.cfg.paths.raw_root / U937).is_dir() and acts._building_dir(ROUND, U937).is_dir()
    assert not staged_dir(acts, U937).exists()  # the final directory appears only when read_stage publishes it
    rebuilt = Path(await acts.prepare_stage(7, ROUND, U937, 1))  # a degraded pyramid's rebuild: its own dirs
    assert rebuilt == workspace.with_name(f"stage-{U937}-r1") and acts._building_dir(ROUND, U937, 1).is_dir()
    assert acts._building_dir(ROUND, U937, 1) != acts._building_dir(ROUND, U937)


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
    await acts.refuse_label(7, "prep:go", "busy", "13")  # MiaPrep refuses a go while its round runs
    assert (7, "prep:go") in acts.gh.removed and "Ignoring `prep:go`: busy" in acts.gh.comments[7][1]["body"]
    with pytest.raises(ApplicationError):
        await acts.refuse_label(7, "data:go", "x", "12")


async def test_limits_come_from_config(acts):
    limits = await acts.limits()
    assert (limits.parallel, limits.walltime_min, limits.service_url) == (4, 120, "http://127.0.0.1:8765")
    assert (limits.parallel_fetch, limits.fetch_walltime_min, limits.fetch_attempts) == (2, 480, 4)
    assert (limits.poll_minutes, limits.lock_ttl_s) == (2, 900)


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
    assert {key: raw[key] for key in ("path", "files")} == {
        "path": str(acts.cfg.paths.raw_root / U937),
        "files": {url: {"size": 12, "source": "download"}},
    }
    assert "state" not in raw  # the fetch phase owns the raw state
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


# -- the fetch phase: locks, quota, raw-cache state

A_TIF, B_ZIP = "https://zenodo.org/a.tif", "https://zenodo.org/b.zip"


async def bound_two_urls(acts) -> Path:
    """A planned round whose U937 binding lists a plain file and a zip member; returns the raw cache dir."""
    await acts.plan(7, ROUND, await admitted(acts))
    write_binding(acts, U937, [A_TIF, B_ZIP + "::m.tif"])
    await acts.read_binding(ROUND, U937)
    return acts.cfg.paths.raw_root / U937


async def test_raw_status_reads_files_json(acts):
    from mia_buffers.drop import merge_manifest

    cache = await bound_two_urls(acts)
    assert await acts.raw_status(ROUND, U937) == RawStatus(False, [A_TIF, B_ZIP])
    merge_manifest(cache, {A_TIF: {"validators": {"size": 3}, "source": "download"}})
    assert await acts.raw_status(ROUND, U937) == RawStatus(False, [B_ZIP])  # the zip is cached whole
    merge_manifest(cache, {B_ZIP: {"validators": {"size": 5}, "source": "download"}})
    assert await acts.raw_status(ROUND, U937) == RawStatus(True)


async def test_check_quota(acts, server):
    await bound_two_urls(acts)
    real = acts.registry
    acts.registry = Registry("http://127.0.0.1:9")  # quota 0 is unlimited: the registry is not asked
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(True)
    acts.cfg.quota.raw_bytes = 100
    with pytest.raises(RegistryError):  # an outage propagates: the workflow retries the activity
        await acts.check_quota(ROUND, U937)
    acts.registry = real
    real.put_asset("raw", "other-record", {"bytes": 90, "state": "ready"})
    plan_path = acts.cfg.paths.state_dir / "rounds" / ROUND / "plan.json"
    plan = json.loads(plan_path.read_text())
    assert next(row for row in plan["rows"] if row["id"] == U937)["bytes"] == 1153470226  # from the record
    for row in plan["rows"]:
        row["bytes"] = 20 if row["id"] == U937 else row["bytes"]
    plan_path.write_text(json.dumps(plan))
    before = time.time()
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(False, over_by=10, expected=20)
    row = real.asset("raw", U937)  # what the keeper frees room for
    assert row["wanted"] == 20 and row["blocked_at"] >= before and "state" not in row
    acts.cfg.quota.raw_bytes = 110
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(True, expected=20)
    workspace = Path(await acts.prepare_fetch(7, ROUND, U937, 1))
    await acts.read_fetch(ROUND, U937, str(workspace))  # the fetch ran: no longer blocked
    row = real.asset("raw", U937)
    assert row["wanted"] is None and row["blocked_at"] is None
    # a resumed fetch: the record's own partial cache is not counted twice (other 60 + own 30 + expected 50)
    real.put_asset("raw", "other-record", {"bytes": 60})
    real.put_asset("raw", U937, {"bytes": 30, "state": "partial"})
    for row in plan["rows"]:
        row["bytes"] = 50 if row["id"] == U937 else row["bytes"]
    plan_path.write_text(json.dumps(plan))
    acts.cfg.quota.raw_bytes = 100
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(False, over_by=10, expected=50)
    acts.cfg.quota.raw_bytes = 110
    assert await acts.check_quota(ROUND, U937) == QuotaCheck(True, expected=50)


async def test_prepare_fetch_makes_a_workspace_per_attempt(acts):
    cache = await bound_two_urls(acts)
    workspace = Path(await acts.prepare_fetch(7, ROUND, U937, 2))
    assert workspace == acts.cfg.paths.work_root / "issue-7" / ROUND / f"fetch-{U937}-2"
    assert yaml.safe_load((workspace / "record.yaml").read_text())["id"] == U937
    assert yaml.safe_load((workspace / "binding.yaml").read_text())["record"] == U937 and cache.is_dir()


async def test_read_fetch_without_a_report_is_partial(acts, server):
    """A job killed at its walltime writes no fetch.json: partial, and the next attempt resumes from the cache."""
    await bound_two_urls(acts)
    workspace = Path(await acts.prepare_fetch(7, ROUND, U937, 1))
    await acts.mark_asset("raw", U937, {"state": "fetching", "job": "j1"})
    result = await acts.read_fetch(ROUND, U937, str(workspace))
    assert result == FetchResult(True, partial=True, errors=["fetch.json missing: the job ended before writing it"])
    row = acts.registry.asset("raw", U937)
    assert row["state"] == "partial" and row["job"] is None and row["last_used"] > 0
    (workspace / "fetch.json").write_text("[1, 2]")
    assert (await acts.read_fetch(ROUND, U937, str(workspace))).partial


async def test_read_fetch_maps_outcomes_and_writes_the_raw_row(acts, server):
    from mia_buffers.drop import merge_manifest

    cache = await bound_two_urls(acts)
    workspace = Path(await acts.prepare_fetch(7, ROUND, U937, 1))
    files = [
        {"url": A_TIF, "outcome": "cached", "bytes": 3, "key": "k1"},
        {"url": B_ZIP + "::m.tif", "outcome": "fetched", "bytes": 5, "key": "k2"},
        {"url": "https://zenodo.org/c.zarr", "outcome": "stream", "reason": "needs a stream"},
        {"url": "https://zenodo.org/d.tif", "outcome": "failed", "reason": "HTTPError: 404"},
    ]
    report = {"ok": False, "partial": False, "bytes_fetched": 5, "files": files, "errors": ["<!-- x -->"]}
    (workspace / "fetch.json").write_text(json.dumps(report))
    result = await acts.read_fetch(ROUND, U937, str(workspace))
    assert result == FetchResult(
        False, False, 5, ["https://zenodo.org/c.zarr"], ["https://zenodo.org/d.tif"], ["<!-- x -->"], None
    )
    assert acts.registry.asset("raw", U937)["state"] == "partial"
    merge_manifest(cache, {A_TIF: {"validators": {"size": 3}, "source": "download"}})
    merge_manifest(cache, {B_ZIP: {"validators": {"size": 5}, "source": "download"}})
    report = {**report, "ok": True, "files": files[:3], "errors": [], "bytes_fetched": "lots"}
    (workspace / "fetch.json").write_text(json.dumps(report))
    result = await acts.read_fetch(ROUND, U937, str(workspace))
    assert result.ok and not result.partial and result.bytes_fetched == 0 and result.stream == [files[2]["url"]]
    row = acts.registry.asset("raw", U937)
    assert row["state"] == "ready" and set(row["files"]) == {A_TIF, B_ZIP} and row["bytes"] > 0
    assert row["stream"] == []  # c.zarr streams but is not a url of this binding
    pending = {**report, "files": [*files[:2], {"url": "https://zenodo.org/e.tif", "outcome": "pending"}]}
    blocked = {**report, "ok": False, "files": [], "host_not_allowed": {"host": "evil.example", "chain": []}}
    (workspace / "fetch.json").write_text(json.dumps(pending))
    assert (await acts.read_fetch(ROUND, U937, str(workspace))).partial
    (workspace / "fetch.json").write_text(json.dumps(blocked))
    result = await acts.read_fetch(ROUND, U937, str(workspace))
    assert not result.ok and result.host_not_allowed == "evil.example"
    acts.registry = Registry("http://127.0.0.1:9")  # the row is best effort: the result still comes back
    assert (await acts.read_fetch(ROUND, U937, str(workspace))).host_not_allowed == "evil.example"
    odd = {"ok": True, "files": [{"outcome": "failed"}, {"url": "", "outcome": "failed"}, {"url": 5}, "x"]}
    (workspace / "fetch.json").write_text(json.dumps(odd))  # entries without a url string are dropped
    assert await acts.read_fetch(ROUND, U937, str(workspace)) == FetchResult(True)
    for files in (5, True, "x", {"url": A_TIF}):
        (workspace / "fetch.json").write_text(json.dumps({**odd, "files": files}))  # not a list: no entries
        assert await acts.read_fetch(ROUND, U937, str(workspace)) == FetchResult(True)


async def test_a_stream_url_is_ready_for_fetch_status_and_stage(acts, server):
    """One rule for a url the harness streams: the fetch records it on the row, raw_status leaves it out of the
    files the cache must hold (and reports it), and a later stage leaves the raw state alone."""
    from mia_buffers.drop import merge_manifest

    cache = await bound_two_urls(acts)
    member = B_ZIP + "::m.tif"
    workspace = Path(await acts.prepare_fetch(7, ROUND, U937, 1))
    assert await acts.raw_status(ROUND, U937) == RawStatus(False, [A_TIF, B_ZIP])
    merge_manifest(cache, {A_TIF: {"validators": {"size": 3}, "source": "download"}})
    files = [{"url": A_TIF, "outcome": "fetched", "bytes": 3}, {"url": member, "outcome": "stream", "reason": "r"}]
    report = {"ok": True, "partial": False, "bytes_fetched": 3, "files": files, "errors": []}
    (workspace / "fetch.json").write_text(json.dumps(report))
    assert (await acts.read_fetch(ROUND, U937, str(workspace))).stream == [member]
    row = acts.registry.asset("raw", U937)
    assert row["state"] == "ready" and row["stream"] == [member]
    assert await acts.raw_status(ROUND, U937) == RawStatus(True, [], stream=[member])
    write_stage_json(staged_dir(acts, U937))
    assert (await acts.read_stage(ROUND, U937)).ok
    row = acts.registry.asset("raw", U937)
    assert row["state"] == "ready" and row["stream"] == [member] and row["last_used"] > 0
    acts.registry = Registry("http://127.0.0.1:9")  # no registry, no recorded stream urls: the zip counts as missing
    assert await acts.raw_status(ROUND, U937) == RawStatus(False, [B_ZIP])


async def test_raw_status_and_a_staged_find_touch_last_used(acts, server):
    """Review fix: what a (possibly waiting) buffer is about to use is touched, so the keeper's hold rule spares
    it: the raw row raw_status reads, and the staged row read_binding reports as reusable."""
    await planned_and_bound(acts)
    acts.registry.put_asset("raw", U937, {"state": "ready", "last_used": 1.0})
    before = time.time()
    await acts.raw_status(ROUND, U937)
    assert acts.registry.asset("raw", U937)["last_used"] >= before
    assert acts.registry.asset("raw", "nobody") is None
    write_stage_json(staged_dir(acts, U937))
    await acts.read_stage(ROUND, U937)
    artifact = staged_dir(acts, U937).name
    acts.registry.put_asset("staged", artifact, {"last_used": 1.0})
    before = time.time()
    assert (await acts.read_binding(ROUND, U937)).staged
    assert acts.registry.asset("staged", artifact)["last_used"] >= before


async def test_take_and_release_lock(acts):
    assert await acts.take_lock("raw", U937, "mia-buffers i7-e11 fetch", 900) == LockTaken(True)
    assert await acts.take_lock("raw", U937, "mia-buffers i7-e11 fetch", 900) == LockTaken(True)  # a retried take
    assert await acts.take_lock("raw", U937, "mia-data i1-e2 smoke", 900) == LockTaken(
        False, "mia-buffers i7-e11 fetch"
    )
    await acts.release_lock("raw", U937, "mia-data i1-e2 smoke")  # not the holder: the lock stays
    assert not (await acts.take_lock("raw", U937, "mia-data i1-e2 smoke", 900)).ok
    await acts.release_lock("raw", U937, "mia-buffers i7-e11 fetch")
    assert (await acts.take_lock("raw", U937, "mia-data i1-e2 smoke", 900)).ok
    assert (acts.cfg.paths.local_root / "locks" / "raw" / U937).is_file()


async def test_mark_asset_is_best_effort(acts, server):
    await acts.mark_asset("raw", U937, {"state": "waiting-drop"})
    await acts.mark_asset("raw", U937, {"job": "j9"})  # merged into the row
    assert acts.registry.asset("raw", U937) == {"state": "waiting-drop", "job": "j9"}
    acts.registry = Registry("http://127.0.0.1:9")
    await acts.mark_asset("raw", U937, {"state": "fetching"})  # buffer-api down: logged, not raised


async def test_read_stage_sets_state_bytes_and_last_used(acts, server):
    await planned_and_bound(acts)
    staged = write_stage_json(staged_dir(acts, U937))
    assert (await acts.read_stage(ROUND, U937)).ok
    row = acts.registry.asset("staged", staged.name)
    assert row["state"] == "ready" and row["job"] is None and row["last_used"] > 0
    assert row["bytes"] == (staged / "stage.json").stat().st_size
    assert acts.registry.asset("raw", U937)["last_used"] > 0
    (staged / "stage.json").write_text(json.dumps({"ok": False, "volumes": [], "errors": ["boom"]}))
    assert not (await acts.read_stage(ROUND, U937)).ok
    assert acts.registry.asset("staged", staged.name)["state"] == "partial"


class SubmitOnce:
    """A job runner that builds the spec it is handed (so the hosts check runs) and answers job id j42."""

    def __init__(self):
        self.specs = []

    async def submit(self, key, build):
        self.specs.append(build())
        return "j42"


async def test_submit_fetch_and_submit_check_hosts_and_mark_the_rows(acts, server, monkeypatch, tmp_path):
    from autodev.jobs import JobSpec
    from mia_buffers.models import FetchJobArgs, StageJobArgs

    def spec(cfg_, args, snap):  # the hosts are what the activity checks; the real specs are test_buffers_jobspec's
        return JobSpec("job", "true", args.workspace, 1, allow_hosts=args.hosts)

    await bound_two_urls(acts)
    acts.jobs = SubmitOnce()
    monkeypatch.setattr(acts_module, "snapshot", lambda cfg_, sha, tokens: tmp_path / "snap")
    monkeypatch.setattr(acts_module, "fetch_job", spec)
    monkeypatch.setattr(acts_module, "stage_job", spec)
    pins = await admitted(acts)
    workspace = await acts.prepare_fetch(7, ROUND, U937, 1)
    with pytest.raises(ApplicationError, match="hosts not allowed by the csub policy: evil.example"):
        await acts.submit_fetch("k1", FetchJobArgs(workspace, ROUND, pins, U937, ["evil.example"], 1))
    assert await acts.submit_fetch("k1", FetchJobArgs(workspace, ROUND, pins, U937, ["zenodo.org"], 1)) == "j42"
    assert acts.registry.asset("raw", U937) == {"state": "fetching", "job": "j42"}
    stage_workspace = await acts.prepare_stage(7, ROUND, U937)
    acts._building_dir(ROUND, U937).rmdir()  # the keeper's sweep took it between prepare and submit
    args = StageJobArgs(stage_workspace, ROUND, pins, U937, [], [1, 2, 4], 256, cache_writable=False)
    assert await acts.submit("k2", args) == "j42" and args.out == str(acts._building_dir(ROUND, U937))
    assert acts._building_dir(ROUND, U937).is_dir()  # the job's `out` mount exists again
    row = acts.registry.asset("staged", staged_dir(acts, U937).name)
    assert row == {
        "state": "staging",
        "ok": False,
        "job": "j42",
        "record": U937,
        "levels_key": "p256-l1-2-4",
        "path": str(staged_dir(acts, U937)),  # where read_stage publishes it
    }
    acts.registry = Registry("http://127.0.0.1:9")  # the rows are best effort: the job id still comes back
    assert await acts.submit_fetch("k1", FetchJobArgs(workspace, ROUND, pins, U937, [], 1)) == "j42"


# -- the cache keeper


@pytest.fixture
def keeper_server(acts, tmp_path):
    """A buffer-api scanning the activities' own roots, wanting the worker token `t`, and the registry client."""
    import threading

    from mia_buffers.api import BufferService, make_server
    from mia_buffers.ledger import Ledger

    paths = acts.cfg.paths
    paths.staged_root.mkdir(parents=True, exist_ok=True)
    (paths.storage_root / "unprepared").mkdir()  # an empty storage root reads as unmounted
    service = BufferService(
        Ledger(tmp_path / "keeper.sqlite"),
        token="t",
        storage_root=paths.storage_root,
        raw_root=paths.raw_root,
        staged_root=paths.staged_root,
    )
    srv = make_server(service, "127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    acts.registry = Registry(f"http://127.0.0.1:{srv.server_address[1]}", "t")
    yield acts.registry
    acts.registry.close()
    srv.shutdown()


def staged_artifact(acts, name: str, size: int) -> Path:
    artifact = acts.cfg.paths.staged_root / "p256-l1-2-4" / "rec" / name
    artifact.mkdir(parents=True)
    (artifact / "stage.json").write_text("{}")
    (artifact / "v0.bin").write_bytes(b"x" * size)
    return artifact


def referenced(registry, artifact: str) -> None:
    """Register a volume of buffer `b` staged as `artifact`, so the buffer references it."""
    from test_buffers_api import CFG, body

    registry.request("PUT", "/buffers/b", json=CFG)
    registry.request("PUT", "/buffers/b/volumes/v", json={**body(), "artifact": artifact})


def catalog_not_due(acts) -> None:
    mark = acts.cfg.paths.state_dir / "keeper" / "catalog-synced-at"
    mark.parent.mkdir(parents=True, exist_ok=True)
    mark.write_text(str(int(__import__("time").time())))


async def test_keep_evicts_a_free_staged_asset_under_quota(acts, keeper_server, monkeypatch):
    import time

    from mia_buffers import cli
    from mia_core.fs import dir_bytes

    registry = keeper_server
    monkeypatch.setattr(cli, "catalog_snapshot", lambda cfg_, ref, tokens: CATALOG)
    acts.gh_catalog.refs["main"] = REF
    free, held = staged_artifact(acts, "free", 1000), staged_artifact(acts, "held", 10)
    referenced(registry, "held")
    old = time.time() - 49 * 3600  # unused longer than quota.hold_hours (48)
    for key in ("free", "held"):
        registry.put_asset("staged", key, {"last_used": old})
    acts.cfg.quota.staged_bytes, acts.cfg.quota.headroom_bytes = dir_bytes(held) + 100, 0

    dry = await acts.keep(dry_run=True)
    assert dry.errors == [] and dry.scanned == 2 and dry.new == 0
    assert dry.evicted == ["staged free"] and free.is_dir() and registry.asset("staged", "free")["state"] == "ready"
    assert dry.catalog == f"{REF} (6 records)" and registry.catalogs()  # due: no mark file yet
    last = registry.request("GET", "/keeper")  # the pass, for the page
    assert last["dry_run"] is True and last["evicted"] == ["staged free"] and last["at"] > old

    report = await acts.keep()
    assert report.errors == [] and report.catalog == ""  # synced a pass ago
    assert report.evicted == ["staged free"] and not free.exists() and held.is_dir()
    row = registry.asset("staged", "free")
    assert row["state"] == "evicted" and row["bytes"] == 0 and row["evicted_at"] > old
    assert registry.asset("staged", "held")["state"] == "ready"
    assert report.over_quota == {} and acts.gh.comments == {}
    assert registry.request("GET", "/keeper")["dry_run"] is False
    assert (await acts.keep()).evicted == []  # under quota now


async def test_keep_posts_once_when_held_set_is_over_quota(acts, keeper_server):
    from mia_core.fs import dir_bytes

    registry = keeper_server
    catalog_not_due(acts)
    held, fresh = staged_artifact(acts, "held", 10), staged_artifact(acts, "fresh", 10)  # fresh: scanned just now
    referenced(registry, "held")
    acts.gh.issues[9] = {"number": 9, "body": "", "labels": [{"name": "bug"}]}  # newer, but not a buffer issue
    acts.cfg.quota.staged_bytes, acts.cfg.quota.headroom_bytes = 1, 0
    over_by = dir_bytes(held) + dir_bytes(fresh) - 1
    for _ in range(2):
        report = await acts.keep()
        assert report.errors == [] and report.evicted == [] and report.over_quota == {"staged": over_by}
    assert held.is_dir() and fresh.is_dir() and 9 not in acts.gh.comments
    [comment] = acts.gh.comments[7]
    assert f"over quota by {over_by} bytes" in comment["body"] and "keeper over-quota staged" in comment["body"]
    dry = await acts.keep(dry_run=True)
    assert dry.over_quota == {} and len(acts.gh.comments[7]) == 1


async def test_keep_resets_stale_builds_and_removes_old_building_dirs(acts, keeper_server):
    import time
    from types import SimpleNamespace

    registry = keeper_server
    catalog_not_due(acts)

    class Csub:
        def status(self, job_ids):
            if job_ids == ["3"]:
                raise RuntimeError("csub down")
            return {"1": [], "2": [SimpleNamespace(terminal=True)], "4": [SimpleNamespace(terminal=False)]}[job_ids[0]]

    acts.jobs = SimpleNamespace(client=Csub())
    registry.put_asset("raw", "r", {"state": "fetching", "job": "1"})
    for key, job in (("s", "2"), ("down", "3"), ("running", "4")):
        registry.put_asset("staged", key, {"state": "staging", "job": job})
    record_dir = acts.cfg.paths.staged_root / "p256-l1-2-4" / "rec"
    old, young = record_dir / ".building-old", record_dir / ".building-young"
    old.mkdir(parents=True)
    young.mkdir()
    stamp = time.time() - 49 * 3600
    os.utime(old, (stamp, stamp))

    dry = await acts.keep(dry_run=True)
    assert dry.stale == ["raw r partial", "staged s missing"] and dry.removed_building == [str(old)]
    assert old.is_dir() and registry.asset("raw", "r")["state"] == "fetching"
    report = await acts.keep()
    assert report.errors == [] and report.stale == dry.stale and report.removed_building == [str(old)]
    assert not old.exists() and young.is_dir()
    assert registry.asset("raw", "r") == {"state": "partial", "job": None}
    assert registry.asset("staged", "s") == {"state": "missing", "job": None}
    assert registry.asset("staged", "down")["state"] == registry.asset("staged", "running")["state"] == "staging"


async def test_keep_reports_failed_steps_without_raising(acts):
    from temporalio.testing import ActivityEnvironment

    acts.registry = Registry("http://127.0.0.1:9")
    acts.cfg.quota.staged_bytes = 1
    env, beats = ActivityEnvironment(), []
    env.on_heartbeat = lambda *details: beats.append(details[0])
    report = await env.run(acts.keep)
    assert beats == [
        "sync datasets",
        "sync assets",
        "buffers",
        "stale builds",
        "evict staged",
        "evict raw",
        "catalog sync",
    ]
    steps = [error.split(":")[0] for error in report.errors]
    assert steps == ["sync datasets", "sync assets", "buffers", "stale builds", "evict staged", "catalog sync"]
    assert report.scanned == 0 and report.evicted == []


async def test_keep_heartbeats_through_a_long_step(acts, monkeypatch):
    """Review fix: a background task heartbeats while one step (here a slow datasets sync) runs."""
    from temporalio.testing import ActivityEnvironment

    monkeypatch.setattr(acts_module, "KEEPER_HEARTBEAT_S", 0.02)
    acts.registry = Registry("http://127.0.0.1:9")
    monkeypatch.setattr(acts.registry, "sync_datasets", lambda: time.sleep(0.3) or {})
    env, beats = ActivityEnvironment(), []
    env.on_heartbeat = lambda *details: beats.append(details[0])
    await env.run(acts.keep)
    assert beats[0] == "sync datasets" and beats.count("sync datasets") >= 4


async def test_keep_spares_a_raw_cache_whose_lock_a_job_holds(acts, keeper_server):
    import time

    from mia_core.fs import dir_bytes
    from mia_data.jobspec import lock_dir

    registry = keeper_server
    catalog_not_due(acts)
    cache = acts.cfg.paths.raw_root / "rec"
    cache.mkdir()
    (cache / "a.tif").write_bytes(b"x" * 100)
    registry.put_asset("raw", "rec", {"last_used": time.time() - 49 * 3600})
    acts.cfg.quota.raw_bytes, acts.cfg.quota.headroom_bytes = 1, 0
    locks = lock_dir(acts.cfg)
    assert locks.take("raw", "rec", "buffer-7-stage", 900)  # a buffer staging from this cache

    for dry_run in (True, False):
        report = await acts.keep(dry_run=dry_run)
        assert report.evicted == [] and report.locked == ["raw rec (buffer-7-stage)"] and cache.is_dir()
    assert report.errors == [] and report.over_quota == {"raw": dir_bytes(cache) - 1}
    locks.release("raw", "rec", "buffer-7-stage")
    report = await acts.keep()
    assert report.evicted == ["raw rec"] and report.locked == [] and not cache.exists()
    assert registry.asset("raw", "rec")["state"] == "evicted" and locks.holder("raw", "rec") is None


async def test_keep_marks_a_half_removed_artifact_missing(acts, keeper_server, monkeypatch):
    import time

    from mia_buffers import keeper

    registry = keeper_server
    catalog_not_due(acts)
    artifact = staged_artifact(acts, "art", 100)
    registry.put_asset("staged", "art", {"last_used": time.time() - 49 * 3600, "ok": True})
    acts.cfg.quota.staged_bytes, acts.cfg.quota.headroom_bytes = 1, 0

    def broken(path):
        raise OSError("device busy")

    monkeypatch.setattr(keeper.shutil, "rmtree", broken)
    report = await acts.keep()
    assert report.evicted == [] and len(report.errors) == 1 and "removing art" in report.errors[0]
    assert not artifact.exists() and registry.asset("staged", "art")["state"] == "missing"
    monkeypatch.undo()
    assert (await acts.keep()).errors == [] and registry.asset("staged", "art")["state"] == "missing"  # not reused


# -- milestone D: publish by rename, the status board, events, retire, the keeper's lifecycle signals


def staged_into(directory: Path, **report) -> Path:
    """write_stage_json into a directory that may exist already (prepare_stage made it)."""
    if directory.exists():
        directory.rmdir()
    staged = write_stage_json(directory)
    if report:
        (staged / "stage.json").write_text(json.dumps({**json.loads((staged / "stage.json").read_text()), **report}))
    return staged


async def test_read_stage_publishes_by_rename_and_reuses_an_existing_artifact(acts, server):
    await planned_and_bound(acts)
    await acts.prepare_stage(7, ROUND, U937)
    final, building = staged_dir(acts, U937), acts._building_dir(ROUND, U937)
    assert building == final.parent / f".building-{final.name}-{job_key(7, ROUND, U937)}"
    staged_into(building)
    assert (await acts.read_stage(ROUND, U937)).ok
    assert not building.exists() and (final / "stage.json").is_file()
    assert "\n" not in (final / "stage.json").read_text()  # compact: it carries the sampled values
    report = json.loads((final / "stage.json").read_text())
    binding = acts.cfg.paths.state_dir / "rounds" / ROUND / "bindings" / f"{U937}.yaml"
    assert report["artifact"] == {
        "id": final.name,
        "binding_hash": hashlib.sha256(binding.read_bytes()).hexdigest(),
        "levels_key": "p256-l1-2-4",
        "voxel_size_nm": {"y": 45.0718, "x": 45.0718},
        "harness_commit": "h" * 40,
        "env_digest": "d" * 64,
    }
    assert acts.registry.asset("staged", final.name)["path"] == str(final)
    assert (await acts.read_stage(ROUND, U937)).ok  # a retry after the rename reads the published directory
    await acts.prepare_stage(7, ROUND, U937)  # staged again (a race, a lost registry row): the first copy wins
    staged_into(building, volumes=[])
    result = await acts.read_stage(ROUND, U937)
    assert result.ok and result.volumes == 1 and not building.exists()
    assert json.loads((final / "stage.json").read_text())["artifact"]["id"] == final.name
    await acts.prepare_stage(7, ROUND, U937, 1)  # a rebuild of a degraded pyramid: the fresh build replaces it
    staged_into(acts._building_dir(ROUND, U937, 1), volumes=[])
    result = await acts.read_stage(ROUND, U937, 1)
    assert result.ok and result.volumes == 0 and not acts._building_dir(ROUND, U937, 1).exists()
    assert len(list(final.parent.glob(f".building-stale-{final.name}-*"))) == 1  # the broken one, for the sweep


async def test_read_stage_leaves_a_failed_build_for_the_keeper(acts, server):
    await planned_and_bound(acts)
    await acts.prepare_stage(7, ROUND, U937)
    building = acts._building_dir(ROUND, U937)
    staged_into(building, ok=False, errors=["boom"])
    result = await acts.read_stage(ROUND, U937)
    assert not result.ok and result.errors == ["boom"]
    assert (building / "stage.json").is_file() and not staged_dir(acts, U937).exists()
    assert acts.registry.asset("staged", staged_dir(acts, U937).name)["state"] == "partial"
    staged_into(staged_dir(acts, U937), ok=False)  # a failed build where milestone C staged in place
    (building / "stage.json").unlink()
    staged_into(building)
    assert (await acts.read_stage(ROUND, U937)).ok  # the good build replaces it; the failed one is set aside
    assert json.loads((staged_dir(acts, U937) / "stage.json").read_text())["ok"] is True
    [aside] = staged_dir(acts, U937).parent.glob(".building-stale-*")
    assert json.loads((aside / "stage.json").read_text())["ok"] is False


async def test_set_stage_writes_the_event(acts, server):
    import httpx

    state = BufferState(7, round_id=ROUND)
    for stage in ("planning", "plan-review", "sourcing", "new", "ready"):  # `new` has no event
        await acts.set_stage(7, stage, state, "x" * 300)
    events = httpx.get(f"{server}/buffers/{ROUND}/events").json()["events"]
    assert [event["kind"] for event in events] == ["created", "planned", "approved", "ready"]
    assert {(event["issue"], event["detail"]) for event in events} == {(7, "x" * 200)}
    acts.registry = Registry("http://127.0.0.1:9")
    await acts.set_stage(7, "idle", state)  # buffer-api down: the label still moves
    assert [label["name"] for label in acts.gh.issues[7]["labels"]] == ["buffer:idle"]


async def test_mark_dataset_and_retire(acts, server):
    await planned_and_bound(acts)
    pins = await admitted(acts)
    await acts.create_buffer(ROUND, ROUND)
    [buffer] = acts.registry.buffers()
    plan_text = (acts.cfg.paths.state_dir / "rounds" / ROUND / "plan.json").read_text()
    assert buffer["config"]["plan_hash"] == plan_hash(plan_text)
    assert buffer["config"]["recipe"]["harness_commit"] == pins.harness_commit
    assert acts.registry.request("GET", f"/buffers/{ROUND}/events")["events"] == []  # create_buffer writes none
    await acts.mark_dataset(ROUND, U937, "fetching", job="j1")
    await acts.mark_dataset(ROUND, U937, "exploded")  # refused by the service: logged, not raised
    assert [(row["record"], row["status"], row["job"]) for row in acts.registry.buffer_datasets(ROUND)] == [
        (U937, "fetching", "j1")
    ]
    assert await acts.raw_ready(ROUND, U937) is False  # nothing fetched
    await acts.buffer_event(ROUND, "idle", "superseded")  # a superseded buffer's idle, as the workflow writes it
    assert set(await acts.retire_buffer(ROUND, ROUND)) >= {"refs", "draws"}
    idle, retired = acts.registry.request("GET", f"/buffers/{ROUND}/events")["events"]
    assert (idle["kind"], idle["issue"], idle["detail"]) == ("idle", 7, "superseded")
    assert (retired["kind"], retired["issue"], retired["detail"]) == ("retired", 7, "archive none")
    acts.registry = Registry("http://127.0.0.1:9")
    await acts.mark_dataset(ROUND, U937, "staging")
    await acts.buffer_event(ROUND, "idle")  # best effort: logged, not raised
    with pytest.raises(RegistryError):  # the workflow retries a retire
        await acts.retire_buffer(ROUND, ROUND)


class FakeTemporal:
    """get_workflow_handle(id).signal(name, args=[...]) recorded as (id, name, args); `gone` ids raise not found."""

    def __init__(self, gone=()):
        self.sent, self.gone = [], set(gone)

    def get_workflow_handle(self, workflow_id):
        from temporalio.service import RPCError, RPCStatusCode

        temporal = self

        class Handle:
            async def signal(self, name, *, args):
                if workflow_id in temporal.gone:
                    raise RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")
                temporal.sent.append((workflow_id, name, args))

        return Handle()


async def test_keeper_signals_degraded_idle_and_retire(acts, keeper_server, monkeypatch):
    from test_buffers_api import CFG, body

    registry, staged_root = keeper_server, acts.cfg.paths.staged_root

    def buffer(buffer_id, kinds, volumes=(), **config):
        registry.request("PUT", f"/buffers/{buffer_id}", json={**CFG, **config})
        for volume_id, present in volumes:
            path = staged_root / buffer_id / volume_id
            if present:
                path.mkdir(parents=True)
            volume = {**body(), "path": str(path), "record": volume_id.split("/")[0]}
            registry.request("PUT", f"/buffers/{buffer_id}/volumes/{volume_id.replace('/', '%2F')}", json=volume)
        for kind in kinds:
            registry.event(buffer_id, kind)

    buffer("i2-e3", ["created", "ready"], [("rec-a/v0.zarr", False), ("rec-b/v0.zarr", False), ("rec-c/v0", True)])
    buffer("i4-e5", ["ready"], [("rec-d/v0", True)], idle_hours=1)  # no consumer for over an hour
    buffer("i6-e7", ["ready", "idle"], retire_after_days=0.01)  # idle for over 864 s
    buffer("i8-e9", ["ready", "idle"], retire_after_days=0.01)  # its workflow is gone
    buffer("i10-e11", ["approved"], [("rec-e/v0", False)])  # still sourcing: not the keeper's to judge
    buffer("hand", ["ready", "idle"], retire_after_days=0.01)  # registered by hand: no workflow
    report = KeeperReport()
    now = time.time() + 2 * 3600
    acts.client = FakeTemporal(gone={acts.cfg.workflow_id(8)})

    await acts._keeper_buffers(report, now, dry_run=True)
    expected = ["degraded i2-e3 rec-a,rec-b", "idle i4-e5", "retire i6-e7", "retire i8-e9"]
    assert report.signalled == expected and acts.client.sent == [] and report.errors == []

    report = KeeperReport()
    await acts._keeper_buffers(report, now, dry_run=False)
    assert report.signalled == expected[:3] and report.errors == []  # the gone workflow is logged, not reported
    assert acts.client.sent == [
        (
            acts.cfg.workflow_id(2),
            "degraded",
            [DegradedSignal(["rec-a", "rec-b"], "pyramid missing: rec-a/v0.zarr, rec-b/v0.zarr", "i2-e3")],
        ),
        (acts.cfg.workflow_id(4), "idle", [None, "i4-e5"]),
        (acts.cfg.workflow_id(6), "retire", [None, "i6-e7"]),
    ]
    assert await acts.missing_records("i2-e3") == ["rec-a", "rec-b"] and await acts.missing_records("i4-e5") == []
    acts.client = None  # the CLI's `assets sync` builds no Temporal client: it reports what it would signal
    report = KeeperReport()
    await acts._keeper_buffers(report, now, dry_run=False)
    assert report.signalled == expected


async def test_publish_reraises_a_failed_rename_when_nothing_was_published(acts, server, monkeypatch):
    await planned_and_bound(acts)
    await acts.prepare_stage(7, ROUND, U937)
    staged_into(acts._building_dir(ROUND, U937))

    def refuse(src, dst):
        raise PermissionError(13, "read-only file system")

    monkeypatch.setattr(acts_module.os, "rename", refuse)
    with pytest.raises(PermissionError):
        await acts.read_stage(ROUND, U937)
    assert acts._building_dir(ROUND, U937).is_dir() and not staged_dir(acts, U937).exists()


async def test_a_failed_build_beside_a_published_artifact_leaves_its_row(acts, server):
    """Another round published the artifact (this one staged again while the registry was away): this build's
    failure must not mark the published pyramid partial; submit's `staging` mark is undone."""
    await planned_and_bound(acts)
    await acts.prepare_stage(7, ROUND, U937)
    staged_into(acts._building_dir(ROUND, U937))
    assert (await acts.read_stage(ROUND, U937)).ok
    artifact = staged_dir(acts, U937).name
    acts.registry.put_asset("staged", artifact, {"state": "staging", "ok": False, "job": "j9"})  # as submit marks it
    await acts.prepare_stage(7, ROUND, U937)
    staged_into(acts._building_dir(ROUND, U937), ok=False, errors=["boom"])
    assert not (await acts.read_stage(ROUND, U937)).ok
    row = acts.registry.asset("staged", artifact)
    assert (row["state"], row["ok"], row["job"], row["volumes"]) == ("ready", True, None, 1)
    await acts.prepare_stage(7, ROUND, U937, 1)  # a rebuild of a degraded pyramid that fails: partial, as before
    staged_into(acts._building_dir(ROUND, U937, 1), ok=False)
    assert not (await acts.read_stage(ROUND, U937, 1)).ok
    assert acts.registry.asset("staged", artifact)["state"] == "partial"


async def test_keeper_spares_an_old_build_whose_artifact_is_staging(acts, keeper_server):
    registry, record_dir = keeper_server, acts.cfg.paths.staged_root / "p256-l1-2-4" / "rec"
    catalog_not_due(acts)
    stamp = time.time() - 49 * 3600
    builds = {name: record_dir / f".building-{name * 16}-buf-7-i7-e11-stage-rec-0a1b2c3d" for name in "ab"}
    for path in builds.values():
        path.mkdir(parents=True)
        os.utime(path, (stamp, stamp))
    registry.put_asset("staged", "a" * 16, {"state": "staging"})  # its job still runs: no job id, never stale
    registry.put_asset("staged", "b" * 16, {"state": "partial"})
    report = await acts.keep()
    assert report.errors == [] and report.removed_building == [str(builds["b"])]
    assert builds["a"].is_dir() and not builds["b"].exists()


async def test_keeper_watch_survives_a_pre_d_config_and_a_failing_buffer(acts, keeper_server, monkeypatch):
    """A config stored before D has no idle_hours; a buffer whose status fails is an error entry; the buffers
    after both are still watched. An idle buffer a consumer used since it went idle is not retired."""
    from test_buffers_api import CFG, CONSUMER

    registry = keeper_server
    for buffer_id, kinds in (("i2-e3", ["ready"]), ("i4-e5", ["ready"]), ("i6-e7", ["ready", "idle"])):
        registry.request("PUT", f"/buffers/{buffer_id}", json={**CFG, "idle_hours": 1, "retire_after_days": 0.01})
        for kind in kinds:
            registry.event(buffer_id, kind)
    registry.request("POST", "/buffers/i6-e7/consumers", json=CONSUMER)  # after its idle event
    listed, status = registry.buffers, registry.status

    def pre_d(self=None):
        rows = listed()
        for key in ("idle_hours", "retire_after_days", "recipe", "plan_hash", "auto_approve"):
            rows[0]["config"].pop(key)
        return rows

    def failing(buffer_id):
        if buffer_id == "i4-e5":
            raise RegistryError(500, "boom")
        return status(buffer_id)

    monkeypatch.setattr(registry, "buffers", pre_d)
    monkeypatch.setattr(registry, "status", failing)
    acts.client = FakeTemporal()
    report = KeeperReport()
    await acts._keeper_buffers(report, time.time() + 30 * 86400, dry_run=False)
    assert report.signalled == ["idle i2-e3"]  # 72 h by default; i6-e7 was used after it went idle
    assert report.errors == ["buffers: i4-e5: boom"]


# -- milestone E: the preparation request

PREP_ROUND = "p8-e21"


def prep_body(**fields) -> str:
    spec = {"schema_version": 1, "kind": "preparation", "catalog": {"repo": "o/catalog", "ref": REF}, **fields}
    return f"Please prepare these.\n\n```yaml\n{yaml.safe_dump(spec, sort_keys=False)}```\n"


def prep_issue(acts, number: int = 8, **fields) -> None:
    acts.gh.issues[number] = {"number": number, "body": prep_body(**fields), "labels": [{"name": "prep:requested"}]}


async def test_prep_admit_and_nudge_report_prepared_records(acts):
    prep_issue(acts, records=[U937, BCSS, U937])
    pins = await acts.prep_admit(8, "21")
    assert pins == PrepPins(PREP_ROUND, REF, [U937, BCSS], 14, issue=8)
    assert await acts.prep_admit(8, "21") == pins  # a retry: the same pins
    round_dir = acts.cfg.paths.state_dir / "rounds" / PREP_ROUND
    assert {row["id"] for row in json.loads((round_dir / "plan.json").read_text())["rows"]} == {U937, BCSS}
    assert acts._record(PREP_ROUND, U937)["id"] == U937  # nudge_dataset reads the record through the round dir
    gh = with_datasets(acts)
    outcome = await acts.prep_nudge(PREP_ROUND, U937)  # the fixture's registry knows every record as prepared
    assert outcome == PrepOutcome(U937, "prepared") and gh.created == [] and gh.issues == {}
    await acts.comment_prep(8, PREP_ROUND, [outcome, PrepOutcome(BCSS, "opened", 100)])
    await acts.comment_prep(8, PREP_ROUND, [outcome])  # once per round
    (comment,) = acts.gh.comments[8]
    assert "| record | outcome | issue |" in comment["body"] and f"| `{U937}` | prepared |  |" in comment["body"]
    assert f"| `{BCSS}` | opened | o/data#100 |" in comment["body"]


async def test_prep_admit_refuses_unknown_records(acts):
    prep_issue(acts, records=[U937, "no-such-record"])
    with pytest.raises(ApplicationError, match="records not in the catalog at .*no-such-record") as error:
        await acts.prep_admit(8, "21")
    assert error.value.non_retryable and U937 not in str(error.value)


async def test_prep_admit_refuses_too_many_records(acts, monkeypatch):
    monkeypatch.setattr(acts_module, "MAX_PREP_RECORDS", 1)
    prep_issue(acts, records=[U937, BCSS])
    with pytest.raises(ApplicationError, match="names 2 records, more than 1") as error:
        await acts.prep_admit(8, "21")
    assert error.value.non_retryable


async def test_prep_admit_selects_records_with_a_select_block(acts):
    select = yaml.safe_load(spec_text())["select"]
    prep_issue(acts, select=select, timeout_days=3)
    pins = await acts.prep_admit(8, "21")
    assert sorted(pins.records) == sorted([BCSS, U937]) and pins.timeout_days == 3


async def test_prep_nudge_opens_once_for_two_requests(acts):
    acts.registry.put_dataset(U937, state="in-progress")  # not prepared: mia-data has work to do
    prep_issue(acts, 8, records=[U937])
    prep_issue(acts, 9, records=[U937])
    first, second = await acts.prep_admit(8, "21"), await acts.prep_admit(9, "22")
    gh = with_datasets(acts)
    assert await acts.prep_nudge(first.round_id, U937) == PrepOutcome(U937, "opened", 100)
    assert await acts.prep_nudge(second.round_id, U937) == PrepOutcome(U937, "in-progress", 100)
    assert len(gh.created) == 1 and [x["name"] for x in gh.issues[100]["labels"]].count("data:go") == 1


async def test_prep_nudge_maps_the_dataset_issue(acts):
    for record in (U937, BCSS):
        acts.registry.put_dataset(record, state="in-progress")
    prep_issue(acts, records=[U937, BCSS])
    await acts.prep_admit(8, "21")
    with_datasets(acts, {5: (U937, ["data:done"]), 6: (BCSS, ["data:new"])})
    done = await acts.prep_nudge(PREP_ROUND, U937)
    assert (done.outcome, done.issue) == ("done-unpublished", 5) and "binding.yaml" in done.note
    assert await acts.prep_nudge(PREP_ROUND, BCSS) == PrepOutcome(BCSS, "nudged", 6)
    acts.registry.put_dataset(BCSS, state="ineligible", issue=6)  # settled in the registry: its row names the issue
    assert await acts.prep_nudge(PREP_ROUND, BCSS) == PrepOutcome(BCSS, "ineligible", 6)


async def test_prep_status_reads_the_registry(acts):
    acts.registry.put_dataset(U937, state="needs-human")
    assert await acts.prep_status([U937, BCSS, "never-seen"]) == {
        U937: "needs-human",
        BCSS: "prepared",
        "never-seen": "unprepared",
    }
    acts.registry = Registry("http://127.0.0.1:9", "t")  # nothing listens: the outage raises
    with pytest.raises(RegistryError):
        await acts.prep_status([U937])


async def test_set_prep_stage_swaps_label_and_comments_once(acts):
    prep_issue(acts)
    state = PrepState(8, round_id=PREP_ROUND, stage="requested")
    await acts.set_prep_stage(8, "done", state, "every record is prepared")
    await acts.set_prep_stage(8, "done", state, "every record is prepared")
    assert [x["name"] for x in acts.gh.issues[8]["labels"]] == ["prep:done"]
    (comment,) = acts.gh.comments[8]
    assert "`prep:done`" in comment["body"] and "every record is prepared" in comment["body"]
    with pytest.raises(ApplicationError):
        await acts.set_prep_stage(8, "new", state, "")
