import json
from pathlib import Path

import httpx
import pytest
import respx

from autodev.github import GitHub
from mia_data import catalog
from mia_data.seed import render_issue, seed
from tests.mia_helpers import make_cfg

API = "https://api.github.com"
FIX = Path(__file__).parent / "fixtures" / "mia"
LIC = ["CC0-1.0", "CC-BY-4.0", "CC-BY-3.0", "MIT"]
PATHS = {"bbbc034-hipsc-nuclei-3d": "datasets/3D/fluorescence-other",
         "czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet": "datasets/3D/cryo-ET",
         "hf-pytc-zebrafinch-j0126-ffn-em": "datasets/3D/SBF-SEM"}


def _gh(repo):
    async def nosleep(s): ...
    return GitHub(repo, lambda: "tok", writes_per_second=1000, sleep=nosleep)


@pytest.fixture
def gh_data():
    return _gh("o/data")


@pytest.fixture
def gh_catalog():
    return _gh("o/catalog")


def mock_catalog(ids=None, existing=()):
    """Mock the catalog repo (state files, tree, records) and the data repo's issue list; returns the create route."""
    paths = {i: f"{d}/{i}.yaml" for i, d in PATHS.items() if ids is None or i in ids}
    paths["synthetic-gpl"] = "datasets/3D/EM/synthetic-gpl.yaml"
    if ids is not None and "synthetic-gpl" not in ids:
        del paths["synthetic-gpl"]
    for f in ("downloads", "downloads-native"):
        respx.get(f"{API}/repos/o/catalog/contents/state/{f}.json", params={"ref": "state"}).respond(
            200, content=(FIX / f"{f}.json").read_bytes())
    respx.get(f"{API}/repos/o/catalog/git/trees/main").respond(200, json={"tree": [
        {"type": "blob", "path": p} for p in paths.values()] + [{"type": "tree", "path": "datasets"}]})
    for i, p in paths.items():
        respx.get(f"{API}/repos/o/catalog/contents/{p}", params={"ref": "main"}).respond(
            200, content=(FIX / "records" / f"{i}.yaml").read_bytes())
    respx.get(f"{API}/repos/o/data/issues").respond(200, json=[{"body": f"<!-- mia-data:record={i} -->"} for i in existing])
    return respx.post(f"{API}/repos/o/data/issues").respond(201, json={"number": 1})


def bodies(route):
    return [json.loads(c.request.content) for c in route.calls]


@respx.mock
async def test_three_fixtures_make_three_issues(tmp_path, gh_data, gh_catalog):
    route = mock_catalog(ids=set(PATHS))
    assert len(await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)) == 3
    by_id = {b["title"].split(":")[0]: b for b in bodies(route)}
    b = by_id["bbbc034-hipsc-nuclei-3d"]
    assert b["title"] == "bbbc034-hipsc-nuclei-3d: hiPSC colony 3D nuclei segmentation (fluorescence)"
    assert b["body"].startswith("<!-- mia-data:record=bbbc034-hipsc-nuclei-3d -->")
    assert set(b["labels"]) == {"data:new", "modality:fluorescence-other", "has-labels"}
    assert "native" in b["body"] and "tensorswitch" in b["body"] and "[52, 1024, 1024]" in b["body"]
    assert "blob/main/datasets/3D/fluorescence-other/bbbc034" in b["body"]
    assert "data:new" in by_id["czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet"]["labels"]


@respx.mock
async def test_second_run_creates_none(tmp_path, gh_data, gh_catalog):
    route = mock_catalog(ids=set(PATHS), existing=[*PATHS])
    assert await seed(make_cfg(tmp_path), gh_data, gh_catalog, False) == [] and route.call_count == 0


@respx.mock
async def test_dry_run_creates_nothing(tmp_path, gh_data, gh_catalog, capsys):
    route = mock_catalog(ids=set(PATHS))
    assert len(await seed(make_cfg(tmp_path), gh_data, gh_catalog, True)) == 3 and route.call_count == 0
    assert "would create" in capsys.readouterr().out


@respx.mock
async def test_record_missing_from_main_is_skipped(tmp_path, gh_data, gh_catalog, caplog):
    route = mock_catalog(ids=set(PATHS) - {"hf-pytc-zebrafinch-j0126-ffn-em"})
    assert len(await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)) == 2
    assert "hf-pytc-zebrafinch-j0126-ffn-em" in caplog.text and route.call_count == 2


@respx.mock
async def test_maybe_small_and_licence_in_body(tmp_path, gh_data, gh_catalog):
    route = mock_catalog(ids=set(PATHS))
    await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)
    by_id = {b["title"].split(":")[0]: b for b in bodies(route)}
    assert "maybe-small" in by_id["hf-pytc-zebrafinch-j0126-ffn-em"]["labels"]
    assert "maybe-small" not in by_id["bbbc034-hipsc-nuclei-3d"]["labels"]
    assert "Licence: MIT" in by_id["hf-pytc-zebrafinch-j0126-ffn-em"]["body"]


@respx.mock
async def test_gpl_is_ineligible(tmp_path, gh_data, gh_catalog):
    route = mock_catalog(ids={"synthetic-gpl"})
    await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)
    (b,) = bodies(route)
    assert "data:ineligible" in b["labels"] and "licence-excluded" in b["labels"] and "data:new" not in b["labels"]
    assert "GPL-3.0" in b["body"] and "not in `crops.licences`" in b["body"]


@respx.mock
async def test_downloaded_union_keeps_newest_and_both_downloaders(gh_catalog):
    mock_catalog()
    d = await catalog.downloaded(gh_catalog)
    e = d["bbbc034-hipsc-nuclei-3d"]
    assert e["downloaders"] == ["tensorswitch", "native"]
    ats = [json.loads((FIX / f).read_text())["downloaded"]["bbbc034-hipsc-nuclei-3d"]["at"]
           for f in ("downloads.json", "downloads-native.json")]
    assert e["at"] == max(ats)
    assert d["czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet"]["downloaders"] == ["tensorswitch"]


def test_shape_without_axes_uses_last_two_dims():
    rec = {"id": "x", "title": "t", "license": {"spdx": "MIT"}, "technical": {"arrays": [
        {"role": "raw", "axes": None, "shape": [10, 512, 512], "dtype": "uint8"}]}}
    entry = {"record_commit": "a" * 40, "dataset_dir": "/d", "downloaders": ["native"]}
    assert "maybe-small" not in render_issue(rec, entry, LIC)[2]
