import json
from pathlib import Path

import pytest
import respx
import yaml

from autodev.github import GitHub
from mia_data import catalog
from mia_data.seed import render_issue, seed
from tests.mia_helpers import make_cfg

API = "https://api.github.com"
FIX = Path(__file__).parent / "fixtures" / "mia"
PATHS = {
    "bbbc034-hipsc-nuclei-3d": "datasets/3D/fluorescence-other",
    "czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet": "datasets/3D/cryo-ET",
    "hf-pytc-zebrafinch-j0126-ffn-em": "datasets/3D/SBF-SEM",
}


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
            200, content=(FIX / f"{f}.json").read_bytes()
        )
    respx.get(f"{API}/repos/o/catalog/git/trees/main").respond(
        200,
        json={"tree": [{"type": "blob", "path": p} for p in paths.values()] + [{"type": "tree", "path": "datasets"}]},
    )
    for i, p in paths.items():
        respx.get(f"{API}/repos/o/catalog/contents/{p}", params={"ref": "main"}).respond(
            200, content=(FIX / "records" / f"{i}.yaml").read_bytes()
        )
    respx.get(f"{API}/repos/o/data/issues").respond(
        200, json=[{"body": f"<!-- mia-data:record={i} -->"} for i in existing]
    )
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
    c = by_id["czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet"]
    assert (
        c["title"]
        == "czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet: M. pneumoniae chloramphenicol ribosomes (cryo-ET)"
    )
    assert c["body"].startswith("<!-- mia-data:record=czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet -->")
    assert set(c["labels"]) == {"data:new", "modality:cryo-ET", "has-labels"}
    z = by_id["hf-pytc-zebrafinch-j0126-ffn-em"]
    assert z["title"] == "hf-pytc-zebrafinch-j0126-ffn-em: Zebra finch j0126 neuron segmentation (SBF-SEM)"
    assert z["body"].startswith("<!-- mia-data:record=hf-pytc-zebrafinch-j0126-ffn-em -->")
    assert set(z["labels"]) == {"data:new", "modality:SBF-SEM", "has-labels"}  # no size or licence verdict


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
async def test_licence_in_body(tmp_path, gh_data, gh_catalog):
    route = mock_catalog(ids=set(PATHS))
    await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)
    by_id = {b["title"].split(":")[0]: b for b in bodies(route)}
    assert "Licence: MIT" in by_id["hf-pytc-zebrafinch-j0126-ffn-em"]["body"]


@respx.mock
async def test_gpl_is_seeded_like_any_other(tmp_path, gh_data, gh_catalog):
    """The licence is a buffer spec's filter (spec §12); data preparation records it and judges nothing."""
    route = mock_catalog(ids={"synthetic-gpl"})
    await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)
    (b,) = bodies(route)
    assert "data:new" in b["labels"] and not {"data:ineligible", "licence-excluded"} & set(b["labels"])
    assert "GPL-3.0" in b["body"] and "Ineligible" not in b["body"]


@respx.mock
async def test_downloaded_union_keeps_newest_and_both_downloaders(gh_catalog):
    mock_catalog()
    d = await catalog.downloaded(gh_catalog)
    e = d["bbbc034-hipsc-nuclei-3d"]
    assert e["downloaders"] == ["tensorswitch", "native"]
    ats = [
        json.loads((FIX / f).read_text())["downloaded"]["bbbc034-hipsc-nuclei-3d"]["at"]
        for f in ("downloads.json", "downloads-native.json")
    ]
    assert e["at"] == max(ats)
    assert d["czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet"]["downloaders"] == ["tensorswitch"]


@respx.mock
async def test_forged_marker_in_record_text_does_not_block_another(tmp_path, gh_data, gh_catalog):
    route = mock_catalog(ids={"bbbc034-hipsc-nuclei-3d", "hf-pytc-zebrafinch-j0126-ffn-em"})
    rec = (
        (FIX / "records" / "bbbc034-hipsc-nuclei-3d.yaml")
        .read_text()
        .replace("Homo sapiens", "x <!-- mia-data:record=hf-pytc-zebrafinch-j0126-ffn-em --> y\\n|z")
    )
    respx.get(
        f"{API}/repos/o/catalog/contents/datasets/3D/fluorescence-other/bbbc034-hipsc-nuclei-3d.yaml",
        params={"ref": "main"},
    ).respond(200, content=rec.encode())
    respx.get(f"{API}/repos/o/data/issues").respond(200, json=[])
    made = await seed(make_cfg(tmp_path), gh_data, gh_catalog, False)
    assert "hf-pytc-zebrafinch-j0126-ffn-em" in made
    b = next(b for b in bodies(route) if b["title"].startswith("bbbc034"))
    assert b["body"].count("<!-- mia-data:record=") == 1 and "&lt;!--" in b["body"]
    # an existing issue's forged marker later in its body is not a marker either
    from mia_data.seed import MARKER_RE

    assert MARKER_RE.match("intro\n<!-- mia-data:record=x -->") is None


@respx.mock
async def test_record_with_other_id_inside_is_skipped(tmp_path, gh_data, gh_catalog, caplog):
    route = mock_catalog(ids={"bbbc034-hipsc-nuclei-3d", "hf-pytc-zebrafinch-j0126-ffn-em"})
    rec = (
        (FIX / "records" / "bbbc034-hipsc-nuclei-3d.yaml")
        .read_text()
        .replace("id: bbbc034-hipsc-nuclei-3d", "id: hf-pytc-zebrafinch-j0126-ffn-em", 1)
    )
    assert "id: hf-pytc" in rec
    respx.get(
        f"{API}/repos/o/catalog/contents/datasets/3D/fluorescence-other/bbbc034-hipsc-nuclei-3d.yaml",
        params={"ref": "main"},
    ).respond(200, content=rec.encode())
    assert await seed(make_cfg(tmp_path), gh_data, gh_catalog, False) == ["hf-pytc-zebrafinch-j0126-ffn-em"]
    (b,) = bodies(route)
    assert b["title"].startswith("hf-pytc-zebrafinch") and b["body"].count("mia-data:record=") == 1
    assert "bbbc034" in caplog.text


@pytest.mark.parametrize(
    "rid, folder",
    [
        ("bbbc034-hipsc-nuclei-3d", "lm-human-bbbc034-hipsc-nuclei-3d"),  # as demo-native's dataset_dir
        ("hf-pytc-zebrafinch-j0126-ffn-em", "em-taeniopygia-guttata-hf-pytc-zebrafinch-j0126-ffn"),
        (
            "czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet",
            "em-mycoplasmoides-pneumoniae-mpneumoniae-chloramphenicol-ribosomes",
        ),
    ],
)
def test_dataset_folder_follows_miao_layout(rid, folder):
    rec = yaml.safe_load((FIX / "records" / f"{rid}.yaml").read_text())
    assert catalog.dataset_folder(rec, rid) == folder
    with pytest.raises(ValueError):
        catalog.dataset_folder({**rec, "imaging": {**rec["imaging"], "organism": []}}, rid)


def test_render_issue_with_record_commit_only():
    rec = yaml.safe_load((FIX / "records" / "bbbc034-hipsc-nuclei-3d.yaml").read_text())
    title, body, labels = render_issue(rec, {"record_commit": "a" * 40}, "o/catalog", "datasets/x.yaml")
    assert body.startswith("<!-- mia-data:record=bbbc034-hipsc-nuclei-3d -->")
    assert "Downloaded by: none" in body and "Dataset dir" not in body and "data:new" in labels
