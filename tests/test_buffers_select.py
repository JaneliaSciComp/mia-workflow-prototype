from pathlib import Path

import pytest
import yaml

from mia_buffers.select import Decision, decide_all, matches
from mia_buffers.spec import InFilter, Spec, load_spec
from mia_buffers.specmap import Facts, facts

FIXTURES = Path(__file__).parent / "fixtures" / "buffers"
CATALOG = FIXTURES / "catalog"
BCSS = "github-bcss-breast-cancer-semantic-segmentation"
U937 = "zenodo-20797339-u937-edu-h2ax-dapi-confocal"
MITOLAB = "empiar-11037-cem-mitolab-mitochondria"


def fixture_facts() -> list[Facts]:
    paths = sorted(CATALOG.glob("datasets/**/*.yaml"))
    return [facts(str(p.relative_to(CATALOG)), yaml.safe_load(p.read_text())) for p in paths]


@pytest.fixture
def spec() -> Spec:
    return load_spec(FIXTURES / "spec.yaml")


def by_id(decisions: list[Decision]) -> dict[str, Decision]:
    return {d.id: d for d in decisions}


def test_example_spec_over_fixture_catalog(spec):
    decisions = by_id(decide_all(fixture_facts(), spec.select))
    assert decisions[BCSS].rule == "admitted"
    assert decisions[U937].rule == "annotation source unknown not in ['manual', 'proofread', 'automatic']"
    assert decisions["synthetic-em-2d-dup"].rule == "license unknown and allow_unknown is false"
    assert decisions["synthetic-em-2d-1ch"].rule == "admitted"
    assert decisions["janelia-hemibrain-v1-2-fib-sem-connectome"].rule == "admitted"
    assert decisions[MITOLAB].rule == "largest raw array has 50176 voxels < min_size_voxels 1000000.0"


def test_dedup_keeps_first_by_path(spec):
    all_facts = fixture_facts()
    original = next(f for f in all_facts if f.id == "synthetic-em-2d-1ch")
    copy = Facts(**{**original.__dict__, "id": "synthetic-em-2d-copy", "path": "datasets/2D/TEM/zz-copy.yaml"})
    decisions = by_id(decide_all([*all_facts, copy], spec.select))
    assert decisions["synthetic-em-2d-1ch"].admitted
    assert decisions["synthetic-em-2d-copy"].rule == "dedup: doi 10.1/dup already kept by synthetic-em-2d-1ch"


def test_allow_unknown_license(spec):
    spec.select.where.license.allow_unknown = True
    decisions = by_id(decide_all(fixture_facts(), spec.select))
    assert decisions["synthetic-em-2d-dup"].rule == "dedup: doi 10.1/dup already kept by synthetic-em-2d-1ch"


def test_exclude_by_id_with_reason(spec):
    spec.select.exclude.ids = [BCSS, U937]
    spec.select.exclude.reasons = {BCSS: "too small"}
    decisions = by_id(decide_all(fixture_facts(), spec.select))
    assert decisions[BCSS].rule == "excluded by id: too small"
    assert decisions[U937].rule == "excluded by id: no reason given"


def test_paths_and_dims(spec):
    spec.select.paths = ["datasets/2D/**"]
    spec.select.where.dims = InFilter(**{"in": ["3D"]})
    decisions = by_id(decide_all(fixture_facts(), spec.select))
    assert decisions["janelia-hemibrain-v1-2-fib-sem-connectome"].rule == "path not in select.paths"
    assert decisions[BCSS].rule == "dims 2D not in ['3D']"


def test_matches():
    assert matches("any", []) and matches(InFilter(**{"in": ["a", "b"]}), ["b"])
    assert not matches(InFilter(**{"in": ["a"]}), ["c"])


def test_record_mia_data_cannot_place_is_out_of_the_pool(spec):
    """mia-data names a dataset folder from one modality family and one organism; a record it would refuse must
    leave the plan here, never fail at sourcing (the StarDist nuclei record of the first live run had organism [])."""
    path, record = next(
        (str(p.relative_to(CATALOG)), yaml.safe_load(p.read_text()))
        for p in CATALOG.glob("datasets/**/*.yaml")
        if p.stem == BCSS
    )
    record["imaging"]["organism"] = []
    decision = decide_all([facts(path, record)], spec.select)[0]
    assert not decision.admitted
    assert (
        decision.rule == "no dataset folder for mia-data: modality histology-WSI / organism none (one of each needed)"
    )
