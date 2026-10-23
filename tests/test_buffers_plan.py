import copy
from pathlib import Path

import pytest
import yaml

from mia_buffers.plan import Fit, Plan, build_plan, fit, positions, split_counts, weights
from mia_buffers.spec import Spec, load_spec
from mia_buffers.specmap import Facts, facts

FIXTURES = Path(__file__).parent / "fixtures" / "buffers"
CATALOG = FIXTURES / "catalog"
BCSS = "github-bcss-breast-cancer-semantic-segmentation"
U937 = "zenodo-20797339-u937-edu-h2ax-dapi-confocal"
MITOLAB = "empiar-11037-cem-mitolab-mitochondria"
HEMIBRAIN = "janelia-hemibrain-v1-2-fib-sem-connectome"
ONE_CH, DUP = "synthetic-em-2d-1ch", "synthetic-em-2d-dup"


def records() -> list[tuple[str, dict]]:
    paths = sorted(CATALOG.glob("datasets/**/*.yaml"))
    return [(str(p.relative_to(CATALOG)), yaml.safe_load(p.read_text())) for p in paths]


F: dict[str, Facts] = {f.id: f for f in (facts(path, record) for path, record in records())}


def spec_with(base: Spec, **edits) -> Spec:
    """A copy of `base` with dotted-path edits applied to its dict form, revalidated."""
    data = base.model_dump(by_alias=True, exclude={"warnings"})
    data = copy.deepcopy(data)
    for dotted, value in edits.items():
        node = data
        *parents, leaf = dotted.split(".")
        for key in parents:
            node = node[key]
        node[leaf] = value
    return Spec.model_validate(data)


@pytest.fixture
def spec() -> Spec:
    return load_spec(FIXTURES / "spec.yaml")


@pytest.fixture
def spec_2d_mixed(spec) -> Spec:  # 2D only, any annotation source: BCSS, U937 and the 1-channel record admitted
    return spec_with(spec, **{"select.where.dims": {"in": ["2D"]}, "select.where.annotations.source": "any"})


@pytest.fixture
def spec_2d(spec_2d_mixed) -> Spec:
    return spec_with(
        spec_2d_mixed, **{"select.exclude.ids": [ONE_CH], "select.exclude.reasons": {ONE_CH: "one channel"}}
    )


def test_fit_rules(spec):
    assert fit(F[MITOLAB], spec).reason == "footprint 1024×1024 exceeds 224×224"
    assert fit(F[BCSS], spec) == Fit(
        True, "fits (shape varies: checked against the representative item)", (1024, 1024), True
    )
    assert fit(F[DUP], spec).reason == "no array shape"
    assert fit(F[U937], spec).reason == "fits"
    assert fit(F[HEMIBRAIN], spec).reason == "fits"


def test_mitolab_small_patch_still_needs_voxel_size(spec):
    small = spec_with(spec, **{"sampling.patch.2D": {"y": 64, "x": 64}, "sampling.levels": [1, 2]})
    assert fit(F[MITOLAB], small).reason == "voxel size unknown"


def test_positions_and_weights(spec_2d):
    assert positions(F[U937], spec_2d) == 100 and positions(F[BCSS], spec_2d) == 906
    plan = build_plan(spec_2d, "h", None, records())
    rows = [plan.rows_by_id[U937], plan.rows_by_id[BCSS]]
    w = weights(rows, ["modality", "dataset"], 0.5)
    assert w == pytest.approx({U937: 10 / (10 + 906**0.5), BCSS: 906**0.5 / (10 + 906**0.5)}, rel=1e-3)
    assert weights(rows, ["dataset"], 0.0) == {U937: 0.5, BCSS: 0.5}
    assert weights(rows, ["dataset"], 1.0) == pytest.approx({U937: 100 / 1006, BCSS: 906 / 1006})


def test_split_counts():
    assert split_counts(25, {"train": 0.8, "val": 0.1, "test": 0.1}) == {"train": 20, "val": 3, "test": 2}
    assert split_counts(151, {"train": 0.8, "val": 0.1, "test": 0.1}) == {"train": 121, "val": 15, "test": 15}


def test_example_spec_is_invalid_for_mixed_dims(spec):
    plan = build_plan(spec_with(spec, **{"select.where.annotations.source": "any"}), "h", None, records())
    assert "one buffer serves one dimensionality; pool has ['2D', '3D']: narrow select.where.dims" in plan.errors
    assert not plan.valid


def test_valid_2d_plan(spec_2d):
    plan = build_plan(spec_2d, "h", None, records())
    assert plan.valid and plan.errors == []
    assert {r.id for r in plan.rows if r.admitted and r.fit is not None and r.fit.ok} == {BCSS, U937}
    assert plan.rows_by_id[ONE_CH].rule == "excluded by id: one channel"
    assert plan.rows_by_id[BCSS].notes == ["download_method other: needs a manual drop (spec v0.4 §3)"]
    assert plan.rows_by_id[U937].splits == {"train": 20, "val": 3, "test": 2}
    assert plan.rows_by_id[U937].weight == pytest.approx(10 / (10 + 906**0.5), rel=1e-3)
    assert plan.rows_by_id[DUP].weight == 0 and plan.rows_by_id[MITOLAB].rule.startswith("largest raw array has 50176")
    assert plan.warnings == [
        "sampling.label_axis is ignored: labels are a separate tensor (spec v0.4 §8.5)",
        "dedup.by content_hash is not available; doi used",
    ]


def test_channel_policy(spec_2d_mixed):
    plan = build_plan(spec_2d_mixed, "h", None, records())
    assert f"patch.c: all needs one channel count; pool has {{1: ['{ONE_CH}'], 3: ['{BCSS}', '{U937}']}}" in plan.errors
    assert build_plan(spec_with(spec_2d_mixed, **{"sampling.patch.c": 1}), "h", None, records()).valid


def test_holdout_goes_to_test(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.holdout_ids": [U937]}), "h", None, records())
    assert plan.rows_by_id[U937].splits == {"train": 0, "val": 0, "test": 25}


def test_no_dataset_admitted(spec):
    plan = build_plan(spec_with(spec, **{"select.paths": ["nowhere/**"]}), "h", None, records())
    assert plan.errors == ["no dataset admitted"]


def test_markdown_has_one_row_per_record_and_errors_first(spec, spec_2d):
    text = build_plan(
        spec_with(spec, **{"select.where.annotations.source": "any"}), "h", "abc", records()
    ).render_markdown()
    assert text.index("## Errors") < text.index("| id |")
    for record_id in F:
        assert text.count(f"| {record_id} |") == 1
    valid = build_plan(spec_2d, "h", None, records())
    assert "## Errors" not in valid.render_markdown() and isinstance(valid, Plan)
    assert valid.to_dict()["rows"][0]["id"] == F[sorted(F, key=lambda i: F[i].path)[0]].id


def test_record_without_technical_is_a_row_not_a_crash(spec_2d):
    row = build_plan(
        spec_with(spec_2d, **{"select.where.license.allow_unknown": True}), "h", None, records()
    ).rows_by_id[DUP]
    assert (
        row.rule == "admitted" and row.fit == Fit(False, "no array shape", (1024, 1024), False) and row.positions == 0
    )
