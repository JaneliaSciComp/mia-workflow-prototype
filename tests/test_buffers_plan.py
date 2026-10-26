import copy
from pathlib import Path

import pytest
import yaml

from mia_buffers.plan import NOT_PREPARED, Fit, Plan, build_plan, fit, positions, split_counts, weights
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
    assert ONE_CH not in plan.rows_by_id and plan.excluded["excluded by id: one channel"] == 1
    assert plan.n_seen == len(records()) and set(plan.rows_by_id) == {BCSS, U937}
    assert plan.rows_by_id[BCSS].notes == ["download_method other: needs a manual drop (spec v0.4 §3)"]
    assert plan.rows_by_id[U937].splits == {"train": 20, "val": 3, "test": 2}
    assert plan.rows_by_id[U937].weight == pytest.approx(10 / (10 + 906**0.5), rel=1e-3)
    assert DUP not in plan.rows_by_id and MITOLAB not in plan.rows_by_id
    assert any(rule.startswith("largest raw array under min_size_voxels") for rule in plan.excluded)
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


def test_markdown_has_one_row_per_admitted_record_and_errors_first(spec, spec_2d):
    plan = build_plan(spec_with(spec, **{"select.where.annotations.source": "any"}), "h", "abc", records())
    text = plan.render_markdown()
    assert text.index("## Errors") < text.index("| id |")
    for record_id in F:
        assert text.count(f"| {record_id} |") == (1 if record_id in plan.rows_by_id else 0)
    assert f"## Excluded by selection ({sum(plan.excluded.values())})" in text
    for rule, count in plan.excluded.items():
        assert f"| {rule} | {count} |" in text
    assert f"({len([r for r in plan.rows if r.in_pool])} in the pool, {len(F)} seen)" in text
    valid = build_plan(spec_2d, "h", None, records())
    assert "## Errors" not in valid.render_markdown() and isinstance(valid, Plan)
    assert [row["id"] for row in valid.to_dict()["rows"]] == [
        row.id for row in sorted(valid.rows, key=lambda r: r.path)
    ]


def test_record_without_technical_is_a_row_not_a_crash(spec_2d):
    row = build_plan(
        spec_with(spec_2d, **{"select.where.license.allow_unknown": True}), "h", None, records()
    ).rows_by_id[DUP]
    assert (
        row.rule == "admitted" and row.fit == Fit(False, "no array shape", (1024, 1024), False) and row.positions == 0
    )


def _record(**overrides) -> dict:
    base = yaml.safe_load((CATALOG / "datasets/2D/TEM/synthetic-em-2d-1ch.yaml").read_text())
    base.update(overrides)
    return base


def test_markdown_escapes_record_text(spec_2d_mixed):
    evil = _record(id="evil | pool | admitted", doi=None, license={"spdx": "x\ny|z", "url": None})
    text = build_plan(spec_2d_mixed, "h", None, [*records(), ("datasets/2D/TEM/zz-evil.yaml", evil)]).render_markdown()
    assert "| license x y\\|z not in allowed | 1 |" in text  # the excluded-rule table escapes the record's text too
    assert "\n| pool |" not in text


def test_malformed_record_is_a_row_not_a_crash(spec_2d_mixed):
    bad = _record(id="bad-arrays", doi=None, technical={"arrays": ["nope"]})
    unreadable = {"id": "bad-yaml", "_unreadable": "ParserError: while parsing"}
    plan = build_plan(
        spec_2d_mixed,
        "h",
        None,
        [*records(), ("datasets/2D/TEM/zz-bad.yaml", bad), ("datasets/2D/TEM/zz-yaml.yaml", unreadable)],
    )
    assert plan.rows_by_id["bad-arrays"].rule.startswith("record unreadable: AttributeError")
    assert plan.rows_by_id["bad-yaml"].rule == "record unreadable: ParserError: while parsing"
    assert plan.valid is False or plan.valid is True  # the plan was built either way
    assert plan.n_seen == len(records()) + 2 and plan.to_dict()["n_seen"] == plan.n_seen


def test_positions_counts_depth_for_3d(spec):
    assert positions(F[HEMIBRAIN], spec) == 1 * (41394 // 32) * (39725 // 1024) * (34427 // 1024)


def test_assumed_voxel_size_admits_mitolab_unless_disabled(spec):
    small = spec_with(spec, **{"sampling.patch.2D": {"y": 64, "x": 64}, "sampling.levels": [1, 2]})
    assert fit(F[MITOLAB], small).reason == (
        "fits (shape varies: checked against the representative item; voxel size assumed for x, y)"
    )
    strict = spec_with(small, **{"sampling.assume_voxel_size": False})
    assert fit(F[MITOLAB], strict).reason == "voxel size unknown"


def test_plan_rows_carry_voxel_size_and_recipe(spec_2d_mixed):
    one = spec_with(spec_2d_mixed, **{"sampling.patch.c": 1})
    plan = build_plan(one, "h", None, records())
    assert plan.valid
    assert plan.rows_by_id[U937].voxel_size_nm == {"x": 45.0718, "y": 45.0718} and plan.rows_by_id[U937].assumed == []
    assert plan.rows_by_id[U937].channel_recipe == {"op": "mean"}
    assert plan.rows_by_id[ONE_CH].channel_recipe == {"op": "channel", "index": 0}
    three = build_plan(spec_with(spec_2d_mixed, **{"sampling.patch.c": 3}), "h", None, records())
    assert three.valid and three.rows_by_id[ONE_CH].channel_recipe == {"op": "replicate"}
    assert three.rows_by_id[BCSS].channel_recipe == {"op": "rgb"}
    two = build_plan(spec_with(spec_2d_mixed, **{"sampling.patch.c": 2}), "h", None, records())
    assert two.errors == [f"patch.c: 2 needs at least 2 channels; ['{ONE_CH}'] have fewer"]
    assert "| 45.0718×45.0718 |" in plan.render_markdown()
    placeable = records()  # the fixture lists four organisms; mia-data places one organism and one modality
    for _, record in placeable:
        if record["id"] == MITOLAB:
            record["imaging"].update(organism=["Mus musculus"], modality=["FIB-SEM"])
    mito = build_plan(
        spec_with(
            one,
            **{"sampling.patch.2D": {"y": 64, "x": 64}, "sampling.levels": [1, 2], "select.where.min_size_voxels": 0},
        ),
        "h",
        None,
        placeable,
    )
    assert "| 8×8 (assumed) |" in mito.render_markdown() and mito.rows_by_id[MITOLAB].assumed == ["x", "y"]


def test_policy_error_lists_are_capped(spec_2d_mixed):
    from mia_buffers.plan import _some

    assert _some(["a", "b"]) == ["a", "b"] and _some([str(i) for i in range(8)]) == [
        "0",
        "1",
        "2",
        "3",
        "4",
        "… 3 more",
    ]


def test_not_prepared_rows_are_out_with_their_rule(spec_2d):
    plan = build_plan(spec_2d, "h", None, records(), prepared={U937})
    rows = plan.rows_by_id
    assert rows[U937].in_pool and rows[BCSS].rule == NOT_PREPARED and not rows[BCSS].in_pool
    assert rows[BCSS].fit is not None and rows[BCSS].fit.ok and rows[BCSS].weight == 0.0 and rows[BCSS].splits == {}
    assert plan.valid and rows[U937].weight == 1.0
    assert "## Not prepared (1)" in plan.render_markdown()
    assert build_plan(spec_2d, "h", None, records()).rows_by_id[BCSS].in_pool  # None: rule not applied
    by_id = {row["id"]: row for row in plan.to_dict({U937: "prepared"})["rows"]}
    assert by_id[U937]["prepared"] == "prepared" and by_id[BCSS]["prepared"] == "unprepared"
    assert "prepared" not in plan.to_dict()["rows"][0]
