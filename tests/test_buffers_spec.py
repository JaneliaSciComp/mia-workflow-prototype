import copy
import hashlib
from pathlib import Path

import pytest
import yaml

from mia_buffers.spec import Spec, SpecError, load_prep_text, load_spec, load_spec_text, spec_hash, spec_text_hash

FIXTURES = Path(__file__).parent / "fixtures" / "buffers"


def test_example_spec_loads():
    spec = load_spec(FIXTURES / "spec.yaml")
    assert spec.name == "microscopy-foundation-pretrain" and spec.sampling.levels == [1, 2, 4]
    assert spec.patch_for("2D") == {"y": 256, "x": 256} and spec.sampling.patch.c == "all"
    assert (
        Spec.model_validate(
            {**spec.model_dump(by_alias=True, exclude={"warnings"}), "sampling": {"patch": {"2D": {"y": 8, "x": 8}}}}
        ).sampling.patch.c
        == 1
    )
    assert spec.select.where.license.in_ == ["CC0-1.0", "CC-BY-3.0", "CC-BY-4.0", "MIT", "BSD-3-Clause"]
    assert spec.sampling.weighting.temperature == 0.5
    assert spec.splits.fractions == {"train": 0.8, "val": 0.1, "test": 0.1}
    assert spec.warnings == ["sampling.label_axis is ignored: labels are a separate tensor (spec v0.4 §8.5)"]


def _merge(base: dict, edit: dict) -> dict:
    out = copy.deepcopy(base)
    for key, value in edit.items():
        out[key] = _merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


@pytest.mark.parametrize(
    "edit,needle",
    [
        ({"sampling": {"weigthing": {}}}, "weigthing"),
        ({"sampling": {"levels": [2, 4]}}, "levels"),
        ({"sampling": {"levels": [1, 1]}}, "levels"),
        ({"splits": {"fractions": {"train": 0.8, "val": 0.1, "test": 0.3}}}, "fractions"),
        ({"catalog": {"ref": "main"}}, "ref"),
        ({"select": {"where": {"sample_type": {"in": ["tissue"]}}}}, "sample_type"),
        ({"sampling": {"patch": {"2D": {"y": 256, "xx": 256}}}}, "xx"),
        ({"sampling": {"patch": {"2D": {"y": 0, "x": 256}}}}, "y"),
    ],
)
def test_refused(tmp_path, edit, needle):
    data = _merge(yaml.safe_load((FIXTURES / "spec.yaml").read_text()), edit)
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(SpecError, match=needle):
        load_spec(path)


def test_spec_hash_is_file_sha256():
    path = FIXTURES / "spec.yaml"
    assert spec_hash(path) == hashlib.sha256(path.read_bytes()).hexdigest()


def test_ready_and_sourcing_defaults():
    spec = load_spec(FIXTURES / "spec.yaml")
    assert spec.ready.min_fraction == 1.0 and spec.sourcing.dataset_timeout_days == 14


@pytest.mark.parametrize("edit", [{"ready": {"min_fraction": 0}}, {"ready": {"min_fraction": 1.5}}])
def test_ready_fraction_bounds(tmp_path, edit):
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(_merge(yaml.safe_load((FIXTURES / "spec.yaml").read_text()), edit)))
    with pytest.raises(SpecError, match="min_fraction"):
        load_spec(path)


def test_load_spec_text_matches_load_spec():
    path = FIXTURES / "spec.yaml"
    assert load_spec_text(path.read_text()) == load_spec(path)
    assert spec_text_hash(path.read_text()) == spec_hash(path)


def test_lifecycle_fields_and_defaults(tmp_path):
    spec = load_spec(FIXTURES / "spec.yaml")
    assert spec.plan.auto_approve is False and spec.sourcing.unprepared == "skip"
    assert spec.idle.hours == 72 and spec.retire_after_days == 14 and spec.health.max_empty_fraction == 0.05
    path = tmp_path / "spec.yaml"
    path.write_text(
        yaml.safe_dump(
            _merge(yaml.safe_load((FIXTURES / "spec.yaml").read_text()), {"sourcing": {"unprepared": "request"}})
        )
    )
    with pytest.raises(SpecError, match="unprepared"):
        load_spec(path)


# -- the preparation request (milestone E)

PREP_SELECT = {"paths": ["datasets/**"], "where": {"license": {"in": ["CC0-1.0"]}}}


@pytest.mark.parametrize(
    "body",
    [{"records": ["a", "b"]}, {"select": PREP_SELECT}],
)
def test_prep_spec_takes_records_or_select(body):
    text = yaml.safe_dump(
        {"schema_version": 1, "kind": "preparation", "catalog": {"repo": "o/c", "ref": "c" * 40}, **body}
    )
    spec = load_prep_text(text)
    assert spec.timeout_days == 14 and spec.max_records == 25 and (spec.records == ["a", "b"]) == ("records" in body)
    assert (spec.select is not None) == ("select" in body)


@pytest.mark.parametrize(
    "body,needle",
    [
        ({"records": ["a"], "select": PREP_SELECT}, "exactly one"),
        ({}, "exactly one"),
        ({"records": []}, "exactly one"),
        ({"records": ["a"], "kind": "buffer"}, "kind"),
        ({"records": ["a"], "timeout_days": 0}, "timeout_days"),
        ({"records": ["a"], "max_records": 0}, "max_records"),
        ({"records": ["a"], "max_records": 201}, "max_records"),
        ({"records": ["a"], "name": "x"}, "name"),
    ],
)
def test_prep_spec_refused(body, needle):
    data = {"schema_version": 1, "kind": "preparation", "catalog": {"repo": "o/c", "ref": "c" * 40}, **body}
    with pytest.raises(SpecError, match=needle):
        load_prep_text(yaml.safe_dump(data))
