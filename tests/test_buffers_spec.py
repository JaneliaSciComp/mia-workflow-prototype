import copy
import hashlib
from pathlib import Path

import pytest
import yaml

from mia_buffers.spec import Spec, SpecError, load_spec, spec_hash

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
