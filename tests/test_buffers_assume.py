"""Baked-in guesses for voxel size and channel recipes (spec v0.5 §2, §9.8)."""

from mia_buffers.assume import FAMILY_NM, VOXEL_NM, channel_recipes, voxel_size
from mia_data.catalog import FAMILIES
from tests.test_buffers_plan import BCSS, MITOLAB, U937, F


def test_every_catalog_modality_has_a_voxel_guess():
    for family, members in FAMILIES.items():
        for modality in members:
            assert (family, modality) in VOXEL_NM or family in FAMILY_NM, (family, modality)


def test_documented_axes_win_and_missing_axes_are_assumed():
    assert voxel_size(F[U937]) == ({"x": 45.0718, "y": 45.0718}, [])  # documented 2D record untouched
    guessed, assumed = voxel_size(F[MITOLAB])  # FIB-SEM 2D with voxel_size_nm null
    assert guessed == {"x": 8.0, "y": 8.0} and assumed == ["x", "y"]


def test_3d_record_missing_only_z_gets_the_modality_z():
    import dataclasses

    hemibrain = F["janelia-hemibrain-v1-2-fib-sem-connectome"]
    partial = dataclasses.replace(hemibrain, voxel_size_nm={"x": 8, "y": 8, "z": None})
    assert voxel_size(partial) == ({"x": 8, "y": 8, "z": 8.0}, ["z"])


def test_unknown_modality_falls_back_to_the_family_then_other():
    import dataclasses

    odd = dataclasses.replace(F[MITOLAB], modalities=["holography"], family="other", voxel_size_nm={})
    assert voxel_size(odd) == ({"x": 300.0, "y": 300.0}, ["x", "y"])


def test_channel_recipes():
    assert channel_recipes(1, "em", "FIB-SEM") == {
        "gray": {"op": "channel", "index": 0},
        "rgb": {"op": "replicate"},
    }
    assert channel_recipes(3, "lm", "histology-WSI") == {"gray": {"op": "luminance"}, "rgb": {"op": "rgb"}}
    assert channel_recipes(4, "lm", "brightfield") == {"gray": {"op": "luminance"}, "rgb": {"op": "rgb"}}
    assert channel_recipes(3, "lm", "confocal") == {"gray": {"op": "mean"}, "rgb": {"op": "additive"}}
    assert channel_recipes(2, "lm", "widefield") == {"gray": {"op": "mean"}, "rgb": {"op": "additive"}}
    assert channel_recipes(3, "other", "other") == {"gray": {"op": "luminance"}, "rgb": {"op": "rgb"}}
    assert channel_recipes(5, "other", "other") == {"gray": {"op": "mean"}, "rgb": {"op": "additive"}}
    assert channel_recipes(3, F[BCSS].family, F[BCSS].modality_group)["rgb"] == {"op": "rgb"}
