"""Pure crop sampling: fit, servable volumes, weighted draws (spec v0.5 §2 serving-time rules)."""

import dataclasses
import random

from mia_buffers.sampler import Level, Volume, draw, fits, footprint, servable


def vol(vid, planes=4, extent=1024, weight=1.0, splits=None):
    lv = [
        Level(
            f,
            (1, planes, extent // f, extent // f),
            {"z": 1.0, "y": 10.0 * f, "x": 10.0 * f},
            {"z": 0.0, "y": 5.0 * (f - 1), "x": 5.0 * (f - 1)},
        )
        for f in (1, 2, 4)
    ]
    return Volume(
        vid,
        f"/staged/{vid}",
        "rec",
        "czyx",
        (1, planes, extent, extent),
        tuple(lv),
        splits or {"train": tuple(range(planes))},
        {},
        weight,
        1,
        {"op": "channel", "index": 0},
        {"y": 10.0, "x": 10.0},
    )


def test_footprint_and_fit():
    assert footprint(256, [1, 2, 4]) == 1024
    assert fits(vol("a"), 256, [1, 2, 4]) is None
    assert fits(vol("a", extent=1000), 256, [1, 2, 4]) == "footprint 1024 exceeds 1000×1000"
    assert vol("a").planes() == 4 and vol("a").extent("x") == 1024


def test_draw_is_in_range_and_weighted():
    volumes = [vol("a", weight=0.9), vol("b", weight=0.1, splits={"val": (0,)})]
    assert [v.id for v in servable(volumes, "train")] == ["a"]
    assert [v.id for v in servable(volumes, "val")] == ["b"]
    rng = random.Random(0)
    d = draw(rng, volumes, "train", patch=256, levels=[1, 2, 4], channels=1)
    assert d is not None
    assert d["volume"] == "a" and d["path"] == "/staged/a" and d["plane"]["z"] in range(4)
    assert d["levels"] == [1, 2, 4] and d["patch"] == 256
    assert 5120.0 <= d["centre_nm"][0] <= 10240.0 - 5120.0 and d["channels"] == {"op": "channel", "index": 0}
    assert draw(rng, volumes, "test", patch=256, levels=[1, 2, 4], channels=1) is None
    every = draw(rng, volumes, "train", patch=256, levels=[1, 2, 4], channels="all")
    assert every is not None and every["channels"] is None
    counts = {"a": 0, "b": 0}
    both = [vol("a", weight=0.9), vol("b", weight=0.1)]
    for _ in range(1000):
        picked = draw(rng, both, "train", patch=256, levels=[1, 2, 4], channels=1)
        assert picked is not None
        counts[picked["volume"]] += 1
    assert 850 < counts["a"] < 950


def test_servable_skips_missing_volumes():
    volumes = [vol("a"), dataclasses.replace(vol("b"), missing=True)]
    assert [v.id for v in servable(volumes, "train")] == ["a"]
    picks = [draw(random.Random(i), volumes, "train", patch=256, levels=[1, 2, 4], channels=1) for i in range(50)]
    assert {pick["volume"] for pick in picks if pick} == {"a"} and all(picks)
