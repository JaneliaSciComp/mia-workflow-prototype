"""Pure crop sampling: fit, servable volumes, weighted draws (spec v0.5 §2 serving-time rules)."""

import dataclasses
import random

from mia_buffers.sampler import Level, Volume, capacity, draw, draw_frozen, fits, footprint, servable


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


def candidates(count):
    """`count` planned samples on plane 0, as a sparse stage.json lists them."""
    return tuple(
        {"plane": {"z": 0}, "centre_nm": [5120.0 + i, 5120.0], "levels": [1, 2, 4], "patch": 256, "starts": {}}
        for i in range(count)
    )


def test_draw_picks_a_candidate_when_the_volume_has_them():
    planned = {"plane": {"z": 0}, "centre_nm": [5120.0, 5120.0], "levels": [1, 2, 4], "patch": 256}
    planned["starts"] = {"1": [384, 384]}
    volume = dataclasses.replace(vol("a"), samples={"train": (planned,)})
    d = draw(random.Random(0), [volume], "train", patch=256, levels=[1, 2, 4], channels=1)
    assert d is not None and d["centre_nm"] == [5120.0, 5120.0] and d["candidate"] == 0
    assert d["starts"] == {"1": [384, 384]} and d["volume"] == "a" and d["path"] == "/staged/a"
    assert d["channels"] == {"op": "channel", "index": 0}


def test_volume_without_candidates_draws_as_before():
    d = draw(random.Random(0), [vol("a")], "train", patch=256, levels=[1, 2, 4], channels=1)
    assert d is not None and "candidate" not in d and 5120.0 <= d["centre_nm"][0] <= 5120.0


def test_servable_follows_candidates_when_a_volume_has_them():
    no_planes = dataclasses.replace(vol("a", splits={"train": (0,)}), samples={"val": candidates(1)})
    assert [v.id for v in servable([no_planes], "val")] == ["a"]  # candidates, though no plane is in val
    assert servable([no_planes], "train") == []  # samples, none in train: the uniform fallback would miss chunks


def test_draw_frozen_is_without_replacement_and_weighted():
    a = dataclasses.replace(vol("a", weight=0.99, splits={"val": (0,)}), samples={"val": candidates(3)})
    b = dataclasses.replace(vol("b", weight=0.01, splits={"val": (0,)}), samples={"val": candidates(3)})
    used: set[tuple[str, int]] = set()
    out = []
    rng = random.Random(0)
    while (d := draw_frozen(rng, [a, b], "val", used, patch=256, levels=[1, 2, 4], channels=1)) is not None:
        used.add((d["volume"], d["candidate"]))
        out.append(d)
    assert len(out) == 6 and len(used) == 6 and [d["volume"] for d in out[:3]].count("a") >= 2
    assert capacity([a, b], "val") == 6 and capacity([a, b], "train") == 0
    legacy = vol("c", splits={"val": (0,)})  # no samples: always one more, drawn as before
    assert capacity([a, b, legacy], "val") is None
    d = draw_frozen(random.Random(0), [a, b, legacy], "val", used, patch=256, levels=[1, 2, 4], channels=1)
    assert d is not None and d["volume"] == "c" and "candidate" not in d


def test_zero_weight_volumes_are_never_drawn_or_counted():
    """A zero-weight volume's candidates would leave `choices` only zero weights once the others run out."""
    a = dataclasses.replace(vol("a", weight=1.0, splits={"val": (0,)}), samples={"val": candidates(1)})
    b = dataclasses.replace(vol("b", weight=0.0, splits={"val": (0,)}), samples={"val": candidates(2)})
    assert capacity([a, b], "val") == 1 and capacity([b], "val") == 0
    used: set[tuple[str, int]] = set()
    first = draw_frozen(random.Random(0), [a, b], "val", used, patch=256, levels=[1, 2, 4], channels=1)
    assert first is not None and first["volume"] == "a"
    used.add(("a", 0))
    assert draw_frozen(random.Random(0), [a, b], "val", used, patch=256, levels=[1, 2, 4], channels=1) is None
