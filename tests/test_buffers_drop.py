"""mia_buffers.drop: names for a manual drop, the harness cache key, the files.json manifest."""

import hashlib
import json

from mia_buffers.drop import cache_key, drop_names, merge_manifest, read_manifest


def test_drop_names_prefer_unique_basenames():
    urls = ["https://h/a/x.zip::m.png", "https://drive.google.com/uc?export=download&id=1", "manual://rec/y%20z.zip"]
    assert drop_names(urls) == [
        ("https://h/a/x.zip", "x.zip"),
        ("https://drive.google.com/uc?export=download&id=1", "file-1"),
        ("manual://rec/y%20z.zip", "y z.zip"),
    ]


def test_drop_names_fall_back_on_collision_and_dedupe_urls():
    assert drop_names(["https://h/a/f.tif", "https://h/b/f.tif", "https://h/a/f.tif::m"]) == [
        ("https://h/a/f.tif", "file-0"),
        ("https://h/b/f.tif", "file-1"),
    ]


def test_drop_names_refuse_path_tricks():
    assert [name for _, name in drop_names(["https://h/a/..", "https://h/.hidden"])] == ["file-0", "file-1"]


def test_cache_key_matches_harness_formula():
    assert cache_key("u", {"size": 1, "sha256": "ab"}) == hashlib.sha256(b'u\n{"sha256": "ab", "size": 1}').hexdigest()


def test_manifest_merge_is_atomic_and_additive(tmp_path):
    assert read_manifest(tmp_path) == {}
    merge_manifest(tmp_path, {"u1": {"validators": {"size": 1}, "source": "manual"}})
    merge_manifest(tmp_path, {"u2": {"validators": {"size": 2}, "source": "download"}})
    assert set(read_manifest(tmp_path)) == {"u1", "u2"}
    assert json.loads((tmp_path / "files.json").read_text())["u1"]["source"] == "manual"
    assert [p.name for p in tmp_path.iterdir()] == ["files.json"]
