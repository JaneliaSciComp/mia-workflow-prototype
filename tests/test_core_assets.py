import json

from mia_core.assets import ASSET_STATES, cache_missing, raw_asset, read_files_json, whole_file_url


def test_raw_asset_is_ready_when_every_binding_url_is_cached(tmp_path):
    (tmp_path / "files.json").write_text(
        json.dumps(
            {
                "https://h/a.zip": {"validators": {"size": 7}, "source": "download"},
                "https://h/b.tif": {"validators": {"size": 3}, "source": "drop"},
            }
        )
    )
    (tmp_path / "k1").write_bytes(b"1234567")
    ready = raw_asset(tmp_path, ["https://h/a.zip::m.png", "https://h/b.tif"])
    assert (
        ready["state"] == "ready"
        and ready["bytes"] == 7 + (tmp_path / "files.json").stat().st_size
        and ready["files"]["https://h/b.tif"]["source"] == "drop"
    )
    assert raw_asset(tmp_path, ["https://h/a.zip", "https://h/c.tif"])["state"] == "partial"
    assert raw_asset(tmp_path / "none", [])["state"] == "partial" and read_files_json(tmp_path / "none") == {}
    stream = ["https://h/c.zarr"]  # read in place by the harness: never in files.json, still ready
    assert raw_asset(tmp_path, ["https://h/b.tif", *stream], stream)["state"] == "ready"
    assert cache_missing({}, stream, stream) == (True, []) and cache_missing({}, stream) == (False, stream)
    assert whole_file_url("https://h/a.zip::m.png") == "https://h/a.zip" and "evicted" in ASSET_STATES


def test_untrusted_files_json_is_reduced(tmp_path):
    (tmp_path / "files.json").write_text(
        json.dumps({"u1": {"validators": "x", "source": "s" * 99}, "u2": {"validators": {"size": "big"}}, "u3": 5})
    )
    files = raw_asset(tmp_path, [])["files"]
    assert files == {"u1": {"size": None, "source": "s" * 50}, "u2": {"size": None, "source": None}}
    (tmp_path / "files.json").write_text("[1]")
    assert read_files_json(tmp_path) == {}
