import json

from mia_core.assets import ASSET_STATES, raw_asset, read_files_json, whole_file_url


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
    assert ready["state"] == "ready" and ready["bytes"] > 7 and ready["files"]["https://h/b.tif"]["source"] == "drop"
    assert raw_asset(tmp_path, ["https://h/a.zip", "https://h/c.tif"])["state"] == "partial"
    assert raw_asset(tmp_path / "none", [])["state"] == "partial" and read_files_json(tmp_path / "none") == {}
    assert whole_file_url("https://h/a.zip::m.png") == "https://h/a.zip" and "evicted" in ASSET_STATES
