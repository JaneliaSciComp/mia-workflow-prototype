"""scan_storage: the prepared datasets under storage_root, one per record."""

import json
import logging
import time

from mia_buffers.sync import scan_assets, scan_storage
from mia_core.fs import dir_bytes
from mia_data.publish import reindex


def write_prepared(folder, record, match=True):
    """A dataset folder with a binding.yaml and a manifest.json whose binding sha256 matches it (or not)."""
    folder.mkdir(parents=True)
    (folder / "binding.yaml").write_text(f"record: {record}\nsnippets_commit: 4c1d\n")
    reindex(folder, {})
    if not match:
        manifest = json.loads((folder / "manifest.json").read_text())
        (folder / "manifest.json").write_text(json.dumps({**manifest, "binding": {"sha256": "0" * 64}}))


def test_scan_storage_skips_dotdirs_and_duplicates(tmp_path, caplog):
    write_prepared(tmp_path / "a-first", "rec")
    write_prepared(tmp_path / "b-second", "rec")
    write_prepared(tmp_path / ".tmp-hidden", "hidden")
    write_prepared(tmp_path / "stale", "stale", match=False)
    (tmp_path / "loose.txt").write_text("not a folder")
    with caplog.at_level(logging.WARNING):
        found = scan_storage(tmp_path)
    assert list(found) == ["rec"] and found["rec"]["folder"] == "a-first" and "record" not in found["rec"]
    assert "b-second" in caplog.text


def test_scan_assets_finds_raw_and_artifact_dirs_and_reuses_sizes(tmp_path, caplog):
    raw, staged = tmp_path / "raw", tmp_path / "staged"
    (raw / "rec").mkdir(parents=True)
    (raw / "rec" / "k").write_bytes(b"x" * 8)
    (raw / "rec" / "files.json").write_text(
        json.dumps({"https://h/a": {"validators": {"size": 8}, "source": "download"}})
    )
    (raw / ".tmp").mkdir()
    art = staged / "p256-l1-2" / "rec" / "0123456789abcdef"
    art.mkdir(parents=True)
    (art / "stage.json").write_text(
        json.dumps(
            {
                "ok": True,
                "artifact": {
                    "binding_hash": "b",
                    "harness_commit": "h",
                    "env_digest": "e",
                    "voxel_size_nm": {"y": 4.0, "x": 4.0},
                },
            }
        )
    )
    (art / "v0.zarr").mkdir()
    (art / "v0.zarr" / "c").write_bytes(b"y" * 20)
    (staged / "p256-l1-2" / "rec" / ".building-0123456789abcdef-job").mkdir()
    (staged / "p256-l1-2" / "old").mkdir()
    (staged / "p256-l1-2" / "old" / "stage.json").write_text("{}")  # pre-artifact layout
    with caplog.at_level(logging.INFO):
        found, building, skipped = scan_assets(raw, staged, known={})
    assert "pre-artifact" in caplog.text
    assert set(found) == {("raw", "rec"), ("staged", "0123456789abcdef")}
    raw_row = found[("raw", "rec")]
    assert raw_row["bytes"] == dir_bytes(raw / "rec") and raw_row["path"] == str(raw / "rec")
    assert raw_row["files"] == {"https://h/a": {"size": 8, "source": "download"}}
    staged_row = found[("staged", "0123456789abcdef")]
    assert (
        staged_row.items()
        >= {
            "record": "rec",
            "levels_key": "p256-l1-2",
            "artifact": "0123456789abcdef",
            "binding_hash": "b",
            "harness_commit": "h",
            "env_digest": "e",
            "voxel_size_nm": {"y": 4.0, "x": 4.0},
            "path": str(art),
        }.items()
    )
    assert staged_row["bytes"] == dir_bytes(art) and [p.name for p in building] == [".building-0123456789abcdef-job"]
    assert skipped == [] and ("staged", "old") not in found
    known = {("staged", "0123456789abcdef"): {"bytes": 1, "scanned_at": time.time() + 100}}
    assert scan_assets(raw, staged, known)[0][("staged", "0123456789abcdef")]["bytes"] == 1  # mtime older: reused
    known = {("staged", "0123456789abcdef"): {"bytes": 1, "scanned_at": 0.0}}
    assert scan_assets(raw, staged, known)[0][("staged", "0123456789abcdef")]["bytes"] == dir_bytes(art)
    assert scan_assets(tmp_path / "none", staged, {})[2] == ["raw"]
    (tmp_path / "empty").mkdir()
    assert scan_assets(tmp_path / "empty", staged, {})[2] == ["raw"]
    assert scan_assets(raw, tmp_path / "empty", {})[2] == ["staged"]


def test_scan_assets_reduces_an_odd_stage_json(tmp_path):
    art = tmp_path / "staged" / "lk" / "rec" / "art"
    art.mkdir(parents=True)
    (art / "stage.json").write_text(json.dumps({"artifact": {"binding_hash": ["x"], "voxel_size_nm": {"y": "big"}}}))
    found = scan_assets(tmp_path / "raw", tmp_path / "staged", {})[0]
    row = found[("staged", "art")]
    assert row["binding_hash"] == "['x']" and row["voxel_size_nm"] == {}
