"""scan_storage: the prepared datasets under storage_root, one per record."""

import json
import logging

from mia_buffers.sync import scan_storage
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
