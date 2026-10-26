"""mia_buffers.keeper: which assets the cache keeper may evict, and the one place it deletes."""

import pytest

from mia_buffers import keeper
from mia_buffers.keeper import evict_path, free_assets, plan_evictions, stale_builds


def test_plan_evictions_skips_referenced_and_held_assets():
    now = 10_000.0
    rows = [
        {"key": "a", "data": {"state": "ready", "bytes": 40, "last_used": 1.0}, "refs": []},
        {"key": "b", "data": {"state": "ready", "bytes": 40, "last_used": 2.0}, "refs": ["buf"]},
        {"key": "c", "data": {"state": "ready", "bytes": 40, "last_used": now - 10}, "refs": []},
        {"key": "d", "data": {"state": "staging", "bytes": 40, "job": "9"}, "refs": []},
    ]
    picked = plan_evictions(
        rows, quota_bytes=100, headroom_bytes=0, hold_s=3600, now=now, in_progress=set(), kind="staged"
    )
    # 160 live → evict a (120) … b referenced, c held, d building; still over, nothing else free
    assert [r["key"] for r in picked] == ["a"]


def test_zero_quota_never_evicts():
    rows = [{"key": "a", "data": {"state": "ready", "bytes": 1e12, "last_used": 0}, "refs": []}]
    assert (
        plan_evictions(rows, quota_bytes=0, headroom_bytes=200e9, hold_s=0, now=1e9, in_progress=set(), kind="raw")
        == []
    )


def test_raw_eviction_spares_a_record_in_progress():
    rows = [{"key": "rec", "data": {"state": "ready", "bytes": 10, "last_used": 0}, "refs": []}]
    assert (
        plan_evictions(rows, quota_bytes=5, headroom_bytes=0, hold_s=0, now=100, in_progress={"rec"}, kind="raw") == []
    )


def test_evict_refuses_paths_outside_the_roots(tmp_path):
    inside, outside = tmp_path / "raw" / "rec", tmp_path / "elsewhere"
    inside.mkdir(parents=True)
    outside.mkdir()
    assert evict_path(inside, (tmp_path / "raw",)) and not inside.exists()
    assert not evict_path(outside, (tmp_path / "raw",)) and outside.exists()
    assert not evict_path(tmp_path / "raw" / ".." / "elsewhere", (tmp_path / "raw",)) and outside.exists()
    assert not evict_path(tmp_path / "raw", (tmp_path / "raw",)) and (tmp_path / "raw").exists()  # never a root


def test_evict_refuses_a_symlink_into_another_asset(tmp_path):
    other = tmp_path / "raw" / "other"
    other.mkdir(parents=True)
    (other / "f").write_text("x")
    (tmp_path / "raw" / "link").symlink_to(other)
    assert not evict_path(tmp_path / "raw" / "link", (tmp_path / "raw",)) and (other / "f").exists()


def test_a_failed_removal_raises_and_leaves_nothing_at_the_path(tmp_path, monkeypatch):
    artifact = tmp_path / "staged" / "lv" / "rec" / "art"
    artifact.mkdir(parents=True)
    (artifact / "stage.json").write_text("{}")

    def broken(path):
        raise OSError("device busy")

    monkeypatch.setattr(keeper.shutil, "rmtree", broken)
    with pytest.raises(OSError, match="device busy"):
        evict_path(artifact, (tmp_path / "staged",))
    assert not artifact.exists()  # moved aside: no scan finds a half-removed artifact to reuse
    [aside] = (tmp_path / "staged" / "lv" / "rec").glob(".building-evicted-art-*")
    assert (aside / "stage.json").is_file()


def test_a_row_without_last_used_counts_as_never_used():
    rows = [
        {"key": "a", "data": {"state": "ready", "bytes": 1, "scanned_at": 99.0}, "refs": []},
        {"key": "b", "data": {"state": "ready", "bytes": 1, "last_used": 99.0}, "refs": []},
    ]
    assert [row["key"] for row in free_assets(rows, hold_s=10, now=100, in_progress=set(), kind="staged")] == ["a"]


def test_stale_builds():
    rows = [
        {"kind": "raw", "key": "r", "data": {"state": "fetching", "job": "1"}},
        {"kind": "staged", "key": "s", "data": {"state": "staging", "job": "2"}},
        {"kind": "staged", "key": "t", "data": {"state": "staging", "job": "3"}},
    ]
    assert stale_builds(rows, finished=lambda job: job != "3") == [("raw", "r", "partial"), ("staged", "s", "missing")]
