"""mia_buffers.keeper: which assets the cache keeper may evict, and the one place it deletes."""

from mia_buffers.keeper import evict_path, plan_evictions, stale_builds


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


def test_stale_builds():
    rows = [
        {"kind": "raw", "key": "r", "data": {"state": "fetching", "job": "1"}},
        {"kind": "staged", "key": "s", "data": {"state": "staging", "job": "2"}},
        {"kind": "staged", "key": "t", "data": {"state": "staging", "job": "3"}},
    ]
    assert stale_builds(rows, finished=lambda job: job != "3") == [("raw", "r", "partial"), ("staged", "s", "missing")]
