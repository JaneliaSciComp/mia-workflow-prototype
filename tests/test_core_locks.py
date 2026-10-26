import json

import pytest

from mia_core.locks import Holder, LockDir


def clock(start=1000.0):
    state = {"t": start}
    return (lambda: state["t"]), (lambda dt: state.__setitem__("t", state["t"] + dt))


def test_take_is_exclusive_until_released(tmp_path):
    now, _ = clock()
    locks = LockDir(tmp_path, now)
    assert locks.take("raw", "rec-1", "mia-data i1-e1 smoke", 900)
    assert not locks.take("raw", "rec-1", "mia-buffers i2-e2 rec-1", 900)
    holder = locks.holder("raw", "rec-1")
    assert holder == Holder("raw", "rec-1", "mia-data i1-e1 smoke", 1000.0, 1000.0, 900, False)
    assert json.loads((tmp_path / "raw" / "rec-1").read_text())["owner"] == "mia-data i1-e1 smoke"
    assert locks.release("raw", "rec-1", "mia-data i1-e1 smoke") and locks.holder("raw", "rec-1") is None
    assert locks.take("raw", "rec-1", "mia-buffers i2-e2 rec-1", 900)


def test_stale_lock_is_renamed_aside_and_taken(tmp_path):
    now, tick = clock()
    locks = LockDir(tmp_path, now)
    assert locks.take("raw", "rec", "old", 900)
    tick(899)
    assert not locks.take("raw", "rec", "new", 900)  # heartbeat 899 s old: still fresh
    tick(2)
    assert locks.holder("raw", "rec").stale and locks.take("raw", "rec", "new", 900)
    assert locks.holder("raw", "rec").owner == "new"
    assert [p.name for p in (tmp_path / "raw").iterdir() if ".stale-" in p.name] == ["rec.stale-1901"]
    assert [h.key for h in locks.holders()] == ["rec"]  # the stale copy is not a holder


def test_heartbeat_and_release_check_the_owner(tmp_path):
    now, tick = clock()
    locks = LockDir(tmp_path, now)
    locks.take("raw", "rec", "old", 900)
    tick(1000)
    locks.take("raw", "rec", "new", 900)
    assert not locks.heartbeat("raw", "rec", "old") and not locks.release("raw", "rec", "old")
    assert locks.holder("raw", "rec").owner == "new"
    tick(10)
    assert locks.heartbeat("raw", "rec", "new") and locks.holder("raw", "rec").heartbeat_at == 2010.0


def test_corrupt_lock_file_is_stale(tmp_path):
    locks = LockDir(tmp_path)
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "rec").write_text("")  # crash between create and write
    assert locks.holder("raw", "rec").stale and locks.take("raw", "rec", "me", 900)


def test_names_are_validated(tmp_path):
    with pytest.raises(ValueError):
        LockDir(tmp_path).take("raw", "../etc", "me", 900)


def test_hold_blocks_then_times_out(tmp_path, monkeypatch):
    now, tick = clock()
    locks = LockDir(tmp_path, now)
    locks.take("snapshot", "abc", "other", 900)
    monkeypatch.setattr("mia_core.locks.time.sleep", lambda s: tick(s))
    with pytest.raises(TimeoutError, match="held by other"):
        with locks.hold("snapshot", "abc", "me", 900, timeout_s=10):
            pass
    locks.release("snapshot", "abc", "other")
    with locks.hold("snapshot", "abc", "me", 900, timeout_s=10):
        assert locks.holder("snapshot", "abc").owner == "me"
    assert locks.holder("snapshot", "abc") is None
