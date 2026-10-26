"""The SQLite ledger: buffers, volumes, consumers, draws and frozen lists."""

import dataclasses
import sqlite3
import threading
import time

import pytest

from mia_buffers.ledger import Ledger
from tests.test_buffers_sampler import vol

INFO = {"name": "n", "host": "h", "pid": 1, "rank": 0, "split": "train", "mode": "local"}


def test_buffer_volume_roundtrip_and_upsert(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.buffer("b") is None
    led.put_buffer("b", {"levels": [1, 2, 4]})
    led.put_volume("b", vol("a"))
    led.put_volume("b", vol("a", weight=0.5))
    assert led.buffer("b") == {"levels": [1, 2, 4]} and [v.weight for v in led.volumes("b")] == [0.5]
    assert led.volumes("b")[0] == vol("a", weight=0.5)
    assert led.volumes("other") == []


def test_consumers_and_staleness(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    cid = led.add_consumer("b", INFO)
    assert led.touch("b", cid) and not led.touch("b", "nope")
    seen = led.consumers("b", stale_s=300)
    assert seen[0]["stale"] is False and seen[0]["name"] == "n" and seen[0]["consumer_id"] == cid
    assert led.consumers("b", stale_s=-1)[0]["stale"] is True


def test_draws_are_sequential_under_threads(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    cid = led.add_consumer("b", INFO)
    out: list[int] = []

    def work():
        for _ in range(20):
            out.extend(d["seq"] for d in led.record_draws("b", cid, [{"volume": "a"}, {"volume": "a"}]))

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(out) == list(range(1, 161)) and led.draws_per_volume("b") == {"a": 160}


def test_frozen_lists(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    assert led.frozen("b", "val") == []
    led.extend_frozen("b", "val", [{"volume": "a", "seq": 0}, {"volume": "a", "seq": 1}])
    assert [d["seq"] for d in led.frozen("b", "val")] == [0, 1]


def test_assets_and_refs(tmp_path):
    """The registry: what exists on disk (by kind and key) and which buffers reference it."""
    led = Ledger(tmp_path / "l.sqlite")
    assert led.asset("staged", "/s/p256/u937") is None and led.assets("staged") == []
    led.put_asset("staged", "/s/p256/u937", {"record": "u937", "binding_hash": "b1"})
    led.put_asset("staged", "/s/p256/u937", {"record": "u937", "binding_hash": "b2"})  # upsert
    assert led.asset("staged", "/s/p256/u937") == {"record": "u937", "binding_hash": "b2"}
    for buffer in ("buf-1", "buf-1", "buf-2"):
        led.add_ref(buffer, "staged", "/s/p256/u937")
    led.add_ref("buf-2", "raw", "u937")  # a reference may precede the asset's own row
    assert led.refs("staged", "/s/p256/u937") == ["buf-1", "buf-2"] and led.refs("raw", "u937") == ["buf-2"]
    assert led.assets("staged") == [
        {"key": "/s/p256/u937", "data": {"record": "u937", "binding_hash": "b2"}, "refs": ["buf-1", "buf-2"]}
    ]
    assert led.assets("raw") == [{"key": "u937", "data": None, "refs": ["buf-2"]}]


def test_records_replace_per_ref_and_catalogs_list(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.put_records("a" * 40, [("datasets/x.yaml", {"id": "x"}), ("datasets/y.yaml", {"id": "y"})]) == 2
    assert led.put_records("a" * 40, [("datasets/z.yaml", {"id": "z", "k": 1})]) == 1
    assert led.records("a" * 40) == [("datasets/z.yaml", {"id": "z", "k": 1})] and led.records("b" * 40) == []
    led.put_records("b" * 40, [])
    assert [c["ref"] for c in led.catalogs()] == ["b" * 40, "a" * 40] and led.latest_catalog() == "b" * 40
    assert led.catalogs()[1]["count"] == 1


def test_dataset_rows_merge(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.dataset("r") is None
    led.put_dataset("r", {"state": "in-progress", "data_stage": "matching", "issue": 7})
    row = led.put_dataset("r", {"data_stage": "review"})
    assert row["state"] == "in-progress" and row["data_stage"] == "review" and row["issue"] == 7 and row["id"] == "r"
    assert [d["id"] for d in led.datasets("in-progress")] == ["r"] and led.datasets("prepared") == []


def test_sync_datasets_keeps_in_progress_rows(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_dataset("busy", {"state": "in-progress", "issue": 1})
    led.put_dataset("stuck", {"state": "needs-human", "issue": 2})
    led.put_dataset("old", {"state": "prepared", "folder": "f-old"})
    out = led.sync_datasets({"new": {"folder": "f-new", "crops": 3}})
    assert out == {"prepared": 1, "demoted": 1}
    assert led.dataset("old") is None and led.dataset("busy")["state"] == "in-progress"
    assert led.dataset("stuck")["state"] == "needs-human" and led.dataset("new")["crops"] == 3


def test_put_volume_with_refs_is_atomic(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    led.put_volume("b", vol("a"), refs=[("staged", "art1"), ("raw", "rec")])
    assert led.refs("staged", "art1") == ["b"] and led.refs("raw", "rec") == ["b"]
    with pytest.raises((sqlite3.InterfaceError, sqlite3.ProgrammingError)):
        led.put_volume("b", vol("c"), refs=[("staged", object())])  # unbindable on purpose
    assert [v.id for v in led.volumes("b")] == ["a"]


def test_backup_is_a_consistent_copy(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {"x": 1})
    led.backup(tmp_path / "bk" / "copy.sqlite")
    assert Ledger(tmp_path / "bk" / "copy.sqlite").buffer("b") == {"x": 1}
    assert not (tmp_path / "bk" / "copy.tmp").exists()


def test_put_asset_merges_and_filters(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("raw", "rec", {"state": "fetching", "job": "1", "path": "/netfs/lab/raw/rec"})
    merged = ledger.put_asset("raw", "rec", {"bytes": 10, "scanned_at": 5.0})
    assert merged["state"] == "fetching" and merged["job"] == "1" and merged["bytes"] == 10
    ledger.put_asset("staged", "art1", {"state": "ready", "record": "rec", "bytes": 100})
    ledger.put_asset("staged", "art2", {"state": "evicted", "record": "rec", "bytes": 0})
    ledger.add_ref("b", "staged", "ref-only")
    assert [a["key"] for a in ledger.assets("staged", state="ready")] == ["art1"]
    assert [a["key"] for a in ledger.assets("staged", record="rec")] == ["art1", "art2"]
    assert [a["key"] for a in ledger.assets("staged")] == ["art1", "art2", "ref-only"]
    assert ledger.asset_bytes("staged") == 100 and ledger.asset_bytes("raw") == 10


def test_sync_assets_marks_missing_but_keeps_building_rows(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("staged", "gone", {"state": "ready", "bytes": 5})
    ledger.put_asset("staged", "building", {"state": "staging", "job": "7"})
    ledger.put_asset("staged", "back", {"state": "missing"})
    ledger.put_asset("raw", "rec", {"state": "ready"})
    found = {
        ("staged", "back"): {"path": "/netfs/lab/s/p/rec/back", "bytes": 3, "scanned_at": 9.0},
        ("staged", "new"): {"path": "/netfs/lab/s/p/rec/new", "bytes": 4, "scanned_at": 9.0, "record": "rec"},
    }
    counts = ledger.sync_assets(found, older_than=time.time() + 1, kinds=("staged",))
    by_key = {a["key"]: a["data"] for a in ledger.assets("staged")}
    assert by_key["gone"]["state"] == "missing" and by_key["building"]["state"] == "staging"
    assert by_key["back"]["state"] == "ready" and by_key["new"]["state"] == "ready" and by_key["new"]["record"] == "rec"
    assert counts == {"scanned": 2, "new": 1, "missing": 1}
    assert by_key["new"]["last_used"] == 9.0  # first seen: used then
    assert by_key["back"]["last_used"] == 9.0  # known, but without last_used (from before it): as first seen
    ledger.sync_assets({("staged", "new"): {"bytes": 4, "scanned_at": 99.0}}, older_than=time.time() + 1, kinds=())
    assert ledger.asset("staged", "new")["last_used"] == 9.0  # a later scan is not a use
    assert ledger.asset("raw", "rec")["state"] == "ready"  # a kind not scanned is never demoted
    # a row written after the scan started is left alone
    ledger.put_asset("staged", "late", {"state": "ready"})
    ledger.sync_assets({}, older_than=time.time() - 60)
    assert {a["key"]: a["data"]["state"] for a in ledger.assets("staged")}["late"] == "ready"


def test_sync_assets_readies_stateless_and_evicted_rows_and_skips_rows_written_during_the_scan(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("staged", "old", {"binding_hash": "b", "bytes": 1})  # a milestone-B row: no state
    ledger.put_asset("staged", "evicted", {"state": "evicted", "bytes": 0})
    started = time.time() + 1
    found = {("staged", key): {"bytes": 7, "scanned_at": 9.0} for key in ("old", "evicted")}
    assert ledger.sync_assets(found, older_than=started) == {"scanned": 2, "new": 0, "missing": 0}
    assert ledger.asset("staged", "old") == {
        "binding_hash": "b",
        "bytes": 7,
        "scanned_at": 9.0,
        "state": "ready",
        "last_used": 9.0,  # a pre-C row is not evictable at once: the hold rule counts from its first scan
    }
    assert ledger.asset("staged", "evicted")["state"] == "ready"
    # evicted by a worker after the scan saw the directory: the scan's row is not merged back
    ledger.put_asset("staged", "evicted", {"state": "evicted", "bytes": 0})
    ledger.sync_assets({("staged", "evicted"): {"bytes": 7, "scanned_at": 10.0}}, older_than=time.time() - 60)
    assert ledger.asset("staged", "evicted") == {"state": "evicted", "bytes": 0, "scanned_at": 9.0, "last_used": 9.0}


def test_staged_last_used_comes_from_draws(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b", {})
    ledger.put_volume("b", dataclasses.replace(vol("a"), artifact="art1"))
    ledger.put_volume("b", vol("plain"))  # no artifact: never in the map
    ledger.put_asset("staged", "art1", {"state": "ready", "last_used": 1.0})
    assert ledger.staged_last_used() == {}
    before = time.time()
    ledger.record_draws("b", "c", [{"volume": "a"}, {"volume": "plain"}])
    after = time.time()
    used = ledger.staged_last_used()
    assert list(used) == ["art1"] and before <= used["art1"] <= after
    ledger.sync_assets({("staged", "art1"): {"bytes": 1, "scanned_at": after}})
    assert ledger.asset("staged", "art1")["last_used"] == used["art1"]
    ledger.put_asset("staged", "art1", {"last_used": after + 100})  # newer than every draw: kept
    ledger.sync_assets({("staged", "art1"): {"bytes": 1, "scanned_at": after}})
    assert ledger.asset("staged", "art1")["last_used"] == after + 100


def test_in_progress_records(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_dataset("busy", {"state": "in-progress"})
    ledger.put_dataset("done", {"state": "prepared"})
    assert ledger.in_progress_records() == {"busy"}
