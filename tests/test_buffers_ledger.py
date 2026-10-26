"""The SQLite ledger: buffers, volumes, consumers, draws and frozen lists."""

import dataclasses
import gzip
import json
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


def test_status_board_and_events(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer_dataset("b1", "rec", "fetching", job="j1")
    row = ledger.put_buffer_dataset("b1", "rec", "servable")
    assert row["status"] == "servable" and row["job"] is None and ledger.buffer_datasets("b1")[0]["record"] == "rec"
    assert ledger.buffer_datasets("b2") == [] and ledger.latest_event("b1") is None
    first = ledger.add_event("b1", "created", issue=2, event_id="e1", detail="")
    ledger.add_event("b1", "ready", issue=2, event_id=None, detail="2 of 2")
    assert first["seq"] == 1 and ledger.latest_event("b1")["kind"] == "ready" and len(ledger.events("b1")) == 2
    assert ledger.add_event("b2", "created", issue=None, event_id=None, detail="")["seq"] == 1  # per buffer


def test_retire_drops_only_this_buffers_refs(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b1", {})
    ledger.put_buffer("b2", {})
    ledger.put_volume("b1", vol("a"), refs=[("staged", "artA"), ("raw", "rec")])
    ledger.put_volume("b2", vol("b"), refs=[("staged", "artB"), ("raw", "rec")])
    cid = ledger.add_consumer("b1", INFO)
    ledger.record_draws("b1", cid, [{"volume": "a"}, {"volume": "a"}])
    ledger.record_draws("b2", cid, [{"volume": "b"}])
    out = ledger.retire("b1", tmp_path / "archive")
    assert out["refs"] == 2 and out["draws"] == 2 and (tmp_path / "archive" / "b1.draws.jsonl.gz").is_file()
    assert ledger.refs("staged", "artA") == [] and ledger.refs("raw", "rec") == ["b2"]
    assert ledger.max_seq("b1") == 0 and len(ledger.volumes("b1")) == 1  # volumes stay for the page
    assert ledger.max_seq("b2") == 1 and ledger.buffer("b1") == {}
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        lines = [json.loads(line) for line in fh]
    assert [line["seq"] for line in lines] == [1, 2] and set(lines[0]) == {"seq", "consumer", "ts", "descriptor"}
    assert lines[0]["consumer"] == cid and lines[0]["descriptor"]["volume"] == "a"
    again = ledger.retire("b1", tmp_path / "archive")  # a retried retire keeps the archive it wrote
    assert again["refs"] == 0 and again["draws"] == 0
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert len(fh.readlines()) == 2
    ledger.record_draws("b1", cid, [{"volume": "a"}])
    ledger.retire("b1", tmp_path / "archive")  # a later retire appends, never overwrites
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert len(fh.readlines()) == 3
    kept = ledger.retire("b2", None)  # a dev service keeps the draws
    assert kept == {"refs": 2, "draws": 0, "archive": None} and ledger.max_seq("b2") == 1


def test_buffers_summary_and_missing_flag(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b1", {"spec_name": "s"})
    ledger.put_buffer("b2", {})
    ledger.put_volume("b1", vol("a"))
    ledger.put_volume("b1", vol("c"))
    ledger.set_volume_missing("b1", "c", True, "/elsewhere")  # a stat of an old path changes nothing
    assert not ledger.volumes("b1")[1].missing
    ledger.set_volume_missing("b1", "c", True, "/staged/c")
    assert [volume.missing for volume in ledger.volumes("b1")] == [False, True]
    ledger.add_event("b1", "ready", issue=1, event_id=None, detail="")
    cid = ledger.add_consumer("b1", INFO)
    ledger.record_draws("b1", cid, [{"volume": "a"}, {"volume": "a"}])
    with ledger._lock:  # one draw from yesterday
        ledger._db.execute("UPDATE draws SET ts = ts - 86400 WHERE seq = 1")
    one, two = ledger.buffers()
    assert one["id"] == "b1" and one["config"] == {"spec_name": "s"} and one["latest_event"]["kind"] == "ready"
    assert one["volumes"] == 2 and one["missing"] == 1 and one["servable"] == {"train": 1}
    assert one["draws_today"] == 1 and one["consumers_seen"] is not None
    assert two == {
        "id": "b2",
        "config": {},
        "latest_event": None,
        "volumes": 0,
        "missing": 0,
        "servable": {},
        "consumers_seen": None,
        "draws_today": 0,
    }
    ledger.set_volume_missing("b1", "c", False, "/staged/c")
    assert ledger.buffers()[0]["servable"] == {"train": 2}
    ledger.add_event("b0", "planned", issue=3, event_id=None, detail="")  # planned, never sourced: no config yet
    ledger.add_event("b1", "idle", issue=1, event_id=None, detail="")  # a configured buffer is listed once
    planned, *rest = ledger.buffers()
    assert [row["id"] for row in rest] == ["b1", "b2"]
    assert planned["id"] == "b0" and planned["config"] is None and planned["latest_event"]["kind"] == "planned"
    assert (planned["volumes"], planned["draws_today"], planned["servable"]) == (0, 0, {})


def test_retire_keeps_a_draw_recorded_while_the_archive_is_written(tmp_path, monkeypatch):
    """The archive is read outside the lock: a draw recorded meanwhile has a newer seq and stays in the table."""
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer("b1", {})
    cid = ledger.add_consumer("b1", INFO)
    ledger.record_draws("b1", cid, [{"volume": "a"}, {"volume": "a"}])
    archive_draws = ledger._archive_draws

    def draw_meanwhile(buffer, archive, last):
        archive_draws(buffer, archive, last)
        ledger.record_draws("b1", cid, [{"volume": "late"}])  # would deadlock if the archive held the lock

    monkeypatch.setattr(ledger, "_archive_draws", draw_meanwhile)
    out = ledger.retire("b1", tmp_path / "archive")
    assert out["draws"] == 2 and ledger.max_seq("b1") == 3
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert [json.loads(line)["seq"] for line in fh] == [1, 2]


def test_usage_counts_empty_answers_and_merges_live_draws(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    day = time.strftime("%Y-%m-%d", time.gmtime())
    led.count_empty("b", day)
    led.count_empty("b", day)
    led.count_empty("b", "2026-01-01")
    led.count_empty("other", day)
    assert led.usage("b") == [
        {"day": "2026-01-01", "draws": 0, "consumers": 0, "empty": 1},
        {"day": day, "draws": 0, "consumers": 0, "empty": 2},
    ]
    led.record_draws("b", "c1", [{"volume": "a"}, {"volume": "a"}])
    led.record_draws("b", "c2", [{"volume": "a"}])
    assert led.usage("b")[1] == {"day": day, "draws": 3, "consumers": 2, "empty": 2}
    assert led.last_hour("b") == {"draws": 3, "requests": 2}  # two requests, three crops
    assert led.last_hour("other") == {"draws": 0, "requests": 0}
    assert led.retire("b", tmp_path / "archive")["draws"] == 3
    assert led.usage("b")[1] == {"day": day, "draws": 3, "consumers": 2, "empty": 2}  # materialised, empty kept
    led.record_draws("b", "c3", [{"volume": "a"}])  # a draw after the retire adds to the materialised day
    assert led.usage("b")[1] == {"day": day, "draws": 4, "consumers": 2, "empty": 2}


def test_kv(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.kv("keeper") is None
    led.put_kv("keeper", {"a": 1})
    led.put_kv("keeper", {"b": 2})
    assert led.kv("keeper") == {"b": 2}
