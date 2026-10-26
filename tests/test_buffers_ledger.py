"""The SQLite ledger: buffers, volumes, consumers, draws and frozen lists."""

import sqlite3
import threading

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
