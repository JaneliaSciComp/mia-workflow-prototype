"""The SQLite ledger: buffers, volumes, consumers, draws and frozen lists."""

import threading

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
