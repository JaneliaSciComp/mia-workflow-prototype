"""buffer-api: spec v0.5 §5 routes over the ledger, through handle() and once over a real socket."""

import json
import threading

import httpx

from mia_buffers.api import BufferService, make_server
from mia_buffers.ledger import Ledger

CFG = {
    "spec_name": "s",
    "spec_hash": "h",
    "levels": [1, 2, 4],
    "patch": 256,
    "channels": 1,
    "splits": {"train": 0.8, "val": 0.2},
    "split_sizes": {"val": 4},
}
CONSUMER = {"name": "n", "host": "h", "pid": 1, "split": "train"}


def body(vid="a", extent=2048, planes=5, split_planes=None):
    return {
        "path": f"/staged/{vid}/v0.zarr",
        "record": "rec",
        "axes": "czyx",
        "shape": [1, planes, extent, extent],
        "levels": [
            {
                "factor": f,
                "shape": [1, planes, extent // f, extent // f],
                "scale_nm": {"z": 1.0, "y": 10.0 * f, "x": 10.0 * f},
                "translation_nm": {"z": 0.0, "y": 5.0 * (f - 1), "x": 5.0 * (f - 1)},
            }
            for f in (1, 2, 4)
        ],
        "split_planes": split_planes or {"train": [0, 1, 2, 3], "val": [4]},
        "weight": 1.0,
        "channels_stored": 1,
        "recipe": {"op": "channel", "index": 0},
        "voxel_size_nm": {"y": 10.0, "x": 10.0},
    }


def svc(tmp_path):
    return BufferService(Ledger(tmp_path / "l.sqlite"))


def call(s, method, path, query=None, payload=None):
    return s.handle(method, path, query or {}, json.dumps(payload).encode() if payload is not None else b"")


def test_register_and_draw(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=body())[0] == 200
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/small", payload=body("small", extent=1000))
    assert status == 422 and out["error"] == "footprint 1024 exceeds 1000×1000"
    assert call(s, "PUT", "/buffers/zz/volumes/a", payload=body())[0] == 404
    status, out, _ = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)
    assert status == 201
    cid = out["consumer_id"]
    status, out, _ = call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": cid})
    assert status == 200 and [d["seq"] for d in out["descriptors"]] == [1, 2, 3]
    assert all(d["plane"]["z"] in (0, 1, 2, 3) and d["path"] == "/staged/a/v0.zarr" for d in out["descriptors"])
    assert call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": "nope"})[0] == 404
    st = call(s, "GET", "/buffers/b/status")[1]
    assert st["draws"] == {"a": 3} and st["servable"] == {"train": 1, "val": 1} and len(st["consumers"]) == 1
    assert st["volumes"] == 1 and st["config"]["levels"] == [1, 2, 4]


def test_frozen_split_is_stable_and_bounded(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    first = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    again = call(s, "GET", "/buffers/b/split/val", {"from": "2", "n": "10", "consumer": cid})[1]
    assert first["size"] == 4 and len(first["descriptors"]) == 4
    assert all(d["plane"]["z"] == 4 for d in first["descriptors"])
    assert again["descriptors"] == first["descriptors"][2:]
    assert call(s, "GET", "/buffers/b/split/val", {"from": "4", "n": "1", "consumer": cid})[0] == 416
    # a new service over the same ledger serves the same list
    second = BufferService(Ledger(tmp_path / "l.sqlite"))
    assert call(second, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1] == first
    assert call(s, "GET", "/buffers/b/split/train", {"from": "0", "n": "1", "consumer": cid})[1]["size"] == 256


def test_empty_split_is_204_and_bad_bodies_are_422(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body(split_planes={"train": [0, 1, 2, 3, 4]}))
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    status, out, headers = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})
    assert status == 204 and out is None and headers["Retry-After"] == "30"
    assert call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": cid})[0] == 204
    status, out, _ = call(s, "PUT", "/buffers/b", payload={**CFG, "bogus": 1})
    assert status == 422 and "bogus" in out["error"]
    assert call(s, "PUT", "/buffers/b", payload=None)[0] == 422
    assert call(s, "GET", "/nothing")[0] == 404
    assert call(s, "GET", "/buffers/zz/status")[0] == 404
    assert call(s, "GET", "/buffers/b/next", {"n": "x", "consumer": cid})[0] == 422


def test_concurrent_next_gives_unique_seqs(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cids = [call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "pid": i})[1]["consumer_id"] for i in range(3)]
    seqs: list[int] = []

    def pull(cid):
        for _ in range(10):
            out = call(s, "GET", "/buffers/b/next", {"n": "4", "consumer": cid})[1]
            seqs.extend(d["seq"] for d in out["descriptors"])

    threads = [threading.Thread(target=pull, args=(c,)) for c in cids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(seqs) == list(range(1, 121))


def test_over_http(tmp_path):
    server = make_server(svc(tmp_path), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        assert httpx.put(f"{base}/buffers/b", json=CFG).status_code == 200
        assert httpx.put(f"{base}/buffers/b/volumes/a", json=body()).status_code == 200
        cid = httpx.post(f"{base}/buffers/b/consumers", json=CONSUMER).json()["consumer_id"]
        r = httpx.get(f"{base}/buffers/b/next", params={"n": 2, "consumer": cid})
        assert r.status_code == 200 and len(r.json()["descriptors"]) == 2
        assert httpx.get(f"{base}/buffers/b/status").json()["draws"] == {"a": 2}
        empty = httpx.get(f"{base}/buffers/b/split/val", params={"from": 0, "n": 1, "consumer": cid})
        assert empty.status_code == 200
        assert httpx.get(f"{base}/nothing").status_code == 404
    finally:
        server.shutdown()
