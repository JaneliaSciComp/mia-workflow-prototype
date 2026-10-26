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
    server = make_server(BufferService(Ledger(tmp_path / "l.sqlite"), token="t"), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        auth = {"Authorization": "Bearer t"}
        assert httpx.put(f"{base}/buffers/b", json=CFG).status_code == 401
        assert httpx.put(f"{base}/buffers/b", json=CFG, headers=auth).status_code == 200
        assert httpx.put(f"{base}/buffers/b/volumes/a", json=body(), headers=auth).status_code == 200
        cid = httpx.post(f"{base}/buffers/b/consumers", json=CONSUMER).json()["consumer_id"]
        r = httpx.get(f"{base}/buffers/b/next", params={"n": 2, "consumer": cid})
        assert r.status_code == 200 and len(r.json()["descriptors"]) == 2
        assert httpx.get(f"{base}/buffers/b/status").json()["draws"] == {"a": 2}
        empty = httpx.get(f"{base}/buffers/b/split/val", params={"from": 0, "n": 1, "consumer": cid})
        assert empty.status_code == 200
        assert httpx.get(f"{base}/nothing").status_code == 404
        assert httpx.put(f"{base}/buffers/b/volumes/rec%2Fv0.zarr", json=body("rec"), headers=auth).status_code == 200
        assert httpx.get(f"{base}/buffers/b/status").json()["volumes"] == 2
    finally:
        server.shutdown()


def test_worker_routes_need_the_token(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="secret")
    assert call(s, "PUT", "/buffers/b", payload=CFG)[:2] == (401, {"error": "unauthorized"})
    for bad in ({}, {"Authorization": "Bearer secre"}, {"Authorization": "Bearer "}, {"authorization": "secret"}):
        assert s.handle("PUT", "/buffers/b", {}, json.dumps(CFG).encode(), bad)[0] == 401
    ok = {"Authorization": "Bearer secret"}
    assert s.handle("PUT", "/buffers/b", {}, json.dumps(CFG).encode(), ok)[0] == 200
    assert s.handle("PUT", "/assets/raw/r", {}, b"{}", {"authorization": "Bearer secret"})[0] == 200  # any case
    assert s.handle("PUT", "/buffers/b", {}, b"not json", {})[0] == 401  # refused before parsing


def test_consumer_and_read_routes_are_open(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="secret")
    ok = {"Authorization": "Bearer secret"}
    s.handle("PUT", "/buffers/b", {}, json.dumps(CFG).encode(), ok)
    assert call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[0] == 201
    assert call(s, "GET", "/buffers/b/status")[0] == 200
    assert call(s, "GET", "/assets/raw")[0] == 200


def test_next_n_is_bounded_and_from_is_non_negative(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    assert call(s, "GET", "/buffers/b/next", {"n": "0", "consumer": cid})[0] == 422
    assert call(s, "GET", "/buffers/b/next", {"n": "100000", "consumer": cid})[0] == 422
    assert call(s, "GET", "/buffers/b/next", {"n": "1024", "consumer": cid})[0] == 200
    assert call(s, "GET", "/buffers/b/split/val", {"from": "-1", "n": "1", "consumer": cid})[0] == 422


def test_volume_levels_are_validated_and_errors_never_drop_the_connection(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    broken = body()
    del broken["levels"][0]["shape"]
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=broken)
    assert status == 422 and "shape" in out["error"]
    no_yx = body()
    no_yx["levels"][0]["scale_nm"] = {"z": 1.0}
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=no_yx)
    assert status == 422 and "scale_nm" in out["error"]
    short = body()
    short["levels"] = short["levels"][:2]
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=short)
    assert status == 422 and "levels" in out["error"] and "4" in out["error"]
    s.ledger.put_volume = lambda *a, **k: 1 / 0  # type: ignore[method-assign]
    status, out, _ = call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    assert status == 500 and out["error"].startswith("ZeroDivisionError")


def test_restart_does_not_replay_the_train_stream(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    first = [
        {k: v for k, v in d.items() if k != "seq"}
        for d in call(s, "GET", "/buffers/b/next", {"n": "5", "consumer": cid})[1]["descriptors"]
    ]
    restarted = BufferService(Ledger(tmp_path / "l.sqlite"))
    again = [
        {k: v for k, v in d.items() if k != "seq"}
        for d in call(restarted, "GET", "/buffers/b/next", {"n": "5", "consumer": cid})[1]["descriptors"]
    ]
    assert again != first


def test_consumer_lookup_is_by_id(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    cid = led.add_consumer("b", {**CONSUMER, "rank": 0, "mode": "local"})
    assert led.consumer("b", cid)["split"] == "train" and led.consumer("b", "nope") is None


def test_descriptor_carries_normalize(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    bounds = {"lo": [1], "hi": [200]}
    assert call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "normalize": bounds})[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/c", payload=body("c", split_planes={"test": [0]}))[0] == 200
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    descriptors = call(s, "GET", "/buffers/b/next", {"n": "2", "consumer": cid})[1]["descriptors"]
    assert all(d["normalize"] == bounds for d in descriptors)  # only a has train planes
    test = call(s, "GET", "/buffers/b/split/test", {"from": "0", "n": "1", "consumer": cid})[1]["descriptors"]
    assert test and test[0]["volume"] == "c" and test[0]["normalize"] is None  # c registered without bounds


def test_assets_registry_and_refs_from_registered_volumes(tmp_path):
    """`PUT/GET assets/<kind>/<key>` keep what exists on disk; registering a volume references its staged
    directory and its record's raw cache for the buffer, so a retire can count what is still in use."""
    from urllib.parse import quote

    s = svc(tmp_path)
    key = quote("/staged/a", safe="")
    assert call(s, "GET", f"/assets/staged/{key}")[0] == 404
    assert call(s, "PUT", f"/assets/staged/{key}", payload=[1])[0] == 422  # a JSON object, nothing else
    assert call(s, "PUT", f"/assets/staged/{key}", payload={"record": "rec", "ok": True})[0] == 200
    status, out, _ = call(s, "GET", f"/assets/staged/{key}")
    assert status == 200 and out == {
        "kind": "staged",
        "key": "/staged/a",
        "data": {"record": "rec", "ok": True},
        "refs": [],
    }
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=body())[0] == 200  # path /staged/a/v0.zarr, record rec
    assert call(s, "GET", f"/assets/staged/{key}")[1]["refs"] == ["b"]
    assert call(s, "GET", "/assets/raw/rec")[1] == {"kind": "raw", "key": "rec", "data": None, "refs": ["b"]}
    assert call(s, "GET", "/assets/staged")[1] == {
        "kind": "staged",
        "assets": [{"key": "/staged/a", "data": {"record": "rec", "ok": True}, "refs": ["b"]}],
    }
    assert call(s, "GET", "/assets/nope/x")[0] == 404
