"""buffer-api: spec v0.5 §5 routes over the ledger, through handle() and once over a real socket."""

import gzip
import json
import re
import shutil
import threading

import httpx
import pytest
import yaml

from mia_buffers.api import BufferService, ledger_path, make_server, open_ledger, prune_backups
from mia_buffers.ledger import Ledger
from mia_buffers.plan import load_records
from mia_core.locks import LockDir
from tests.test_buffers_config import make_cfg
from tests.test_buffers_plan import CATALOG as FIXTURE_CATALOG
from tests.test_buffers_plan import FIXTURES, ONE_CH, U937
from tests.test_buffers_sync import write_prepared


def _spec_2d_text() -> str:
    """The fixture spec restricted to 2D, as the `spec_2d` fixture builds it, as YAML text."""
    data = yaml.safe_load((FIXTURES / "spec.yaml").read_text())
    data["select"]["where"]["dims"] = {"in": ["2D"]}
    data["select"]["where"]["annotations"]["source"] = "any"
    data["select"]["exclude"].update(ids=[ONE_CH], reasons={ONE_CH: "one channel"})
    return yaml.safe_dump(data)


SPEC_2D_TEXT = _spec_2d_text()

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
        "bytes": 0,
    }
    assert call(s, "GET", "/assets/nope/x")[0] == 404


def test_records_datasets_and_plan(tmp_path):
    s = svc(tmp_path)
    ref = "a" * 40
    records = [{"path": p, "record": r} for p, r in load_records(FIXTURE_CATALOG)]
    assert call(s, "PUT", f"/records/{ref}", payload={"records": records})[1]["count"] == len(records)
    assert call(s, "PUT", "/records/main", payload={"records": []})[0] == 422
    assert call(s, "PUT", f"/records/{ref}", payload={"records": [{"path": "p"}]})[0] == 422
    assert call(s, "GET", "/catalogs")[1]["catalogs"][0]["ref"] == ref
    text = re.sub(r"ref: [0-9a-f]{40}", f"ref: {ref}", SPEC_2D_TEXT)  # the 2D fixture spec's text, at the synced ref
    status, out, _ = call(s, "POST", "/plan", payload={"spec": text})
    assert status == 200 and all(row["prepared"] == "unprepared" for row in out["rows"])
    assert all(row["rule"] == "not prepared" for row in out["rows"] if row["fit"] and row["fit"]["ok"])
    assert call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "crops": 16})[1]["crops"] == 16
    assert call(s, "PUT", f"/datasets/{U937}", payload={"state": "nope"})[0] == 422
    assert call(s, "PUT", f"/datasets/{U937}", payload=["state"])[0] == 422
    assert call(s, "GET", f"/datasets/{U937}")[1]["state"] == "prepared"
    assert call(s, "GET", "/datasets/absent")[0] == 404
    assert [d["id"] for d in call(s, "GET", "/datasets", {"state": "prepared"})[1]["datasets"]] == [U937]
    out = call(s, "POST", "/plan", payload={"spec": text})[1]
    by_id = {row["id"]: row for row in out["rows"]}
    assert by_id[U937]["prepared"] == "prepared" and by_id[U937]["admitted"] and by_id[U937]["weight"] > 0
    status, out, _ = call(s, "POST", "/plan", payload={"spec": SPEC_2D_TEXT})  # the fixture's own ref is not synced
    assert status == 409 and "mia-buffers catalog sync --ref ab6a013d5da39c80651ddc845915d034b2eae82a" in out["error"]
    assert call(s, "POST", "/plan", payload={"spec": "schema_version: 2"})[0] == 422


def test_sync_datasets_route(tmp_path):
    root = tmp_path / "crops"
    write_prepared(root / "confocal-human-u937", U937)
    write_prepared(root / "tem-mouse-stale", "stale", match=False)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    s.ledger.put_dataset("busy", {"state": "in-progress"})
    assert call(s, "POST", "/sync/datasets")[1] == {"prepared": 1, "demoted": 0}
    assert call(s, "GET", "/datasets/stale")[0] == 404 and call(s, "GET", "/datasets/busy")[1]["state"] == "in-progress"
    assert call(s, "GET", f"/datasets/{U937}")[1]["folder"] == "confocal-human-u937"
    assert call(s, "POST", "/sync/datasets")[0] == 200
    assert BufferService(Ledger(tmp_path / "l2.sqlite")).handle("POST", "/sync/datasets", {}, b"")[0] == 503
    worker = BufferService(Ledger(tmp_path / "l3.sqlite"), token="t", storage_root=root)
    assert worker.handle("POST", "/sync/datasets", {}, b"")[0] == 401


def test_sync_keeps_a_row_prepared_during_the_scan(tmp_path, monkeypatch):
    root = tmp_path / "crops"
    write_prepared(root / "u937", U937)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    s.ledger.put_dataset("old", {"state": "prepared"})

    def scan_while_set_stage_runs(path):
        s.ledger.put_dataset("late", {"state": "prepared"})  # mia-data's set_stage("done"), after the scan passed
        return {}

    monkeypatch.setattr("mia_buffers.api.scan_storage", scan_while_set_stage_runs)
    assert call(s, "POST", "/sync/datasets")[1] == {"prepared": 0, "demoted": 1}
    assert s.ledger.dataset("late")["state"] == "prepared" and s.ledger.dataset("old") is None


def test_records_with_a_shared_or_integer_id_all_land(tmp_path):
    s = svc(tmp_path)
    ref = "a" * 40
    records = [
        {"path": "datasets/a.yaml", "record": {"id": "same"}},
        {"path": "datasets/b.yaml", "record": {"id": "same"}},
        {"path": "datasets/c.yaml", "record": {"id": 2024}},
    ]
    assert call(s, "PUT", f"/records/{ref}", payload={"records": records}) == (200, {"ref": ref, "count": 3}, {})
    assert [path for path, _ in s.ledger.records(ref)] == ["datasets/a.yaml", "datasets/b.yaml", "datasets/c.yaml"]


def test_sync_refuses_a_missing_or_empty_storage_root(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_dataset("rec", {"state": "prepared", "folder": "rec"})
    for root in (tmp_path / "unmounted", tmp_path / "empty"):
        if root.name == "empty":
            root.mkdir()
        status, out, _ = call(BufferService(led, storage_root=root), "POST", "/sync/datasets")
        assert status == 503 and out == {"error": f"storage_root {root} is missing or empty; not syncing"}
    assert led.dataset("rec")["state"] == "prepared"


def test_sync_409_while_running(tmp_path):
    root = tmp_path / "crops"
    write_prepared(root / "u937", U937)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    with s._sync_lock:
        status, out, _ = call(s, "POST", "/sync/datasets")
    assert status == 409 and out == {"error": "a datasets sync is running"}
    assert s.ledger.datasets() == []


def buffers_cfg(tmp_path, local_root):
    return make_cfg(tmp_path, paths={"local_root": str(local_root) if local_root else None})


def test_open_ledger_moves_the_old_file_once(tmp_path, capsys):
    cfg = buffers_cfg(tmp_path, tmp_path / "local")
    old = Ledger(cfg.paths.state_dir / "buffers.sqlite")
    old.put_buffer("b", {"x": 1})
    old.close()  # the pre-B service is stopped before the upgrade
    assert open_ledger(cfg).buffer("b") == {"x": 1} and "moved the ledger" in capsys.readouterr().out
    assert not (tmp_path / "state" / "buffers.sqlite").exists()
    assert Ledger(tmp_path / "state" / "buffers.sqlite.moved").buffer("b") == {"x": 1}
    Ledger(cfg.paths.state_dir / "buffers.sqlite").put_buffer("b", {"x": 2})
    assert open_ledger(cfg).buffer("b") == {"x": 1}  # already moved: the old file is not read again


def test_open_ledger_refuses_a_missing_ledger_when_backups_exist(tmp_path):
    cfg = buffers_cfg(tmp_path, tmp_path / "local")
    backups = cfg.paths.state_dir / "backups"
    for stamp in ("20261001T000000Z", "20261024T120000Z"):
        Ledger(backups / f"buffers-{stamp}.sqlite").put_buffer("b", {"x": 1})
    with pytest.raises(RuntimeError, match="buffers-20261024T120000Z.sqlite") as err:
        open_ledger(cfg)
    assert str(ledger_path(cfg)) in str(err.value) and not ledger_path(cfg).exists()


def test_open_ledger_first_install_starts_empty(tmp_path):
    led = open_ledger(buffers_cfg(tmp_path, tmp_path / "local"))
    assert led.catalogs() == [] and led.datasets() == []


def test_prune_backups_keeps_the_newest(tmp_path):
    for i in range(50):
        (tmp_path / f"buffers-2026010100{i:02d}00Z.sqlite").write_text("")
    prune_backups(tmp_path, keep=48)
    assert len(list(tmp_path.glob("buffers-*.sqlite"))) == 48
    assert not (tmp_path / "buffers-20260101000000Z.sqlite").exists()


def test_serve_refuses_without_local_root(tmp_path):
    with pytest.raises(RuntimeError, match="local_root"):
        ledger_path(buffers_cfg(tmp_path, None))


def test_serve_refuses_a_network_root(tmp_path, monkeypatch):
    monkeypatch.setattr("mia_core.fs.mount_type", lambda path, mounts=None: "nfs4")
    with pytest.raises(RuntimeError, match="network filesystem"):
        open_ledger(buffers_cfg(tmp_path, tmp_path / "local"))


def test_volume_refs_are_keyed_by_artifact(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "artifact": "art1"})
    assert call(s, "GET", "/assets/staged/art1")[1]["refs"] == ["b"]
    call(s, "PUT", "/buffers/b/volumes/c", payload=body("c"))  # no artifact: the directory, as before
    assert call(s, "GET", "/assets/staged/%2Fstaged%2Fc")[1]["refs"] == ["b"]


def test_assets_merge_filter_and_totals(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/assets/staged/a1", payload={"state": "staging", "record": "rec", "job": "7"})[0] == 200
    status, out, _ = call(s, "PUT", "/assets/staged/a1", payload={"state": "ready", "bytes": 100})
    assert status == 200 and out == {
        "kind": "staged",
        "key": "a1",
        "data": {"state": "ready", "record": "rec", "job": "7", "bytes": 100},
    }
    call(s, "PUT", "/assets/staged/a2", payload={"state": "evicted", "record": "rec", "bytes": 50})
    status, out, _ = call(s, "PUT", "/assets/staged/a1", payload={"state": "gone"})
    assert status == 422 and "state must be one of" in out["error"]
    assert call(s, "PUT", "/assets/staged/a1", payload=[1])[0] == 422
    assert s.handle("PUT", "/assets/staged/a1", {}, b"not json")[0] == 422
    status, out, _ = call(s, "GET", "/assets/staged", {"state": "ready"})
    assert status == 200 and [a["key"] for a in out["assets"]] == ["a1"] and out["bytes"] == 100
    assert [a["key"] for a in call(s, "GET", "/assets/staged", {"record": "rec"})[1]["assets"]] == ["a1", "a2"]


def test_locks_route_lists_holders(tmp_path):
    locks = LockDir(tmp_path / "locks")
    assert locks.take("raw", "rec", "job-1", 900)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", locks=locks)
    status, out, _ = call(s, "GET", "/locks")  # a read route: no token
    assert status == 200 and [(h["kind"], h["key"], h["owner"], h["stale"]) for h in out["locks"]] == [
        ("raw", "rec", "job-1", False)
    ]
    assert call(svc(tmp_path / "other"), "GET", "/locks")[1] == {"locks": []}


def test_sync_assets_skips_a_missing_or_empty_root(tmp_path):
    raw = tmp_path / "raw"
    (raw / "rec").mkdir(parents=True)
    (raw / "rec" / "k").write_bytes(b"x" * 8)
    led = Ledger(tmp_path / "l.sqlite")
    led.put_asset("staged", "art", {"state": "ready", "bytes": 5})
    led.put_asset("raw", "gone", {"state": "ready", "bytes": 5})
    s = BufferService(led, raw_root=raw, staged_root=tmp_path / "unmounted")
    status, out, _ = call(s, "POST", "/sync/assets")
    assert status == 200 and out == {"scanned": 1, "new": 1, "missing": 1, "building": [], "skipped": ["staged"]}
    assert led.asset("staged", "art")["state"] == "ready" and led.asset("raw", "gone")["state"] == "missing"
    assert led.asset("raw", "rec")["state"] == "ready" and led.asset("raw", "rec")["bytes"] == 8
    assert BufferService(led).handle("POST", "/sync/assets", {}, b"")[0] == 503
    worker = BufferService(led, token="t", raw_root=raw)
    assert worker.handle("POST", "/sync/assets", {}, b"")[0] == 401


def test_sync_assets_is_exclusive(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), raw_root=tmp_path / "raw", staged_root=tmp_path / "staged")
    with s._sync_lock:
        status, out, _ = call(s, "POST", "/sync/assets")
    assert status == 409 and out == {"error": "a sync is running"}


AUTH = {"Authorization": "Bearer t"}


def worker(s, method, path, payload=None):
    return s.handle(method, path, {}, json.dumps(payload).encode() if payload is not None else b"", AUTH)


def test_records_route_defaults_to_the_newest_ref(tmp_path):
    s = svc(tmp_path)
    assert call(s, "GET", "/records")[:2] == (404, {"error": "no catalog synced"})
    old, new = "a" * 40, "b" * 40
    call(s, "PUT", f"/records/{old}", payload={"records": [{"path": "x.yaml", "record": {"id": "x"}}]})
    call(s, "PUT", f"/records/{new}", payload={"records": [{"path": "y.yaml", "record": {"id": "y"}}]})
    status, out, _ = call(s, "GET", "/records")
    assert status == 200 and out == {"ref": new, "records": [{"path": "y.yaml", "record": {"id": "y"}}]}
    assert call(s, "GET", "/records", {"ref": old})[1]["records"][0]["path"] == "x.yaml"
    assert call(s, "GET", "/records", {"ref": "c" * 40})[0] == 404


def test_status_board_routes(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t")
    worker(s, "PUT", "/buffers/b", CFG)
    assert call(s, "PUT", "/buffers/b/datasets/rec", payload={"status": "fetching"})[0] == 401
    status, out, _ = worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "fetching", "job": "j1"})
    assert status == 200 and out["status"] == "fetching" and out["job"] == "j1"
    status, out, _ = worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "done"})
    assert status == 422 and "status must be one of" in out["error"]
    assert worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "failed", "nope": 1})[0] == 422
    assert worker(s, "PUT", "/buffers/zz/datasets/rec", {"status": "failed"})[0] == 404
    worker(s, "PUT", "/buffers/b/datasets/rec", {"status": "failed", "reason": "boom"})
    rows = call(s, "GET", "/buffers/b/datasets")[1]["datasets"]
    assert [(row["record"], row["status"], row["reason"], row["job"]) for row in rows] == [
        ("rec", "failed", "boom", None)
    ]
    assert call(s, "POST", "/buffers/b/events", payload={"kind": "created"})[0] == 401
    status, out, _ = worker(s, "POST", "/buffers/b/events", {"kind": "created", "issue": 3, "event_id": "e1"})
    assert status == 200 and out["seq"] == 1 and out["detail"] == "" and out["issue"] == 3
    worker(s, "POST", "/buffers/b/events", {"kind": "needs-human", "detail": "why"})
    status, out, _ = worker(s, "POST", "/buffers/b/events", {"kind": "exploded"})
    assert status == 422 and "kind must be one of" in out["error"]
    events = call(s, "GET", "/buffers/b/events")[1]["events"]
    assert [(event["seq"], event["kind"]) for event in events] == [(1, "created"), (2, "needs-human")]
    assert call(s, "GET", "/buffers")[1]["buffers"][0]["latest_event"]["kind"] == "needs-human"
    # a round's `created` and `planned` come before its buffer config: the events routes need none
    assert worker(s, "POST", "/buffers/early/events", {"kind": "created"})[0] == 200
    assert [event["kind"] for event in call(s, "GET", "/buffers/early/events")[1]["events"]] == ["created"]


def test_retire_route_archives_and_frees_refs(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), token="t", archive_dir=tmp_path / "archive")
    worker(s, "PUT", "/buffers/b", CFG)
    worker(s, "PUT", "/buffers/b/volumes/a", {**body(), "artifact": "art1"})
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": cid})
    assert call(s, "DELETE", "/buffers/b/refs")[0] == 401
    status, out, _ = worker(s, "DELETE", "/buffers/b/refs")
    archive = tmp_path / "archive" / "b.draws.jsonl.gz"
    assert status == 200 and out == {"refs": 2, "draws": 3, "archive": str(archive)}
    with gzip.open(archive, "rt") as fh:
        assert [json.loads(line)["seq"] for line in fh] == [1, 2, 3]
    assert call(s, "GET", "/assets/staged/art1")[0] == 404  # no row and no reference left
    assert call(s, "GET", "/buffers/b/status")[1]["volumes"] == 1  # the page still shows the retired buffer
    assert worker(s, "DELETE", "/buffers/zz/refs")[0] == 404


def test_status_stats_pyramids_and_next_skips_missing(tmp_path, monkeypatch):
    staged = tmp_path / "staged"
    s = BufferService(Ledger(tmp_path / "l.sqlite"), staged_root=staged)
    call(s, "PUT", "/buffers/b", payload=CFG)
    for vid in ("a", "c"):
        (staged / vid / "v0.zarr").mkdir(parents=True)
        call(s, "PUT", f"/buffers/b/volumes/{vid}", payload={**body(vid), "path": str(staged / vid / "v0.zarr")})
    out = call(s, "GET", "/buffers/b/status")[1]
    assert out["missing"] == [] and out["servable"] == {"train": 2, "val": 2}
    shutil.rmtree(staged / "c")
    seen = []
    real_isdir = __import__("os").path.isdir
    monkeypatch.setattr("mia_buffers.api.os.path.isdir", lambda path: seen.append(path) or real_isdir(path))
    out = call(s, "GET", "/buffers/b/status")[1]
    assert out["missing"] == ["c"] and out["servable"] == {"train": 1, "val": 1} and out["volumes"] == 2
    assert len(seen) == 2  # each volume's path once per call
    cid = call(s, "POST", "/buffers/b/consumers", payload=CONSUMER)[1]["consumer_id"]
    descriptors = call(s, "GET", "/buffers/b/next", {"n": "50", "consumer": cid})[1]["descriptors"]
    assert {d["volume"] for d in descriptors} == {"a"}
    (staged / "c" / "v0.zarr").mkdir(parents=True)  # back on disk: the next status serves it again
    assert call(s, "GET", "/buffers/b/status")[1]["missing"] == []
    assert call(s, "GET", "/buffers/b/status")[1]["servable"] == {"train": 2, "val": 2}


def test_status_without_a_staged_root_does_not_stat(tmp_path, monkeypatch):
    s = svc(tmp_path)  # a dev service has no disk view: every registered volume stays servable
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())

    def boom(path):
        raise AssertionError(f"stat {path}")

    monkeypatch.setattr("mia_buffers.api.os.path.isdir", boom)
    out = call(s, "GET", "/buffers/b/status")[1]
    assert out["missing"] == [] and out["servable"] == {"train": 1, "val": 1}


def test_buffers_list_does_not_stat(tmp_path, monkeypatch):
    s = BufferService(Ledger(tmp_path / "l.sqlite"), staged_root=tmp_path / "staged")
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body())  # its path does not exist
    call(s, "GET", "/buffers/b/status")  # marks it missing

    def boom(path):
        raise AssertionError(f"stat {path}")

    monkeypatch.setattr("mia_buffers.api.os.path.isdir", boom)
    monkeypatch.setattr("os.path.isdir", boom)
    status, out, _ = call(s, "GET", "/buffers")
    assert status == 200 and [row["id"] for row in out["buffers"]] == ["b"]
    assert out["buffers"][0]["missing"] == 1 and out["buffers"][0]["servable"] == {"train": 0, "val": 0}


def test_buffer_config_takes_the_lifecycle_fields(tmp_path):
    s = svc(tmp_path)
    recipe = {"tensorizer": "muvit-2d", "params": {}, "harness_commit": "c", "env_digest": "d"}
    lifecycle = {"recipe": recipe, "plan_hash": "p", "idle_hours": 1.5, "retire_after_days": 2, "auto_approve": True}
    assert call(s, "PUT", "/buffers/b", payload={**CFG, **lifecycle})[0] == 200
    config = call(s, "GET", "/buffers/b/status")[1]["config"]
    assert {key: config[key] for key in lifecycle} == {**lifecycle, "retire_after_days": 2.0}
    assert call(s, "PUT", "/buffers/d", payload=CFG)[0] == 200
    defaults = call(s, "GET", "/buffers/d/status")[1]["config"]
    assert (defaults["idle_hours"], defaults["retire_after_days"], defaults["auto_approve"]) == (72, 14, False)
