"""BufferClient and the local-mode BufferDataset against a real buffer-api in a thread; tensorize is faked."""

import threading
from pathlib import Path

import httpx
import pytest

from mia_buffers import client as client_module
from mia_buffers.api import BufferService, make_server
from mia_buffers.client import BufferClient, BufferDataset, BufferEmpty
from mia_buffers.ledger import Ledger
from tests.test_buffers_api import CFG, body

SNAP = Path("/snap")


def fake_tensorize(pyramid_dir, descriptor, snapshot, name, params):
    import array

    assert snapshot == SNAP and name == "muvit-2d" and params["channels"] == {"op": "channel", "index": 0}
    assert params["normalize"] == "percentile" and params["scope"] == "volume"
    assert params["stats"] == {"lo": [0], "hi": [9]}
    patch, levels = descriptor["patch"], len(descriptor["levels"])
    return {
        "img": _Arr((levels, 1, patch, patch)),
        "bbox": _Arr((levels, 2, 2)),
        "label/gt": _Arr((levels, patch, patch)),
        "pixel_size": _Arr((levels, 2)),
        "meta": {"pyramid": str(pyramid_dir), "descriptor": descriptor, "array": array.array("b")},
    }


class _Arr:
    """Stands in for a numpy array: shape and ndim are all the dataset and the sanity check look at."""

    def __init__(self, shape):
        self.shape, self.ndim = shape, len(shape)


@pytest.fixture
def buffer_server(tmp_path, monkeypatch):
    monkeypatch.setattr(client_module, "tensorize", fake_tensorize)
    monkeypatch.setattr(client_module, "torch", None)
    srv = make_server(BufferService(Ledger(tmp_path / "l.sqlite")), "127.0.0.1", 0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    cfg = {**CFG, "tensorizer_params": {"normalize": "percentile", "scope": "volume"}}
    assert httpx.put(f"{base}/buffers/b", json=cfg).status_code == 200
    volume = {**body(), "normalize": {"lo": [0], "hi": [9]}}
    assert httpx.put(f"{base}/buffers/b/volumes/a", json=volume).status_code == 200
    yield base
    srv.shutdown()


def consumers(base):
    return httpx.get(f"{base}/buffers/b/status").json()["consumers"]


def test_client_calls(buffer_server):
    c = BufferClient(buffer_server, "b")
    assert c.status()["volumes"] == 1
    cid = c.register("train")
    assert len(c.next(cid, 3)) == 3
    descriptors, size = c.split(cid, "val", 0, 2)
    assert size == 4 and len(descriptors) == 2
    with pytest.raises(BufferEmpty, match="split test"):
        c.split(cid, "test", 0, 1)
    with pytest.raises(httpx.HTTPStatusError):
        c.next("nope", 1)


def test_train_dataset_streams_and_val_is_frozen(buffer_server):
    train = BufferDataset(buffer_server, "b", "train", n_samples=10, tensorizers=SNAP, prefetch=4)
    assert len(train) == 10 and train.levels == (1, 2, 4) and train.n_channels == 1 and train.ndim == 2
    s = train[0]
    assert s["img"].shape == (3, 1, 256, 256) and s["bbox"].shape == (3, 2, 2)
    assert s["meta"]["descriptor"]["plane"]["z"] in range(4) and s["meta"]["descriptor"]["path"] == "/staged/a/v0.zarr"
    seqs = [train[i]["meta"]["descriptor"]["seq"] for i in range(10)]
    assert len(set(seqs)) == 10 and "array" not in s["meta"]  # a stream of distinct draws; meta is JSON-safe
    val = BufferDataset(buffer_server, "b", "val", tensorizers=SNAP, prefetch=2)
    assert len(val) == 4
    first = [val[i]["meta"]["descriptor"]["seq"] for i in range(4)]
    assert first == [0, 1, 2, 3] and first == [val[i]["meta"]["descriptor"]["seq"] for i in range(4)]
    assert all(val[i]["meta"]["descriptor"]["plane"]["z"] == 4 for i in range(4))
    assert [c["split"] for c in consumers(buffer_server)] == ["train", "val"]
    with pytest.raises(IndexError):
        val[4]


def test_two_consumers_share_one_ledger(buffer_server):
    one = BufferDataset(buffer_server, "b", "train", n_samples=20, tensorizers=SNAP, prefetch=8)
    two = BufferDataset(buffer_server, "b", "train", n_samples=20, tensorizers=SNAP, prefetch=8)
    seen = {d[i]["meta"]["descriptor"]["seq"] for d in (one, two) for i in range(20)}
    assert len(seen) == 40  # every draw distinct across the two consumers
    status = httpx.get(f"{buffer_server}/buffers/b/status").json()
    assert status["draws"]["a"] >= 40 and len(status["consumers"]) == 2


def test_empty_split_and_unreachable_server(buffer_server):
    with pytest.raises(RuntimeError, match="buffer b has nothing servable for split test"):
        BufferDataset(buffer_server, "b", "test", tensorizers=SNAP)
    with pytest.raises(httpx.ConnectError, match="127.0.0.1:9"):
        BufferDataset("http://127.0.0.1:9", "b", "train", n_samples=1, tensorizers=SNAP, timeout=0.5)
    with pytest.raises(ValueError, match="tensorizers"):
        BufferDataset(buffer_server, "b", "train", n_samples=1, tensorizers=None)


def test_worker_reregisters_on_pid_change(buffer_server, monkeypatch):
    ds = BufferDataset(buffer_server, "b", "train", n_samples=5, tensorizers=SNAP, prefetch=2)
    ds[0]
    assert len(consumers(buffer_server)) == 1
    monkeypatch.setattr(client_module.os, "getpid", lambda: 999_999)
    ds[1]
    assert [c["pid"] for c in consumers(buffer_server)][-1] == 999_999 and len(consumers(buffer_server)) == 2


def test_meta_has_no_none_values(buffer_server, monkeypatch):
    def with_nones(pyramid_dir, descriptor, snapshot, name, params):
        out = fake_tensorize(pyramid_dir, descriptor, snapshot, name, params)
        out["meta"] = {**out["meta"], "record": None, "descriptor": {**descriptor, "channels": None}}
        return out

    monkeypatch.setattr(client_module, "tensorize", with_nones)
    meta = BufferDataset(buffer_server, "b", "train", n_samples=1, tensorizers=SNAP)[0]["meta"]

    def has_none(value):
        if isinstance(value, dict):
            return any(v is None or has_none(v) for v in value.values())
        return isinstance(value, list) and any(v is None or has_none(v) for v in value)

    assert "record" not in meta and "channels" not in meta["descriptor"] and not has_none(meta)


def test_collate_stacks_the_shared_keys_and_keeps_meta_as_a_list(monkeypatch):
    """A buffer mixes datasets whose label layers (and meta keys) differ; torch's default_collate needs identical
    dicts, so BufferDataset.collate stacks what every sample has and leaves the rest out."""
    from mia_buffers.client import collate

    monkeypatch.setattr(client_module, "_stack", lambda values: ("stacked", *values))  # torch/numpy stack stand-in
    a = {
        "img": "ia",
        "bbox": "ba",
        "label/nuclei": "na",
        "label/edu": "ea",
        "meta": {"plane": {"z": 1}, "normalize": 1},
    }
    b = {"img": "ib", "bbox": "bb", "label/nuclei": "nb", "meta": {"plane": {"z": 0}}}
    out = collate([a, b])
    assert out == {
        "img": ("stacked", "ia", "ib"),
        "bbox": ("stacked", "ba", "bb"),
        "label/nuclei": ("stacked", "na", "nb"),
        "meta": [a["meta"], b["meta"]],
    }
    assert BufferDataset.collate is collate
