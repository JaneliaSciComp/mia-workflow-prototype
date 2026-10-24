# mia-buffers milestone 2: buffer-api and the local-mode BufferDataset — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** one HTTP service (`buffer-api`) is the only sampler and ledger for a buffer; a `BufferDataset` in the
MuViT contract pulls crop descriptors from it and tensorises them locally through the milestone-1 harness, so the
demo notebook's training cells run on hand-registered staged volumes with the plan's weights.

**Architecture:** everything in `mia_buffers/` of this repository. `sampler.py` is pure (volumes + rng → descriptor),
`ledger.py` is the SQLite record (buffers, volumes, consumers, draws, frozen lists), `api.py` is a stdlib
`ThreadingHTTPServer` exposing spec §5's calls (no `crops/<seq>`: remote mode is milestone 6), `register.py` turns a
`plan.json` plus staged volumes into registrations, `client.py` holds `BufferClient` (httpx) and `BufferDataset`.
No new dependencies: `http.server`, `sqlite3`, `json`, `httpx` (already present). torch, muvit and `mia_harness`
are imported lazily so the planner environment stays small and the tests run without them.

**Tech Stack:** Python 3.12 in the root pixi env (`.pixi/envs/default/bin/python`, `pixi run` is broken on this
checkout), pydantic for request bodies, httpx for the client and the API tests, pytest. The real-data smoke runs
under the harness pixi env (`<harness.dir>/harness/.pixi/envs/default`, which has numpy, zarr, pydantic, httpx,
yaml) with `PYTHONPATH` pointing at this repository.

**Spec:** `notes/specs/2026-10-21-training-buffers.md` v0.5 §2 (serving-time rules: weights, uniform plane and
centre, split keys, train stream, frozen val/test), §4 (descriptor, `tensorize`), §5 (the service table and the
client), §7 (consumers are untrusted), §10 milestone 2. mia-snippets `harness/src/mia_harness/tensorize.py`
(`tensorize(pyramid_dir, descriptor, snapshot, name, params) -> Sample`) and `stage.py` (`stage.json` layout).
Consumer contract: `weigertlab/muvit` `MuViTDataset` (`ndim`, `levels`, `n_channels`, `__len__`, `__getitem__`,
`_sanity_check` on construction) and the demo notebook cells 3 to 5.

## Global Constraints

- Tests: `.pixi/envs/default/bin/python -m pytest tests -q`; lint `.pixi/envs/default/bin/ruff check mia_buffers
  tests` and `ruff format --check`; `.pixi/envs/default/bin/python -m mypy mia_buffers`. All three clean per task.
- No real paths or hostnames in tracked files (`git diff | grep -n 'rokickik\|/nrs/'` empty).
- Line length 120, descriptive names, a plain-language line per module and class, `# ponytail:` on shortcuts.
- Consumers are untrusted (§7): every request body is validated with pydantic `extra="forbid"`; a consumer can
  only name its split; it never chooses a volume or a centre.
- Commit on branch `training-buffers`; do not push or open a PR.

## Decisions the spec leaves open, fixed here

- **Descriptor** (wire form, JSON): `{"seq": int, "volume": str, "path": str, "plane": {"z": int}, "centre_nm":
  [y, x], "levels": [int], "patch": int, "channels": recipe | None}`. `path` is the staged `v<i>.zarr` directory so
  the client needs no second lookup. `seq` is per buffer, monotonic, assigned inside the ledger transaction.
- **Centre.** Level-0 pixel coordinates, uniform integers in `[F/2, extent − F/2]` per axis with `F = patch ×
  max(levels)`, converted with the level-0 scale and translation. A volume that cannot hold `F` is refused at
  registration (`422`), never silently skipped.
- **Split keys = (volume, plane).** `register` assigns each volume's planes to splits with a seeded shuffle and
  largest-remainder counts over the buffer's fractions (reusing `plan.split_counts`); a one-plane volume lands in
  the split chosen by the same shuffle over its single plane, so a dataset of many single-plane volumes still splits
  by fraction. The service draws a train centre only from a volume's train planes, so val and test planes are
  never seen in training. Spec §2's regex split keys are a later milestone.
- **Weights.** A plan row's `weight` is divided over the record's volumes in proportion to their plane counts;
  at serving time weights renormalise over the volumes that have planes in the requested split.
- **Frozen lists.** `GET split/<name>?from=&n=` generates descriptors `from..from+n` lazily from
  `numpy.random.default_rng([seed, split_index])` style seeding (stdlib `random.Random(f"{seed}:{split}")` here;
  no numpy in the service) and stores them; the buffer config carries `split_sizes` (`--val-size`, `--test-size`,
  default 256) so `len(val_ds)` is known before the list exists.
- **Heartbeat.** Every `next` and `split` call updates the consumer's `last_seen`; `status` reports consumers not
  seen for `stale_s` (default 300) as stale. Nothing is reserved per consumer, so a killed consumer costs its own
  delay only (§10's criterion) with no recovery code.
- **`BufferDataset` base class.** `muvit.data.MuViTDataset` when muvit imports, else a tiny stand-in with the same
  abstract surface and the same `_sanity_check`, so the contract is checked in both environments. Samples are
  torch tensors when torch imports, numpy arrays otherwise. `ndim` is 2 for this milestone (3D is refused).
- **`n_channels`** is the buffer's `channels` policy when it is 1 or 3, else the first volume's stored count
  (policy `all` means every volume has the same count, which the plan enforced).

## Review Focus

1. A split with no planes anywhere (`val` on a one-plane buffer that landed in train): `next` and `split` return
   `204` with `Retry-After`, `BufferDataset` raises a clear `RuntimeError` naming the split, nothing hangs. (Task 3, 5)
2. Two consumers calling `next` concurrently: every `seq` unique and every draw recorded exactly once. (Task 3)
3. A request body with an unknown field or a wrong type: `422` with the validator's message, no traceback. (Task 3)
4. The service is down or unreachable: the client fails within the httpx timeout with the URL in the message. (Task 5)
5. A volume registered twice (re-run of `register`): replaced, not duplicated; draws on the old row remain. (Task 2)

---

### Task 1: Pure sampling

**Files:**
- Create: `mia_buffers/sampler.py`
- Test: `tests/test_buffers_sampler.py`

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True)
  class Level: factor: int; shape: tuple[int, ...]; scale_nm: dict[str, float]; translation_nm: dict[str, float]
  @dataclass(frozen=True)
  class Volume:
      id: str; path: str; record: str; axes: str; shape: tuple[int, ...]; levels: tuple[Level, ...]
      split_planes: dict[str, tuple[int, ...]]; labels: dict[str, str]; weight: float
      channels_stored: int; recipe: dict | None; voxel_size_nm: dict[str, float]
      def extent(self, axis: str) -> int        # level-0 size along a letter of `axes`
      def planes(self) -> int                   # extent("z") or 1
  def footprint(patch: int, levels: list[int]) -> int                       # patch * max(levels)
  def fits(volume: Volume, patch: int, levels: list[int]) -> str | None     # None or the reason
  def servable(volumes: list[Volume], split: str) -> list[Volume]           # volumes with planes in split
  def draw(rng: random.Random, volumes: list[Volume], split: str, *, patch: int, levels: list[int],
           channels: int | str) -> dict | None                               # descriptor without seq, or None
  ```
- `draw` picks the volume with `rng.choices(weights)` over `servable`, the plane uniformly from
  `volume.split_planes[split]`, the centre as fixed in Decisions; `channels` is `volume.recipe` (already chosen by
  the plan for policy 1 or 3) and `None` for `all`.

- [ ] **Step 1: Write the failing tests**

```python
import random
from mia_buffers.sampler import Level, Volume, draw, fits, footprint, servable

def vol(vid, planes=4, extent=1024, weight=1.0, splits=None):
    lv = [Level(f, (1, planes, extent // f, extent // f), {"z": 1.0, "y": 10.0 * f, "x": 10.0 * f},
                {"z": 0.0, "y": 5.0 * (f - 1), "x": 5.0 * (f - 1)}) for f in (1, 2, 4)]
    return Volume(vid, f"/staged/{vid}", "rec", "czyx", (1, planes, extent, extent), tuple(lv),
                  splits or {"train": tuple(range(planes))}, {}, weight, 1, {"op": "channel", "index": 0}, {"y": 10.0, "x": 10.0})

def test_footprint_and_fit():
    assert footprint(256, [1, 2, 4]) == 1024
    assert fits(vol("a"), 256, [1, 2, 4]) is None
    assert fits(vol("a", extent=1000), 256, [1, 2, 4]) == "footprint 1024 exceeds 1000×1000"

def test_draw_is_in_range_and_weighted():
    volumes = [vol("a", weight=0.9), vol("b", weight=0.1, splits={"val": (0,)})]
    assert [v.id for v in servable(volumes, "train")] == ["a"] and [v.id for v in servable(volumes, "val")] == ["b"]
    rng = random.Random(0)
    d = draw(rng, volumes, "train", patch=256, levels=[1, 2, 4], channels=1)
    assert d["volume"] == "a" and d["plane"]["z"] in range(4) and d["levels"] == [1, 2, 4] and d["patch"] == 256
    assert 5120.0 <= d["centre_nm"][0] <= 10240.0 - 5120.0 and d["channels"] == {"op": "channel", "index": 0}
    assert draw(rng, volumes, "test", patch=256, levels=[1, 2, 4], channels=1) is None
    assert draw(rng, volumes, "train", patch=256, levels=[1, 2, 4], channels="all")["channels"] is None
    counts = {"a": 0, "b": 0}
    both = [vol("a", weight=0.9), vol("b", weight=0.1)]
    for _ in range(1000):
        counts[draw(rng, both, "train", patch=256, levels=[1, 2, 4], channels=1)["volume"]] += 1
    assert 850 < counts["a"] < 950
```

- [ ] **Step 2: Run them** — `pytest tests/test_buffers_sampler.py -q`. Expected: FAIL, `ModuleNotFoundError`.
- [ ] **Step 3: Implement `mia_buffers/sampler.py`** as in Interfaces. Centre: `lo = F // 2`, `hi = extent − F // 2`
  (inclusive, `hi >= lo` guaranteed by `fits`), `centre_nm = px * scale0 + translation0` per axis.
- [ ] **Step 4: Run them** — Expected: PASS. Lint and mypy clean.
- [ ] **Step 5: Commit** — `mia-buffers: pure crop sampler (volumes, fit, weighted draw)`.

### Task 2: The SQLite ledger

**Files:**
- Create: `mia_buffers/ledger.py`
- Test: `tests/test_buffers_ledger.py`

**Interfaces:**
- Consumes: `Volume`, `Level` from Task 1.
- Produces:
  ```python
  class Ledger:
      def __init__(self, path: Path): ...                  # creates tables; WAL; one lock around writes
      def put_buffer(self, buffer: str, config: dict) -> None
      def buffer(self, buffer: str) -> dict | None
      def put_volume(self, buffer: str, volume: Volume) -> None   # upsert by (buffer, volume.id)
      def volumes(self, buffer: str) -> list[Volume]
      def add_consumer(self, buffer: str, info: dict) -> str      # returns consumer_id (uuid4 hex)
      def touch(self, buffer: str, consumer_id: str) -> bool      # False when unknown
      def consumers(self, buffer: str, stale_s: float) -> list[dict]   # each with "stale": bool
      def record_draws(self, buffer: str, consumer_id: str, descriptors: list[dict]) -> list[dict]
          # assigns seq = max(seq)+1.. inside one transaction, stores, returns descriptors with "seq"
      def frozen(self, buffer: str, split: str) -> list[dict]
      def extend_frozen(self, buffer: str, split: str, descriptors: list[dict]) -> None
      def draws_per_volume(self, buffer: str) -> dict[str, int]
  ```
- Tables: `buffers(id TEXT PK, config TEXT)`, `volumes(buffer, id, data TEXT, PRIMARY KEY(buffer, id))`,
  `consumers(id TEXT PK, buffer, info TEXT, last_seen REAL)`, `draws(buffer, seq INTEGER, consumer, descriptor
  TEXT, ts REAL, PRIMARY KEY(buffer, seq))`, `frozen(buffer, split, idx INTEGER, descriptor TEXT, PRIMARY KEY(buffer,
  split, idx))`. Volumes serialise with `dataclasses.asdict` and rebuild with the Level tuples.

- [ ] **Step 1: Write the failing tests**

```python
import threading
from mia_buffers.ledger import Ledger
from tests.test_buffers_sampler import vol

def test_buffer_volume_roundtrip_and_upsert(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    assert led.buffer("b") is None
    led.put_buffer("b", {"levels": [1, 2, 4]})
    led.put_volume("b", vol("a")); led.put_volume("b", vol("a", weight=0.5))
    assert led.buffer("b") == {"levels": [1, 2, 4]} and [v.weight for v in led.volumes("b")] == [0.5]
    assert led.volumes("b")[0] == vol("a", weight=0.5)

def test_consumers_and_staleness(tmp_path):
    led = Ledger(tmp_path / "l.sqlite"); led.put_buffer("b", {})
    cid = led.add_consumer("b", {"name": "n", "host": "h", "pid": 1, "rank": 0, "split": "train", "mode": "local"})
    assert led.touch("b", cid) and not led.touch("b", "nope")
    assert led.consumers("b", stale_s=300)[0]["stale"] is False and led.consumers("b", stale_s=-1)[0]["stale"] is True

def test_draws_are_sequential_under_threads(tmp_path):
    led = Ledger(tmp_path / "l.sqlite"); led.put_buffer("b", {})
    cid = led.add_consumer("b", {"name": "n", "host": "h", "pid": 1, "rank": 0, "split": "train", "mode": "local"})
    out: list[int] = []
    def work():
        for _ in range(20):
            out.extend(d["seq"] for d in led.record_draws("b", cid, [{"volume": "a"}, {"volume": "a"}]))
    threads = [threading.Thread(target=work) for _ in range(4)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert sorted(out) == list(range(1, 161)) and led.draws_per_volume("b") == {"a": 160}

def test_frozen_lists(tmp_path):
    led = Ledger(tmp_path / "l.sqlite"); led.put_buffer("b", {})
    assert led.frozen("b", "val") == []
    led.extend_frozen("b", "val", [{"volume": "a", "seq": 0}, {"volume": "a", "seq": 1}])
    assert [d["seq"] for d in led.frozen("b", "val")] == [0, 1]
```

- [ ] **Step 2: Run** — Expected: FAIL, `ModuleNotFoundError`.
- [ ] **Step 3: Implement `mia_buffers/ledger.py`.** One `sqlite3.connect(path, check_same_thread=False)` with
  `PRAGMA journal_mode=WAL`, a `threading.Lock` around every write, `BEGIN IMMEDIATE` for `record_draws`.
  `# ponytail: one process, one connection; a pool if the service ever runs multi-process`.
- [ ] **Step 4: Run** — Expected: PASS. Lint, mypy clean.
- [ ] **Step 5: Commit** — `mia-buffers: SQLite ledger (buffers, volumes, consumers, draws, frozen lists)`.

### Task 3: buffer-api, the HTTP service

**Files:**
- Create: `mia_buffers/api.py`
- Modify: `mia_buffers/cli.py` (add `serve`)
- Test: `tests/test_buffers_api.py`

**Interfaces:**
- Consumes: `Ledger` (Task 2); `draw`, `fits`, `servable`, `Volume` (Task 1).
- Produces:
  ```python
  class BufferConfig(_Strict):   # pydantic, extra="forbid"
      spec_name: str; spec_hash: str; catalog_ref: str | None = None
      levels: list[int]; patch: int; channels: int | Literal["all"]; seed: int = 0
      tensorizer: str = "muvit-2d"; tensorizer_params: dict = {}
      splits: dict[str, float]; split_sizes: dict[str, int] = {}     # frozen list lengths for val/test
  class VolumeBody(_Strict): path: str; record: str; axes: str; shape: list[int]; levels: list[dict]
      split_planes: dict[str, list[int]]; labels: dict[str, str] = {}; weight: float
      channels_stored: int; recipe: dict | None = None; voxel_size_nm: dict[str, float]
  class ConsumerBody(_Strict): name: str; host: str; pid: int; rank: int = 0; split: str; mode: Literal["local", "remote"] = "local"
  class BufferService:
      def __init__(self, ledger: Ledger, stale_s: float = 300.0): ...
      def handle(self, method: str, path: str, query: dict[str, str], body: bytes) -> tuple[int, dict | None, dict[str, str]]
          # (status, json body or None, extra headers); all routing and errors live here, testable without sockets
  def serve(state_dir: Path, host: str, port: int) -> None   # ThreadingHTTPServer, handler delegates to BufferService.handle
  def make_server(service: BufferService, host: str, port: int) -> http.server.ThreadingHTTPServer
  ```
- Routes (status codes): `PUT /buffers/<id>` 200 · `PUT /buffers/<id>/volumes/<vol>` 200, 404 unknown buffer,
  422 when `fits` fails or axes lack `y`/`x` · `POST /buffers/<id>/consumers` 201 `{"consumer_id"}` ·
  `GET /buffers/<id>/next?n=&consumer=` 200 `{"descriptors": [...]}`, 404 unknown consumer, 204 + `Retry-After: 30`
  when `servable(volumes, split)` is empty · `GET /buffers/<id>/split/<name>?from=&n=&consumer=` 200
  `{"descriptors": [...], "size": split_sizes[name]}`, 204 when nothing servable, 416 when `from >= size` ·
  `GET /buffers/<id>/status` 200 `{"buffer", "volumes": n, "servable": {split: n}, "draws": {volume: n},
  "consumers": [...]}` · anything else 404 `{"error": ...}` · invalid body 422 `{"error": <pydantic message>}`.
  The train rng is `random.Random(config.seed)` held by the service per buffer; the frozen rng is
  `random.Random(f"{seed}:{split}")` and the list is extended on demand up to `from+n`, capped at `size`.
- `mia-buffers serve --state-dir DIR [--host 127.0.0.1] [--port 8765]`.

- [ ] **Step 1: Write the failing tests** (through `handle`, plus one socket test with httpx against `make_server`
  on port 0 in a thread):

```python
import json, threading
import httpx
from mia_buffers.api import BufferService, make_server
from mia_buffers.ledger import Ledger

CFG = {"spec_name": "s", "spec_hash": "h", "levels": [1, 2, 4], "patch": 256, "channels": 1,
       "splits": {"train": 0.8, "val": 0.2}, "split_sizes": {"val": 4}}
def body(vid="a", extent=2048, planes=5, split_planes=None):
    return {"path": f"/staged/{vid}/v0.zarr", "record": "rec", "axes": "czyx", "shape": [1, planes, extent, extent],
            "levels": [{"factor": f, "shape": [1, planes, extent // f, extent // f], "scale_nm": {"z": 1.0, "y": 10.0 * f, "x": 10.0 * f},
                        "translation_nm": {"z": 0.0, "y": 5.0 * (f - 1), "x": 5.0 * (f - 1)}} for f in (1, 2, 4)],
            "split_planes": split_planes or {"train": [0, 1, 2, 3], "val": [4]}, "weight": 1.0, "channels_stored": 1,
            "recipe": {"op": "channel", "index": 0}, "voxel_size_nm": {"y": 10.0, "x": 10.0}}
def svc(tmp_path): return BufferService(Ledger(tmp_path / "l.sqlite"))
def call(s, method, path, query=None, payload=None):
    return s.handle(method, path, query or {}, json.dumps(payload).encode() if payload is not None else b"")

def test_register_and_draw(tmp_path):
    s = svc(tmp_path)
    assert call(s, "PUT", "/buffers/b", payload=CFG)[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/a", payload=body())[0] == 200
    assert call(s, "PUT", "/buffers/b/volumes/small", payload=body("small", extent=1000))[0] == 422
    assert call(s, "PUT", "/buffers/zz/volumes/a", payload=body())[0] == 404
    status, out, _ = call(s, "POST", "/buffers/b/consumers", payload={"name": "n", "host": "h", "pid": 1, "split": "train"})
    assert status == 201
    cid = out["consumer_id"]
    status, out, _ = call(s, "GET", "/buffers/b/next", {"n": "3", "consumer": cid})
    assert status == 200 and [d["seq"] for d in out["descriptors"]] == [1, 2, 3]
    assert all(d["plane"]["z"] in (0, 1, 2, 3) and d["path"] == "/staged/a/v0.zarr" for d in out["descriptors"])
    assert call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": "nope"})[0] == 404
    st = call(s, "GET", "/buffers/b/status")[1]
    assert st["draws"] == {"a": 3} and st["servable"] == {"train": 1, "val": 1} and len(st["consumers"]) == 1

def test_frozen_split_is_stable_and_bounded(tmp_path):
    s = svc(tmp_path); call(s, "PUT", "/buffers/b", payload=CFG); call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cid = call(s, "POST", "/buffers/b/consumers", payload={"name": "n", "host": "h", "pid": 1, "split": "val"})[1]["consumer_id"]
    first = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    again = call(s, "GET", "/buffers/b/split/val", {"from": "2", "n": "10", "consumer": cid})[1]
    assert first["size"] == 4 and len(first["descriptors"]) == 4 and all(d["plane"]["z"] == 4 for d in first["descriptors"])
    assert again["descriptors"] == first["descriptors"][2:]
    assert call(s, "GET", "/buffers/b/split/val", {"from": "4", "n": "1", "consumer": cid})[0] == 416

def test_empty_split_is_204_and_bad_bodies_are_422(tmp_path):
    s = svc(tmp_path); call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload=body(split_planes={"train": [0, 1, 2, 3, 4]}))
    cid = call(s, "POST", "/buffers/b/consumers", payload={"name": "n", "host": "h", "pid": 1, "split": "val"})[1]["consumer_id"]
    status, out, headers = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})
    assert status == 204 and out is None and headers["Retry-After"] == "30"
    assert call(s, "GET", "/buffers/b/next", {"n": "1", "consumer": cid})[0] == 204
    status, out, _ = call(s, "PUT", "/buffers/b", payload={**CFG, "bogus": 1})
    assert status == 422 and "bogus" in out["error"]
    assert call(s, "GET", "/nothing")[0] == 404

def test_concurrent_next_gives_unique_seqs(tmp_path):
    s = svc(tmp_path); call(s, "PUT", "/buffers/b", payload=CFG); call(s, "PUT", "/buffers/b/volumes/a", payload=body())
    cids = [call(s, "POST", "/buffers/b/consumers", payload={"name": "n", "host": "h", "pid": i, "split": "train"})[1]["consumer_id"] for i in range(3)]
    seqs: list[int] = []
    def pull(cid):
        for _ in range(10):
            seqs.extend(d["seq"] for d in call(s, "GET", "/buffers/b/next", {"n": "4", "consumer": cid})[1]["descriptors"])
    threads = [threading.Thread(target=pull, args=(c,)) for c in cids]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert sorted(seqs) == list(range(1, 121))

def test_over_http(tmp_path):
    server = make_server(svc(tmp_path), "127.0.0.1", 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        assert httpx.put(f"{base}/buffers/b", json=CFG).status_code == 200
        assert httpx.put(f"{base}/buffers/b/volumes/a", json=body()).status_code == 200
        cid = httpx.post(f"{base}/buffers/b/consumers", json={"name": "n", "host": "h", "pid": 1, "split": "train"}).json()["consumer_id"]
        r = httpx.get(f"{base}/buffers/b/next", params={"n": 2, "consumer": cid})
        assert r.status_code == 200 and len(r.json()["descriptors"]) == 2
        assert httpx.get(f"{base}/buffers/b/status").json()["draws"] == {"a": 2}
    finally:
        server.shutdown()
```

- [ ] **Step 2: Run** — Expected: FAIL, `ModuleNotFoundError`.
- [ ] **Step 3: Implement `mia_buffers/api.py`** as in Interfaces; the `BaseHTTPRequestHandler` reads
  `Content-Length`, calls `service.handle`, writes JSON (`204` with no body), logs nothing (`log_message` is a
  no-op). Add `serve` to `cli.py` (`Ledger(state_dir / "buffers.sqlite")`, `state_dir.mkdir(parents=True)`).
- [ ] **Step 4: Run** — Expected: PASS. Lint, mypy clean.
- [ ] **Step 5: Commit** — `mia-buffers: buffer-api service (stdlib HTTP over the ledger) and serve command`.

### Task 4: `register`: plan plus staged volumes → a buffer

**Files:**
- Create: `mia_buffers/register.py`
- Modify: `mia_buffers/cli.py` (add `register`)
- Test: `tests/test_buffers_register.py`

**Interfaces:**
- Consumes: `plan.json` (`Plan.to_dict()`: rows with `id`, `in_pool` is `admitted and fit.ok`, `weight`,
  `channel_recipe`, `splits`, `n_items`), `stage.json` per staged record (`volumes[]` with `path`, `axes`, `shape`,
  `dtype`, `voxel_size_nm`, `labels`, `levels[]` with `path`, `factor`, `shape`, `scale_nm`, `translation_nm`),
  `split_counts` (plan.py), `BufferConfig`/`VolumeBody` (Task 3), `httpx`.
- Produces:
  ```python
  def buffer_config(plan: dict, spec: Spec, *, tensorizer: str, split_sizes: dict[str, int]) -> dict
  def volume_bodies(row: dict, staged: Path, spec: Spec) -> list[tuple[str, dict]]   # (volume id, VolumeBody dict)
      # volume id f"{row['id']}/{volume['path']}"; split_planes via seeded shuffle + split_counts over planes
  def register(url: str, buffer: str, plan: dict, spec: Spec, staged_root: Path, *, tensorizer: str,
               split_sizes: dict[str, int]) -> dict      # {"registered": [...], "skipped": {id: reason}, "refused": {id: error}}
  ```
- `mia-buffers register PLAN.json SPEC.yaml --staged-root DIR --url URL --buffer ID [--tensorizer muvit-2d]
  [--val-size 256] [--test-size 256]`; prints the summary as JSON; exit 0 if at least one volume registered, else 2.
  Rows not in the pool are ignored; pooled rows without `<staged_root>/<id>/stage.json` are `skipped` ("not
  staged"); a `422` from the service is `refused` with the message.

- [ ] **Step 1: Write the failing tests** (fixture `stage.json` written into `tmp_path/staged/<id>/`, service via
  `make_server` as in Task 3):

```python
def test_volume_bodies_split_planes_by_fraction(tmp_path, spec_2d):   # spec_2d fixture from test_buffers_plan
    staged = write_stage_json(tmp_path / "u937", planes=25, extent=2048, labels={"nuclei": "labels/nuclei"})
    row = {"id": "u937", "weight": 0.5, "channel_recipe": {"op": "mean"}, "splits": {"train": 20, "val": 3, "test": 2}, "n_items": 25}
    [(vid, vb)] = volume_bodies(row, staged, spec_2d)
    assert vid == "u937/v0.zarr" and vb["weight"] == 0.5 and vb["recipe"] == {"op": "mean"} and vb["labels"] == {"nuclei": "labels/nuclei"}
    assert {k: len(v) for k, v in vb["split_planes"].items()} == {"train": 20, "val": 3, "test": 2}
    assert sorted(p for ps in vb["split_planes"].values() for p in ps) == list(range(25))
    assert volume_bodies(row, staged, spec_2d) == [(vid, vb)]          # deterministic

def test_register_end_to_end(tmp_path, spec_2d):
    # one staged record, one pooled-but-unstaged record, one too-small staged record → registered / skipped / refused
    ...
    assert out["registered"] == ["u937/v0.zarr"] and out["skipped"] == {"bcss": "not staged"}
    assert "footprint" in out["refused"]["tiny/v0.zarr"]
    assert httpx.get(f"{base}/buffers/demo/status").json()["servable"] == {"train": 1, "val": 1, "test": 1}
```

- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement `mia_buffers/register.py`** and the CLI subcommand. Weight per volume =
  `row.weight × planes / total planes of the record`. Shuffle: `random.Random(f"{spec.sampling.seed}:{volume id}")`.
- [ ] **Step 4: Run** — Expected: PASS. Lint, mypy clean.
- [ ] **Step 5: Commit** — `mia-buffers: register command (plan + staged volumes → buffer-api)`.

### Task 5: The client: `BufferClient` and `BufferDataset`

**Files:**
- Create: `mia_buffers/client.py`
- Test: `tests/test_buffers_client.py`

**Interfaces:**
- Consumes: the service routes (Task 3); `mia_harness.tensorize.tensorize` (lazy import).
- Produces:
  ```python
  class BufferClient:
      def __init__(self, url: str, buffer: str, *, timeout: float = 30.0): ...
      def status(self) -> dict
      def register(self, split: str, mode: str = "local", name: str | None = None) -> str   # consumer_id
      def next(self, consumer: str, n: int) -> list[dict]           # raises BufferEmpty(split) on 204
      def split(self, consumer: str, name: str, start: int, n: int) -> tuple[list[dict], int]   # (descriptors, size)
  class BufferEmpty(RuntimeError): ...
  class BufferDataset(_Base):      # _Base = muvit.data.MuViTDataset if importable else a stand-in with _sanity_check
      def __init__(self, url: str, buffer: str, split: str = "train", n_samples: int | None = None,
                   mode: Literal["local"] = "local", tensorizers: Path | None = None, prefetch: int = 64): ...
      ndim: int = 2
      @property
      def levels(self) -> tuple[int, ...]
      @property
      def n_channels(self) -> int
      def __len__(self) -> int           # n_samples (train) or the split size (val/test)
      def __getitem__(self, idx: int) -> dict   # img, bbox, label/* , pixel_size as torch tensors or numpy; meta dict
  ```
- `tensorizers` defaults to `MIA_BUFFERS_TENSORIZERS` (the mia-snippets checkout) and is required for local mode.
  The consumer is registered lazily on first `__getitem__` and re-registered when `os.getpid()` changes (DataLoader
  workers). Train: a deque refilled with `next(prefetch)`; `idx` ignored. Val/test: `split(name, idx, 1)` with a
  small page cache of `prefetch` descriptors. Tensorising: `tensorize(Path(d["path"]), d, tensorizers,
  config["tensorizer"], {**config["tensorizer_params"], "channels": d["channels"]})`; arrays become
  `torch.from_numpy` when torch imports. `_sanity_check` runs on construction (as MuViT's metaclass does) and so
  draws one real sample; a `BufferEmpty` there becomes `RuntimeError("buffer <id> has nothing servable for split
  <name>")`.

- [ ] **Step 1: Write the failing tests** (server via `make_server` from Task 3's test helpers; `tensorize`
  monkeypatched to a fake returning numpy arrays of the right shapes from the descriptor; muvit/torch absent):

```python
def test_train_dataset_streams_and_val_is_frozen(buffer_server, monkeypatch):
    base = buffer_server  # fixture: server with buffer "b", volume "a" (planes 0..3 train, 4 val), CFG
    monkeypatch.setattr("mia_buffers.client.tensorize", fake_tensorize)
    train = BufferDataset(base, "b", "train", n_samples=10, tensorizers=Path("/snap"))
    assert len(train) == 10 and train.levels == (1, 2, 4) and train.n_channels == 1 and train.ndim == 2
    s = train[0]
    assert s["img"].shape == (3, 1, 256, 256) and s["bbox"].shape == (3, 2, 2) and s["meta"]["descriptor"]["plane"]["z"] in range(4)
    val = BufferDataset(base, "b", "val", tensorizers=Path("/snap"))
    assert len(val) == 4
    first = [val[i]["meta"]["descriptor"]["seq"] for i in range(4)]
    assert first == [val[i]["meta"]["descriptor"]["seq"] for i in range(4)]          # identical across epochs
    assert all(val[i]["meta"]["descriptor"]["plane"]["z"] == 4 for i in range(4))
    assert httpx.get(f"{base}/buffers/b/status").json()["consumers"].__len__() == 2

def test_two_consumers_share_one_ledger(buffer_server, monkeypatch):
    ... two BufferDataset("train") pulling 20 each → status draws == {"a": 40 + sanity draws}, seqs disjoint

def test_empty_split_and_unreachable_server(buffer_server, monkeypatch, tmp_path):
    with pytest.raises(RuntimeError, match="nothing servable for split test"):
        BufferDataset(buffer_server, "b", "test", tensorizers=Path("/snap"))
    with pytest.raises(httpx.ConnectError):
        BufferDataset("http://127.0.0.1:9", "b", "train", n_samples=1, tensorizers=Path("/snap"), timeout=0.5)

def test_worker_reregisters_on_pid_change(buffer_server, monkeypatch):
    ... monkeypatch os.getpid to a new value → a second consumer appears in status
```

- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement `mia_buffers/client.py`**.
- [ ] **Step 4: Run** — Expected: PASS. Lint, mypy clean (torch/muvit imports under `TYPE_CHECKING`/try).
- [ ] **Step 5: Commit** — `mia-buffers: BufferClient and the local-mode BufferDataset`.

### Task 6: Real-data smoke, docs

**Files:**
- Modify: `docs/development.md` (mia_buffers section: serve, register, BufferDataset, the notebook cell),
  `AGENTS.md` (one-line package description), `notes/specs/2026-10-21-training-buffers.md` §10 milestone 2 row
  ("done when" annotated with what was verified), `mia_buffers/cli.py` module docstring.

- [ ] **Step 1: Serve and register U937.** In the scratchpad: `mia-buffers plan` with the gray example spec over
  the catalog checkout → `plan.json`; `mia-buffers serve --state-dir <scratch>/state --port 0`-style on a free
  port in the background; `mia-buffers register plan.json spec --staged-root <work_root>/_buffers/staged --url ...
  --buffer u937-demo`. Expected: `registered: ["zenodo-20797339-u937-edu-h2ax-dapi-confocal/v0.zarr"]`, status
  `servable: {train: 1, val: 1, test: 1}`.
- [ ] **Step 2: Consume under the harness env.** `PYTHONPATH=<repo> <harness env python> -c` a script that builds
  `BufferDataset(url, "u937-demo", "train", n_samples=32, tensorizers=<harness.dir>)`, iterates all 32, times it,
  builds `val` twice and asserts identical `seq` lists, and starts a second train dataset: report samples/s, RSS,
  `status.draws`. Expected: 32 samples, `img (3, 1, 256, 256)` float32 in [0, 1] (recipe `mean` over three
  channels), `label/nuclei`, `label/gh2ax-foci`, `label/edu` present, val identical, two consumers listed.
- [ ] **Step 3: Docs.** The notebook replacement cell:
  ```python
  from mia_buffers.client import BufferDataset
  train_ds = BufferDataset(URL, "u937-demo", "train", n_samples=4000)
  val_ds = BufferDataset(URL, "u937-demo", "val")
  ```
  followed by the notebook's own `DataLoader` and `MuViTMAE2d` cells unchanged (`in_channels=train_ds.n_channels`,
  `levels=train_ds.levels`). Note that torch and muvit must be in the consumer's environment and that this
  session could not run cell 5 (no torch here).
- [ ] **Step 4: Full suite, lint, mypy; commit** — `mia-buffers: milestone 2 docs and U937 serving smoke`.
