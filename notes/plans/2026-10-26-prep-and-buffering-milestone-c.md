# Prep and buffering, milestone C: the cache — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** one writer per record's raw cache across both pipelines (a host-local lock file taken by the workers),
a fetch phase before staging (`harness fetch`, cache read-write, then an offline stage), asset rows that carry a
state and a size, and a keeper that scans the roots, evicts free assets under a quota and repairs rows whose job
died.

**Architecture:** `mia_core/locks.py` is thirty lines of `O_EXCL` + JSON + rename; both workers take
`<local_root>/locks/raw/<record>` before any job that mounts a record's cache read-write, keep it alive from
`await_job`'s heartbeat, and release it after the job's result is read. The buffer workflow gains a fetch phase per
dataset (weight-ordered, its own semaphore) and stages with the cache read-only unless the fetch reported a URL it
could not get as a whole file. Asset rows gain `state`, `bytes`, `job`, `last_used`, `scanned_at`; `PUT /assets`
merges; the service scans the roots on `POST /sync/assets`; a scheduled `MiaCacheKeeper` under the buffers worker
runs the syncs, evicts, and fixes stale builds. Nothing in the service deletes; only the worker calls `rmtree`.

**Tech Stack:** Python 3.12 stdlib (`os.open` with `O_EXCL`, `os.scandir`, `os.replace`, `shutil.rmtree`,
`sqlite3`, `http.server`), httpx, pydantic 2, Temporal (`workflow.patched`, schedules), csub via `autodev.jobs`;
pytest, ruff, mypy via pixi. The harness change lives in the mia-snippets checkout (`harness.dir` of `mia.yaml`,
branch `training-buffers`, its own `pixi run test`).

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` v0.2: §1.6, §1.7, §1.10, §3 (`assets`), §3.1
(`PUT /assets`, `GET /assets`, `GET /locks`, `POST /sync/assets`), §5.2 steps 2–3, §5.4, §5.5, §5.6, §8 (`quota`,
`limits.fetch_*`), §9 rows `mia-snippets harness`, `mia_core/`, `mia_data/activities.py, workflows.py`,
`mia_data/jobspec.py`, `mia_buffers/ledger.py, api.py`, `mia_buffers/workflows.py`, `mia_buffers/activities.py`,
`mia_buffers/cli.py`, §10 row C.

Deviations from the spec, decided here so no task re-decides them:

- **A live round exists** (`i2-e34263875481` is sourcing on the host), so §9's "no in-flight histories" no longer
  holds. Every workflow change in this milestone sits behind `workflow.patched(...)`: `"c-cache"` in
  `MiaBuffer._source_steps`, `"c-raw-lock"` in `MiaDataset`. The old branch stays verbatim; the existing replay
  fixtures stay and keep passing; a second fixture per workflow is recorded for the new branch.
- `PUT /assets/<kind>/<key>` **merges** fields into the row (like `PUT /datasets`), so the keeper's scan can
  update `bytes`/`scanned_at` without clobbering a worker's `state`/`job`, and vice versa.
- `last_used` of a staged asset is set from `draws` during `POST /sync/assets` (newest draw per artifact), not by
  an hourly service thread. `last_used` of a raw asset is set by whichever worker wrote the row last.
- `POST /sync/assets` **skips** a root that is missing or empty and reports it under `skipped`, instead of marking
  every asset of that kind `missing` (an unmounted root looks exactly like that).
- A zip member is cached as its own entry under its own key, as the harness's `io.fetch` already does; "a zip's
  members fetch the zip once" is not built. `fetch.json` lists one entry per binding URL.
- The stage job of a non-stream dataset runs **offline**: cache read-only, no hosts; the drop case (files placed by
  hand) stages offline too, because the drop already wrote `files.json`. Only a dataset whose `fetch.json` lists a
  `stream` URL stages with the cache read-write and the binding's hosts, as today.
- `gc --dry-run` is `mia-buffers assets sync --dry-run`; there is no separate `gc` command.
- The asset list stays at `GET /assets/<kind>` (existing route) and gains `?state=&record=` filters and a `bytes`
  total; the spec's `GET /assets?kind=` spelling is not added.
- Quotas are per root, so "staged before raw" in §5.4 has no effect and is not implemented.
- The per-dataset status board (`buffer_datasets`), `buffer_events`, `degraded`/`idle`/`retire`, `status`
  stat-ing pyramids, the build-under-a-temporary-name rename and planning from the registry are milestone D.
  `waiting-lock`, `fetching` and `blocked` are `DatasetStatus.status` values only, shown in the issue comment the
  way `waiting-drop` is today.
- The catalog sync the keeper runs daily reuses `cmd_catalog_sync`'s body through a shared function; its "due"
  mark is a file `<state_dir>/keeper/catalog-synced-at` (epoch seconds), not a registry column.

## Global Constraints

- Work on branch `training-buffers` in this checkout, one commit per task, no merge and no PR. Task 2 commits in
  the mia-snippets checkout at `harness.dir` of `mia.yaml`, branch `training-buffers`, same rules.
- `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` clean after every task (in mia-snippets:
  `pixi run test` from its `harness/` directory); no new `type: ignore` or `noqa` without a reason beside it.
- Line length 120; descriptive names; a one-line docstring on every new module; `# ponytail:` on deliberate
  shortcuts naming the ceiling and the upgrade path.
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` is empty before each
  commit (the one pre-existing hit is the rule's own text in `AGENTS.md`). Test fixtures use `/netfs/lab`-style
  placeholders.
- `mia_buffers/models.py` and `mia_data/models.py` dataclasses are wire format: fields are added with defaults,
  never renamed or removed. This milestone adds: `DatasetStatus.weight: float = 0.0`;
  `SourcingLimits.parallel_fetch: int = 2`, `.fetch_walltime_min: int = 480`, `.fetch_attempts: int = 4`,
  `.poll_minutes: int = 2`, `.lock_ttl_s: int = 900`; `StageJobArgs.cache_writable: bool = True`; the new
  dataclasses `FetchJobArgs`, `FetchResult`, `LockTaken`, `QuotaCheck`, `RawStatus`, `KeeperReport`; and
  `mia_data.models.RoundLimits` gains `poll_minutes: int = 2` (filled from `cfg.temporal.poll_minutes`).
- Workflow code changes only inside `workflow.patched("c-cache")` (`mia_buffers/workflows.py`) and
  `workflow.patched("c-raw-lock")` (`mia_data/workflows.py`) branches; the existing replay tests must keep
  passing unchanged; new fixtures are recorded with `MIA_BUFFERS_RECORD_HISTORY=1` / `MIA_DATA_RECORD_HISTORY=1`
  (see `docs/development.md`).
- Import direction: `mia_core` imports no application package; `mia_data → autodev`; `mia_buffers → mia_data,
  autodev`; never the reverse.
- Values from the spec: lock path `<paths.local_root>/locks/<kind>/<key>`; lock file JSON `{owner, taken_at,
  heartbeat_at, ttl_s}`; stale = `heartbeat_at + ttl_s < now`; a stale file is renamed to `<key>.stale-<int(now)>`
  before the new one is created; TTL 900 s for job locks, heartbeat with every `await_job` poll (≤ 60 s apart);
  asset states `fetching · staging · waiting-drop · ready · partial · missing · evicted`; `quota: {raw_bytes: 0,
  staged_bytes: 0, headroom_bytes: 200e9, hold_hours: 48}`, `0` = unlimited; `limits: {fetch_bytes: 50e9,
  fetch_walltime_min: 480, fetch_attempts: 4}`; `keeper.minutes` 30, `keeper.catalog_sync_hours` 24; `fetch.json`
  exactly as §5.6 (`ok`, `partial`, `bytes_fetched`, `files[] {url, outcome, bytes?, key?, reason?}`, `errors`);
  the fetch job's command `harness fetch binding.yaml --record record.yaml --out fetch.json`; eviction = free
  assets only (`refs` empty, `state == "ready"`, `last_used` older than `quota.hold_hours`, and for `raw` no
  `datasets` row `in-progress` for its record), oldest `last_used` first, until under quota minus headroom;
  `.building-*` directories older than `quota.hold_hours` are removed.
- The registry's writes from workers stay best effort except the ones the workflow depends on (artifact lookup,
  `create_buffer`, `register_dataset`, and now `check_quota`, which raises so the activity retries).
- Only worker code calls `shutil.rmtree` on the roots, and only on a path that resolves under `paths.raw_root` or
  `paths.staged_root`; the service deletes nothing.

## Review Focus

1. A lock file that is empty or holds corrupt JSON (a crash between `O_EXCL` create and the write) must count as
   stale and be taken over, never block forever (Task 1, `test_corrupt_lock_file_is_stale`).
2. A fetch job killed by the walltime leaves no `fetch.json`; `read_fetch` must report `partial`, not a failure,
   so the next attempt resumes from the cache (Task 6, `test_read_fetch_without_a_report_is_partial`).
3. The keeper must never `rmtree` a path outside the two roots even when a registry row says so, and never evict an
   asset that has a reference (Task 8, `test_evict_refuses_paths_outside_the_roots`,
   `test_plan_evictions_skips_referenced_and_held_assets`).
4. `quota.raw_bytes: 0` means unlimited: no eviction at all, whatever `headroom_bytes` says (Task 8,
   `test_zero_quota_never_evicts`).
5. `POST /sync/assets` against a missing or empty root must skip that kind, not demote every asset of it to
   `missing` (Task 3, `test_sync_assets_skips_a_missing_or_empty_root`).
6. After another owner took over a stale lock, the old owner's late heartbeat and release must not touch the new
   file (Task 1, `test_heartbeat_and_release_check_the_owner`).

---

### Task 1: `mia_core/locks.py`, `dir_bytes`, `QuotaCfg`, `mia_core/assets.py`

**Files:**
- Create: `mia_core/locks.py`, `mia_core/assets.py`
- Modify: `mia_core/fs.py` (add `dir_bytes`), `mia_core/config.py` (add `QuotaCfg`), `mia.yaml.example`
  (add a `quota:` section after `service:`, before `keeper:`), `mia_data/config.py` (`LimitsCfg` gains
  `fetch_bytes: float = 50e9`, `fetch_walltime_min: int = 480`, `fetch_attempts: int = 4`; `mia_buffers.Config`
  gains `quota: QuotaCfg = QuotaCfg()`)
- Test: `tests/test_core_locks.py` (new), `tests/test_core_assets.py` (new), `tests/test_core_fs.py`

**Interfaces:**
- Produces, `mia_core/locks.py`:
  - `NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,200}")` — kind and key must match; else `ValueError`.
  - `@dataclass class Holder: kind: str; key: str; owner: str; taken_at: float; heartbeat_at: float; ttl_s: float; stale: bool`
  - `class LockDir:`
    - `__init__(self, root: Path, now: Callable[[], float] = time.time)`
    - `path(self, kind: str, key: str) -> Path` → `root / kind / key`
    - `take(self, kind: str, key: str, owner: str, ttl_s: float) -> bool`
    - `holder(self, kind: str, key: str) -> Holder | None` (None when no file; a file with unreadable JSON is a
      `Holder` with `owner=""`, `heartbeat_at=0`, `stale=True`)
    - `heartbeat(self, kind: str, key: str, owner: str) -> bool` (False when the file is missing or names another owner)
    - `release(self, kind: str, key: str, owner: str) -> bool` (same rule; unlinks)
    - `holders(self) -> list[Holder]` (every lock file under root, `*.stale-*` excluded, sorted by kind then key)
    - `hold(self, kind: str, key: str, owner: str, ttl_s: float, timeout_s: float, poll_s: float = 2.0)` — a
      `contextmanager` that blocks (`time.sleep(poll_s)`) until `take` succeeds, raises `TimeoutError(f"{kind}/{key} held by {holder.owner}")` after `timeout_s`, and releases on exit.
- Produces, `mia_core/fs.py`: `dir_bytes(path: Path) -> int` — sum of `st_size` of regular files under `path`
  via `os.scandir`, not following symlinks; `0` for a missing path.
- Produces, `mia_core/assets.py`:
  - `ASSET_STATES = ("fetching", "staging", "waiting-drop", "ready", "partial", "missing", "evicted")`
  - `MANIFEST = "files.json"`
  - `read_files_json(cache_dir: Path) -> dict` (the harness's `files.json`: url → `{validators, source}`; `{}` when
    missing or unreadable; reads with `read_untrusted(cache_dir, MANIFEST, 1 << 20)`)
  - `whole_file_url(url: str) -> str` — the part before `::` (a zip member's zip)
  - `raw_asset(cache_dir: Path, urls: Iterable[str]) -> dict` → `{"path": str(cache_dir), "bytes": dir_bytes,
    "files": {url[:500]: {"size", "source"} for each files.json entry}, "state": "ready" if every
    `whole_file_url(url)` of `urls` is a files.json key else "partial"}` (no `urls` → `"ready"` when files.json has
    any entry, else `"partial"`).
- Produces, `mia_core/config.py`: `class QuotaCfg(BaseModel): raw_bytes: float = 0; staged_bytes: float = 0;
  headroom_bytes: float = 200e9; hold_hours: float = 48` (all `ge=0`).

- [ ] **Step 1: Write the failing lock tests** in `tests/test_core_locks.py`

```python
import json
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
    import pytest
    with pytest.raises(ValueError):
        LockDir(tmp_path).take("raw", "../etc", "me", 900)

def test_hold_blocks_then_times_out(tmp_path, monkeypatch):
    now, tick = clock()
    locks = LockDir(tmp_path, now)
    locks.take("snapshot", "abc", "other", 900)
    monkeypatch.setattr("mia_core.locks.time.sleep", lambda s: tick(s))
    import pytest
    with pytest.raises(TimeoutError, match="held by other"):
        with locks.hold("snapshot", "abc", "me", 900, timeout_s=10):
            pass
    locks.release("snapshot", "abc", "other")
    with locks.hold("snapshot", "abc", "me", 900, timeout_s=10):
        assert locks.holder("snapshot", "abc").owner == "me"
    assert locks.holder("snapshot", "abc") is None
```

- [ ] **Step 2: Run them to see them fail**: `pixi run pytest tests/test_core_locks.py -q` → `ModuleNotFoundError`.

- [ ] **Step 3: Implement `mia_core/locks.py`** per the Interfaces block. `take`: `os.open(path, O_CREAT | O_EXCL
  | O_WRONLY)`; on `FileExistsError` read the holder, return False unless stale, else `os.replace(path,
  path.with_name(f"{key}.stale-{int(now)}"))` and try the `O_EXCL` create once more (a second taker loses the
  race on `FileExistsError` and returns False). Write the JSON then `os.fsync` is not needed (local disk).
  `heartbeat`/`release`: read, compare `owner`, then write via `<path>.tmp-<pid>` + `os.replace` / `os.unlink`.

- [ ] **Step 4: Write the failing `dir_bytes` and assets tests**

```python
# tests/test_core_fs.py (append)
def test_dir_bytes_sums_regular_files_only(tmp_path):
    (tmp_path / "a").write_bytes(b"x" * 10)
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b").write_bytes(b"y" * 5)
    (tmp_path / "link").symlink_to(tmp_path / "a")
    assert fs.dir_bytes(tmp_path) == 15 and fs.dir_bytes(tmp_path / "missing") == 0

# tests/test_core_assets.py
import json
from mia_core.assets import ASSET_STATES, raw_asset, read_files_json, whole_file_url

def test_raw_asset_is_ready_when_every_binding_url_is_cached(tmp_path):
    (tmp_path / "files.json").write_text(json.dumps({
        "https://h/a.zip": {"validators": {"size": 7}, "source": "download"},
        "https://h/b.tif": {"validators": {"size": 3}, "source": "drop"}}))
    (tmp_path / "k1").write_bytes(b"1234567")
    ready = raw_asset(tmp_path, ["https://h/a.zip::m.png", "https://h/b.tif"])
    assert ready["state"] == "ready" and ready["bytes"] == 7 and ready["files"]["https://h/b.tif"]["source"] == "drop"
    assert raw_asset(tmp_path, ["https://h/a.zip", "https://h/c.tif"])["state"] == "partial"
    assert raw_asset(tmp_path / "none", [])["state"] == "partial" and read_files_json(tmp_path / "none") == {}
    assert whole_file_url("https://h/a.zip::m.png") == "https://h/a.zip" and "evicted" in ASSET_STATES
```

- [ ] **Step 5: Implement `dir_bytes`, `mia_core/assets.py`, `QuotaCfg`, the `LimitsCfg` fields, `Config.quota`,
  the `quota:` example block** (values and comments from §8: `raw_bytes: 0 # bytes; 0 = unlimited`,
  `staged_bytes: 0`, `headroom_bytes: 200e9`, `hold_hours: 48 # an asset unused this long is evictable`).

- [ ] **Step 6: Run the full checks**: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test -q` → all pass.

- [ ] **Step 7: Commit** — `mia-core: lock files on the local root, dir_bytes, asset helpers, QuotaCfg and the fetch limits`.

### Task 2: `harness fetch` in mia-snippets

**Files** (in the mia-snippets checkout at `harness.dir`, branch `training-buffers`):
- Create: `harness/src/mia_harness/fetch.py`, `harness/tests/test_fetch.py`
- Modify: `harness/src/mia_harness/cli.py` (a `fetch` subcommand, dispatched through the same `from_env` /
  `io.close()` block as `stage`)

**Interfaces:**
- Produces: `fetch(binding_path: Path, record_path: Path, io: IO) -> dict` returning exactly the §5.6 shape:
  `{"ok": bool, "partial": bool, "bytes_fetched": int, "files": [{"url", "outcome", "bytes"?, "key"?, "reason"?}],
  "errors": [str], "host_not_allowed"?: {"host", "chain"}}`. CLI: `harness fetch BINDING --record RECORD [--work
  DIR] --out fetch.json`; exit 0 when `ok` and not `partial`, 2 when `partial`, 1 otherwise.
- Consumes: `IO.fetch(url) -> Path`, `IO.bytes_fetched`, `NeedsFetch`, `BudgetExceeded`, `HostNotAllowed`
  (`mia_harness.api`), `load_binding(path).raw/.labels[].urls`.

Outcome rules, per URL of `[*binding.raw, *binding.labels]` in order, each URL once:
- `io.bytes_fetched` unchanged across `io.fetch(url)` → `cached`; grown → `fetched`; both carry `bytes` =
  the returned path's `st_size` and `key` = the path's name (the cache key).
- `NeedsFetch` → `stream` with `reason` = the exception text.
- `BudgetExceeded` → this URL and every later one get `outcome: "pending"`, `partial: true`, the loop stops; the
  exception text goes to `errors`.
- `HostNotAllowed` → `host_not_allowed: {host, chain}`, `ok: false`, the loop stops.
- any other `Exception` → `failed` with `reason` `f"{type(e).__name__}: {e}"`, the loop continues.
- `ok` = no `failed` entry and no `host_not_allowed`; `bytes_fetched` = `io.bytes_fetched`.

- [ ] **Step 1: Write the failing tests** in `harness/tests/test_fetch.py`, using `rangeserver.server` and
  `test_io.make_io` as `tests/test_io.py` does, and `test_smoke.write` for a binding whose URLs point at the server:

```python
def test_fetch_reports_fetched_then_cached(server, make_io, tmp_path):
    server.files["/a.bin"] = b"a" * 1000
    server.files["/b.bin"] = b"b" * 10
    binding, record = write_binding(tmp_path, [server.url("/a.bin")], [server.url("/b.bin")])  # helper in this file
    first = fetch(binding, record, make_io())
    assert first["ok"] and not first["partial"] and first["bytes_fetched"] == 1010
    assert [(f["outcome"], f["bytes"]) for f in first["files"]] == [("fetched", 1000), ("fetched", 10)]
    again = fetch(binding, record, make_io())
    assert [f["outcome"] for f in again["files"]] == ["cached", "cached"] and again["bytes_fetched"] == 0
    assert again["files"][0]["key"] == first["files"][0]["key"]

def test_fetch_partial_on_byte_cap_and_failed_on_404(server, make_io, tmp_path):
    server.files["/a.bin"] = b"a" * 1000
    binding, record = write_binding(tmp_path, [server.url("/a.bin"), server.url("/missing.bin")], [])
    capped = fetch(binding, record, make_io(cap=10))
    assert capped["partial"] and [f["outcome"] for f in capped["files"]] == ["pending", "pending"]
    full = fetch(binding, record, make_io())
    assert not full["ok"] and [f["outcome"] for f in full["files"]] == ["fetched", "failed"]
    assert "404" in full["files"][1]["reason"]

def test_fetch_stream_when_a_url_cannot_be_fetched_whole(server, make_io, tmp_path, monkeypatch):
    server.files["/a.bin"] = b"a" * 10
    binding, record = write_binding(tmp_path, [server.url("/a.bin"), "https://h/store.zarr/"], [])
    real = IO.fetch
    def fake_fetch(self, url):
        if url.endswith(".zarr/"):
            raise NeedsFetch("no Content-Length")
        return real(self, url)
    monkeypatch.setattr(IO, "fetch", fake_fetch)
    rep = fetch(binding, record, make_io())
    assert [f["outcome"] for f in rep["files"]] == ["fetched", "stream"] and rep["ok"] and not rep["partial"]
    assert rep["files"][1]["reason"] == "no Content-Length"

def test_fetch_host_not_allowed_stops_the_job(server, make_io, tmp_path):
    binding, record = write_binding(tmp_path, ["https://forbidden.example/x.bin", server.url("/a.bin")], [])
    rep = fetch(binding, record, make_io(hosts=("127.0.0.1",)))
    assert not rep["ok"] and rep["host_not_allowed"]["host"] == "forbidden.example" and rep["files"] == []

def test_fetch_cli_exit_codes(server, tmp_path, monkeypatch):
    server.files["/a.bin"] = b"a" * 1000
    binding, record = write_binding(tmp_path, [server.url("/a.bin")], [])
    monkeypatch.setenv("MIA_HARNESS_ALLOWED_HOSTS", "127.0.0.1")
    args = ["fetch", str(binding), "--record", str(record), "--work", str(tmp_path / "work"), "--out", str(tmp_path / "fetch.json")]
    monkeypatch.setenv("MIA_HARNESS_JOB_BYTES", "10")
    assert cli.main(args) == 2 and json.loads((tmp_path / "fetch.json").read_text())["partial"]
    monkeypatch.setenv("MIA_HARNESS_JOB_BYTES", "100000")
    assert cli.main(args) == 0 and json.loads((tmp_path / "fetch.json").read_text())["files"][0]["outcome"] == "fetched"
```

`write_binding(tmp_path, raw_urls, label_urls) -> (binding_path, record_path)` is a helper in this test file,
built the way `test_smoke.write` builds its binding (same snippet names and `params`), with the given URLs.

- [ ] **Step 2: Run** `pixi run test tests/test_fetch.py -q` (from `harness/`) → import error.
- [ ] **Step 3: Implement `fetch.py` and the CLI subcommand.** The CLI block mirrors `stage`'s: `io =
  from_env(args.work)`, result written to `--out` in a `finally` so a partial report always lands.
- [ ] **Step 4: Run** the harness suite: `pixi run test` → passes.
- [ ] **Step 5: Commit** in mia-snippets — `harness: fetch — every URL of a binding into the raw cache, fetch.json
  with per-file outcomes`. Record the new HEAD sha in the task report: Task 6's live note needs it (a buffer round
  admitted after this commit pins it automatically through `harness_pin`).

### Task 3: asset states in the ledger and the API; `scan_assets`; `GET /locks`; the registry client

**Files:**
- Modify: `mia_buffers/ledger.py`, `mia_buffers/api.py`, `mia_buffers/sync.py`, `mia_core/registry.py`
- Test: `tests/test_buffers_ledger.py`, `tests/test_buffers_api.py`, `tests/test_buffers_sync.py`,
  `tests/test_core_registry.py`

**Interfaces:**
- Consumes: `mia_core.assets.ASSET_STATES`, `read_files_json`, `mia_core.fs.dir_bytes`, `mia_core.locks.LockDir`,
  `mia_core.untrusted.loads`/`read_untrusted` (Task 1).
- Produces, `Ledger`:
  - `put_asset(self, kind: str, key: str, data: dict) -> dict` — merges `data` into the existing row's JSON
    (new row when none), sets `updated`, returns the merged dict. Callers that need a full replace pass every field.
  - `assets(self, kind: str, state: str | None = None, record: str | None = None) -> list[dict]` — as now
    (`{key, data, refs}`), filtered on `data["state"]` / `data["record"]` when given; a ref-only row (no data)
    passes no filter.
  - `asset_bytes(self, kind: str) -> int` — Σ `data["bytes"]` over rows whose state is not `missing`/`evicted`.
  - `sync_assets(self, found: dict[tuple[str, str], dict], *, older_than: float | None = None,
    kinds: tuple[str, ...] = ("raw", "staged")) -> dict` — for each `(kind, key)` in `found`: merge `path`,
    `bytes`, `scanned_at`, and the scan's extra fields (`record`, `levels_key`, `artifact`, `binding_hash`,
    `harness_commit`, `env_digest`, `voxel_size_nm`, `files`); set `state = "ready"` when the row is new or its
    state is `missing`/`evicted`; otherwise keep the state. Every row of a kind in `kinds` that is not in `found`,
    whose state is not in (`fetching`, `staging`, `waiting-drop`, `evicted`) and whose `updated` is older than
    `older_than` becomes `missing`. Then `staged_last_used()` is applied. Returns `{"scanned": n, "new": n,
    "missing": n}`.
  - `staged_last_used(self) -> dict[str, float]` — artifact id → newest draw `ts` among the volumes carrying that
    `artifact` (join `volumes.data` JSON with `draws` on the descriptor's volume id as `record_draws` stores it);
    `sync_assets` merges it as `last_used` when newer than the row's.
  - `in_progress_records(self) -> set[str]` — dataset ids whose state is `in-progress`.
- Produces, `mia_buffers/sync.py`:
  - `scan_assets(raw_root: Path, staged_root: Path, known: dict[tuple[str, str], dict]) -> tuple[dict[tuple[str,
    str], dict], list[Path]]` — raw: every non-dot directory `raw_root/<record>` → `("raw", record)`: `{"path",
    "bytes", "files"}` (`files` from `read_files_json`, trimmed as `raw_asset` trims); staged: every
    `staged_root/<levels key>/<record>/<artifact>/stage.json` whose name is not `.building-*` → `("staged",
    artifact)`: `{"path", "bytes", "record", "levels_key", "artifact"}` plus the `artifact` block of `stage.json`
    when it has one (`binding_hash`, `harness_commit`, `env_digest`, `voxel_size_nm`). A `stage.json` directly
    under `<levels key>/<record>/` (the pre-artifact layout) is skipped with a log line. The second element lists
    every `.building-*` directory found. `bytes` is re-summed only when the directory's `st_mtime` is newer than
    `known[(kind, key)]["scanned_at"]`; otherwise `known`'s `bytes` is reused (`# ponytail:` a nested write does
    not bump the top mtime; the keeper's full re-sum every N passes is the upgrade). A root that is missing or has
    no entries contributes nothing and its kind is returned in a third element: make the return
    `(found, building, skipped_kinds: list[str])`.
- Produces, `mia_buffers/api.py`:
  - `BufferService.__init__(..., storage_root=None, raw_root: Path | None = None, staged_root: Path | None =
    None, locks: LockDir | None = None)`.
  - `PUT /assets/<kind>/<key>`: body must be a JSON object (422 otherwise); `state`, when present, must be in
    `ASSET_STATES` (422); merges; returns the merged row.
  - `GET /assets/<kind>?state=&record=` → `{"kind", "assets": [...], "bytes": asset_bytes(kind)}`.
  - `GET /locks` (read) → `{"locks": [asdict(holder) ...]}`; `[]` when `locks` is None.
  - `POST /sync/assets` (worker): 409 while a sync runs (the same `_sync_lock` as datasets); runs
    `scan_assets(raw_root, staged_root, known)` with `known` = current rows, then `ledger.sync_assets(found,
    older_than=started, kinds=the kinds not skipped)`; returns the counts plus `{"building": [str], "skipped":
    [kind]}`. 503 only when both roots are unconfigured (`None`).
  - `serve(cfg)` passes `raw_root=cfg.paths.raw_root`, `staged_root=cfg.paths.staged_root`,
    `locks=LockDir(cfg.paths.local_root / "locks")`.
- Produces, `mia_core/registry.py`: `assets(self, kind: str, state: str | None = None, record: str | None =
  None) -> dict` (the whole body), `sync_assets(self) -> dict`, `locks(self) -> list[dict]`,
  `in_progress_records` is **not** a route: the keeper reads `datasets(state="in-progress")`.

- [ ] **Step 1: Write the failing ledger tests** (append to `tests/test_buffers_ledger.py`)

```python
def test_put_asset_merges_and_filters(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("raw", "rec", {"state": "fetching", "job": "1", "path": "/netfs/lab/raw/rec"})
    merged = ledger.put_asset("raw", "rec", {"bytes": 10, "scanned_at": 5.0})
    assert merged["state"] == "fetching" and merged["job"] == "1" and merged["bytes"] == 10
    ledger.put_asset("staged", "art1", {"state": "ready", "record": "rec", "bytes": 100})
    ledger.put_asset("staged", "art2", {"state": "evicted", "record": "rec", "bytes": 0})
    assert [a["key"] for a in ledger.assets("staged", state="ready")] == ["art1"]
    assert [a["key"] for a in ledger.assets("staged", record="rec")] == ["art1", "art2"]
    assert ledger.asset_bytes("staged") == 100

def test_sync_assets_marks_missing_but_keeps_building_rows(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_asset("staged", "gone", {"state": "ready", "bytes": 5})
    ledger.put_asset("staged", "building", {"state": "staging", "job": "7"})
    ledger.put_asset("staged", "back", {"state": "missing"})
    found = {("staged", "back"): {"path": "/netfs/lab/s/p/rec/back", "bytes": 3, "scanned_at": 9.0},
             ("staged", "new"): {"path": "/netfs/lab/s/p/rec/new", "bytes": 4, "scanned_at": 9.0, "record": "rec"}}
    counts = ledger.sync_assets(found, older_than=time.time() + 1)
    by_key = {a["key"]: a["data"] for a in ledger.assets("staged")}
    assert by_key["gone"]["state"] == "missing" and by_key["building"]["state"] == "staging"
    assert by_key["back"]["state"] == "ready" and by_key["new"]["state"] == "ready" and by_key["new"]["record"] == "rec"
    assert counts == {"scanned": 2, "new": 1, "missing": 1}
    # a row written after the scan started is left alone
    ledger.put_asset("staged", "late", {"state": "ready"})
    ledger.sync_assets({}, older_than=time.time() - 60)
    assert {a["key"]: a["data"]["state"] for a in ledger.assets("staged")}["late"] == "ready"

def test_staged_last_used_comes_from_draws(tmp_path):
    ... # put_buffer + put_volume with artifact "art1"; record_draws for that volume; sync_assets({}) sets
        # assets("staged")[art1]["last_used"] == the draw's ts (build the volume with register.volume_bodies or a
        # Volume literal as the existing ledger tests do)
```

- [ ] **Step 2: Write the failing sync and API tests**

```python
# tests/test_buffers_sync.py (append)
def test_scan_assets_finds_raw_and_artifact_dirs_and_reuses_sizes(tmp_path):
    raw, staged = tmp_path / "raw", tmp_path / "staged"
    (raw / "rec").mkdir(parents=True); (raw / "rec" / "k").write_bytes(b"x" * 8)
    (raw / "rec" / "files.json").write_text(json.dumps({"https://h/a": {"validators": {"size": 8}, "source": "download"}}))
    art = staged / "p256-l1-2" / "rec" / "0123456789abcdef"; art.mkdir(parents=True)
    (art / "stage.json").write_text(json.dumps({"ok": True, "artifact": {"binding_hash": "b", "harness_commit": "h", "env_digest": "e", "voxel_size_nm": {"y": 4.0, "x": 4.0}}}))
    (art / "v0.zarr").mkdir(); (art / "v0.zarr" / "c").write_bytes(b"y" * 20)
    (staged / "p256-l1-2" / "rec" / ".building-0123456789abcdef-job").mkdir()
    (staged / "p256-l1-2" / "old" ).mkdir(); (staged / "p256-l1-2" / "old" / "stage.json").write_text("{}")  # pre-artifact layout
    found, building, skipped = scan_assets(raw, staged, known={})
    assert found[("raw", "rec")]["bytes"] == dir_bytes(raw / "rec") and "https://h/a" in found[("raw", "rec")]["files"]
    staged_row = found[("staged", "0123456789abcdef")]
    assert staged_row.items() >= {"record": "rec", "levels_key": "p256-l1-2", "binding_hash": "b", "harness_commit": "h"}.items()
    assert staged_row["bytes"] == dir_bytes(art) and [p.name for p in building] == [".building-0123456789abcdef-job"]
    assert skipped == [] and ("staged", "old") not in found
    known = {("staged", "0123456789abcdef"): {"bytes": 1, "scanned_at": time.time() + 100}}
    assert scan_assets(raw, staged, known)[0][("staged", "0123456789abcdef")]["bytes"] == 1  # mtime older: reused
    assert scan_assets(tmp_path / "none", staged, {})[2] == ["raw"]
    (tmp_path / "empty").mkdir()
    assert scan_assets(tmp_path / "empty", staged, {})[2] == ["raw"]

# tests/test_buffers_api.py (append; use this file's `svc`/`call` helpers; the token fixture for worker routes)
def test_assets_merge_filter_and_totals(tmp_path): ...  # PUT merges, bad state 422, non-object 422,
                                                        # GET /assets/staged?state=ready filters, "bytes" total
def test_locks_route_lists_holders(tmp_path): ...       # BufferService(..., locks=LockDir(tmp_path / "locks")): take one, GET /locks shows it; without locks → []
def test_sync_assets_skips_a_missing_or_empty_root(tmp_path): ...  # staged_root missing: rows of kind staged untouched, response["skipped"] == ["staged"]; raw synced
def test_sync_assets_is_exclusive(tmp_path): ...        # hold service._sync_lock; POST → 409
```

- [ ] **Step 3: Write the failing client tests** (append to `tests/test_core_registry.py`): `assets("staged",
  state="ready")` sends `?state=ready` and returns the body; `sync_assets()` POSTs `/sync/assets`; `locks()`
  returns `body["locks"]`.

- [ ] **Step 4: Run** the four files → failures on the new names.
- [ ] **Step 5: Implement** ledger, sync, API, client, `serve` wiring.
- [ ] **Step 6: Full checks** → pass.
- [ ] **Step 7: Commit** — `buffer-api: asset states and sizes, merged PUT, assets sync, GET /locks`.

### Task 4: the snapshot locks move off `fcntl`

**Files:**
- Modify: `mia_data/jobspec.py` (`archive_snapshot`, `harness_snapshot`; add `lock_dir(cfg) -> LockDir`),
  `tests/mia_helpers.py` and any `make_cfg` that lacks `paths.local_root` (set it to `tmp_path / "local"`)
- Test: `tests/test_mia_jobspec.py` (or wherever `archive_snapshot`/`harness_snapshot` are tested today — find with
  `grep -rn "archive_snapshot\|harness_snapshot" tests/`)

**Interfaces:**
- Produces: `lock_dir(cfg: Config) -> LockDir` → `LockDir(cfg.paths.local_root / "locks")`; raises
  `RuntimeError("paths.local_root is not set: locks need host-local disk")` when `local_root` is None.
- Both snapshot functions wrap fetch-and-extract in `lock_dir(cfg).hold("snapshot", sha, owner=f"{cfg.deployment}
  pid {os.getpid()}", ttl_s=900, timeout_s=3600)`; the `.lock` files beside `dest` and `import fcntl` go.

- [ ] **Step 1: Write the failing test**: two threads calling `harness_snapshot(cfg, commit)` on a slow fake `git
  archive` (monkeypatch `subprocess.run`) extract once; the lock file `<local_root>/locks/snapshot/<commit>` exists
  during the extraction and is gone after; a `cfg` with `local_root=None` raises `RuntimeError` mentioning
  `paths.local_root`.
- [ ] **Step 2: Run** → fails (no `lock_dir`).
- [ ] **Step 3: Implement**; run the whole suite — every test config must now carry `local_root`.
- [ ] **Step 4: Commit** — `mia-data: snapshot extraction serialised by a lock file on the local root, not fcntl on NFS`.

### Task 5: mia-data takes the record's lock around cache-writing jobs and writes the raw asset

**Files:**
- Modify: `mia_data/activities.py` (new activities `take_lock`, `release_lock`; `await_job(job_id, workspace,
  lock: list[str] | None = None)`; `read_smoke` and `read_verify` end with a best-effort `put_asset("raw")`;
  the `limits` activity's result gains `poll_minutes`), `mia_data/models.py` (`LockTaken` dataclass; the limits
  dataclass gains `poll_minutes: int = 2`), `mia_data/workflows.py` (`_hold_raw`, `_release_raw`; smoke and trusted
  crop under `workflow.patched("c-raw-lock")`), `docs/mia-data-workflow.md` (a paragraph under "The steps" 3 and 6:
  the lock, what the comment says while waiting)
- Test: `tests/test_mia_activities.py` (or the file that tests mia-data activities today), `tests/test_mia_workflows.py`,
  new fixture `tests/fixtures/mia/dataset_history_c.json`

**Interfaces:**
- Produces (activities, both deployments use the same shapes, defined in `mia_data/models.py`):
  - `@dataclass class LockTaken: ok: bool; holder: str = ""`
  - `take_lock(self, kind: str, key: str, owner: str, ttl_s: int) -> LockTaken` → `lock_dir(cfg).take(...)`;
    on False `holder` = the current holder's owner string.
  - `release_lock(self, kind: str, key: str, owner: str) -> None`
  - `await_job(self, job_id, workspace, lock=None)`: when `lock` is `[kind, key, owner]`, the heartbeat callback
    also calls `lock_dir(cfg).heartbeat(kind, key, owner)` (a False result is logged once, not raised).
- Workflow: `owner = f"mia-data {state.round_id} {kind}"`; `_hold_raw(kind)`: loop `take_lock("raw",
  state.pins.record, owner, 900)`; on `ok=False` comment once per round (marker `f"lock {round_id}"`): ``waiting
  for the raw cache of `<record>`: held by <holder>``, then `workflow.sleep(timedelta(minutes=limits.poll_minutes))`
  and retry. Smoke: `_hold_raw("smoke")` before `_prepare("smoke")`, `await_job` with the lock, `release_lock` in
  a `finally` after `read_smoke`. Crop with `trusted=True`: the same around `_crop`'s crop job + `read_verify`
  (the verify job mounts no cache; release before it is fine, but releasing after `read_verify` keeps one shape).
  Untrusted crops (a PR head) and the match agent take no lock.
- `read_smoke` / `read_verify` (trusted path): `best_effort(lambda: registry.put_asset("raw", record,
  {**raw_asset(cache_dir, urls), "last_used": time.time()}), ...)` when `self.registry` is set; `urls` are the
  `raw` and `labels` URLs of the `binding.yaml` in that job's workspace; `cache_dir` = `cfg.paths.work_root /
  "_cache" / record` (what `_spec` mounts); `record` = the round's `pins.record`.

- [ ] **Step 1: Write the failing activity tests**: `take_lock` twice with different owners → second `LockTaken(False,
  holder="<first owner>")`; `await_job(..., lock=[...])` with a fake `jobs.wait` that calls the heartbeat twice →
  the lock file's `heartbeat_at` advanced; `read_smoke` on a workspace with a cache holding `files.json` →
  the fake registry saw `PUT /assets/raw/<record>` with `state: ready` and `last_used`.
- [ ] **Step 2: Write the failing workflow test** (`tests/test_mia_workflows.py`, the fake-activities style of
  that file): with `take_lock` returning `ok=False` once then `ok=True`, a round comments the waiting line once,
  submits the smoke job only after the second take, passes `lock=["raw", record, owner]` to `await_job`, and
  calls `release_lock` even when `read_smoke` reports `ok=False`.
- [ ] **Step 3: Run** → fail.
- [ ] **Step 4: Implement** under `workflow.patched("c-raw-lock")`; `test_replay_dataset_history` and
  `test_replay_develop_history` must still pass against the existing fixtures; record
  `dataset_history_c.json` (`MIA_DATA_RECORD_HISTORY=1 ...`) and add `test_replay_dataset_history_c`.
- [ ] **Step 5: Docs**; full checks → pass.
- [ ] **Step 6: Commit** — `mia-data: the record's raw-cache lock around smoke and trusted crop jobs; raw asset rows`.

### Task 6: mia-buffers activities for the fetch phase, locks, quotas and asset states

**Files:**
- Modify: `mia_buffers/models.py` (`DatasetStatus.weight`, `SourcingLimits` fields, `StageJobArgs.cache_writable`,
  new `FetchJobArgs`, `FetchResult`, `QuotaCheck`, `RawStatus`; `LockTaken` is imported from `mia_data.models`),
  `mia_buffers/jobspec.py` (`fetch_job`; `stage_job` honours `cache_writable`), `mia_buffers/plan.py`
  (`Row.bytes: int | None = None` from `Facts.bytes`), `mia_buffers/activities.py`
- Test: `tests/test_buffers_jobspec.py`, `tests/test_buffers_activities.py`, `tests/test_buffers_plan.py`

**Interfaces:**
- Produces (models):
  - `FetchJobArgs(workspace: str, round_id: str, pins: BufferPins, record: str, hosts: list[str], attempt: int)`
  - `FetchResult(ok: bool, partial: bool = False, bytes_fetched: int = 0, stream: list[str] =
    field(default_factory=list), failed: list[str] = field(default_factory=list), errors: list[str] =
    field(default_factory=list), host_not_allowed: str | None = None)`
  - `QuotaCheck(ok: bool, over_by: int = 0, expected: int = 0)`
  - `RawStatus(ready: bool, missing: list[str] = field(default_factory=list))`
  - `SourcingLimits(..., parallel_fetch=cfg.sourcing.parallel_fetch, fetch_walltime_min=cfg.limits.fetch_walltime_min,
    fetch_attempts=cfg.limits.fetch_attempts, poll_minutes=cfg.temporal.poll_minutes, lock_ttl_s=900)`;
    `SourcingCfg.parallel_fetch: int = Field(default=2, ge=1)` in `mia_buffers/config.py`.
- Produces (`jobspec.py`): `fetch_job(cfg, args: FetchJobArgs, snapshot: Path) -> JobSpec` — body
  `harness fetch binding.yaml --record record.yaml --out fetch.json`, hosts as given, cache read-write
  (`rw_mounts` + `MIA_HARNESS_CACHE_WRITABLE=1`), `walltime_min = cfg.limits.fetch_walltime_min`,
  `env["MIA_HARNESS_JOB_BYTES"] = str(int(cfg.limits.fetch_bytes))`, job name `fetch`. `stage_job`: when
  `args.cache_writable` is False the cache goes in `ro_mounts` with `MIA_HARNESS_CACHE_WRITABLE=0` (hosts are
  whatever the caller passed; the workflow passes `[]` for an offline stage).
- Produces (activities):
  - `take_lock(kind, key, owner, ttl_s) -> LockTaken`, `release_lock(kind, key, owner) -> None` — same bodies as
    mia-data's (share the implementation: a small `mia_core/locks.py`-level helper is fine, or copy the eight lines).
  - `await_job(job_id, workspace, lock=None)` as in Task 5.
  - `raw_status(round_id, record) -> RawStatus` — `raw_asset(cache_dir, binding urls)["state"] == "ready"`;
    `missing` = the whole-file URLs not in `files.json`.
  - `check_quota(round_id, record) -> QuotaCheck` — `quota.raw_bytes == 0` → ok; else `used = registry.assets("raw")["bytes"]`,
    `expected = plan row's bytes or 0`; ok when `used + expected <= raw_bytes`; `over_by = used + expected - raw_bytes`.
    A `RegistryError` propagates (the activity retries).
  - `prepare_fetch(issue, round_id, record, attempt) -> str` — workspace `<round workspace>/fetch-<record>-<attempt>`
    with `record.yaml` and `binding.yaml` (as `prepare_stage` does).
  - `submit_fetch(key, args: FetchJobArgs) -> str` — like `submit` but builds `fetch_job`; before submitting,
    best-effort `put_asset("raw", record, {"state": "fetching", "job": <id>})` after the id is known.
  - `read_fetch(round_id, record, workspace) -> FetchResult` — `fetch.json` via `read_untrusted`; missing or not an
    object → `FetchResult(ok=True, partial=True, errors=["fetch.json missing: the job ended before writing it"])`;
    else map the fields (`stream` = urls with outcome `stream`, `failed` = outcome `failed`). Then best-effort
    `put_asset("raw", record, {**raw_asset(cache_dir, urls), "state": "ready" if not partial and ok else "partial",
    "job": None, "last_used": time.time()})`.
  - `mark_asset(kind, key, fields: dict) -> None` — best-effort `put_asset` (the workflow uses it for
    `waiting-drop` and `staging`).
  - `read_stage` → `_record_assets` sets the staged row's `state: "ready"` (ok) or `"partial"` (not ok),
    `bytes = dir_bytes(staged dir)`, `last_used = time.time()`, `job: None`; the raw row gets `last_used` too.
  - `submit` (stage): best-effort `mark_asset("staged", artifact, {"state": "staging", "job": <id>, "record", "levels_key", "path"})`.
- Consumes: `LockDir`, `raw_asset`, `dir_bytes` (Task 1); `Registry.assets` (Task 3).

- [ ] **Step 1: Write the failing jobspec tests**: `fetch_job` command string, walltime `480`, `MIA_HARNESS_JOB_BYTES
  == "50000000000"`, cache in `rw_mounts`; `stage_job(cache_writable=False)` puts the cache in `ro_mounts` with
  `MIA_HARNESS_CACHE_WRITABLE == "0"`; `cache_writable=True` unchanged.
- [ ] **Step 2: Write the failing activity tests** (in the `acts`/`server` fixture style):

```python
async def test_raw_status_reads_files_json(acts): ...   # binding with two urls; files.json with one → RawStatus(False, [the other]); both → True
async def test_check_quota(acts, server, monkeypatch): ...  # quota 0 → ok; raw_bytes=100 with a raw asset of bytes 90 and a plan row bytes 20 → QuotaCheck(False, over_by=10, expected=20)
async def test_read_fetch_without_a_report_is_partial(acts, server): ...  # no fetch.json → partial, ok; asset row state "partial"
async def test_read_fetch_maps_outcomes_and_writes_the_raw_row(acts, server): ...  # fetch.json with cached/fetched/stream/failed → FetchResult fields; row state ready when ok and not partial
async def test_take_and_release_lock(acts): ...          # second owner sees the first's owner string
async def test_read_stage_sets_state_bytes_and_last_used(acts, server): ...
```

- [ ] **Step 3: Run** → fail; **Step 4: Implement**; **Step 5: Full checks** → pass.
- [ ] **Step 6: Commit** — `mia-buffers: fetch job, lock and quota activities, asset rows with state and size`.

### Task 7: the buffer workflow: lock, fetch phase, offline stage, blocked, weight order

**Files:**
- Modify: `mia_buffers/workflows.py` (`_sourcing`, `_source`, `_source_steps`, new `_lock`, `_quota`, `_fetch`,
  `_job` gains `cache_writable` and `lock`; `WAITING` gains `"blocked"`), `mia_buffers/models.py`
  (`DatasetStatus` docstring lists `waiting-lock | fetching | blocked`; `fetch_job_key`); the workflow's `_round`
  builds `DatasetStatus(..., weight=dataset.weight)` from `DatasetPlan.weight` (workflows.py line ~207)
- Test: `tests/test_buffers_workflows.py`, new fixture `tests/fixtures/buffers/buffer_history_c.json`

**Interfaces:**
- Consumes: Task 6's activities and dataclasses.
- Behaviour under `workflow.patched("c-cache")` (the `else` branch is today's code, untouched):
  1. `read_binding` and the `waiting-dataset` path exactly as now; `binding.manual` → `_await_drop` as now, with
     `mark_asset("raw", record, {"state": "waiting-drop"})` first.
  2. `binding.staged` → straight to `register_dataset` (no lock, no job), as now.
  3. `owner = f"mia-buffers {round_id} {record}"`; `_lock(dataset)`: loop `take_lock("raw", record, owner,
     limits.lock_ttl_s)`; on `ok=False`: `dataset.status, dataset.reason = "waiting-lock", f"raw cache held by
     {holder}"`, `workflow.sleep(timedelta(minutes=limits.poll_minutes))`, retry. Then `try: … finally:
     release_lock`.
  4. With the lock: `read_binding` again; `staged` now → register (the holder built it; comment says so).
  5. Else `raw = raw_status(...)`. If `not raw.ready and not binding.manual`: under the **fetch** semaphore
     (`asyncio.Semaphore(limits.parallel_fetch)`, created in `_sourcing`): `dataset.status = "fetching"`;
     `_quota(dataset)`: loop `check_quota`; on `ok=False`: `dataset.status, dataset.reason = "blocked", f"raw quota
     over by {over_by} bytes"`, comment once (marker `f"dataset {round_id} {record} blocked"`), sleep
     `poll_minutes`, retry. Then attempts `1..limits.fetch_attempts`: `prepare_fetch`, `submit_fetch` with key
     `fetch_job_key(issue, round_id, record, attempt)` — a new helper beside `job_key` in `mia_buffers/models.py`
     returning `f"buf-{issue}-{round_id}-fetch{attempt}-{record[:40]}-{digest}"` (same digest rule; stays within
     `JOB_KEY_RE`'s 120 characters), `await_job` on the await queue with walltime
     `limits.fetch_walltime_min` and `lock=["raw", record, owner]`, `read_fetch`. `host_not_allowed` or
     `not ok` → `_fail(dataset, …)`; `partial` → next attempt; after the last attempt still partial → `_fail(dataset,
     f"fetch still partial after {n} attempts")`. Keep `result.stream`.
  6. Under the **stage** semaphore: `dataset.status = "staging"`, `prepare_stage`, `_job(record, workspace,
     hosts=binding.hosts if stream else [], voxel, cache_writable=bool(stream), lock=[...])`, `read_stage`, then
     register as now. The `_job` helper passes `cache_writable` into `StageJobArgs` and `lock` into `await_job`.
  7. `_sourcing` creates the tasks in **descending `dataset.weight`** order (stable on record id), and `WAITING`
     includes `"blocked"` so the day-limit drop covers it; `waiting-lock` is not dropped at the timeout.
- Comment text for a reused artifact built by the lock's previous holder: `` `<record>` is servable: n volume(s)
  registered with buffer `<id>`. The pyramid was staged by <holder> while this buffer waited and reused.``

- [ ] **Step 1: Write the failing workflow tests** in the `Mocks` style of `tests/test_buffers_workflows.py`:
  - `test_two_buffers_one_record_second_waits_then_reuses`: one `MiaBuffer` run; `take_lock` fake answers
    `LockTaken(False, "mia-buffers i9-e9 rec")` twice then `ok=True`; `read_binding` answers `staged=False` before
    the lock and `staged=True` after it; assert the dataset passed through `waiting-lock` with the holder in
    `reason` (query `status` while sleeping, or record the status at each `take_lock` call), no `submit_fetch`
    and no `submit` call happened, `release_lock` was called, the servable comment mentions the holder.
  - `test_fetch_then_offline_stage`: `raw_status` → not ready; `read_fetch` → `partial` on attempt 1, ok on attempt
    2; assert two `submit_fetch` calls with attempts 1 and 2, `await_job` received `lock=["raw", rec, owner]`, the
    stage `StageJobArgs.cache_writable is False` and `hosts == []`, then `register_dataset`.
  - `test_stream_url_keeps_the_stage_online`: `read_fetch` → `stream=["https://h/x.zarr/"]` → `cache_writable
    is True` and `hosts == binding.hosts`.
  - `test_blocked_waits_for_quota_then_fetches`: `check_quota` → `QuotaCheck(False, over_by=5)` once then ok;
    status `blocked` with `"over by 5 bytes"` seen; one blocked comment; then the fetch runs.
  - `test_heaviest_dataset_is_sourced_first`: two datasets, weights 0.2 and 0.8 → the first `read_binding` call is
    the heavy one.
  - `test_replay_buffer_history` unchanged and passing; new `test_replay_buffer_history_c` on the recorded fixture.
- [ ] **Step 2: Run** → fail. **Step 3: Implement**. **Step 4: Record the fixture**
  (`MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay`, following the
  recording test's own instructions so the old fixture file is not overwritten — add a second recording path for
  the new fixture if the recorder writes a fixed name). **Step 5: Full checks** → pass.
- [ ] **Step 6: Commit** — `mia-buffers: lock, fetch phase and offline stage per dataset; blocked on quota; heaviest first`.

### Task 8: the keeper

**Files:**
- Create: `mia_buffers/keeper.py`, `tests/test_buffers_keeper.py`
- Modify: `mia_buffers/activities.py` (`keep` activity), `mia_buffers/workflows.py` (`MiaCacheKeeper`),
  `mia_buffers/cli.py` (`cmd_schedule` applies `<deployment>-keeper`; `cmd_worker` registers the workflow;
  `assets sync [--dry-run]`; `catalog_sync(cfg, ref=None) -> tuple[str, int]` extracted from `cmd_catalog_sync` so the
  keeper can call it), `mia_buffers/models.py` (`KeeperReport`)
- Test: `tests/test_buffers_keeper.py`, `tests/test_buffers_cli.py`, `tests/test_buffers_workflows.py`

**Interfaces:**
- Produces, `mia_buffers/keeper.py` (pure, no I/O except where named):
  - `free_assets(assets: list[dict], *, hold_s: float, now: float, in_progress: set[str], kind: str) -> list[dict]`
    — rows with `refs == []`, `state == "ready"`, `(last_used or scanned_at or 0) + hold_s < now`, and for
    `kind == "raw"` `record not in in_progress` (a raw row's record is its key); sorted by that time ascending.
  - `plan_evictions(assets: list[dict], *, quota_bytes: float, headroom_bytes: float, hold_s: float, now: float,
    in_progress: set[str], kind: str) -> list[dict]` — `[]` when `quota_bytes <= 0`; otherwise take `free_assets`
    in order until `live_bytes − Σ evicted ≤ quota_bytes − headroom_bytes`, where `live_bytes` = Σ `bytes` of rows
    whose state is not `missing`/`evicted`.
  - `evict_path(path: Path, roots: tuple[Path, ...]) -> bool` — `rmtree` only when `path.resolve()` is relative to
    one of `roots` (resolved); returns False (and the caller logs) otherwise or when the path is gone.
  - `stale_builds(assets: list[dict], finished: Callable[[str], bool]) -> list[tuple[str, str, str]]` — for rows
    in `fetching`/`staging` with a `job`, when `finished(job)` → `(kind, key, "partial" if kind == "raw" else "missing")`.
- Produces, `models.py`: `KeeperReport(scanned: int = 0, missing: int = 0, new: int = 0, evicted: list[str] =
  [], stale: list[str] = [], removed_building: list[str] = [], over_quota: dict[str, int] = {}, catalog: str = "",
  errors: list[str] = [])` (lists/dicts with `default_factory`).
- Produces, activity `keep(self, dry_run: bool = False) -> KeeperReport`, in this order, each step best-effort and
  its failure appended to `errors`:
  1. `registry.sync_datasets()`; 2. `registry.sync_assets()` → `scanned/missing/new` and the `building` list;
  3. catalog sync when `<state_dir>/keeper/catalog-synced-at` is missing or older than `keeper.catalog_sync_hours`
  (`catalog_sync(cfg)` of the catalog's `main` head; write the file after); 4. stale builds: `finished(job)` =
  csub `status([job])` is empty or terminal (`self.jobs.client`), each → `put_asset(kind, key, {"state": …, "job": None})`;
  5. `.building-*` directories from the sync response older than `quota.hold_hours` (mtime) → `evict_path`;
  6. per kind (`staged`, `raw`): `plan_evictions` over `registry.assets(kind)["assets"]` with `in_progress` =
  ids of `registry.datasets(state="in-progress")` → `evict_path(row path)` → `put_asset(kind, key, {"state":
  "evicted", "evicted_at": now, "bytes": 0})`; log one line per eviction; 7. when a kind is still over quota after
  that and `not dry_run`: `over_quota[kind] = over_by` and one comment on the newest open buffer issue (marker
  `f"keeper over-quota {kind}"`, `_comment_once`). With `dry_run`, steps 4–6 only report what they would do.
- Produces, `MiaCacheKeeper` (`@workflow.defn(name="MiaCacheKeeper")`): `run(self) -> KeeperReport` =
  `execute_activity("keep", start_to_close_timeout=timedelta(minutes=25), heartbeat_timeout=timedelta(minutes=5))`
  (the activity heartbeats between steps). `cmd_schedule` applies both `<deployment>-poll` and
  `<deployment>-keeper` (every `keeper.minutes`, overlap `SKIP`); `cmd_worker` lists the workflow.
- CLI: `mia-buffers assets sync [--dry-run]` → `Activities.keep(dry_run)` directly (no Temporal), prints the
  report one field per line; exit 1 when `errors`.

- [ ] **Step 1: Write the failing keeper tests**

```python
def test_plan_evictions_skips_referenced_and_held_assets():
    now = 10_000.0
    rows = [
        {"key": "a", "data": {"state": "ready", "bytes": 40, "last_used": 1.0}, "refs": []},
        {"key": "b", "data": {"state": "ready", "bytes": 40, "last_used": 2.0}, "refs": ["buf"]},
        {"key": "c", "data": {"state": "ready", "bytes": 40, "last_used": now - 10}, "refs": []},
        {"key": "d", "data": {"state": "staging", "bytes": 40, "job": "9"}, "refs": []},
    ]
    picked = plan_evictions(rows, quota_bytes=100, headroom_bytes=0, hold_s=3600, now=now, in_progress=set(), kind="staged")
    assert [r["key"] for r in picked] == ["a"]  # 160 live → evict a (120) … b referenced, c held, d building; still over, nothing else free

def test_zero_quota_never_evicts():
    rows = [{"key": "a", "data": {"state": "ready", "bytes": 1e12, "last_used": 0}, "refs": []}]
    assert plan_evictions(rows, quota_bytes=0, headroom_bytes=200e9, hold_s=0, now=1e9, in_progress=set(), kind="raw") == []

def test_raw_eviction_spares_a_record_in_progress():
    rows = [{"key": "rec", "data": {"state": "ready", "bytes": 10, "last_used": 0}, "refs": []}]
    assert plan_evictions(rows, quota_bytes=5, headroom_bytes=0, hold_s=0, now=100, in_progress={"rec"}, kind="raw") == []

def test_evict_refuses_paths_outside_the_roots(tmp_path):
    inside, outside = tmp_path / "raw" / "rec", tmp_path / "elsewhere"
    inside.mkdir(parents=True); outside.mkdir()
    assert evict_path(inside, (tmp_path / "raw",)) and not inside.exists()
    assert not evict_path(outside, (tmp_path / "raw",)) and outside.exists()
    assert not evict_path(tmp_path / "raw" / ".." / "elsewhere", (tmp_path / "raw",)) and outside.exists()

def test_stale_builds():
    rows = [{"kind": "raw", "key": "r", "data": {"state": "fetching", "job": "1"}}, {"kind": "staged", "key": "s", "data": {"state": "staging", "job": "2"}},
            {"kind": "staged", "key": "t", "data": {"state": "staging", "job": "3"}}]
    assert stale_builds(rows, finished=lambda job: job != "3") == [("raw", "r", "partial"), ("staged", "s", "missing")]
```

  Plus, in `tests/test_buffers_activities.py`: `test_keep_evicts_a_free_staged_asset_under_quota(acts, server, tmp_path)`:
  a staged directory on disk + a `ready` row with `bytes` above `quota.staged_bytes`, `last_used` old, no refs →
  after `keep()` the directory is gone and the row is `evicted`; a second artifact with a ref is untouched; with
  `dry_run=True` nothing is deleted and the report lists it. And `test_keep_posts_once_when_held_set_is_over_quota`.
  In `tests/test_buffers_cli.py`: `schedule` applies two schedules; `assets sync --dry-run` prints the report.
- [ ] **Step 2: Run** → fail. **Step 3: Implement.** **Step 4: Full checks** → pass.
- [ ] **Step 5: Commit** — `mia-buffers: the cache keeper — syncs, stale builds, eviction under quota; assets sync`.

### Task 9: documentation and the spec's record

**Files:**
- Modify: `docs/development.md` (new section "## The cache (milestone C)" after "The registry": the lock rule and
  owner strings, the fetch phase and `fetch.json`, offline staging and the `stream` exception, asset states, the
  keeper's pass and quotas with `0` = unlimited, `mia exec mia-buffers assets sync --dry-run`, `GET /locks`, and
  the operator note that hand-run harness commands bypass the lock and must stay off records a round is touching);
  `AGENTS.md` (two bullets under "Rules that will bite you": no job writes a record's raw cache unless its worker
  holds `<local_root>/locks/raw/<record>`; only the buffers worker's keeper deletes under the roots, the service
  never does); `docs/mia-data-workflow.md` (if Task 5 left anything out); `notes/specs/2026-10-25-prep-and-buffering.md`
  (a short "Built in C" note under §10 row C listing this plan's deviations by one line each);
  `mia.yaml.example` (comments on `quota:` if Task 1 left them thin).

- [ ] **Step 1: Write the docs.** **Step 2:** `pixi run lint` (markdown is not linted, but the real-path grep is).
- [ ] **Step 3: Commit** — `docs: the cache — locks, fetch, asset states, the keeper`.

## Live steps after the branch is green (for the operator, not the executor)

1. mia-snippets: the `fetch` commit is on `training-buffers` in the `harness.dir` checkout; a buffer round admitted
   after it pins it. Rounds admitted earlier keep the old path through `patched()`.
2. `mia restart mia-data mia-buffers` with no buffer round in `buffer:sourcing` and no dataset round in a job.
   `mia exec mia-buffers schedule` is run by `mia up` (the keeper schedule appears beside the poll).
3. `mia exec mia-buffers assets sync --dry-run` once, then without `--dry-run`; `curl $URL/assets/staged` shows
   states and bytes; `curl $URL/locks` is empty while nothing runs.
4. The §10-C acceptance: two buffer issues naming the same record, `buffer:go` on both within a minute: the second
   shows `waiting-lock` with the first's owner in its plan comment's dataset line, then "staged by … and reused".
