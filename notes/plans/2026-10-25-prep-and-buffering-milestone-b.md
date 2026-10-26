# Prep and buffering, milestone B: the registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** buffer-api becomes the registry: its SQLite file lives on the host-local root with periodic backups,
mutating routes need the worker token, it knows the catalog's records and every dataset's preparation state,
it runs the planner server-side over them, staged assets are keyed by artifact id, and a volume's row and its
references are written in one transaction.

**Architecture:** the ledger keeps its one-connection SQLite shape and gains three tables (`catalogs`,
`records`, `datasets`). The service gains a capability per route (consumer, read, worker) and reads the
mia-buffers block of `mia.yaml` directly, so it knows the roots, the token and the ledger location. A small
synchronous HTTP client in `mia_core/registry.py` is the one way the two workers write to it. mia-data's
`set_stage` reports every dataset's state best-effort; a datasets sync inside the service rebuilds the
`prepared` rows from `storage_root`; the worker's `catalog sync` loads a catalog commit's records. The workflow
keeps its shape: only the staged directory, the asset key and the registry calls change under it.

**Tech Stack:** Python 3.12, stdlib `sqlite3` (incl. `Connection.backup`), `http.server`, `hmac`, `httpx`
(client), pydantic 2, PyYAML; pytest with `httpx.MockTransport`, ruff, mypy via pixi.

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` v0.2: §1.10–1.13, §3, §3.1, §4.1, §4.2, §5.7
(artifact on the volume), §9 rows `mia_core/`, `mia_buffers/ledger.py, api.py`, `mia_buffers/plan.py`,
`mia_data/activities.py`, `mia_buffers/cli.py`, §10 row B.

Deviations from the spec, decided here so no task re-decides them:

- `records.data` holds the **raw catalog record** (what `plan.load_records` yields, including its `unreadable`
  marker), not the planner's `Facts`. `build_plan` then runs on the server exactly as it runs in the worker, from
  the same input, and `Facts` needs no (de)serialisation. The page's record views (milestone F) derive facts on
  read.
- `read_binding` finds an earlier artifact by **computing its id** and `GET /assets/staged/<id>`; the filtered
  `GET /assets/staged?record=…` query of §3.1 is not built (same information, no query code).
- `POST /plan` returns one extra column per row, `prepared` (the dataset state or `unprepared`). The `raw` and
  `staged` columns wait for asset states (milestone C).
- The worker's `plan` activity passes `prepared=None`, so the live `MiaBuffer` path (nudge, `waiting-dataset`)
  is unchanged until milestone D moves planning to the server. Only `POST /plan` applies `not prepared`.
- The staged directory becomes `<staged_root>/<levels key>/<record>/<artifact id>` now (the id is needed for the
  key); the build-under-a-temporary-name-then-rename of §1.10 is milestone D.
- `GET /records`, `buffer_datasets`, `buffer_events`, `usage`, `GET /locks`, `POST /sync/assets`, draws archive
  and `status` stat-ing pyramids are milestones C, D and F.

## Global Constraints

- Work on branch `training-buffers` in this checkout, one commit per task, no merge and no PR.
- `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` clean after every task; no new
  `type: ignore` or `noqa` without a reason beside it.
- Line length 120; descriptive names; a one-line docstring on every new module; `# ponytail:` on deliberate
  shortcuts naming the ceiling and the upgrade path.
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` is empty before each
  commit. Test fixtures use `/netfs/lab`-style placeholders.
- `mia_buffers/models.py` and `mia_data/models.py` dataclasses are wire format: fields are added with defaults,
  never renamed or removed. This milestone adds `BindingInfo.artifact: str = ""` and nothing else.
- Workflow code (`mia_buffers/workflows.py`, `mia_data/workflows.py`) is not edited in this milestone; replay
  fixtures stay as they are.
- Import direction: `mia_core` imports no application package; `mia_data → autodev`; `mia_buffers → mia_data,
  autodev`; never the reverse. `mia_core/registry.py` imports `httpx` and `mia_core.config` only.
- Values from the spec: worker token = the content of `service.token_file`, sent as `Authorization: Bearer
  <token>`; a missing or wrong token is `401` with body `{"error": "unauthorized"}` and no other detail; the
  ledger is `<paths.local_root>/service/buffers.sqlite`; backups go to `<paths.state_dir>/backups/` every
  `keeper.minutes` (default 30), keeping the last 48; a dataset is `prepared` when `<storage_root>/<folder>/
  binding.yaml` exists and `manifest.json`'s `binding.sha256` equals its sha256; dataset states are
  `in-progress`, `prepared`, `ineligible`, `needs-human` (no row = `unprepared`); `POST /plan` on an unsynced
  ref is `409` naming `mia-buffers catalog sync --ref <ref>`; `POST /sync/datasets` is `409` while one runs.
- The registry's writes from workers: one HTTP call, 30 s timeout; best effort (log and continue) except
  `create_buffer`, `register_dataset` and the artifact lookup, which raise as they do today.

## Review Focus

1. A `PUT` with a token that differs by one byte, or with `Authorization: Bearer` and an empty token, is `401`
   with no detail; a `GET` and the consumer routes never ask for one (Task 2, `test_worker_routes_need_the_token`,
   `test_consumer_and_read_routes_are_open`).
2. `POST /sync/datasets` must not delete a dataset row mia-data marked `in-progress` or `needs-human` just
   because the folder has no binding yet; it demotes only rows that *were* `prepared` and are no longer (Task 4,
   `test_sync_datasets_keeps_in_progress_rows`).
3. A binding whose `manifest.json` is missing or whose `binding.sha256` is stale (a new binding published, crops
   not yet re-verified) is not `prepared` (Task 8, `test_prepared_row_requires_matching_manifest`).
4. Two volumes of one record registered concurrently must each leave their volume and both refs, or nothing;
   an exception after the volume insert must roll the volume back (Task 3, `test_put_volume_with_refs_is_atomic`).
5. `serve` started while a legacy per-app config lacks `local_root`, or while `local_root` is on a network
   filesystem, exits with the reason instead of opening a ledger on NFS (Task 5, `test_serve_refuses_without_
   local_root`, `test_serve_refuses_a_network_root`).

---

### Task 1: `mia_core/registry.py` and `KeeperCfg`

**Files:**
- Create: `mia_core/registry.py`
- Modify: `mia_core/config.py` (add `KeeperCfg`), `mia_buffers/config.py` (`keeper: KeeperCfg = KeeperCfg()`),
  `mia_data/config.py:_resolve_paths` (token_file `.expanduser().absolute()`), `mia.yaml.example` (a `keeper:`
  section after `service:`)
- Test: `tests/test_core_registry.py`, `tests/test_core_config.py` (one test), `tests/test_buffers_config.py`
  (one test)

**Interfaces:**
- Produces (`mia_core/config.py`): `class KeeperCfg(BaseModel): minutes: int = 30; catalog_sync_hours: int = 24`
  (both `ge=1`).
- Produces (`mia_core/registry.py`):
  - `class RegistryError(RuntimeError)` with attributes `status: int | None` (None for transport errors) and
    `message: str`.
  - `def read_token(path: Path) -> str`: the file's text stripped; raises `RegistryError(None, ...)` when
    missing or empty.
  - `class Registry`: `__init__(self, url: str, token: str | None = None, *, timeout: float = 30.0,
    transport: httpx.BaseTransport | None = None)`; one `httpx.Client(base_url=url, timeout=timeout,
    transport=transport)` held for the object's life; `close()`; context manager.
  - `@classmethod from_config(cls, service: ServiceCfg) -> Registry`: `Registry(service.url,
    read_token(service.token_file) if service.token_file else None)`.
  - `request(self, method: str, path: str, *, json: Any = None, params: dict | None = None) -> Any`: sends
    `Authorization: Bearer <token>` when a token is set; returns the parsed JSON body (`None` on 204); raises
    `RegistryError(status, body["error"] or text)` on any status ≥ 400 and `RegistryError(None, str(error))` on
    `httpx.HTTPError`.
  - Thin methods, each one `request` call: `put_dataset(record: str, **fields) -> dict` (`PUT /datasets/<record>`),
    `dataset(record: str) -> dict | None` (None on 404), `datasets(state: str | None = None) -> list[dict]`
    (`GET /datasets?state=`), `put_asset(kind: str, key: str, data: dict) -> dict`, `asset(kind: str, key: str)
    -> dict | None` (the `data` field, None on 404), `put_records(ref: str, records: list[tuple[str, dict]]) ->
    dict` (`PUT /records/<ref>` with body `{"records": [{"path": p, "record": r}, ...]}`), `plan(spec_text: str)
    -> dict` (`POST /plan` body `{"spec": spec_text}`), `sync_datasets() -> dict` (`POST /sync/datasets`),
    `catalogs() -> list[dict]`.
  - `def best_effort(action: Callable[[], Any], what: str, log: logging.Logger) -> bool`: runs `action`,
    returns True; on `RegistryError` logs `"%s: registry write skipped: %s"` at WARNING and returns False.
  - Keys and record ids go through `urllib.parse.quote(value, safe="")`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_core_registry.py
import httpx, pytest
from mia_core.registry import Registry, RegistryError, best_effort, read_token

def transport(seen, status=200, body=None):
    def handler(request):
        seen.append(request)
        return httpx.Response(status, json=body if body is not None else {"ok": True})
    return httpx.MockTransport(handler)

def test_bearer_header_and_quoting():
    seen = []
    reg = Registry("http://svc", "tok", transport=transport(seen))
    reg.put_asset("staged", "a/b c", {"x": 1})
    assert seen[0].headers["authorization"] == "Bearer tok" and seen[0].url.path == "/assets/staged/a%2Fb%20c"

def test_no_token_sends_no_header():
    seen = []
    Registry("http://svc", None, transport=transport(seen)).datasets("prepared")
    assert "authorization" not in seen[0].headers and seen[0].url.params["state"] == "prepared"

def test_errors_carry_status_and_message():
    reg = Registry("http://svc", "t", transport=transport([], 401, {"error": "unauthorized"}))
    with pytest.raises(RegistryError) as err:
        reg.put_dataset("rec", state="prepared")
    assert err.value.status == 401 and err.value.message == "unauthorized"

def test_404_is_none_for_lookups():
    reg = Registry("http://svc", "t", transport=transport([], 404, {"error": "no"}))
    assert reg.dataset("rec") is None and reg.asset("staged", "k") is None

def test_transport_error_is_registry_error():
    def boom(request):
        raise httpx.ConnectError("down")
    with pytest.raises(RegistryError) as err:
        Registry("http://svc", "t", transport=httpx.MockTransport(boom)).catalogs()
    assert err.value.status is None

def test_best_effort_logs_and_continues(caplog):
    def fail():
        raise RegistryError(None, "down")
    assert best_effort(fail, "dataset rec", logging.getLogger("t")) is False
    assert "dataset rec: registry write skipped: down" in caplog.text

def test_read_token(tmp_path):
    (tmp_path / "t").write_text("abc\n")
    assert read_token(tmp_path / "t") == "abc"
    with pytest.raises(RegistryError):
        read_token(tmp_path / "missing")
```

Plus `test_keeper_cfg_defaults` in `tests/test_buffers_config.py` (`cfg.keeper.minutes == 30` from a config
without the section; `minutes: 0` refused) and `test_service_token_file_is_absolute` in `tests/test_core_config.py`
(a mia-data `Config` built with `service: {url: ..., token_file: "~/x"}` has an absolute `service.token_file`).

- [ ] **Step 2: Run them to verify they fail** — `pixi run pytest tests/test_core_registry.py -q` → ImportError.
- [ ] **Step 3: Implement** `KeeperCfg`, `mia_core/registry.py` as in Interfaces, the `keeper` field, the
  token path fix, and the example section:
  ```yaml
  keeper:                                   # buffer-api's housekeeping (milestone C adds quota and eviction)
    minutes: 30                             # SQLite backup cadence; the keeper pass in milestone C
    catalog_sync_hours: 24
  ```
- [ ] **Step 4: Run the tests and the four checks** — all pass.
- [ ] **Step 5: Commit** — `mia_core.registry: the worker-side client of buffer-api, with the bearer token; KeeperCfg`.

---

### Task 2: capability levels and the worker token in `BufferService`

**Files:**
- Modify: `mia_buffers/api.py` (`ROUTES`, `BufferService.__init__`, `handle`, `make_server`)
- Test: `tests/test_buffers_api.py`

**Interfaces:**
- Produces: `ROUTES` entries become 4-tuples `(method, pattern, handler, capability)` with capability in
  `{"consumer", "read", "worker"}`: consumer = `add_consumer`, `next`, `split`, `status`; read = every other
  `GET`; worker = every other `PUT`, `POST`, `DELETE`.
- `BufferService.__init__(self, ledger: Ledger, stale_s: float = 300.0, *, token: str | None = None)`.
  `token=None` disables the check (tests and the offline `register` CLI against a dev service); `serve` always
  passes one (Task 5).
- `handle(self, method, path, query, body, headers: dict[str, str] | None = None) -> Response`; header names
  are matched case-insensitively. For a `worker` route with a token set: the `Authorization` header must be
  exactly `Bearer <token>` compared with `hmac.compare_digest`; otherwise `401, {"error": "unauthorized"}, {}`
  before any body parsing.
- `make_server` passes `dict(self.headers.items())` to `handle`; `do_DELETE = _dispatch` is added.
- Every existing test keeps passing with `BufferService(Ledger(...))` (no token).

- [ ] **Step 1: Write the failing tests**

```python
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
```

and extend `test_over_http` so the socket path sends the header for the `PUT`s (construct the service with
`token="t"` and use `httpx.put(..., headers={"Authorization": "Bearer t"})`).

- [ ] **Step 2: Run to verify they fail** — the `401` assertions fail (today everything is 200).
- [ ] **Step 3: Implement** the capability column, the check at the top of `handle`, and the header plumbing.
- [ ] **Step 4: Run the tests and the four checks.**
- [ ] **Step 5: Commit** — `buffer-api: consumer, read and worker routes; worker routes take the bearer token`.

---

### Task 3: ledger tables for catalogs, records and datasets; atomic volume + refs; backup

**Files:**
- Modify: `mia_buffers/ledger.py` (`SCHEMA`, new methods, `put_volume` signature)
- Test: `tests/test_buffers_ledger.py`

**Interfaces:**
- Produces (schema additions, all `CREATE TABLE IF NOT EXISTS`):
  - `catalogs (ref TEXT PRIMARY KEY, synced_at REAL, count INTEGER)`
  - `records (ref TEXT, id TEXT, path TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY (ref, id))`
  - `datasets (id TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at REAL)`; `data` is a JSON object holding
    `state`, `data_stage`, `issue`, `folder`, `binding_hash`, `binding_commit`, `published_at`, `crops`,
    `evaluation` (any subset).
- Produces (methods):
  - `put_records(self, ref: str, records: list[tuple[str, dict]]) -> int`: one transaction: delete the ref's
    rows, insert each `(ref, record["id"], path, json)`, upsert `catalogs(ref, now, n)`; returns n. The id is
    `record["id"]` (load_records guarantees it).
  - `records(self, ref: str) -> list[tuple[str, dict]]` sorted by path; `catalogs(self) -> list[dict]`
    (`{ref, synced_at, count}` newest first); `latest_catalog(self) -> str | None`.
  - `put_dataset(self, record: str, fields: dict) -> dict`: merge `fields` into the existing JSON (new keys
    win), set `updated_at`, return the merged row with `id` and `updated_at` added.
  - `dataset(self, record: str) -> dict | None`; `datasets(self, state: str | None = None) -> list[dict]` sorted
    by id, each with `id` and `updated_at`.
  - `sync_datasets(self, prepared: dict[str, dict]) -> dict`: one transaction: for each `(record, fields)`
    merge `{**fields, "state": "prepared"}`; for every row whose current `state` is `prepared` and whose id is
    not in `prepared`, delete the row. Rows in other states are untouched. Returns
    `{"prepared": n, "demoted": m}`.
  - `put_volume(self, buffer: str, volume: Volume, refs: Iterable[tuple[str, str]] = ()) -> None`: one
    `BEGIN IMMEDIATE … COMMIT` covering the volume upsert and `INSERT OR IGNORE` of every ref; rollback on error.
    `add_ref` stays for callers that only add a reference.
  - `backup(self, dest: Path) -> None`: under the lock, `self._db.backup(sqlite3.connect(tmp))` into
    `dest.with_suffix(".tmp")` then `os.replace` to `dest`.
- `Ledger` is still one connection, one process (keep the `# ponytail` note).

- [ ] **Step 1: Write the failing tests**

```python
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

def test_put_volume_with_refs_is_atomic(tmp_path, monkeypatch):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {})
    led.put_volume("b", vol("a"), refs=[("staged", "art1"), ("raw", "rec")])
    assert led.refs("staged", "art1") == ["b"] and led.refs("raw", "rec") == ["b"]
    real = led._db.executemany
    def broken(*args, **kwargs):
        raise sqlite3.OperationalError("boom")
    monkeypatch.setattr(led._db, "executemany", broken)
    with pytest.raises(sqlite3.OperationalError):
        led.put_volume("b", vol("c"), refs=[("staged", "art2")])
    monkeypatch.setattr(led._db, "executemany", real)
    assert [v.id for v in led.volumes("b")] == ["a"] and led.refs("staged", "art2") == []

def test_backup_is_a_consistent_copy(tmp_path):
    led = Ledger(tmp_path / "l.sqlite")
    led.put_buffer("b", {"x": 1})
    led.backup(tmp_path / "bk" / "copy.sqlite")
    assert Ledger(tmp_path / "bk" / "copy.sqlite").buffer("b") == {"x": 1}
    assert not (tmp_path / "bk" / "copy.tmp").exists()
```

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** as in Interfaces. `put_volume`'s refs are inserted with `executemany` (that is what
  the atomicity test breaks).
- [ ] **Step 4: Run the tests and the four checks.**
- [ ] **Step 5: Commit** — `ledger: catalogs, records and datasets tables; volume + refs in one transaction; backup`.

---

### Task 4: the planner on the server: `not prepared`, `POST /plan`, records, datasets and the datasets sync

**Files:**
- Modify: `mia_buffers/spec.py` (`load_spec_text`), `mia_buffers/plan.py` (`build_plan(..., prepared=None)`,
  `Plan.to_dict(states=None)`), `mia_buffers/api.py` (routes, `BufferService.__init__(…, storage_root: Path |
  None = None)`), `mia_data/publish.py` (`prepared_row`)
- Create: `mia_buffers/sync.py`
- Test: `tests/test_buffers_spec.py`, `tests/test_buffers_plan.py`, `tests/test_buffers_api.py`,
  `tests/test_buffers_sync.py`, `tests/test_mia_publish.py` (one test; create the file if there is none)

**Interfaces:**
- Produces (`mia_buffers/spec.py`): `def load_spec_text(text: str) -> Spec` (the body of today's `load_spec`
  from a string; `load_spec(path)` calls it); `def spec_text_hash(text: str) -> str`.
- Produces (`mia_buffers/plan.py`): `build_plan(spec, spec_hash, catalog_ref, records, prepared: set[str] |
  None = None) -> Plan`. With a set given, after `fit`: a row with `row.fit.ok` whose id is not in `prepared`
  gets `admitted = False`, `rule = "not prepared"`, keeps its `fit`, and is excluded from `pool` (so from
  weights, splits and policy errors). `NOT_PREPARED = "not prepared"` is a module constant. `Plan.to_dict(self,
  states: dict[str, str] | None = None) -> dict`: when `states` is given, every row dict gains `"prepared":
  states.get(row.id, "unprepared")`. `render_markdown` lists `not prepared` rows under their own heading
  `## Not prepared (<n>)` after the records table, ids only (the link to the page is milestone F).
- Produces (`mia_data/publish.py`): `def prepared_row(dataset_dir: Path) -> dict | None`: reads
  `binding.yaml` and `manifest.json`; returns `None` unless both exist, `manifest["binding"]["sha256"]` equals
  the sha256 of `binding.yaml`, and `binding["record"]` is a string. Otherwise returns `{"state": "prepared",
  "record": binding["record"], "folder": dataset_dir.name, "binding_hash": <sha256>, "binding_commit":
  binding.get("snippets_commit"), "crops": int(manifest.get("count") or 0), "published_at": <manifest mtime,
  ISO 8601 UTC>}`. Files are read with `mia_core.untrusted.read_untrusted`-style bounds (1 MiB each; both files
  are the worker's own output, but the helper is also the service's scan, so bound it).
- Produces (`mia_buffers/sync.py`): `def scan_storage(storage_root: Path) -> dict[str, dict]`: for every
  immediate subdirectory (dot-dirs skipped) with a `prepared_row`, `{record: row without "record"}`; a record
  seen in two folders keeps the first by folder name and logs a warning.
- Produces (`mia_buffers/api.py`):
  - `BufferService.__init__(self, ledger, stale_s=300.0, *, token=None, storage_root: Path | None = None)`;
    `self._sync_lock = threading.Lock()`.
  - Routes (capabilities in brackets): `PUT /records/<ref>` [worker] body `{"records": [{"path", "record"}]}`,
    ref must match `^[0-9a-f]{40}$` else 422, answers `{"ref", "count"}`; `GET /catalogs` [read];
    `PUT /datasets/<id>` [worker] body a JSON object whose `state`, when present, is one of the four states
    (422 otherwise), answers the merged row; `GET /datasets/<id>` [read] (404 when absent);
    `GET /datasets?state=` [read] `{"datasets": [...]}`; `POST /plan` [read] body `{"spec": <text>}`: `SpecError`
    → 422 with the message; no records at `spec.catalog.ref` → 409 `{"error": "catalog ref <ref> is not synced;
    run: mia-buffers catalog sync --ref <ref>"}`; else `build_plan(spec, spec_text_hash(text), ref, records,
    prepared={ids with state prepared})` and `200, plan.to_dict(states)`; `POST /sync/datasets` [worker]:
    `storage_root is None` → 503 `{"error": "the service has no storage_root"}`; `_sync_lock` already held →
    409 `{"error": "a datasets sync is running"}`; else `ledger.sync_datasets(scan_storage(storage_root))`
    and its counts.
  - The route for `GET /datasets` (no id) must be listed before `GET /datasets/<id>`; `GET /assets/<kind>` keeps
    working.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_buffers_plan.py  (records = load_records(FIXTURE_CATALOG), the fixture catalog under tests/fixtures/buffers)
def test_not_prepared_rows_are_out_with_their_rule(spec_2d, records):
    plan = build_plan(spec_2d, "h", None, records, prepared={U937})
    rows = plan.rows_by_id
    assert rows[U937].in_pool and rows[BCSS].rule == NOT_PREPARED and not rows[BCSS].in_pool
    assert rows[BCSS].fit is not None and rows[BCSS].fit.ok and rows[BCSS].weight == 0.0
    assert "## Not prepared (1)" in plan.render_markdown()
    assert build_plan(spec_2d, "h", None, records).rows_by_id[BCSS].in_pool  # None: rule not applied
    assert plan.to_dict({U937: "prepared"})["rows"][0]["prepared"] in ("prepared", "unprepared")

# tests/test_buffers_spec.py
def test_load_spec_text_matches_load_spec(spec_2d):  # the Path fixture
    assert load_spec_text(spec_2d.read_text()) == load_spec(spec_2d)
    assert spec_text_hash(spec_2d.read_text()) == spec_hash(spec_2d)

# tests/test_buffers_api.py
def test_records_datasets_and_plan(tmp_path):
    s = BufferService(Ledger(tmp_path / "l.sqlite"))
    ref = "a" * 40
    records = [{"path": p, "record": r} for p, r in load_records(FIXTURE_CATALOG)]
    assert call(s, "PUT", f"/records/{ref}", payload={"records": records})[1]["count"] == len(records)
    assert call(s, "PUT", "/records/main", payload={"records": []})[0] == 422
    assert call(s, "GET", "/catalogs")[1]["catalogs"][0]["ref"] == ref
    text = re.sub(r"ref: [0-9a-f]{40}", f"ref: {ref}", SPEC_2D_TEXT)  # the 2D fixture spec's text, at the synced ref
    status, out, _ = call(s, "POST", "/plan", payload={"spec": text})
    assert status == 200 and all(row["prepared"] == "unprepared" for row in out["rows"])
    assert all(row["rule"] == "not prepared" for row in out["rows"] if row["fit"] and row["fit"]["ok"])
    assert call(s, "PUT", f"/datasets/{U937}", payload={"state": "prepared", "crops": 16})[1]["crops"] == 16
    assert call(s, "PUT", f"/datasets/{U937}", payload={"state": "nope"})[0] == 422
    assert call(s, "GET", f"/datasets/{U937}")[1]["state"] == "prepared"
    assert [d["id"] for d in call(s, "GET", "/datasets", {"state": "prepared"})[1]["datasets"]] == [U937]
    out = call(s, "POST", "/plan", payload={"spec": text})[1]
    by_id = {row["id"]: row for row in out["rows"]}
    assert by_id[U937]["prepared"] == "prepared" and by_id[U937]["admitted"] and by_id[U937]["weight"] > 0
    assert call(s, "POST", "/plan", payload={"spec": SPEC_2D_TEXT})[0] == 409  # the fixture's own ref is not synced
    assert call(s, "POST", "/plan", payload={"spec": "schema_version: 2"})[0] == 422

def test_sync_datasets_route(tmp_path):
    root = tmp_path / "crops"
    write_prepared(root / "confocal-human-u937", U937)          # helper: binding.yaml + matching manifest.json
    write_prepared(root / "tem-mouse-stale", "stale", match=False)
    s = BufferService(Ledger(tmp_path / "l.sqlite"), storage_root=root)
    s.ledger.put_dataset("busy", {"state": "in-progress"})
    assert call(s, "POST", "/sync/datasets")[1] == {"prepared": 1, "demoted": 0}
    assert call(s, "GET", "/datasets/stale")[0] == 404 and call(s, "GET", "/datasets/busy")[1]["state"] == "in-progress"
    assert call(s, "GET", f"/datasets/{U937}")[1]["folder"] == "confocal-human-u937"
    assert call(s, "POST", "/sync/datasets", )[0] == 200
    assert BufferService(Ledger(tmp_path / "l2.sqlite")).handle("POST", "/sync/datasets", {}, b"")[0] == 503
```

plus `test_sync_409_while_running` (hold `s._sync_lock` in the test and call the route) and, in
`tests/test_mia_publish.py`, `test_prepared_row_requires_matching_manifest` (binding alone → None; binding +
manifest with another sha → None; matching → the row with `crops` and `binding_commit`), and in
`tests/test_buffers_sync.py`, `test_scan_storage_skips_dotdirs_and_duplicates`.

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement** in the order spec.py → plan.py → publish.py → sync.py → api.py.
- [ ] **Step 4: Run the tests and the four checks.**
- [ ] **Step 5: Commit** — `buffer-api: records, datasets, POST /plan with the not-prepared rule, datasets sync`.

---

### Task 5: `serve` from the configuration: ledger on `local_root`, backups, token

**Files:**
- Modify: `mia_buffers/api.py` (`serve` signature), `mia_buffers/cli.py` (`serve` becomes a deployment
  command), `mia_ops/services.py` (the api window's command line), `docs/development.md` (the "Serve" step and
  the ledger location), `notes/autodev-runbook.md` (ledger location paragraph)
- Test: `tests/test_buffers_cli.py`, `tests/test_buffers_api.py`, `tests/test_mia_ops_services.py`

**Interfaces:**
- Produces (`mia_buffers/api.py`):
  - `def ledger_path(cfg: Config) -> Path`: `cfg.paths.local_root / "service" / "buffers.sqlite"`; raises
    `RuntimeError("set paths.local_root in mia.yaml: buffer-api keeps its SQLite file on host-local disk")`
    when `local_root` is None.
  - `def open_ledger(cfg: Config) -> Ledger`: `require_local_dir(path.parent)` (from `mia_core.fs`; it raises
    on a network filesystem); if the file does not exist and `cfg.paths.state_dir / "buffers.sqlite"` does, copy
    it once with `Ledger(old).backup(path)` and print `"buffer-api: moved the ledger from <old> to <new>"`;
    return `Ledger(path)`.
  - `def prune_backups(directory: Path, keep: int = 48) -> None`: delete the oldest `buffers-*.sqlite` beyond
    `keep`.
  - `def backup_forever(ledger: Ledger, directory: Path, minutes: int, stop: threading.Event) -> None`: every
    `minutes`, `ledger.backup(directory / f"buffers-{UTC %Y%m%dT%H%M%SZ}.sqlite")` then `prune_backups`; errors
    are logged, never raised.
  - `def serve(cfg: Config) -> None`: host and port from `urlparse(cfg.service.url)` (port default 8765);
    token = `read_token(cfg.service.token_file)` (a missing `token_file` is `SystemExit("set service.token_file
    in mia.yaml and run \`mia init-token\`")`); `BufferService(open_ledger(cfg), token=token,
    storage_root=cfg.paths.storage_root)`; the backup thread (daemon) on `cfg.paths.state_dir / "backups"`;
    prints `"buffer-api on http://<host>:<port> ledger <path> backups <dir>"`.
- Produces (`mia_buffers/cli.py`): `serve` joins the `--config` commands (`_deployment` runs `serve` through
  `asyncio.to_thread`-free direct call: add it to the command map as a sync function; the simplest is a
  separate `_serve(args)` that calls `load_config(args.config)` then `api.serve(cfg)`). The `--state-dir`,
  `--host`, `--port` flags are removed (the help test lists `serve` still).
- Produces (`mia_ops/services.py`): the api window runs `PY -m mia_buffers.cli serve` with `svc.env()` in the
  command (`env MIA_SERVICE=… MIA_CONFIG=…`, or the legacy `MIA_BUFFERS_CONFIG=…`); the `--state-dir/--host/
  --port` arguments go. The `allow` entry for the api host stays.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_buffers_api.py
def test_open_ledger_moves_the_old_file_once(tmp_path, capsys):
    cfg = buffers_cfg(tmp_path, local_root=tmp_path / "local")    # helper in tests/test_buffers_config.py or local
    Ledger(cfg.paths.state_dir / "buffers.sqlite").put_buffer("b", {"x": 1})
    assert open_ledger(cfg).buffer("b") == {"x": 1} and "moved the ledger" in capsys.readouterr().out
    Ledger(cfg.paths.state_dir / "buffers.sqlite").put_buffer("b", {"x": 2})
    assert open_ledger(cfg).buffer("b") == {"x": 1}  # already moved: the old file is not read again

def test_prune_backups_keeps_the_newest(tmp_path):
    for i in range(50):
        (tmp_path / f"buffers-2026010100{i:02d}00Z.sqlite").write_text("")
    prune_backups(tmp_path, keep=48)
    assert len(list(tmp_path.glob("buffers-*.sqlite"))) == 48 and not (tmp_path / "buffers-20260101000000Z.sqlite").exists()

def test_serve_refuses_without_local_root(tmp_path):
    with pytest.raises(RuntimeError, match="local_root"):
        ledger_path(buffers_cfg(tmp_path, local_root=None))

def test_serve_refuses_a_network_root(tmp_path, monkeypatch):
    monkeypatch.setattr("mia_core.fs.mount_type", lambda path, mounts=None: "nfs4")
    with pytest.raises(RuntimeError, match="network filesystem"):
        open_ledger(buffers_cfg(tmp_path, local_root=tmp_path / "local"))

# tests/test_mia_ops_services.py: replace the assertion at the old line 417 with
    assert "-m mia_buffers.cli serve" in api and "--state-dir" not in api and "MIA_SERVICE=mia-buffers" in api
```

and in `tests/test_buffers_cli.py` a `test_serve_takes_config_only` (`--help` of `serve` mentions `--config`
and not `--port`).

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.** Docs: `docs/development.md` step 3 of "Serving a buffer" becomes
  `mia-buffers serve` (config: `service.url` for host and port, ledger at `<local_root>/service/buffers.sqlite`,
  backups under `<state_dir>/backups/`, every worker write needs the token from `service.token_file`); the
  runbook's ledger paragraph says the same and that the first start moves the old file.
- [ ] **Step 4: Run the tests and the four checks.**
- [ ] **Step 5: Commit** — `mia-buffers serve: configured from mia.yaml; ledger on the local root, backups, token`.

---

### Task 6: CLI: `catalog sync`, `datasets sync`, `register --token-file`

**Files:**
- Modify: `mia_buffers/cli.py`, `mia_buffers/register.py` (`register(..., token: str | None = None)`)
- Test: `tests/test_buffers_cli.py`, `tests/test_buffers_register.py`

**Interfaces:**
- Produces (`mia_buffers/cli.py`):
  - `mia-buffers catalog sync [--ref SHA] [--config …]`: `cfg = load_config(args.config)`; `gh = _gh(cfg,
    cfg.github.catalog_repo)`; ref = `args.ref` or `asyncio.run(gh.get_ref_sha("main"))`; `snapshot =
    catalog_snapshot(cfg, ref, gh.tokens)`; `Registry.from_config(cfg.service).put_records(ref,
    load_records(snapshot))`; prints `"synced <ref> (<n> records)"`; exit 1 with the message on `RegistryError`.
  - `mia-buffers datasets sync [--config …]`: `Registry.from_config(cfg.service).sync_datasets()`; prints the
    counts; exit 1 on `RegistryError` (a 409 prints "a datasets sync is running; try again").
  - Both are implemented as `async def cmd_catalog_sync(cfg, args)` / `cmd_datasets_sync(cfg, args)` run by
    `_deployment`, which now passes `args` to the command functions (`cmd_worker` and friends ignore it).
  - `register` gains `--token-file PATH` (default none); `register.register(url, …, token=…)` sends the bearer
    header on its `PUT`s. The `httpx.Client` there gets `headers={"Authorization": f"Bearer {token}"}` when set.
- The argparse tree: `catalog` and `datasets` are sub-parsers with one sub-command each (`sync`), so later
  milestones add `assets sync` beside them.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_buffers_cli.py
def test_catalog_sync_loads_the_snapshot_into_the_registry(tmp_path, monkeypatch, capsys):
    cfg = cli_cfg(tmp_path)  # the Config the existing check test builds, with service.url and token_file
    monkeypatch.setattr(cli, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(cli, "catalog_snapshot", lambda cfg_, ref, tokens: FIXTURE_CATALOG)
    monkeypatch.setattr(cli, "_gh", lambda cfg_, repo: FakeGH(repo, refs={"main": "c" * 40}))
    sent = []
    monkeypatch.setattr(cli.Registry, "put_records", lambda self, ref, records: sent.append((ref, len(records))) or {})
    assert cli.main(["catalog", "sync"]) == 0 and sent == [("c" * 40, 6)]
    assert cli.main(["catalog", "sync", "--ref", "d" * 40]) == 0 and sent[-1][0] == "d" * 40
    assert "synced" in capsys.readouterr().out

def test_datasets_sync_reports_counts_and_409(tmp_path, monkeypatch, capsys):
    ...  # Registry.sync_datasets returns {"prepared": 2, "demoted": 0} → exit 0 and both numbers printed;
         # raising RegistryError(409, "a datasets sync is running") → exit 1 and "try again" on stderr

# tests/test_buffers_register.py: in test_register_end_to_end, build the server with token="t" and pass
# token="t" to register(); assert a second register(...) without a token leaves out["refused"] mentioning
# "unauthorized" for the buffer PUT (raise_for_status → the test expects httpx.HTTPStatusError).
```

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.** The `_deployment` signature change touches four existing commands; keep the diff to
  the parameter.
- [ ] **Step 4: Run the tests and the four checks.**
- [ ] **Step 5: Commit** — `mia-buffers: catalog sync and datasets sync; register sends the worker token`.

---

### Task 7: artifact ids: the staged directory, the asset key, the volume's reference

**Files:**
- Modify: `mia_buffers/jobspec.py` (`artifact_id`), `mia_buffers/models.py` (`BindingInfo.artifact`),
  `mia_buffers/sampler.py` (`Volume.artifact: str | None = None`), `mia_buffers/api.py` (`VolumeBody.artifact`,
  `_put_volume` refs), `mia_buffers/register.py` (`volume_bodies` sets `artifact`), `mia_buffers/activities.py`
  (`_staged_dir`, `_staged_before`, `_record_assets`, `create_buffer`, `register_dataset` through `Registry`)
- Test: `tests/test_buffers_jobspec.py`, `tests/test_buffers_api.py`, `tests/test_buffers_register.py`,
  `tests/test_buffers_activities.py`

**Interfaces:**
- Produces (`mia_buffers/jobspec.py`): `def artifact_id(binding_hash: str, levels_key: str, voxel_size_nm:
  dict[str, float] | None, harness_commit: str, env_digest: str) -> str`: the first 16 hex characters of the
  sha256 of `json.dumps([binding_hash, levels_key, {axis: float(v) for axis, v in sorted((voxel_size_nm or
  {}).items())}, harness_commit, env_digest], separators=(",", ":"))`.
- Produces (`mia_buffers/activities.py`):
  - `self.registry: Registry` built in `__init__` from `cfg.service` (`Registry.from_config`); tests may set
    `acts.registry = Registry(server_url)`.
  - `_artifact(self, round_id: str, record: str) -> str`: `artifact_id(sha256 of the round's binding copy,
    levels_key(patch, levels), self._row(round_id, record).get("voxel_size_nm"), pins.harness_commit,
    pins.env_digest)`; raises `ValueError(f"{record}: no binding copied into round {round_id}")` when the copy
    is missing (`read_binding` runs first).
  - `_staged_dir(self, round_id: str, record: str) -> Path` = `staged_root / levels_key / record / artifact`.
  - `_staged_before(self, round_id, record) -> tuple[bool, str]`: `(found, artifact)`; found when
    `registry.asset("staged", artifact)` is a dict with `ok is True` and `<staged dir>/stage.json` is a file;
    a `RegistryError` logs and counts as not found (as today).
  - `read_binding` copies the binding first (as today), then sets `BindingInfo.artifact` and `staged`.
  - `_record_assets`: `put_asset("staged", artifact, {"record", "levels_key", "path": str(staged dir),
    "artifact": artifact, "binding_hash", "harness_commit", "env_digest", "voxel_size_nm", "round", "ok",
    "volumes", "bytes_fetched", "snippets_commit"})` and `put_asset("raw", record, {...as today})`, through
    `asyncio.to_thread(self.registry.put_asset, …)`; failures raise (the stage result must be recorded).
  - `create_buffer` and `register_dataset` use `self.registry.request(...)` (the latter keeps per-volume
    status handling: a 200 registers, anything else lands in `refused` with the error message).
  - The `voxel_size_nm` the stage job receives (`StageJobArgs`) is the same plan-row value, so the id and the
    pyramid agree.
- Produces (`mia_buffers/register.py`): `volume_bodies(row, staged, spec, report=None, refused=None, *,
  artifact: str | None = None)`; each body carries `"artifact": artifact or staged.name`.
- Produces (`mia_buffers/api.py`): `VolumeBody.artifact: str | None = None`; `_put_volume` calls
  `ledger.put_volume(buffer, volume, refs=[("staged", parsed.artifact or str(PurePosixPath(parsed.path).parent)),
  ("raw", parsed.record)])`; `Volume.artifact` is stored with the volume.
- Produces (`mia_buffers/models.py`): `BindingInfo.artifact: str = ""` (last field).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_buffers_jobspec.py
def test_artifact_id_depends_on_every_input():
    base = ("b" * 64, "p256-l1-2-4", {"y": 10.0, "x": 10.0}, "c" * 40, "e" * 64)
    assert len(artifact_id(*base)) == 16 and artifact_id(*base) == artifact_id(*base)
    assert artifact_id(*base) == artifact_id("b" * 64, "p256-l1-2-4", {"x": 10, "y": 10}, "c" * 40, "e" * 64)
    for i, other in enumerate(("x" * 64, "p512-l1", {"y": 11.0, "x": 10.0}, "d" * 40, "f" * 64)):
        changed = list(base); changed[i] = other
        assert artifact_id(*changed) != artifact_id(*base)
    assert artifact_id("b" * 64, "k", None, "c" * 40, "e" * 64) != artifact_id(*base)

# tests/test_buffers_api.py
def test_volume_refs_are_keyed_by_artifact(tmp_path):
    s = svc(tmp_path)
    call(s, "PUT", "/buffers/b", payload=CFG)
    call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "artifact": "art1"})
    assert call(s, "GET", "/assets/staged/art1")[1]["refs"] == ["b"]
    call(s, "PUT", "/buffers/b/volumes/c", payload=body("c"))  # no artifact: the directory, as before
    assert call(s, "GET", "/assets/staged/%2Fstaged%2Fc")[1]["refs"] == ["b"]

# tests/test_buffers_activities.py (adapt the four existing asset tests)
#  - staged_dir(acts, record) becomes staged_root / "p256-l1-2-4" / record / <artifact> where <artifact> is
#    computed with artifact_id from the binding the test wrote, the plan row's voxel size and the round's pins;
#  - test_read_stage_records_the_staged_and_raw_assets asserts the staged asset under /assets/staged/<artifact>
#    with record, levels_key, path, binding_hash, harness_commit, env_digest and ok;
#  - test_read_binding_reports_a_pyramid_staged_earlier_for_the_same_binding also asserts
#    (await acts.read_binding(ROUND, U937)).artifact == <artifact>, and that a changed binding yields another id;
#  - the `server` fixture's service takes token="t" and acts.registry = Registry(server, "t"); one assertion that
#    acts.registry built from a config without a token gets 401 on put_asset (RegistryError.status == 401).
```

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement.** Remove the direct `httpx.AsyncClient` uses from `mia_buffers/activities.py`
  (`import httpx` goes if nothing else uses it).
- [ ] **Step 4: Run the tests and the four checks; `pixi run pytest tests/test_buffers_workflows.py -q` must
  still pass unchanged (the workflow sees only `BindingInfo` with one more defaulted field).**
- [ ] **Step 5: Commit** — `mia-buffers: staged artifacts keyed by artifact id; registry client in the activities`.

---

### Task 8: mia-data reports every dataset's state; docs

**Files:**
- Modify: `mia_data/activities.py` (`Activities.__init__` builds `self.registry`, `set_stage` writes the row),
  `docs/mia-data-workflow.md` (registry paragraph), `docs/development.md` ("Layout" line for `mia_core/registry.py`,
  a "The registry" subsection), `AGENTS.md` (one bullet), `notes/autodev-runbook.md` (the sync commands)
- Test: `tests/test_mia_activities.py`

**Interfaces:**
- Consumes: `Registry`, `best_effort` (Task 1); `prepared_row` (Task 4).
- Produces (`mia_data/activities.py`):
  - `self.registry: Registry | None` = `Registry.from_config(cfg.service)` when `cfg.service` is set, else None;
    tests may assign one built on a `MockTransport`.
  - `def dataset_fields(self, issue: int, round_id: str, stage: str) -> dict`: `pins = self._pins(round_id)`;
    `row = prepared_row(self._dataset_dir(round_id, pins))`; if `row` → `{**row without "record", "data_stage":
    stage}` (state `prepared`); else `{"state": {"ineligible": "ineligible", "needs-human": "needs-human"}.get(
    stage, "in-progress"), "data_stage": stage}`; always adds `"issue": issue`.
  - `set_stage`: after the label swap, when `state and state.round_id` and `self.registry` is set:
    `best_effort(lambda: registry.put_dataset(pins.record, **fields), f"dataset {pins.record}", activity.logger)`
    run through `asyncio.to_thread`. The record id is `pins.record` (from the issue marker), never the record
    body. The write happens before the comment, so a comment failure never hides it, and failures never fail
    the activity.
- `publish_crops` writes nothing itself: the `done` (or `needs-human`) stage that follows it carries the
  manifest's facts through `prepared_row`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_mia_activities.py
async def test_set_stage_reports_the_dataset_state(tmp_path):
    acts = make_acts(tmp_path)   # the existing helper of this file
    seen = []
    def handler(request):
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={})
    acts.registry = Registry("http://svc", "t", transport=httpx.MockTransport(handler))
    state = await admitted_state(acts)  # a DatasetState with round_id and pins, as other tests build it
    await acts.set_stage(1, "matching", state)
    assert seen[-1] == (f"/datasets/{state.pins.record}", {"state": "in-progress", "data_stage": "matching", "issue": 1})
    await acts.set_stage(1, "ineligible", state)
    assert seen[-1][1]["state"] == "ineligible"
    publish_binding_and_manifest(acts, state)  # writes binding.yaml + matching manifest.json under the dataset dir
    await acts.set_stage(1, "done", state)
    row = seen[-1][1]
    assert row["state"] == "prepared" and row["crops"] == 2 and row["binding_hash"] and row["folder"]
    await acts.set_stage(1, "matching", None)  # no round: no write
    assert len(seen) == 3

async def test_set_stage_survives_a_down_registry(tmp_path, caplog):
    def boom(request):
        raise httpx.ConnectError("down")
    acts = make_acts(tmp_path)
    acts.registry = Registry("http://svc", "t", transport=httpx.MockTransport(boom))
    await acts.set_stage(1, "matching", await admitted_state(acts))   # no exception
    assert "registry write skipped" in caplog.text
    labels = ...  # the label swap still happened
```

- [ ] **Step 2: Run to verify they fail.**
- [ ] **Step 3: Implement**, then the docs:
  - `docs/mia-data-workflow.md`: after "How the issue reads", a paragraph "The registry hears every stage":
    `set_stage` sends the dataset's state to buffer-api (`PUT /datasets/<record>`, best effort); `prepared`
    means binding.yaml plus a matching manifest.json; `mia-buffers datasets sync` rebuilds the prepared rows
    from `storage_root` when the service missed a write.
  - `docs/development.md`: a "The registry (milestone B)" subsection: what the ledger holds now, where it
    lives, the three capability levels and the token, the sync commands with their examples, and
    `POST /plan` with a `curl` example whose spec is read from a file.
  - `AGENTS.md` "Rules that will bite you": `- **Worker routes of buffer-api take the bearer token.** Every
    PUT/POST/DELETE other than the consumer routes needs \`Authorization: Bearer <service.token_file>\`; use
    \`mia_core.registry.Registry\`, never a bare httpx call.`
  - `notes/autodev-runbook.md`: the mia-buffers section gains `mia exec mia-buffers catalog sync` and
    `datasets sync` after the first `mia up`.
- [ ] **Step 4: Run the tests and the four checks.**
- [ ] **Step 5: Commit** — `mia-data: set_stage reports the dataset's state to the registry; docs for milestone B`.

---

## Acceptance (spec §10 row B), to run by hand on the host after the last task

1. `mia restart mia-buffers`; the api log says `ledger <local_root>/service/buffers.sqlite` and, the first time,
   that the old file was moved.
2. `mia exec mia-buffers datasets sync` then `curl $URL/datasets/<the U937 record id>` → `"state": "prepared"`.
3. `mia exec mia-buffers catalog sync --ref <the live-test spec's ref>`; `curl -XPOST $URL/plan --data-binary
   @<(python -c 'import json,sys;print(json.dumps({"spec": open(sys.argv[1]).read()}))' <spec>)` → the same
   pool as `mia-buffers plan <spec> --catalog-dir <checkout>` minus the rows whose `prepared` is not
   `prepared`, each of those with rule `not prepared`.
4. `curl -XPUT $URL/datasets/x -d '{}'` → `401 {"error":"unauthorized"}`; with `-H "Authorization: Bearer
   $(cat ~/.config/mia/service-token)"` → 200.
5. `mia exec mia-data check` and `mia exec mia-buffers check` still pass; a mia-data round's stage comments
   show up as `PUT /datasets` rows (`curl $URL/datasets?state=in-progress` during a round).
