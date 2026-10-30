# Prep and buffering, milestone F: the page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** buffer-api serves one HTML page with four views (datasets, buffers, cache, preparation) that reads only
the service's GET routes, runs the real planner through `POST /plan`, and composes buffer and preparation issues
as prefilled GitHub links; plus the small service additions the page needs (a static route, a meta route, usage
rows, the keeper's last pass).

**Architecture:** the page is `mia_buffers/ui/index.html`, the pool explorer's layout, tokens and chart code with
its embedded JSON replaced by `fetch` calls and its JavaScript planner replaced by `POST /plan`; no build step, no
framework. The service gains `GET /` and `GET /ui/<file>` (bytes responses), `GET /meta` (repo names, root URLs,
quotas), `GET /usage/<buffer>` (daily draws, distinct consumers, empty responses; rows materialised on retire),
`PUT/GET /keeper` (the last keeper pass). A test checks every route the page fetches exists and is open.

**Tech Stack:** as E; `importlib.resources` for the page; vanilla JS, SVG charts (the explorer's `hbars`).

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` v0.2: §1.3, §1.8, §3 (`usage`), §3.1 (`GET /usage`,
`GET /` and `/ui/*`), §5.3 (`health.max_empty_fraction` reported), §6, §7, §9 rows `ledger.py, api.py` (daily
usage rollup, static page), `mia_buffers/ui/`; §10 row F.

Deviations from the spec, decided here so no task re-decides them:

- **`GET /meta`** exposes the repo names (`buffers_repo`, `datasets_repo`, `catalog_repo`), the roots' browser URLs,
  the quotas and the keeper cadence so the page can build GitHub links and meters. Repo names are not credentials;
  the service still holds no token for GitHub. `serve(cfg)` passes them in; the spec's "reads `paths` and `service`,
  never `github`" is relaxed to the two repo names and the catalog repo name.
- **The keeper's last pass** is stored by the worker through `PUT /keeper` (worker route, a small `kv` table) and
  read by the page through `GET /keeper`; the spec listed it on the cache view without a route.
- **Usage rows** are materialised on retire (from the draws being archived) and computed live from `draws` for an
  unretired buffer; `empty` (204) responses are counted per `(buffer, day)` in the `usage` table as they happen.
  `GET /usage/<buffer>` returns the daily rows plus `last_hour: {draws, empty}` and the buffer's
  `max_empty_fraction` so the page can show the health line; nothing acts on it.
- **The buffer's spec YAML** is not stored by the service; the buffer page shows the config (name, hash, levels,
  patch, recipe) and links to the issue, which holds the YAML.
- **Open `prep:*` issues** cannot be listed by a service without GitHub access; the preparation view shows the
  registry's `in-progress`/`needs-human` datasets with issue links and a link to the GitHub search for `prep:`
  labels in the buffers repo.
- **The datasets view's "buffers using it"** comes from the `refs` on the raw asset row (`GET /assets/raw`), not
  from a per-record route.
- **The spec YAML the page composes** covers the controls it has (`select.paths`, `select.where` dims, licence,
  annotations, `min_size_voxels`, `dedup`, `sampling.patch` 2D (+ z when 3D is selected), `levels`, `weighting`
  temperature, `splits.fractions` defaults, `plan.auto_approve: true`, `catalog.ref` = the page's ref); everything
  else takes the spec grammar's defaults. The same YAML text is what the page POSTs to `/plan`, so the issue it opens
  plans identically.
- **Browser testing** is not automated in this repository (no browser in CI); the page's correctness is covered by
  the route-coverage test, the service tests, and the operator's acceptance run.

## Global Constraints

- Branch `training-buffers`, this checkout; one commit per task (Task 1 and Task 2 share no files and run in
  parallel); full checks clean; real-path grep empty; `pyproject.toml` `include` gains `mia_buffers/ui/*`.
- The page uses only consumer and read routes; it holds no token; every action that needs an identity is a
  GitHub link (`https://github.com/<repo>/issues/new?title=<urlencoded>&body=<urlencoded>`); a link longer than
  8,000 characters falls back to "copy YAML" with a notice.
- No external script dependencies; the Google Fonts stylesheet the explorer uses is allowed (same tokens, light
  and dark). No real paths or hostnames in the page; it derives everything from `GET /meta` and the routes.
- Wire format: `BufferConfig` gains `max_empty_fraction: float = 0.05` (default); `buffer_config` emits it from
  `spec.health.max_empty_fraction`; `KeeperReport` unchanged.
- Values from the spec: debounce 300 ms on the plan; four views; a top bar with the catalog ref and when it was
  synced; the datasets table's extra columns (preparation state with issue link, raw cache state and bytes, staged
  levels keys, buffers using it); the three toggles (prepared only, raw cached, staged for this geometry); the
  funnel includes `not prepared`; the buffer page shows events, board, volumes with draws, consumers with last-seen,
  the daily usage sparkline, links to the issue and the Fileglancer folders; the cache view shows two tables, the
  lock holders, two quota meters and the keeper's last pass; the preparation view shows `in-progress` with stage
  and issue and `needs-human` with the reason.

## Review Focus

1. A path traversal on `GET /ui/../something` must 404 (Task 1, `test_ui_route_refuses_traversal`).
2. `GET /usage/<buffer>` on a retired buffer returns the materialised rows and no live rows; on a buffer with no
   draws returns `[]` and zero counts (Task 1, `test_usage_live_and_materialised`).
3. The page must render with an empty registry (no catalog synced, no buffers): the datasets view says "no catalog
   synced; run `mia-buffers catalog sync`", the other views show empty tables, nothing throws (Task 2, by
   construction; the final reviewer runs the service on an empty ledger and fetches the page).
4. A plan over the registry with a spec that fails validation must show the 422 message in the page, not a blank
   (Task 2).
5. The route-coverage test must fail when the page fetches a path no read/consumer route serves (Task 3).

---

### Task 1: service — static page route, meta, usage, keeper pass, `max_empty_fraction`

**Files:**
- Modify: `mia_buffers/api.py`, `mia_buffers/ledger.py`, `mia_buffers/register.py` (`buffer_config` emits
  `max_empty_fraction`), `mia_core/registry.py` (`put_keeper(report: dict)`), `mia_buffers/activities.py` (`keep`
  posts the report best effort at the end), `pyproject.toml`, `mia_buffers/ui/__init__.py` (empty, so the package
  data ships)
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_ledger.py`, `tests/test_buffers_register.py`,
  `tests/test_buffers_activities.py`, `tests/test_core_registry.py`

**Interfaces:**
- `Response = tuple[int, dict | bytes | None, dict[str, str]]`: a `bytes` payload is written as-is with the
  `Content-Type` the handler put in the headers; `make_server._dispatch` handles both.
- Routes (all `read` unless noted): `GET /` → the page (`text/html; charset=utf-8`); `GET /ui/<file>` → a file
  from `mia_buffers/ui/` by basename only (`[A-Za-z0-9._-]+`; anything else 404; content type by suffix for
  `.html .js .css .svg .png`); `GET /meta` → `{"buffers_repo", "datasets_repo", "catalog_repo", "service_url",
  "roots": {"work_root", "storage_root", "raw_root", "staged_root"} (browser URLs or null), "quota": {raw_bytes,
  staged_bytes, headroom_bytes, hold_hours}, "keeper": {minutes, catalog_sync_hours}}`; `GET /usage/<buffer>` →
  `{"buffer", "days": [{"day": "YYYY-MM-DD", "draws", "consumers", "empty"}], "last_hour": {"draws", "empty"},
  "max_empty_fraction"}`; `PUT /keeper` (worker) body any JSON object → stored with `at`; `GET /keeper` → the
  stored object or `null`.
- `BufferService.__init__(..., meta: dict | None = None, ui_dir: Path | None = None)`; `serve(cfg)` passes `meta`
  built from `cfg` and `ui_dir = importlib.resources.files("mia_buffers") / "ui"` (as a `Path` via `as_file` or
  the package's `__file__` directory).
- `Ledger`: `CREATE TABLE usage (buffer TEXT, day TEXT, draws INTEGER, consumers INTEGER, empty INTEGER, PRIMARY
  KEY (buffer, day))`; `CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)`; `count_empty(buffer, day)` (UPSERT
  `empty + 1`, called from `_next`/`_split` on a 204); `usage(buffer) -> list[dict]` = materialised rows merged with
  live rows from `draws` (`strftime('%Y-%m-%d', ts, 'unixepoch')`, `COUNT(*)`, `COUNT(DISTINCT consumer)`) — a day
  present in both keeps the materialised draws/consumers and the `empty` counter; `retire(...)` materialises the
  live rows before deleting the draws; `last_hour(buffer) -> dict` (draws with `ts >= now - 3600`; `empty` from an
  in-memory per-buffer deque of 204 timestamps in the service, pruned to one hour — the table has only days);
  `put_kv(key, value: dict)`, `kv(key) -> dict | None`.
- `Registry.put_keeper(report: dict) -> dict`; the `keep` activity ends with `best_effort(lambda:
  registry.put_keeper({**dataclasses.asdict(report), "at": now, "dry_run": dry_run}), ...)`.

- [ ] **Step 1: Write the failing tests** (`test_root_serves_the_page_and_ui_files`, `test_ui_route_refuses_traversal`,
  `test_meta_route`, `test_usage_live_and_materialised` (draws today and yesterday via a monkeypatched `time.time`
  in `record_draws`, two consumers, two 204s → rows; retire → rows persist, draws gone), `test_keeper_kv_routes`
  (PUT needs the token; GET null before), ledger tests for `count_empty`/`usage`/`put_kv`, `buffer_config` emits
  `max_empty_fraction`, `Registry.put_keeper`, `keep` posts the report).
- [ ] **Step 2: Run → fail. Step 3: Implement. Step 4: Full checks → pass.**
- [ ] **Step 5: Commit** — `buffer-api: the page's routes — static files, meta, usage, the keeper's last pass`.

### Task 2: the page

**Files:**
- Create: `mia_buffers/ui/index.html` (one file; the explorer `notes/pool-explorer.html` is the base for layout,
  tokens, `el`/`svgEl`/`hbars`/tooltip helpers and the table; its embedded `R` data, `ruleFor`/`positions`/
  `evaluate`/`weigh` planner, the sweep chart and the "findings" section are dropped)
- Test: none in pytest beyond Task 3's coverage test; the implementer verifies by running `mia-buffers serve`
  against a scratch `mia.yaml`-less `BufferService` (a tiny `python -c` that starts `make_server` on an empty
  ledger with `meta` and `ui_dir`) and fetching `/` with `curl`, and by loading the file in a browser if one is
  available (not required).

**Interfaces (what the page fetches; all exist after Task 1):**
`GET /meta`, `GET /catalogs`, `GET /records?ref=` (only for the datasets table's titles/paths; the plan rows carry
the rest), `POST /plan` `{spec}`, `GET /datasets`, `GET /assets/raw`, `GET /assets/staged`, `GET /locks`,
`GET /keeper`, `GET /buffers`, `GET /buffers/<id>/status`, `GET /buffers/<id>/datasets`, `GET /buffers/<id>/events`,
`GET /usage/<id>`.

Behaviour:
1. **Shell.** A top bar: title, the four view tabs (hash routing `#datasets`, `#buffers/<id>`, `#cache`,
   `#preparation`), the newest catalog ref (short) and its `synced_at` as "synced N hours ago", a theme toggle (the
   explorer's). Errors from any fetch show in a dismissable banner with the status and message; a 422 from
   `POST /plan` shows under the controls.
2. **Datasets.** The explorer's filter row, restricted to what the planner serves: paths glob (text, default
   `datasets/**`), dims chips, licence chips + allow unknown, annotations required + source, min voxels, patch,
   coarsest level (levels `1..L` doubling), z patch, dedup by DOI, temperature; plus the three registry toggles
   (client-side filters over the plan rows joined with the registry). `toSpecYaml(controls, ref)` builds the YAML
   text (a fixed-shape emitter, 2-space indent, quoted strings where YAML needs it); `POST /plan` debounced 300 ms
   on any change; the response's `rows` (pool and admitted-but-out) and `excluded` drive: KPIs (pool count, seen,
   prepared / in progress / unprepared among the pool rows, bytes of the pool from `size_bytes` when the row has it
   — add `bytes` to the row join from the `records` fetch when absent), the funnel (`excluded` per rule, then the
   rows' own rules for fit / not prepared), pool by modality, weight share, heaviest list, and the records table
   with columns id · decision/rule · dims · modality · licence · preparation (state, link to the dataset issue
   `https://github.com/<datasets_repo>/issues/<n>` when the row has `issue`) · raw cache (state, bytes) · staged
   (levels keys of `GET /assets/staged` rows for the record) · buffers (raw row `refs`) · weight · split. Actions:
   **Download spec YAML** (a Blob download named `<name>.yaml`), **Open buffer issue** (the prefilled link; title =
   the spec name; body = a short line + the ```yaml fence; > 8,000 chars → copy-to-clipboard button + notice),
   **Request preparation** (a prep issue with `records:` = the pool rows whose preparation state is not `prepared`;
   same URL rule; disabled when none).
3. **Buffers.** A table from `GET /buffers` (id, spec name, latest event + age, volumes, missing, servable per split,
   consumers seen, draws today); a row opens `#buffers/<id>`: events list, the board table (record, status, reason,
   job, updated), volumes with draws (`status.draws` per volume id) and `missing` flagged, consumers with last-seen
   and stale, the usage sparkline (SVG bars per day: draws; a second thin series: empty) with the health line
   (`last_hour.empty / (draws + empty)` vs `max_empty_fraction`, coloured), the config card (name, hash short,
   levels, patch, channels, recipe tensorizer + harness commit short, plan hash short), links: the issue
   (`https://github.com/<buffers_repo>/issues/<n>` from the id `i<n>-e…`), the roots (meta URLs). A retired buffer
   shows its usage rows and the archive path from the `retired` event's detail when present.
4. **Cache.** Raw table (record, state, bytes, last used, refs count, job, stream count); staged table (artifact,
   record, levels key, binding hash short, harness commit short, state, bytes, last used, refs, job); lock holders
   (kind, key, owner, age, stale); two meters (raw / staged bytes vs quota; "unlimited" when 0); the keeper's last
   pass card (`at`, scanned/missing/new, evicted, stale, removed building, locked, over quota, errors, catalog).
5. **Preparation.** `in-progress` datasets (record, `data_stage`, issue link, updated), `needs-human` (record,
   issue link, updated), and a link "open preparation requests" to
   `https://github.com/<buffers_repo>/issues?q=is:issue+is:open+label:prep:requested`.
6. Empty states everywhere ("no catalog synced; run `mia-buffers catalog sync`", "no buffers yet", …); no
   exceptions on empty data; numbers formatted (bytes as GB/TB with one decimal; times as relative).

- [ ] **Step 1: Build the page** from the explorer; keep its tokens, fonts, `hbars`, tooltip and table code.
- [ ] **Step 2: Verify** against an empty service and against a scratch service filled by the API tests' helpers
  (a small script in the task report, not committed): the datasets view plans the example spec; the buffers view
  shows a registered buffer; the cache view shows assets and locks. Record the `curl -s localhost:PORT/ | head`
  output and the browser check if done.
- [ ] **Step 3: Commit** — `mia-buffers: the page — datasets, buffers, cache and preparation views over the registry`.

### Task 3: the route-coverage test, docs, the spec's record

**Files:**
- Test: `tests/test_buffers_ui.py` (new): parse every `fetch(` / `api(` path literal and template in
  `mia_buffers/ui/index.html` (a regex over `"/…"` strings and `` `/…${…}` `` templates with `${…}` replaced by `x`),
  resolve each against `ROUTES` with method GET (or POST for `/plan`) and assert the capability is `read` or
  `consumer`; also assert the page contains no `http://` or absolute host other than `https://github.com/` and
  `https://fonts.googleapis.com/`.
- Docs: `docs/development.md` "## The page (milestone F)": the URL, the four views, what each reads, the issue links
  and the 8 KB fallback, the empty-registry message, the usage/health line, the keeper card; `AGENTS.md` (one
  bullet: the page is read-only and holds no token; its only actions are GitHub links); the spec's "**Built in F
  (date).**" paragraph.
- Commit — `docs: the page`.

## Live steps (operator)

1. Restart buffer-api (`mia restart mia-buffers`); open `http://<host>:8765/`.
2. §10-F acceptance: compose a 2D gray buffer on the datasets view, open the issue from the page, add `buffer:go`
   on GitHub (the only click that leaves the page), follow it on the buffers view to `ready`.
