# Prep and buffering, milestone D: the buffering rework — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the buffer plans from the registry (so the plan, the page and the worker agree on "prepared"), a staged
pyramid is built under a temporary name and published by rename, registration applies the plan row's split policy
and pins the recipe, the client refuses a checkout at another harness commit, the service keeps a per-dataset
status board and a lifecycle event log, a ready buffer is watched (missing pyramids, idle, retire), and waiting
datasets share one wake-up timer per round instead of one timer each.

**Architecture:** the service (`ledger.py`, `api.py`) gains two tables and five routes plus `missing` on volumes;
the planner and registration modules (`plan.py`, `register.py`, `client.py`, `spec.py`) carry the split policy and
the recipe without touching the workflow; the worker's activities plan through the registry, publish artifacts by
rename, write the board and events, and give the keeper a lifecycle step that signals the buffer workflow; the
workflow adds four signals and a ready-state loop behind `workflow.patched("d-lifecycle")`, and replaces the
per-dataset backoff timers with one ticker task per round behind `workflow.patched("d-tick")`.

**Tech Stack:** as C (Python 3.12 stdlib, httpx, pydantic 2, Temporal, csub; pytest/ruff/mypy via pixi); `gzip`
for the draws archive.

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` v0.2: §1.10, §1.13, §3 (`refs`, `buffer_datasets`,
`buffer_events`, draws archive), §3.1 (`GET /records`, `PUT/GET /buffers/<id>/datasets`, `POST/GET
/buffers/<id>/events`, `DELETE /buffers/<id>/refs`, `GET /buffers`, `status` stats pyramids), §5.1, §5.2 steps 3–4,
§5.3, §5.7, §9 rows `plan.py, select.py`, `register.py, client.py`, `workflows.py`, `activities.py`, `spec.py`,
`ledger.py, api.py`; §10 row D (acceptance).

Deviations from the spec, decided here so no task re-decides them:

- **Patch markers again.** The host runs rounds; every workflow change sits behind `workflow.patched("d-lifecycle")`
  (signals, auto-approve, the board, the ready-state loop) or `workflow.patched("d-tick")` (the shared wake-up).
  The `c-cache` branch and the three existing fixtures stay; a fourth fixture `buffer_history_d.json` is recorded.
- **`degraded` carries the records.** The keeper does not know weighting groups; it signals `degraded(records,
  reason)` with the records whose volumes are `missing`, and the workflow (which knows groups and
  `ready.min_fraction`) re-stages them when their raw asset is `ready`, else goes `needs-human` only if a group is
  below the floor. A re-stage reuses the dataset's normal path (lock, raw status, stage, register) under the same
  artifact id; the rename replaces the missing directory.
- **Idle and retire are detected by the keeper** from `GET /buffers` (newest consumer `last_seen`, the latest
  event) and delivered as signals; `buffer:keep` and `buffer:retire` labels are delivered by the poller like `go`.
  The per-buffer thresholds (`idle.hours`, `retire_after_days`) travel in the buffer config so the keeper reads them
  from the service, not from the issue.
- **`health.max_empty_fraction`** is accepted by the spec model and reported nowhere yet (F's page shows it).
- **`usage` rows** (daily rollup) are F; on retire the draws are archived and deleted, and `GET /buffers` counts
  today's draws live.
- **Plan from the registry, records still snapshotted.** The `plan` activity reads the records and the prepared set
  from the registry (syncing the ref through `catalog_sync` when the registry lacks it) and runs `build_plan` with
  `prepared=`; the git snapshot at `work_root/_catalog/<ref>` stays, because jobs read `record.yaml` from it.
- **`plan_hash`** is the sha256 of the `plan.json` text the round wrote; `spec_hash` stays as is.
- **The hand `register` CLI** pins the recipe from `harness_pin(cfg)` at run time and takes `--plan-hash` from the
  plan file it is given.
- **`sourcing.unprepared: skip`** is the only value; the field exists in the spec model and is validated.
- **The one-shared-wake-up fix** replaces the C backoff timers in `_lock` and `_quota`: a round runs one ticker
  (`poll_minutes`, doubling while no dataset made progress, capped at 30 min); waiters `wait_condition` on the tick
  counter. mia-data's `_hold_raw` keeps its own backoff (one dataset per workflow).

## Global Constraints

- Branch `training-buffers`, this checkout; one commit per task (Task 1 and Task 2 may run in parallel: they share
  no files); no merge, no PR. `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test -q` clean per
  task; `git diff | grep -n 'rokickik\|/nrs/'` empty (the one `AGENTS.md` hit is the rule's own text).
- Wire format: `mia_buffers/models.py` and `mia_buffers/sampler.py::Volume` fields are added with defaults only.
  This milestone adds `Volume.missing: bool = False`; `DatasetStatus.rebuilds: int = 0`; `BufferState.idle_since:
  str | None = None`, `BufferState.retired: bool = False`; `PlanResult.auto_approve: bool = False`,
  `PlanResult.plan_hash: str = ""`; `SourcingLimits.max_tick_minutes: int = 30`; `DegradedSignal`,
  `LifecycleEvent` as new dataclasses; `BufferConfig` (pydantic, `api.py`) gains `recipe: dict | None = None`,
  `plan_hash: str | None = None`, `idle_hours: float = 72`, `retire_after_days: float = 14`, `auto_approve: bool
  = False`.
- Workflow changes only inside `workflow.patched("d-lifecycle")` / `workflow.patched("d-tick")`; the three existing
  fixtures (`buffer_history.json`, `buffer_history_c.json`, mia-data's) untouched and passing; `buffer_history_d.json`
  recorded with a scoped `-k replay_buffer_history_d`.
- Values from the spec: event kinds `created · planned · approved · ready · idle · retired · needs-human`; status
  board statuses are `DatasetStatus.status` values; `idle.hours` default 72; `retire_after_days` default 14;
  `health.max_empty_fraction` default 0.05; `plan.auto_approve` default false; `sourcing.unprepared` only `skip`;
  the staged build directory `<levels key>/<record>/.building-<artifact id>-<job key>`; `stage.json` gains
  `artifact: {id, binding_hash, levels_key, voxel_size_nm, harness_commit, env_digest}` on publish; the archive
  `<state_dir>/archive/<buffer>.draws.jsonl.gz`; `recipe: {tensorizer, params, harness_commit, env_digest}` on the
  buffer config; a holdout record's every plane is `test`; `split.seed` = `splits.seed`; `splits.stratify_by`
  non-empty is a plan error; `BufferDataset(..., allow_recipe_mismatch=False)` raises on a mismatch, warns once
  and records `meta["harness_commit"]` when allowed; `GET /buffers/<id>/status` stats each volume path once per
  call and `next` skips `missing` volumes.
- Untrusted job output (`stage.json`) is read through `read_untrusted`/`loads`; the publish rename happens in the
  worker, never in a job; the service deletes nothing on disk (the archive is a file it writes).
- Import direction unchanged; `mia_core` imports no application package.

## Review Focus

1. A `stage.json` that is `ok` but whose directory rename fails (the final name already exists from a hand build
   or a concurrent buffer) must not leave the dataset failed: the existing artifact is reused and the build
   directory removed (Task 3, `test_read_stage_publishes_by_rename_and_reuses_an_existing_artifact`).
2. A `retire` on a buffer another buffer's volumes do not reference must make its staged assets free (refs gone)
   while a raw asset shared with a live buffer keeps its ref (Task 1, `test_retire_drops_only_this_buffers_refs`).
3. A `degraded` signal for a record whose raw cache is also gone must end in `needs-human` naming the record, not
   in a fetch loop (the binding's URLs may be unreachable now) — unless the group is still above the floor, in
   which case the buffer stays `ready` with the volume missing (Task 4, `test_degraded_without_raw_is_needs_human_only_below_the_floor`).
4. `BufferDataset` on a buffer registered before D (no `recipe` in the config) must still work (Task 2,
   `test_dataset_without_a_recipe_is_accepted`).
5. The ticker must not keep a finished round alive: when every dataset is done (or the round is abandoned) the
   ticker task is cancelled and no further timers fire (Task 4, `test_ticker_stops_with_the_round`).
6. `GET /buffers/<id>/status` on a buffer with a thousand volumes stats a thousand paths per call; the consumer's
   `status()` at construction is the only caller today, but `GET /buffers` must not stat (it reads the stored
   `missing` flags) (Task 1, `test_buffers_list_does_not_stat`).

---

### Task 1: the service — records route, status board, events, retire with archive, missing volumes, `GET /buffers`

**Files:**
- Modify: `mia_buffers/ledger.py`, `mia_buffers/api.py`, `mia_buffers/sampler.py` (`Volume.missing`,
  `servable` excludes missing), `mia_core/registry.py`
- Test: `tests/test_buffers_ledger.py`, `tests/test_buffers_api.py`, `tests/test_buffers_sampler.py`,
  `tests/test_core_registry.py`

**Interfaces:**
- Produces, `Ledger`:
  - `CREATE TABLE buffer_datasets (buffer TEXT, record TEXT, status TEXT, reason TEXT, job TEXT, updated_at REAL,
    PRIMARY KEY (buffer, record))`; `CREATE TABLE buffer_events (buffer TEXT, seq INTEGER, kind TEXT, at REAL,
    issue INTEGER, event_id TEXT, detail TEXT, PRIMARY KEY (buffer, seq))`.
  - `put_buffer_dataset(buffer, record, status, reason="", job=None) -> dict`, `buffer_datasets(buffer) ->
    list[dict]` (`record, status, reason, job, updated_at`, by record).
  - `add_event(buffer, kind, *, issue: int | None, event_id: str | None, detail: str) -> dict` (next `seq`, `at`
    = now), `events(buffer) -> list[dict]`, `latest_event(buffer) -> dict | None`.
  - `buffers() -> list[dict]`: every buffer id with `config`, `latest_event`, `volumes` (count), `missing`
    (count of volumes with `missing`), `servable` (per split, excluding missing), `consumers_seen` (newest
    `last_seen` or None), `draws_today` (draws with `ts >= midnight UTC`), from stored rows only (no stat).
  - `set_volume_missing(buffer, volume_id, missing: bool) -> None`; `volumes()` returns the flag.
  - `retire(buffer, archive_dir: Path | None) -> dict`: in one transaction delete this buffer's `refs` rows and its
    `draws` rows after writing them to `<archive_dir>/<buffer>.draws.jsonl.gz` (one JSON object per line: `seq,
    consumer, ts, descriptor`); when `archive_dir` is None the draws are kept (a dev service); returns `{"refs": n,
    "draws": n, "archive": path | None}`. Volumes and the config stay (the page shows a retired buffer).
- Produces, `api.py` routes (capability in brackets): `GET /records?ref=` [read] → `{"ref", "records": [{"path",
  "record"}]}` (ref defaults to the newest synced; 404 when none); `PUT /buffers/<id>/datasets/<record>` [worker]
  body `{status, reason?, job?}` (status must be a `DatasetStatus` value: `pending · waiting-dataset · waiting-lock ·
  waiting-drop · fetching · blocked · staging · servable · failed · dropped`); `GET /buffers/<id>/datasets`
  [read]; `POST /buffers/<id>/events` [worker] body `{kind, issue?, event_id?, detail?}` (kind in the seven);
  `GET /buffers/<id>/events` [read]; `DELETE /buffers/<id>/refs` [worker] → `ledger.retire(...)`; `GET /buffers`
  [read] → `{"buffers": ledger.buffers()}`. `GET /buffers/<id>/status` stats every volume's `path` once
  (`os.path.isdir`), updates the `missing` flag when it changed, excludes missing volumes from `servable` and adds
  `"missing": [volume ids]`. `BufferService(..., archive_dir: Path | None = None)`; `serve(cfg)` passes
  `cfg.paths.state_dir / "archive"`. `BufferConfig` gains the five fields of the Global Constraints.
- Produces, `sampler.py`: `Volume.missing: bool = False`; `servable()` skips `missing` volumes (so `next` and
  `split` do).
- Produces, `Registry`: `records(ref) -> list[tuple[str, dict]]`, `put_buffer_dataset(buffer, record, status,
  reason="", job=None) -> dict`, `event(buffer, kind, **fields) -> dict`, `buffers() -> list[dict]`,
  `buffer_datasets(buffer) -> list[dict]`, `retire(buffer) -> dict`, `status(buffer) -> dict`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_buffers_ledger.py (append)
def test_status_board_and_events(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ledger.put_buffer_dataset("b1", "rec", "fetching", job="j1")
    row = ledger.put_buffer_dataset("b1", "rec", "servable")
    assert row["status"] == "servable" and row["job"] is None and ledger.buffer_datasets("b1")[0]["record"] == "rec"
    first = ledger.add_event("b1", "created", issue=2, event_id="e1", detail="")
    ledger.add_event("b1", "ready", issue=2, event_id=None, detail="2 of 2")
    assert first["seq"] == 1 and ledger.latest_event("b1")["kind"] == "ready" and len(ledger.events("b1")) == 2

def test_retire_drops_only_this_buffers_refs(tmp_path):
    ledger = Ledger(tmp_path / "l.sqlite")
    ... # put_buffer b1 and b2; put_volume for both with refs ("staged", "artA")/("raw", "rec") for b1 and
        # ("staged", "artB")/("raw", "rec") for b2; record_draws for b1 (two draws)
    out = ledger.retire("b1", tmp_path / "archive")
    assert out["refs"] == 2 and out["draws"] == 2 and (tmp_path / "archive" / "b1.draws.jsonl.gz").is_file()
    assert ledger.refs("staged", "artA") == [] and ledger.refs("raw", "rec") == ["b2"]
    assert ledger.max_seq("b1") == 0 and len(ledger.volumes("b1")) == 1  # volumes stay for the page
    with gzip.open(tmp_path / "archive" / "b1.draws.jsonl.gz", "rt") as fh:
        assert [json.loads(line)["seq"] for line in fh] == [1, 2]

def test_buffers_summary_and_missing_flag(tmp_path): ...  # buffers() shows latest_event, servable excludes a
                                                           # volume set missing, draws_today counts only today's

# tests/test_buffers_api.py (append)
def test_records_route_defaults_to_the_newest_ref(tmp_path): ...  # PUT /records/<ref>, GET /records → that ref; GET /records?ref=unknown → 404
def test_status_board_routes(tmp_path): ...   # PUT needs the token; bad status 422; GET lists; events POST/GET; unknown kind 422
def test_retire_route_archives_and_frees_refs(tmp_path): ...
def test_status_stats_pyramids_and_next_skips_missing(tmp_path): ...  # register two volumes with real dirs; rmtree one; GET status → missing [id]; next never returns it; GET /buffers reads the flag without stat (monkeypatch os.path.isdir to raise)
def test_buffers_list_does_not_stat(tmp_path): ...  # (Review Focus 6)
```

- [ ] **Step 2: Run** the four files → fail on the new names.
- [ ] **Step 3: Implement**; `BufferConfig` fields; `servable` skips missing; client methods and their tests.
- [ ] **Step 4: Full checks** → pass. **Step 5: Commit** — `buffer-api: records route, status board, events, retire with a draws archive, missing volumes, GET /buffers`.

### Task 2: the plan row's split policy, the recipe pin, spec fields, the client's recipe check

**Files:**
- Modify: `mia_buffers/spec.py` (`PlanOptions(auto_approve: bool = False)` as `Spec.plan`; `Sourcing.unprepared:
  Literal["skip"] = "skip"`; `Idle(hours: float = 72)` as `Spec.idle`; `Spec.retire_after_days: float = 14`;
  `Health(max_empty_fraction: float = 0.05)` as `Spec.health`), `mia_buffers/plan.py` (`Row.split: dict | None =
  None`; `splits.stratify_by` non-empty → plan error `splits.stratify_by is not implemented; remove it`),
  `mia_buffers/register.py` (`volume_bodies` reads `row["split"]`; `buffer_config(..., recipe: dict, plan_hash:
  str)` adds `recipe`, `plan_hash`, `idle_hours`, `retire_after_days`, `auto_approve`), `mia_buffers/cli.py`
  (`register` command: `--plan-hash` optional, recipe from `harness_pin(cfg)` + `cfg.sourcing.tensorizer` +
  the spec's normalize params), `mia_buffers/client.py`
- Test: `tests/test_buffers_spec.py`, `tests/test_buffers_plan.py`, `tests/test_buffers_register.py`,
  `tests/test_buffers_client.py`, `tests/test_buffers_cli.py`

**Interfaces:**
- Produces, `plan.py`: for every row in the pool `row.split = {"policy": "holdout" if row.id in
  spec.splits.holdout_ids else "fractions", "fractions": dict(spec.splits.fractions), "seed": spec.splits.seed}`
  (also in `to_dict`). `plan_hash(plan_json_text: str) -> str` = sha256 hex of the text (a module function;
  Task 3 uses it).
- Produces, `register.py`: `_split_planes(volume_id: str, planes: int, split: dict) -> dict[str, list[int]]` —
  policy `holdout` → `{name: all planes if name == "test" else []}`; else the seeded shuffle with
  `random.Random(f"{split['seed']}:{volume_id}")` and the row's fractions. `volume_bodies` raises `ValueError(
  f"{row['id']}: plan row has no split policy; plan again")` when `row.get("split")` is None. `buffer_config(plan,
  spec, *, tensorizer, split_sizes, recipe: dict, plan_hash: str) -> dict` adds `recipe` (as given), `plan_hash`,
  `idle_hours=spec.idle.hours`, `retire_after_days=spec.retire_after_days`, `auto_approve=spec.plan.auto_approve`.
  `recipe_for(spec, *, tensorizer: str, harness_commit: str, env_digest: str) -> dict` builds `{"tensorizer",
  "params": the same `tensorizer_params` dict `buffer_config` emits, "harness_commit", "env_digest"}`.
- Produces, `client.py`: `BufferDataset(..., allow_recipe_mismatch: bool = False)`; `harness_commit(tensorizers:
  Path) -> str | None` = `os.environ.get("MIA_HARNESS_COMMIT")` or `git -C <tensorizers> rev-parse HEAD` (None
  when neither works). A config with `recipe.harness_commit` that differs from a known checkout commit raises
  `RuntimeError(f"buffer {id} was staged with harness {pinned[:12]}, this checkout is {actual[:12]}; pass
  allow_recipe_mismatch=True to use it anyway")`; allowed → `warnings.warn` once and every sample's
  `meta["harness_commit"] = actual`. No `recipe` in the config, or no commit determinable → accepted silently.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_buffers_plan.py
def test_rows_carry_the_split_policy(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.holdout_ids": [U937]}), "h", None, records())
    assert plan.rows_by_id[U937].split == {"policy": "holdout", "fractions": {"train": 0.8, "val": 0.1, "test": 0.1}, "seed": 0}
    assert plan.rows_by_id[BCSS].split["policy"] == "fractions" and plan.to_dict()["rows"][0]["split"]
def test_stratify_by_is_a_plan_error(spec_2d):
    plan = build_plan(spec_with(spec_2d, **{"splits.stratify_by": ["modality"]}), "h", None, records())
    assert "splits.stratify_by is not implemented; remove it" in plan.errors

# tests/test_buffers_register.py
def test_holdout_volume_registers_all_test(spec_2d, tmp_path): ...  # row split policy holdout → split_planes == {"train": [], "val": [], "test": [0..n-1]}
def test_split_seed_comes_from_the_row_not_the_spec(spec_2d, tmp_path): ...  # two rows differing only in split.seed give different plane sets; spec.sampling.seed change gives the same
def test_volume_bodies_refuse_a_row_without_a_split(spec_2d, tmp_path): ...
def test_buffer_config_carries_recipe_plan_hash_and_lifecycle(spec_2d): ...  # recipe_for(...)["params"] == config["tensorizer_params"]; idle_hours 72; retire_after_days 14; auto_approve False

# tests/test_buffers_client.py
def test_dataset_refuses_a_checkout_at_another_commit(buffer_server, monkeypatch): ...  # config recipe commit "a"*40, MIA_HARNESS_COMMIT "b"*40 → RuntimeError; allow → one warning, meta["harness_commit"] == "b"*40
def test_dataset_without_a_recipe_is_accepted(buffer_server, monkeypatch): ...  # (Review Focus 4)

# tests/test_buffers_spec.py
def test_lifecycle_fields_and_defaults(): ...  # plan.auto_approve False; sourcing.unprepared "skip" only ("request" → SpecError); idle.hours 72; retire_after_days 14; health.max_empty_fraction 0.05
```

- [ ] **Step 2: Run** → fail. **Step 3: Implement.** **Step 4: Full checks** → pass.
- [ ] **Step 5: Commit** — `mia-buffers: the plan row decides the split, registration pins the recipe, the client checks the harness commit`.

### Task 3: worker activities — plan from the registry, publish by rename, board and events, retire, keeper lifecycle

**Files:**
- Modify: `mia_buffers/activities.py`, `mia_buffers/jobspec.py` (none expected; `stage_job` takes `args.out`),
  `mia_buffers/models.py` (`PlanResult.auto_approve`, `.plan_hash`; `DegradedSignal(records: list[str], reason:
  str)`; `LifecycleEvent(kind: str, detail: str = "")`; `KeeperReport.signalled: list[str] = []`),
  `mia_buffers/config.py` (`HUMAN_LABELS` gains `keep` and `retire` with descriptions), `mia_buffers/cli.py`
  (`init-labels` picks them up; `create_buffer` recipe)
- Test: `tests/test_buffers_activities.py`, `tests/test_buffers_cli.py`

**Interfaces:**
- Consumes: Task 1's routes and client methods; Task 2's `plan_hash`, `recipe_for`, `buffer_config` signature.
- Produces (activities):
  - `plan(issue, round_id, pins) -> PlanResult`: `ref = pins.catalog_ref`; if `ref` not in `registry.catalogs()`
    refs → `await catalog_sync(cfg, gh_catalog, registry, ref)`; `records = registry.records(ref)`; `prepared =
    {row["id"] for row in registry.datasets(state="prepared")}`; `build_plan(spec, spec_hash, ref, records,
    prepared=prepared)`; writes `plan.json`/`plan.md` as now; `PlanResult.auto_approve = spec.plan.auto_approve`,
    `.plan_hash = plan_hash(plan_json_text)`. A registry outage raises (the activity retries). The git snapshot
    is still taken in `admit` for `record.yaml`.
  - `create_buffer(round_id, buffer_id)`: `buffer_config(..., recipe=recipe_for(spec, tensorizer=cfg.sourcing.tensorizer,
    harness_commit=pins.harness_commit, env_digest=pins.env_digest), plan_hash=<from the round's plan.json text>)`;
    It writes no event: `set_stage("sourcing")` writes `approved`.
  - `_building_dir(round_id, record) -> Path` = `<staged_root>/<levels key>/<record>/.building-<artifact>-<job key>`
    (the stage job key, `job_key(issue, round_id, record)`); `submit` sets `args.out` to it (the directory is
    created by `prepare_stage`); `read_stage(round_id, record)`: read `stage.json` from the building dir when it
    exists, else from the final dir; when ok and the building dir exists: write the `artifact` block into the
    report (`write_atomic`), then `os.rename(building, final)`; `FileExistsError`/`OSError` because `final` exists
    → `shutil.rmtree(building)` and continue with `final` (reuse); not ok → leave the building dir. Idempotent on
    retry. `_record_assets` unchanged (reads the final dir).
  - `mark_dataset(buffer, record, status, reason="", job=None) -> None` best effort (`registry.put_buffer_dataset`).
  - `set_stage(...)`: after the label swap, best-effort `registry.event(state.buffer_id or round_id, kind,
    issue=issue, event_id=None, detail=detail[:200])` with kind = `{"planning": "created", "plan-review":
    "planned", "sourcing": "approved", "ready": "ready", "idle": "idle", "retired": "retired", "needs-human":
    "needs-human"}[stage]` when the stage is in that map (the buffer id is the round id, so a buffer's events start with
    `created` from `planning`).
  - `retire_buffer(round_id, buffer_id) -> dict` → `registry.retire(buffer_id)` (raises on failure: the workflow
    retries).
  - `raw_ready(round_id, record) -> bool` → `raw_status(...).ready` (for `degraded`).
  - Keeper step `_keeper_buffers(report, now, dry_run)`, run after the syncs: for each buffer from
    `registry.buffers()` whose latest event is `ready` or `idle` (a buffer id parses as `i<issue>-e<event>`;
    others skipped): `status = registry.status(buffer)` (this stats the pyramids); if `status["missing"]` →
    signal `degraded(DegradedSignal(records of the missing volumes, "pyramid missing: <ids>"))` to
    `cfg.workflow_id(issue)` once per pass per buffer (report `signalled`); if the latest event is `ready` and
    `consumers_seen` is None or older than `config["idle_hours"] * 3600` and the ready event itself is older than
    that → signal `idle`; if the latest event is `idle` and older than `config["retire_after_days"] * 86400` →
    signal `retire`. Signals go through `self.temporal.get_workflow_handle(...).signal(name, arg)`; a missing
    workflow is logged, not an error. Dry run reports what it would signal.
- `HUMAN_LABELS`: `keep: ("0e8a16", "Keep this idle buffer; back to ready")`, `retire: ("b60205", "Retire this
  buffer: release its staged data")`; the poller's `deliver_label` loop over `HUMAN_LABELS` then delivers them as
  signals `keep`/`retire` with a `LabelEvent` (the workflow signal signatures in Task 4 take `LabelEvent`).

- [ ] **Step 1: Write the failing tests** (the `acts`/`server` fixtures):

```python
async def test_plan_reads_the_registry_and_syncs_the_ref_on_demand(acts, server, monkeypatch): ...  # catalogs empty → catalog_sync called once (monkeypatched to PUT the fixture records); prepared from datasets; rows not prepared carry the rule; auto_approve and plan_hash set
async def test_read_stage_publishes_by_rename_and_reuses_an_existing_artifact(acts, server): ...  # stage.json in the building dir, ok → final dir exists with the artifact block, building gone; second call idempotent; when final already exists → building removed, result ok
async def test_read_stage_leaves_a_failed_build_for_the_keeper(acts, server): ...
async def test_set_stage_writes_the_event(acts, server): ...  # GET /buffers/<id>/events shows created/planned/approved/ready in order
async def test_mark_dataset_and_retire(acts, server): ...
async def test_keeper_signals_degraded_idle_and_retire(acts, server, monkeypatch): ...  # fake temporal handle records (workflow id, signal, arg); a buffer with a missing volume → degraded with the record; ready older than idle_hours with no consumer → idle; idle older than retire_after_days → retire; dry run signals nothing
```

  And in `tests/test_buffers_cli.py`: `init-labels` creates `buffer:keep` and `buffer:retire`.
- [ ] **Step 2: Run** → fail. **Step 3: Implement.** **Step 4: Full checks** → pass.
- [ ] **Step 5: Commit** — `mia-buffers: plan from the registry, publish by rename, status board and events, retire, the keeper watches ready buffers`.

### Task 4: the workflow — auto-approve, the board, the shared ticker, lifecycle signals, degraded re-stage

**Files:**
- Modify: `mia_buffers/workflows.py`, `mia_buffers/models.py` (`DatasetStatus.rebuilds`, `BufferState.idle_since`,
  `BufferState.retired`, `SourcingLimits.max_tick_minutes`)
- Test: `tests/test_buffers_workflows.py`, new fixture `tests/fixtures/buffers/buffer_history_d.json`

**Interfaces:**
- Consumes: Task 3's activities (`mark_dataset`, `retire_buffer`, `raw_ready`, `plan` with `auto_approve`).
- Behaviour, all under `workflow.patched("d-lifecycle")` unless noted:
  1. **Signals** `degraded(self, signal: DegradedSignal)`, `idle(self, event: LabelEvent | None = None)`,
     `keep(self, event: LabelEvent)`, `retire(self, event: LabelEvent | None = None)` append to
     `self.lifecycle: list[tuple[str, object]]`.
  2. **Auto-approve.** In `_round`, when `plan.auto_approve` and the plan is valid: skip `plan-review`, comment
     once ``plan.auto_approve: sourcing starts without review``, go straight to `_sourcing`.
  3. **Board.** A helper `_set(dataset, status, reason="")` sets the fields and, when the status changed, calls
     `mark_dataset(buffer_id, record, status, reason)` as a fire-and-forget activity (`asyncio.create_task` of
     `_act(...)`, errors logged) — the board is best effort and must not slow sourcing. Every assignment of
     `dataset.status` in the `c-cache` path goes through `_set` when `patched("d-lifecycle")`.
  4. **Ready-state loop.** After `_announce_ready` and the stragglers, instead of returning to the main loop, the
     round enters `_steady()`: `wait_condition(lambda: self.lifecycle or self.go_events or self.approve_events or
     _rollover_due())`; `go` → return (the main loop plans again; the buffer stays registered); `approve` →
     refused; `idle` → stage `idle`, `state.idle_since = now`, comment; `keep` → stage `ready` (comment ``kept``),
     `idle_since = None`; `retire` → `retire_buffer`, stage `retired` with the archive path in the comment,
     `state.retired = True`, return; `degraded(records, reason)` → for each named record whose dataset is
     `servable`: if `raw_ready` → `_set(dataset, "pending")`, `dataset.rebuilds += 1`, re-run `_source(...)` for
     it with fresh semaphores (`parallel`, `parallel_fetch`), then if `_ready_now()` stay `ready` (comment
     ``rebuilt <records>``) else `needs-human`; if `raw_ready` is False → mark the dataset `failed` with reason
     `pyramid missing and raw cache gone`; after the pass, `needs-human` only when a group is below the floor
     (`not _ready_now()`), naming the records. A rollover inside `_steady` continues-as-new with the state
     (stage `ready`/`idle`), and `run()` re-enters `_steady()` when `state.stage in ("ready", "idle")`.
  5. **Shared ticker** (`workflow.patched("d-tick")`): `_sourcing` starts `self._ticker = asyncio.create_task(
     self._tick_forever(limits))` and cancels it in a `finally`; `_tick_forever`: `quiet = 0; while True: await
     workflow.sleep(timedelta(minutes=min(limits.poll_minutes * 2**quiet, limits.max_tick_minutes))); self.tick
     += 1; quiet = 0 if self.progress != seen else quiet + 1; seen = self.progress`. `_set` bumps `self.progress`.
     `_lock` and `_quota` replace `workflow.sleep(_backoff(...))` with `await self._next_tick()` (`seen =
     self.tick; await workflow.wait_condition(lambda: self.tick > seen)`); under the old patch they keep the
     backoff.
- Comment texts: idle ``No consumer for {hours:g} hours: idle. `buffer:keep` returns it to ready; `buffer:retire`
  or {days:g} days of idleness retires it.``; retired ``Retired: refs released; draws archived to {path}``.

- [ ] **Step 1: Write the failing tests** (the `Mocks` harness; add fakes `mark_dataset`, `retire_buffer`, `raw_ready`):
  - `test_auto_approve_skips_review`;
  - `test_board_is_written_on_every_status_change` (the fake records `(record, status)` pairs in order:
    pending → fetching → staging → servable);
  - `test_idle_keep_and_retire` (signals in that order; stages idle → ready → retired; `retire_buffer` called
    once; the comment names the archive path);
  - `test_degraded_rebuilds_from_the_raw_cache` (one servable dataset signalled; `raw_ready` True → the stage
    path runs again, `rebuilds == 1`, buffer stays ready);
  - `test_degraded_without_raw_is_needs_human_only_below_the_floor` (two datasets, min_fraction 0.5: one
    degraded with `raw_ready` False → stays ready with that dataset `failed`; both degraded → needs-human);
  - `test_waiters_share_one_ticker` (three datasets `waiting-lock`; the history has one timer per tick, not three;
    timers 2, 4, 8 … minutes while nothing progresses; a status change resets to 2);
  - `test_ticker_stops_with_the_round` (Review Focus 5);
  - `test_replay_buffer_history_d` on the recorded fixture; the three existing replay tests unchanged.
- [ ] **Step 2: Run** → fail. **Step 3: Implement.** **Step 4: Record the fixture** (scoped `-k`). **Step 5: Full checks** → pass.
- [ ] **Step 6: Commit** — `mia-buffers: auto-approve, the status board, one ticker per round, idle/keep/retire, degraded re-stage`.

### Task 5: documentation and the spec's record

**Files:**
- Modify: `docs/development.md` (a section "## The buffer's life after ready (milestone D)": plan from the
  registry and what `not prepared` means for a buffer, auto-approve, the board and events routes, publish by rename
  and the `.building-*` name, the recipe pin and `allow_recipe_mismatch`, idle/keep/retire with the labels and the
  archive, degraded, the ticker; the "Serving a buffer" section's `register` step gains `--plan-hash` and the
  recipe note), `AGENTS.md` (one bullet: a staged artifact is published by the worker's rename and never written
  again), `notes/specs/2026-10-25-prep-and-buffering.md` (a "**Built in D (date).**" paragraph listing this plan's
  deviations one line each), `mia.yaml.example` if any key changed (none expected).
- [ ] Write, `pixi run lint`, real-path grep, commit — `docs: the buffer after ready — board, events, recipe, idle/retire, degraded`.

## Live steps after the branch is green (operator)

1. `mia exec mia-buffers init-labels` (adds `buffer:keep`, `buffer:retire`).
2. Restart mia-buffers; a mid-round restart is safe (`patched()`); rounds admitted before it stay on the C path.
3. `buffer:go` on the live-test issue: the plan comment now lists `not prepared` records from the registry; with
   `plan.auto_approve: true` in the spec, sourcing starts on `go`.
4. §10-D acceptance: a second buffer with the same geometry reaches `ready` with no stage job; delete a pyramid
   directory under a ready buffer → `GET /buffers/<id>/status` shows it `missing`, the next keeper pass signals
   `degraded`, the rebuild lands under the same artifact id; `buffer:retire` → refs released, the keeper evicts
   once the quota asks.
