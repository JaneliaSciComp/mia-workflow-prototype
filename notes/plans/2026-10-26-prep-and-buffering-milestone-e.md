# Prep and buffering, milestone E: preparation requests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** asking for a dataset to be prepared is an explicit action: a `prep:go` issue in the buffers repo that a
small workflow turns into mia-data issues with `data:go`, follows through the registry, and closes; the buffer
workflow no longer nudges mia-data or waits for bindings, and the poller's binding watch goes.

**Architecture:** one new workflow `MiaPrep` beside `MiaBuffer`, driven by the same poller and label discipline
(`prep:go` → `prep:requested` → `prep:done | prep:needs-human`). Its activities reuse `nudge_dataset`, the issue
fence parsing and the catalog snapshot through a prep "round" directory, and read dataset states from the
registry. `MiaBuffer`'s not-found binding becomes a plain failure naming the preparation request (the plan from the
registry already keeps unprepared records out of the pool), behind `workflow.patched("e-prep")`.

**Tech Stack:** as D.

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` v0.2: §1.1, §4.1, §4.3 (the `evaluate.enabled` switch
only), §4.4, §9 rows `workflows.py` (`MiaPrep`), `activities.py` (`nudge_dataset` moves to `MiaPrep`'s use; `poll`
drops the binding watch, adds `prep:go`), `mia_data/config.py` (`evaluate.enabled`); §10 row E.

Deviations from the spec, decided here so no task re-decides them:

- **Patch marker `e-prep`** on the one `MiaBuffer` change (a not-found binding fails the dataset instead of nudging
  and waiting). The `dataset_update` signal and `_await_dataset` stay in the file for pre-E runs; the poller's
  `notice_datasets` is removed, so a pre-E round still in `waiting-dataset` never wakes: it must be re-planned
  (`buffer:go`) once the dataset is prepared. The host's live round is in that state and is documented as such.
- **A prep issue's records** are either the explicit `records:` list or the result of the buffer spec's `select`
  block over the registry's records at the request's `catalog.ref` (selection only: no fit, no weights).
- **Progress is read from the registry** (`GET /datasets`), polled on a backoff (`poll_minutes` doubling to 30 min,
  as the buffer waits do): a record is settled when its row is `prepared`, `ineligible` or `needs-human`, or when
  `nudge_dataset` reported it ineligible; `prep:done` when every record is settled (and none is `needs-human`),
  `prep:needs-human` naming the rest when any is `needs-human` or after `timeout_days` (prep spec field, default 14).
- **`evaluate.enabled: false`** is added to mia-data's config as a switch only; no evaluate job is built.
- **Workflow id** `mia-buffers/<buffers_repo>/prep-<issue>`; round id `p<issue>-e<event>`; round dir under
  `state_dir/rounds/` like a buffer round (so `_record`/`_row` work from a minimal `plan.json` of `{id, path}` rows).

## Global Constraints

- Branch `training-buffers`, this checkout; one commit per task; no merge, no PR; full checks clean per task;
  real-path grep empty; wire format: `mia_buffers/models.py` adds `PrepState`, `PrepPins`, `PrepOutcome` (new
  dataclasses) and nothing else changes shape; `MiaBuffer` edits only inside `workflow.patched("e-prep")`; the five
  buffer fixtures untouched and passing; a `prep_history.json` fixture recorded for `MiaPrep`.
- Values from the spec: issue label `prep:go`; stages `prep:requested`, `prep:done`, `prep:needs-human`; the issue
  body one ```yaml fence with `schema_version: 1`, `kind: preparation`, `catalog: {repo, ref}`, and `records: [...]`
  or `select: {...}` (the buffer spec's `Select` model); one comment listing per record what was done (issue
  opened, `data:go` added, already in progress, already prepared, ineligible, done-but-unpublished); the existing
  `nudge_dataset`, `DATA_IN_PROGRESS` and trusted-bot `data:go` unchanged.
- Import direction unchanged; `mia_core` imports no application package.

## Review Focus

1. A prep issue naming a record that is already `prepared` must be reported as such and settle at once, never open
   a mia-data issue (Task 2, `test_prep_admit_and_nudge_report_prepared_records`).
2. A `prep:go` on an issue whose fence lists an unknown record id (not in the catalog at the ref) must end
   `prep:needs-human` naming it, not crash the workflow (Task 2, `test_prep_admit_refuses_unknown_records`).
3. The same record requested by two prep issues must not get two `data:go` labels or two dataset issues
   (`nudge_dataset`'s find-by-marker and in-progress checks cover it; Task 2 test pins it).
4. A `MiaBuffer` round planned before E whose dataset is `waiting-dataset` must keep replaying (the `else` branch
   untouched; Task 3's replay tests).
5. The prep poll loop must not hold the workflow forever when the registry is down: `prep_status` raises → the
   activity retries with the standard budget, then the workflow ends `prep:needs-human` with the error (Task 3,
   `test_prep_registry_outage_ends_needs_human`).

---

### Task 1: the prep spec, state, labels, poller and activities

**Files:**
- Modify: `mia_buffers/spec.py` (`PrepSpec`, `load_prep_text`), `mia_buffers/config.py` (`PREP_STAGES`,
  `PREP_HUMAN_LABELS`, `Config.prep_label(stage)`, `Config.prep_workflow_id(issue)`), `mia_buffers/models.py`
  (`PrepPins`, `PrepOutcome`, `PrepState`), `mia_buffers/activities.py` (`prep_admit`, `prep_nudge`,
  `prep_status`, `set_prep_stage`, `comment_prep`; `_record`/`_row` tolerate a prep round's `plan.json`),
  `mia_buffers/poll.py` (deliver `prep:go` to `MiaPrep`; remove `notice_datasets` and its call), `mia_buffers/cli.py`
  (`init-labels` creates the prep labels; `cmd_worker` registers `MiaPrep` — Task 2 defines it; import guarded),
  `mia_data/config.py` (`EvaluateCfg(enabled: bool = False)` as `Config.evaluate`), `mia.yaml.example`
  (`evaluate: {enabled: false}` under `services.mia-data`, commented)
- Test: `tests/test_buffers_spec.py`, `tests/test_buffers_activities.py`, `tests/test_buffers_workflows.py` (poll
  tests), `tests/test_buffers_cli.py`, `tests/test_mia_config.py`

**Interfaces:**
- Produces, `spec.py`: `class PrepSpec(_Strict): schema_version: Literal[1]; kind: Literal["preparation"];
  catalog: CatalogRef; records: list[str] = []; select: Select | None = None; timeout_days: float =
  Field(default=14, gt=0)` with a validator requiring exactly one of `records` (non-empty) or `select`;
  `load_prep_text(text: str) -> PrepSpec` (`SpecError` on failure, like `load_spec_text`).
- Produces, `config.py`: `PREP_STAGES = {"requested": (color, desc), "done": ..., "needs-human": ...}`,
  `PREP_HUMAN_LABELS = {"go": (...)}`; `prep_label(stage) -> f"prep:{stage}"`; `prep_workflow_id(issue) ->
  f"mia-buffers/{buffers_repo}/prep-{issue}"`.
- Produces, `models.py`: `PrepPins(round_id: str, catalog_ref: str, records: list[str], timeout_days: float)`;
  `PrepOutcome(record: str, outcome: str, issue: int | None = None, note: str = "")` with outcome in
  `opened · nudged · in-progress · prepared · ineligible · done-unpublished · unknown`; `PrepState(issue: int,
  schema: int = 1, handled: list[str] = [], round_id: str | None = None, stage: str = "new", pins: PrepPins | None
  = None, outcomes: list[PrepOutcome] = [], settled: dict[str, str] = {})`.
- Produces (activities):
  - `prep_admit(issue, event_id) -> PrepPins`: fence → `load_prep_text`; snapshot the catalog at the ref
    (`catalog_snapshot`); records = the explicit list, or ids of `decide_all(facts over the snapshot's records,
    spec.select)` admitted; unknown ids (not in the snapshot) raise `ValueError` naming them (permanent → the
    workflow ends `prep:needs-human` with the message); writes `spec.yaml`, `pins.json` and a `plan.json`
    `{"rows": [{"id", "path"}]}` into `state_dir/rounds/p<issue>-e<event>/` so `_record`/`_row` work.
  - `prep_nudge(round_id, record) -> PrepOutcome`: first `registry.dataset(record)` (best effort): state `prepared`
    → outcome `prepared` without touching GitHub; `ineligible`/`needs-human` → that outcome; else
    `nudge_dataset(round_id, record)` mapped: issue created → `opened`; `nudged` → `nudged`; stage in
    `DATA_IN_PROGRESS` or `data:go` present → `in-progress`; `done` with note → `done-unpublished`; `ineligible`.
  - `prep_status(records: list[str]) -> dict[str, str]`: `registry.datasets()` filtered to `records`; value = row
    state or `"unprepared"`; raises on `RegistryError`.
  - `set_prep_stage(issue, stage, state: PrepState | None, detail: str)`: the `prep:*` label swap and one comment
    per stage per round (marker `prep stage {round_id} {stage}`), like `set_stage`.
  - `comment_prep(issue, round_id, outcomes: list[PrepOutcome])`: one comment, a table `record | outcome | issue`.
- Poller: `run_poll` adds a loop over `PREP_HUMAN_LABELS` with `deliver_label(..., workflow="MiaPrep",
  make_state=PrepState, guard=None)` using `cfg.prep_label(kind)` and the id `cfg.prep_workflow_id(number)`
  (`deliver_label` takes `cfg.workflow_id`; add an optional `workflow_id: Callable[[int], str] | None` parameter
  to it in `mia_data/poll.py`, default the existing behaviour). `notice_datasets` and its call are deleted.

- [ ] **Step 1: Write the failing tests** (spec: `PrepSpec` accepts the two forms and refuses both/neither; activities:
  `test_prep_admit_and_nudge_report_prepared_records`, `test_prep_admit_refuses_unknown_records`,
  `test_prep_admit_selects_records_with_a_select_block`, `test_prep_nudge_opens_once_for_two_requests`,
  `test_prep_status_reads_the_registry`; poll: a `prep:go` label starts `MiaPrep` with the prep workflow id and the
  removed binding watch no longer signals; cli: `init-labels` creates `prep:go/requested/done/needs-human`; mia-data
  config: `evaluate.enabled` default False).
- [ ] **Step 2: Run → fail. Step 3: Implement. Step 4: Full checks → pass.**
- [ ] **Step 5: Commit** — `mia-buffers: the preparation request — prep spec, labels, poller and activities; the binding watch goes`.

### Task 2: `MiaPrep`, and `MiaBuffer` stops nudging

**Files:**
- Modify: `mia_buffers/workflows.py` (`MiaPrep`; `MiaBuffer._source_cached`/`_source_steps` not-found branch
  under `patched("e-prep")`), `mia_buffers/cli.py` (`workflows=[..., MiaPrep]`)
- Test: `tests/test_buffers_workflows.py`, fixtures `tests/fixtures/buffers/prep_history.json` and
  `buffer_history_e.json`

**Interfaces:**
- `@workflow.defn(name="MiaPrep") class MiaPrep`: `__init__(state: PrepState)`; signal `go(event: LabelEvent)`;
  query `status() -> PrepState`; `run`: like `MiaBuffer.run`'s loop (wait for a go, dedupe by `handled`, one
  round at a time, `continue_as_new` on rollover). A round: `prep_admit` (a `ValueError` → `prep:needs-human` with
  the message) → `set_prep_stage("requested")` → `prep_nudge` per record (sequentially; outcomes collected;
  `ineligible` and `prepared` are settled at once) → `comment_prep` → poll `prep_status` with the backoff until
  every record is settled or `pins.timeout_days` pass → `prep:done` (comment lists the prepared records) or
  `prep:needs-human` (comment names the unsettled and the `needs-human` ones). A `go` during a round is refused
  (busy comment) like the buffer's.
- `MiaBuffer`: under `patched("e-prep")`, a not-found binding → `_fail(dataset, "not prepared: request preparation
  with a prep:go issue")` (no nudge, no wait); the `else` keeps today's code.

- [ ] **Step 1: Write the failing tests** (`Mocks` style with fakes `prep_admit`, `prep_nudge`, `prep_status`,
  `set_prep_stage`, `comment_prep`): `test_prep_round_opens_nudges_and_closes_done`; `test_prep_needs_human_on_timeout`;
  `test_prep_registry_outage_ends_needs_human`; `test_prep_admit_error_is_needs_human`;
  `test_buffer_fails_an_unprepared_record_instead_of_waiting` (e-prep); `test_replay_prep_history`;
  `test_replay_buffer_history_e`; the five existing replays unchanged.
- [ ] **Step 2: Run → fail. Step 3: Implement. Step 4: Record the fixtures (scoped `-k`). Step 5: Full checks → pass.**
- [ ] **Step 6: Commit** — `mia-buffers: MiaPrep follows a preparation request to done; a buffer no longer waits on mia-data`.

### Task 3: documentation and the spec's record

- `docs/development.md`: a section "## Preparation requests (milestone E)" (the issue body, the labels, what the
  comment lists, how it settles, the `MiaBuffer` change, the pre-E round note); the "Datasets without a binding"
  section (milestone 4) gets a one-paragraph note that the nudge-and-wait path is superseded.
- `AGENTS.md`: one bullet — a buffer never asks mia-data for a dataset; a `prep:go` issue does.
- Spec: "**Built in E (date).**" paragraph with the deviations.
- Commit — `docs: preparation requests`.

## Live steps (operator)

1. `mia exec mia-buffers init-labels` (prep labels), restart mia-buffers.
2. The live round waiting on U937 will not wake: once issue 57 is resolved and U937 is `prepared`, `buffer:go` again.
3. Acceptance: a `prep:go` issue naming an unprepared record → its mia-data issue opens with `data:go`, the registry
   shows `in-progress`, then `prepared` after `data:approve` and publication; the prep issue goes `prep:done`.
