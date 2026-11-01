# Milestone K: the staged gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer round stages before it asks: `buffer:go` plans, sources and waits at `buffer:staged`; `buffer:plan` also waits at `plan-review`; `buffer:approve` at the staged gate serves what is staged, partial or not, and no job runs between that approve and `ready`.

**Architecture:** One Temporal workflow (`MiaBuffer`) gains a second gate between sourcing and ready, behind one patch marker `staged-gate` evaluated once per run like `d-lifecycle`. Sourcing no longer announces `ready` progressively: it runs until no dataset has a job left, then stops at `staged`. The approve records each group's servable fraction as the buffer's floor (`BufferState.accepted`), which `_ready_now`/`_below` honour afterwards. buffer-api refuses consumers until the buffer has a `ready` event; the keeper retires a staged buffer nobody approves; the poller delivers `buffer:plan` and drops at `staged`.

**Tech Stack:** Python 3.12, temporalio (patched markers, replay fixtures), httpx, SQLite ledger, the static page in `mia_buffers/ui/index.html`, pixi tasks `fmt`/`lint`/`typecheck`/`test`.

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` §15 (amendment v0.6), with §5.1–5.3 for the flow it amends. Milestone row K in §10.

## Global Constraints

- `models.py` dataclasses are wire format: add fields with defaults only; never rename or remove (`AGENTS.md`).
- Workflow code is deterministic: every new command on a path a live history can reach sits behind `workflow.patched("staged-gate")`; the recorded fixtures `tests/fixtures/buffers/*.json` replay unchanged; a new fixture records the new flow (`MIA_BUFFERS_RECORD_HISTORY=1`).
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` empty before every commit.
- `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` clean before every commit; `pixi run pytest <file>` runs one file.
- Untrusted job output through `read_untrusted`/`neutralize`; the page is read-only and holds no token; `tests/test_buffers_ui.py` enforces read routes only.
- Import direction `mia_buffers → mia_data → autodev`; all may import `mia_core`.
- Commit trailers: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01MtRZKQzAoqWfTv3k7ja6Ex`.
- Copy pinned by the spec: label `buffer:staged`; event kinds `sourcing`, `staged`; HTTP `409 buffer not approved`; the staged comment's call to action "approve with `buffer:approve` to serve it as it is, or edit the spec and add `buffer:go` again"; the ready comment's short-group line of the form "below ready.min_fraction 1: brightfield 0/1, approved".

## Review Focus

1. An approve given while a `go` round is still sourcing (no gate open yet) must be refused with a comment naming the gate the round is at, and must not be consumed by the staged gate later. Test in Task 4 (`test_approve_while_sourcing_is_refused_and_not_reused`).
2. A `dropped` signal that arrives while the round waits at `staged` must resume the waiting dataset (its job runs, it lands, the board changes) and the gate must stay open. Test in Task 4 (`test_gate_waits_while_a_drop_is_pending`).
3. The keeper's `retire` for the round's own buffer, arriving at the gate, must end the round `retired`, not be ignored as "no ready buffer". Test in Task 4 (`test_keeper_retire_at_the_gate_retires_the_round`).
4. A consumer already registered on a buffer that is later retired keeps getting 410, and one asking at `staged` gets 409 for `next`, `split` and `POST consumers`, while `status` stays 200 for the page. Test in Task 2.
5. The live run of mia-buffers#5 predates the marker: its history must replay on the new code with the old branches (`workflow.patched` returns False in replay without the marker). Covered by the unchanged fixtures in Task 4 plus the per-run evaluation of the marker.

---

### Task 1: Constants, wire fields, poller

**Files:**
- Modify: `mia_buffers/models.py` (BufferState, LabelEvent doc), `mia_buffers/config.py` (STAGES, HUMAN_LABELS), `mia_buffers/ledger.py:41` (EVENT_KINDS), `mia_buffers/activities.py:106-113` (STAGE_EVENTS), `mia_buffers/poll.py:30,69`, `mia_buffers/ui/index.html` (`STAGE_OF`), `mia_buffers/spec.py:218` (doc line of `auto_approve`)
- Test: `tests/test_core_assets.py` is untouched; `tests/test_buffers_config.py`, `tests/test_buffers_api.py`, `tests/test_buffers_workflows.py` (poll tests)

**Interfaces:**
- Produces: `BufferState.accepted: dict[str, float]` (group → servable fraction at the approve, default `{}`); `BufferState.auto_approve: bool = False` (the plan's value, kept for the gate); `BufferState.staged_at: str | None = None` (the staged comment's time; an approve must be newer, as `planned_at` for the plan); `STAGES["staged"] = ("1d76db", "mia-buffers: staged, waiting for buffer:approve")`; `HUMAN_LABELS["plan"] = ("0e8a16", "Plan this buffer and wait for approval before staging")`; `EVENT_KINDS = ("created", "planned", "sourcing", "approved", "staged", "ready", "idle", "retired", "needs-human")`; `STAGE_EVENTS`: `"sourcing": "sourcing"`, `"staged": "staged"` (ready stays `ready`; `approved` is written by the approve, Task 4).

- [ ] **Step 1: Failing tests.** In `tests/test_buffers_api.py` add `test_events_accept_sourcing_and_staged` posting `{"kind": "sourcing"}` then `{"kind": "staged"}` to `/buffers/b/events` and asserting 200 and that `GET /buffers/b/events` lists both in order. In `tests/test_buffers_workflows.py::test_poll_delivers_labels_and_drops` add an issue `4: issue(4, "buffer:staged")` whose state has a `waiting-drop` dataset with a file under `incoming/`, and assert the second pass logs `dropped #4 <record>` beside the existing one. In `tests/test_buffers_config.py` (where init-labels is tested) assert the label set includes `buffer:staged` and `buffer:plan`.
- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_api.py -k events_accept` and the two others; expected FAIL (422 on kind; no drop for #4; labels missing).
- [ ] **Step 3: Implement** the constants above; in `poll.py` add `"staged"` to `LIVE_STAGES` and to the drop loop `for stage in ("sourcing", "staged", "ready")`; in `index.html` add `sourcing: "sourcing", staged: "staged"` to `STAGE_OF`; in `spec.py` reword the `auto_approve` doc line to "skips both gates when every weighting group meets ready.min_fraction at the staged point". `LabelEvent.kind` doc in `mia_data/models.py` becomes `go | plan | approve | reject`.
- [ ] **Step 4: Run** the three tests, then `pixi run test`; expected PASS (the poller's `HUMAN_LABELS` loop now also lists `buffer:plan` issues and will call `deliver_label(kind="plan")`, which signals `plan`: the signal arrives in Task 4; until then no live issue carries the label).
- [ ] **Step 5: Commit** `buffers: staged stage and plan label, sourcing/staged events, accepted floor and staged_at on the state`.

### Task 2: buffer-api serves only an approved buffer

**Files:**
- Modify: `mia_buffers/api.py:271-276` (`_serving`), `mia_buffers/ledger.py` (an `has_event(buffer, kind) -> bool`), `mia_buffers/client.py:46-70`
- Test: `tests/test_buffers_api.py`, `tests/test_buffers_client.py`

**Interfaces:**
- Produces: `Ledger.has_event(buffer: str, kind: str) -> bool`; `_serving` raises `HttpError(409, "buffer not approved")` when no `ready` event exists and the latest is not `retired` (retired keeps 410); `mia_buffers.client.BufferNotApproved(RuntimeError)` raised by `_get` and `register` on 409 with the message `buffer <id> is not approved yet (409 buffer not approved)`.

- [ ] **Step 1: Failing tests.** `test_an_unapproved_buffer_serves_no_consumer(tmp_path)` modelled on `test_a_retired_buffer_serves_no_consumer` (api.py:555): PUT config and a volume, post events `created`, `sourcing`, `staged`; assert `GET next`, `GET split/val` and `POST consumers` return `(409, {"error": "buffer not approved"})`, `GET status` and `GET events` return 200; post `ready`; assert `POST consumers` is 200 and `next` serves; post `retired`; assert 410 again. In `tests/test_buffers_client.py` add `test_client_names_an_unapproved_buffer(buffer_server)`: a buffer with volumes but no `ready` event; `BufferDataset`/`BufferClient.register` raises `BufferNotApproved` whose message contains `not approved yet`.
- [ ] **Step 2: Run** both files; expected FAIL (200 where 409 expected; no such exception).
- [ ] **Step 3: Implement.** `has_event` as one `SELECT 1 FROM buffer_events WHERE buffer=? AND kind=? LIMIT 1`. `_serving`: after the retired check, `if not self.ledger.has_event(buffer, "ready"): raise HttpError(409, "buffer not approved")`. Client: in `_get` and `register`, before `raise_for_status`, `if response.status_code == 409: raise BufferNotApproved(...)`.
- [ ] **Step 4: Fix the fixtures the gate now blocks.** Every API/client test that registers volumes and then draws must post a `ready` event first: the `buffer_server` fixture in `tests/test_buffers_client.py`, and the `call(s, "PUT", "/buffers/b", ...)` setups in `tests/test_buffers_api.py` that go on to `next`/`split`/`consumers` (grep `"/consumers"`). Add one helper `ready(s, buffer="b")` in `test_buffers_api.py` that posts the event, and call it where needed; the sampler tests (`tests/test_buffers_sampler.py`) talk to `Sampler`, not the routes, and need nothing.
- [ ] **Step 5: Run** `pixi run test`; expected PASS.
- [ ] **Step 6: Commit** `buffer-api: consumers get 409 until the buffer has a ready event; the client names it`.

### Task 3: The staged comment and the gate's summary

**Files:**
- Modify: `mia_buffers/status.py` (beside `render_ready`), `mia_buffers/models.py` (`StagedSummary`), `mia_buffers/activities.py` (new activity `staged_summary`; `set_stage` unchanged)
- Test: `tests/test_buffers_status.py` (or where `render_ready` is tested), `tests/test_buffers_activities.py`

**Interfaces:**
- Produces: `render_staged(datasets: list[tuple[str, str, str]], groups: dict[str, tuple[int, int]], min_fraction: float, summary: StagedSummary) -> str` where `datasets` is `(record, status, reason)` in pool order and `groups` is `_groups()`'s shape; `render_ready(buffer_id, service_url, servable, short: list[str] = ())` gains the short-group lines ("below ready.min_fraction 1: brightfield 0/1, approved" per entry of `short`, already formatted by the caller); `StagedSummary(servable: dict[str, int] = {}, bytes: int = 0, waiting: list[str] = [])` (wire, all defaults); activity `staged_summary(buffer_id: str) -> StagedSummary` reading the buffer's row from `GET /buffers` (`servable` per split) and summing `bytes` of the staged assets whose `refs` include the buffer (`GET /assets/staged`), `waiting` left for the workflow to fill.

- [ ] **Step 1: Failing tests.** `test_render_staged_lists_outcomes_groups_and_the_call_to_action`: two datasets, one `servable`, one `failed` with reason `stage job 1 exited with code 1: host 'zenodo.org' is not allowed`, groups `{"brightfield": (0, 1), "confocal": (1, 1)}`, `min_fraction=1`, summary servable `{"train": 2, "val": 1, "test": 0}`, bytes `2_000_000`, waiting `["bcss"]`; assert the text contains the failed reason (neutralized), `brightfield 0/1` marked below, `confocal 1/1`, `train 2`, `2.0 MB` (use the module's existing size formatter if any, else `f"{bytes/1e6:.1f} MB"`), `bcss` under a "waiting" line, and the exact call to action from Global Constraints. `test_render_ready_names_short_groups`: `short=["brightfield 0/1"]` yields a line `below ready.min_fraction 1: brightfield 0/1, approved`. In `tests/test_buffers_activities.py` `test_staged_summary_reads_servable_and_bytes(acts, server)`: register a buffer with two volumes across splits and two staged assets (one referenced by it), assert `StagedSummary(servable={...}, bytes=<the referenced asset's bytes>)`.
- [ ] **Step 2: Run** the tests; expected FAIL (no function/activity).
- [ ] **Step 3: Implement** them; `render_staged` uses `safe_text` on every string as `render_ready` does; the activity is `@activity.defn(name="staged_summary")` with `@mia_permanent_errors`, registered in the worker's activity list next to `buffer_event`.
- [ ] **Step 4: Run** `pixi run test`; expected PASS.
- [ ] **Step 5: Commit** `buffers: the staged comment (render_staged, staged_summary) and short groups in the ready comment`.

### Task 4: The workflow: `plan` signal, the staged gate, the accepted floor

**Files:**
- Modify: `mia_buffers/workflows.py` (`__init__`, signals, `_round`, `_review`, `_ready_now`, `_below`, `_source_all`, `_announce_ready`, `_steady`'s go branch, `_retire_failed` caller)
- Test: `tests/test_buffers_workflows.py` (Mocks: add `staged_summary` returning `StagedSummary()` by default; helpers)

**Interfaces:**
- Consumes: Task 1 fields and constants; Task 3 `render_staged`, `render_ready(short=)`, activity `staged_summary`.
- Produces: signal `plan(event: LabelEvent)` (appends to `go_events`; `event.kind == "plan"`); `self.gate_on = workflow.patched("staged-gate")` set in `run()` beside `lifecycle_on`; `_floor(group) -> float = min(state.min_fraction, state.accepted.get(group, 1.0))` used by `_ready_now` and `_below`; `_quiet(tasks) -> bool` (every unfinished task's dataset status in `WAITING`, i.e. no job or registration in flight; the same predicate as `_await_stragglers.abandonable` minus the non-empty requirement, so extract it once as a module-level `_only_waiting(pairs)`); `_staged_gate(tasks, limits) -> bool` (True: approved, continue to `_await_stragglers`; False: round over or a new go); `_approve_at_gate(limits)` (records `accepted`, writes the `approved` event via activity `buffer_event(buffer_id, "approved")`, then `_announce_ready(limits, short=...)`); test helpers `approve_staged(h, m, eid="13", dt=1)` (waits for `idle(h, m, "staged")`, signals approve newer than `staged_at`) and `started(..., signal="plan")` default so existing tests keep the plan-review gate.

Behaviour to implement, in `_round` and `_source_all` only when `self.gate_on` (the `else` is today's code, untouched):

- `_round`: `state.auto_approve = plan.auto_approve`; after `comment_plan`: a `plan` event → `_stage("plan-review", ...)` then `_from_review()` as today; a `go` event → `_refuse_queued(gos=False)` with reason `NOT_GATE = "`buffer:approve` counts only while the issue is in `buffer:plan-review` or `buffer:staged`."` for approves that queued during planning, then `_serve()`. `AUTO_APPROVE` is no longer posted at the plan (auto-approve moves to the gate).
- `_source_all`: the wait condition becomes `all done or (_quiet(tasks) and any servable) or lifecycle`; no `_ready_now()` exit. On the timeout: `_drop_waiting` as today, then re-evaluate (no `needs-human` for a short group any more; a round with no servable dataset and nothing waiting after the drops is `needs-human` with today's message). When quiet with at least one servable: `_staged_gate(tasks, limits)`.
- `_staged_gate`: `state.staged_at = workflow.now().isoformat()`; `summary = staged_summary(buffer_id)` with `waiting` filled from the datasets in `WAITING`; `_stage("staged", <call to action>, extra=render_staged(...))`. If `state.auto_approve and _ready_now()`: `_approve_at_gate` at once. Else loop on `wait_condition(approve_events or go_events or lifecycle or any task finished since (progress changed) or rollover-never-while-tasks-live)`: an approve newer than `staged_at` → `_approve_at_gate`, return True; an older one → `_refuse(APPROVE, "it is older than the staged comment; add it again to approve it", event)`; a go → if `_quiet(tasks)` (always true at the gate unless a straggler job started): cancel the waiting tasks with `ABANDONED`, mark the buffer for retire (`state.stage` is `staged`; `_round` retires it like a `needs-human` one: widen `_retire_failed`'s trigger to `state.stage in ("needs-human", "staged")`), return False; else `_refuse(GO, BUSY, event)` and keep waiting; a lifecycle `retire` for `state.buffer_id` (the keeper's, Task 6) → `retire_buffer`, `_stage("retired", ...)`, cancel the waiting tasks, return False; other lifecycle signals → `_refuse_lifecycle(reason="the buffer is staged, not ready")`; a dataset that landed meanwhile (progress changed) → `_comment(render_staged(...), f"staged {round_id} {self.progress}")` once per change.
- `_approve_at_gate`: `state.accepted = {group: servable / total for group, (servable, total) in self._groups().items()}`; `buffer_event(buffer_id, "approved", "")`; `short = [f"{g} {s}/{t}" for g, (s, t) in groups if s / t < state.min_fraction]`; `_announce_ready(limits, short)` (its `render_ready` call passes `short`, and the ready detail says "approved below ready.min_fraction" when `short` is non-empty).
- `_steady`'s go branch and `_retire_failed` keep today's behaviour; `_review` is reused for the plan gate unchanged.

- [ ] **Step 1: Failing tests** (all in `tests/test_buffers_workflows.py`; `Mocks` gets `staged_summary`):
  - `test_go_stages_then_waits_at_staged_and_approve_serves`: `started(env, m, signal="go")` with `Mocks()` (PLAN: u937 + bcss waiting-drop): assert stages reach `["planning", "sourcing"]`, drop bcss, then `idle(h, m, "staged")`; the staged `set_stage` extra contains `u937` and the call to action; no `register`-free `ready`; `approve_staged(h, m)`; `idle(h, m, "ready")`; assert `m.stages == ["planning", "sourcing", "staged", "ready"]`, `state.accepted == {"confocal": 1.0, "histology-WSI": 1.0}` (groups of PLAN), `m.of("buffer_event") == [("i7-e11", "approved", "")]`, no `submit` after the approve (count submits before and after).
  - `test_plan_label_waits_twice`: `started(env, m, first=ev("plan", "11"), signal="plan")`: `approve(h, m)` (plan-review), then `idle(h, m, "staged")`, `approve_staged`, `idle(h, m, "ready")`; stages `["planning", "plan-review", "sourcing", "staged", "ready"]`.
  - `test_partial_approve_sets_the_floor_and_a_rebuild_respects_it`: `Mocks(plan=TWO_GROUPS, stage={"b": StageResult(False, 0, ["raw 0: voxel size unknown"])})` where `TWO_GROUPS` has `a` in group `TEM`, `b` in group `SEM`, `min_fraction=1`; `go`; at `staged` the extra names `SEM 0/1`; `approve_staged`; ready detail contains `below ready.min_fraction 1: SEM 0/1, approved`; `state.accepted == {"TEM": 1.0, "SEM": 0.0}`; then signal `degraded` for `a` with `missing_records` returning `["a"]` and `raw_ready` True and a passing re-stage: round stays `ready` (no `needs-human`), `m.of("comment")` has `rebuilt a`.
  - `test_auto_approve_full_set_reaches_ready_without_a_label` (`Mocks(plan=replace(ONLY_U937, auto_approve=True))`, `go`): stages `["planning", "sourcing", "staged", "ready"]` with no approve signal; and `test_auto_approve_short_set_waits_at_staged` (same with the stage failing): stages end at `staged`.
  - `test_approve_while_sourcing_is_refused_and_not_reused` (Review Focus 1): `go` with a stage gated by `Mocks(gate=...)` so sourcing is in flight; signal approve; assert `refuse_label` with `NOT_GATE`; release the gate; at `staged` no `ready` follows until a new approve.
  - `test_gate_waits_while_a_drop_is_pending` (Review Focus 2): PLAN's bcss waiting-drop, u937 servable: `staged` opens with `waiting: ["bcss"]` in the extra; signal `dropped bcss`; `until(register_dataset for bcss)`; a second staged comment (`comment` with marker starting `staged i7-e11`) appears; stage list still ends at `staged`; `approve_staged` → `ready` with both servable.
  - `test_go_at_staged_starts_a_new_round_and_retires_the_staged_buffer`: at `staged`, signal `go` `ev("go", "15")`; `idle(h, m, "staged")` of the new round (or `plan-review` for a `plan` event); assert `m.of("retire_buffer") == [("i7-e11",)]` and the new `round_id == "i7-e15"`.
  - `test_stale_approve_at_staged_is_refused`: approve with `at` older than `staged_at` → `refuse_label` containing `older than the staged comment`; round stays `staged`.
  - `test_keeper_retire_at_the_gate_retires_the_round` (Review Focus 3): at `staged`, `h.signal(MiaBuffer.retire, args=[None, "i7-e11"])` → `retire_buffer` called, stages end `["...", "staged", "retired"]`.
  - `test_timeout_at_the_gate_drops_waiting_and_keeps_the_gate`: `Mocks(limits=...)` with `timeout_days` tiny via the plan: bcss waiting-drop, u937 servable → `staged`; after the deadline `bcss` is `dropped` and a staged comment re-posted; still `staged`.
- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_workflows.py -k "staged or plan_label or partial_approve or auto_approve or gate"`; expected FAIL (no `plan` signal, stages go to plan-review).
- [ ] **Step 3: Implement** the behaviour above. Keep today's code as the `else` of `self.gate_on` branches; do not restructure `_source_all` beyond its exit conditions and the new call. `_announce_ready(limits, short=())` passes `short` to `render_ready`.
- [ ] **Step 4: Migrate the existing workflow tests** mechanically: `started()`'s default `signal` becomes `"plan"` and `first` defaults to `ev("plan", "11")`, so every test that calls `approve(h, m)` still meets `plan-review`; before each `idle(h, m, "ready")` (and `until(... "ready")`) insert `await approve_staged(h, m)`, and insert `"staged"` before `"ready"` in every `m.stages == [...]` assertion. Tests whose meaning changes: `test_progressive_ready_with_lowered_fraction` → rename `test_lowered_fraction_no_longer_opens_ready_early`: with `min_fraction=0.5` the round still stops at `staged` once quiet, and the approve serves; `test_timeout_without_ready_is_needs_human` → the timeout now drops the waiters and, with nothing servable, is `needs-human`; with something servable it stops at `staged` (split into two asserts). `test_timeout_after_ready_changes_nothing` and the straggler tests keep their meaning after `approve_staged`. The replay tests stay as they are (fixtures replay with the marker absent).
- [ ] **Step 5: Run** `pixi run test`; expected PASS, including `test_replay_buffer_history*` unchanged.
- [ ] **Step 6: Record the new fixture.** Add `HISTORY_K = fixtures/buffers/buffer_history_k.json` and `test_replay_buffer_history_k(env)` modelled on `test_replay_buffer_history_c` (buffers_workflows.py:751): a `go` round through `staged` and an approve to `ready`; record with `MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay_buffer_history_k`, scrub as the others, commit the JSON (check it for real paths).
- [ ] **Step 7: Run** `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`; expected clean.
- [ ] **Step 8: Commit** `buffers: the staged gate (staged-gate): go stages then waits, plan waits twice, approve at the gate records the accepted floor`.

### Task 5: The keeper retires an unapproved staged buffer

**Files:**
- Modify: `mia_buffers/activities.py:1225-1270` (`_keeper_buffers`, `_keeper_buffer`)
- Test: `tests/test_buffers_activities.py::test_keeper_signals_degraded_idle_and_retire`

**Interfaces:**
- Consumes: Task 4's handling of `("retire", None, buffer_id)` at the gate.
- Produces: `_keeper_buffers` also watches buffers whose latest event is `staged`; for those the only rule is `now - at > config.retire_after_days * 86400` → signal `retire` (no `degraded`, no `idle`: nothing serves yet).

- [ ] **Step 1: Failing test.** In the keeper test add `buffer("i12-e13", ["created", "sourcing", "staged"], retire_after_days=0.01)` (staged for over 864 s at `now`) and `buffer("i14-e15", ["created", "sourcing", "staged"])` (fresh); expected signals gain `retire i12-e13` and nothing for `i14-e15`; a staged buffer with a missing pyramid gets no `degraded`.
- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_activities.py -k keeper_signals`; expected FAIL.
- [ ] **Step 3: Implement**: the filter `latest.get("kind") in ("ready", "idle", "staged")`; in `_keeper_buffer`, when `kind == "staged"`, skip the missing/idle rules and append `("retire", [None, buffer_id], "")` under the age rule.
- [ ] **Step 4: Run** the file, then `pixi run test`; expected PASS.
- [ ] **Step 5: Commit** `keeper: a staged buffer unapproved for retire_after_days is retired`.

### Task 6: Page, docs, diagrams, deploy notes

**Files:**
- Modify: `mia_buffers/ui/index.html:704-708` (detail notices), `docs/development.md:443-477` (walkthrough steps 1, 3, 5), `README.md` (the buffers paragraph naming the labels, if it names them), `notes/specs/2026-10-25-prep-and-buffering.md` §10 row K build note, `mia.yaml.example` (`plan.auto_approve` comment if present)
- Test: `tests/test_buffers_ui.py` (route scan) stays green; `python3`-side `node --check` of the page script as done in this branch.

- [ ] **Step 1: Page.** In `viewBuffer`, when the latest event is `staged`, add a notice: "Staged, waiting for `buffer:approve` on the issue: approve serves what the board shows as servable, partial or not; consumers are refused until then." Reword the "Not sourced yet" notice to say "after `buffer:approve` (a `buffer:plan` round) or at once (a `buffer:go` round)". Run `node --check` on the extracted script.
- [ ] **Step 2: Docs.** `development.md` walkthrough: step 1 names both labels and what each gates; step 3 becomes "Approval (plan rounds)"; step 4 unchanged; step 5 becomes "Staged, then ready": the quiet rule, the staged comment, approve accepting a partial buffer with the accepted floor, consumers 409 before `ready`, the keeper's retire of an unapproved staged buffer, `buffer:go` at `staged` superseding and retiring it. Add a line to the labels paragraph that `mia exec mia-buffers init-labels` creates `buffer:plan` and `buffer:staged`. In the spec's §10 add to row K's note: built, commit range, date.
- [ ] **Step 3: Diagrams.** Update the lifecycle diagram in the artifact `https://claude.ai/artifact/Nqk7xz3jQajtPAFqw1296i` (source `training-buffers-explainer.html` in the session scratchpad; read it before editing): the buffer stage flow gains `staged` between `sourcing` and `ready`, the `plan` label's early gate drawn as a dashed bypass; conventions: no wrapped lines, "web UI", solid arrows between steps, dashed to resources; version note v0.6.
- [ ] **Step 4: Run** `pixi run test` and the path check; commit `docs: the staged gate on the page, in the walkthrough and the diagrams`.
- [ ] **Step 5: Deploy notes for the operator** (in the final report, not a file): `mia exec mia-buffers init-labels` once; `mia restart mia-buffers` (worker and buffer-api: an old buffer-api lacks the 409 and the new event kinds); mia-buffers#5's live run keeps the old branches until it ends (close and reopen the issue, or wait for its rollover) before the first `buffer:go` on the new flow; existing `ready` buffers are unaffected (they have the `ready` event).
