# Sparse OME-Zarr 0.6 staging, part B: planner, job, registry, sampler — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the buffer pipeline fixes a record-local sample plan before staging, hands it to the harness with the
artifact id, puts the plan digest into the artifact id, registers each volume's planned samples as candidates,
and serves train from them by weight and val/test as a frozen list drawn without replacement from them.

**Architecture:** `spec.py` gains `sampling.plan.samples`; `plan.py` writes each row's *rule* and `plan_digest`
and estimates planned counts and bytes for `plan.md`; `jobspec.artifact_id` hashes the digest; `prepare_stage`
writes `plan-rule.json` and `voxel_size.json` into the job workspace and `stage_job` names them; `_publish` only
renames; `register.volume_bodies` passes `samples`; `VolumeBody`/`Volume` carry candidates; `sampler.draw` picks a
candidate when a volume has them; `_split` draws the frozen list without replacement over candidates.

**Tech Stack:** Python 3.12, pydantic 2, SQLite, Temporal (no new patch), pytest/ruff/mypy via `pixi run`.
Branch `sparse-staging` in this repo. Depends on part A's file formats (`plan-rule.json`, `voxel_size.json`,
`stage.json` keys) but not on its code: this repo's tests use hand-written `stage.json` fixtures as today.

**Spec:** `docs/specs/2026-11-01-sparse-ome-zarr-0.6-staging.md` v0.2: §2.1, §2.3 (budget), §2.5 (plan-time
report), §4.1 (job inputs), §4.3 (`stage.json`), §4.4, §7, §8 item 3 and the deployment note.

## Global Constraints

- Activity result dataclasses in `models.py` are wire format: add fields with defaults, never rename or remove.
- No `workflow.patched` is needed: the stage command is built in the `submit` activity; replay tests must pass
  unchanged (`pixi run pytest tests/test_buffers_workflows.py -k replay`).
- The rule hashed into `plan_digest` is record-local: `seed`, `patch`, `levels`, `samples`, `split` policy,
  `channel_recipe`. Never a weight, never a pool total.
- `split_planes` in `register.py` must produce exactly what the harness's `plan.split_planes` produces (golden
  test shared by both repos).
- Eval semantics unchanged: weighted draw, `sourcing.val_size`/`test_size` as the list length, append-only
  frozen list, never re-sorted. Only the candidate pool and "without replacement" are new.
- No real paths or hostnames in tracked files. Descriptive names, type hints, line length 120.
- Deploy with no round in sourcing (an in-flight round's pinned harness does not know `--plan`).

## Review Focus

1. A plan row whose record has no planes in `test` (single-image 2D volumes) must not produce a plan error
   unless **no** record can supply `test` (Task 2 test `test_split_unsupplied_by_every_record_is_an_error`).
2. A `PUT volumes` body without `samples` (an old 0.5 artifact re-registered) keeps today's draw (Task 5 test
   `test_volume_without_candidates_draws_as_before`).
3. A val list that exhausts the candidates before `val_size` must answer with what exists and `exhausted: true`,
   not 500 or an endless loop (Task 5 test `test_frozen_list_stops_when_candidates_run_out`).
4. A retry of `register_dataset` re-PUTting the same volume must not duplicate candidates or change frozen
   indices already served (Task 5 test `test_re_put_replaces_candidates_and_keeps_frozen`).
5. `read_stage` on a build whose `stage.json` says a different `artifact.id` than the worker expects must refuse
   to publish (Task 4 test `test_read_stage_refuses_a_foreign_artifact_id`).

---

### Task 1: `sampling.plan.samples` in the spec grammar

**Files:**
- Modify: `mia_buffers/spec.py` (`Sampling`), `docs/specs/buffer-2d-gray.example.yaml`, `tests/fixtures/buffers/spec.yaml`; Test: `tests/test_buffers_spec.py`

**Interfaces:**
- Produces: `class SamplePlan(_Strict): samples: dict[str, PositiveInt] = {"train": 1000, "val": 20, "test": 20}`; `Sampling.plan: SamplePlan = SamplePlan()`; `Spec` validator: every key of `sampling.plan.samples` is a key of `splits.fractions` and vice versa (error text `sampling.plan.samples must name exactly the splits {…}`).

- [ ] **Step 1: Write the failing test**

```python
def test_sample_plan_defaults_and_must_match_splits(spec_text):
    spec = load_spec_text(spec_text)
    assert spec.sampling.plan.samples == {"train": 1000, "val": 20, "test": 20}
    with pytest.raises(SpecError, match="must name exactly the splits"):
        load_spec_text(spec_text.replace("fractions:", "fractions:\n      extra: 0.0\n     ", 1))  # or build the dict and dump
```

- [ ] **Step 2: Run** `pixi run pytest tests/test_buffers_spec.py -k sample_plan -v` → FAIL (`AttributeError: plan`)
- [ ] **Step 3: Implement** the model and validator; add `plan: {samples: {train: 1000, val: 20, test: 20}}` under `sampling:` in both YAML files with a one-line comment (planned samples per volume per split; placeholder until the planner's estimates on the real pool set it).
- [ ] **Step 4: Run** the spec tests → PASS; `pixi run lint && pixi run typecheck` clean.
- [ ] **Step 5: Commit** (`feat(buffers): sampling.plan.samples, the per-volume sample counts`)

---

### Task 2: The planner: rule, digest, estimates, errors, `plan.md`

**Files:**
- Modify: `mia_buffers/plan.py` (`Row`, `build_plan`, `render_markdown`, `_policy_errors`); Test: `tests/test_buffers_plan.py`

**Interfaces:**
- Produces:
  - `Row.rule: dict | None` = `{"seed", "patch", "levels", "samples", "split", "channel_recipe"}` for pool rows (same keys and value types as the harness `PlanRule`; `split` is the row's existing `split` dict); `Row.plan_digest: str = ""`; `Row.planned: dict[str, int]` (estimated samples per split over the record's raw arrays from catalog shapes: `min(samples[split], capacity)` summed, capacity = `(ey // (patch·max)) * (ex // (patch·max)) * planes_in_split`, planes per split from `split_counts`/holdout as `row.splits` already gives); `Row.planned_bytes: int` (upper bound: Σ_levels planned_samples × (up to 4 chunks) × patch² × channels × itemsize from `Facts` dtype, default 2).
  - `plan_rule(row: Row, spec: Spec) -> dict` and `plan_digest(rule: dict) -> str` (sha256 of `json.dumps(rule, sort_keys=True, separators=(",", ":"))`; the harness computes the same from the same JSON).
  - `_policy_errors` adds `f"split {name!r}: no record can supply it (planes); the buffer would serve an empty {name} set"` when every pool row has `planned[name] == 0`.
  - `render_markdown`: columns `planned` (`train 10 / 1000 · val 3 / 20 · test 0 / 20`) and `est. bytes`; a line after the table: `planned N of requested M samples; ~X GB uncompressed`.

- [ ] **Step 1: Write the failing tests** (in `tests/test_buffers_plan.py`, using the existing `spec_2d`/catalog fixtures)

```python
def test_rows_carry_a_record_local_rule_and_digest(plan_2d, spec_2d):
    row = next(r for r in plan_2d.rows if r.in_pool)
    assert set(row.rule) == {"seed", "patch", "levels", "samples", "split", "channel_recipe"}
    assert row.rule["samples"] == spec_2d.sampling.plan.samples and "weight" not in row.rule
    assert row.plan_digest == plan_digest(row.rule) and len(row.plan_digest) == 64
    other = next(r for r in plan_2d.rows if r.in_pool and r.id != row.id)
    assert (row.plan_digest == other.plan_digest) == (row.rule == other.rule)

def test_planned_counts_are_capped_by_capacity_and_reported(plan_2d):
    row = next(r for r in plan_2d.rows if r.in_pool)
    assert all(row.planned[s] <= row.rule["samples"][s] for s in row.planned) and row.planned_bytes > 0
    md = plan_2d.render_markdown()
    assert "planned" in md and "of requested" in md

def test_split_unsupplied_by_every_record_is_an_error(spec_2d, records_single_plane):
    plan = build_plan(spec_2d, "h", None, records_single_plane)   # every record: one plane, fractions put all in train
    assert any(e.startswith("split 'test': no record can supply it") for e in plan.errors)
```

- [ ] **Step 2: Run** → FAIL (`AttributeError: rule`)
- [ ] **Step 3: Implement** in `build_plan` after weights/splits are set; estimates use `Facts.raw_arrays` extents.
- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_plan.py -v` → PASS
- [ ] **Step 5: Commit** (`feat(buffers): plan rows carry the sample rule, its digest and planned-count estimates`)

---

### Task 3: Artifact id and the stage job's inputs

**Files:**
- Modify: `mia_buffers/jobspec.py` (`artifact_id`, `stage_job`), `mia_data/config.py` (`LimitsCfg.stage_bytes: float | None = None`), `mia.yaml.example` (`limits.stage_bytes` comment); Tests: `tests/test_buffers_jobspec.py`

**Interfaces:**
- Produces:
  - `artifact_id(binding_hash, levels_key, voxel_size_nm, harness_commit, env_digest, plan_digest: str = "") -> str` (appended to the hashed list; `""` reproduces today's ids).
  - `stage_job` command: `harness stage binding.yaml --snippets <snap> --record record.yaml --plan plan-rule.json --voxel-size voxel_size.json --out <out>`; env `MIA_HARNESS_STAGE_BYTES = str(int(cfg.limits.stage_bytes or cfg.limits.job_bytes))`. `--levels`, `--patch`, `--voxel-size-nm` are gone; `StageJobArgs.levels/patch/voxel_size_nm` stay (wire format) but are no longer read by `stage_job`.

- [ ] **Step 1: Write the failing tests**

```python
def test_artifact_id_takes_the_plan_digest():
    base = ("b" * 64, "p256-l1-2-4", {"y": 10.0, "x": 10.0}, "c" * 40, "e" * 64)
    assert artifact_id(*base) == artifact_id(*base, "")
    assert artifact_id(*base, "d" * 64) != artifact_id(*base) and len(artifact_id(*base, "d" * 64)) == 16

def test_stage_job_names_the_plan_and_voxel_files_and_the_stage_cap(tmp_path):
    cfg = _cfg(tmp_path, limits={"job_bytes": 1e6, "stage_bytes": 2e6})
    spec = stage_job(cfg, _args(cfg, cfg.paths.work_root / "w" / "stage-rec", [], str(tmp_path / "out")), tmp_path / "snap")
    assert "--plan plan-rule.json --voxel-size voxel_size.json" in spec.command and "--levels" not in spec.command
    assert spec.env["MIA_HARNESS_STAGE_BYTES"] == "2000000"
```

- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement**; update `test_stage_job_mounts_cache_and_out_rw_and_allows_hosts` (drop the `--levels`/`--voxel-size-nm` assertions).
- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_jobspec.py tests/test_mia_config.py -v` → PASS
- [ ] **Step 5: Commit** (`feat(buffers): plan digest in the artifact id; stage job takes plan-rule.json and voxel_size.json`)

---

### Task 4: Activities: write the job inputs, stop writing into builds, refuse foreign ids

**Files:**
- Modify: `mia_buffers/activities.py` (`_artifact`, `_artifact_block`, `prepare_stage`, `_publish`, `read_stage`, `_record_assets`); Tests: `tests/test_buffers_activities.py`

**Interfaces:**
- Produces:
  - `_artifact(round_id, record)` passes `self._row(round_id, record).get("plan_digest", "")`.
  - `_artifact_block` adds `"plan_digest"`; the staged asset row (`_record_assets`) adds `plan_digest`.
  - `prepare_stage` writes `workspace/plan-rule.json` = `{"artifact_id": self._artifact(...), "levels_key": self._levels_key(round_id), "rule": row["rule"]}` and `workspace/voxel_size.json` = `{axis: {"nm": v, "source": "assumed" if axis in row["assumed"] else "record", "basis": <assume basis or None>}}` plus `"z": {"nm": 1.0, "source": "synthetic"}` for a 2D row (the logic of `_voxel`). `_voxel` is replaced by `voxel_size_json(row: dict) -> dict` (module function, tested).
  - `_publish` no longer writes `stage.json`: it checks `report["artifact"]["id"] == self._artifact(round_id, record)` and otherwise returns a report with `ok: False` and the error `stage.json artifact id <x> is not the expected <y>`; then renames as today.

- [ ] **Step 1: Write the failing tests**

```python
def test_prepare_stage_writes_plan_rule_and_voxel_size(acts, round_with_row):
    ws = Path(await_(acts.prepare_stage(1, ROUND, "rec")))
    rule = json.loads((ws / "plan-rule.json").read_text())
    assert rule["artifact_id"] == acts._artifact(ROUND, "rec") and rule["levels_key"] == "p256-l1-2-4" and rule["rule"]["samples"]
    vox = json.loads((ws / "voxel_size.json").read_text())
    assert vox["z"] == {"nm": 1.0, "source": "synthetic", "basis": None} and vox["y"]["source"] in ("record", "assumed")

def test_read_stage_refuses_a_foreign_artifact_id(acts, build_dir):
    write_stage_json(build_dir, ok=True, artifact_id="f" * 16)
    result = await_(acts.read_stage(ROUND, "rec"))
    assert not result.ok and "is not the expected" in result.errors[0] and build_dir.exists()   # not published

def test_publish_does_not_rewrite_stage_json(acts, build_dir):
    write_stage_json(build_dir, ok=True, artifact_id=acts._artifact(ROUND, "rec"))
    before = (build_dir / "stage.json").read_bytes()
    await_(acts.read_stage(ROUND, "rec"))
    assert (acts._staged_dir(ROUND, "rec") / "stage.json").read_bytes() == before
```

- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement**; keep `_artifact_block` for the asset row only.
- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_activities.py -v && pixi run pytest tests/test_buffers_workflows.py -k replay` → PASS
- [ ] **Step 5: Commit** (`feat(buffers): prepare_stage writes plan-rule.json and voxel_size.json; publish refuses a foreign artifact id and never rewrites a build`)

---

### Task 5: Candidates in the registry and the sampler; eval lists without replacement

**Files:**
- Modify: `mia_buffers/sampler.py` (`Volume`, `draw`, new `draw_frozen`), `mia_buffers/api.py` (`VolumeBody`, `_put_volume`, `_split`), `mia_buffers/ledger.py` (`_volume_from`), `mia_buffers/register.py` (`volume_bodies`, `check_volume`); Tests: `tests/test_buffers_sampler.py`, `tests/test_buffers_api.py`, `tests/test_buffers_register.py`

**Interfaces:**
- Produces:
  - `Volume.samples: dict[str, tuple[dict, ...]] = {}` (candidates per split, each `{"plane", "centre_nm", "levels", "patch", "starts"}`; `ledger._volume_from` converts lists to tuples).
  - `draw(rng, volumes, split, *, patch, levels, channels)`: unchanged signature; after picking the volume by weight, if `volume.samples.get(split)` is non-empty, `candidate = rng.choice(...)` and the descriptor is `{**candidate, "volume", "path", "channels", "normalize", "candidate": index}`; else today's uniform centre.
  - `draw_frozen(rng, volumes, split, used: set[tuple[str, int]], *, patch, levels, channels) -> dict | None`: weighted volume pick among volumes with an unused candidate in `split` (a volume without candidates counts as always having one, drawn as today); picks an unused candidate uniformly; returns `None` when nothing is left.
  - `_split`: builds the prefix with `draw_frozen` and `used = {(d["volume"], d["candidate"]) for d in frozen if "candidate" in d}`; stops when `draw_frozen` returns `None`; the response is `{"descriptors", "size": min(config size, len(frozen) if exhausted else size), "exhausted": bool}`; a `from` past an exhausted list is 416 as today.
  - `VolumeBody.samples: dict[str, list[dict]] = {}`; `_put_volume` stores them (`INSERT OR REPLACE` already replaces the volume row, which is the candidate store).
  - `register.volume_bodies` copies `volume.get("samples") or {}` into the body; `check_volume` accepts a missing `samples`.
  - `register._split_planes` unchanged; `tests/test_buffers_register.py::test_split_planes_golden` asserts the same numbers as the harness's `tests/test_plan.py::test_split_planes_golden`.

- [ ] **Step 1: Write the failing tests**

```python
# test_buffers_sampler.py
def test_draw_picks_a_candidate_when_the_volume_has_them():
    cand = [{"plane": {"z": 0}, "centre_nm": [5120.0, 5120.0], "levels": [1, 2, 4], "patch": 256, "starts": {"1": [384, 384]}}]
    v = dataclasses.replace(vol("a"), samples={"train": tuple(cand)})
    d = draw(random.Random(0), [v], "train", patch=256, levels=[1, 2, 4], channels=1)
    assert d["centre_nm"] == [5120.0, 5120.0] and d["candidate"] == 0 and d["starts"] == {"1": [384, 384]} and d["volume"] == "a"

def test_volume_without_candidates_draws_as_before():
    d = draw(random.Random(0), [vol("a")], "train", patch=256, levels=[1, 2, 4], channels=1)
    assert "candidate" not in d and 5120.0 <= d["centre_nm"][0] <= 5120.0

def test_draw_frozen_is_without_replacement_and_weighted():
    cands = lambda n: tuple({"plane": {"z": 0}, "centre_nm": [5120.0 + i, 5120.0], "levels": [1, 2, 4], "patch": 256, "starts": {}} for i in range(n))
    a, b = dataclasses.replace(vol("a", weight=0.99, splits={"val": (0,)}), samples={"val": cands(3)}), dataclasses.replace(vol("b", weight=0.01, splits={"val": (0,)}), samples={"val": cands(3)})
    used, out = set(), []
    rng = random.Random(0)
    while (d := draw_frozen(rng, [a, b], "val", used, patch=256, levels=[1, 2, 4], channels=1)) is not None:
        used.add((d["volume"], d["candidate"])); out.append(d)
    assert len(out) == 6 and len(used) == 6 and [d["volume"] for d in out[:3]].count("a") >= 2

# test_buffers_api.py
def test_frozen_list_stops_when_candidates_run_out(tmp_path):
    s = svc(tmp_path); call(s, "PUT", "/buffers/b", payload=CFG)   # split_sizes val 4
    call(s, "PUT", "/buffers/b/volumes/a", payload={**body(), "samples": {"val": CANDS[:2]}})
    cid = call(s, "POST", "/buffers/b/consumers", payload={**CONSUMER, "split": "val"})[1]["consumer_id"]
    out = call(s, "GET", "/buffers/b/split/val", {"from": "0", "n": "4", "consumer": cid})[1]
    assert len(out["descriptors"]) == 2 and out["size"] == 2 and out["exhausted"] is True
    assert {d["candidate"] for d in out["descriptors"]} == {0, 1}

def test_re_put_replaces_candidates_and_keeps_frozen(tmp_path):
    ... PUT volume with 4 candidates, read val from 0..2, PUT the same volume again, read val 0..2 again: identical; GET /status volumes == 1

# test_buffers_register.py
def test_volume_bodies_carry_samples(tmp_path, spec_2d):
    ... stage.json volume with "samples": {"train": [...]} → body["samples"] == that
def test_split_planes_golden():
    assert _split_planes("r/v0.zarr", 10, {"policy": "fractions", "fractions": {"train": 0.6, "val": 0.2, "test": 0.2}, "seed": 0}) == {"train": [0, 1, 4, 5, 7, 8], "val": [3, 6], "test": [2, 9]}   # the volume id volume_bodies uses; the harness pins the same
```

- [ ] **Step 2: Run** → FAIL
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** `pixi run pytest tests/test_buffers_sampler.py tests/test_buffers_api.py tests/test_buffers_register.py tests/test_buffers_ledger.py -v` → PASS
- [ ] **Step 5: Commit** (`feat(buffers): planned samples register as candidates; train draws them by weight, eval lists draw them without replacement`)

---

### Task 6: Client, docs, example spec, final checks

**Files:**
- Modify: `mia_buffers/client.py` (one test that `NotStaged` propagates out of `__getitem__`; no catch added), `docs/development.md` (the buffers walkthrough: `harness stage --plan … --voxel-size …`, candidates, `exhausted`), `docs/specs/2026-10-21-training-buffers.md` §3 (one "as built" paragraph pointing at the new spec); Test: `tests/test_buffers_client.py`

- [ ] **Step 1: Write the failing test**

```python
def test_not_staged_propagates(monkeypatch, dataset):
    class NotStaged(LookupError): ...
    monkeypatch.setattr(client_module, "tensorize", lambda *a, **k: (_ for _ in ()).throw(NotStaged("c/0/0/0")))
    with pytest.raises(LookupError, match="c/0/0/0"):
        dataset[0]
```

- [ ] **Step 2: Run** → confirm it already passes or fails; if `__getitem__` swallows exceptions, remove that.
- [ ] **Step 3: Update the docs** (development.md walkthrough steps 2 and 4; a sentence in the register section on `samples`).
- [ ] **Step 4: Run everything**

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`
Expected: all clean; `git diff | grep -n 'rokickik\|/nrs/'` empty.

- [ ] **Step 5: Commit and open the PR** (`docs(buffers): sparse staging walkthrough`; PR body: compat note that old artifacts are not rewritten and new ids never match them; deploy with no round in sourcing; depends on mia-snippets PR from part A being merged and `harness.dir` fast-forwarded).
