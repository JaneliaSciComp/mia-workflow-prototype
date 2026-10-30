# mia-buffers milestone 4: sourcing through mia-data, binding publication, normalisation statistics

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer spec naming a dataset that has no approved binding reaches `buffer:ready` after a human approves that dataset on its mia-workflow-data issue; the staged volumes carry intensity statistics so `normalize.scope: volume|dataset` works.

**Architecture:** The buffer workflow hands an unmatched dataset to the `mia_data` workflow through its issue: a new `nudge_dataset` activity finds or opens the dataset issue in mia-workflow-data and adds `data:go`; the dataset waits in a `waiting-dataset` status until the buffer poller sees the approved binding under `storage_root/<dataset folder>/binding.yaml` (or `data:ineligible` on the issue) and signals `dataset_update`. mia-data bends to this (the user: its demo rules are not a constraint): it admits any catalog record on main, trusts the shared App's bot login for `go`, and publishes the approved binding beside the crops with its sha256 in the manifest. Datasets that end ineligible, or are still waiting when the buffer's timeout fires, are *dropped*: they leave the ready denominator and the plan is re-posted without them. For statistics, the harness's `stage` keeps a sorted intensity subsample per channel per volume in `stage.json`; registration turns it into `lo`/`hi` bounds for the spec's range at volume or dataset scope; the descriptor carries them and the muvit-2d tensorizer uses them instead of per-patch percentiles.

**Tech Stack:** as milestone 3 (Python 3.12, temporalio, pydantic, httpx, PyYAML; numpy/zarr only inside the harness).

**Spec:** `notes/specs/2026-10-21-training-buffers.md` v0.6: §1 (sourcing row), §3 step 1 and the recipe paragraph, §6.1–6.2, §10 row 4. Decisions taken in the 2026-10-24 brainstorm with the user: mia-data's rules may be overridden freely (it is a demo that will be deleted); BCSS and the `png-pairs` snippet are left to a live run.

## Global Constraints

- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` must be empty.
- Activity result dataclasses are wire format: fields added with defaults only. Workflow code is deterministic; the recorded histories `tests/fixtures/buffers/buffer_history.json` and `tests/fixtures/mia/dataset_history.json` must replay (guard an intended command change with `workflow.patched()` and re-record).
- Job output is untrusted: `read_untrusted`/`_job_json`, small typed results, `neutralize` before posting.
- `mia_buffers` imports from `mia_data` and `autodev`, never the reverse. Style per AGENTS.md; `# ponytail:` on deliberate shortcuts.
- Every repo stays on branch `training-buffers`; nothing is pushed, no PR. No real issue is created or labelled in mia-workflow-data during this milestone (the user wants BCSS as the live test).
- Checks: `.pixi/envs/default/bin/python -m pytest`, `.pixi/envs/default/bin/ruff check . && .pixi/envs/default/bin/ruff format --check .`, `.pixi/envs/default/bin/python -m mypy autodev mia_data mia_ops mia_buffers`. Harness: `<harness.dir>/harness/.pixi/envs/default/bin/python -m pytest harness/tests` from `<harness.dir>` (path in `mia_data.yaml` `harness.dir`, also in the scratchpad's `hd.txt`).

## Review Focus

1. A dataset whose issue is already `data:done` but whose binding file is missing (an older mia-data run that never published one): `nudge_dataset` does not add `data:go` blindly; the dataset waits and the poller cannot signal, so the timeout drops it with a reason naming the missing file. (Task 5: `test_nudge_dataset_done_without_binding_reports_it`.)
2. A `dataset_update` signal for a record that is not waiting (a late or duplicate signal): nothing changes, nothing crashes. (Task 5: `test_late_dataset_update_is_ignored`.)
3. `stage.json` whose `intensity_sample` is absent, not a list of lists, or holds non-numbers, with scope `volume` or `dataset`: the volume is refused with a reason that says to re-stage, never registered with `normalize: null`. (Task 3: `test_scope_without_sample_is_refused`, `test_malformed_sample_is_refused`.)
4. A `data:go` from a login that is neither in `trusted_bots` nor a writer is still refused; a trusted bot login that merely looks like one (`name[bot]` not listed) is refused too. (Task 1: `test_poll_accepts_go_only_from_listed_bot`.)
5. The buffer timeout when some datasets are staging (job in flight) and others waiting: only the waiting ones are dropped; the staging ones finish; ready is announced if the groups now allow it. (Task 5: `test_timeout_drops_waiting_datasets_and_may_reach_ready`.)

---

## File structure

| file | responsibility |
|---|---|
| `mia_data/config.py` | `GitHubCfg.trusted_bots` |
| `mia_data/poll.py` | `deliver_label` trusts listed bots |
| `mia_data/activities.py` | `admit` without a downloads entry; `publish_crops(..., binding)` |
| `mia_data/publish.py` | `publish(..., binding=None)` copies it; `reindex` records `binding.sha256` |
| `mia_data/seed.py` | `render_issue` tolerates an entry with only `record_commit` |
| `mia_data/workflows.py` | `_produce` passes `state.binding` |
| mia-snippets `harness/src/mia_harness/stage.py` | `intensity_sample` per volume |
| mia-snippets `tensorizers/muvit-2d/tensorizer.py`, `README.md` | `params["stats"]` bounds |
| `mia_buffers/register.py` | `normalize_bounds`, `volume_bodies` sets `normalize`; `buffer_config` adds `scope` |
| `mia_buffers/api.py`, `sampler.py`, `client.py` | `VolumeBody.normalize`, `Volume.normalize`, descriptor `normalize`, client passes `stats` |
| `mia_buffers/config.py` | `GitHubCfg.datasets_repo` |
| `mia_buffers/models.py` | `DatasetStatus.issue`, statuses `waiting-dataset`/`dropped`, `DatasetIssue` |
| `mia_buffers/status.py` | `render_plan_comment(..., dropped)` |
| `mia_buffers/activities.py` | `gh_datasets`, `nudge_dataset`, `comment_plan(..., dropped)` |
| `mia_buffers/poll.py` | `notice_datasets` → `dataset_update` signals |
| `mia_buffers/workflows.py` | `dataset_update` signal, `_await_dataset`, `_drop`, timeout drops waiters, `_groups` excludes dropped |
| `mia_buffers/cli.py` | worker builds the datasets-repo client |
| `mia_buffers.yaml.example`, `mia_data.yaml.example` | `datasets_repo`, `trusted_bots` |
| tests | `test_mia_config.py`, `test_mia_workflows.py`, `test_mia_activities.py`, `test_mia_publish.py`, `test_mia_seed.py`, `test_buffers_register.py`, `test_buffers_api.py`, `test_buffers_client.py`, `test_buffers_config.py`, `test_buffers_activities.py`, `test_buffers_workflows.py`; harness `tests/test_stage.py`, `tests/test_tensorize.py` |
| docs | `docs/development.md`, spec → v0.7 |

Fixed values: dataset statuses `pending | waiting-dataset | waiting-drop | staging | servable | failed | dropped`; signal `dataset_update(record, outcome)` with outcome `binding | ineligible`; mia-data issue marker `<!-- mia-data:record=<id> -->` (`mia_data.seed.MARKER`); the dataset issue's labels read as `data:<stage>`; manifest field `"binding": {"sha256": <hex>}` or `null`; `stage.json` volume field `"intensity_sample": [[sorted numbers] per channel]` with at most 4,096 values per channel from 8 random level-0 patch boxes per channel; `VolumeBody.normalize = {"lo": [per channel], "hi": [per channel]}` or `None`; tensorizer param `stats` = that dict, `scope` = the spec's scope.

---

### Task 1: mia-data bends: admission from main, trusted bots, binding publication

**Files:**
- Modify: `mia_data/config.py:54-66` (GitHubCfg), `mia_data/poll.py:67-110`, `mia_data/activities.py:320-356` (admit) and `publish_crops`, `mia_data/publish.py:51-135`, `mia_data/seed.py:29-90`, `mia_data/workflows.py:375-395`
- Test: `tests/test_mia_config.py`, `tests/test_mia_workflows.py`, `tests/test_mia_activities.py`, `tests/test_mia_publish.py`, `tests/test_mia_seed.py`

**Interfaces:**
- Produces: `GitHubCfg.trusted_bots: list[str] = []` (validator: each ends in `[bot]`, as `autodev.config` does); `publish(dataset_dir, src_dir, verified, withdrawn, *, tmp_max_age_s, binding: Path | None = None) -> dict`; `reindex` writes `manifest["binding"]`; `publish_crops(issue, round_id, crop_workspace, verified, sha, binding: str | None = None)`; `render_issue(record, entry, licences, repo, path, record_id)` accepting `entry = {"record_commit": sha}` alone; the published file `<storage_root>/<dataset folder>/binding.yaml` (what mia-buffers' `read_binding` already reads).

- [ ] **Step 1: Failing tests**

`tests/test_mia_config.py`:
```python
def test_trusted_bots_must_be_app_logins(tmp_path):
    with pytest.raises(SystemExit):  # or the ValueError load_config maps it to; match the existing config tests
        _load(tmp_path, github_extra={"trusted_bots": ["alice"]})
    assert _load(tmp_path, github_extra={"trusted_bots": ["mia-bot[bot]"]}).github.trusted_bots == ["mia-bot[bot]"]
```
(use whatever helper `test_mia_config.py` already has to write and load a yaml; the point is one refusal and one acceptance.)

`tests/test_mia_workflows.py`, beside `test_poll_refuses_go_on_licence_excluded`:
```python
async def test_poll_accepts_go_only_from_listed_bot(tmp_path):
    # cfg.github.trusted_bots = ["mia-bot[bot]"]; issue 1 labelled data:go by "mia-bot[bot]" (permission "none"),
    # issue 2 labelled data:go by "other[bot]" (permission "none")
    assert await run_poll(acts) == ["go #1"]
    assert gh.removed == [(1, "data:go"), (2, "data:go")] and "does not have write access" in gh.comments[2][0]["body"]
```

`tests/test_mia_activities.py`:
```python
async def test_admit_without_downloads_entry_pins_main(tmp_path):
    # catalog fake: record on main, no downloads state entry; gh_catalog.get_ref_sha("main") == MAIN_SHA
    pins = await t.acts.admit(7, "11")
    assert pins.record_commit == MAIN_SHA
    assert json.loads((t.round_dir / "downloads.json").read_text()) == {"record_commit": MAIN_SHA, "downloaders": []}

async def test_publish_crops_publishes_the_binding(tmp_path):
    # as test_publish_crops_into_dataset_folder, plus binding=<a binding-1.yaml stored in the round dir>
    assert (dataset_dir / "binding.yaml").read_bytes() == binding_bytes
    assert json.loads((dataset_dir / "manifest.json").read_text())["binding"] == {"sha256": sha256(binding_bytes)}

async def test_publish_crops_refuses_a_binding_outside_the_round(tmp_path):
    with pytest.raises(ValueError, match="not one of round"):
        await t.acts.publish_crops(7, "i7-e11", ws, verified, SHA, binding=str(tmp_path / "elsewhere.yaml"))
```

`tests/test_mia_publish.py`:
```python
def test_publish_writes_binding_and_manifest_sha(tmp_path):
    binding = tmp_path / "b.yaml"; binding.write_text("record: r\n")
    publish(ds, src, verified, {}, tmp_max_age_s=1, binding=binding)
    assert (ds / "binding.yaml").read_text() == "record: r\n"
    assert json.loads((ds / "manifest.json").read_text())["binding"]["sha256"] == hashlib.sha256(b"record: r\n").hexdigest()
    publish(ds, src, verified, {}, tmp_max_age_s=1)  # no binding passed: the published one stays
    assert json.loads((ds / "manifest.json").read_text())["binding"]["sha256"] == hashlib.sha256(b"record: r\n").hexdigest()

def test_manifest_without_binding_says_null(tmp_path):
    publish(ds, src, verified, {}, tmp_max_age_s=1)
    assert json.loads((ds / "manifest.json").read_text())["binding"] is None
```

`tests/test_mia_seed.py`:
```python
def test_render_issue_with_record_commit_only():
    title, body, labels = render_issue(RECORD, {"record_commit": "a" * 40}, ["CC-BY-4.0"], "org/cat", "datasets/x.yaml")
    assert body.startswith("<!-- mia-data:record=") and "Downloaded by: none" in body and "Dataset dir" not in body
```

- [ ] **Step 2: Run them**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_mia_config.py tests/test_mia_workflows.py tests/test_mia_activities.py tests/test_mia_publish.py tests/test_mia_seed.py -k "trusted or listed_bot or without_downloads or publishes_the_binding or outside_the_round or binding_and_manifest or without_binding or record_commit_only" -q`
Expected: every new test FAILS (unknown field / unexpected keyword / KeyError), the rest of each file passes.

- [ ] **Step 3: Implement**

- `GitHubCfg.trusted_bots: list[str] = []` plus the `[bot]` suffix check in `Config`'s validator (copy the two lines from `autodev/config.py:148-149`).
- `deliver_label`: `trusted = actor in getattr(cfg.github, "trusted_bots", [])`; refuse only when `not actor or not (trusted or await gh_data.permission(actor) in WRITE_PERMISSIONS)`.
- `admit`: `entry = downloaded.get(record_id)`; when missing, `entry = {"record_commit": await self.gh_catalog.get_ref_sha("main"), "downloaders": []}`; keep the `SHA_RE` check on the result; the "no record on main" error stays.
- `render_issue`: `entry.get("downloaders") or []` → `"none (jobs fetch on demand)"`; the `Dataset dir` line only when `entry.get("dataset_dir")`; `bytes_on_disk` → `entry.get("bytes_on_disk", "unknown")`.
- `publish(..., binding=None)`: when given, write `dataset_dir / "binding.yaml"` through a `.tmp-binding-<uuid>` + `os.replace`, before the `try`; `reindex` adds `"binding": {"sha256": ...}` from the file when present, else `None`.
- `publish_crops(..., binding=None)`: resolve under `self._round_dir(round_id)` as `prepare_job` does (`is_relative_to`), raise `ValueError(f"binding {binding} is not one of round {round_id}'s")` otherwise, pass `binding=Path(binding)`.
- `_produce`: `publish_crops` call gains `state.binding` after `sha`.

- [ ] **Step 4: Run the file set and the replay**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_mia_config.py tests/test_mia_workflows.py tests/test_mia_activities.py tests/test_mia_publish.py tests/test_mia_seed.py -q`
Expected: all pass, including `test_replay_dataset_history` (activity inputs are not part of Temporal's determinism check). If the replay fails, wrap the new argument in `if workflow.patched("publish-binding")` and re-record per the test's docstring.

- [ ] **Step 5: Commit**

```bash
git add mia_data tests/test_mia_config.py tests/test_mia_workflows.py tests/test_mia_activities.py tests/test_mia_publish.py tests/test_mia_seed.py
git commit -m "mia-data: admit any record on main, trust listed bot logins for go, publish the approved binding beside the crops"
```

---

### Task 2: Harness: intensity sample at stage time; muvit-2d honours given bounds (mia-snippets)

**Files:**
- Modify: `harness/src/mia_harness/stage.py` (`_verify` neighbourhood, `stage`), `tensorizers/muvit-2d/tensorizer.py` (`_normalise`, meta), `tensorizers/muvit-2d/README.md`, `SPEC.md` (one paragraph)
- Test: `harness/tests/test_stage.py`, `harness/tests/test_tensorize.py`

**Interfaces:**
- Produces: `stage.json` volume entry field `"intensity_sample": list[list[int|float]]`, one sorted list per channel (channel count = the `c` extent, 1 when there is no `c` axis), at most `SAMPLE_KEEP = 4096` values drawn from `SAMPLE_BOXES = 8` random level-0 patch boxes per channel with the stage's `rng`; `intensity_sample(pyramid: Pyramid, patch: int, rng) -> list[list[float]]` in `stage.py`. Tensorizer: `params["stats"] = {"lo": [C], "hi": [C]}` replaces the per-patch percentiles; `meta["normalize_scope"] = params.get("scope", "patch")`.

- [ ] **Step 1: Failing tests**

`harness/tests/test_stage.py`:
```python
def test_stage_records_a_sorted_intensity_sample_per_channel(tmp_path, io):
    b, r = write(tmp_path, (2, 300, 300), (2, 300, 300))
    rep = stage(b, SNAP, r, io, levels=[1, 2], patch=64, out=tmp_path / "out", voxel_size_nm=VOX)
    sample = rep["volumes"][0]["intensity_sample"]
    assert len(sample) == 1 and 1 <= len(sample[0]) <= 4096 and sample[0] == sorted(sample[0])
    assert all(isinstance(v, (int, float)) for v in sample[0])
    assert json.loads((tmp_path / "out" / "stage.json").read_text())["volumes"][0]["intensity_sample"] == sample
```
(if `write` can make a `c` axis, add the multi-channel assertion `len(sample) == C`; otherwise test `intensity_sample` directly on a pyramid written from a `(2, 2, 100, 100)` `czyx` array.)

`harness/tests/test_tensorize.py`:
```python
def test_stats_bounds_replace_patch_percentiles():
    img = np.full((2, 8, 8), 100, dtype=np.uint16); img[1] = 50
    lv = LevelPatch(1, img, {}, (10.0, 10.0), (0.0, 0.0), (0, 0))
    stack = Stack([lv], 8, {"z": 0}, (40.0, 40.0))
    mod = load_snippet(TENSORIZERS, "muvit-2d", {}, {}, kind="tensorizers")
    sample = mod.tensorize(stack, {"stats": {"lo": [0, 0], "hi": [200, 100]}, "scope": "volume"})
    assert np.allclose(sample["img"][0, 0], 0.5) and np.allclose(sample["img"][0, 1], 0.5)
    assert sample["meta"]["normalize_scope"] == "volume"
    patch_only = mod.tensorize(stack, {})
    assert np.allclose(patch_only["img"], 0.0)  # constant patch: lo == hi → 0
```

- [ ] **Step 2: Run them**

Run (from `<harness.dir>`): `harness/.pixi/envs/default/bin/python -m pytest harness/tests/test_stage.py harness/tests/test_tensorize.py -k "intensity_sample or stats_bounds" -q`
Expected: FAIL (`KeyError: 'intensity_sample'`; the stats sample equals the patch-only result).

- [ ] **Step 3: Implement**

`stage.py`: `intensity_sample(p, patch, rng)`: `size = dict(zip(p.axes, p.levels[0].shape))`; for each `c in range(size.get("c", 1))`: 8 boxes via the same random-box logic as `_verify` with the plane fixed to `{"c": c}` when `c` is an axis and a random `z` when `z` is; concatenate the flattened reads; if longer than 4096, `rng.choice(values, 4096, replace=False)`; `np.sort(...).tolist()`. Call it in `stage()` for the raw pyramid and put the result in the volume dict. Refactor `random_box` out of `_verify` so both use it (one helper, no duplicate).
`tensorizer.py` `_normalise`: when `params.get("stats")`: `lo = np.asarray(stats["lo"], np.float32)[:, None, None]`, same for `hi`, apply the same `(x - lo) / max(hi - lo, EPS)` and clip; raise `ValueError` when the bound lengths differ from the channel count. Meta gains `normalize_scope`.
README and SPEC.md: one paragraph each on `intensity_sample` and `stats`.

- [ ] **Step 4: Run the harness suite**

Run: `harness/.pixi/envs/default/bin/python -m pytest harness/tests -q`
Expected: all pass (154 + 2).

- [ ] **Step 5: Commit (mia-snippets, branch training-buffers)**

```bash
git add harness/src/mia_harness/stage.py tensorizers/muvit-2d harness/tests/test_stage.py harness/tests/test_tensorize.py SPEC.md
git commit -m "harness: stage records a per-channel intensity sample; muvit-2d normalises with given bounds"
```

---

### Task 3: Registration computes normalisation bounds; the service and client carry them

**Files:**
- Modify: `mia_buffers/register.py`, `mia_buffers/api.py:62-75`, `mia_buffers/sampler.py:22-40,70-90`, `mia_buffers/client.py:188-193`
- Test: `tests/test_buffers_register.py`, `tests/test_buffers_api.py`, `tests/test_buffers_client.py`

**Interfaces:**
- Consumes: Task 2's `intensity_sample`.
- Produces: `normalize_bounds(samples: list[list[float]], range: tuple[float, float]) -> dict` → `{"lo": [...], "hi": [...]}` (per channel; nearest-rank percentile on the sorted list, no numpy); `check_sample(value, channels: int) -> str | None`; `volume_bodies` adds `"normalize"` to each body (None for scope `patch`); `buffer_config` adds `"scope"` to `tensorizer_params`; `VolumeBody.normalize: dict | None = None`; `Volume.normalize: dict | None = None` (last field, default, so existing ledgers load); descriptor key `"normalize"`; client params key `"stats"`.

- [ ] **Step 1: Failing tests**

`tests/test_buffers_register.py`:
```python
def test_normalize_bounds_nearest_rank():
    assert normalize_bounds([[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]], (10, 90)) == {"lo": [10], "hi": [90]}
    assert normalize_bounds([[5, 5, 5], [1, 2, 3]], (0, 100)) == {"lo": [5, 1], "hi": [5, 3]}

def test_volume_scope_bounds_from_sample(tmp_path):
    # stage.json with two volumes, each with intensity_sample [[...]] ; spec normalize.scope = "volume", range (1, 99)
    bodies = dict(volume_bodies(ROW, staged, spec))
    assert bodies[f"{ROW['id']}/v0.zarr"]["normalize"] == normalize_bounds(SAMPLE_V0, (1, 99))
    assert bodies[f"{ROW['id']}/v1.zarr"]["normalize"] == normalize_bounds(SAMPLE_V1, (1, 99))

def test_dataset_scope_pools_volumes(tmp_path):
    # same stage.json; scope "dataset": both bodies carry the bounds of the concatenated samples
    pooled = normalize_bounds([sorted(SAMPLE_V0[0] + SAMPLE_V1[0])], (1, 99))
    assert all(body["normalize"] == pooled for body in bodies.values())

def test_patch_scope_has_no_bounds(tmp_path):
    assert all(body["normalize"] is None for body in bodies.values())  # scope "patch" (the default)

def test_scope_without_sample_is_refused(tmp_path):
    # scope "volume", a volume entry without intensity_sample
    refused = {}
    assert volume_bodies(ROW, staged, spec, refused=refused) == []
    assert "re-stage" in refused[f"{ROW['id']}/v0.zarr"]

def test_malformed_sample_is_refused(tmp_path):
    # intensity_sample = [["a", "b"]] and intensity_sample = [[1, 2], [3]] with channels_stored 1 → refused, not registered

def test_buffer_config_carries_scope():
    assert buffer_config(PLAN, spec, tensorizer="muvit-2d", split_sizes={"val": 1, "test": 1})["tensorizer_params"]["scope"] == "patch"
```

`tests/test_buffers_api.py`: in the existing put-volume + draw test, add `"normalize": {"lo": [1], "hi": [200]}` to the body and assert the drawn descriptor has `descriptor["normalize"] == {"lo": [1], "hi": [200]}`; a body without the field still registers and draws `None`.

`tests/test_buffers_client.py`: in the test that mocks `tensorize`, add `"normalize": {"lo": [0], "hi": [9]}` to the descriptor and assert the params passed to `tensorize` include `"stats": {"lo": [0], "hi": [9]}` and `"scope"` from the config.

- [ ] **Step 2: Run them**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_register.py tests/test_buffers_api.py tests/test_buffers_client.py -q`
Expected: the new tests FAIL (ImportError / KeyError 'normalize' / missing 'stats'), the old ones pass.

- [ ] **Step 3: Implement**

`register.py`: `normalize_bounds` with nearest rank `values[min(n - 1, max(0, math.ceil(p / 100 * n) - 1))]`; `check_sample` (list of `channels` lists of ints/floats, each non-empty); in `volume_bodies`, when `spec.sampling.normalize.scope != "patch"`, a volume with a bad sample goes to `refused` with `"stage.json has no usable intensity_sample (<why>); re-stage with the current harness"`; dataset scope pools the per-channel lists of every accepted volume, sorted (`# ponytail: dataset bounds pool each volume's sample with equal weight, not by plane count; weight by planes if a dataset of unequal volumes shows it matters`). `buffer_config` adds `"scope": normalize.scope`.
`api.py`: `VolumeBody.normalize: dict | None = None` → `Volume(normalize=...)`. `sampler.py`: `Volume.normalize: dict | None = None`; descriptor `"normalize": volume.normalize`. `client.py` `_tensorize`: `params = {**tensorizer_params, "channels": ..., "stats": descriptor.get("normalize")}`.

- [ ] **Step 4: Run the three files, then the suite**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_register.py tests/test_buffers_api.py tests/test_buffers_client.py -q` then `.pixi/envs/default/bin/python -m pytest -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/register.py mia_buffers/api.py mia_buffers/sampler.py mia_buffers/client.py tests/test_buffers_register.py tests/test_buffers_api.py tests/test_buffers_client.py
git commit -m "mia-buffers: registration turns the stage sample into normalisation bounds at volume or dataset scope; descriptors carry them"
```

---

### Task 4: Config, models and comments for the hand-off

**Files:**
- Modify: `mia_buffers/config.py` (GitHubCfg), `mia_buffers/models.py` (DatasetStatus, new DatasetIssue), `mia_buffers/status.py` (`render_plan_comment`), `mia_buffers.yaml.example`, `mia_data.yaml.example`
- Test: `tests/test_buffers_config.py`, `tests/test_buffers_activities.py` (status rendering tests live there or in a small new `tests/test_buffers_status.py`)

**Interfaces:**
- Produces: `GitHubCfg.datasets_repo: str` (required; `make_cfg` sets `"org/data"`); `DatasetStatus.issue: int | None = None`; docstring statuses `pending | waiting-dataset | waiting-drop | staging | servable | failed | dropped`; `@dataclass DatasetIssue(number: int, stage: str, nudged: bool, note: str = "")`; `render_plan_comment(plan, link, dropped: dict[str, str] | None = None)` which leaves dropped records out of the pool table and lists them under **Dropped** with their reasons.

- [ ] **Step 1: Failing tests**

```python
def test_config_requires_datasets_repo(tmp_path):  # test_buffers_config.py
    with pytest.raises(SystemExit):
        load_without(tmp_path, "datasets_repo")
    assert make_cfg(tmp_path).github.datasets_repo == "org/data"

def test_plan_comment_lists_dropped_records():  # beside the existing render_plan_comment tests
    body = render_plan_comment(PLAN_JSON, None, dropped={"bcss": "data:ineligible on org/data#5"})
    assert "`bcss`" not in body.split("**Dropped**")[0] and "**Dropped**" in body and "data:ineligible" in body
    assert "1 in the pool of" in body  # the count excludes dropped records
```

- [ ] **Step 2: Run** `.pixi/envs/default/bin/python -m pytest tests/test_buffers_config.py tests/test_buffers_activities.py -q -k "datasets_repo or dropped"`
Expected: FAIL (field missing / unexpected keyword).

- [ ] **Step 3: Implement** the fields, the dataclass, the rendering (pool rows filtered by `row["id"] not in dropped`; a `**Dropped**` list after the warnings), and the two example files (`datasets_repo: JaneliaSciComp/mia-workflow-data` with a comment; `trusted_bots: []  # app logins ("name[bot]") whose data:go counts, e.g. the buffer worker's`).

- [ ] **Step 4: Run** the same two files, then `ruff` and `mypy`.
Expected: pass, clean.

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/config.py mia_buffers/models.py mia_buffers/status.py mia_buffers.yaml.example mia_data.yaml.example tests/test_buffers_config.py tests/test_buffers_activities.py
git commit -m "mia-buffers: datasets repo in the config, dataset issue in the status, dropped records in the plan comment"
```

---

### Task 5: The hand-off: `nudge_dataset`, `dataset_update`, drops, the timeout

**Files:**
- Modify: `mia_buffers/activities.py` (`__init__`, `all`, `comment_plan`, new `nudge_dataset`), `mia_buffers/poll.py` (`run_poll`, new `notice_datasets`), `mia_buffers/workflows.py` (signal, `_groups`, `_sourcing` timeout branch, `_await_stragglers.abandonable`, `_source_steps`, new `_await_dataset`, `_drop`), `mia_buffers/cli.py` (`cmd_worker`)
- Test: `tests/test_buffers_activities.py`, `tests/test_buffers_workflows.py`, `tests/test_buffers_cli.py` (if it constructs Activities)

**Interfaces:**
- Consumes: Task 1's binding file and trusted-bot `go`; Task 4's `DatasetIssue`, `DatasetStatus.issue`, `render_plan_comment(..., dropped)`.
- Produces: `Activities(cfg, gh_issues, gh_snippets, gh_catalog, jobs, bot, client=None, probe=None, gh_datasets: GitHub | None = None)`; activity `nudge_dataset(round_id: str, record: str) -> DatasetIssue`; activity `comment_plan(issue: int, round_id: str, dropped: dict[str, str] | None = None)` (marker `plan <round>` when `dropped` is empty, else `plan <round> minus <sha256 of the sorted dropped ids>[:8]`); workflow signal `dataset_update(record: str, outcome: str)`; poller log lines `"dataset #<issue> <record> <outcome>"`.

`nudge_dataset` rules (the dataset issue repo is `cfg.github.datasets_repo`, client `gh_datasets`):
1. `issue = await gh_datasets.find_issue_by_marker(MARKER.format(record))`; none → `create_issue(*render_issue(self._record(round_id, record), {"record_commit": pins.catalog_ref}, cfg.crops.licences, cfg.github.catalog_repo, row["path"], record))`.
2. `labels = {name}`; stage = the `data:<x>` label present, else `"new"`.
3. `ineligible` or `licence-excluded` present → `DatasetIssue(n, "ineligible", False)`.
4. `done` → `DatasetIssue(n, "done", False, note="data:done but no binding at <storage_root>/<folder>/binding.yaml: run the dataset again (data:go) so mia-data publishes it")` — the workflow treats stage `done` without a binding like any other wait (a human must act; the timeout drops it with that note).
5. `data:go` already present, or stage in `matching testing review cropping` → `DatasetIssue(n, stage, False)`.
6. else (`new`, `needs-human`) → `add_labels(n, ["data:go"])`, `DatasetIssue(n, stage, True)`.

Workflow `_source_steps` prefix:
```python
binding = await self._act("read_binding", ...)
if not binding.found:
    if not await self._await_dataset(dataset):   # False: dropped
        return
    binding = await self._act("read_binding", ...)
    if not binding.found:
        return await self._fail(dataset, "; ".join(binding.errors) or "binding not found")
```
`_await_dataset`: `info = nudge_dataset(...)`; `dataset.issue = info.number`; `info.stage == "ineligible"` → `_drop(dataset, f"data:ineligible on {repo}#{n}")`, return False; else `dataset.status = "waiting-dataset"`, one comment (marker `dataset <round> <record> waiting`) saying `data:go` was added (or that the issue is already in `data:<stage>`; include `info.note`); `await workflow.wait_condition(lambda: dataset.record in self.dataset_updates)`; outcome = pop; `"ineligible"` → drop, False; else True.
`_drop(dataset, reason)`: status `dropped`, reason; comment (marker `dataset <round> <record> dropped`); `comment_plan(issue, round_id, {d.record: d.reason for d in datasets if d.status == "dropped"})`.
`_groups`: skip `dropped` datasets; a group whose datasets are all dropped disappears. `_ready_now` is unchanged (and stays False when no group remains).
Timeout branch: `waiting = [t, d for pairs if not t.done() and d.status in ("waiting-dataset", "waiting-drop")]`; cancel those tasks, set `dropped` with reason `f"still {status} after {timeout_days:g} days"`, one `comment_plan(..., dropped)`; then if `_ready_now()` fall through to the ready path (announce + stragglers), else cancel the rest and `needs-human` as today, naming the groups below threshold.
`abandonable`: pending statuses ⊆ `{"waiting-drop", "waiting-dataset"}`.
Poller `notice_datasets(activities, number)`: for `waiting-dataset` datasets: binding file at `storage_root / dataset_folder(record_yaml, record) / "binding.yaml"` exists → signal `("binding")`; else `dataset.issue` and `data:ineligible` or `licence-excluded` among `gh_datasets.get_issue(issue)["labels"]` → signal `("ineligible")`; each `(number, record, outcome)` signalled once per process (`activities._signalled: set`). The record YAML comes from `activities._record(state.round_id, record)`.
`cmd_worker`: `gh_datasets=_gh(cfg, cfg.github.datasets_repo)`, closed with the others.

- [ ] **Step 1: Failing activity tests** (`tests/test_buffers_activities.py`, with the fake GitHub the file already uses, extended with `find_issue_by_marker`, `create_issue`, `add_labels`, `get_issue`)

```python
async def test_nudge_dataset_opens_the_issue_and_adds_go(tmp_path)        # no issue → created with the marker first line, labels data:new…, then data:go added; DatasetIssue(n, "new", True)
async def test_nudge_dataset_in_progress_does_not_relabel(tmp_path)        # issue labelled data:testing → nudged False, no add_labels
async def test_nudge_dataset_ineligible(tmp_path)                           # licence-excluded → stage "ineligible"
async def test_nudge_dataset_done_without_binding_reports_it(tmp_path)      # data:done → stage "done", note names binding.yaml, no add_labels
async def test_comment_plan_reposts_once_per_dropped_set(tmp_path)          # two calls with the same dropped dict → one comment; a different set → another
```

- [ ] **Step 2: Run** `.pixi/envs/default/bin/python -m pytest tests/test_buffers_activities.py -q -k "nudge or reposts"` → Expected: FAIL (`AttributeError: nudge_dataset` / unexpected keyword).

- [ ] **Step 3: Implement the activities and the CLI wiring; run Step 2 again** → Expected: PASS.

- [ ] **Step 4: Failing workflow and poll tests** (`tests/test_buffers_workflows.py`; `Mocks` gains `nudges: dict[str, DatasetIssue]`, a `nudge_dataset` mock, and `comment_plan` records `dropped`; `read_binding` returns `BindingInfo(False, errors=["no binding"])` for records in `self.unbound: set` until `self.bound.add(record)`)

```python
async def test_unmatched_dataset_waits_for_binding_then_stages(env):
    # bcss unbound; nudge → DatasetIssue(5, "new", True)
    # after approve: bcss status "waiting-dataset", issue 5, a comment mentions "data:go"; u937 servable; not ready
    # m.bound.add("bcss"); signal dataset_update("bcss", "binding") → bcss staged, registered; ready

async def test_ineligible_dataset_is_dropped_and_plan_reposted(env):
    # plan=ONE_GROUP-like with two groups; bcss nudge → DatasetIssue(5, "ineligible", False)
    # bcss status "dropped", comment_plan called twice, the second with {"bcss": ...}; ready on u937 alone

async def test_late_dataset_update_is_ignored(env):
    # signal dataset_update("u937", "binding") while u937 is servable → statuses unchanged, no extra activity calls

async def test_timeout_drops_waiting_datasets_and_may_reach_ready(env):
    # ONE_GROUP min_fraction 0.5 is too easy: use PLAN (two groups), bcss waiting-dataset forever, u937 servable;
    # env.sleep(15 days) → bcss dropped ("still waiting-dataset after 14 days"), plan re-posted, stage "ready"

async def test_poll_signals_dataset_updates(tmp_path):
    # waiting-dataset datasets: "u937" (binding file appears under storage_root/<folder>/) and "bcss" (issue 5 labelled
    # data:ineligible) → ["dataset #2 u937 binding", "dataset #2 bcss ineligible"], each once; a second pass → []
```
The existing timeout tests keep their assertions (`test_timeout_without_ready_is_needs_human` has a staging dataset, so it still ends needs-human).

- [ ] **Step 5: Run** `.pixi/envs/default/bin/python -m pytest tests/test_buffers_workflows.py -q -k "unmatched or ineligible or late_dataset or drops_waiting or dataset_updates"` → Expected: FAIL (no signal `dataset_update`, no `nudge_dataset`).

- [ ] **Step 6: Implement the workflow and the poller; run the whole file**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_workflows.py -q`
Expected: all pass, including `test_replay_buffer_history` (the recorded run had every binding found; the new branch issues no command on that path). If the replay fails, guard the new branch with `workflow.patched("dataset-handoff")` and re-record per the docstring.

- [ ] **Step 7: Full suite, ruff, mypy** → Expected: pass, clean.

- [ ] **Step 8: Commit**

```bash
git add mia_buffers tests/test_buffers_activities.py tests/test_buffers_workflows.py tests/test_buffers_cli.py
git commit -m "mia-buffers: unmatched datasets go to mia-data (data:go on the dataset issue), wait for the published binding, or are dropped from the plan"
```

---

### Task 6: Docs and spec v0.7

**Files:**
- Modify: `docs/development.md` (milestone 3 section: step 4 and the "Not in milestone 3" paragraph; new short section "Datasets without a binding and normalisation statistics (milestone 4)"), `notes/specs/2026-10-21-training-buffers.md` (header status/version, §1 sourcing row, §3 step 1 and the recipe paragraph with *As built in milestone 4* notes, §6.1–6.2 as built, §10 row 4 note), `mia_data` docs line in `docs/development.md` on `trusted_bots` and the mia_data.yaml entry the operator must add

- [ ] **Step 1: Write** the docs: the operator steps (add `datasets_repo` to `mia_buffers.yaml`; add the buffer worker's bot login to `trusted_bots` in `mia_data.yaml`; both deployments on the same GitHub App); what a human sees on the dataset issue (opened by the buffer bot with the record body; `data:go`; the usual mia-data review); where the binding lands (`<storage_root>/<dataset folder>/binding.yaml`, `manifest.json.binding.sha256`); the drop rule; the statistics (`intensity_sample`, `normalize` bounds, `scope`); the re-stage requirement for older staged directories when scope is not `patch`.
- [ ] **Step 2: Check** `git diff | grep -n 'rokickik\|/nrs/'` → Expected: empty.
- [ ] **Step 3: Commit**

```bash
git add docs/development.md notes/specs/2026-10-21-training-buffers.md
git commit -m "docs: milestone 4 hand-off and statistics; spec v0.7"
```

---

### Task 7: Verification on real data (by hand, no worker)

No real issue in mia-workflow-data is created or labelled (the user keeps BCSS for the live run). What runs:

- [ ] **Step 1: Stage U937 again** with the Task 2 harness: rebuild the stage job command exactly as in milestone 3 (`m3_bcss_stage.py`'s sibling for U937 in the scratchpad, or `jobspec.stage_job(...)` body printed and run with the job's environment variables, cache read-write, no network needed since the cache is warm) into a fresh `staged_root/<levels key>/<record>/`.
Expected: `stage.json` `ok: true`, `volumes[0].intensity_sample` has one sorted list of 4,096 values within the dtype's range; wall time within a minute of milestone 3's 35 s.
- [ ] **Step 2: Register at volume scope**: a copy of the issue-1 spec with `normalize: {scope: volume}` through `create_buffer` + `register_dataset` (scratchpad script as `m3_register.py`) against a local `mia-buffers serve`.
Expected: the volume is registered; `GET` of a draw's descriptor shows `normalize.lo/hi` numbers matching `normalize_bounds(sample, (1, 99))`.
- [ ] **Step 3: Consume** 16 samples with `BufferDataset` under the harness env (as in milestone 2/3).
Expected: `img` values in `[0, 1]`, `meta.normalize_scope == "volume"`, and two patches of different brightness are no longer both stretched to the full range (compare their means to the patch-scope run).
- [ ] **Step 4: Patch scope regression**: register with the default spec; descriptors carry `normalize: null`; consumption unchanged from milestone 3.
- [ ] **Step 5: The hand-off against GitHub, read-only**: with the real config and `gh auth token`, call `gh_datasets.find_issue_by_marker(MARKER.format(<u937 record id>))` and confirm it returns `None` (no issue exists, so a live run would open one); do not create it.
- [ ] **Step 6: Record** the numbers in the spec's §10 row 4 note and the ledger; commit as part of Task 6's files if the spec changes.

```bash
git add notes/specs/2026-10-21-training-buffers.md
git commit -m "mia-buffers: milestone 4 verification notes"
```

---

## Self-review

- Spec coverage: §3 step 1 (data:go, wait, ineligible/timeout drop + re-post) → Tasks 4–5; §6.1 binding publication → Task 1; §6.2 trust → Task 1; recipe statistics → Tasks 2–3; §10 row 4 done-when → Task 5's `test_unmatched_dataset_waits_for_binding_then_stages` plus the human approval on the dataset issue, which is mia-data's existing review (not re-tested here). mia-data's downloads-state coupling is removed (user's call) → Task 1.
- Type consistency: `DatasetIssue(number, stage, nudged, note)` in Tasks 4 and 5; `normalize_bounds` / `normalize` / `stats` names in Tasks 2, 3 and the client; `dataset_update(record, outcome)` in Task 5's workflow, poller and tests.
- Proportion: seven tasks, test names and signatures, bodies only where the signature leaves a choice (nearest-rank formula, nudge rules, timeout branch).
