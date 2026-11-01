# mia-buffers milestone 3: `MiaBuffer` and `MiaBufferPoll` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A buffer issue in `JaneliaSciComp/mia-buffers` goes `buffer:go` → planning → plan-review → `buffer:approve` → sourcing (stage job per dataset, or a manual drop) → register with buffer-api → `buffer:ready`, with datasets joining progressively.

**Architecture:** A Temporal workflow `MiaBuffer` built like `mia_data.workflows.MiaDataDataset` (signals for human labels, handled-event list, every side effect an activity called by name, `continue_as_new` at quiet points), a poller `MiaBufferPoll` that turns labels and dropped files into signals, and a `mia_buffers.Activities` class that reuses mia-data's job machinery (`jobspec._spec`, snapshots, `CsubJobs`, `read_untrusted`) and milestone 2's `register` functions. Download is folded into the stage job (the reader snippets are reviewed code, so the raw cache is mounted read-write as mia-data's trusted crop job does). A dataset whose hosts are not allowlisted, or whose binding uses `manual://` URLs, waits for a human to drop the files; the worker moves them into the cache under the harness's key and writes `files.json`, which the harness reads instead of HEAD.

**Tech Stack:** Python 3.12, temporalio, pydantic, httpx, PyYAML; the mia-snippets harness (zarr, numpy) for the stage job. Tests: pytest with Temporal's time-skipping test server (as `tests/test_mia_workflows.py`).

**Spec:** `notes/specs/2026-10-21-training-buffers.md` v0.5 (§1 lifecycle, §3 sourcing, §5 service calls, §6.4, §10 milestone 3). mia-snippets `SPEC.md` for the harness.

## Global Constraints

- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` must be empty. `mia_buffers.yaml` is git-ignored; `mia_buffers.yaml.example` uses placeholders.
- Activity result dataclasses are wire format: fields added with defaults only. Workflow code is deterministic; a recorded history fixture guards it.
- Output of sandboxed jobs is untrusted: read with `read_untrusted`, reduce to a small typed result, `neutralize` before posting.
- `mia_buffers` imports from `mia_data` and `autodev`, never the reverse. Style per AGENTS.md: descriptive names, module docstrings, type hints on object parameters, line length 120, `# ponytail:` on deliberate shortcuts.
- Every repo touched stays on branch `training-buffers`; nothing is pushed, no PR.
- Checks run with `.pixi/envs/default/bin/python -m pytest`, `.pixi/envs/default/bin/ruff check . && ruff format --check .`, `.pixi/envs/default/bin/python -m mypy autodev mia_data mia_ops mia_buffers` (`pixi run` is broken in this checkout). Harness tests run with `<harness.dir>/harness/.pixi/envs/default/bin/python -m pytest harness/tests`.

## Review Focus

1. An issue body with no ```` ```yaml ```` fence, two fences, or a fence holding an invalid spec: `admit` refuses with the validator's message and the issue goes `needs-human`; nothing crashes. (Task 4: `test_admit_refuses_issue_without_one_yaml_fence`.)
2. A stage job that exits 0 but whose `stage.json` is missing, malformed or says `ok: false`: the dataset is `failed` with the errors, and nothing is registered. (Task 5: `test_stage_report_not_ok_fails_dataset_without_registering`.)
3. A drop where a listed file is missing, empty, or a symlink: `verify_drop` moves nothing, reports the missing names, and the dataset keeps waiting. (Task 4: `test_verify_drop_rejects_symlink_and_empty_file`.)
4. An `approve` older than the plan comment, or any label while sourcing runs: refused with a comment, never acted on. (Task 5: `test_stale_approve_is_refused`, `test_go_while_sourcing_is_refused_busy`.)
5. The timeout: after `sourcing.dataset_timeout_days` with the ready threshold unmet, the issue goes `needs-human` naming the groups below threshold; a buffer that was already ready is not touched. (Task 5: `test_timeout_without_ready_is_needs_human`, `test_timeout_after_ready_changes_nothing`.)

---

## File structure

| file | responsibility |
|---|---|
| `mia_buffers/config.py` (new) | `mia_buffers.yaml`: a `Config` subclassing `mia_data.config.Config` (same sections plus `raw_root`, `staged_root`, `service`, `sourcing`), `STAGES`, `HUMAN_LABELS`, `load_config` |
| `mia_buffers/models.py` (new) | wire-format dataclasses: `BufferPins`, `PlanResult`/`DatasetPlan`, `BindingInfo`, `StageJobArgs`, `StageResult`, `DropResult`, `RegisterResult`, `SourcingLimits`, `DatasetStatus`, `BufferState`, `job_key` |
| `mia_buffers/jobspec.py` (new) | `levels_key`, `catalog_snapshot` (catalog repo at a commit), `stage_job` (the one job kind) |
| `mia_buffers/drop.py` (new) | pure functions for the manual drop: `drop_names`, `cache_key`, `files_manifest` read/merge |
| `mia_buffers/activities.py` (new) | the `Activities` class |
| `mia_buffers/poll.py` (new) | `run_poll`: labels → signals, dropped files → `dropped` signals |
| `mia_buffers/workflows.py` (new) | `MiaBuffer`, `MiaBufferPoll` |
| `mia_buffers/status.py` (new) | `render_plan_comment`, `render_drop_comment`, `render_ready` |
| `mia_buffers/cli.py` | `worker`, `schedule apply`, `init-labels` |
| `mia_buffers/spec.py` | `ready.min_fraction`, `sourcing.dataset_timeout_days` |
| `mia_buffers/register.py` | `volume_bodies(..., report=None)` |
| `mia_data/config.py` | `PathsCfg.ROOTS` class attribute; `load_config` parametrised |
| `mia_data/jobspec.py` | `archive_snapshot` helper extracted from `snapshot`; `_spec`/`assert_pin` typed with a `HarnessPinned` protocol |
| `mia_data/poll.py` | `deliver_label(..., workflow, make_state, guard)` |
| `mia_data/status.py` | `render_stage(stage, detail, prefix="data")` |
| `mia_ops/manifest.py`, `mia_ops/services.py` | app kind `mia-buffers` |
| mia-snippets `harness/src/mia_harness/io.py`, `cache.py`, `SPEC.md` | `files.json` manifest, range reads from a cached whole file, `manual://` |
| tests | `test_buffers_config.py`, `test_buffers_jobspec.py`, `test_buffers_drop.py`, `test_buffers_activities.py`, `test_buffers_workflows.py` (+ `tests/fixtures/buffers/buffer_history.json`), additions to `test_buffers_spec.py`, `test_buffers_cli.py`, `test_mia_ops_services.py`, `test_mia_ops_manifest.py`; harness `tests/test_io.py` |
| docs | `docs/development.md`, `notes/specs/2026-10-21-training-buffers.md` → v0.6, `mia_buffers.yaml.example`, `.gitignore` |

Fixed values used throughout: labels `buffer:<stage>`; stages `new planning plan-review sourcing ready idle retired needs-human`; human labels `go approve`; workflow id `mia-buffers/<buffers repo>/issue-<n>`; marker `<!-- mia-buffers: ... -->`; round id `i<issue>-e<event id>`; buffer id = round id; raw cache `raw_root/<record id>/` (files keyed by sha256, `incoming/` for drops, `files.json` manifest); staged pyramids `staged_root/<levels key>/<record id>/` with `levels_key(256, [1,2,4]) == "p256-l1-2-4"`; job key `buf-<issue>-<round>-stage-<record[:40]>-<sha256(record)[:8]>`.

---

### Task 1: Spec fields, config, ops app kind

**Files:**
- Modify: `mia_buffers/spec.py` (after `Cache`, and `Spec`)
- Modify: `mia_data/config.py:66-92` (`PathsCfg`), `:218-238` (`load_config`)
- Create: `mia_buffers/config.py`, `mia_buffers.yaml.example`
- Modify: `.gitignore` (add `mia_buffers.yaml`), `mia_ops/manifest.py:25` (`ServiceCfg.app`), `mia_ops/services.py:92-101`
- Test: `tests/test_buffers_spec.py`, `tests/test_buffers_config.py` (new), `tests/test_mia_ops_services.py`

**Interfaces:**
- Produces: `mia_buffers.spec.Ready(min_fraction: float = 1.0)` (`0 < min_fraction <= 1`), `Sourcing(dataset_timeout_days: float = 14)` (`> 0`), `Spec.ready: Ready = Ready()`, `Spec.sourcing: Sourcing = Sourcing()`.
- Produces: `mia_buffers.config.Config` with `cfg.github.buffers_repo` (property; the field is `data_repo`, YAML key `buffers_repo`), `cfg.paths.raw_root`, `cfg.paths.staged_root` (+ `_url` variants, `{path, url}` form accepted like `work_root`), `cfg.paths.storage_root` (mia-data's, where bindings are read), `cfg.service.url: str`, `cfg.sourcing.parallel: int = 4`, `cfg.sourcing.tensorizer: str = "muvit-2d"`, `cfg.sourcing.val_size: int = 256`, `cfg.sourcing.test_size: int = 256`, `cfg.label(stage) -> "buffer:<stage>"`, `cfg.workflow_id(issue) -> "mia-buffers/<buffers_repo>/issue-<n>"`, `cfg.marker(text) -> "<!-- mia-buffers: <text> -->"`, `cfg.temporal.namespace` default `"mia-buffers"`, `cfg.deployment` default `"mia-buffers"`; `STAGES: dict[str, tuple[str, str]]`, `HUMAN_LABELS`; `load_config(path=None) -> Config` (env `MIA_BUFFERS_CONFIG`, default `PROJECT_DIR / "mia_buffers.yaml"`, env override `MIA_BUFFERS_TEMPORAL_VIA_PROXY`).
- Produces: `mia_data.config.PathsCfg.ROOTS: ClassVar[tuple[str, ...]] = ("work_root", "storage_root")` used by `_flatten` and `link`; `mia_data.config.load_config(path=None, *, cls=Config, env_var="MIA_DATA_CONFIG", default_name="mia_data.yaml", prog="mia-data", overrides=_ENV_OVERRIDES)`.
- Produces: `mia_ops.services.APPS["mia-buffers"] = App("mia_buffers", "MIA_BUFFERS_CONFIG", "MIA_BUFFERS_TEMPORAL_VIA_PROXY")`; `App.extra_mounts` for `mia_buffers`: `[("--ro", cfg.harness.dir), ("--ro", cfg.paths.storage_root), ("--rw", cfg.paths.raw_root), ("--rw", cfg.paths.staged_root)]`.

- [ ] **Step 1: Failing tests**

```python
# tests/test_buffers_spec.py
def test_ready_and_sourcing_defaults(tmp_path):
    spec = load_spec(write(tmp_path, MINIMAL))  # the file's existing minimal-spec helper
    assert spec.ready.min_fraction == 1.0 and spec.sourcing.dataset_timeout_days == 14

def test_ready_fraction_bounds(tmp_path):
    with pytest.raises(SpecError, match="min_fraction"):
        load_spec(write(tmp_path, MINIMAL + "\nready:\n  min_fraction: 0\n"))
```

```python
# tests/test_buffers_config.py
from mia_buffers.config import Config, STAGES, HUMAN_LABELS, load_config

def make_cfg(tmp_path, **overrides) -> Config:  # mirrors mia_helpers.make_cfg with buffers_repo, raw_root, staged_root, service
    ...

def test_labels_ids_and_markers(tmp_path):
    cfg = make_cfg(tmp_path)
    assert cfg.label("plan-review") == "buffer:plan-review"
    assert cfg.workflow_id(7) == "mia-buffers/o/buffers/issue-7"
    assert cfg.marker("x") == "<!-- mia-buffers: x -->"
    assert cfg.github.buffers_repo == "o/buffers" and cfg.github.data_repo == "o/buffers"
    assert cfg.deployment == "mia-buffers" and cfg.task_queue == "mia-buffers" and cfg.temporal.namespace == "mia-buffers"
    assert set(STAGES) == {"new", "planning", "plan-review", "sourcing", "ready", "idle", "retired", "needs-human"}
    assert set(HUMAN_LABELS) == {"go", "approve"}

def test_roots_accept_path_and_url_form(tmp_path):
    cfg = make_cfg(tmp_path, paths={"raw_root": {"path": str(tmp_path / "raw"), "url": "https://b/raw"}})
    assert cfg.paths.raw_root == (tmp_path / "raw").resolve() and cfg.paths.raw_root_url == "https://b/raw"
    assert cfg.paths.link(cfg.paths.raw_root / "r 1") == "https://b/raw/r%201"
    assert cfg.sourcing.parallel == 4 and cfg.sourcing.tensorizer == "muvit-2d"

def test_data_repo_key_is_refused(tmp_path):
    with pytest.raises(ValidationError):
        make_cfg(tmp_path, github={"data_repo": "o/x"})  # only buffers_repo names the issue repo

def test_load_config_reads_env_path(tmp_path, monkeypatch):  # writes a YAML, sets MIA_BUFFERS_CONFIG, asserts cfg.service.url
```

```python
# tests/test_mia_ops_services.py
def test_mia_buffers_app_mounts(tmp_path):  # a Config via test_buffers_config.make_cfg → extra_mounts has the four entries, rw for raw/staged
```

- [ ] **Step 2: Run them**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_spec.py tests/test_buffers_config.py tests/test_mia_ops_services.py -q 2>&1 | tail -5`
Expected: FAIL (`ImportError: mia_buffers.config`, `AttributeError: ready`)

- [ ] **Step 3: Implement**

`spec.py`: `class Ready(_Strict): min_fraction: float = Field(default=1.0, gt=0, le=1)`; `class Sourcing(_Strict): dataset_timeout_days: float = Field(default=14, gt=0)`; add both to `Spec` with defaults. Update the module docstring's "grammar" sentence.

`mia_data/config.py`: `ROOTS` class attribute; `_flatten` and `link` iterate `cls.ROOTS` / `self.ROOTS` (`getattr(self, root)`, `getattr(self, f"{root}_url")`); `load_config` keyword-parametrised, same behaviour for mia-data.

`mia_buffers/config.py`: `GitHubCfg(mia_data.config.GitHubCfg)` with `data_repo: str = Field(alias="buffers_repo")` and a `buffers_repo` property; `PathsCfg(mia_data.config.PathsCfg)` with `ROOTS = ("work_root", "storage_root", "raw_root", "staged_root")`, the four new fields; `ServiceCfg`, `SourcingCfg` (validate `parallel >= 1`, sizes `>= 1`); `Config(mia_data.config.Config)` overriding `deployment`, `github`, `paths`, `temporal: TemporalCfg = TemporalCfg(namespace="mia-buffers")`, adding `service`, `sourcing`, and the three name methods. `_resolve_paths` is inherited; it resolves every `Path` in `paths`, so the new roots are covered. `load_config` calls `mia_data.config.load_config(path, cls=Config, env_var="MIA_BUFFERS_CONFIG", default_name="mia_buffers.yaml", prog="mia-buffers", overrides={"TEMPORAL_ADDRESS": ..., "MIA_BUFFERS_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy")})`.

`mia_buffers.yaml.example`: every section with placeholders (`/shared/TBD/...`, `JaneliaSciComp/mia-buffers`, `service: {url: http://buffer-api.example.org:8765}`), one comment per non-obvious key: `storage_root` is mia-data's (bindings), `raw_root` may be mia-data's `<work_root>/_cache` to share downloads.

`mia_ops`: `Literal["autodev", "mia-data", "mia-buffers"]`; `APPS` entry; `extra_mounts` branch for `mia_buffers`.

- [ ] **Step 4: Run the tests and the three checks**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_spec.py tests/test_buffers_config.py tests/test_mia_ops_services.py tests/test_mia_config.py -q 2>&1 | tail -3 && .pixi/envs/default/bin/ruff check . && .pixi/envs/default/bin/python -m mypy autodev mia_data mia_ops mia_buffers 2>&1 | tail -1`
Expected: all PASS; `All checks passed!`; `Success: no issues found`

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/spec.py mia_buffers/config.py mia_buffers.yaml.example .gitignore mia_data/config.py mia_ops tests/test_buffers_spec.py tests/test_buffers_config.py tests/test_mia_ops_services.py
git commit -m "mia-buffers: config (mia_buffers.yaml), ready/sourcing spec blocks, mia-buffers app kind"
```

---

### Task 2: Models, drop helpers, job spec

**Files:**
- Create: `mia_buffers/models.py`, `mia_buffers/drop.py`, `mia_buffers/jobspec.py`
- Modify: `mia_data/jobspec.py:77-137` (extract `archive_snapshot`), `:186-196` and `:241-294` (`HarnessPinned` protocol)
- Test: `tests/test_buffers_drop.py` (new), `tests/test_buffers_jobspec.py` (new)

**Interfaces:**
- Produces (`models.py`, all `@dataclass`, JSON-only types):
  - `BufferPins(spec_hash: str, catalog_ref: str, harness_commit: str, env_digest: str)`
  - `DatasetPlan(record: str, group: str, weight: float, manual: bool)`; `PlanResult(valid: bool, errors: list[str], pool: list[DatasetPlan], n_seen: int, min_fraction: float = 1.0, timeout_days: float = 14)`
  - `BindingInfo(found: bool, path: str | None = None, hash: str | None = None, hosts: list[str] = [], manual: list[list[str]] = [], errors: list[str] = [])` where `manual` is `[[url, incoming name], ...]`
  - `StageJobArgs(workspace: str, round_id: str, pins: BufferPins, record: str, hosts: list[str], levels: list[int], patch: int, out: str, voxel_size_nm: dict[str, float] | None = None)`
  - `StageResult(ok: bool, volumes: int = 0, errors: list[str] = [], bytes_fetched: int = 0, host_not_allowed: str | None = None)`
  - `DropResult(ok: bool, placed: list[str] = [], missing: list[str] = [], errors: list[str] = [])`
  - `RegisterResult(registered: list[str] = [], refused: dict[str, str] = {})`
  - `SourcingLimits(parallel: int, walltime_min: int, service_url: str)`
  - `DatasetStatus(record: str, group: str, manual: bool, status: str = "pending", volumes: int = 0, reason: str = "")` with statuses `pending | waiting-drop | staging | servable | failed`
  - `BufferState(issue: int, schema: int = 1, handled: list[str] = [], round_id: str | None = None, stage: str = "new", pins: BufferPins | None = None, buffer_id: str | None = None, planned_at: str | None = None, datasets: list[DatasetStatus] = [], ready: bool = False)`
  - `job_key(issue: int, round_id: str, record: str) -> str` as fixed above; matches `autodev.jobs.JOB_KEY_RE`.
- Produces (`drop.py`): `drop_names(urls: list[str]) -> list[tuple[str, str]]` (name = unquoted last path segment of the URL when non-empty and unique among the URLs, else `file-<index>`; a `::member` suffix is stripped from the URL first, the name comes from the file, not the member); `cache_key(url: str, validators: dict) -> str` (`sha256(f"{url}\n{json.dumps(validators, sort_keys=True)}")`, identical to the harness `_key`); `manifest_path(cache_dir: Path) -> Path` (`files.json`); `read_manifest(cache_dir) -> dict`; `merge_manifest(cache_dir, entries: dict[str, dict]) -> None` (atomic write).
- Produces (`jobspec.py`): `levels_key(patch: int, levels: list[int]) -> str`; `catalog_snapshot(cfg: Config, ref: str, tokens) -> Path` (`work_root/_catalog/<ref>` holding `datasets/`, read-only, from a mirror at `state_dir/mirror/catalog.git`); `stage_job(cfg: Config, args: StageJobArgs, snapshot: Path) -> JobSpec` with body `harness stage binding.yaml --snippets <snapshot> --record record.yaml --levels 1,2,4 --patch 256 --out <args.out> [--voxel-size-nm z,y,x]`, `allow_hosts == args.hosts`, `rw_mounts` containing `raw_root/<record>` and `args.out`, env `MIA_HARNESS_CACHE_DIR=raw_root/<record>`, `MIA_HARNESS_CACHE_WRITABLE=1`, `ro_mounts` containing the snapshot.
- Produces (`mia_data/jobspec.py`): `archive_snapshot(cfg: Config, *, mirror: str, repo: str, git_url: str | None, sha: str, paths: list[str], dest: Path, tokens, strip: int = 0) -> Path` doing what `snapshot` does today (lock, mirror sync when the sha is missing, `git archive`, read-only, rename); `snapshot` becomes a call to it. `class HarnessPinned(Protocol): harness_commit: str; env_digest: str`; `assert_pin(cfg, pins: HarnessPinned)`, `_spec(..., pins: HarnessPinned, ...)`.

- [ ] **Step 1: Failing tests**

```python
# tests/test_buffers_drop.py
def test_drop_names_prefer_unique_basenames():
    urls = ["https://h/a/x.zip::m.png", "https://drive.google.com/uc?export=download&id=1", "manual://rec/y.zip"]
    assert drop_names(urls) == [("https://h/a/x.zip", "x.zip"), ("https://drive.google.com/uc?export=download&id=1", "file-1"), ("manual://rec/y.zip", "y.zip")]

def test_drop_names_fall_back_on_collision():
    assert [n for _, n in drop_names(["https://h/a/f.tif", "https://h/b/f.tif"])] == ["file-0", "file-1"]

def test_cache_key_matches_harness_formula():
    assert cache_key("u", {"size": 1, "sha256": "ab"}) == hashlib.sha256(b'u\n{"sha256": "ab", "size": 1}').hexdigest()

def test_manifest_merge_is_atomic_and_additive(tmp_path):  # merge twice, read back both entries, no .tmp left
```

```python
# tests/test_buffers_jobspec.py
def test_levels_key():
    assert levels_key(256, [1, 2, 4]) == "p256-l1-2-4"

def test_stage_job_mounts_cache_and_out_rw_and_allows_hosts(tmp_path):
    cfg = make_cfg(tmp_path)  # from test_buffers_config; harness.dir gets a fake pixi.lock and git repo? no: monkeypatch harness_snapshot/assert_pin
    ...  # write workspace/binding.yaml + record.yaml; args with hosts ["zenodo.org"], out = cfg.paths.staged_root/"p256-l1-2-4"/"rec"
    spec = stage_job(cfg, args, tmp_path / "snap")
    assert "harness stage binding.yaml" in spec.command and "--levels 1,2,4 --patch 256" in spec.command
    assert spec.allow_hosts == ["zenodo.org"] and str(cfg.paths.raw_root / "rec") in spec.rw_mounts and str(args.out) in spec.rw_mounts
    assert spec.env["MIA_HARNESS_CACHE_DIR"] == str(cfg.paths.raw_root / "rec") and spec.env["MIA_HARNESS_CACHE_WRITABLE"] == "1"
    assert spec.name == "mia-buffers-stage"

def test_stage_job_without_hosts_is_offline(tmp_path):  # hosts [] (manual drop) → allow_hosts == [] (package hosts are not added)

def test_catalog_snapshot_archives_datasets_at_ref(tmp_path):  # a local git repo as the "mirror" source via git_url=file://..., two commits; snapshot at the first ref holds only that commit's datasets/*.yaml and is read-only

def test_job_key_is_valid_and_distinct():
    assert JOB_KEY_RE.fullmatch(job_key(7, "i7-e1", "a" * 121)) and job_key(7, "i7-e1", "x") != job_key(7, "i7-e1", "y")
```

- [ ] **Step 2: Run them**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_drop.py tests/test_buffers_jobspec.py -q 2>&1 | tail -5`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

`mia_data/jobspec.py`: extract `archive_snapshot`; keep `snapshot`'s behaviour byte-for-byte (its tests stay green). Add the `HarnessPinned` protocol and use it in `assert_pin` and `_spec`.

`mia_buffers/jobspec.py`: `stage_job` calls `mia_data.jobspec._spec(cfg, "stage", workspace, args.pins, body, args.hosts, [snapshot])` (hosts exactly as given: a manual-drop dataset runs with none), then appends the cache dir and `args.out` to `rw_mounts` and sets the two cache env vars. `# ponytail: download folded into the stage job (reviewed reader code, cache rw); a separate download job returns when limits.job_bytes streaming is needed`.

- [ ] **Step 4: Run tests and checks**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_drop.py tests/test_buffers_jobspec.py tests/test_mia_jobspec.py -q 2>&1 | tail -3 && .pixi/envs/default/bin/ruff check . && .pixi/envs/default/bin/python -m mypy autodev mia_data mia_ops mia_buffers 2>&1 | tail -1`
Expected: PASS, clean, clean

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/models.py mia_buffers/drop.py mia_buffers/jobspec.py mia_data/jobspec.py tests/test_buffers_drop.py tests/test_buffers_jobspec.py
git commit -m "mia-buffers: wire models, drop helpers, stage job spec and catalog snapshot"
```

---

### Task 3: Harness: `files.json` manifest, offline range reads, `manual://` (mia-snippets)

**Files (in the mia-snippets checkout, branch `training-buffers`):**
- Modify: `harness/src/mia_harness/cache.py`, `harness/src/mia_harness/io.py` (`_head`, `_read`, `fetch`), `SPEC.md` §6.6
- Test: `harness/tests/test_io.py`

**Interfaces:**
- Consumes: the manifest format from Task 2: `files.json` in the shared cache dir, `{url: {"validators": {...}, "source": "manual" | "download", "at": iso}}`; keys via `cache_key(url, validators)`.
- Produces: `DatasetCache.manifest_get(url) -> dict | None` (validators), `DatasetCache.manifest_put(url, validators, source)` (writable caches only; atomic merge); `IO._head` consults the manifest before HEAD and records a successful HEAD into it when writable; `IO._read` serves ranges from a cached whole file when the whole file is in the cache (no network); a `manual://` URL with no manifest entry raises `FileNotFoundError("<url>: not in the raw cache's files.json; a human has to drop this file (buffers spec §3)")`.

- [ ] **Step 1: Failing tests** (in `harness/tests/test_io.py`, using the existing `server` and `make_io` fixtures)

```python
def test_manifest_entry_skips_head_and_serves_from_cache(server, make_io, tmp_path):
    server.files["/d.bin"] = DATA
    io1 = make_io()
    io1.fetch(server.url("/d.bin"))                      # writes files.json beside the entry
    manifest = json.loads((tmp_path / "shared" / "files.json").read_text())
    assert manifest[server.url("/d.bin")]["source"] == "download"
    heads = len(server.heads())
    io2 = make_io(hosts=())                              # no host allowed: any request would raise HostNotAllowed
    assert io2.fetch(server.url("/d.bin")).read_bytes() == DATA and len(server.heads()) == heads

def test_range_reads_come_from_a_cached_whole_file(server, make_io):
    server.files["/d.bin"] = DATA
    io1 = make_io(); io1.fetch(server.url("/d.bin"))
    io2 = make_io(hosts=())
    f = io2.open(server.url("/d.bin")); f.seek(BLOCK + 3)
    assert f.read(10) == DATA[BLOCK + 3:BLOCK + 13] and io2.bytes_fetched == 0

def test_zip_member_from_a_dropped_archive_needs_no_network(make_io, tmp_path):
    data = zip_bytes({"m.tif": (b"x" * 100, zipfile.ZIP_STORED)})
    validators = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    url = "manual://rec/a.zip"
    key = hashlib.sha256(f"{url}\n{json.dumps(validators, sort_keys=True)}".encode()).hexdigest()
    (tmp_path / "shared").mkdir(); (tmp_path / "shared" / key).write_bytes(data)
    (tmp_path / "shared" / "files.json").write_text(json.dumps({url: {"validators": validators, "source": "manual"}}))
    io = make_io(hosts=())
    assert io.open(url + "::m.tif").read() == b"x" * 100

def test_manual_url_without_manifest_entry_says_so(make_io):
    with pytest.raises(FileNotFoundError, match="files.json"):
        make_io(hosts=()).fetch("manual://rec/a.zip")
```

(`server.heads()` may need adding to `rangeserver.py` alongside `gets()`; add it if missing.)

- [ ] **Step 2: Run them**

Run: `cd <harness.dir>/harness && .pixi/envs/default/bin/python -m pytest tests/test_io.py -q 2>&1 | tail -5`
Expected: 4 FAIL (`AttributeError`/`HostNotAllowed`)

- [ ] **Step 3: Implement**

`cache.py`: `manifest_get`/`manifest_put` over `shared / "files.json"` (and `private` for reads), merge-write via a temp file and `os.replace`. `io.py` `_head`: manifest first; else for `manual://` raise `FileNotFoundError` as specified; else HEAD, then `manifest_put(url, validators, "download")` when the cache is writable. `_read`: before the block loop, `if (local := self.cache.get(_key(url, self._head(url)))) is not None: return local.read_bytes()[start:end]` using `os.pread`-style slicing (open, seek, read `end - start`). `fetch` already hits the cache by key. SPEC.md §6.6: one paragraph on `files.json` and `manual://`.

- [ ] **Step 4: Run the harness suite and ci tests**

Run: `cd <harness.dir>/harness && .pixi/envs/default/bin/python -m pytest tests -q 2>&1 | tail -3 && cd .. && harness/.pixi/envs/default/bin/python -m pytest ci -q 2>&1 | tail -2`
Expected: all PASS (152 harness, 21 ci)

- [ ] **Step 5: Commit (mia-snippets)**

```bash
git -C <harness.dir> add harness/src/mia_harness/cache.py harness/src/mia_harness/io.py harness/tests SPEC.md
git -C <harness.dir> commit -m "harness: files.json manifest in the raw cache, offline range reads, manual:// drops"
```

---

### Task 4: Activities

**Files:**
- Create: `mia_buffers/activities.py`, `mia_buffers/status.py`
- Modify: `mia_buffers/register.py:66` (`volume_bodies(row, staged, spec, report: dict | None = None)`), `mia_data/status.py:22` (`render_stage(stage, detail="", prefix="data")`)
- Test: `tests/test_buffers_activities.py` (new), `tests/test_buffers_register.py` (one test for `report=`)

**Interfaces:**
- Consumes: Task 1 `Config`; Task 2 models, `drop.*`, `jobspec.*`; `mia_data.activities` helpers (`mia_permanent_errors`, `write_atomic`, `_job_json`, `_texts`, `_text`), `mia_data.jobspec.snapshot`, `harness_pin`, `harness_snapshot`, `check_hosts_allowed`, `HostsNotAllowed`, `PinMoved`; `mia_data.binding.hosts`, `missing_hosts`; `mia_data.catalog.dataset_folder`; `mia_buffers.cli.load_records`; `mia_buffers.plan.build_plan`, `_group_key`, `MANUAL_DROP_NOTE`; `mia_buffers.register.buffer_config`, `volume_bodies`.
- Produces: `class Activities(cfg: Config, gh_issues: GitHub, gh_snippets: GitHub, gh_catalog: GitHub, jobs: CsubJobs, bot: str, client: Client | None = None, probe: Callable[[], list[str]] | None = None)` with `all()`, `await_job`, `temporal`, and these activities (names as registered):
  - `admit(issue: int, event_id: str) -> BufferPins`: round dir `state_dir/rounds/<round>/`; the spec is the first ```` ```yaml ```` fence of the issue body (exactly one, else `ValueError("issue #n: expected exactly one ```yaml block holding the buffer spec, found k")`), written to `spec.yaml`, loaded with `load_spec` (`SpecError` → `ValueError(f"spec refused: ...")`); `catalog_snapshot` at `spec.catalog.ref`; harness pin and snapshot; `pins.json` last; a retry returns the stored pins.
  - `plan(issue: int, round_id: str, pins: BufferPins) -> PlanResult`: `build_plan(spec, pins.spec_hash, pins.catalog_ref, load_records(snapshot))`; appends `f"no tensorizer {name} in the harness checkout"` to the errors when `cfg.harness.dir / "tensorizers" / cfg.sourcing.tensorizer / "tensorizer.yaml"` is missing; writes `plan.json` and `plan.md` to the round dir and to `work_root/issue-<n>/<round>/` (the browsable copy); `errors[:20]`; pool rows → `DatasetPlan(row.id, _group_key(row, over[0]), row.weight, MANUAL_DROP_NOTE in row.notes)`; `min_fraction`, `timeout_days` from the spec.
  - `limits() -> SourcingLimits(cfg.sourcing.parallel, cfg.limits.job_walltime_min, cfg.service.url)`.
  - `set_stage(issue, stage, state: BufferState | None = None, detail: str = "", extra: str = "") -> None`: swap `buffer:*` label; within a round post once per `(round, stage)` with `render_stage(stage, detail, prefix="buffer")`, `extra`, and `render_links({"Workspace": link(work_root/issue-n/round), "Staged": link(staged_root/<levels key>)})`.
  - `comment(issue, body, marker) -> str | None`; `refuse_label(issue, label, reason, event_id)`; `comment_plan(issue, round_id) -> None` (marker `plan <round>`, body from `status.render_plan_comment(plan_dict, link)`: errors, warnings, `n in the pool of m seen`, a table of pool rows `id | modality | voxel nm | weight | splits | notes`, cut at 60,000 characters with a "truncated, full plan: <link>" line).
  - `read_binding(round_id: str, record: str) -> BindingInfo`: record YAML from the catalog snapshot (path from `plan.json`); `dataset_folder` (`ValueError` → `found=False` with the error); `storage_root/<folder>/binding.yaml` (absent → `found=False`); `yaml.safe_load`, `binding["record"] == record` else error; copy to `round dir/bindings/<record>.yaml`, sha256; `urls` = every `raw`/`labels` URL; `manual` when any URL is `manual://` or `missing_hosts(hosts(https urls, cfg), probe())` is non-empty: then `manual = drop_names(urls)` and `hosts = []`, else `hosts = hosts(urls, cfg)`.
  - `prepare_stage(issue: int, round_id: str, record: str) -> str`: workspace `work_root/issue-<n>/<round>/stage-<record>/` with `record.yaml` (from the snapshot) and `binding.yaml` (from the round dir); `mkdir -p raw_root/<record>` and `staged_root/<levels key>/<record>`; returns the workspace path.
  - `submit(key: str, args: StageJobArgs) -> str`: snippets snapshot at the binding's `snippets_commit` (read from `workspace/binding.yaml`), `check_hosts_allowed`, `jobs.submit(key, build)`; `CsubError` handling as mia-data's.
  - `await_job(job_id, workspace) -> JobDone` (mia_data.models.JobDone).
  - `read_stage(round_id: str, record: str) -> StageResult`: `read_untrusted(staged_root/<key>/<record>, "stage.json", 1 << 20)`; missing/malformed → `StageResult(False, errors=["stage.json is missing, unreadable or not a JSON object"])`; else `ok is True`, `len(volumes)`, `_texts(errors)`, `bytes_fetched`, `host_not_allowed.host`.
  - `drop_list(round_id, record) -> list[list[str]]` is not an activity: the workflow already has `BindingInfo.manual`.
  - `verify_drop(round_id: str, record: str) -> DropResult`: for each `(url, name)` in the binding's manual list: `incoming = raw_root/<record>/incoming/<name>`; `os.lstat` must be a regular file (not a symlink) with size > 0, else `missing`; sha256 streamed; `validators = {"size": size, "sha256": digest}`; `os.replace(incoming, raw_root/<record>/<cache_key(url, validators)>)`; `merge_manifest(raw_root/<record>, {url: {"validators": validators, "source": "manual", "at": now}})`; `ok` iff nothing missing. Files already in the manifest are skipped (a retry is a no-op).
  - `create_buffer(round_id: str, buffer_id: str) -> None`: `PUT {service.url}/buffers/<id>` with `buffer_config(plan, spec, tensorizer=cfg.sourcing.tensorizer, split_sizes={"val": ..., "test": ...})`; `raise_for_status`.
  - `register_dataset(round_id: str, record: str, buffer_id: str) -> RegisterResult`: row from `plan.json`, `report = _job_json(staged dir, "stage.json")`, `volume_bodies(row, staged_dir, spec, report=report)`, one `PUT volumes/<quoted id>` each; 200 → registered, else refused with the error text.
  - `poll() -> list[str]` → `mia_buffers.poll.run_poll(self)` (Task 5).
- Produces (`status.py`): `render_plan_comment(plan: dict, link: str | None) -> str`, `render_drop_comment(record: str, raw_dir_link: str | None, files: list[list[str]], missing: list[str] = []) -> str` ("Place these files under `raw_root/<record>/incoming/`: `name` ← url"), `render_ready(buffer_id: str, service_url: str, servable: list[str]) -> str` (the `BufferDataset` snippet from docs/development.md with the real id and URL).

- [ ] **Step 1: Failing tests** (`tests/test_buffers_activities.py`; `FakeGH` from `mia_helpers`, `make_cfg` from `test_buffers_config`; a fixture `acts(tmp_path)` building `Activities` with fake GitHub clients, `jobs=None`, `probe=lambda: ["zenodo.org", "*.zenodo.org"]`; `catalog_snapshot` monkeypatched to copy `tests/fixtures/buffers/catalog` into `work_root/_catalog/<ref>`; `harness_pin`/`harness_snapshot` monkeypatched)

```python
async def test_admit_writes_spec_and_pins(acts):  # issue body with one ```yaml fence of tests/fixtures/buffers/spec.yaml → pins.catalog_ref == spec ref, round dir has spec.yaml, pins.json; second call returns equal pins
async def test_admit_refuses_issue_without_one_yaml_fence(acts):  # body without a fence → ApplicationError non_retryable, message contains "```yaml"; two fences likewise
async def test_admit_refuses_invalid_spec(acts):  # fence with "schema_version: 2" → ApplicationError with "spec refused"
async def test_plan_writes_files_and_summarises(acts):  # after admit: result.valid, pool ids == the fixture's 2D pool, groups by modality, manual flag on the Drive record, plan.json + plan.md in both places, min_fraction 1.0
async def test_plan_without_tensorizer_is_invalid(acts):  # harness.dir has no tensorizers/muvit-2d → valid False, error names it
async def test_read_binding_found_hosts_and_copy(acts):  # binding at storage_root/<dataset_folder>/binding.yaml for the U937-like fixture record → found, hosts == ["zenodo.org"], manual == [], copy under round dir, hash
async def test_read_binding_manual_when_host_not_allowed(acts):  # urls on drive.google.com → manual == drop_names(urls), hosts == []
async def test_read_binding_missing(acts):  # no file → found False, errors ["no binding at ..."]
async def test_verify_drop_moves_files_and_writes_manifest(acts):  # binding with manual://rec/a.zip; write incoming/a.zip → ok, file at cache_key(...), files.json entry with validators {size, sha256}, incoming empty; second call ok (idempotent)
async def test_verify_drop_rejects_symlink_and_empty_file(acts):  # a symlink and a 0-byte file → ok False, both in missing, nothing moved, no files.json
async def test_read_stage_reduces_report(acts):  # stage.json ok with 2 volumes → StageResult(True, 2); malformed → ok False with the fixed message; host_not_allowed → field set
async def test_register_dataset_against_live_service(acts, server):  # `server` fixture from test_buffers_register; write_stage_json into staged_root/<key>/<record>; create_buffer then register_dataset → registered == ["<record>/v0.zarr"]
async def test_set_stage_swaps_label_and_comments_once(acts):  # FakeGH issue with buffer:planning → buffer:plan-review added, planning removed; two calls, one comment containing "buffer:plan-review"
def test_render_plan_comment_truncates(…):  # 2,000 pool rows → len(body) <= 61_000 and "truncated" in body
```

```python
# tests/test_buffers_register.py
def test_volume_bodies_accepts_a_report(spec_2d):  # report dict given, no stage.json on disk → same bodies as from the file
```

- [ ] **Step 2: Run them**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_activities.py tests/test_buffers_register.py -q 2>&1 | tail -5`
Expected: FAIL with `ModuleNotFoundError: mia_buffers.activities`

- [ ] **Step 3: Implement** `activities.py`, `status.py`, the `report=` parameter, the `prefix` parameter. Every activity that reads a job's file goes through `read_untrusted`; every text posted goes through `neutralize`. The `all()` list holds every activity but `await_job`.

- [ ] **Step 4: Run tests and checks**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_activities.py tests/test_buffers_register.py tests/test_mia_status.py -q 2>&1 | tail -3 && .pixi/envs/default/bin/ruff check . && .pixi/envs/default/bin/python -m mypy autodev mia_data mia_ops mia_buffers 2>&1 | tail -1`
Expected: PASS, clean, clean

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/activities.py mia_buffers/status.py mia_buffers/register.py mia_data/status.py tests/test_buffers_activities.py tests/test_buffers_register.py
git commit -m "mia-buffers: activities (admit, plan, bindings, stage job, manual drop, register) and comment rendering"
```

---

### Task 5: Workflows and the poller

**Files:**
- Create: `mia_buffers/workflows.py`, `mia_buffers/poll.py`
- Modify: `mia_data/poll.py:49-82` (`deliver_label` parameters)
- Test: `tests/test_buffers_workflows.py` (new), `tests/fixtures/buffers/buffer_history.json` (recorded)

**Interfaces:**
- Consumes: Task 2 models; activity names and signatures from Task 4; `mia_data.models.LabelEvent`, `JobDone`; `autodev.workflows.QUEUE_SLACK`, `RETRY_MAX_INTERVAL`, `error_message`.
- Produces: `mia_data.poll.deliver_label(activities, kind, label, issue, *, workflow: str = "MiaDataDataset", make_state=DatasetState, guard: Callable[[dict], str | None] | None = <the licence-excluded check>) -> bool` (mia-data's behaviour unchanged by default).
- Produces (`workflows.py`): `@workflow.defn(name="MiaBuffer") class MiaBuffer` with `@workflow.init __init__(state: BufferState)`, signals `go(LabelEvent)`, `approve(LabelEvent)`, `dropped(record: str)`, query `status() -> BufferState`, `run(state)`; `@workflow.defn(name="MiaBufferPoll") class MiaBufferPoll.run() -> list[str]` calling activity `poll` (5 min, 1 attempt). Constants `GO = "buffer:go"`, `APPROVE = "buffer:approve"`, `BUSY`, `NOT_REVIEW = "`buffer:approve` counts only while the issue is in `buffer:plan-review`, for the plan under review."`, `MAX_ROUNDS_PER_EXECUTION = 20`.
- Produces (`poll.py`): `run_poll(activities) -> list[str]`: for `go`/`approve` labels on open issues → `deliver_label(..., workflow="MiaBuffer", make_state=BufferState, guard=None)`; then for open issues labelled `buffer:sourcing` or `buffer:ready`: query `status`, and for each dataset in `waiting-drop` whose `raw_root/<record>/incoming/` holds at least one regular file, signal `dropped(record)` once per (issue, record, directory mtime) (`activities._dropped: dict`). Log lines `"go #n"`, `"dropped #n <record>"`.

Workflow behaviour (the plan's decisions; the implementer writes the code following `MiaDataDataset`):

- `run`: if `state.stage == "plan-review"` on entry (rolled over while waiting), resume `_from_review()`. Then the mia-data loop: wait for events or rollover; refuse stray approves when idle; pop a `go`; skip if seen; `_guard(_round(event))`; `continue_as_new` after 20 rounds or when suggested and idle.
- `_round(event)`: `round_id = f"i{issue}-e{event_id}"`; reset `pins, buffer_id, planned_at, datasets, ready`; `pins = admit` (300 s); `_stage("planning")`; `plan = act("plan", ..., seconds=1800)`; `comment_plan`; if `not plan.valid`: `_finish("needs-human", "the plan is invalid: " + "; ".join(errors))`, return; `state.datasets = [DatasetStatus(d.record, d.group, d.manual) for d in plan.pool]`; `self.plan = plan` is not durable: keep `min_fraction` and `timeout_days` in `BufferState`? No: add them to `BufferState` as `min_fraction: float = 1.0`, `timeout_days: float = 14` (wire-format: defaults). `state.planned_at = workflow.now().isoformat()`; `_stage("plan-review", "approve the plan with `buffer:approve`, or edit the spec and add `buffer:go` again")`; `_refuse_queued(approves=False)`; `_from_review()`.
- `_review()`: as mia-data's, with `planned_at` in place of the test's `finished_at`.
- `_sourcing()`: `_stage("sourcing")`; `buffer_id = round_id`; `limits = act("limits")`; `create_buffer(round_id, buffer_id)`; `semaphore = asyncio.Semaphore(limits.parallel)`; one `asyncio.create_task(self._source(dataset, semaphore, limits))` per dataset; then

  ```
  try:
      await workflow.wait_condition(lambda: self._ready_now() or all tasks done, timeout=timedelta(days=state.timeout_days))
  except asyncio.TimeoutError:
      if not self._ready_now(): cancel tasks; _finish("needs-human", f"after {days} days these groups are below ready.min_fraction: ..."); return
  if self._ready_now() and not state.ready: state.ready = True; _stage("ready", extra=render_ready(...)) via act("comment"...)  # the ready comment carries the BufferDataset snippet
  await asyncio.gather(*tasks)   # the rest join as they finish
  if not state.ready: _finish("needs-human", "no weighting group reached ready.min_fraction: " + per-dataset reasons)
  ```
  `# ponytail: no continue-as-new during sourcing (tasks live in this run); fine below ~2,000 datasets per buffer, after that persist per-dataset progress and resume`.
- `_ready_now()`: for every group, `servable / total >= state.min_fraction`.
- `_source(dataset, semaphore, limits)`: `binding = act("read_binding", round_id, record)`; not found or errors → `status = "failed"`, `reason`, comment once (marker `dataset <round> <record> failed`), return. If `binding.manual`: `status = "waiting-drop"`; `act("comment", issue, render_drop_comment(...), f"drop {round} {record}")`; loop: `await workflow.wait_condition(lambda: record in self.drops)`; `self.drops.discard(record)`; `result = act("verify_drop", ...)`; break when ok, else comment the missing names (marker `drop {round} {record} {attempt}`). Then `async with semaphore`: `status = "staging"`; `workspace = act("prepare_stage")`; `job = _job(record, workspace, binding.hosts, levels/patch from `limits`? no: from `BufferState`: add `levels: list[int] = []`, `patch: int = 0` set at `_round` from `PlanResult`; add those two fields to `PlanResult` too)`; `job.state not in ("DONE", "EXIT")` → failed `describe_failure`-style text; `stage = act("read_stage")`; not ok → failed with `"; ".join(errors)` (+ host not allowed); `reg = act("register_dataset")`; nothing registered → failed with the refusals; else `status = "servable"`, `volumes = len(reg.registered)`, comment once `"`<record>` is servable: n volumes"`, and if `state.ready` already: nothing else (the service renormalises weights).
- `_job(record, workspace, hosts, out)`: `submit(job_key(issue, round, record), StageJobArgs(...))` 600 s, then `await_job` on `<task queue>-await` with `walltime + QUEUE_SLACK`, heartbeat 3 min, as mia-data's `_job`.
- `_stage`, `_finish`, `_refuse_queued`, `_refuse`, `_seen`, `_act`, `_guard`: as mia-data's, over `BufferState`.

- [ ] **Step 1: Failing tests** (`tests/test_buffers_workflows.py`, structure copied from `test_mia_workflows.py`: `Mocks` with configurable `plan`, `bindings: dict[record, BindingInfo]`, `stage: dict[record, StageResult]`, `drops: dict[record, list[DropResult]]`, `states` for `await_job`; `started(env, m)` runs `MiaBuffer` with `BufferState(7)` and a first `go`; `idle(h, m, stage)`; `approve_review(h, m)` signals an approve newer than `planned_at` from the `status` query)

```python
PLAN = PlanResult(True, [], [DatasetPlan("u937", "confocal", 0.6, False), DatasetPlan("bcss", "histology-WSI", 0.4, True)], 10, levels=[1, 2, 4], patch=256)

async def test_go_plan_approve_sourcing_ready(env):  # bcss manual: after approve, u937 stages and registers; stages == [planning, plan-review, sourcing]; not ready (bcss group empty); signal dropped("bcss") → verify_drop ok → stage → register → ready; m.of("submit") has two keys; the ready comment contains "BufferDataset(" and the buffer id "i7-e11"
async def test_progressive_ready_with_lowered_fraction(env):  # min_fraction 0.5? no: fraction is per group. Use PLAN with both datasets in one group and min_fraction 0.5 → ready after the first servable; the second joins later; stages end with ready and the second dataset's "servable" comment exists
async def test_invalid_plan_is_needs_human(env):  # PlanResult(False, ["patch.c all: counts differ"], [], 10) → stages [planning, needs-human], comment_plan called, no create_buffer
async def test_redelivered_go_is_ignored(env):  # second go with the same event id while in plan-review → one admit
async def test_stale_approve_is_refused(env):  # approve dated before planned_at → refuse_label with "older"; still plan-review
async def test_go_while_sourcing_is_refused_busy(env):  # await gate in await_job; signal go → refuse_label BUSY
async def test_stage_report_not_ok_fails_dataset_without_registering(env):  # stage u937 ok False errors ["raw 0: voxel size unknown"] → dataset failed with that reason, register_dataset never called for it, and (single-dataset plan) final stage needs-human "no weighting group reached"
async def test_job_exit_failure_fails_dataset(env):  # states {"stage-u937": ("EXIT", 1)} → failed, reason mentions exit
async def test_missing_binding_fails_dataset(env):  # BindingInfo(found=False, errors=["no binding at ..."]) → failed, comment posted once
async def test_drop_with_missing_files_keeps_waiting(env):  # first verify_drop DropResult(False, missing=["a.zip"]) → comment with "a.zip", status waiting-drop; second dropped signal with ok → proceeds
async def test_timeout_without_ready_is_needs_human(env):  # manual dataset never dropped; time-skipping env advances 14 days → needs-human names "histology-WSI"
async def test_timeout_after_ready_changes_nothing(env):  # one group ready, the other waiting; after 14 days the stage stays ready (no needs-human)
async def test_rollover_in_plan_review_then_approve(env, monkeypatch):  # as mia-data's: _rollover_due True → continue_as_new in plan-review; approve on the new run → sourcing
async def test_poll_delivers_labels_and_drops(tmp_path):  # PollGH with buffer:go on #1 (writer), buffer:sourcing on #2 whose status query (FakeClient.query) says bcss waiting-drop and raw_root/bcss/incoming has a file → fc.started == [("MiaBuffer", BufferState(1), ...)], fc.signals == [(wid2, "dropped", "bcss")]; a second pass sends nothing new
async def test_replay_buffer_history(env):  # records with MIA_BUFFERS_RECORD_HISTORY=1 (the happy path), replays tests/fixtures/buffers/buffer_history.json
```

- [ ] **Step 2: Run them**

Run: `.pixi/envs/default/bin/python -m pytest tests/test_buffers_workflows.py -q -x 2>&1 | tail -5`
Expected: FAIL with `ModuleNotFoundError: mia_buffers.workflows`

- [ ] **Step 3: Implement** `workflows.py`, `poll.py`, the `deliver_label` parameters (mia-data's poll tests stay green), the extra `BufferState`/`PlanResult` fields (`min_fraction`, `timeout_days`, `levels`, `patch`), then record the history fixture:

Run: `MIA_BUFFERS_RECORD_HISTORY=1 .pixi/envs/default/bin/python -m pytest tests/test_buffers_workflows.py -k replay -q 2>&1 | tail -2`
Expected: PASS and `tests/fixtures/buffers/buffer_history.json` written (identities scrubbed)

- [ ] **Step 4: Run the whole suite and checks**

Run: `.pixi/envs/default/bin/python -m pytest -q 2>&1 | tail -3 && .pixi/envs/default/bin/ruff check . && .pixi/envs/default/bin/ruff format --check . && .pixi/envs/default/bin/python -m mypy autodev mia_data mia_ops mia_buffers 2>&1 | tail -1`
Expected: all PASS (≈ 446 + new), clean, clean, clean

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/workflows.py mia_buffers/poll.py mia_buffers/models.py mia_data/poll.py tests/test_buffers_workflows.py tests/fixtures/buffers/buffer_history.json
git commit -m "mia-buffers: MiaBuffer workflow (go → plan → approve → sourcing → ready), MiaBufferPoll, recorded history"
```

---

### Task 6: CLI, docs, spec v0.6

**Files:**
- Modify: `mia_buffers/cli.py` (`worker`, `schedule apply`, `init-labels`), `docs/development.md` (layout row; new section "Running a buffer from an issue (milestone 3)"), `AGENTS.md` (package line mentions the workflow), `notes/specs/2026-10-21-training-buffers.md`
- Test: `tests/test_buffers_cli.py`

**Interfaces:**
- Consumes: `autodev.cli._client`, `_setup_logging`; `autodev.auth.bot_login`; `mia_data.cli._github_clients` (works on the buffers `Config`: `data_repo` is the buffers repo); `autodev.jobs.CsubJobs`; Task 4 `Activities`; Task 5 workflows.
- Produces: `mia-buffers worker` (two Temporal workers: `cfg.task_queue` with `[MiaBuffer, MiaBufferPoll]` + `activities.all()`, `cfg.await_queue` with `await_job`), `mia-buffers schedule apply` (schedule `<deployment>-poll` running `MiaBufferPoll` every `temporal.poll_minutes`, `SKIP` overlap), `mia-buffers init-labels` (`STAGES` and `HUMAN_LABELS` as `buffer:<name>` with their colours), all taking `--config` (default: `load_config()`); `plan`, `serve`, `register` unchanged.

- [ ] **Step 1: Failing tests**

```python
def test_help_lists_commands(capsys):  # "worker", "schedule", "init-labels", "plan", "serve", "register" in --help
async def test_init_labels_creates_buffer_labels(tmp_path, monkeypatch):  # fake GitHub with ensure_label → the 10 names buffer:new … buffer:approve, each once
```

- [ ] **Step 2: Run** → Expected: FAIL (`init-labels` unknown)

- [ ] **Step 3: Implement** the three commands (`cmd_worker` is mia-data's with the buffers classes; keep it under 40 lines), then the docs:

`docs/development.md`: layout row adds "`MiaBuffer`/`MiaBufferPoll` (issue → plan → approval → sourcing → ready)"; new section after the milestone 2 walkthrough: the issue body format (title, one ```` ```yaml ```` fence), labels, what each stage posts, the manual drop (`raw_root/<record>/incoming/<name>`, the poller notices), where things land (`state_dir/rounds/<round>/`, `work_root/issue-n/<round>/`, `raw_root/<record>/files.json`, `staged_root/<levels key>/<record>/`), running it (`mia-buffers init-labels`, `serve`, `worker`, `schedule apply`; `mia.yaml` `app: mia-buffers`), and what milestone 3 leaves out (bindings are read from mia-data's `storage_root` and placed by hand until milestone 4; no tensorizer develop path; no idle/retire).

Spec → v0.6, dated today: status line; §1 note that `ready.min_fraction` and `sourcing.dataset_timeout_days` are spec fields; §3 "Download" paragraph: in milestone 3 download runs inside the stage job (reviewed reader code, cache read-write) and the manual drop works as built (`incoming/<name>` from `drop_names`, verified as a regular non-empty file, keyed and listed in `files.json`, which the harness reads instead of HEAD); §6.4 lists the new modules; §9.1 resolved: `JaneliaSciComp/mia-buffers`; §10 milestone 3 row annotated with the date and what was verified (Task 7 fills the numbers).

- [ ] **Step 4: Run tests and checks** → Expected: suite PASS, ruff/mypy clean, `git diff | grep -n 'rokickik\|/nrs/'` empty

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/cli.py docs/development.md AGENTS.md notes/specs/2026-10-21-training-buffers.md tests/test_buffers_cli.py
git commit -m "mia-buffers: worker, schedule and init-labels commands; milestone 3 docs; spec v0.6"
```

---

### Task 7: Real-data verification (U937 and BCSS) and the issue repo

Not TDD: a scripted run against the real catalog, raw cache and buffer-api, with its numbers recorded in the ledger and the spec's §10 row. Real paths stay in the scratchpad.

- [ ] **Step 1: Labels on the real repo.** Write the real `mia_buffers.yaml` (git-ignored) from the example: `buffers_repo: JaneliaSciComp/mia-buffers`, mia-data's `storage_root` and `harness.dir`, `raw_root` = mia-data's `<work_root>/_cache` (shares the U937 download), `staged_root` = `<work_root>/_buffers/staged`, `service.url: http://127.0.0.1:8765`, `github.auth: token` with `GITHUB_TOKEN` from `gh auth token` for this step only (the App installation does not cover the new repo yet). Run `mia-buffers init-labels`. Expected: ten `created buffer:*` lines; `gh label list -R JaneliaSciComp/mia-buffers` shows them.
- [ ] **Step 2: Bindings in place.** Compute `catalog.dataset_folder` for U937 and BCSS; copy the milestone 1 U937 binding to `<storage_root>/<folder>/binding.yaml`; write a BCSS binding with `manual://github-bcss-breast-cancer-semantic-segmentation/bcss-0.25mpp.zip::rgbs_colorNormalized/<file>.png` URLs for a png reader snippet name that does not exist yet (`png-pairs`) so the drop path is exercised up to the stage job. Expected: both files exist.
- [ ] **Step 3: Activities on real data (scratchpad script, root env, `-I`).** Build `Activities` with real GitHub clients (token) and `jobs=None`: `admit` on a body holding `notes/specs/buffer-2d-gray.example.yaml` (issue #1 created in the repo with that body: `gh issue create`), `plan` (expect the pool of 421 and `plan.md` in the round workspace), `read_binding` for U937 (`found`, hosts `zenodo.org`) and BCSS (`manual` list of names), `prepare_stage` for U937. Record: pool size, plan time.
- [ ] **Step 4: Stage job command, run locally.** `stage_job(cfg, args, snapshot)` → take `spec.command`'s `harness stage ...` line and run it under the harness env with the job's env vars (as milestone 1 did), out = `staged_root/p256-l1-2-4/<U937 record>`. Expected: `stage.json` ok, 1 volume, `files.json` now in the raw cache with the four U937 URLs as `download` entries (the harness wrote them on HEAD), bytes fetched 0 (cache hit). Record wall time.
- [ ] **Step 5: Offline re-run.** Same command with `MIA_HARNESS_ALLOWED_HOSTS=` empty. Expected: `stage.json` ok again (manifest served the validators, no HEAD).
- [ ] **Step 6: Register and consume.** `mia-buffers serve --state-dir <scratch>/state3` in the background; `create_buffer` + `register_dataset` through the activities → 1 volume registered; `consume.py` from milestone 2 against the new buffer id → 16 train samples, `status` shows the volume. Record samples/s.
- [ ] **Step 7: Manual drop on BCSS.** Drop a small zip (two real BCSS sample files from the record's `technical.sample.urls` if reachable, else a stub) into `raw_root/<bcss>/incoming/bcss-0.25mpp.zip`; `verify_drop` → ok, file keyed, `files.json` has a `manual` entry; `read_stage` after a stage attempt reports the missing `png-pairs` snippet (expected: the dataset fails for want of a reader snippet, which is milestone 4's `data:go`). Record it.
- [ ] **Step 8: Worker and poller (if a Temporal dev server is reachable).** Try `mia_ops` with `app: mia-buffers` only if `mia up` works for the existing `mia-data` service in this session; otherwise record "worker not run here: no Temporal server or csub broker on this host; the workflow is covered by the time-skipping tests and the recorded history". Do not install or start infrastructure beyond `mia up`.
- [ ] **Step 9: Ledger and spec row.** Write the numbers into the ledger and §10 milestone 3 row; `git diff | grep -n 'rokickik\|/nrs/'` empty; commit `notes/specs/...` as "mia-buffers: milestone 3 verification notes".

---

## Self-review

- Spec coverage: §1 lifecycle (new → planning → plan-review → sourcing → ready, needs-human; idle/retired are labels only): Tasks 1, 5. §3 binding (read from `storage_root`), download (inside the stage job), manual drop (`incoming/`, verify, `files.json`), staging, registration: Tasks 2–5. §5 `PUT buffers`, `PUT volumes`: Task 4. §6.4 `MiaBuffer`, `MiaBufferPoll`, activities: Tasks 4–5. §6.5 app kind `mia-buffers`: Task 1 (`buffer-api` as an app kind stays for milestone 5). §7 trust: `read_untrusted`, `neutralize`, reviewed code only in the stage job: Task 4. §10 row: Task 7.
- Type consistency: `BindingInfo.manual: list[list[str]]` everywhere (JSON has no tuples); `PlanResult.levels`/`patch` and `BufferState.levels`/`patch`/`min_fraction`/`timeout_days` added in Task 5 with defaults; `StageJobArgs.out` is the staged dir the activity computes (`prepare_stage` returns the workspace, the workflow builds `out` from `levels_key`? no: the workflow cannot know `staged_root`; `prepare_stage` returns `str` workspace and the workflow passes `out=""`; `submit` fills `out` from config). **Fix:** `StageJobArgs.out` is set by `submit` (activity) from `cfg.paths.staged_root / levels_key(args.patch, args.levels) / args.record`; the workflow leaves it `""`. Same for `read_stage` and `register_dataset`, which compute the dir themselves. Interfaces above read accordingly.
- Review Focus tests are placed: 1 → Task 4, 2 → Task 5, 3 → Task 4, 4 → Task 5, 5 → Task 5.
- Proportion: ~330 lines of plan for an estimated 1,300 lines of code and tests.
