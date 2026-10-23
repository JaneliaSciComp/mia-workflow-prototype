# mia-buffers milestone 0: the offline planner — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mia-buffers plan <spec.yaml> --catalog-dir <checkout>` turns a buffer spec and a catalog checkout into a
plan: one row per record with the rule that admitted or excluded it, the fit decision, weights, a split preview, and
either a valid pool or the errors that stop it.

**Architecture:** A new package `mia_buffers/` with four pure modules and a CLI. `spec.py` validates the YAML
(pydantic, unknown keys refused). `specmap.py` reduces one catalog record to the facts the planner uses, naming
every approximation. `select.py` applies `select`/`exclude`/`dedup`. `plan.py` applies fit, the channel and
dimensionality policies, the weighting formula and the split preview, and renders JSON and markdown. No network, no
Temporal, no GitHub: the catalog is a directory, pinned by comparing its `HEAD` to `catalog.ref`.

**Tech Stack:** Python 3.12, pydantic 2, PyYAML, argparse, pytest. Reuses `mia_data.catalog.FAMILIES`.

**Spec:** `notes/specs/2026-10-21-training-buffers.md` v0.4 (§1 planning row, §2 "where does dataset filtering
happen", §8, §10 milestone 0, §11). Buffer spec grammar: the catalog's `examples/spec-microscopy-pretrain.yaml`.

## Global Constraints

- `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` clean; no `type: ignore` or `noqa` without a reason.
- `mia_buffers` imports from `mia_data` and `autodev`, never the reverse.
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` empty.
- Descriptive names, one-line module docstrings, type hints on object parameters, line length 120, `# ponytail:` on deliberate shortcuts.
- Tests: one `tests/test_buffers_<module>.py` per module; fixtures under `tests/fixtures/buffers/`.
- The spec is untrusted text: `yaml.safe_load`, pydantic `extra="forbid"`, refused with the validator's message.
- Every record id in the plan comes from the catalog directory, never from the spec.

## Decisions the spec leaves open, fixed here

- `sampling.levels: list[int]`, default `[1]`, strictly ascending, first element 1. Levels scale y and x only.
- `sampling.patch.c`: `all` or a positive int. `all` requires one channel count across the admitted pool; an int
  requires every admitted array to have at least that many channels.
- One buffer serves one dimensionality: a pool mixing `imaging.dimensionality` values is an error.
- `label_axis` is accepted and ignored with a warning (v0.4 §8.5).
- Weighting group for `over: modality` is the record's first `imaging.modality` value (§2); the family is reported
  beside it.
- Measure for weighting: `positions = n_items × ⌊extent_y / (patch.y × L)⌋ × ⌊extent_x / (patch.x × L)⌋` summed over
  raw arrays, with `L = max(levels)`. Temperature `T`: sibling weights ∝ `positions ** T`, normalised per parent,
  applied down the `over` hierarchy; a dataset's weight is the product along its path.
- Split preview: keys = `n_items` per dataset (default split key is the volume); counts by largest remainder of
  `splits.fractions` in the spec's order; `holdout_ids` put the whole dataset in `test`.
- Fit order per record: technical arrays present with `axes` and `shape` → footprint fits (`patch.y × L ≤ extent_y`
  and `patch.x × L ≤ extent_x`, and `patch.z ≤ extent_z` for 3D) → voxel size known for y, x (and z for 3D) →
  `min_size_voxels` against the largest raw array. `shape_varies: true` admits with `fit.approximate = true`.
- Spec vocabulary mapped to the catalog's: `annotations.source` `auto-predict → automatic`; `annotations.types`
  `point → points`, `bbox → bounding-box`; `sample_type`, `annotations.encoding` (other than `any`) and
  `dedup.by: content_hash` are unsupported: the first two are errors, the third a warning with `doi` kept.

## Review Focus

1. A record without `technical`, or whose raw array has `shape: null` or `axes: null`, is excluded with rule
   `no array shape`, never a crash. (Task 4)
2. `license.spdx: unknown` is excluded when `allow_unknown: false` and admitted when `true`. (Task 3)
3. A raw array whose axes carry `s` (RGB samples) counts `s` as channels, and `t` or `z` as planes. (Task 2)
4. A spec with a misspelt key is refused and the message names the key. (Task 1)
5. A catalog directory whose `HEAD` differs from `catalog.ref` is refused unless `--allow-ref-mismatch`. (Task 5)

---

### Task 1: Spec models

**Files:**
- Create: `mia_buffers/__init__.py` (docstring only), `mia_buffers/spec.py`
- Create: `tests/fixtures/buffers/spec.yaml` (the catalog's `examples/spec-microscopy-pretrain.yaml` at
  `ab6a013d5da39c80651ddc845915d034b2eae82a`, with `catalog.ref` set to that sha and `levels: [1, 2, 4]` added under
  `sampling`)
- Test: `tests/test_buffers_spec.py`
- Modify: `pyproject.toml` (hatch `packages`, `typecheck` task, script `mia-buffers = "mia_buffers.cli:main"`)

**Interfaces:**
- Produces: `load_spec(path: Path) -> Spec` (raises `SpecError(str)` wrapping `yaml` and pydantic errors, message is
  the validator's); `spec_hash(path: Path) -> str` (sha256 hex of the file bytes); models `Spec`, `Select`, `Where`,
  `Sampling`, `Weighting`, `Splits`; `Filter = Literal["any"] | InFilter | ContainsFilter | RangeFilter` with
  `InFilter(in_: list[str])` (alias `in`), `ContainsFilter(contains: str)`, `RangeFilter(range: tuple[float, float])`;
  `Spec.patch_for(dims: str) -> dict[str, int]`; `Sampling.levels: list[int]`; `Sampling.patch.c: Literal["all"] | int`;
  `Spec.warnings: list[str]` (filled by a validator: `label_axis` present).

- [ ] **Step 1: Write the failing tests**

```python
def test_example_spec_loads():
    spec = load_spec(FIXTURES / "spec.yaml")
    assert spec.name == "microscopy-foundation-pretrain" and spec.sampling.levels == [1, 2, 4]
    assert spec.patch_for("2D") == {"y": 256, "x": 256} and spec.sampling.patch.c == "all"
    assert spec.select.where.license.in_ == ["CC0-1.0", "CC-BY-3.0", "CC-BY-4.0", "MIT", "BSD-3-Clause"]
    assert spec.sampling.weighting.temperature == 0.5 and spec.splits.fractions == {"train": 0.8, "val": 0.1, "test": 0.1}
    assert spec.warnings == ["sampling.label_axis is ignored: labels are a separate tensor (spec v0.4 §8.5)"]

@pytest.mark.parametrize("edit,needle", [
    ({"sampling": {"weigthing": {}}}, "weigthing"),
    ({"sampling": {"levels": [2, 4]}}, "levels"),
    ({"sampling": {"levels": [1, 1]}}, "levels"),
    ({"splits": {"fractions": {"train": 0.8, "val": 0.1}}}, "fractions"),
    ({"catalog": {"ref": "main"}}, "ref"),
    ({"select": {"where": {"sample_type": {"in": ["tissue"]}}}}, "sample_type"),
])
def test_refused(tmp_path, edit, needle):  # writes the fixture with `edit` merged in, expects SpecError naming `needle`

def test_spec_hash_is_file_sha256(): ...
```

- [ ] **Step 2: Run the tests, expect ImportError**

Run: `pixi run pytest tests/test_buffers_spec.py -v`

- [ ] **Step 3: Implement `mia_buffers/spec.py`**

Pydantic models with `model_config = ConfigDict(extra="forbid", populate_by_name=True)`. Validators: `catalog.ref`
matches `^[0-9a-f]{40}$`; `levels` ascending, unique, `levels[0] == 1`; `fractions` values sum to 1 within 1e-6;
`where.sample_type` and `where.annotations.encoding` must be `"any"` (message: `<field> is not in the catalog record
schema yet (spec v0.4 §8.1)`); `label_axis` appends the warning. `load_spec` catches `yaml.YAMLError` and
`ValidationError`, raises `SpecError(str(error))`.

- [ ] **Step 4: Run the tests, expect PASS; run `pixi run lint typecheck`**

- [ ] **Step 5: Commit** `mia-buffers: spec models`

### Task 2: Record facts

**Files:**
- Create: `mia_buffers/specmap.py`
- Create: `tests/fixtures/buffers/catalog/datasets/...` the four records below fetched at
  `ab6a013d5da39c80651ddc845915d034b2eae82a` with
  `gh api "repos/AI-HHMI/mia-agentic-search/contents/<path>?ref=<sha>" -H "Accept: application/vnd.github.raw"`:
  `2D/FIB-SEM/empiar-11037-cem-mitolab-mitochondria.yaml`, `2D/histology-WSI/github-bcss-breast-cancer-semantic-segmentation.yaml`,
  `2D/confocal/zenodo-20797339-u937-edu-h2ax-dapi-confocal.yaml`, `3D/FIB-SEM/janelia-hemibrain-v1-2-fib-sem-connectome.yaml`;
  plus two hand-written records: `2D/TEM/synthetic-em-2d-1ch.yaml` (1 channel, 4096×4096 uint8 tiff, voxel 4 nm,
  CC-BY-4.0, doi `10.1/dup`, n_items 10) and `2D/TEM/synthetic-em-2d-dup.yaml` (same doi, license `unknown`, no
  `technical` block). Also `tests/fixtures/buffers/catalog/.git-head` is **not** used: tests pass `ref=None`.
- Test: `tests/test_buffers_specmap.py`

**Interfaces:**
- Produces: `facts(path: str, record: dict) -> Facts` and dataclasses
  `ArrayFacts(role, axes, shape, shape_varies, extent_y, extent_x, extent_z, channels, planes, voxels, dtype, format, compression, encoding)`
  (`extent_*` are `int | None`; `channels` = shape at `c` or `s`, else 1; `planes` = product of shape at `t` and `z`,
  else 1), `Facts(id, path, dims, modalities, modality_group, family, organisms, license_spdx, annotations_present,
  annotation_types, annotation_source, label_encodings, raw_arrays, voxel_size_nm, n_items, size_bytes,
  download_method, download_host, doi)`; `Facts.largest_raw -> ArrayFacts | None`; `Facts.voxel_size_known(dims) -> bool`;
  `family_of(modality: str) -> str` using `mia_data.catalog.FAMILIES` (`"other"` when unmapped);
  `SPEC_TYPES = {"point": "points", "bbox": "bounding-box"}`, `SPEC_SOURCES = {"auto-predict": "automatic"}`.

- [ ] **Step 1: Write the failing tests**

```python
def test_bcss_samples_axis_is_channels():
    f = facts(*load("2D/histology-WSI/github-bcss-breast-cancer-semantic-segmentation.yaml"))
    raw = f.largest_raw
    assert (raw.extent_y, raw.extent_x, raw.channels, raw.planes) == (2467, 3394, 3, 1) and raw.shape_varies
    assert f.family == "lm" and f.modality_group == "histology-WSI" and f.download_method == "other"

def test_u937_t_axis_is_planes():
    raw = facts(*load("2D/confocal/zenodo-20797339-u937-edu-h2ax-dapi-confocal.yaml")).largest_raw
    assert (raw.channels, raw.planes, raw.extent_y, raw.extent_x) == (3, 25, 2048, 2048)

def test_hemibrain_is_3d_with_z_extent():
    f = facts(*load("3D/FIB-SEM/janelia-hemibrain-v1-2-fib-sem-connectome.yaml"))
    assert f.dims == "3D" and f.largest_raw.extent_z == 41394 and f.voxel_size_known("3D")

def test_mitolab_voxel_size_unknown():
    f = facts(*load("2D/FIB-SEM/empiar-11037-cem-mitolab-mitochondria.yaml"))
    assert not f.voxel_size_known("2D") and f.largest_raw.voxels == 224 * 224 and f.label_encodings == ["instance-ids"]

def test_record_without_technical():
    f = facts(*load("2D/TEM/synthetic-em-2d-dup.yaml"))
    assert f.raw_arrays == [] and f.largest_raw is None and f.license_spdx == "unknown"
```

- [ ] **Step 2: Run, expect failure**
- [ ] **Step 3: Fetch the four records, write the two synthetic ones, implement `specmap.py`**

`facts` never raises on a missing or null field: it uses `.get` with `None` defaults and skips arrays whose `axes` or
`shape` is null. `extent_y` is `shape[axes.index("y")]`. `download_host` is `urlparse(download_url).hostname`.

- [ ] **Step 4: Run, expect PASS; lint, typecheck**
- [ ] **Step 5: Commit** `mia-buffers: record facts from catalog records`

### Task 3: Selection

**Files:**
- Create: `mia_buffers/select.py`
- Test: `tests/test_buffers_select.py`

**Interfaces:**
- Consumes: `Facts` (Task 2), `Select`, `Where`, `Filter` (Task 1).
- Produces: `Decision(id: str, admitted: bool, rule: str)`; `decide_all(all_facts: list[Facts], select: Select) -> list[Decision]`
  in catalog path order; `matches(filter_: Filter, values: list[str]) -> bool` (`any` → True; `in` → intersection;
  `contains` → substring in any value; `range` → inclusive on floats). Rule strings, exactly:
  `admitted`, `path not in select.paths`, `excluded by path`, `excluded by id: <reason or "no reason given">`,
  `dims <v> not in <list>`, `modality <v...> not in <list>`, `organism ...`, `license <spdx> not in allowed`,
  `license unknown and allow_unknown is false`, `annotations required but absent`, `annotation types <v> not in <list>`,
  `annotation source <v> not in <list>`, `largest raw array has <n> voxels < min_size_voxels <m>`,
  `dedup: doi <doi> already kept by <id>`.

- [ ] **Step 1: Write the failing tests**

```python
def test_example_spec_over_fixture_catalog():
    decisions = {d.id: d for d in decide_all(fixture_facts(), spec.select)}
    assert decisions["github-bcss-breast-cancer-semantic-segmentation"].rule == "admitted"
    assert decisions["zenodo-20797339-u937-edu-h2ax-dapi-confocal"].rule == "annotation source unknown not in ['manual', 'proofread', 'automatic']"
    assert decisions["synthetic-em-2d-dup"].rule == "license unknown and allow_unknown is false"

def test_dedup_keeps_first_by_path(): ...  # synthetic-em-2d-1ch kept, a copy with another id under a later path → "dedup: doi 10.1/dup already kept by synthetic-em-2d-1ch"
def test_allow_unknown_license(): ...      # same facts, allow_unknown True → admitted
def test_exclude_by_id_with_reason(): ...  # rule "excluded by id: too small"
def test_min_size_voxels(): ...            # MitoLab 50176 < 1.0e6 → the exact rule string
```

Note the U937 case: the example spec's `annotations.source` filter drops a dataset the lead named. That is a fact
about the example spec, not a bug; the plan comment will show it, and the test pins it.

- [ ] **Step 2: Run, expect failure**
- [ ] **Step 3: Implement `select.py`**: one function per `where` field returning `str | None`, applied in the order of the rule list above; dedup last, over admitted records only, `keep: first` in path order.
- [ ] **Step 4: Run, expect PASS; lint, typecheck**
- [ ] **Step 5: Commit** `mia-buffers: select, exclude and dedup over record facts`

### Task 4: The plan

**Files:**
- Create: `mia_buffers/plan.py`
- Test: `tests/test_buffers_plan.py`

**Interfaces:**
- Consumes: `Spec` (Task 1), `Facts` (Task 2), `Decision`, `decide_all` (Task 3).
- Produces: `Fit(ok: bool, reason: str, footprint: tuple[int, int], approximate: bool)`; `fit(f: Facts, spec: Spec) -> Fit`;
  `positions(f: Facts, spec: Spec) -> int`; `weights(rows: list[Row], over: list[str], temperature: float) -> dict[str, float]`
  (dataset id → weight, sums to 1 over admitted-and-fit rows); `split_counts(n: int, fractions: dict[str, float]) -> dict[str, int]`;
  `Row(id, path, dims, modality_group, family, channels, admitted, rule, fit: Fit | None, positions: int, weight: float, splits: dict[str, int], notes: list[str])`;
  `Plan(spec_name, spec_hash, catalog_ref, rows, errors: list[str], warnings: list[str])` with `valid` property
  (`not errors`), `rows_by_id: dict[str, Row]`, `to_dict()`, `render_markdown() -> str`; `build_plan(spec: Spec, spec_hash: str, catalog_ref: str | None, records: list[tuple[str, dict]]) -> Plan`.
  Fit reasons, exactly: `no array shape`, `footprint <fy>×<fx> exceeds <ey>×<ex>`, `voxel size unknown`, `fits`,
  `fits (shape varies: checked against the representative item)`. Errors, exactly:
  `patch.c: all needs one channel count; pool has {1: [...], 3: [...]}`,
  `one buffer serves one dimensionality; pool has ['2D', '3D']: narrow select.where.dims`,
  `no dataset admitted`. Row notes: `download_method other: needs a manual drop (spec v0.4 §3)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_fit_rules():
    assert fit(F["empiar-11037-cem-mitolab-mitochondria"], spec).reason == "footprint 1024×1024 exceeds 224×224"
    assert fit(F["github-bcss-breast-cancer-semantic-segmentation"], spec) == Fit(True, "fits (shape varies: checked against the representative item)", (1024, 1024), True)
    assert fit(F["synthetic-em-2d-dup"], spec).reason == "no array shape"
    assert fit(F["zenodo-20797339-u937-edu-h2ax-dapi-confocal"], spec).reason == "fits"

def test_positions_and_weights():
    assert positions(F["zenodo-20797339-u937-edu-h2ax-dapi-confocal"], spec) == 100     # 25 × 2 × 2
    assert positions(F["github-bcss-breast-cancer-semantic-segmentation"], spec) == 906 # 151 × 2 × 3
    w = weights([row_u937, row_bcss], ["modality", "dataset"], 0.5)
    assert w == pytest.approx({"zenodo-...": 10 / 40.1, "github-...": 30.1 / 40.1}, rel=1e-3)
    assert weights([row_u937, row_bcss], ["dataset"], 0.0) == {"zenodo-...": 0.5, "github-...": 0.5}

def test_split_counts():
    assert split_counts(25, {"train": 0.8, "val": 0.1, "test": 0.1}) == {"train": 20, "val": 3, "test": 2}
    assert split_counts(151, {"train": 0.8, "val": 0.1, "test": 0.1}) == {"train": 121, "val": 15, "test": 15}

def test_example_spec_is_invalid_for_mixed_dims():  # spec as-is over the fixture catalog, with source filter widened to include unknown
    plan = build_plan(spec_with(where={"annotations": {"source": "any"}}), "h", None, records)
    assert "one buffer serves one dimensionality; pool has ['2D', '3D']: narrow select.where.dims" in plan.errors

def test_valid_2d_plan():  # dims {in: [2D]}, source any, exclude.ids [synthetic-em-2d-1ch]
    plan = build_plan(spec_2d, "h", None, records)
    assert plan.valid and {r.id for r in plan.rows if r.admitted and r.fit.ok} == {"github-...", "zenodo-..."}
    assert plan.rows_by_id["synthetic-em-2d-1ch"].rule == "excluded by id: one channel"

def test_channel_policy():  # same spec without the exclusion: 1-channel and 3-channel arrays in one pool
    assert "patch.c: all needs one channel count; pool has {1: ['synthetic-em-2d-1ch'], 3: ['github-...', 'zenodo-...']}" in build_plan(spec_2d_mixed, "h", None, records).errors
    assert build_plan(spec_with(spec_2d_mixed, patch_c=1), "h", None, records).valid

def test_mitolab_small_patch_still_needs_voxel_size():  # spec v0.4 §10 milestone 0 says patch 64 at levels 1,2 admits it; §8.2 wins
    assert fit(F["empiar-11037-cem-mitolab-mitochondria"], spec_with(spec, patch_2d={"y": 64, "x": 64}, levels=[1, 2])).reason == "voxel size unknown"

```python
def test_markdown_has_one_row_per_record_and_errors_first(): ...  # every fixture id appears once; errors section precedes the table when present
def test_record_without_technical_is_a_row_not_a_crash(): ...
```

- [ ] **Step 2: Run, expect failure**
- [ ] **Step 3: Implement `plan.py`**

`build_plan`: facts for every record (sorted by path) → `decide_all` → for admitted rows `fit`, `positions`, note on
`download_method` → channel and dimensionality checks over admitted-and-fit rows → `weights` → `split_counts` per row
(`holdout_ids` → all in `test`) → warnings from `spec.warnings` plus `dedup.by content_hash is not available; doi used`
when the spec lists it. Weights: group rows by the `over` path (`modality` → `modality_group`, `organism` → first
organism, `dims` → `dims`, `dataset` → id), compute per-level sibling weights with `positions ** T` (uniform when all
positions are 0), multiply down. Markdown: `## Errors` list if any, `## Warnings`, then a table
`id | decision | rule | dims | modality | channels | positions | weight | split preview`.

- [ ] **Step 4: Run, expect PASS; lint, typecheck**
- [ ] **Step 5: Commit** `mia-buffers: fit, weights, split preview and the plan document`

### Task 5: CLI and wiring

**Files:**
- Create: `mia_buffers/cli.py`
- Modify: `docs/development.md` (layout table row for `mia_buffers/`), `AGENTS.md` (first paragraph: three packages),
  `pyproject.toml` if anything remains from Task 1
- Test: `tests/test_buffers_cli.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `main(argv: list[str] | None = None) -> int`; subcommand `plan SPEC --catalog-dir DIR [--json PATH] [--allow-ref-mismatch]`.
  Exit codes: 0 valid plan, 2 plan with errors, 1 spec refused or catalog ref mismatch (message on stderr).
  `load_records(catalog_dir: Path) -> list[tuple[str, dict]]` (every `datasets/**/*.yaml`, path relative to the
  checkout, `yaml.safe_load`, sorted); `catalog_head(catalog_dir: Path) -> str | None` (`git -C DIR rev-parse HEAD`,
  `None` when not a git checkout).

- [ ] **Step 1: Write the failing tests**

```python
def test_plan_prints_markdown_and_writes_json(tmp_path, capsys):
    code = main(["plan", str(spec_2d_path), "--catalog-dir", str(FIXTURE_CATALOG), "--allow-ref-mismatch", "--json", str(tmp_path / "plan.json")])
    assert code == 0 and "| github-bcss-breast-cancer-semantic-segmentation |" in capsys.readouterr().out
    assert json.loads((tmp_path / "plan.json").read_text())["catalog_ref"] is None

def test_ref_mismatch_refused(tmp_path, capsys):  # a git repo in tmp_path with one commit as catalog dir; spec.catalog.ref is another sha → exit 1, stderr names both shas

def test_invalid_spec_exit_1(tmp_path, capsys):  # misspelt key → exit 1, stderr contains the key

def test_plan_errors_exit_2(): ...  # the example spec (mixed dims) → exit 2, "## Errors" in stdout
```

- [ ] **Step 2: Run, expect failure**
- [ ] **Step 3: Implement `cli.py`**; argparse like `mia_data/cli.py`; `pixi install` so the `mia-buffers` script exists
- [ ] **Step 4: Run the full suite: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test`; then `pixi run mia-buffers plan tests/fixtures/buffers/spec.yaml --catalog-dir tests/fixtures/buffers/catalog --allow-ref-mismatch` and read the table**
- [ ] **Step 5: Update `docs/development.md` and `AGENTS.md`; commit** `mia-buffers: plan command, docs`

### Task 6: Run it against the real catalog

Not a code task. Clone `AI-HHMI/mia-agentic-search` at `ab6a013d…` into the scratchpad, run `mia-buffers plan` with
the fixture spec and with a 2D-only, `source: any` variant, and paste the two summaries (counts per rule, the admitted
pool, the errors) into the final report. Rules that fire on many records are the first thing to show the lead.
