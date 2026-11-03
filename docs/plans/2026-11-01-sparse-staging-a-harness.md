# Sparse OME-Zarr 0.6 staging, part A: the harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `harness stage` writes one sparse OME-Zarr 0.6 image per raw volume (only the chunks under planned
samples, 0.6 coordinate systems and dataset transforms, provenance under `attributes.mia`), the reader resolves
samples from the 0.6 metadata and refuses absent chunks, and a snippet-free `harness check-stage` validates the
result against the vendored 0.6 schemas and the plan.

**Architecture:** a new pure module `plan.py` turns a *rule* (seed, patch, levels, per-split sample counts, split
policy) and a volume's real shape into sample descriptors and a per-level stage plan (box origin, extent, chunk
set). `pyramid.py` writes from that plan and reads 0.5 and 0.6 alike, exposing index ↔ nm arithmetic from the
dataset transform. `provenance.py` is one pydantic model written to every group and serialized to `stage.json`.
`check.py` validates `zarr.json` files against vendored schemas, checks coverage, writes `mia.verification`, then
digests and rewrites `stage.json`. `stage.py` orchestrates; the geometry check reads fresh from the source.

**Tech Stack:** Python 3.12, numpy, zarr-python 3.4 (`zarr_format=3`, `config={"write_empty_chunks": True}`),
pydantic 2, `jsonschema` + `referencing` (new, conda-forge), pytest. Repo: mia-snippets, `harness/` (its own
`pixi run test`). Develop in a fresh clone (`~/dev/mia-snippets`, branch `sparse-staging`), **never on a branch of
the live `harness.dir` checkout**: the worker pins `git rev-parse HEAD` of that directory.

**Spec:** `docs/specs/2026-11-01-sparse-ome-zarr-0.6-staging.md` v0.2 (this repo): §2 (plan), §3 (geometry),
§4.1–4.3, §5 (provenance), §6 (verification), §7 (tests), §8 item 2 and 4. Part B (this repo's `mia_buffers`) is
`2026-11-01-sparse-staging-b-buffers.md` and consumes the file formats fixed here.

## Global Constraints

- Only released OME-Zarr 0.6 (ome/ngff-spec tag `0.6`): no RFC-8/RFC-3/RFC-9 constructs, no `scene`, no
  `projectAxis`; `version` only at `ome > version`; `attributes.ome.version == "0.6"`; Zarr v3.
- Dataset transform: exactly `{"type": "sequence", "input": {"path": "s<k>"}, "output": {"name": <cs>},
  "transformations": [scale, translation]}`; same `output.name` on every level; axes order `czyx` subset.
- Stored axis order is the harness canonical `czyx` subset; the raw array's order goes to `mia.geometry.source_axes`.
- Arrays are created with `write_empty_chunks=True`; chunks `(1, 1, patch, patch)` aligned to the box origin.
- Edge rule: clamp the start against the **raw volume's** level grid, never pad, never recompute in the box grid.
- Everything that reads voxels goes through `io`; the planned byte volume is computed before the first read.
- Labels: integer dtype (bool → uint8, float → error); same boxes and chunk sets as the raw.
- Artifacts are never written after the job: the harness writes the whole `artifact` block itself.
- Style: descriptive names, a plain-language line per module/class/non-obvious function, type hints, line length
  120, `# ponytail:` on deliberate shortcuts. `filterwarnings = error` in pytest.

## Review Focus

1. A 2D record (`yx` axes, no `z`) with one plane: `derive_samples` and the writer must work with no plane axis at
   all (Task 2 and 4 tests cover `zyx` and `yx`).
2. A label volume that is all background in some planned chunk: with `write_empty_chunks=True` the chunk exists;
   coverage must pass (Task 4 test `test_all_background_label_chunk_is_written`).
3. A legacy 0.5 artifact with an absent all-zero chunk: the reader must **not** raise `NotStaged` (Task 5 test
   `test_legacy_dense_pyramid_is_exempt_from_presence_check`).
4. A sample whose clamped start differs from the unclamped one (centre near the raw edge) at a coarse level: the
   stored `starts` and the reader's inverse must agree (Task 5 property test over edge centres).
5. A planned byte volume over the cap: `stage.json` says `ok: false` with the exact error and no voxel is read
   (Task 6 test asserts `io.bytes_fetched == 0` and no `v0.zarr` arrays).

---

### Task 1: Vendor the 0.6 schemas and add `jsonschema`

**Files:**
- Create: `harness/src/mia_harness/schemas/ngff-0.6/{axes,coordinate_systems,coordinate_transformations,image,label,ome,ome_zarr,bf2raw,plate,well,scene,_version}.schema` and the `strict_*.schema` files, plus `harness/src/mia_harness/schemas/ngff-0.6/COMMIT` (one line: the tag and commit sha of ome/ngff-spec `0.6`).
- Modify: `harness/pixi.toml` (add `jsonschema = "*"` under `[dependencies]`), `harness/pyproject.toml` (`[tool.setuptools.package-data] mia_harness = ["schemas/ngff-0.6/*"]`).
- Create: `harness/src/mia_harness/schemas/__init__.py` (empty), `harness/tests/test_schemas.py`.

**Interfaces:**
- Produces: `mia_harness.schemas.ngff.validator(name: str) -> jsonschema.Draft202012Validator` for `name` in `image`, `label`, `coordinate_systems`, `coordinate_transformations`, `axes`, `ome_zarr`, `strict_image`, `strict_label`, …; `mia_harness.schemas.ngff.SCHEMAS_COMMIT: str`; `mia_harness.schemas.ngff.errors(name, document) -> list[str]` (each `"<json pointer>: <message>"`).

- [ ] **Step 1: Download the tag and copy the schemas**

```bash
cd /tmp && curl -sSL -o ngff-0.6.tar.gz https://github.com/ome/ngff-spec/archive/refs/tags/0.6.tar.gz && tar xzf ngff-0.6.tar.gz
mkdir -p ~/dev/mia-snippets/harness/src/mia_harness/schemas/ngff-0.6
cp ngff-spec-0.6/schemas/*.schema ~/dev/mia-snippets/harness/src/mia_harness/schemas/ngff-0.6/
git ls-remote https://github.com/ome/ngff-spec refs/tags/0.6   # the sha goes into COMMIT as "0.6 <sha>"
```

- [ ] **Step 2: Write the failing test**

```python
# tests/test_schemas.py
import json
from mia_harness.schemas import ngff

def test_strict_example_validates_and_a_wrong_version_does_not():
    doc = {"ome": {"version": "0.6", "multiscales": [{"coordinateSystems": [{"name": "p", "axes": [
        {"name": "y", "type": "space", "unit": "nanometer"}, {"name": "x", "type": "space", "unit": "nanometer"}]}],
        "datasets": [{"path": "s0", "coordinateTransformations": [{"type": "scale", "scale": [1.0, 1.0],
        "input": {"path": "s0"}, "output": {"name": "p"}}]}]}]}}
    assert ngff.errors("image", doc) == []
    assert ngff.errors("image", {"ome": {**doc["ome"], "version": "0.5"}})
    assert len(ngff.SCHEMAS_COMMIT) == 40
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cd ~/dev/mia-snippets/harness && pixi install && pixi run python -m pytest tests/test_schemas.py -v`
Expected: FAIL with `ModuleNotFoundError: mia_harness.schemas.ngff`

- [ ] **Step 4: Implement `schemas/ngff.py`**

`validator(name)` loads `ngff-0.6/<name>.schema` and a `referencing.Registry` that resolves every
`https://ngff.openmicroscopy.org/0.6/schemas/<file>` to the vendored file (walk the directory once; cache
validators with `functools.lru_cache`). `errors(name, document)` = sorted `f"{'/'.join(map(str, e.absolute_path))}: {e.message}"` over `validator(name).iter_errors(document)`. `SCHEMAS_COMMIT` is read from `COMMIT`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `pixi run python -m pytest tests/test_schemas.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add harness/pixi.toml harness/pixi.lock harness/pyproject.toml harness/src/mia_harness/schemas harness/tests/test_schemas.py
git commit -m "harness: vendor the OME-Zarr 0.6 JSON schemas and a jsonschema validator"
```

---

### Task 2: `plan.py`: rule, sample derivation, capacity, overlap

**Files:**
- Create: `harness/src/mia_harness/plan.py`, `harness/tests/test_plan.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class PlanRule: seed: int; patch: int; levels: tuple[int, ...]; samples: dict[str, int]; split: dict; channel_recipe: dict | None` with `rule.digest() -> str` (sha256 hex of `json.dumps(asdict, sort_keys=True, separators=(",", ":"))`).
  - `@dataclass(frozen=True) class PlanInput: rule: PlanRule; artifact_id: str; levels_key: str` and `load_plan(path: Path) -> PlanInput` reading `plan-rule.json`: `{"artifact_id": ..., "levels_key": ..., "rule": {seed, patch, levels, samples, split, channel_recipe}}`.
  - `split_planes(volume_id: str, planes: int, split: dict) -> dict[str, list[int]]`: byte-for-byte the algorithm of `mia_buffers.register._split_planes` (part B keeps its copy; `tests/test_plan.py::test_split_planes_golden` pins outputs both repos assert).
  - `capacity(extent_y: int, extent_x: int, planes: int, patch: int, levels: tuple[int, ...]) -> int` = `(extent_y // (patch·max)) * (extent_x // (patch·max)) * planes`.
  - `derive_samples(rule: PlanRule, record: str, volume: int, axes: str, shape: tuple[int, ...], voxel_size_nm: dict[str, float]) -> dict[str, list[dict]]`: per split, descriptors `{"plane": {"z": int} | {}, "centre_nm": [y, x], "levels": [...], "patch": int, "starts": {"<L>": [y0, x0], ...}}`; `rng = random.Random(f"{rule.seed}:{record}:v{volume}:{split}")`; plane uniform over `split_planes(f"{record}/v{volume}", planes, rule.split)[split]`; centre as `mia_buffers.sampler.draw` (level-0 index in `[half, extent − half]`, `half = patch·max(levels)//2`, times voxel size, plus 0 translation); starts via `start_index(centre_nm, voxel_nm, factor, patch, extent)` (round then clamp, the formula of `tensorize._start` with `shift = (factor−1)/2·voxel`); count = `min(rule.samples[split], capacity)`; val/test reject overlap > `sample.OVERLAP_MAX` of the coarsest footprint with an earlier sample on the same plane, budget `8 × count` draws; drop exact duplicates in every split.
  - `start_index(centre_nm: float, voxel_nm: float, factor: int, patch: int, extent_raw: int) -> int`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_plan.py
import random
from mia_harness.plan import PlanRule, capacity, derive_samples, split_planes, start_index

RULE = PlanRule(seed=0, patch=64, levels=(1, 2, 4), samples={"train": 10, "val": 3, "test": 3},
                split={"policy": "fractions", "fractions": {"train": 0.6, "val": 0.2, "test": 0.2}, "seed": 0},
                channel_recipe=None)
VOX = {"z": 300.0, "y": 10.0, "x": 10.0}

def test_digest_is_stable_and_record_local():
    assert RULE.digest() == PlanRule(**{**RULE.__dict__}).digest()
    assert RULE.digest() != PlanRule(**{**RULE.__dict__, "seed": 1}).digest()

def test_split_planes_golden():   # part B asserts the same numbers in tests/test_buffers_register.py
    assert split_planes("r/v0", 10, RULE.split) == {"train": [0, 1, 3, 5, 6, 8], "val": [2, 9], "test": [4, 7]} or True  # replace `or True` with the real output after the first run; both repos must agree

def test_start_index_rounds_then_clamps():
    assert start_index(3000.0, 10.0, 1, 64, 600) == 268 and start_index(3000.0, 10.0, 2, 64, 600) == 118
    assert start_index(0.0, 10.0, 1, 64, 600) == 0 and start_index(1e9, 10.0, 4, 64, 600) == 150 - 64

def test_derive_is_deterministic_and_a_prefix():
    a = derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX)
    b = derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX)
    assert a == b and len(a["train"]) == 10 and set(a) == {"train", "val", "test"}
    more = derive_samples(PlanRule(**{**RULE.__dict__, "samples": {"train": 20, "val": 3, "test": 3}}), "r", 0, "zyx", (10, 600, 700), VOX)
    assert more["train"][:10] == a["train"]
    for s in a["train"]:
        assert set(s["starts"]) == {"1", "2", "4"} and s["plane"]["z"] in split_planes("r/v0", 10, RULE.split)["train"]

def test_capacity_caps_and_eval_rejects_overlap():
    assert capacity(600, 700, 10, 64, (1, 2, 4)) == 2 * 2 * 10
    tiny = derive_samples(RULE, "r", 0, "yx", (256, 256), {"y": 10.0, "x": 10.0})
    assert sum(len(v) for v in tiny.values()) == 1   # one plane, one position: one sample in one split
    evals = derive_samples(RULE, "r", 0, "zyx", (10, 600, 700), VOX)["val"]
    assert len({(s["plane"]["z"], tuple(s["starts"]["4"])) for s in evals}) == len(evals)

def test_2d_volume_has_no_plane_key():
    assert derive_samples(RULE, "r", 0, "yx", (600, 700), {"y": 10.0, "x": 10.0})["train"][0]["plane"] == {}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pixi run python -m pytest tests/test_plan.py -v`
Expected: FAIL with `ModuleNotFoundError: mia_harness.plan`

- [ ] **Step 3: Implement `plan.py`** with the signatures above. `derive_samples` draws from one `Random` per split so the list is a prefix; the overlap check uses `sample.overlap_frac({"raw": 0, "plane": z, "y": y0, "x": x0}, ..., patch)` on the coarsest level's starts.

- [ ] **Step 4: Run the tests; fix the golden numbers in `test_split_planes_golden` from the first run and remove `or True`**

Run: `pixi run python -m pytest tests/test_plan.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add harness/src/mia_harness/plan.py harness/tests/test_plan.py
git commit -m "harness: plan rule, deterministic sample derivation, capacity and eval overlap rule"
```

---

### Task 3: Stage plan per level and the byte budget

**Files:**
- Modify: `harness/src/mia_harness/plan.py`; Test: `harness/tests/test_plan.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class LevelPlan: factor: int; origin: dict[str, int]; extent: dict[str, int]; shape: tuple[int, ...]; planned_planes: tuple[int, ...]; chunks: frozenset[tuple[int, ...]]` (origin/extent keyed by spatial axis in raw index space; `shape` in level indices over the stored `czyx` subset; chunk tuples are chunk-grid indices over the stored axes, `c` always 0 for the raw's chunk set and expanded per channel by the writer).
  - `@dataclass(frozen=True) class StagePlan: levels: tuple[LevelPlan, ...]` and `stage_plan(samples: dict[str, list[dict]], axes: str, shape: tuple[int, ...], levels: tuple[int, ...], patch: int) -> StagePlan`.
  - `planned_bytes(plan: StagePlan, itemsize: int, channels: int) -> int` = Σ `len(chunks) × channels × patch² × itemsize`.

- [ ] **Step 1: Write the failing tests**

```python
def test_stage_plan_boxes_align_and_only_touched_chunks_are_planned():
    samples = {"train": [{"plane": {"z": 2}, "centre_nm": [0, 0], "levels": [1, 2, 4], "patch": 64,
                          "starts": {"1": [100, 200], "2": [50, 100], "4": [25, 50]}}]}
    plan = stage_plan(samples, "zyx", (10, 600, 700), (1, 2, 4), 64)
    l4 = plan.levels[2]
    assert l4.origin == {"z": 2, "y": 100, "x": 200} and l4.origin["y"] % 4 == 0 and l4.extent == {"z": 1, "y": 64 * 4, "x": 64 * 4}
    assert l4.shape == (1, 64, 64) and l4.planned_planes == (2,) and l4.chunks == {(0, 0, 0)}
    l1 = plan.levels[0]
    assert l1.origin == {"z": 2, "y": 100, "x": 200} and l1.chunks == {(0, 0, 0)}   # a chunk grid aligned to the box origin
    assert planned_bytes(plan, 2, 1) == 3 * 64 * 64 * 2

def test_unplanned_planes_between_planned_ones_have_no_chunks_and_overlaps_dedupe():
    s = lambda z, y0: {"plane": {"z": z}, "centre_nm": [0, 0], "levels": [1], "patch": 64, "starts": {"1": [y0, 0]}}
    plan = stage_plan({"train": [s(1, 0), s(4, 32), s(4, 40)]}, "zyx", (10, 600, 700), (1,), 64)
    l1 = plan.levels[0]
    assert l1.origin == {"z": 1, "y": 0, "x": 0} and l1.extent["z"] == 4 and l1.planned_planes == (1, 4)
    assert l1.chunks == {(0, 0, 0), (3, 0, 0), (3, 1, 0)}   # plane 4 is box index 3; y 32..104 touches chunk rows 0 and 1
```

- [ ] **Step 2: Run to verify they fail** (`ImportError: stage_plan`)

- [ ] **Step 3: Implement `stage_plan` and `planned_bytes`**: per level `L`, raw-space start of a sample = `starts[L]·L`, end = `(starts[L] + patch)·L`; origin = `min(start) // L * L`, extent = `max(end) − origin` (trimmed to whole blocks inside the raw shape); `shape` = `(c?, planes_in_box?, extent_y // L, extent_x // L)`; chunk index of a sample = `((starts[L] − origin/L) // patch .. ((starts[L] + patch − 1 − origin/L) // patch)` on y and x, plane index `z − origin_z`.

- [ ] **Step 4: Run the tests to verify they pass**

- [ ] **Step 5: Commit** (`git commit -m "harness: per-level stage plan (aligned boxes, chunk sets) and the planned byte count"`)

---

### Task 4: The 0.6 writer from a stage plan

**Files:**
- Modify: `harness/src/mia_harness/pyramid.py` (`write_pyramid`, `write_volume_groups`, `_axis_meta`, `OME_VERSION`); Test: `harness/tests/test_pyramid.py`

**Interfaces:**
- Produces:
  - `write_pyramid(array: Array, out: Path, *, plan: StagePlan, voxel_size_nm: dict[str, float], kind: Literal["raw", "label"], coordinate_system: str, mia: dict, downsampling_metadata: dict) -> PyramidInfo`. `LevelInfo` gains `origin: dict[str, int]`. The group's `attributes.ome` is `{"version": "0.6", "multiscales": [{"name": kind-or-label-key, "type": "mean"|"nearest", "metadata": downsampling_metadata, "coordinateSystems": [{"name": coordinate_system, "axes": [...]}], "datasets": [...]}]}` plus, for a label, `"image-label": {"source": {"image": "../../raw"}, "colors": [...]?, "properties": [...]?}` (colors only when `mia["segmentation_type"] == "binary"`: values 0 and 1); `attributes.mia = mia`.
  - Dataset transform per §3: `scale = [1 if c, vz, L·vy, L·vx]`, `translation = [0 if c, oz·vz, (oy + (L−1)/2)·vy, (ox + (L−1)/2)·vx]`.
  - `write_volume_groups(vdir: Path, labels: list[str], mia_summary: dict) -> None`: root `ome` with `version`, `multiscales` over `raw/s<k>` (coordinateSystems copied, `input.path` = `raw/s<k>`), `labels`; `labels/zarr.json` `{"ome": {"version": "0.6", "labels": [...]}}`; root `attributes.mia = mia_summary`.
  - Reads: `plane`-access arrays are read whole per plane and cut; other access reads each written chunk's raw region `[o + c·L, o + (c+patch)·L)`.

- [ ] **Step 1: Write the failing tests** (replace `test_writes_levels_scales_and_translation` and `test_label_levels_are_nearest`; keep `block_mean`/`stride` tests)

```python
def plan_for(shape, levels=(1, 2, 4), patch=64, centres=((3000.0, 3500.0),), z=(1,)):
    rule = PlanRule(0, patch, tuple(levels), {"train": 1}, {"policy": "holdout", "fractions": {"train": 1.0}, "seed": 0}, None)
    samples = {"train": [{"plane": {"z": zz}, "centre_nm": list(c), "levels": list(levels), "patch": patch,
                          "starts": {str(L): [start_index(c[0], 10.0, L, patch, shape[-2]), start_index(c[1], 10.0, L, patch, shape[-1])] for L in levels}}
                         for zz in z for c in centres]}
    return stage_plan(samples, "zyx", shape, tuple(levels), patch)

def test_writes_0_6_sparse_levels(tmp_path):
    arr, plan = volume(), plan_for((3, 600, 700))
    info = write_pyramid(arr, tmp_path / "raw", plan=plan, voxel_size_nm=VOX, kind="raw", coordinate_system="r.v0", mia={"schema": 1}, downsampling_metadata={"factors": [1, 2, 4]})
    g = zarr.open_group(tmp_path / "raw", mode="r")
    ome = g.attrs["ome"]
    assert ome["version"] == "0.6" and "version" not in ome["multiscales"][0]
    ms = ome["multiscales"][0]
    assert ms["coordinateSystems"] == [{"name": "r.v0", "axes": [{"name": a, "type": "space", "unit": "nanometer"} for a in "zyx"]}]
    o = info.levels[1].origin
    assert ms["datasets"][1]["coordinateTransformations"] == [{"type": "sequence", "input": {"path": "s1"}, "output": {"name": "r.v0"},
        "transformations": [{"type": "scale", "scale": [300.0, 20.0, 20.0]},
                            {"type": "translation", "translation": [o["z"] * 300.0, (o["y"] + 0.5) * 10.0, (o["x"] + 0.5) * 10.0]}]}]
    assert ms["type"] == "mean" and ms["metadata"] == {"factors": [1, 2, 4]}
    assert g["s0"].shape == (1, 64, 64) and g["s0"].chunks == (1, 64, 64)
    y0, x0 = info.levels[0].origin["y"], info.levels[0].origin["x"]
    np.testing.assert_array_equal(g["s0"][0], arr.data[1, y0:y0 + 64, x0:x0 + 64])
    y2, x2 = info.levels[2].origin["y"], info.levels[2].origin["x"]
    np.testing.assert_array_equal(g["s2"][0], block_mean(arr.data[1, y2:y2 + 256, x2:x2 + 256], 4))
    assert ngff.errors("image", {"ome": ome}) == [] and ngff.errors("strict_image", {"ome": ome}) == []

def test_unplanned_plane_chunks_are_absent_and_background_label_chunks_are_present(tmp_path):
    plan = plan_for((5, 600, 700), levels=(1,), z=(0, 4))
    write_pyramid(volume(z=5), tmp_path / "raw", plan=plan, voxel_size_nm=VOX, kind="raw", coordinate_system="r.v0", mia={}, downsampling_metadata={})
    assert (tmp_path / "raw" / "s0" / "c" / "0" / "0" / "0").is_file() and (tmp_path / "raw" / "s0" / "c" / "4" / "0" / "0").is_file()
    assert not (tmp_path / "raw" / "s0" / "c" / "2").exists()
    zeros = NpArray("zyx", np.zeros((5, 600, 700), dtype=np.uint8))
    write_pyramid(zeros, tmp_path / "lab", plan=plan, voxel_size_nm=VOX, kind="label", coordinate_system="r.v0", mia={"segmentation_type": "binary"}, downsampling_metadata={})
    assert (tmp_path / "lab" / "s0" / "c" / "0" / "0" / "0").is_file()   # write_empty_chunks=True
    lab = zarr.open_group(tmp_path / "lab", mode="r").attrs["ome"]
    assert lab["image-label"]["source"] == {"image": "../../raw"} and [c["label-value"] for c in lab["image-label"]["colors"]] == [0, 1]
    assert ngff.errors("label", {"ome": lab}) == []

def test_2d_yx_volume_writes_without_a_plane_axis(tmp_path):
    arr = NpArray("yx", np.arange(600 * 700, dtype=np.uint16).reshape(600, 700))
    samples = {"train": [{"plane": {}, "centre_nm": [3000.0, 3500.0], "levels": [1], "patch": 64, "starts": {"1": [268, 318]}}]}
    info = write_pyramid(arr, tmp_path / "yx", plan=stage_plan(samples, "yx", (600, 700), (1,), 64), voxel_size_nm={"y": 10.0, "x": 10.0}, kind="raw", coordinate_system="r.v0", mia={}, downsampling_metadata={})
    assert info.levels[0].shape == (64, 64) and zarr.open_group(tmp_path / "yx", mode="r")["s0"].chunks == (64, 64)

def test_bool_label_becomes_uint8_and_float_label_is_refused(tmp_path):
    plan = plan_for((3, 600, 700), levels=(1,))
    info = write_pyramid(NpArray("zyx", np.ones((3, 600, 700), dtype=bool)), tmp_path / "b", plan=plan, voxel_size_nm=VOX, kind="label", coordinate_system="r.v0", mia={}, downsampling_metadata={})
    assert info.dtype == "uint8"
    with pytest.raises(ValueError, match="integer"):
        write_pyramid(NpArray("zyx", np.ones((3, 600, 700), dtype=np.float32)), tmp_path / "f", plan=plan, voxel_size_nm=VOX, kind="label", coordinate_system="r.v0", mia={}, downsampling_metadata={})

def test_volume_root_is_one_0_6_image(tmp_path):
    ... write raw + one label as above into tmp_path / "v0.zarr" / {"raw", "labels/gt"}; write_volume_groups(tmp_path / "v0.zarr", ["gt"], {"schema": 1})
    root = json.loads((tmp_path / "v0.zarr" / "zarr.json").read_text())["attributes"]
    assert root["ome"]["labels"] == ["gt"] and [d["path"] for d in root["ome"]["multiscales"][0]["datasets"]] == ["raw/s0"]
    assert root["ome"]["multiscales"][0]["datasets"][0]["coordinateTransformations"][0]["input"] == {"path": "raw/s0"}
    assert ngff.errors("image", {"ome": root["ome"]}) == [] and root["mia"] == {"schema": 1}
```

- [ ] **Step 2: Run to verify they fail** (`TypeError: write_pyramid() got an unexpected keyword argument 'plan'`)

- [ ] **Step 3: Implement the writer**: `group.create_array(name, shape=level.shape, chunks=..., dtype=..., config={"write_empty_chunks": True})`; iterate `sorted(level.chunks)` grouped by plane; per plane and `access == "plane"` read the plane once (`array.read({z: plane, y: slice(None), x: slice(None)})`), else per chunk read `[o + c·L, o + (c+patch)·L)`; downsample with `block_mean`/`stride`; assign `arr[c_index, plane_index, cy*patch:(cy+1)*patch, cx*patch:(cx+1)*patch]`; label dtype rule; metadata per the interface. Delete the 0.5 branch of the writer entirely (the reader keeps 0.5).

- [ ] **Step 4: Run `tests/test_pyramid.py` to verify it passes**

- [ ] **Step 5: Commit** (`git commit -m "harness: write sparse OME-Zarr 0.6 pyramids from a stage plan"`)

---

### Task 5: The reader: 0.5 and 0.6, inverse mapping, `NotStaged`

**Files:**
- Modify: `harness/src/mia_harness/pyramid.py` (`Pyramid`, `LevelInfo`), `harness/src/mia_harness/tensorize.py` (`read_stack`, `_start`, `_level_index`); Tests: `harness/tests/test_pyramid.py`, `harness/tests/test_tensorize.py`

**Interfaces:**
- Produces:
  - `class NotStaged(LookupError)` with `.keys: list[str]` in `pyramid.py`.
  - `Pyramid.sparse: bool` (true when `attrs["mia"]` has `"plan"`), `Pyramid.raw_shape: dict[str, int]` (`mia.geometry.raw_shape` by axis, else the finest level's shape), `Pyramid.voxel_nm: dict[str, float]` (`mia.voxel_size[axis].nm` when present, else the finest level's scale).
  - `LevelInfo.origin: dict[str, int]` (raw index space; `{a: 0}` for dense 0.5 pyramids), `LevelInfo.factor` = `round(scale_nm["x"] / voxel_nm["x"])`.
  - `Pyramid.locate(level: int, centre_nm: tuple[float, float], patch: int, plane: dict) -> dict` → region `{"z": int, "y": slice, "x": slice}` in **array** indices: start in the raw level grid via `plan.start_index`, minus `origin[axis] // factor`; raises `NotStaged` if any index is outside the array or (when `sparse`) any touched chunk key is absent (`self._arrays[level].store.exists(key)` for keys from `arr.metadata.chunk_key_encoding.encode_chunk_key(chunk_coords)`).
  - `Pyramid.bbox_nm(level: int, region: dict, patch: int) -> np.ndarray` ([[y0, x0], [y1, x1]] from scale/translation, MuViT convention) and `Pyramid.raw_index_box(level: int, region: dict) -> dict[str, tuple[int, int]]` (`[o + i·L, o + (i+patch)·L)` per axis).
  - `tensorize.read_stack` uses `locate`; `LevelPatch.start_px` stays the **array** start (what `bbox_of` multiplies), so `bbox_of` is unchanged.

- [ ] **Step 1: Write the failing tests**

```python
# test_pyramid.py
def test_reads_0_5_pyramid_written_by_the_old_writer(tmp_path):
    # fixture: tests/fixtures/pyramid-0.5/ (copy a small raw/ group written by the pre-change writer; check it in)
    p = open_pyramid(FIXTURES / "pyramid-0.5" / "raw")
    assert not p.sparse and [lv.factor for lv in p.levels] == [1, 2, 4] and p.levels[1].origin == {"z": 0, "y": 0, "x": 0}

def test_legacy_dense_pyramid_is_exempt_from_presence_check(tmp_path):
    g = zarr.open_group(tmp_path / "d", mode="w", zarr_format=3)
    a = g.create_array("s0", shape=(1, 256, 256), chunks=(1, 64, 64), dtype="uint8")   # default write_empty_chunks=False
    a[0, :64, :64] = 7
    g.attrs["ome"] = {"version": "0.5", "multiscales": [{"axes": [...zyx nm...], "datasets": [{"path": "s0", "coordinateTransformations": [{"type": "scale", "scale": [1.0, 10.0, 10.0]}]}]}]}
    p = open_pyramid(tmp_path / "d")
    region = p.locate(0, (2000.0, 2000.0), 64, {"z": 0})   # an all-zero, never-written chunk
    assert p.read(0, region).sum() == 0

def test_sparse_locate_raises_not_staged_with_keys(staged_sparse):
    p = open_pyramid(staged_sparse / "raw")
    with pytest.raises(NotStaged) as e:
        p.locate(0, (5950.0, 6950.0), 64, {"z": 1})        # the far corner, never planned
    assert e.value.keys and all(k.startswith("c/") for k in e.value.keys)

def test_inverse_mapping_round_trips_over_edges(staged_sparse):
    p = open_pyramid(staged_sparse / "raw")
    for level in range(3):
        for centre in PLANNED_CENTRES + [(0.0, 0.0), (1e9, 1e9)]:    # PLANNED_CENTRES from the fixture's plan
            try:
                region = p.locate(level, centre, 64, {"z": 1})
            except NotStaged:
                continue
            box = p.raw_index_box(level, region)
            L = p.levels[level].factor
            assert (box["y"][1] - box["y"][0]) == 64 * L and box["y"][0] % L == 0
            np.testing.assert_allclose(p.bbox_nm(level, region, 64)[0], [(box["y"][0] + (L - 1) / 2) * 10.0, (box["x"][0] + (L - 1) / 2) * 10.0])
```

```python
# test_tensorize.py: the `staged` fixture now stages with a rule whose samples include D's centre; add
def test_stack_from_a_sparse_pyramid_matches_the_source(staged, source):   # `source`: the raw ndarray the fixture staged
    s = read_stack(staged, D)
    p = open_pyramid(staged / "raw")
    for i, lv in enumerate(s.levels):
        region = p.locate(i, tuple(D["centre_nm"]), D["patch"], D["plane"])
        box = p.raw_index_box(i, region)
        fresh = source[1, box["y"][0]:box["y"][1], box["x"][0]:box["x"][1]]
        np.testing.assert_array_equal(lv.img[0], block_mean(fresh, lv.factor))

def test_unplanned_descriptor_raises_not_staged(staged):
    with pytest.raises(NotStaged):
        read_stack(staged, {**D, "centre_nm": [5950.0, 6950.0]})
```

- [ ] **Step 2: Run to verify they fail**

- [ ] **Step 3: Implement** `Pyramid` parsing both forms (0.6: `multiscales[0].coordinateSystems[0].axes`, dataset `sequence`/`scale`/`identity` with `input`/`output`; 0.5: axes on the multiscale, bare list); `origin[axis] = round(translation/voxel − (L−1)/2)` for 0.6 (0 when the artifact is dense); `locate`, `bbox_nm`, `raw_index_box`, `NotStaged`; `read_stack` via `locate`.

- [ ] **Step 4: Run `tests/test_pyramid.py tests/test_tensorize.py`** to verify they pass

- [ ] **Step 5: Commit** (`git commit -m "harness: Pyramid reads 0.5 and 0.6, locates samples from the dataset transform, refuses absent chunks"`)

---

### Task 6: `provenance.py` and `stage()` from a plan with the budget

**Files:**
- Create: `harness/src/mia_harness/provenance.py`; Modify: `harness/src/mia_harness/stage.py`, `harness/src/mia_harness/cli.py`; Tests: `harness/tests/test_stage.py`, `harness/tests/test_provenance.py`

**Interfaces:**
- Produces (pydantic, `extra="forbid"`), in `provenance.py`:
  - `VoxelAxis(nm: float, source: Literal["record", "file", "assumed", "synthetic"], basis: str | None = None, range: tuple[float, float] | None = None)`
  - `Artifact(id, levels_key, binding_hash, plan_digest, harness_commit, env_digest, snippets_commit, record, record_commit)`
  - `SourceFile(url, member: str | None, validators: dict, snippet: dict, access: str)`
  - `LevelRecord(factor, path, origin: dict[str, int], extent: dict[str, int], shape: list[int], planned_planes: list[int], chunks_written: int, bytes_written: int)`
  - `Geometry(raw_shape: list[int], source_axes: str, axes: str, dtype: str, levels: list[LevelRecord])`
  - `Downsampling(raw: str = "mean", label: str = "nearest", edge: str = "clip", nearest_offset: str = "(L-1)//2")`
  - `Plan(digest: str, rule: dict, samples: dict[str, list[dict]])`
  - `Verification(checks: dict[str, Any] = {}, ok: bool | None = None, harness_commit: str | None = None, time: str | None = None, schemas_commit: str | None = None)`
  - `Provenance(schema: Literal[1] = 1, kind: Literal["raw", "label"], label_key: str | None, segmentation_type: str | None, artifact: Artifact, derived_from: list[SourceFile], geometry: Geometry, voxel_size: dict[str, VoxelAxis], downsampling: Downsampling, plan: Plan, statistics: dict | None, verification: Verification)`
  - `summary(volume_provenances: list[Provenance]) -> dict` (root `mia`: `artifact`, `derived_from`, `voxel_size`, `plan.digest`, `verification`).
  - `stage_json(ok: bool, errors: list[str], artifact: Artifact, volumes: list[VolumeSummary], bytes_fetched: int, digests: dict[str, str]) -> dict` where `VolumeSummary(path, axes, shape, dtype, voxel_size_nm, levels: list[dict] (factor, shape, scale_nm, translation_nm, origin, extent), labels, intensity_sample, samples)` keeps every key `mia_buffers.register.volume_bodies` reads today.
  - `stage(binding_path, snapshot, record_path, io, *, plan: PlanInput, voxel: dict[str, VoxelAxis], out: Path, seed: int = 0, stage_bytes: int | None = None) -> dict` in `stage.py` (replaces the `levels/patch/voxel_size_nm` keywords). `stage_bytes` defaults to `int(os.environ.get("MIA_HARNESS_STAGE_BYTES", io.byte_cap))`.
  - CLI: `harness stage BINDING --snippets S --record R --plan plan-rule.json --voxel-size voxel_size.json --out DIR [--seed N]`; `--levels/--patch/--voxel-size-nm` removed. `voxel_size.json` = `{"<axis>": {"nm": 8.0, "source": "assumed", "basis": "..."}}`; a file value wins and is recorded as `source: file`.
  - `intensity_sample(p, patch, rng)` samples from written level-0 chunks (`p.levels[0]` chunk set from `mia.geometry`), 8 chunks per channel.
  - Geometry check: for 8 planned samples per volume (and every level), `read_stack`-equivalent read vs. `array.read(raw_index_box)` block-meaned/strided; results in `Verification.checks["geometry"] = {"samples": n, "levels": n, "mismatches": [...]}`.

- [ ] **Step 1: Write the failing tests** (test constants: `plan_input(levels, patch, samples)` returns a
  `PlanInput` with `artifact_id="a" * 16`, `levels_key="p64-l1-2-4"`; `VOXEL = {"z": VoxelAxis(300.0, "record"),
  "y": VoxelAxis(8.0, "assumed", "test"), "x": VoxelAxis(8.0, "assumed", "test")}`)

```python
# test_provenance.py
def test_stage_json_keeps_the_registry_keys():
    sj = stage_json(True, [], ART, [VOL], 0, {"v0.zarr/raw/zarr.json": "ab"})
    assert set(sj["volumes"][0]) >= {"path", "axes", "shape", "dtype", "voxel_size_nm", "levels", "labels", "intensity_sample", "samples"}
    assert sj["artifact"]["plan_digest"] == ART.plan_digest and sj["zarr_json_sha256"] == {"v0.zarr/raw/zarr.json": "ab"}

# test_stage.py (rewrite the three tests; helper `plan_input(levels, patch, samples)` writes plan-rule.json)
def test_stage_writes_sparse_0_6_volume_with_provenance(tmp_path, io):
    b, r = write(tmp_path, (3, 600, 700), (3, 600, 700))
    rep = stage(b, SNAP, r, io, plan=plan_input((1, 2, 4), 64, {"train": 4, "val": 1, "test": 1}), voxel=VOXEL, out=tmp_path / "out")
    assert rep["ok"], rep["errors"]
    mia = zarr.open_group(tmp_path / "out" / "v0.zarr" / "raw", mode="r").attrs["mia"]
    assert mia["schema"] == 1 and mia["artifact"]["id"] == "a" * 16 and mia["artifact"]["plan_digest"] == plan_input(...).rule.digest()
    assert mia["derived_from"][0]["snippet"] == {"name": "npy-local", "commit": "1" * 40} and mia["derived_from"][0]["access"] == "chunk"
    assert mia["voxel_size"]["y"] == {"nm": 8.0, "source": "assumed", "basis": "test", "range": None}
    assert sum(len(v) for v in mia["plan"]["samples"].values()) == 6 and mia["geometry"]["raw_shape"] == [3, 600, 700]
    assert mia["verification"]["checks"]["geometry"]["mismatches"] == []
    lab = zarr.open_group(tmp_path / "out" / "v0.zarr" / "labels" / "gt", mode="r").attrs["mia"]
    assert lab["kind"] == "label" and lab["geometry"]["levels"] == mia["geometry"]["levels"]   # same boxes, same chunks
    sj = json.loads((tmp_path / "out" / "stage.json").read_text())
    assert sj["artifact"]["id"] == "a" * 16 and sj["volumes"][0]["samples"].keys() == {"train", "val", "test"}

def test_stage_refuses_a_plan_over_the_byte_cap(tmp_path, io):
    b, r = write(tmp_path, (3, 600, 700))
    rep = stage(b, SNAP, r, io, plan=plan_input((1, 2, 4), 64, {"train": 50}), voxel=VOXEL, out=tmp_path / "out", stage_bytes=1000)
    assert not rep["ok"] and rep["errors"][0].startswith("plan exceeds stage cap: ") and io.bytes_fetched == 0
    assert not (tmp_path / "out" / "v0.zarr" / "raw" / "s0").exists()

def test_stage_cli(tmp_path, monkeypatch):
    ... as today but with `--plan` and `--voxel-size` files; `--levels` now exits 2 (argparse)
```

- [ ] **Step 2: Run to verify they fail**

- [ ] **Step 3: Implement** `provenance.py`; in `stage.py`: open arrays → per raw: `derive_samples` → `stage_plan` → `planned_bytes` (raw + its labels) against `stage_bytes` → write raw and labels (same plan) → `write_volume_groups` → geometry check → `intensity_sample` → `Provenance` per group written to `attrs["mia"]` → `stage.json` via `stage_json` (digests left `{}` for `check-stage` to fill). `derived_from` from `binding.raw[i].urls`, `io.validators`, `io.cache.manifest_get(url)` (`sha256` when present), the snippet name and commit, `array.access`.

- [ ] **Step 4: Run `tests/test_stage.py tests/test_provenance.py tests/test_tensorize.py`** to verify they pass

- [ ] **Step 5: Commit** (`git commit -m "harness: stage from a plan rule with a byte budget; provenance under attributes.mia and stage.json from one model"`)

---

### Task 7: `harness check-stage`: schemas, coverage, digests, verification record

**Files:**
- Create: `harness/src/mia_harness/check.py`, `harness/tests/test_check.py`; Modify: `harness/src/mia_harness/cli.py`

**Interfaces:**
- Produces: `check_stage(out_dir: Path, *, harness_commit: str | None = None) -> dict` (loads no snippet code; imports only `pyramid`, `plan`, `provenance`, `schemas`): for every `zarr.json` under `out_dir`: `ngff.errors` for `image` (volume root and `raw`), `label` (each label), `ome_zarr`, and `strict_image`/`strict_label` **advisory**; coverage: every sample in `mia.plan.samples` of every group → `Pyramid.locate` must not raise; `stage.json`'s `artifact.id` equals each group's `mia.artifact.id`; then writes `mia.verification` (`checks = {"schemas": {...}, "strict": {...}, "coverage": {...}, "geometry": <from stage>}`, `ok`, `harness_commit`, `time`, `schemas_commit`) into every group's `zarr.json`, computes sha256 of each `zarr.json`, rewrites `stage.json` with `zarr_json_sha256` and `verification`; `ok = schemas ok and coverage ok and geometry ok and digests written`. Returns the report. CLI: `harness check-stage DIR` → exit 0 iff ok. `MIA_HARNESS_COMMIT` env is the default `harness_commit`.

- [ ] **Step 1: Write the failing tests**

```python
def test_check_stage_passes_a_good_artifact_and_records_verification(staged_out):
    rep = check_stage(staged_out, harness_commit="h" * 40)
    assert rep["ok"] and rep["checks"]["schemas"]["errors"] == [] and rep["checks"]["coverage"]["missing"] == []
    assert rep["checks"]["strict"]["label"]  # instance label fails strict_label (no colors): advisory, recorded
    mia = zarr.open_group(staged_out / "v0.zarr" / "raw", mode="r").attrs["mia"]
    assert mia["verification"]["ok"] and mia["verification"]["schemas_commit"] == ngff.SCHEMAS_COMMIT
    sj = json.loads((staged_out / "stage.json").read_text())
    assert sj["zarr_json_sha256"]["v0.zarr/raw/zarr.json"] == hashlib.sha256((staged_out / "v0.zarr/raw/zarr.json").read_bytes()).hexdigest()

def test_check_stage_fails_on_a_removed_chunk_and_on_bad_metadata(staged_out):
    next((staged_out / "v0.zarr/raw/s0/c").rglob("*")).unlink() if False else None
    chunk = next(p for p in (staged_out / "v0.zarr/raw/s0/c").rglob("*") if p.is_file()); chunk.unlink()
    rep = check_stage(staged_out)
    assert not rep["ok"] and rep["checks"]["coverage"]["missing"]
    z = staged_out / "v0.zarr/raw/zarr.json"; d = json.loads(z.read_text()); d["attributes"]["ome"]["version"] = "0.5"; z.write_text(json.dumps(d))
    assert not check_stage(staged_out)["ok"]

def test_check_stage_imports_no_snippet_code():
    source = inspect.getsource(mia_harness.check)
    assert not re.search(r"from \.(loader|run|io|fetch|stage) import|import mia_harness\.(loader|run|io|fetch|stage)", source)
```

- [ ] **Step 2: Run to verify they fail**

- [ ] **Step 3: Implement `check.py`** and the CLI subcommand; the import test reads `check.py`'s source and asserts no `from .loader`, `from .run`, `from .io`, `from .fetch` lines.

- [ ] **Step 4: Run the tests, then the whole suite** (`pixi run test`) to verify they pass

- [ ] **Step 5: Commit** (`git commit -m "harness: check-stage validates 0.6 schemas, coverage and digests without snippet code"`)

---

### Task 8: SPEC.md, README, the frictions note, final review

**Files:**
- Modify: `SPEC.md` §7 (the `stage` line and a `check-stage` line), §8 (a paragraph: a Stack comes from a sparse 0.6 artifact; `NotStaged`), `harness/README.md` if it lists commands.
- Create (this repo, not mia-snippets): `notes/ome-zarr-0.6-frictions.md` (local, untracked) with the entries of spec §8 item 4 as a bulleted list, one line each, plus anything Tasks 4–7 ran into.

- [ ] **Step 1: Update SPEC.md §7**

```
harness stage   <binding.yaml> --snippets <dir> --record <record.yaml> --plan plan-rule.json
                --voxel-size voxel_size.json --out DIR [--seed N]    # one sparse OME-Zarr 0.6 image per raw
                                                                      # (planned samples only), labels alongside,
                                                                      # provenance under attributes.mia, stage.json
harness check-stage DIR                                               # 0.6 schemas, coverage, digests; no snippet code
```

- [ ] **Step 2: Write `notes/ome-zarr-0.6-frictions.md` (local, untracked)** (this repo) from spec §8.4 plus findings.

- [ ] **Step 3: Run the full harness suite and the CI checker's tests**

Run: `cd ~/dev/mia-snippets/harness && pixi run test && pixi run ci-test`
Expected: all pass

- [ ] **Step 4: Commit and open the PR** (`git commit -m "docs: SPEC §7–8 for sparse 0.6 staging"`; `gh pr create` on `sparse-staging`, body states that old artifacts are not rewritten and that part B must deploy with no round in sourcing).
