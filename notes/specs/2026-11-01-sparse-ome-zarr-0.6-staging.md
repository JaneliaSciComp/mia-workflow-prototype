# MIA staged artifacts: sparse OME-Zarr 0.6 images that carry their own provenance — Spec

**Status:** draft v0.2 · 2026-11-01 · not yet approved. v0.2 takes the review in [`2026-11-01-sparse-ome-zarr-0.6-staging-review.md`](2026-11-01-sparse-ome-zarr-0.6-staging-review.md): sample indices computed in the raw level grid (§2.2, §4.2), chunk presence enforced only for sparse artifacts (§4.2), verification against fresh source reads (§6), eval lists drawn by weight from the planned candidates (§4.4), recipe-addressed ids (§2.1), an overlap rule for eval samples (§2.5), no Temporal patch (§7–8). Amends the staging step of the training-buffers spec
([`2026-10-21-training-buffers.md`](2026-10-21-training-buffers.md) §3) and the artifact rules of the prep-and-buffering
spec ([`2026-10-25-prep-and-buffering.md`](2026-10-25-prep-and-buffering.md) §1.10, §5.2). It changes what a staged
artifact *is*; it does not change how datasets are prepared, fetched, locked, registered or served, except where §4
says so.
**Inputs:** the released OME-Zarr 0.6 specification, [ome/ngff-spec](https://github.com/ome/ngff-spec) tag `0.6`
(`index.md`, `schemas/`; version history dates it 2026-09-14); the current harness
(mia-snippets `harness/src/mia_harness/{pyramid,stage,tensorize,tensor_api,verify,io}.py`); the buffer sampler and
registry (`mia_buffers/{sampler,register,api,ledger,jobspec,activities}.py`); the crop provenance record
(`crop.json`, written by `mia_harness.run`).
**Vocabulary:** a **sample** is what a buffer serves: one multi-resolution stack at a planned centre, described by
a *descriptor* and turned into tensors by a tensorizer (whose output type is already `Sample`). A **crop** stays
what mia-data publishes for human review: the PNGs and `crop.json`. `intensity_sample` keeps its name; it is a
statistics sample, not a served one.
**Repos:** mia-snippets (harness writer, reader, verify), this repo (`mia_buffers/` planner, job spec, activities,
registry, client), the 0.6 schemas vendored into the harness.

## 0. Summary

A staged volume today is a whole-volume OME-Zarr 0.5 pyramid at every level of the recipe, with a loose `mia`
attribute; its fullest provenance lives in mia-data's review crops (`crop.json`), not in the Zarr. After this spec a staged
volume is an OME-Zarr **0.6** image that holds **only the chunks that cover planned samples** (an unaligned 256²
sample touches up to four spatial chunks per plane): per level, the bounding box of the planned samples, written
chunk by chunk where a sample lands and absent everywhere else. The image says in
spec-defined terms where it sits in the raw volume (dataset `scale` + `translation` into one named coordinate
system, the raw volume's physical frame) and says under `attributes.mia`, in a versioned schema, exactly which raw
bytes and which code it was derived from. `stage.json` stays, generated from the same in-memory model, as the
registry's index.

Two things make this possible, and both are new:

1. **A sample plan exists before staging.** Today buffer-api draws every centre at request time, uniformly over the
   whole volume (`mia_buffers.sampler.draw`); only val and test have frozen lists, and those are drawn lazily. A
   sparse artifact needs the samples first. §2 makes the plan a *rule* the planner fixes (seed, samples per split,
   patch, levels, recipe) and the stage job a *deterministic function* of that rule and the opened arrays' real
   shapes; every split, train included, is then served from a finite planned list.
2. **Geometry is arithmetic on metadata.** Array index ↔ raw physical nm ↔ raw index is computed from the 0.6
   dataset transform alone (§3). The tensorizer's `bbox` comes from it, never from `mia`.

## 1. Goals, non-goals, anti-goals

**Goals.** Stage only planned voxels; express each sample's place in the raw volume with 0.6 `coordinateSystems` and
dataset transforms that every conforming reader must parse; make the artifact self-describing (bytes, code, plan,
checks) so `crop.json`-grade provenance lives in the Zarr; keep artifacts immutable, content-identified and
build-then-rename; keep every voxel read behind `io`.

**Non-goals.** Viewer friendliness (no extra levels, no full planes); rewriting existing 0.5 artifacts (new inputs
give new artifact ids; old artifacts age out through the keeper); 3D (z-extended) patches, which the sampler does
not draw today; changing the sampler's weighting or split policy.

**Anti-goals.** Anything from an RFC not in released 0.6: no RFC-8 collections, nodes, `Reference`/`Path`
objects, prefixed `mia:` keys or extension registry; no RFC-3 axes; no RFC-9 zips. No `scene`, no `projectAxis`,
no `version` key anywhere but `ome > version`. No fill value ever handed to a model as data. No invented voxels
at edges.

## 2. The sample plan

### 2.1 What the planner fixes (the *rule*)

The planner (`mia-buffers plan`, worker activity `plan`) already decides per record: admission, fit, weight,
voxel size (with `assumed` axes), channel recipe, split policy. It gains, from a new spec block, the sample counts:

```yaml
sampling:
  plan:
    samples: {train: 1000, val: 20, test: 20}   # planned samples per volume per split, the same for every record
```

Per plan row the rule is: `seed` (spec `sampling.seed`), `patch`, `levels`, `channel_recipe`, the split policy
and `samples`. Every term is **record-local**: nothing in it depends on which other records are in the pool, so
two rounds with the same spec derive the same samples for a record and its artifact is reused (`_staged_before`
keeps working), and one record's artifact id never moves because another record joined or left. `plan_digest`
is the sha256 of the rule as canonical JSON. It does **not** include the samples themselves: they depend on the
volume's real shape, which the planner may only know approximately (catalog `shape_varies`). Note what the id is
and is not: `binding_hash` hashes the binding YAML (URLs, params), not the bytes behind the URLs, so artifact ids
are **recipe-addressed**, as they are today. The bytes actually read are pinned afterwards by `derived_from`
(validators, sha256 where known), and a changed upstream file shows up as a validator mismatch at stage time
(the raw cache keys include the validators). Content-addressing would need a pinned source manifest before the
job; it is an existing limitation, recorded in the frictions note, not something this spec fixes.

The dataset weights are **not** in the rule. The sampler applies them at draw time as it does today: pick a volume
by weight, then a sample uniformly from that volume's list. The served distribution follows the weights whatever
the list lengths are; `samples` only sets how many distinct samples each volume contributes. A volume with planes
in a split contributes exactly `samples[split]` for it (overlap allowed, as train overlap is unlimited today).
The planner prints the pool total and a byte estimate so the budget is visible at the plan gate.

> **Call (yours to confirm, §9 Q6):** train is served from a finite planned list, drawn with replacement. Today
> train centres are unbounded. A flat per-volume count was chosen over a pool-wide total apportioned by weight
> (which would put the whole pool into every artifact id) and over a density per position (record-local too, but
> an odd unit). A flat count covers a huge volume more thinly than a tiny one; switch to a density if that matters.

### 2.2 What the stage job derives (the *samples*)

Inside `harness stage`, after the arrays are open and before any voxel is read:

1. For each raw volume `i`, planes are split by the row's split policy exactly as `register._split_planes` does
   today (the code moves to a shared pure function so planner, job and registry agree).
2. For each split, `rng = Random(f"{seed}:{record}:v{i}:{split}")` draws `samples[split]` samples as a prefix of
   one record-local sequence (a later change of the count extends the list, never redraws it): a plane uniformly
   from the split's planes, a centre uniformly as `sampler.draw` does now (`half = patch·max(levels)/2`, centre
   in `[half, extent − half]` at level-0 index, converted to nm with the raw voxel size). The descriptor is
   `{plane: {z}, centre_nm: [y, x], levels, patch}`.
3. For each level `L` and each sample: the start index is computed **in the full level-L grid of the raw volume**
   (`_start` from `tensorize.py`: round, then clamp into the raw volume's level-L extent), so a sample never runs
   off the volume. The box-relative index is that start minus the box origin, subtracted afterwards. Python's
   `round` is ties-to-even and not invariant under integer shifts, so the start is never recomputed in the box's
   own grid; the descriptor's start indices per level are stored with it. **Edge rule: clip by clamping the start
   against the raw volume; never pad.** The fit rule (`patch·max(levels) ≤ extent`) guarantees a clamped sample
   still has `patch` voxels.

The descriptors are written into the artifact (`mia.plan.samples`, §5) and into `stage.json`; registration loads
them into the ledger's `frozen` table for every split, and `draw` picks an index instead of a centre (§4.4).

### 2.3 The stage plan per level

For level `L` (y and x are downsampled by `L`; the plane axis `z` and `c` are not, factor 1):

- **Box origin** `o_L` in raw index space: per spatial axis, the minimum sample start (in raw index space) across
  that level's samples, floored to a multiple of the axis factor. Level-L index `i` then covers raw indices
  `[o_L + i·L, o_L + (i+1)·L)` exactly.
- **Box shape**: from `o_L` to the maximum sample end, in level-L indices, trimmed to whole blocks inside the raw
  volume. On the plane axis the box runs from the lowest to the highest planned plane; planes in between that no
  sample uses are **absent chunks**.
- **Chunks**: `(1 for c, 1 for z, patch, patch)` as today, aligned to the box origin. Only chunks a planned sample
  intersects are written; a chunk two samples share is written once (chunk set = union; writes idempotent).
- **Budget**: planned bytes = Σ over every array of the record (all volumes, the raw with all its channels, every
  label) and every level of `chunks_written × chunk_bytes`, **uncompressed** (the compressed size on disk is
  smaller and is what the keeper measures afterwards). Computed before the first voxel is read; above the cap the
  job writes a `stage.json` with `ok: false` and the error `plan exceeds stage cap: <planned> > <cap> bytes` and
  reads nothing.
  > **Call (§9 Q5):** the cap is `limits.stage_bytes` in `mia.yaml`, default = `limits.job_bytes` (5 GB), passed
  > to the job as `MIA_HARNESS_STAGE_BYTES`. The planner also prints a per-dataset estimate (upper bound from the
  > catalog shape) in `plan.md`, so a human sees "exceeds cap" at the plan gate, and the worker surfaces the job's
  > refusal as dataset status `failed` with that reason in the sourcing status comment (existing mechanism).
- **Record** per level: factor, `origin` (raw index space), `extent` (raw index space, the box's span), `shape`
  (the array's shape, level-L indices: what the registry's `LevelBody.shape` has always been), `planned_planes`,
  `chunks_written` (count), `bytes_written`. The volume's `shape` in `stage.json` and `VolumeBody` stays the **raw**
  shape: the split policy, the plane weight and the fit check all read it and must agree with the planner.

### 2.4 Labels

> **Call (§9 Q2):** labels get the **same boxes and the same written chunks** as their raw volume. The tensorizer
> reads label samples at the raw's box, 0.6 requires a label's `datasets` to have as many levels as the image, and
> one coverage check then serves both. Full label planes would cost more and prove nothing.

Label arrays must be integer typed (0.6 `labels`). A boolean label is written as `uint8`; a floating label is
refused at stage with a clear error (no label today is one; if a snippet produces one, that is a snippet bug).
`colors`/`properties`: a `binary` label gets the two known values (0 background, 1 foreground); `semantic` and
`instance` labels get none, since the binding carries only `segmentation_type`. `strict_label` requires `colors`,
so instance labels fail it by design; §6 makes the `strict_*` results advisory.

### 2.5 When a volume cannot supply the requested samples

A draw never fails outright: centres may overlap, and the fit rule guarantees at least one position. What a small
volume cannot supply is **distinct** samples, and a frozen val or test list full of near-duplicates would
overstate the evaluation set. So each volume has a per-split **capacity**: the number of non-overlapping coarsest
tiles times the planes it has in that split (the planner's `positions` measure, per volume and per split). The
planned count is `min(samples[split], capacity)`; the cap is a count heuristic, not a diversity guarantee. For
**val and test** a candidate is also rejected when it shares more than `OVERLAP_MAX` (0.25, `mia_harness.sample`,
the rule the review crops already use) of its coarsest footprint with an earlier candidate of the same volume
and plane, and the sequence keeps drawing until the count is met or a draw budget (`8 × count`) is spent. Train
candidates may overlap freely, as train does today. A volume with no planes in a split contributes nothing to
it.

The planner applies the same rule to the catalog shape and reports it at the plan gate, in `plan.md`: per record,
`planned / requested` per split with the reason (`capacity`, `no planes in split`), and a pool total
`planned N of requested M`. A split that no record can supply at all is a plan error (the buffer would serve an
empty eval set), alongside today's `_policy_errors`. The stage job applies the rule to the real shape, so the
plan's numbers are an estimate and the artifact's `mia.plan.samples` are the truth; the two differ only where the
catalog shape was wrong, which `plan.md` already marks (`shape varies`). The capacity rule is derivation code
(pinned by the harness commit), not part of the rule hashed into `plan_digest`.

## 3. Geometry in 0.6 terms

Per staged multiscale group (raw, and each label):

- `attributes.ome.version = "0.6"`, Zarr v3, `zarr_format: 3`. `version` appears nowhere else.
- `multiscales[0].coordinateSystems`: exactly one, the **intrinsic** system, the raw volume's physical frame.
  > **Call (§9 Q3):** named `<record>.v<i>`, e.g. `empiar-11037.v0`, the same for the raw and every label of
  > that volume. Record ids are already `[A-Za-z0-9._-]`. (Your sketch said `raw0`; `v<i>` matches the volume
  > directory and reads as the volume's frame rather than the raw array's.)
  Axes as today: `{"name": "c", "type": "channel", "discrete": true}`, spatial
  `{"name": a, "type": "space", "unit": "nanometer"}`. Axis order is the harness's canonical `czyx` subset, which
  is the order 0.6 mandates (channel before space); the raw array's own order is recorded as `mia.geometry.source_axes`
  as today. Dimensionality equals the raw volume's.
- `datasets[k]`: `path: "s<k>"` for `k` in recipe order, largest first. The spec grammar requires recipes to
  start at 1 (`spec.py`), so `s0` is level 1 in every artifact this system writes; the reader still resolves
  levels by scale (§4.2) for linked pyramids. Its one transformation:

  ```json
  {"type": "sequence", "input": {"path": "s<k>"}, "output": {"name": "<record>.v<i>"},
   "transformations": [
     {"type": "scale", "scale": [1, vz, L·vy, L·vx]},
     {"type": "translation", "translation": [0, oz·vz, (oy + (L−1)/2)·vy, (ox + (L−1)/2)·vx]}]}
  ```

  `v` is the raw voxel size, `o` the level's box origin in raw index space, and `(L−1)/2` the 0.6 pixel-centre
  convention applied to block downsampling by `L` (the current writer's term, now plus the box offset). The
  plane axis has factor 1, so its term is plain `oz·vz`. This is the one form 0.6 allows in
  `multiscales > datasets` besides a lone `scale` or `identity`: `input` carries `path` only, `output` carries
  `name` only, same `output` name on every level.
- **Consequence relied on everywhere:** for level-L index `i` on a spatial axis, physical nm is
  `(o + i·L + (L−1)/2)·v`, raw index of the block is `[o + i·L, o + (i+1)·L)`, and the inverse is
  `i = ((nm / v) − o − (L−1)/2) / L`. The reader exposes `bbox_nm(sample)` and `raw_index_box(sample)` as these
  formulas, and `raw_index_box(bbox_nm(w)) == w` is a property test.
- `multiscales[0].name`: `"raw"` or the label key. `type`: `"mean"` (raw, block mean) or `"nearest"` (labels,
  stride with offset `(L−1)//2`, as today). `metadata`: `{"factors": [...], "module": "mia_harness.pyramid",
  "commit": <harness commit>, "edge": "clip"}`.
- **Volume root** `v<i>.zarr/zarr.json`: as today, a multiscale over `raw/s<k>` (same `coordinateSystems`, dataset
  `path` and `input.path` both `raw/s<k>`) so the volume is one OME-Zarr image, plus `labels: [<keys>]`.
  `labels/zarr.json` lists the label images (0.6 `labels` group). Each label group carries `image-label` with
  `source: {"image": "../../raw"}` and `colors`/`properties` when the binding knows them (binary and semantic
  labels: one entry per known value; instance: none). The existing `pairs_with` logic decides which label goes
  with which raw.

## 4. Code changes

### 4.1 Harness writer (`pyramid.py`, `stage.py`)

- `write_pyramid` takes a `StagePlan` (per level: origin, shape, chunk set, planned planes) instead of writing
  whole planes. Reads go through `array.read` as today (byte caps, host allowlist and validators unchanged). For
  a `plane`-access array the whole plane is read once and every level's chunks of that plane are cut from it
  (the `run.crop` last-plane cache); for `chunk`/`file` access each written chunk's raw region
  `[o + c·L, o + (c+patch)·L)` is read on its own. Downsampling stays `block_mean` / `stride` per block. Arrays
  are created with `write_empty_chunks=True`: zarr-python 3 otherwise skips a chunk equal to the fill value (an
  all-background label chunk is common), which coverage would then report as absent.
- `write_volume_groups` writes the 0.6 root and labels groups.
- `stage()` derives the plan (§2.2), checks the budget (§2.3), writes, verifies (§6), builds the `Provenance`
  model (§5), writes it to every group and `stage.json` from the same object.
- New CLI inputs: `--plan plan-rule.json` (the row's rule plus the artifact `id` and `levels_key` the worker
  already computes before submit, written by `prepare_stage` into the job workspace beside `binding.yaml`) and
  `--voxel-size voxel_size.json` (`{axis: {nm, source: record|file|assumed|synthetic, basis?}}`, replacing the
  bare `--voxel-size-nm`; the job still prefers what the file says, recorded as `source: file`; `synthetic` is
  the 1 nm plane spacing mia-buffers gives a 2D stack). The harness therefore writes the full `artifact` block
  itself and the worker's `_publish` only renames: nothing writes into a build after the job. `stage_job` is built
  inside the `submit` activity, not in workflow code, so no `workflow.patched` guard and no fixture re-recording
  is needed; new `StageJobArgs` fields take defaults.

### 4.2 Harness reader (`Pyramid`, `tensorize.py`)

- Parses 0.6 (`coordinateSystems`, dataset `sequence` of `scale` + `translation`, or a lone `scale`/`identity`)
  and keeps parsing 0.5 (axes on the multiscale, bare transform list) for existing artifacts and the lead's
  linked pyramids.
- Levels are resolved by scale relative to the raw voxel size: factor `= scale_x / vx`, where `vx` is
  `mia.voxel_size` when present, else the finest level's scale (0.5 and linked pyramids). `s0` is never assumed
  to be level 1.
- `centre_nm` and plane map to array indices by inverting the dataset transform (§3) into the **raw volume's**
  level-L grid, with `_start`'s round-then-clamp against the raw extent (known from `mia.geometry.raw_shape`,
  or from the array shape for a dense artifact), then the box origin is subtracted. The reader never clamps
  against the box: a sample outside it gets a negative or out-of-range index and fails the next check instead
  of sliding onto an edge chunk and passing.
- For an artifact that declares sparse storage (`mia.schema ≥ 1` with a `plan`), the reader lists the chunk keys
  the sample touches (array `chunk_key_encoding`, `store.exists`) and raises `NotStaged(keys)` if any is absent.
  Fill values never reach the model. Dense 0.5 artifacts and linked pyramids are exempt: they were written with
  zarr-python's default `write_empty_chunks=False`, so an all-background chunk is legitimately absent there.
  A legacy all-background-label fixture pins this.
- `LevelPatch` gains nothing; `bbox_of` already uses the dataset `scale`/`translation`, which now carry the box
  offset, so `bbox` lands in the raw frame with no tensorizer change. `Pyramid.bbox_nm(sample)` and
  `raw_index_box(sample)` are added for verification and tests.

### 4.3 Verification (§6) and `stage.json`

`stage.json` is the registry's summary, generated from the `Provenance` model: `ok`, `errors`, the `artifact`
block (now with `plan_digest`), per volume `path`, `axes`, `shape` (raw), `dtype`, `voxel_size_nm`, `levels` (as
the registry's `LevelBody` wants them, plus `origin` and `extent`), `labels`, `intensity_sample`, `samples` per
split, and `zarr_json_sha256` per group it summarizes. Order of writing (§6): `stage` writes the groups and a
provisional `stage.json`; `check-stage` verifies, writes `mia.verification` into every `zarr.json`, then digests
them and rewrites `stage.json` from the same model. The digests describe the final files.

> **Call (§9 Q4):** `stage.json` stays. Retiring it would move the registry onto reading five `zarr.json` files per
> volume and leave `intensity_sample` (≤4096 values per channel, ~150 KB per RGB volume) nowhere to live; keeping
> it costs one generator function. It is an index, never a source of truth: the per-group digests let the registry
> or a human prove it matches the Zarr.

### 4.4 Registry, sampler, client (this repo)

- `jobspec.artifact_id` hashes `plan_digest` too (append; the id is 16 hex of a sha256 over a JSON list, so old ids
  simply never match). `_artifact_block` and the staged asset row carry `plan_digest`.
- `register.volume_bodies` passes `samples` per split; `VolumeBody` gains `samples: dict[str, list[dict]] = {}`
  (additive, wire-safe). The service keeps them with the volume as **candidates**.
- `sampler.draw` picks a volume by weight as now, then a planned candidate uniformly (train with replacement). A
  volume registered without `samples` (a 0.5 artifact) keeps today's behaviour, so nothing already serving changes.
- Val and test keep today's semantics exactly: `_split` builds the frozen list on demand by the same weighted
  `draw`, but without replacement over the candidates (a candidate used once is not used again in that split),
  sized by `sourcing.val_size`/`test_size` as now, append-only and never re-sorted. A volume that registers after
  indices were frozen only affects indices not yet drawn, as today. The eval distribution therefore follows the
  weights (two volumes at 99:1 give a 99:1 eval set, not 50:50), and `size` stays the configured length; if the
  candidates run out first the list is shorter and `/split` says so (`size` = what exists, `exhausted: true`).
- `BufferDataset.__getitem__` lets `NotStaged` propagate (the loader dies naming the chunk keys). It is a staging
  bug by construction; skipping and redrawing would hide it, and a placeholder is the fill value by another name.
- `PUT volumes` **replaces** that volume's candidates (a retry or a rebuild cannot duplicate them); frozen
  entries already drawn from an earlier registration of the same volume stay as they are.
- Planner: the `sampling.plan` block, per-row counts, `plan_digest` in `plan.json`, the byte estimate in `plan.md`.
- The page is unchanged; it reads nothing new.

## 5. Provenance under `attributes.mia`

A pydantic model in the harness, `mia_harness.provenance`, `"schema": 1`, written in full on each multiscale
group (raw and labels) and summarized on the volume root (`artifact`, `derived_from`, `voxel_size`, `plan`
digest, `verification`). Field by field:

- `artifact`: `id`, `levels_key`, `binding_hash`, `plan_digest`, `harness_commit`, `env_digest` (sha256 of the
  harness `pixi.lock`), `snippets_commit`, `record`, `record_commit`.
- `derived_from` (**required**, the only place that says which bytes): one entry per source file:
  `url`, `member` (path inside an archive, or null), `validators` (`etag`, `last_modified`, `size`, and
  `sha256` when `files.json` has it: dropped files and fetched files), `snippet` `{name, commit}`, `access`
  (`chunk | plane | file`). Lifted from `io.validators`, the binding and `files.json`, as `crop.json` does today.
- `geometry`: `raw_shape` and `source_axes` (the raw array's own order), `axes` (stored order), `dtype`, and
  `levels[]`: `{factor, path, origin, extent, shape, planned_planes, chunks_written, bytes_written}` with `origin`
  and `extent` in raw index space and `shape` the array's shape in level-L indices (§2.3).
- `voxel_size`: per axis `{nm, source: record | file | assumed | synthetic}`; for `assumed`, `basis`
  (`"(em, FIB-SEM) median at catalog ab6a013d"` as `assume.py` knows it) and `range` when the table has one;
  `synthetic` is the 1 nm plane spacing mia-buffers gives a 2D stack. A bare `scale` cannot say it was guessed;
  this field can.
- `downsampling`: `{raw: "mean", label: "nearest", edge: "clip", nearest_offset: "(L-1)//2"}`.
- `plan`: `digest`, `rule` (the row's rule verbatim), `samples` per split (the descriptors of §2.2, each with its
  per-level start indices in the raw level grid).
- `statistics`: `intensity_sample` per channel (sorted, ≤4096 values) on the raw group only, drawn from
  **planned level-0 chunks** (8 random written chunks per channel), never from arbitrary boxes that may be holes.
- `verification`: §6's results, `harness_commit`, `time` (UTC ISO), `schemas_commit` (the vendored 0.6 tag).

Nothing may exist only in `stage.json`.

## 6. Verification

Two commands, because the geometry check needs the snippet and the schema check must not load one:

- **In `harness stage`** (has the snippet; this replaces today's `_verify`): for `N` sampled planned samples
  (default 8, every level of each), the sample read through the new reader against the snippet reading the raw at
  `raw_index_box(sample)`, block-meaned or strided afresh for coarser levels. Today's "coarser level against the
  staged base" check is dropped: a 256² sample at factor 4 covers a 1024² raw region of which the base level
  stores only 256², so the base cannot be the reference in a sparse artifact. Fresh source reads prove the
  translations and the pixels at once.
- **`harness check-stage DIR`** (loads no snippet code; runs as the second command of the same job script, so one
  job, one lock, one `read_stage`): validates every `zarr.json` against the vendored 0.6 schemas (`image`, `label`,
  `coordinate_systems`, `coordinate_transformations`, `axes`, `ome_zarr`, and the `strict_*` variants, each
  reported separately) with `jsonschema` and a `referencing` registry that resolves the
  `https://ngff.openmicroscopy.org/0.6/schemas/…` ids to the vendored files; runs `ngff-zarr`'s validator too if
  it is importable (it is not in the harness env today; not added); checks **coverage** (every chunk of every
  planned sample exists); checks that `stage.json`'s digests match each `zarr.json`; writes results into
  `mia.verification` of every group and into `stage.json`. `jsonschema` is one new harness dependency.

The non-strict schemas, coverage, digests and the geometry checks decide `ok`. The `strict_*` results are
**advisory**: recorded, never blocking (instance labels have no `colors`, which `strict_label` requires).

The worker's `read_stage` publishes (rename) only when `ok`, as today.

## 7. Tests

- Transform math: pixel-centre shift, box origin alignment to `L`, inverse mapping, edge clamping; property test
  `raw_index_box(bbox_nm(w)) == w` over random samples, factors and origins (hypothesis if present, else a seeded
  loop).
- Round trip on a synthetic volume with known values: only planned chunks exist, unplanned planes are absent,
  reading an unplanned sample raises `NotStaged`, planned samples read back equal to the source and to a fresh
  downsample, `check-stage` passes every schema.
- 0.5 artifacts and a linked pyramid still open and read (fixture from the current `test_pyramid.py`).
- Plan derivation is deterministic (same rule + shape → same samples, same digest).
- Temporal replay tests pass unchanged (the stage command is built in an activity; no workflow patch); the
  registry round trip
  (`PUT volumes` with `samples`, `frozen` loaded, `draw` picks from it) in `test_buffers_api.py`.

## 8. Deliverables and order

1. This spec, reviewed.
2. PR A (mia-snippets): `provenance` model, 0.6 writer with a plan, reader for 0.5 + 0.6 with `NotStaged`,
   vendored schemas and `check-stage`, tests. Old artifacts are not rewritten.
3. PR B (this repo): `sampling.plan`, `plan_digest` in the artifact id and rows, job spec with the two new files,
   `samples` through registration as candidates, eval lists drawn from them, sampler by candidate, client
   `NotStaged`, the status reason for a cap refusal, docs (`docs/development.md` buffers section, SPEC.md §7–8 in
   mia-snippets).
   **Deployment note:** a round pins its harness commit, and an old harness does not know `--plan`; deploy PR B
   with no round in sourcing (the same rule as the milestone-B restart), or the worker must pass the new flags
   only when the pinned harness is new enough.
4. `notes/ome-zarr-0.6-frictions.md`: what 0.6 could not say (first entries: no slot for an *estimated* transform
   value; none for verification records; none for derivation from non-Zarr sources; no way to say "this chunk is
   intentionally absent" as opposed to "fill"; `labels` and the `raw/` sub-group both being multiscales over the
   same arrays; `(L−1)/2` is a note, not a MUST; no place to say what bytes a non-Zarr source had, so ids stay
   recipe-addressed).

## 9. Confirm before implementing

1. **Plan source.** Sample descriptors only (recommended). Label-biased samples would be more descriptors from the
   same planner, nothing in the artifact changes; add when a buffer asks for it.
2. **Labels.** Same sparse boxes and chunk sets as the raw (recommended, §2.4).
3. **Coordinate system name.** `<record>.v<i>` (recommended) or your `<record>.raw<i>`.
4. **`stage.json`.** Stays as the registry index with per-group digests (recommended, §4.3).
5. **Byte cap.** `limits.stage_bytes`, default `job_bytes`; plan-time estimate in `plan.md`, job refusal as dataset
   status `failed` (§2.3).
6. **New, and the one that changes serving:** train is drawn from a finite planned list with replacement (§2.1).
   Without it there is no finite plan and nothing can be sparse. The count is a flat **per-volume** number per
   split, so artifact ids stay independent of the pool and weights keep acting at draw time. `{train: 1000,
   val: 20, test: 20}` is a placeholder: the planner's byte and capacity estimates on the real pool
   (`plan.md`, §2.5) should set the defaults before they are fixed in the example spec.
