# mia-buffers milestone 1: stage, pyramid reader, tensorize, muvit-2d — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `harness stage` turns a dataset binding into one OME-Zarr pyramid per raw volume (labels alongside),
`harness tensorize` turns a pyramid plus a crop descriptor into a MuViT sample through a pluggable tensorizer, and
both work on the U937 confocal record and on the ML lead's existing FANC crop.

**Architecture:** All code lives in the mia-snippets checkout (`<harness.dir>` in `mia_data.yaml`; never write the
real path into a tracked file), on a branch, as maintainer changes to `harness/` plus one new top-level
`tensorizers/` directory. Three new harness modules: `pyramid.py` (write and read NGFF 0.5 pyramids),
`stage.py` (binding → pyramids, verified), `tensorize.py` (descriptor → `Stack` → tensorizer → `Sample`, checked
against `describe()`). The existing `IO` gains one behaviour: `open()` serves a plain URL from the dataset cache
when the whole file is already there, so a staged dataset is downloaded once by `io.fetch` and read locally after.
The dataset cache (`MIA_HARNESS_CACHE_DIR`) is the raw cache for this milestone.

**Tech Stack:** Python 3.12 in the harness pixi environment (`<harness.dir>/harness/.pixi/envs/default`), numpy,
zarr 3.4 (zarr v3 stores, NGFF 0.5 metadata written by hand as group attributes), tifffile via the existing
`tiff-zip-member` snippet, pytest. No torch: the MuViT shape contract is mirrored in `check_sample`.

**Spec:** `notes/specs/2026-10-21-training-buffers.md` v0.4 §2 (reading, snippets), §3 step 3 (staging layout),
§4 (harnesses, descriptor, Sample), §6.3, §10 milestone 1, §11. mia-snippets `SPEC.md` §2, §4, §5, §7.
Consumer contract: weigertlab/muvit `MuViTDataset._sanity_check` and the demo notebook's `__getitem__`.

## Global Constraints

- Run tests with `<harness.dir>/harness/.pixi/envs/default/bin/python -m pytest -q` from `<harness.dir>/harness`
  (the `test` pixi task); `pyproject.toml` sets `filterwarnings = ["error"]`, so no warnings may leak.
- Snippets stay pure readers (SPEC §2); a tensorizer is pure too: no I/O, imports only `mia_harness.tensor_api`,
  numpy and allowlisted packages. Harness code may import anything in the harness environment.
- Byte caps and host allowlists come from the job environment as today (`from_env`); staging never raises them.
- No real paths or hostnames in tracked files, in either repository.
- Line length 120, one-line module docstrings, `# ponytail:` on deliberate shortcuts.
- Commit on the mia-snippets branch `buffers-m1`; do not push or open a PR.

## Decisions the spec leaves open, fixed here

- **Pyramid layout.** `<out>/v<i>.zarr/` per raw array `i` of the binding: zarr v3 group; `raw/` group with
  arrays `s0..sN` and `ome.multiscales` (version 0.5); `labels/<label_key>/` groups of the same form plus
  `ome.image-label`. Axes are the Array's `czyx` letters in that order (c `channel`, others `space`, unit
  `nanometer`). Level factor `f` scales y and x only: `scale = (vz, vy·f, vx·f)`, `translation = (0, vy·(f−1)/2,
  vx·(f−1)/2)` (the half-pixel centring the lead's crop uses). Chunks `(1, 1, patch, patch)` over `(c, z, y, x)`.
  Raw levels are block means in float then cast back to the source dtype (rounded); labels are strided (nearest).
  Each level trims the remainder: `shape_f = shape0 // f`. Group attribute `mia` records record id, binding hash,
  snippets commit, levels, patch, source axes and access.
- **Streaming.** Staging reads one `(c, z)` plane at a time through the snippet and writes all levels of that
  plane. `# ponytail:` a plane is held in memory; tile when a plane exceeds a few GB.
- **Verification.** After writing: for 8 random `s0` chunks, re-read the box through the snippet and compare
  equal; for every coarser level, recompute 4 random blocks from `s0` and compare equal. Any mismatch fails the
  command and `stage.json` says which.
- **Voxel size.** From the Array (`voxel_size_nm`) when present, else `--voxel-size-nm z,y,x` on the command
  line (the record's value, passed by the worker); neither → the command fails with `voxel size unknown`.
- **Descriptor.** `{"plane": {"z": int}, "centre_nm": [y, x], "levels": [1, 2, 4], "patch": 256, "channel": null}`.
  Per level: `scale_L`, `translation_L` from the pyramid; `start = round((centre − translation_L) / scale_L − patch/2)`
  clamped to `[0, n_L − patch]` per axis; z index `min(int(z // (zscale_L / zscale_0)), nz_L − 1)`. `channel: null`
  reads every channel; an int reads one.
- **Sample.** A dict of numpy arrays: `img (L, C, P, P)`, `bbox (L, 2, 2)` in nm with `[[y0, x0], [y1, x1]]`
  where `y1 = (start_y + P − 1)·scale + translation` (the notebook's inclusive last pixel), optional
  `label (L, P, P)` int32 per label key as `label/<key>`, `pixel_size (L, 2)` nm, and `meta` (JSON-safe dict).
- **Tensorizer interface.** `tensorizers/<name>/tensorizer.py` with `SPEC_VERSION = 1`,
  `describe(params: dict) -> TensorSpec`, `tensorize(stack: Stack, params: dict) -> Sample`; `tensorizer.yaml`
  with `name`, `spec_version`, `summary`, `params` schema, `requires`. Loaded exactly like snippets.
- **muvit-2d.** Params `normalize: percentile|none` (default percentile), `range: [1, 99]`, `clip: true`,
  `dtype: float32`. Percentiles per level per channel over the patch; output in `[0, 1]`; `none` divides by the
  dtype maximum for integer input. Labels pass through as int32.
- **Reader axis map.** `open_pyramid` accepts any axis order the NGFF metadata declares (the lead's crop is
  `x, y, z`) and exposes reads in `czyx` order.

## Review Focus

1. A source plane whose y or x is not a multiple of the coarsest factor stages without error and the trimmed
   extents are consistent between raw and labels. (Task 2)
2. A descriptor centre near the volume edge clamps every level inside the array and the bbox still reports the
   true clamped box, never a negative start. (Task 4)
3. `io.open` on a URL that is in the cache issues no range GET and counts no bytes. (Task 1)
4. A tensorizer whose output shape or dtype differs from its `describe()` fails `tensorize` with the key named. (Task 4)
5. The lead's `x, y, z` pyramid reads back the same pixels and bbox through `open_pyramid` as through the
   notebook's own slicing. (Task 6)

---

### Task 1: Cache-hit opens

**Files:**
- Modify: `harness/src/mia_harness/io.py` (`IO.open`)
- Test: `harness/tests/test_io.py` (append)

**Interfaces:**
- Produces: `IO.open(url)` returns a seekable local `BinaryIO` when `self.cache.get(_key(url, self._head(url)))`
  exists for a plain (non-member) URL; otherwise the existing `_RangeFile`. No other signature changes.

- [ ] **Step 1: Write the failing test** (uses the `server` fixture and the `io` construction pattern of
  `test_smoke.py`)

```python
def test_open_serves_a_fetched_file_locally(server, tmp_path):
    server.files["/a.bin"] = bytes(range(256)) * 1000
    io = make_io(tmp_path)  # DatasetCache(shared, private, writable=True), hosts {"127.0.0.1"}
    io.fetch(server.url("/a.bin"))
    fetched = io.bytes_fetched
    before = len(server.requests)
    f = io.open(server.url("/a.bin"))
    f.seek(1000); assert f.read(3) == bytes([232, 233, 234])
    assert io.bytes_fetched == fetched
    assert [m for m, _ in server.requests[before:]] == ["HEAD"]
```

- [ ] **Step 2: Run it, expect FAIL** (a range GET appears, or bytes_fetched grows)
- [ ] **Step 3: Implement** the cache lookup at the top of the `member is None` branch of `IO.open`; return
  `open(path, "rb")`.
- [ ] **Step 4: Run `test_io.py`, expect PASS; run the whole suite, expect the baseline count plus one**
- [ ] **Step 5: Commit** `harness: io.open serves cached files locally`

### Task 2: Pyramid writer and reader

**Files:**
- Create: `harness/src/mia_harness/pyramid.py`
- Test: `harness/tests/test_pyramid.py`

**Interfaces:**
- Produces:
  - `write_pyramid(array: Array, out: Path, *, levels: list[int], patch: int, voxel_size_nm: dict[str, float], kind: Literal["raw", "label"], attrs: dict) -> PyramidInfo`
    where `PyramidInfo(axes: str, shape: tuple[int, ...], dtype: str, levels: list[LevelInfo])` and
    `LevelInfo(path: str, factor: int, shape: tuple[int, ...], scale_nm: dict[str, float], translation_nm: dict[str, float])`.
    Writes `out/` as a zarr v3 group with `s<i>` arrays and the `ome` attributes described above; `kind="label"`
    adds `ome.image-label` and uses nearest downsampling.
  - `open_pyramid(path: Path) -> Pyramid` with `.axes: str` (czyx subset, canonical order), `.levels: list[LevelInfo]`,
    `.dtype`, `.attrs: dict`, `.read(level: int, region: dict[str, int | slice]) -> np.ndarray` (axes in canonical
    order, transposing storage order as needed), `.factor_of(level) -> int` (round of x-scale ratio to level 0).
  - `block_mean(plane: np.ndarray, f: int) -> np.ndarray` and `stride(plane: np.ndarray, f: int) -> np.ndarray`
    (2-D, trim remainder), exported for the verifier.

- [ ] **Step 1: Write the failing tests**

```python
def test_writes_levels_scales_and_translation(tmp_path):
    arr = NpArray(axes="zyx", data=np.arange(3*600*700, dtype=np.uint16).reshape(3, 600, 700))  # test Array impl
    info = write_pyramid(arr, tmp_path / "v0.zarr" / "raw", levels=[1, 2, 4], patch=256,
                         voxel_size_nm={"z": 300.0, "y": 10.0, "x": 10.0}, kind="raw", attrs={"record": "r"})
    assert [l.shape for l in info.levels] == [(3, 600, 700), (3, 300, 350), (3, 150, 175)]
    assert info.levels[2].scale_nm == {"z": 300.0, "y": 40.0, "x": 40.0}
    assert info.levels[1].translation_nm == {"z": 0.0, "y": 5.0, "x": 5.0}
    g = zarr.open_group(tmp_path / "v0.zarr" / "raw", mode="r")
    ms = g.attrs["ome"]["multiscales"][0]
    assert g.attrs["ome"]["version"] == "0.5" and [a["name"] for a in ms["axes"]] == ["z", "y", "x"]
    assert ms["datasets"][1]["coordinateTransformations"] == [{"type": "scale", "scale": [300.0, 20.0, 20.0]}, {"type": "translation", "translation": [0.0, 5.0, 5.0]}]
    assert g["s0"].chunks == (1, 256, 256) and g["s0"].dtype == np.uint16
    np.testing.assert_array_equal(g["s0"][:], arr.data)
    np.testing.assert_array_equal(g["s1"][1], block_mean(arr.data[1], 2))

def test_label_levels_are_nearest(tmp_path): ...  # kind="label": s1 == stride(plane, 2); attrs has ome["image-label"]

def test_remainder_is_trimmed_consistently(tmp_path): ...  # y=601, x=701, levels [1,2,4] → shapes (…,150,175) at s2 for raw and for a label of the same shape

def test_open_pyramid_reads_canonical_order_from_xyz_storage(tmp_path):
    # write a tiny group by hand in the lead's layout: axes x,y,z; s0 (40, 30, 5) stored xyz; scale [4.3,4.3,45]; s1 scale [8.6,8.6,45] translation [2.15,2.15,0]
    p = open_pyramid(tmp_path / "crop.zarr" / "raw")
    assert p.axes == "zyx" and p.factor_of(1) == 2 and p.levels[1].translation_nm == {"z": 0.0, "y": 2.15, "x": 2.15}
    np.testing.assert_array_equal(p.read(0, {"z": 2, "y": slice(0, 30), "x": slice(0, 40)}), data_xyz[:, :, 2].T)

def test_open_pyramid_roundtrip(tmp_path): ...  # write_pyramid then open_pyramid: read(1, …) equals g["s1"] slice
```

- [ ] **Step 2: Run, expect ImportError**
- [ ] **Step 3: Implement `pyramid.py`.** Writing: create arrays with `zarr.create_array(store, shape, chunks,
  dtype)` for each level, then loop planes `for c in range(C) for z in range(Z)`: `plane = array.read({...})`,
  write `s0[c, z] = plane`, and for each coarser level write `block_mean`/`stride` of it. Metadata written once
  as `group.attrs["ome"]` and `group.attrs["mia"]`. Reading: parse `ome.multiscales[0]`, map axis names to storage
  positions, convert units (`micrometer` → ×1000), compute the transpose to canonical order.
- [ ] **Step 4: Run, expect PASS; whole suite green**
- [ ] **Step 5: Commit** `harness: NGFF 0.5 pyramid writer and reader`

### Task 3: `harness stage`

**Files:**
- Create: `harness/src/mia_harness/stage.py`
- Modify: `harness/src/mia_harness/cli.py` (new subcommand), `SPEC.md` §7 (one line)
- Test: `harness/tests/test_stage.py`

**Interfaces:**
- Consumes: `open_arrays` (run.py), `write_pyramid`, `open_pyramid`, `block_mean`, `stride` (Task 2), `IO`.
- Produces: `stage(binding_path: Path, snapshot: Path, record_path: Path, io: IO, *, levels: list[int], patch: int, out: Path, voxel_size_nm: dict[str, float] | None = None, seed: int = 0) -> dict`
  returning the `stage.json` content: `{"ok": bool, "volumes": [{"path": "v0.zarr", "axes", "shape", "dtype", "access", "voxel_size_nm", "levels": [...], "labels": {key: "labels/<key>"}}], "checks": {"s0_chunks": n, "level_blocks": n}, "bytes_fetched": int, "errors": [str]}`.
  CLI: `harness stage <binding> --snippets DIR --record record.yaml --levels 1,2,4 --patch 256 --out DIR [--voxel-size-nm z,y,x] [--work DIR]`,
  exit 0 when `ok`, 1 otherwise; always writes `DIR/stage.json`.
  Order of work: `io.fetch` every URL of the binding first (so plane-access snippets read locally after Task 1),
  then `open_arrays`, then per raw `i`: `write_pyramid(raw, out/"v{i}.zarr"/"raw", kind="raw")` and for each
  label paired with it `write_pyramid(label, out/"v{i}.zarr"/"labels"/label_key, kind="label")`, then verify.
  Voxel size: `raw.voxel_size_nm` if it covers the spatial axes, else the `voxel_size_nm` argument, else error
  `raw {i}: voxel size unknown`.

- [ ] **Step 1: Write the failing tests** (binding and record via `test_smoke.write`, snapshot `SNAP`)

```python
def test_stage_writes_raw_and_label_pyramids(tmp_path, io):
    b, r = write(tmp_path, (2, 300, 300), (2, 300, 300))
    rep = stage(b, SNAP, r, io, levels=[1, 2], patch=64, out=tmp_path / "out", voxel_size_nm={"z": 100.0, "y": 8.0, "x": 8.0})
    assert rep["ok"] and rep["volumes"][0]["labels"] == {"gt": "labels/gt"} and rep["checks"] == {"s0_chunks": 8, "level_blocks": 4}
    assert open_pyramid(tmp_path / "out" / "v0.zarr" / "raw").levels[1].shape == (2, 150, 150)
    assert json.loads((tmp_path / "out" / "stage.json").read_text())["ok"]

def test_stage_without_voxel_size_fails(tmp_path, io): ...  # errors == ["raw 0: voxel size unknown"], ok False, exit 1 via cli.main

def test_stage_cli(tmp_path, io, monkeypatch): ...  # monkeypatch MIA_HARNESS_JOB_BYTES etc.; cli.main(["stage", ...]) == 0; stage.json exists

def test_stage_verification_catches_corruption(tmp_path, io, monkeypatch): ...  # monkeypatch pyramid.block_mean to return zeros during write only → ok False, error names "s1"
```

- [ ] **Step 2: Run, expect failure**
- [ ] **Step 3: Implement `stage.py` and the CLI entry; add the one-line CLI description to SPEC §7**
- [ ] **Step 4: Run, expect PASS; whole suite green**
- [ ] **Step 5: Commit** `harness: stage command writes verified pyramids`

### Task 4: Tensorizer API, loader, `muvit-2d`, `harness tensorize`

**Files:**
- Create: `harness/src/mia_harness/tensor_api.py`, `harness/src/mia_harness/tensorize.py`,
  `tensorizers/muvit-2d/tensorizer.py`, `tensorizers/muvit-2d/tensorizer.yaml`, `tensorizers/muvit-2d/README.md`
- Modify: `harness/src/mia_harness/loader.py` (`load_snippet` gains `kind: Literal["snippets", "tensorizers"] = "snippets"`
  and the required callable name per kind), `harness/src/mia_harness/cli.py`
- Test: `harness/tests/test_tensorize.py`, fixture `harness/tests/tensorizers/bad-shape/` (returns `img` with one level too few)

**Interfaces:**
- Consumes: `open_pyramid`, `Pyramid.read`, `LevelInfo` (Task 2).
- Produces (`tensor_api.py`): `LevelPatch(factor: int, img: np.ndarray, labels: dict[str, np.ndarray], scale_nm: tuple[float, float], translation_nm: tuple[float, float], start_px: tuple[int, int])`
  (img `(C, P, P)`, labels `(P, P)` each); `Stack(levels: list[LevelPatch], patch: int, plane: dict, centre_nm: tuple[float, float], meta: dict)`;
  `TensorSpec(arrays: dict[str, tuple[tuple[str | int, ...], str]])` where a shape entry uses the symbols
  `"L"`, `"C"`, `"P"` or ints, e.g. `{"img": (("L", "C", "P", "P"), "float32"), "bbox": (("L", 2, 2), "float32"), "label/*": (("L", "P", "P"), "int32"), "pixel_size": (("L", 2), "float32")}`;
  `Sample = dict[str, np.ndarray | dict]`; `check_sample(sample, spec, *, L: int, C: int, P: int) -> None` raising
  `ValueError("<key>: expected shape … dtype …, got …")`; `bbox_of(level: LevelPatch, patch: int) -> np.ndarray` `(2, 2)`.
- Produces (`tensorize.py`): `read_stack(pyramid_dir: Path, descriptor: dict) -> Stack` (opens `raw` and every
  `labels/*` group), `tensorize(pyramid_dir: Path, descriptor: dict, snapshot: Path, name: str, params: dict) -> Sample`
  (loads the tensorizer, runs `describe`, `tensorize`, `check_sample`); CLI
  `harness tensorize <pyramid_dir> --tensorizers DIR --name muvit-2d --descriptor desc.json [--params p.json] --out sample.npz`
  writing arrays to the npz and `meta` to `<out>.json`, exit 0; exit 1 with the `ValueError` message on a contract
  failure.
- `tensorizers/muvit-2d/tensorizer.py`: `describe(params)` returns the spec above (labels only when present is
  decided by the harness: `label/*` means zero or more keys); `tensorize(stack, params)` as in Decisions.

- [ ] **Step 1: Write the failing tests** (stage a synthetic `(3, 600, 700)` uint16 volume with voxel 10 nm and a label, levels `[1, 2, 4]`, patch 64 via Task 3's `stage`)

```python
def test_stack_is_co_centred_and_bbox_is_in_nm(staged):
    d = {"plane": {"z": 1}, "centre_nm": [3000.0, 3500.0], "levels": [1, 2, 4], "patch": 64, "channel": None}
    s = read_stack(staged / "v0.zarr", d)
    assert [l.start_px for l in s.levels] == [(268, 318), (134, 159), (67, 79)]   # round(centre/scale − translation/scale − 32)
    np.testing.assert_allclose(bbox_of(s.levels[1], 64), [[134*20+5, 159*20+5], [(134+63)*20+5, (159+63)*20+5]])
    assert s.levels[0].img.shape == (1, 64, 64) and s.levels[0].labels["gt"].shape == (64, 64)

def test_edge_centre_clamps(staged): ...  # centre_nm [0, 0] → start (0, 0) at every level; centre beyond the far edge → start n_L − 64

def test_muvit_2d_sample(staged):
    out = tensorize(staged / "v0.zarr", d, TENSORIZERS, "muvit-2d", {})
    assert out["img"].shape == (3, 1, 64, 64) and out["img"].dtype == np.float32 and 0.0 <= out["img"].min() and out["img"].max() <= 1.0
    assert out["bbox"].shape == (3, 2, 2) and out["label/gt"].dtype == np.int32 and out["pixel_size"].tolist() == [[10, 10], [20, 20], [40, 40]]
    assert out["meta"]["levels"] == [1, 2, 4]

def test_bad_tensorizer_fails_contract(staged):
    with pytest.raises(ValueError, match=r"img: expected shape \(3, 1, 64, 64\)"):
        tensorize(staged / "v0.zarr", d, TEST_TENSORIZERS, "bad-shape", {})

def test_tensorize_cli(staged, tmp_path): ...  # writes sample.npz with img and bbox, sample.json with meta; exit 0

def test_loader_kind_tensorizers(): ...  # load_snippet(TENSORIZERS, "muvit-2d", set(), {}, kind="tensorizers") has tensorize and describe; kind="snippets" on it raises ValueError
```

- [ ] **Step 2: Run, expect failure**
- [ ] **Step 3: Implement** `tensor_api.py`, the loader `kind`, `tensorize.py`, the `muvit-2d` tensorizer and the CLI entry. The percentile normalisation: `lo, hi = np.percentile(img, range, axis=(1, 2), keepdims=True)`; `(img − lo) / max(hi − lo, eps)`, clipped when `clip`.
- [ ] **Step 4: Run, expect PASS; whole suite green**
- [ ] **Step 5: Commit** `harness: tensorizer API, tensorize command, muvit-2d tensorizer`

### Task 5: CI and spec text for `tensorizers/`

**Files:**
- Modify: `ci/check_pr.py` (a PR may add one new `tensorizers/<name>/` with `tensorizer.py`, `tensorizer.yaml`,
  `README.md`, same static import check, same name rules; a PR adding both a snippet and a tensorizer fails),
  `SPEC.md` §1 layout and a new short §8 "Tensorizers", `README.md` (one paragraph), `harness/allowlist.txt` unchanged
- Test: `ci/test_check_pr.py`

**Interfaces:** none new.

- [ ] **Step 1: Write the failing tests** (`test_good_new_tensorizer_passes`, `test_snippet_and_tensorizer_in_one_pr_fail`, `test_tensorizer_missing_readme_fails`)
- [ ] **Step 2: Run `ci` tests, expect failure**
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run `ci` tests and the harness suite, expect PASS**
- [ ] **Step 5: Commit** `ci, spec: tensorizers/ directory`

### Task 6: Real data

Not a code task; nothing it writes is tracked. Work in the scratchpad plus the mia-data `work_root` from
`mia_data.yaml`.

1. Fetch the U937 record at `ab6a013d…` to `record.yaml`. Write `u937.yaml`: `tiff-zip-member` for all four files
   (the four `zenodo.org/api/records/20797339/files/<name>/content` URLs from the record); raw params
   `{axes: zcyx}` with the reason "ImageJ frames are fields of view; t is read as the plane axis"; labels
   `nuclei` (instance), `gh2ax-foci` (instance), `edu` (binary), each `pairs_with: 0`.
2. `MIA_HARNESS_JOB_BYTES=2000000000 MIA_HARNESS_ALLOWED_HOSTS=zenodo.org MIA_HARNESS_CACHE_DIR=<work_root>/_cache/zenodo-20797339-u937-edu-h2ax-dapi-confocal MIA_HARNESS_CACHE_WRITABLE=1 harness smoke u937.yaml --snippets <harness.dir> --record record.yaml --out smoke.json`; expect `ok`, shapes `[25, 3, 2048, 2048]` and three `[25, 2048, 2048]`.
3. `harness stage u937.yaml … --levels 1,2,4 --patch 256 --out <work_root>/_buffers/staged/zenodo-20797339-u937-edu-h2ax-dapi-confocal`; expect `ok`, `v0.zarr` with `raw/s0..s2` and three label groups; note bytes fetched (the second run must fetch 0).
4. `harness tensorize <that>/v0.zarr --tensorizers <harness.dir> --name muvit-2d --descriptor '{"plane": {"z": 13}, "centre_nm": [46000, 46000], "levels": [1, 2, 4], "patch": 256, "channel": null}' --out u937.npz`; expect `img (3, 3, 256, 256)` float32 and `label/nuclei` present.
5. The lead's crop: `harness tensorize <lead crop>/raw … --descriptor` with a centre inside the volume and levels `[1, 2, 4]`; then a scratch script that reproduces the notebook's `__getitem__` arithmetic for the same centre and z (its `imgs.append(arr[x0:x0 + P, y0:y0 + P, zi].T)` and bbox lines) and asserts pixel equality and bbox equality to 1e-6 nm. Treat `labels/` as absent if its layout differs.
6. Record every number in the final report: bytes fetched, staging wall time, pyramid size on disk, and the
   equality check result.
