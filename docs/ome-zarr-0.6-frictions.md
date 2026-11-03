# OME-Zarr 0.6 frictions

We built sparse OME-Zarr 0.6 images (Zarr v3, released 0.6 only) as ML training samples: only the planned chunks of
each volume exist. These are the places the spec or its schemas did not fit; the audience is the OME-Zarr hackathon.

## What 0.6 could not say

- No slot for an *estimated* transform value, such as a voxel size we inferred rather than read.
- No slot for verification records (what was checked, by which tool and commit).
- No slot for derivation from non-Zarr sources (source URLs, validators, sha256).
- No way to say what bytes a non-Zarr source had, so artifact ids stay recipe-addressed, not content-addressed.
- No way to say "this chunk is intentionally absent" as opposed to "fill". Absent chunks mean two things, so we need
  `write_empty_chunks=True` to tell "planned but background" from "not planned"; neither the spec nor zarr says more.
- No field for a level's sparse box origin. We encode it in `translation`, so a reader divides by `scale` and
  subtracts the half-pixel shift to recover the index origin.
- Nothing says the levels of one multiscale cover the same extent (only same dimensionality, dtype and at most 5
  axes). Our levels do not: level 0 holds the planned boxes, a coarser level may hold the whole volume. Valid, but
  every viewer assumes the levels coincide.
- No schema for the downsampling description (method, factors, edge policy, nearest offset): `type` is a free
  string and `multiscales[].metadata` a free-form object (both SHOULD), so ours live there with no validation.
- `(L-1)/2` (the half-pixel centre shift between levels) is a note for "classical binning", not a MUST, so two
  writers can disagree; for `nearest` (labels) nothing says which pixel of the block is kept, so we record
  `nearest_offset` ourselves.
- Nothing announces labels on the image itself: a reader discovers them by probing `labels/zarr.json`. We repeat
  the `labels` list on the root image, a key no schema knows.
- No schema covers the `labels/` container group (the bundle has no `labels.schema`); an `ome` document with
  `labels` fails every branch of `ome_zarr`, so we skip it and say so.

## What was awkward

- The root image and its `raw/` sub-group are both multiscales over the same arrays (the root's `datasets` point
  at `raw/s0`, `raw/s1`), with nothing to say they are one data.
- A label image's coordinate system is tied to the image's only by convention ("usually understood to be the
  same"); the optional link is an identity transform to `labels/<path>` on the image's `coordinateTransformations`.
  We reuse the coordinate system's name and write no link.
- The Zarr v3 array's `dimension_names` and the OME `axes` can both name the axes; the spec does not mention
  `dimension_names`, so nothing says they must agree.
- The root image repeats every transform's `input.path`: `raw/` appears on the dataset `path` and again inside each
  sequence. It is one fact stated twice, and the two can drift.
- The `sequence` wrapper is verbose: `input` and `output` around what is just scale plus translation (a two-item
  list in 0.5).
- `colors[].label-value` is a `number` while `properties[].label-value` is an `integer`.
- `label.schema` checks only `image-label` and `version`, not `image`, so we apply `image` to label groups ourselves.
- `ome_zarr` is an `anyOf` that already passes whenever `image` passes, so it adds nothing for us.
- `strict_label` requires `colors`: an instance label without a palette always fails it.
- zarr-python 3.4 exposes only a count of initialized chunks (`nchunks_initialized`); the key listing is private
  (`_shards_initialized`, which lists the whole store prefix and then walks every possible chunk key). We probe
  `store.exists` per chunk a read touches instead.
