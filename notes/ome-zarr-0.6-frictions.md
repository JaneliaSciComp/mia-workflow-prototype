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
- No schema for the downsampling description (method, factors, edge policy, nearest offset):
  `multiscales[].metadata` is free-form, so ours lives there with no validation.
- `(L-1)/2` (the half-pixel centre shift between levels) is a note, not a MUST, so two writers can disagree.
- No schema covers the `labels/` container group (the bundle has no `labels.schema`); an `ome` document with
  `labels` fails every branch of `ome_zarr`, so we skip it and say so.

## What was awkward

- `labels` and the `raw/` sub-group are both multiscales over the same arrays, with nothing to say they are one data.
- The root image repeats every transform's `input.path`: `raw/` appears on the dataset `path` and again inside each
  sequence. It is one fact stated twice, and the two can drift.
- The `sequence` wrapper is verbose: `input` and `output` around what is just scale plus translation (a two-item
  list in 0.5).
- `colors[].label-value` is a `number` while `properties[].label-value` is an `integer`.
- `label.schema` checks only `image-label` and `version`, not `image`, so we apply `image` to label groups ourselves.
- `ome_zarr` is an `anyOf` that already passes whenever `image` passes, so it adds nothing for us.
- `strict_label` requires `colors`: an instance label without a palette always fails it.
- zarr-python 3.4 has no public listing of initialized chunks (the private `_shards_initialized` walks every
  possible key); we list the store prefix instead.
