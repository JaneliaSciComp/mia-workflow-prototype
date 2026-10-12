# MIA crops: snippets, development and production cropping — Spec

**Status:** draft v0.1 · 2026-10-11 · not yet approved
**Repos:** this repo (orchestration, new package `mia_data/`), [JaneliaSciComp/mia-workflow-data](https://github.com/JaneliaSciComp/mia-workflow-data) (issues only),
[JaneliaSciComp/mia-snippets](https://github.com/JaneliaSciComp/mia-snippets) (conversion snippets + their harness), [AI-HHMI/mia-agentic-search](https://github.com/AI-HHMI/mia-agentic-search) (read-only input)

## 0. Summary

Every dataset that mia-agentic-search has already downloaded successfully gets one issue in `mia-workflow-data`.
A human triggers the issue, and a durable Temporal workflow takes it from there:

1. **Match.** Find an existing snippet in `mia-snippets` that can read the dataset's format.
2. **Develop** (only if no snippet fits). Open an issue in `mia-snippets` that autodev turns into a PR adding a new
   snippet. Test crops are extracted with the PR's snippet, and the data issue is updated to "PR ready" with a link
   to view them. A human approves, and autodev merges.
3. **Produce.** An agent extracts the production crops with the merged snippet. The worker validates them and
   records them.

A **crop** is a single-channel 128×128×128 voxel block at native resolution, cut from a 3D raw volume, together
with the matching label blocks. Only the bytes a crop needs are downloaded. When the format forces it (a TIFF
member of a zip, one file per Z slice), a Z-slab of 128 slices is staged once and several crops are cut from it.

A **snippet** is a small, self-contained reader for one source format and layout. It is written to a fixed spec,
is independent of every other snippet, and is immutable once merged. Any change ships as a new snippet.

## 1. Goals and non-goals

**Goals**
- One issue per downloaded dataset, created idempotently. Re-running the seed picks up new downloads.
- The same snippet can be used by every dataset whose format and layout it covers. A dataset only causes
  development when no existing snippet can read it.
- A human approves every new snippet (a PR review in `mia-snippets`) before it is used in production. The data
  issue always shows where the dataset stands, with links to the PR and to the sample crops.
- Crops are always exactly 128³ voxels, can be reproduced from their recorded seed and coordinates, and carry
  provenance: the record and its commit, the source file, channel and bounding box, and the snippet and its commit.
- Restarts never lose a round or run a job twice (Temporal, the same as autodev).

**Non-goals (for now)**
- 2D, 2D+t or 3D+t data. Only 3D raw volumes are cropped.
- Resampling, padding or stitching tiles to reach 128³. Datasets that can't supply a native 128³ block are marked
  ineligible (§4.1).
- Storage layout, quotas and retention. The storage root and its public URL are configuration (§10).
- Discovering new datasets (mia-agentic-search does that), and assembling a training set across datasets.
- Any change to mia-agentic-search.

## 2. Context

- **Input.** On mia-agentic-search's `state` branch, `state/downloads.json` (the TensorSwitch downloader) and
  `state/downloads-native.json` (the native downloader) have a `downloaded` map with the record id as key. Each
  entry holds `record_commit`, `scope`, `dataset_dir`, `crops` and `labels`. As of 2026-10-11 the union is
  **49 datasets**, all `imaging.dimensionality: 3D`, all still on `main`. Their
  `dataset_dir/crop-NNN.zarr/conversion.json` (native downloader) holds axis and transform decisions with reasons,
  which the match agent should reuse.
- **Sizes.** 45 TB combined, median 5.6 GB, largest 26.8 TB.
- **Sources.** 28 come from zips. Hosts: zenodo, ftp.ebi.ac.uk, Allen Cell S3, Broad, huggingface, figshare,
  cryoetdataportal. Formats: TIFF (31), OME-TIFF (7), MRC (7), HDF5 (6), and a few OME-Zarr, N5, CZI and ND2.
  Records use the `<zip url>::<member>` convention for zip members, and `technical.arrays[]` gives each array's
  role, path pattern, axes, shape and dtype.
- **Shapes.** Going by `technical.arrays` shapes, only 17 of the 49 have a raw array with all three spatial axes
  ≥ 128. Several others are one file per Z slice (e.g. EMPIAR FIB-SEM `yx` slices), which reach 128 in Z once
  stacked. The rest really are shallow (e.g. bbbc034 Z=52, zenodo-3713326 Z=13). The match step decides, from the
  opened volume rather than the record.
- **TensorSwitch** readers subclass `tensorswitch_v2.readers.base.BaseReader`, but there is no plugin mechanism.
  A snippet may wrap a TensorSwitch reader. It never patches TensorSwitch.
- **autodev** already implements "labelled issue → PR → revise → human approval → squash-merge" against one repo
  per deployment. Development reuses it as a second deployment, `snippets`, pointed at `mia-snippets`.

## 3. Components

| where | what | changes |
|---|---|---|
| `mia-workflow-data` | one issue per dataset, labels show its stage (§4) | new repo, issues only, no code or data |
| `mia-snippets` | `SPEC.md`, `harness/`, `snippets/<name>/`, CI checks | new repo |
| this repo, `mia_data/` | seed CLI, `DatasetWorkflow`, `PollWorkflow`, activities, prompts | new package |
| this repo, `autodev/` | GitHub client and app auth (`autodev.github`, `autodev.auth`), csub agent runner (`autodev/agent.py`), await-queue pattern | reused by import, no extraction yet (§12) |
| this repo, autodev | `snippets` deployment (config only) + trusted bot triggers | small change (§12) |
| Temporal | namespace `mia-data` on the shared server; queues `mia-data`, `mia-data-await` | step 0 (§12) |
| mia-agentic-search | records on `main`, downloads state on `state` | read-only |

The data workflow and the autodev `snippets` workflow live in different namespaces. They interact only through
GitHub (an issue and its PR in `mia-snippets`), never through Temporal signals.

## 4. Lifecycle of a data issue

Each stage is shown as a label `data:<stage>`, exactly one at a time, set only by the worker. `data:go` is the
human trigger.

```
data:new ──(human adds data:go)──> data:matching ──match found──────────────────────> data:testing
                                        │                                                  │
                                        └─no snippet fits──> data:developing               │
                                                               │ (autodev PR opened)       │
                                                               v                           │
                                                          data:testing ──> data:pr-ready   │
                                                               ^   (new PR head)  │        │
                                                               └──────────────────┤        │
                                                                         PR merged│        │
                                                                                  v        v
                                                                            data:cropping ──> data:done
any stage ──> data:needs-human (comment says why) · data:ineligible (no 128³ block, or not 3D)
```

- **Re-trigger.** Adding `data:go` to a `done`, `needs-human` or `ineligible` issue starts again from matching,
  with a new seed. A finished dataset gets additional crops. Earlier crops are never deleted.
- **Status comment.** One bot comment per issue is edited in place (found by a hidden marker). It holds the
  stage, the binding (§5.4), the snippet PR link, the sample-crop link and production totals. Every stage
  change also posts a short new comment, so the history stays readable.

### 4.1 Eligibility

A raw volume is eligible if, once opened through its binding, it is 3D with every spatial axis ≥ 128 voxels.
Channels and other non-spatial axes are allowed: each crop takes one channel. A dataset with no eligible raw
volume goes to `data:ineligible`, and the comment gives the shapes found. No resampling, padding or tiling.

## 5. Snippet spec (`mia-snippets/SPEC.md`, spec version 1)

### 5.1 Layout

```
mia-snippets/
├── SPEC.md                    this section, the contract (human-owned)
├── harness/                   executable side of the spec (human-owned, §6)
├── .github/workflows/check.yml
└── snippets/<name>/           one directory per snippet, immutable once merged
    ├── snippet.yaml           metadata (§5.3)
    ├── snippet.py             the reader (§5.2)
    ├── README.md              what it reads, how params are chosen, known limits
    └── examples/<record-id>.yaml   ≥ 1 binding it was tested on (§5.4)
```

`<name>` is a lowercase slug that is never reused, e.g. `tiff-zip-member`, `mrc-http`, `tiff-slices-zip`,
`hdf5-dataset`, `tensorswitch-auto`. A changed snippet is a new directory with a new name, such as
`tiff-zip-member-2`, and its `snippet.yaml` sets `supersedes: tiff-zip-member`. Bindings keep pointing at the
version they were tested with.

### 5.2 Interface

```python
# snippets/<name>/snippet.py
SPEC_VERSION = 1

def open_array(src: harness.Source, io: harness.IO) -> harness.Array:
    """Open one array (raw or label) described by `src`. Must not read voxel data yet."""

class Array(Protocol):                     # defined in harness/api.py
    axes: str                              # e.g. "zyx", "czyx": letters from "czyx", one per dimension
    shape: tuple[int, ...]
    dtype: numpy.dtype
    voxel_size_nm: dict[str, float] | None # {"z":…, "y":…, "x":…} if the file says so, else None
    access: Literal["chunk", "plane", "file"]   # cheapest read unit (cost model, §6.2)
    def read(self, region: dict[str, slice]) -> numpy.ndarray   # e.g. {"c": 0, "z": slice(…), …}; result in `axes` order minus integer-indexed axes
```

- `Source` holds `urls: list[str]` (one per file, sorted. Several when the dataset is one file per Z slice. Zip
  members use `<zip url>::<member>`) and `params: dict` (snippet-specific, declared in `snippet.yaml`, e.g. HDF5
  key or axis override).
- **All I/O goes through `io`.** `io.open(url)` returns a seekable binary file backed by HTTP or S3 range requests
  and an on-disk cache. `io.fetch(url)` returns a local `Path` (the whole file, staged once). `io.list(url)` lists
  zip members or a directory. `io` counts bytes and enforces the round's budget. Snippets never open sockets,
  call `requests`/`urllib`/`fsspec` directly, run subprocesses or write outside `io`'s staging.
- **Pure readers.** A snippet reads; it never crops, samples, resamples, normalises or writes output. Those belong
  to the harness, so every crop goes through the same code.
- **Independent.** A snippet imports only the harness API, its own module and the dependencies on the allowlist
  (§5.3). It never imports another snippet. Shared code goes into a new snippet, not into a common module.

### 5.3 `snippet.yaml`

```yaml
name: tiff-zip-member
spec_version: 1
summary: One multi-page TIFF inside a remote zip, read member-wise (deflated members are fetched whole).
formats: [tiff, ome-tiff]          # mia-agentic-search data.formats values it handles
containers: [zip, http]            # zip | http | s3 | ftp | local
access: plane
params:                            # every param the binding may set; nothing else is accepted
  axes: {type: str, required: false, doc: "override when the TIFF's own axes are wrong; reason required in binding"}
requires: [numpy, tifffile, imagecodecs]   # must be in harness/env allowlist
tested_on: [bbbc034-hipsc-nuclei-3d]       # = examples/*.yaml
supersedes: null
```

The dependency allowlist lives in `harness/` (the harness environment). A snippet that needs a new dependency
can't add it. The agent reports this, and a human extends the environment.

### 5.4 Binding

A binding says how to read one dataset. It is data, not code: it is stored in the data issue's status comment,
and as `examples/<record-id>.yaml` when a snippet is introduced.

```yaml
record: bbbc034-hipsc-nuclei-3d
record_commit: 37e18f6e…
snippets_commit: 4c1d…              # mia-snippets commit the snippets below were taken from
raw:
  - snippet: tiff-zip-member
    urls: [https://data.broadinstitute.org/bbbc/BBBC034/BBBC034_v1_dataset.zip::AICS_12_134_C=2.tif]
    params: {}
labels:
  - snippet: tiff-zip-member
    urls: [https://data.broadinstitute.org/bbbc/BBBC034/BBBC034_v1_DatasetGroundTruth.zip::ground_truth_segmented.tif]
    params: {}
    pairs_with: 0                  # index into raw; spatial shape must match
    label_key: manual_gt-nucleus-instance   # miao#13 label folder name, from the record (mia-agentic-search's rules)
reasons: ["axes zyx from ImageJ metadata, matches record", …]   # one per non-default decision
```

`raw` may list several volumes (e.g. one per file set). Crops are drawn across them (§6.2).

### 5.5 Immutability and review (CI in `mia-snippets`)

`check.yml` runs on every PR, and branch protection requires it:
- A PR may only **add** files under one new `snippets/<name>/`. Changing or deleting anything under an existing
  snippet fails. So does touching `harness/`, `SPEC.md` or `.github/`, unless the PR author is a listed human
  maintainer. autodev already refuses agent commits that touch `.github/`.
- `snippet.yaml` validates against the harness schema. `name` matches the directory and isn't already used. Every
  `requires` is on the allowlist, and every `tested_on` has an example binding.
- A static import check on `snippet.py` rejects network, subprocess and `os.system` imports, imports of other
  snippets, and `pickle` or `np.load(allow_pickle=True)`.
- Real-data tests are **not** run in CI, because data hosts are slow and large. The test-crop job (§7.4) is the
  real-data test, and its result is posted on the PR.

## 6. Harness (`mia-snippets/harness/`)

The harness is the executable half of the spec. It is a pip-installable package with a CLI, run in one pixi
environment on the cluster. Humans own it. Agents use it and never modify it.

### 6.1 Commands

```
harness smoke   <binding.yaml> [--snippets <dir>]           # open every array, check axes/shape/dtype vs record,
                                                           # read one 128³ block per raw (+ its labels), report JSON
harness crop    <binding.yaml> --n 16 --seed S --out DIR   # sample + write crops (§6.2, §6.3), report JSON
         [--budget-bytes B] [--per-slab K]
harness verify  DIR                                        # re-open written crops, run the checks of §6.4
harness preview DIR                                        # PNGs: mid z/y/x slices of raw with label overlay, per crop
```

`--snippets` points at a checkout at a given commit, such as merged `main` or a PR head. Snippets are imported
from that directory only.

### 6.2 Sampling

- **The seed** is chosen by the worker and recorded. Given the same binding, seed and snippet commits, the same
  crop coordinates come out.
- **Choose a volume.** Each eligible raw volume (and channel, if it has several) is chosen with probability
  proportional to its number of 128³ positions, so large volumes aren't underrepresented.
- **Cost-aware reads, chosen from `Array.access`:**
  - `chunk` (zarr, N5, precomputed, chunked HDF5 over S3): read each crop's block directly.
  - `plane` (uncompressed TIFF or MRC over range requests, one file per Z slice): pick `z0` uniformly in
    `[0, Z−128]`, read the 128-plane slab once into local staging, then cut up to `--per-slab` (default 4)
    non-overlapping crops at random `(y0, x0)` within it.
  - `file` (deflated zip member, gzip, FTP): `io.fetch` the file once, then sample several slabs and crops from
    it.
- **Content filter.** Reject a block when the raw data is effectively empty or saturated, i.e. the standard
  deviation is below a threshold, or more than 95% of voxels sit at one value. Redraw at most 10× per crop. Count
  rejections in the report. Thresholds are harness constants for now.
- **Labels.** Each label paired with the chosen raw is read at the same block. A label whose spatial shape differs
  from its raw's makes `smoke` fail. It is never silently dropped.
- **Budget.** `io` stops the round with a clear error when bytes fetched would exceed `--budget-bytes`.
- **Cleanup.** Staging is deleted when the command exits.

### 6.3 Output (per crop)

```
crop-NNN.zarr/
├── raw/                       OME-NGFF 0.5, zarr v3, single scale s0 = 128³, one 128³ chunk, zstd-5, source dtype
├── labels/<label_key>/        same, label dtype kept; zarr.json carries the miao#13 label metadata
└── crop.json                  record, record_commit, raw index, source url(s), channel, offset {z,y,x} in source voxels,
                               voxel_size_nm, snippet name + snippets_commit per array, seed, harness version, bytes fetched
```

Crops for a dataset go in `<storage.root>/{modality}-{organism}-{name}/` (folder naming as mia-agentic-search's
`tools/miao_layout.py`), with `manifest.json` listing every crop. Crop numbers only increase, so earlier crops
are never renumbered.

### 6.4 Verification (deterministic, run by the worker, never trusted from an agent)

Every crop must pass these checks:
- it opens
- raw shape is exactly 128³ (plus no other axes)
- dtype equals the source array's
- the offset plus 128 lies within the source shape
- raw passes the content filter
- each label is 128³ and integer-typed (or boolean)
- `crop.json` is complete
- the crop's checksum is unique within the dataset (no duplicates)

## 7. Steps in detail

### 7.1 Seed: `mia-data seed [--dry-run]`

1. Read `state/downloads.json` and `state/downloads-native.json` from mia-agentic-search's `state` branch and take
   the union of `downloaded`.
2. For each record id, read the record at `main` (still accepted) and note `record_commit` from the downloads
   entry.
3. Create an issue in `mia-workflow-data` unless one already carries `<!-- mia-data:record=<id> -->`.
   - Title: `<id>: <short_name or title>`.
   - Body: the marker, a link to the record (at `main` and at `record_commit`), modality, organism, voxel size,
     `technical.arrays` (role, axes, shape, dtype), data size, source URL(s), which downloader succeeded, and its
     `dataset_dir`.
   - Labels: `data:new`, `modality:<family>`, `has-labels` if the record has label arrays, and `maybe-small` when
     no recorded raw shape has every spatial axis ≥ 128 (a hint only; §4.1 decides).
4. Rate limits: `autodev.github.GitHub` already throttles creates. The seed is resumable, and re-running only adds what's new.

### 7.2 Match (agent job)

**Input.** The round directory contains:
- the record YAML at `record_commit`
- the mia-agentic-search downloads entry, plus that downloader's `conversion.json` / `manifest.json` from
  `dataset_dir` (mounted read-only) when they exist
- every `snippet.yaml` and README from `mia-snippets` `main`, at a recorded commit
- `SPEC.md` and the harness CLI

**Task.** The agent picks a snippet for each raw and label array, and writes the binding with its `reasons`. It
then runs `harness smoke` until the binding passes. The agent may try several snippets and params. It may not
write or alter snippet code.

**Output (`result.json`).** One of:
- `{"outcome": "matched", "binding": …, "smoke": …}`
- `{"outcome": "needs_snippet", "draft_binding": …, "what_fails": …, "tried": […]}`
- `{"outcome": "ineligible", "shapes": …}`
- `{"outcome": "blocked", "reason": …}`

The `blocked` case covers things like an undecidable label class, or a dependency missing from the allowlist.

**Worker.** It re-runs `harness smoke` itself for `matched` and `ineligible`, so the agent's claim is checked
rather than trusted. It then moves to `testing`, `developing`, `ineligible` or `needs-human`.

Match rules are mia-agentic-search's: never invent values, ask rather than guess (`blocked`), and require
evidence for any axis override or transform.

### 7.3 Develop (autodev `snippets` deployment)

1. The worker opens an issue in `mia-snippets`:
   - **title:** `New snippet for <format>/<container> (mia-workflow-data#N)`
   - **body:** the record excerpt, the match agent's `what_fails` and `tried`, the `draft_binding`, and links to
     `SPEC.md` and the data issue
   - **labels:** `snippets:go`, so autodev starts its first round
2. autodev's agent, running in its usual csub sandbox, adds `snippets/<new-name>/`, including
   `examples/<record-id>.yaml`. It runs `harness smoke` (and optionally a small `harness crop`) in its round
   against the real data, and opens the PR. It doesn't need to know about mia-data.
3. `PollWorkflow` notices the PR (via autodev's `snippets/issue-K` branch, or a link in the PR body) and signals
   the data workflow with the PR number and head SHA. The data issue moves to `testing`.
4. Review works as in autodev: the reviewer requests changes or comments `/revise …`. Every new head SHA triggers
   a fresh test-crop job (§7.4).
5. A human approves, and autodev squash-merges. The poller signals `merged`, along with the merge commit. The
   binding's `snippets_commit` becomes that commit, and the issue moves to `cropping`.
6. If the PR is closed without merging, or the issue gets `snippets:needs-human`, the data issue moves to
   `needs-human` with a link.

### 7.4 Test crops (deterministic job, no agent)

**Run.** An LSF job runs `harness crop --n 4` with the binding, against a checkout of the snippets at the PR head
(new snippet) or at `main` (matched snippet), then `harness verify` and `harness preview`. Output goes to
`<storage.root>/_tests/<record-id>/<sha-or-round>/`.

**Report.** The worker posts on the data issue, and on the PR if there is one:
- the verify results
- the bytes fetched
- a table of crop offsets
- **a link to view the sample crops**: `storage.public_url` + path to the preview PNGs, plus a Neuroglancer link
  for the crops if `viewer.neuroglancer_template` is set (§10)

**Next stage:**
- For a PR, the data issue moves to `data:pr-ready` with the PR link and the sample-crop link.
- For a matched existing snippet, the issue goes straight to `cropping`, because that snippet was already
  human-approved and verification is deterministic. (Open question §13.2.)
- If verify fails on a PR head, the worker comments that on the PR. The human can `/revise`, and the issue stays
  `testing`.

### 7.5 Production (agent job)

**Input.** The binding pinned to merged `snippets_commit` (the snippet checkout is mounted read-only), the crop
count `n` (config, default 16), the seed, and the budget.

**Task.** The agent drives the harness. It runs `harness crop`, looks at `harness preview` output, and judges
quality: labels sitting on the structures, no obvious artefacts or padding regions, plausible intensity. It may
reject individual crops with a reason and ask the harness for replacements (new seed suffix). It can't change
code, the binding or the output layout.

**Output.** The crop directory plus `result.json` (`kept`, `rejected` with reasons, harness reports).

**Worker.**
1. Runs `harness verify` itself.
2. Moves verified crops into the dataset folder and appends `manifest.json`.
3. Posts totals, a link to the previews, and the agent's rejection notes.
4. Sets `data:done`, or `needs-human` if fewer than half of `n` survive.

## 8. Temporal design

- **Namespace and queues.** Namespace `mia-data`. Task queues are `mia-data` (workflows and short activities) and
  `mia-data-await` (long LSF waits), the same pattern as autodev. Workflow types are named
  explicitly: `MiaDataDataset` and `MiaDataPoll`.
- **`MiaDataDataset`.** Id `mia-data/<repo>/issue-<N>`. It is thin: a loop over the stages in §4, with every
  side effect in activities. It waits for signals (`go`, `snippet_pr`, `pr_head`, `merged`, `pr_closed`) and for
  LSF results through the await queue. History is bounded with `continue_as_new` when
  `is_continue_as_new_suggested()`, like autodev's `PollWorkflow`.
- **`MiaDataPoll`.** Runs on a schedule (every 2 min). It lists `mia-workflow-data` issues with `data:go`
  (checking the labeler has write access, as autodev does), and the `mia-snippets` issues and PRs it opened. It
  then signals the dataset workflows, using signal-with-start for a first `go`.
- **Agent jobs** (match, production) go through the shared csub runner (§12), one round directory each, exactly
  like autodev rounds. Test-crop jobs are plain LSF jobs running the harness CLI.
- **Replay test.** One test against a recorded `MiaDataDataset` history, as for autodev.

## 9. Trust boundaries

- **GitHub writes.** Agents never get GitHub credentials or network access to GitHub. The worker does every
  GitHub write: issues, comments, labels. autodev does the snippet branch pushes and the merge, as today.
- **Network.** Agent and harness jobs get a per-round allowlist: Anthropic, package hosts, and the hosts in the
  binding's or record's URLs. The worker checks every one against the record before submitting. The csub broker
  policy (`broker.toml`) must allow the catalog's data hosts. FTP through the allowlist proxy is unverified and
  gets tested first (the 4 FTP datasets).
- **Unreviewed code.** PR-head snippet code is agent-written and unreviewed, but it runs only inside a csub
  sandbox (test-crop job) with no GitHub access, no secrets, and read-write access only to its own round and test
  directories.
- **Untrusted data.** Never run code shipped with a dataset, never unpickle, and never install what a dataset
  names. Snippets enforce this by construction (§5.2, §5.5).
- **Required approval.** Merging into `mia-snippets` needs a human approval of the exact head commit (autodev),
  and branch protection with the required `check` workflow is the real gate.

## 10. Configuration (`mia_data.yaml`; details later)

```yaml
github: {data_repo: JaneliaSciComp/mia-workflow-data, snippets_repo: JaneliaSciComp/mia-snippets,
         catalog_repo: AI-HHMI/mia-agentic-search, auth: app, …}
temporal: {address: …, namespace: mia-data}
storage: {root: /path/TBD, public_url: null}        # public_url: HTTP base that serves root (e.g. fileglancer)
viewer:  {neuroglancer_template: null}
crops:   {per_round: 16, test_per_round: 4, per_slab: 4, budget_bytes: 20e9}
agent:   {…same shape as autodev's agent block…}
harness: {pixi_manifest: /path/to/mia-snippets/harness/pixi.toml}
```

## 11. Testing

**This repo** (no GitHub, LSF or Temporal server needed, like the existing tests):
- seed against a fixture of both downloads files and three records (idempotency, labels, `maybe-small`)
- the stage machine, with mocked activities in the time-skipping environment, covering:
  - the match → test → crop path
  - the develop path, with a revise and a merge
  - a closed PR
  - ineligible
- the status-comment rendering
- the replay test

**`mia-snippets`:**
- unit tests for the harness: sampling reproducibility, slab reuse, the content filter, the budget stop,
  `verify`, and `io` range/cache on a local HTTP server
- CI-check tests with good and bad PR file lists

**Integration (marked `integration`):** one real dataset end to end. Most small samples are ineligible
(bbbc034 Z=52, snemi3d Z=100), so the first run uses a slice-stack dataset that reaches ≥ 128 in Z once
stacked: one of the EMPIAR FIB-SEM records (`empiar-11746`, `empiar-12017`, `empiar-12661`).

## 12. Changes to existing code (step 0)

1. **Shared Temporal server.** The dev server that `scripts/autodev-up.sh` starts also creates namespace
   `mia-data` (`start-dev --namespace mia-data`), and a `mia-data` worker runs beside autodev's. autodev stays in
   its current namespace. Moving it would strand its in-flight workflows.
2. **No `common/` package yet.** `mia_data` imports `autodev.github.GitHub` (one client per repo: data, snippets,
   catalog), `autodev.auth` and the csub runner in `autodev/agent.py`. Its config supplies the same-shaped `agent`
   block and a `deployment` name. Move them into a shared package only when autodev-specific coupling gets in the
   way, e.g. `GitHub.for_autodev` or the prompt rendering in `agent.py`.
3. **autodev.**
   - `github.trusted_bots: [<mia-data app>[bot]]`, so the mia-data bot's `snippets:go` label counts as a valid
     trigger. Today only a human with write access does.
   - A `snippets` deployment config with `agent.allow_hosts` set to the catalog's data hosts, and `extra_mounts`
     giving read-only access to the harness environment, so its agent can run `harness smoke` on real data.

## 13. Open questions (defaults in bold)

1. **Datasets with an axis < 128** (up to 31 of 49 by record metadata): **ineligible** / pad / resample Z.
2. **Matched existing snippet:** **go straight to production** after passing test crops / wait for a human
   `data:go` again.
3. **Channels:** **one random channel per crop** / every channel, stored as `c` in one crop.
4. **Crop pyramid:** **s0 only** / a 64³ and 32³ pyramid as in miao#13.
5. **Where crops are served for the "view sample crops" link** (fileglancer over the storage root?). Until it is
   set, comments give file paths only.
6. **Production by an agent vs plain harness job.** The harness already samples deterministically, and the agent
   adds a visual quality review. If that review rarely rejects anything, production could become a plain job like
   test crops.

## 14. Build order

| # | milestone | done when |
|---|---|---|
| 0 | step 0 (§12.1) | `mia-data` namespace on the autodev server; empty `mia_data` worker connects; tests green |
| 1 | `mia-snippets`: `SPEC.md`, harness (`smoke`, `crop`, `verify`, `preview`, `io`), CI check, **two hand-written snippets** (`tensorswitch-auto`, `tiff-zip-member`) via normal human PRs | harness unit tests pass; `smoke` passes on 2 real datasets |
| 2 | `mia-data seed` | 49 issues in `mia-workflow-data`, re-run creates none |
| 3 | `MiaDataDataset` match → test → crop (no develop path) | one eligible dataset reaches `data:done` with verified crops |
| 4 | develop path: autodev `snippets` deployment (§12.3), PR detection, test crops on PR head, `pr-ready` status | one dataset goes through a new snippet PR, human approval and production |
| 5 | run over all eligible datasets | every issue is `done`, `ineligible` or `needs-human` with a reason |
