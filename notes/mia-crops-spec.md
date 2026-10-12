# MIA crops: snippets, development and production cropping — Spec

**Status:** draft v0.3 · 2026-10-11 · not yet approved. A proof of concept: 2D PNG crops only.
**Repos:** this repo (orchestration, new package `mia_data/`), [JaneliaSciComp/mia-workflow-data](https://github.com/JaneliaSciComp/mia-workflow-data) (issues only),
[JaneliaSciComp/mia-snippets](https://github.com/JaneliaSciComp/mia-snippets) (conversion snippets + their harness), [AI-HHMI/mia-agentic-search](https://github.com/AI-HHMI/mia-agentic-search) (read-only input)

## 0. Summary

The point of this version is to build the **architectural pieces** end to end, using the simplest crop that
exercises them. Generalising to 3D, 4D, zarr and multiscale comes later (§15).

Every dataset that mia-agentic-search has already downloaded successfully gets one issue in `mia-workflow-data`.
A human triggers the issue, and a durable Temporal workflow takes it from there:

1. **Match.** An agent finds an existing snippet in `mia-snippets` that can read the dataset's format, and
   writes a binding for it. The binding records which files to read and with which parameters.
2. **Develop** (only if no snippet fits). The worker opens an issue in `mia-snippets`, and autodev turns it
   into a PR adding a new snippet.
   - Test crops are extracted with the PR's snippet. The worker publishes a required check on that exact
     head commit, and updates the data issue to "PR ready" with a link to view the crops.
   - A human approves and autodev merges. A tree-hash comparison then confirms the merged snippet is the one
     that was tested.
3. **Produce.** A plain harness job (no agent) extracts the production crops with the merged snippet. The
   worker verifies them independently and publishes them.

A **crop**, in this version, is one 256×256 PNG: a single-channel XY patch taken from one plane of a source
volume at native resolution, together with the matching label patches as PNGs. Planes smaller than 256×256
are rejected. Only the bytes a crop needs are downloaded.

A **snippet** is a small, self-contained reader for one source format and layout. It is written to a fixed spec,
is independent of every other snippet, and is immutable once merged. Any change ships as a new snippet. Snippets
are not specific to 2D: the same readers will serve the 3D crops later.

## 1. Goals and non-goals

**Goals**
- One issue per downloaded dataset, created idempotently. Re-running the seed picks up new downloads.
- The same snippet can be used by every dataset whose format and layout it covers. A dataset only causes
  development when no existing snippet can read it.
- **A human approves every new snippet** (a PR review in `mia-snippets`) before production uses it, and that
  approval is tied to a passing crop check on the same head commit.
- **The data issue always shows where the dataset stands**, with links to the PR and to the sample crops.
- **Crops are reproducible and carry provenance.** Every crop can be regenerated from its recorded seed and
  coordinates, and records the dataset record and its commit, licence, source file and its validators, plane,
  box, and the snippet and harness versions that produced it.
- **Restarts recover.** Restarting the orchestrator reattaches to running cluster jobs, and publishing a
  crop again is a no-op. A failed agent process counts as a failed attempt; it is never resumed (§8.2).

**Non-goals (for now)**
- 3D, 4D or 2D+t crops; zarr output; multiscale pyramids (§15).
- Resampling, padding or stitching to reach 256×256. Smaller planes are rejected.
- Storage layout, quotas and retention. The roots and public URL are configuration (§10).
- Discovering datasets (mia-agentic-search does that), assembling a training set, or any change to
  mia-agentic-search.

## 2. Context

- **Input.** On mia-agentic-search's `state` branch, `state/downloads.json` (the TensorSwitch downloader) and
  `state/downloads-native.json` (the native downloader) have a `downloaded` map with the record id as key. Each
  entry holds `record_commit`, `scope`, `dataset_dir`, `crops` and `labels`.
  - As of 2026-10-11 the union is **49 datasets**, all `imaging.dimensionality: 3D`, all still on `main`.
  - The native downloader's `dataset_dir/crop-NNN.zarr/conversion.json` holds axis and transform decisions
    with reasons. The match agent should reuse them.
- **Size.** By `technical.arrays` shapes, **40 of the 49 have planes of at least 256×256**. The other 9 have
  smaller planes (e.g. 150×150, 128×128, 266×252), so their issues will end up `ineligible`.
- **Raw dtypes.** uint16 (24), uint8 (15), float32 (8), int16 (3), int8 (1). §6.3 maps each of these to PNG.
- **Licences.** Every one of the 49 has a known licence: CC-BY-4.0 (24), CC0 (12), custom (6), MIT (3),
  CC-BY-3.0 (2), CC-BY-NC-SA-3.0 (1), GPL-3.0 (1).
- **Sources.** 28 come from zips. Hosts: zenodo, ftp.ebi.ac.uk, Allen Cell S3, Broad, huggingface, figshare,
  cryoetdataportal. Formats: TIFF (31), OME-TIFF (7), MRC (7), HDF5 (6), and a few OME-Zarr, N5, CZI and ND2.
  Records write zip members as `<zip url>::<member>`, and `technical.arrays[]` gives each array's role, path
  pattern, axes, shape and dtype.
- **TensorSwitch** readers subclass `tensorswitch_v2.readers.base.BaseReader`, but there is no plugin mechanism.
  A snippet may wrap a TensorSwitch reader. It never patches TensorSwitch.
- **autodev** already implements "labelled issue → PR → revise → human approval → squash-merge" against one repo
  per deployment. Development reuses it as a second deployment, `snippets`, pointed at `mia-snippets`.

## 3. Components

| where | what | changes |
|---|---|---|
| `mia-workflow-data` | one issue per dataset; labels show its stage (§4) | new repo, issues only |
| `mia-snippets` | `SPEC.md`, `harness/`, `snippets/<name>/`, `WITHDRAWN.yaml`, CI checks | new repo |
| this repo, `mia_data/` | seed CLI, `MiaDataDataset`, `MiaDataPoll`, activities, match prompt | new package |
| this repo, `autodev/` | GitHub client and app auth, csub job runner, await-queue pattern | reused by import (§12) |
| this repo, autodev | `snippets` deployment, trusted bot trigger, `mia-data/crops` required check | changes in §12 |
| Temporal | namespace `mia-data` on the autodev dev server; queues `mia-data`, `mia-data-await` | step 0 (§12) |
| mia-agentic-search | records on `main`, downloads state on `state` | read-only |

The data workflow and the autodev `snippets` workflow live in different namespaces. They interact only through
GitHub (an issue and its PR in `mia-snippets`), never through Temporal signals.

## 4. Lifecycle of a data issue

Each stage is shown as a label `data:<stage>`, exactly one at a time, set only by the worker. `data:go` is the
human trigger.

```
data:new ──(human adds data:go)──> data:matching
  match found:      data:testing (matched snippets_commit) ──pass──> data:cropping ──> data:done
  no snippet fits:  data:developing ──PR opened──> data:testing (PR head) ──pass──> data:pr-ready
                                                      ^                              │
                                                      └────────new PR head───────────┤
                                                                                     │ merged
                                       snippet tree at merge == tree at tested head? ┘
                                          yes ──> data:cropping ──> data:done
                                          no  ──> data:testing (merge SHA) ──pass──> data:cropping
any stage ──> data:needs-human (comment says why) · data:ineligible (no plane ≥ 256×256)
```

- **Re-trigger.** A new `data:go` on a `done`, `needs-human` or `ineligible` issue starts a new round from
  matching, with a new seed. Rounds never overlap:
  - a `go` that arrives while a round is running is acknowledged with a "busy, ignored" comment and dropped
  - before a new round starts, any job left from the old round must be terminal (§8.2)

  A finished dataset gets additional crops this way. Earlier crops are never deleted.
- **Status comment.** One bot comment per issue is edited in place (found by a hidden marker). It shows the
  stage, the binding (§5.4), the snippet PR link, the sample-crop link and production totals. It is a
  display, not where execution state is stored (§8.1). Every stage change also posts a short new comment, so
  the history stays readable.

### 4.1 Eligibility

A raw volume is eligible if, once opened through its binding, it has Y ≥ 256 and X ≥ 256. Every other axis
(z, c) indexes planes. A dataset with no eligible raw volume goes to `data:ineligible`, and the comment gives
the shapes found.

## 5. Snippet spec (`mia-snippets/SPEC.md`, spec version 1)

### 5.1 Layout

```
mia-snippets/
├── SPEC.md                    this section, the contract (human-owned)
├── WITHDRAWN.yaml             snippets found to be wrong (human-owned, §5.6)
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
    axes: str                              # e.g. "zyx", "czyx", "yx": letters from "czyx", one per dimension
    shape: tuple[int, ...]
    dtype: numpy.dtype
    voxel_size_nm: dict[str, float] | None # per spatial axis, if the file says so, else None
    access: Literal["chunk", "plane", "file"]   # cheapest read unit (cost model, §6.2)
    def read(self, region: dict[str, int | slice]) -> numpy.ndarray   # e.g. {"c": 0, "z": 17, "y": slice(…), "x": slice(…)}
```

- **`Source`** holds:
  - `urls: list[str]`: one per file, sorted. There are several when the dataset is one file per Z slice.
    Zip members are written `<zip url>::<member>`.
  - `params: dict`: snippet-specific parameters declared in `snippet.yaml`, e.g. an HDF5 key or an axis
    override.
- **All I/O goes through `io`**, which counts bytes and enforces the job's byte cap (§6.5):
  - `io.open(url)`: a seekable binary file backed by HTTP or S3 range requests and an on-disk cache
  - `io.fetch(url)`: a local `Path` holding the whole file, staged once
  - `io.list(url)`: lists zip members or a directory

  `io` also records each file's validators (ETag, Last-Modified, size) for provenance. Snippets never open
  sockets, call `requests`/`urllib`/`fsspec` directly, run subprocesses or write outside `io`'s staging.
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
requires: [numpy, tifffile, imagecodecs]   # must be in the harness environment's allowlist
tested_on: [bbbc034-hipsc-nuclei-3d]       # = examples/*.yaml
supersedes: null
```

A snippet that needs a dependency outside the allowlist can't add it. The agent reports this, and a human
extends the harness environment.

### 5.4 Binding

A binding says how to read one dataset. It is data, not code. The worker writes it once per stage into the
round directory, and the workflow holds its path and hash (§8.1). The status comment shows a copy. A new
snippet also includes it as `examples/<record-id>.yaml`.

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
    pairs_with: 0                  # index into raw; shape must match on every axis except c
    label_key: manual_gt-nucleus-instance   # miao#13 label name, from the record (mia-agentic-search's rules)
    segmentation_type: instance    # instance | semantic | binary (decides PNG encoding, §6.3)
reasons: ["axes zyx from ImageJ metadata, matches record", …]   # one per non-default decision
```

`raw` may list several volumes (e.g. one per file set). Crops are drawn across them (§6.2).

### 5.5 Immutability and review (CI in `mia-snippets`)

`check.yml` runs on every PR, and branch protection requires it:
- **Only additions.** A PR may only add files under one new `snippets/<name>/`. It fails if it changes or
  deletes anything under an existing snippet, or touches `harness/`, `SPEC.md`, `WITHDRAWN.yaml` or
  `.github/`, unless the PR author is a listed human maintainer. autodev already refuses agent commits that
  touch `.github/`.
- **Valid metadata.** `snippet.yaml` validates against the harness schema, `name` matches the directory and
  isn't already used, every `requires` is on the allowlist, and every `tested_on` has an example binding.
- **Static import check.** It rejects network, subprocess and `os.system` imports, imports of other snippets,
  and `pickle` or `np.load(allow_pickle=True)`. This supplements the sandbox; it doesn't replace it.
- **Real-data tests run on the cluster, not on the CI runner.** The mia-data worker publishes the
  `mia-data/crops` check on the exact PR head SHA (§7.4), alongside a readable PR comment.
  - Branch protection requires both `check` and `mia-data/crops`, with the crop check restricted to the
    trusted app that publishes it.
  - Comments and a `data:pr-ready` label never authorise a merge.
  - Human-authored PRs (the first hand-written snippets) go through the same check via a manual admission
    command (§12).

### 5.6 Withdrawing a snippet

`WITHDRAWN.yaml` lists snippets that turned out to be wrong, e.g. one with a wrong axis order:

```yaml
- {name: tiff-zip-member, reason: "transposes yx on BigTIFF", at: 2026-11-02, by: <maintainer>}
```

Withdrawing is human-only (§5.5) and never deletes the snippet. Its effects:
- The match agent skips withdrawn and superseded snippets.
- The harness refuses to run a withdrawn snippet, so pending tests and production for it stop.
- `mia-data crops-by-snippet <name>` lists every published crop whose `crop.json` names that snippet, so they
  can be reviewed or removed.

## 6. Harness (`mia-snippets/harness/`)

The harness is the executable half of the spec. It is a pip-installable package with a CLI, run in a pixi
environment identified by the harness commit and its lockfile digest. Humans own it. Agents use it and never
modify it. Each round records the environment it was admitted with; updating the configured harness only
affects later rounds.

### 6.1 Commands

```
harness smoke   <binding.yaml> --snippets <dir>             # open every array, check axes/shape/dtype vs record,
                                                           # read one 256×256 patch per raw (+ its labels), report JSON
harness crop    <binding.yaml> --snippets <dir> --n 16 --seed S --out DIR [--existing MANIFEST]
                                                           # sample + write crops (§6.2, §6.3), report JSON
harness verify  DIR [--existing MANIFEST]                  # re-open written crops, run the checks of §6.4
harness preview DIR                                        # overlay PNG per crop + index.html gallery
```

`--snippets` is a read-only checkout of the exact SHA recorded in the workflow, never a branch resolved again.
Snippets are imported from that directory only. Every command takes its byte cap from the job environment, and
command-line options can't raise it (§6.5).

### 6.2 Sampling

- **Seed.** The workflow picks the seed when it admits a round (§8.1). With the same source data, binding,
  seed and pinned environment, the same crops come out.
- **Choose a volume and plane.** Each eligible raw volume is chosen with probability proportional to its
  number of 256×256 positions summed over its planes, so large volumes aren't underrepresented. A plane is then
  a uniform choice of the non-spatial indices (`z`, and `c` if there are several channels). A crop is always
  one channel.
- **Read cost, from `Array.access`:**
  - `chunk` (zarr, N5, chunked HDF5, uncompressed TIFF or MRC over range requests): read each 256×256 patch
    directly.
  - `plane` (compressed TIFF pages, one file per Z slice): read the plane once, then cut up to `per_plane`
    (default 4) crops from it.
  - `file` (deflated zip member, gzip, FTP): `io.fetch` the file once, then cut up to `per_file` (default 8)
    crops from it across its planes.
- **Content filter.** Reject a patch when the raw is effectively empty or saturated: standard deviation below
  a threshold, or more than 95% of pixels at one value. Redraw at most 10× per crop. Rejections are counted in
  the report. Thresholds are harness constants.
- **Overlap.** Reject a patch that overlaps any crop from the same volume, plane and channel by more than 25%
  of its area. "Any crop" covers crops already in this run and those in `--existing`, the dataset's published
  manifest.
- **Labels.** Each label paired with the chosen raw is read at the same plane and box. A label whose shape
  doesn't match its raw's makes `smoke` fail. It is never silently dropped.
- **Cleanup.** Download staging is deleted when the command exits. Output and reports stay in the job's
  workspace.

### 6.3 Output

Each crop is a directory named by its `crop_id`:

```
<crop_id>/                     crop_id = <round>-<batch>-<NNN>, e.g. r7f3a91-b0-003 (round = short hash of round_id)
├── raw.png                    256×256 grayscale
├── labels/<label_key>.png     256×256, one per paired label
└── crop.json                  provenance + encoding (below)
```

**Raw to PNG.** PNG holds only unsigned 8- or 16-bit greyscale, so every mapping is recorded in `crop.json`:

| source dtype | PNG | mapping |
|---|---|---|
| uint8, uint16 | 8-bit, 16-bit | none |
| int8, int16 | 8-bit, 16-bit | `+128` / `+32768` (lossless, offset recorded) |
| float32 and anything else | 16-bit | the crop's min→0 and max→65535, linear (lossy; min/max recorded) |

**Labels to PNG.**

| `segmentation_type` | PNG | mapping |
|---|---|---|
| binary | 8-bit, values 0/1 | none |
| semantic | 16-bit | values kept; a value > 65535 fails the crop |
| instance | 16-bit | values kept if ≤ 65535; otherwise renumbered 1…k within the crop (instance IDs are arbitrary) |

**`crop.json`** records:
- the record, `record_commit` and licence SPDX
- the source volume id (raw index and URL(s)), plus each source file's validators from `io`
- the plane `{z, c}` and box `{y, x, h: 256, w: 256}`
- `voxel_size_nm` and the physical extent in µm
- the source dtype and its PNG mapping
- raw intensity statistics: min, p1, p50, p99, max
- the label keys and their PNG mappings
- the snippet name and `snippets_commit` for each array, plus the harness commit and environment digest
- the seed and batch, `round_id`, `crop_id`, and bytes fetched

**Placement.** Published crops go in `<storage.root>/{modality}-{organism}-{name}/<crop_id>/`, with folder
naming as in mia-agentic-search's `tools/miao_layout.py`. `manifest.json` in that folder is an index rebuilt
from the `crop.json` files. Being present in the folder is what makes a crop published (§7.6).

### 6.4 Verification

Verification is deterministic. The worker runs it as its own sandboxed job and never takes an agent's word
for it. Every crop must pass these checks:
- every PNG decodes to 256×256 with the bit depth its mapping specifies
- the plane index and box lie within the source volume's shape
- raw passes the content filter, and no crop overlaps another (§6.2) by more than 25%
- each paired label is present
- `crop.json` is complete, including the licence and source validators
- the raw PNG's checksum is unique within the dataset, except that the same `crop_id` with the same checksum
  counts as already published

### 6.5 Limits

Limits are fixed bounds checked at admission, not a running ledger:
- **Every job has a byte cap and a walltime.**
  - The byte cap is enforced by `io` for harness reads. Whether agent-initiated downloads can be capped at the
    sandbox's allowlist proxy is to be checked; if they can't, walltime is the hard bound for them.
  - Agent jobs (match, and autodev's develop rounds) also run with `claude -p --max-turns`. There is no
    model-spend cap unless the CLI turns out to provide one.
- **Every round has a bounded number of jobs:**
  - match: up to `max_stage_attempts` (default 1)
  - test crops: one per PR head, with at most `max_pr_revisions` heads (default 5)
  - verification: one per test or production job
  - production: one
- **The round's worst case is therefore the per-job caps multiplied by the job count.** A config that can't
  produce a finite bound is rejected at startup.
- **Hitting a limit** stops the round as `needs-human`, reporting usage and keeping the surviving artifacts.
  Retrying infrastructure never grants a new allowance.

## 7. Steps in detail

### 7.1 Seed: `mia-data seed [--dry-run]`

1. Read both downloads files from mia-agentic-search's `state` branch and take the union of `downloaded`.
2. For each record id, read the record at `main` (still accepted) and note `record_commit` from the downloads
   entry.
3. Create an issue in `mia-workflow-data` unless one already carries `<!-- mia-data:record=<id> -->`:
   - **Title:** `<id>: <short_name or title>`.
   - **Body:** the marker, links to the record at `main` and at `record_commit`, licence, modality, organism,
     voxel size, `technical.arrays` (role, axes, shape, dtype), data size, source URL(s), which downloader
     succeeded, and its `dataset_dir`.
   - **Labels:** `data:new`, `modality:<family>`, and `has-labels` if the record has label arrays. Add
     `maybe-small` when no recorded raw shape has Y and X ≥ 256; it is a hint only, and §4.1 decides.
4. Rate limits: `autodev.github.GitHub` already throttles creates. Re-running only adds what's new.

### 7.2 Match (agent job)

**Input.** The round directory contains:
- the record YAML at `record_commit`
- the downloads entry, plus that downloader's `conversion.json` / `manifest.json` from `dataset_dir` (mounted
  read-only) when they exist
- every `snippet.yaml` and README from `mia-snippets` at the round's recorded commit, minus withdrawn and
  superseded snippets
- `SPEC.md` and the harness CLI

**Task.** The agent picks a snippet for each raw and label array, and writes the binding with its `reasons`.
It then runs `harness smoke` until the binding passes or the agent reaches its limits. It may try several
snippets and params. It may not write or alter snippet code.

**Output (`result.json`).** One of:
- `{"outcome": "matched", "binding": …}`
- `{"outcome": "needs_snippet", "draft_binding": …, "what_fails": …, "tried": […]}`
- `{"outcome": "ineligible", "shapes": …}`
- `{"outcome": "blocked", "reason": …}`

The `blocked` case covers things like an undecidable label class, or a dependency missing from the allowlist.

**Worker.** For `matched` and `ineligible`, it runs its own sandboxed `harness smoke` job, so the agent's
claim is checked rather than trusted. If that agrees, it records the binding (§8.1) and moves on to `testing`
or `ineligible`. `needs_snippet` goes to `developing`, and `blocked` to `needs-human`.

Match rules are mia-agentic-search's: never invent values, ask rather than guess (`blocked`), and require
evidence for any axis override or transform.

### 7.3 Develop (autodev `snippets` deployment)

1. The worker opens an issue in `mia-snippets`, with a hidden round marker so a retry finds it rather than
   opening a second one:
   - **title:** `New snippet for <format>/<container> (mia-workflow-data#N)`
   - **body:** the record excerpt, the match agent's `what_fails` and `tried`, the `draft_binding`, and links to
     `SPEC.md` and the data issue
   - **labels:** `snippets:go`, so autodev starts its first round
2. autodev's agent, running in its usual csub sandbox, adds `snippets/<new-name>/`, including
   `examples/<record-id>.yaml`. It runs `harness smoke` against the real data in its round, and autodev's
   trusted worker opens the PR. The agent doesn't need to know about mia-data.
3. `MiaDataPoll` finds the PR through autodev's `snippets/issue-K` branch and signals the data workflow with
   the PR number and head SHA. The data issue moves to `testing`.
4. Review works as in autodev: the reviewer requests changes or comments `/revise …`. Each new head SHA gets a
   fresh test-crop job (§7.4) and invalidates readiness for the old head.
5. A human approves the exact head, both required checks pass, and autodev squash-merges. The poller signals
   `merged` with the head SHA and the merge SHA. The worker then compares the `snippets/<name>` tree hash at
   the merge commit with the one at the tested head:
   - **Equal** (the normal case, since CI only allows a new directory): the head's test applies to the merge.
     The binding is pinned to the merge SHA and the issue moves to `cropping`.
   - **Different:** §7.4 runs again on the merge SHA, and only a pass admits `cropping`.
6. If the PR is closed without merging, or the issue gets `snippets:needs-human`, the data issue moves to
   `needs-human` with a link.

### 7.4 Test crops (plain job)

**Run.** One sandboxed LSF job runs three harness commands in turn, with the round's test seed and binding,
against exactly the recorded SHA (a PR head, a merge commit, or a matched binding's `snippets_commit`):
1. `harness crop --n 4`
2. `harness verify`
3. `harness preview`

Output goes to `<storage.root>/_tests/<record-id>/<round-id>/<sha>/`. Before adopting a PR's example binding,
the worker validates it: same record, URLs from the record, valid schema.

**Report.** The worker posts on the data issue, and on the PR if there is one:
- the verify results
- the bytes fetched
- a table of crop planes and boxes
- **a link to view the sample crops**: `storage.public_url` plus the path to the test's `index.html` gallery
  (§10)

For a PR head, the worker also publishes `mia-data/crops` (pending, then success or failure) on that exact SHA,
with the gallery link. A result for an old head or an old round stays visible as history, but never advances
the stage or overwrites the current check.

**Next stage:**
- **Current PR head passes:** `data:pr-ready`, with the PR link and the gallery link.
- **Matched existing snippet passes:** straight to `cropping`. That snippet was already human-approved, and
  verification is deterministic.
- **Merge commit passes** (only when tree hashes differed): `cropping`.
- **PR head fails:** the worker comments on the PR, the check stays failed, and the issue stays `testing`
  until a `/revise` produces a new head.
- **Matched snippet or merge commit fails:** `needs-human`, never production.

### 7.5 Production (plain job)

**Run.** One sandboxed LSF job runs `harness crop` with:
- the pinned binding and SHA
- `n` crops (config, default 16)
- the round's production seed
- `--existing` set to the dataset's current `manifest.json`

No agent is involved: the harness samples, filters and writes deterministically, and the human already judged
the sample crops at review time. A label-alignment check or an agent quality review can be added later if the
galleries show they're needed (§15).

**Worker.** After the job ends:
1. It runs `harness verify` as its own sandboxed job against the job's output.
2. It publishes the crops that pass (§7.6).
3. It posts the totals and the gallery link.
4. It sets `data:done`, or `needs-human` if fewer than half of `n` survive.

### 7.6 Publication

Publication is an idempotent Activity. Its input is the list of verified crops (`crop_id` and checksums) that
the verify step returned.

1. For each verified crop:
   - If `<dataset folder>/<crop_id>/` already exists with the same checksums, it was published by an earlier
     attempt: skip it.
   - If it exists with different checksums, stop and go to `needs-human`.
   - Otherwise copy it to `<dataset folder>/.<crop_id>.tmp/`, check the checksums, and rename it into place.
2. Rebuild `manifest.json` from the `crop.json` files: write it to a temporary file, then rename. Directories
   starting with `.` are ignored, and leftovers from an interrupted copy are deleted on the next run.

There is no number allocation and no lock:
- **One writer per dataset.** One workflow per dataset means one publisher per dataset folder.
- **Zombie attempts are harmless.** If an older attempt of the same Activity is still running, renaming onto
  an existing directory fails, so the first rename wins and the retry sees an identical crop.
- **Retries are always safe.** Retrying after a crash at any point gives the same result.

GitHub comments and totals are a projection of the result, not part of publishing.

## 8. Temporal design

- **Namespace and queues.** Namespace `mia-data`. Task queues are `mia-data` (workflows and short activities)
  and `mia-data-await` (long LSF waits), the same pattern as autodev. Workflow types are named explicitly:
  `MiaDataDataset` and `MiaDataPoll`.
- **`MiaDataDataset`.** Id `mia-data/<repo>/issue-<N>`. It is thin: a loop over the stages in §4, with every
  side effect in activities. It waits on signals (`go`, `snippet_pr`, `pr_head`, `merged`, `pr_closed`) and on
  LSF results through the await queue. `done`, `ineligible` and `needs-human` are dataset states; the workflow
  keeps running, waiting for the next `go`.
- **`MiaDataPoll`.** Runs on a schedule (every 2 min). It works like autodev's poller:
  - It lists `mia-workflow-data` issues labelled `data:go` and checks that the labeler has write access.
  - It signals the dataset workflow (signal-with-start) with the label event's ID, then removes the label.
  - It also observes the `mia-snippets` issues and PRs it opened, and signals PR number, head SHA and merge SHA.
- **Jobs.** Match, smoke, test, production and verify jobs all go through the csub runner (§12), with one
  workspace per job. Temporal owns submission, waiting and result collection, not the agent's tool loop.
  Activity inputs and results are small metadata and paths. Pixels, logs and transcripts stay out of history.

### 8.1 Identity and state: Temporal is the record

The workflow's own state, recorded in its history, is the durable record. There is no parallel state store.
- **`round_id`** is `<issue>-<label event id>`. The workflow keeps the set of event IDs it has admitted, so a
  redelivered `go` is a no-op.
- **The seed** comes from `workflow.random()` at admission, so it is recorded in history. Test and production
  seeds are derived from it.
- **What the workflow carries:** the record commit, the `snippets_commit`, the harness commit and environment
  digest, the binding's path and hash, PR and tested/merged SHAs, job IDs, and verify and publication results.
  An admitted stage never consults a branch, an environment variable or an issue comment again.
- **Inputs on disk** (record snapshot, binding, prompt) are written once per stage into `paths.state_dir`,
  which is never mounted into agent jobs. Job workspaces live under `paths.work_root`, and each job sees only
  its own workspace plus approved read-only inputs.
- **Missing inputs or outputs fail closed:** the round goes to `needs-human` and is never silently recreated.

### 8.2 Jobs and recovery

Reuse autodev's runner semantics. Before submitting, it looks up the job by its workspace (the job-ID file and a
cwd match), so a retried submission reattaches instead of submitting twice. The await Activity polls with a
heartbeat, and a worker restart reattaches to the same job.

- **A job that fails is a failed attempt.** Its output is kept. With `max_stage_attempts=1` (the default) the
  round goes to `needs-human`. A configured retry makes a new attempt only after the old job is terminal.
- **Agent sessions are not resumable.** `claude -p` doesn't checkpoint its session, so a killed agent is a
  failed attempt. A partial `result.json` is not a success.
- **Uncertain job state blocks new work.** If a job's state is uncertain (e.g. scheduler records have
  expired), the round goes to `needs-human` instead of starting overlapping work. A monitoring timeout doesn't
  mean LSF stopped the job.
- **Errors.** Transient GitHub and monitoring errors retry with finite deadlines. Input, authorisation and
  verification errors don't retry.

### 8.3 Events and revisions

- **Go events.** The poller delivers at least once, and the workflow deduplicates by event ID (§8.1). The poller
  removes the label only after the signal is accepted. A missed removal just means a redelivery that is
  ignored.
- **PR observations** carry the PR number, head SHA and merge SHA, and duplicates are no-ops.
- **Arrival order is not revision order.** On every PR signal, the workflow asks GitHub (an Activity) for the
  current head instead of trusting the order signals arrive in.
- **Results must match the expected test.** A test result advances the stage only if its round, SHA and test
  ID all match the test the workflow is waiting for. A stale result is kept as history and nothing more.
- **GitHub writes** (issues, comments, check runs) carry hidden markers or stable external IDs, so a retry
  finds the earlier write instead of repeating it.

### 8.4 Continue-As-New

The workflow rolls over when `is_continue_as_new_suggested()` (as autodev's `PollWorkflow` does), at an idle
wait or a stage boundary with no Activity in flight. It carries one schema-versioned state object: everything
listed in §8.1, plus the admitted event IDs and any pending signals. It drains pending signals before rolling
over, so none are lost. A state that can't be carried fails explicitly; it never restarts from matching.

See [Temporal agents and failure boundaries](temporal-agents-opencompany.md) for the rationale behind the
coarse-grained split: Temporal orchestrates jobs, agents run inside them.

## 9. Trust boundaries

- **GitHub writes.** Agents never get GitHub credentials or network access to GitHub. The worker does every
  GitHub write: issues, comments, labels, check runs. autodev does the snippet branch pushes and the merge.
- **Network.** Agent and harness jobs get a per-job allowlist: Anthropic (agent jobs only), package hosts,
  and the hosts in the binding's URLs, each checked against the record. The csub broker policy (`broker.toml`)
  must allow the catalog's data hosts. FTP through the allowlist proxy is unverified and is tested first.
- **Unreviewed code.** PR-head snippet code is agent-written and unreviewed. It runs only in csub sandboxes,
  with no GitHub access, no secrets, and write access to its own workspace only. The credential-bearing worker
  never imports a snippet; even its own smoke and verify checks run as sandboxed jobs.
- **Untrusted data.** Never run code shipped with a dataset, never unpickle, and never install what a dataset
  names.
- **Required approval.** Merging into `mia-snippets` needs human approval of the exact head, plus the required
  `check` and `mia-data/crops` checks on that head. Production additionally needs the merged snippet tree to
  match the tested one, or a passing test of the merge commit. A late result for an old head satisfies
  neither.

## 10. Configuration (`mia_data.yaml`)

```yaml
github:   {data_repo: JaneliaSciComp/mia-workflow-data, snippets_repo: JaneliaSciComp/mia-snippets,
           catalog_repo: AI-HHMI/mia-agentic-search, auth: app, …}
temporal: {address: …, namespace: mia-data}
paths:    {state_dir: /shared/TBD/mia-data-state, work_root: /shared/TBD/mia-data-work}
storage:  {root: /shared/TBD/mia-crops, public_url: null}   # public_url: HTTP base serving root (e.g. fileglancer)
crops:    {size: 256, per_round: 16, test_per_round: 4, per_plane: 4, per_file: 8}
limits:   {job_bytes: 5e9, job_walltime_min: 120, agent_max_turns: 200,
           max_stage_attempts: 1, max_pr_revisions: 5}
agent:    {…same shape as autodev's agent block…}
harness:  {pixi_manifest: /path/to/mia-snippets/harness/pixi.toml}
```

All paths must be on storage that the worker and the cluster jobs both see. Config changes apply to new
rounds, never to an admitted one.

## 11. Testing

**This repo.** Unit tests need no external services. Workflow tests use Temporal's time-skipping test server
with stub Activities.
- **Seed** against a fixture of both downloads files and three records: idempotency, labels, `maybe-small`.
- **The stage machine:**
  - the match → test → crop path
  - the develop path, with a revise, a merge with equal tree hashes, and a merge with different tree hashes
  - a closed PR
  - ineligible
  - a limit being hit
- **Event handling:**
  - a redelivered `go`, and a `go` while busy
  - a stale PR head or test result arriving late
  - a merge arriving before its test finishes
  - signals arriving during Continue-As-New
  
  In every case: no extra round, no lost event, and no production without matching verification.
- **Recovery:**
  - worker loss after a job is submitted but before its ID is recorded: reattach, don't resubmit
  - worker loss while awaiting a job
  - worker loss after the job finishes but before its result is recorded
  
  Each must reattach rather than resubmit. Missing artifacts must fail closed.
- **Publication** interrupted mid-copy, after renames, and after the manifest rebuild: retry gives the same
  crops and totals.
- **Pinning:** changing config, `main` or the harness between retries doesn't change an admitted round.
- **The status comment** renders correctly, and the **replay test** passes on recorded histories for both
  paths.

**`mia-snippets`:**
- harness unit tests:
  - sampling reproducibility, the plane and file batching, the content filter and overlap rejection
  - the PNG mappings for every dtype in §6.3
  - the byte cap, and `io` range and cache behaviour against a local HTTP server
- CI-check tests with good and bad PR file lists, including `WITHDRAWN.yaml` edits by a bot

**Integration (marked `integration`).** One real dataset end to end. `bbbc034-hipsc-nuclei-3d` is the first
candidate: a zip member with labels and 1024×1024 planes. Before scaling out, also exercise:
- replacing the worker against real csub
- check-run publication and branch protection on a real PR

## 12. Changes to existing code (step 0)

1. **Shared Temporal server.** The dev server that `scripts/autodev-up.sh` starts also creates namespace
   `mia-data` (`start-dev --namespace mia-data`), and a `mia-data` worker runs beside autodev's. autodev stays in
   its current namespace. Moving it would strand its in-flight workflows.
2. **No `common/` package yet.**
   - `mia_data` imports `autodev.github.GitHub` (one client per repo: data, snippets, catalog), `autodev.auth`
     and the csub runner in `autodev/agent.py`.
   - Split `agent.py` so that submit/await is separate from the issue-to-PR prompt and the git-bundle
     collection. Match jobs and plain harness jobs can then supply their own command and result type.
   - Move code into a shared package only when coupling warrants it.
3. **autodev.**
   - `github.trusted_bots: [<mia-data app>[bot]]`, so the mia-data bot's `snippets:go` label counts as a valid
     trigger. Today only a human with write access does.
   - A `snippets` deployment config with `agent.allow_hosts` set to the catalog's data hosts, and `extra_mounts`
     giving read-only access to the pinned harness environment, so its agent can run `harness smoke` on real
     data.
   - Require `mia-data/crops` alongside `check` and exact-head approval before merging. Add check-run
     publishing to the GitHub client and the app's permissions.
4. **`mia-data test-pr <pr> --binding <file>`.** A manual admission command that runs §7.4 on an explicit PR
   head and publishes the check. Human-authored snippet PRs, including the two bootstrap ones, need it before
   any data issue exists. It doesn't bypass approval.

## 13. Open questions (defaults in bold)

1. **Float to PNG:** **per-crop min/max** / per-dataset percentiles (needs a stats pass first).
2. **Matched existing snippet:** **go straight to production** after test crops pass / wait for a human `data:go`.
3. **Licences:** **crop everything and record the licence**; the training-set builder filters later.
   Alternatively, skip NC, GPL and custom licences now (8 of 49).
4. **Gallery hosting:** where `storage.public_url` points (fileglancer over the storage root?). Until it's set,
   comments give file paths.
5. **Byte cap for agent downloads:** can the agentic-sandbox allowlist proxy count bytes per job? If not,
   walltime is the bound (§6.5).

## 14. Build order

| # | milestone | done when |
|---|---|---|
| 0 | step 0 (§12.1–12.2) | `mia-data` worker connects to its namespace; `agent.py` split; autodev tests green |
| 1 | `mia-snippets`: `SPEC.md`, harness (`smoke`, `crop`, `verify`, `preview`, `io`), CI check, two hand-written snippets (`tiff-zip-member`, `tensorswitch-auto`) | harness tests pass; `test-pr` posts a passing `mia-data/crops` check on both PR heads before human merge |
| 2 | `mia-data seed` | 49 issues in `mia-workflow-data`; a re-run creates none |
| 3 | `MiaDataDataset`: match → test → crop → publish (no develop path) | bbbc034 reaches `data:done` with 16 verified PNG crops and a gallery; a redelivered `go`, a worker restart and an interrupted publication don't duplicate anything |
| 4 | develop path: autodev `snippets` deployment (§12.3), PR detection, head-bound test crops, tree-hash merge check | one dataset gets a new snippet through a PR, human approval and production; a stale head's result can't pass a gate |
| 5 | run over all eligible datasets | every issue is `done`, `ineligible` or `needs-human` with a reason |

## 15. Later

- **3D and 4D crops** (128³ and beyond) in zarr with multiscale pyramids. Snippets are already n-dimensional,
  so this is a harness change (sampler, writer, verify) plus slab staging for `plane` and `file` access.
- **Anisotropy limits** for 3D crops.
- **2D datasets from the catalog**, not just planes of 3D volumes.
- **A label-alignment check or an agent quality review** in production, if the galleries show bad crops get
  through.
- **Byte metering across a whole round**, if fixed per-job caps turn out to be too loose.
- **A training-set builder** over the manifests: licence filters, splits by source volume, normalisation from
  the recorded intensity statistics.
- **A `common/` package**, once a third app shares autodev's pieces.
