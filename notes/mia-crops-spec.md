# MIA crops: snippets, development and production cropping — Spec

**Status:** draft v0.9 · 2026-10-12 · not yet approved. A proof of concept: 2D PNG crops only.
v0.9: test crops stay in their job workspaces (§7.4); jobs run the harness environment's entry point directly (§6).
**Repos:** this repo (orchestration, new package `mia_data/`), [JaneliaSciComp/mia-workflow-data](https://github.com/JaneliaSciComp/mia-workflow-data) (issues and crop galleries),
[JaneliaSciComp/mia-snippets](https://github.com/JaneliaSciComp/mia-snippets) (conversion snippets + their harness), [AI-HHMI/mia-agentic-search](https://github.com/AI-HHMI/mia-agentic-search) (read-only input)

## 0. Summary

The point of this version is to build the **architectural pieces** end to end, using the simplest crop that
exercises them. Generalising to 3D, 4D, zarr and multiscale comes later (§15).

Every dataset that mia-agentic-search has already downloaded successfully, under an allowed licence, gets one
issue in `mia-workflow-data`. A human triggers the issue, and a durable Temporal workflow takes it from there:

1. **Match.** An agent finds an existing snippet in `mia-snippets` that can read the dataset's format, and
   writes a binding: which files to read, with which parameters.
2. **Develop** (only if no snippet fits). The worker opens an issue in `mia-snippets`, and autodev turns it
   into a PR adding a new snippet together with the binding it was tested on.
3. **Test and review.** Test crops are extracted, committed to a `galleries` branch of `mia-workflow-data`
   and shown in the issue. **A human approves every binding before production.**
   - A new snippet is approved by reviewing its PR. The worker publishes a required check on the PR's exact
     head commit, and after the merge a tree-hash comparison confirms that the merged snippet is the one that
     was tested.
   - A matched binding is approved on the data issue, with a `data:approve` label.
4. **Produce.** A plain harness job (no agent) extracts the production crops. The worker verifies them in a
   separate job and publishes them.

A **crop**, in this version, is one 256×256 PNG: a single-channel XY patch from one plane of a source volume, at
native resolution, together with the matching label patches as PNGs. Planes smaller than 256×256 are rejected.

**Downloads.** Where the source allows range reads (HTTP or S3 files, stored zip members, chunked formats),
only the bytes a crop needs are downloaded. Most of the catalog doesn't allow it: 28 of the 49 datasets come
from zips, whose members are usually deflated, so a member is fetched whole. Such fetches are cached for the
dataset's later jobs (§6.6), so a member is downloaded once per dataset rather than once per job.

A **snippet** is a small, self-contained reader for one source format and layout. It is written to a fixed spec,
is independent of every other snippet, and is immutable once merged. Any change ships as a new snippet. Snippets
are not specific to 2D: the same readers will serve the 3D crops later.

## 1. Goals and non-goals

**Goals**
- One issue per downloaded dataset, created idempotently. Re-running the seed picks up new downloads.
- The same snippet can be used by every dataset whose format and layout it covers. A dataset only causes
  development when no existing snippet can read it.
- **A human approves every binding and every new snippet before production.** Approval always follows a look
  at test crops made from that exact binding and snippet commit. New snippets are approved by a PR review that
  is tied to a passing crop check on the same head. Matched bindings are approved with `data:approve` on the
  data issue.
- **The data issue always shows where the dataset stands**, with a link to the PR and the sample crops inline.
- **Crops are reproducible and carry provenance.** Every crop can be regenerated from its recorded seed and
  coordinates, and records the dataset record and its commit, licence, source file and its validators, plane,
  box, and the snippet and harness versions that produced it.
- **Restarts recover.** Restarting the orchestrator reattaches to running cluster jobs, and publishing a crop
  again is a no-op. A killed agent counts as a failed attempt; it is not resumed (§8.2).

**Non-goals (for now)**
- 3D, 4D or 2D+t crops; zarr output; multiscale pyramids (§15).
- Resampling, padding or stitching to reach 256×256. Smaller planes are rejected.
- Storage layout, quotas and retention. The roots and public URL are configuration (§10).
- Datasets under non-commercial, copyleft or custom licences (§7.1).
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
  smaller planes (e.g. 150×150, 128×128, 266×252).
- **Raw dtypes.** uint16 (24), uint8 (15), float32 (8), int16 (3), int8 (1). §6.3 maps each of these to PNG.
- **Licences.** CC-BY-4.0 (24), CC0 (12), custom (6), MIT (3), CC-BY-3.0 (2), CC-BY-NC-SA-3.0 (1),
  GPL-3.0 (1). The last three groups (8 datasets) are excluded for now (§7.1).
- **Sources.** 28 come from zips. Hosts: zenodo, ftp.ebi.ac.uk, Allen Cell S3, Broad, huggingface, figshare,
  cryoetdataportal. Formats: TIFF (31), OME-TIFF (7), MRC (7), HDF5 (6), and a few OME-Zarr, N5, CZI and ND2.
  Records write zip members as `<zip url>::<member>`, and `technical.arrays[]` gives each array's role, path
  pattern, axes, shape and dtype.
- **csub networking.** The policy's `allowed_hosts` is a static list of fnmatch patterns in the service
  account's `broker.toml`, so `*.huggingface.co` works there.
  - Each job requests its own `allow_hosts`. The broker rejects the whole job if any requested host matches no
    pattern (`csub/broker/resolve.py`, `check_hosts`). It passes the hosts that survive to the sandbox as one
    `--allow` per host (`wrapper.py`, `_allow_flags`). `csub probe` returns the policy's patterns.
  - Agent jobs get the Anthropic hosts from the policy's `[agents]` table (`claude = [...]`), not from the job's
    request. In `deploy/broker.janelia.toml` that table is commented out, so the service account's policy must
    enable it.
  - **Egress goes through an HTTPS proxy only, so FTP doesn't work.** The ftp.ebi.ac.uk datasets must use
    EMPIAR's `https://ftp.ebi.ac.uk/...` mirror; their records already give https URLs.
- **The record's host is often not where the bytes come from.** Several hosts redirect downloads elsewhere,
  and the proxy blocks a redirect target that isn't allowed:
  - figshare's `ndownloader.figshare.com` redirects to an S3 bucket host
  - huggingface redirects LFS files to a CDN or xet host
  - zenodo sometimes bounces through `zenodo.org/api/...` on the same host

  Following the chain is the only way to learn it (§12.5).
- **TensorSwitch** readers subclass `tensorswitch_v2.readers.base.BaseReader`, but there is no plugin mechanism.
  A snippet may wrap a TensorSwitch reader. It never patches TensorSwitch.
- **autodev** already implements "labelled issue → PR → revise → human approval → squash-merge" against one repo
  per deployment. Development reuses it as a second deployment, `snippets`, pointed at `mia-snippets`.
- **Claude Code CLI.** `claude -p` supports `--max-budget-usd` (print mode only) and can resume sessions
  (`--resume`). Agent jobs use the first; this design chooses not to use the second (§8.2).

## 3. Components

| where | what | changes |
|---|---|---|
| `mia-workflow-data` | one issue per dataset; labels show its stage (§4); a `galleries` branch holds the test and production crops (§7.4) | new repo |
| `mia-snippets` | `SPEC.md`, `harness/`, `snippets/<name>/`, `WITHDRAWN.yaml`, CI checks | new repo |
| this repo, `mia_data/` | seed CLI, `MiaDataDataset`, `MiaDataPoll`, activities, match prompt, `test-pr` | new package |
| this repo, `autodev/` | GitHub client and app auth, csub job runner, await-queue pattern | reused by import (§12) |
| this repo, autodev | `snippets` deployment, trusted bot trigger, required status contexts | changes in §12 |
| Temporal | one dev server; namespace `mia-data` (queues `mia-data`, `mia-data-await`), namespace `snippets` | step 0 (§12) |
| mia-agentic-search | records on `main`, downloads state on `state` | read-only |

The data workflow and the autodev `snippets` workflow live in different namespaces. They interact only through
GitHub (an issue and its PR in `mia-snippets`, and the `mia-data/crops` commit status), never through Temporal
signals.

## 4. Lifecycle of a data issue

Each stage is shown as a label `data:<stage>`, exactly one at a time, set only by the worker. `data:go` and
`data:approve` are human labels, accepted only from people with write access.

```
data:new ──(data:go)──> data:matching
  match found:     data:testing (matched snippets_commit) ──pass──> data:review ──(data:approve)──> data:cropping ──> data:done
  no snippet fits: data:developing ──PR opened──> data:testing (PR head) ──pass──> data:pr-ready
                                                     ^                              │
                                                     └────────new PR head───────────┤
                                                                                    │ merged (human-approved PR)
                                      snippet tree at merge == tree at tested head? ┘
                                         yes ──> data:cropping ──> data:done
                                         no  ──> data:testing (merge SHA) ──pass──> data:cropping
any stage ──> data:needs-human (comment says why) · data:ineligible (no plane ≥ 256×256, or licence excluded)
```

- **Approve (matched path).** `data:approve` counts only while the issue is in `data:review`, and only for
  the round that is under review: its label event must be newer than the round's test result. Anywhere else
  it is removed with an "ignored" comment.
- **Re-trigger.** A new `data:go` on a `done`, `needs-human` or `ineligible` issue starts a new round from
  matching, with a new seed. Rounds never overlap:
  - a `go` that arrives while a round is running is acknowledged with a "busy, ignored" comment and dropped
  - a new round starts only after every job from the previous one is terminal (§8.2)

  A finished dataset gets additional crops this way. The system never deletes a crop (§5.6).
- **Status comment.** One bot comment per issue is edited in place (found by a hidden marker). It shows the
  stage, the binding (§5.4) and its hash, the snippet PR link, the latest crops and production totals. It is
  a display, not where execution state is stored (§8.1). Every stage change also posts a short new comment, so
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
├── ci/                        PR rules (human-owned, §5.5)
├── .github/workflows/check.yml
└── snippets/<name>/           one directory per snippet, immutable once merged
    ├── snippet.yaml           metadata (§5.3)
    ├── snippet.py             the reader (§5.2)
    ├── README.md              what it reads, how params are chosen, known limits
    └── examples/<record-id>.yaml   ≥ 1 binding it was tested on (§5.4)
```

`<name>` is a lowercase slug that is never reused, e.g. `tiff-zip-member`, `mrc-http`, `tiff-slices-zip`,
`hdf5-dataset`. A changed snippet is a new directory with a new name, such as `tiff-zip-member-2`, and its
`snippet.yaml` sets `supersedes: tiff-zip-member`. Bindings keep pointing at the version they were tested with.

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
    access: Literal["chunk", "plane", "file"]   # cheapest read unit for THIS file (§6.2); may differ per file
    def read(self, region: dict[str, int | slice]) -> numpy.ndarray   # e.g. {"c": 0, "z": 17, "y": slice(…), "x": slice(…)}
```

- **`Source`** holds:
  - `urls: list[str]`: one per file, sorted. There are several when the dataset is one file per Z slice.
    Zip members are written `<zip url>::<member>`.
  - `params: dict`: snippet-specific parameters declared in `snippet.yaml`, e.g. an HDF5 key or an axis
    override.
- **`access` is reported at runtime** by the opened `Array`, because the same snippet can meet a stored zip
  member in one dataset and a deflated one in the next. `snippet.yaml` doesn't declare it.
- **All I/O goes through `io`**, which counts network bytes and enforces the job's byte cap (§6.5):
  - `io.open(url)`: a seekable binary file backed by HTTP or S3 range requests and an on-disk cache
  - `io.fetch(url)`: a local `Path` holding the whole file, staged once
  - `io.list(url)`: lists zip members

  `io` records each file's validators (ETag, Last-Modified, size) for provenance, and refuses a server that
  answers a range request with the whole body.
- **`io` follows redirects itself** and checks every hop's host against the hosts the job was granted
  (`MIA_HARNESS_ALLOWED_HOSTS`, set by the worker) before requesting it. A hop outside that set raises
  `HostNotAllowed`, with the host and the redirect chain so far, instead of failing later as a generic proxy or
  connection error. `ftp://` URLs raise the same error. The harness reports it as `host_not_allowed` in the
  command's JSON output, and the match agent's prompt says to report `blocked` with that host rather than
  work around it.
- Snippets never open sockets, call `requests`/`urllib`/`fsspec`
  directly, run subprocesses or write outside `io`'s staging.
- **Pure readers.** A snippet reads; it never crops, samples, resamples, normalises or writes output. Those belong
  to the harness, so every crop goes through the same code.
- **Independent.** A snippet imports only the harness API, its own module and the dependencies on the allowlist
  (§5.3). It never imports another snippet. Shared code goes into a new snippet, not into a common module.

### 5.3 `snippet.yaml`

```yaml
name: tiff-zip-member
spec_version: 1
summary: One TIFF from a zip member or a plain URL; deflated members are fetched whole.
formats: [tiff, ome-tiff]          # mia-agentic-search data.formats values it handles
containers: [zip, http]            # zip | http | s3 | local
params:                            # every param the binding may set; nothing else is accepted
  axes: {type: str, required: false, doc: "override when the TIFF's own axes are wrong; reason required in binding"}
requires: [numpy, tifffile, imagecodecs]   # must be in the harness environment's allowlist
tested_on: [bbbc034-hipsc-nuclei-3d]       # = examples/*.yaml
supersedes: null
```

A snippet that needs a dependency outside the allowlist can't add it. The agent reports this, and a human
extends the harness environment.

### 5.4 Binding

A binding says how to read one dataset. It is data, not code. The worker writes it once per stage into
`paths.state_dir`, and the workflow holds its path and hash (§8.1). The status comment shows a copy. A new
snippet's PR includes the binding as `examples/<record-id>.yaml`, so its reviewer reviews both.

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
  deletes anything under an existing snippet, or touches `harness/`, `ci/`, `SPEC.md`, `WITHDRAWN.yaml` or
  `.github/`, unless the PR author is a listed human maintainer. autodev already refuses agent commits that
  touch `.github/`.
- **Valid metadata.** `snippet.yaml` validates against the harness schema, `name` matches the directory and
  isn't already used, every `requires` is on the allowlist, and every `tested_on` has an example binding.
- **Static import check.** It rejects network, subprocess and `os.system` imports, `__import__`, imports of other
  snippets, and `pickle` or `np.load(allow_pickle=True)`. This supplements the sandbox; it doesn't replace it.
- **Real-data tests run on the cluster, not on the CI runner.** The mia-data worker publishes a
  `mia-data/crops` **commit status** on the exact PR head SHA (§7.4), alongside a readable PR comment.
  - Branch protection requires both `check` and `mia-data/crops`, with the status restricted to the trusted
    app that publishes it.
  - Comments and labels never authorise a merge.
- **PRs that add no snippet** (harness, `ci/`, `SPEC.md` or `WITHDRAWN.yaml` changes by a maintainer) get
  `mia-data/crops` = success, "no snippet in this PR", from the poller (§8). The status is set when the head's
  diff against `main` touches nothing under `snippets/`.
- **Human-authored snippet PRs** (the first hand-written snippets) get their status from the manual
  admission command `mia-data test-pr` (§12).

### 5.6 Withdrawing a snippet

`WITHDRAWN.yaml` lists snippets that turned out to be wrong, e.g. one with a wrong axis order:

```yaml
- {name: tiff-zip-member, reason: "transposes yx on BigTIFF", at: 2026-11-02, by: <maintainer>}
```

Withdrawing is human-only (§5.5) and never deletes the snippet. Its effects:
- The match agent skips withdrawn and superseded snippets.
- The harness refuses to run a withdrawn snippet, so pending tests and production for it stop.
- **Published crops are never deleted by the system.** When `manifest.json` is rebuilt (on every publication,
  or with `mia-data reindex`), every crop whose `crop.json` names a withdrawn snippet is marked
  `withdrawn: true` with the reason. Consumers must skip those crops.
- `mia-data crops-by-snippet <name>` lists them. Deleting files is a manual operator action outside the
  system.

## 6. Harness (`mia-snippets/harness/`)

The harness is the executable half of the spec. It is a pip-installable package with a CLI, run in a pixi
environment. Humans own it. Agents use it and never modify it.

- **Pinning.** `harness.dir` is a configured checkout of `mia-snippets` with `pixi install` done in `harness/`.
  Jobs run `<harness.dir>/harness/.pixi/envs/default/bin/harness` directly, not `pixi run`, which wants the
  install lock on a prefix that jobs mount read-only. A round
  records that checkout's `git rev-parse HEAD` and the sha256 of `harness/pixi.lock` when it is admitted. A
  job is refused, sending the round to `needs-human`, if the checkout has moved since. `ponytail:` one
  environment per harness commit, if harness updates ever need to overlap in-flight rounds.
- **Harness and snippets are pinned separately.** A snippet snapshot (§7.2) contains only `snippets/`,
  `WITHDRAWN.yaml` and `SPEC.md` from its SHA, never `harness/`. The harness never imports anything from a
  snapshot except `snippets/<name>/snippet.py`, through its loader. So a snapshot whose own `harness/`
  differs from the pinned one doesn't matter.

### 6.1 Commands

```
harness smoke   <binding.yaml> --snippets <dir> --record <record.yaml>   # open every array, check vs record,
                                                                       # read one 256×256 patch per raw (+ labels)
harness crop    <binding.yaml> --snippets <dir> --record <record.yaml> --n N --seed S --round R --batch B
                --out DIR [--existing MANIFEST]                        # sample + write crops (§6.2, §6.3)
harness verify  DIR --binding <binding.yaml> [--existing MANIFEST]     # §6.4 checks; loads no snippet code
harness preview DIR --out PREVIEW                                      # label overlay PNG per crop (§6.7)
```

`--snippets` is a read-only snapshot of the exact SHA recorded in the workflow, never a branch resolved again.
Snippets are imported from that directory only. Every command takes its byte cap from the job environment, and
command-line options can't raise it (§6.5).

### 6.2 Sampling

- **Seed.** The workflow picks the seed when it admits a round (§8.1). With the same source data, binding,
  seed and pinned environment, the same crops come out.
- **Choose a volume and plane.** Each eligible raw volume is chosen with probability proportional to its
  number of 256×256 positions summed over its planes. A plane is a uniform choice of the non-spatial indices
  (`z`, and `c` if there are several channels). A crop is always one channel.
- **Capacity.** A dataset's capacity is its number of non-overlapping tiles:
  Σ over eligible volumes of planes × ⌊Y/256⌋ × ⌊X/256⌋. The requested `n` is lowered to at most half the
  capacity, and both the lowered `n` and the capacity are reported. A small dataset therefore isn't asked for
  crops it can't hold.
- **Read cost**, from the opened file's `Array.access`:
  - `chunk` (zarr, N5, chunked HDF5, uncompressed TIFF or MRC over range requests): read each 256×256 patch
    directly.
  - `plane` (compressed TIFF pages, one file per Z slice): read the plane once, then cut up to `per_plane`
    (default 4) crops from it.
  - `file` (deflated zip member, server without range support): `io.fetch` the file once, through the dataset
    cache (§6.6), then cut up to `per_file` (default 8) crops from it across its planes.
- **Rejections.** A candidate patch is rejected when:
  - **Content:** the raw is effectively empty or saturated (standard deviation below a threshold, or more
    than 95% of pixels at one value), or more than 5% of pixels are non-finite.
  - **Overlap:** it overlaps by more than 25% of its area any crop from the same volume, plane and channel,
    whether that crop was made in this run or is already in `--existing` (the published manifest).

  Both kinds share one redraw budget: at most `10 × n` candidates are drawn per run. Rejections are counted
  by kind in the report. Thresholds are harness constants.
- **Labels.** Each label paired with the chosen raw is read at the same plane and box. A label whose shape
  doesn't match its raw's makes `smoke` fail. It is never silently dropped.
- **Cleanup.** The job's private staging is deleted when the command exits. The dataset cache (§6.6), the
  output and the reports remain.

### 6.3 Output

Each crop is a directory named by its `crop_id`:

```
<crop_id>/                     crop_id = <round_id>-b<batch>-<NNN>, e.g. i12-e15599412345-b1-003
├── raw.png                    256×256 grayscale
├── labels/<label_key>.png     256×256, one per paired label
└── crop.json                  provenance + encoding (below)
```

`round_id` is `i<issue>-e<label event id>` (§8.1): short, readable and collision-free, because GitHub event IDs
are unique.

**Raw to PNG.** PNG holds only unsigned 8- or 16-bit greyscale, so every mapping is recorded in `crop.json`:

One mapping for every source dtype: **a per-crop percentile stretch to 16-bit.** `lo` = p0.1 and `hi` = p99.9
of the crop's finite values; `(v - lo) / (hi - lo)` is clipped to 0–1 and scaled to 0–65535. `+inf` maps to
65535, `-inf` and NaN to 0. `crop.json` records the source dtype, `lo`, `hi`, the percentiles used and the
non-finite counts. A constant crop is already rejected (§6.2), so `hi > lo`.

The stretch is adapted from zarrcade's `stretch_with_max_gain_bg_guard`
([`zarrcade/core/thumbnails.py`](https://github.com/JaneliaSciComp/zarrcade/blob/main/zarrcade/core/thumbnails.py)).
What is kept: the p0.1–p99.9 percentile stretch and the 16-bit target. What is dropped, and why:
- **The gain cap (`max_gain=8`)** assumes input already spans a 16-bit range. On uint8 data the gain is 257, so
  the cap would leave the image black; on 12-bit camera data it would halve the brightness. Its purpose, not
  amplifying noise in near-empty patches, is served by the content filter (§6.2).
- **The MAD black-level guard** is disabled by default in zarrcade (`k_bg=-inf`) and needs scipy. Not needed
  with the flat-patch filter.
- MIP, multiscale selection, colorify, CLAHE and matplotlib don't apply to a single-channel 256×256 patch.

The result is a numpy-only function of a few lines. It is lossy by design: 0.2% of values are clipped and
integer data is rescaled, which is the point of the question this answers (float data has no natural range)
and makes `raw.png` directly viewable in a browser and in a GitHub comment. Exact source values are always
recoverable by re-reading the recorded box from the source, and approximately from `lo` and `hi`.

**Labels to PNG.**

| `segmentation_type` | PNG | mapping |
|---|---|---|
| binary | 8-bit, values 0/1 | any nonzero → 1 (masks stored as 0/255 or 0/1 alike) |
| semantic | 16-bit | values kept; a value < 0 or > 65535 fails the crop |
| instance | 16-bit | values kept if within 0–65535; otherwise renumbered 1…k within the crop (instance IDs are arbitrary) |

**`crop.json`** records:
- the record, `record_commit` and licence SPDX
- the source volume id (raw index and URL(s)), plus each source file's validators from `io`
- the plane `{z, c}` and box `{y, x, h: 256, w: 256}`
- `voxel_size_nm` and the physical extent in µm
- the source dtype and the stretch (`lo`, `hi`, percentiles, non-finite counts)
- raw intensity statistics over finite values: min, p1, p50, p99, max
- the label keys and their PNG mappings
- the snippet name and `snippets_commit` for each array, plus the harness commit and environment digest
- the seed and batch, `round_id`, `crop_id`, and network bytes fetched

**Placement.** Published crops go in `<storage.root>/{modality}-{organism}-{name}/<crop_id>/`, with folder
naming as in mia-agentic-search's `tools/miao_layout.py`. `manifest.json` in that folder is an index rebuilt
from the `crop.json` files and `WITHDRAWN.yaml` (§5.6). Being present in the folder is what makes a crop
published (§7.6).

### 6.4 Verification

`harness verify` runs as its own sandboxed job. It mounts the crops read-only and **loads no snippet code**:
a PR-head snippet runs in the crop job, so it could forge any report that job writes. The worker accepts only
the verify job's report, never the crop job's. Every crop must pass these checks:
- every PNG decodes to 256×256 with the bit depth its mapping specifies
- the plane index and box lie within the source shape recorded by the crop job (a sanity check, since that
  shape comes from the crop job)
- raw passes the content filter, and no crop overlaps another (§6.2) by more than 25%
- each paired label is present
- `crop.json` is complete, including the licence and source validators
- the raw PNG's checksum is unique within the dataset, except that the same `crop_id` with the same checksum
  counts as already published

The verify job then runs `harness preview` to draw the label overlays (§6.7).

### 6.5 Limits

Limits are fixed bounds checked at admission, not a running ledger:
- **Every job has a byte cap and a walltime.**
  - The byte cap is enforced by `io` for network reads; cache hits are free (§6.6). Whether agent-initiated
    downloads can be capped at the sandbox's allowlist proxy is to be checked; if they can't, walltime is the
    hard bound for them.
  - Agent jobs (match, and autodev's develop rounds) also run with `claude -p --max-budget-usd`.
- **Every round has a bounded number of jobs:**
  - match: up to `max_stage_attempts` (default 1)
  - worker smoke: one
  - test crops: one per tested SHA, with at most `max_pr_revisions` PR heads (default 5) plus one merge commit
  - verification: one per crop job
  - production: one
- **The round's worst case is the per-job caps multiplied by the job count.** A config that can't produce a
  finite bound is rejected at startup.
- **Hitting a limit** stops the round as `needs-human`, reporting usage and keeping the surviving artifacts.
  Retrying infrastructure never grants a new allowance.

### 6.6 Dataset cache

Whole-file fetches (`io.fetch`: deflated zip members, servers without range support) go into a per-dataset
cache, `work_root/_cache/<record-id>/`, keyed by the sha256 of URL + validators.
- **Who writes it.** Only jobs that run reviewed code: the worker's smoke job and crop jobs on matched or merged
  SHAs, plus production. Their own cache is mounted read-write.
- **Who can't.** Jobs that run unreviewed code (the match agent, and crop jobs on a PR head) mount the dataset
  cache read-only and keep a private cache. Unreviewed code therefore can't poison what production reads.
- **Checks on every use.** A zip member's CRC32 from the central directory is checked whenever it is fetched or
  read from the cache. A validator mismatch, meaning the remote file changed, falls through to a fresh fetch.
- **Concurrency.** Entries are written to a temporary file and then renamed. One workflow per dataset runs its
  jobs one at a time, so there are no concurrent writers.
- **Retention** is configuration (§10). `ponytail:` no eviction yet; the cache is deleted by hand once a
  dataset is `done`.

### 6.7 Label overlays

`raw.png` is already viewable: the stretch in §6.3 is the same one a thumbnail would get. Label PNGs are not,
since instance IDs and class values render as near-black. So `harness preview` writes one `overlay.png` per
crop: `raw.png` with every paired label's boundaries drawn on it, one colour per label key, 8-bit RGB. The
colours and the boundary width are harness constants. There are no separate thumbnails and no contact sheet;
the comment embeds `raw.png` and `overlay.png` directly (§7.4).

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
   - **Licence.** If `license.spdx` is not in `crops.licences` (default `[CC0-1.0, CC-BY-4.0, CC-BY-3.0, MIT]`),
     the issue is created with `data:ineligible` and `licence-excluded`, and a `go` on it is refused with a
     comment. Today that covers 8 datasets (custom, NC, GPL). Crops are committed to a GitHub repo, and
     recording a licence doesn't make publishing them compliant.
4. Rate limits: `autodev.github.GitHub` already throttles creates. Re-running only adds what's new.

### 7.2 Match (agent job)

**Input.** The round directory contains:
- the record YAML at `record_commit`
- the downloads entry, plus that downloader's `conversion.json` / `manifest.json` from `dataset_dir` (mounted
  read-only) when they exist
- a read-only snapshot of `mia-snippets` at the round's `snippets_commit` (`snippets/`, `WITHDRAWN.yaml`,
  `SPEC.md`), with withdrawn and superseded snippets hidden from its catalog
- the harness CLI

**Task.** The agent picks a snippet for each raw and label array, and writes the binding with its `reasons`.
It then runs `harness smoke` until the binding passes or the agent reaches its limits. It may try several
snippets and params. It may not write or alter snippet code.

**Output (`outcome.json`).** One of:
- `{"outcome": "matched", "binding": …}`
- `{"outcome": "needs_snippet", "draft_binding": …, "what_fails": …, "tried": […]}`
- `{"outcome": "ineligible", "shapes": …}`
- `{"outcome": "blocked", "reason": …}`

The `blocked` case covers things like an undecidable label class, a dependency missing from the allowlist, or
a host the job isn't allowed to reach (`host_not_allowed`, §5.2).

**Worker.**
1. It checks the binding: record and commit match, URL hosts come from the record, every URL is `https`
   (or `s3` rewritten to https), and the snippets exist.
2. For `matched` and `ineligible`, it runs its own sandboxed `harness smoke` job, so the agent's claim is
   checked rather than trusted.
3. If those agree, it records the binding (§8.1) and moves to `testing` or `ineligible`.

`needs_snippet` goes to `developing`, and `blocked` to `needs-human`.

Smoke proves only that the binding reads data of the right shape. It can't tell whether an axes override
transposed the image to make the shapes line up. That is why a matched binding still needs a human
`data:approve` after test crops (§7.4).

Match rules are mia-agentic-search's: never invent values, ask rather than guess (`blocked`), and require
evidence for any axis override or transform.

### 7.3 Develop (autodev `snippets` deployment)

1. The worker opens an issue in `mia-snippets`, with a hidden round marker so a retry finds it rather than
   opening a second one:
   - **title:** `New snippet for <format>/<container> (mia-workflow-data#N)`
   - **body:** the record excerpt, the match agent's `what_fails` and `tried`, the `draft_binding`, and links to
     `SPEC.md` and the data issue
   - **labels:** `snippets:go`, so autodev starts its first round
2. autodev's agent, running in its usual csub sandbox, adds `snippets/<new-name>/`, including the binding it
   tested as `examples/<record-id>.yaml`. It runs `harness smoke` against the real data in its round, and
   autodev's trusted worker opens the PR. The agent doesn't need to know about mia-data.
3. `MiaDataPoll` finds the PR through autodev's `snippets/issue-K` branch and signals the data workflow with
   the PR number. The workflow asks GitHub for the current head and merge state; it never trusts the order in
   which signals arrive. The data issue moves to `testing`.
4. Review works as in autodev: the reviewer requests changes or comments `/revise …`. Each new head SHA gets a
   fresh test-crop job (§7.4) and invalidates readiness for the old head.
5. A human approves the exact head, both required statuses pass (autodev waits for them via
   `required_checks`, §12), and autodev squash-merges. The poller signals `merged`. The worker then compares
   the `snippets/<name>` tree hash at the merge commit with the one at the tested head:
   - **Equal** (the normal case, since CI only allows a new directory): the head's test applies to the merge.
     The binding is pinned to the merge SHA and the issue moves to `cropping`. The PR review approved both the
     snippet and its example binding, so no `data:approve` is needed.
   - **Different:** §7.4 runs again on the merge SHA, and only a pass admits `cropping`.
6. If the PR is closed without merging, or the issue gets `snippets:needs-human`, the data issue moves to
   `needs-human` with a link.

### 7.4 Test crops (plain jobs)

**Run.** Two sandboxed LSF jobs, with the round's test seed and binding, against exactly the recorded SHA (a
PR head, a merge commit, or a matched binding's `snippets_commit`):
1. **crop job:** `harness crop --n 4` (`test_per_round`)
2. **verify job:** `harness verify`, then `harness preview` (no snippet code, §6.4)

Test crops stay in the job workspaces under `work_root/issue-<n>/<round-id>/`: the crop job writes `crops/` in
its workspace, and the verify job writes `preview/` in its own (§6.7); a `mia-data test-pr` run's are under
`work_root/test-pr/<pr>/<head>/`. The `galleries` branch (below) is the
reviewed artefact. Only files the verify job checked (`raw.png`, which must decode as a 256×256 16-bit PNG) or
wrote (`overlay.png`) leave the cluster; anything else a PR-head snippet drops in `crops/` is never published.
`storage.root` must still be under the csub policy's `allowed_roots`: production crop and verify jobs mount the
dataset folder read-only for `--existing` (§7.5). Before adopting a PR's example binding, the worker validates
it: same record, URLs from the record, valid schema.

**Report.** The worker posts on the data issue, and on the PR if there is one:
- the verify results
- the bytes fetched
- a table of crop planes and boxes
- the binding and its `reasons`
- **the crops, inline.** The worker commits each crop's `raw.png` and `overlay.png` to the `galleries` branch
  of `mia-workflow-data` under `galleries/<record-id>/<round-id>/<sha>/<crop_id>/`, with the binding and the
  verify report beside them, and embeds them in the comment as a two-column table (raw, overlay; one row per
  crop, captioned with `crop_id`, plane and box) using
  `![…](https://github.com/<org>/mia-workflow-data/blob/galleries/<path>?raw=true)`. This is zarr-doctor's
  results path, which already rendered PNGs in issue comments from this codebase: an orphan branch created on
  first use, one Contents API `PUT` per file guarded by an existence check and an in-process lock (concurrent
  PUTs to one branch race with 409), and the `?raw=true` blob URL. The code is in git history at
  `7abe195~1:zarr_doctor/github_api.py` (`ensure_branch`, `file_exists`, `upsert_file`) and
  `zarr_doctor/activities.py` (`publish_results`); it was dropped when the client was folded into autodev, so
  restore it rather than rewrite it. Four test crops are eight files of about 100 KB each. `ponytail:` PNGs on
  a branch is fine for a demo; move to real hosting (§15) before the branch reaches a few hundred MB.

For a PR head, the worker also publishes the `mia-data/crops` status (pending, then success or failure) on that
exact SHA, with the comment's URL as the status target. A result for an old head or an old round stays visible as history, but never
advances the stage or overwrites the current status.

**Next stage:**
- **Current PR head passes:** `data:pr-ready`, with the PR link and the crops.
- **Matched snippet passes:** `data:review`. A human looks at the crops and the binding, then adds
  `data:approve`, which moves the issue to `cropping`. If something is wrong, they comment and re-trigger, or
  leave it.
- **Merge commit passes** (only when tree hashes differed): `cropping`.
- **PR head fails:** the worker comments on the PR, the status stays failed, and the issue stays `testing`
  until a `/revise` produces a new head.
- **Matched snippet or merge commit fails:** `needs-human`, never production.

### 7.5 Production (plain jobs)

**Run.** One sandboxed crop job runs `harness crop` with:
- the approved binding and pinned SHA
- `n` crops (config, default 16, lowered by capacity, §6.2)
- the round's production seed
- `--existing` set to the dataset's current `manifest.json`

No agent is involved: the harness samples, filters and writes deterministically, and a human already judged
the test crops from this binding. A label-alignment check or an agent quality review can be added later if the
galleries show they're needed (§15).

**Worker.** After the job ends:
1. It runs the verify job.
2. It publishes every crop that passes (§7.6).
3. It commits the published crops' `raw.png` and `overlay.png` under
   `galleries/<record-id>/<round-id>/production/<crop_id>/` on the `galleries` branch and posts the totals with
   the table of crops inside a `<details>` block.
4. It sets `data:done`, or `needs-human` if fewer than half of the (lowered) `n` passed.

**Partial results stay published.** Crops that passed verification are published even when the round ends in
`needs-human`. They each passed every check. The status comment says how many were published, and that the
round still needs attention.

### 7.6 Publication

Publication is an idempotent Activity. Its input is the list of verified crops (`crop_id` and file checksums)
from the verify job's report.

1. For each verified crop:
   - If `<dataset folder>/<crop_id>/` already exists with the same checksums, an earlier attempt published it:
     skip it.
   - If it exists with different checksums, stop and go to `needs-human`.
   - Otherwise copy the crop into a temporary directory named per attempt,
     `<dataset folder>/.tmp-<crop_id>-<uuid4>/` (a fresh UUID, not Temporal's attempt number, which restarts
     at 1 when the Activity is scheduled again), check the checksums, and rename it into place.
2. Rebuild `manifest.json` from the `crop.json` files and `WITHDRAWN.yaml`: write it to a temporary file, then
   rename.

Temporary directories older than twice the activity's start-to-close timeout are deleted at the start of a
publication. Newer ones may belong to a live attempt and are left alone. Directories starting with `.` are
ignored by the manifest.

There is no number allocation and no lock:
- **One writer per dataset.** One workflow per dataset means one publisher per dataset folder.
- **Zombie attempts are harmless.** If an older attempt of the same Activity is still running, it writes to its
  own temporary directory. Renaming onto an existing crop directory fails, so the first rename wins and the
  other attempt sees an identical crop.
- **Retries are always safe.** Retrying after a crash at any point gives the same result.

GitHub comments and totals are a projection of the result, not part of publishing.

## 8. Temporal design

- **Namespace and queues.** Namespace `mia-data`. Task queues are `mia-data` (workflows and short activities)
  and `mia-data-await` (long LSF waits), the same pattern as autodev. Workflow types are named explicitly:
  `MiaDataDataset` and `MiaDataPoll`.
- **`MiaDataDataset`.** Id `mia-data/<repo>/issue-<N>`. It is thin: a loop over the stages in §4, with every
  side effect in activities. It waits on signals (`go`, `approve`, `pr_update`) and on LSF results through the
  await queue. `done`, `ineligible` and `needs-human` are dataset states; the workflow keeps running, waiting
  for the next `go`.
- **`MiaDataPoll`.** Runs on a schedule (every 2 min). It works like autodev's poller:
  - It lists `mia-workflow-data` issues labelled `data:go` or `data:approve` and checks that the labeler has
    write access.
  - It signals the dataset workflow (signal-with-start for `go`) with the label event's ID, then removes the
    label.
  - It watches open `mia-snippets` PRs:
    - for PRs on `snippets/issue-K` branches that it opened issues for, it signals `pr_update` with the PR
      number
    - for any PR whose head touches nothing under `snippets/`, it posts `mia-data/crops` = success (§5.5)
- **Jobs.** Match, smoke, crop and verify jobs all go through the csub runner (§12), with one workspace per
  job. Temporal owns submission, waiting and result collection, not the agent's tool loop. Activity inputs and
  results are small metadata and paths. Pixels, logs and transcripts stay out of history.

### 8.1 Identity and state: Temporal is the record

The workflow's own state, recorded in its history, is the durable record. There is no parallel state store.
- **`round_id`** is `i<issue>-e<label event id>`. The workflow keeps the set of event IDs it has handled
  (`go` and `approve`), so a redelivered label is a no-op.
- **The seed** comes from `workflow.random()` at admission, so it is recorded in history. Test and production
  seeds are derived from it.
- **What the workflow carries:** the record commit, the `snippets_commit`, the harness commit and environment
  digest, the binding's path and hash, PR and tested/merged SHAs with their tree hashes, job IDs, and verify and
  publication results. An admitted stage never consults a branch, an environment variable or an issue comment
  again.
- **Inputs on disk** (record snapshot, binding, prompt) are written once per stage into `paths.state_dir`,
  which is never mounted into jobs. Job workspaces live under `paths.work_root`, and each job sees only its own
  workspace plus approved read-only inputs (snapshot, harness, dataset cache).
- **Missing inputs or outputs fail closed:** the round goes to `needs-human` and is never silently recreated.

### 8.2 Jobs and recovery

Reuse autodev's runner semantics. Before submitting, it looks up the job by its workspace (the job-ID file and a
cwd match), so a retried submission reattaches instead of submitting twice. The await Activity polls with a
heartbeat, and a worker restart reattaches to the same job.

- **A job that fails is a failed attempt.** Its output is kept. With `max_stage_attempts=1` (the default) the
  round goes to `needs-human`. A configured retry makes a new attempt only after the old job is terminal.
- **Agent sessions are not resumed. This is a choice, not a CLI limitation:** `claude -p` can resume a session
  with `--resume`. A fresh attempt with the same inputs is simpler to reason about, and it is bounded by
  `max_stage_attempts`. Resuming could save cost on long match jobs later. A partial `outcome.json` is never a
  success.
- **Uncertain job state blocks new work.** If a job's state is uncertain (e.g. scheduler records have
  expired), the round goes to `needs-human` instead of starting overlapping work. A monitoring timeout doesn't
  mean LSF stopped the job.
- **Errors.** Transient GitHub and monitoring errors retry with finite deadlines. Input, authorisation and
  verification errors don't retry.

### 8.3 Events and revisions

- **Label events.** The poller delivers at least once, and the workflow deduplicates by event ID (§8.1). The
  poller removes the label only after the signal is accepted. A missed removal just means a redelivery that is
  ignored.
- **PR observations** are wake-ups. On each one, the workflow calls an Activity that reads the PR's current head,
  state and merge SHA from GitHub, so a delayed signal can never replace a newer head.
- **Results must match the expected test.** A test result advances the stage only if its round, SHA and test
  ID all match the test the workflow is waiting for. A stale result is kept as history and nothing more.
- **GitHub writes** (issues, comments, statuses, gallery files) carry hidden markers or are naturally idempotent
  (a status is set per SHA and context; a gallery file is written by path and skipped when it already exists
  with the same content), so a retry finds the earlier write instead of repeating it.

### 8.4 Continue-As-New

The workflow rolls over when `is_continue_as_new_suggested()` (as autodev's `PollWorkflow` does), at an idle
wait or a stage boundary with no Activity in flight. It carries one schema-versioned state object: everything
listed in §8.1, plus the handled event IDs and any pending signals. It drains pending signals before rolling
over, so none are lost. A state that can't be carried fails explicitly; it never restarts from matching.

The split is coarse-grained on purpose: Temporal orchestrates submission, waiting and results for whole jobs.
Agents run inside jobs, and their internal tool loops are not Temporal activities.

## 9. Trust boundaries

- **GitHub writes.** Agents never get GitHub credentials or network access to GitHub. The worker does every
  GitHub write: issues, comments, labels, statuses. autodev does the snippet branch pushes and the merge.
- **Network.** The shape is a maintained ceiling plus a narrow per-job request:
  - **The ceiling.** The csub policy on the service account allows every catalog data host and the redirect
    targets found for it (§12.5). It is maintained by hand. For today's 49 datasets it is a short list.
  - **Each job requests only what it needs.** That is the hosts in its binding's URLs (for the match agent,
    the record's URLs), each checked against the record, plus the redirect hosts configured for them
    (`network.redirect_hosts`, §10), plus package hosts for the environment.
  - **Anthropic hosts** come from the policy's `[agents]` table, and only for agent jobs. Verify jobs get no
    network access.
  - **Egress is HTTPS-proxy only.** No FTP; bindings must use https URLs.
- **New hosts fail closed, before submission.** The worker derives a job's hosts and compares them with the
  policy patterns from `csub probe`, which it fetches at worker start and again whenever a job is about to be
  submitted. If any host is missing, it doesn't submit. The issue goes to `needs-human`, naming the missing
  host and saying that it needs a policy edit. Adding a host is an edit to `broker.toml` on the service account
  (plus `network.redirect_hosts` if the host redirects), never a code change. The same applies when a running
  job reports `host_not_allowed` for a redirect target nobody had probed: the comment names that host, the
  operator probes it (§12.5) and adds it, and a new `data:go` re-runs the round.
- **Unreviewed code.**
  - PR-head snippet code is agent-written and unreviewed. It runs only in csub sandboxes, with no GitHub access
    and no secrets, writing only to its own workspace and private cache.
  - The credential-bearing worker never imports a snippet or the harness. Its smoke and verify checks run as
    sandboxed jobs, and verify jobs load no snippet code.
- **Untrusted data.** Never run code shipped with a dataset, never unpickle, and never install what a dataset
  names.
- **Required approval.**
  - **New snippets.** Merging into `mia-snippets` needs human approval of the exact head, plus the required
    `check` and `mia-data/crops` statuses on that head. Production then needs the merged snippet tree to match
    the tested one, or a passing test of the merge commit.
  - **Matched bindings** need `data:approve` from a human with write access, after test crops for that round.
  - A late result for an old head or an old round satisfies neither gate.

## 10. Configuration (`mia_data.yaml`)

```yaml
github:   {data_repo: JaneliaSciComp/mia-workflow-data, snippets_repo: JaneliaSciComp/mia-snippets,
           catalog_repo: AI-HHMI/mia-agentic-search, auth: app, …}
temporal: {address: …, namespace: mia-data}
paths:    {state_dir: /shared/TBD/mia-data-state, work_root: /shared/TBD/mia-data-work}
storage:  {root: /shared/TBD/mia-crops}
gallery:  {branch: galleries}                               # orphan branch of github.data_repo (§7.4)
crops:    {size: 256, per_round: 16, test_per_round: 4, per_plane: 4, per_file: 8,
           licences: [CC0-1.0, CC-BY-4.0, CC-BY-3.0, MIT]}
limits:   {job_bytes: 5e9, job_walltime_min: 120, agent_max_budget_usd: 5,
           max_stage_attempts: 1, max_pr_revisions: 5}
agent:    {…same shape as autodev's agent block…}
harness:  {dir: /path/to/mia-snippets-checkout}           # pinned per round by commit + pixi.lock digest
network:
  package_hosts: [pypi.org, files.pythonhosted.org, conda.anaconda.org, repo.prefix.dev]
  redirect_hosts:                                          # filled from the §12.5 probe; exact hosts, no patterns
    ndownloader.figshare.com: [s3-eu-west-1.amazonaws.com]
    huggingface.co: [cdn-lfs.huggingface.co, cas-bridge.xethub.hf.co]
```

The `redirect_hosts` values above are illustrative; the probe decides the real ones. Jobs request exact hosts,
so the sandbox never depends on wildcard support. The policy may use patterns.

All paths must be on storage that the worker and the cluster jobs both see. Config changes apply to new
rounds, never to an admitted one.

## 11. Testing

**This repo.** Unit tests need no external services. Workflow tests use Temporal's time-skipping test server
with stub Activities.
- **Seed** against a fixture of both downloads files and a few records: idempotency, labels, `maybe-small`,
  licence exclusion.
- **The stage machine:**
  - the match → test → review → approve → crop path, and approve arriving outside `review` (ignored)
  - the develop path, with a revise, a merge with equal tree hashes, and a merge with different tree hashes
  - a closed PR
  - ineligible, licence excluded
  - a limit being hit
  - partial production (fewer than half pass: the passing crops are published and the issue is
    `needs-human`)
- **Event handling:**
  - a redelivered `go` or `approve`, and a `go` while busy
  - a stale PR head or test result arriving late
  - a merge arriving before its test finishes
  - signals arriving during Continue-As-New

  In every case: no extra round, no lost event, and no production without matching verification and approval.
- **Recovery:**
  - worker loss after a job is submitted but before its ID is recorded
  - worker loss while awaiting a job
  - worker loss after the job finishes but before its result is recorded

  Each must reattach rather than resubmit. Missing artifacts must fail closed.
- **Publication:**
  - interrupted mid-copy, after renames, and after the manifest rebuild: a retry gives the same crops and
    totals
  - two concurrent attempts (simulating a zombie) give one crop, with neither attempt's temporary directory
    deleted while it is live
  - withdrawal marks crops in the manifest without deleting them
- **Hosts:** a binding host missing from the probed policy → no submission, and `needs-human` naming the
  host. A redirect host is added to the job's request from `network.redirect_hosts`. A GitHub host is never
  requested.
- **Gallery commits:** writing the same file twice makes one commit; a retry after a crash completes the set;
  the branch is created once (zarr-doctor's `test_github_api.py` at `7abe195~1` covered this).
- **Pinning:** changing config, `main` or the harness between retries doesn't change an admitted round.
- **The status comment** renders correctly, and the **replay test** passes on recorded histories for both
  paths.

**`mia-snippets`:**
- harness unit tests:
  - sampling reproducibility, the plane and file batching, capacity lowering, and the shared redraw budget
  - the content filter (including non-finite values) and overlap rejection
  - the raw stretch of §6.3 on uint8, uint16, int16 and float patches, with hot pixels, NaN and inf: a
    deterministic 16-bit result, `lo`/`hi` recorded, 0.2% clipped; the label mappings, including 0/255 binary
    masks
  - `preview` draws one overlay per crop with a boundary colour per label key
  - the byte cap, `io` range and cache behaviour against a local HTTP server, and refusing a server that
    ignores `Range`
  - a redirect to a host outside `MIA_HARNESS_ALLOWED_HOSTS` raises `HostNotAllowed` with the chain, before
    any request to that host; `ftp://` raises the same
  - dataset-cache CRC32 checks, and read-only use by unreviewed jobs
- CI-check tests with good and bad PR file lists, including `WITHDRAWN.yaml` edits by a bot

**Integration (marked `integration`).** One real dataset end to end. `bbbc034-hipsc-nuclei-3d` is the first
candidate: a zip member with labels and 1024×1024 planes. Before scaling out, also exercise:
- replacing the worker against real csub
- `mia-data/crops` status publication and branch protection on a real PR

## 12. Changes to existing code (step 0)

1. **One Temporal server, a namespace per app.** `scripts/autodev-up.sh` reuses a server that is already
   healthy instead of starting a second one, and makes sure its deployment's namespace exists. The script that
   started the server owns it. The existing autodev deployment stays on `default`; moving it would strand its
   in-flight workflows. The `snippets` deployment uses namespace `snippets`. A new `scripts/mia-data-up.sh`
   starts the mia-data broker and worker against the same server.
2. **No `common/` package yet.**
   - `mia_data` imports `autodev.github.GitHub` (one client per repo: data, snippets, catalog), `autodev.auth`
     and the csub runner.
   - Split `autodev/agent.py` so that a generic job runner (submit-once by key with cwd reattachment, await) is
     separate from the issue-to-PR prompt and the git-bundle collection. Match jobs and plain harness jobs can
     then supply their own command and result type.
   - Move code into a shared package only when coupling warrants it.
3. **autodev.**
   - `github.trusted_bots: [<mia-data app>[bot]]`, so the mia-data bot's `snippets:go` label counts as a valid
     trigger. Today only a human with write access does.
   - `github.required_checks: [mia-data/crops]`. A required context that is missing or pending counts as
     pending, so autodev waits instead of attempting a merge that branch protection would refuse.
   - The GitHub client gains commit statuses, tree SHAs of a directory at a ref, raw file reads, and issues and
     comments found by marker. It gets back zarr-doctor's `ensure_branch`, `file_exists` and `upsert_file`
     (with its commit lock) from `7abe195~1:zarr_doctor/github_api.py` for the gallery files. The mia-data app
     needs Contents RW on `mia-workflow-data`.
   - A `snippets` deployment config with `agent.allow_hosts` set to the catalog's data hosts and their redirect
     hosts, and `extra_mounts`
     giving read-only access to the pinned harness checkout, so its agent can run `harness smoke` on real data.
     Its agent jobs use `--max-budget-usd` and walltime like mia-data's.
4. **`mia-data test-pr <pr> --binding <file>`.** A manual admission command that runs §7.4 on an explicit PR
   head and publishes the status. Human-authored snippet PRs, including the two bootstrap ones, need it before
   any data issue exists. It doesn't bypass approval.
5. **Network setup** (one-time, then again for every new host):
   - Enable the `[agents]` `claude` table in the service account's `broker.toml`.
   - Add every catalog data host to `allowed_hosts`.
   - **Redirect probe.** From a host with normal egress (the worker host, not a sandboxed job: the proxy would
     block the very hop you are trying to discover), run `curl -sIL` on each binding or sample URL, and record
     the redirect chain. If only sandboxed egress is available, iterate: allow one hop, re-run, repeat. Add every host in the chain to the policy, and add the hops that aren't the record's own
     host to `network.redirect_hosts`. Keep the probe output in the runbook so the next person can see why a
     host is there.
   - Confirm that FTP is unusable and that the EMPIAR records' https mirrors work.

## 13. Open questions (defaults in bold)

1. **Byte cap for agent downloads:** can the agentic-sandbox allowlist proxy count bytes per job? If not,
   walltime is the bound (§6.5).
2. **Review fatigue on the matched path:** every matched binding needs a `data:approve` today. If crops for
   default bindings (empty `params`, no overrides in `reasons`) are routinely fine, they could skip review.
   Keep the gate until there is evidence.

Decided on 2026-10-11: every raw crop is stored with zarrcade's percentile stretch to 16-bit, so `raw.png` is
both the data and the preview (§6.3); crops are committed per file to a `galleries` branch of
`mia-workflow-data` and embedded in the issue, as zarr-doctor did (§7.4); the csub policy is edited by hand
for now (§15).

## 14. Build order

| # | milestone | done when |
|---|---|---|
| 0 | step 0 (§12.1–12.3) | one Temporal server with `mia-data` and `snippets` namespaces; job runner split; autodev tests green |
| 1 | `mia-snippets`: `SPEC.md`, harness (`smoke`, `crop`, `verify`, `preview`, `io`, dataset cache), CI check, two hand-written snippets (`tiff-zip-member`, `mrc-http`) | harness tests pass; `test-pr` posts a passing `mia-data/crops` on both PR heads before human merge |
| 2 | `mia-data seed` | 49 issues in `mia-workflow-data` (8 licence-excluded); a re-run creates none |
| 3 | `MiaDataDataset`: match → test → review → approve → crop → publish (no develop path) | bbbc034 reaches `data:done` with 16 verified PNG crops shown in its issue; a redelivered `go`, a worker restart and an interrupted publication don't duplicate anything |
| 4 | develop path: autodev `snippets` deployment (§12.3), PR detection, head-bound test crops, tree-hash merge check | one dataset gets a new snippet through a PR, human approval and production; a stale head's result can't pass a gate |
| 5 | run over all eligible datasets | every issue is `done`, `ineligible` or `needs-human` with a reason |

`mrc-http` replaces the earlier `tensorswitch-auto` choice. It range-reads a 1.9 GB tomogram (`czcdp-10003`)
and fetches only a few MB, which proves the range-read path. It also keeps Java out of the harness
environment.

## 15. Later

- **3D and 4D crops** (128³ and beyond) in zarr with multiscale pyramids. Snippets are already n-dimensional,
  so this is a harness change (sampler, writer, verify) plus slab staging for `plane` and `file` access.
- **Anisotropy limits** for 3D crops.
- **2D datasets from the catalog**, not just planes of 3D volumes.
- **A label-alignment check or an agent quality review** in production, if the galleries show bad crops get
  through.
- **Gallery hosting off GitHub** (fileglancer or an HTTP server over `storage.root`) with an `index.html`,
  once the `galleries` branch outgrows a demo.
- **Per-dataset stretch** (percentiles over the whole volume instead of per crop), if per-crop stretching
  turns out to flatten contrast between crops of one dataset.
- **csub policy maintenance.** For now a human edits `broker.toml` on the service account whenever a dataset
  needs a new host or redirect target, and `needs-human` names the host (§9). If that happens often, add
  `mia-data probe-hosts <record>`, which runs the §12.5 probe and prints the policy lines to add. The edit
  itself stays manual.
- **Resuming killed agent sessions** (`claude -p --resume`) if match jobs become long and costly.
- **Byte metering across a whole round**, and **dataset-cache eviction**.
- **Licences beyond the default allowlist**, once their terms for redistribution and training are settled.
- **A training-set builder** over the manifests: licence filters, withdrawn-crop filtering, splits by source
  volume, normalisation from the recorded intensity statistics.
- **A `common/` package**, once a third app shares autodev's pieces.
