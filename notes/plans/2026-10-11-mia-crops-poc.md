# MIA crops proof of concept (2D PNG) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> Each task lists its interfaces, the tests to write first, and the commands that prove it works. Write the
> tests, see them fail, implement, see them pass, commit.

**Goal:** A human adds `data:go` to a dataset issue in `JaneliaSciComp/mia-workflow-data`. That dataset then
gets 256×256 PNG crops, published with provenance. A new conversion snippet is written by autodev and approved
by a human whenever no existing snippet can read the dataset.

**Architecture:**
- **Workflows.** One long-lived Temporal `MiaDataDataset` workflow per data issue (namespace `mia-data`),
  started and fed by a scheduled `MiaDataPoll`. Workflows are thin state machines; every side effect is an
  Activity.
- **Jobs.** Every piece of work is a csub job with its own workspace. That covers the match agent, smoke
  checks, crop jobs and verify jobs.
- **Snippets and harness** live in `JaneliaSciComp/mia-snippets`. Snippets are immutable readers; the harness
  is the human-owned CLI that samples, encodes, verifies and previews.
- **Development** reuses autodev as a second deployment (`snippets`) on `mia-snippets`. The two systems
  interact only through GitHub: the snippets issue, its PR, and a `mia-data/crops` commit status.
- **The trusted worker** owns the GitHub credentials and is the only process that writes to GitHub or
  publishes crops. It never imports snippet code.

**Tech Stack:**
- Python 3.12 (pixi), Temporal Python SDK, httpx, pydantic 2, PyYAML
- csub, Claude Code CLI (match jobs only)
- Harness: numpy, tifffile, imagecodecs, mrcfile, h5py, pypng
- pytest, pytest-asyncio, respx

**Spec:** [`notes/mia-crops-spec.md`](../mia-crops-spec.md) v0.3. Section numbers like §7.6 below refer to it.

## Before you start (prerequisites, not tasks)

- [ ] **P1. Repos.** Create `JaneliaSciComp/mia-workflow-data` (issues only, empty README) and
  `JaneliaSciComp/mia-snippets` (empty). Install the GitHub App used by autodev on both, with these
  permissions:
  - Issues RW, Pull requests RW, Contents RW (`mia-snippets` only)
  - Commit statuses RW, Checks R, Metadata R

  Check: `gh api repos/JaneliaSciComp/mia-snippets` and `gh api repos/JaneliaSciComp/mia-workflow-data` both
  succeed with the app's installation token (`pixi run ad check` against a config pointing at each).
- [ ] **P2. Shared directories** visible from the worker host and from compute nodes, as a sketch:
  - `paths.state_dir`: trusted, never mounted into jobs
  - `paths.work_root`: job workspaces; must be under the csub policy's `allowed_roots`
  - `storage.root`: published crops

  Check: `ls` all three from the worker host and from inside a job,
  `csub submit --walltime 5 --wait -- ls <root>`.
- [ ] **P3. Broker policy** (`~/.config/csub/broker.toml` of the service account):
  - `allowed_roots` contains `work_root`
  - `allowed_hosts` adds the catalog's data hosts: `zenodo.org`, `ftp.ebi.ac.uk`, `www.ebi.ac.uk`,
    `data.broadinstitute.org`, `files.cryoetdataportal.cziscience.com`, `huggingface.co`, `*.huggingface.co`,
    `ndownloader.figshare.com`, `figshare.com`, `allencell.s3.amazonaws.com`, `s3.amazonaws.com`

  Check: `csub submit --walltime 5 --wait --allow-host data.broadinstitute.org -- curl -sI https://data.broadinstitute.org/bbbc/BBBC034/BBBC034_v1_dataset.zip`
  returns `200`.
- [ ] **P4. FTP probe** (open question 5, §9). Check whether an FTP URL works through the allowlist proxy:
  `csub submit --walltime 5 --wait --allow-host ftp.ebi.ac.uk -- curl -sI ftp://ftp.ebi.ac.uk/empiar/world_availability/11969/data/`.
  Record the result in this plan. If it fails, snippets use the `https://ftp.ebi.ac.uk/...` mirror (the
  records already give https URLs), and FTP stays unsupported.
- [ ] **P5. Byte metering probe** (open question 5). Check whether the agentic-sandbox allowlist proxy logs
  bytes per connection. Record yes or no. With no, walltime is the only bound on agent downloads, as the spec
  says.
- [ ] **P6. Branch.** `git checkout main && git pull && git checkout -b mia-crops-poc`.

## Global constraints

- **Where code goes.** New orchestration code lives in `mia_data/`. Reuse autodev by import (`autodev.github`,
  `autodev.auth`, `autodev.jobs`, the `_read_agent_file` helper); don't copy it. Autodev changes must keep
  `pixi run test` green, including `test_replay_after_log_change` and `test_replay_recorded_history`.
- **Tests.** This repo's tests go in `tests/test_mia_*.py`, with helpers in `tests/mia_helpers.py`.
  `mia-snippets` has its own `pixi run test`.
- **The worker never imports snippet code**, and never imports the harness either. Smoke, crop and verify
  always run as csub jobs.
- **Agent output is untrusted data.** Everything a job writes is read with `autodev.agent._read_agent_file`
  (bounded, no symlinks) and parsed defensively. Nothing a job writes is executed by the worker.
- **No GitHub access for jobs.** Jobs never get GitHub hosts in `allow_hosts`, never get secrets, and get
  read-write access only to their own workspace.
- **Workflow code does no I/O**, calls activities by name, and stays replay-safe. Every workflow change
  extends the replay test (Task 18).
- **Keep it small.** Each `ponytail:` comment names a deliberate shortcut and its upgrade path.

## Review focus

Reviewers should check these first:
1. **Exact-SHA gating.** Production happens only after one of these:
   - a passing test of the exact merged snippet tree (tree-hash equality with a tested head)
   - a passing test of the merge commit
   - a passing test of the matched `snippets_commit`

   Nothing resolves `main` again after admission.
2. **Idempotency.** Look at go-event deduplication, the job-id files, publication (rename-if-absent), and
   comments and statuses with markers.
3. **Trust.** The worker never imports a snippet. Jobs can't reach GitHub. Agent files are read safely.
4. **PNG mappings** match the spec §6.3 tables exactly, and every mapping is recorded in `crop.json`.

## File structure

**This repo (`temporal-prototype`):**
```
autodev/
  jobs.py            NEW  generic csub submit/await (split out of agent.py; Task 1)
  agent.py           CHANGED  AgentJobs becomes a thin wrapper over jobs.CsubJobs
  github.py          CHANGED  new calls: label_event, create_issue, comments by marker, statuses, dir_sha, get_file (Task 2)
  config.py          CHANGED  github.trusted_bots, github.required_checks (Task 3)
  activities.py      CHANGED  poll_issues trusts trusted_bots; observe_pr applies required_checks (Task 3)
mia_data/
  __init__.py
  config.py          Config, load_config (Task 12)
  models.py          dataclasses for workflow state, signals, activity I/O (Task 12)
  catalog.py         downloads state + records from the catalog repo (Task 13)
  seed.py            issue body/labels, idempotent create (Task 13)
  binding.py         worker-side binding checks (no harness import) (Task 14)
  status.py          status-comment and stage-comment rendering (Task 14)
  publish.py         rename-if-absent publication + manifest rebuild (Task 14)
  jobspec.py         job scripts for match / smoke / crop / verify; snapshots of snippets at a SHA (Task 15)
  prompts/match.md   match agent prompt (Task 15)
  activities.py      (Task 16)
  workflows.py       MiaDataDataset, MiaDataPoll (Tasks 17, 20)
  cli.py             check, init-labels, seed, worker, schedule, test-pr, crops-by-snippet (Task 18)
mia_data.yaml.example
scripts/autodev-up.sh   CHANGED  reuse a running Temporal server; ensure the namespace (Task 4)
scripts/mia-data-up.sh  NEW      broker + sandboxed mia-data worker (Task 4)
tests/mia_helpers.py, tests/test_mia_*.py, tests/fixtures/mia/…
```

**`mia-snippets`:**
```
SPEC.md                    §5 of the spec, verbatim, plus the harness CLI reference
WITHDRAWN.yaml             []
README.md
.github/workflows/check.yml
ci/check_pr.py             PR rules (§5.5), pure + CLI
ci/maintainers.txt         human logins allowed to touch harness/, SPEC.md, WITHDRAWN.yaml, .github/
harness/
  pixi.toml, pixi.lock
  src/mia_harness/
    api.py                 Source, Array (Protocol), SPEC_VERSION
    io.py                  IO: open/fetch/list, range files, cache, byte cap, validators
    remotezip.py           zip central directory + member access over ranges
    binding.py             pydantic Binding + load_binding
    loader.py              load a snippet module from a snapshot dir; withdrawn/superseded/spec_version checks
    sample.py              pure planner: volumes + rng -> CropSpec list; overlap test
    png.py                 raw/label encoders per §6.3, decoders for verify
    run.py                 smoke and crop execution
    verify.py              verify (no snippet code)
    preview.py             overlays + index.html
    cli.py                 `harness smoke|crop|verify|preview`
  tests/…
snippets/tiff-zip-member/…  (Task 10)
snippets/mrc-http/…         (Task 11)
```

---

### Task 0: Fold this plan's decisions into the spec

**Files:** `notes/mia-crops-spec.md`

- [ ] Edit the spec to match these planning decisions, and bump it to v0.4:
  1. **§7.4 and §7.5: verification is always a separate job.** The test job runs only `harness crop`. A
     second job, `harness verify` followed by `harness preview`, mounts the crops read-only and loads no
     snippet code. (PR-head snippet code runs in the crop job and could forge a verify report written by the
     same process.)
  2. **§5.5 and §7.4: `mia-data/crops` is a commit status, not a check run.** The statuses API works under
     both token and app auth, and branch protection can require a status context.
  3. **§6: the harness is pinned to a configured checkout.** `harness.dir` is a checkout of `mia-snippets`
     with `pixi install` done. A round records its `git rev-parse HEAD` and the sha256 of
     `harness/pixi.lock`. A job is refused (`needs-human`) if the checkout has moved since admission.
     `ponytail:` per-commit environments if harness updates need to overlap in-flight rounds.
  4. **§12.3: autodev gets `github.required_checks`.** A required context that is missing or pending counts as
     pending, so autodev waits rather than attempting a merge before `mia-data/crops` reports.
  5. **§14 milestone 1: the hand-written snippets are `tiff-zip-member` and `mrc-http`**, not
     `tensorswitch-auto`. `mrc-http` range-reads a 1.9 GB tomogram (`czcdp-10003`) and keeps Java out of the
     harness environment.
- [ ] Commit: `git commit -am "mia-crops spec v0.4: decisions from planning"`.

---

## Phase A — autodev groundwork (this repo)

### Task 1: Split a generic csub job runner out of `autodev/agent.py`

**Why:** mia-data submits several kinds of job: match agent jobs (`claude: true`) and plain harness jobs
(`claude: false`), each with its own command, mounts and hosts. Today `AgentJobs.submit` hard-codes
autodev's job script.

**Files:** create `autodev/jobs.py`; modify `autodev/agent.py`; test `tests/test_autodev_jobs.py`.

**Interface (`autodev/jobs.py`):**
```python
@dataclass
class JobSpec:
    name: str                      # csub job name (no username in it)
    command: str                   # shell script; runs with cwd = workspace
    workspace: str                 # rw mount + cwd; unique per job (the cwd-lookup key)
    walltime_min: int
    cpus: int = 1
    mem_mb: int = 4000
    allow_hosts: list[str] = field(default_factory=list)
    ro_mounts: list[str] = field(default_factory=list)   # absolute paths
    claude: bool = False
    env: dict[str, str] = field(default_factory=dict)

class CsubJobs:
    def __init__(self, session: str, id_dir: Path, client_factory=None, sleep=asyncio.sleep, poll_s: float = 60.0): ...
    async def submit(self, key: str, spec: JobSpec) -> str:
        """Submit at most once per key: id file -> cwd lookup -> submit. Returns the job id."""
    async def wait(self, job_id: str, workspace: str, heartbeat: Callable[[str], None]) -> JobOutcome: ...
```
- `key` names the id file `id_dir/<key>.id`. Validate it against `[A-Za-z0-9._-]{1,120}`, since it names
  files.
- Move `submit` and `wait` bodies here unchanged in behaviour: the id file is written atomically, and the
  csub `status()` cwd match is checked before submitting.
- `AgentJobs` keeps its public API (`submit(issue, key, rnd, ws)`, `wait(...)`). It builds a `JobSpec` from
  `job_script(...)` and delegates.

**Tests first (`tests/test_autodev_jobs.py`, fake csub client like `test_autodev_agent.py` uses):**
- `test_submit_once_per_key`: a second `submit` with the same key returns the id from the file, with no client
  call.
- `test_reattach_by_cwd`: no id file, but `status()` lists a job with `cwd == workspace`. That id is returned
  and the file is written.
- `test_plain_job_spec`: `claude=False` sends `"claude": False`, `ro_mounts` become `{"mode": "ro"}`, and env
  is passed.
- `test_bad_key_rejected`: `"../x"` raises `ValueError`.
- All existing `tests/test_autodev_agent.py` tests pass unchanged.

**Verify:** `pixi run test` → all green (155 + new).
**Commit:** `autodev: split a generic csub job runner out of agent.py`.

### Task 2: GitHub client additions

**Files:** `autodev/github.py`; test `tests/test_autodev_github.py` (respx).

**Add these methods** (all on the repo-bound `GitHub`; mia-data makes one client per repo):
```python
async def label_event(self, issue: int, label: str) -> tuple[str | None, str | None]:
    """(actor login, event id) of the most recent `labeled` event for `label`."""
async def create_issue(self, title: str, body: str, labels: list[str]) -> dict
async def find_issue_by_marker(self, marker: str, labels: list[str] | None = None) -> dict | None   # scans open+closed
async def find_comment(self, number: int, marker: str) -> dict | None
async def edit_comment(self, comment_id: int, body: str) -> dict
async def set_status(self, sha: str, context: str, state: str, description: str, target_url: str | None) -> None
async def statuses(self, sha: str) -> list[dict]          # combined-status list, for required_checks
async def dir_sha(self, path: str, ref: str) -> str | None
    """Tree SHA of directory `path` at `ref` (GET contents of its parent, pick the entry). None if absent."""
async def get_file(self, path: str, ref: str) -> bytes | None   # Accept: application/vnd.github.raw
async def get_ref_sha(self, branch: str) -> str
async def list_prs(self, state: str = "all", per_page: int = 50) -> list[dict]   # first page, sorted by updated
```
- Keep `label_actor` as a wrapper around `label_event`, so autodev is unchanged.
- `set_status` truncates the description to 140 characters, GitHub's limit.

**Tests first:**
- one respx test per method: the request path and params, and the parsed result
- `dir_sha` returns `None` when the directory is missing at that ref
- `set_status` counts as a write, so the throttle applies

**Verify:** `pixi run test`. **Commit:** `autodev: GitHub calls for issues by marker, statuses, tree SHAs, raw files`.

### Task 3: autodev `trusted_bots` and `required_checks`

**Files:** `autodev/config.py`, `autodev/activities.py` (`poll_issues`, `observe_pr`), `autodev.yaml.example`;
tests in `tests/test_autodev_activities.py`, `tests/test_autodev_config.py`.

- **Config.** `GitHubCfg.trusted_bots: list[str] = []` (logins ending in `[bot]`; the validator rejects
  anything else) and `GitHubCfg.required_checks: list[str] = []`.
- **`poll_issues`.** An actor in `trusted_bots` passes the label gate without a permission lookup. Today
  `GitHub.permission` returns `"none"` for bots, which stays the case for everyone else.
- **`observe_pr`.**
  1. Combine as today.
  2. Then, if any `required_checks` context is absent or pending in `statuses(head)` and `check-runs(head)`,
     set `checks = "pending"`.
  3. A required context that failed sets `"failure"`.

  `decide()` is unchanged: pending already means WAIT.

**Tests first:**
- `test_trusted_bot_label_starts_workflow`, and `test_untrusted_bot_label_is_ignored`
- `test_required_check_missing_means_pending`: approved and the `check` workflow succeeded, but
  `mia-data/crops` is absent. The snapshot has `checks="pending"` and `decide` returns WAIT.
- `test_required_check_failed_means_failure`
- the config validator rejects `trusted_bots: [alice]`

**Verify:** `pixi run test`. **Commit:** `autodev: trusted bot triggers and required status contexts`.

### Task 4: One Temporal server, per-app namespaces, mia-data run script

**Files:** `scripts/autodev-up.sh`; new `scripts/mia-data-up.sh`; `autodev/ops.py` if `up` / `ps` need the
change.

- **`autodev-up.sh` reuses a running server.** If `temporal operator cluster health --address
  $TEMPORAL_ADDRESS` already succeeds, skip the `temporal` tmux window. The script that started the server
  owns it. Then ensure the deployment's namespace exists:
  ```bash
  "$TEMPORAL" operator namespace describe -n "$NS" --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1 \
    || "$TEMPORAL" operator namespace create -n "$NS" --address "$TEMPORAL_ADDRESS"
  ```
  `NS` comes from `c.temporal.namespace` (add it to the vars block). The existing deployment stays on
  `default`.
- **`mia-data-up.sh`** does the same as `autodev-up.sh` for mia-data, reading `mia_data.yaml`: ensure the
  `mia-data` namespace, start its own csub broker on `/scratch/$USER/mia-data/csub.sock`, and start the worker
  inside `sandbox-run.sh`. The sandbox mounts are:
  - read-only: repo, config, secret, `harness.dir`
  - read-write: `state_dir`, `work_root`, `storage.root`

  Allowed hosts are `github.com`, `api.github.com` and `localhost`. The script then runs
  `mia-data schedule apply`. It never starts a Temporal server: it exits with a message if none is healthy.
- `ponytail:` two up-scripts that share a lot. Merge them only if a third app appears.

**Verify (manual):**
1. Run `pixi run ad up` twice with two configs whose deployments differ (the existing one, then a scratch
   `autodev-snippets-test`). Only one `temporal` window exists, and
   `temporal operator namespace list` shows both namespaces.
2. Run `pixi run ad down` for each.

**Commit:** `scripts: share one Temporal dev server; ensure namespaces; add mia-data-up.sh`.

---

## Phase B — `mia-snippets`: spec, harness, CI, first snippets

All Phase B work happens in a clone of `JaneliaSciComp/mia-snippets`. It is human-authored and merged by
normal PR review, except that milestone 1 also requires `mia-data test-pr` (Task 19) to pass on the snippet
PRs. Tasks 10–11 therefore merge after Task 19 works, or with a maintainer override recorded in the PR.

### Task 5: Repo bootstrap, `SPEC.md`, harness skeleton, `api.py`, `binding.py`

**Files:** `SPEC.md`, `README.md`, `WITHDRAWN.yaml` (`[]`), `harness/pixi.toml` (python 3.12, numpy,
tifffile, imagecodecs, mrcfile, h5py, pypng, httpx, pyyaml, pydantic; test feature: pytest),
`harness/src/mia_harness/{__init__,api,binding}.py`, `harness/tests/test_binding.py`.

**`api.py`** (the contract; copy §5.2 exactly):
```python
SPEC_VERSION = 1
AXES = "czyx"

@dataclass(frozen=True)
class Source:
    urls: tuple[str, ...]
    params: dict

class Array(Protocol):
    axes: str; shape: tuple[int, ...]; dtype: np.dtype
    voxel_size_nm: dict[str, float] | None
    access: Literal["chunk", "plane", "file"]
    def read(self, region: dict[str, int | slice]) -> np.ndarray: ...

def check_array(a: Array) -> None:
    """axes ⊆ czyx, unique, contain y and x, len(axes) == len(shape), access valid. Raises ValueError."""
```
**`binding.py`:** pydantic models `ArrayBinding(snippet, urls, params)` and
`LabelBinding(ArrayBinding + pairs_with, label_key, segmentation_type ∈ {instance, semantic, binary})`. Also
`Binding(record, record_commit, snippets_commit, raw: list[ArrayBinding] (≥ 1), labels: list[LabelBinding],
reasons: list[str])`, `load_binding(path) -> Binding` and `binding_hash(path) -> str`, the sha256 of the
bytes.

**Tests first:**
- the §5.4 example parses
- unknown keys, an empty `raw`, `pairs_with` out of range and a bad `segmentation_type` are rejected
- `check_array` rejects `axes="zxy "` and missing `y`

**Verify:** `cd harness && pixi run test`. **Commit:** `harness: contract types and binding schema`.

### Task 6: `io`: range files, cache, byte cap, zip members, validators

**Files:** `harness/src/mia_harness/{io,remotezip}.py`, `harness/tests/test_io.py`,
`harness/tests/rangeserver.py` (a thread running an `http.server` handler that honours `Range` and serves ETag
and Last-Modified; the stdlib handler ignores `Range`).

**Interface:**
```python
class BudgetExceeded(RuntimeError): ...

class IO:
    def __init__(self, cache_dir: Path, staging_dir: Path, byte_cap: int, client: httpx.Client | None = None): ...
    def open(self, url: str) -> BinaryIO          # seekable; reads = range GETs (block-aligned, 1 MiB blocks, cached on disk)
    def fetch(self, url: str) -> Path             # whole file into staging, once (cache hit = 0 bytes)
    def list(self, url: str) -> list[str]         # zip members only (via remotezip)
    bytes_fetched: int
    validators: dict[str, dict]                   # url -> {"etag", "last_modified", "size"}
def from_env(work: Path) -> IO                    # byte cap from MIA_HARNESS_JOB_BYTES (required in managed jobs)
```
- **URL schemes.** Only `https`/`http` URLs, plus the `<zip url>::<member>` form. `s3://bucket/key` is
  rewritten to `https://bucket.s3.amazonaws.com/key` (public buckets only).
- **Byte cap.** It is checked before each request (`bytes_fetched + request_size > cap` → `BudgetExceeded`)
  and counts response bytes.
- **Zip members.** `remotezip`:
  1. reads the end-of-central-directory record from the last 64 KiB and the central directory, including
     ZIP64
  2. for a stored member (method 0), `open` returns a range file over its data offset
  3. for a deflated member (method 8), `open` raises `NeedsFetch`, and `fetch` downloads the compressed bytes
     and inflates them into staging

  A snippet calls `io.fetch` for deflated members, which is why `tiff-zip-member` has `access: file` when the
  members are deflated.
- **Path safety.** The cache key is the sha256 of the URL. Staging paths never come from member names, so a
  member called `../x` can't escape.

**Tests first:**
- range reads return the right bytes and are counted
- a second read is served from the cache, adding 0 bytes
- going over the cap raises `BudgetExceeded`, and the counter is not reset by a new `IO` over the same
  `cache_dir` (the cap is per job, i.e. per process)
- validators are recorded
- a stored zip member is readable by range
- a deflated member needs `fetch`, and the fetched bytes match
- a server that answers a range request with `200` and the whole body → `io` raises instead of silently
  downloading the whole file
- a ZIP64 fixture works
- member `../evil` is fetched to a safe staging path

**Verify:** `pixi run test`. **Commit:** `harness: io with range reads, cache, byte cap and remote zip members`.

### Task 7: Snippet loader and `harness smoke`

**Files:** `harness/src/mia_harness/{loader,run,cli}.py`, `harness/tests/test_loader.py`,
`harness/tests/test_smoke.py`, `harness/tests/snippets/` (two toy snippets: `npy-local`, a valid reader of a
local `.npy` file for tests, and `bad-imports`).

**`loader.py`:**
```python
def load_snippet(snapshot_dir: Path, name: str, withdrawn: set[str]) -> ModuleType:
    """Import snippets/<name>/snippet.py from the snapshot only (importlib.util.spec_from_file_location,
    unique module name). Refuse: withdrawn, missing snippet.yaml, spec_version != SPEC_VERSION,
    name mismatch."""
def withdrawn_names(snapshot_dir: Path) -> set[str]       # WITHDRAWN.yaml in the snapshot
def catalog(snapshot_dir: Path) -> list[dict]             # every snippet.yaml, minus withdrawn and superseded
```
**`harness smoke <binding> --snippets <snapshot> --record <record.yaml> --out smoke.json`:**
1. Open every raw and label array through its snippet and run `check_array`.
2. Compare against the record's `technical.arrays` dtype: a mismatch is a warning, not a failure, because
   records are inferred.
3. Check that each label's shape equals its raw's shape on every axis except `c`.
4. Check eligibility per raw: Y ≥ 256 and X ≥ 256.
5. Read one 256×256 patch from plane 0 of the first eligible raw and its labels.

The result is `{"ok", "eligible", "shapes", "errors", "bytes_fetched"}`. Exit 0 when the binding works (even
if the dataset is ineligible), 1 when it doesn't.

**Tests first:**
- smoke on `npy-local` with a 300×300 array → ok and eligible
- with a 200×200 array → ok and not eligible
- a mismatched label shape → error
- a withdrawn snippet is refused
- `spec_version` 2 is refused

**Verify:** `pixi run test`. **Commit:** `harness: snippet loader and smoke`.

### Task 8: Sampler and PNG encoding (pure)

**Files:** `harness/src/mia_harness/{sample,png}.py`, `harness/tests/test_sample.py`,
`harness/tests/test_png.py`.

**`sample.py`:**
```python
@dataclass(frozen=True)
class Volume:            # one eligible raw, by binding index
    index: int; axes: str; shape: tuple[int, ...]; access: str

@dataclass(frozen=True)
class CropSpec:
    raw: int; plane: dict[str, int]; y: int; x: int; size: int = 256

def positions(v: Volume, size: int) -> int:          # planes * (Y-size+1) * (X-size+1)
def candidates(volumes: list[Volume], seed: str, *, size=256, per_plane=4, per_file=8) -> Iterator[CropSpec]:
    """Endless, deterministic for a given seed (random.Random(seed)). Volume chosen ∝ positions; plane
    uniform over non-spatial indices; (y, x) uniform. Groups draws so that `plane` access yields up to
    per_plane candidates per plane, `file` access up to per_file per volume, before moving on."""
# The caller (Task 9) takes candidates until n are accepted or n * max_redraw (10) have been drawn.
def overlap_frac(a: dict, b: dict, size: int) -> float   # same raw/plane only; else 0
```
**`png.py`** (the §6.3 tables, exactly):
```python
def encode_raw(a: np.ndarray) -> tuple[np.ndarray, int, dict]:
    """-> (uint8|uint16 array, bitdepth, mapping). uint8/16: {"kind": "identity"}; int8/16: {"kind": "offset",
    "offset": 128|32768}; else {"kind": "minmax", "min": float, "max": float} (constant crop -> all zeros)."""
def encode_label(a: np.ndarray, seg_type: str) -> tuple[np.ndarray, int, dict]:
    """binary -> uint8 0/1; semantic -> uint16, ValueError if max > 65535 or min < 0; instance -> uint16 kept,
    or renumbered 1..k ({"kind": "renumbered"}) if max > 65535."""
def write_png(path: Path, a: np.ndarray, bitdepth: int) -> None    # pypng, greyscale
def read_png(path: Path) -> tuple[np.ndarray, int]
def stats(a: np.ndarray) -> dict                                    # min, p1, p50, p99, max (source values)
def content_ok(a: np.ndarray) -> bool                               # std > 1e-6 * (max-min+1) and top value share ≤ 0.95
```

**Tests first:**
- the same seed gives the same candidates, and a different seed gives different ones
- volumes are weighted: a 10× larger volume gets about 10× the crops over 2,000 draws, within a tolerance
- `per_plane` grouping
- a rejection causes a redraw, up to the limit
- an existing crop with 30% overlap is rejected, and one with 20% overlap passes
- every row of both §6.3 tables round-trips through `write_png`/`read_png` (int16 offset, float min/max,
  renumbering beyond 65535)
- a semantic label with a value above 65535 raises
- `content_ok` is false for a constant array and for one that is 96% zeros

**Verify:** `pixi run test`. **Commit:** `harness: deterministic sampler and PNG encodings`.

### Task 9: `harness crop`, `harness verify`, `harness preview`

**Files:** `harness/src/mia_harness/{run,verify,preview,cli}.py`, `harness/tests/test_crop_verify.py`.

- **`harness crop <binding> --snippets S --record R --n N --seed SEED --round ROUND --batch B --out DIR [--existing MANIFEST]`:**
  - Opens the arrays and builds `Volume`s for the eligible raws.
  - Drives `candidates(...)`. For each candidate it reads the raw patch (honouring `access`, so for a `plane` group
    it reads the plane once and slices it), then applies `content_ok` and the overlap check.
  - Reads the paired labels at the same plane and box, then encodes and writes
    `DIR/<crop_id>/{raw.png, labels/<label_key>.png, crop.json}`. Here
    `crop_id = f"{ROUND}-b{B}-{i:03d}"`, and `crop.json` has every field in §6.3, including
    `harness_commit` and `env_digest` from `MIA_HARNESS_COMMIT` and `MIA_HARNESS_ENV_DIGEST`.
  - Writes `DIR/report.json` with `{made, rejected_content, rejected_overlap, bytes_fetched, errors}`.
  - Catches `BudgetExceeded`, keeps what was made, and records the error.
- **`harness verify DIR --binding B [--existing MANIFEST] --out verify.json`** loads **no snippets**. It runs
  the §6.4 checks using `crop.json`, the PNGs, and the source shapes the crop job recorded in `report.json`.
  Its output is `{"crops": [{"crop_id", "ok", "errors", "files": {relpath: sha256}}], "ok_count"}`.
- **`harness preview DIR --out PREVIEW`:** one overlay PNG per crop (raw with label boundaries drawn in, using
  numpy only), plus `index.html` with relative links, the crop id, plane, box and licence. No JavaScript.

**Tests first (end to end on `npy-local` with a synthetic 2×600×600 volume and labels):**
- `crop` makes 4 crops whose `crop.json` matches the candidates
- running it again with the same seed gives byte-identical PNGs
- `verify` passes them
- tampering with a PNG makes `verify` fail that crop
- a duplicate checksum under a different `crop_id` fails
- the same `crop_id` with the same checksum in `--existing` passes
- `preview` writes `index.html` that links every crop
- with the byte cap at 1 KiB, `crop` makes 0 crops and reports `BudgetExceeded`

**Verify:** `pixi run test`. **Commit:** `harness: crop, verify and preview commands`.

### Task 10: CI check (`ci/check_pr.py`, `.github/workflows/check.yml`)

**Files:** `ci/check_pr.py`, `ci/maintainers.txt`, `ci/test_check_pr.py`, `.github/workflows/check.yml`.

**`check_pr.py`:**
```python
def check(changed: list[tuple[str, str]], author: str, existing_snippets: set[str], maintainers: set[str],
          read: Callable[[str], str]) -> list[str]:
    """changed = [(status A|M|D|R, path)]. Returns error strings (empty = pass)."""
```
The rules (§5.5):
1. If the author is not a maintainer, every changed path must be added (`A`) under a single
   `snippets/<name>/`, where `<name>` is not in `existing_snippets`.
2. Any `M`, `D` or `R` under an existing snippet fails, whoever the author is (immutability).
3. `snippet.yaml` must validate: `name` equals the directory, `spec_version` is 1, `requires` ⊆ the
   allowlist in `harness/allowlist.txt`, and every `tested_on` entry has `examples/<id>.yaml`.
4. The static import check (an `ast` walk over `snippet.py`) rejects imports of `socket`, `subprocess`,
   `requests`, `urllib`, `http`, `httpx`, `fsspec`, `ftplib`, `pickle`, `shelve`, `ctypes` and `importlib`,
   calls to `os.system`, `os.popen`, `os.exec*`, `os.spawn*` and `__import__`, `allow_pickle=True`, and
   `from snippets` / relative imports.

`check.yml` runs on `pull_request`, computes `changed` with `git diff --name-status origin/main...HEAD`, and
runs `ci/check_pr.py` plus the harness tests.

**Tests first:**
- a good new snippet passes
- a bot editing an existing snippet, editing `harness/`, or editing `WITHDRAWN.yaml` fails
- a maintainer editing `harness/` passes
- two new snippet directories in one PR fail
- `import subprocess`, `__import__("socket")` (rejected via the call to `__import__`) and
  `np.load(..., allow_pickle=True)` fail
- a `tested_on` entry without an example fails

**Verify:** `pytest ci`.
**Commit:** `ci: PR rules for immutable snippets`.
**Then:** set branch protection on `main`: require `check` and the `mia-data/crops` status, the latter from
the mia-data app once Task 19 exists; require one approving review; dismiss stale approvals.

### Task 11: First snippets, `tiff-zip-member` and `mrc-http` (human PRs)

**Files:** `snippets/tiff-zip-member/{snippet.yaml, snippet.py, README.md, examples/bbbc034-hipsc-nuclei-3d.yaml}`
and `snippets/mrc-http/{…, examples/czcdp-10003-mpneumoniae-chloramphenicol-ribosomes-cryoet.yaml}`.

- **`tiff-zip-member`** reads one TIFF (multi-page or single) from a zip member or a plain URL, with
  `tifffile`.
  - If the member is deflated, it calls `io.fetch` and sets `access: file`. If it is stored, it uses
    `io.open` with `access: plane`.
  - Axes come from tifffile's series axes, mapped to `czyx` (`Q`/`I` → `z`, `S` → `c`). The `axes` param
    overrides them and requires a reason in the binding.
- **`mrc-http`** reads an MRC/REC file over range requests.
  - It parses the 1024-byte header itself (`nx, ny, nz`, mode, `nsymbt`), handling modes 0, 1, 2, 6 and 12
    (float16).
  - Data starts at `1024 + nsymbt`. The axes are `zyx` and `access: chunk`.
  - `read({"z": k, "y": slice, "x": slice})` fetches only the rows `y0…y1` of plane `k` as one range
    (`ny`-row block × `nx` × itemsize), then slices `x`.
  - The voxel size comes from the header cell dimensions.
- **Each snippet's own tests** run on synthetic files served by the test `rangeserver` (a deflated zip, a
  stored zip, a small MRC).
- **Real data**, from the `harness/` checkout:
  `harness smoke <example binding> --snippets . --record <record.yaml>` succeeds.
  - For bbbc034, both raw and label are 52×1024×1024, so the dataset is eligible.
  - For czcdp-10003, `bytes_fetched` stays under 20 MB.
- **PRs.** Open one PR per snippet. CI passes. Merge after `mia-data test-pr` posts a passing `mia-data/crops`
  (Task 19). Until then, record a maintainer override comment on the PR.

**Commit:** one per snippet, `snippets: tiff-zip-member` and `snippets: mrc-http`.

---

## Phase C — `mia_data` package (this repo)

### Task 12: Package, config, models, labels

**Files:** `mia_data/{__init__,config,models}.py`, `mia_data.yaml.example`, `pyproject.toml` (add
`mia_data` to the wheel packages and a `mia-data = "mia_data.cli:main"` script; pixi task `md = "mia-data"`),
`tests/mia_helpers.py`, `tests/test_mia_config.py`.

- **`config.py`** follows `autodev/config.py`: pydantic models, YAML plus env overrides (`MIA_DATA_CONFIG`,
  `TEMPORAL_ADDRESS`), and secrets only from env or files.
  - The models mirror §10: `github` (repos, auth fields as in autodev's `GitHubCfg`), `temporal`, `paths`,
    `storage`, `crops`, `limits`, `agent` (autodev's `AgentCfg` shape: model, walltime, cpus, mem, extra
    mounts, `path_prepend`), and `harness.dir`.
  - Add `deployment: str = "mia-data"`, which names the csub session.
  - Validation: every path is resolved, and `state_dir` doesn't overlap `work_root` or `storage.root`. Limits
    are finite and positive, and `agent.allow_hosts` never contains GitHub hosts (reuse autodev's check).
- **`STAGES`** = `{go, matching, developing, testing, pr-ready, cropping, done, needs-human, ineligible, new}`
  with colours and descriptions, as `data:<stage>`.
- **`models.py`** (plain dataclasses, JSON-serialisable for Temporal):
  ```python
  @dataclass
  class GoEvent: event_id: str; actor: str
  @dataclass
  class PrUpdate: pr: int; head_sha: str; state: str; merge_sha: str | None; snippet_issue: int; needs_human: bool
  @dataclass
  class Pins: record: str; record_commit: str; snippets_commit: str; harness_commit: str; env_digest: str
  @dataclass
  class JobDone: job_id: str; state: str; exit_code: int | None
  @dataclass
  class Test: sha: str; test_id: str; ok: bool | None = None; gallery: str | None = None; tree_sha: str | None = None
  @dataclass
  class DatasetState:
      issue: int
      schema: int = 1
      admitted: list[str] = field(default_factory=list)   # go event ids, newest last; keep ≤ 50
      round_id: str | None = None
      seed: str | None = None
      stage: str = "new"
      pins: Pins | None = None
      binding: str | None = None        # path under state_dir
      binding_hash: str | None = None
      snippet_name: str | None = None
      snippet_issue: int | None = None
      pr: int | None = None
      tests: list[Test] = field(default_factory=list)
      attempts: dict[str, int] = field(default_factory=dict)   # stage -> attempts used
      pending: list[PrUpdate] = field(default_factory=list)
  ```
  plus the small activity input and output dataclasses, defined in Task 16 next to their activities.

**Tests first:**
- the example config loads
- an overlapping `state_dir` is rejected
- `allow_hosts: [api.github.com]` is rejected
- `max_pr_revisions: 0` is rejected

**Commit:** `mia_data: package, config and models`.

### Task 13: Catalog reader and `mia-data seed`

**Files:** `mia_data/{catalog,seed}.py`, `tests/test_mia_seed.py`, `tests/fixtures/mia/{downloads.json,
downloads-native.json, records/*.yaml}` (trimmed real data: bbbc034, czcdp-10003, and one small dataset such
as `hf-pytc-zebrafinch-j0126-ffn-em`).

- **`catalog.py`:**
  - `downloaded(gh_catalog) -> dict[id, entry]` reads the union of both files from `state` via `get_file`.
    For an id in both, it keeps the newest `at` and remembers both downloaders.
  - `record_paths(gh_catalog) -> dict[id, path]` is one git-trees call on `main`.
  - `record(gh_catalog, path, ref) -> dict`.
- **`seed.py`:**
  - `render_issue(rec, entry) -> (title, body, labels)` follows §7.1. `body` starts with
    `<!-- mia-data:record=<id> -->`. `maybe-small` is set when no raw array in `technical.arrays` has
    Y and X ≥ 256 (axes may be missing; then use the last two dimensions).
  - `async seed(cfg, gh_data, gh_catalog, dry_run) -> list[str]` creates the missing issues. It looks for
    existing ones with `find_issue_by_marker`, fetched once as a set of markers from all issues, not one
    search per record.
- **Rate limits.** The client already throttles creates. Seed prints progress and is safe to interrupt.

**Tests first (respx):**
- three fixtures make three issues with the right titles, labels and markers
- a second run creates none
- a record missing from `main` is skipped with a warning
- `maybe-small` is set for the 150³ record and not for bbbc034
- the licence appears in the body

**Commit:** `mia_data: seed one issue per downloaded dataset`.

### Task 14: Binding checks, status rendering, publication

**Files:** `mia_data/{binding,status,publish}.py`, `tests/test_mia_binding.py`, `tests/test_mia_status.py`,
`tests/test_mia_publish.py`.

- **`binding.py`** (worker side; no harness import, plain YAML and dicts):
  `check_binding(b: dict, record: dict, catalog_names: set[str]) -> list[str]`. It checks:
  - `record` and `record_commit` match
  - every URL's host appears in the record's URLs (`data.download_url`, `technical.sample.urls`,
    `technical.arrays[].path_pattern` hosts)
  - every `snippet` is in `catalog_names`
  - `label_key` matches `^[a-z_]+-[a-z0-9_]+-(instance|semantic|binary|points)$`, and `pairs_with` is in
    range

  `hosts(b) -> list[str]` gives the job allowlist.
- **`status.py`:**
  - `render_status(state, links) -> str` renders the in-place status comment, with marker
    `<!-- mia-data:status -->`, the stage, the binding in a fenced YAML block, the PR, the gallery and the
    totals.
  - `render_stage(stage, detail) -> str` renders the short per-transition comment.
  - Every piece of agent-derived text goes through `autodev.activities.neutralize`.
- **`publish.py`:**
  ```python
  def publish(dataset_dir: Path, src_dir: Path, verified: list[dict]) -> dict:
      """verified = [{"crop_id", "files": {rel: sha256}}]. For each: skip if dest exists with same hashes;
      raise Conflict if hashes differ; else copytree to .<id>.tmp, re-hash, os.rename into place
      (FileExistsError/OSError ENOTEMPTY -> re-check hashes). Then rebuild manifest.json from */crop.json
      (tmp + os.replace). Deletes stale .*.tmp. Returns {"published": [...], "already": [...], "total": n}."""
  ```

**Tests first:**
- `publish` twice → the second call publishes nothing and reports everything as "already"
- a conflicting existing crop raises
- a crash simulated between copy and rename (leave `.<id>.tmp`) → the next run cleans it up and publishes
- the manifest lists exactly the crop directories
- a host not in the record is rejected
- an unknown snippet is rejected
- the status comment contains no `<!--` from agent text

**Commit:** `mia_data: binding checks, status comments, idempotent publication`.

### Task 15: Job specs, snippet snapshots, match prompt

**Files:** `mia_data/jobspec.py`, `mia_data/prompts/match.md`, `tests/test_mia_jobspec.py`.

- **Snippet snapshots.**
  - `snapshot(cfg, sha) -> Path` keeps a bare mirror of `mia-snippets` in
    `state_dir/mirror/mia-snippets.git`. It uses `autodev.gitops`'s token-in-env pattern: plain fetch, with
    hooks disabled.
  - It extracts `git archive <sha> snippets WITHDRAWN.yaml SPEC.md` into
    `work_root/_snapshots/<sha>/` (write the `.tmp` directory, then rename; skip if it exists) and makes it
    read-only (`chmod -R a-w`).
  - It returns the path, which jobs mount read-only.
- **Harness pin.**
  - `harness_pin(cfg) -> (commit, env_digest)` reads `git -C harness.dir rev-parse HEAD` and the sha256 of
    `harness.dir/harness/pixi.lock`.
  - `assert_pin(cfg, pins)` raises a non-retryable error when the pin has moved since admission (Task 0,
    decision 3).
- **Job builders.** Each returns an `autodev.jobs.JobSpec`. All of them set `MIA_HARNESS_JOB_BYTES`,
  `MIA_HARNESS_COMMIT`, `MIA_HARNESS_ENV_DIGEST` and `ro_mounts` (`harness.dir`, the snapshot, extra inputs),
  and all write `exit_code`. The command prefix is
  `pixi run --manifest-path <harness.dir>/harness/pixi.toml harness …`.

  | builder | what it runs | `claude` | `allow_hosts` |
  |---|---|---|---|
  | `match_job(cfg, ws, pins, record_path, dl_dir)` | `claude -p "$(cat prompt.md)" --max-turns N --allowedTools "Bash(harness smoke:*),Read,Write,Glob,Grep" --output-format json > claude.json` | true | Anthropic + `hosts(record)` |
  | `smoke_job(cfg, ws, binding, snapshot)` | `harness smoke …` | false | `hosts(binding)` |
  | `crop_job(cfg, ws, binding, snapshot, n, seed, round_id, batch, existing)` | `harness crop …` | false | `hosts(binding)` |
  | `verify_job(cfg, ws, crops_dir, binding, existing)` | `harness verify … && harness preview …`; crops mounted read-only | false | none |

  The Anthropic hosts are `api.anthropic.com`, `claude.ai` and `platform.claude.com`.
- **`prompts/match.md`** is §7.2 as instructions:
  - Read `record.yaml`, `downloads.json`, any `conversion.json`, `catalog/*.yaml` and `SPEC.md`.
  - Write `binding.yaml`, and iterate with `harness smoke binding.yaml --snippets ./snapshot --record record.yaml --out smoke.json`.
  - Finally write `outcome.json`, with one of the four outcomes.
  - Rules: never invent values, give a reason for every override, never write snippet code, stop when the
    dataset is ineligible.

**Tests first:**
- `snapshot` twice for the same SHA extracts once, and the result is read-only (against a local repo made
  with `tests/autodev_helpers.make_origin`)
- `assert_pin` raises after a new commit in a fake `harness.dir`
- `match_job` hosts include Anthropic plus record hosts and never GitHub
- `verify_job` has no hosts and mounts the crops read-only
- every builder sets `MIA_HARNESS_JOB_BYTES` from `limits.job_bytes`

**Commit:** `mia_data: job specs, snippet snapshots, match prompt`.

### Task 16: Activities

**Files:** `mia_data/activities.py`, `tests/test_mia_activities.py`.

`class Activities` holds `cfg`, `gh_data`, `gh_snippets`, `gh_catalog`, `jobs` (an `autodev.jobs.CsubJobs`
with session `cfg.deployment` and `id_dir = state_dir/jobs`) and a Temporal `client`. Wrap every activity in
autodev's `permanent_errors`. Activities return small dataclasses.

| activity | does |
|---|---|
| `admit(issue, event_id) -> Pins` | read the issue body marker → record id; resolve `record_commit` from downloads state, `snippets_commit = get_ref_sha("main")`, `harness_pin`; write `state_dir/rounds/<round_id>/record.yaml` (from `record_commit`) once |
| `set_stage(issue, stage)` | swap `data:*` stage labels; edit or create the status comment |
| `comment(issue, body, marker)` | once per marker (reuse autodev's pattern) |
| `prepare_match(issue, round_id, pins) -> str` | workspace with `prompt.md`, `record.yaml`, `downloads.json`, `conversion.json` copies, `catalog/` (snippet.yaml files from the snapshot), `SPEC.md`; returns ws path |
| `submit(key, kind, args) -> str` | build the `JobSpec` for `kind` ∈ match/smoke/crop/verify, `assert_pin`, `jobs.submit(key, spec)` |
| `await_job(job_id, ws) -> JobDone` | `jobs.wait` with heartbeat (runs on the `-await` queue) |
| `read_match(ws) -> MatchResult` | `_read_agent_file(ws, "outcome.json")` + `binding.yaml`; parse defensively; on `matched`/`ineligible`, `check_binding`; copy the binding to `state_dir/rounds/<round>/binding-<n>.yaml`, return path + hash |
| `read_smoke(ws) -> SmokeResult` / `read_verify(ws) -> VerifyResult` | parse the harness JSON outputs |
| `publish_test(issue, round_id, sha, crop_ws, verify_ws) -> str` | copy previews + crops to `storage.root/_tests/<record>/<round>/<sha>/`; return gallery URL (or path) |
| `publish_crops(issue, round_id, crop_ws, verified) -> dict` | `publish.publish(...)` into the dataset folder (Task 14) |
| `open_snippet_issue(issue, round_id, draft) -> int` | find by marker `<!-- mia-data:issue=N round=R -->` in `mia-snippets`, else create with label `snippets:go` |
| `pr_state(pr) -> PrUpdate` | `get_pr` + labels on the snippets issue |
| `set_crop_status(sha, state, url)` | `gh_snippets.set_status(sha, "mia-data/crops", …)` |
| `tree_sha(sha, name) -> str` | `gh_snippets.dir_sha(f"snippets/{name}", sha)` |
| `poll()` | Task 17 |

- **Workspace layout.** `work_root/issue-<n>/<round_id>/<kind>-<attempt>`. Job keys are
  `mia-<n>-<round_id>-<kind>-<attempt>`, which keeps them unique per round, stage and attempt.
- **`round_id`** is `r` plus the first 7 characters of sha1(`<issue>/<event_id>`), used as the crop-id
  prefix.

**Tests first (fakes for GitHub and csub, as in `test_autodev_activities.py`):**
- `admit` is idempotent, writing `record.yaml` once
- `read_match` with a symlinked `outcome.json` → treated as missing
- `read_match` with a binding that names a host outside the record → `needs-human`, with the reason
- `submit` refuses when the harness pin has moved
- `open_snippet_issue` twice → one issue
- `publish_test` returns `public_url` + path when it is set, and the path when it isn't

**Commit:** `mia_data: activities`.

### Task 17: `MiaDataDataset` (match → test → crop → publish) and `MiaDataPoll`

**Files:** `mia_data/workflows.py`, `tests/test_mia_workflows.py` (time-skipping environment with mocked
activities, as `tests/test_autodev_workflows.py` does).

**Workflow skeleton** (thin; all names are activity names):
```python
@workflow.defn(name="MiaDataDataset")
class MiaDataDataset:
    @workflow.init
    def __init__(self, s: DatasetState): self.s = s; self.go: list[GoEvent] = []

    @workflow.signal
    def go(self, e: GoEvent) -> None: self.go.append(e)
    @workflow.signal
    def pr_update(self, u: PrUpdate) -> None: self.s.pending.append(u)
    @workflow.query
    def status(self) -> DatasetState: return self.s

    @workflow.run
    async def run(self, s: DatasetState) -> None:
        while True:
            await workflow.wait_condition(lambda: bool(self.go))
            e = self.go.pop(0)
            if e.event_id in self.s.admitted: continue                 # redelivery
            self.s.admitted = (self.s.admitted + [e.event_id])[-50:]
            await self._round(e)                                       # never raises; ends in a terminal stage
            if workflow.info().is_continue_as_new_suggested() and not self.go:
                workflow.continue_as_new(self.s)
```
- **`_round`:**
  1. Set `round_id`, and `seed = str(workflow.random().getrandbits(64))`. Call `admit` → `pins`, and set
     stage `matching`.
  2. Submit `match` and await it, then `read_match`.
  3. On `matched`:
     1. smoke job → stage `testing`
     2. `crop` (n = `test_per_round`, seed = `seed + "-t"`, batch 0)
     3. `verify`
     4. `publish_test`, then report on the issue with the gallery
     5. on pass → stage `cropping`: `crop` (n = `per_round`, seed = `seed + "-p"`, batch 1, existing =
        manifest) → `verify` → `publish_crops` → `done`, or `needs-human` if fewer than half of n survive
  4. On `ineligible` → `ineligible`. On `blocked`, or any failure → `needs-human` with the reason (autodev's
     `report_failure` style).
  5. On `needs_snippet` → Task 20 (until then → `needs-human` "development not wired yet").
- **Signals while busy.** When `_round` returns, every `go` still queued arrived while the round was running.
  Reject each one: post a "busy, ignored" comment (marker `busy <event_id>`) and add its ID to `admitted`, so
  it is never admitted later. A `go` that arrives after that, while the workflow waits, starts the next
  round.
- **Limits.** `attempts[kind] < limits.max_stage_attempts`, or the round goes to `needs-human`. The job count
  per round is fixed by this code path (§6.5).
- **Timeouts.** Await timeouts follow autodev: walltime + queue slack, heartbeat 3 minutes.

**`MiaDataPoll`** is a scheduled workflow that calls the `poll` activity. That activity:
1. For each open data issue labelled `data:go`, gets `(actor, event_id) = label_event`.
   - If the actor lacks write access: remove the label and comment once, as autodev does.
   - Otherwise `signal_with_start(MiaDataDataset.run, DatasetState(n), id=f"mia-data/{repo}/issue-{n}",
     start_signal="go", start_signal_args=[GoEvent(event_id, actor)])`, then remove the label.
2. For PRs in `mia-snippets` whose head branch is `snippets/issue-<K>`, reads the issue K body marker
   `mia-data:issue=N round=R` and signals `pr_update` to dataset N with `get_pr` state. This covers the first
   page of `list_prs(state="all")` (`ponytail:` first page only; paginate if more than 50 PRs are active).

**Tests first:**
- `test_match_test_crop_done`: the activities are called in order, the stage labels in order are
  `matching, testing, cropping, done`, and `publish_crops` is called once
- `test_redelivered_go_is_ignored`: the same event_id signalled twice gives one round
- `test_go_while_busy_is_rejected_with_comment`
- `test_ineligible`, `test_blocked_goes_needs_human`, `test_failed_job_needs_human_no_retry`
- `test_test_crop_failure_never_produces`: verify returns `ok_count=0` → `needs-human`, and `crop` is never
  called with batch 1
- `test_worker_death_mid_await_reattaches`: the await activity fails once, and `submit` is called once per key
- `test_continue_as_new_keeps_admitted`: a forced rollover, then redelivering the old event → no new round
- `test_seed_is_recorded`: the seed in the workflow state equals the one passed to the crop jobs; replay is covered
  in Task 18

**Commit:** `mia_data: dataset workflow (match/test/crop) and poller`.

### Task 18: CLI and replay test

**Files:** `mia_data/cli.py`, `tests/test_mia_cli.py`, `tests/fixtures/mia/dataset_history.json`, a replay
test in `tests/test_mia_workflows.py`.

- **Commands** (mirror `autodev/cli.py`):
  - `check`: GitHub identity on all three repos, Temporal, the namespace, csub probe, directories, harness pin
  - `init-labels` on the data repo
  - `seed [--dry-run]`
  - `worker`: a main queue `mia-data` with workflows and activities, and the `mia-data-await` queue with
    `await_job`
  - `schedule apply`
  - `test-pr` (Task 19)
  - `crops-by-snippet <name>`, which scans `storage.root/*/manifest.json` and prints the crops whose
    `crop.json` names that snippet
- **Replay test.** After Task 17's tests pass, record the history of `test_match_test_crop_done` into the
  fixture and add `test_replay_dataset_history` using `temporalio.worker.Replayer`, as
  `test_replay_recorded_history` does for autodev.

**Verify:** `pixi run test`; `pixi run md check` against a real config.
**Commit:** `mia_data: CLI and replay test`.

### Task 19: `mia-data test-pr`, manual crop-test admission

**Files:** `mia_data/cli.py`, `mia_data/activities.py` (reuse), `tests/test_mia_cli.py`.

`mia-data test-pr <pr> --binding <file> --record <id>` does the following, without Temporal:
1. Resolves the PR head SHA and posts `mia-data/crops` as `pending`.
2. Snapshots that SHA, runs `check_binding`, then runs the crop job, then the verify job, through
   `CsubJobs`, waiting synchronously.
3. Copies the result to `_tests`, posts the PR comment with the gallery link, and sets the status to success
   or failure on that SHA.

This is how the human-authored snippet PRs from Task 11 get their required status (spec §12.4).

**Tests first:**
- with fakes, the sequence is: pending status → crop job → verify job → success status on the same SHA
- verify failing → failure status
- the head moving during the run → the status is still posted on the original SHA, and the output says the
  head moved

**Verify (real):** run on the `tiff-zip-member` PR. The PR shows `mia-data/crops` ✓, and the gallery shows 4
bbbc034 crops with nucleus overlays. Then merge Task 11's PRs.
**Commit:** `mia_data: test-pr command for crop checks on snippet PRs`.

### Task 20: Milestone 3, end to end on bbbc034

**No new code** unless the run exposes a bug. If it does, fix it test-first in the owning task's files.

- [ ] Fill in `mia_data.yaml` (from the example). Run `pixi run md init-labels`, then `pixi run md seed`.
  Check that 49 issues exist and that a second run creates none.
- [ ] Start everything:
  1. `pixi run ad up`, for the existing deployment and the Temporal server
  2. `scripts/mia-data-up.sh`
- [ ] Add `data:go` to the bbbc034 issue. Expect, within about 2 minutes, `matching`, then `testing` (a
  gallery link with 4 crops), then `cropping`, then `done`, with 16 crops. Check that each `crop.json` has
  the licence, validators, mapping and stats.
- [ ] Redeliver the trigger: add `data:go` again while the issue is in `cropping`. Expect a "busy, ignored"
  comment and no second round.
- [ ] Restart the worker mid-round (`tmux kill-window` on the worker, then rerun the script). Expect the
  round to finish, no duplicate job IDs in `state_dir/jobs`, and the same crop set.
- [ ] Interrupt publication: kill the worker while `publish_crops` runs (add a temporary sleep under an env
  flag, or use a large n). After restart: the same crops, `manifest.json` complete, and no `.tmp` left.
- [ ] Run `pixi run md crops-by-snippet tiff-zip-member` and check that it lists the 16 crops.
- [ ] Write down what happened (times, bytes fetched, rough cost) in a short section of
  `notes/autodev-runbook.md`, or in a new `notes/mia-data-runbook.md` if the material outgrows a section.

**Commit:** `mia-data: runbook and milestone 3 notes`.

---

## Phase D — development path

### Task 21: Develop path in `MiaDataDataset`

**Files:** `mia_data/workflows.py`, `mia_data/activities.py`, `tests/test_mia_workflows.py`; extend the
replay fixture.

- **On `needs_snippet`:**
  1. `open_snippet_issue(draft)`, record `snippet_issue`, and set stage `developing`.
  2. Then loop on `pending` `PrUpdate`s. For every update, call `pr_state(pr)` and use its answer, never the
     signal's (arrival order is not revision order, §8.3). Then:

| current | update | action |
|---|---|---|
| any | `needs_human` on the snippets issue, or PR `closed` without merge | `needs-human` with link; end round |
| developing/testing/pr-ready | open PR, head `H` not yet tested, and `len(tests) < max_pr_revisions` | stage `testing`; `set_crop_status(H, pending)`; read the PR's example binding (`get_file` at `H`), `check_binding`, store it; crop → verify → `publish_test` → `set_crop_status(H, success/failure, gallery)`; record `Test(H, …, tree_sha=tree_sha(H, name))`; success → `pr-ready` |
| any | head `H` tested already | no-op |
| any | open PR, revisions exhausted | `needs-human` "revision limit" |
| pr-ready/testing | `merged`, `merge_sha M` | `t = tree_sha(M, name)`; if a passing `Test` has `tree_sha == t` → pin the binding to `M` (new binding file, `snippets_commit = M`), stage `cropping`; else test `M` as in §7.4 (no status posted; it's not a PR head) and only a pass goes to `cropping` |

  3. Production as in Task 17, with the pinned binding.
- **Snippet name.** The snippet name is the single new directory in the PR. Read it from the example binding's
  `snippet` field and confirm it with `dir_sha(…, H)` being non-null.
- **Stale results.** A result for a test whose `sha` is not the current head is recorded but changes neither
  the stage nor the status of the current head.
- **The poller** already sends `pr_update` (Task 17). Add the snippets issue's `snippets:needs-human` label to
  `PrUpdate.needs_human`.

**Tests first:**
- `test_develop_happy_path`: needs_snippet → issue opened → PR head h1 → test → pr-ready → merged M with an
  equal tree → cropping → done. The statuses posted are pending then success on h1.
- `test_revise_new_head_retests`: h1 passes, then h2 arrives → h2 is tested, and h1's success is not reused
  for pr-ready on h2
- `test_stale_result_ignored`: h1's result arrives after h2 is current → the stage stays on h2
- `test_merge_with_different_tree_retests_merge_commit`, plus its failure case → `needs-human` with no
  production
- `test_closed_pr_needs_human`, `test_revision_limit`
- `test_merge_signal_before_test_finishes`: `pr_state` reports merged while the h1 test is still running →
  wait for the test, then compare trees
- replay of a recorded develop-path history

**Commit:** `mia_data: development path through autodev snippet PRs`.

### Task 22: The autodev `snippets` deployment and milestone 4

- [ ] Write `autodev-snippets.yaml`:
  - `deployment: snippets`, `github.repo: JaneliaSciComp/mia-snippets`
  - `temporal.namespace: snippets`
  - `github.trusted_bots: [<mia-data app login>[bot]]`
  - `github.required_checks: [mia-data/crops]`
  - `agent.allow_hosts`: the data hosts from P3
  - `agent.extra_mounts: ["<harness.dir>:ro"]`
  - `agent.path_prepend` so that `pixi` is on PATH
- [ ] Add to `mia-snippets`' `CLAUDE.md`: read `SPEC.md`, add exactly one new `snippets/<name>/`, put the
  binding you tested in `examples/`, and run
  `harness smoke examples/<id>.yaml --snippets . --record <record>` before finishing. (This is a maintainer
  PR, so CI allows it.)
- [ ] Bring up the snippets autodev: `AUTODEV_CONFIG=autodev-snippets.yaml pixi run ad up`. It reuses the
  running Temporal server (Task 4).
- [ ] Pick a dataset that no current snippet reads, e.g. an HDF5 record such as
  `zenodo-6025935-vesselexpress-lsfm-vasculature`, or an EMPIAR slice stack. Add `data:go` and expect:
  1. `developing`, with a `mia-snippets` issue labelled `snippets:go`
  2. autodev opens a PR
  3. `testing`, with `mia-data/crops` pending on the head
  4. `pr-ready`, with the gallery
  5. review with one `/revise` → a new head is tested
  6. approve → autodev waits for `mia-data/crops` (`required_checks`), then merges
  7. tree-hash equality → `cropping` → `done`
- [ ] **Negative checks:**
  - approving a head before its crop test finishes → autodev doesn't merge
  - pushing a commit by hand that edits an existing snippet → CI fails
- [ ] Add a "development path" section to the runbook.

**Commit (this repo):** `autodev-snippets.yaml.example` and the runbook notes.

### Task 23: Milestone 5, every eligible dataset

- [ ] Add `data:go` to all 49 issues. `ponytail:` by hand or with `gh issue edit` in a loop; there is no
  bulk command.
- [ ] Triage every issue that ends up `needs-human`:
  - fix the binding by re-running the round
  - or let it go through development
  - or confirm that the reason is genuine (licence, unreadable format, missing dependency)
- [ ] **Done when** every issue is `done`, `ineligible` (expect about 9) or `needs-human` with a stated
  reason.
- [ ] Record what each dataset taught us in the runbook: snippets created, failure modes, bytes per crop. This
  feeds the spec's §15 "Later" list.

---

## Self-review notes (for the executor)

- **Spec coverage:**

  | spec | tasks |
  |---|---|
  | §4 lifecycle | 17, 21 |
  | §5 snippet contract | 5, 7, 10, 11 |
  | §6 harness | 6–9 |
  | §7.1 seed | 13 |
  | §7.2 match | 15–17 |
  | §7.3 develop | 21–22 |
  | §7.4 test crops | 17, 19, 21 |
  | §7.5 production | 17 |
  | §7.6 publication | 14 |
  | §8 Temporal | 17, 18, 21 |
  | §9 trust | 1, 10, 15 |
  | §10 config | 12 |
  | §11 tests | throughout |
  | §12 changes to existing code | 1–4, 19, 22 |
- **Deliberately not built:** byte metering across a whole round, 3D/zarr, an agent in production, a
  `common/` package, pagination of PRs beyond the first page.
- **Riskiest assumptions, tested earliest:**
  - P3/P4: data hosts and FTP through csub
  - Task 6: range reads against the real hosts (some return 200 without honouring `Range`; `io` must detect
    that and refuse rather than download whole files silently: a test for it belongs in Task 6)
  - Task 22: autodev's agent can run the harness inside its sandbox
