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
- **Galleries.** The worker commits test and production crops (`raw.png`, `overlay.png`) to a `galleries`
  branch of `mia-workflow-data` and embeds them in issue comments, the way zarr-doctor published histograms.
- **Approval.** A human approves every binding: a new snippet by PR review, a matched binding with
  `data:approve` on the data issue after looking at the test crops.

**Tech Stack:**
- Python 3.12 (pixi), Temporal Python SDK, httpx, pydantic 2, PyYAML
- csub, Claude Code CLI (match jobs only)
- Harness: numpy, tifffile, imagecodecs, mrcfile, h5py, pypng
- pytest, pytest-asyncio, respx

**Spec:** [`notes/mia-crops-spec.md`](../mia-crops-spec.md) v0.8. Section numbers like §7.6 below refer to it.
The spec already holds every planning decision, so there is no spec-editing task (Task 0).

## Before you start (prerequisites, not tasks)

- [ ] **P1. Repos.** Create `JaneliaSciComp/mia-workflow-data` (issues plus a `galleries` branch the worker
  creates on first use, empty README) and `JaneliaSciComp/mia-snippets` (empty). Install the GitHub App used
  by autodev on both, with these permissions:
  - Issues RW, Pull requests RW, Contents RW (both repos: snippet branches and merges on one, gallery files
    on the other)
  - Commit statuses RW, Checks R, Metadata R

  Check: `gh api repos/JaneliaSciComp/mia-snippets` and `gh api repos/JaneliaSciComp/mia-workflow-data` both
  succeed with the app's installation token (`pixi run ad check` against a config pointing at each).
- [ ] **P2. Shared directories** visible from the worker host and from compute nodes, as a sketch:
  - `paths.state_dir`: trusted, never mounted into jobs
  - `paths.work_root`: job workspaces; must be under the csub policy's `allowed_roots`
  - `storage.root`: published crops

  Check: `ls` all three from the worker host and from inside a job,
  `csub submit --walltime 5 --wait -- ls <root>`.
- [ ] **P3. Broker policy** (`~/.config/csub/broker.toml` of the service account). It is edited by hand for
  now (spec §15); the worker only checks it (`csub probe`) and fails closed.
  - `allowed_roots` contains `work_root` and `storage.root`
  - the `[agents]` table is enabled: `claude = ["api.anthropic.com", "claude.ai", "platform.claude.com"]`.
    It is commented out in `csub/deploy/broker.janelia.toml`. Agent jobs get the Anthropic hosts from here,
    never from their own request.
  - `allowed_hosts` adds the catalog's data hosts: `zenodo.org`, `ftp.ebi.ac.uk`, `www.ebi.ac.uk`,
    `data.broadinstitute.org`, `files.cryoetdataportal.cziscience.com`, `huggingface.co`, `*.huggingface.co`,
    `ndownloader.figshare.com`, `figshare.com`, `allencell.s3.amazonaws.com`, `s3.amazonaws.com`
  - **Redirect probe** (§12.5). From the worker host, not from a sandboxed job (the proxy would block the
    hop you are trying to discover): `curl -sIL <url> | grep -i '^location:'` for one URL per host above.
    Add every host in each chain to `allowed_hosts`, and every hop that differs from the record's own host to
    `network.redirect_hosts` in `mia_data.yaml` (exact hosts, no patterns). Paste the probe output into the
    runbook so the next person knows why each host is there.

  Check: `csub submit --walltime 5 --wait --allow data.broadinstitute.org -- curl -sI https://data.broadinstitute.org/bbbc/BBBC034/BBBC034_v1_dataset.zip`
  returns `200`, and `csub probe` lists every host above.
- [ ] **P4. HTTPS mirrors.** Egress is HTTPS-proxy only, so FTP is out (§2). Confirm with the command above
  that the EMPIAR records' `https://ftp.ebi.ac.uk/...` URLs answer `200` through the proxy. Record it.
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
  read-write access only to their own workspace (plus the dataset cache, for jobs that run reviewed code).
  Verify jobs get no network at all.
- **Hosts are a subset of the policy.** Every job's `allow_hosts` is derived from the record or binding plus
  `network.redirect_hosts` and `network.package_hosts`, and is checked against `csub probe` before submission.
  A missing host is `needs-human`, never a workaround.
- **Workflow code does no I/O**, calls activities by name, and stays replay-safe. Every workflow change
  extends the replay test (Task 18).
- **Keep it small.** Each `ponytail:` comment names a deliberate shortcut and its upgrade path.

## Review focus

Reviewers should check these first:
1. **Exact-SHA gating.** Production happens only after one of these:
   - a passing test of the exact merged snippet tree (tree-hash equality with a tested head)
   - a passing test of the merge commit
   - a passing test of the matched `snippets_commit` **and** a human `data:approve` whose label event is
     newer than that test's result

   Nothing resolves `main` again after admission.
2. **Idempotency.** Look at go-event deduplication, the job-id files, publication (rename-if-absent), and
   comments and statuses with markers.
3. **Trust.** The worker never imports a snippet. Jobs can't reach GitHub. Agent files are read safely.
4. **The raw stretch and label mappings** match §6.3 exactly: one percentile stretch for every dtype, `lo`
   and `hi` recorded in `crop.json`, no gain cap.
5. **Galleries.** Only files the verify job checked or wrote reach the `galleries` branch, and only the worker
   writes there.

## File structure

**This repo (`temporal-prototype`):**
```
autodev/
  jobs.py            NEW  generic csub submit/await (split out of agent.py; Task 1)
  agent.py           CHANGED  AgentJobs becomes a thin wrapper over jobs.CsubJobs
  github.py          CHANGED  label_event, create_issue, comments by marker, statuses, dir_sha, get_file; ensure_branch/file_exists/upsert_file back from zarr-doctor (Task 2)
  config.py          CHANGED  github.trusted_bots, github.required_checks, agent.max_budget_usd (Task 3)
  activities.py      CHANGED  poll_issues trusts trusted_bots; observe_pr applies required_checks (Task 3)
mia_data/
  __init__.py
  config.py          Config, load_config (Task 12)
  models.py          dataclasses for workflow state, signals, activity I/O (Task 12)
  catalog.py         downloads state + records from the catalog repo (Task 13)
  seed.py            issue body/labels, idempotent create (Task 13)
  binding.py         worker-side binding checks, job hosts, probe check (no harness import) (Task 14)
  status.py          status-comment, stage-comment and crops-table rendering (Task 14)
  gallery.py         commit crop PNGs to the galleries branch (Task 14)
  publish.py         rename-if-absent publication + manifest rebuild with withdrawn marks (Task 14)
  jobspec.py         job scripts for match / smoke / crop / verify; snapshots of snippets at a SHA (Task 15)
  prompts/match.md   match agent prompt (Task 15)
  activities.py      (Task 16)
  workflows.py       MiaDataDataset, MiaDataPoll (Tasks 17, 20)
  cli.py             check, init-labels, seed, worker, schedule, test-pr, crops-by-snippet, reindex (Task 18)
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
    io.py                  IO: open/fetch/list, range files, byte cap, validators, redirect host check
    cache.py               per-dataset cache for whole-file fetches (§6.6)
    remotezip.py           zip central directory + member access over ranges
    binding.py             pydantic Binding + load_binding
    loader.py              load a snippet module from a snapshot dir; withdrawn/superseded/spec_version checks
    sample.py              pure planner: volumes + rng -> CropSpec list; overlap test
    png.py                 raw percentile stretch + label encoders per §6.3, decoders for verify
    run.py                 smoke and crop execution
    verify.py              verify (no snippet code)
    preview.py             label overlays (§6.7)
    cli.py                 `harness smoke|crop|verify|preview`
  allowlist.txt            dependencies a snippet may `requires`
  tests/…
snippets/tiff-zip-member/…  (Task 10)
snippets/mrc-http/…         (Task 11)
```

---

### Task 0: Spec

No task. `notes/mia-crops-spec.md` v0.8 already contains every decision this plan relies on: separate verify
jobs, `mia-data/crops` as a commit status, the pinned harness checkout, autodev `required_checks`,
`tiff-zip-member` and `mrc-http`, the `data:approve` gate, the dataset cache, the percentile stretch, the
galleries branch, licence exclusion and the csub host rules. If implementation forces a change, edit the spec
first, bump its version, and cite the section here.

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
async def pr_files(self, number: int) -> list[str]                                # first page of paths, for "touches snippets/?"
async def ensure_branch(self, branch: str) -> bool                                # orphan commit if missing
async def file_exists(self, path: str, ref: str) -> bool
async def upsert_file(self, path: str, content: bytes, branch: str, message: str) -> bool
```
- Keep `label_actor` as a wrapper around `label_event`, so autodev is unchanged. `label_event` also returns
  the event's `created_at`, which the approve gate compares with the test result time.
- `set_status` truncates the description to 140 characters, GitHub's limit.
- `ensure_branch`, `file_exists` and `upsert_file` come back verbatim from zarr-doctor
  (`git show 7abe195~1:zarr_doctor/github_api.py`), including the in-process commit lock: Contents API PUTs
  to one branch race with 409. Their tests are in `7abe195~1:tests/test_github_api.py`; port them.

**Tests first:**
- one respx test per method: the request path and params, and the parsed result
- `dir_sha` returns `None` when the directory is missing at that ref
- `set_status` counts as a write, so the throttle applies
- `upsert_file` skips an existing path without a PUT; `ensure_branch` is a no-op the second time

**Verify:** `pixi run test`. **Commit:** `autodev: GitHub calls for issues by marker, statuses, tree SHAs, raw files, branch files`.

### Task 3: autodev `trusted_bots` and `required_checks`

**Files:** `autodev/config.py`, `autodev/activities.py` (`poll_issues`, `observe_pr`), `autodev.yaml.example`;
tests in `tests/test_autodev_activities.py`, `tests/test_autodev_config.py`.

- **Config.** `GitHubCfg.trusted_bots: list[str] = []` (logins ending in `[bot]`; the validator rejects
  anything else), `GitHubCfg.required_checks: list[str] = []`, and `AgentCfg.max_budget_usd: float | None`.
  When set, `job_script` passes `--max-budget-usd` to `claude -p` (spec §6.5, §12.3).
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
- `max_budget_usd: 5` puts `--max-budget-usd 5` in the job script; unset leaves it out

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

### Task 6: `io`: range files, dataset cache, byte cap, zip members, validators, host check

**Files:** `harness/src/mia_harness/{io,cache,remotezip}.py`, `harness/tests/test_io.py`,
`harness/tests/test_cache.py`, `harness/tests/rangeserver.py` (a thread running an `http.server` handler that
honours `Range`, serves ETag and Last-Modified, and can answer with a redirect; the stdlib handler ignores
`Range`).

**Interface:**
```python
class BudgetExceeded(RuntimeError): ...
class HostNotAllowed(RuntimeError): ...        # .host, .chain: the redirect hops so far
class NeedsFetch(Exception): ...

class DatasetCache:                            # §6.6: work_root/_cache/<record-id>/
    def __init__(self, shared: Path, private: Path, writable: bool): ...
    def get(self, key: str) -> Path | None     # shared first, then private
    def put(self, key: str, write: Callable[[Path], None]) -> Path   # tmp + rename; shared if writable, else private

class IO:
    def __init__(self, cache: DatasetCache, staging_dir: Path, byte_cap: int, allowed_hosts: set[str],
                 client: httpx.Client | None = None): ...
    def open(self, url: str) -> BinaryIO       # seekable; reads = range GETs (block-aligned, 1 MiB blocks, cached on disk)
    def fetch(self, url: str) -> Path          # whole file through the dataset cache; a hit costs 0 bytes
    def list(self, url: str) -> list[str]      # zip members only (via remotezip)
    bytes_fetched: int
    validators: dict[str, dict]                # url -> {"etag", "last_modified", "size"}

def from_env(work: Path) -> IO
# MIA_HARNESS_JOB_BYTES (required in managed jobs), MIA_HARNESS_ALLOWED_HOSTS (comma-separated exact hosts),
# MIA_HARNESS_CACHE_DIR, MIA_HARNESS_CACHE_WRITABLE=0|1
```
- **URL schemes.** Only `https`/`http` URLs, plus the `<zip url>::<member>` form. `s3://bucket/key` is
  rewritten to `https://bucket.s3.amazonaws.com/key` (public buckets only). `ftp://` raises `HostNotAllowed`.
- **Hosts and redirects (§5.2).** `io` follows redirects itself (`follow_redirects=False`, loop by hand) and
  checks every hop's host against `allowed_hosts` before requesting it. A hop outside the set raises
  `HostNotAllowed` with the chain so far, instead of a generic proxy error later. The CLIs report it as
  `host_not_allowed` in their JSON.
- **Byte cap.** Checked before each request (`bytes_fetched + request_size > cap` → `BudgetExceeded`) and
  counts response bytes. Cache hits count 0.
- **Dataset cache.** Key = sha256 of URL + validators. Changed validators miss and refetch. A zip member's
  CRC32 from the central directory is checked on every fetch and on every cache hit. Jobs that run reviewed
  code get the shared directory writable; the match agent and PR-head crop jobs get it read-only plus a
  private directory in their workspace (§6.6).
- **Zip members.** `remotezip`:
  1. reads the end-of-central-directory record from the last 64 KiB and the central directory, including
     ZIP64
  2. for a stored member (method 0), `open` returns a range file over its data offset
  3. for a deflated member (method 8), `open` raises `NeedsFetch`, and `fetch` downloads the compressed bytes,
     inflates them into the cache and verifies the CRC32

  A snippet calls `io.fetch` for deflated members, which is why `tiff-zip-member` reports `access: file`
  when the members are deflated.
- **Path safety.** Cache and staging paths never come from member names, so a member called `../x` can't
  escape.

**Tests first:**
- range reads return the right bytes and are counted
- a second read is served from the block cache, adding 0 bytes
- going over the cap raises `BudgetExceeded`
- validators are recorded
- a stored zip member is readable by range; a deflated member needs `fetch`, and the fetched bytes match
- a server that answers a range request with `200` and the whole body → `io` raises instead of silently
  downloading the whole file
- a ZIP64 fixture works
- member `../evil` lands on a safe path
- a redirect to a host outside `allowed_hosts` raises `HostNotAllowed` with the chain, and the server log
  shows no request to that host; `ftp://x` raises the same
- dataset cache: a second `fetch` with the same validators hits and costs 0 bytes; changed validators miss;
  a corrupted cached member fails the CRC32 and is refetched; with `writable=False` nothing is written to the
  shared directory

**Verify:** `pixi run test`.
**Commit:** `harness: io with range reads, dataset cache, byte cap, remote zip members and host checks`.

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

The result is `{"ok", "eligible", "shapes", "errors", "bytes_fetched"}`, plus `"host_not_allowed": {"host",
"chain"}` when `io` raised it. Exit 0 when the binding works (even if the dataset is ineligible), 1 when it
doesn't.

**Tests first:**
- smoke on `npy-local` with a 300×300 array → ok and eligible
- with a 200×200 array → ok and not eligible
- a mismatched label shape → error
- a withdrawn snippet is refused
- `spec_version` 2 is refused
- a redirect to a disallowed host → exit 1 with `host_not_allowed` in the JSON

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

def positions(v: Volume, size: int) -> int:            # planes * (Y-size+1) * (X-size+1): the sampling weight
def capacity(volumes: list[Volume], size: int) -> int  # Σ planes * ⌊Y/size⌋ * ⌊X/size⌋: non-overlapping tiles (§6.2)
def effective_n(n: int, cap: int) -> int               # min(n, cap // 2)
def candidates(volumes: list[Volume], seed: str, *, size=256, per_plane=4, per_file=8) -> Iterator[CropSpec]:
    """Endless, deterministic for a given seed (random.Random(seed)). Volume chosen ∝ positions; plane
    uniform over non-spatial indices; (y, x) uniform. Groups draws so that `plane` access yields up to
    per_plane candidates per plane, `file` access up to per_file per volume, before moving on."""
# The caller (Task 9) takes candidates until effective_n are accepted or 10 × effective_n have been drawn:
# one shared budget for content and overlap rejections.
def overlap_frac(a: dict, b: dict, size: int) -> float   # same raw, plane and channel only; else 0
```
**`png.py`** (§6.3, exactly):
```python
def stretch_raw(a: np.ndarray, p_lo: float = 0.1, p_hi: float = 99.9) -> tuple[np.ndarray, dict]:
    """Every source dtype -> uint16. lo/hi = percentiles of the finite values; (v - lo) / (hi - lo) clipped to
    0..1, × 65535. +inf -> 65535, -inf and NaN -> 0. mapping = {"kind": "stretch", "lo", "hi",
    "percentiles": [p_lo, p_hi], "nonfinite": {"nan", "posinf", "neginf"}, "source_dtype"}.
    Adapted from zarrcade's stretch_with_max_gain_bg_guard without the gain cap and the MAD black level;
    spec §6.3 says why. ValueError if hi <= lo (content_ok rejects those first)."""
def encode_label(a: np.ndarray, seg_type: str) -> tuple[np.ndarray, int, dict]:
    """binary -> uint8, nonzero -> 1; semantic -> uint16, ValueError if max > 65535 or min < 0; instance ->
    uint16 kept if within 0..65535, else renumbered 1..k ({"kind": "renumbered"})."""
def write_png(path: Path, a: np.ndarray, bitdepth: int) -> None    # pypng; greyscale, or RGB for overlays
def read_png(path: Path) -> tuple[np.ndarray, int]
def stats(a: np.ndarray) -> dict                                    # min, p1, p50, p99, max over finite source values
def content_ok(a: np.ndarray) -> bool                               # finite share ≥ 0.95, std above threshold, top value share ≤ 0.95
```

**Tests first:**
- the same seed gives the same candidates, and a different seed gives different ones
- volumes are weighted: a 10× larger volume gets about 10× the crops over 2,000 draws, within a tolerance
- `per_plane` grouping
- `capacity` of a 2×600×600 volume is 8, and `effective_n(16, 8)` is 4
- the shared redraw budget stops after 10 × n candidates, with rejections counted by kind
- an existing crop with 30% overlap is rejected, and one with 20% overlap passes
- `stretch_raw` on uint8, uint16, int16 and float32 patches: a deterministic uint16 result, `lo`/`hi`
  recorded, exactly the top and bottom 0.1% clipped, and one hot pixel doesn't darken the rest (the case the
  gain cap was for)
- `stretch_raw` with NaN and ±inf: counts recorded, NaN → 0, +inf → 65535
- a 0/255 binary mask encodes to 0/1; renumbering beyond 65535; a semantic value above 65535 raises
- every mapping round-trips through `write_png` / `read_png`
- `content_ok` is false for a constant array, for one that is 96% zeros, and for one that is 10% NaN

**Verify:** `pixi run test`. **Commit:** `harness: deterministic sampler, raw stretch and label encodings`.

### Task 9: `harness crop`, `harness verify`, `harness preview`

**Files:** `harness/src/mia_harness/{run,verify,preview,cli}.py`, `harness/tests/test_crop_verify.py`.

- **`harness crop <binding> --snippets S --record R --n N --seed SEED --round ROUND --batch B --out DIR [--existing MANIFEST]`:**
  - Opens the arrays and builds `Volume`s for the eligible raws, lowers `N` to `effective_n`, and reports
    both numbers and the capacity.
  - Drives `candidates(...)`. For each candidate it reads the raw patch (honouring `access`, so for a `plane`
    group it reads the plane once and slices it), then applies `content_ok` and the overlap check against this
    run and `--existing`.
  - Reads the paired labels at the same plane and box, then encodes and writes
    `DIR/<crop_id>/{raw.png, labels/<label_key>.png, crop.json}`. Here
    `crop_id = f"{ROUND}-b{B}-{i:03d}"`, with `ROUND` = `i<issue>-e<event id>` (§6.3), and `crop.json` has
    every field in §6.3, including the stretch mapping, plus `harness_commit` and `env_digest` from
    `MIA_HARNESS_COMMIT` and `MIA_HARNESS_ENV_DIGEST`.
  - Writes `DIR/report.json` with `{requested_n, n, capacity, made, rejected_content, rejected_overlap,
    bytes_fetched, source_shapes, errors}`.
  - Catches `BudgetExceeded` and `HostNotAllowed`, keeps what was made, and records the error
    (`host_not_allowed` carries the host and chain).
- **`harness verify DIR --binding B [--existing MANIFEST] --out verify.json`** loads **no snippets**. It runs
  the §6.4 checks using `crop.json`, the PNGs (every `raw.png` must decode as 256×256 16-bit), and the source
  shapes the crop job recorded in `report.json`. Its output is
  `{"crops": [{"crop_id", "ok", "errors", "files": {relpath: sha256}}], "ok_count", "n"}`.
- **`harness preview DIR --out PREVIEW`:** one `PREVIEW/<crop_id>/overlay.png` per crop: `raw.png` with
  every paired label's boundaries drawn in, one colour per label key, 8-bit RGB, numpy only (§6.7). No
  thumbnails and no `index.html`; `raw.png` is already viewable because of the stretch.

**Tests first (end to end on `npy-local` with a synthetic 2×600×600 volume and labels):**
- `crop --n 16` makes 4 crops (capacity 8, halved) and reports `requested_n: 16, n: 4`
- running it again with the same seed gives byte-identical PNGs
- `verify` passes them
- tampering with a PNG makes `verify` fail that crop
- a duplicate checksum under a different `crop_id` fails
- the same `crop_id` with the same checksum in `--existing` passes
- `preview` writes one `overlay.png` per crop, and pixels on a label boundary differ from `raw.png`
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
the mia-data app once Task 19 exists; require one approving review; dismiss stale approvals. PRs that touch
nothing under `snippets/` get their `mia-data/crops` success from the poller (Task 17, §5.5); until the
poller runs, merge maintainer PRs by review alone, before protection is switched on.

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
    `storage` (`root` only; there is no `public_url`), `crops` (with `licences`, default
    `[CC0-1.0, CC-BY-4.0, CC-BY-3.0, MIT]`), `limits` (`agent_max_budget_usd`, not a turn count), `agent`
    (autodev's `AgentCfg` shape), `harness.dir`, `network.package_hosts`, `network.redirect_hosts`
    (`dict[str, list[str]]`) and `gallery.branch` (default `galleries`).
  - Add `deployment: str = "mia-data"`, which names the csub session.
  - Validation: every path is resolved, and `state_dir` doesn't overlap `work_root` or `storage.root`. Limits
    are finite and positive. No GitHub host anywhere in `network.*` (reuse autodev's check). `redirect_hosts`
    values are exact hosts, no `*`. `crops.licences` is non-empty.
- **Labels.** Stage labels `data:<stage>` for `STAGES` = `{new, matching, developing, testing, pr-ready,
  review, cropping, done, needs-human, ineligible}`; human labels `data:go` and `data:approve`; plain labels
  `has-labels`, `maybe-small`, `licence-excluded`, `modality:<family>`. All with colours and descriptions.
- **`models.py`** (plain dataclasses, JSON-serialisable for Temporal):
  ```python
  @dataclass
  class LabelEvent: kind: str; event_id: str; actor: str; at: str      # kind: go | approve; at: ISO-8601 from GitHub
  @dataclass
  class PrUpdate: pr: int; head_sha: str; state: str; merge_sha: str | None; snippet_issue: int; needs_human: bool
  @dataclass
  class Pins: record: str; record_commit: str; snippets_commit: str; harness_commit: str; env_digest: str
  @dataclass
  class JobDone: job_id: str; state: str; exit_code: int | None
  @dataclass
  class Test: sha: str; test_id: str; ok: bool | None = None; finished_at: str | None = None; gallery_prefix: str | None = None; tree_sha: str | None = None
  @dataclass
  class DatasetState:
      issue: int
      schema: int = 1
      handled: list[str] = field(default_factory=list)    # go + approve event ids, newest last; keep ≤ 50
      round_id: str | None = None                          # i<issue>-e<event id>
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
- `package_hosts: [api.github.com]` is rejected
- `redirect_hosts: {a.example: ["*.b.example"]}` is rejected
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
  - `render_issue(rec, entry, licences) -> (title, body, labels)` follows §7.1. `body` starts with
    `<!-- mia-data:record=<id> -->`. `maybe-small` is set when no raw array in `technical.arrays` has
    Y and X ≥ 256 (axes may be missing; then use the last two dimensions). When `license.spdx` is not in
    `licences`, the labels are `data:ineligible` and `licence-excluded` instead of `data:new`, and the body
    says so.
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
- a GPL-3.0 record is created with `data:ineligible` and `licence-excluded`, not `data:new`

**Commit:** `mia_data: seed one issue per downloaded dataset`.

### Task 14: Binding checks, job hosts, status rendering, galleries, publication

**Files:** `mia_data/{binding,status,gallery,publish}.py`, `tests/test_mia_binding.py`,
`tests/test_mia_status.py`, `tests/test_mia_gallery.py`, `tests/test_mia_publish.py`.

- **`binding.py`** (worker side; no harness import, plain YAML and dicts):
  - `check_binding(b: dict, record: dict, catalog_names: set[str]) -> list[str]` checks:
    - `record` and `record_commit` match
    - every URL is `https` (an `s3://` URL is rewritten to its https form first); `http://` and `ftp://` fail
    - every URL's host appears in the record's URLs (`data.download_url`, `technical.sample.urls`,
      `technical.arrays[].path_pattern` hosts)
    - every `snippet` is in `catalog_names`
    - `label_key` matches `^[a-z_]+-[a-z0-9_]+-(instance|semantic|binary|points)$`, and `pairs_with` is in
      range
  - `hosts(urls: list[str], cfg) -> list[str]`: the URL hosts, plus `cfg.network.redirect_hosts[h]` for each,
    plus `cfg.network.package_hosts`; sorted exact hosts. Used for bindings and, in the match job, for the
    record's URLs.
  - `missing_hosts(hosts, patterns) -> list[str]`: `fnmatch` against `csub probe`'s `allowed_hosts`, the same
    rule as `csub/broker/resolve.py: check_hosts`.
- **`status.py`:**
  - `render_status(state, links) -> str` renders the in-place status comment, with marker
    `<!-- mia-data:status -->`, the stage, the binding in a fenced YAML block and its hash, the PR, the latest
    crops table and the totals.
  - `render_stage(stage, detail) -> str` renders the short per-transition comment.
  - `render_crops_table(rows) -> str`: a markdown table with columns raw, overlay, caption. Images are
    `![raw](<url>?raw=true)`; the caption is `crop_id · z=… c=… · (y, x)`. More than 4 rows go inside
    `<details>`.
  - Every piece of agent-derived text goes through `autodev.activities.neutralize`.
- **`gallery.py`:**
  ```python
  def gallery_files(crops_dir: Path, preview_dir: Path, verified: list[dict], binding: Path, verify_json: Path) -> dict[str, bytes]:
      """<crop_id>/raw.png and <crop_id>/overlay.png for verified crops only, plus binding.yaml and verify.json."""
  async def publish_gallery(gh_data, branch: str, prefix: str, files: dict[str, bytes]) -> dict[str, str]:
      """ensure_branch once; upsert_file for each prefix/<rel> (existing files are skipped); returns
      rel -> https://github.com/<repo>/blob/<branch>/<prefix>/<rel>?raw=true."""
  ```
  `prefix` is `galleries/<record-id>/<round-id>/<sha>` for a test and
  `galleries/<record-id>/<round-id>/production` for production (§7.4, §7.5). This is zarr-doctor's
  `publish_results` (`7abe195~1:zarr_doctor/activities.py`) with crops instead of histograms.
- **`publish.py`:**
  ```python
  def publish(dataset_dir: Path, src_dir: Path, verified: list[dict], withdrawn: dict[str, str], *, tmp_max_age_s: float) -> dict:
      """verified = [{"crop_id", "files": {rel: sha256}}]. First delete .tmp-* older than tmp_max_age_s
      (younger ones may belong to a live attempt). For each crop: skip if dest exists with the same hashes;
      raise Conflict if they differ; else copytree to .tmp-<crop_id>-<uuid4>, re-hash, os.rename into place
      (FileExistsError / ENOTEMPTY -> re-check hashes). Then rebuild manifest.json from */crop.json, marking
      withdrawn: true + reason for crops whose snippet is in `withdrawn` (tmp + os.replace).
      Returns {"published": [...], "already": [...], "total": n}."""
  def reindex(dataset_dir: Path, withdrawn: dict[str, str]) -> None     # manifest rebuild only, for the CLI
  ```
  The caller passes `tmp_max_age_s = 2 × the Activity's start-to-close timeout` (§7.6).

**Tests first:**
- `publish` twice → the second call publishes nothing and reports everything as "already"
- a conflicting existing crop raises
- a crash between copy and rename leaves a `.tmp-…` directory: a young one is left alone and the crop is
  still published from a fresh tmp; an old one (mtime set back) is deleted first
- two threads publishing the same crop set at once → one copy of each crop, no error
- the manifest lists exactly the crop directories, with `withdrawn: true` on crops whose snippet is withdrawn
- `check_binding`: a host not in the record, an unknown snippet, an `http://` URL and an `ftp://` URL are
  rejected; an `s3://` URL is accepted after rewriting
- `hosts` adds the redirect hosts and the package hosts, never a GitHub host; `missing_hosts` lets
  `*.huggingface.co` match `cdn-lfs.huggingface.co` and names a host no pattern matches
- `publish_gallery` with a fake client: `ensure_branch` once, one PUT per new file, no PUT for a file that
  exists, every URL ends in `?raw=true`
- `render_crops_table` with 4 rows is a plain table and with 16 rows is inside `<details>`
- the status comment contains no `<!--` from agent text

**Commit:** `mia_data: binding and host checks, status comments, galleries, idempotent publication`.

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
  `MIA_HARNESS_COMMIT`, `MIA_HARNESS_ENV_DIGEST`, `MIA_HARNESS_ALLOWED_HOSTS` (equal to the job's
  `allow_hosts`), `MIA_HARNESS_CACHE_DIR` and `MIA_HARNESS_CACHE_WRITABLE`, plus `ro_mounts` (`harness.dir`,
  the snapshot, extra inputs), and all write `exit_code`. The command prefix is
  `pixi run --manifest-path <harness.dir>/harness/pixi.toml harness …`.

  | builder | what it runs | `claude` | `allow_hosts` | dataset cache |
  |---|---|---|---|---|
  | `match_job(cfg, ws, pins, record_path, dl_dir)` | `claude -p "$(cat prompt.md)" --max-budget-usd B --allowedTools "Bash(harness smoke:*),Read,Write,Glob,Grep" --output-format json > claude.json` | true | `hosts(record urls)` | ro |
  | `smoke_job(cfg, ws, binding, snapshot)` | `harness smoke …` | false | `hosts(binding urls)` | rw |
  | `crop_job(cfg, ws, binding, snapshot, n, seed, round_id, batch, existing, trusted)` | `harness crop …` | false | `hosts(binding urls)` | rw if `trusted` (matched or merged SHA, production), else ro |
  | `verify_job(cfg, ws, crops_dir, binding, existing)` | `harness verify … && harness preview …`; crops mounted read-only | false | none | none |

  The Anthropic hosts are never requested: the policy's `[agents]` table adds them to `claude` jobs (P3).
  The dataset cache is `work_root/_cache/<record-id>/` (§6.6).
- **Pre-submit host check.** `check_hosts_allowed(spec, probe_patterns)` runs `missing_hosts`; a non-empty
  result raises a non-retryable `HostsNotAllowed(missing)`, which the workflow turns into `needs-human`
  naming the hosts and the `broker.toml` edit (§9). The probe patterns come from `csub probe`, fetched at
  worker start and again before each submit.
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
- `match_job` hosts are the record hosts plus their redirect hosts and the package hosts, never GitHub and
  never Anthropic; the command carries `--max-budget-usd`
- `crop_job(trusted=False)` mounts the cache read-only with `MIA_HARNESS_CACHE_WRITABLE=0`; `trusted=True`
  mounts it read-write
- `verify_job` has no hosts and mounts the crops read-only
- `check_hosts_allowed` names the missing host when the probe lacks it
- every builder sets `MIA_HARNESS_JOB_BYTES` from `limits.job_bytes` and `MIA_HARNESS_ALLOWED_HOSTS` equal to
  `allow_hosts`

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
| `probe_hosts() -> list[str]` | `csub probe` → the policy's `allowed_hosts` patterns |
| `submit(key, kind, args) -> str` | build the `JobSpec` for `kind` ∈ match/smoke/crop/verify, `assert_pin`, `check_hosts_allowed` against a fresh probe, `jobs.submit(key, spec)` |
| `await_job(job_id, ws) -> JobDone` | `jobs.wait` with heartbeat (runs on the `-await` queue) |
| `read_match(ws) -> MatchResult` | `_read_agent_file(ws, "outcome.json")` + `binding.yaml`; parse defensively; on `matched`/`ineligible`, `check_binding`; copy the binding to `state_dir/rounds/<round>/binding-<n>.yaml`, return path + hash |
| `read_smoke(ws) -> SmokeResult` / `read_verify(ws) -> VerifyResult` | parse the harness JSON outputs |
| `publish_gallery(issue, round_id, kind, crop_ws, verify_ws, verified) -> GalleryResult` | `gallery_files` for the verified crops; `publish_gallery` to `gallery.branch` under `galleries/<record>/<round>/<kind>` (`kind` = the tested SHA, or `production`); return the URLs and the rendered crops table. The test job's own output already sits under `storage.root/_tests/<record>/<round>/<sha>/` (§7.4) |
| `publish_crops(issue, round_id, crop_ws, verified) -> dict` | `publish.publish(...)` into the dataset folder with the snapshot's `WITHDRAWN.yaml` (Task 14) |
| `refuse_label(issue, label, reason)` | remove a human label and comment once (busy `go`, `approve` outside `review`, `go` on `licence-excluded`) |
| `open_snippet_issue(issue, round_id, draft) -> int` | find by marker `<!-- mia-data:issue=N round=R -->` in `mia-snippets`, else create with label `snippets:go` |
| `pr_state(pr) -> PrUpdate` | `get_pr` + labels on the snippets issue |
| `set_crop_status(sha, state, url)` | `gh_snippets.set_status(sha, "mia-data/crops", …)` |
| `tree_sha(sha, name) -> str` | `gh_snippets.dir_sha(f"snippets/{name}", sha)` |
| `poll()` | Task 17 |

- **Workspace layout.** `work_root/issue-<n>/<round_id>/<kind>-<attempt>`. Job keys are
  `mia-<n>-<round_id>-<kind>-<attempt>`, which keeps them unique per round, stage and attempt.
- **`round_id`** is `i<issue>-e<event_id>` (§8.1), used as the crop-id prefix. GitHub event IDs are unique,
  so there is no hashing.

**Tests first (fakes for GitHub and csub, as in `test_autodev_activities.py`):**
- `admit` is idempotent, writing `record.yaml` once
- `read_match` with a symlinked `outcome.json` → treated as missing
- `read_match` with a binding that names a host outside the record → `needs-human`, with the reason
- `submit` refuses when the harness pin has moved
- `open_snippet_issue` twice → one issue
- `submit` refuses when a binding host is missing from the probe, naming the host
- `publish_gallery` commits `raw.png` and `overlay.png` for verified crops only, skips files that exist, and
  returns one table row per crop with `?raw=true` URLs

**Commit:** `mia_data: activities`.

### Task 17: `MiaDataDataset` (match → test → review → approve → crop → publish) and `MiaDataPoll`

**Files:** `mia_data/workflows.py`, `tests/test_mia_workflows.py` (time-skipping environment with mocked
activities, as `tests/test_autodev_workflows.py` does).

**Workflow skeleton** (thin; all names are activity names):
```python
@workflow.defn(name="MiaDataDataset")
class MiaDataDataset:
    @workflow.init
    def __init__(self, s: DatasetState): self.s = s; self.go: list[LabelEvent] = []; self.approve: list[LabelEvent] = []

    @workflow.signal
    def go(self, e: LabelEvent) -> None: self.go.append(e)
    @workflow.signal
    def approve(self, e: LabelEvent) -> None: self.approve.append(e)
    @workflow.signal
    def pr_update(self, u: PrUpdate) -> None: self.s.pending.append(u)
    @workflow.query
    def status(self) -> DatasetState: return self.s

    @workflow.run
    async def run(self, s: DatasetState) -> None:
        while True:
            await workflow.wait_condition(lambda: bool(self.go))
            e = self.go.pop(0)
            if e.event_id in self.s.handled: continue                  # redelivery
            self.s.handled = (self.s.handled + [e.event_id])[-50:]
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
     2. `crop` (n = `test_per_round`, seed = `seed + "-t"`, batch 0, `trusted=True`)
     3. `verify`
     4. `publish_gallery`, then report on the issue with the crops table and the binding
     5. on pass → stage `review`. Wait for an `approve` whose event is not in `handled` and whose `at` is
        newer than the test's `finished_at`. Any other `approve` (older, or arriving in another stage) is
        recorded in `handled` and answered with `refuse_label`.
     6. on approve → stage `cropping`: `crop` (n = `per_round`, seed = `seed + "-p"`, batch 1, existing =
        manifest, `trusted=True`) → `verify` → `publish_crops` for every passing crop → `publish_gallery`
        (`production`) → `done` if at least half of the lowered `n` passed, else `needs-human`. Passing crops
        stay published either way (§7.5).
  4. On `ineligible` → `ineligible`. On `blocked`, or any failure → `needs-human` with the reason (autodev's
     `report_failure` style).
  5. On `needs_snippet` → Task 20 (until then → `needs-human` "development not wired yet").
- **Signals while busy.** When `_round` returns, every `go` still queued arrived while the round was running.
  Reject each one with `refuse_label` ("busy, ignored", marker `busy <event_id>`) and add its ID to
  `handled`, so it is never admitted later. A `go` that arrives after that, while the workflow waits, starts
  the next round. `approve` outside `review` is refused the same way.
- **Limits.** `attempts[kind] < limits.max_stage_attempts`, or the round goes to `needs-human`. The job count
  per round is fixed by this code path (§6.5).
- **Timeouts.** Await timeouts follow autodev: walltime + queue slack, heartbeat 3 minutes.

**`MiaDataPoll`** is a scheduled workflow that calls the `poll` activity. That activity:
1. For each open data issue labelled `data:go` or `data:approve`, gets `(actor, event_id, at) = label_event`.
   - If the actor lacks write access: remove the label and comment once, as autodev does.
   - A `go` on an issue labelled `licence-excluded`: `refuse_label` with the reason; never signalled.
   - Otherwise `signal_with_start(MiaDataDataset.run, DatasetState(n), id=f"mia-data/{repo}/issue-{n}",
     start_signal="go" | "approve", start_signal_args=[LabelEvent(...)])`, then remove the label.
2. For PRs in `mia-snippets` whose head branch is `snippets/issue-<K>`, reads the issue K body marker
   `mia-data:issue=N round=R` and signals `pr_update` to dataset N with `get_pr` state. This covers the first
   page of `list_prs(state="all")` (`ponytail:` first page only; paginate if more than 50 PRs are active).
3. For every open PR in `mia-snippets` whose `pr_files` touch nothing under `snippets/`, and whose head has
   no `mia-data/crops` status yet, sets `mia-data/crops` = success, "no snippet in this PR" (§5.5).

**Tests first:**
- `test_match_test_review_approve_crop_done`: the activities are called in order, the stage labels in order
  are `matching, testing, review, cropping, done`, `publish_crops` is called once, and no batch-1 `crop` is
  submitted before `approve`
- `test_approve_outside_review_is_refused`, and `test_stale_approve_is_refused` (event `at` older than the
  test's `finished_at`)
- `test_redelivered_go_is_ignored`: the same event_id signalled twice gives one round
- `test_go_while_busy_is_rejected_with_comment`
- `test_partial_production`: 5 of 16 pass → `publish_crops` publishes 5 and the stage is `needs-human`
- `test_poll_refuses_go_on_licence_excluded`, `test_poll_sets_status_for_non_snippet_pr`
- `test_ineligible`, `test_blocked_goes_needs_human`, `test_failed_job_needs_human_no_retry`
- `test_test_crop_failure_never_produces`: verify returns `ok_count=0` → `needs-human`, and `crop` is never
  called with batch 1
- `test_worker_death_mid_await_reattaches`: the await activity fails once, and `submit` is called once per key
- `test_continue_as_new_keeps_handled`: a forced rollover, then redelivering the old event → no new round
- `test_seed_is_recorded`: the seed in the workflow state equals the one passed to the crop jobs; replay is covered
  in Task 18

**Commit:** `mia_data: dataset workflow (match/test/review/crop) and poller`.

### Task 18: CLI and replay test

**Files:** `mia_data/cli.py`, `tests/test_mia_cli.py`, `tests/fixtures/mia/dataset_history.json`, a replay
test in `tests/test_mia_workflows.py`.

- **Commands** (mirror `autodev/cli.py`):
  - `check`: GitHub identity on all three repos, Temporal, the namespace, csub probe (and that every host in
    `network.*` plus the P3 data hosts matches a policy pattern), directories, harness pin
  - `init-labels` on the data repo
  - `seed [--dry-run]`
  - `worker`: a main queue `mia-data` with workflows and activities, and the `mia-data-await` queue with
    `await_job`
  - `schedule apply`
  - `test-pr` (Task 19)
  - `crops-by-snippet <name>`, which scans `storage.root/*/manifest.json` and prints the crops whose
    `crop.json` names that snippet
  - `reindex [dataset]`, which rebuilds `manifest.json` with the current `WITHDRAWN.yaml` marks (§5.6)
- **Replay test.** After Task 17's tests pass, record the history of `test_match_test_review_approve_crop_done`
  into the fixture and add `test_replay_dataset_history` using `temporalio.worker.Replayer`, as
  `test_replay_recorded_history` does for autodev.

**Verify:** `pixi run test`; `pixi run md check` against a real config.
**Commit:** `mia_data: CLI and replay test`.

### Task 19: `mia-data test-pr`, manual crop-test admission

**Files:** `mia_data/cli.py`, `mia_data/activities.py` (reuse), `tests/test_mia_cli.py`.

`mia-data test-pr <pr> --binding <file> --record <id>` does the following, without Temporal:
1. Resolves the PR head SHA and posts `mia-data/crops` as `pending`.
2. Snapshots that SHA, runs `check_binding`, then runs the crop job, then the verify job, through
   `CsubJobs`, waiting synchronously.
3. Commits the verified crops to the `galleries` branch (`publish_gallery`), posts the PR comment with the
   crops table, and sets the status to success or failure on that SHA with the comment URL as target.

This is how the human-authored snippet PRs from Task 11 get their required status (spec §12.4).

**Tests first:**
- with fakes, the sequence is: pending status → crop job → verify job → success status on the same SHA
- verify failing → failure status
- the head moving during the run → the status is still posted on the original SHA, and the output says the
  head moved

**Verify (real):** run on the `tiff-zip-member` PR. The PR shows `mia-data/crops` ✓, and the PR comment shows
4 bbbc034 crops with nucleus overlays inline. This is also the first check that `?raw=true` images render in
this private repo, as they did for zarr-doctor. Then merge Task 11's PRs.
**Commit:** `mia_data: test-pr command for crop checks on snippet PRs`.

### Task 20: Milestone 3, end to end on bbbc034

**No new code** unless the run exposes a bug. If it does, fix it test-first in the owning task's files.

- [ ] Fill in `mia_data.yaml` (from the example). Run `pixi run md init-labels`, then `pixi run md seed`.
  Check that 49 issues exist and that a second run creates none.
- [ ] Start everything:
  1. `pixi run ad up`, for the existing deployment and the Temporal server
  2. `scripts/mia-data-up.sh`
- [ ] Add `data:go` to the bbbc034 issue. Expect, within about 2 minutes, `matching`, then `testing`, then
  `review` with a comment showing the 4 test crops and overlays inline plus the binding. Add `data:approve`.
  Expect `cropping`, then `done`, with 16 crops in a `<details>` table. Check that each `crop.json` has the
  licence, validators, the stretch (`lo`, `hi`) and stats.
- [ ] Add `data:approve` while the issue is still in `testing`: expect an "ignored" comment and the label
  removed; the round must still stop at `review` afterwards.
- [ ] Add `data:go` to a `licence-excluded` issue: expect a refusal comment and no round.
- [ ] Redeliver the trigger: add `data:go` again while the issue is in `cropping`. Expect a "busy, ignored"
  comment and no second round.
- [ ] Restart the worker mid-round (`tmux kill-window` on the worker, then rerun the script). Expect the
  round to finish, no duplicate job IDs in `state_dir/jobs`, and the same crop set.
- [ ] Interrupt publication: kill the worker while `publish_crops` runs (add a temporary sleep under an env
  flag, or use a large n). After restart: the same crops, `manifest.json` complete, and no `.tmp` left.
- [ ] Run `pixi run md crops-by-snippet tiff-zip-member` and check that it lists the 16 crops. Add a fake
  entry to `WITHDRAWN.yaml` in the harness checkout, run `pixi run md reindex`, and check the manifest marks
  them `withdrawn: true`; then revert.
- [ ] Check the dataset cache: the production crop job reports 0 bytes fetched for the zip member that the
  smoke job already staged.
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
| developing/testing/pr-ready | open PR, head `H` not yet tested, and `len(tests) < max_pr_revisions` | stage `testing`; `set_crop_status(H, pending)`; read the PR's example binding (`get_file` at `H`), `check_binding`, store it; crop (`trusted=False`) → verify → `publish_gallery` → comment on the PR and the issue → `set_crop_status(H, success/failure, comment URL)`; record `Test(H, …, tree_sha=tree_sha(H, name))`; success → `pr-ready` |
| any | head `H` tested already | no-op |
| any | open PR, revisions exhausted | `needs-human` "revision limit" |
| pr-ready/testing | `merged`, `merge_sha M` | `t = tree_sha(M, name)`; if a passing `Test` has `tree_sha == t` → pin the binding to `M` (new binding file, `snippets_commit = M`), stage `cropping` (no `data:approve`: the PR review approved snippet and binding together); else test `M` as in §7.4 with `trusted=True` (no status posted; it's not a PR head) and only a pass goes to `cropping` |

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
  - `agent.allow_hosts`: the data hosts from P3 and their redirect hosts (never GitHub; the config rejects it)
  - `agent.max_budget_usd`, matching mia-data's `limits.agent_max_budget_usd`
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
  4. `pr-ready`, with the test crops shown on the PR and the issue
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
  | §6 harness, §6.6 cache, §6.7 overlays | 6–9, 15 |
  | §7.1 seed | 13 |
  | §7.2 match | 15–17 |
  | §7.3 develop | 21–22 |
  | §7.4 test crops and galleries | 14, 16, 17, 19, 21 |
  | §7.5 production (partial results) | 17 |
  | §7.6 publication | 14 |
  | §8 Temporal | 17, 18, 21 |
  | §9 trust and hosts | 1, 10, 14, 15, P3 |
  | §10 config | 12 |
  | §11 tests | throughout |
  | §12 changes to existing code | 1–4, 19, 22 |
- **Deliberately not built:** byte metering across a whole round, 3D/zarr, an agent in production, a
  `common/` package, pagination of PRs beyond the first page, thumbnails or a contact sheet (the stretched
  `raw.png` is the preview), gallery hosting off GitHub, dataset-cache eviction, automation of the csub
  policy edit.
- **Riskiest assumptions, tested earliest:**
  - P3: data hosts and their redirect targets through csub; a host the probe missed shows up as
    `host_not_allowed` in the first smoke job, and the fix is a policy edit
  - Task 19: `?raw=true` images render in the private `mia-workflow-data` repo (zarr-doctor's did; confirm on
    the first PR comment)
  - Task 6: range reads against the real hosts (some return 200 without honouring `Range`; `io` must detect
    that and refuse rather than download whole files silently: a test for it belongs in Task 6)
  - Task 22: autodev's agent can run the harness inside its sandbox
