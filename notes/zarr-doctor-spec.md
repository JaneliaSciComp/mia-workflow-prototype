# Zarr Doctor — Proof-of-Concept Spec

**Audience:** Claude Code, running on the Janelia LSF cluster (submit host), building this from scratch.
**Status:** v0.1 · 2026-10-05
**Owner:** MIAAI data engineering

## 0. One-paragraph summary

Build `zarr-doctor`, a small Python service that (1) walks the lmd data store and opens one GitHub issue per zarr volume, (2) watches those issues and, when a human moves one into the "Run" stage, (3) submits an LSF job via `py-cluster-api` that computes a value histogram of the volume and renders it as a PNG, and (4) posts the PNG and summary statistics back to the issue as a comment. Orchestration is Temporal (one durable workflow per issue). There is **no LLM** in this version. The point is to prove the event path (GitHub → cluster → GitHub), the durable per-issue workflow, and the LSF job lifecycle, using real data read-only.

## 1. Goals and non-goals

**Goals**
- Every volume under `$LMD_ROOT/data` becomes exactly one issue, idempotently, with its metadata and "legacy trait" labels.
- Moving an issue to the Run stage (Projects v2 Status field, or a label in fallback mode) triggers processing within ~3 minutes, with no inbound network from GitHub (polling only).
- Processing runs on LSF through `py-cluster-api`, survives restarts of the service, and reports success or failure back to the issue every time. No silent stalls.
- Results comment contains the histogram PNG, a stats table, and job metadata.
- Everything is re-runnable: moving an issue to Run again produces a new run and a new comment.

**Non-goals (explicitly out of scope)**
- No LLM, no agent framework, no MCP servers.
- No OpenShift deployment; everything runs on the submit host for this PoC.
- No writes anywhere under `$LMD_ROOT`. The store is read-only to this service.
- No session sandbox / untrusted code execution. The histogram job is fixed code.
- No warm pool, no ArgoCD, no external payload storage.

## 2. Context and hard constraints

- **Data layout** (from the MIAAI architecture doc): `$LMD_ROOT/data/{modality}-{organism}-{dataset}/crop-NNN_descriptor.zarr/` containing `raw/` (OME-NGFF multiscale: `s0`, `s1`, …) and `labels/{provenance}-{class}-{info}/` (also multiscale). Some volumes are zarr v2, some v3. Mixed dtypes and chunkings; the "canonical" target is zarr3, 128³ chunks, zstd, bf16/uint8 raw, int32 labels.
- **No inbound connectivity from GitHub.** Outbound HTTPS from the submit host to `api.github.com` works. Verify early with `curl -I https://api.github.com`.
- **Cluster:** IBM LSF. Submit from the submit host. Use `py-cluster-api` (`pip install py-cluster-api`, import `cluster_api`) for submit/monitor/track; read its `README.md`, `docs/API.md` and `CLAUDE.md` before writing cluster code: https://github.com/JaneliaSciComp/py-cluster-api
- **Orchestration:** Temporal. For the PoC run the dev server on the submit host (`temporal server start-dev --db-filename $STATE_DIR/temporal.db`); production will point `TEMPORAL_ADDRESS` at a server on OpenShift. Nothing in the code may assume the dev server.
- **GitHub auth:** a fine-grained PAT in `$GITHUB_TOKEN` for the PoC (issues: RW, contents: RW, projects: RW, metadata: R). Structure the client so swapping to a GitHub App installation token later is a one-function change.
- **Python 3.11+, pixi** for the environment (matches `py-cluster-api` and `micro-agent`). The histogram job must be runnable with the same pixi env on compute nodes (shared filesystem). Containerizing it is a follow-up, not part of this PoC.
- **Rate limits:** GitHub secondary limits on content creation are real. Issue/comment creation ≤ 1 request/second and ≤ 400/hour, with resumable discovery so a 995-volume backlog can be worked through across several runs.

## 3. Runtime architecture

Four long-lived things on the submit host, plus transient LSF jobs:

```
 Temporal dev server (:7233, UI :8233)
        ▲ gRPC (outbound from worker)
        │
 zarr-doctor worker ─── one process: workflows + activities
   ├─ PollWorkflow      every 2 min via a Temporal Schedule → poll_github activity
   ├─ IssueWorkflow     one per issue, id "zarr-doctor/issue-<n>"
   └─ activities        github_* (REST/GraphQL), cluster_* (py-cluster-api), publish_*
                                  │
                                  ▼  bsub / bjobs (via py-cluster-api LSF executor)
                           LSF job: python -m zarr_doctor.jobs.histogram
                           (compute node; reads $LMD_ROOT read-only; writes $RESULTS_DIR/<issue>/<run>/)
```

- The poller is **not** a separate daemon. It is a scheduled workflow whose single activity queries GitHub and calls `signal_with_start` on the relevant `IssueWorkflow`s. This keeps the deployable surface to one worker process plus Temporal.
- `zarr-doctor discover` is a CLI command (run manually or from cron), not a workflow, because it is a bounded batch job with its own resumable state.
- State the service owns: a SQLite file at `$STATE_DIR/zarr_doctor.sqlite` with tables `volumes(path PRIMARY KEY, issue_number, fingerprint, created_at)` and `poll_cursor(key, value)`. Temporal holds all per-run state.

## 4. GitHub conventions

**Repository:** configurable (`github.repo: AI-HHMI/zarr-doctor-poc` by default). Issues live here; results PNGs are committed to a branch of this same repo.

**Issue identity.** Every issue body ends with a machine marker on its own line:

```
<!-- zarr-doctor: path=data/em-mouse-foo/crop-001_desc.zarr fingerprint=sha1:... -->
```

`path` is relative to `$LMD_ROOT`. Discovery dedupes against the SQLite table first and, on first run or cache miss, against GitHub (search `in:body "zarr-doctor: path=<path>"`). Never create a second issue for a path.

**Stage trigger — two modes, selected by `trigger.mode`:**

| mode | "move to next stage" means | service writes back |
|---|---|---|
| `project` (default) | Projects v2 Status field = `Run` | Status → `Running` → `Done` / `Failed` |
| `label` | label `zd:run` added | swap to `zd:running` → `zd:done` / `zd:failed` |

In `project` mode discovery also adds each new issue to the project with Status `Discovered`. The project is identified by `trigger.project_number` (org-level project); the Status field and its options (`Discovered`, `Run`, `Running`, `Done`, `Failed`) must exist; `zarr-doctor check` verifies and prints the IDs it resolved.

**Labels** (created by `zarr-doctor init-labels` if missing):
- stage labels (label mode only): `zd:run`, `zd:running`, `zd:done`, `zd:failed`
- trait labels (both modes, set at discovery): `trait:zarr2`, `trait:raw-float32`, `trait:raw-float64`, `trait:labels-int64`, `trait:labels-uint64`, `trait:chunks-not-128`, `trait:no-zstd`, `trait:no-voxel-size`, `trait:no-labels`
- `modality:<x>`, `organism:<y>` parsed from the dataset directory name

**Results branch:** `zarr-doctor-results`, created from an orphan commit if absent. Files at `results/<issue>/<run_id>/histogram.png`, `stats.json`, `histogram.json`. Commits made via the Contents API (one PUT per file; base64 content).

## 5. Discovery (`zarr-doctor discover`)

Inputs: `$LMD_ROOT`, `--max-new N` (default 10), `--dry-run`, `--filter <glob>`.

For each `data/*/crop-*.zarr` directory (also accept any direct child of a dataset dir that is a zarr group containing `raw/`; log and skip anything that isn't):

1. Read `zarr.json` (v3) or `.zgroup`/`.zattrs` (v2) at the group, `raw/`, and `raw/s0`. Never open chunk data during discovery; this must run across ~1,000 volumes in minutes.
2. Extract: zarr format version, `raw/s0` shape, dtype, chunk shape, codecs/compressor name, voxel size and units from OME-NGFF `multiscales[0].datasets[0].coordinateTransformations` (or v2 equivalents in `.zattrs`), number of scale levels, list of label group names with their dtypes, and the five vocabulary attributes on each label group if present (`class`, `segmentation_type`, `provenance`, `proofreading_status`, `coverage`).
3. Compute a `fingerprint` = sha1 over the extracted metadata (not file mtimes), so reruns can detect changed volumes.
4. Derive trait labels (section 4) and modality/organism from the directory name `{modality}-{organism}-{dataset}`.
5. If the path is unknown: create the issue (title `[{modality}-{organism}-{dataset}] crop-NNN_descriptor` — fall back to the raw directory name if the pattern doesn't parse), body per Appendix A, labels, add to project (project mode), record in SQLite, and `signal_with_start` the `IssueWorkflow` with a `discovered` payload. If known and the fingerprint changed: post a short "metadata changed" comment and update the body's metadata block; do not create a new issue.
6. Stop after `--max-new` creations; print a summary (`found / existing / created / skipped / errors`). Exit non-zero only on an unexpected error, not on skips.

Throttle creations to ≤ 1/s. Respect `Retry-After` and `x-ratelimit-reset`; back off and resume rather than failing.

## 6. Polling (`PollWorkflow` on a Schedule)

- A Temporal Schedule `zarr-doctor-poll` runs `PollWorkflow` every 2 minutes, overlap policy `SKIP`, created/updated by `zarr-doctor schedule apply`.
- `PollWorkflow` executes one activity, `poll_github`, with `start_to_close_timeout=90s`, `retry maximum_attempts=1` (the next scheduled run is the retry).
- `poll_github` finds issues currently in the Run stage:
  - project mode: page through `organization.projectV2(number).items(first:100, after)` with `fieldValueByName(name:"Status")` and `content { ... on Issue { number } }`; collect items whose Status is `Run`.
  - label mode: `GET /repos/{repo}/issues?labels=zd:run&state=open&per_page=100` (paged).
- For each hit, `signal_with_start(IssueWorkflow, id="zarr-doctor/issue-<n>", signal="run", arg=RunRequest(trigger="project"|"label", requested_at=<now>))`. Duplicates are harmless: the workflow ignores a `run` while a run is in progress, and the workflow itself moves the issue out of the Run stage within seconds of starting, so steady state produces no hits.
- Also record `last_poll_at` in SQLite and expose it via `zarr-doctor status`.

Why no diffing: moving the stage *is* the ack. Diffing would require state that can drift; this design has none.

## 7. `IssueWorkflow`

Workflow id `zarr-doctor/issue-<n>`; task queue `zarr-doctor`. Inputs: `issue_number`, `path`. It is long-lived: it waits for `run` signals indefinitely (use `continue_as_new` after every 50 runs or when history exceeds ~5,000 events).

State machine (expose via a `status` query):

```
IDLE ──run──▶ ACKED ──▶ SUBMITTED ──▶ RUNNING ──▶ PUBLISHING ──▶ IDLE
                 │            │           │            │
                 └────────────┴───────────┴────────────┴──▶ FAILED-REPORTED ──▶ IDLE
```

Per run (all steps are activities unless noted; timeouts/retries given):

1. `ack_run(issue, run_id)` — move Status to `Running` / swap labels; post a short comment "Run `<run_id>` started; job pending on LSF." 30s, 5 retries.
2. `submit_histogram_job(issue, path, run_id)` → `job_id`. 60s, 3 retries. **Idempotent:** before submitting, call `executor.reconnect()` (requires `job_name_prefix` in the cluster config) and, if a job named `zd-<issue>-<run_id>` already exists, return its id instead of submitting again.
3. `await_job(job_id)` → `JobOutcome(status, exit_code, max_mem, runtime_s, log_path)`. `start_to_close_timeout = walltime + 15 min`, `heartbeat_timeout = 2 min`, retries 3. The activity seeds the executor with `executor.track(job_id, status=RUNNING)` (so a restarted worker resumes monitoring rather than losing the job), starts a `JobMonitor`, heartbeats every poll with the current LSF status, and returns when the job exits. Pending-in-queue time counts; if the queue is slow the activity just keeps heartbeating.
4. (workflow) if `JobOutcome.status != success` → go to step 7 with the log tail.
5. `publish_results(issue, run_id)` — read `$RESULTS_DIR/<issue>/<run_id>/{stats.json,histogram.png,histogram.json}`, commit them to the results branch, build the comment (Appendix B), post it. 2 min, 5 retries. Idempotent: if the files already exist on the branch at that path, skip the commit; if a comment containing `<!-- zarr-doctor: run=<run_id> result -->` already exists, skip posting.
6. `finish_run(issue, outcome=done)` — Status → `Done` / label `zd:done`. 30s, 5 retries. Back to IDLE.
7. On any failure: `report_failure(issue, run_id, error, log_tail)` — comment per Appendix C; Status → `Failed` / label `zd:failed`. This step must be wrapped so that it runs even if the workflow is being cancelled (`asyncio.shield`). Then back to IDLE; another `run` signal starts a fresh run.

`cancel` signal: if a job is running, `cluster_cancel(job_id)`; then report as failed with reason "cancelled". `discovered` signal (from discovery) just records `path`/metadata in workflow state; no action.

Keep workflow code free of I/O and of imports from `zarr`, `cluster_api`, `requests`/`httpx`; those live in activities. Use dataclasses or Pydantic models for all activity inputs/outputs so they serialize cleanly.

## 8. The cluster job (`zarr_doctor.jobs.histogram`)

A standalone script with **no Temporal or GitHub dependencies** (only `zarr>=3`, `numpy`, `matplotlib`, `tensorstore` optional). Must be independently runnable: `python -m zarr_doctor.jobs.histogram --zarr <abs path to crop.zarr> --out <dir> [--level s0] [--max-chunks 256] [--seed 0]`.

Submission (via `py-cluster-api`): `ResourceSpec(cpus=4, memory="16 GB", walltime="01:00", queue=<config>)`, name `zd-<issue>-<run_id>`, `inherit_env=False`, `login_shell=True`, `script_prologue` that `cd`s to the project and activates the pixi env, `env={"ZD_ISSUE": ..., "ZD_RUN_ID": ...}`. Stdout/stderr to `$RESULTS_DIR/<issue>/<run_id>/lsf.{out,err}`.

Algorithm:
1. Open `raw/<level>` read-only (zarr-python 3 auto-detects v2/v3; fall back to tensorstore if zarr can't open it, and record which reader was used).
2. Enumerate the chunk grid. If total chunks ≤ `max_chunks`, read all; otherwise pick `max_chunks` chunks evenly spaced in flattened chunk index order (deterministic given `--seed`), and record `sampled_fraction`.
3. Accumulate a histogram in a dtype-appropriate way:
   - `uint8`: 256 exact bins.
   - other integers: exact min/max over the sample, then ≤ 1024 bins over `[min, max]` (bin width an integer ≥ 1).
   - floats (incl. bf16 read as float32): 512 bins between the 0.1st and 99.9th percentile of the sample; count outliers separately.
   - Count exact zeros separately (fill-value / empty-chunk indicator). Skip chunks that are entirely fill value quickly if the reader exposes that; otherwise just count.
4. Write `stats.json` (shape, dtype, chunks, level, reader, total_chunks, chunks_read, sampled_fraction, voxels_counted, min, max, mean, std, p01, p50, p99, zero_fraction, elapsed_s), `histogram.json` (bin edges + counts), and `histogram.png` (1400×700, two panels: linear and log-y; title = relative zarr path + level; subtitle = dtype, sampled fraction, zero fraction).
5. Exit 0 on success; non-zero with a one-line error on stderr otherwise. Never write outside `--out`.

Memory: process one chunk at a time; never materialize the volume. A 256-chunk sample of 128³ uint8 chunks is 512 MB of reads, which is the design point.

## 9. Publishing

- Commit `histogram.png`, `stats.json`, `histogram.json` to `zarr-doctor-results` at `results/<issue>/<run_id>/`.
- Embed the PNG in the comment with the blob URL `https://github.com/{owner}/{repo}/blob/zarr-doctor-results/results/<issue>/<run_id>/histogram.png?raw=true`. GitHub renders this for viewers with repository access. Verify on the first real run; if it does not render in this repo (can happen for private repos depending on org settings), fall back to the plain link plus a 40-column ASCII histogram in the comment, and note the finding in the README.
- Always include the stats table in the comment so it is useful without the image.

## 10. Configuration

`zarr_doctor.yaml` (path via `--config` or `$ZARR_DOCTOR_CONFIG`; example committed as `zarr_doctor.yaml.example`):

```yaml
lmd_root: /nrs/.../lmd-v0.0.1          # read-only
results_dir: /nrs/.../zarr-doctor/results
state_dir: ~/.local/state/zarr-doctor
github:
  repo: AI-HHMI/zarr-doctor-poc
  token_env: GITHUB_TOKEN
  results_branch: zarr-doctor-results
trigger:
  mode: project                         # project | label
  project_number: 7                     # org project number (project mode)
  status_field: Status
  statuses: {discovered: Discovered, run: Run, running: Running, done: Done, failed: Failed}
temporal:
  address: localhost:7233
  namespace: default
  task_queue: zarr-doctor
  poll_every_minutes: 2
cluster:
  config_path: ./cluster_api.yaml       # py-cluster-api config
  profile: janelia_lsf
  job_name_prefix: zd
  resources: {cpus: 4, memory: "16 GB", walltime: "01:00", queue: local}
histogram:
  level: s0
  max_chunks: 256
discovery:
  max_new_default: 10
  creates_per_second: 1
```

Secrets only via environment variables. Fail fast at startup with a clear message if any required value is missing.

## 11. CLI (`zarr-doctor …`)

| command | purpose |
|---|---|
| `check` | verify token scopes, repo access, project/field/option IDs, Temporal connectivity, `bsub` on PATH, `$LMD_ROOT` readable, `$RESULTS_DIR` writable. Prints a table; non-zero if anything fails. |
| `init-labels` | create missing labels (idempotent). |
| `discover [--max-new N] [--dry-run] [--filter GLOB]` | section 5. |
| `worker` | run the Temporal worker (workflows + activities). |
| `schedule apply` / `schedule delete` | manage the poll Schedule. |
| `poll-once` | run the poll activity logic once, locally, print what it would signal. |
| `run --issue N` | send a `run` signal manually (bypasses the stage). |
| `status [--issue N]` | per-issue workflow state via query; overall counts; last poll time. |
| `histogram PATH --out DIR` | run the job script locally (no LSF). |
| `janitor` | list LSF jobs with prefix `zd-` whose workflow is not running, and `--kill` them; delete result dirs for issues closed > 30 days. |

## 12. Repository layout

```
zarr-doctor/
  pyproject.toml            # console script: zarr-doctor = zarr_doctor.cli:main
  pixi.toml / pixi.lock
  README.md                 # how to run the four processes; findings (e.g. image rendering)
  zarr_doctor.yaml.example
  cluster_api.yaml          # py-cluster-api profiles: janelia_lsf + local
  zarr_doctor/
    __init__.py
    config.py               # pydantic-settings model, loads YAML + env
    github_api.py           # thin httpx client: REST + GraphQL, retries, rate-limit handling,
                            # idempotent helpers (find_issue_by_marker, upsert_file, comment_once)
    projects.py             # Projects v2 lookups and updateProjectV2ItemFieldValue
    discovery.py            # walk, metadata extraction, traits, fingerprint
    state.py                # SQLite access
    models.py               # dataclasses/pydantic models shared by workflows & activities
    workflows.py            # IssueWorkflow, PollWorkflow (no I/O imports)
    activities.py           # github_*, publish_*, poll_github, report_failure
    cluster.py              # py-cluster-api wrapper: submit/track/await/cancel, reconnect-by-name
    jobs/
      __init__.py
      histogram.py          # compute-node script, dependency-light
    cli.py
    scripts/run-all.sh      # tmux: temporal dev server, worker, schedule apply
  tests/
    conftest.py             # builds tiny zarr v2 + v3 fixtures (uint8, uint16, float32)
    test_discovery.py
    test_histogram.py       # runs the job script on fixtures; asserts stats + PNG exist
    test_workflows.py       # Temporal test env, activities mocked, happy path + failure path
    test_replay.py          # replay a recorded history after a trivial code change
    test_github_api.py      # respx-mocked REST/GraphQL, rate-limit backoff, idempotency
    test_cluster_local.py   # py-cluster-api local executor end to end (submit → await)
```

## 13. Testing requirements

- Unit tests must not touch GitHub, LSF, Temporal, or `$LMD_ROOT`. Use `respx` for HTTP, `py-cluster-api`'s `local` executor, and Temporal's `WorkflowEnvironment.start_time_skipping()`.
- `test_workflows.py` covers: run → done; job failure → failure comment; worker restart between `submit` and `await` (simulate by failing the activity attempt after submit and asserting no second submission); duplicate `run` signal ignored while running; `cancel` during RUNNING.
- `test_replay.py`: record a history from the test env, change a log line in the workflow, replay, assert no nondeterminism error.
- An `integration/` marker for tests that need the real cluster or GitHub; skipped by default, run manually.

## 14. Milestones and acceptance criteria

Build in this order; each milestone is demoable on its own.

**M1 — Discover (dry run).** `zarr-doctor discover --dry-run` lists all volumes with metadata and traits in under 5 minutes for the full store; zero writes anywhere.
**M2 — Issues.** `discover --max-new 10` creates 10 issues with correct bodies/labels, added to the project in `Discovered`; a second run creates 0; `check` passes.
**M3 — Durable wait.** Worker + schedule running. Moving an issue to `Run` yields the "started" comment within 3 minutes and Status `Running`; a `run` signal via CLI does the same. Killing and restarting the worker while an issue is RUNNING resumes without re-submitting.
**M4 — Cluster + results.** Full path: Run → LSF job → PNG + stats comment → `Done`. Works for a zarr v2 and a zarr v3 volume, for uint8 and non-uint8 dtypes. A deliberately broken path (rename in config) produces a `Failed` status and a comment with the log tail.
**M5 — Hardening.** `janitor` finds and kills an orphan; Temporal restart mid-run recovers; GitHub 403 secondary-rate-limit is handled with backoff (test with respx); label mode works end to end when `trigger.mode: label`.

Acceptance (all must hold): no writes under `$LMD_ROOT` (verify with `inotifywait`/`strace -e trace=open` once, or run as a user without write permission); every run ends in exactly one result or failure comment; restart of any component never loses a run or duplicates an LSF job; the full backlog can be discovered with repeated `discover` runs without creating duplicates.

## 15. Operational notes for the PoC

- Run the four processes under `tmux` via `scripts/run-all.sh` (dev server, worker, schedule apply; discovery on demand). Document how to re-attach and how to tail logs (`$STATE_DIR/logs/`).
- Temporal UI at `http://<submit-host>:8233` is the debugging surface; link to it from the README.
- Worker concurrency: cap activities at 8 and limit concurrent `await_job` activities to 20, since each holds an executor poller.
- Log every GitHub write and every `bsub` with issue number, run id, and job id; never log the token.

## 16. Decisions already made (do not relitigate without asking)

- Temporal stays even though the workflow is trivial; validating it is the point.
- Poll, don't webhook.
- One repo for issues and result files; results on a dedicated branch, not in `main`.
- Histogram over a chunk sample, not the full volume; `s0` by default.
- The compute job knows nothing about GitHub or Temporal.
- `project` trigger mode is the default; `label` mode is a supported fallback, not a second-class citizen.

Open items to raise early if they bite: whether `?raw=true` images render in the target repo; which LSF queue to use for a 1-hour, 4-CPU job; whether the pixi env is available on compute nodes at the same path.

---

## Appendix A — Issue body template

```
### {dataset} / {crop}

| field | value |
|---|---|
| path | `data/{dataset}/{crop}` |
| zarr format | v{2|3} |
| raw/s0 shape | 2048 × 2048 × 2048 (z, y, x) |
| raw dtype | uint8 |
| raw chunks | 64 × 64 × 64 |
| codecs | blosc(zstd, 5) |
| voxel size | 8 × 8 × 8 nm |
| scale levels | 5 |
| labels | `manual_gt-cell-dense` (uint64), `auto_pred-mito-v2` (int32) |
| label vocab complete | 1 of 2 |

**Traits:** zarr2, labels-uint64, chunks-not-128

_Discovered {iso timestamp} by zarr-doctor {version}._

<!-- zarr-doctor: path=data/{dataset}/{crop} fingerprint=sha1:… -->
```

## Appendix B — Result comment template

```
### Zarr Doctor — value histogram (`raw/s0`)

Sampled **256 of 32,768 chunks (0.8%)** · 536,870,912 voxels · zarr v2 read with zarr-python

| min | p1 | median | mean | std | p99 | max | zeros |
|---|---|---|---|---|---|---|---|
| 0 | 12 | 127 | 126.4 | 41.2 | 240 | 255 | 3.1% |

![histogram](https://github.com/{owner}/{repo}/blob/zarr-doctor-results/results/{issue}/{run_id}/histogram.png?raw=true)

<details><summary>Job details</summary>

LSF job `123456` · 4m 12s · peak memory 3.2 GB · exit 0
Files: [`stats.json`](…) · [`histogram.json`](…) · log: `{results_dir}/{issue}/{run_id}/lsf.out`
</details>

<!-- zarr-doctor: run={run_id} result -->
```

## Appendix C — Failure comment template

```
### Zarr Doctor — run `{run_id}` failed

**Stage:** await_job · **LSF job:** `123456` · **exit:** 137 · **runtime:** 58m 02s

```
<last 40 lines of lsf.err>
```

Move the issue back to **Run** to retry. Status set to **Failed**.

<!-- zarr-doctor: run={run_id} failed -->
```

## Appendix D — Projects v2 GraphQL you will need

- Resolve project: `organization(login:$org){ projectV2(number:$n){ id fields(first:50){ nodes{ ... on ProjectV2SingleSelectField { id name options{ id name } } } } } }`
- List items with status: `node(id:$projectId){ ... on ProjectV2 { items(first:100, after:$cursor){ pageInfo{hasNextPage endCursor} nodes{ id fieldValueByName(name:"Status"){ ... on ProjectV2ItemFieldSingleSelectValue { name } } content{ ... on Issue { number } } } } } }`
- Add issue: `addProjectV2ItemById(input:{projectId:$p, contentId:$issueNodeId}){ item{ id } }`
- Set status: `updateProjectV2ItemFieldValue(input:{projectId:$p, itemId:$i, fieldId:$f, value:{singleSelectOptionId:$o}}){ projectV2Item{ id } }`

Cache project/field/option IDs in SQLite after `check`; they do not change.
