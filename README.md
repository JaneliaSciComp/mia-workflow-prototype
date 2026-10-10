# zarr-doctor (PoC)

One GitHub issue per zarr volume in the LMD store. Moving an issue to **Run** submits an LSF job that
histograms the volume, and the PNG and stats are posted back to the issue. Orchestration uses Temporal,
with one durable workflow per issue. There's no LLM. Spec: [`notes/zarr-doctor-spec.md`](notes/zarr-doctor-spec.md). Code walkthrough: [`notes/temporal-guide.md`](notes/temporal-guide.md).

## Setup

```bash
pixi install
cp zarr_doctor.yaml.example zarr_doctor.yaml   # edit paths / repo / trigger mode
echo 'GITHUB_TOKEN=github_pat_...' >> .env      # read by the service; never committed
pixi run zarr-doctor check                      # every row must say ok
pixi run zarr-doctor init-labels
```

**Token.** Use a fine-grained PAT scoped to the one repo with **Issues: RW**, **Contents: RW** and
**Metadata: R**. Project mode also needs Projects access: **Organization → Projects: RW** for an org project.
Fine-grained PATs can't reach Projects v2 boards owned by a *user* account, so for a user-owned
repo use `trigger.mode: label`, or switch to a classic PAT with `repo` + `project` scopes. Token lookup lives in
`github_api.get_token()`; replace that one function to use a GitHub App installation token.

## Running

```bash
scripts/run-all.sh          # tmux session "zarr-doctor": Temporal dev server + worker, then `schedule apply`
tmux attach -t zarr-doctor  # re-attach; logs are in $STATE_DIR/logs/{worker,cli,temporal}.log
pixi run zarr-doctor discover --max-new 10     # create issues (resumable; rerun to work through the backlog)
```

The Temporal UI is at `http://<submit-host>:8233`. It's the main debugging surface: workflow
`zarr-doctor/issue-<n>`, schedule `zarr-doctor-poll`.

| command | |
|---|---|
| `check` | token/repo access, project field + option IDs (cached in SQLite), Temporal, `bsub`, LMD_ROOT readable, RESULTS_DIR writable |
| `init-labels` | stage, trait, modality and organism labels (idempotent) |
| `discover [--max-new N] [--dry-run] [--filter GLOB]` | dry run lists all 1,071 volumes in ~25 s and writes nothing |
| `worker` | workflows + activities (queue `zarr-doctor`, max 8) and `await_job` (queue `zarr-doctor-await`, max 20) |
| `schedule apply\|delete` | poll every `poll_every_minutes`, overlap SKIP |
| `poll-once` | print which issues the poller would signal |
| `run --issue N` / `cancel --issue N` | manual signals |
| `status [--issue N]` | workflow state(s) via query, last poll time |
| `histogram PATH --out DIR` | run the job locally, no LSF, no config needed |
| `janitor [--kill] [--days 30]` | orphan `zd-*` LSF jobs and result dirs of issues closed > N days; `--kill` acts on both |

Stage flow. In label mode: `zd:run` → `zd:running` → `zd:done` / `zd:failed`. In project mode: Status `Run` →
`Running` → `Done` / `Failed`. Either way, put the issue back in Run to rerun it.

## Tests

```bash
pixi run test                                   # unit tests: no GitHub, LSF, Temporal server or LMD access
pixi run pytest -m integration -s               # real bsub against two real volumes (run on an LSF submit host)
```

## Findings and deviations from the spec

- **Dedupe.** Each `discover` run lists the repo's issues once (paged) and reconciles the marker comments into
  SQLite. That replaces the per-path `in:body` search: the search index lags new issues, so search-based dedupe could
  create duplicates right after a creation burst, and it's rate-limited to 30 requests/min.
- **Two task queues.** "Cap activities at 8" and "allow 20 concurrent `await_job`" can't both hold on one worker,
  so `await_job` runs on `<task_queue>-await` inside the same worker process.
- **Idempotent submit.** The job id is written to `$RESULTS_DIR/<issue>/<run>/job_id`. A retry returns it. If the
  worker died between `bsub` returning and that write, `reconnect()` finds the live job by name (`zd-<issue>-<run>`).
- **Restart resume.** Verified locally. The worker was killed while RUNNING and restarted: one submission, and the run
  completed and published. On LSF, `track()` + `bjobs` resumes polling. The local executor can't re-poll a job
  from a previous process, so its profile sets `zombie_timeout_minutes: 1`. After that, a job whose `stats.json`
  and `histogram.png` exist counts as done.
- **Store specifics.** Most v3 volumes are sharded (512³ shards of 128³ chunks). "chunks" and the
  `chunks-not-128` trait use the *inner* chunk shape. Label groups carry `label_class` rather than `class`, and either
  one counts toward vocab completeness.
- **Result comment.** Besides the embedded `?raw=true` image, the result comment always includes a 40-column ASCII
  histogram inside `<details>`, so it stays useful if the image doesn't render.
- Non-uint8 percentiles are interpolated from the histogram, so they're accurate to one bin width.
- **Concurrent publishes.** Contents API commits to one branch race each other, and GitHub returns 409 "is at X but
  expected Y". Temporal's retry recovered every time, but commits are now serialized inside the worker.
- **Real-cluster runs (2026-10-05).** The four runs triggered by `zd:run` (v3 uint8 ×2, v2 uint16, v3 float32) each
  started within one poll and finished in about 20 s in the `short` queue. Killing and restarting the worker
  mid-run caused no resubmission. A renamed `LMD_ROOT` produced `zd:failed` with the job's one-line error.

## Open items

- [ ] Confirm `?raw=true` images render in the target repo (private-repo behaviour)
- [x] LSF queue: none by default; `-W 01:00` auto-routes to `short` on the Janelia cluster
- [ ] LSF billing project (`cluster.project`, passed as `bsub -P`); required for scicompsoft members
- [ ] The job activates the env with `pixi shell-hook` from this checkout. The checkout must be on a filesystem compute nodes can see, and `pixi` must be on the login-shell PATH there

## autodev

An agentic issue → PR → approval → merge loop built on the same Temporal stack: see
[`notes/autodev-runbook.md`](notes/autodev-runbook.md).
