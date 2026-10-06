# zarr-doctor (PoC)

One GitHub issue per zarr volume in the LMD store. Moving an issue to **Run** submits an LSF job that
histograms the volume, and the PNG and stats are posted back to the issue. There's no LLM.
Spec: [`notes/zarr-doctor-spec.md`](notes/zarr-doctor-spec.md).

> **This is the `prefect` branch:** the same service with orchestration on **Prefect 3** instead of Temporal
> (`main`). Each run request is one flow run tagged `issue:<n>`. For the differences, see
> [`notes/temporal-vs-prefect.md`](notes/temporal-vs-prefect.md).

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
scripts/run-all.sh                  # tmux session "zarr-doctor-prefect": Prefect server + worker, then `setup`
tmux attach -t zarr-doctor-prefect  # re-attach; logs are in $STATE_DIR/logs/{worker,cli,prefect-server}.log
pixi run zarr-doctor discover --max-new 10     # create issues (resumable; rerun to work through the backlog)
```

The Prefect UI is at `http://<host>:4200`. It's the main debugging surface: flow runs `issue-<n>` of
`histogram-run` (filter by tag `issue:<n>`), the scheduled `poll-github` deployment, and Automations.

| command | |
|---|---|
| `check` | token/repo access, project field + option IDs (cached in SQLite), Prefect API health, `bsub`, LMD_ROOT readable, RESULTS_DIR writable |
| `init-labels` | stage, trait, modality and organism labels (idempotent) |
| `discover [--max-new N] [--dry-run] [--filter GLOB]` | dry run lists all 1,071 volumes in ~25 s and writes nothing |
| `worker` | serves `histogram-run` and `poll-github` (every `poll_every_minutes`, collisions cancelled); at most `prefect.concurrency` runs, each a subprocess |
| `setup` | creates the automation that marks flow runs with no heartbeat for 90 s as Crashed (idempotent) |
| `poll-once` | print which issues the poller would start runs for |
| `run --issue N` / `cancel --issue N` | start a run (refused if one is active) / cancel the active run (kills the LSF job) |
| `status [--issue N]` | flow-run states and current task, last poll time |
| `histogram PATH --out DIR` | run the job locally, no LSF, no config needed |
| `janitor [--kill] [--days 30]` | orphan `zd-*` LSF jobs and result dirs of issues closed > N days; `--kill` acts on both |

Stage flow. In label mode: `zd:run` → `zd:running` → `zd:done` / `zd:failed`. In project mode: Status `Run` →
`Running` → `Done` / `Failed`. Either way, put the issue back in Run to rerun it.

## Tests

```bash
pixi run test                                   # unit tests: no GitHub, LSF or LMD access; a throwaway Prefect server
pixi run pytest -m integration -s               # real bsub against two real volumes (run on an LSF submit host)
```

## Findings and deviations from the spec

- **Dedupe.** Each `discover` run lists the repo's issues once (paged) and reconciles the marker comments into
  SQLite. That replaces the per-path `in:body` search: the search index lags new issues, so search-based dedupe could
  create duplicates right after a creation burst, and it's rate-limited to 30 requests/min.
- **Concurrency.** `prefect.concurrency` (default 20) caps concurrent flow runs on the serving process. Each run is
  its own Python subprocess (about 106 MB resident each before doing any work), whereas Temporal runs the whole
  service in one worker (128 MB idle).
- **Idempotent submit.** The job id is written to `$RESULTS_DIR/<issue>/<run>/job_id`. A retry returns it. If the
  worker died between `bsub` returning and that write, `reconnect()` finds the live job by name (`zd-<issue>-<run>`).
- **Crash recovery (Prefect).** Flow runs emit heartbeats every 30 s. The `setup` automation marks a run Crashed
  after 90 s without one. The next poll reschedules a crashed run, keeping the same flow-run id and therefore the same
  run id and LSF job, up to 3 attempts, and then reports failure. Verified locally on 2026-10-06: SIGKILL of the
  worker and its flow-run processes while the job ran led to Crashed after 90 s; after restart the run was
  rescheduled, reattached to the finished job without resubmitting, and published (`zd:done`). On LSF,
  `track()` + `bjobs` resumes polling. The local executor can't re-poll a job from a previous process, so its
  profile sets `zombie_timeout_minutes: 1`; after that, a job whose `stats.json` and `histogram.png` exist counts
  as done.
- **Prefect setup gotchas.**
  - conda-forge's Prefect 3.8 needs `greenlet` for its server.
  - Its scheduler fails on SQLAlchemy 2.1 ("Can't evaluate bulk DML statement"), so no scheduled runs were ever
    created. SQLAlchemy is pinned below 2.1.
  - Deployments use module-path entrypoints (`zarr_doctor.flows:…`), because file-path entrypoints can't resolve
    the package's relative imports.
  - The server's SQLite database must be on **local disk**. On NFS (`/groups`) it failed within minutes with
    `database is locked`, then a full connection pool, then HTTP 500s. `run-all.sh` puts it in
    `/scratch/$USER/zarr-doctor/prefect` (override with `PREFECT_HOME`). `/scratch` is cleaned periodically, so
    production should use Postgres. Temporal's dev-server SQLite on the same NFS mount ran fine for a day.
  - `prefect server start` can stop at an interactive "profile URL mismatch" prompt, and the UI tells browsers to
    use `127.0.0.1` for the API. `run-all.sh` sets `PREFECT_CLI_PROMPT=false` and
    `PREFECT_UI_API_URL=http://<fqdn>:4200/api`.
- **Store specifics.** Most v3 volumes are sharded (512³ shards of 128³ chunks). "chunks" and the
  `chunks-not-128` trait use the *inner* chunk shape. Label groups carry `label_class` rather than `class`, and either
  one counts toward vocab completeness.
- **Result comment.** Besides the embedded `?raw=true` image, the result comment always includes a 40-column ASCII
  histogram inside `<details>`, so it stays useful if the image doesn't render.
- Non-uint8 percentiles are interpolated from the histogram, so they're accurate to one bin width.
- **Concurrent publishes.** Contents API commits to one branch race each other, and GitHub returns 409 "is at X but
  expected Y". Retries recovered every time, but commits are now serialized inside each process.
- **Real-cluster runs (2026-10-05, Temporal version on `main`; the cluster and GitHub code is shared).** The four runs triggered by `zd:run` (v3 uint8 ×2, v2 uint16, v3 float32) each
  started within one poll and finished in about 20 s in the `short` queue. Killing and restarting the worker
  mid-run caused no resubmission. A renamed `LMD_ROOT` produced `zd:failed` with the job's one-line error.

## Open items

- [ ] Confirm `?raw=true` images render in the target repo (private-repo behaviour)
- [x] LSF queue: none by default; `-W 01:00` auto-routes to `short` on the Janelia cluster
- [ ] LSF billing project (`cluster.project`, passed as `bsub -P`); required for scicompsoft members
- [ ] The job activates the env with `pixi shell-hook` from this checkout. The checkout must be on a filesystem compute nodes can see, and `pixi` must be on the login-shell PATH there
