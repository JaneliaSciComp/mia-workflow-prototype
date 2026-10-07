# A guided tour of zarr-doctor on Prefect

This is the `prefect` branch's counterpart to the Temporal guide on `main`
([`notes/temporal-guide.md`](https://github.com/krokicki/mia-workflow-prototype/blob/main/notes/temporal-guide.md)).
It follows one histogram run through the code in the order things happen, then maps the Prefect concepts that
appear along the way to where they live. For the design comparison between the two orchestrators, read
[`temporal-vs-prefect.md`](temporal-vs-prefect.md).

> Code links are **permalinks to commit `439411c`**, the `prefect` branch at the time of writing, so their line
> ranges stay accurate. To see the latest version, press `y`/`Esc` on GitHub or swap the SHA for `prefect` in the URL.

---

## 0. The system in one picture

```
 GitHub issue  ──(label zd:run)──┐
                                 │  every 2 min (deployment schedule)
 Prefect server ───────────────▶ poll_github flow run ──run_deployment──▶ histogram_run flow run "issue-<n>"
 (API + UI + DB; schedules,       (one subprocess)                         (one subprocess, tagged issue:<n>)
  automations; never runs                                                   │
  our code)                                                                 ▼
                                        ack_run → submit_histogram_job → await_job → publish_results → finish_run
                                        (GitHub)        (bsub)          (bjobs loop)    (commit PNG,      (label
                                                                                          comment)          zd:done)
                                                           │
                                                           ▼
                                               LSF job: python -m zarr_doctor.jobs.histogram
                                               (knows nothing about Prefect or GitHub)
```

One process, `zarr-doctor worker`, is a Prefect **runner**: it registers the two deployments with the server, polls
for flow runs that are due, and spawns **one Python subprocess per flow run**. All our code runs in those
subprocesses, except the cancellation hook, which runs in the runner itself.

There is no long-lived object per issue. A run request becomes a flow run tagged `issue:<n>`, and the server's
flow-run list, filtered by that tag, *is* the issue's history.

---

## 1. One run, start to finish

Each link points at the function where a step *starts*. Helpers defined next to it in the same file (such as
`find_run_hits`, `start_run`, `render_result`) are named but not linked.

### 1.0 Before anything runs

[`scripts/run-all.sh`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/scripts/run-all.sh)
starts `prefect server start` (API and UI on port 4200, SQLite under `/scratch`), waits for its health endpoint,
runs `zarr-doctor setup`, then starts `zarr-doctor worker`, all under tmux.

- **Setup.** [`cmd_setup`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L190-L215)
  creates one **Automation** on the server: for each flow run that has emitted a heartbeat, if 90 s pass with no
  further heartbeat and no terminal state, set the run to **Crashed**. It's looked up by name, so rerunning `setup`
  is a no-op. This is the only crash detection Prefect has for a dead host; see §1.5.
- **The worker.** [`cmd_worker`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L167-L187)
  turns the two flows into two **deployments** named `zarr-doctor`: `histogram-run/zarr-doctor` with no schedule
  (runs are created on demand), and `poll-github/zarr-doctor` with an interval schedule of `poll_every_minutes`,
  a concurrency limit of 1 and `CANCEL_NEW` so two polls never overlap. `aserve(...)` registers both with the server
  and loops forever, spawning a subprocess for each flow run that comes due, at most `prefect.concurrency` at once,
  each emitting a heartbeat every 30 s.
- **Per-process clients.** Every flow run is a fresh Python process, so the GitHub, SQLite and LSF clients are built
  lazily, once per process, by
  [`deps()`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L51-L61)
  on first use. It calls
  [`load_config`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/config.py#L112-L131),
  which also exports `PREFECT_API_URL` so the Prefect client in that process finds the server. Tests swap the
  whole bundle with `set_deps()`.
- **The issues.** `zarr-doctor discover` is a plain CLI.
  [`cmd_discover`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L145-L164)
  calls [`discover`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/discovery.py#L207-L258),
  which walks `$LMD_ROOT/data`, reads each volume's zarr JSON (never chunk data), and creates one GitHub issue per
  new volume with a hidden `<!-- zarr-doctor: path=… fingerprint=… -->` marker in the body. Unlike `main`, nothing
  else happens: there's no per-issue workflow to create. The issue and its marker are the whole record.

### 1.1 You move an issue to Run

Add the `zd:run` label (label mode) or set the project Status to **Run** (project mode). Nothing in our code reacts
to this directly, because GitHub never calls us. The next poll notices.

### 1.2 The poll, every 2 minutes

1. The server's scheduler creates a `poll-github` flow run in the **Scheduled** state. The runner sees it is due,
   spawns a subprocess, which imports `zarr_doctor.flows` and calls `poll_github`.
2. [`poll_github`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L305-L322)
   calls `find_run_hits`, which asks GitHub which issues are in the Run stage: in project mode through
   [`projects.issues_with_status`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/projects.py#L54-L66),
   in label mode by listing open issues with `zd:run`. For each hit, `path_for_issue` looks up the volume path,
   first in SQLite
   ([`State.path_for_issue`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/state.py#L45-L47)),
   else from the marker in the issue body.
3. For each hit, `start_run` is the **duplicate-run guard**: `runs_for_issue` asks the server for `histogram-run`
   flow runs tagged `issue:<n>` in an active state (Scheduled, Pending, Running, Cancelling, Paused). If there is
   one, nothing happens. Otherwise `run_deployment("histogram-run/zarr-doctor", parameters={issue, path, trigger},
   tags=["issue:<n>"], timeout=0)` asks the server to create a flow run named `issue-<n>` and returns at once
   without waiting for it.
4. `recover_crashed` looks for Crashed runs to reschedule (§1.5).
5. The poll writes `last_poll_at` to SQLite and returns the list of issues it started.

`zarr-doctor poll-once`
([`cmd_poll_once`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L218-L236))
does the lookup and the duplicate check and prints what the poll would do. `zarr-doctor run --issue N`
([`cmd_run`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L239-L254))
skips the poll and calls `start_run` directly.

### 1.3 The run flow starts

The runner sees the Scheduled `issue-<n>` run, spawns a subprocess, and that process calls
[`histogram_run(issue, path, trigger)`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L226-L247).

Its first line sets `run_id = run_id_for(flow_run.id)`, which is `p-` plus the first eight hex digits of the flow-run
id. That is deterministic *per flow run*, so if this same flow run is executed again after a crash it gets the same
run id, therefore the same results directory and the same LSF job. Everything below depends on that.

### 1.4 The steps

`histogram_run` is plain Python. Each step is a `@task` that it `await`s directly; the task runs in the same
subprocess, and Prefect retries it in place on failure, with the `retries` and `retry_delay_seconds` on the
decorator. There are no determinism rules, so the flow can read config at runtime.

1. **Ack.** [`ack_run`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L158-L163)
   moves the issue to the running stage
   ([`projects.set_stage`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/projects.py#L79-L92):
   swap `zd:*` labels, or set the project field) and posts a "Run … started" comment through
   [`GitHub.comment_once`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/github_api.py#L183-L189),
   which first checks for a comment carrying this run's marker so a retry or rerun can't post twice.
   *On GitHub:* `zd:running`, one "started" comment.

2. **Submit.** [`submit_histogram_job`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L166-L168)
   calls [`Cluster.submit`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cluster.py#L56-L88),
   which has three layers of "did I already do this?": if `$RESULTS_DIR/<issue>/<run>/job_id` exists, return it;
   else look for a live LSF job named `zd-<issue>-<run>`; else `bsub` (via `py-cluster-api`) a job that cd's into
   this checkout, activates the pixi env and runs `python -m zarr_doctor.jobs.histogram`. It then writes the
   `job_id` file and returns the job id.

3. **Wait.** The flow calls `await_job.with_options(timeout_seconds=(walltime + 15) * 60)`, deriving the task
   timeout from the configured LSF walltime at runtime.
   [`await_job`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L171-L176)
   calls [`Cluster.wait`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cluster.py#L90-L112):
   a `JobMonitor` polls `bjobs`, and every poll writes a debug line through `get_run_logger()`, which ships it to
   the server so it shows in the UI. If this process didn't submit the job (a rerun), `executor.track(job_id)`
   adopts it first. It returns a `JobOutcome` (status, exit code, runtime, peak memory).
   *Meanwhile, on a compute node:*
   [`jobs/histogram.py`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/jobs/histogram.py#L185-L224)
   opens `raw/<level>`, samples up to 256 chunks deterministically, computes stats and a histogram, and writes
   `stats.json`, `histogram.json` and `histogram.png` into the run directory. It imports nothing from Prefect or
   GitHub.

4. **Check.** If `outcome.status` isn't `done`, the flow raises `RunFailed` and goes to §1.5.

5. **Publish.** [`publish_results`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L179-L191)
   creates the results branch if missing
   ([`ensure_branch`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/github_api.py#L193-L203)),
   commits the three files
   ([`upsert_file`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/github_api.py#L208-L215),
   skipping any that already exist), then `comment_once` posts the result comment rendered by `render_result`.
   *On GitHub:* commits on `zarr-doctor-results`, the histogram comment.

6. **Finish.** [`finish_run`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L194-L197)
   calls `set_stage(…, "done")`.
   *On GitHub:* `zd:done`.

7. **Done.** The flow returns `run_id`, the flow run goes to **Completed**, and the subprocess exits. Nothing is
   left running for this issue. Moving it back to Run later creates a brand-new flow run.

Every task is idempotent, so a task retry, or a whole-flow rerun after a crash, repeats the step without
duplicating its effect: comments are guarded by markers, files by existence, the LSF job by the `job_id` file and
the job name. Prefect itself remembers nothing between attempts here (no result persistence or caching is
configured), so this idempotency is what makes reruns correct.

### 1.5 When it goes wrong

**A step fails.** When a task exhausts its retries, or the flow raises `RunFailed`, the `except` in `histogram_run`
runs the [`report_failure`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L200-L202)
task. Through `_report_failure` it grabs the last 40 lines of `lsf.err`
([`Cluster.log_tail`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cluster.py#L117-L123)),
posts a failure comment with the stage, error and log tail (again through `comment_once`), and calls
`set_stage(…, "failed")`. The flow then re-raises, so the flow run ends **Failed** in the UI with the traceback in
its logs.

**Cancel.** `zarr-doctor cancel --issue N`
([`cmd_cancel`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L257-L270))
finds the active flow runs tagged `issue:<n>` and sets each to **Cancelling**. The runner notices and **kills the
subprocess**, so no code inside `histogram_run` gets to react. Then the runner runs
[`on_cancelled`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L211-L223),
registered on the flow as an `on_cancellation` hook, *in the runner process*: it reads the `job_id` file from the
run directory, calls [`Cluster.cancel`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cluster.py#L114-L115)
(`bkill`), and reports the failure the same way as above, with stage `cancelled`. The flow run ends **Cancelled**.

**The process or host dies.** The subprocess is gone, so its heartbeats stop. After 90 s the automation from
`setup` sets the flow run to **Crashed**. Prefect does nothing further on its own. On the next poll,
[`recover_crashed`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L283-L302)
finds Crashed `histogram-run` runs that don't carry the `zd:reported` tag. If the run has been attempted fewer
than three times and the issue has no other active run, it force-sets the run back to **Scheduled**. It is the same
flow run with the same id, so when the runner picks it up again `histogram_run` starts from the top with the same
`run_id`: `ack_run` finds the "started" marker and posts nothing, `Cluster.submit` finds the `job_id` file and
returns it, `Cluster.wait` adopts the job via `track()`, and the run carries on to publish. After three attempts,
`recover_crashed` reports the failure itself and tags the run `zd:reported` so it isn't reported again.

### 1.6 Watching it happen

- The Prefect UI (`:4200`) → **Flow Runs**, filter by tag `issue:<n>`: each `issue-<n>` run with its state, its
  parameters, its task runs (`ack_run`, `submit_histogram_job`, `await_job`, …) and its logs. **Deployments** →
  `poll-github` lists every poll tick. **Automations** shows the crash detector.
- `zarr-doctor status [--issue N]`
  ([`cmd_status`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L282-L304))
  reads flow-run states from the server and shows the most recent task run as the run's current stage. No worker
  needs to be up to answer.
- `$STATE_DIR/logs/worker.log` has the runner's output only. Log lines written inside a flow run go to the server
  (through `get_run_logger()`), not to this file. `$RESULTS_DIR/<issue>/<run>/lsf.{out,err}` has the job's.

---

## 2. Prefect concepts → code

| concept | what it is | where |
|---|---|---|
| **Flow** (`@flow`) | the unit of orchestration; one flow run per invocation, with a state machine (Scheduled → Pending → Running → Completed / Failed / Crashed / Cancelled) tracked by the server | [`histogram_run`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L226-L247), [`poll_github`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L305-L322) |
| `flow_run_name`, tags, parameters | how a flow run is found again later; tags are our per-issue key | `flow_run_name="issue-{issue}"` on the flow; `tags=[issue_tag(n)]` in `start_run` |
| **Task** (`@task`) with `retries`, `retry_delay_seconds`, `timeout_seconds` | a step inside a flow run, retried in-process | [flows.py L158–L202](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L158-L202) |
| `task.with_options(...)` | override task settings at call time | `await_job.with_options(timeout_seconds=…)` in `histogram_run` |
| `get_run_logger()` | per-run logger whose output is stored on the server and shown in the UI | `await_job`, `poll_github` |
| **Deployment** | a flow plus how and when to run it (schedule, concurrency, entrypoint) | [`cmd_worker`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L167-L187): `ato_deployment(...)` |
| Interval schedule, concurrency limit, `CANCEL_NEW` | the server creates the poll runs; overlapping ticks are cancelled | same, on the `poll-github` deployment |
| `serve` / runner | the process that polls for due flow runs and spawns one subprocess per run; `limit` caps concurrency, `heartbeat_seconds` makes runs emit liveness events | same: `aserve(...)` |
| `run_deployment(...)` | create a flow run of a deployment from code | `start_run` in flows.py |
| Client reads: `read_flow_runs` with `FlowRunFilter` on tags and state | querying the server for "what runs exist for this issue?" | `runs_for_issue` in flows.py, [`cmd_status`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L282-L304) |
| `set_flow_run_state(..., Cancelling())` / `Scheduled()` | drive a run's state from outside: cancel, or reschedule a crashed run | [`cmd_cancel`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L257-L270), `recover_crashed` in flows.py |
| **State hooks** (`on_cancellation`) | code the runner executes when a run reaches a state; no task retries, plain code | [`on_cancelled`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/flows.py#L211-L223) |
| **Automation** with a proactive `EventTrigger` | server-side rule: "if event X isn't followed by event Y within T, do Z" | [`cmd_setup`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/zarr_doctor/cli.py#L190-L215) |
| Module-path entrypoints | flow-run subprocesses import `zarr_doctor.flows:…` as a package module so relative imports resolve | `EntrypointType.MODULE_PATH` in `cmd_worker` |
| `prefect_test_harness()` | a throwaway real server for tests; fakes injected with `set_deps()`, delays zeroed with `with_options(retry_delay_seconds=0)` | [`tests/test_flows.py`](https://github.com/krokicki/mia-workflow-prototype/blob/439411c94db0f260f57806b156fb14997cb6380f/tests/test_flows.py) |

What Prefect does **not** give us, and where we wrote it ourselves: uniqueness per issue (`start_run`'s
check-then-act guard), rescheduling after a crash (`recover_crashed`), and dead-host detection (the `setup`
automation). On `main`, Temporal provides all three structurally; see
[`temporal-vs-prefect.md`](temporal-vs-prefect.md) §1.3.
