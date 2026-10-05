# A guided tour of zarr-doctor, and of Temporal

This guide is for an experienced engineer who hasn't used Temporal. It has three parts:

1. **Temporal concepts**, explained in detail and mapped to where each one shows up in this code base.
2. **A code tour**: links in a teaching order. Each link says what the code is, what to look for, and what it depends on.
3. **Hands-on labs**: things to run against the live system and the test suite so you see each concept happen.

> All code links are **permalinks to commit `74aed15`**, so their line ranges stay accurate as `main` moves.
> To see the latest version, press `y`/`Esc` on GitHub or swap the SHA for `main` in the URL.

---

## 0. The system in one picture

```
 GitHub issue  ──(label zd:run)──┐
                                 │  every 2 min
 Temporal server ── Schedule ──▶ PollWorkflow ──▶ poll_github activity ──signal "run"──▶ IssueWorkflow (one per issue)
 (history + queues;                                                                       │
  never runs our code)                                                                    ▼
                                         ack_run → submit_histogram_job → await_job → publish_results → finish_run
                                         (GitHub)        (bsub)          (bjobs loop,     (commit PNG,      (label
                                                                          heartbeats)      comment)          zd:done)
                                                            │
                                                            ▼
                                                LSF job: python -m zarr_doctor.jobs.histogram
                                                (knows nothing about Temporal or GitHub)
```

One Python process, the **worker** (`zarr-doctor worker`), runs every box above except the Temporal server and
the LSF job.

---

## 1. Temporal concepts

### 1.1 The server stores state; your workers run all the code

The **Temporal server** is a database plus a set of queues. It stores each workflow's **event history**, holds
**task queues**, and fires **timers** and **Schedules**. It never runs your code.

All code, workflow logic and activities alike, runs in **workers**: processes you start that connect to the server
over gRPC and **long-poll** task queues. With no worker polling, nothing is lost but nothing advances. Tasks wait in
the queue until a worker appears. This explains a lot of behaviour you'll see below. For example, `zarr-doctor
status` hangs (we added a timeout) when no worker is up, because answering a query requires running workflow code.

### 1.2 Workflows: durable functions built on event sourcing

A **workflow** is an ordinary `async` function, `IssueWorkflow.execute`, that can run for months and survive
crashes of any process. It's identified by:

- **Workflow type**: the class or function name (`IssueWorkflow`).
- **Workflow ID**: a business key you choose (`zarr-doctor/issue-9`). The server allows at most one *running*
  execution per ID, which gives you deduplication for free.
- **Run ID**: a UUID per execution. Continue-as-new (below) keeps the Workflow ID and gets a new Run ID.

**How durability works.** Every time the workflow reaches a decision (start activity X, start timer Y, complete),
the worker sends **commands** to the server. The server records the outcomes as **events** in the history:
`ActivityTaskScheduled`, `ActivityTaskCompleted`, `WorkflowExecutionSignaled`, and so on. When a workflow needs to
continue on a different worker, perhaps after a crash, that worker **replays** the history through the workflow
function from the top. Every `await execute_activity(...)` whose result is already in the history returns that
recorded result immediately instead of running the activity again. When replay reaches the end of the history, the
function's local variables, loop counters and `self.*` fields are exactly as they were. That's why
`IssueWorkflow` can keep its state in plain instance attributes (`self.runs`, `self.state`) with no database.

**Workflow task vs. activity task.** When something happens to a workflow (started, signalled, an activity
finished, a timer fired), the server puts a **workflow task** on the queue. A worker takes it, runs or replays the
workflow code until every coroutine is blocked again, and replies with new commands. Workflow tasks take
milliseconds. **Activity tasks** are separate: they're how the real work gets done.

**Sticky execution.** Workers cache running workflows in memory. A worker that already holds a workflow just
applies the new events, and a full replay only happens on a different worker or after the cache evicts it. Replay is
still what makes recovery correct, so your code must always be replay-safe.

### 1.3 Determinism is the one hard rule

Replay only works if workflow code makes **the same decisions in the same order** every time it runs against the
same history. So workflow code must not:

- do I/O (network, files, databases); that's what activities are for;
- read the wall clock or random numbers directly. Use `workflow.now()`, `workflow.random()` and `workflow.uuid4()`,
  which are recorded and replayed;
- use threads or real `asyncio.sleep`. Use `workflow.sleep()` or `workflow.wait_condition()`, which become
  durable server-side timers;
- branch on anything that differs between first execution and replay, such as env vars or config files.

The Python SDK enforces much of this with a **sandbox**: workflow modules are re-imported in isolation and known
nondeterministic calls are blocked. `workflow.unsafe.imports_passed_through()` tells the sandbox that some imports
(our plain dataclasses) are safe to share instead of re-importing. If you break the rule, the worker raises a
**nondeterminism error** (`TMPRL1100`). Lab 5 makes you trigger one on purpose.

### 1.4 Activities: where side effects happen

An **activity** is a normal function, with no rules, that does the real work: HTTP calls, `bsub`, file I/O. The
workflow schedules it with `workflow.execute_activity(...)`, and the result is recorded in history.

- **At-least-once.** An activity can run more than once: a worker may die after doing the work but before
  reporting success, and then a retry runs it again. So **activities must be idempotent**. Most of the care in
  `activities.py`, `cluster.py` and `github_api.py` goes into this. Examples: comments carry a hidden marker and are
  only posted if absent; the LSF job id is written to a file before success is reported.
- **Timeouts**:
  - `start_to_close_timeout`: maximum time for one attempt.
  - `schedule_to_close_timeout`: maximum across all attempts.
  - `schedule_to_start_timeout`: maximum time waiting in the queue.
  - `heartbeat_timeout`: maximum time between heartbeats.

  At least one of the first two is required.
- **Retry policy.** By default an activity retries forever with exponential backoff (1 s, ×2, capped). We set
  `maximum_attempts` everywhere. When retries run out, the workflow gets an `ActivityError`.
- **Heartbeats.** A long activity calls `activity.heartbeat(details)` periodically. This does two jobs. First,
  **liveness**: if heartbeats stop for `heartbeat_timeout`, the server treats the attempt as failed and retries it,
  possibly on another worker, which is how a crashed worker is detected quickly. Second, **cancellation delivery**:
  an activity only learns it was cancelled through a heartbeat response. `await_job` heartbeats on every LSF poll.
  The SDK throttles how often heartbeats are actually sent.

### 1.5 Task queues and workers

A **task queue** is a named queue on the server. Workflows and activities are scheduled onto queues by name, and
workers poll the queues they serve. Queues are how you route work. We use two:

- `zarr-doctor`: all workflow tasks and the short activities (GitHub calls, `bsub`), with up to 8 concurrent
  activities.
- `zarr-doctor-await`: only `await_job`, which can hold a slot for an hour while an LSF job runs, so it gets its
  own pool of 20.

Both are served by one process here. On another cluster you could run the `-await` worker somewhere else without
changing workflow code. The same mechanism can later put "cluster-only" activities on workers that run as LSF jobs.

### 1.6 Messages: signals, queries (and updates)

- **Signal**: an asynchronous, durable message *into* a running workflow, recorded as `WorkflowExecutionSignaled`.
  The handler mutates workflow state. Ours are `run`, `cancel` and `discovered`.
- **Query**: a synchronous, **read-only** request answered by running a handler on a worker against the
  workflow's current state. It writes nothing to history. Ours is `status`.
- **Signal-with-start**: "send this signal to workflow ID X, starting it first if it isn't running", as a single
  atomic operation. The poller and discovery use this, so they never need to know whether an issue's workflow
  exists. In Python it's `client.start_workflow(..., start_signal=...)`.
- **Update**: a newer request/response message, not used here.

### 1.7 Waiting, timers and cancellation

- `await workflow.wait_condition(lambda: ...)` blocks until the predicate becomes true. It's re-checked after
  every workflow task (for example, after a signal). That's how `IssueWorkflow` sleeps for weeks between runs at zero
  cost: no worker thread is held, it's just a workflow sitting in history.
- **Activity cancellation**: `handle.cancel()` asks for it, and the activity sees it at its next heartbeat.
- **Workflow cancellation**: `temporal workflow cancel` raises `asyncio.CancelledError` inside the workflow at its
  next `await`. You can catch it to clean up and then re-raise. `asyncio.shield` keeps the cleanup itself from being
  cancelled.

### 1.8 Long-lived workflows: continue-as-new

Histories have hard limits (about 50k events or 50 MB) and replay cost grows with length. A workflow that lives
forever periodically calls **`continue_as_new(args)`**: the current run closes and a new run starts with the same
Workflow ID, fresh history and whatever state you pass forward. `workflow.info().is_continue_as_new_suggested()`
tells you when the server thinks it's time.

### 1.9 Schedules

A **Schedule** is a server-side cron that starts a workflow. It has an overlap policy (we use `SKIP`: don't start a
new poll while the previous one is still running) and a catch-up window. The Schedule fires even with no worker up,
but the workflow it starts still needs a worker to make progress. Lab 2 shows the consequence.

### 1.10 Changing code safely: versioning and replay testing

You can freely change activity code. Workflow code that changes the **sequence of commands** breaks every
in-flight execution on its next replay. Two tools help:

- **`workflow.patched("id")`** returns True on first execution and records a marker. During replay it returns
  whatever was recorded, so old histories take the old path and new runs take the new one. (Lab 5)
- **Replay testing**: replay recorded histories against new code before you deploy.
  [`scripts/replay_history.py`](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/scripts/replay_history.py)
  does this for a history exported from the live server.

### 1.11 Testing

`WorkflowEnvironment.start_time_skipping()` starts a local test server whose clock can **jump forward**. While the
test awaits a workflow result (`handle.result()`) or calls `env.sleep()`, and nothing else is running, the server
fast-forwards to the next timer, so a workflow that sleeps for a week finishes instantly. Our tests mostly poll the
`status` query instead, so their short retry backoffs (1 s, 2 s) run in real time. That's fine at these sizes.
Activities are plain functions, so tests register **fake activities with the same names** and drive the real
workflow code against a real server.

### 1.12 Concept → code map

| concept | where |
|---|---|
| workflow definition, `@workflow.init`, `@workflow.run` | `workflows.py` L37–L88 |
| signals / query | `workflows.py` L54–L73 |
| durable wait | `workflows.py` L80, L115 |
| activity options (timeouts, retries, routing) | `workflows.py` L90–L93, L110–L114 |
| cancellation + `asyncio.shield` | `workflows.py` L126–L156 |
| continue-as-new | `workflows.py` L83–L88 |
| activity definitions, heartbeat | `activities.py` L122–L192 |
| signal-with-start | `activities.py` L22–L31 |
| workers and task queues | `cli.py` L184–L211 |
| Schedule | `cli.py` L213–L236 |
| time-skipping tests | `tests/test_workflows.py` |
| replay / versioning | `tests/test_replay.py`, `scripts/replay_history.py` |

---

## 2. Code tour

Read in this order. Each step builds on the previous one.

### Stage A: orientation

1. **[notes/zarr-doctor-spec.md](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/notes/zarr-doctor-spec.md)**
   *What:* the original spec this PoC implements.
   *Look for:* §3 (runtime architecture), §7 (the `IssueWorkflow` state machine and per-step timeouts and retries),
   §16 (decisions already made).
   *Depends on:* nothing.

2. **[README.md](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/README.md)**
   *What:* how to run it, plus "Findings and deviations".
   *Look for:* the deviations section. Each entry is a spot where real data or real infrastructure disagreed with the
   spec.
   *Depends on:* nothing.

3. **[pyproject.toml](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/pyproject.toml)**
   *What:* package metadata, dependencies and the pixi environment, all in one file.
   *Look for:* `temporalio` (the Python SDK), `py-cluster-api` (LSF wrapper), `httpx`, `zarr`. Also the pytest config
   (`asyncio_mode = "auto"`, a session-scoped event loop so one Temporal test server is shared, and the
   `integration` marker that's skipped by default).
   *Depends on:* pixi (conda-forge + PyPI).

### Stage B: the data contract

4. **[zarr_doctor/models.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/models.py)**
   *What:* every value that crosses the workflow↔activity boundary, as plain dataclasses.
   *Look for:* the absence of logic. Temporal serializes these to JSON into the history, so they are the
   **persisted schema** of the system. Adding a field with a default is safe; renaming or removing one can break
   replay of in-flight workflows. Note `IssueInput.pending`, which carries queued run requests across
   continue-as-new.
   *Depends on:* stdlib `dataclasses` only, deliberately, so the workflow sandbox can import it cheaply.

### Stage C: the workflow (read slowly; this is the Temporal part)

5. **[workflows.py L1–L35: imports and helpers](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L1-L35)**
   *What:* module setup for workflow code.
   *Look for:* `workflow.unsafe.imports_passed_through()` (§1.3). There are no imports of `httpx`, `zarr` or
   `cluster_api`, and none of `activities.py`: activities are called **by name** (strings), so the workflow module
   stays pure. Also `workflow_id()`, the business-key ID, and `RUN_STARTED_LOG`, which Lab 5 and the replay test use.
   *Depends on:* `temporalio.workflow`, `temporalio.common.RetryPolicy`, `temporalio.exceptions.ActivityError`, `models`.

6. **[workflows.py L37–L73: state, signals, query](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L37-L73)**
   *What:* the workflow's in-memory state and its message handlers.
   *Look for:*
   - `@workflow.init` lets `__init__` receive the workflow input. That matters because with signal-with-start the
     signal handler can run **before** `execute()` does, and it needs initialized fields.
   - `on_run` **ignores** a run signal unless IDLE. That's the "duplicate run is harmless" rule from the spec, done
     with plain Python state.
   - The `status` query is read-only, as queries must be.
   *Depends on:* `models.RunRequest`, `Discovered`, `WorkflowStatus`.

7. **[workflows.py L76–L93: the main loop and activity options](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L76-L93)**
   *What:* a workflow that never ends. It waits for a signal, does a run, and repeats.
   *Look for:*
   - `wait_condition(lambda: bool(self.pending))`: a durable wait (§1.7). 20 workflows sit here right now holding
     zero threads.
   - The continue-as-new check: run count, `is_continue_as_new_suggested()`, history length. Note
     `wait_condition(workflow.all_handlers_finished)` before it, so no signal handler is cut off mid-flight.
   - `_act()`: one place that turns "seconds, attempts" into `start_to_close_timeout` + `RetryPolicy`.
   *Depends on:* `temporalio.workflow` APIs only.

8. **[workflows.py L95–L130: one run, step by step](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L95-L130)**
   *What:* the spec §7 state machine. ACKED → SUBMITTED → RUNNING → PUBLISHING → IDLE.
   *Look for:*
   - `run_id` is built from `workflow.now()`, not `datetime.now()`. That's determinism (§1.3).
   - `execute_activity` vs `start_activity`. The latter returns a handle, so the workflow can wait on *either*
     "job finished" *or* "cancel signal arrived" (L115).
   - `await_job` is routed to the `-await` task queue with `heartbeat_timeout=2 min` and a `start_to_close`
     derived from the LSF walltime returned by `submit`. That keeps config out of the workflow.
   - Errors: every failed activity surfaces as `ActivityError`, and `_RunFailed` covers business failures.
     `asyncio.CancelledError` means the *workflow itself* is being cancelled.
   *Depends on:* the activity names defined in `activities.py` (a string contract), plus `models`.

9. **[workflows.py L132–L156: failure reporting that survives cancellation](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L132-L156)**
   *What:* "every run ends in exactly one result or failure comment".
   *Look for:* cancelling the in-flight `await_job` handle, killing the LSF job with `cluster_cancel`, then
   `report_failure`, all wrapped in `asyncio.shield(...)` so a workflow cancellation can't interrupt its own
   cleanup. Failures *of the cleanup* are logged, not raised, so the workflow always returns to IDLE.
   *Depends on:* the `cluster_cancel` and `report_failure` activities.

10. **[workflows.py L159–L165: PollWorkflow](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L159-L165)**
    *What:* the workflow the Schedule starts every 2 minutes.
    *Look for:* `maximum_attempts=1`. A failed poll isn't retried, because the next scheduled run *is* the retry.
    This is a common Temporal idiom: let the schedule supply retries.
    *Depends on:* the `poll_github` activity.

### Stage D: activities, where side effects live

11. **[activities.py L22–L48: signal-with-start and poll helpers](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/activities.py#L22-L48)**
    *What:* how the outside world (the poller, the CLI, discovery) talks to issue workflows.
    *Look for:* `client.start_workflow(..., id=workflow_id(issue), start_signal="run", ...)`. That's
    signal-with-start (§1.6): it starts the workflow if needed, then signals it, atomically. Because Workflow IDs are
    unique, polling the same issue 10 times can't create 10 workflows.
    *Depends on:* `temporalio.client.Client` (passed in), `projects` (find Run-stage issues), `state` (path lookup),
    `discovery.MARKER_RE`.

12. **[activities.py L122–L153: the Activities class, ack/submit/await/cancel](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/activities.py#L122-L153)**
    *What:* activity implementations as methods on one object that holds the shared clients.
    *Look for:*
    - `@activity.defn(name=...)`: these names are the strings the workflow uses.
    - `await_job` passes `activity.heartbeat` as the per-poll callback, so every LSF status check becomes a
      heartbeat (§1.4).
    - `ack_run` uses `comment_once` with a marker, so a retried ack never posts twice.
    - `all()` omits `await_job` because it's registered on the other queue (see step 15).
    *Depends on:* `temporalio.activity`, `cluster.Cluster`, `github_api.GitHub`, `projects.set_stage`.

13. **[activities.py L155–L192: publish, finish, failure, poll](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/activities.py#L155-L192)**
    *What:* the GitHub side of the run.
    *Look for:* idempotency at each layer. Files are only committed if absent (`upsert_file`), and the result
    comment carries `<!-- zarr-doctor: run=<id> result -->` so a retry after a partial publish completes the job
    without duplicating it. That exact retry happened during the real-cluster test (see the README).
    *Depends on:* `github_api`, `projects`, the comment templates at L51–L119.

### Stage E: wiring workers, the Schedule and the CLI

14. **[cli.py L40–L44: connecting a client](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/cli.py#L40-L44)**
    *What:* `Client.connect(address, namespace)`.
    *Look for:* a **namespace** is the server's tenancy boundary (we use `default`). The client is cheap and
    long-lived, and is shared by workers, signals and queries.
    *Depends on:* `temporalio.client`.

15. **[cli.py L184–L211: the worker process](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/cli.py#L184-L211)**
    *What:* two `Worker` objects in one process, one per task queue (§1.5).
    *Look for:*
    - `workflows=[...]` registers workflow types and `activities=[...]` registers activity functions.
    - `max_concurrent_activities` is the backpressure knob.
    - On SIGTERM or Ctrl-C the `async with` exits and the workers shut down. In-flight activities fail with "Worker
      is shutting down", and Temporal retries them on the next worker. You'll see exactly that in Lab 1.
    *Depends on:* `temporalio.worker.Worker`, `activities.Activities`, `cluster.Cluster`, `github_api`, `state`.

16. **[cli.py L213–L236: the Schedule](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/cli.py#L213-L236)**
    *What:* creating or updating the server-side cron (§1.9).
    *Look for:* `ScheduleActionStartWorkflow`, `ScheduleIntervalSpec(every=...)`, `ScheduleOverlapPolicy.SKIP`, and
    create-or-update via `ScheduleAlreadyRunningError`, which makes `schedule apply` idempotent.
    *Depends on:* `temporalio.client` schedule types, `workflows.PollWorkflow`.

17. **[cli.py L253–L301: run, cancel, status](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/cli.py#L253-L301)**
    *What:* operator commands built from signals, queries and **visibility** (`client.list_workflows(query)`, which
    lists or filters executions server-side).
    *Look for:* `rpc_timeout=QUERY_TIMEOUT` on queries. A query blocks while no worker is up (§1.1).
    *Depends on:* `temporalio.client`, `workflows`, `activities.signal_run`.

### Stage F: the cluster side

18. **[cluster.py L21–L88: idempotent submission](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/cluster.py#L21-L88)**
    *What:* wraps `py-cluster-api` so that "submit" can safely be retried.
    *Look for:* three layers of "did I already submit this?":
    - the `job_id` file in the run directory;
    - `executor.reconnect()`, which finds a live job by its deterministic name `zd-<issue>-<run>`;
    - only then `bsub`.

    Also: `inherit_env=False` + `login_shell=True` + a prologue that activates the pixi env on the compute node,
    and the `-P` billing argument.
    *Depends on:* `cluster_api` (`create_executor`, `ResourceSpec`, `JobMonitor`), `config.PROJECT_DIR`.

19. **[cluster.py L90–L123: restart-safe waiting](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/cluster.py#L90-L123)**
    *What:* the body of `await_job`.
    *Look for:* when the job isn't in this process's memory (a new worker after a crash), `executor.track(job_id)`
    seeds it so `bjobs` polling resumes. That's the code path behind "restart never duplicates an LSF job". The loop
    wakes every `interval` to call `on_poll`, which is the heartbeat.
    *Depends on:* `cluster_api.JobMonitor`, `JobStatus`, `models.JobOutcome`.

20. **[cluster_api.yaml](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/cluster_api.yaml)**
    *What:* `py-cluster-api` profiles: `janelia_lsf` (real `bsub`/`bjobs`) and `local` (subprocesses, for tests).
    *Look for:* `zombie_timeout_minutes`, the safety net for jobs the scheduler forgets.
    *Depends on:* read by `cluster_api.create_executor`.

21. **[jobs/histogram.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/jobs/histogram.py)**
    *What:* the compute job itself, a standalone script.
    *Look for:* zero imports of Temporal or GitHub (a spec §16 decision); compute code shouldn't depend on the
    orchestrator. Deterministic chunk sampling (L56–L75); one- or two-pass histograms by dtype (L77–L143); a one-line
    error on stderr (L208+) so the failure comment's log tail is readable.
    *Depends on:* `numpy`, `zarr` (v2 and v3), `matplotlib` (Agg), optionally `tensorstore`.

### Stage G: the GitHub side

22. **[github_api.py L34–L130: the HTTP client](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/github_api.py#L34-L130)**
    *What:* a thin async REST + GraphQL client.
    *Look for:*
    - `_rate_limit_delay`: GitHub's primary (`x-ratelimit-*`) and secondary (403/429 with or without
      `retry-after`) rate limits.
    - `_throttle`: at most 1 write/s and 400 creates/hour.
    - `get_token()` (L20), the single place to switch to a GitHub App token.

    Retrying *inside* the client handles short rate-limit waits; Temporal's activity retries handle everything else.
    *Depends on:* `httpx`.

23. **[github_api.py L183–L215: idempotent helpers](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/github_api.py#L183-L215)**
    *What:* `comment_once`, `ensure_branch` (an orphan commit through the Git Data API), and `upsert_file`.
    *Look for:* `_commit_lock`. Concurrent Contents-API commits to one branch return 409, which we hit for real and
    fixed.
    *Depends on:* `httpx`, `base64`.

24. **[projects.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/projects.py)**
    *What:* the two trigger modes behind one function, `set_stage()` (L79). It's either a Projects v2 Status field
    (GraphQL) or `zd:*` labels.
    *Look for:* project, field and option IDs resolved once and cached in SQLite.
    *Depends on:* `github_api.GitHub.graphql`, `state`.

### Stage H: discovery, state, config (no Temporal here)

25. **[discovery.py L80–L134: metadata extraction and traits](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/discovery.py#L80-L134)**
    *What:* reads only `zarr.json` / `.zarray` / `.zattrs`, never chunk data, so all 1,071 volumes take about 25 s.
    *Look for:* sharded v3 arrays (inner vs outer chunk shape), the OME-NGFF 0.4 vs 0.5 attribute locations, and
    `fingerprint()` (a sha1 of the extracted metadata), which detects changed volumes.
    *Depends on:* stdlib `json`, `hashlib`, `numpy` (dtype names only).

26. **[discovery.py L212–L265: the discover loop](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/discovery.py#L212-L265)**
    *What:* creates one issue per new volume, resumable across runs.
    *Look for:*
    - A GitHub reconcile pass first, so dedupe works even with SQLite deleted.
    - The SQLite row written *immediately* after creation, so a crash can't duplicate.
    - The `signal` callback, which signal-with-starts the workflow with a `discovered` payload.

    Discovery is a plain CLI rather than a workflow. It's bounded batch work with its own resumable state.
    *Depends on:* injected `gh`, `state`, `set_stage` and `signal` callables (that injection is what makes it testable).

27. **[state.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/state.py)** and
    **[config.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/config.py)**
    *What:* the only state the service owns (volumes ↔ issues, a key/value table), and YAML config with env overrides
    plus a tiny `.env` loader.
    *Look for:* how little state there is. Temporal holds all per-run state. Config never enters the workflow;
    activities read it, and the workflow only sees values passed in, such as `walltime_minutes`.
    *Depends on:* stdlib `sqlite3`; `pydantic`, `pyyaml`.

### Stage I: tests (the best way to see the workflow move)

28. **[tests/test_workflows.py L20–L98: test harness](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/test_workflows.py#L20-L98)**
    *What:* the time-skipping test server (§1.11), fake activities registered **under the real names**, and two
    real `Worker`s on per-test task queues.
    *Look for:* `Mocks.await_act` can block (`self.block`) while heartbeating, which is how tests hold a run in
    RUNNING to send it signals. `wait_for` polls the `status` query.
    *Depends on:* `temporalio.testing.WorkflowEnvironment`, `temporalio.worker.Worker`, `temporalio.activity`.

29. **[tests/test_workflows.py L100–L186: the scenarios](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/test_workflows.py#L100-L186)**
    *What:* the spec §13 list: success, job failure, restart without resubmitting, duplicate signal, cancel, and
    workflow cancellation.
    *Look for:* `test_restart_between_submit_and_await_does_not_resubmit`. The fake `await_job` fails its first
    attempt and the test asserts `submit` ran once. Temporal's retry is the restart.
    *Depends on:* the harness above, `workflows.IssueWorkflow`.

30. **[tests/test_replay.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/test_replay.py)** and
    **[scripts/replay_history.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/scripts/replay_history.py)**
    *What:* determinism checks (§1.10). The test records a history, changes a log line (not a command), and replays.
    The script replays a history you export from the live server.
    *Look for:* `Replayer(...).replay_workflow(history)`. The unsandboxed runner in the test lets the monkeypatched
    module constant reach the workflow.
    *Depends on:* `temporalio.worker.Replayer`, `temporalio.client.WorkflowHistory`.

31. **[tests/test_activities.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/test_activities.py)**,
    **[tests/test_github_api.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/test_github_api.py)**,
    **[tests/test_cluster_local.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/test_cluster_local.py)**,
    **[tests/conftest.py](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/tests/conftest.py)**
    *What:* activities are plain async methods, so they're tested *without* Temporal, against fakes, `respx` (mocked
    HTTP) and the `local` cluster executor. `conftest.py` builds tiny zarr v2/v3 volumes.
    *Look for:* the split. Workflow tests mock activities; activity tests skip Temporal entirely.
    *Depends on:* `pytest`, `pytest-asyncio`, `respx`, `zarr`.

### Stage J: operations

32. **[scripts/run-all.sh](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/scripts/run-all.sh)**
    *What:* starts the Temporal dev server and the worker under tmux, then applies the Schedule.
    *Look for:* `temporal server start-dev --db-filename ...`. The dev server is a full single-binary Temporal with
    SQLite persistence and the Web UI on :8233. Production would point `TEMPORAL_ADDRESS` at a real cluster with no
    code changes.
    *Depends on:* the `temporal` CLI (downloaded if missing), tmux, pixi.

---

## 3. Hands-on labs

Run these from the repo root on the host where the system is running (currently `e06u05`). `T` below is the
Temporal CLI:

```bash
T=~/.local/state/zarr-doctor/bin/temporal
```

### Lab 1: read a real history, the restart included

```bash
$T workflow show -w zarr-doctor/issue-9
```

You'll see about 39 events. Map them onto [`_run_once`](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L95-L125):

- 1–5: started, with signal 2 (`discovered`), then the first workflow task, which ends in `wait_condition`.
- 6: the `run` signal from the poller at 16:50. 7–9: a workflow task. 10–12: `ack_run`.
- 16–18: `submit_histogram_job`.
- **22: `await_job` scheduled at 16:50; 23: started at 17:02.** That 12-minute gap is the worker being down.

Now look at the attempt details:

```bash
$T workflow show -w zarr-doctor/issue-9 --output json | python3 -c "
import json,sys
for e in json.load(sys.stdin)['events']:
    a = e.get('activityTaskStartedEventAttributes')
    if a: print(e['eventId'], 'attempt', a.get('attempt'), '|', (a.get('lastFailure') or {}).get('message'))"
```

Event 23 shows **attempt 2**, last failure *"Worker is shutting down and this activity did not complete in time"*.
Temporal writes the `ActivityTaskStarted` event only when the activity finally finishes, so retries don't bloat
history; this is that compaction. The new worker reattached to the *same* LSF job (`track()`, step 19) and the run
carried on to `publish_results` (28) and `finish_run` (34).

### Lab 2: live state without reading code

```bash
$T workflow list --limit 25                                    # 20 IssueWorkflows, all Running (waiting for signals)
$T workflow query -w zarr-doctor/issue-9 --type status        # the @workflow.query handler, run on the worker
$T workflow describe -w zarr-doctor/issue-9                    # pending activities, history length
$T schedule describe -s zarr-doctor-poll                       # spec, overlap policy, ActionCounts
$T task-queue describe --task-queue zarr-doctor                # backlog + which workers are polling
```

In `schedule describe`, look for `SkippedOverlap` (it was 5). While the worker was down, a poll workflow started by
the Schedule couldn't finish, because no worker ran its tasks. Each later tick saw it still running and, under
`SKIP`, didn't start another (§1.9). In `task-queue describe`, the `Pollers` section is the server's view of live
workers (`pid@host`).

### Lab 3: the Web UI

Open **http://e06u05.int.janelia.org:8233**:
- **Workflows** → `zarr-doctor/issue-9` → *Event History*: the same events as Lab 1, plus a timeline view. Click
  an activity to see its input, result, attempts and last failure.
- *Queries* tab → `status`.
- **Schedules** → `zarr-doctor-poll`: recent and upcoming runs.

### Lab 4: run the tests and watch time skip

```bash
pixi run pytest tests/test_workflows.py -v
```

Six scenarios run against a real (local) Temporal server in a few seconds. Now break one on purpose: in
`test_restart_between_submit_and_await_does_not_resubmit`, change `Mocks(await_failures=1)` to
`Mocks(await_failures=99)` so the fake `await_job` fails *every* attempt, then rerun just that test:

```bash
pixi run pytest tests/test_workflows.py -q -k restart
```

The log shows `Completing activity as failed ... 'attempt': 1`, then 2, then 3, and no fourth. That's
`RetryPolicy(maximum_attempts=3)`. The workflow then takes the failure path and returns to IDLE, so the test times
out waiting for `finish`. Its assertion message shows the reported failure:
`state='IDLE', ..., last_error='worker died mid-await'`. Revert with `git checkout tests/test_workflows.py`.

### Lab 5: break determinism, then fix it with `patched`

```bash
$T workflow show -w zarr-doctor/issue-9 --output json > /tmp/h9.json
pixi run python scripts/replay_history.py /tmp/h9.json         # ok: current code replays this history
```

Now add a timer before the ack in
[`_run_once`](https://github.com/krokicki/mia-workflow-prototype/blob/74aed15da79d8663e52152c9cd98bd1914b88673/zarr_doctor/workflows.py#L101-L102):

```python
        try:
            await workflow.sleep(1)                     # NEW: an extra command
            await self._act("ack_run", ...)
```

```bash
pixi run python scripts/replay_history.py /tmp/h9.json
# NONDETERMINISTIC: ... [TMPRL1100] Nondeterminism error: Timer machine does not handle this event:
#                       HistoryEvent(id: 10, ActivityTaskScheduled)
```

The new code emits `StartTimer` where history recorded `ActivityTaskScheduled` (event 10). Deploying this would
wedge every in-flight run at that point. Now make the change version-aware:

```python
            if workflow.patched("ack-delay"):
                await workflow.sleep(1)
```

Replay passes again: old histories have no `ack-delay` marker, so `patched()` returns False during their replay,
while new runs take the new path. Revert your edit afterwards with `git checkout zarr_doctor/workflows.py`. The
running worker loaded the original code at startup, so it's unaffected either way.

### Lab 6: trigger a real run (costs one short LSF job)

Pick an issue that's never run, such as #4. Then either add the `zd:run` label in the GitHub UI and wait up to 2
minutes for the poll, or skip the poll:

```bash
pixi run zarr-doctor run --issue 4
watch -n 5 'pixi run -q zarr-doctor status --issue 4 2>/dev/null'
```

Watch it go ACKED → SUBMITTED → RUNNING → PUBLISHING → IDLE in `status`, in the UI's history, and on the issue
(started comment, `zd:running`, then the result comment and `zd:done`). `bjobs -w` shows `zd-4-r1-…` in the
`short` queue.

### Lab 7: kill the worker mid-run

During Lab 6 while the state is RUNNING, stop the worker: `Ctrl-c` in the tmux `worker` window, *don't* press
Enter, then restart it with `pixi run zarr-doctor worker` in that window. The LSF job keeps running. In the UI, the
`await_job` activity shows a failed attempt, and the new worker picks up attempt 2 and resumes polling the same job
id. Verify that nothing was resubmitted:

```bash
grep -c "submitted job" ~/.local/state/zarr-doctor/logs/worker.log   # unchanged by the restart
```

### Lab 8: cancel

Start a run (Lab 6), then while it's RUNNING:

```bash
pixi run zarr-doctor cancel --issue 4
```

This follows `on_cancel` → `wait_condition` wakes → `handle.cancel()` → `cluster_cancel` (`bkill`) →
`report_failure`. Check the issue for a "failed … cancelled" comment and `zd:failed`, and check `bjobs -a` for the
killed job. For the whole-workflow version (§1.7), see `test_workflow_cancellation_still_reports`. Don't cancel the
live workflows themselves: they're meant to live forever, and the next poll would recreate them anyway.

---

## Glossary

| term | meaning |
|---|---|
| **History** | append-only event log of one workflow execution; the source of truth |
| **Command** | what workflow code asks the server to do (schedule activity, start timer, complete…) |
| **Workflow task** | "run the workflow code forward from the latest events"; milliseconds |
| **Activity task** | "run this function"; any duration, retried by policy |
| **Replay** | re-running workflow code against history to rebuild its state |
| **Task queue** | named queue; the routing unit between server and workers |
| **Worker** | your process that polls task queues and runs workflow/activity code |
| **Signal / Query / Update** | async write / sync read / sync read-write message to a workflow |
| **Signal-with-start** | atomically start (if needed) and signal a workflow by ID |
| **Heartbeat** | periodic liveness ping from a long activity; also how it receives cancellation |
| **Continue-as-new** | end this run, start a fresh one (same ID, new history) |
| **Schedule** | server-side cron that starts workflows |
| **Patch / version marker** | `workflow.patched()`: branch on whether a history predates a code change |
| **Namespace** | tenancy boundary on the server (`default` here) |
