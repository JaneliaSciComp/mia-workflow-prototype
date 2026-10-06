# Temporal vs Prefect for zarr-doctor

The same service exists twice:

- **`main`**: orchestration on **Temporal** (Python SDK 1.34, dev server 1.32), in
  [`workflows.py`](https://github.com/krokicki/mia-workflow-prototype/blob/cf13b519513461a6c95fb627378d08de96ff866e/zarr_doctor/workflows.py)
  and [`activities.py`](https://github.com/krokicki/mia-workflow-prototype/blob/cf13b519513461a6c95fb627378d08de96ff866e/zarr_doctor/activities.py).
- **`prefect`**: orchestration on **Prefect 3.8**, in
  [`flows.py`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/flows.py).

Everything else is shared and unchanged: discovery, the GitHub client, Projects/labels, the `py-cluster-api`
wrapper, and the histogram job. Both branches pass their test suites. Both were run end to end against a scratch
server with the local cluster executor and a fake GitHub: the same issue states, comments, result files, and
identical statistics for the same volume.

**Summary.**
- **Temporal makes the *execution* durable.** A workflow is a function that resumes mid-line after any crash, and
  a long-lived per-issue workflow waits for signals at no cost. You pay for that with determinism rules,
  versioning discipline, and running your own always-on workers.
- **Prefect makes the *runs* observable and easy to write.** Flows are plain Python with logs, a UI and
  automations out of the box. You pay with coarser recovery: a crashed run is rerun from the top, crash detection
  needs heartbeats plus an automation, and we wrote our own reschedule logic. Correctness on rerun depends entirely
  on idempotent steps. There is also no native way to address one long-lived execution per business key.

---

## 1. The programming model, side by side

### 1.1 What the unit of work is

| | Temporal (`main`) | Prefect (`prefect`) |
|---|---|---|
| Per-issue object | **One workflow per issue**, ID `zarr-doctor/issue-<n>`, open indefinitely and waiting for `run` signals ([`execute`](https://github.com/krokicki/mia-workflow-prototype/blob/cf13b519513461a6c95fb627378d08de96ff866e/zarr_doctor/workflows.py#L76-L88)) | **One flow run per run request**, tagged `issue:<n>` ([`histogram_run`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/flows.py#L226-L249)). There's no per-issue object; the flow-run list filtered by tag is the history. |
| Step | **Activity**, scheduled by name: `execute_activity("ack_run", …)` | **Task**, an ordinary decorated function: `await ack_run(…)` |
| Where steps run | Activities run on whichever worker polls their task queue, so they can sit on another host or queue (`await_job` → `zarr-doctor-await`) | Tasks run *inside the flow-run process*, optionally on a task runner (threads, Dask, Ray) |
| Where "the workflow" runs | Workflow code runs as short **workflow tasks** on any worker; the function's state is rebuilt from history | The flow is one Python process, a subprocess spawned by `serve`/a worker, alive for the whole run |

The two shapes are easiest to see in how a duplicate trigger is refused.

**Temporal:** state inside the long-lived workflow ([`on_run`](https://github.com/krokicki/mia-workflow-prototype/blob/cf13b519513461a6c95fb627378d08de96ff866e/zarr_doctor/workflows.py#L54-L59)):
```python
@workflow.signal(name="run")
def on_run(self, req: RunRequest) -> None:
    if self.state != "IDLE" or self.pending:
        return                       # duplicate: ignored
    self.pending.append(req)
```

**Prefect:** a query against the server before creating a run ([`start_run`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/flows.py#L275-L280)):
```python
if await runs_for_issue(client, issue, ACTIVE):
    return None                      # duplicate: ignored
return await run_deployment("histogram-run/zarr-doctor", parameters={...}, tags=[f"issue:{issue}"], timeout=0)
```
Temporal enforces uniqueness through the **Workflow ID**, so it can't be raced. The Prefect version is
check-then-act, safe here only because the poller is single-flight (deployment concurrency 1). Prefect's
`idempotency_key` on `run_deployment` or a global concurrency limit per issue would make it stricter.

### 1.2 Code constraints

- **Temporal workflow code must be deterministic.** That means no I/O, no wall clock (`workflow.now()`), no
  `random`, and no direct imports of I/O libraries (the sandbox and `imports_passed_through`). Activities are
  called by *string name*, which keeps the workflow module free of `httpx`, `zarr` and `cluster_api`.
- **Prefect flow code is plain Python.** The flow calls tasks directly and reads config. Our flow even calls `deps()`
  and `with_options()` at runtime to derive the `await_job` timeout, which a Temporal workflow couldn't do
  without receiving the value from an activity (`SubmitResult.walltime_minutes`).

### 1.3 Recovery after a crash, the biggest semantic difference

| | Temporal | Prefect |
|---|---|---|
| Worker process dies mid-`await_job` | The activity's **heartbeat timeout** (2 min) expires and the server **re-dispatches that activity** to the next worker. The workflow never notices; only the activity reruns. | If the runner is alive, the flow run goes **Crashed** (`on_crashed` hooks run). If the whole host died, the run stays **Running forever** until something notices. |
| Detecting a dead host | Built in (heartbeat and start-to-close timeouts) | **Heartbeat events plus an automation** we create in [`setup`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/cli.py#L190-L216): "no `prefect.flow-run.heartbeat` for 90 s → set Crashed" |
| Resuming | Mid-function, at the exact `await` that was in flight; completed steps are not re-executed | **Rerun the flow from the top.** Prefect doesn't reschedule crashed runs; [`recover_crashed`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/flows.py#L283-L302) in the poller sets them back to Scheduled (same flow-run id), up to 3 attempts |
| What keeps reruns safe | Event history (plus idempotent activities for at-least-once) | **Only** idempotent steps: `comment_once`, `upsert_file`, the `job_id` file, and the run id derived from the flow-run id so a rerun reattaches to the same LSF job |
| Verified | Worker killed mid-run on real LSF → `await_job` attempt 2 resumed polling the same job ("Worker is shutting down…") | SIGKILL of worker + flow-run processes → Crashed after 90 s → rescheduled after restart → reattached to the finished job, no resubmit, `zd:done` |

Prefect does offer a way to skip completed steps on rerun: **result persistence plus cache policies**
(`persist_result=True`, `cache_policy=INPUTS`), or **transactions**. We didn't need them because every task is
idempotent, but that becomes a requirement as soon as a step isn't.

### 1.4 Messages into a running execution

| need | Temporal | Prefect |
|---|---|---|
| "Start a run" | Signal (`signal-with-start`) to the per-issue workflow | Create a flow run from the deployment (`run_deployment`) |
| "Cancel" | `cancel` **signal**, handled *inside* the workflow: cancel the `await_job` handle, `cluster_cancel`, `report_failure`, with `asyncio.shield` cleanup. Cooperative, in workflow code, with retries. | Set the run to **Cancelling**. The runner **kills the flow-run process**, then runs [`on_cancelled`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/flows.py#L211-L223) **in the runner process**: plain code, no task retries. |
| "What state are you in?" | `status` **query**: a custom handler answered by a worker (blocks if no worker is up) | Read flow-run and task-run **states** from the server. No custom code, no worker needed. [`cmd_status`](https://github.com/krokicki/mia-workflow-prototype/blob/e0db9a7c26ca4c007739ba769bc8bc59a1d5d99d/zarr_doctor/cli.py) shows the latest task as the stage. |
| Arbitrary input to a running execution | Signals and updates (request/response) | Only pause/suspend with `wait_for_input` (a typed form in the UI or `send_input`); suspend **tears down the process and reruns the flow from the top** on resume |

### 1.5 Scheduling

| | Temporal Schedule | Prefect deployment schedule |
|---|---|---|
| Spec | interval / cron / calendar; jitter; pause; backfill | interval / cron / rrule; pause |
| Overlap | 6 policies; we use `SKIP` | concurrency limit + collision strategy `ENQUEUE` or `CANCEL_NEW`; we use limit 1 + `CANCEL_NEW` |
| Created where | `zarr-doctor schedule apply` (separate API object) | Part of the deployment registered by `serve` (`interval=`) |
| Gotcha we hit | With no worker, a started poll can't finish and later ticks are skipped (`SkippedOverlap: 5`) | conda-forge Prefect 3.8 + SQLAlchemy 2.1: the server's scheduler fails on every loop, so no runs are created, and nothing fails visibly except the server log. Pinned SQLAlchemy below 2.1. |
| Storage gotcha | Dev-server SQLite on NFS (`/groups`): fine (single writer) | Server SQLite on NFS: `database is locked` → full pool → HTTP 500s within minutes. Needs local disk (`/scratch`) or Postgres. |

### 1.6 Workers and processes

- **Temporal:** one `zarr-doctor worker` process hosts all workflows and activities. Concurrency is per task queue
  (`max_concurrent_activities` 8 and 20). It's 128 MB resident when idle, for all 20 issue workflows.
- **Prefect:** `zarr-doctor worker` is a **runner** (`serve`) that polls the server and spawns **one subprocess per
  flow run**, about 106 MB each before doing any work, capped by `limit=20`. On top of `serve`, Prefect has **work
  pools**: workers that provision infrastructure *per flow run* (Docker, Kubernetes, ECS, serverless push pools).
  There's no LSF work pool. On LSF you'd write a custom worker, or use `prefect-dask` with `dask-jobqueue`'s
  `LSFCluster` for task-level fan-out. With Temporal, a worker on LSF is just a worker process that happens to run
  in a job.
- **Logging:** our module loggers configured in the CLI don't reach `worker.out` from flow-run subprocesses. Prefect
  wants `get_run_logger()`, which ships logs **to the server and UI per run**, something Temporal doesn't do at all.

### 1.7 Changing code

- **Temporal:** changing the *sequence of commands* in workflow code breaks in-flight executions on replay. Use
  `workflow.patched()` and replay testing; see `scripts/replay_history.py` and Lab 5 in
  [`temporal-guide.md`](https://github.com/krokicki/mia-workflow-prototype/blob/cf13b519513461a6c95fb627378d08de96ff866e/notes/temporal-guide.md).
- **Prefect:** no replay, so a deploy simply applies to new runs, and to reruns of crashed ones, which then execute
  the *new* code from the top. Simpler, but a rerun after a deploy may do different things than its first attempt
  did. Idempotency has to cover that too.

### 1.8 Testing

| | Temporal | Prefect |
|---|---|---|
| Harness | `WorkflowEnvironment.start_time_skipping()`, a real server with a skippable clock | `prefect_test_harness()`, a throwaway real server (SQLite) |
| Replacing side effects | Register fake activities **under the same names**; the real workflow drives them | Inject fakes through `set_deps()`; patch retry delays with `task.with_options(retry_delay_seconds=0)` |
| Unit-testing a step | Activities are plain methods, so call them | `task.fn(...)` calls the undecorated function |
| Unique to it | Replay tests (nondeterminism) | Calling a flow directly as a function, `return_state=True` to assert on the final state |
| Suite | 36 tests | 35 tests (the replay test has no Prefect equivalent; crash-recovery and duplicate-guard tests are new) |

### 1.9 Size of the orchestration code

| | Temporal | Prefect |
|---|---|---|
| Workflow/flow + steps | `workflows.py` 165 + `activities.py` 192 = **357** lines | `flows.py` **322** lines (includes the comment templates, which lived in `activities.py`) |
| Extra machinery we wrote | signal helpers, query, continue-as-new, two task queues | duplicate guard, crash rescheduler, zombie automation, deps injection |
| Branch diff | | 16 files, +686 / −968 |

The code size came out about the same. The work just moved: Temporal needed code for *state and messages*,
Prefect needed code for *recovery and uniqueness*.

---

## 2. Diverging capabilities

### Only Temporal (or much harder in Prefect)

- **Durable mid-function resume** with no re-execution of completed steps, and no caching configuration.
- **Long-lived entity workflows**: one execution per business key (Workflow ID uniqueness) that sleeps for weeks
  on `wait_condition` or durable timers, holding state in plain variables, at zero cost.
- **Signals, queries and updates** into a running execution; signal-with-start.
- **Per-activity heartbeats** carrying progress details and delivering cancellation; automatic redispatch to a
  live worker after a crash, with timeouts at four levels (schedule-to-start, start-to-close, schedule-to-close,
  heartbeat).
- **Cooperative cancellation** with cleanup *inside* the workflow (shielded, with retries), plus child workflows
  with parent-close policies and saga-style compensation.
- **Deterministic replay**, so you can test new code against recorded production histories before deploying, and
  version it with `patched()`.
- **Polyglot SDKs** (Go, Java, TypeScript, Python, .NET, Ruby, PHP): workflows and activities in different
  languages on one task queue.
- **Task-queue routing**: any step can run on any pool of workers (e.g. LSF-hosted workers) without changing the
  workflow.

### Only Prefect (or much easier in Prefect)

- **Plain Python**: no determinism rules, sandbox or versioning discipline.
- **A UI that understands the work**: flow and task runs, per-run **logs stored on the server**, parameters, a
  "Run" button with a typed parameter form, and **artifacts** (markdown and tables per run). The histogram stats
  could be published as a Prefect artifact as well as a GitHub comment.
- **Events and automations**: event triggers, proactive "something didn't happen" triggers, and actions (change
  state, run deployment, pause, webhooks, notifications), all configured server-side without worker code. Our zombie
  detector is one automation.
- **Result persistence and caching** (`cache_policy`, `persist_result`, result storage on local disk or object
  stores) to skip work across runs; transactions with rollback hooks.
- **Infrastructure per run**: work pools provision a container or pod per flow run. Data-parallel `.map()` and
  task runners (Dask, Ray) for fan-out.
- **Lighter server**: a Python app with SQLite or Postgres. Temporal's dev server is a single binary too, but a
  production Temporal cluster has more moving parts (several services plus Cassandra/Postgres/MySQL).

### Neither, out of the box

- **LSF-native execution.** Both drive LSF the same way here (an activity or task calls `py-cluster-api`).
  Temporal can host workers in LSF jobs directly; Prefect would need a custom LSF worker or Dask.
- **GitHub webhooks.** Both poll, per the spec. Prefect Cloud has inbound webhooks that could turn GitHub events
  into Prefect events. Self-hosted Prefect and Temporal both need a small receiver.

---

## 3. Fit for this project

| requirement (spec / later discussion) | better fit |
|---|---|
| "Every run ends in exactly one result or failure comment; restart never loses a run or duplicates a job" | **Temporal** gets this structurally. Prefect gets it from idempotent steps plus our reschedule logic, which is more code that can be wrong. |
| One long-lived, addressable object per issue (status, cancel, future multi-step agentic work) | **Temporal** |
| Operators watching runs, reading logs, pressing "retry" in a UI | **Prefect** |
| Future agentic tools on LSF compute nodes, from the earlier worker-placement discussion | **Temporal**: activity on a task queue served by LSF-hosted workers, with heartbeats. Prefect would need custom infrastructure. |
| Lowest learning curve for contributors | **Prefect** |

For this service as specified, Temporal's guarantees match the requirements more directly. Prefect's main wins
are observability and the lower conceptual load. A common combination is Temporal for orchestration, plus a
results dashboard built on its visibility API or on the GitHub issues themselves.
