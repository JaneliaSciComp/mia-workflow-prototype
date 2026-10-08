# Application-side production pains with Temporal (Python SDK), and the tooling that addresses them (as of Oct 2026)

Scope: what goes wrong in application code, not server ops. Our design: temporalio 1.34, about 1,000 long-lived per-issue entity workflows (`zarr-doctor/issue-<n>`) idle on signals, about 7 activities per signal (one is an LSF long-poll with heartbeats), continue-as-new (CaN) after about 50 runs, one scheduled poller workflow, an existing replay-test script, frequent code changes with LLM/agent steps planned.

Research was done on 2026-10-07. Where a source has no date I say so. Most doc pages are living documents with no date, so treat them as "current as of fetch".

## 1. Non-determinism errors (NDEs) after deploys: frequency, symptoms, detection, recovery, patching pain

### Takeaway
Temporal's own docs and the people who write about it put NDEs from code changes at the top of production problems for new teams. An NDE does not fail loudly. The workflow gets stuck and the worker retries its workflow task forever. Recovery today means rolling back, or resetting (including batch reset, filtered by build ID). Prevention means replay tests plus either patching (`workflow.patched` / `deprecate_patch`) or Worker Versioning. Temporal now recommends Worker Versioning "strongly" over patching alone.

### Cited Findings
- Symptom: NDEs are "usually pretty bad in production." The workflow is stuck, makes no progress, and does not answer queries. Workers keep retrying in a loop, which costs resources on both the worker and server side. — [Qlong, "Myths in Temporal Non Deterministic Errors" (Medium, undated)](https://medium.com/@qlong/myths-in-temporal-non-deterministic-errors-4e0ca7a6aa1b) (author is a Temporal community/ex-staff contributor; secondary source)
- Two root causes: (a) non-deterministic logic inside the workflow (random numbers, wall-clock time), and (b) code changes that emit commands in a different order for the same input. Changes of type (b) "cannot be deployed to the same task queue without potentially breaking outstanding Workflow Executions." — [Temporal docs: Safely deploying changes to Workflow code](https://docs.temporal.io/develop/safe-deployments)
- Official guidance: protect non-deterministic changes with Worker Versioning (pin workflows to code revisions) or with patching APIs. Worker Versioning is "strongly recommended as users see improved error rates when adopting it." No numbers are published. — [Temporal docs: Safe deployments](https://docs.temporal.io/develop/safe-deployments)
- Replay testing ("takes one or more existing Event Histories... and runs them against your current Workflow code") is called the best way to check compatibility before deploying. — [Temporal docs: Safe deployments](https://docs.temporal.io/develop/safe-deployments)
- Community view: "the most common production issue for teams new to Temporal is breaking determinism when deploying new workflow code." — [Practical Guide to Temporal (HackerNoon, undated, secondary)](https://hackernoon.com/a-practical-guide-to-temporal-what-it-does-how-it-compares-and-when-to-use-it)
- A real Python-team account (Deriv, 2025-12-16): calling `datetime.now()` inside a workflow broke replay. — [Ammar Azman, "Learning Temporal the hard way"](https://derivai.substack.com/p/learning-temporal-the-hard-way)
- Patching lifecycle in Python has three deploys: (1) wrap new code in `workflow.patched("id")`; (2) switch to `workflow.deprecate_patch("id")` once pre-patch workflows "have left retention"; (3) remove both. — [Temporal docs: Python versioning](https://docs.temporal.io/develop/python/versioning)
- Temporal itself says patches pile up: "Because Temporal Workflows can run for days, weeks, or even years, patches accumulate into real code complexity and cognitive overhead." — [Temporal blog, 2026-03-30: GA Worker Versioning / Upgrade on CaN](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new)
- Recovery tooling:
  - `temporal workflow reset-batch -q "<visibility query>" --type FirstWorkflowTask --reason ...` recovers workflows broken by a bad deploy after rollback.
  - A `--non-deterministic` flag resets only executions whose last event is a WorkflowTaskFailed with an NDE.
  - A build-ID reset type resets to the first workflow task processed by a given build ID.
  - You can find affected workflows with the search attributes `TemporalWorkerDeploymentVersion='Deployment:BuildID'` and `TemporalReportedProblems` (e.g. `category=WorkflowTaskFailed`).
  - A ramping bad version can be set back to 0% with `temporal worker deployment set-ramping-version ... --percentage 0`.
  — [Temporal docs: Recover pinned Workflows after a bad rollout](https://docs.temporal.io/production-deployment/worker-deployments/recover-pinned-workflows); [Temporal CLI cmd options](https://docs.temporal.io/cli/cmd-options)
- One forum thread reports the opposite failure: a worker "blindly runs the activity" without reporting an NDE, i.e. some incompatible changes pass silently instead of erroring. — [Temporal community forum thread 13084](https://community.temporal.io/t/not-showing-non-determinism-error-because-worker-blindly-runs-the-activity-without-event-history-reconciliation/13084) (I did not read the thread in full; treat this as a reported edge case)

### Inferences
- Our design is fully exposed. We have about 1,000 workflows that never close, and we change code often. Every non-deterministic edit to the per-issue workflow (reordering activities, adding an LLM step, changing a branch condition) hits workflows that are mid-run or parked.
- The patching rule "deprecate once pre-patch workflows have left retention" never comes true for workflows that never close. In practice the deadline is "once every open workflow has continued-as-new past the patch." So our CaN every ~50 runs is the main way old patches get cleared. Workflows that see few signals may take a very long time to reach 50 runs. Forcing CaN on a deploy-version change (Section 2) shortens that window.
- An NDE in our system would be quiet: the issue workflow would stop responding to signals while workers retry. We need an alert on workflow-task failures (Section 5).
- Our replay-test script is the right control. Its value depends on replaying histories that are actually in production: workflows parked mid-sequence, mid-LSF-poll, and just after CaN.

### Gaps
- I found no quantitative data (survey or incident statistics) on how often NDEs happen after deploys. Temporal's "improved error rates" claim comes with no numbers.
- I did not confirm whether a docs page for finding executions still on a patch (e.g. the `TemporalChangeVersion` search attribute) exists for Python. The Python versioning page I fetched does not cover it.

## 2. Worker Versioning / Worker Deployments: history, GA status, Python and self-hosted support, fit for long-lived entity workflows, Upgrade on Continue-as-New

### Takeaway
The original build-ID "compatible version sets" feature (2023 draft) was deprecated and replaced. The current Worker Deployments model, with per-workflow PINNED or AUTO_UPGRADE behavior, reached GA on 2026-03-30 across all SDKs. On self-hosted it needs Server 1.29.1 or later and Python SDK 1.11 or later. For entity workflows that already use CaN, Temporal now recommends PINNED plus "Upgrade on Continue-as-New". That feature is still in Public Preview. The flow: the workflow checks `workflow.info().is_target_worker_deployment_version_changed()` and calls `continue_as_new(..., initial_versioning_behavior=AUTO_UPGRADE)`. This fits our design closely, with one important caveat about idle workflows.

### Cited Findings
- History: "The 2023 draft of Worker Versioning was deprecated and was not made available in Temporal Cloud. The 2024 draft was available in Cloud on an opt-in basis." The legacy build-ID / compatible-sets docs now say to use the Worker Deployment API instead. — [Temporal docs: Worker versioning (legacy)](https://docs.temporal.io/encyclopedia/worker-versioning-legacy); [Python legacy page](https://docs.temporal.io/develop/python/workflows/worker-versioning-legacy)
- The Python SDK first shipped (legacy) Worker Versioning in v1.3.0 (2023-07-24). — [Temporal Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk)
- There was a Public Preview stage ("Pin Workflows to a single code version") before GA. — [Temporal blog: Worker Versioning Public Preview](https://temporal.io/blog/announcing-worker-versioning-public-preview-pin-workflows-to-a-single-code) (I did not fetch it, so its date is unverified)
- GA: "Worker Versioning is now Generally Available across all Temporal SDKs." Published 2026-03-30, announced at Replay 2026. — [Temporal blog 2026-03-30](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new); [Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements)
- Minimum versions for self-hosted: Temporal Server v1.29.1, CLI v1.4.1, UI v2.38.0. Python SDK v1.11. — [Temporal docs: Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- Python API: `from temporalio.common import VersioningBehavior, WorkerDeploymentVersion`, `from temporalio.worker import Worker, WorkerDeploymentConfig`. Every workflow needs a behavior, either PINNED or AUTO_UPGRADE. You set it per workflow in `@workflow.defn` or as a worker default with `default_versioning_behavior`. — [Temporal docs: Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- Official behavior guidance (docs page):
  - Pinned is best for short workflows that finish before the next deploy, and needs no patching.
  - Auto-Upgrade is recommended for "medium to long-running workflows spanning multiple deployments" and "requires manual patching."
  - Long-running workflows that use CaN can upgrade at the CaN boundary without patching.
  - Workflows without CaN should use Auto-Upgrade plus patching.
  - Concepts: ramping (percentage of new workflows), draining, sunsetting old versions.
  - Caveat: do not enable versioning on worker-specific task queues.
  — [Temporal docs: Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- The GA blog recommends "PINNED versioning combined with Upgrade on Continue-as-New" for entity workflows and AI agents that already use CaN, which it calls "zero-patch deployments." — [Temporal blog 2026-03-30](https://temporal.io/blog/ga-worker-versioning-public-preview-upgrade-on-continue-as-new)
- How Upgrade on CaN works:
  - When a new version becomes Current or Ramping, workflows can detect it via `target_worker_deployment_version_changed`. The flag is refreshed after each workflow task completes.
  - In Python: `workflow.info().is_target_worker_deployment_version_changed()` and `workflow.continue_as_new(..., initial_versioning_behavior=ContinueAsNewVersioningBehavior.AUTO_UPGRADE)`.
  - By default, Pinned workflows stay on their original version even across CaN.
  - The new run starts AUTO_UPGRADE on the task queue's target version. After its first workflow task it uses whatever behavior the workflow code declares.
  - Status: Public Preview. Supported in Go, Java, Python, TypeScript, .NET and Ruby (no version numbers given).
  — [Temporal docs: Upgrade on Continue-as-New](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/upgrade-on-continue-as-new); [Python API: ContinueAsNewVersioningBehavior](https://python.temporal.io/temporalio.workflow.ContinueAsNewVersioningBehavior.html)
- Key caveats for idle entity workflows:
  - "Workflows must execute a step to receive the target-version-changed information. Sleeping Workflows won't proactively get it." The docs suggest sending them a Signal to wake them.
  - "Ensure your Workflow input provided by the previous version's workflow definition is compatible with the new version."
  — [Temporal docs: Upgrade on Continue-as-New](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/upgrade-on-continue-as-new)
- Deploy tooling: the Kubernetes temporal-worker-controller and GitHub Actions automation are the official way to run several versions side by side. — [temporal-worker-controller](https://github.com/temporalio/temporal-worker-controller); [Temporal blog: Worker Versioning on Kubernetes](https://temporal.io/blog/safe-deployments-with-temporal-worker-versioning-on-kubernetes); [Temporal blog: Automated Worker Versioning with GitHub Actions](https://temporal.io/blog/automated-worker-versioning-with-github-actions)

### Inferences
- Pure PINNED does not work for us. With about 1,000 idle workflows that never close, old versions never drain, so every deployed version's workers would have to run forever. Pure AUTO_UPGRADE brings back patching for every change.
- Upgrade on CaN is the right fit. Because of the "sleeping workflows won't get it" caveat, the pattern is:
  - on each signal-driven wakeup, check `is_target_worker_deployment_version_changed()`, and if true, CaN right away (before running the new 7-activity sequence) instead of waiting for run 50;
  - after a deploy, send a "nudge" signal to all open workflows (possible via batch signal) if we want them moved eagerly.
  - Until a parked workflow wakes, the old version's workers must stay up. Plan for two worker versions (old and new) to coexist, which means a longer-lived deployment process on our infrastructure.
- Pin-until-CaN means the CaN input (our carried state) becomes a cross-version contract. Schema evolution of the CaN payload replaces patching as the main thing to watch. Add replay or unit tests that feed version N's CaN input to version N+1.
- Risk: Upgrade on CaN is Public Preview as of Oct 2026, and our server must be at least 1.29.1. We must check our self-hosted server version, and whether the feature needs extra dynamic config on OSS (not documented, see Gaps). We must also check that temporalio 1.34 exposes `is_target_worker_deployment_version_changed` (the Python API docs list `ContinueAsNewVersioningBehavior`, which suggests it does).
- The scheduled poller workflow is short-lived per run, so PINNED fits it.

### Gaps
- I found no explicit statement on whether Upgrade on CaN needs a specific server version or dynamic-config flag on self-hosted OSS. The docs page does not say.
- I could not find exact deprecation or removal dates for the legacy build-ID APIs in the Python SDK, or the date Worker Versioning (v3/Deployments) Public Preview started.
- I found no independent (non-Temporal) production reports on how well Worker Versioning GA works, since it is only about six months old.

## 3. Python-SDK-specific pitfalls: sandbox, deadlock detector, worker tuning, heartbeats/timeouts, payload and history limits

### Takeaway
The pitfalls specific to Python are:
- the workflow sandbox, which re-imports non-passthrough modules per run (CPU and memory cost) and blocks some calls like `date.today()`;
- the roughly 2-second "potential deadlock" (TMPRL1101) detector, triggered by blocking or CPU-heavy work in workflow code or an overloaded worker;
- mixing sync I/O into async activities.

The hard platform limits to design around are a 2 MB payload (warning at 256 KB), a 4 MB gRPC request, and history capped at 51,200 events / 50 MB (warning at 10,240 / 10 MB).

### Cited Findings
- How the sandbox works:
  - The workflow file is `exec`'d into a fresh sandbox for each workflow run. Non-stdlib, non-Temporal modules "reload for every workflow run," dynamic imports "cause memory overhead," and importing "can be time-consuming."
  - `datetime.date.today()` and similar calls are restricted, and the sandbox "is not completely isolated."
  - Fixes: `with workflow.unsafe.imports_passed_through():`, or `SandboxRestrictions.default.with_passthrough_modules(...)` at worker creation.
  - Only pass through "known-side-effect-free" modules.
  - `sandboxed=False` / `UnsandboxedWorkflowRunner()` exist but are not recommended, since they lose the determinism checks.
  — [Temporal docs: Python SDK sandbox](https://docs.temporal.io/develop/python/best-practices/python-sdk-sandbox)
- A long-running community thread on structuring sandbox-friendly activity imports shows this is a common stumbling block. — [community.temporal.io thread 6546](https://community.temporal.io/t/structuring-sandbox-friendly-activity-imports-when-using-the-python-sdk/6546)
- Deadlock detector:
  - The SDK raises "[TMPRL1101] Potential deadlock detected" when a workflow task does not yield within about 2 s.
  - Causes: CPU-heavy work, slow local activities, slow replay, worker resource starvation, blocking calls.
  — [Temporal docs: Performance bottlenecks troubleshooting](https://docs.temporal.io/troubleshooting/performance-bottlenecks); [forum: increase the 2 s deadlock timeout in Python](https://community.temporal.io/t/is-there-a-way-to-increase-the-workflow-deadlock-detection-timeout-from-the-default-2-seconds-in-python-sdk/18256)
  - One user hit it because Redis lock calls in a workflow interceptor sometimes took more than 2 s. — [forum: Cause of temporal deadlock errors](https://community.temporal.io/t/cause-of-temporal-deadlock-errors/10797)
- Field report (2025-07-17, Python SDK 1.6.0):
  - TMPRL1101 under about 120 parallel activities at 100% CPU hung the worker process until a pod restart. One contributor was sync boto3 calls inside `async def` activities.
  - Staff advice: set `max_concurrent_activities` so CPU stays under 70% and scale horizontally; batch fan-out; run workflow and activity workers as separate processes (this fixed it); `debug_mode=True` / `TEMPORAL_DEBUG=1` turns off the detector but is not for production.
  — [forum: TMPRL1101 deadlock under high parallelism causes worker hang](https://community.temporal.io/t/tmprl1101-deadlock-error-under-high-parallelism-causes-worker-hang/17947)
- Worker tuning: Worker Auto-Tuning (slot suppliers) was announced as pre-release 2024-09-12, and resource-based auto-tuning entered Public Preview 2024-11-12. — [Temporal Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk)
- Payload limits:
  - A single payload's limit is 2 MB, with a warning at 256 KB. An event-history transaction is limited to 4 MB.
  - Each gRPC request is limited to 4 MB in total, including all payloads and command metadata.
  - Fixes: shrink the data, or compress it with a custom payload codec.
  — [Temporal docs: Troubleshoot payload and gRPC message size limit errors](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
- History limits:
  - Hard limit 51,200 events or 50 MB; warning at 10,240 events or 10 MB.
  - Default pending limits of 2,000 each for activities, child workflows, signals and cancellations (30 for Nexus).
  - Breaching a pending limit makes the workflow task fail and retry.
  — [Temporal docs: Workflow Execution limits](https://docs.temporal.io/workflow-execution/limits)
- Field report: a Python ETL team went over the payload limits by passing large API datasets through activities. Retries with full payloads bloated history and hurt performance. — [Deriv, 2025-12-16](https://derivai.substack.com/p/learning-temporal-the-hard-way)
- The large-payload fix now ships in the product. External Storage ("claim check", S3 or custom driver) went pre-release 2026-04-08 and Public Preview 2026-05-14, for Python and Go. — [Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk); [Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements)

### Inferences
- Deadlock risk in our design is low as long as the workflow body only orchestrates. The LSF long-poll is an activity, and heartbeats run in the activity, not the workflow. The risk goes up if we later parse large LLM outputs or Zarr metadata inside workflow code, or start using heavy libraries (pydantic models, LLM SDKs) without passing them through.
- We have about 1,000 workflows, but most are idle, so the sticky cache evicts them and each signal triggers a replay of up to about 50 runs' worth of history. Combined with sandbox re-imports, this is the CPU and memory cost to watch. Keeping CaN at about 50 runs bounds it.
- History-size check: each run is about 7 activities × 3 events + about 3 workflow-task events per step + a signal. That is roughly 30–60 events per run, so about 2,000–3,000 events per 50-run chain, well under the 10,240-event warning. Heartbeats do not add events; only the last heartbeat detail is kept. A size problem would come from payload bytes, such as LSF logs or LLM transcripts stored in results, not from event count.
- For the LSF activity: set `heartbeat_timeout` well below `start_to_close_timeout`, have the activity really call `activity.heartbeat()` on each poll iteration, and make the poll loop async or run it in a thread pool. A sync blocking poll inside `async def` repeats the boto3 failure from the forum report. I did not fetch a dedicated heartbeat-misconfiguration source, so this is general guidance, not a cited finding.

### Gaps
- I did not find a GitHub issue with numbers on sandbox memory growth or slowness in recent Python SDK versions (1.2x–1.34). The memory concern is documented only qualitatively.
- I found no cited source on common heartbeat/timeout misconfiguration in Python (e.g. sync activities that never heartbeat, `heartbeat_timeout` left unset). This is general knowledge only.
- I could not confirm whether the 2 s deadlock timeout can be configured in Python 1.34. The forum thread title asks the question, but I did not read its answer.
- I did not establish whether resource-based tuning has reached GA (it was last seen in Public Preview, Nov 2024).

## 4. Long-lived entity workflow guidance: continue-as-new, signals during CaN, updates

### Takeaway
The documented rules are:
- wait for all signal and update handlers to finish before CaN or completion (`await workflow.wait_condition(workflow.all_handlers_finished)`);
- never call CaN from inside an update handler;
- guard concurrent async handlers with `asyncio.Lock`;
- carry pending state across in the CaN input.

The 2026 Upgrade on CaN feature makes CaN both the history-truncation point and the version-upgrade point.

### Cited Findings
- `await workflow.wait_condition(workflow.all_handlers_finished)` lets async handlers finish before the workflow ends or continues-as-new. Workers log warnings when a workflow finishes with unfinished handlers. You can suppress them per handler with `unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON`. — [Temporal docs: Python message passing](https://docs.temporal.io/develop/python/message-passing)
- "Temporal does not support Continue-as-New functionality within Update handlers." Finish handlers first, then CaN from the main workflow method. — [Temporal docs: Python message passing](https://docs.temporal.io/develop/python/message-passing)
- Update validators (`@<handler>.validator`) reject an update before anything is written to history. Use `asyncio.Lock` (created in `__init__`) to serialize concurrent handlers. — [Temporal docs: Python message passing](https://docs.temporal.io/develop/python/message-passing)
- `@workflow.init` (2024-10-10) lets `__init__` receive workflow arguments, so handlers that run before `run()` see initialized state. — [Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk)
- Upgrade on CaN is explicitly aimed at "Entity Workflows that run for months or years" and "AI agent Workflows with long sleeps waiting for user input." — [Temporal docs: Upgrade on CaN](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/upgrade-on-continue-as-new)
- Field lesson: use `continue_as_new` and child workflows instead of monolithic designs. Separately, a scheduled workflow's ID has a timestamp appended, which makes it hard to target with signals. — [Deriv, 2025-12-16](https://derivai.substack.com/p/learning-temporal-the-hard-way)

### Inferences
- Our signal-queue pattern needs:
  - a signal handler that only appends to a list (no awaits);
  - a main loop that runs the activity sequence per item;
  - at CaN time, `wait_condition(all_handlers_finished)`, then CaN with the unprocessed signal queue in the input, so signals that arrived during the last run are not dropped.
- `workflow.info().is_continue_as_new_suggested()` exists in the Python SDK (from SDK knowledge; I did not re-fetch it). Triggering CaN on "50 runs OR suggested OR version changed" is more robust than a fixed count.
- If the poller workflow, or anything else, needs request/response with an issue workflow (e.g. "accepted?"), workflow Updates with validators are the modern replacement for signal-plus-query.

### Gaps
- I did not fetch a dedicated doc on draining signals during CaN. The behavior that signals received in the same workflow task as the CaN command are not lost (the server fails the CaN and retries the task) is from prior knowledge and was not re-verified for Oct 2026.

## 5. Observability for applications

### Takeaway
The Python SDK emits metrics via `Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(...)))` or an OpenTelemetry config. There is a newer `OpenTelemetryPlugin`, recommended over the legacy `TracingInterceptor`, which records real span durations. `workflow.logger` and `activity.logger` add workflow context to log lines. Alert thresholds are left to the user.

### Cited Findings
- Metrics are set up with `Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address="0.0.0.0:9000")))`, using `global_tags` (team, service, environment) for grouping. — [Temporal docs: Python observability](https://docs.temporal.io/develop/python/observability)
- Tracing: `OpenTelemetryPlugin` is recommended. It propagates context and has real span durations, and needs `create_tracer_provider()` set up before the client connects. The legacy `TracingInterceptor` produces workflow spans with no duration. — [Temporal docs: Python observability](https://docs.temporal.io/develop/python/observability)
- Logging uses stdlib `logging` through `workflow.logger`. SDK core logs at WARN by default. — [Temporal docs: Python observability](https://docs.temporal.io/develop/python/observability)
- Third-party integrations exist: Braintrust (Public Preview 2026-01-20) and a LangSmith tracing package for workflows and activities. — [Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk); [releases.sh LangSmith entry](https://releases.sh/release/rel_LNedoiy5Jq6NV2m6nOi7S-langsmith-tracing-package-for-workflows-and-activities)
- Problem workflows can be found with the `TemporalReportedProblems` search attribute (e.g. `category=WorkflowTaskFailed`). — [Temporal docs: Recover pinned Workflows](https://docs.temporal.io/production-deployment/worker-deployments/recover-pinned-workflows)

### Inferences
- The most valuable alert for us is the workflow-task failure rate or count, which catches NDEs and deadlocks. Pair it with a periodic visibility query on `TemporalReportedProblems` so we see stuck issue workflows even when metrics are quiet. Next most useful: activity failure rate and heartbeat timeouts on the LSF activity, and schedule-to-start latency (worker starvation).

### Gaps
- The Python observability page I fetched did not list specific SDK metric names to alert on. The SDK metrics reference page was not fetched, so exact names (e.g. `temporal_workflow_task_execution_failed`) are not confirmed here.
- I did not check the production checklist page.

## 6. Learning curve and footgun lists

### Takeaway
People consistently describe a "steep learning curve" caused by the durable-execution and determinism model. Reported time to productivity is anecdotal ("weeks"). I found no rigorous data. The usual footguns: wall-clock time or randomness in workflows, large payloads, sync I/O in async code, unbounded retries, history growth without CaN, and changing workflow code without versioning.

### Cited Findings
- Deriv (Python, 2025-12-16): "steep learning curve, primarily because durable execution is a fundamentally different model" compared to Airflow or Prefect. The author doubted the tool choice several times. Lessons:
  - use CaN and child workflows;
  - avoid stateful classes;
  - set timestamps outside the workflow;
  - make everything async;
  - configure retries on purpose, since not everything should retry forever.
  — [Ammar Azman, "Learning Temporal the hard way"](https://derivai.substack.com/p/learning-temporal-the-hard-way)
- "Many teams shipping to production within weeks". Separately, Temporal's own "State of Development 2025" report is cited as saying only 1 in 4 teams describe their workflow operations as "smooth". This statistic came from a search-result summary of secondary blogs; I did not verify it against the original report. — [HackerNoon practical guide (secondary)](https://hackernoon.com/a-practical-guide-to-temporal-what-it-does-how-it-compares-and-when-to-use-it); [Israeli Tech Radar: Temporal Best Practices (Medium)](https://medium.com/israeli-tech-radar/temporal-best-practices-821073ca3376)
- Temporal released a "Temporal Developer Skill" for coding agents (Public Preview 2026-03-23), aimed at lowering the learning curve when using AI coding tools. — [Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk)

### Inferences
- Our team has already absorbed much of the learning curve: we have a working prototype, a replay script, and a guide in the repo (notes/temporal-guide.md). The remaining learning-curve risk is versioning discipline, not basic concepts.

### Gaps
- I found no credible quantitative "time to productive" data. HN searches returned unrelated results. The "1 in 4 smooth" statistic is unverified at its primary source.

## 7. Agentic / LLM workflows on Temporal (Python)

### Takeaway
LLM agents are now a first-class use case for Temporal:
- the OpenAI Agents SDK integration reached GA on 2026-03-23 (its sandbox-agents integration was in Public Preview 2026-04-16);
- Pydantic AI natively supports Temporal through `TemporalAgent`;
- Google ADK and Braintrust integrations exist.

The two known pains, large payloads (prompts and transcripts) and streaming tokens to UIs, are now addressed by External Storage and Workflow Streams. Both are in Public Preview as of Oct 2026.

### Cited Findings
- The OpenAI Agents SDK + Temporal Python integration was Public Preview on 2025-07-30 and GA as of 2026-03-23. Model calls and tool calls run as activities. — [Temporal changelog: OpenAI Agents SDK Public Preview](https://temporal.io/changelog/open-ai-agents-sdk-integration-pp); [Temporal blog: announcing OpenAI Agents SDK integration](https://temporal.io/blog/announcing-openai-agents-sdk-integration); search summary citing GA on 2026-03-23 from [Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements) (I saw the GA date in a search summary, not in a page I fetched; the 2026-04-16 changelog entry covers the sandbox integration's Public Preview)
- Code lives in [temporalio/ai-integrations (python/openai_agents)](https://github.com/temporalio/ai-integrations/tree/main/python/openai_agents) and the [Temporal AI cookbook](https://docs.temporal.io/ai-cookbook/openai-agents-sdk-python).
- Pydantic AI: `TemporalAgent` wraps an Agent. Model calls, tool calls and external APIs become activities with retry policies, and coordination runs as a deterministic workflow. It also offers OpenTelemetry and MCP support. — [Pydantic AI docs: Durable execution with Temporal](https://ai.pydantic.dev/temporal); [Temporal blog: Pydantic AI + Temporal](https://temporal.io/blog/build-durable-ai-agents-pydantic-ai-and-temporal)
- Replay 2026: a Google ADK integration; External Storage for large payloads (S3 or custom driver), described as "especially useful for agentic and AI data processing"; Workflow Streams (durable streaming over Signals/Updates for "token batches"); Serverless Workers (pre-release, AWS Lambda); Standalone Activities (GA 2026-09-15). — [Replay 2026 announcements](https://temporal.io/blog/replay-2026-product-announcements); [Python SDK changelog](https://temporal.io/changelog/product-area/python-sdk)

### Inferences
- When LLM steps land in our issue workflows:
  - Transcripts and tool outputs will quickly approach the 256 KB warning and the 2 MB error limit. Store them outside Temporal (we already push results to a git branch) and pass references, or adopt External Storage once it leaves preview.
  - Every added agent step is a non-deterministic change to a long-lived workflow (Section 1). Upgrade-on-CaN is what makes adding steps cheap.
  - Agent loops grow history quickly (each model call and tool call is an activity). Cut CaN on `is_continue_as_new_suggested()` as well as on run count.
- If a single external SDK owns the agent loop (OpenAI Agents or Pydantic AI), its workflow-side code runs in our sandbox. Expect passthrough configuration and replay-compat risk whenever that library is upgraded.

### Gaps
- I did not find independent production reports (outside Temporal or Pydantic) on these AI integrations, such as bugs, payload blowups, or replay breakage from library upgrades.
- I did not establish whether External Storage and Workflow Streams work on self-hosted OSS server or only on Cloud (the External Storage changelog entry mentions "cloud, server, and multiple SDKs").
