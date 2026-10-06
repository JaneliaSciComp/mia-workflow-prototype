"""Prefect flows: one flow run per requested histogram run, plus a scheduled GitHub poller.

Temporal's version (main branch) keeps one long-lived workflow per issue that waits for signals. Prefect has no
durable "wait for a message" primitive across processes, so here each run request becomes its own flow run, tagged
`issue:<n>`. The Prefect server's flow-run list is the per-issue state.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from prefect import flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (FlowFilter, FlowFilterName, FlowRunFilter, FlowRunFilterState,
                                            FlowRunFilterStateType, FlowRunFilterTags)
from prefect.client.schemas.objects import StateType
from prefect.context import get_run_context
from prefect.deployments import run_deployment
from prefect.states import Scheduled

from . import projects
from .discovery import MARKER_RE
from .models import FailureInput, JobOutcome

log = logging.getLogger(__name__)

RUN_FLOW, POLL_FLOW, DEPLOYMENT = "histogram-run", "poll-github", "zarr-doctor"
ACTIVE = [StateType.SCHEDULED, StateType.PENDING, StateType.RUNNING, StateType.CANCELLING, StateType.PAUSED]
MAX_ATTEMPTS = 3  # whole-flow attempts after crashes (the poller reschedules a crashed run up to this many times)
REPORTED_TAG = "zd:reported"


# --- per-process dependencies ------------------------------------------------------------------
# Every flow run is its own subprocess (and cancellation hooks run in the serving process), so clients are built
# lazily per process. Tests inject fakes with set_deps().

@dataclass
class Deps:
    cfg: object
    gh: object
    state: object
    cluster: object


_deps: Deps | None = None


def deps() -> Deps:
    global _deps
    if _deps is None:
        from .cluster import Cluster
        from .config import load_config
        from .github_api import GitHub
        from .state import State

        cfg = load_config()
        _deps = Deps(cfg, GitHub.from_config(cfg), State(cfg.state_dir), Cluster(cfg))
    return _deps


def set_deps(d: Deps | None) -> None:
    global _deps
    _deps = d


def issue_tag(issue: int) -> str:
    return f"issue:{issue}"


def run_id_for(flow_run_id) -> str:
    """Run id derived from the flow-run id, so a rescheduled (crash-recovered) run reuses the same LSF job."""
    return f"p-{str(flow_run_id)[:8]}"


# --- comment templates -------------------------------------------------------------------------

def _num(v) -> str:
    if isinstance(v, float) and not v.is_integer():
        return f"{v:.4g}"
    return f"{int(v):,}"


def _dur(s: float | None) -> str:
    if s is None:
        return "—"
    m, sec = divmod(int(s), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {sec:02d}s" if h else f"{m}m {sec:02d}s"


def ascii_histogram(edges: list[float], counts: list[int], rows: int = 16, width: int = 40) -> str:
    group = max(1, -(-len(counts) // rows))
    sums = [sum(counts[i:i + group]) for i in range(0, len(counts), group)]
    top = max(sums) or 1
    return "\n".join(f"{_num(edges[i * group]):>10} | {'#' * round(width * c / top)}" for i, c in enumerate(sums))


def render_result(cfg, issue: int, run_id: str, stats: dict, hist: dict, outcome: JobOutcome) -> str:
    base = f"https://github.com/{cfg.github.repo}/blob/{cfg.github.results_branch}/results/{issue}/{run_id}"
    s = stats
    return f"""### Zarr Doctor — value histogram (`raw/{s['level']}`)

Sampled **{s['chunks_read']:,} of {s['total_chunks']:,} chunks ({s['sampled_fraction']:.1%})** · {s['voxels_counted']:,} voxels · zarr v{s['zarr_format']} read with {s['reader']}

| min | p1 | median | mean | std | p99 | max | zeros |
|---|---|---|---|---|---|---|---|
| {_num(s['min'])} | {_num(s['p01'])} | {_num(s['p50'])} | {s['mean']:.4g} | {s['std']:.4g} | {_num(s['p99'])} | {_num(s['max'])} | {s['zero_fraction']:.1%} |

![histogram]({base}/histogram.png?raw=true)

<details><summary>Job details</summary>

LSF job `{outcome.job_id}` · {_dur(outcome.runtime_s)} · peak memory {outcome.max_mem or '—'} · exit {outcome.exit_code}
Files: [`stats.json`]({base}/stats.json) · [`histogram.json`]({base}/histogram.json) · log: `{cfg.results_dir}/{issue}/{run_id}/lsf.out`

```
{ascii_histogram(hist['edges'], hist['counts'])}
```
</details>

<!-- zarr-doctor: run={run_id} result -->
"""


def render_failure(cfg, a: FailureInput, log_tail: str) -> str:
    o = a.outcome
    job = (f" · **LSF job:** `{o.job_id}` · **exit:** {'—' if o.exit_code is None else o.exit_code}"
           f" · **runtime:** {_dur(o.runtime_s)}") if o else ""
    retry = ("Move the issue back to **Run** to retry. Status set to **Failed**." if cfg.trigger.mode == "project"
             else "Add the `zd:run` label to retry. Label set to `zd:failed`.")
    return f"""### Zarr Doctor — run `{a.run_id}` failed

**Stage:** {a.stage}{job}

**Error:** {a.error}

````
{log_tail}
````

{retry}

<!-- zarr-doctor: run={a.run_id} failed -->
"""


# --- tasks (the side effects; each is idempotent so retries and whole-flow reruns are safe) -----

async def _report_failure(d: Deps, a: FailureInput) -> None:
    tail = d.cluster.log_tail(a.issue, a.run_id)
    await d.gh.comment_once(a.issue, render_failure(d.cfg, a, tail), f"<!-- zarr-doctor: run={a.run_id} failed -->")
    await projects.set_stage(d.cfg, d.gh, d.state, a.issue, "failed")


@task(retries=4, retry_delay_seconds=[1, 2, 4, 8], timeout_seconds=30)
async def ack_run(issue: int, run_id: str, trigger: str) -> None:
    d = deps()
    await projects.set_stage(d.cfg, d.gh, d.state, issue, "running")
    marker = f"<!-- zarr-doctor: run={run_id} started -->"
    await d.gh.comment_once(issue, f"Run `{run_id}` started (trigger: {trigger}); job pending on LSF.\n\n{marker}", marker)


@task(retries=2, retry_delay_seconds=[2, 4], timeout_seconds=60)
async def submit_histogram_job(issue: int, path: str, run_id: str) -> str:
    return await deps().cluster.submit(issue, path, run_id)


@task(retries=2, retry_delay_seconds=[10, 30])
async def await_job(issue: int, run_id: str, job_id: str) -> JobOutcome:
    logger = get_run_logger()
    out = await deps().cluster.wait(issue, run_id, job_id, on_poll=lambda s: logger.debug("job %s: %s", job_id, s))
    logger.info("job %s finished %s (exit %s)", job_id, out.status, out.exit_code)
    return out


@task(retries=4, retry_delay_seconds=[2, 4, 8, 16], timeout_seconds=120)
async def publish_results(issue: int, run_id: str, outcome: JobOutcome) -> None:
    d = deps()
    rd = d.cluster.run_dir(issue, run_id)
    branch = d.cfg.github.results_branch
    await d.gh.ensure_branch(branch)
    for name in ("histogram.png", "stats.json", "histogram.json"):
        await d.gh.upsert_file(f"results/{issue}/{run_id}/{name}", (rd / name).read_bytes(), branch,
                               f"zarr-doctor: results for #{issue} run {run_id}")
    stats = json.loads((rd / "stats.json").read_text())
    hist = json.loads((rd / "histogram.json").read_text())
    await d.gh.comment_once(issue, render_result(d.cfg, issue, run_id, stats, hist, outcome),
                            f"<!-- zarr-doctor: run={run_id} result -->")


@task(retries=4, retry_delay_seconds=[1, 2, 4, 8], timeout_seconds=30)
async def finish_run(issue: int) -> None:
    d = deps()
    await projects.set_stage(d.cfg, d.gh, d.state, issue, "done")


@task(retries=4, retry_delay_seconds=[1, 2, 4, 8], timeout_seconds=60)
async def report_failure(a: FailureInput) -> None:
    await _report_failure(deps(), a)


# --- the run flow ------------------------------------------------------------------------------

class RunFailed(Exception):
    pass


async def on_cancelled(flow, flow_run, state) -> None:
    """Runs in the serving process after Prefect has killed the flow-run subprocess."""
    d = deps()
    issue, run_id = flow_run.parameters["issue"], run_id_for(flow_run.id)
    id_file = d.cluster.run_dir(issue, run_id) / "job_id"
    job_id = id_file.read_text().strip() if id_file.exists() else None
    if job_id:
        try:
            await d.cluster.cancel(job_id)
        except Exception as e:
            log.warning("issue #%d: cancelling job %s failed: %s", issue, job_id, e)
    await _report_failure(d, FailureInput(issue, run_id, "cancelled", "cancelled",
                                          JobOutcome(job_id, "killed") if job_id else None))


@flow(name=RUN_FLOW, flow_run_name="issue-{issue}", on_cancellation=[on_cancelled])
async def histogram_run(issue: int, path: str, trigger: str = "cli") -> str:
    run_id = run_id_for(get_run_context().flow_run.id)
    d = deps()
    stage, job_id, outcome = "ack_run", None, None
    try:
        await ack_run(issue, run_id, trigger)
        stage = "submit"
        job_id = await submit_histogram_job(issue, path, run_id)
        stage = "await_job"
        outcome = await await_job.with_options(timeout_seconds=(d.cfg.walltime_minutes + 15) * 60)(issue, run_id, job_id)
        if outcome.status != "done":
            raise RunFailed(f"LSF job {outcome.job_id} {outcome.status} (exit {outcome.exit_code})")
        stage = "publish"
        await publish_results(issue, run_id, outcome)
        stage = "finish_run"
        await finish_run(issue)
    except Exception as e:
        await report_failure(FailureInput(issue, run_id, stage, str(e) or type(e).__name__,
                                          outcome or (JobOutcome(job_id, "unknown") if job_id else None)))
        raise
    return run_id


# --- poller ------------------------------------------------------------------------------------

async def find_run_hits(cfg, gh, state) -> list[int]:
    """Issue numbers currently in the Run stage."""
    if cfg.trigger.mode == "project":
        ids = await projects.resolve(gh, cfg, state)
        return sorted(await projects.issues_with_status(gh, cfg, ids, cfg.trigger.statuses["run"]))
    return sorted(i["number"] for i in await gh.list_issues(labels=projects.STAGE_LABELS["run"], state="open"))


async def path_for_issue(gh, state, issue: int) -> str | None:
    if path := state.path_for_issue(issue):
        return path
    m = MARKER_RE.search((await gh.get_issue(issue)).get("body") or "")
    return m.group(1) if m else None


async def runs_for_issue(client, issue: int, states: list[StateType] | None = None):
    return await client.read_flow_runs(
        flow_filter=FlowFilter(name=FlowFilterName(any_=[RUN_FLOW])),
        flow_run_filter=FlowRunFilter(
            tags=FlowRunFilterTags(all_=[issue_tag(issue)]),
            state=FlowRunFilterState(type=FlowRunFilterStateType(any_=states)) if states else None))


async def start_run(client, issue: int, path: str, trigger: str):
    """Create a run unless one is already active for this issue (the duplicate-run guard)."""
    if await runs_for_issue(client, issue, ACTIVE):
        return None
    return await run_deployment(f"{RUN_FLOW}/{DEPLOYMENT}", parameters={"issue": issue, "path": path, "trigger": trigger},
                                tags=[issue_tag(issue)], timeout=0, as_subflow=False, client=client)


async def recover_crashed(client, d: Deps) -> list[str]:
    """Reschedule crashed runs (process/host died) so they rerun and reattach to their LSF job; give up after
    MAX_ATTEMPTS and report. This is the Prefect stand-in for Temporal retrying an activity on another worker."""
    actions = []
    crashed = await client.read_flow_runs(
        flow_filter=FlowFilter(name=FlowFilterName(any_=[RUN_FLOW])),
        flow_run_filter=FlowRunFilter(state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.CRASHED]))))
    for fr in crashed:
        if REPORTED_TAG in fr.tags:
            continue
        issue = fr.parameters["issue"]
        if fr.run_count < MAX_ATTEMPTS and not await runs_for_issue(client, issue, ACTIVE):
            await client.set_flow_run_state(fr.id, Scheduled(message="rescheduled by zarr-doctor after crash"), force=True)
            actions.append(f"rescheduled {fr.name} ({fr.id})")
        else:
            run_id = run_id_for(fr.id)
            await _report_failure(d, FailureInput(issue, run_id, "crashed", fr.state.message or "flow run crashed"))
            await client.update_flow_run(fr.id, tags=[*fr.tags, REPORTED_TAG])
            actions.append(f"reported {fr.name} ({fr.id})")
    return actions


@flow(name=POLL_FLOW)
async def poll_github() -> list[int]:
    logger = get_run_logger()
    d = deps()
    started = []
    async with get_client() as client:
        for n in await find_run_hits(d.cfg, d.gh, d.state):
            path = await path_for_issue(d.gh, d.state, n)
            if path is None:
                logger.warning("issue #%d is in the Run stage but has no zarr-doctor marker; ignoring", n)
            elif await start_run(client, n, path, d.cfg.trigger.mode):
                started.append(n)
        for a in await recover_crashed(client, d):
            logger.warning(a)
    d.state.set("last_poll_at", datetime.now(timezone.utc).isoformat())
    if started:
        logger.info("poll: started runs for %s", started)
    return started
