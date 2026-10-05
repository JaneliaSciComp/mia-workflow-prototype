"""Temporal activities: GitHub writes, LSF submit/await/cancel, publishing, polling."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from temporalio import activity

from . import projects
from .discovery import MARKER_RE
from .models import (AckInput, AwaitInput, Discovered, FailureInput, FinishInput, IssueInput, JobOutcome,
                     PublishInput, RunRequest, SubmitInput, SubmitResult)
from .workflows import IssueWorkflow, workflow_id

log = logging.getLogger(__name__)


# --- signalling helpers (used by poll_github and the CLI) -------------------------------------

async def signal_run(client, cfg, issue: int, path: str, trigger: str) -> None:
    await client.start_workflow(
        IssueWorkflow.execute, IssueInput(issue, path), id=workflow_id(issue), task_queue=cfg.temporal.task_queue,
        start_signal="run", start_signal_args=[RunRequest(trigger, datetime.now(timezone.utc).isoformat())])


async def signal_discovered(client, cfg, issue: int, path: str, fp: str) -> None:
    await client.start_workflow(
        IssueWorkflow.execute, IssueInput(issue, path), id=workflow_id(issue), task_queue=cfg.temporal.task_queue,
        start_signal="discovered", start_signal_args=[Discovered(path, fp)])


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


# --- comment templates ------------------------------------------------------------------------

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
    job = f" · **LSF job:** `{o.job_id}` · **exit:** {'—' if o.exit_code is None else o.exit_code} · **runtime:** {_dur(o.runtime_s)}" if o else ""
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


# --- activities -------------------------------------------------------------------------------

class Activities:
    def __init__(self, cfg, gh, state, cluster, client=None):
        self.cfg, self.gh, self.state, self.cluster, self.client = cfg, gh, state, cluster, client

    def all(self) -> list:
        return [self.ack_run, self.submit_histogram_job, self.publish_results, self.finish_run,
                self.report_failure, self.cluster_cancel, self.poll_github]

    async def _stage(self, issue: int, stage: str) -> None:
        await projects.set_stage(self.cfg, self.gh, self.state, issue, stage)

    @activity.defn(name="ack_run")
    async def ack_run(self, a: AckInput) -> None:
        await self._stage(a.issue, "running")
        marker = f"<!-- zarr-doctor: run={a.run_id} started -->"
        await self.gh.comment_once(a.issue, f"Run `{a.run_id}` started (trigger: {a.trigger}); job pending on LSF.\n\n{marker}", marker)

    @activity.defn(name="submit_histogram_job")
    async def submit_histogram_job(self, a: SubmitInput) -> SubmitResult:
        job_id = await self.cluster.submit(a.issue, a.path, a.run_id)
        return SubmitResult(job_id, self.cfg.walltime_minutes)

    @activity.defn(name="await_job")
    async def await_job(self, a: AwaitInput) -> JobOutcome:
        out = await self.cluster.wait(a.issue, a.run_id, a.job_id, on_poll=activity.heartbeat)
        log.info("issue #%d run %s: job %s finished %s (exit %s)", a.issue, a.run_id, a.job_id, out.status, out.exit_code)
        return out

    @activity.defn(name="cluster_cancel")
    async def cluster_cancel(self, job_id: str) -> None:
        log.info("cancelling job %s", job_id)
        await self.cluster.cancel(job_id)

    @activity.defn(name="publish_results")
    async def publish_results(self, a: PublishInput) -> None:
        rd = self.cluster.run_dir(a.issue, a.run_id)
        branch = self.cfg.github.results_branch
        await self.gh.ensure_branch(branch)
        for name in ("histogram.png", "stats.json", "histogram.json"):
            await self.gh.upsert_file(f"results/{a.issue}/{a.run_id}/{name}", (rd / name).read_bytes(), branch,
                                      f"zarr-doctor: results for #{a.issue} run {a.run_id}")
        stats = json.loads((rd / "stats.json").read_text())
        hist = json.loads((rd / "histogram.json").read_text())
        await self.gh.comment_once(a.issue, render_result(self.cfg, a.issue, a.run_id, stats, hist, a.outcome),
                                   f"<!-- zarr-doctor: run={a.run_id} result -->")

    @activity.defn(name="finish_run")
    async def finish_run(self, a: FinishInput) -> None:
        await self._stage(a.issue, "done")

    @activity.defn(name="report_failure")
    async def report_failure(self, a: FailureInput) -> None:
        tail = self.cluster.log_tail(a.issue, a.run_id)
        await self.gh.comment_once(a.issue, render_failure(self.cfg, a, tail), f"<!-- zarr-doctor: run={a.run_id} failed -->")
        await self._stage(a.issue, "failed")

    @activity.defn(name="poll_github")
    async def poll_github(self) -> list[int]:
        hits = await find_run_hits(self.cfg, self.gh, self.state)
        signalled = []
        for n in hits:
            path = await path_for_issue(self.gh, self.state, n)
            if path is None:
                log.warning("issue #%d is in the Run stage but has no zarr-doctor marker; ignoring", n)
                continue
            await signal_run(self.client, self.cfg, n, path, self.cfg.trigger.mode)
            signalled.append(n)
        self.state.set("last_poll_at", datetime.now(timezone.utc).isoformat())
        if signalled:
            log.info("poll: signalled run for %s", signalled)
        return signalled
