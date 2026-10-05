"""zarr-doctor command line."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import shutil
import signal
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import projects
from .config import load_config

log = logging.getLogger("zarr_doctor")

TRAIT_COLOR, STAGE_COLOR, META_COLOR = "d4c5f9", "0e8a16", "c5def5"


def _setup_logging(cfg, name: str | None) -> None:
    """Log to stderr, and to $STATE_DIR/logs/<name>.log unless name is None."""
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if name:
        logs = cfg.state_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(logs / f"{name}.log"))
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # request lines are noisy; we log writes ourselves


async def _client(cfg, timeout: float = 10.0):
    from temporalio.client import Client

    return await asyncio.wait_for(Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace), timeout)


def _gh(cfg):
    from .github_api import GitHub

    return GitHub.from_config(cfg)


def _state(cfg, readonly: bool = False):
    from .state import State

    return State(cfg.state_dir, readonly=readonly)


# --- commands ---------------------------------------------------------------------------------

async def cmd_check(cfg, args) -> int:
    from .github_api import get_token

    rows: list[tuple[str, bool, str]] = []

    async def row(name, fn):
        try:
            rows.append((name, True, str(await fn())))
        except Exception as e:
            rows.append((name, False, f"{type(e).__name__}: {e}"))

    gh, state = _gh(cfg), _state(cfg)

    async def token():
        get_token(cfg)
        return f"${cfg.github.token_env} is set"

    async def repo():
        r = await gh.request("GET", f"/repos/{cfg.github.repo}")
        perms = r.json().get("permissions", {})
        if not perms.get("push"):
            raise RuntimeError(f"token lacks write access to {cfg.github.repo}: {perms}")
        scopes = r.headers.get("x-oauth-scopes")
        return f"{cfg.github.repo} push ok" + (f" (classic scopes: {scopes})" if scopes is not None else " (fine-grained)")

    async def issues():
        await gh.request("GET", f"/repos/{cfg.github.repo}/issues", params={"per_page": 1})
        return "issues readable"

    async def branch():
        exists = await gh.request("GET", f"/repos/{cfg.github.repo}/git/ref/heads/{cfg.github.results_branch}", ok404=True)
        return f"{cfg.github.results_branch} " + ("exists" if exists else "missing (created on first publish)")

    async def trigger():
        if cfg.trigger.mode == "label":
            missing = [lb for lb in projects.STAGE_LABELS.values()
                       if not await gh.request("GET", f"/repos/{cfg.github.repo}/labels/{lb}", ok404=True)]
            if missing:
                raise RuntimeError(f"missing labels {missing}; run `zarr-doctor init-labels`")
            return "label mode; stage labels present"
        ids = await projects.resolve(gh, cfg, state, refresh=True)
        return f"project {ids.project_id} field {ids.field_id} options {ids.options}"

    async def temporal():
        c = await _client(cfg, 5)
        return f"{cfg.temporal.address} ns={c.namespace}"

    async def bsub():
        p = shutil.which("bsub")
        if not p:
            raise RuntimeError("bsub not on PATH")
        return p

    async def lmd():
        if not (cfg.lmd_root / "data").is_dir() or not os.access(cfg.lmd_root / "data", os.R_OK | os.X_OK):
            raise RuntimeError(f"{cfg.lmd_root}/data not readable")
        return f"{cfg.lmd_root} readable" + (" (WRITABLE — expected read-only)" if os.access(cfg.lmd_root, os.W_OK) else "")

    async def results():
        cfg.results_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(cfg.results_dir, os.W_OK):
            raise RuntimeError(f"{cfg.results_dir} not writable")
        return f"{cfg.results_dir} writable"

    for name, fn in [("github token", token), ("repo access", repo), ("issues", issues), ("results branch", branch),
                     (f"trigger ({cfg.trigger.mode})", trigger), ("temporal", temporal), ("bsub", bsub),
                     ("LMD_ROOT", lmd), ("RESULTS_DIR", results)]:
        await row(name, fn)
    await gh.aclose()
    w = max(len(r[0]) for r in rows)
    for name, ok, detail in rows:
        print(f"{name:<{w}}  {'ok  ' if ok else 'FAIL'}  {detail}")
    return 0 if all(ok for _, ok, _ in rows) else 1


async def cmd_init_labels(cfg, args) -> int:
    from .discovery import TRAITS, parse_dataset

    gh = _gh(cfg)
    wanted = [(lb, STAGE_COLOR, "zarr-doctor stage") for lb in projects.STAGE_LABELS.values()]
    wanted += [(f"trait:{t}", TRAIT_COLOR, "zarr-doctor legacy trait") for t in TRAITS]
    mo = {parse_dataset(d.name) for d in (cfg.lmd_root / "data").iterdir() if d.is_dir()} - {None}
    wanted += sorted({(f"modality:{m}", META_COLOR, "") for m, _ in mo} | {(f"organism:{o}", META_COLOR, "") for _, o in mo})
    created = [name for name, color, desc in wanted if await gh.ensure_label(name, color, desc)]
    await gh.aclose()
    print(f"{len(created)} created, {len(wanted) - len(created)} already present")
    return 0


async def cmd_discover(cfg, args) -> int:
    from .activities import signal_discovered
    from .discovery import discover

    max_new = cfg.discovery.max_new_default if args.max_new is None else args.max_new
    if args.dry_run:
        s = await discover(cfg, state=_state(cfg, readonly=True), dry_run=True, pattern=args.filter)
        print(f"found {s.found} / known {s.existing} / new {s.created} / skipped {s.skipped} / errors {s.errors}")
        return 0
    gh, state = _gh(cfg), _state(cfg)
    try:
        client = await _client(cfg)
    except Exception as e:
        log.warning("Temporal unreachable (%s); issue workflows will start on their first run signal", e)
        client = None

    async def signal_fn(n, path, fp):
        try:
            await signal_discovered(client, cfg, n, path, fp)
        except Exception as e:
            log.warning("could not signal workflow for #%d: %s", n, e)

    async def set_stage(n, stage):
        await projects.set_stage(cfg, gh, state, n, stage)

    try:
        s = await discover(cfg, gh=gh, state=state, set_stage=set_stage, signal=signal_fn if client else None,
                           max_new=max_new, pattern=args.filter)
    finally:
        await gh.aclose()
    print(f"found {s.found} / existing {s.existing} / created {s.created} / updated {s.updated} / "
          f"deferred {s.deferred} / skipped {s.skipped} / errors {s.errors}")
    return 0


async def cmd_worker(cfg, args) -> int:
    from temporalio.worker import Worker

    from .activities import Activities
    from .cluster import Cluster
    from .workflows import IssueWorkflow, PollWorkflow

    client = await _client(cfg)
    gh, state, cluster = _gh(cfg), _state(cfg), Cluster(cfg)
    acts = Activities(cfg, gh, state, cluster, client)
    tq = cfg.temporal.task_queue
    main = Worker(client, task_queue=tq, workflows=[IssueWorkflow, PollWorkflow], activities=acts.all(),
                  max_concurrent_activities=8)
    # await_job holds an LSF poller per job, so it gets its own queue and cap.
    waits = Worker(client, task_queue=f"{tq}-await", activities=[acts.await_job], max_concurrent_activities=20)
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    log.info("worker up: %s ns=%s queues %s, %s-await", cfg.temporal.address, cfg.temporal.namespace, tq, tq)
    try:
        async with main, waits:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        log.info("worker shutting down")
    finally:
        await cluster.close()
        await gh.aclose()
    return 0


async def cmd_schedule(cfg, args) -> int:
    from temporalio.client import (Schedule, ScheduleActionStartWorkflow, ScheduleAlreadyRunningError,
                                   ScheduleIntervalSpec, ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec,
                                   ScheduleUpdate)

    from .workflows import PollWorkflow

    client, sid = await _client(cfg), "zarr-doctor-poll"
    if args.action == "delete":
        await client.get_schedule_handle(sid).delete()
        print(f"deleted schedule {sid}")
        return 0
    sched = Schedule(
        action=ScheduleActionStartWorkflow(PollWorkflow.execute, id="zarr-doctor-poll-run", task_queue=cfg.temporal.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_every_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP))
    try:
        await client.create_schedule(sid, sched)
        print(f"created schedule {sid} (every {cfg.temporal.poll_every_minutes} min)")
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(sid).update(lambda _: ScheduleUpdate(schedule=sched))
        print(f"updated schedule {sid} (every {cfg.temporal.poll_every_minutes} min)")
    return 0


async def cmd_poll_once(cfg, args) -> int:
    from .activities import find_run_hits, path_for_issue

    gh, state = _gh(cfg), _state(cfg)
    try:
        hits = await find_run_hits(cfg, gh, state)
        for n in hits:
            print(f"would signal run: #{n} {await path_for_issue(gh, state, n) or '(no zarr-doctor marker; ignored)'}")
        if not hits:
            print("no issues in the Run stage")
    finally:
        await gh.aclose()
    return 0


async def cmd_run(cfg, args) -> int:
    from .activities import path_for_issue, signal_run

    gh = _gh(cfg)
    try:
        path = await path_for_issue(gh, _state(cfg), args.issue)
    finally:
        await gh.aclose()
    if not path:
        sys.exit(f"#{args.issue} is not a zarr-doctor issue")
    await signal_run(await _client(cfg), cfg, args.issue, path, "cli")
    print(f"signalled run for #{args.issue} ({path})")
    return 0


async def cmd_cancel(cfg, args) -> int:
    from .workflows import workflow_id

    await (await _client(cfg)).get_workflow_handle(workflow_id(args.issue)).signal("cancel")
    print(f"signalled cancel for #{args.issue}")
    return 0


async def cmd_status(cfg, args) -> int:
    from .workflows import IssueWorkflow, workflow_id

    client, state = await _client(cfg), _state(cfg, readonly=True)
    if args.issue:
        st = await client.get_workflow_handle(workflow_id(args.issue)).query(IssueWorkflow.status)
        for k, v in vars(st).items():
            print(f"{k:10} {v}")
        return 0
    counts, busy = Counter(), []
    async for wf in client.list_workflows('WorkflowType="IssueWorkflow" AND ExecutionStatus="Running"'):
        try:
            st = await client.get_workflow_handle(wf.id).query(IssueWorkflow.status)
        except Exception as e:
            counts["query-failed"] += 1
            log.warning("query %s failed: %s", wf.id, e)
            continue
        counts[st.state] += 1
        if st.state != "IDLE":
            busy.append(st)
    print(f"volumes known: {state.count_volumes()}   last poll: {state.get('last_poll_at') or 'never'}")
    print("workflows: " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none running"))
    for st in busy:
        print(f"  #{st.issue} {st.state} run={st.run_id} job={st.job_id}")
    return 0


async def cmd_janitor(cfg, args) -> int:
    from .cluster import Cluster
    from .workflows import IssueWorkflow, workflow_id

    client, cluster = await _client(cfg), Cluster(cfg)
    prefix = re.escape(cfg.cluster.job_name_prefix)
    try:
        jobs = await cluster.executor.reconnect()
    except NotImplementedError:
        sys.exit("janitor needs a scheduler that supports reconnect (LSF)")
    for j in jobs:
        m = re.fullmatch(rf"{prefix}-(\d+)-(.+)", j.name)
        if not m:
            continue
        issue, run_id = int(m.group(1)), m.group(2)
        try:
            st = await client.get_workflow_handle(workflow_id(issue)).query(IssueWorkflow.status)
            live = st.run_id == run_id and st.state in ("ACKED", "SUBMITTED", "RUNNING")
        except Exception:
            live = False  # no running workflow
        if live:
            continue
        print(f"orphan: job {j.job_id} {j.name} ({j.status.value})" + (" -> killing" if args.kill else ""))
        if args.kill:
            await cluster.cancel(j.job_id)
    gh = _gh(cfg)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    try:
        for i in await gh.list_issues(state="closed"):
            rd = cfg.results_dir / str(i["number"])
            closed = datetime.fromisoformat(i["closed_at"].replace("Z", "+00:00"))
            if closed < cutoff and rd.is_dir():
                print(f"stale results: {rd} (#{i['number']} closed {closed:%Y-%m-%d})" + (" -> deleting" if args.kill else ""))
                if args.kill:
                    shutil.rmtree(rd)
    finally:
        await gh.aclose()
    return 0


# --- entry ------------------------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="zarr-doctor")
    p.add_argument("--config", help="path to zarr_doctor.yaml (default $ZARR_DOCTOR_CONFIG or ./zarr_doctor.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="verify GitHub, project, Temporal, LSF and filesystem access")
    sub.add_parser("init-labels", help="create missing labels")
    d = sub.add_parser("discover", help="create one issue per new volume")
    d.add_argument("--max-new", type=int)
    d.add_argument("--dry-run", action="store_true")
    d.add_argument("--filter", help="glob over paths relative to LMD_ROOT, e.g. 'data/em-*/*'")
    sub.add_parser("worker", help="run the Temporal worker")
    s = sub.add_parser("schedule", help="manage the poll Schedule")
    s.add_argument("action", choices=["apply", "delete"])
    sub.add_parser("poll-once", help="show which issues the poller would signal")
    for name in ("run", "cancel"):
        r = sub.add_parser(name, help=f"send a {name} signal to an issue workflow")
        r.add_argument("--issue", type=int, required=True)
    st = sub.add_parser("status", help="workflow states and last poll time")
    st.add_argument("--issue", type=int)
    h = sub.add_parser("histogram", help="run the histogram job locally (no LSF)")
    h.add_argument("path", type=Path)
    h.add_argument("--out", type=Path, required=True)
    h.add_argument("--level", default="s0")
    h.add_argument("--max-chunks", type=int, default=256)
    j = sub.add_parser("janitor", help="find orphan LSF jobs and stale result dirs")
    j.add_argument("--kill", action="store_true", help="kill orphans and delete stale result dirs")
    j.add_argument("--days", type=int, default=30)
    args = p.parse_args(argv)

    if args.cmd == "histogram":  # config-free on purpose
        from .jobs.histogram import main as hist_main

        return hist_main(["--zarr", str(args.path.resolve()), "--out", str(args.out), "--level", args.level,
                          "--max-chunks", str(args.max_chunks)])
    cfg = load_config(args.config)
    dry = args.cmd == "discover" and args.dry_run  # dry run writes nothing, logs included
    _setup_logging(cfg, None if dry else "worker" if args.cmd == "worker" else "cli")
    cmd = {"check": cmd_check, "init-labels": cmd_init_labels, "discover": cmd_discover, "worker": cmd_worker,
           "schedule": cmd_schedule, "poll-once": cmd_poll_once, "run": cmd_run, "cancel": cmd_cancel,
           "status": cmd_status, "janitor": cmd_janitor}[args.cmd]
    try:
        return asyncio.run(cmd(cfg, args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
