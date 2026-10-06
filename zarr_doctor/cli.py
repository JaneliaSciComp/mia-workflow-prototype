"""zarr-doctor command line."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import shutil
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

    async def prefect():
        from prefect.client.orchestration import get_client

        async with get_client() as c:
            if err := await c.api_healthcheck():
                raise RuntimeError(err)
        return f"{cfg.prefect.api_url} healthy"

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
                     (f"trigger ({cfg.trigger.mode})", trigger), ("prefect", prefect), ("bsub", bsub),
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
    from .discovery import discover

    max_new = cfg.discovery.max_new_default if args.max_new is None else args.max_new
    if args.dry_run:
        s = await discover(cfg, state=_state(cfg, readonly=True), dry_run=True, pattern=args.filter)
        print(f"found {s.found} / known {s.existing} / new {s.created} / skipped {s.skipped} / errors {s.errors}")
        return 0
    gh, state = _gh(cfg), _state(cfg)

    async def set_stage(n, stage):
        await projects.set_stage(cfg, gh, state, n, stage)

    try:
        s = await discover(cfg, gh=gh, state=state, set_stage=set_stage, max_new=max_new, pattern=args.filter)
    finally:
        await gh.aclose()
    print(f"found {s.found} / existing {s.existing} / created {s.created} / updated {s.updated} / "
          f"deferred {s.deferred} / skipped {s.skipped} / errors {s.errors}")
    return 0


async def cmd_worker(cfg, args) -> int:
    from prefect import aserve
    from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy

    from .flows import DEPLOYMENT, histogram_run, poll_github

    run_dep = await histogram_run.ato_deployment(DEPLOYMENT, tags=["zarr-doctor"])
    poll_dep = await poll_github.ato_deployment(
        DEPLOYMENT, tags=["zarr-doctor"], interval=timedelta(minutes=cfg.prefect.poll_every_minutes),
        # overlap policy: never two polls at once; a tick that collides is cancelled, not queued
        concurrency_limit=ConcurrencyLimitConfig(limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW))
    log.info("serving %s and %s against %s (limit %d)", histogram_run.name, poll_github.name, cfg.prefect.api_url,
             cfg.prefect.concurrency)
    # heartbeat_seconds makes each flow-run process emit heartbeat events; `zarr-doctor setup` turns missing
    # heartbeats into Crashed, and the poller reschedules crashed runs.
    await aserve(run_dep, poll_dep, limit=cfg.prefect.concurrency, heartbeat_seconds=30, pause_on_shutdown=False)
    return 0


ZOMBIE_AUTOMATION = "zarr-doctor: crash flow runs with no heartbeat"


async def cmd_setup(cfg, args) -> int:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.objects import StateType
    from prefect.events.actions import ChangeFlowRunState
    from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

    async with get_client() as client:
        if await client.read_automations_by_name(ZOMBIE_AUTOMATION):
            print(f"automation exists: {ZOMBIE_AUTOMATION}")
            return 0
        await client.create_automation(AutomationCore(
            name=ZOMBIE_AUTOMATION,
            trigger=EventTrigger(
                after={"prefect.flow-run.heartbeat"},
                expect={"prefect.flow-run.heartbeat", "prefect.flow-run.Completed", "prefect.flow-run.Failed",
                        "prefect.flow-run.Cancelled", "prefect.flow-run.Crashed"},
                match={"prefect.resource.id": ["prefect.flow-run.*"]},
                for_each={"prefect.resource.id"}, posture=Posture.Proactive, threshold=1,
                within=timedelta(seconds=90)),
            actions=[ChangeFlowRunState(state=StateType.CRASHED,
                                        message="no heartbeat for 90 s; marked Crashed by zarr-doctor automation")]))
    print(f"created automation: {ZOMBIE_AUTOMATION}")
    return 0


async def cmd_poll_once(cfg, args) -> int:
    from prefect.client.orchestration import get_client

    from .flows import ACTIVE, find_run_hits, path_for_issue, runs_for_issue

    gh, state = _gh(cfg), _state(cfg)
    try:
        hits = await find_run_hits(cfg, gh, state)
        async with get_client() as client:
            for n in hits:
                path = await path_for_issue(gh, state, n)
                busy = path and await runs_for_issue(client, n, ACTIVE)
                print(f"#{n} {path or '(no zarr-doctor marker; ignored)'}" + (" — already running" if busy else
                                                                             " — would start a run" if path else ""))
        if not hits:
            print("no issues in the Run stage")
    finally:
        await gh.aclose()
    return 0


async def cmd_run(cfg, args) -> int:
    from prefect.client.orchestration import get_client

    from .flows import path_for_issue, start_run

    gh = _gh(cfg)
    try:
        path = await path_for_issue(gh, _state(cfg), args.issue)
    finally:
        await gh.aclose()
    if not path:
        sys.exit(f"#{args.issue} is not a zarr-doctor issue")
    async with get_client() as client:
        fr = await start_run(client, args.issue, path, "cli")
    print(f"started flow run {fr.name} ({fr.id}) for #{args.issue}" if fr else f"#{args.issue} already has an active run")
    return 0


async def cmd_cancel(cfg, args) -> int:
    from prefect.client.orchestration import get_client
    from prefect.states import Cancelling

    from .flows import ACTIVE, runs_for_issue

    async with get_client() as client:
        runs = await runs_for_issue(client, args.issue, ACTIVE)
        for fr in runs:
            await client.set_flow_run_state(fr.id, Cancelling(message="cancelled via zarr-doctor cancel"))
            print(f"cancelling flow run {fr.name} ({fr.id})")
    if not runs:
        print(f"#{args.issue} has no active run")
    return 0


async def _current_task(client, flow_run_id) -> str:
    from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
    from prefect.client.schemas.sorting import TaskRunSort

    trs = await client.read_task_runs(flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=[flow_run_id])),
                                      sort=TaskRunSort.EXPECTED_START_TIME_DESC, limit=1)
    return f"{trs[0].name} {trs[0].state.name}" if trs else "—"


async def cmd_status(cfg, args) -> int:
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowFilter, FlowFilterName, FlowRunFilter

    from .flows import ACTIVE, RUN_FLOW, run_id_for, runs_for_issue

    state = _state(cfg, readonly=True)
    async with get_client() as client:
        if args.issue:
            for fr in sorted(await runs_for_issue(client, args.issue), key=lambda r: r.created, reverse=True)[:10]:
                print(f"{fr.created:%Y-%m-%d %H:%M} {fr.state.name:10} run={run_id_for(fr.id)} attempts={fr.run_count} "
                      f"last task: {await _current_task(client, fr.id)}")
            return 0
        recent = await client.read_flow_runs(flow_filter=FlowFilter(name=FlowFilterName(any_=[RUN_FLOW])),
                                             flow_run_filter=FlowRunFilter(), limit=200)
        counts = Counter(fr.state.name for fr in recent)
        print(f"volumes known: {state.count_volumes()}   last poll: {state.get('last_poll_at') or 'never'}")
        print("histogram runs (last 200): " + (", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "none"))
        for fr in recent:
            if fr.state.type in ACTIVE:
                print(f"  #{fr.parameters['issue']} {fr.state.name} run={run_id_for(fr.id)} "
                      f"task: {await _current_task(client, fr.id)}")
    return 0


async def cmd_janitor(cfg, args) -> int:
    from prefect.client.orchestration import get_client

    from .cluster import Cluster
    from .flows import ACTIVE, run_id_for, runs_for_issue

    cluster = Cluster(cfg)
    prefix = re.escape(cfg.cluster.job_name_prefix)
    try:
        jobs = await cluster.executor.reconnect()
    except NotImplementedError:
        sys.exit("janitor needs a scheduler that supports reconnect (LSF)")
    async with get_client() as client:
        for j in jobs:
            m = re.fullmatch(rf"{prefix}-(\d+)-(.+)", j.name)
            if not m:
                continue
            issue, run_id = int(m.group(1)), m.group(2)
            if any(run_id_for(fr.id) == run_id for fr in await runs_for_issue(client, issue, ACTIVE)):
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
    sub.add_parser("check", help="verify GitHub, project, Prefect, LSF and filesystem access")
    sub.add_parser("init-labels", help="create missing labels")
    d = sub.add_parser("discover", help="create one issue per new volume")
    d.add_argument("--max-new", type=int)
    d.add_argument("--dry-run", action="store_true")
    d.add_argument("--filter", help="glob over paths relative to LMD_ROOT, e.g. 'data/em-*/*'")
    sub.add_parser("worker", help="serve the Prefect deployments (histogram-run + the scheduled poller)")
    sub.add_parser("setup", help="create the zombie-run automation on the Prefect server (idempotent)")
    sub.add_parser("poll-once", help="show which issues the poller would start runs for")
    for name in ("run", "cancel"):
        r = sub.add_parser(name, help=f"{name} the histogram run for an issue")
        r.add_argument("--issue", type=int, required=True)
    st = sub.add_parser("status", help="flow-run states and last poll time")
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
           "setup": cmd_setup, "poll-once": cmd_poll_once, "run": cmd_run, "cancel": cmd_cancel,
           "status": cmd_status, "janitor": cmd_janitor}[args.cmd]
    try:
        return asyncio.run(cmd(cfg, args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
