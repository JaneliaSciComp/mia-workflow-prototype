"""mia-data command line (mirrors autodev/cli.py)."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from datetime import timedelta

from autodev.auth import bot_login, token_fn
from autodev.cli import _client, _setup_logging
from autodev.github import GitHub

from .config import EXTRA_LABELS, HUMAN_LABELS, STAGES, load_config

log = logging.getLogger("mia_data")


def _gh(cfg, repo: str, tokens=None) -> GitHub:
    return GitHub(repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)


async def cmd_seed(cfg, args) -> int:
    from .seed import seed

    gh_data, gh_catalog = _gh(cfg, cfg.github.data_repo), _gh(cfg, cfg.github.catalog_repo)
    try:
        made = await seed(cfg, gh_data, gh_catalog, args.dry_run)
    finally:
        await gh_data.aclose()
        await gh_catalog.aclose()
    print(f"{'would create' if args.dry_run else 'created'} {len(made)} issue(s)")
    return 0


async def cmd_init_labels(cfg, args) -> int:
    gh = _gh(cfg, cfg.github.data_repo)
    labels = {**{cfg.label(k): v for k, v in {**STAGES, **HUMAN_LABELS}.items()}, **EXTRA_LABELS}
    try:
        for name, (color, desc) in labels.items():
            if await gh.ensure_label(name, color, desc):
                print(f"created {name}")
    finally:
        await gh.aclose()
    return 0


async def cmd_worker(cfg, args) -> int:
    from temporalio.worker import Worker

    from autodev.jobs import CsubJobs

    from .activities import Activities
    from .workflows import MiaDataDataset, MiaDataPoll

    client = await _client(cfg)
    tokens, g = token_fn(cfg), cfg.github
    ghs = [_gh(cfg, r, tokens) for r in (g.data_repo, g.snippets_repo, g.catalog_repo)]
    bot = await bot_login(cfg)
    acts = Activities(cfg, *ghs, CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"), bot, client)
    main = Worker(client, task_queue=cfg.task_queue, workflows=[MiaDataDataset, MiaDataPoll], activities=acts.all(),
                  max_concurrent_activities=8)
    waits = Worker(client, task_queue=cfg.await_queue, activities=[acts.await_job], max_concurrent_activities=20)
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    log.info("worker up as %s: %s ns=%s queues %s, %s", bot, cfg.temporal.address, cfg.temporal.namespace,
             cfg.task_queue, cfg.await_queue)
    try:
        async with main, waits:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        log.info("worker shutting down")
    finally:
        for gh in ghs:
            await gh.aclose()
    return 0


async def cmd_schedule(cfg, args) -> int:
    from temporalio.client import (Schedule, ScheduleActionStartWorkflow, ScheduleAlreadyRunningError,
                                   ScheduleIntervalSpec, ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec,
                                   ScheduleUpdate)

    from .workflows import MiaDataPoll

    client = await _client(cfg)
    sid = f"{cfg.deployment}-poll"
    sched = Schedule(
        action=ScheduleActionStartWorkflow(MiaDataPoll.run, id=f"{cfg.deployment}-poll-run", task_queue=cfg.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP))
    try:
        await client.create_schedule(sid, sched)
        print(f"created schedule {sid}")
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(sid).update(lambda _: ScheduleUpdate(schedule=sched))
        print(f"updated schedule {sid}")
    return 0


async def cmd_later(cfg, args) -> int:
    print(f"mia-data {args.cmd}: not implemented yet", file=sys.stderr)
    return 2


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="mia-data", description=__doc__)
    p.add_argument("--config", help="path to mia_data.yaml (default: $MIA_DATA_CONFIG or ./mia_data.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("seed", help="create one issue per downloaded dataset").add_argument("--dry-run", action="store_true")
    sub.add_parser("init-labels", help="create the stage, human and extra labels on the data repo")
    sub.add_parser("worker", help="run the Temporal workers (main and await queues)")
    sub.add_parser("schedule", help="manage the poll Schedule").add_argument("action", choices=["apply"])
    for name in ("check", "test-pr", "crops-by-snippet", "reindex"):  # Task 18 / 19
        sub.add_parser(name, help="(not implemented yet)").add_argument("rest", nargs="*")
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    cmd = {"seed": cmd_seed, "init-labels": cmd_init_labels, "worker": cmd_worker, "schedule": cmd_schedule}
    return asyncio.run(cmd.get(args.cmd, cmd_later)(cfg, args))


if __name__ == "__main__":
    sys.exit(main())
