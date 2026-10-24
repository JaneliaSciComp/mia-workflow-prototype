"""The `mia-buffers` command line: `plan` (offline planner), `serve` (buffer-api), `register` (plan → buffer) and
the deployment's `worker`, `schedule` and `init-labels` (which read mia_buffers.yaml)."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from autodev.auth import bot_login
from autodev.cli import _client, _setup_logging
from mia_data.cli import _gh, _github_clients

from .config import HUMAN_LABELS, STAGES, load_config
from .plan import build_plan, load_records
from .spec import SpecError, load_spec, spec_hash

if TYPE_CHECKING:
    from .config import Config


def catalog_head(catalog_dir: Path) -> str | None:
    """The checkout's HEAD commit, or None when the directory is not a git checkout."""
    result = subprocess.run(["git", "-C", str(catalog_dir), "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


def _plan(args: argparse.Namespace) -> int:
    try:
        spec = load_spec(args.spec)
    except SpecError as error:
        print(f"spec refused: {error}", file=sys.stderr)
        return 1
    head = catalog_head(args.catalog_dir)
    if head != spec.catalog.ref and not args.allow_ref_mismatch:
        print(
            f"catalog checkout is at {head or 'no commit'} but the spec pins {spec.catalog.ref}; "
            "check out that commit or pass --allow-ref-mismatch",
            file=sys.stderr,
        )
        return 1
    plan = build_plan(
        spec, spec_hash(args.spec), head if head == spec.catalog.ref else None, load_records(args.catalog_dir)
    )
    if args.json:
        args.json.write_text(json.dumps(plan.to_dict(), indent=1))
    print(plan.render_markdown(), end="")
    return 0 if plan.valid else 2


def _serve(args: argparse.Namespace) -> int:
    from .api import serve

    serve(args.state_dir.expanduser(), args.host, args.port)
    return 0


def _register(args: argparse.Namespace) -> int:
    from .register import register

    try:
        spec = load_spec(args.spec)
    except SpecError as error:
        print(f"spec refused: {error}", file=sys.stderr)
        return 1
    out = register(
        args.url,
        args.buffer,
        json.loads(args.plan.read_text()),
        spec,
        args.staged_root,
        tensorizer=args.tensorizer,
        split_sizes={"val": args.val_size, "test": args.test_size},
    )
    print(json.dumps(out, indent=1))
    return 0 if out["registered"] else 2


async def cmd_worker(cfg: Config) -> int:
    """Run the Temporal workers until SIGTERM: one for the workflows and short activities, one for the job waits."""
    from temporalio.worker import Worker

    from autodev.jobs import CsubJobs

    from .activities import Activities
    from .workflows import MiaBuffer, MiaBufferPoll

    client = await _client(cfg)
    clients = _github_clients(cfg)
    bot = await bot_login(cfg)
    activities = Activities(
        cfg, *clients, jobs=CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"), bot=bot, client=client
    )
    main_worker = Worker(
        client,
        task_queue=cfg.task_queue,
        workflows=[MiaBuffer, MiaBufferPoll],
        activities=activities.all(),
        max_concurrent_activities=8,
    )
    await_worker = Worker(
        client, task_queue=cfg.await_queue, activities=[activities.await_job], max_concurrent_activities=20
    )
    task = asyncio.current_task()
    assert task is not None  # we are inside the worker coroutine
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    print(
        f"worker up as {bot}: {cfg.temporal.address} ns={cfg.temporal.namespace} queues {cfg.task_queue}, {cfg.await_queue}"
    )
    try:
        async with main_worker, await_worker:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        print("worker shutting down")
    finally:
        for gh in clients:
            await gh.aclose()
    return 0


async def cmd_schedule(cfg: Config) -> int:
    """Create or update the Temporal schedule that runs MiaBufferPoll every poll interval."""
    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleAlreadyRunningError,
        ScheduleIntervalSpec,
        ScheduleOverlapPolicy,
        SchedulePolicy,
        ScheduleSpec,
        ScheduleUpdate,
    )

    from .workflows import MiaBufferPoll

    client = await _client(cfg)
    schedule_id = f"{cfg.deployment}-poll"
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            MiaBufferPoll.run, id=f"{cfg.deployment}-poll-run", task_queue=cfg.task_queue
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )
    try:
        await client.create_schedule(schedule_id, schedule)
        print(f"created schedule {schedule_id}")
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(schedule_id).update(lambda _: ScheduleUpdate(schedule=schedule))
        print(f"updated schedule {schedule_id}")
    return 0


async def cmd_init_labels(cfg: Config) -> int:
    """Create the stage and human labels (`buffer:*`) on the issue repo."""
    gh = _gh(cfg, cfg.github.buffers_repo)
    try:
        for kind, (color, description) in {**STAGES, **HUMAN_LABELS}.items():
            if await gh.ensure_label(cfg.label(kind), color, description):
                print(f"created {cfg.label(kind)}")
    finally:
        await gh.aclose()
    return 0


def _deployment(args: argparse.Namespace) -> int:
    """Run a command that needs mia_buffers.yaml and Temporal."""
    cfg = load_config(args.config)
    _setup_logging(cfg, args.command if args.command == "worker" else None)
    command = {"worker": cmd_worker, "schedule": cmd_schedule, "init-labels": cmd_init_labels}[args.command]
    return asyncio.run(command(cfg))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mia-buffers")
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan", help="turn a buffer spec and a catalog checkout into a plan (markdown on stdout)")
    plan.add_argument("spec", type=Path)
    plan.add_argument(
        "--catalog-dir", type=Path, required=True, help="a checkout of the catalog repo at the spec's ref"
    )
    plan.add_argument("--json", type=Path, help="also write plan.json here")
    plan.add_argument(
        "--allow-ref-mismatch", action="store_true", help="plan even when the checkout is not at catalog.ref"
    )
    plan.set_defaults(func=_plan)
    serve = sub.add_parser("serve", help="run buffer-api, the sampler and ledger for buffers (spec §5)")
    serve.add_argument("--state-dir", type=Path, default=Path("~/.local/state/mia-buffers"))
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=_serve)
    reg = sub.add_parser("register", help="register a plan's staged volumes with a running buffer-api")
    reg.add_argument("plan", type=Path, help="plan.json from `mia-buffers plan --json`")
    reg.add_argument("spec", type=Path)
    reg.add_argument("--staged-root", type=Path, required=True, help="<root>/<record id>/stage.json per record")
    reg.add_argument("--url", required=True, help="buffer-api base URL")
    reg.add_argument("--buffer", required=True, help="buffer id")
    reg.add_argument("--tensorizer", default="muvit-2d")
    reg.add_argument("--val-size", type=int, default=256, help="frozen val list length")
    reg.add_argument("--test-size", type=int, default=256, help="frozen test list length")
    reg.set_defaults(func=_register)
    for name, help_text in (
        ("worker", "run the Temporal workers (main and await queues)"),
        ("schedule", "create or update the poll Schedule"),
        ("init-labels", "create the buffer:* labels on the issue repo"),
    ):
        command = sub.add_parser(name, help=help_text)
        command.add_argument(
            "--config", help="path to mia_buffers.yaml (default: $MIA_BUFFERS_CONFIG or ./mia_buffers.yaml)"
        )
        if name == "schedule":
            command.add_argument("action", choices=["apply"])
        command.set_defaults(func=_deployment)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
