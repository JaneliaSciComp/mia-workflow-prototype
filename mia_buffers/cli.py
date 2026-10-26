"""The `mia-buffers` command line: `plan` (offline planner), `serve` (buffer-api), `register` (plan → buffer) and
the deployment's `worker`, `schedule` and `init-labels` (which read mia_buffers.yaml)."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from autodev.auth import bot_login
from autodev.cli import _client, _setup_logging, apply_schedule, run_checks
from mia_core.fs import require_local_dir, require_secret_file
from mia_core.registry import Registry, RegistryError, read_token
from mia_data.cli import _gh, _github_clients
from mia_data.jobspec import harness_env, harness_pin

from .config import HUMAN_LABELS, PREP_HUMAN_LABELS, PREP_STAGES, STAGES, load_config
from .jobspec import catalog_snapshot
from .plan import build_plan, load_records, plan_hash
from .spec import SpecError, load_spec, spec_hash

if TYPE_CHECKING:
    from autodev.github import GitHub

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

    serve(load_config(args.config))
    return 0


def _register(args: argparse.Namespace) -> int:
    from .register import recipe_for, register

    try:
        spec = load_spec(args.spec)
    except SpecError as error:
        print(f"spec refused: {error}", file=sys.stderr)
        return 1
    try:
        token = read_token(args.token_file) if args.token_file else None
    except RegistryError as error:
        print(error.message, file=sys.stderr)
        return 1
    plan_text = args.plan.read_text()
    commit, digest = harness_pin(load_config(args.config))
    out = register(
        args.url,
        args.buffer,
        json.loads(plan_text),
        spec,
        args.staged_root,
        tensorizer=args.tensorizer,
        split_sizes={"val": args.val_size, "test": args.test_size},
        recipe=recipe_for(spec, tensorizer=args.tensorizer, harness_commit=commit, env_digest=digest),
        plan_hash=args.plan_hash or plan_hash(plan_text),
        token=token,
    )
    print(json.dumps(out, indent=1))
    return 0 if out["registered"] else 2


async def cmd_worker(cfg: Config, args: argparse.Namespace | None = None) -> int:
    """Run the Temporal workers until SIGTERM: one for the workflows and short activities, one for the job waits."""
    from temporalio.worker import Worker

    from autodev.jobs import CsubJobs

    from .activities import Activities
    from .workflows import MiaBuffer, MiaBufferPoll, MiaCacheKeeper, MiaPrep

    client = await _client(cfg)
    clients = (*_github_clients(cfg), _gh(cfg, cfg.github.datasets_repo))
    bot = await bot_login(cfg)
    activities = Activities(
        cfg,
        *clients[:3],
        jobs=CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"),
        bot=bot,
        client=client,
        gh_datasets=clients[3],
    )
    main_worker = Worker(
        client,
        task_queue=cfg.task_queue,
        workflows=[MiaBuffer, MiaBufferPoll, MiaCacheKeeper, MiaPrep],
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


async def cmd_schedule(cfg: Config, args: argparse.Namespace | None = None) -> int:
    """Create or update the Temporal schedules: MiaBufferPoll every poll interval, MiaCacheKeeper every
    `keeper.minutes`."""
    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleIntervalSpec,
        ScheduleOverlapPolicy,
        SchedulePolicy,
        ScheduleSpec,
    )

    from .workflows import MiaBufferPoll, MiaCacheKeeper

    client = await _client(cfg)
    for name, run, minutes in (
        ("poll", MiaBufferPoll.run, cfg.temporal.poll_minutes),
        ("keeper", MiaCacheKeeper.run, cfg.keeper.minutes),
    ):
        schedule_id = f"{cfg.deployment}-{name}"
        schedule = Schedule(
            action=ScheduleActionStartWorkflow(run, id=f"{schedule_id}-run", task_queue=cfg.task_queue),
            spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=minutes))]),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
        )
        print(f"{await apply_schedule(client, schedule_id, schedule)} schedule {schedule_id}")
    return 0


async def cmd_init_labels(cfg: Config, args: argparse.Namespace | None = None) -> int:
    """Create the stage and human labels of buffers (`buffer:*`) and preparation requests (`prep:*`) on the issue
    repo."""
    gh = _gh(cfg, cfg.github.buffers_repo)
    labels = {cfg.label(kind): value for kind, value in {**STAGES, **HUMAN_LABELS}.items()}
    labels |= {cfg.prep_label(kind): value for kind, value in {**PREP_STAGES, **PREP_HUMAN_LABELS}.items()}
    try:
        for name, (color, description) in labels.items():
            if await gh.ensure_label(name, color, description):
                print(f"created {name}")
    finally:
        await gh.aclose()
    return 0


async def cmd_check(cfg: Config, args: argparse.Namespace | None = None) -> int:
    """Try every external dependency and print one ok/BAD line per check."""

    def repo_check(repo: str):
        async def check():
            gh = _gh(cfg, repo)
            try:
                info = (await gh.request("GET", f"/repos/{repo}")).json()
            finally:
                await gh.aclose()
            return f"{info['full_name']} as {await bot_login(cfg)}"

        return check

    def write_probe(directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        probe_file = directory / ".write-test"
        probe_file.touch()
        probe_file.unlink()

    async def temporal():
        from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

        client = await _client(cfg)
        await client.workflow_service.describe_namespace(DescribeNamespaceRequest(namespace=cfg.temporal.namespace))
        return f"{cfg.temporal.address} ns={cfg.temporal.namespace}"

    async def dirs():
        paths = cfg.paths
        written = (paths.state_dir, paths.work_root, paths.raw_root, paths.staged_root)
        for directory in written:
            write_probe(directory)
        if not os.access(paths.storage_root, os.R_OK):
            raise RuntimeError(f"{paths.storage_root} is not readable")
        return ", ".join(str(directory) for directory in (*written, paths.storage_root))

    async def local_root():
        if cfg.paths.local_root is None:
            raise RuntimeError("set paths.local_root in mia.yaml (host-local disk)")
        require_local_dir(cfg.paths.local_root)
        write_probe(cfg.paths.local_root / "locks")
        return str(cfg.paths.local_root)

    async def service_token():
        if cfg.service.token_file is None:
            raise RuntimeError("set service.token_file in mia.yaml and run `mia init-token`")
        require_secret_file(cfg.service.token_file)
        return str(cfg.service.token_file)

    async def harness():
        commit, digest = harness_pin(cfg)
        tensorizer = cfg.harness.dir / "tensorizers" / cfg.sourcing.tensorizer / "tensorizer.yaml"
        if not tensorizer.is_file():
            raise RuntimeError(f"{tensorizer} is missing")
        return f"{commit[:12]} env {digest[:12]} tensorizer {cfg.sourcing.tensorizer}"

    async def harness_fetch():
        """The harness checkout has the `fetch` subcommand the fetch jobs run (milestone C)."""
        command = [str(harness_env(cfg)), "-c", "import sys; from mia_harness.cli import main; sys.exit(main())"]
        try:
            result = subprocess.run([*command, "fetch", "--help"], capture_output=True, text=True, timeout=120)
        except OSError as error:
            raise RuntimeError(f"cannot run the harness environment's python: {error}") from error
        if result.returncode:
            raise RuntimeError(f"`harness fetch --help` exited {result.returncode}: {result.stderr.strip()[-300:]}")
        return f"{cfg.harness.dir} has `harness fetch`"

    async def buffer_api():
        url = f"{cfg.service.url}/buffers/_check_/status"
        status = httpx.get(url, timeout=10).status_code
        if status not in (200, 404):  # 404: a reachable service does not know this buffer
            raise RuntimeError(f"{url} answered {status}")
        return cfg.service.url

    repos = [
        (f"github {kind}", repo_check(getattr(cfg.github, f"{kind}_repo")))
        for kind in ("buffers", "datasets", "snippets", "catalog")
    ]
    # ponytail: no csub row; mia-data's check covers the same broker policy. Add one when the deployments get
    # separate policies.
    return await run_checks(
        [
            *repos,
            ("temporal", temporal),
            ("directories", dirs),
            ("local root", local_root),
            ("service token", service_token),
            ("harness", harness),
            ("harness fetch", harness_fetch),
            ("buffer-api", buffer_api),
        ]
    )


async def catalog_sync(cfg: Config, gh: GitHub, registry: Registry, ref: str | None = None) -> tuple[str, int]:
    """Load the catalog repo's records at `ref` (default: main's head) into the registry; (ref, record count)."""
    ref = ref or await gh.get_ref_sha("main")
    snapshot = await asyncio.to_thread(catalog_snapshot, cfg, ref, gh.tokens)
    records = load_records(snapshot)
    await asyncio.to_thread(registry.put_records, ref, records)
    return ref, len(records)


async def cmd_catalog_sync(cfg: Config, args: argparse.Namespace) -> int:
    """Load the catalog repo's records at `--ref` (default: main's head) into the registry."""
    gh = _gh(cfg, cfg.github.catalog_repo)
    try:
        with Registry.from_config(cfg.service) as registry:
            ref, count = await catalog_sync(cfg, gh, registry, args.ref)
    except RegistryError as error:
        print(error.message, file=sys.stderr)
        return 1
    finally:
        await gh.aclose()
    print(f"synced {ref} ({count} records)")
    return 0


async def cmd_assets_sync(cfg: Config, args: argparse.Namespace) -> int:
    """Run one cache keeper pass here, without Temporal (`--dry-run`: report, change nothing on disk). A pass that
    may evict runs only where the workers' lock directory is: elsewhere it would not see a running job's lock."""
    from dataclasses import asdict

    from autodev.jobs import CsubJobs

    from .activities import Activities

    locks = cfg.paths.local_root / "locks" if cfg.paths.local_root else None
    if not args.dry_run and not (locks and locks.is_dir()):
        print(
            f"refusing to evict on {socket.gethostname()}: {locks or 'paths.local_root/locks'} does not exist, so the "
            "workers' raw-cache locks are not here. Run it on the host `mia up` started the workers on, or pass "
            "--dry-run.",
            file=sys.stderr,
        )
        return 2

    clients = (*_github_clients(cfg), _gh(cfg, cfg.github.datasets_repo))
    try:
        activities = Activities(
            cfg,
            *clients[:3],
            jobs=CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"),
            bot=await bot_login(cfg),
            gh_datasets=clients[3],
        )
        report = await activities.keep(dry_run=args.dry_run)
    finally:
        for gh in clients:
            await gh.aclose()
    for name, value in asdict(report).items():
        print(f"{name}: {value}")
    return 1 if report.errors else 0


async def cmd_datasets_sync(cfg: Config, args: argparse.Namespace) -> int:
    """Ask buffer-api to rescan mia-data's published datasets and reconcile its registry."""
    try:
        with Registry.from_config(cfg.service) as registry:
            counts = registry.sync_datasets()
    except RegistryError as error:
        print("a datasets sync is running; try again" if error.status == 409 else error.message, file=sys.stderr)
        return 1
    print(", ".join(f"{name} {count}" for name, count in counts.items()))
    return 0


async def cmd_retire(cfg: Config, args: argparse.Namespace) -> int:
    """Release a buffer's references and archive its draws by hand, then write its `retired` event (buffer-api then
    refuses its consumers). The issue's stage stays as it is: this is a `buffer:retire` without the workflow."""
    try:
        with Registry.from_config(cfg.service) as registry:
            retired = registry.retire(args.buffer)
            registry.event(args.buffer, "retired", detail=f"by hand; archive {retired.get('archive') or 'none'}")
    except RegistryError as error:
        print(error.message, file=sys.stderr)
        return 1
    print(json.dumps(retired))
    return 0


def _deployment(args: argparse.Namespace) -> int:
    """Run a command that needs mia_buffers.yaml and Temporal."""
    cfg = load_config(args.config)
    _setup_logging(cfg, args.command if args.command == "worker" else None)
    command = {
        "worker": cmd_worker,
        "schedule": cmd_schedule,
        "init-labels": cmd_init_labels,
        "check": cmd_check,
        "catalog": cmd_catalog_sync,
        "datasets": cmd_datasets_sync,
        "assets": cmd_assets_sync,
        "retire": cmd_retire,
    }[args.command]
    return asyncio.run(command(cfg, args))


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
    reg = sub.add_parser("register", help="register a plan's staged volumes with a running buffer-api")
    reg.add_argument("plan", type=Path, help="plan.json from `mia-buffers plan --json`")
    reg.add_argument("spec", type=Path)
    reg.add_argument("--staged-root", type=Path, required=True, help="<root>/<record id>/stage.json per record")
    reg.add_argument("--url", required=True, help="buffer-api base URL")
    reg.add_argument("--buffer", required=True, help="buffer id")
    reg.add_argument("--token-file", type=Path, help="the service's worker token (needed when buffer-api has one)")
    reg.add_argument("--tensorizer", default="muvit-2d")
    reg.add_argument("--config", help="path to mia.yaml, for the harness pin (default: $MIA_CONFIG or ./mia.yaml)")
    reg.add_argument("--plan-hash", help="the plan's hash (default: sha256 of the plan file's text)")
    reg.add_argument("--val-size", type=int, default=256, help="frozen val list length")
    reg.add_argument("--test-size", type=int, default=256, help="frozen test list length")
    reg.set_defaults(func=_register)
    for name, help_text in (
        ("serve", "run buffer-api, the sampler and ledger for buffers (spec §5)"),
        ("worker", "run the Temporal workers (main and await queues)"),
        ("schedule", "create or update the poll Schedule"),
        ("init-labels", "create the buffer:* labels on the issue repo"),
        (
            "check",
            "verify GitHub repos, Temporal, directories, the local root, the token, the harness (and its fetch) and "
            "buffer-api",
        ),
    ):
        command = sub.add_parser(name, help=help_text)
        command.add_argument(
            "--config", help="path to mia_buffers.yaml (default: $MIA_BUFFERS_CONFIG or ./mia_buffers.yaml)"
        )
        if name == "schedule":
            command.add_argument("action", choices=["apply"])
        command.set_defaults(func=_serve if name == "serve" else _deployment)
    for name, help_text in (
        ("catalog", "the catalog repo's records"),
        ("datasets", "mia-data's published datasets"),
        ("assets", "the raw and staged roots (one cache keeper pass: syncs, stale builds, evictions)"),
    ):
        group = sub.add_parser(name, help=f"sync {help_text} into buffer-api's registry")
        sync = group.add_subparsers(dest="action", required=True).add_parser("sync", help=f"sync {help_text}")
        sync.add_argument("--config", help="path to mia.yaml (default: $MIA_CONFIG or ./mia.yaml)")
        if name == "catalog":
            sync.add_argument("--ref", help="catalog commit to load (default: head of main)")
        if name == "assets":
            sync.add_argument("--dry-run", action="store_true", help="report what would be removed; remove nothing")
        sync.set_defaults(func=_deployment)
    retire = sub.add_parser("retire", help="release a buffer's references and archive its draws (buffer-api)")
    retire.add_argument("buffer", help="buffer id (the round id, i<issue>-e<event>)")
    retire.add_argument("--config", help="path to mia.yaml (default: $MIA_CONFIG or ./mia.yaml)")
    retire.set_defaults(func=_deployment)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
