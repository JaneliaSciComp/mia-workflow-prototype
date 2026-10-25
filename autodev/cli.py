"""The `autodev` command line: health checks, label setup, the worker and the poll schedule. Heavy imports
happen inside the commands that need them so `--help` stays fast. Starting and stopping deployments is
`pixi run mia` (mia_ops).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from .config import STAGES, load_config

if TYPE_CHECKING:
    from temporalio.client import Client

    from .config import Config, Deployment

log = logging.getLogger("autodev")


def _setup_logging(cfg: Deployment, name: str | None) -> None:
    """Log to stderr, and also to state_dir/logs/<name>.log when `name` is given (the worker)."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if name:
        logs = cfg.paths.state_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(logs / f"{name}.log"))
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def proxy_target(environ: Mapping[str, str]) -> str | None:
    """The host:port of $https_proxy, if set."""
    url = environ.get("https_proxy") or environ.get("HTTPS_PROXY")
    return urlsplit(url).netloc if url else None


async def _client(cfg: Deployment) -> Client:
    """Connect to Temporal, through the HTTPS proxy when the config says the sandbox requires it."""
    from temporalio.client import Client
    from temporalio.service import HttpConnectProxyConfig

    proxy = None
    if cfg.temporal.via_https_proxy:  # inside agentic-sandbox the only way out is the allowlist proxy
        if not (target := proxy_target(os.environ)):
            sys.exit(
                "autodev: temporal.via_https_proxy is set but $https_proxy is not (run under sandbox-run.sh --allow)"
            )
        proxy = HttpConnectProxyConfig(target_host=target)
    return await asyncio.wait_for(
        Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace, http_connect_proxy_config=proxy), 10
    )


def _csub_problems(cfg: Config, probe: Any) -> list[str]:
    """Ways the csub broker policy (from `csub probe`) would stop agent jobs from running."""
    need = set(cfg.agent.allow_hosts)  # Anthropic hosts come from the policy's [agents] claude entry
    work_root = cfg.paths.work_root.resolve()
    return [
        *([] if "claude" in probe.agents else ['agent "claude" not in the policy [agents] table']),
        *(f"host not allowed: {host}" for host in sorted(need - set(probe.allowed_hosts))),
        *(
            []
            if any(work_root.is_relative_to(Path(root).resolve()) for root in probe.allowed_roots)
            else [f"work_root {work_root} not under allowed_roots {probe.allowed_roots}"]
        ),
    ]


async def run_checks(checks: Sequence[tuple[str, Callable[[], Awaitable[Any]]]]) -> int:
    """Run every named check (none is skipped because an earlier one failed), print one ok/BAD line each
    and return 1 if any failed. Shared with mia-data's `check`."""
    rows: list[tuple[str, bool, str]] = []
    for name, check in checks:
        try:
            rows.append((name, True, str(await check())))
        except Exception as error:  # report every check, then fail
            rows.append((name, False, f"{type(error).__name__}: {error}"))
    for name, ok, detail in rows:
        print(f"{'ok ' if ok else 'BAD'}  {name:16} {detail}")
    return 0 if all(ok for _, ok, _ in rows) else 1


async def cmd_check(cfg: Config, args: argparse.Namespace) -> int:
    """Try every external dependency and print one ok/BAD line per check."""
    from .agent import csub_client
    from .auth import bot_login
    from .github import GitHub

    async def repo():
        gh = GitHub.for_autodev(cfg)
        try:
            info = (await gh.request("GET", f"/repos/{cfg.github.repo}")).json()
        finally:
            await gh.aclose()
        return f"{info['full_name']} (default branch {info['default_branch']})"

    async def csub_ok():
        probe = await asyncio.to_thread(csub_client(cfg).probe)
        problems = _csub_problems(cfg, probe)
        if problems:
            raise RuntimeError("; ".join(problems))
        return f"broker user {probe.user}, sandbox {probe.sandbox}"

    async def temporal():
        client = await _client(cfg)
        return f"{cfg.temporal.address} ns={cfg.temporal.namespace} ({client.identity})"

    async def dirs():
        for directory in (cfg.paths.state_dir, cfg.paths.work_root):
            directory.mkdir(parents=True, exist_ok=True)
            (directory / ".write-test").touch()
            (directory / ".write-test").unlink()
        return f"{cfg.paths.state_dir}, {cfg.paths.work_root}"

    return await run_checks(
        [
            ("github identity", lambda: bot_login(cfg)),
            ("github repo", repo),
            ("temporal", temporal),
            ("csub broker", csub_ok),
            ("directories", dirs),
        ]
    )


async def cmd_init_labels(cfg: Config, args: argparse.Namespace) -> int:
    from .github import GitHub

    gh = GitHub.for_autodev(cfg)
    for stage, (color, description) in STAGES.items():
        if await gh.ensure_label(cfg.label(stage), color, description):
            print(f"created {cfg.label(stage)}")
    await gh.aclose()
    return 0


async def cmd_worker(cfg: Config, args: argparse.Namespace) -> int:
    """Run the Temporal worker until SIGTERM: one worker for workflows and short activities, one for the
    long-running job waits."""
    from temporalio.worker import Worker

    from .activities import Activities
    from .agent import AgentJobs
    from .auth import bot_login, noreply_email, token_fn
    from .github import GitHub
    from .gitops import Mirror
    from .workflows import IssueWorkflow, PollWorkflow

    client = await _client(cfg)
    tokens = token_fn(cfg)
    gh = GitHub.for_autodev(cfg, tokens)
    bot = await bot_login(cfg)
    # Agent commits are authored as the account that pushes them and opens the PR.
    mirror = Mirror(
        cfg.paths.state_dir / "mirror" / f"{cfg.repo_slug}.git",
        cfg.git_url,
        tokens,
        author=(bot, await noreply_email(cfg, bot)),
    )
    activities = Activities(cfg, gh, mirror, AgentJobs(cfg), bot, client)
    task_queue = cfg.task_queue
    main_worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[IssueWorkflow, PollWorkflow],
        activities=activities.all(),
        max_concurrent_activities=8,
    )
    await_worker = Worker(
        client, task_queue=f"{task_queue}-await", activities=[activities.await_agent], max_concurrent_activities=20
    )
    task = asyncio.current_task()
    assert task is not None  # we are inside the worker coroutine
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    log.info(
        "worker up as %s: %s ns=%s queues %s, %s-await",
        bot,
        cfg.temporal.address,
        cfg.temporal.namespace,
        task_queue,
        task_queue,
    )
    try:
        async with main_worker, await_worker:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        log.info("worker shutting down")
    finally:
        await gh.aclose()
    return 0


async def apply_schedule(client: Any, schedule_id: str, schedule: Any) -> str:
    """Create the schedule, update it when it exists, or delete and recreate it when it exists but cannot be
    updated. Returns "created", "updated" or "recreated".

    The SDK raises ScheduleAlreadyRunningError only when the ALREADY_EXISTS answer carries the
    WorkflowExecutionAlreadyStartedFailure detail; newer servers (and the HTTPS proxy) answer a plain ALREADY_EXISTS
    ("schedule ... is already registered"), which is the same situation. Temporal runs each schedule as an internal
    workflow; when that workflow is wedged (a failed workflow task after a server upgrade, say) describe and update
    fail with "Unable to query workflow", and the only repair is a fresh schedule. A poll schedule holds no state
    worth keeping, so that is done here."""
    from temporalio.client import ScheduleAlreadyRunningError, ScheduleUpdate
    from temporalio.service import RPCError, RPCStatusCode

    try:
        await client.create_schedule(schedule_id, schedule)
        return "created"
    except ScheduleAlreadyRunningError:
        pass
    except RPCError as error:
        if error.status != RPCStatusCode.ALREADY_EXISTS:
            raise
    handle = client.get_schedule_handle(schedule_id)
    try:
        await handle.update(lambda _: ScheduleUpdate(schedule=schedule))
        return "updated"
    except RPCError as error:
        log.warning("schedule %s cannot be updated (%s); deleting and creating it again", schedule_id, error)
    await handle.delete()
    await client.create_schedule(schedule_id, schedule)
    return "recreated"


async def cmd_schedule(cfg: Config, args: argparse.Namespace) -> int:
    """Create or update the Temporal schedule that runs PollWorkflow every poll interval."""
    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleIntervalSpec,
        ScheduleOverlapPolicy,
        SchedulePolicy,
        ScheduleSpec,
    )

    from .workflows import PollWorkflow

    client = await _client(cfg)
    schedule_id = f"{cfg.deployment}-poll"
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            PollWorkflow.execute, id=f"{cfg.deployment}-poll-run", task_queue=cfg.task_queue
        ),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )
    print(f"{await apply_schedule(client, schedule_id, schedule)} schedule {schedule_id}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="autodev", description=__doc__)
    parser.add_argument("--config", help="path to autodev.yaml (default: $AUTODEV_CONFIG or ./autodev.yaml)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="verify GitHub identity, Temporal, csub policy and directories")
    sub.add_parser("init-labels", help="create the stage labels")
    sub.add_parser("worker", help="run the Temporal worker")
    schedule = sub.add_parser("schedule", help="manage the poll Schedule")
    schedule.add_argument("action", choices=["apply"])
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    cmd = {"check": cmd_check, "init-labels": cmd_init_labels, "worker": cmd_worker, "schedule": cmd_schedule}[args.cmd]
    return asyncio.run(cmd(cfg, args))


if __name__ == "__main__":
    sys.exit(main())
