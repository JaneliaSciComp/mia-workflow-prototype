"""autodev command line."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

from . import ops
from .config import PROJECT_DIR, STAGES, load_config

OPS = {"up": ops.cmd_up, "down": ops.cmd_down, "restart": ops.cmd_restart, "ps": ops.cmd_ps,
       "attach": ops.cmd_attach, "logs": ops.cmd_logs}

log = logging.getLogger("autodev")


def _setup_logging(cfg, name: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if name:
        logs = cfg.paths.state_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(logs / f"{name}.log"))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def proxy_target(environ) -> str | None:
    url = environ.get("https_proxy") or environ.get("HTTPS_PROXY")
    return urlsplit(url).netloc if url else None


async def _client(cfg):
    from temporalio.client import Client
    from temporalio.service import HttpConnectProxyConfig

    proxy = None
    if cfg.temporal.via_https_proxy:  # inside agentic-sandbox the only way out is the allowlist proxy
        if not (target := proxy_target(os.environ)):
            sys.exit("autodev: temporal.via_https_proxy is set but $https_proxy is not (run under sandbox-run.sh --allow)")
        proxy = HttpConnectProxyConfig(target_host=target)
    return await asyncio.wait_for(Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace,
                                                 http_connect_proxy_config=proxy), 10)


def _csub_problems(cfg, p) -> list[str]:
    need = set(cfg.agent.allow_hosts) | {"api.anthropic.com", "claude.ai", "platform.claude.com"}
    work = cfg.paths.work_root.resolve()
    return [*([] if p.allow_claude else ["allow_claude is false"]),
            *(f"host not allowed: {h}" for h in sorted(need - set(p.allowed_hosts))),
            *([] if any(work.is_relative_to(Path(r).resolve()) for r in p.allowed_roots)
              else [f"work_root {work} not under allowed_roots {p.allowed_roots}"])]


async def cmd_check(cfg, args) -> int:
    from .agent import csub_client
    from .auth import bot_login
    from .github import GitHub

    rows: list[tuple[str, bool, str]] = []

    async def row(name, fn):
        try:
            rows.append((name, True, str(await fn())))
        except Exception as e:  # report every check, then fail
            rows.append((name, False, f"{type(e).__name__}: {e}"))

    gh = None

    async def repo():
        nonlocal gh
        gh = GitHub.for_autodev(cfg)
        r = (await gh.request("GET", f"/repos/{cfg.github.repo}")).json()
        return f"{r['full_name']} (default branch {r['default_branch']})"

    async def csub_ok():
        p = await asyncio.to_thread(csub_client(cfg).probe)
        problems = _csub_problems(cfg, p)
        if problems:
            raise RuntimeError("; ".join(problems))
        return f"broker user {p.user}, sandbox {p.sandbox}"

    async def temporal():
        c = await _client(cfg)
        return f"{cfg.temporal.address} ns={cfg.temporal.namespace} ({c.identity})"

    async def dirs():
        for d in (cfg.paths.state_dir, cfg.paths.work_root):
            d.mkdir(parents=True, exist_ok=True)
            (d / ".write-test").touch()
            (d / ".write-test").unlink()
        return f"{cfg.paths.state_dir}, {cfg.paths.work_root}"

    await row("github identity", lambda: bot_login(cfg))
    await row("github repo", repo)
    await row("temporal", temporal)
    await row("csub broker", csub_ok)
    await row("directories", dirs)
    if gh:
        await gh.aclose()
    for name, ok, detail in rows:
        print(f"{'ok ' if ok else 'BAD'}  {name:16} {detail}")
    return 0 if all(ok for _, ok, _ in rows) else 1


async def cmd_init_labels(cfg, args) -> int:
    from .github import GitHub

    gh = GitHub.for_autodev(cfg)
    for stage, (color, desc) in STAGES.items():
        if await gh.ensure_label(cfg.label(stage), color, desc):
            print(f"created {cfg.label(stage)}")
    await gh.aclose()
    return 0


async def cmd_worker(cfg, args) -> int:
    from temporalio.worker import Worker

    from .activities import Activities
    from .agent import AgentJobs
    from .auth import bot_login, token_fn
    from .github import GitHub
    from .gitops import Mirror
    from .workflows import IssueWorkflow, PollWorkflow

    client = await _client(cfg)
    tokens = token_fn(cfg)
    gh = GitHub.for_autodev(cfg, tokens)
    bot = await bot_login(cfg)
    mirror = Mirror(cfg.paths.state_dir / "mirror" / f"{cfg.repo_slug}.git", cfg.git_url, tokens)
    acts = Activities(cfg, gh, mirror, AgentJobs(cfg), bot, client)
    tq = cfg.task_queue
    main = Worker(client, task_queue=tq, workflows=[IssueWorkflow, PollWorkflow], activities=acts.all(),
                  max_concurrent_activities=8)
    waits = Worker(client, task_queue=f"{tq}-await", activities=[acts.await_agent], max_concurrent_activities=20)
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    log.info("worker up as %s: %s ns=%s queues %s, %s-await", bot, cfg.temporal.address, cfg.temporal.namespace, tq, tq)
    try:
        async with main, waits:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        log.info("worker shutting down")
    finally:
        await gh.aclose()
    return 0


async def cmd_schedule(cfg, args) -> int:
    from temporalio.client import (Schedule, ScheduleActionStartWorkflow, ScheduleAlreadyRunningError,
                                   ScheduleIntervalSpec, ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec,
                                   ScheduleUpdate)

    from .workflows import PollWorkflow

    client = await _client(cfg)
    sid = f"{cfg.deployment}-poll"
    sched = Schedule(
        action=ScheduleActionStartWorkflow(PollWorkflow.execute, id=f"{cfg.deployment}-poll-run", task_queue=cfg.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP))
    try:
        await client.create_schedule(sid, sched)
        print(f"created schedule {sid}")
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(sid).update(lambda _: ScheduleUpdate(schedule=sched))
        print(f"updated schedule {sid}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="autodev", description=__doc__)
    p.add_argument("--config", help="path to autodev.yaml (default: $AUTODEV_CONFIG or ./autodev.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="verify GitHub identity, Temporal, csub policy and directories")
    sub.add_parser("init-labels", help="create the stage labels")
    sub.add_parser("worker", help="run the Temporal worker")
    s = sub.add_parser("schedule", help="manage the poll Schedule")
    s.add_argument("action", choices=["apply"])
    sub.add_parser("up", help="start this deployment (tmux: Temporal, broker, worker)")
    sub.add_parser("down", help="stop this deployment's tmux session")
    sub.add_parser("restart", help="down, then up")
    sub.add_parser("ps", help="show whether this deployment is running")
    sub.add_parser("attach", help="attach to the deployment's tmux session")
    s = sub.add_parser("logs", help="tail a log (default: worker)")
    s.add_argument("name", nargs="?", default="worker", choices=["worker", "temporal", "broker"])
    s.add_argument("-n", "--lines", type=int, default=50)
    s.add_argument("--no-follow", action="store_true", help="print the last lines and exit")
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    if args.cmd in OPS:
        args.config_path = Path(args.config or os.environ.get("AUTODEV_CONFIG") or PROJECT_DIR / "autodev.yaml").resolve()
        return OPS[args.cmd](cfg, args)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    cmd = {"check": cmd_check, "init-labels": cmd_init_labels, "worker": cmd_worker, "schedule": cmd_schedule}[args.cmd]
    return asyncio.run(cmd(cfg, args))


if __name__ == "__main__":
    sys.exit(main())
