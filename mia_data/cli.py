"""mia-data command line (mirrors autodev/cli.py)."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import json
from datetime import timedelta

from autodev.auth import bot_login, token_fn
from autodev.cli import _client, _setup_logging
from autodev.github import GitHub

from .config import EXTRA_LABELS, HUMAN_LABELS, STAGES, load_config

log = logging.getLogger("mia_data")

# P3 (plan): the catalog's data hosts the broker policy must allow
DATA_HOSTS = ["zenodo.org", "ftp.ebi.ac.uk", "www.ebi.ac.uk", "data.broadinstitute.org",
              "files.cryoetdataportal.cziscience.com", "huggingface.co", "*.huggingface.co", "ndownloader.figshare.com",
              "figshare.com", "allencell.s3.amazonaws.com", "s3.amazonaws.com"]


def _gh(cfg, repo: str, tokens=None) -> GitHub:
    return GitHub(repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)


async def cmd_check(cfg, args) -> int:
    from autodev.jobs import csub_client

    from .binding import missing_hosts
    from .jobspec import harness_pin

    rows: list[tuple[str, bool, str]] = []

    async def row(name, fn):
        try:
            rows.append((name, True, str(await fn())))
        except Exception as e:  # report every check, then fail
            rows.append((name, False, f"{type(e).__name__}: {e}"))

    def repo(r):
        async def probe():
            gh = _gh(cfg, r)
            try:
                d = (await gh.request("GET", f"/repos/{r}")).json()
            finally:
                await gh.aclose()
            return f"{d['full_name']} as {await bot_login(cfg)}"
        return probe

    async def temporal():
        from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

        c = await _client(cfg)
        await c.workflow_service.describe_namespace(DescribeNamespaceRequest(namespace=cfg.temporal.namespace))
        return f"{cfg.temporal.address} ns={cfg.temporal.namespace}"

    async def csub():
        p = await asyncio.to_thread(csub_client(cfg.deployment).probe)
        n = cfg.network
        want = [*n.package_hosts, *n.redirect_hosts, *(x for v in n.redirect_hosts.values() for x in v), *DATA_HOSTS]
        bad = [f"host not allowed: {h}" for h in missing_hosts(want, list(p.allowed_hosts))]
        if not p.allow_claude:
            bad.append("allow_claude is false")
        if bad:
            raise RuntimeError("; ".join(bad))
        return f"broker user {p.user}, {len(want)} hosts ok"

    async def dirs():
        for d in (cfg.paths.state_dir, cfg.paths.work_root, cfg.storage.root):
            t = d / ".write-test"
            t.touch()
            t.unlink()
        return f"{cfg.paths.state_dir}, {cfg.paths.work_root}, {cfg.storage.root}"

    async def pin():
        c, digest = harness_pin(cfg)
        return f"{c[:12]} env {digest[:12]}"

    for kind in ("data", "snippets", "catalog"):
        await row(f"github {kind}", repo(getattr(cfg.github, f"{kind}_repo")))
    await row("temporal", temporal)
    await row("csub broker", csub)
    await row("directories", dirs)
    await row("harness pin", pin)
    for name, ok, detail in rows:
        print(f"{'ok ' if ok else 'BAD'}  {name:16} {detail}")
    return 0 if all(ok for _, ok, _ in rows) else 1


async def cmd_crops_by_snippet(cfg, args) -> int:
    for m in sorted(cfg.storage.root.glob("*/manifest.json")):
        for c in json.loads(m.read_text())["crops"]:
            if any(s.get("name") == args.name for s in c.get("snippets") or []):
                print(m.parent.name, c["crop_id"], str(c.get("withdrawn", False)).lower())
    return 0


async def cmd_reindex(cfg, args) -> int:
    from .activities import _withdrawn
    from .publish import reindex

    withdrawn = _withdrawn(cfg.harness.dir)
    root = cfg.storage.root
    for d in [root / args.dataset] if args.dataset else sorted(p for p in root.iterdir() if p.is_dir()):
        print(f"{d.name}: {reindex(d, withdrawn)} crop(s)")
    return 0


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


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="mia-data", description=__doc__)
    p.add_argument("--config", help="path to mia_data.yaml (default: $MIA_DATA_CONFIG or ./mia_data.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="verify GitHub, Temporal, csub policy, directories and the harness pin")
    sub.add_parser("crops-by-snippet", help="list crops made with a snippet").add_argument("name")
    sub.add_parser("reindex", help="rebuild manifest.json with current WITHDRAWN marks").add_argument("dataset", nargs="?")
    sub.add_parser("seed", help="create one issue per downloaded dataset").add_argument("--dry-run", action="store_true")
    sub.add_parser("init-labels", help="create the stage, human and extra labels on the data repo")
    sub.add_parser("worker", help="run the Temporal workers (main and await queues)")
    sub.add_parser("schedule", help="manage the poll Schedule").add_argument("action", choices=["apply"])
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    cmd = {"check": cmd_check, "crops-by-snippet": cmd_crops_by_snippet, "reindex": cmd_reindex, "seed": cmd_seed, "init-labels": cmd_init_labels, "worker": cmd_worker, "schedule": cmd_schedule}
    return asyncio.run(cmd[args.cmd](cfg, args))


if __name__ == "__main__":
    sys.exit(main())
