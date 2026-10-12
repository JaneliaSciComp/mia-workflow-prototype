"""mia-data command line (mirrors autodev/cli.py)."""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import os
import shutil
import signal
import subprocess
import sys
import json
from datetime import timedelta
from fnmatch import fnmatchcase
from pathlib import Path

import yaml

from autodev.auth import bot_login, token_fn
from autodev.cli import OPS, _client, _setup_logging
from autodev.github import GitHub

from .config import EXTRA_LABELS, HUMAN_LABELS, PROJECT_DIR, STAGES, load_config

log = logging.getLogger("mia_data")

# P3 (plan): the catalog's data hosts the broker policy must allow
DATA_HOSTS = ["zenodo.org", "ftp.ebi.ac.uk", "www.ebi.ac.uk", "data.broadinstitute.org",
              "files.cryoetdataportal.cziscience.com", "huggingface.co", "*.huggingface.co", "ndownloader.figshare.com",
              "figshare.com", "allencell.s3.amazonaws.com", "s3.amazonaws.com"]


def _lsf_queues() -> set[str] | None:
    """The cluster's LSF queue names, or None where bqueues is not installed (then the check is skipped)."""
    if not (bq := shutil.which("bqueues")):
        return None
    out = subprocess.run([bq, "-w"], capture_output=True, text=True, timeout=60).stdout.splitlines()
    return {l.split()[0] for l in out[1:] if l.strip()}


def _gh(cfg, repo: str, tokens=None) -> GitHub:
    return GitHub(repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)


async def cmd_check(cfg, args) -> int:
    from autodev.jobs import csub_client

    from .binding import missing_hosts
    from .jobspec import HARNESS_ENV, harness_bin, harness_pin

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
        if "claude" not in p.agents:
            bad.append('agent "claude" not permitted (broker.toml: add [agents] claude = ["api.anthropic.com", '
                       '"claude.ai", "platform.claude.com"])')
        if miss := [e for e in HARNESS_ENV if not any(fnmatchcase(e, pat) for pat in p.env_allow)]:
            bad.append(f"env not allowed: {', '.join(miss)} (broker.toml [broker]: add \"MIA_HARNESS_*\" to env_allow)")
        mounted = [cfg.harness.dir, cfg.paths.work_root, cfg.paths.storage_root]  # every job mounts under these
        if miss := [str(d) for d in mounted if not any(d.is_relative_to(r) for r in p.allowed_roots)]:
            bad.append(f"outside allowed_roots: {', '.join(miss)} (broker.toml [broker]: add "
                       f"{', '.join(json.dumps(d) for d in miss)} to allowed_roots)")
        home = Path.home().resolve()
        if inside := [str(d) for d in mounted if d.is_relative_to(home) or home.is_relative_to(d)]:
            bad.append(f"under or containing the home directory {home}, which csub never mounts: {', '.join(inside)} "
                       "(move it out of $HOME)")
        # a policy queue table named after a queue LSF does not have fails every submit with "No such queue"
        if (have := await asyncio.to_thread(_lsf_queues)) is not None:
            if miss := [n for n, q in p.queues.items() if not q.get("gpu") and n not in have]:
                bad.append(f"not an LSF queue: {', '.join(miss)} (broker.toml: name the CPU [queues.<name>] tables and "
                           f"[limits] cpu_short_queue/cpu_default_queue after real queues from `bqueues`, e.g. short, local)")
        if bad:
            raise RuntimeError("; ".join(bad))
        return f"broker user {p.user}, {len(want)} hosts ok"

    async def dirs():
        for d in (cfg.paths.state_dir, cfg.paths.work_root, cfg.paths.storage_root):
            t = d / ".write-test"
            t.touch()
            t.unlink()
        return f"{cfg.paths.state_dir}, {cfg.paths.work_root}, {cfg.paths.storage_root}"

    async def pin():
        c, digest = harness_pin(cfg)
        if not (b := harness_bin(cfg)).is_file() or not os.access(b, os.X_OK):
            raise RuntimeError(f"{b} is missing or not executable: run `cd harness && pixi install` in harness.dir")
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
    for m in sorted(cfg.paths.storage_root.glob("*/manifest.json")):
        for c in json.loads(m.read_text())["crops"]:
            if any(s.get("name") == args.name for s in c.get("snippets") or []):
                print(m.parent.name, c["crop_id"], str(c.get("withdrawn", False)).lower())
    return 0


async def cmd_reindex(cfg, args) -> int:
    from .activities import _withdrawn
    from .publish import reindex

    withdrawn = _withdrawn(cfg.harness.dir)
    root = cfg.paths.storage_root
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


async def run_test_pr(acts, pr: int, binding_file: Path, record_id: str) -> int:
    """§12.4: §7.4 on PR <pr>'s head in the snippets repo, synchronously, with `mia-data/crops` on that SHA."""
    from . import catalog
    from .activities import _catalog, _write
    from .binding import check_binding, https
    from .crop_test import judge, run_crop_test
    from .jobspec import SHA_RE, harness_pin
    from .models import Pins
    from .seed import ID_RE

    if not ID_RE.fullmatch(record_id):
        raise ValueError(f"bad record id {record_id!r}")
    cfg, gs, gc = acts.cfg, acts.gh_snippets, acts.gh_catalog
    head = (await gs.get_pr(pr))["head"]["sha"]
    rd = acts._round_dir(rid := f"i0-e{int(pr)}-{head[:7]}")
    b = yaml.safe_load(binding_file.read_text())
    for a in [x for k in ("raw", "labels") if isinstance(b, dict) and isinstance(b.get(k), list) for x in b[k]]:
        if isinstance(a, dict) and isinstance(a.get("urls"), list):  # check_binding's s3:// -> https rewrite
            a["urls"] = [https(u) if isinstance(u, str) else u for u in a["urls"]]
    if isinstance(b, dict):
        b["snippets_commit"] = head  # the head under test, whatever the file says
    text = yaml.safe_dump(b, sort_keys=False)
    if (rd / "binding.yaml").is_file() and (rd / "binding.yaml").read_text() != text:  # its jobs would be reused
        print(f"{head} was already tested with another binding ({rd}); push a new head to test this one")
        return 1  # the head's status stays as it is
    await acts.set_crop_status(head, "pending", None, "crop test running")
    try:
        rc, note = ((await catalog.downloaded(gc)).get(record_id) or {}).get("record_commit"), ""
        if not SHA_RE.fullmatch(str(rc)):
            rc = await gc.get_ref_sha("main")
            note = f" `{record_id}` is not in the downloads state: its record was read at the catalog's main, `{rc}`."
        if (path := (await catalog.record_paths(gc)).get(record_id)) is None:
            raise ValueError(f"{record_id}: no record on the catalog's main")
        rec = await catalog.record(gc, path, rc)
        pins = Pins(record_id, rc, head, *harness_pin(cfg))
        if errs := check_binding(b, rec, set(_catalog(await acts._snapshot(head))), pins=pins):
            print("binding check failed:", *errs, sep="\n  ")
            await acts.set_crop_status(head, "failure", None, "binding check failed")
            return 1
        _write(rd / "record.yaml", yaml.safe_dump(rec, sort_keys=False))
        _write(rd / "binding.yaml", text)
        _write(rd / "pins.json", json.dumps(dataclasses.asdict(pins)))
        v, g, jobs = await run_crop_test(acts, issue=0, round_id=rid, pins=pins, sha=head, trusted=False,
                                         binding_path=str(rd / "binding.yaml"), seed=f"test-pr-{pr}-{head}")
        passed, bad = judge(v, jobs)
        summary = f"{v.ok_count} of {v.n} crops verified"
        body = "\n".join([f"Crop test of `{head}` with `{record_id}`: **{'passed' if passed else 'failed'}**, {summary}; "
                          f"the crop job fetched {v.bytes_fetched} bytes.{note}", *(f"- {x}" for x in bad + v.errors),
                          "", g.table if g else ""])
        m = cfg.marker(f"test-pr {head}")
        body = f"{acts._clean(body)}\n\n{m}"
        if c := await acts._bot_comment(gs, pr, m):  # a rerun: the comment shows the latest result
            if c.get("body") != body:
                await gs.edit_comment(c["id"], body)
        else:
            c = await gs.comment(pr, body)
        url = c.get("html_url")
        await acts.set_crop_status(head, "success" if passed else "failure", url, summary)
    except Exception as e:  # never leave the required status pending
        await acts.set_crop_status(head, "error", None, acts._clean(f"{type(e).__name__}: {e}"))
        raise
    print(f"PR #{pr} at {head}: {'passed' if passed else 'failed'}, {summary}: {url}")
    if (now := (await gs.get_pr(pr))["head"]["sha"]) != head:
        print(f"the PR head moved to {now} during the test; the status is on {head} only")
    return 0 if passed else 1


async def cmd_test_pr(cfg, args) -> int:
    from autodev.jobs import CsubJobs

    from .activities import Activities

    tokens, g = token_fn(cfg), cfg.github
    ghs = [_gh(cfg, r, tokens) for r in (g.data_repo, g.snippets_repo, g.catalog_repo)]
    try:
        acts = Activities(cfg, *ghs, CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"), await bot_login(cfg))
        return await run_test_pr(acts, args.pr, Path(args.binding), args.record)
    finally:
        for gh in ghs:
            await gh.aclose()


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
    t = sub.add_parser("test-pr", help="run the test crops on a snippets PR head and post mia-data/crops")
    t.add_argument("pr", type=int)
    t.add_argument("--binding", required=True, help="binding YAML to test (e.g. the PR's examples/<record>.yaml)")
    t.add_argument("--record", required=True, help="catalog record id")
    sub.add_parser("worker", help="run the Temporal workers (main and await queues)")
    sub.add_parser("schedule", help="manage the poll Schedule").add_argument("action", choices=["apply"])
    sub.add_parser("up", help="start this deployment (tmux: broker, worker; Temporal must already run)")
    sub.add_parser("down", help="stop this deployment's tmux session")
    sub.add_parser("restart", help="down, then up")
    sub.add_parser("ps", help="show whether this deployment is running")
    sub.add_parser("attach", help="attach to the deployment's tmux session")
    s = sub.add_parser("logs", help="tail a log (default: worker)")
    s.add_argument("name", nargs="?", default="worker", choices=["worker", "broker"])
    s.add_argument("-n", "--lines", type=int, default=50)
    s.add_argument("-f", "--follow", action="store_true", help="keep following the log")
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    if args.cmd in OPS:
        args.config_path = Path(args.config or os.environ.get("MIA_DATA_CONFIG") or PROJECT_DIR / "mia_data.yaml").resolve()
        args.up_script, args.config_env = PROJECT_DIR / "scripts" / "mia-data-up.sh", "MIA_DATA_CONFIG"
        return OPS[args.cmd](cfg, args)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    cmd = {"check": cmd_check, "crops-by-snippet": cmd_crops_by_snippet, "reindex": cmd_reindex, "seed": cmd_seed, "init-labels": cmd_init_labels, "worker": cmd_worker, "test-pr": cmd_test_pr, "schedule": cmd_schedule}
    return asyncio.run(cmd[args.cmd](cfg, args))


if __name__ == "__main__":
    sys.exit(main())
