"""The `mia-data` command line: health checks, seeding issues, labels, the worker, the poll schedule, a
synchronous crop test for snippet PRs and storage maintenance (deployments start and stop with
`pixi run mia`). Heavy imports happen inside the commands that need them.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
from collections.abc import Awaitable, Callable
from datetime import timedelta
from fnmatch import fnmatchcase
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from autodev.auth import bot_login, token_fn
from autodev.cli import _client, _setup_logging, apply_schedule, run_checks
from autodev.github import GitHub

from .config import EXTRA_LABELS, HUMAN_LABELS, STAGES, load_config

if TYPE_CHECKING:
    from .activities import Activities
    from .config import Config

log = logging.getLogger("mia_data")

# the catalog's data hosts the broker policy must allow
DATA_HOSTS = [
    "zenodo.org",
    "ftp.ebi.ac.uk",
    "www.ebi.ac.uk",
    "data.broadinstitute.org",
    "files.cryoetdataportal.cziscience.com",
    "huggingface.co",
    "*.huggingface.co",
    "ndownloader.figshare.com",
    "figshare.com",
    "allencell.s3.amazonaws.com",
    "s3.amazonaws.com",
]


def _lsf_queues() -> set[str] | None:
    """The cluster's LSF queue names, or None where bqueues is not installed (then the check is skipped)."""
    if not (bqueues := shutil.which("bqueues")):
        return None
    out = subprocess.run([bqueues, "-w"], capture_output=True, text=True, timeout=60).stdout.splitlines()
    return {line.split()[0] for line in out[1:] if line.strip()}


def _gh(cfg: Config, repo: str, tokens: Callable[[], str | None] | None = None) -> GitHub:
    return GitHub(repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)


def _wanted_hosts(cfg: Config) -> list[str]:
    """Every host a mia-data job may need: package indexes, redirect sources and targets, the data hosts."""
    network = cfg.network
    return [
        *network.package_hosts,
        *network.redirect_hosts,
        *(target for targets in network.redirect_hosts.values() for target in targets),
        *DATA_HOSTS,
    ]


def _csub_problems(cfg: Config, probe: Any, lsf_queues: set[str] | None) -> list[str]:
    """Everything in the broker policy (`csub probe`) that would stop mia-data's jobs: hosts, the agent, env
    vars, mounts, queues. Each message says what to change in broker.toml."""
    from .binding import missing_hosts
    from .jobspec import HARNESS_ENV

    problems = [f"host not allowed: {host}" for host in missing_hosts(_wanted_hosts(cfg), list(probe.allowed_hosts))]
    if "claude" not in probe.agents:
        problems.append(
            'agent "claude" not permitted (broker.toml: add [agents] claude = ["api.anthropic.com", '
            '"claude.ai", "platform.claude.com"])'
        )
    if missing := [var for var in HARNESS_ENV if not any(fnmatchcase(var, pattern) for pattern in probe.env_allow)]:
        problems.append(
            f'env not allowed: {", ".join(missing)} (broker.toml [broker]: add "MIA_HARNESS_*" to env_allow)'
        )
    problems += _mount_problems(cfg, probe)
    problems += _queue_problems(probe, lsf_queues)
    return problems


def _mount_problems(cfg: Config, probe: Any) -> list[str]:
    """The directories every job mounts must be under the policy's allowed_roots and away from $HOME, which
    csub never mounts."""
    mounted = [cfg.harness.dir, cfg.paths.work_root, cfg.paths.storage_root]
    problems = []
    if outside := [
        str(directory)
        for directory in mounted
        if not any(directory.is_relative_to(root) for root in probe.allowed_roots)
    ]:
        problems.append(
            f"outside allowed_roots: {', '.join(outside)} (broker.toml [broker]: add "
            f"{', '.join(json.dumps(directory) for directory in outside)} to allowed_roots)"
        )
    home = Path.home().resolve()
    if inside := [
        str(directory) for directory in mounted if directory.is_relative_to(home) or home.is_relative_to(directory)
    ]:
        problems.append(
            f"under or containing the home directory {home}, which csub never mounts: {', '.join(inside)} "
            "(move it out of $HOME)"
        )
    return problems


def _queue_problems(probe: Any, lsf_queues: set[str] | None) -> list[str]:
    """A policy queue table named after a queue LSF does not have fails every submit with "No such queue".
    Skipped (None) where bqueues is not installed."""
    if lsf_queues is None:
        return []
    if missing := [name for name, queue in probe.queues.items() if not queue.get("gpu") and name not in lsf_queues]:
        return [
            f"not an LSF queue: {', '.join(missing)} (broker.toml: name the CPU [queues.<name>] tables and "
            f"[limits] cpu_short_queue/cpu_default_queue after real queues from `bqueues`, e.g. short, local)"
        ]
    return []


async def cmd_check(cfg: Config, args: argparse.Namespace) -> int:
    """Try every external dependency and print one ok/BAD line per check."""
    from autodev.jobs import csub_client

    from .jobspec import harness_env, harness_pin

    def repo_check(repo: str) -> Callable[[], Awaitable[str]]:
        async def check():
            gh = _gh(cfg, repo)
            try:
                info = (await gh.request("GET", f"/repos/{repo}")).json()
            finally:
                await gh.aclose()
            return f"{info['full_name']} as {await bot_login(cfg)}"

        return check

    async def temporal():
        from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

        client = await _client(cfg)
        await client.workflow_service.describe_namespace(DescribeNamespaceRequest(namespace=cfg.temporal.namespace))
        return f"{cfg.temporal.address} ns={cfg.temporal.namespace}"

    async def csub():
        probe = await asyncio.to_thread(csub_client(cfg.deployment).probe)
        lsf_queues = await asyncio.to_thread(_lsf_queues)
        if bad := _csub_problems(cfg, probe, lsf_queues):
            raise RuntimeError("; ".join(bad))
        return f"broker user {probe.user}, {len(_wanted_hosts(cfg))} hosts ok"

    async def dirs():
        for directory in (cfg.paths.state_dir, cfg.paths.work_root, cfg.paths.storage_root):
            probe_file = directory / ".write-test"
            probe_file.touch()
            probe_file.unlink()
        return f"{cfg.paths.state_dir}, {cfg.paths.work_root}, {cfg.paths.storage_root}"

    async def pin():
        commit, digest = harness_pin(cfg)
        if not (python := harness_env(cfg)).is_file() or not os.access(python, os.X_OK):
            raise RuntimeError(
                f"{python} is missing or not executable: run `cd harness && pixi install` in harness.dir"
            )
        return f"{commit[:12]} env {digest[:12]}"

    repos = [
        (f"github {kind}", repo_check(getattr(cfg.github, f"{kind}_repo"))) for kind in ("data", "snippets", "catalog")
    ]
    return await run_checks(
        [*repos, ("temporal", temporal), ("csub broker", csub), ("directories", dirs), ("harness pin", pin)]
    )


async def cmd_crops_by_snippet(cfg: Config, args: argparse.Namespace) -> int:
    """List every published crop made with the named snippet, from the dataset manifests."""
    for manifest in sorted(cfg.paths.storage_root.glob("*/manifest.json")):
        for crop in json.loads(manifest.read_text())["crops"]:
            if any(snippet.get("name") == args.name for snippet in crop.get("snippets") or []):
                print(manifest.parent.name, crop["crop_id"], str(crop.get("withdrawn", False)).lower())
    return 0


async def cmd_reindex(cfg: Config, args: argparse.Namespace) -> int:
    """Rebuild manifest.json for one dataset or all of them, using the harness checkout's WITHDRAWN.yaml."""
    from .activities import withdrawn_snippets
    from .publish import reindex

    withdrawn = withdrawn_snippets(cfg.harness.dir)
    root = cfg.paths.storage_root
    datasets = [root / args.dataset] if args.dataset else sorted(path for path in root.iterdir() if path.is_dir())
    for dataset_dir in datasets:
        print(f"{dataset_dir.name}: {reindex(dataset_dir, withdrawn)} crop(s)")
    return 0


async def cmd_seed(cfg: Config, args: argparse.Namespace) -> int:
    from .seed import seed

    gh_data, gh_catalog = _gh(cfg, cfg.github.data_repo), _gh(cfg, cfg.github.catalog_repo)
    try:
        created = await seed(cfg, gh_data, gh_catalog, args.dry_run)
    finally:
        await gh_data.aclose()
        await gh_catalog.aclose()
    print(f"{'would create' if args.dry_run else 'created'} {len(created)} issue(s)")
    return 0


async def cmd_init_labels(cfg: Config, args: argparse.Namespace) -> int:
    gh = _gh(cfg, cfg.github.data_repo)
    labels = {**{cfg.label(kind): spec for kind, spec in {**STAGES, **HUMAN_LABELS}.items()}, **EXTRA_LABELS}
    try:
        for name, (color, description) in labels.items():
            if await gh.ensure_label(name, color, description):
                print(f"created {name}")
    finally:
        await gh.aclose()
    return 0


async def run_test_pr(activities: Activities, pr: int, binding_file: Path, record_id: str) -> int:
    """Run the test crops on a snippets PR's head with a binding from disk, synchronously, and post the
    `mia-data/crops` status and a result comment on the PR. Returns 0 if the crops passed.

    Used by `mia-data test-pr` for snippets developed by hand, outside a dataset round."""
    from . import catalog
    from .activities import visible_snippets, write_atomic
    from .binding import check_binding, https
    from .crop_test import judge, run_crop_test
    from .jobspec import SHA_RE, harness_pin
    from .models import Pins
    from .seed import ID_RE

    if not ID_RE.fullmatch(record_id):
        raise ValueError(f"bad record id {record_id!r}")
    cfg, gh_snippets, gh_catalog = activities.cfg, activities.gh_snippets, activities.gh_catalog
    head = (await gh_snippets.get_pr(pr))["head"]["sha"]
    round_id = f"i0-e{int(pr)}-{head[:7]}"
    round_dir = activities._round_dir(round_id)
    binding = yaml.safe_load(binding_file.read_text())
    arrays = [
        array
        for key in ("raw", "labels")
        if isinstance(binding, dict) and isinstance(binding.get(key), list)
        for array in binding[key]
    ]
    for array in arrays:
        if isinstance(array, dict) and isinstance(array.get("urls"), list):  # check_binding's s3:// -> https rewrite
            array["urls"] = [https(url) if isinstance(url, str) else url for url in array["urls"]]
    if isinstance(binding, dict):
        binding["snippets_commit"] = head  # the head under test, whatever the file says
    text = yaml.safe_dump(binding, sort_keys=False)
    if (round_dir / "binding.yaml").is_file() and (
        round_dir / "binding.yaml"
    ).read_text() != text:  # its jobs would be reused
        print(f"{head} was already tested with another binding ({round_dir}); push a new head to test this one")
        return 1  # the head's status stays as it is
    await activities.set_crop_status(head, "pending", None, "crop test running")
    try:
        entry = (await catalog.downloaded(gh_catalog)).get(record_id) or {}
        record_commit = str(entry.get("record_commit") or "")
        note = ""
        if not SHA_RE.fullmatch(record_commit):
            record_commit = await gh_catalog.get_ref_sha("main")
            note = (
                f" `{record_id}` is not in the downloads state: its record was read at the catalog's main, "
                f"`{record_commit}`."
            )
        if (path := (await catalog.record_paths(gh_catalog)).get(record_id)) is None:
            raise ValueError(f"{record_id}: no record on the catalog's main")
        record = await catalog.record(gh_catalog, path, record_commit)
        pins = Pins(record_id, record_commit, head, *harness_pin(cfg))
        if errors := check_binding(binding, record, set(visible_snippets(await activities._snapshot(head))), pins=pins):
            print("binding check failed:", *errors, sep="\n  ")
            await activities.set_crop_status(head, "failure", None, "binding check failed")
            return 1
        write_atomic(round_dir / "record.yaml", yaml.safe_dump(record, sort_keys=False))
        write_atomic(round_dir / "binding.yaml", text)
        write_atomic(round_dir / "pins.json", json.dumps(dataclasses.asdict(pins)))
        verify, gallery, jobs = await run_crop_test(
            activities,
            issue=0,
            round_id=round_id,
            pins=pins,
            sha=head,
            trusted=False,
            binding_path=str(round_dir / "binding.yaml"),
            seed=f"test-pr-{pr}-{head}",
        )
        passed, failed_jobs = judge(verify, jobs)
        summary = f"{verify.ok_count} of {verify.n} crops verified"
        body = "\n".join(
            [
                f"Crop test of `{head}` with `{record_id}`: **{'passed' if passed else 'failed'}**, {summary}; "
                f"the crop job fetched {verify.bytes_fetched} bytes.{note}",
                *(f"- {line}" for line in failed_jobs + verify.errors),
                "",
                gallery.table if gallery else "",
            ]
        )
        marker = cfg.marker(f"test-pr {head}")
        body = f"{activities._clean(body)}\n\n{marker}"
        if posted := await activities._bot_comment(
            gh_snippets, pr, marker
        ):  # a rerun: the comment shows the latest result
            if posted.get("body") != body:
                await gh_snippets.edit_comment(posted["id"], body)
        else:
            posted = await gh_snippets.comment(pr, body)
        url = posted.get("html_url")
        await activities.set_crop_status(head, "success" if passed else "failure", url, summary)
    except Exception as error:  # never leave the required status pending
        await activities.set_crop_status(head, "error", None, activities._clean(f"{type(error).__name__}: {error}"))
        raise
    print(f"PR #{pr} at {head}: {'passed' if passed else 'failed'}, {summary}: {url}")
    if (current := (await gh_snippets.get_pr(pr))["head"]["sha"]) != head:
        print(f"the PR head moved to {current} during the test; the status is on {head} only")
    return 0 if passed else 1


def _github_clients(cfg: Config) -> tuple[GitHub, GitHub, GitHub]:
    """Clients for the data, snippets and catalog repos, in the order Activities takes them."""
    tokens, github_cfg = token_fn(cfg), cfg.github
    return (
        _gh(cfg, github_cfg.data_repo, tokens),
        _gh(cfg, github_cfg.snippets_repo, tokens),
        _gh(cfg, github_cfg.catalog_repo, tokens),
    )


async def cmd_test_pr(cfg: Config, args: argparse.Namespace) -> int:
    from autodev.jobs import CsubJobs

    from .activities import Activities

    clients = _github_clients(cfg)
    try:
        activities = Activities(
            cfg, *clients, jobs=CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"), bot=await bot_login(cfg)
        )
        return await run_test_pr(activities, args.pr, Path(args.binding), args.record)
    finally:
        for gh in clients:
            await gh.aclose()


async def cmd_worker(cfg: Config, args: argparse.Namespace) -> int:
    """Run the Temporal worker until SIGTERM: one worker for workflows and short activities, one for the
    long-running job waits."""
    from temporalio.worker import Worker

    from autodev.jobs import CsubJobs

    from .activities import Activities
    from .workflows import MiaDataDataset, MiaDataPoll

    client = await _client(cfg)
    clients = _github_clients(cfg)
    bot = await bot_login(cfg)
    activities = Activities(
        cfg, *clients, jobs=CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs"), bot=bot, client=client
    )
    main_worker = Worker(
        client,
        task_queue=cfg.task_queue,
        workflows=[MiaDataDataset, MiaDataPoll],
        activities=activities.all(),
        max_concurrent_activities=8,
    )
    await_worker = Worker(
        client, task_queue=cfg.await_queue, activities=[activities.await_job], max_concurrent_activities=20
    )
    task = asyncio.current_task()
    assert task is not None  # we are inside the worker coroutine
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    log.info(
        "worker up as %s: %s ns=%s queues %s, %s",
        bot,
        cfg.temporal.address,
        cfg.temporal.namespace,
        cfg.task_queue,
        cfg.await_queue,
    )
    try:
        async with main_worker, await_worker:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        log.info("worker shutting down")
    finally:
        for gh in clients:
            await gh.aclose()
    return 0


async def cmd_schedule(cfg: Config, args: argparse.Namespace) -> int:
    """Create or update the Temporal schedule that runs MiaDataPoll every poll interval."""
    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleIntervalSpec,
        ScheduleOverlapPolicy,
        SchedulePolicy,
        ScheduleSpec,
    )

    from .workflows import MiaDataPoll

    client = await _client(cfg)
    schedule_id = f"{cfg.deployment}-poll"
    schedule = Schedule(
        action=ScheduleActionStartWorkflow(MiaDataPoll.run, id=f"{cfg.deployment}-poll-run", task_queue=cfg.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )
    print(f"{await apply_schedule(client, schedule_id, schedule)} schedule {schedule_id}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mia-data", description=__doc__)
    parser.add_argument("--config", help="path to mia_data.yaml (default: $MIA_DATA_CONFIG or ./mia_data.yaml)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="verify GitHub, Temporal, csub policy, directories and the harness pin")
    sub.add_parser("crops-by-snippet", help="list crops made with a snippet").add_argument("name")
    sub.add_parser("reindex", help="rebuild manifest.json with current WITHDRAWN marks").add_argument(
        "dataset", nargs="?"
    )
    sub.add_parser("seed", help="create one issue per downloaded dataset").add_argument(
        "--dry-run", action="store_true"
    )
    sub.add_parser("init-labels", help="create the stage, human and extra labels on the data repo")
    test_pr = sub.add_parser("test-pr", help="run the test crops on a snippets PR head and post mia-data/crops")
    test_pr.add_argument("pr", type=int)
    test_pr.add_argument("--binding", required=True, help="binding YAML to test (e.g. the PR's examples/<record>.yaml)")
    test_pr.add_argument("--record", required=True, help="catalog record id")
    sub.add_parser("worker", help="run the Temporal workers (main and await queues)")
    sub.add_parser("schedule", help="manage the poll Schedule").add_argument("action", choices=["apply"])
    args = parser.parse_args(argv)
    cfg = load_config(args.config)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    commands = {
        "check": cmd_check,
        "crops-by-snippet": cmd_crops_by_snippet,
        "reindex": cmd_reindex,
        "seed": cmd_seed,
        "init-labels": cmd_init_labels,
        "worker": cmd_worker,
        "test-pr": cmd_test_pr,
        "schedule": cmd_schedule,
    }
    return asyncio.run(commands[args.cmd](cfg, args))


if __name__ == "__main__":
    sys.exit(main())
