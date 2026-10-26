"""The mia-buffers Temporal activities: the only code that touches GitHub, csub, buffer-api and the filesystem.

The worker keeps each round's trusted inputs (spec, pins, plan, checked bindings) under state_dir/rounds/<round>,
never mounted into a job. A stage job gets copies in its workspace under work_root and writes into the raw cache
and the staged directory; everything it wrote is read back with `read_untrusted` and reduced to the small
dataclasses in models.py before it enters workflow history.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import os
import re
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

import yaml
from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.activities import PERMANENT_CSUB
from autodev.jobs import JobSpec
from mia_core.registry import Registry, RegistryError
from mia_core.untrusted import loads, neutralize, read_untrusted, text, texts, write_atomic
from mia_data.activities import mia_permanent_errors
from mia_data.binding import hosts as job_hosts
from mia_data.binding import missing_hosts
from mia_data.catalog import dataset_folder
from mia_data.jobspec import SHA_RE, check_hosts_allowed, harness_pin, harness_snapshot, snapshot
from mia_data.models import JobDone
from mia_data.publish import prepared_row
from mia_data.seed import ID_RE, MARKER, render_issue
from mia_data.status import render_links, render_stage

from .config import HUMAN_LABELS, STAGES
from .drop import cache_key, drop_names, merge_manifest, read_manifest
from .jobspec import artifact_id, catalog_snapshot, levels_key, stage_job
from .models import (
    BindingInfo,
    BufferPins,
    BufferState,
    DatasetIssue,
    DatasetPlan,
    DropResult,
    PlanResult,
    RegisterResult,
    SourcingLimits,
    StageJobArgs,
    StageResult,
)
from .plan import MANUAL_DROP_NOTE, Row, _group_key, build_plan, load_records
from .poll import run_poll
from .register import buffer_config, volume_bodies
from .spec import SpecError, load_spec
from .status import render_drop_comment, render_plan_comment

if TYPE_CHECKING:
    from temporalio.client import Client

    from autodev.github import GitHub
    from autodev.jobs import CsubJobs

    from .config import Config

ROUND_RE = re.compile(r"i\d+-e\d+")
SPEC_FENCE = re.compile(r"^```ya?ml[ \t]*\n(.*?)^```[ \t]*$", re.MULTILINE | re.DOTALL)
MANIFEST_SOURCE_MANUAL = "manual"
STAGE_JSON_LIMIT = 64 << 20  # stage.json carries a 4,096-value sample per channel per volume: ~150 KB per RGB volume
DATA_LABEL = "data:{}"  # mia-data's labels on a dataset issue (mia_data.config.Config.label)
DATA_GO = DATA_LABEL.format("go")
DATA_IN_PROGRESS = {"matching", "developing", "testing", "pr-ready", "review", "cropping"}


def _voxel(row: Row) -> dict[str, float]:
    """The stage job's voxel size: the plan's per-axis nm, plus 1 nm for the plane axis a 2D stack keeps (its fields
    of view, not a physical z; the harness has no other source for it and refuses to write a pyramid without)."""
    known = {axis: float(value) for axis, value in row.voxel_size_nm.items() if value is not None}
    return {"z": 1.0, **known} if row.dims == "2D" else known


class Activities:
    """The activity implementations, bound to the GitHub clients (issue, snippets, catalog and mia-data's
    datasets repos), the csub job runner, buffer-api's registry (from the config's `service`) and the Temporal
    client (for the poller)."""

    def __init__(
        self,
        cfg: Config,
        gh_issues: GitHub,
        gh_snippets: GitHub,
        gh_catalog: GitHub,
        jobs: CsubJobs | None,
        bot: str,
        client: Client | None = None,
        probe: Callable[[], list[str]] | None = None,
        gh_datasets: GitHub | None = None,
    ):
        self.cfg, self.gh_issues, self.gh_snippets, self.gh_catalog = cfg, gh_issues, gh_snippets, gh_catalog
        self.jobs, self.bot, self.client = jobs, bot, client
        self._gh_datasets = gh_datasets
        self.registry = Registry.from_config(cfg.service)  # synchronous: call it through asyncio.to_thread
        self.probe = probe or (lambda: list(jobs.client.probe().allowed_hosts) if jobs else [])
        self._dropped: dict[tuple[int, str], tuple] = {}  # poll: (issue, record) -> incoming/ listing last signalled
        self._seen_listing: dict[tuple[int, str], tuple] = {}  # poll: the listing seen on the previous pass
        self._signalled: set[tuple[int, str, str]] = set()  # poll: (issue, record, outcome) dataset updates sent

    @property
    def gh_data(self) -> GitHub:
        """mia-data's name for the issue repo client (mia_data.poll.deliver_label posts through it)."""
        return self.gh_issues

    @property
    def gh_datasets(self) -> GitHub:
        """The client for mia-data's issue repo (`github.datasets_repo`)."""
        if self._gh_datasets is None:
            raise RuntimeError("no datasets-repo client: this activity runs only inside the worker")
        return self._gh_datasets

    @gh_datasets.setter
    def gh_datasets(self, gh: GitHub) -> None:
        self._gh_datasets = gh

    @property
    def temporal(self) -> Client:
        if self.client is None:
            raise RuntimeError("no Temporal client: this activity runs only inside the worker")
        return self.client

    def all(self) -> list:
        """The main queue's activities; await_job runs on the -await queue."""
        return [
            self.admit,
            self.plan,
            self.limits,
            self.set_stage,
            self.comment,
            self.comment_plan,
            self.refuse_label,
            self.read_binding,
            self.prepare_stage,
            self.submit,
            self.read_stage,
            self.verify_drop,
            self.create_buffer,
            self.register_dataset,
            self.comment_drop,
            self.nudge_dataset,
            self.poll,
        ]

    # --- helpers -----------------------------------------------------------------------------

    def _clean(self, text: str) -> str:
        return neutralize(text, self.gh_issues.tokens())

    async def _comment_once(self, gh: GitHub, number: int, body: str, marker: str) -> str | None:
        """Post unless the bot already commented with this marker (an outsider's copy does not count)."""
        for comment in await gh.issue_comments(number):
            if (comment.get("user") or {}).get("login") == self.bot and marker in (comment.get("body") or ""):
                return comment.get("html_url")
        return (await gh.comment(number, body)).get("html_url")

    def _round_dir(self, round_id: str) -> Path:
        if not ROUND_RE.fullmatch(round_id):
            raise ValueError(f"bad round id {round_id!r}")
        return self.cfg.paths.state_dir / "rounds" / round_id

    def _round_workspace(self, issue: int, round_id: str) -> Path:
        return self.cfg.paths.work_root / f"issue-{int(issue)}" / self._round_dir(round_id).name

    def _pins(self, round_id: str) -> BufferPins:
        return BufferPins(**json.loads((self._round_dir(round_id) / "pins.json").read_text()))

    def _spec(self, round_id: str):
        return load_spec(self._round_dir(round_id) / "spec.yaml")

    def _plan(self, round_id: str) -> dict:
        return json.loads((self._round_dir(round_id) / "plan.json").read_text())

    def _row(self, round_id: str, record: str) -> dict:
        if not ID_RE.fullmatch(record):
            raise ValueError(f"bad record id {record!r}")
        row = next((row for row in self._plan(round_id)["rows"] if row["id"] == record), None)
        if row is None:
            raise ValueError(f"{record} is not in round {round_id}'s plan")
        return row

    def _record(self, round_id: str, record: str) -> dict:
        """The catalog record, from the round's catalog snapshot (the path the plan saw it at)."""
        path = (
            self.cfg.paths.work_root
            / "_catalog"
            / self._pins(round_id).catalog_ref
            / self._row(round_id, record)["path"]
        )
        return yaml.safe_load(path.read_text()) or {}

    def _binding_path(self, round_id: str, record: str) -> Path:
        return self._round_dir(round_id) / "bindings" / f"{record}.yaml"

    def _levels_key(self, round_id: str) -> str:
        spec = self._spec(round_id)
        patch = (spec.patch_for("2D") or {}).get("y") or 0
        return levels_key(int(patch), spec.sampling.levels)

    def _artifact(self, round_id: str, record: str) -> str:
        """The record's staged artifact id: the round's binding copy, the levels key, the plan row's voxel size
        (the value the stage job is given) and the round's harness pins."""
        binding = self._binding_path(round_id, record)
        if not binding.is_file():
            raise ValueError(f"{record}: no binding copied into round {round_id}")
        voxel = self._row(round_id, record).get("voxel_size_nm") or {}
        pins = self._pins(round_id)
        return artifact_id(
            hashlib.sha256(binding.read_bytes()).hexdigest(),
            self._levels_key(round_id),
            {axis: size for axis, size in voxel.items() if size is not None},  # the plan keeps unknown axes as None
            pins.harness_commit,
            pins.env_digest,
        )

    def _staged_dir(self, round_id: str, record: str) -> Path:
        """<staged_root>/<levels key>/<record>/<artifact id>: one immutable directory per distinct pyramid."""
        return self.cfg.paths.staged_root / self._levels_key(round_id) / record / self._artifact(round_id, record)

    def _cache_dir(self, record: str) -> Path:
        if not ID_RE.fullmatch(record):
            raise ValueError(f"bad record id {record!r}")
        return self.cfg.paths.raw_root / record

    # --- admission and planning --------------------------------------------------------------

    @activity.defn(name="admit")
    @mia_permanent_errors
    async def admit(self, issue: int, event_id: str) -> BufferPins:
        """Pin the round: the spec from the issue body's one ```yaml fence, the catalog at the spec's ref and
        the harness checkout. Written once to state_dir/rounds/<round>/; a retry returns the same pins."""
        if not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"bad event id {event_id!r}")
        round_dir = self._round_dir(f"i{int(issue)}-e{event_id}")
        if (round_dir / "pins.json").is_file():
            return self._pins(round_dir.name)
        body = ((await self.gh_issues.get_issue(issue)).get("body") or "").replace("\r\n", "\n")  # GitHub saves CRLF
        fences = SPEC_FENCE.findall(body)
        if len(fences) != 1:
            raise ValueError(
                f"issue #{issue}: expected exactly one ```yaml block holding the buffer spec, found {len(fences)}"
            )
        round_dir.mkdir(parents=True, exist_ok=True)
        spec_path = round_dir / "spec.yaml"
        write_atomic(spec_path, fences[0])
        try:
            spec = load_spec(spec_path)
        except SpecError as error:
            raise ValueError(f"spec refused: {error}") from error
        await asyncio.to_thread(catalog_snapshot, self.cfg, spec.catalog.ref, self.gh_catalog.tokens)
        pins = BufferPins(
            hashlib.sha256(spec_path.read_bytes()).hexdigest(),
            spec.catalog.ref,
            *await asyncio.to_thread(harness_pin, self.cfg),
        )
        await asyncio.to_thread(harness_snapshot, self.cfg, pins.harness_commit)  # freeze the source now
        write_atomic(round_dir / "pins.json", json.dumps(dataclasses.asdict(pins)))  # last: marks the round admitted
        return pins

    @activity.defn(name="plan")
    @mia_permanent_errors
    async def plan(self, issue: int, round_id: str, pins: BufferPins) -> PlanResult:
        """Build the plan over the catalog snapshot; plan.json and plan.md go to the round dir and to the round's
        browsable workspace. Returns the summary the workflow needs."""
        spec = self._spec(round_id)
        snapshot_dir = self.cfg.paths.work_root / "_catalog" / pins.catalog_ref
        plan = await asyncio.to_thread(
            build_plan, spec, pins.spec_hash, pins.catalog_ref, await asyncio.to_thread(load_records, snapshot_dir)
        )
        tensorizer = self.cfg.sourcing.tensorizer
        if not (self.cfg.harness.dir / "tensorizers" / tensorizer / "tensorizer.yaml").is_file():
            plan.errors.append(f"no tensorizer {tensorizer} in the harness checkout")
        plan_json, plan_md = json.dumps(plan.to_dict(), indent=1), plan.render_markdown()
        workspace = self._round_workspace(issue, round_id)
        workspace.mkdir(parents=True, exist_ok=True)
        for directory in (self._round_dir(round_id), workspace):
            write_atomic(directory / "plan.json", plan_json)
            write_atomic(directory / "plan.md", plan_md)
        over = spec.sampling.weighting.over
        pool = [
            DatasetPlan(row.id, _group_key(row, over[0]), row.weight, MANUAL_DROP_NOTE in row.notes, _voxel(row))
            for row in plan.rows
            if row.in_pool
        ]
        patch = (spec.patch_for("2D") or {}).get("y") or 0
        return PlanResult(
            plan.valid,
            plan.errors[:20],
            pool,
            plan.n_seen,
            spec.ready.min_fraction,
            spec.sourcing.dataset_timeout_days,
            list(spec.sampling.levels),
            int(patch),
        )

    @activity.defn(name="limits")
    async def limits(self) -> SourcingLimits:
        cfg = self.cfg
        return SourcingLimits(cfg.sourcing.parallel, cfg.limits.job_walltime_min, cfg.service.url)

    # --- the issue ---------------------------------------------------------------------------

    @activity.defn(name="set_stage")
    @mia_permanent_errors
    async def set_stage(
        self, issue: int, stage: str, state: BufferState | None = None, detail: str = "", extra: str = ""
    ) -> None:
        """Swap the `buffer:<stage>` label and, within a round, post the stage's comment once."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}")
        gh, want = self.gh_issues, self.cfg.label(stage)
        current = {label["name"] for label in (await gh.get_issue(issue)).get("labels", [])}
        for other in STAGES:
            if (name := self.cfg.label(other)) in current and name != want:
                await gh.remove_label(issue, name)
        if want not in current:
            await gh.add_labels(issue, [want])
        if not (state and state.round_id):
            return
        paths = self.cfg.paths
        links = {"Workspace": paths.link(self._round_workspace(issue, state.round_id))}
        if state.levels and state.patch:
            links["Staged"] = paths.link(paths.staged_root / levels_key(state.patch, state.levels))
        marker = self.cfg.marker(f"stage {state.round_id} {stage}")
        parts = [
            render_stage(stage, self._clean(detail), prefix="buffer"),
            self._clean(extra) if extra else "",
            render_links(links),
        ]
        await self._comment_once(
            self.gh_issues, issue, "\n".join(part for part in parts if part) + f"\n{marker}", marker
        )

    @activity.defn(name="comment")
    @mia_permanent_errors
    async def comment(self, issue: int, body: str, marker: str) -> str | None:
        """Post once per marker. Returns the comment's URL."""
        full_marker = self.cfg.marker(marker)
        return await self._comment_once(self.gh_issues, issue, f"{self._clean(body)}\n\n{full_marker}", full_marker)

    @activity.defn(name="comment_plan")
    @mia_permanent_errors
    async def comment_plan(self, issue: int, round_id: str, dropped: dict[str, str] | None = None) -> None:
        """Post the round's plan (errors, warnings, the pool) once, linking the browsable plan files; with
        `dropped` (record -> reason), re-post it without those datasets, once per distinct dropped set."""
        suffix = ""
        if dropped:
            suffix = " minus " + hashlib.sha256("\n".join(sorted(dropped)).encode()).hexdigest()[:8]
        marker = self.cfg.marker(f"plan {round_id}{suffix}")
        link = self.cfg.paths.link(self._round_workspace(issue, round_id))
        body = render_plan_comment(self._plan(round_id), link, dropped)
        await self._comment_once(self.gh_issues, issue, f"{self._clean(body)}\n{marker}", marker)

    @activity.defn(name="nudge_dataset")
    @mia_permanent_errors
    async def nudge_dataset(self, round_id: str, record: str) -> DatasetIssue:
        """Hand a dataset without a binding to mia-data (spec §3 step 1): find its issue in the datasets repo by
        the record marker, open one if there is none (mia-data's own issue body, pinned to the round's catalog
        ref), and add `data:go` unless mia-data is already working on it, has finished, or found it ineligible."""
        gh, record_yaml = self.gh_datasets, self._record(round_id, record)
        folder = dataset_folder(record_yaml, record)
        issue = await gh.find_issue_by_marker(MARKER.format(record))
        if issue is None:
            title, body, labels = render_issue(
                record_yaml,
                {"record_commit": self._pins(round_id).catalog_ref},
                self.cfg.crops.licences,
                self.cfg.github.catalog_repo,
                self._row(round_id, record)["path"],
                record,
            )
            issue = await gh.create_issue(title, body, labels)
        number = int(issue["number"])
        names = {label["name"] for label in issue.get("labels") or []}
        stages = [name[len("data:") :] for name in names if name.startswith("data:")]
        stages = [stage for stage in stages if stage not in ("go", "approve")]  # human labels are not stages
        stage = stages[0] if stages else "new"
        if stage == "ineligible" or "licence-excluded" in names:
            return DatasetIssue(number, "ineligible", False, folder=folder)
        if stage == "done":
            path = self.cfg.paths.storage_root / folder
            note = (
                f"the dataset issue is `data:done` but `{path}` has no binding.yaml with a matching manifest.json: "
                "run the dataset again (add `data:go` on it) so mia-data publishes both"
            )
            return DatasetIssue(number, stage, False, note, folder)
        if DATA_GO in names or stage in DATA_IN_PROGRESS:
            return DatasetIssue(number, stage, False, folder=folder)
        await gh.add_labels(number, [DATA_GO])
        return DatasetIssue(number, stage, True, folder=folder)

    @activity.defn(name="comment_drop")
    @mia_permanent_errors
    async def comment_drop(self, issue: int, round_id: str, record: str, missing: list[str]) -> None:
        """Ask for a dataset's files (once), or list the ones still missing after a failed verification (once
        per distinct missing set)."""
        binding = yaml.safe_load(self._binding_path(round_id, record).read_text()) or {}
        urls = [
            str(url)
            for array in (binding.get("raw") or []) + (binding.get("labels") or [])
            for url in (array.get("urls") or [])
        ]
        files = [list(pair) for pair in drop_names(urls)]
        link = self.cfg.paths.link(self._cache_dir(record) / "incoming")
        suffix = " " + hashlib.sha256("\n".join(sorted(missing)).encode()).hexdigest()[:8] if missing else ""
        marker = self.cfg.marker(f"drop {round_id} {record}{suffix}")
        body = render_drop_comment(record, link, files, list(missing))
        await self._comment_once(self.gh_issues, issue, f"{self._clean(body)}\n{marker}", marker)

    @activity.defn(name="poll")
    async def poll(self) -> list[str]:
        return await run_poll(self)

    @activity.defn(name="refuse_label")
    @mia_permanent_errors
    async def refuse_label(self, issue: int, label: str, reason: str, event_id: str) -> None:
        """Remove a human label we won't act on and say why, once per event."""
        if label not in {self.cfg.label(kind) for kind in HUMAN_LABELS} or not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"refuse_label: bad label/event {label!r}/{event_id!r}")
        await self.gh_issues.remove_label(issue, label)
        marker = self.cfg.marker(f"refused {label} {event_id}")
        await self._comment_once(
            self.gh_issues, issue, f"Ignoring `{label}`: {self._clean(reason)}\n\n{marker}", marker
        )

    # --- sourcing: binding, drop, stage job, registration ------------------------------------

    @activity.defn(name="read_binding")
    @mia_permanent_errors
    async def read_binding(self, round_id: str, record: str) -> BindingInfo:
        """The dataset's approved binding from <storage_root>/<dataset folder>/binding.yaml (mia-data's output,
        trusted; prepared means a manifest.json published for that binding), copied into the round dir. Hosts the
        stage job needs, or the files a human must drop when a host is not allowed to jobs or a URL is manual://."""
        try:
            folder = dataset_folder(self._record(round_id, record), record)
        except ValueError as error:
            return BindingInfo(False, errors=[str(error)])
        dataset_dir = self.cfg.paths.storage_root / folder
        source = dataset_dir / "binding.yaml"
        if (row := prepared_row(dataset_dir)) is None:  # spec §4.2: the registry's rule, a hand-placed binding is not
            return BindingInfo(
                False,
                errors=[
                    f"no prepared dataset at {dataset_dir}: mia-data publishes binding.yaml with a matching manifest.json"
                    " (the dataset is not data:done)"
                ],
            )
        if row["record"] != record:
            return BindingInfo(False, errors=[f"{source} names record {row['record']!r}, not {record}"])
        binding = yaml.safe_load(source.read_text())
        urls = [
            str(url)
            for array in (binding.get("raw") or []) + (binding.get("labels") or [])
            for url in (array.get("urls") or [])
        ]
        dest = self._binding_path(round_id, record)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, dest)
        digest = hashlib.sha256(dest.read_bytes()).hexdigest()
        url_hosts = sorted({host for url in urls if (host := (urlsplit(url).hostname or "").lower())})
        manual = any(url.startswith("manual://") for url in urls) or bool(
            missing_hosts(url_hosts, await asyncio.to_thread(self.probe))
        )
        staged, artifact = await self._staged_before(round_id, record)
        if manual:
            return BindingInfo(
                True, str(dest), digest, [], [list(pair) for pair in drop_names(urls)], staged=staged, artifact=artifact
            )
        return BindingInfo(True, str(dest), digest, job_hosts(urls, self.cfg), staged=staged, artifact=artifact)

    async def _staged_before(self, round_id: str, record: str) -> tuple[bool, str]:
        """(found, artifact id): found when buffer-api's registry holds an `ok` pyramid under this artifact id
        and its stage.json is still on disk. Buffer-api unreachable or refusing: not found (stage again)."""
        artifact = self._artifact(round_id, record)
        try:
            data = await asyncio.to_thread(self.registry.asset, "staged", artifact)
        except RegistryError as error:
            activity.logger.warning("%s: registry unavailable, staging again: %s", record, error)
            return False, artifact
        found = isinstance(data, dict) and data.get("ok") is True
        return found and (self._staged_dir(round_id, record) / "stage.json").is_file(), artifact

    @activity.defn(name="prepare_stage")
    @mia_permanent_errors
    async def prepare_stage(self, issue: int, round_id: str, record: str) -> str:
        """The stage job's workspace with record.yaml and binding.yaml; the raw cache and staged directories
        exist afterwards (a mount must). Returns the workspace path."""
        workspace = self._round_workspace(issue, round_id) / f"stage-{record}"
        workspace.mkdir(parents=True, exist_ok=True)
        write_atomic(workspace / "record.yaml", yaml.safe_dump(self._record(round_id, record), sort_keys=False))
        shutil.copyfile(self._binding_path(round_id, record), workspace / "binding.yaml")
        self._cache_dir(record).mkdir(parents=True, exist_ok=True)
        self._staged_dir(round_id, record).mkdir(parents=True, exist_ok=True)
        return str(workspace)

    @activity.defn(name="submit")
    @mia_permanent_errors
    async def submit(self, key: str, args: StageJobArgs) -> str:
        """Build the stage job (snippets at the binding's commit, the staged directory as `out`), refuse hosts a
        fresh `csub probe` doesn't allow, and submit at most once per key. Returns the job id."""
        from csub import CsubError  # pixi-only dependency, imported where used (as autodev does)

        if self.jobs is None:
            raise RuntimeError("no job runner: submit runs only inside the worker")
        binding = yaml.safe_load(Path(args.workspace, "binding.yaml").read_text()) or {}
        sha = str(binding.get("snippets_commit") or "")
        if not SHA_RE.fullmatch(sha):
            raise ValueError(f"binding of {args.record} has no snippets_commit")
        args.out = str(self._staged_dir(args.round_id, args.record))
        patterns = await asyncio.to_thread(self.probe)
        snapshot_dir = await asyncio.to_thread(snapshot, self.cfg, sha, self.gh_snippets.tokens)

        def build() -> JobSpec:
            spec = stage_job(self.cfg, args, snapshot_dir)
            check_hosts_allowed(spec, patterns)
            return spec

        try:
            return await self.jobs.submit(key, build)
        except CsubError as error:
            if error.code in PERMANENT_CSUB:
                raise ApplicationError(str(error), non_retryable=True) from error
            raise

    @activity.defn(name="await_job")
    async def await_job(self, job_id: str, workspace: str) -> JobDone:
        if self.jobs is None:
            raise RuntimeError("no job runner: await_job runs only inside the worker")
        outcome = await self.jobs.wait(job_id, workspace, activity.heartbeat)
        return JobDone(outcome.job_id, outcome.state, outcome.exit_code)

    @activity.defn(name="read_stage")
    @mia_permanent_errors
    async def read_stage(self, round_id: str, record: str) -> StageResult:
        """The stage job's stage.json, reduced; tolerant of any shape."""
        report = _stage_json(self._staged_dir(round_id, record))
        if not isinstance(report, dict):
            return StageResult(False, errors=["stage.json is missing, unreadable or not a JSON object"])
        fetched = report.get("bytes_fetched")
        blocked = report.get("host_not_allowed")
        volumes = report.get("volumes")
        result = StageResult(
            report.get("ok") is True,
            len(volumes) if isinstance(volumes, list) else 0,
            texts(report.get("errors")),
            fetched if type(fetched) is int and fetched >= 0 else 0,
            text(blocked.get("host"), 300) if isinstance(blocked, dict) and blocked.get("host") else None,
        )
        await self._record_assets(round_id, record, result)
        return result

    async def _record_assets(self, round_id: str, record: str, result: StageResult) -> None:
        """Register with buffer-api what the stage job left on disk: the staged artifact under its id (with every
        input the id hashes, so a later round can reuse it) and the record's raw cache as files.json lists it."""
        artifact, pins = self._artifact(round_id, record), self._pins(round_id)
        binding_path, cache = self._binding_path(round_id, record), self._cache_dir(record)
        binding = yaml.safe_load(binding_path.read_text())
        staged = {
            "record": record,
            "levels_key": self._levels_key(round_id),
            "path": str(self._staged_dir(round_id, record)),
            "artifact": artifact,
            "binding_hash": hashlib.sha256(binding_path.read_bytes()).hexdigest(),
            "harness_commit": pins.harness_commit,
            "env_digest": pins.env_digest,
            "voxel_size_nm": self._row(round_id, record).get("voxel_size_nm"),
            "round": round_id,
            "ok": result.ok,
            "volumes": result.volumes,
            "bytes_fetched": result.bytes_fetched,
            "snippets_commit": str(binding.get("snippets_commit") or "") if isinstance(binding, dict) else "",
        }
        files = {
            str(url)[:500]: {"size": (entry.get("validators") or {}).get("size"), "source": entry.get("source")}
            for url, entry in read_manifest(cache).items()
            if isinstance(entry, dict)
        }
        raw = {"path": str(cache), "files": files}
        await asyncio.to_thread(self.registry.put_asset, "staged", artifact, staged)
        await asyncio.to_thread(self.registry.put_asset, "raw", record, raw)

    @activity.defn(name="verify_drop")
    @mia_permanent_errors
    async def verify_drop(self, round_id: str, record: str) -> DropResult:
        """Move the files a human placed under raw_root/<record>/incoming/ into the cache under the harness's
        key and list them in files.json. A listed file that is missing, empty or a symlink stays missing."""
        binding = yaml.safe_load(self._binding_path(round_id, record).read_text()) or {}
        urls = [
            str(url)
            for array in (binding.get("raw") or []) + (binding.get("labels") or [])
            for url in (array.get("urls") or [])
        ]
        cache, known = self._cache_dir(record), read_manifest(self._cache_dir(record))
        placed, missing, entries = [], [], {}
        for url, name in drop_names(urls):
            if url in known:
                continue
            path = cache / "incoming" / name
            try:
                info = os.lstat(path)
            except OSError:
                missing.append(name)
                continue
            if not os.path.isfile(path) or os.path.islink(path) or info.st_size == 0:
                missing.append(name)
                continue
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
            validators = {"size": info.st_size, "sha256": digest.hexdigest()}
            os.replace(path, cache / cache_key(url, validators))
            entries[url] = {
                "validators": validators,
                "source": MANIFEST_SOURCE_MANUAL,
                "at": datetime.now(UTC).isoformat(),
            }
            placed.append(name)
        if entries:
            merge_manifest(cache, entries)
        return DropResult(not missing, placed, missing)

    @activity.defn(name="create_buffer")
    @mia_permanent_errors
    async def create_buffer(self, round_id: str, buffer_id: str) -> None:
        """`PUT buffers/<id>` from the round's plan and spec."""
        cfg = self.cfg
        body = buffer_config(
            self._plan(round_id),
            self._spec(round_id),
            tensorizer=cfg.sourcing.tensorizer,
            split_sizes={"val": cfg.sourcing.val_size, "test": cfg.sourcing.test_size},
        )
        await asyncio.to_thread(self.registry.request, "PUT", f"/buffers/{quote(buffer_id, safe='')}", json=body)

    @activity.defn(name="register_dataset")
    @mia_permanent_errors
    async def register_dataset(self, round_id: str, record: str, buffer_id: str) -> RegisterResult:
        """`PUT volumes/<vol>` for every volume the stage job wrote for this record."""
        staged = self._staged_dir(round_id, record)
        report = _stage_json(staged)
        if not isinstance(report, dict):
            return RegisterResult(refused={record: "stage.json is missing, unreadable or not a JSON object"})
        result = RegisterResult()
        bodies = volume_bodies(
            self._row(round_id, record),
            staged,
            self._spec(round_id),
            report=report,
            refused=result.refused,
            artifact=staged.name,
        )
        for volume_id, body in bodies:
            path = f"/buffers/{quote(buffer_id, safe='')}/volumes/{quote(volume_id, safe='')}"
            try:
                await asyncio.to_thread(self.registry.request, "PUT", path, json=body)
            except RegistryError as error:
                if error.status is None or error.status == 401 or error.status >= 500:
                    raise  # buffer-api down, our token refused, or its own fault: retry, never refuse the volume
                result.refused[volume_id] = text(error.message, 500)
            else:
                result.registered.append(volume_id)
        return result


def _stage_json(staged: Path) -> Any:
    """The stage job's stage.json (untrusted job output, read within STAGE_JSON_LIMIT), or None."""
    return loads(read_untrusted(staged, "stage.json", STAGE_JSON_LIMIT))
