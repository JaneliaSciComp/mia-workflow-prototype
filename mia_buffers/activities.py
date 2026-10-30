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
import logging
import os
import re
import shutil
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

import yaml
from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.activities import PERMANENT_CSUB
from autodev.jobs import JobSpec
from mia_core.assets import cache_missing, raw_asset, read_files_json
from mia_core.fs import dir_bytes
from mia_core.locks import heartbeat_through, release_or_log, take_or_holder
from mia_core.registry import Registry, RegistryError, best_effort
from mia_core.untrusted import loads, neutralize, read_untrusted, text, texts, write_atomic
from mia_data.activities import mia_permanent_errors
from mia_data.binding import hosts as job_hosts
from mia_data.binding import missing_hosts
from mia_data.catalog import dataset_folder
from mia_data.jobspec import SHA_RE, check_hosts_allowed, harness_pin, harness_snapshot, lock_dir, snapshot
from mia_data.models import JobDone, LockTaken
from mia_data.publish import prepared_row
from mia_data.seed import ID_RE, MARKER, render_issue
from mia_data.status import render_links, render_stage

from .api import BufferConfig
from .config import HUMAN_LABELS, PREP_HUMAN_LABELS, PREP_STAGES, STAGES
from .drop import cache_key, drop_names, merge_manifest, read_manifest
from .jobspec import artifact_id, catalog_snapshot, fetch_job, levels_key, stage_job
from .keeper import demand, evict_path, free_assets, live_bytes, plan_evictions, stale_builds
from .models import (
    MAX_POLL_MINUTES,
    BindingInfo,
    BufferPins,
    BufferState,
    DatasetIssue,
    DatasetPlan,
    DegradedSignal,
    DropResult,
    FetchJobArgs,
    FetchResult,
    KeeperReport,
    PlanResult,
    PrepOutcome,
    PrepPins,
    PrepState,
    QuotaCheck,
    RawStatus,
    RegisterResult,
    SourcingLimits,
    StagedSummary,
    StageJobArgs,
    StageResult,
    job_key,
)
from .plan import MANUAL_DROP_NOTE, UNREADABLE, Row, _group_key, build_plan, load_records, plan_hash
from .poll import run_poll
from .register import buffer_config, recipe_for, volume_bodies
from .select import decide_all
from .spec import SpecError, load_prep_text, load_spec
from .specmap import facts
from .status import render_drop_comment, render_plan_comment

if TYPE_CHECKING:
    from temporalio.client import Client

    from autodev.github import GitHub
    from autodev.jobs import CsubJobs

    from .config import Config

log = logging.getLogger(__name__)
ROUND_RE = re.compile(r"[ip]\d+-e\d+")  # i<issue>-e<event>: a buffer round; p<issue>-e<event>: a preparation request
BUILDING_DIR_RE = re.compile(r"\.building-([0-9a-f]{16})-buf-.+")  # a stage job's build: the artifact id, the job key
SPEC_FENCE = re.compile(r"^```ya?ml[ \t]*\n(.*?)^```[ \t]*$", re.MULTILINE | re.DOTALL)
MANIFEST_SOURCE_MANUAL = "manual"
STAGE_JSON_LIMIT = 64 << 20  # stage.json carries a 4,096-value sample per channel per volume: ~150 KB per RGB volume
FETCH_JSON_LIMIT = 16 << 20  # fetch.json has one short entry per url of the binding
FETCH_JSON_MISSING = "fetch.json missing: the job ended before writing it"
DATA_LABEL = "data:{}"  # mia-data's labels on a dataset issue (mia_data.config.Config.label)
DATA_GO = DATA_LABEL.format("go")
DATA_IN_PROGRESS = {"matching", "developing", "testing", "pr-ready", "review", "cropping"}
KEEPER = "keeper"  # the cache keeper's lock owner
KEEPER_LOCK_TTL_S = 900
KEEPER_HEARTBEAT_S = 60  # the keep activity heartbeats at least this often, whatever step it is in
STAGE_EVENTS = {  # a stage label's lifecycle event on the buffer's page (the buffer id is the round id)
    "planning": "created",
    "plan-review": "planned",
    "sourcing": "sourcing",  # `approved` is the approve at the staged gate (the workflow writes it)
    "staged": "staged",
    "ready": "ready",
    "idle": "idle",  # `retired` is retire_buffer's: a superseded buffer is retired without a stage label
    "needs-human": "needs-human",
}
OVER_QUOTA = (
    "The {kind} root is over quota by {over_by} bytes and nothing is evictable (every asset is referenced, held or "
    "building). Raise `quota.{kind}_bytes`, retire a buffer, or wait for `quota.hold_hours`."
)


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
            self.take_lock,
            self.release_lock,
            self.raw_status,
            self.check_quota,
            self.prepare_fetch,
            self.submit_fetch,
            self.read_fetch,
            self.mark_asset,
            self.verify_drop,
            self.create_buffer,
            self.register_dataset,
            self.mark_dataset,
            self.staged_summary,
            self.buffer_event,
            self.retire_buffer,
            self.raw_ready,
            self.missing_records,
            self.comment_drop,
            self.nudge_dataset,
            self.prep_admit,
            self.prep_nudge,
            self.prep_status,
            self.set_prep_stage,
            self.comment_prep,
            self.poll,
            self.keep,
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

    async def _swap_label(self, issue: int, want: str, stage_labels: list[str]) -> None:
        """Leave `want` as the issue's only label among `stage_labels`."""
        gh = self.gh_issues
        current = {label["name"] for label in (await gh.get_issue(issue)).get("labels", [])}
        for name in stage_labels:
            if name in current and name != want:
                await gh.remove_label(issue, name)
        if want not in current:
            await gh.add_labels(issue, [want])

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

    def _building_dir(self, round_id: str, record: str, rebuild: int = 0) -> Path:
        """Where the round's stage job writes the pyramid, next to its final directory; read_stage renames it there
        once the job says ok (`.building-<artifact>-<stage job key>`, so two rounds, or a rebuild, never share one)."""
        staged = self._staged_dir(round_id, record)
        return staged.parent / f".building-{staged.name}-{job_key(_issue_of(round_id), round_id, record, rebuild)}"

    def _cache_dir(self, record: str) -> Path:
        if not ID_RE.fullmatch(record):
            raise ValueError(f"bad record id {record!r}")
        return self.cfg.paths.raw_root / record

    def _binding_urls(self, round_id: str, record: str) -> list[str]:
        """Every url of the round's copy of the record's binding (trusted: read_binding checked it), raw then labels."""
        binding = yaml.safe_load(self._binding_path(round_id, record).read_text()) or {}
        return [
            str(url)
            for array in (binding.get("raw") or []) + (binding.get("labels") or [])
            for url in (array.get("urls") or [])
        ]

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
        """Build the plan over buffer-api's records at the round's catalog ref (synced into it first when it has
        never seen the ref) and its prepared datasets; plan.json and plan.md go to the round dir and to the round's
        browsable workspace. Returns the summary the workflow needs. A registry outage raises: the activity retries."""
        from .cli import catalog_sync  # the `catalog sync` command's body; imported here, not with the activities

        spec, ref, registry = self._spec(round_id), pins.catalog_ref, self.registry
        if ref not in {catalog["ref"] for catalog in await asyncio.to_thread(registry.catalogs)}:
            await catalog_sync(self.cfg, self.gh_catalog, registry, ref)
        records = await asyncio.to_thread(registry.records, ref)
        prepared = {row["id"] for row in await asyncio.to_thread(registry.datasets, "prepared")}
        plan = await asyncio.to_thread(build_plan, spec, pins.spec_hash, ref, records, prepared)
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
        if plan.valid and spec.plan.auto_approve:  # no plan-review stage to write `planned`: the timeline needs it
            await asyncio.to_thread(
                best_effort,
                lambda: registry.event(round_id, "planned", issue=issue, event_id=None, detail="auto-approved"),
                f"planned event of {round_id}",
                log,
            )
        return PlanResult(
            plan.valid,
            plan.errors[:20],
            pool,
            plan.n_seen,
            spec.ready.min_fraction,
            spec.sourcing.dataset_timeout_days,
            list(spec.sampling.levels),
            int(patch),
            auto_approve=spec.plan.auto_approve,
            plan_hash=plan_hash(plan_json),
            idle_hours=spec.idle.hours,
            retire_after_days=spec.retire_after_days,
        )

    @activity.defn(name="limits")
    async def limits(self) -> SourcingLimits:
        cfg = self.cfg
        return SourcingLimits(
            cfg.sourcing.parallel,
            cfg.limits.job_walltime_min,
            cfg.service.url,
            parallel_fetch=cfg.sourcing.parallel_fetch,
            fetch_walltime_min=cfg.limits.fetch_walltime_min,
            fetch_attempts=cfg.limits.fetch_attempts,
            poll_minutes=cfg.temporal.poll_minutes,
            max_tick_minutes=MAX_POLL_MINUTES,
        )

    # --- the issue ---------------------------------------------------------------------------

    @activity.defn(name="set_stage")
    @mia_permanent_errors
    async def set_stage(
        self, issue: int, stage: str, state: BufferState | None = None, detail: str = "", extra: str = ""
    ) -> None:
        """Swap the `buffer:<stage>` label and, within a round, post the stage's comment once."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}")
        await self._swap_label(issue, self.cfg.label(stage), [self.cfg.label(other) for other in STAGES])
        if not (state and state.round_id):
            return
        if (kind := STAGE_EVENTS.get(stage)) is not None:
            buffer, registry = state.buffer_id or state.round_id, self.registry
            await asyncio.to_thread(
                best_effort,
                lambda: registry.event(buffer, kind, issue=issue, event_id=None, detail=detail[:200]),
                f"{kind} event of {buffer}",
                log,
            )
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
        ref), and add `data:go` unless mia-data is already working on it or has finished. A stale `data:ineligible`
        (a verdict mia-data no longer gives, spec §12) is nudged like `new`: mia-data's first stage replaces it."""
        return (await self._nudge(round_id, record))[0]

    async def _nudge(self, round_id: str, record: str) -> tuple[DatasetIssue, bool]:
        """nudge_dataset's work, and whether it opened the dataset issue."""
        gh, record_yaml = self.gh_datasets, self._record(round_id, record)
        folder = dataset_folder(record_yaml, record)
        issue = await gh.find_issue_by_marker(MARKER.format(record))
        created = issue is None
        if issue is None:
            title, body, labels = render_issue(
                record_yaml,
                {"record_commit": self._pins(round_id).catalog_ref},
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
        if stage == "done":
            path = self.cfg.paths.storage_root / folder
            note = (
                f"the dataset issue is `data:done` but `{path}` has no binding.yaml with a matching manifest.json: "
                "run the dataset again (add `data:go` on it) so mia-data publishes both"
            )
            return DatasetIssue(number, stage, False, note, folder), created
        if DATA_GO in names or stage in DATA_IN_PROGRESS:
            return DatasetIssue(number, stage, False, folder=folder), created
        await gh.add_labels(number, [DATA_GO])
        return DatasetIssue(number, stage, True, folder=folder), created

    # --- preparation requests (milestone E) --------------------------------------------------

    @activity.defn(name="prep_admit")
    @mia_permanent_errors
    async def prep_admit(self, issue: int, event_id: str) -> PrepPins:
        """Pin a preparation request: the prep spec from the issue body's one ```yaml fence, the catalog at its ref
        and the records it names (or its `select` admits there). Written once to state_dir/rounds/p<issue>-e<event>/
        with a plan.json of {id, path} rows and buffer-shaped pins, so nudge_dataset reads the records as it does a
        buffer round's; a retry returns the same pins."""
        if not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"bad event id {event_id!r}")
        round_dir = self._round_dir(f"p{int(issue)}-e{event_id}")
        if (round_dir / "prep.json").is_file():
            return PrepPins(**json.loads((round_dir / "prep.json").read_text()))
        body = ((await self.gh_issues.get_issue(issue)).get("body") or "").replace("\r\n", "\n")
        fences = SPEC_FENCE.findall(body)
        if len(fences) != 1:
            raise ValueError(
                f"issue #{issue}: expected exactly one ```yaml block holding the preparation request, found {len(fences)}"
            )
        try:
            spec = load_prep_text(fences[0])
        except SpecError as error:
            raise ValueError(f"preparation request refused: {error}") from error
        ref = spec.catalog.ref
        snapshot = await asyncio.to_thread(catalog_snapshot, self.cfg, ref, self.gh_catalog.tokens)
        readable = [
            (path, record)
            for path, record in await asyncio.to_thread(load_records, snapshot)
            if UNREADABLE not in record
        ]
        paths = {record["id"]: path for path, record in readable}
        if spec.select is None:
            records = list(dict.fromkeys(spec.records))
            if unknown := [record for record in records if record not in paths]:
                raise ValueError(f"records not in the catalog at {ref}: {', '.join(unknown)}")
        else:
            decisions = decide_all([facts(path, record) for path, record in readable], spec.select)
            records = [decision.id for decision in decisions if decision.admitted]
            if not records:
                raise ValueError(f"the select block admits no record at {ref}")
        if len(records) > spec.max_records:  # a human wrote (or left) the number; each record is an agent run
            raise ValueError(
                f"the request names {len(records)} records, more than max_records {spec.max_records}: "
                f"set max_records: {len(records)} in the request to confirm, or split it"
            )
        round_dir.mkdir(parents=True, exist_ok=True)
        write_atomic(round_dir / "spec.yaml", fences[0])
        write_atomic(
            round_dir / "plan.json", json.dumps({"rows": [{"id": id_, "path": paths[id_]} for id_ in records]})
        )
        spec_hash = hashlib.sha256(fences[0].encode()).hexdigest()
        write_atomic(round_dir / "pins.json", json.dumps(dataclasses.asdict(BufferPins(spec_hash, ref, "", ""))))
        pins = PrepPins(round_dir.name, ref, records, spec.timeout_days, int(issue))
        write_atomic(round_dir / "prep.json", json.dumps(dataclasses.asdict(pins)))  # last: marks the request admitted
        return pins

    @activity.defn(name="prep_nudge")
    @mia_permanent_errors
    async def prep_nudge(self, round_id: str, record: str) -> PrepOutcome:
        """Hand one requested record to mia-data unless the registry already settled it (prepared or needs-human:
        GitHub is not touched); otherwise nudge_dataset, its result named for the request's comment."""
        try:
            row = await asyncio.to_thread(self.registry.dataset, record)
        except RegistryError as error:  # best effort: the dataset issue tells the rest
            log.warning("prep_nudge: registry row of %s unread: %s", record, error)
            row = None
        if (state := (row or {}).get("state")) in ("prepared", "needs-human"):
            issue = (row or {}).get("issue")
            return PrepOutcome(record, state, issue if isinstance(issue, int) else None)
        found, created = await self._nudge(round_id, record)
        if found.stage == "done":
            outcome = "done-unpublished"
        elif found.nudged:
            outcome = "opened" if created else "nudged"
        else:  # data:go present already, or mia-data is working on it
            outcome = "in-progress"
        return PrepOutcome(record, outcome, found.number, found.note)

    @activity.defn(name="prep_status")
    @mia_permanent_errors
    async def prep_status(self, records: list[str]) -> dict[str, str]:
        """Each record's registry state, `unprepared` when it has no row. A registry outage raises."""
        states = {row["id"]: row.get("state", "unprepared") for row in await asyncio.to_thread(self.registry.datasets)}
        return {record: states.get(record, "unprepared") for record in records}

    @activity.defn(name="set_prep_stage")
    @mia_permanent_errors
    async def set_prep_stage(self, issue: int, stage: str, state: PrepState | None = None, detail: str = "") -> None:
        """Swap the `prep:<stage>` label and, within a request's round, post the stage's comment once."""
        if stage not in PREP_STAGES:
            raise ValueError(f"unknown preparation stage {stage!r}")
        await self._swap_label(issue, self.cfg.prep_label(stage), [self.cfg.prep_label(other) for other in PREP_STAGES])
        if not (state and state.round_id):
            return
        marker = self.cfg.marker(f"prep stage {state.round_id} {stage}")
        body = render_stage(stage, self._clean(detail), prefix="prep") + f"\n{marker}"
        await self._comment_once(self.gh_issues, issue, body, marker)

    @activity.defn(name="comment_prep")
    @mia_permanent_errors
    async def comment_prep(self, issue: int, round_id: str, outcomes: list[PrepOutcome]) -> None:
        """Post, once per round, what was done for each requested record, with its dataset issue."""
        repo = self.cfg.github.datasets_repo  # the dataset issues live there: a bare #N would link this repo's
        lines = ["| record | outcome | issue |", "|---|---|---|"]
        for entry in outcomes:
            outcome = f"{entry.outcome}: {entry.note}" if entry.note else entry.outcome
            lines.append(f"| `{entry.record}` | {outcome} | {f'{repo}#{entry.issue}' if entry.issue else ''} |")
        marker = self.cfg.marker(f"prep outcomes {round_id}")
        body = "\n".join(lines)
        await self._comment_once(self.gh_issues, issue, f"{self._clean(body)}\n\n{marker}", marker)

    @activity.defn(name="comment_drop")
    @mia_permanent_errors
    async def comment_drop(self, issue: int, round_id: str, record: str, missing: list[str]) -> None:
        """Ask for a dataset's files (once), or list the ones still missing after a failed verification (once
        per distinct missing set)."""
        files = [list(pair) for pair in drop_names(self._binding_urls(round_id, record))]
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
        humans = {self.cfg.label(kind) for kind in HUMAN_LABELS} | {
            self.cfg.prep_label(kind) for kind in PREP_HUMAN_LABELS
        }
        if label not in humans or not re.fullmatch(r"\d+", str(event_id)):
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
        and its stage.json is still on disk (its `last_used` is then touched). Buffer-api unreachable or refusing:
        not found (stage again)."""
        artifact = self._artifact(round_id, record)
        try:
            data = await asyncio.to_thread(self.registry.asset, "staged", artifact)
        except RegistryError as error:
            activity.logger.warning("%s: registry unavailable, staging again: %s", record, error)
            return False, artifact
        found = isinstance(data, dict) and data.get("ok") is True
        found = found and (self._staged_dir(round_id, record) / "stage.json").is_file()
        if found:  # a buffer is about to reuse it: the keeper's hold rule spares it
            await self.mark_asset("staged", artifact, {"last_used": time.time()})
        return found, artifact

    @activity.defn(name="prepare_stage")
    @mia_permanent_errors
    async def prepare_stage(self, issue: int, round_id: str, record: str, rebuild: int = 0) -> str:
        """The stage job's workspace with record.yaml and binding.yaml; the raw cache and the round's building
        directory exist afterwards (a mount must). A rebuild gets its own (`stage-<record>-r<n>`). Returns the
        workspace path."""
        workspace = self._round_workspace(issue, round_id) / (f"stage-{record}" + (f"-r{rebuild}" if rebuild else ""))
        workspace.mkdir(parents=True, exist_ok=True)
        write_atomic(workspace / "record.yaml", yaml.safe_dump(self._record(round_id, record), sort_keys=False))
        shutil.copyfile(self._binding_path(round_id, record), workspace / "binding.yaml")
        self._cache_dir(record).mkdir(parents=True, exist_ok=True)
        self._building_dir(round_id, record, rebuild).mkdir(parents=True, exist_ok=True)
        return str(workspace)

    @activity.defn(name="submit")
    @mia_permanent_errors
    async def submit(self, key: str, args: StageJobArgs) -> str:
        """Build the stage job (snippets at the binding's commit, the round's building directory as `out`), refuse
        hosts a fresh `csub probe` doesn't allow, and submit at most once per key. Returns the job id; the staged
        asset row says `staging` under it (best effort), at the final path read_stage publishes to."""
        staged = self._staged_dir(args.round_id, args.record)
        args.out = str(self._building_dir(args.round_id, args.record, args.rebuild))
        Path(args.out).mkdir(parents=True, exist_ok=True)  # a mount must exist; the sweep may have taken it since
        job_id = await self._submit_job(key, args.workspace, args.record, lambda snap: stage_job(self.cfg, args, snap))
        fields = {
            "state": "staging",
            "ok": False,  # until read_stage says otherwise: an earlier round's `ok` must not make this one reusable
            "job": job_id,
            "record": args.record,
            "levels_key": self._levels_key(args.round_id),
        }
        await self.mark_asset("staged", staged.name, {**fields, "path": str(staged)})
        return job_id

    async def _submit_job(self, key: str, workspace: str, record: str, build_spec: Callable[[Path], JobSpec]) -> str:
        """Submit `build_spec(snippets snapshot at the binding's commit)` at most once per key, refusing hosts a
        fresh `csub probe` doesn't allow. Returns the job id."""
        from csub import CsubError  # pixi-only dependency, imported where used (as autodev does)

        if self.jobs is None:
            raise RuntimeError("no job runner: submit runs only inside the worker")
        binding = yaml.safe_load(Path(workspace, "binding.yaml").read_text()) or {}
        sha = str(binding.get("snippets_commit") or "")
        if not SHA_RE.fullmatch(sha):
            raise ValueError(f"binding of {record} has no snippets_commit")
        patterns = await asyncio.to_thread(self.probe)
        snapshot_dir = await asyncio.to_thread(snapshot, self.cfg, sha, self.gh_snippets.tokens)

        def build() -> JobSpec:
            spec = build_spec(snapshot_dir)
            check_hosts_allowed(spec, patterns)
            return spec

        try:
            return await self.jobs.submit(key, build)
        except CsubError as error:
            if error.code in PERMANENT_CSUB:
                raise ApplicationError(str(error), non_retryable=True) from error
            raise

    @activity.defn(name="await_job")
    async def await_job(self, job_id: str, workspace: str, lock: list[str] | None = None) -> JobDone:
        """Wait for the job, heartbeating; with `lock` ([kind, key, owner]) each heartbeat also refreshes that lock."""
        if self.jobs is None:
            raise RuntimeError("no job runner: await_job runs only inside the worker")
        heartbeat = heartbeat_through(lock_dir(self.cfg), lock, activity.heartbeat) if lock else activity.heartbeat
        outcome = await self.jobs.wait(job_id, workspace, heartbeat)
        return JobDone(outcome.job_id, outcome.state, outcome.exit_code)

    @activity.defn(name="read_stage")
    @mia_permanent_errors
    async def read_stage(self, round_id: str, record: str, rebuild: int = 0) -> StageResult:
        """The stage job's stage.json, reduced; tolerant of any shape. An ok build is published: the artifact block
        written into its stage.json, then the building directory renamed to the final one (when that exists already,
        an identical pyramid won: the build is removed and the final one reused). A failed build stays where it is,
        for the keeper's `.building-*` rule. Idempotent on retry."""
        final, building = self._staged_dir(round_id, record), self._building_dir(round_id, record, rebuild)
        report = _stage_json(building if building.exists() else final)
        if isinstance(report, dict) and report.get("ok") is True and building.exists():
            report = await asyncio.to_thread(self._publish, round_id, record, report, building, final, rebuild > 0)
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
        await self._record_assets(round_id, record, result, rebuild > 0)
        return result

    def _artifact_block(self, round_id: str, record: str) -> dict:
        """Every input the staged artifact id hashes, as stage.json's `artifact` and the staged asset row keep them."""
        pins = self._pins(round_id)
        return {
            "id": self._artifact(round_id, record),
            "binding_hash": hashlib.sha256(self._binding_path(round_id, record).read_bytes()).hexdigest(),
            "levels_key": self._levels_key(round_id),
            "voxel_size_nm": self._row(round_id, record).get("voxel_size_nm"),
            "harness_commit": pins.harness_commit,
            "env_digest": pins.env_digest,
        }

    def _publish(
        self, round_id: str, record: str, report: dict, building: Path, final: Path, replace: bool = False
    ) -> Any:
        """Write the artifact block into the build's stage.json and rename the build to its final directory. Returns
        the published report: the build's, or the final directory's when that was there first. `replace` (a rebuild
        of a degraded pyramid): the final directory, broken though its stage.json says ok, is set aside first."""
        report["artifact"] = self._artifact_block(round_id, record)
        write_atomic(building / "stage.json", json.dumps(report))  # compact: it carries ~150 KB of samples
        if replace and final.exists():  # for the `.building-*` sweep
            os.rename(final, final.with_name(f".building-stale-{final.name}-{time.time_ns()}"))
        try:
            os.rename(building, final)
        except OSError:
            if not final.exists():  # not a lost race: the rename itself failed
                raise
            published = _stage_json(final)
            if isinstance(published, dict) and published.get("ok") is True:
                log.info("%s: %s was published already; reusing it, removing %s", record, final, building.name)
                shutil.rmtree(building)
                return published
            # a failed build left in place before builds had their own directory: aside, for the `.building-*` sweep
            os.rename(final, final.with_name(f".building-stale-{final.name}-{time.time_ns()}"))
            os.rename(building, final)
        return report

    async def _record_assets(self, round_id: str, record: str, result: StageResult, rebuild: bool = False) -> None:
        """Register with buffer-api what the stage job left on disk: the staged artifact under its id (with every
        input the id hashes, so a later round can reuse it; `ready`, or `partial` when the job failed) and the
        record's raw cache as files.json lists it, both just used. A failed build beside a published artifact (not
        a rebuild of it) leaves that artifact's row as it was before submit marked it `staging`."""
        block, cache = self._artifact_block(round_id, record), self._cache_dir(record)
        binding = yaml.safe_load(self._binding_path(round_id, record).read_text())
        staged_dir, now, artifact = self._staged_dir(round_id, record), time.time(), block["id"]
        published = _stage_json(staged_dir)
        if not result.ok and not rebuild and isinstance(published, dict) and published.get("ok") is True:
            log.info("%s: the build failed; %s stays published and reusable", record, staged_dir)
            staged: dict[str, Any] = {"state": "ready", "ok": True, "job": None, "last_used": now}  # undo `staging`
        else:
            staged = {
                "record": record,
                "levels_key": block["levels_key"],
                "path": str(staged_dir),
                "artifact": artifact,
                "binding_hash": block["binding_hash"],
                "harness_commit": block["harness_commit"],
                "env_digest": block["env_digest"],
                "voxel_size_nm": block["voxel_size_nm"],
                "round": round_id,
                "ok": result.ok,
                "volumes": result.volumes,
                "bytes_fetched": result.bytes_fetched,
                "snippets_commit": str(binding.get("snippets_commit") or "") if isinstance(binding, dict) else "",
                "state": "ready" if result.ok else "partial",
                "bytes": await asyncio.to_thread(dir_bytes, staged_dir),
                "last_used": now,
                "job": None,
            }
        raw = await asyncio.to_thread(raw_asset, cache, self._binding_urls(round_id, record))
        del raw["state"]  # the fetch phase owns the raw state (it knows the stream urls); a stage updates the rest
        raw["last_used"] = now
        await asyncio.to_thread(self.registry.put_asset, "staged", artifact, staged)
        await asyncio.to_thread(self.registry.put_asset, "raw", record, raw)

    # --- the fetch phase: locks, quota, the fetch job, raw-cache state ---------------------------

    @activity.defn(name="take_lock")
    @mia_permanent_errors
    async def take_lock(self, kind: str, key: str, owner: str, ttl_s: int) -> LockTaken:
        """Take the lock file `<local_root>/locks/<kind>/<key>`; when someone else holds it, say who."""
        return LockTaken(*await asyncio.to_thread(take_or_holder, lock_dir(self.cfg), kind, key, owner, ttl_s))

    @activity.defn(name="release_lock")
    @mia_permanent_errors
    async def release_lock(self, kind: str, key: str, owner: str) -> None:
        """Release the lock if `owner` still holds it; a lock that lapsed or changed hands is logged and left."""
        await asyncio.to_thread(release_or_log, lock_dir(self.cfg), kind, key, owner)

    @activity.defn(name="raw_status")
    @mia_permanent_errors
    async def raw_status(self, round_id: str, record: str) -> RawStatus:
        """Whether files.json of the record's raw cache lists every whole file of the round's binding but the
        stream urls an earlier fetch recorded on the raw row (buffer-api unreachable: none known). The row's
        `last_used` is touched, so the keeper's hold rule spares what this buffer is about to use."""
        try:
            row = await asyncio.to_thread(self.registry.asset, "raw", record)
        except RegistryError as error:
            log.warning("%s: registry unavailable, no stream urls known: %s", record, error)
            row = None
        if isinstance(row, dict):
            await self.mark_asset("raw", record, {"last_used": time.time()})
        recorded = row.get("stream") if isinstance(row, dict) else None
        urls = self._binding_urls(round_id, record)
        stream = [url for url in urls if isinstance(recorded, list) and url in recorded]
        manifest = await asyncio.to_thread(read_files_json, self._cache_dir(record))
        ready, missing = cache_missing(manifest, urls, stream)  # raw_asset's `ready` rule, without the du
        return RawStatus(ready, missing, texts(stream))

    @activity.defn(name="raw_ready")
    @mia_permanent_errors
    async def raw_ready(self, round_id: str, record: str) -> bool:
        """Whether the record's raw cache still holds its binding's files: a missing pyramid can be staged again."""
        return (await self.raw_status(round_id, record)).ready

    @activity.defn(name="check_quota")
    @mia_permanent_errors
    async def check_quota(self, round_id: str, record: str) -> QuotaCheck:
        """Whether fetching the record (its catalog size) keeps the raw cache within `quota.raw_bytes` (0 is no
        limit). The record's own row is left out of the total, so a resumed fetch does not count its partial
        cache twice. Over: the row records `wanted` (the catalog size) and `blocked_at`, best effort, for the
        keeper. A RegistryError propagates: the workflow retries rather than fetch past the quota."""
        limit = int(self.cfg.quota.raw_bytes)
        if limit == 0:
            return QuotaCheck(True)
        expected = int(self._row(round_id, record).get("bytes") or 0)
        listing = await asyncio.to_thread(self.registry.assets, "raw")
        own = next((asset.get("data") or {} for asset in listing["assets"] if asset.get("key") == record), {})
        counted = own.get("state") not in ("missing", "evicted")  # the rows the registry's `bytes` total sums
        own_bytes = own.get("bytes")
        own_bytes = own_bytes if counted and type(own_bytes) is int else 0
        over_by = int(listing["bytes"]) - own_bytes + expected - limit
        if over_by > 0:  # the keeper frees room for the largest blocked fetch (`wanted`), not only the headroom
            await self.mark_asset("raw", record, {"wanted": expected, "blocked_at": time.time()})
        return QuotaCheck(over_by <= 0, max(over_by, 0), expected)

    @activity.defn(name="prepare_fetch")
    @mia_permanent_errors
    async def prepare_fetch(self, issue: int, round_id: str, record: str, attempt: int) -> str:
        """The fetch job's workspace, one per attempt, with record.yaml and binding.yaml; the raw cache exists
        afterwards (a mount must). Returns the workspace path."""
        workspace = self._round_workspace(issue, round_id) / f"fetch-{record}-{int(attempt)}"
        workspace.mkdir(parents=True, exist_ok=True)
        write_atomic(workspace / "record.yaml", yaml.safe_dump(self._record(round_id, record), sort_keys=False))
        shutil.copyfile(self._binding_path(round_id, record), workspace / "binding.yaml")
        self._cache_dir(record).mkdir(parents=True, exist_ok=True)
        return str(workspace)

    @activity.defn(name="submit_fetch")
    @mia_permanent_errors
    async def submit_fetch(self, key: str, args: FetchJobArgs) -> str:
        """Submit the fetch job at most once per key, hosts checked as `submit` does. Returns the job id; the raw
        asset row says `fetching` under it (best effort)."""
        job_id = await self._submit_job(key, args.workspace, args.record, lambda snap: fetch_job(self.cfg, args, snap))
        await self.mark_asset("raw", args.record, {"state": "fetching", "job": job_id})
        return job_id

    @activity.defn(name="read_fetch")
    @mia_permanent_errors
    async def read_fetch(self, round_id: str, record: str, workspace: str) -> FetchResult:
        """The fetch job's fetch.json, reduced; none (a job killed at its walltime) is partial, since the next
        attempt resumes from the cache. The raw asset row is then put from the cache as it is (best effort)."""
        report = loads(read_untrusted(Path(workspace), "fetch.json", FETCH_JSON_LIMIT))
        stream: list[str] | None = None  # no report: the row keeps the stream urls it has
        if not isinstance(report, dict):
            result = FetchResult(True, partial=True, errors=[FETCH_JSON_MISSING])
        else:
            reported = report.get("files")
            entries = reported if isinstance(reported, list) else []
            files = [entry for entry in entries if isinstance(entry, dict) and isinstance(entry.get("url"), str)]
            files = [entry for entry in files if entry["url"]]

            def urls(outcome: str) -> list[str]:
                return texts([entry["url"] for entry in files if entry.get("outcome") == outcome])

            fetched, blocked, failed = report.get("bytes_fetched"), report.get("host_not_allowed"), urls("failed")
            result = FetchResult(
                report.get("ok") is True and not failed,
                report.get("partial") is True or any(entry.get("outcome") == "pending" for entry in files),
                fetched if type(fetched) is int and fetched >= 0 else 0,
                urls("stream"),
                failed,
                texts(report.get("errors")),
                text(blocked.get("host"), 300) if isinstance(blocked, dict) and blocked.get("host") else None,
            )
            # the binding's own urls the harness streams, kept on the row so raw_status leaves them out
            streamed = {entry["url"] for entry in files if entry.get("outcome") == "stream"}
            stream = [url for url in self._binding_urls(round_id, record) if url in streamed]
        row = await asyncio.to_thread(raw_asset, self._cache_dir(record), self._binding_urls(round_id, record))
        row.update(state="ready" if result.ok and not result.partial else "partial", job=None, last_used=time.time())
        row.update(wanted=None, blocked_at=None)  # the fetch ran: no longer blocked
        if stream is not None:
            row["stream"] = stream
        await self.mark_asset("raw", record, row)
        return result

    @activity.defn(name="mark_asset")
    @mia_permanent_errors
    async def mark_asset(self, kind: str, key: str, fields: dict) -> None:
        """Merge `fields` into buffer-api's asset row, best effort: an outage is logged, never raised."""
        registry = self.registry
        await asyncio.to_thread(best_effort, lambda: registry.put_asset(kind, key, fields), f"{kind} asset {key}", log)

    @activity.defn(name="verify_drop")
    @mia_permanent_errors
    async def verify_drop(self, round_id: str, record: str) -> DropResult:
        """Move the files a human placed under raw_root/<record>/incoming/ into the cache under the harness's
        key and list them in files.json. A listed file that is missing, empty or a symlink stays missing. When none
        is, the raw row becomes `ready` (best effort)."""
        urls = self._binding_urls(round_id, record)
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
        if not missing:  # the row said waiting-drop: the files are in (best effort)
            await self.mark_asset("raw", record, {"state": "ready"})
        return DropResult(not missing, placed, missing)

    @activity.defn(name="create_buffer")
    @mia_permanent_errors
    async def create_buffer(self, round_id: str, buffer_id: str) -> None:
        """`PUT buffers/<id>` from the round's plan and spec, pinned to the round's harness and plan.json; no event
        (set_stage("sourcing") writes `approved`)."""
        cfg, spec, pins = self.cfg, self._spec(round_id), self._pins(round_id)
        plan_text = (self._round_dir(round_id) / "plan.json").read_text()
        body = buffer_config(
            json.loads(plan_text),
            spec,
            tensorizer=cfg.sourcing.tensorizer,
            split_sizes={"val": cfg.sourcing.val_size, "test": cfg.sourcing.test_size},
            recipe=recipe_for(
                spec,
                tensorizer=cfg.sourcing.tensorizer,
                harness_commit=pins.harness_commit,
                env_digest=pins.env_digest,
            ),
            plan_hash=plan_hash(plan_text),
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

    @activity.defn(name="mark_dataset")
    @mia_permanent_errors
    async def mark_dataset(
        self, buffer: str, record: str, status: str, reason: str = "", job: str | None = None
    ) -> None:
        """Put the dataset's row on the buffer's status board, best effort: an outage is logged, never raised."""
        registry = self.registry
        await asyncio.to_thread(
            best_effort,
            lambda: registry.put_buffer_dataset(buffer, record, status, reason, job),
            f"board {buffer} {record}",
            log,
        )

    @activity.defn(name="missing_records")
    @mia_permanent_errors
    async def missing_records(self, buffer_id: str) -> list[str]:
        """The records of the buffer's volumes whose path is gone from disk now (buffer-api stats each one)."""
        missing = (await asyncio.to_thread(self.registry.status, buffer_id))["missing"]
        return sorted({str(volume).split("/", 1)[0] for volume in missing})

    @activity.defn(name="staged_summary")
    @mia_permanent_errors
    async def staged_summary(self, buffer_id: str) -> StagedSummary:
        """What the staged comment needs from buffer-api (spec §15): servable volumes per split from the buffer's
        row and the bytes of the staged assets it references; an unknown buffer gives the empty summary."""
        registry = self.registry
        rows = [row for row in await asyncio.to_thread(registry.buffers) if row.get("id") == buffer_id]
        servable = {str(k): int(v) for k, v in ((rows[0].get("servable") or {}) if rows else {}).items()}
        assets = (await asyncio.to_thread(registry.assets, "staged")).get("assets") or []
        total = 0
        for asset in assets:
            size = (asset.get("data") or {}).get("bytes")
            if buffer_id in (asset.get("refs") or []) and isinstance(size, int) and not isinstance(size, bool):
                total += size
        return StagedSummary(servable=servable, bytes=total)

    @activity.defn(name="buffer_event")
    @mia_permanent_errors
    async def buffer_event(self, buffer_id: str, kind: str, detail: str = "") -> None:
        """Append a lifecycle event to a buffer that is not the round's own (a superseded one), best effort."""
        registry = self.registry
        await asyncio.to_thread(
            best_effort,
            lambda: registry.event(buffer_id, kind, issue=_issue_of(buffer_id), event_id=None, detail=detail[:200]),
            f"{kind} event of {buffer_id}",
            log,
        )

    @activity.defn(name="retire_buffer")
    @mia_permanent_errors
    async def retire_buffer(self, round_id: str, buffer_id: str) -> dict:
        """Release the buffer's references and archive its draws ({refs, draws, archive}), then write its `retired`
        event (best effort; buffer-api then refuses its consumers). A round that ended before sourcing was never
        registered (its events precede its config): nothing to release, the event alone. A failed retire raises,
        so the workflow retries."""
        registry = self.registry
        try:
            retired = await asyncio.to_thread(registry.retire, buffer_id)
            detail = f"archive {retired.get('archive') or 'none'}"
        except RegistryError as error:
            if error.status != 404:
                raise
            retired, detail = {"refs": 0, "draws": 0, "archive": None}, "never registered"
        await asyncio.to_thread(
            best_effort,
            lambda: registry.event(buffer_id, "retired", issue=_issue_of(buffer_id), event_id=None, detail=detail),
            f"retired event of {buffer_id}",
            log,
        )
        return retired

    # --- the cache keeper ----------------------------------------------------------------------

    @activity.defn(name="keep")
    async def keep(self, dry_run: bool = False) -> KeeperReport:
        """One pass of the cache keeper (rules in keeper.py), each step best effort, a failure an `errors` entry:
        sync datasets and assets; signal the workflows of ready and idle buffers (degraded, idle, retire); the catalog
        when `keeper.catalog_sync_hours` have passed; rows building under an
        ended job reset; `.building-*` directories and empty raw directories older than `quota.hold_hours` removed; free assets evicted per
        kind until under quota, and a comment on the newest buffer issue when a kind stays over. With `dry_run`
        the resets, removals and evictions are only reported. The catalog sync runs last, so a slow one never
        holds up an eviction; a background task heartbeats through any single long step."""
        report, registry, quota, now = KeeperReport(), self.registry, self.cfg.quota, time.time()
        roots = (self.cfg.paths.raw_root, self.cfg.paths.staged_root)
        hold_s = quota.hold_hours * 3600
        current = ["start"]  # the step under way, the heartbeat's detail

        async def keepalive() -> None:
            while True:
                await asyncio.sleep(KEEPER_HEARTBEAT_S)
                _keeper_beat(current[0])

        async def step(name: str, work: Awaitable[Any]) -> Any:
            current[0] = name
            _keeper_beat(name)
            try:
                return await work
            except Exception as error:  # every step is best effort; the next pass tries again
                log.warning("keeper: %s failed: %s", name, error)
                report.errors.append(text(f"{name}: {error}", 500))
                return None

        beating = asyncio.create_task(keepalive())
        try:
            await step("sync datasets", asyncio.to_thread(registry.sync_datasets))
            synced = await step("sync assets", asyncio.to_thread(registry.sync_assets)) or {}
            report.scanned, report.missing, report.new = (
                int(synced.get(name) or 0) for name in ("scanned", "missing", "new")
            )
            await step("buffers", self._keeper_buffers(report, now, dry_run))
            await step("stale builds", self._keeper_stale(report, dry_run))
            for path in synced.get("building") or []:
                await step("building", self._keeper_building(report, Path(path), roots, hold_s, now, dry_run))
            for path in synced.get("empty") or []:
                await step("empty", self._keeper_empty(report, Path(path), roots, hold_s, now, dry_run))
            for kind in ("staged", "raw"):
                await step(f"evict {kind}", self._keeper_evict(report, kind, roots, hold_s, now, dry_run))
            await step("catalog sync", self._keeper_catalog(report, now))
        finally:
            beating.cancel()
        report_body = {**dataclasses.asdict(report), "dry_run": dry_run}  # the page shows the last pass
        await asyncio.to_thread(best_effort, lambda: registry.put_keeper(report_body), "keeper report", log)
        return report

    async def _keeper_catalog(self, report: KeeperReport, now: float) -> None:
        """Sync the catalog's main head when the last sync is `keeper.catalog_sync_hours` old (or never ran)."""
        from .cli import catalog_sync  # the `catalog sync` command's body; imported here, not with the activities

        mark = self.cfg.paths.state_dir / "keeper" / "catalog-synced-at"
        try:
            last = float(mark.read_text())
        except (OSError, ValueError):
            last = 0.0
        if now - last < self.cfg.keeper.catalog_sync_hours * 3600:
            return
        ref, count = await catalog_sync(self.cfg, self.gh_catalog, self.registry)
        write_atomic(mark, str(int(now)))
        report.catalog = f"{ref} ({count} records)"

    async def _keeper_buffers(self, report: KeeperReport, now: float, dry_run: bool) -> None:
        """Watch the ready and idle buffers of a workflow (id `i<issue>-e<event>`): a pyramid gone from disk signals
        `degraded` with its records, a ready buffer no consumer has seen for `idle_hours` signals `idle`, an idle one
        older than `retire_after_days`, with no consumer since it went idle, signals `retire`. A staged buffer
        (spec §15) nobody approved for `retire_after_days` signals `retire` too; nothing else about it is judged,
        nothing serves it yet. A buffer that fails
        (an odd config, buffer-api refusing its status) is an error entry; the next buffer is still watched. Dry
        run, or no Temporal client (the CLI): only reported."""
        for buffer in await asyncio.to_thread(self.registry.buffers):
            latest = buffer.get("latest_event") or {}
            if not (ROUND_RE.fullmatch(buffer["id"]) and latest.get("kind") in ("ready", "idle", "staged")):
                continue
            try:
                await self._keeper_buffer(report, buffer, latest, now, dry_run)
            except Exception as error:  # one buffer's failure must not stop the watch of the others
                log.warning("keeper: buffer %s failed: %s", buffer["id"], error)
                report.errors.append(text(f"buffers: {buffer['id']}: {error}", 500))

    async def _keeper_buffer(self, report: KeeperReport, buffer: dict, latest: dict, now: float, dry_run: bool) -> None:
        from temporalio.service import RPCError, RPCStatusCode

        buffer_id, kind = buffer["id"], latest["kind"]
        config = BufferConfig.model_validate(buffer["config"])  # a config stored before D gets D's defaults
        signals: list[tuple[str, list[Any], str]] = []
        if kind == "staged":  # unapproved past retire_after_days: retire; a missing pyramid is not yet its problem
            if now - float(latest["at"]) > config.retire_after_days * 86400:
                signals.append(("retire", [None, buffer_id], ""))
            missing = []
        else:
            missing = (await asyncio.to_thread(self.registry.status, buffer_id))["missing"]  # stats the pyramids
        if missing:
            records = sorted({volume.split("/", 1)[0] for volume in missing})
            reason = "pyramid missing: " + ", ".join(missing)
            signals.append(("degraded", [DegradedSignal(records, text(reason, 500), buffer_id)], ",".join(records)))
        idle_s, at, seen = config.idle_hours * 3600, float(latest["at"]), buffer.get("consumers_seen")
        if kind == "ready" and now - at > idle_s and (seen is None or now - float(seen) > idle_s):
            signals.append(("idle", [None, buffer_id], ""))  # no label event; the buffer it is about
        unused = seen is None or float(seen) < at  # no consumer since the idle event: nothing is training on it
        if kind == "idle" and now - at > config.retire_after_days * 86400 and unused:
            signals.append(("retire", [None, buffer_id], ""))
        workflow_id = self.cfg.workflow_id(_issue_of(buffer_id))
        for name, args, detail in signals:
            line = f"{name} {buffer_id} {detail}".rstrip()
            if not dry_run and self.client is not None:
                try:
                    await self.client.get_workflow_handle(workflow_id).signal(name, args=args)
                except RPCError as error:
                    if error.status != RPCStatusCode.NOT_FOUND:
                        raise
                    log.info("keeper: no workflow %s for %s, %s not signalled", workflow_id, buffer_id, name)
                    continue
                log.info("keeper: signalled %s", line)
            report.signalled.append(line)

    def _job_finished(self, job_id: str) -> bool:
        """Whether csub says the job ended (or no longer knows it); a csub error counts as still running."""
        if self.jobs is None:
            return False
        try:
            statuses = self.jobs.client.status([job_id])
        except Exception as error:  # csub unreachable: leave the row for the next pass
            log.warning("keeper: csub status of job %s failed: %s", job_id, error)
            return False
        return not statuses or bool(statuses[0].terminal)

    async def _keeper_stale(self, report: KeeperReport, dry_run: bool) -> None:
        registry = self.registry
        rows = [
            {"kind": kind, **row}
            for kind in ("raw", "staged")
            for row in (await asyncio.to_thread(registry.assets, kind))["assets"]
        ]
        for kind, key, state in await asyncio.to_thread(stale_builds, rows, self._job_finished):
            if not dry_run:
                await asyncio.to_thread(registry.put_asset, kind, key, {"state": state, "job": None})
                log.info("keeper: %s %s was building under an ended job, now %s", kind, key, state)
            report.stale.append(f"{kind} {key} {state}")

    async def _keeper_building(
        self, report: KeeperReport, path: Path, roots: tuple[Path, ...], hold_s: float, now: float, dry_run: bool
    ) -> None:
        try:
            modified = path.stat().st_mtime
        except FileNotFoundError:
            return  # the job finished and renamed it since the scan
        if modified + hold_s >= now:
            return
        # a stage job writes deep inside its build, which leaves the build directory's own mtime old: spare a build
        # whose artifact row says `staging` (the stale-builds step has already reset one whose job ended)
        if building := BUILDING_DIR_RE.fullmatch(path.name):
            row = await asyncio.to_thread(self.registry.asset, "staged", building[1])
            if isinstance(row, dict) and row.get("state") == "staging":
                log.info("keeper: %s is old but its artifact is staging: kept", path)
                return
        if not dry_run:
            if not await asyncio.to_thread(evict_path, path, roots):
                raise RuntimeError(f"could not remove {path}")
            log.info("keeper: removed %s, untouched for %.0f h", path, (now - modified) / 3600)
        report.removed_building.append(str(path))

    async def _keeper_empty(
        self, report: KeeperReport, path: Path, roots: tuple[Path, ...], hold_s: float, now: float, dry_run: bool
    ) -> None:
        """Remove an empty raw directory (a job's mount point that never received a file) once it is older than
        `hold_hours`; a young one may be a job about to start. rmdir refuses a directory that filled meanwhile."""
        try:
            modified = path.stat().st_mtime
        except FileNotFoundError:
            return
        if modified + hold_s >= now or not any(path.resolve().is_relative_to(root.resolve()) for root in roots):
            return
        if not dry_run:
            try:
                await asyncio.to_thread(os.rmdir, path)
            except OSError as error:  # filled since the scan, or gone
                log.info("keeper: %s not removed: %s", path, error)
                return
            log.info("keeper: removed empty %s, untouched for %.0f h", path, (now - modified) / 3600)
        report.removed_empty.append(str(path))

    async def _keeper_evict(
        self, report: KeeperReport, kind: str, roots: tuple[Path, ...], hold_s: float, now: float, dry_run: bool
    ) -> None:
        """Evict `kind`'s free assets until under quota; when still over, say so on the newest buffer issue."""
        registry, limit = self.registry, getattr(self.cfg.quota, f"{kind}_bytes")
        if limit <= 0:
            return
        rows = (await asyncio.to_thread(registry.assets, kind))["assets"]
        in_progress: set[str] = set()
        if kind == "raw":  # a prep job may be reading the raw cache of a record mia-data is working on
            in_progress = {row["id"] for row in await asyncio.to_thread(registry.datasets, "in-progress")}
        rules: dict[str, Any] = {"hold_s": hold_s, "now": now, "in_progress": in_progress, "kind": kind}
        locks = lock_dir(self.cfg)

        def lock_key(row: dict) -> str:  # a buffer holds raw/<record> across its fetch and stage of the record
            return row["key"] if kind == "raw" else str((row.get("data") or {}).get("record") or row["key"])

        def spare(row: dict, owner: str) -> dict:  # a lock holder counts as a reference: plan around the row
            report.locked.append(f"{kind} {row['key']} ({owner})")
            return {**row, "refs": [*(row.get("refs") or []), f"lock {owner}"]}

        free = {row["key"] for row in free_assets(rows, **rules)}
        planned = []
        for row in rows:
            holder = locks.holder("raw", lock_key(row)) if row["key"] in free else None
            planned.append(spare(row, holder.owner) if holder and not holder.stale else row)
        picked = plan_evictions(
            planned,
            quota_bytes=limit,
            headroom_bytes=self.cfg.quota.headroom_bytes,
            demand_bytes=demand(rows, hold_s=hold_s, now=now) if kind == "raw" else 0,
            **rules,
        )
        evicted: list[dict] = []
        for row in picked:
            if not dry_run:
                _keeper_beat(f"evict {kind} {row['key']}")
                taken, owner = await asyncio.to_thread(
                    take_or_holder, locks, "raw", lock_key(row), KEEPER, KEEPER_LOCK_TTL_S
                )
                if not taken:  # a job took it since the plan
                    spare(row, owner)
                    continue
                try:
                    removed = await self._keeper_remove(report, kind, row, roots, now)
                finally:
                    await asyncio.to_thread(release_or_log, locks, "raw", lock_key(row), KEEPER)
                if not removed:
                    continue
            evicted.append(row)
            report.evicted.append(f"{kind} {row['key']}")
        over_by = int(live_bytes(rows) - live_bytes(evicted) - limit)
        if over_by > 0 and not dry_run:
            report.over_quota[kind] = over_by
            await self._over_quota_comment(kind, over_by)

    async def _keeper_remove(
        self, report: KeeperReport, kind: str, row: dict, roots: tuple[Path, ...], now: float
    ) -> bool:
        """Remove one asset's directory and mark its row `evicted`. A refusal leaves the row; a removal that failed
        half way marks it damaged (`partial` raw, a fetch resumes it; `missing` staged, it is staged again)."""
        data, key, registry = row["data"], row["key"], self.registry
        path = data.get("path")
        try:
            removed = isinstance(path, str) and await asyncio.to_thread(evict_path, Path(path), roots)
        except OSError as error:
            damaged = "partial" if kind == "raw" else "missing"
            report.errors.append(text(f"evict {kind}: removing {key} at {path} failed, now {damaged}: {error}", 500))
            await asyncio.to_thread(registry.put_asset, kind, key, {"state": damaged, "job": None})
            return False
        if not removed:
            report.errors.append(text(f"evict {kind}: refused to remove {key} at {path}", 500))
            return False
        await asyncio.to_thread(registry.put_asset, kind, key, {"state": "evicted", "evicted_at": now, "bytes": 0})
        log.info("keeper: evicted %s %s (%s bytes, last used %s)", kind, key, data.get("bytes"), data.get("last_used"))
        return True

    async def _over_quota_comment(self, kind: str, over_by: int) -> None:
        """Comment once on the newest open issue labelled `buffer:*`; with none open, only log."""
        prefix = self.cfg.label("")
        issues = [
            issue
            for issue in await self.gh_issues.list_issues(state="open")
            if any(str((label or {}).get("name", "")).startswith(prefix) for label in issue.get("labels") or [])
        ]
        message = OVER_QUOTA.format(kind=kind, over_by=over_by)
        if not issues:
            log.warning("keeper: %s (no open buffer issue to tell)", message)
            return
        marker = self.cfg.marker(f"keeper over-quota {kind}")
        newest = max(issues, key=lambda issue: issue["number"])
        await self._comment_once(self.gh_issues, newest["number"], f"{message}\n\n{marker}", marker)


def _issue_of(round_id: str) -> int:
    """The issue number of a round (buffer) id `i<issue>-e<event>`."""
    if not ROUND_RE.fullmatch(round_id):
        raise ValueError(f"bad round id {round_id!r}")
    return int(round_id[1:].split("-", 1)[0])


def _keeper_beat(detail: str) -> None:
    """Heartbeat when running as a Temporal activity (not under `mia-buffers assets sync`)."""
    if activity.in_activity():
        activity.heartbeat(detail)


def _stage_json(staged: Path) -> Any:
    """The stage job's stage.json (untrusted job output, read within STAGE_JSON_LIMIT), or None."""
    return loads(read_untrusted(staged, "stage.json", STAGE_JSON_LIMIT))
