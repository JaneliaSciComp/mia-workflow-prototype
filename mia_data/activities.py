"""The mia-data Temporal activities: the only code that touches GitHub, csub and the filesystem.

The worker keeps each round's trusted inputs (record, downloads entry, pins, checked bindings) under
state_dir/rounds/<round_id>, which is never mounted into a job. Jobs get copies in their workspaces under
work_root. Every file a job wrote is read back with `read_untrusted` and parsed defensively, and the
results that enter workflow history are the small dataclasses in models.py.
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import hashlib
import json
import logging
import os
import re
import shutil
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml
from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.activities import PERMANENT_CSUB, neutralize, permanent_errors
from autodev.jobs import JobSpec, read_untrusted

from . import catalog
from . import publish as crop_publish
from .binding import check_binding, hosts, record_urls
from .config import HUMAN_LABELS, STAGES
from .gallery import gallery_files, publish_gallery
from .jobspec import (
    SHA_RE,
    HostsNotAllowed,
    PinMoved,
    check_hosts_allowed,
    crop_job,
    harness_pin,
    harness_snapshot,
    match_job,
    match_prompt,
    smoke_job,
    snapshot,
    verify_job,
)
from .models import (
    DatasetState,
    GalleryResult,
    JobArgs,
    JobDone,
    MatchResult,
    Pins,
    PrUpdate,
    RoundLimits,
    SmokeResult,
    VerifyResult,
)
from .poll import run_poll
from .publish import CROP_ID_RE, Conflict
from .seed import ID_RE, MARKER_RE
from .status import render_binding, render_crops_table, render_links, render_stage

if TYPE_CHECKING:
    from temporalio.client import Client

    from autodev.github import GitHub
    from autodev.jobs import CsubJobs

    from .config import Config

log = logging.getLogger(__name__)

TEST_PR_RE = re.compile(r"i0-e(\d+)-[0-9a-f]{7}")  # `mia-data test-pr`'s round id: PR <n> at a head, no data issue
ROUND_RE = re.compile(rf"i\d+-e\d+|{TEST_PR_RE.pattern}")
KIND_RE = re.compile(r"[a-z][a-z0-9-]{0,60}")  # a job kind, optionally with a suffix (crop-<sha>)
HEX64 = re.compile(r"[0-9a-f]{64}")
NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # a snippet directory
CROP_STATUS = {
    "pending": "test crops running",
    "success": "test crops passed",
    "failure": "test crops failed",
    "error": "test crops could not run",
}
PERMANENT = (
    HostsNotAllowed,
    PinMoved,
    Conflict,
    ValueError,
    FileNotFoundError,
    yaml.YAMLError,
)  # inputs that a retry can't fix


def mia_permanent_errors[Fn: Callable[..., Awaitable]](fn: Fn) -> Fn:
    """Decorator: autodev's permanent_errors, plus mia-data's own permanent failures.

    A moved pin, a host outside the policy, a publication conflict, a bad input (malformed YAML too) or a
    missing state file send the round to needs-human instead of burning retries."""
    inner = permanent_errors(fn)

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await inner(*args, **kwargs)
        except PERMANENT as error:
            raise ApplicationError(
                f"{type(error).__name__}: {error}", type=type(error).__name__, non_retryable=True
            ) from error

    return cast(Fn, wrapper)


def write_atomic(path: Path, data: str | bytes) -> None:
    """Write via a temp file and rename, so a crash never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(data.encode() if isinstance(data, str) else data)
    os.replace(tmp, path)


def _text(value: Any, limit: int = 2000) -> str:
    """A job-reported value as a bounded string ("" for None)."""
    return "" if value is None else str(value)[:limit]


def _texts(value: Any, limit: int = 20) -> list[str]:
    """A job-reported value as a bounded list of bounded strings (a scalar becomes a one-item list)."""
    items = value if isinstance(value, list) else [] if value is None else [value]
    return [_text(item, 500) for item in items[:limit]]


def _small_dict(value: Any, limit: int = 16384) -> dict | None:
    """Job-reported JSON goes into workflow history only as a small dict (the result fields are typed dict)."""
    try:
        return value if isinstance(value, dict) and len(json.dumps(value)) <= limit else None
    except (TypeError, ValueError):
        return None


def _loads(text: str | bytes | None) -> Any:
    """json.loads that returns None instead of raising."""
    try:
        return json.loads(text or "null")
    except (ValueError, RecursionError):
        return None


def _job_json(workspace: str | Path, rel: str) -> Any:
    """A JSON file a job wrote, or None if missing, unreadable or malformed."""
    return _loads(read_untrusted(Path(workspace), rel, 1 << 20))


def _fenced(text: str, lang: str = "") -> str:
    """`text` in a markdown code fence longer than any backtick run inside it."""
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", text)), default=0))
    return f"{fence}{lang}\n{text.rstrip()}\n{fence}"


def withdrawn_snippets(snapshot_dir: Path) -> dict[str, str]:
    """snippet name -> reason, from the snapshot's WITHDRAWN.yaml."""
    entries = yaml.safe_load(read_untrusted(snapshot_dir, "WITHDRAWN.yaml", 1 << 20) or "") or []
    return {
        entry["name"]: _text(entry.get("reason"))
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("name"), str)
    }


def visible_snippets(snapshot_dir: Path) -> dict[str, str]:
    """snippet name -> its snippet.yaml text, hiding withdrawn snippets and ones another snippet supersedes."""
    root = snapshot_dir / "snippets"
    catalog_text = {
        entry.name: text
        for entry in sorted(root.iterdir() if root.is_dir() else [])
        if (text := read_untrusted(snapshot_dir, f"snippets/{entry.name}/snippet.yaml", 1 << 20)) is not None
    }
    hidden = set(withdrawn_snippets(snapshot_dir))
    for text in catalog_text.values():
        parsed = yaml.safe_load(text)
        superseded = parsed and isinstance(parsed, dict) and parsed.get("supersedes")
        if isinstance(superseded, str):
            hidden.add(superseded)
        elif isinstance(superseded, list):
            hidden |= {name for name in superseded if isinstance(name, str)}
    return {name: text for name, text in catalog_text.items() if name not in hidden}


class Activities:
    """The activity implementations, bound to the three GitHub clients (data, snippets and catalog repos),
    the csub job runner and the Temporal client.

    `bot` is the GitHub login mia-data posts as. `probe` returns the csub policy's allowed_hosts patterns;
    it is injectable for tests."""

    def __init__(
        self,
        cfg: Config,
        gh_data: GitHub,
        gh_snippets: GitHub,
        gh_catalog: GitHub,
        jobs: CsubJobs,
        bot: str,
        client: Client | None = None,
        probe: Callable[[], list[str]] | None = None,
    ):
        self.cfg, self.gh_data, self.gh_snippets, self.gh_catalog = cfg, gh_data, gh_snippets, gh_catalog
        self.jobs, self.bot, self.client = jobs, bot, client
        self.probe = probe or (lambda: list(jobs.client.probe().allowed_hosts))
        # ponytail: in memory, so a worker restart re-sends each PR once (the workflow dedupes by pr_state)
        self._sent: dict[int, tuple] = {}  # poll: PR number -> the (head, state, merge, needs_human) last signalled

    @property
    def temporal(self) -> Client:
        """The Temporal client; only the worker's activities (poll) have one."""
        if self.client is None:
            raise RuntimeError("no Temporal client: this activity runs only inside the worker")
        return self.client

    def all(self) -> list:
        """The main queue's activities; await_job runs on the -await queue."""
        return [
            self.admit,
            self.limits,
            self.poll,
            self.set_stage,
            self.comment,
            self.prepare_match,
            self.prepare_job,
            self.probe_hosts,
            self.submit,
            self.read_match,
            self.read_smoke,
            self.read_verify,
            self.publish_gallery,
            self.publish_crops,
            self.refuse_label,
            self.open_snippet_issue,
            self.pr_state,
            self.set_crop_status,
            self.tree_sha,
            self.adopt_pr_binding,
        ]

    # --- helpers -----------------------------------------------------------------------------

    def _clean(self, text: str) -> str:
        return neutralize(text, self.gh_data.tokens())

    async def _bot_comment(self, gh: GitHub, number: int, marker: str) -> dict | None:
        """The bot's own comment carrying `marker` (an outsider's copy must not count)."""
        return next(
            (
                comment
                for comment in await gh.issue_comments(number)
                if (comment.get("user") or {}).get("login") == self.bot and marker in (comment.get("body") or "")
            ),
            None,
        )

    async def _comment_once(self, gh: GitHub, number: int, body: str, marker: str) -> str | None:
        """Post the comment unless one with this marker exists. Returns the comment's URL (the earlier one, on a retry)."""
        return (await self._bot_comment(gh, number, marker) or await gh.comment(number, body)).get("html_url")

    def _round_dir(self, round_id: str) -> Path:
        """state_dir/rounds/<round_id>: the round's record, downloads entry, pins and bindings (never mounted)."""
        if not ROUND_RE.fullmatch(round_id):
            raise ValueError(f"bad round id {round_id!r}")
        return self.cfg.paths.state_dir / "rounds" / round_id

    def _pins(self, round_id: str) -> Pins:
        return Pins(**json.loads((self._round_dir(round_id) / "pins.json").read_text()))

    def _round_workspace(self, issue: int, round_id: str) -> Path:
        """work_root/issue-<n>/<round>, the parent of the round's job workspaces; a test-pr round's is
        work_root/test-pr/<pr>/<head>."""
        round_dir = self._round_dir(round_id)
        if match := TEST_PR_RE.fullmatch(round_id):
            return self.cfg.paths.work_root / "test-pr" / match.group(1) / self._pins(round_id).snippets_commit
        return self.cfg.paths.work_root / f"issue-{int(issue)}" / round_dir.name

    def _make_workspace(self, issue: int, round_id: str, kind: str, attempt: int) -> Path:
        """Create (if needed) and return the workspace for one job: <round workspace>/<kind>-<attempt>."""
        if not (KIND_RE.fullmatch(kind) and type(attempt) is int and attempt >= 1):
            raise ValueError(f"bad job kind/attempt {kind!r}/{attempt!r}")
        workspace = self._round_workspace(issue, round_id) / f"{kind}-{attempt}"
        workspace.mkdir(parents=True, exist_ok=True)
        return workspace

    def _job_workspace(self, issue: int, round_id: str, workspace: str) -> Path:
        """Check that a workspace path (an activity argument) really is one of this issue's round."""
        path = Path(workspace)
        if path.parent != self._round_workspace(issue, round_id) or path.is_symlink():
            raise ValueError(f"{workspace} is not a workspace of issue {issue} round {round_id}")
        return path

    @staticmethod
    def _crops_dir(workspace: Path) -> Path:
        """The crops/ directory a crop job wrote, which must be a real directory inside its workspace."""
        crops_dir = workspace / "crops"
        if crops_dir.is_symlink() or not crops_dir.is_dir():
            raise ValueError(f"{crops_dir} is not a directory")
        return crops_dir

    def _record(self, round_id: str) -> dict:
        return yaml.safe_load((self._round_dir(round_id) / "record.yaml").read_text()) or {}

    def _dataset_dir(self, round_id: str, pins: Pins) -> Path:
        """Where the dataset's crops are published under storage_root."""
        return self.cfg.paths.storage_root / catalog.dataset_folder(self._record(round_id), pins.record)

    async def _snapshot(self, sha: str) -> Path:
        return await asyncio.to_thread(snapshot, self.cfg, sha, self.gh_snippets.tokens)

    # --- admission and display ---------------------------------------------------------------

    @activity.defn(name="admit")
    @mia_permanent_errors
    async def admit(self, issue: int, event_id: str) -> Pins:
        """Pin the round: the record id from the issue's first-line marker, record_commit from the downloads
        state, snippets main, and the harness checkout. Written once to state_dir/rounds/<round_id>/; a retry
        returns the same pins."""
        if not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"bad event id {event_id!r}")
        round_dir = self._round_dir(f"i{int(issue)}-e{event_id}")
        if (round_dir / "pins.json").is_file():
            return Pins(**json.loads((round_dir / "pins.json").read_text()))
        match = MARKER_RE.match((await self.gh_data.get_issue(issue)).get("body") or "")
        if not (match and ID_RE.fullmatch(match.group(1))):
            raise ValueError(f"issue #{issue} has no valid record marker on its first line")
        record_id = match.group(1)
        entry = (await catalog.downloaded(self.gh_catalog)).get(record_id)
        if not (entry and SHA_RE.fullmatch(str(entry.get("record_commit")))):
            raise ValueError(f"{record_id}: not in the downloads state, or no record_commit")
        path = (await catalog.record_paths(self.gh_catalog)).get(record_id)
        if path is None:
            raise ValueError(f"{record_id}: no record on main")
        record = await catalog.record(self.gh_catalog, path, entry["record_commit"])
        pins = Pins(
            record_id,
            entry["record_commit"],
            await self.gh_snippets.get_ref_sha("main"),
            *await asyncio.to_thread(harness_pin, self.cfg),
        )
        await asyncio.to_thread(harness_snapshot, self.cfg, pins.harness_commit)  # freeze the source now
        write_atomic(round_dir / "record.yaml", yaml.safe_dump(record, sort_keys=False))
        write_atomic(round_dir / "downloads.json", json.dumps(entry, indent=1))
        write_atomic(round_dir / "pins.json", json.dumps(dataclasses.asdict(pins)))  # last: marks the round admitted
        return pins

    @activity.defn(name="limits")
    async def limits(self) -> RoundLimits:
        """The config values the workflow needs but cannot read itself."""
        cfg = self.cfg
        return RoundLimits(
            cfg.crops.test_per_round, cfg.crops.per_round, cfg.limits.job_walltime_min, cfg.limits.max_pr_revisions
        )

    @activity.defn(name="set_stage")
    @mia_permanent_errors
    async def set_stage(
        self, issue: int, stage: str, state: DatasetState | None = None, detail: str = "", extra: str = ""
    ) -> None:
        """Swap the `data:<stage>` label and, within a round, post the stage's comment once.

        The comment has the stage line, `detail` (neutralized), the caller's `extra` (crops table, totals, PR
        link) and links to the folders to look at. Comments are never edited: each stage and result is a new one."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}")
        gh, want = self.gh_data, self.cfg.label(stage)
        current = {label["name"] for label in (await gh.get_issue(issue)).get("labels", [])}
        for other in STAGES:
            if (name := self.cfg.label(other)) in current and name != want:
                await gh.remove_label(issue, name)
        if want not in current:
            await gh.add_labels(issue, [want])
        if not (state and state.round_id):
            return
        round_id, paths = state.round_id, self.cfg.paths
        links = {"Workspace": paths.link(self._round_workspace(issue, round_id))}
        if stage == "done":
            pins = self._pins(round_id)
            links["Crops"] = paths.link(self._dataset_dir(round_id, pins))
            links["Gallery"] = (
                f"https://github.com/{gh.repo}/tree/{self.cfg.gallery.branch}/galleries/{pins.record}/{round_id}"
            )
        marker = self.cfg.marker(f"stage {round_id} {stage}")
        parts = [render_stage(stage, self._clean(detail)), self._clean(extra) if extra else "", render_links(links)]
        await self._comment_once(gh, issue, "\n".join(part for part in parts if part) + f"\n{marker}", marker)

    @activity.defn(name="comment")
    @mia_permanent_errors
    async def comment(
        self,
        issue: int,
        body: str,
        marker: str,
        repo: str = "data",
        binding_path: str | None = None,
        binding_hash: str | None = None,
    ) -> str | None:
        """Post once per marker, on a data issue or (repo="snippets") a snippets issue or PR, with the binding
        appended when one is given. Returns the comment's URL."""
        full_marker = self.cfg.marker(marker)
        body = self._clean(body) + (f"\n\n{render_binding(binding_path, binding_hash)}" if binding_path else "")
        return await self._comment_once(
            self.gh_snippets if repo == "snippets" else self.gh_data, issue, f"{body}\n\n{full_marker}", full_marker
        )

    @activity.defn(name="refuse_label")
    @mia_permanent_errors
    async def refuse_label(self, issue: int, label: str, reason: str, event_id: str) -> None:
        """Remove a human label we won't act on and say why, once per event (busy `go`, stray `approve`, ...)."""
        if label not in {self.cfg.label(kind) for kind in HUMAN_LABELS} or not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"refuse_label: bad label/event {label!r}/{event_id!r}")
        await self.gh_data.remove_label(issue, label)
        marker = self.cfg.marker(f"refused {label} {event_id}")
        await self._comment_once(self.gh_data, issue, f"Ignoring `{label}`: {self._clean(reason)}\n\n{marker}", marker)

    # --- jobs --------------------------------------------------------------------------------

    @activity.defn(name="prepare_match")
    @mia_permanent_errors
    async def prepare_match(self, issue: int, round_id: str, pins: Pins, attempt: int = 1) -> str:
        """Make the match agent's workspace: prompt.md, record.yaml, downloads.json, the downloader's
        conversion.json / manifest.json, catalog/<name>.yaml (visible snippets only) and SPEC.md. Returns its path."""
        workspace, round_dir = self._make_workspace(issue, round_id, "match", attempt), self._round_dir(round_id)
        snapshot_dir = await self._snapshot(pins.snippets_commit)
        shutil.rmtree(workspace / "catalog", ignore_errors=True)
        (workspace / "catalog").mkdir()
        for name, text in visible_snippets(snapshot_dir).items():
            (workspace / "catalog" / f"{name}.yaml").write_text(text)
        shutil.copyfile(round_dir / "record.yaml", workspace / "record.yaml")
        shutil.copyfile(round_dir / "downloads.json", workspace / "downloads.json")
        dataset_dir = json.loads((round_dir / "downloads.json").read_text()).get("dataset_dir")
        for name in ("conversion.json", "manifest.json"):
            if isinstance(dataset_dir, str) and (data := read_untrusted(Path(dataset_dir), name, 16 << 20)) is not None:
                (workspace / name).write_text(data)
        (workspace / "SPEC.md").write_text(read_untrusted(snapshot_dir, "SPEC.md", 1 << 20) or "")
        (workspace / "prompt.md").write_text(match_prompt(pins))
        return str(workspace)

    @activity.defn(name="prepare_job")
    @mia_permanent_errors
    async def prepare_job(self, issue: int, round_id: str, kind: str, attempt: int, binding: str | None) -> str:
        """Make a smoke/crop/verify workspace with record.yaml and (copied from state_dir) binding.yaml.
        Returns its path."""
        workspace, round_dir = self._make_workspace(issue, round_id, kind, attempt), self._round_dir(round_id)
        shutil.copyfile(round_dir / "record.yaml", workspace / "record.yaml")
        if binding:
            if not Path(binding).resolve().is_relative_to(round_dir.resolve()):
                raise ValueError(f"binding {binding} is not one of round {round_id}'s")
            shutil.copyfile(binding, workspace / "binding.yaml")
        return str(workspace)

    @activity.defn(name="probe_hosts")
    @mia_permanent_errors
    async def probe_hosts(self) -> list[str]:
        """`csub probe`: the policy's allowed_hosts patterns."""
        return list(await asyncio.to_thread(self.probe))

    def _spec(self, kind: str, args: JobArgs, snapshot_dir: Path | None) -> JobSpec:
        """Build the JobSpec for one job kind from its arguments."""
        cfg = self.cfg
        if kind == "match":
            return match_job(cfg, args)
        existing = self._dataset_dir(args.round_id, args.pins) / "manifest.json" if args.existing else None
        existing = existing if existing and existing.is_file() else None
        if kind in ("smoke", "crop"):
            if snapshot_dir is None:
                raise ValueError(f"{kind} job needs a snippets snapshot")
            if kind == "smoke":
                return smoke_job(cfg, args, snapshot_dir)
            return crop_job(cfg, args, snapshot_dir, existing)
        if kind == "verify":
            if args.crop_workspace is None:
                raise ValueError("verify job needs crop_workspace")
            crop_workspace = Path(args.crop_workspace)
            if crop_workspace.parent != Path(args.workspace).parent:
                raise ValueError(f"crop workspace {args.crop_workspace} is not of this round")
            return verify_job(cfg, args, self._crops_dir(crop_workspace), existing)
        raise ValueError(f"unknown job kind {kind!r}")

    @activity.defn(name="submit")
    @mia_permanent_errors
    async def submit(self, key: str, kind: str, args: JobArgs) -> str:
        """Build the job (the builders refuse a moved harness pin), refuse hosts a fresh `csub probe` doesn't
        allow, and submit at most once per key. Returns the job id."""
        from csub import CsubError  # pixi-only dependency, imported where used (as autodev does)

        patterns = await asyncio.to_thread(self.probe)
        snapshot_dir = (
            await self._snapshot(args.sha or args.pins.snippets_commit) if kind in ("smoke", "crop") else None
        )

        def build() -> JobSpec:
            spec = self._spec(kind, args, snapshot_dir)
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
        outcome = await self.jobs.wait(job_id, workspace, activity.heartbeat)
        return JobDone(outcome.job_id, outcome.state, outcome.exit_code)

    # --- job results -------------------------------------------------------------------------

    @activity.defn(name="read_match")
    @mia_permanent_errors
    async def read_match(self, workspace: str) -> MatchResult:
        """Read the match agent's outcome.json and, for matched/ineligible, check its binding.yaml.

        A checked binding is stored (with the round's snippets_commit) as
        state_dir/rounds/<round>/binding-<attempt>.yaml. Anything we can't trust is `blocked` with the reason."""
        path = Path(workspace)
        if not (match := re.fullmatch(r"match-(\d+)", path.name)):
            raise ValueError(f"{workspace} is not a match workspace")
        round_id = path.parent.name
        round_dir, pins = self._round_dir(round_id), self._pins(round_id)
        outcome = _job_json(workspace, "outcome.json")
        if not isinstance(outcome, dict):
            return MatchResult("blocked", "outcome.json is missing, unreadable or not a JSON object")
        kind = outcome.get("outcome")
        if kind == "blocked":
            return MatchResult(
                "blocked", _text(outcome.get("reason")) or "the match agent reported blocked, with no reason"
            )
        if kind == "needs_snippet":
            draft = _small_dict({key: outcome.get(key) for key in ("draft_binding", "what_fails", "tried")}, 32768)
            return MatchResult("needs_snippet", draft=draft or {"what_fails": "(the agent's draft was too large)"})
        if kind not in ("matched", "ineligible"):
            return MatchResult("blocked", f"unknown match outcome {_text(kind, 100)!r}")
        binding, errors = await self._check_binding(
            read_untrusted(path, "binding.yaml", 1 << 20), round_id, pins, pins.snippets_commit
        )
        if errors:
            return MatchResult("blocked", _text("binding check failed: " + "; ".join(errors)))
        binding_path, digest = self._store_binding(
            round_dir / f"binding-{match.group(1)}.yaml", {**binding, "snippets_commit": pins.snippets_commit}
        )
        return MatchResult(
            kind,
            binding_path=binding_path,
            binding_hash=digest,
            shapes=_small_dict(outcome.get("shapes")) if kind == "ineligible" else None,
            reasons=_texts(binding.get("reasons")),
        )

    async def _check_binding(self, text: str | None, round_id: str, pins: Pins, sha: str) -> tuple[dict, list[str]]:
        """Parse an agent-written binding and check it against the round's record and the snippets at `sha`.
        Returns (binding, errors); the binding is empty when there are errors."""
        if text is None:
            return {}, ["the binding is missing or unreadable"]
        try:
            binding = yaml.safe_load(text)
            errors = check_binding(
                binding, self._record(round_id), set(visible_snippets(await self._snapshot(sha))), pins=pins
            )
            if not errors:  # hosts() raises if a URL would grant a GitHub host
                hosts(
                    [url for array in binding["raw"] + (binding.get("labels") or []) for url in array["urls"]], self.cfg
                )
        except (yaml.YAMLError, ValueError, RecursionError) as error:  # a malformed URL raises in urlsplit
            errors = [f"{type(error).__name__}: {error}"]
        return binding if not errors else {}, errors

    @staticmethod
    def _store_binding(path: Path, binding: dict) -> tuple[str, str]:
        """Write the binding as YAML. Returns (path, sha256 of the written bytes)."""
        write_atomic(path, data := yaml.safe_dump(binding, sort_keys=False).encode())
        return str(path), hashlib.sha256(data).hexdigest()

    @activity.defn(name="read_smoke")
    @mia_permanent_errors
    async def read_smoke(self, workspace: str) -> SmokeResult:
        """Read the smoke job's smoke.json into a SmokeResult, tolerating any shape."""
        report = _job_json(workspace, "smoke.json")
        if not isinstance(report, dict):
            return SmokeResult(False, errors=["smoke.json is missing, unreadable or not a JSON object"])
        fetched = report.get("bytes_fetched")
        return SmokeResult(
            report.get("ok") is True,
            report.get("eligible") is True,
            _small_dict(report.get("shapes")),
            _texts(report.get("errors")),
            _texts(report.get("warnings")),
            fetched if type(fetched) is int and fetched >= 0 else 0,
            _text(report.get("host_not_allowed"), 300) or None,
        )

    @activity.defn(name="read_verify")
    @mia_permanent_errors
    async def read_verify(self, workspace: str, crop_workspace: str | None = None) -> VerifyResult:
        """Read verify.json: only crops with ok, a valid id and sha256s for crop.json and raw.png count.

        Top-level errors or n == 0 mean nothing was verified. With `crop_workspace`, n_effective and
        bytes_fetched come from the crop job's report.json."""
        report = _job_json(workspace, "verify.json")
        if not isinstance(report, dict):
            return VerifyResult(0, 0, ["verify.json is missing, unreadable or not a JSON object"], [])
        errors = _texts(report.get("errors"))
        n_asked, crops = report.get("n"), report.get("crops")
        n_crops = n_asked if type(n_asked) is int and n_asked >= 0 else 0
        verified, failed, seen = [], [], set()
        for crop in (crops if isinstance(crops, list) else [])[:1000]:
            if not isinstance(crop, dict):
                continue
            crop_id, files = crop.get("crop_id"), crop.get("files")
            if (
                crop.get("ok") is True
                and isinstance(crop_id, str)
                and CROP_ID_RE.fullmatch(crop_id)
                and crop_id not in seen
                and isinstance(files, dict)
                and len(files) <= 64
                and {"crop.json", "raw.png"} <= files.keys()
                and all(
                    isinstance(rel, str) and isinstance(digest, str) and HEX64.fullmatch(digest)
                    for rel, digest in files.items()
                )
            ):
                verified.append({"crop_id": crop_id, "files": files})
                seen.add(crop_id)
            else:
                failed.append(f"{_text(crop_id, 100)}: {'; '.join(_texts(crop.get('errors'), 5)) or 'not verified'}")
        if not n_crops and not errors:
            errors = ["verify checked no crops"]
        if errors:
            verified = []
        crop_report = _job_json(crop_workspace, "crops/report.json") if crop_workspace else None
        crop_report = crop_report if isinstance(crop_report, dict) else {}

        def count(key: str) -> int | None:
            value = crop_report.get(key)
            return value if type(value) is int and value >= 0 else None

        return VerifyResult(
            len(verified), n_crops, (errors + failed)[:20], verified, count("n"), count("bytes_fetched") or 0
        )

    # --- publication -------------------------------------------------------------------------

    @activity.defn(name="publish_gallery")
    @mia_permanent_errors
    async def publish_gallery(
        self, issue: int, round_id: str, kind: str, crop_workspace: str, verify_workspace: str, verified: list[dict]
    ) -> GalleryResult:
        """Publish raw.png + overlay.png of the verified crops, with the binding and verify report, under
        galleries/<record>/<round>/<kind> (kind = the tested SHA, or production) on the galleries branch; a
        test-pr round's go under galleries/_test-pr/<record>/<pr>/<kind>. Returns the URLs and a crops table."""
        if not (kind == "production" or SHA_RE.fullmatch(kind)):
            raise ValueError(f"bad gallery kind {kind!r}")
        crop_dir = self._job_workspace(issue, round_id, crop_workspace)
        verify_dir = self._job_workspace(issue, round_id, verify_workspace)
        record_id, test_pr = self._pins(round_id).record, TEST_PR_RE.fullmatch(round_id)
        prefix = (
            f"galleries/_test-pr/{record_id}/{test_pr.group(1)}/{kind}"
            if test_pr
            else f"galleries/{record_id}/{round_id}/{kind}"
        )
        files = await asyncio.to_thread(gallery_files, crop_dir, verify_dir, verified)
        urls = await publish_gallery(self.gh_data, self.cfg.gallery.branch, prefix, files)
        rows = []
        for crop in verified:
            crop_id = crop["crop_id"]
            raw = read_untrusted(crop_dir, f"crops/{crop_id}/crop.json", 1 << 20, binary=True)
            if raw is None or hashlib.sha256(raw).hexdigest() != crop["files"]["crop.json"]:
                raise ValueError(f"{crop_id}/crop.json is missing or differs from the verified checksum")
            crop_json = parsed if isinstance(parsed := _loads(raw), dict) else {}
            rows.append(
                {
                    "crop_id": crop_id,
                    "plane": crop_json.get("plane") if isinstance(crop_json.get("plane"), dict) else {},
                    "box": crop_json.get("box") if isinstance(crop_json.get("box"), dict) else {},
                    "raw_url": urls[f"{crop_id}/raw.png"],
                    "overlay_url": urls[f"{crop_id}/overlay.png"],
                }
            )
        return GalleryResult(prefix, urls, render_crops_table(rows))

    @activity.defn(name="publish_crops")
    @mia_permanent_errors
    async def publish_crops(
        self, issue: int, round_id: str, crop_workspace: str, verified: list[dict], sha: str
    ) -> dict:
        """Publish the verified crops into the dataset folder, marking crops of snippets withdrawn in
        mia-snippets at `sha`. Returns {"published", "already", "total"}."""
        crops_dir = self._crops_dir(self._job_workspace(issue, round_id, crop_workspace))
        withdrawn = withdrawn_snippets(await self._snapshot(sha))
        timeout = activity.info().start_to_close_timeout if activity.in_activity() else None
        return await asyncio.to_thread(
            crop_publish.publish,
            self._dataset_dir(round_id, self._pins(round_id)),
            crops_dir,
            verified,
            withdrawn,
            tmp_max_age_s=2 * (timeout.total_seconds() if timeout else 3600),
        )

    # --- snippet development -----------------------------------------------------------------

    @activity.defn(name="open_snippet_issue")
    @mia_permanent_errors
    async def open_snippet_issue(self, issue: int, round_id: str, draft: dict, snippet_issue: int | None = None) -> int:
        """Open the mia-snippets issue asking autodev for a new snippet, or find it again: the one the workflow
        remembers as `snippet_issue` from an earlier round, while it is still open, else by this round's
        first-line marker on a retry. Returns the issue number."""
        gh, github_cfg, pins = self.gh_snippets, self.cfg.github, self._pins(round_id)
        marker = self.cfg.marker(f"issue={int(issue)} round={round_id}")

        def ours(existing: dict, prefix: str) -> bool:  # filter by author: an outsider can copy the marker
            return (existing.get("user") or {}).get("login") == self.bot and (existing.get("body") or "").startswith(
                prefix
            )

        if snippet_issue:  # any round's marker for this data issue will do: the round id is new on a re-trigger
            remembered = await gh.get_issue(int(snippet_issue))
            any_round = self.cfg.marker(f"issue={int(issue)} round=").removesuffix(" -->")
            if remembered.get("state") != "closed" and ours(remembered, any_round):
                return remembered["number"]
        # ponytail: lists every issue; one call per round.
        for existing in await gh.list_issues(state="all"):
            if ours(existing, marker):
                return existing["number"]
        record = self._record(round_id)
        urls = record_urls(record)
        data_format = _text(((record.get("data") or {}).get("formats") or ["unknown"])[0], 40)
        # ponytail: container guessed from the record's URLs; the agent's what_fails is the authority
        container = (
            "zip"
            if any("::" in url or url.split("?")[0].endswith(".zip") for url in urls)
            else "s3"
            if any(url.startswith("s3://") for url in urls)
            else "http"
        )
        excerpt = yaml.safe_dump(
            {key: record.get(key) for key in ("title", "imaging", "data", "technical")}, sort_keys=False
        )
        draft = draft or {}
        body = "\n\n".join(
            [
                marker,
                f"For [{github_cfg.data_repo}#{issue}](https://github.com/{github_cfg.data_repo}/issues/{issue}): "
                f"dataset `{pins.record}` has no snippet that reads it.",
                f"Read [SPEC.md](https://github.com/{github_cfg.snippets_repo}/blob/main/SPEC.md) first, and add the "
                f"binding you test as `examples/{pins.record}.yaml`.",
                f"**Record** (at `{pins.record_commit}`):",
                _fenced(self._clean(excerpt[:8000]), "yaml"),
                "**What fails** (the match agent's report):",
                _fenced(self._clean(_text(draft.get("what_fails"), 4000))),
                "**Tried:**",
                _fenced(self._clean(json.dumps(draft.get("tried"), indent=1)[:8000]), "json"),
                "**Draft binding:**",
                _fenced(self._clean(yaml.safe_dump(draft.get("draft_binding"), sort_keys=False)[:16000]), "yaml"),
            ]
        )
        title = f"New snippet for {self._clean(data_format)}/{container}: {self._clean(pins.record)}"
        return (await gh.create_issue(title, body, ["snippets:go"]))["number"]

    @activity.defn(name="pr_state")
    @mia_permanent_errors
    async def pr_state(self, pr: int) -> PrUpdate:
        """The PR's current head, state and merge SHA, read fresh, and whether its issue needs a human."""
        pr_data = await self.gh_snippets.get_pr(pr)
        match = re.fullmatch(r"snippets/issue-(\d+)", pr_data["head"]["ref"])
        if not match or ((pr_data["head"].get("repo") or {}).get("full_name")) != self.gh_snippets.repo:
            raise ValueError(f"PR #{pr} is not on a snippets/issue-<K> branch of {self.gh_snippets.repo}")
        snippet_issue, merged = int(match.group(1)), bool(pr_data.get("merged"))
        labels = {label["name"] for label in (await self.gh_snippets.get_issue(snippet_issue)).get("labels", [])}
        return PrUpdate(
            pr,
            pr_data["head"]["sha"],
            "merged" if merged else pr_data["state"],
            pr_data.get("merge_commit_sha") if merged else None,
            snippet_issue,
            "snippets:needs-human" in labels,
            pr_data.get("html_url"),
        )

    @activity.defn(name="set_crop_status")
    @mia_permanent_errors
    async def set_crop_status(self, sha: str, state: str, url: str | None, description: str = "") -> None:
        """Post the `mia-data/crops` commit status on a snippets PR head."""
        if not SHA_RE.fullmatch(sha) or state not in CROP_STATUS:
            raise ValueError(f"bad status {sha!r}/{state!r}")
        await self.gh_snippets.set_status(sha, "mia-data/crops", state, description or CROP_STATUS[state], url)

    @activity.defn(name="tree_sha")
    @mia_permanent_errors
    async def tree_sha(self, sha: str, name: str) -> str | None:
        """Tree SHA of snippets/<name> at `sha` (tells whether a merge changed the snippet); None if absent."""
        if not SHA_RE.fullmatch(sha) or not NAME_RE.fullmatch(name):
            raise ValueError(f"bad tree lookup {sha!r}/{name!r}")
        return await self.gh_snippets.dir_sha(f"snippets/{name}", sha)

    @activity.defn(name="adopt_pr_binding")
    @mia_permanent_errors
    async def adopt_pr_binding(self, round_id: str, pr: int, sha: str) -> dict:
        """Take the PR's example binding, `snippets/<name>/examples/<record>.yaml` at `sha` (a head or the merge
        commit), where <name> is the one snippet directory the PR adds.

        It is checked like the match agent's binding (this record, its hosts, the schema) and must use <name>,
        then stored with `snippets_commit: <sha>` as state_dir/rounds/<round>/binding-pr-<sha7>.yaml. Returns
        {"path", "hash", "name", "errors": []}, or {"errors": [...]}: a bad example fails that head's test."""
        if not SHA_RE.fullmatch(sha):
            raise ValueError(f"bad sha {sha!r}")
        gh, round_dir, pins = self.gh_snippets, self._round_dir(round_id), self._pins(round_id)
        names = sorted(
            {file.split("/")[1] for file in await gh.pr_files(pr) if file.startswith("snippets/") and "/" in file[9:]}
        )
        if len(names) != 1 or not NAME_RE.fullmatch(name := names[0]):
            return {"errors": [_text(f"the PR must add exactly one snippets/<name>/ directory, not {names}", 500)]}
        if not await gh.dir_sha(f"snippets/{name}", sha):
            return {"errors": [f"snippets/{name} is not in {sha}"]}
        rel = f"snippets/{name}/examples/{pins.record}.yaml"
        raw = await gh.get_file(rel, sha)
        text = raw.decode(errors="replace") if raw is not None and len(raw) <= 1 << 20 else None
        binding, errors = await self._check_binding(text, round_id, pins, sha)
        if binding and name not in {array["snippet"] for array in binding["raw"] + (binding.get("labels") or [])}:
            errors = [f"{rel} does not use snippet {name!r}"]
        if errors:
            return {"errors": [f"{rel}: {error}" for error in _texts(errors)]}
        binding_path, digest = self._store_binding(
            round_dir / f"binding-pr-{sha[:7]}.yaml", {**binding, "snippets_commit": sha}
        )
        return {"path": binding_path, "hash": digest, "name": name, "errors": []}

    # --- the poller --------------------------------------------------------------------------

    @activity.defn(name="poll")
    @mia_permanent_errors
    async def poll(self) -> list[str]:
        """One scheduled pass over GitHub: labels become signals, changed snippet PRs wake their workflows
        (see poll.py). Returns a log of what was done."""
        return await run_poll(self)
