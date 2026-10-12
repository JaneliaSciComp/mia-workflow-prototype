"""Temporal activities for mia-data: the only code that touches GitHub, csub and the filesystem (spec §7, §8, §9).

Every file a job wrote is read with `read_untrusted` and parsed defensively; results are small dataclasses."""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import hashlib
import json
import os
import re
import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from temporalio import activity
from temporalio.exceptions import ApplicationError

from autodev.activities import PERMANENT_CSUB, neutralize, permanent_errors
from autodev.jobs import read_untrusted
from autodev.review import WRITE_PERMISSIONS

from . import catalog
from . import publish as pub
from .binding import check_binding, hosts, record_urls
from .config import HUMAN_LABELS, STAGES
from .gallery import gallery_files, publish_gallery
from .jobspec import (SHA_RE, HostsNotAllowed, PinMoved, check_hosts_allowed, crop_job, harness_pin, match_job,
                      match_prompt, smoke_job, snapshot, verify_job)
from .models import (DatasetState, GalleryResult, JobArgs, JobDone, LabelEvent, MatchResult, Pins, PrUpdate,
                     RoundLimits, SmokeResult, VerifyResult)
from .publish import CROP_ID_RE, Conflict
from .seed import ID_RE, MARKER_RE
from .status import render_binding, render_crops_table, render_links, render_stage

log = logging.getLogger(__name__)

TEST_PR_RE = re.compile(r"i0-e(\d+)-[0-9a-f]{7}")  # `mia-data test-pr`'s round: PR <n> at a head, no data issue
ROUND_RE = re.compile(rf"i\d+-e\d+|{TEST_PR_RE.pattern}")
SNIPPET_ISSUE_RE = re.compile(r"<!-- mia-data: issue=(\d+) round=(i\d+-e\d+) -->")  # open_snippet_issue's marker
KIND_RE = re.compile(r"[a-z][a-z0-9-]{0,60}")  # a job kind, optionally with a suffix (crop-<sha>)
HEX64 = re.compile(r"[0-9a-f]{64}")
NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")  # a snippet directory
CROP_STATUS = {"pending": "test crops running", "success": "test crops passed", "failure": "test crops failed",
               "error": "test crops could not run"}
PERMANENT = (HostsNotAllowed, PinMoved, Conflict, ValueError, FileNotFoundError,
             yaml.YAMLError)  # inputs that a retry can't fix


def mia_permanent_errors(fn):
    """autodev's permanent_errors, plus our refusals: a moved pin, a host outside the policy, a publication
    conflict, a bad input (malformed YAML too) or a missing state file go to needs-human instead of burning
    retries (§8.2)."""
    inner = permanent_errors(fn)

    @functools.wraps(fn)
    async def wrapper(*args, **kw):
        try:
            return await inner(*args, **kw)
        except PERMANENT as e:
            raise ApplicationError(f"{type(e).__name__}: {e}", type=type(e).__name__, non_retryable=True) from e
    return wrapper


def _write(p: Path, data: str | bytes) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f".{p.name}.tmp")
    tmp.write_bytes(data.encode() if isinstance(data, str) else data)
    os.replace(tmp, p)


def _text(x, n: int = 2000) -> str:
    return "" if x is None else str(x)[:n]


def _texts(x, n: int = 20) -> list[str]:
    return [_text(e, 500) for e in (x if isinstance(x, list) else [] if x is None else [x])[:n]]


def _small(x, limit: int = 16384) -> dict | None:
    """Job-reported JSON goes into workflow history only as a small dict (the result fields are typed dict)."""
    try:
        return x if isinstance(x, dict) and len(json.dumps(x)) <= limit else None
    except (TypeError, ValueError):
        return None


def _loads(text: str | bytes | None):
    try:
        return json.loads(text or "null")
    except (ValueError, RecursionError):
        return None


def _json(ws: str | Path, rel: str):
    return _loads(read_untrusted(Path(ws), rel, 1 << 20))


def _fenced(text: str, lang: str = "") -> str:
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", text)), default=0))
    return f"{fence}{lang}\n{text.rstrip()}\n{fence}"


def _withdrawn(snap: Path) -> dict[str, str]:
    ys = yaml.safe_load(read_untrusted(snap, "WITHDRAWN.yaml", 1 << 20) or "") or []
    return {e["name"]: _text(e.get("reason")) for e in ys if isinstance(e, dict) and isinstance(e.get("name"), str)}


def _catalog(snap: Path) -> dict[str, str]:
    """name -> snippet.yaml text, hiding withdrawn and superseded snippets (§7.2)."""
    root = snap / "snippets"
    out = {d.name: t for d in sorted(root.iterdir() if root.is_dir() else [])
           if (t := read_untrusted(snap, f"snippets/{d.name}/snippet.yaml", 1 << 20)) is not None}
    gone = set(_withdrawn(snap))
    for t in out.values():
        s = (y := yaml.safe_load(t)) and isinstance(y, dict) and y.get("supersedes")
        gone |= {s} if isinstance(s, str) else {x for x in s if isinstance(x, str)} if isinstance(s, list) else set()
    return {k: v for k, v in out.items() if k not in gone}


class Activities:
    def __init__(self, cfg, gh_data, gh_snippets, gh_catalog, jobs, bot: str, client=None, probe=None):
        self.cfg, self.gh_data, self.gh_snippets, self.gh_catalog = cfg, gh_data, gh_snippets, gh_catalog
        self.jobs, self.bot, self.client = jobs, bot, client
        self.probe = probe or (lambda: list(jobs.client.probe().allowed_hosts))  # csub probe: the policy's patterns
        # ponytail: in memory, so a worker restart re-sends each PR once (the workflow dedupes by pr_state)
        self._sent: dict[int, tuple] = {}  # poll: PR number -> the (head, state, merge, needs_human) last signalled

    def all(self) -> list:
        """The main queue's activities; await_job runs on the -await queue."""
        return [self.admit, self.limits, self.poll, self.set_stage, self.comment, self.prepare_match, self.prepare_job, self.probe_hosts,
                self.submit, self.read_match, self.read_smoke, self.read_verify, self.publish_gallery,
                self.publish_crops, self.refuse_label, self.open_snippet_issue, self.pr_state, self.set_crop_status,
                self.tree_sha, self.adopt_pr_binding]

    # --- helpers -----------------------------------------------------------------------------

    def _clean(self, text: str) -> str:
        return neutralize(text, self.gh_data.tokens())

    async def _bot_comment(self, gh, n: int, marker: str) -> dict | None:
        """The bot's own comment carrying `marker` (an outsider's copy must not count)."""
        return next((c for c in await gh.issue_comments(n)
                     if (c.get("user") or {}).get("login") == self.bot and marker in (c.get("body") or "")), None)

    async def _comment_once(self, gh, n: int, body: str, marker: str) -> str | None:
        """The comment's URL (the earlier one, on a retry)."""
        return (await self._bot_comment(gh, n, marker) or await gh.comment(n, body)).get("html_url")

    def _round_dir(self, round_id: str) -> Path:
        """state_dir/rounds/<round_id>: the round's record, downloads entry, pins and bindings (never mounted)."""
        if not ROUND_RE.fullmatch(round_id):
            raise ValueError(f"bad round id {round_id!r}")
        return self.cfg.paths.state_dir / "rounds" / round_id

    def _pins(self, round_id: str) -> Pins:
        return Pins(**json.loads((self._round_dir(round_id) / "pins.json").read_text()))

    def _round_ws(self, issue: int, round_id: str) -> Path:
        """work_root/issue-<n>/<round>, the parent of the round's workspaces; a test-pr round's is
        work_root/test-pr/<pr>/<head>."""
        rd = self._round_dir(round_id)
        if m := TEST_PR_RE.fullmatch(round_id):
            return self.cfg.paths.work_root / "test-pr" / m.group(1) / self._pins(round_id).snippets_commit
        return self.cfg.paths.work_root / f"issue-{int(issue)}" / rd.name

    def _ws(self, issue: int, round_id: str, kind: str, attempt: int) -> Path:
        if not (KIND_RE.fullmatch(kind) and type(attempt) is int and attempt >= 1):
            raise ValueError(f"bad job kind/attempt {kind!r}/{attempt!r}")
        ws = self._round_ws(issue, round_id) / f"{kind}-{attempt}"
        ws.mkdir(parents=True, exist_ok=True)
        return ws

    def _job_ws(self, issue: int, round_id: str, ws: str) -> Path:
        """A workspace of this issue's round, as prepare_* made it."""
        p = Path(ws)
        if p.parent != self._round_ws(issue, round_id) or p.is_symlink():
            raise ValueError(f"{ws} is not a workspace of issue {issue} round {round_id}")
        return p

    @staticmethod
    def _crops(ws: Path) -> Path:
        d = ws / "crops"  # written by the crop job: it must not point outside its workspace
        if d.is_symlink() or not d.is_dir():
            raise ValueError(f"{d} is not a directory")
        return d

    def _record(self, round_id: str) -> dict:
        return yaml.safe_load((self._round_dir(round_id) / "record.yaml").read_text()) or {}

    def _dataset_dir(self, round_id: str, pins: Pins) -> Path:
        return self.cfg.paths.storage_root / catalog.dataset_folder(self._record(round_id), pins.record)

    async def _snapshot(self, sha: str) -> Path:
        return await asyncio.to_thread(snapshot, self.cfg, sha, self.gh_snippets.tokens)

    # --- admission and display ---------------------------------------------------------------

    @activity.defn(name="admit")
    @mia_permanent_errors
    async def admit(self, issue: int, event_id: str) -> Pins:
        """Pin the round (§8.1): record id from the issue's first-line marker, record_commit from the downloads
        state, snippets main, the harness checkout. Written once to state_dir/rounds/<round_id>/; a retry returns
        the same pins."""
        if not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"bad event id {event_id!r}")
        rd = self._round_dir(f"i{int(issue)}-e{event_id}")
        if (rd / "pins.json").is_file():
            return Pins(**json.loads((rd / "pins.json").read_text()))
        m = MARKER_RE.match((await self.gh_data.get_issue(issue)).get("body") or "")
        if not (m and ID_RE.fullmatch(m.group(1))):
            raise ValueError(f"issue #{issue} has no valid record marker on its first line")
        rid = m.group(1)
        entry = (await catalog.downloaded(self.gh_catalog)).get(rid)
        if not (entry and SHA_RE.fullmatch(str(entry.get("record_commit")))):
            raise ValueError(f"{rid}: not in the downloads state, or no record_commit")
        path = (await catalog.record_paths(self.gh_catalog)).get(rid)
        if path is None:
            raise ValueError(f"{rid}: no record on main")
        rec = await catalog.record(self.gh_catalog, path, entry["record_commit"])
        pins = Pins(rid, entry["record_commit"], await self.gh_snippets.get_ref_sha("main"),
                    *await asyncio.to_thread(harness_pin, self.cfg))
        _write(rd / "record.yaml", yaml.safe_dump(rec, sort_keys=False))
        _write(rd / "downloads.json", json.dumps(entry, indent=1))
        _write(rd / "pins.json", json.dumps(dataclasses.asdict(pins)))  # last: marks the round admitted
        return pins

    @activity.defn(name="limits")
    async def limits(self) -> RoundLimits:
        c = self.cfg
        return RoundLimits(c.crops.test_per_round, c.crops.per_round, c.limits.job_walltime_min,
                           c.limits.max_pr_revisions)

    @activity.defn(name="set_stage")
    @mia_permanent_errors
    async def set_stage(self, issue: int, stage: str, state: DatasetState | None = None, detail: str = "",
                        extra: str = "") -> None:
        """Swap the `data:<stage>` label and, within a round, post the stage's comment once (§4): the stage line,
        `detail` (neutralized), the caller's `extra` (crops table, totals, PR link) and links to the folders to
        look at. Comments are never edited: each stage and result is a new one."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage {stage!r}")
        gh, want = self.gh_data, self.cfg.label(stage)
        current = {lb["name"] for lb in (await gh.get_issue(issue)).get("labels", [])}
        for s in STAGES:
            if (name := self.cfg.label(s)) in current and name != want:
                await gh.remove_label(issue, name)
        if want not in current:
            await gh.add_labels(issue, [want])
        if not (state and state.round_id):
            return
        rid, paths = state.round_id, self.cfg.paths
        links = {"Workspace": paths.link(self._round_ws(issue, rid))}
        if stage == "done":
            pins = self._pins(rid)
            links["Crops"] = paths.link(self._dataset_dir(rid, pins))
            links["Gallery"] = (f"https://github.com/{gh.repo}/tree/{self.cfg.gallery.branch}/galleries/"
                                f"{pins.record}/{rid}")
        m = self.cfg.marker(f"stage {rid} {stage}")
        parts = [render_stage(stage, self._clean(detail)), self._clean(extra) if extra else "", render_links(links)]
        await self._comment_once(gh, issue, "\n".join(x for x in parts if x) + f"\n{m}", m)

    @activity.defn(name="comment")
    @mia_permanent_errors
    async def comment(self, issue: int, body: str, marker: str, repo: str = "data", binding_path: str | None = None,
                      binding_hash: str | None = None) -> str | None:
        """Once per marker, on a data issue or (repo="snippets") a snippets issue or PR, with the binding appended
        when one is given. Returns its URL."""
        m = self.cfg.marker(marker)
        body = self._clean(body) + (f"\n\n{render_binding(binding_path, binding_hash)}" if binding_path else "")
        return await self._comment_once(self.gh_snippets if repo == "snippets" else self.gh_data, issue,
                                        f"{body}\n\n{m}", m)

    @activity.defn(name="refuse_label")
    @mia_permanent_errors
    async def refuse_label(self, issue: int, label: str, reason: str, event_id: str) -> None:
        """Remove a human label we won't act on and say why, once per event (busy `go`, stray `approve`, ...)."""
        if label not in {self.cfg.label(k) for k in HUMAN_LABELS} or not re.fullmatch(r"\d+", str(event_id)):
            raise ValueError(f"refuse_label: bad label/event {label!r}/{event_id!r}")
        await self.gh_data.remove_label(issue, label)
        m = self.cfg.marker(f"refused {label} {event_id}")
        await self._comment_once(self.gh_data, issue, f"Ignoring `{label}`: {self._clean(reason)}\n\n{m}", m)

    # --- jobs --------------------------------------------------------------------------------

    @activity.defn(name="prepare_match")
    @mia_permanent_errors
    async def prepare_match(self, issue: int, round_id: str, pins: Pins, attempt: int = 1) -> str:
        """The match workspace (§7.2): prompt.md, record.yaml, downloads.json, the downloader's conversion.json /
        manifest.json, catalog/<name>.yaml (visible snippets only) and SPEC.md. Returns its path."""
        ws, rd = self._ws(issue, round_id, "match", attempt), self._round_dir(round_id)
        snap = await self._snapshot(pins.snippets_commit)
        shutil.rmtree(ws / "catalog", ignore_errors=True)
        (ws / "catalog").mkdir()
        for name, text in _catalog(snap).items():
            (ws / "catalog" / f"{name}.yaml").write_text(text)
        shutil.copyfile(rd / "record.yaml", ws / "record.yaml")
        shutil.copyfile(rd / "downloads.json", ws / "downloads.json")
        dl = json.loads((rd / "downloads.json").read_text()).get("dataset_dir")
        for f in ("conversion.json", "manifest.json"):
            if isinstance(dl, str) and (t := read_untrusted(Path(dl), f, 16 << 20)) is not None:
                (ws / f).write_text(t)
        (ws / "SPEC.md").write_text(read_untrusted(snap, "SPEC.md", 1 << 20) or "")
        (ws / "prompt.md").write_text(match_prompt(pins))
        return str(ws)

    @activity.defn(name="prepare_job")
    @mia_permanent_errors
    async def prepare_job(self, issue: int, round_id: str, kind: str, attempt: int, binding: str | None) -> str:
        """A smoke/crop/verify workspace with record.yaml and (from state_dir) binding.yaml. Returns its path."""
        ws, rd = self._ws(issue, round_id, kind, attempt), self._round_dir(round_id)
        shutil.copyfile(rd / "record.yaml", ws / "record.yaml")
        if binding:
            if not Path(binding).resolve().is_relative_to(rd.resolve()):
                raise ValueError(f"binding {binding} is not one of round {round_id}'s")
            shutil.copyfile(binding, ws / "binding.yaml")
        return str(ws)

    @activity.defn(name="probe_hosts")
    @mia_permanent_errors
    async def probe_hosts(self) -> list[str]:
        """`csub probe`: the policy's allowed_hosts patterns."""
        return list(await asyncio.to_thread(self.probe))

    def _spec(self, kind: str, a: JobArgs, snap: Path | None):
        ws, pins, cfg = Path(a.ws), a.pins, self.cfg
        if kind == "match":
            return match_job(cfg, ws, pins, "record.yaml")
        existing = self._dataset_dir(a.round_id, pins) / "manifest.json" if a.existing else None
        existing = existing if existing and existing.is_file() else None
        if kind == "smoke":
            return smoke_job(cfg, ws, pins, "binding.yaml", snap, "record.yaml")
        if kind == "crop":
            return crop_job(cfg, ws, pins, "binding.yaml", snap, "record.yaml", a.n, a.seed, a.round_id, a.batch,
                            existing, a.trusted)
        if kind == "verify":
            if Path(a.crop_ws).parent != ws.parent:
                raise ValueError(f"crop workspace {a.crop_ws} is not of this round")
            return verify_job(cfg, ws, pins, self._crops(Path(a.crop_ws)), "binding.yaml", existing)
        raise ValueError(f"unknown job kind {kind!r}")

    @activity.defn(name="submit")
    @mia_permanent_errors
    async def submit(self, key: str, kind: str, a: JobArgs) -> str:
        """Build the job (the builders refuse a moved harness pin), refuse hosts a fresh `csub probe` doesn't allow
        (§9), and submit at most once per key. Returns the job id."""
        from csub import CsubError  # pixi-only dependency, imported where used (as autodev does)

        patterns = await asyncio.to_thread(self.probe)
        snap = await self._snapshot(a.sha or a.pins.snippets_commit) if kind in ("smoke", "crop") else None

        def build():
            spec = self._spec(kind, a, snap)
            check_hosts_allowed(spec, patterns)
            return spec
        try:
            return await self.jobs.submit(key, build)
        except CsubError as e:
            if e.code in PERMANENT_CSUB:
                raise ApplicationError(str(e), non_retryable=True) from e
            raise

    @activity.defn(name="await_job")
    async def await_job(self, job_id: str, ws: str) -> JobDone:
        o = await self.jobs.wait(job_id, ws, activity.heartbeat)
        return JobDone(o.job_id, o.state, o.exit_code)

    # --- job results -------------------------------------------------------------------------

    @activity.defn(name="read_match")
    @mia_permanent_errors
    async def read_match(self, ws: str) -> MatchResult:
        """outcome.json, and for matched/ineligible the checked binding.yaml, copied (with the round's
        snippets_commit) to state_dir/rounds/<round>/binding-<attempt>.yaml. Anything we can't trust is `blocked`
        with the reason."""
        p = Path(ws)
        if not (m := re.fullmatch(r"match-(\d+)", p.name)):
            raise ValueError(f"{ws} is not a match workspace")
        rd, pins = self._round_dir(p.parent.name), self._pins(p.parent.name)
        o = _json(ws, "outcome.json")
        if not isinstance(o, dict):
            return MatchResult("blocked", "outcome.json is missing, unreadable or not a JSON object")
        kind = o.get("outcome")
        if kind == "blocked":
            return MatchResult("blocked", _text(o.get("reason")) or "the match agent reported blocked, with no reason")
        if kind == "needs_snippet":
            draft = _small({k: o.get(k) for k in ("draft_binding", "what_fails", "tried")}, 32768)
            return MatchResult("needs_snippet", draft=draft or {"what_fails": "(the agent's draft was too large)"})
        if kind not in ("matched", "ineligible"):
            return MatchResult("blocked", f"unknown match outcome {_text(kind, 100)!r}")
        b, errs = await self._check(read_untrusted(p, "binding.yaml", 1 << 20), p.parent.name, pins,
                                    pins.snippets_commit)
        if errs:
            return MatchResult("blocked", _text("binding check failed: " + "; ".join(errs)))
        path, h = self._store(rd / f"binding-{m.group(1)}.yaml", {**b, "snippets_commit": pins.snippets_commit})
        return MatchResult(kind, binding_path=path, binding_hash=h,
                           shapes=_small(o.get("shapes")) if kind == "ineligible" else None,
                           reasons=_texts(b.get("reasons")))

    async def _check(self, text: str | None, round_id: str, pins: Pins, sha: str) -> tuple[dict | None, list[str]]:
        """An agent-written binding, parsed and checked against the round's record and the snippets at `sha`."""
        if text is None:
            return None, ["the binding is missing or unreadable"]
        try:
            b = yaml.safe_load(text)
            errs = check_binding(b, self._record(round_id), set(_catalog(await self._snapshot(sha))), pins=pins)
            if not errs:
                hosts([u for a in b["raw"] + (b.get("labels") or []) for u in a["urls"]], self.cfg)
        except (yaml.YAMLError, ValueError, RecursionError) as e:  # a malformed URL raises in urlsplit
            errs = [f"{type(e).__name__}: {e}"]
        return b if not errs else None, errs

    @staticmethod
    def _store(path: Path, b: dict) -> tuple[str, str]:
        _write(path, data := yaml.safe_dump(b, sort_keys=False).encode())
        return str(path), hashlib.sha256(data).hexdigest()

    @activity.defn(name="read_smoke")
    @mia_permanent_errors
    async def read_smoke(self, ws: str) -> SmokeResult:
        o = _json(ws, "smoke.json")
        if not isinstance(o, dict):
            return SmokeResult(False, errors=["smoke.json is missing, unreadable or not a JSON object"])
        b = o.get("bytes_fetched")
        return SmokeResult(o.get("ok") is True, o.get("eligible") is True, _small(o.get("shapes")), _texts(o.get("errors")),
                           _texts(o.get("warnings")), b if type(b) is int and b >= 0 else 0,
                           _text(o.get("host_not_allowed"), 300) or None)

    @activity.defn(name="read_verify")
    @mia_permanent_errors
    async def read_verify(self, ws: str, crop_ws: str | None = None) -> VerifyResult:
        """verify.json: only crops with ok, a valid id and sha256s for crop.json and raw.png count. Top-level errors
        or n == 0 mean nothing was verified. With `crop_ws`, n_effective is the crop report's lowered n."""
        o = _json(ws, "verify.json")
        if not isinstance(o, dict):
            return VerifyResult(0, 0, ["verify.json is missing, unreadable or not a JSON object"], [])
        errors, n = _texts(o.get("errors")), o.get("n") if type(o.get("n")) is int and o["n"] >= 0 else 0
        ok, failed, seen = [], [], set()
        for c in (o.get("crops") if isinstance(o.get("crops"), list) else [])[:1000]:
            if not isinstance(c, dict):
                continue
            cid, files = c.get("crop_id"), c.get("files")
            if (c.get("ok") is True and isinstance(cid, str) and CROP_ID_RE.fullmatch(cid) and cid not in seen
                    and isinstance(files, dict) and len(files) <= 64 and {"crop.json", "raw.png"} <= files.keys()
                    and all(isinstance(k, str) and isinstance(v, str) and HEX64.fullmatch(v) for k, v in files.items())):
                ok.append({"crop_id": cid, "files": files})
                seen.add(cid)
            else:
                failed.append(f"{_text(cid, 100)}: {'; '.join(_texts(c.get('errors'), 5)) or 'not verified'}")
        if not n and not errors:
            errors = ["verify checked no crops"]
        if errors:
            ok = []
        rep = _json(crop_ws, "crops/report.json") if crop_ws else None
        rep = rep if isinstance(rep, dict) else {}
        num = lambda k: rep[k] if type(rep.get(k)) is int and rep[k] >= 0 else None  # noqa: E731
        return VerifyResult(len(ok), n, (errors + failed)[:20], ok, num("n"), num("bytes_fetched") or 0)

    # --- publication -------------------------------------------------------------------------

    @activity.defn(name="publish_gallery")
    @mia_permanent_errors
    async def publish_gallery(self, issue: int, round_id: str, kind: str, crop_ws: str, verify_ws: str,
                              verified: list[dict]) -> GalleryResult:
        """raw.png + overlay.png of the verified crops, with the binding and verify report, under
        galleries/<record>/<round>/<kind> (kind = the tested SHA, or production) on the galleries branch (§7.4); a
        test-pr round's under galleries/_test-pr/<record>/<pr>/<kind>."""
        if not (kind == "production" or SHA_RE.fullmatch(kind)):
            raise ValueError(f"bad gallery kind {kind!r}")
        cw, vw = self._job_ws(issue, round_id, crop_ws), self._job_ws(issue, round_id, verify_ws)
        rec, m = self._pins(round_id).record, TEST_PR_RE.fullmatch(round_id)
        prefix = f"galleries/_test-pr/{rec}/{m.group(1)}/{kind}" if m else f"galleries/{rec}/{round_id}/{kind}"
        files = await asyncio.to_thread(gallery_files, cw, vw, verified)
        urls = await publish_gallery(self.gh_data, self.cfg.gallery.branch, prefix, files)
        rows = []
        for v in verified:
            cid = v["crop_id"]
            raw = read_untrusted(cw, f"crops/{cid}/crop.json", 1 << 20, binary=True)
            if raw is None or hashlib.sha256(raw).hexdigest() != v["files"]["crop.json"]:
                raise ValueError(f"{cid}/crop.json is missing or differs from the verified checksum")
            cj = d if isinstance(d := _loads(raw), dict) else {}
            rows.append({"crop_id": cid, "plane": cj.get("plane") if isinstance(cj.get("plane"), dict) else {},
                         "box": cj.get("box") if isinstance(cj.get("box"), dict) else {},
                         "raw_url": urls[f"{cid}/raw.png"], "overlay_url": urls[f"{cid}/overlay.png"]})
        return GalleryResult(prefix, urls, render_crops_table(rows))

    @activity.defn(name="publish_crops")
    @mia_permanent_errors
    async def publish_crops(self, issue: int, round_id: str, crop_ws: str, verified: list[dict], sha: str) -> dict:
        """Publish the verified crops into the dataset folder (§7.6), marking crops of snippets withdrawn in
        mia-snippets at `sha`. Returns {"published", "already", "total"}."""
        crops = self._crops(self._job_ws(issue, round_id, crop_ws))
        withdrawn = _withdrawn(await self._snapshot(sha))
        t = activity.info().start_to_close_timeout if activity.in_activity() else None
        return await asyncio.to_thread(pub.publish, self._dataset_dir(round_id, self._pins(round_id)), crops, verified,
                                       withdrawn, tmp_max_age_s=2 * (t.total_seconds() if t else 3600))

    # --- snippet development -----------------------------------------------------------------

    @activity.defn(name="open_snippet_issue")
    @mia_permanent_errors
    async def open_snippet_issue(self, issue: int, round_id: str, draft: dict) -> int:
        """The mia-snippets issue for a new snippet (§7.3), found again by its first-line marker on a retry."""
        gh, g, pins = self.gh_snippets, self.cfg.github, self._pins(round_id)
        m = self.cfg.marker(f"issue={int(issue)} round={round_id}")
        # ponytail: lists every issue; one call per round. Filter by author here: an outsider can copy the marker.
        for i in await gh.list_issues(state="all"):
            if (i.get("user") or {}).get("login") == self.bot and (i.get("body") or "").startswith(m):
                return i["number"]
        rec = self._record(round_id)
        urls = record_urls(rec)
        fmt = _text(((rec.get("data") or {}).get("formats") or ["unknown"])[0], 40)
        # ponytail: container guessed from the record's URLs; the agent's what_fails is the authority
        container = ("zip" if any("::" in u or u.split("?")[0].endswith(".zip") for u in urls)
                     else "s3" if any(u.startswith("s3://") for u in urls) else "http")
        excerpt = yaml.safe_dump({k: rec.get(k) for k in ("title", "imaging", "data", "technical")}, sort_keys=False)
        d = draft or {}
        body = "\n\n".join([
            m, f"New snippet needed for dataset `{pins.record}` ([{g.data_repo}#{issue}]"
               f"(https://github.com/{g.data_repo}/issues/{issue})). Read "
               f"[SPEC.md](https://github.com/{g.snippets_repo}/blob/main/SPEC.md) first, and add the binding you "
               f"test as `examples/{pins.record}.yaml`.",
            f"**Record** (at `{pins.record_commit}`):", _fenced(self._clean(excerpt[:8000]), "yaml"),
            "**What fails** (the match agent's report):", _fenced(self._clean(_text(d.get("what_fails"), 4000))),
            "**Tried:**", _fenced(self._clean(json.dumps(d.get("tried"), indent=1)[:8000]), "json"),
            "**Draft binding:**", _fenced(self._clean(yaml.safe_dump(d.get("draft_binding"), sort_keys=False)[:16000]), "yaml")])
        title = f"New snippet for {self._clean(fmt)}/{container} ({g.data_repo.split('/')[-1]}#{issue})"
        return (await gh.create_issue(title, body, ["snippets:go"]))["number"]

    @activity.defn(name="pr_state")
    @mia_permanent_errors
    async def pr_state(self, pr: int) -> PrUpdate:
        """The PR's current head, state and merge SHA, read fresh (§8.3), and whether its issue needs a human."""
        p = await self.gh_snippets.get_pr(pr)
        m = re.fullmatch(r"snippets/issue-(\d+)", p["head"]["ref"])
        if not m or ((p["head"].get("repo") or {}).get("full_name")) != self.gh_snippets.repo:
            raise ValueError(f"PR #{pr} is not on a snippets/issue-<K> branch of {self.gh_snippets.repo}")
        k, merged = int(m.group(1)), bool(p.get("merged"))
        labels = {lb["name"] for lb in (await self.gh_snippets.get_issue(k)).get("labels", [])}
        return PrUpdate(pr, p["head"]["sha"], "merged" if merged else p["state"],
                        p.get("merge_commit_sha") if merged else None, k, "snippets:needs-human" in labels,
                        p.get("html_url"))

    @activity.defn(name="set_crop_status")
    @mia_permanent_errors
    async def set_crop_status(self, sha: str, state: str, url: str | None, description: str = "") -> None:
        if not SHA_RE.fullmatch(sha) or state not in CROP_STATUS:
            raise ValueError(f"bad status {sha!r}/{state!r}")
        await self.gh_snippets.set_status(sha, "mia-data/crops", state, description or CROP_STATUS[state], url)

    @activity.defn(name="tree_sha")
    @mia_permanent_errors
    async def tree_sha(self, sha: str, name: str) -> str | None:
        """Tree SHA of snippets/<name> at `sha` (§7.3 step 5); None if absent."""
        if not SHA_RE.fullmatch(sha) or not NAME_RE.fullmatch(name):
            raise ValueError(f"bad tree lookup {sha!r}/{name!r}")
        return await self.gh_snippets.dir_sha(f"snippets/{name}", sha)

    @activity.defn(name="adopt_pr_binding")
    @mia_permanent_errors
    async def adopt_pr_binding(self, round_id: str, pr: int, sha: str) -> dict:
        """The PR's example binding, `snippets/<name>/examples/<record>.yaml` at `sha` (a head or the merge commit),
        where <name> is the one snippet directory the PR adds. Checked like the match agent's (§7.4: this record,
        its hosts, the schema) and using <name>, then stored with `snippets_commit: <sha>` as
        state_dir/rounds/<round>/binding-pr-<sha7>.yaml. Returns {"path", "hash", "name", "errors": []}, or
        {"errors"}: a bad example fails that head's test."""
        if not SHA_RE.fullmatch(sha):
            raise ValueError(f"bad sha {sha!r}")
        gs, rd, pins = self.gh_snippets, self._round_dir(round_id), self._pins(round_id)
        names = sorted({f.split("/")[1] for f in await gs.pr_files(pr) if f.startswith("snippets/") and "/" in f[9:]})
        if len(names) != 1 or not NAME_RE.fullmatch(name := names[0]):
            return {"errors": [_text(f"the PR must add exactly one snippets/<name>/ directory, not {names}", 500)]}
        if not await gs.dir_sha(f"snippets/{name}", sha):
            return {"errors": [f"snippets/{name} is not in {sha}"]}
        rel = f"snippets/{name}/examples/{pins.record}.yaml"
        raw = await gs.get_file(rel, sha)
        text = raw.decode(errors="replace") if raw is not None and len(raw) <= 1 << 20 else None
        b, errs = await self._check(text, round_id, pins, sha)
        if b and name not in {a["snippet"] for a in b["raw"] + (b.get("labels") or [])}:
            errs = [f"{rel} does not use snippet {name!r}"]
        if errs:
            return {"errors": [f"{rel}: {x}" for x in _texts(errs)]}
        path, h = self._store(rd / f"binding-pr-{sha[:7]}.yaml", {**b, "snippets_commit": sha})
        return {"path": path, "hash": h, "name": name, "errors": []}

    # --- the poller --------------------------------------------------------------------------

    @activity.defn(name="poll")
    @mia_permanent_errors
    async def poll(self) -> list[str]:
        """MiaDataPoll (§8): deliver `data:go`/`data:approve` as signals, wake dataset workflows when their
        snippet PR changes, and pass `mia-data/crops` on PRs that add no snippet (§5.5). One bad issue or PR never
        skips the rest; the next poll retries it."""
        from temporalio.service import RPCError, RPCStatusCode

        cfg, gd, gs, did = self.cfg, self.gh_data, self.gh_snippets, []
        for kind in HUMAN_LABELS:
            label = cfg.label(kind)
            for i in await gd.list_issues(labels=label, state="open"):
                n = i["number"]
                try:
                    actor, eid, at = await gd.label_event(n, label)
                    if not actor or await gd.permission(actor) not in WRITE_PERMISSIONS:
                        await gd.remove_label(n, label)
                        m = cfg.marker(f"ignored {label} {eid}")
                        await self._comment_once(gd, n, f"Ignoring `{label}`: it was added by `{actor or 'unknown'}`, "
                                                        f"who does not have write access to this repository.\n\n{m}", m)
                    elif not (eid and at):
                        log.warning("poll: #%s %s: label event has no id or time", n, label)
                    elif kind == "go" and "licence-excluded" in {lb["name"] for lb in i.get("labels", [])}:
                        await self.refuse_label(n, label, "the dataset's licence is not in `crops.licences` "
                                                          "(`licence-excluded`)", eid)
                    else:
                        await self.client.start_workflow(
                            "MiaDataDataset", DatasetState(n), id=cfg.workflow_id(n), task_queue=cfg.task_queue,
                            start_signal=kind, start_signal_args=[LabelEvent(kind, eid, actor, at)])
                        await gd.remove_label(n, label)  # only once the signal is accepted (§8.3)
                        did.append(f"{kind} #{n}")
                except Exception:
                    log.exception("poll: issue #%s %s failed", n, label)
        recent = datetime.now(timezone.utc) - timedelta(days=1)
        # ponytail: the first page (50 most recently updated PRs); paginate if more than 50 are active
        for p in await gs.list_prs(state="all"):
            try:
                head = p["head"]
                if p["state"] == "open" and not any(s.get("context") == "mia-data/crops"
                                                    for s in await gs.statuses(head["sha"])):
                    files = await gs.pr_files(p["number"])
                    if len(files) < 100 and not any(f.startswith("snippets/") for f in files):  # 100: a full page
                        await gs.set_status(head["sha"], "mia-data/crops", "success", "no snippet in this PR", None)
                        did.append(f"status PR #{p['number']}")
                m = re.fullmatch(r"snippets/issue-(\d+)", head["ref"])
                if (not m or (head.get("repo") or {}).get("full_name") != gs.repo
                        or (p["state"] != "open" and datetime.fromisoformat(p["updated_at"]) < recent)):
                    continue
                k = await gs.get_issue(int(m.group(1)))
                mk = SNIPPET_ISSUE_RE.match(k.get("body") or "")
                if not mk or (k.get("user") or {}).get("login") != self.bot:  # an outsider can copy the marker
                    continue
                merged = bool(p.get("merged_at"))
                u = PrUpdate(p["number"], head["sha"], "merged" if merged else p["state"],
                             p.get("merge_commit_sha") if merged else None, k["number"],
                             "snippets:needs-human" in {lb["name"] for lb in k.get("labels", [])}, p.get("html_url"))
                if self._sent.get(u.pr) == (key := (u.head_sha, u.state, u.merge_sha, u.needs_human)):
                    continue  # signalled already: a wake-up per change, not per poll
                try:
                    await self.client.get_workflow_handle(cfg.workflow_id(int(mk.group(1)))).signal("pr_update", u)
                    self._sent[u.pr] = key
                    did.append(f"pr_update PR #{p['number']}")
                except RPCError as e:
                    if e.status != RPCStatusCode.NOT_FOUND:  # no dataset workflow (any more): nothing to wake
                        raise
            except Exception:
                log.exception("poll: PR #%s failed", p.get("number"))
        if did:
            log.info("poll: %s", ", ".join(did))
        return did
