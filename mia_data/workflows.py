"""MiaDataDataset and MiaDataPoll (spec §4, §7, §8). No I/O here; activities are called by name (activities.py)."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from autodev.workflows import QUEUE_SLACK, RETRY_MAX_INTERVAL, _err
    from .models import (DatasetState, GalleryResult, JobArgs, JobDone, LabelEvent, MatchResult, Pins, PrUpdate,
                         RoundLimits, SmokeResult, Test, VerifyResult, job_key)

GO, APPROVE = "data:go", "data:approve"
BUSY = "busy, ignored: a round is already running. Add it again once the issue shows a final stage."
NOT_REVIEW = "`data:approve` counts only while the issue is in `data:review`, for the round under review."
MAX_ROUNDS_PER_EXECUTION = 20  # roll over between rounds at the latest after this many


class RoundFailed(Exception):
    pass


def _rollover_due() -> bool:
    return workflow.info().is_continue_as_new_suggested()


def _when(iso: str | None) -> datetime | None:
    try:
        d = datetime.fromisoformat(iso)
    except (TypeError, ValueError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _ok(d: JobDone) -> bool:
    return d.state == "DONE" and d.exit_code in (0, None)


def _failed(kind: str, d: JobDone) -> str:
    return f"{kind} job {d.job_id} ended {d.state} (exit {d.exit_code})"


@workflow.defn(name="MiaDataDataset")
class MiaDataDataset:
    """One per dataset issue, forever: each new `go` runs a round (match → test → review → approve → crop →
    publish) that ends in a dataset stage (done, needs-human, ineligible), then waits for the next `go`."""

    @workflow.init
    def __init__(self, s: DatasetState) -> None:
        self.s = s
        self.gos: list[LabelEvent] = []
        self.approves: list[LabelEvent] = []
        self.lim: RoundLimits | None = None

    @workflow.signal
    def go(self, e: LabelEvent) -> None:
        self.gos.append(e)

    @workflow.signal
    def approve(self, e: LabelEvent) -> None:
        self.approves.append(e)

    @workflow.signal
    def pr_update(self, u: PrUpdate) -> None:
        if u not in self.s.pending:  # recorded only; the develop path (Task 21) consumes them
            self.s.pending = (self.s.pending + [u])[-50:]

    @workflow.query
    def status(self) -> DatasetState:
        return self.s

    @workflow.run
    async def run(self, s: DatasetState) -> None:
        rounds = 0
        if s.stage == "review" and s.tests:  # rolled over while waiting for an approve
            await self._guard(self._from_review())
        while True:
            await workflow.wait_condition(lambda: bool(self.gos or self.approves) or _rollover_due())
            if not (self.gos or self.approves):
                workflow.continue_as_new(self.s)  # idle, nothing queued (§8.4)
            await self._refuse_queued(gos=False)  # idle: no round is under review
            if not self.gos:
                continue
            e = self.gos.pop(0)
            if self._seen(e):  # redelivery
                continue
            rounds += 1
            await self._guard(self._round(e))
            if (rounds >= MAX_ROUNDS_PER_EXECUTION or _rollover_due()) and not (self.gos or self.approves):
                workflow.continue_as_new(self.s)

    # --- helpers ---------------------------------------------------------------------------

    async def _act(self, name: str, *args, seconds: int = 60, budget: timedelta = timedelta(hours=6), result_type=None):
        # retry by time budget; activities raise non-retryable ApplicationError for permanent failures
        return await workflow.execute_activity(
            name, args=list(args), start_to_close_timeout=timedelta(seconds=seconds), schedule_to_close_timeout=budget,
            retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL), result_type=result_type)

    async def _guard(self, step) -> None:
        try:
            await step
        except (ActivityError, RoundFailed) as x:
            await self._finish("needs-human", _err(x))

    def _seen(self, e: LabelEvent) -> bool:
        if e.event_id in self.s.handled:
            return True
        self.s.handled = (self.s.handled + [e.event_id])[-50:]
        return False

    async def _stage(self, stage: str, detail: str = "", budget: timedelta = timedelta(hours=6)) -> None:
        self.s.stage = stage
        await self._act("set_stage", self.s.issue, stage, self.s, self.s.links, detail, budget=budget)

    async def _refuse_queued(self, gos: bool = True, approves: bool = True) -> None:
        """Every label queued now arrived while it couldn't count: answer each new one with refuse_label."""
        q = []
        if approves:
            q, self.approves = [(APPROVE, NOT_REVIEW, e) for e in self.approves], []
        if gos:
            q, self.gos = q + [(GO, BUSY, e) for e in self.gos], []
        for label, reason, e in q:
            if not self._seen(e):
                await self._refuse(label, reason, e)

    async def _refuse(self, label: str, reason: str, e: LabelEvent) -> None:
        try:
            await self._act("refuse_label", self.s.issue, label, reason, e.event_id)
        except ActivityError as x:  # best effort: the event is handled either way
            workflow.logger.warning("issue #%d: could not refuse %s %s: %s", self.s.issue, label, e.event_id, _err(x))

    async def _finish(self, stage: str, detail: str = "") -> None:
        """End the round in a dataset stage, then refuse whatever queued up while it ran. Never raises."""
        try:
            await self._stage(stage, detail, budget=timedelta(hours=24))
        except ActivityError as x:
            workflow.logger.error("issue #%d: could not set %s: %s", self.s.issue, stage, _err(x))
        await self._refuse_queued()

    async def _prep(self, kind: str) -> str:
        s = self.s
        return await self._act("prepare_job", s.issue, s.round_id, kind, 1, s.binding, seconds=300, result_type=str)

    async def _job(self, kind: str, ws: str, wskind: str | None = None, **kw) -> JobDone:
        """Submit (at most once per key) and wait on the await queue; a worker restart reattaches (§8.2)."""
        s = self.s
        jid = await self._act("submit", job_key(s.issue, s.round_id, wskind or kind, 1), kind,
                              JobArgs(ws, s.round_id, s.pins, **kw), seconds=600, result_type=str)
        wall = timedelta(minutes=self.lim.walltime_min)
        return await workflow.execute_activity(
            "await_job", args=[jid, ws], result_type=JobDone, task_queue=workflow.info().task_queue + "-await",
            start_to_close_timeout=wall + QUEUE_SLACK, schedule_to_close_timeout=wall + timedelta(hours=24),
            heartbeat_timeout=timedelta(minutes=3), retry_policy=RetryPolicy(maximum_interval=RETRY_MAX_INTERVAL))

    async def _crop(self, tag: str, sha: str, n: int, seed: str, batch: int) -> tuple[str, str, VerifyResult, JobDone,
                                                                                  JobDone]:
        """crop → verify (+ preview) → read_verify. Workspaces crop-<tag>, verify-<tag>; tag = tested SHA or
        production. The jobs are returned, so the caller judges their exit; a job whose state is uncertain fails."""
        cw = await self._prep(f"crop-{tag}")
        c = await self._job("crop", cw, f"crop-{tag}", sha=sha, n=n, seed=seed, batch=batch, trusted=True,
                            existing=batch > 0)
        if c.state not in ("DONE", "EXIT"):
            raise RoundFailed(_failed("crop", c))
        vw = await self._prep(f"verify-{tag}")
        d = await self._job("verify", vw, f"verify-{tag}", crop_ws=cw, existing=batch > 0)
        if d.state not in ("DONE", "EXIT"):
            raise RoundFailed(_failed("verify", d))
        return cw, vw, await self._act("read_verify", vw, cw, result_type=VerifyResult), c, d

    async def _gallery(self, tag: str, cw: str, vw: str, v: VerifyResult) -> GalleryResult:
        s = self.s
        g = await self._act("publish_gallery", s.issue, s.round_id, tag, cw, vw, v.crops, seconds=300,
                            result_type=GalleryResult)
        self.s.links["crops"] = g.table
        return g

    # --- the round -------------------------------------------------------------------------

    async def _round(self, e: LabelEvent) -> None:
        s = self.s
        s.round_id, s.seed = f"i{s.issue}-e{e.event_id}", str(workflow.random().getrandbits(64))
        s.pins = s.binding = s.binding_hash = s.snippet_name = s.snippet_issue = s.pr = None
        s.tests, s.attempts, s.links = [], {}, {}
        s.pins = await self._act("admit", s.issue, e.event_id, seconds=300, result_type=Pins)
        self.lim = await self._act("limits", result_type=RoundLimits)
        await self._stage("matching")

        # match (§7.2): the agent's binding, then our own smoke of it
        ws = await self._act("prepare_match", s.issue, s.round_id, s.pins, 1, seconds=600, result_type=str)
        if not _ok(d := await self._job("match", ws)):
            return await self._finish("needs-human", _failed("match", d))
        m = await self._act("read_match", ws, seconds=300, result_type=MatchResult)
        if m.outcome == "needs_snippet":
            return await self._develop(m)
        if m.outcome not in ("matched", "ineligible"):
            return await self._finish("needs-human", f"match blocked: {m.reason}")
        s.binding, s.binding_hash = m.binding_path, m.binding_hash
        ws = await self._prep("smoke")
        if not _ok(d := await self._job("smoke", ws)):
            return await self._finish("needs-human", _failed("smoke", d))
        sm = await self._act("read_smoke", ws, result_type=SmokeResult)
        if not sm.ok:
            why = "; ".join(sm.errors) or "no errors reported"
            return await self._finish("needs-human", f"smoke failed: {why}" + (
                f" (host not allowed: {sm.host_not_allowed})" if sm.host_not_allowed else ""))
        if sm.eligible != (m.outcome == "matched"):
            return await self._finish("needs-human", f"the match agent says {m.outcome}, but smoke found the data "
                                                     f"{'eligible' if sm.eligible else 'ineligible'}")
        if not sm.eligible:
            return await self._finish("ineligible", f"no raw volume is at least 256×256; shapes: {sm.shapes}")

        # test crops at the matched snippets_commit (§7.4)
        sha = s.pins.snippets_commit
        await self._stage("testing")
        cw, vw, v, c, d = await self._crop(sha, sha, self.lim.test_n, s.seed + "-t", 0)
        bad = [_failed(k, j) for k, j in (("crop", c), ("verify", d)) if not _ok(j)]
        passed = not bad and v.ok_count > 0 and not v.errors  # both jobs succeeded and every test crop verified
        g = await self._gallery(sha, cw, vw, v) if v.crops else None
        s.tests.append(Test(sha, f"{s.round_id}-{sha[:12]}", passed, workflow.now().isoformat(), g and g.prefix))
        lines = [f"Test crops at `{sha}`: {v.ok_count} of {v.n} verified; the crop job fetched {v.bytes_fetched} "
                 f"bytes.", *bad, *(f"- {x}" for x in v.errors), "", "Binding reasons:",
                 *(f"- {x}" for x in m.reasons), "", g.table if g else ""]
        await self._act("comment", s.issue, "\n".join(lines), f"test {s.round_id} {sha}")
        if not passed:
            return await self._finish("needs-human", "; ".join(["the test crops failed; nothing was produced", *bad]))
        await self._stage("review")
        await self._refuse_queued(approves=False)  # gos that came while the round ran; approves are judged below
        await self._from_review()

    async def _from_review(self) -> None:
        if await self._review():
            await self._produce()  # else a new go came during review: the next round starts from matching

    async def _produce(self) -> None:
        """Production (§7.5). Passing crops stay published even when the round ends in needs-human."""
        s = self.s
        sha = s.pins.snippets_commit
        await self._stage("cropping")
        self.lim = self.lim or await self._act("limits", result_type=RoundLimits)  # gone after a rollover
        n = self.lim.per_round
        cw, vw, v, c, _ = await self._crop("production", sha, n, s.seed + "-p", 1)
        # verify's exit is not checked: harness verify exits 1 whenever a crop fails; its report decides
        r = await self._act("publish_crops", s.issue, s.round_id, cw, v.crops, sha, seconds=300, result_type=dict)
        if v.crops:
            await self._gallery("production", cw, vw, v)
        n_eff = n if v.n_effective is None else min(n, v.n_effective)
        need = max(1, math.ceil(n_eff / 2))
        s.links["totals"] = (f"{len(r['published'])} new crops published this round ({v.ok_count} of {n_eff} "
                             f"passed); {r['total']} in the dataset")
        if not _ok(c):  # a limit or a crash (§6.5): the surviving crops are published, the round needs a human
            return await self._finish("needs-human", f"{_failed('crop', c)}; the {v.ok_count} crops that passed "
                                                     f"are published")
        if v.ok_count >= need:
            return await self._finish("done")
        await self._finish("needs-human", f"only {v.ok_count} of {n_eff} crops passed (at least {need} needed); the "
                                          f"ones that passed are published")

    async def _review(self) -> bool:
        """Wait for an approve newer than the test result (True), or a new go (False). Stale approves are refused."""
        fin = _when(self.s.tests[-1].finished_at)
        while True:
            await workflow.wait_condition(lambda: bool(self.approves or self.gos) or _rollover_due())
            if not (self.approves or self.gos):
                workflow.continue_as_new(self.s)  # stage review: the next run resumes here (§8.4)
            while self.approves:
                e = self.approves.pop(0)
                if self._seen(e):
                    continue
                if (at := _when(e.at)) and at > fin:
                    return True
                await self._refuse(APPROVE, "it is older than the round's test crops; add it again to approve them", e)
            self.gos = [g for g in self.gos if g.event_id not in self.s.handled]
            if self.gos:
                return False

    async def _develop(self, m: MatchResult) -> None:
        """No snippet fits (§7.3): Task 21 opens the snippets issue, tests PR heads and crops after the merge."""
        await self._finish("needs-human", "development path not wired yet (Task 21)")


@workflow.defn(name="MiaDataPoll")
class MiaDataPoll:
    @workflow.run
    async def run(self) -> list[str]:
        return await workflow.execute_activity(
            "poll", start_to_close_timeout=timedelta(minutes=5), retry_policy=RetryPolicy(maximum_attempts=1),
            result_type=list[str])
