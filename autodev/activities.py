"""Temporal activities: the only code that touches GitHub, git, csub and the filesystem."""

from __future__ import annotations

import asyncio
import functools
import logging
import re
from pathlib import Path

from temporalio import activity
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError

from .agent import log_tail, read_result, render_prompt
from .config import STAGES
from .github import GitHubError
from .gitops import RejectedBundleError
from .models import (AwaitInput, CommentInput, FailureInput, InvalidKeyError, IssueInput, JobOutcome, JobRef,
                     MergeInput, MergeResult, ObserveInput, PrRef, PrSnapshot, PublishInput, RoundInput, StageInput,
                     SubmitInput, Workspace, check_key)
from .review import WRITE_PERMISSIONS, approved, feedback, handled_ids, latest_round_at
from .workflows import IssueWorkflow

log = logging.getLogger(__name__)

PERMANENT_CSUB = {"policy_violation", "invalid_request"}  # the broker or client rejected the spec; csub's
# ProtocolError and ConfigError carry invalid_request. transport_error, lsf_error etc. may pass, so they retry.
PERMANENT_HTTP = {400, 403, 404, 410, 422}  # rate-limit 403s are retried inside the GitHub client
# ponytail: known secret shapes plus our own token (see neutralize); not a general secret scanner.
SECRET_RE = re.compile(r"sk-ant-[\w-]+|gh[pousr]_\w+|github_pat_\w+|eyJ[\w-]+\.[\w-]+\.[\w-]+")


def neutralize(text: str, token: str | None = None) -> str:
    """Agent-controlled text must not forge our `<!-- autodev: ... -->` markers, nor publish a credential that a
    prompt-injected agent printed (e.g. from ~/.claude/.credentials.json)."""
    if token:
        text = text.replace(token, "[redacted]")
    return SECRET_RE.sub("[redacted]", text).replace("<!--", "&lt;!--")


def permanent_errors(fn):
    """Retrying a GitHub 4xx (or a bad key) that cannot change just burns the workflow's retry budget."""
    @functools.wraps(fn)
    async def wrapper(*args, **kw):
        try:
            return await fn(*args, **kw)
        except InvalidKeyError as e:
            raise ApplicationError(str(e), non_retryable=True) from e
        except GitHubError as e:
            if e.status in PERMANENT_HTTP:
                raise ApplicationError(str(e), non_retryable=True) from e
            raise
    return wrapper


# --- comment templates ------------------------------------------------------------------------

def issue_lead(body: str | None) -> str:
    """The issue's first paragraph, markers and HTML comments removed, for the PR description."""
    text = re.sub(r"<!--.*?-->", "", body or "", flags=re.S).strip()
    lead = re.split(r"\n\s*\n", text, 1)[0].strip()
    return lead if len(lead) <= 300 else lead[:297].rstrip() + "..."


def render_pr_body(cfg, issue: int, lead: str = "") -> str:
    return f"""Closes #{issue}{": " + lead if lead else ""}

Implemented by **{cfg.deployment}**: Claude Code running in a sandboxed job on the cluster. The agent's own
summary is the first comment.

- **Merge:** approve this PR, or comment `/approve <head sha>` (needed when running under a personal token).
  {cfg.deployment} merges once an approval matches the current head and checks are not failing.
- **Request changes:** submit a review with *Request changes*, or comment `/revise <what to change>`.
  Up to {cfg.agent.max_revisions} revision rounds.
- **Stop:** close this PR.

{cfg.marker("pr")}"""


def render_summary(cfg, rnd: int, branch: str, head: str | None, summary: str, cost: float | None) -> str:
    what = f"pushed `{head[:12]}` to `{branch}`" if head else "made no commits"
    cost_s = f" · ${cost:.2f}" if cost is not None else ""
    return f"### {cfg.deployment}: round {rnd} {what}{cost_s}\n\n{summary}"


def render_failure(cfg, error: str, tail: str) -> str:
    return f"""### {cfg.deployment}: stopped

**Error:** {error}

````
{tail.replace("````", "'''") or "(no log output)"}
````

Fix the cause (or edit the issue), then add the `{cfg.label("go")}` label to try again. An open PR is picked up
where it left off; close it first to start over."""


class Activities:
    def __init__(self, cfg, gh, mirror, jobs, bot: str, client=None):
        self.cfg, self.gh, self.mirror, self.jobs, self.bot, self.client = cfg, gh, mirror, jobs, bot, client

    def all(self) -> list:
        return [self.set_stage, self.comment, self.find_pr, self.prepare_round, self.submit_agent,
                self.publish_round, self.observe_pr, self.merge_pr, self.report_failure, self.poll_issues]

    async def _comment_once(self, number: int, body: str, marker: str) -> None:
        """Post unless the *bot* already posted this marker (an outsider's copy must not suppress us)."""
        if not any((c.get("user") or {}).get("login") == self.bot and marker in (c.get("body") or "")
                   for c in await self.gh.issue_comments(number)):
            await self.gh.comment(number, body)

    def _clean(self, text: str) -> str:
        return neutralize(text, self.gh.tokens())

    def _round_dir(self, issue: int, key: str, rnd: int) -> Path:
        return self.cfg.paths.work_root / self.cfg.repo_slug / f"issue-{issue}" / check_key(key) / f"round-{rnd}"

    @activity.defn(name="set_stage")
    @permanent_errors
    async def set_stage(self, a: StageInput) -> None:
        want = self.cfg.label(a.stage) if a.stage else None
        current = {lb["name"] for lb in (await self.gh.get_issue(a.issue)).get("labels", [])}
        for stage in STAGES:
            name = self.cfg.label(stage)
            if name in current and name != want:
                await self.gh.remove_label(a.issue, name)
        if want and want not in current:
            await self.gh.add_labels(a.issue, [want])

    @activity.defn(name="comment")
    @permanent_errors
    async def comment(self, a: CommentInput) -> None:
        marker = self.cfg.marker(a.marker)
        await self._comment_once(a.number, f"{a.body}\n\n{marker}", marker)

    @activity.defn(name="find_pr")
    @permanent_errors
    async def find_pr(self, issue: int) -> PrRef:
        pr = await self.gh.find_pr(self.cfg.branch(issue))
        return PrRef(pr["number"], pr["head"]["sha"]) if pr else PrRef(None, None)

    @activity.defn(name="prepare_round")
    @permanent_errors
    async def prepare_round(self, a: RoundInput) -> Workspace:
        d = self._round_dir(a.issue, a.key, a.round)
        branch = self.cfg.branch(a.issue)
        start = branch if a.pr else self.cfg.github.base_branch
        await asyncio.to_thread(self.mirror.sync)
        base = await asyncio.to_thread(self.mirror.workspace, d, start, branch, self.cfg.git_url)
        issue = await self.gh.get_issue(a.issue)
        comments = await self.gh.issue_comments(a.issue)
        logins = {(c.get("user") or {}).get("login") for c in comments} - {None}
        writers = {x for x in logins if await self.gh.permission(x) in WRITE_PERMISSIONS}
        comments = [c for c in comments if (c.get("user") or {}).get("login") in writers]  # outsiders must not steer the agent
        (d / "prompt.md").write_text(render_prompt(self.cfg, issue, comments, branch, base, a.pr, a.feedback))
        return Workspace(str(d), base, branch)

    @activity.defn(name="submit_agent")
    @permanent_errors
    async def submit_agent(self, a: SubmitInput) -> JobRef:
        from csub import CsubError  # pixi-only dependency, imported where used (as in agent.csub_client)

        try:
            return await self.jobs.submit(a.issue, a.key, a.round, a.workspace)
        except CsubError as e:
            if e.code in PERMANENT_CSUB:
                raise ApplicationError(str(e), non_retryable=True) from e
            raise

    @activity.defn(name="await_agent")
    async def await_agent(self, a: AwaitInput) -> JobOutcome:
        return await self.jobs.wait(a.job_id, a.workspace, activity.heartbeat)

    @activity.defn(name="publish_round")
    @permanent_errors
    async def publish_round(self, a: PublishInput) -> PrRef:
        ws = Path(a.workspace.dir)
        summary, cost = read_result(ws)
        try:
            head = await asyncio.to_thread(
                self.mirror.publish, ws / "result.bundle", a.workspace.base_sha, a.workspace.branch,
                f"refs/autodev/issue-{a.issue}/{a.key}-r{a.round}", self.cfg.github.protected_paths,
                a.pr is None)  # round 1 owns the branch; revisions must fast-forward
        except RejectedBundleError as e:  # permanent; other GitErrors (push, locks) may be transient
            raise ApplicationError(str(e), non_retryable=True) from e
        pr = a.pr
        if head and pr is None:
            existing = await self.gh.find_pr(a.workspace.branch)
            if existing:
                pr = existing["number"]
            else:
                issue = await self.gh.get_issue(a.issue)
                body = render_pr_body(self.cfg, a.issue, self._clean(issue_lead(issue.get("body"))))
                pr = (await self.gh.create_pr(issue["title"] or f"Issue #{a.issue}", body,
                                              a.workspace.branch, self.cfg.github.base_branch))["number"]
        markers = [f"round {a.key}-{a.round}"] + ([f"handled {a.feedback_id}"] if a.feedback_id else [])
        body = render_summary(self.cfg, a.round, a.workspace.branch, head, self._clean(summary), cost)
        body += "\n\n" + "\n".join(self.cfg.marker(m) for m in markers)
        await self._comment_once(pr or a.issue, body, self.cfg.marker(markers[0]))
        return PrRef(pr, head)

    @activity.defn(name="observe_pr")
    @permanent_errors
    async def observe_pr(self, a: ObserveInput) -> PrSnapshot:
        pr = await self.gh.get_pr(a.pr)
        head = pr["head"]["sha"]
        if pr.get("merged") or pr["state"] != "open":
            return PrSnapshot(a.pr, "merged" if pr.get("merged") else pr["state"], head, False, "none")
        reviews = await self.gh.pr_reviews(a.pr)
        inline = await self.gh.pr_review_comments(a.pr)
        comments = await self.gh.issue_comments(a.pr)
        logins = {(x.get("user") or {}).get("login") for x in [*reviews, *comments]} - {None}
        # ponytail: one permission lookup per participant per poll; cache if the rate limit ever matters.
        perms = {login: await self.gh.permission(login) for login in logins}
        mp, allow_self = self.cfg.marker_prefix, self.cfg.github.auth == "token"
        done = handled_ids(comments, mp, self.bot)
        fb = [f for f in feedback(reviews, inline, comments, perms, self.bot, allow_self, mp) if f.id not in done]
        ok = approved(head, reviews, comments, perms, self.bot, allow_self, mp, latest_round_at(comments, mp, self.bot))
        return PrSnapshot(a.pr, "open", head, ok, await self.gh.check_state(head, self.cfg.github.required_checks), fb)

    @activity.defn(name="merge_pr")
    @permanent_errors
    async def merge_pr(self, a: MergeInput) -> MergeResult:
        merged, msg = await self.gh.merge_pr(a.pr, a.sha)
        if not merged and (await self.gh.get_pr(a.pr)).get("merged"):  # a retry after a merge that went through
            merged, msg = True, "merged"
        if merged:
            await self.gh.delete_branch(self.cfg.branch(a.issue))
            await self.gh.update_issue(a.issue, state="closed", state_reason="completed")
        return MergeResult(merged, msg)

    @activity.defn(name="report_failure")
    @permanent_errors
    async def report_failure(self, a: FailureInput) -> None:
        tail = log_tail(Path(a.workspace.dir)) if a.workspace else ""
        marker = self.cfg.marker(f"failed {a.key}-{a.round}")
        await self._comment_once(a.issue, f"{render_failure(self.cfg, self._clean(a.error), self._clean(tail))}\n\n{marker}", marker)
        await self.set_stage(StageInput(a.issue, "needs-human"))

    @activity.defn(name="poll_issues")
    @permanent_errors
    async def poll_issues(self) -> list[int]:
        go = self.cfg.label("go")
        started = []
        for i in await self.gh.list_issues(labels=go, state="open"):
            n = i["number"]
            try:
                actor = await self.gh.label_actor(n, go)
                if not actor or (actor not in self.cfg.github.trusted_bots
                                 and await self.gh.permission(actor) not in WRITE_PERMISSIONS):
                    await self.gh.remove_label(n, go)
                    who = actor or "unknown"
                    marker = self.cfg.marker(f"ignored-label {who}")
                    await self._comment_once(n, f"Ignoring `{go}`: it was added by `{who}`, who does not have write "
                                                f"access to this repository.\n\n{marker}", marker)
                    continue
                await self.client.start_workflow(
                    IssueWorkflow.execute, IssueInput(n, self.cfg.agent.max_revisions, self.cfg.temporal.poll_minutes),
                    id=self.cfg.workflow_id(n), task_queue=self.cfg.task_queue,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING)
                started.append(n)
            except Exception:  # one bad issue must not skip the rest; the next poll retries it
                log.exception("poll: issue #%s failed", n)
        if started:
            log.info("poll: started/kept workflows for %s", started)
        return started
