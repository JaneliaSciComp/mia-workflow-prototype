"""The autodev Temporal activities: the only code that touches GitHub, git, csub and the filesystem.

Each activity is small and idempotent where it matters (comments are posted once per marker, jobs are
submitted once per key), because Temporal may retry any of them. Anything the agent wrote is treated as
untrusted: read with bounds, never executed, and scrubbed of secrets and fake markers before it is posted.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, cast

from temporalio import activity
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError

from mia_core.untrusted import SECRET_RE as SECRET_RE
from mia_core.untrusted import neutralize as neutralize

from .agent import log_tail, read_result, render_prompt
from .config import STAGES
from .decide import WAIT, decide
from .github import GitHubError
from .gitops import RejectedBundleError
from .models import (
    AwaitInput,
    CommentInput,
    FailureInput,
    InvalidKeyError,
    IssueInput,
    JobOutcome,
    JobRef,
    MergeInput,
    MergeResult,
    ObserveInput,
    PrRef,
    PrSnapshot,
    PublishInput,
    RoundInput,
    StageInput,
    SubmitInput,
    Workspace,
    check_key,
)
from .review import WRITE_PERMISSIONS, Reviewers, approved, feedback, handled_ids, latest_round_at
from .workflows import IssueWorkflow

if TYPE_CHECKING:
    from temporalio.client import Client

    from .agent import AgentJobs
    from .config import Config
    from .github import GitHub
    from .gitops import Mirror

log = logging.getLogger(__name__)

# csub error codes meaning the broker or client rejected the spec itself; csub's ProtocolError and ConfigError
# carry invalid_request. transport_error, lsf_error etc. may pass on retry, so they are left retryable.
PERMANENT_CSUB = {"policy_violation", "invalid_request"}
PERMANENT_HTTP = {400, 403, 404, 410, 422}  # rate-limit 403s are retried inside the GitHub client


def permanent_errors[Fn: Callable[..., Awaitable]](fn: Fn) -> Fn:
    """Decorator: turn GitHub 4xx errors and bad keys into non-retryable failures.

    Retrying a request that cannot succeed would only burn the workflow's retry budget."""

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except InvalidKeyError as error:
            raise ApplicationError(str(error), non_retryable=True) from error
        except GitHubError as error:
            if error.status in PERMANENT_HTTP:
                raise ApplicationError(str(error), non_retryable=True) from error
            raise

    return cast(Fn, wrapper)


# --- comment templates ------------------------------------------------------------------------


def issue_lead(body: str | None) -> str:
    """The issue's first paragraph, markers and HTML comments removed, for the PR description."""
    text = re.sub(r"<!--.*?-->", "", body or "", flags=re.S).strip()
    lead = re.split(r"\n\s*\n", text, maxsplit=1)[0].strip()
    return lead if len(lead) <= 300 else lead[:297].rstrip() + "..."


def render_pr_body(cfg: Config, issue: int, lead: str = "") -> str:
    return f"""Closes #{issue}{": " + lead if lead else ""}

Implemented by **{cfg.deployment}**: Claude Code running in a sandboxed job on the cluster. The agent's own
summary is the first comment.

- **Merge:** approve this PR, or comment `/approve <head sha>` (needed when running under a personal token).
  {cfg.deployment} merges once an approval matches the current head and checks are not failing.
- **Request changes:** submit a review with *Request changes*, or comment `/revise <what to change>`.
  Up to {cfg.agent.max_revisions} revision rounds.
- **Stop:** close this PR.

{cfg.marker("pr")}"""


def render_summary(
    cfg: Config, round_number: int, branch: str, head_sha: str | None, summary: str, cost: float | None
) -> str:
    """The comment posted after each round: what was pushed, what it cost, and the agent's own summary."""
    what = f"pushed `{head_sha[:12]}` to `{branch}`" if head_sha else "made no commits"
    cost_text = f" · ${cost:.2f}" if cost is not None else ""
    return f"### {cfg.deployment}: round {round_number} {what}{cost_text}\n\n{summary}"


def render_failure(cfg: Config, error: str, tail: str) -> str:
    return f"""### {cfg.deployment}: stopped

**Error:** {error}

````
{tail.replace("````", "'''") or "(no log output)"}
````

Fix the cause (or edit the issue), then add the `{cfg.label("go")}` label to try again. An open PR is picked up
where it left off; close it first to start over."""


class Activities:
    """The activity implementations, bound to one deployment's GitHub client, git mirror and job runner.

    `bot` is the GitHub login autodev posts as; it decides which existing comments count as ours."""

    def __init__(
        self, cfg: Config, gh: GitHub, mirror: Mirror, jobs: AgentJobs, bot: str, client: Client | None = None
    ):
        self.cfg, self.gh, self.mirror, self.jobs, self.bot, self.client = cfg, gh, mirror, jobs, bot, client

    @property
    def temporal(self) -> Client:
        """The Temporal client; only the worker's activities (poll_issues) have one."""
        if self.client is None:
            raise RuntimeError("no Temporal client: this activity runs only inside the worker")
        return self.client

    def all(self) -> list:
        """The activities registered on the main task queue (await_agent runs on the -await queue)."""
        return [
            self.set_stage,
            self.comment,
            self.find_pr,
            self.prepare_round,
            self.submit_agent,
            self.publish_round,
            self.observe_pr,
            self.merge_pr,
            self.report_failure,
            self.poll_issues,
        ]

    async def _comment_once(self, number: int, body: str, marker: str) -> None:
        """Post the comment unless the bot already posted one carrying this marker.

        Only the bot's own comments count: an outsider pasting our marker must not be able to suppress us."""
        if not any(
            (comment.get("user") or {}).get("login") == self.bot and marker in (comment.get("body") or "")
            for comment in await self.gh.issue_comments(number)
        ):
            await self.gh.comment(number, body)

    def _clean(self, text: str) -> str:
        return neutralize(text, self.gh.tokens())

    def _round_dir(self, issue: int, key: str, round_number: int) -> Path:
        return (
            self.cfg.paths.work_root / self.cfg.repo_slug / f"issue-{issue}" / check_key(key) / f"round-{round_number}"
        )

    @activity.defn(name="set_stage")
    @permanent_errors
    async def set_stage(self, request: StageInput) -> None:
        """Make the requested stage the issue's only stage label (None removes them all)."""
        want = self.cfg.label(request.stage) if request.stage else None
        current = {label["name"] for label in (await self.gh.get_issue(request.issue)).get("labels", [])}
        for stage in STAGES:
            name = self.cfg.label(stage)
            if name in current and name != want:
                await self.gh.remove_label(request.issue, name)
        if want and want not in current:
            await self.gh.add_labels(request.issue, [want])

    @activity.defn(name="comment")
    @permanent_errors
    async def comment(self, request: CommentInput) -> None:
        marker = self.cfg.marker(request.marker)
        await self._comment_once(request.number, f"{request.body}\n\n{marker}", marker)

    @activity.defn(name="find_pr")
    @permanent_errors
    async def find_pr(self, issue: int) -> PrRef:
        """The open PR on this issue's branch, if an earlier attempt already made one."""
        pr = await self.gh.find_pr(self.cfg.branch(issue))
        return PrRef(pr["number"], pr["head"]["sha"]) if pr else PrRef(None, None)

    @activity.defn(name="prepare_round")
    @permanent_errors
    async def prepare_round(self, request: RoundInput) -> Workspace:
        """Make the round's workspace: a fresh clone of the branch plus the prompt the agent will read.

        Only comments from people with write access go into the prompt, so outsiders cannot steer the agent."""
        round_dir = self._round_dir(request.issue, request.key, request.round)
        branch = self.cfg.branch(request.issue)
        start_branch = branch if request.pr else self.cfg.github.base_branch
        await asyncio.to_thread(self.mirror.sync)
        base_sha = await asyncio.to_thread(self.mirror.workspace, round_dir, start_branch, branch, self.cfg.git_url)
        issue = await self.gh.get_issue(request.issue)
        comments = await self.gh.issue_comments(request.issue)
        logins = {login for comment in comments if (login := (comment.get("user") or {}).get("login"))}
        writers = {login for login in logins if await self.gh.permission(login) in WRITE_PERMISSIONS}
        comments = [comment for comment in comments if (comment.get("user") or {}).get("login") in writers]
        (round_dir / "prompt.md").write_text(
            render_prompt(self.cfg, issue, comments, branch, base_sha, request.pr, request.feedback)
        )
        return Workspace(str(round_dir), base_sha, branch)

    @activity.defn(name="submit_agent")
    @permanent_errors
    async def submit_agent(self, request: SubmitInput) -> JobRef:
        from csub import CsubError  # pixi-only dependency, imported where used (as in agent.csub_client)

        try:
            return await self.jobs.submit(request.issue, request.key, request.round, request.workspace)
        except CsubError as error:
            if error.code in PERMANENT_CSUB:
                raise ApplicationError(str(error), non_retryable=True) from error
            raise

    @activity.defn(name="await_agent")
    async def await_agent(self, request: AwaitInput) -> JobOutcome:
        return await self.jobs.wait(request.job_id, request.workspace, activity.heartbeat)

    @activity.defn(name="publish_round")
    @permanent_errors
    async def publish_round(self, request: PublishInput) -> PrRef:
        """Push the agent's commits, open the PR if there is none yet, and post the round summary."""
        workspace_dir = Path(request.workspace.dir)
        summary, cost = read_result(workspace_dir)
        try:
            head_sha = await asyncio.to_thread(
                self.mirror.publish,
                workspace_dir / "result.bundle",
                request.workspace.base_sha,
                request.workspace.branch,
                f"refs/autodev/issue-{request.issue}/{request.key}-r{request.round}",
                self.cfg.github.protected_paths,
                request.pr is None,
            )  # round 1 owns the branch and may force-push; revisions must fast-forward
        except RejectedBundleError as error:  # permanent; other GitErrors (push, locks) may be transient
            raise ApplicationError(str(error), non_retryable=True) from error
        pr_number = request.pr
        if head_sha and pr_number is None:
            existing = await self.gh.find_pr(request.workspace.branch)
            if existing:
                pr_number = existing["number"]
            else:
                issue = await self.gh.get_issue(request.issue)
                body = render_pr_body(self.cfg, request.issue, self._clean(issue_lead(issue.get("body"))))
                created = await self.gh.create_pr(
                    issue["title"] or f"Issue #{request.issue}",
                    body,
                    request.workspace.branch,
                    self.cfg.github.base_branch,
                )
                pr_number = created["number"]
        markers = [f"round {request.key}-{request.round}"]
        if request.feedback_id:
            markers.append(f"handled {request.feedback_id}")  # tells observe_pr this feedback was addressed
        body = render_summary(self.cfg, request.round, request.workspace.branch, head_sha, self._clean(summary), cost)
        body += "\n\n" + "\n".join(self.cfg.marker(marker) for marker in markers)
        await self._comment_once(pr_number or request.issue, body, self.cfg.marker(markers[0]))
        return PrRef(pr_number, head_sha)

    @activity.defn(name="observe_pr")
    @permanent_errors
    async def observe_pr(self, request: ObserveInput) -> PrSnapshot:
        """Read the PR's state, approvals, pending change requests and CI status into one snapshot.

        Raises a retryable error while there is nothing for the workflow to act on, so Temporal's retry
        policy (not a workflow timer) spaces the polls and the history stays flat."""
        snapshot = await self._snapshot(request.pr)
        if decide(snapshot, request.handled, request.revisions, request.max_revisions, request.escalated)[0] == WAIT:
            raise ApplicationError(f"PR #{request.pr}: nothing to do yet")
        return snapshot

    async def _snapshot(self, number: int) -> PrSnapshot:
        pr = await self.gh.get_pr(number)
        head_sha = pr["head"]["sha"]
        if pr.get("merged") or pr["state"] != "open":
            return PrSnapshot(number, "merged" if pr.get("merged") else pr["state"], head_sha, False, "none")
        reviews = await self.gh.pr_reviews(number)
        inline_comments = await self.gh.pr_review_comments(number)
        comments = await self.gh.issue_comments(number)
        logins = {login for item in [*reviews, *comments] if (login := (item.get("user") or {}).get("login"))}
        # ponytail: one permission lookup per participant per poll; cache if the rate limit ever matters.
        permissions = {login: await self.gh.permission(login) for login in logins}
        # under a personal token the reviewer and the bot are the same user
        reviewers = Reviewers(permissions, self.bot, self.cfg.github.auth == "token", self.cfg.marker_prefix)
        handled = handled_ids(comments, reviewers)
        pending = [item for item in feedback(reviews, inline_comments, comments, reviewers) if item.id not in handled]
        is_approved = approved(head_sha, reviews, comments, reviewers, latest_round_at(comments, reviewers))
        checks = await self.gh.check_state(head_sha, self.cfg.github.required_checks)
        return PrSnapshot(number, "open", head_sha, is_approved, checks, pending)

    @activity.defn(name="merge_pr")
    @permanent_errors
    async def merge_pr(self, request: MergeInput) -> MergeResult:
        """Squash-merge the approved commit, then delete the branch and close the issue."""
        merged, message = await self.gh.merge_pr(request.pr, request.sha)
        if not merged and (await self.gh.get_pr(request.pr)).get("merged"):  # a retry after a merge that went through
            merged, message = True, "merged"
        if merged:
            await self.gh.delete_branch(self.cfg.branch(request.issue))
            await self.gh.update_issue(request.issue, state="closed", state_reason="completed")
        return MergeResult(merged, message)

    @activity.defn(name="report_failure")
    @permanent_errors
    async def report_failure(self, request: FailureInput) -> None:
        """Post the error and the agent's log tail on the issue, and label it needs-human."""
        tail = log_tail(Path(request.workspace.dir)) if request.workspace else ""
        marker = self.cfg.marker(f"failed {request.key}-{request.round}")
        body = render_failure(self.cfg, self._clean(request.error), self._clean(tail))
        await self._comment_once(request.issue, f"{body}\n\n{marker}", marker)
        await self.set_stage(StageInput(request.issue, "needs-human"))

    @activity.defn(name="poll_issues")
    @permanent_errors
    async def poll_issues(self) -> list[int]:
        """Start (or keep) a workflow for every open issue labelled `go` by someone with write access.

        A `go` from anyone else is removed with an explanatory comment."""
        go_label = self.cfg.label("go")
        started = []
        for issue in await self.gh.list_issues(labels=go_label, state="open"):
            number = issue["number"]
            try:
                actor = await self.gh.label_actor(number, go_label)
                if not actor or (
                    actor not in self.cfg.github.trusted_bots
                    and await self.gh.permission(actor) not in WRITE_PERMISSIONS
                ):
                    await self.gh.remove_label(number, go_label)
                    who = actor or "unknown"
                    marker = self.cfg.marker(f"ignored-label {who}")
                    await self._comment_once(
                        number,
                        f"Ignoring `{go_label}`: it was added by `{who}`, who does not "
                        f"have write access to this repository.\n\n{marker}",
                        marker,
                    )
                    continue
                await self.temporal.start_workflow(
                    IssueWorkflow.execute,
                    IssueInput(number, self.cfg.agent.max_revisions, self.cfg.temporal.poll_minutes),
                    id=self.cfg.workflow_id(number),
                    task_queue=self.cfg.task_queue,
                    id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                )
                started.append(number)
            except Exception:  # one bad issue must not skip the rest; the next poll retries it
                log.exception("poll: issue #%s failed", number)
        if started:
            log.info("poll: started/kept workflows for %s", started)
        return started
