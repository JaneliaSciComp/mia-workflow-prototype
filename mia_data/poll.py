"""The poller: one scheduled pass that turns GitHub state into workflow signals.

Three jobs, each done per item so one bad issue or PR never skips the rest (the next poll retries it):

- deliver a `data:go` / `data:approve` label on a dataset issue as a signal, starting the dataset workflow if needed;
- wake the dataset workflow whose snippet PR changed (head, state, merge, needs-human);
- pass the `mia-data/crops` status on snippet PRs that add no snippet, so it never blocks them.

Runs as the `poll` activity (Activities.poll) and uses the activity object's clients and helpers.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from autodev.review import WRITE_PERMISSIONS

from .config import HUMAN_LABELS
from .models import DatasetState, LabelEvent, PrUpdate

if TYPE_CHECKING:
    from .activities import Activities

log = logging.getLogger(__name__)

SNIPPET_ISSUE_RE = re.compile(r"<!-- mia-data: issue=(\d+) round=(i\d+-e\d+) -->")  # open_snippet_issue's marker


async def run_poll(activities: Activities) -> list[str]:
    """One pass over the data repo's labelled issues and the snippets repo's recent PRs. Returns a log of
    what was done."""
    done: list[str] = []
    for kind in HUMAN_LABELS:
        label = activities.cfg.label(kind)
        for issue in await activities.gh_data.list_issues(labels=label, state="open"):
            try:
                if await deliver_label(activities, kind, label, issue):
                    done.append(f"{kind} #{issue['number']}")
            except Exception:
                log.exception("poll: issue #%s %s failed", issue["number"], label)
    recent = datetime.now(UTC) - timedelta(days=1)
    # ponytail: the first page (50 most recently updated PRs); paginate if more than 50 are active
    for pr_data in await activities.gh_snippets.list_prs(state="all"):
        try:
            if pr_data["state"] == "open" and await pass_status_without_snippet(activities, pr_data):
                done.append(f"status PR #{pr_data['number']}")
            if await wake_dataset_workflow(activities, pr_data, recent):
                done.append(f"pr_update PR #{pr_data['number']}")
        except Exception:
            log.exception("poll: PR #%s failed", pr_data.get("number"))
    if done:
        log.info("poll: %s", ", ".join(done))
    return done


def _licence_guard(kind: str, issue: dict) -> str | None:
    """mia-data's refusal: `go` on a licence-excluded dataset."""
    if kind == "go" and "licence-excluded" in {entry["name"] for entry in issue.get("labels", [])}:
        return "the dataset's licence is not in `crops.licences` (`licence-excluded`)"
    return None


async def deliver_label(
    activities: Any,
    kind: str,
    label: str,
    issue: dict,
    *,
    workflow: str = "MiaDataDataset",
    make_state: Callable[[int], Any] = DatasetState,
    guard: Callable[[str, dict], str | None] | None = _licence_guard,
) -> bool:
    """Turn one human label on an issue into a signal, or remove it with a comment saying why not.

    Only a person with write access, or a bot login listed in `github.trusted_bots` (the buffer worker, spec §6.2),
    counts; `guard` may name a reason to refuse the label. The label is removed
    only once Temporal has accepted the signal, so a crash in between leaves it for the next poll. Returns True
    when a signal was delivered. mia-buffers reuses this with its own workflow, state and no guard."""
    cfg, gh_data, number = activities.cfg, activities.gh_data, issue["number"]
    actor, event_id, labeled_at = await gh_data.label_event(number, label)
    trusted = actor in getattr(cfg.github, "trusted_bots", [])
    if not actor or not (trusted or await gh_data.permission(actor) in WRITE_PERMISSIONS):
        await gh_data.remove_label(number, label)
        marker = cfg.marker(f"ignored {label} {event_id}")
        await activities._comment_once(
            gh_data,
            number,
            f"Ignoring `{label}`: it was added by `{actor or 'unknown'}`, who does "
            f"not have write access to this repository.\n\n{marker}",
            marker,
        )
        return False
    if not (event_id and labeled_at):
        log.warning("poll: #%s %s: label event has no id or time", number, label)
        return False
    if guard and (reason := guard(kind, issue)):
        await activities.refuse_label(number, label, reason, event_id)
        return False
    # start_signal: starts the workflow if needed and delivers the label as a signal either way
    await activities.temporal.start_workflow(
        workflow,
        make_state(number),
        id=cfg.workflow_id(number),
        task_queue=cfg.task_queue,
        start_signal=kind,
        start_signal_args=[LabelEvent(kind, event_id, actor, labeled_at)],
    )
    await gh_data.remove_label(number, label)  # only once the signal is accepted
    return True


async def pass_status_without_snippet(activities: Activities, pr_data: dict) -> bool:
    """Set `mia-data/crops` to success on an open PR that touches nothing under snippets/, if it has no such
    status yet. Returns True when the status was set."""
    gh_snippets, head_sha = activities.gh_snippets, pr_data["head"]["sha"]
    if any(status.get("context") == "mia-data/crops" for status in await gh_snippets.statuses(head_sha)):
        return False
    files = await gh_snippets.pr_files(pr_data["number"])
    if len(files) >= 100 or any(file.startswith("snippets/") for file in files):  # 100: a full page, unknown rest
        return False
    await gh_snippets.set_status(head_sha, "mia-data/crops", "success", "no snippet in this PR", None)
    return True


async def wake_dataset_workflow(activities: Activities, pr_data: dict, recent: datetime) -> bool:
    """Signal `pr_update` to the dataset workflow behind a `snippets/issue-N` PR when something about the PR
    changed since the last poll. Closed PRs older than `recent` are skipped. Returns True when a signal was sent."""
    from temporalio.service import RPCError, RPCStatusCode

    gh_snippets, head = activities.gh_snippets, pr_data["head"]
    match = re.fullmatch(r"snippets/issue-(\d+)", head["ref"])
    if (
        not match
        or (head.get("repo") or {}).get("full_name") != gh_snippets.repo
        or (pr_data["state"] != "open" and datetime.fromisoformat(pr_data["updated_at"]) < recent)
    ):
        return False
    snippet_issue = await gh_snippets.get_issue(int(match.group(1)))
    issue_marker = SNIPPET_ISSUE_RE.match(snippet_issue.get("body") or "")
    if not issue_marker or (snippet_issue.get("user") or {}).get("login") != activities.bot:
        return False  # not one of ours: an outsider can copy the marker, but not post as the bot
    merged = bool(pr_data.get("merged_at"))
    update = PrUpdate(
        pr_data["number"],
        head["sha"],
        "merged" if merged else pr_data["state"],
        pr_data.get("merge_commit_sha") if merged else None,
        snippet_issue["number"],
        "snippets:needs-human" in {entry["name"] for entry in snippet_issue.get("labels", [])},
        pr_data.get("html_url"),
    )
    fingerprint = (update.head_sha, update.state, update.merge_sha, update.needs_human)
    if activities._sent.get(update.pr) == fingerprint:
        return False  # signalled already: a wake-up per change, not per poll
    try:
        workflow_id = activities.cfg.workflow_id(int(issue_marker.group(1)))
        await activities.temporal.get_workflow_handle(workflow_id).signal("pr_update", update)
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:  # no dataset workflow (any more): nothing to wake
            raise
        return False
    activities._sent[update.pr] = fingerprint
    return True
