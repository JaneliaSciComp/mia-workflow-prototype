"""Reading a PR's reviews and comments: who approved which commit, what change requests are pending, and
how CI looks.

These are pure functions over the dicts GitHub returns, so the rules (who counts as a reviewer, which
approval counts for which commit) are in one place and easy to test. A `Reviewers` says who counts: the
participants' permissions, the bot's login and the marker prefix that identifies the bot's own comments.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from .models import Feedback

WRITE_PERMISSIONS = {"admin", "maintain", "write"}
APPROVE_RE = re.compile(r"^/approve\s+([0-9a-f]{7,40})\b", re.M)
REVISE_RE = re.compile(r"^/revise\b(.*)", re.M | re.S)
_FAILED = {"failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"}


def _login(item: dict) -> str | None:
    """The author login of a review or comment dict."""
    return (item.get("user") or {}).get("login")


@dataclass(frozen=True)
class Reviewers:
    """Who counts on a PR: `permissions` maps each participant's login to their repo permission, `bot` is our
    login and `marker_prefix` starts every comment we posted. `allow_self` lets the bot's own account review
    (under a personal token the reviewer and the bot are the same user)."""

    permissions: dict[str, str]
    bot: str
    allow_self: bool
    marker_prefix: str

    def is_human_writer(self, login: str | None) -> bool:
        """A person with write access, and not the bot itself unless `allow_self`."""
        if not login:
            return False
        return self.permissions.get(login) in WRITE_PERMISSIONS and (self.allow_self or login != self.bot)

    def is_ours(self, body: str | None) -> bool:
        return self.marker_prefix in (body or "")


def latest_round_at(comments: list[dict], reviewers: Reviewers) -> str | None:
    """When the current head was published: the time of the bot's latest round-summary comment."""
    return max(
        (
            comment.get("created_at") or ""
            for comment in comments
            if _login(comment) == reviewers.bot and f"{reviewers.marker_prefix} round " in (comment.get("body") or "")
        ),
        default=None,
    )


def approved(head_sha: str, reviews: list[dict], comments: list[dict], reviewers: Reviewers, since: str | None) -> bool:
    """Whether a human with write access approved exactly `head_sha`, by review or by `/approve <sha>` comment.

    `/approve` comments count only if posted after `since`: a short prefix of an older head must not approve
    a later head the agent happens to produce. GitHub's ISO-8601 UTC timestamps compare correctly as strings."""
    latest: dict[str, dict] = {}
    for review in reviews:  # GitHub returns reviews oldest first; a reviewer's latest verdict counts
        if review.get("state") in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED") and (login := _login(review)):
            latest[login] = review
    if any(
        review["state"] == "APPROVED" and review.get("commit_id") == head_sha and reviewers.is_human_writer(login)
        for login, review in latest.items()
    ):
        return True
    for comment in comments:
        if reviewers.is_ours(comment.get("body")) or (since and (comment.get("created_at") or "") <= since):
            continue
        match = APPROVE_RE.search(comment.get("body") or "")
        if match and head_sha.startswith(match.group(1)) and reviewers.is_human_writer(_login(comment)):
            return True
    return False


def feedback(
    reviews: list[dict], review_comments: list[dict], comments: list[dict], reviewers: Reviewers
) -> list[Feedback]:
    """Change requests from people with write access, oldest first.

    A "changes requested" review becomes one Feedback with its inline comments attached; a `/revise` comment
    becomes one Feedback with the rest of the comment as its text."""
    items: list[tuple[str, Feedback]] = []  # (timestamp, feedback) for sorting
    for review in reviews:
        if review.get("state") != "CHANGES_REQUESTED" or not reviewers.is_human_writer(_login(review)):
            continue
        inline = [
            f"- `{comment.get('path')}` line {comment.get('line') or comment.get('original_line')}: "
            f"{comment.get('body')}"
            for comment in review_comments
            if comment.get("pull_request_review_id") == review["id"]
        ]
        text = "\n".join(filter(None, [review.get("body") or "", *inline])) or "(changes requested, no details)"
        items.append((review.get("submitted_at") or "", Feedback(f"review-{review['id']}", text)))
    for comment in comments:
        if reviewers.is_ours(comment.get("body")):
            continue
        match = REVISE_RE.search(comment.get("body") or "")
        if match and reviewers.is_human_writer(_login(comment)):
            items.append(
                (
                    comment.get("created_at") or "",
                    Feedback(f"comment-{comment['id']}", match.group(1).strip() or "(no details)"),
                )
            )
    return [item for _, item in sorted(items, key=lambda pair: pair[0])]


def handled_ids(comments: list[dict], reviewers: Reviewers) -> set[str]:
    """Feedback ids that a round already addressed: only the bot's own `<prefix> handled <id> -->` markers count."""
    handled_re = re.compile(re.escape(reviewers.marker_prefix) + r" handled (\S+) -->")
    handled: set[str] = set()
    for comment in comments:
        if _login(comment) == reviewers.bot:
            handled.update(handled_re.findall(comment.get("body") or ""))
    return handled


def combine_checks(check_runs: list[dict], statuses: list[dict], required: Sequence[str] = ()) -> str:
    """Fold check runs and commit statuses into one state: failure beats pending beats success.

    "none" means nothing reported and nothing is required; a missing required check counts as pending."""
    states = [
        "pending" if run.get("status") != "completed" else "failure" if run.get("conclusion") in _FAILED else "success"
        for run in check_runs
    ]
    states += [
        {"success": "success", "pending": "pending"}.get(status.get("state") or "", "failure") for status in statuses
    ]
    seen = {run.get("name") for run in check_runs} | {status.get("context") for status in statuses}
    if not states and not required:
        return "none"
    worst = next((state for state in ("failure", "pending") if state in states), "success")
    return "pending" if worst == "success" and not seen.issuperset(required) else worst
