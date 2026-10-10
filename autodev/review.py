"""Pure readers of GitHub PR data: who approved which commit, what change requests are pending, how CI looks."""

from __future__ import annotations

import re

from .models import Feedback

WRITE_PERMISSIONS = {"admin", "maintain", "write"}
APPROVE_RE = re.compile(r"^/approve\s+([0-9a-f]{7,40})\b", re.M)
REVISE_RE = re.compile(r"^/revise\b(.*)", re.M | re.S)
_FAILED = {"failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"}


def _login(x: dict) -> str | None:
    return (x.get("user") or {}).get("login")


def _human(login: str | None, perms: dict[str, str], bot: str, allow_self: bool) -> bool:
    """A person with write access. `allow_self`: under a personal token you are also the bot."""
    return bool(login) and perms.get(login) in WRITE_PERMISSIONS and (allow_self or login != bot)


def _ours(body: str | None, marker_prefix: str) -> bool:
    return marker_prefix in (body or "")


def approved(head_sha: str, reviews: list[dict], comments: list[dict], perms: dict[str, str], bot: str,
             allow_self: bool, marker_prefix: str) -> bool:
    latest: dict[str, dict] = {}
    for r in reviews:  # GitHub returns reviews oldest first; a reviewer's latest verdict counts
        if r.get("state") in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED") and _login(r):
            latest[_login(r)] = r
    if any(r["state"] == "APPROVED" and r.get("commit_id") == head_sha and _human(login, perms, bot, allow_self)
           for login, r in latest.items()):
        return True
    for c in comments:
        if _ours(c.get("body"), marker_prefix):
            continue
        m = APPROVE_RE.search(c.get("body") or "")
        if m and head_sha.startswith(m.group(1)) and _human(_login(c), perms, bot, allow_self):
            return True
    return False


def feedback(reviews: list[dict], review_comments: list[dict], comments: list[dict], perms: dict[str, str],
             bot: str, allow_self: bool, marker_prefix: str) -> list[Feedback]:
    """Change requests from people with write access, oldest first."""
    items: list[tuple[str, Feedback]] = []
    for r in reviews:
        if r.get("state") != "CHANGES_REQUESTED" or not _human(_login(r), perms, bot, allow_self):
            continue
        inline = [f"- `{c.get('path')}` line {c.get('line') or c.get('original_line')}: {c.get('body')}"
                  for c in review_comments if c.get("pull_request_review_id") == r["id"]]
        text = "\n".join(filter(None, [r.get("body") or "", *inline])) or "(changes requested, no details)"
        items.append((r.get("submitted_at") or "", Feedback(f"review-{r['id']}", text)))
    for c in comments:
        if _ours(c.get("body"), marker_prefix):
            continue
        m = REVISE_RE.search(c.get("body") or "")
        if m and _human(_login(c), perms, bot, allow_self):
            items.append((c.get("created_at") or "", Feedback(f"comment-{c['id']}", m.group(1).strip() or "(no details)")))
    return [f for _, f in sorted(items, key=lambda t: t[0])]


def handled_ids(comments: list[dict], marker_prefix: str, bot: str) -> set[str]:
    """Feedback ids that a round already addressed: only the bot's own `<prefix> handled <id> -->` markers count."""
    handled_re = re.compile(re.escape(marker_prefix) + r" handled (\S+) -->")
    out: set[str] = set()
    for c in comments:
        if _login(c) == bot:
            out.update(handled_re.findall(c.get("body") or ""))
    return out


def combine_checks(check_runs: list[dict], statuses: list[dict]) -> str:
    states = ["pending" if r.get("status") != "completed" else
              "failure" if r.get("conclusion") in _FAILED else "success" for r in check_runs]
    states += [{"success": "success", "pending": "pending"}.get(s.get("state"), "failure") for s in statuses]
    if not states:
        return "none"
    return next((s for s in ("failure", "pending") if s in states), "success")
