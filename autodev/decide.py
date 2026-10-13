"""The decision table for an open PR: given what the PR looks like, what should the workflow do next?

This is a pure function so the workflow can call it directly and tests can enumerate every case.
"""

from __future__ import annotations

from .models import Feedback, PrSnapshot

DONE, CLOSED, MERGE, REVISE, ESCALATE, WAIT = "DONE", "CLOSED", "MERGE", "REVISE", "ESCALATE", "WAIT"


def escalation(reason: str, snapshot: PrSnapshot) -> str:
    """The identity of an escalation: the same reason for the same head is raised with a human only once."""
    return f"{reason} {snapshot.head_sha}"


def decide(
    snapshot: PrSnapshot, handled: list[str], revisions: int, max_revisions: int, escalated: str | None = None
) -> tuple[str, Feedback | str | None]:
    """Return (action, detail): the Feedback to act on for REVISE, the reason for ESCALATE, else None.

    Pending change requests win over an approval while revisions remain; once the limit is hit, a pending
    request escalates to a human instead. An escalation already raised (`escalated`) is a WAIT: the PR
    is in a human's hands until something about it changes."""

    def escalate(reason: str) -> tuple[str, str | None]:
        return (WAIT, None) if escalation(reason, snapshot) == escalated else (ESCALATE, reason)

    if snapshot.state == "merged":
        return DONE, None
    if snapshot.state == "closed":
        return CLOSED, None
    pending = [item for item in snapshot.feedback if item.id not in handled]
    if pending and revisions < max_revisions:
        return REVISE, pending[0]
    if snapshot.approved:
        if snapshot.checks in ("success", "none"):
            return MERGE, None
        if snapshot.checks == "failure":
            return escalate("approved, but checks are failing")
    if pending:
        return escalate(f"revision limit ({max_revisions}) reached")
    return WAIT, None
