"""The decision table for an open PR: given what the PR looks like, what should the workflow do next?

This is a pure function so the workflow can call it directly and tests can enumerate every case.
"""

from __future__ import annotations

from .models import Feedback, PrSnapshot

DONE, CLOSED, MERGE, REVISE, ESCALATE, WAIT = "DONE", "CLOSED", "MERGE", "REVISE", "ESCALATE", "WAIT"


def decide(snapshot: PrSnapshot, handled: list[str], revisions: int,
           max_revisions: int) -> tuple[str, Feedback | str | None]:
    """Return (action, detail): the Feedback to act on for REVISE, the reason for ESCALATE, else None.

    Pending change requests win over an approval while revisions remain; once the limit is hit, a pending
    request escalates to a human instead."""
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
            return ESCALATE, "approved, but checks are failing"
    if pending:
        return ESCALATE, f"revision limit ({max_revisions}) reached"
    return WAIT, None
