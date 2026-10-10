"""What to do next with an open PR. Pure, so the workflow can call it and tests can table it."""

from __future__ import annotations

from .models import Feedback, PrSnapshot

DONE, CLOSED, MERGE, REVISE, ESCALATE, WAIT = "DONE", "CLOSED", "MERGE", "REVISE", "ESCALATE", "WAIT"


def decide(snap: PrSnapshot, handled: list[str], revisions: int,
           max_revisions: int) -> tuple[str, Feedback | str | None]:
    if snap.state == "merged":
        return DONE, None
    if snap.state == "closed":
        return CLOSED, None
    pending = [f for f in snap.feedback if f.id not in handled]
    if pending and revisions < max_revisions:
        return REVISE, pending[0]
    if snap.approved:
        if snap.checks in ("success", "none"):
            return MERGE, None
        if snap.checks == "failure":
            return ESCALATE, "approved, but checks are failing"
    if pending:
        return ESCALATE, f"revision limit ({max_revisions}) reached"
    return WAIT, None
