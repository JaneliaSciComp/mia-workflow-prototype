from autodev.decide import CLOSED, DONE, ESCALATE, MERGE, REVISE, WAIT, decide, escalation
from autodev.models import Feedback, PrSnapshot

F1, F2 = Feedback("review-1", "a"), Feedback("comment-2", "b")


def snap(state="open", approved=False, checks="none", feedback=()):
    return PrSnapshot(5, state, "h", approved, checks, list(feedback))


def test_terminal_states():
    assert decide(snap("merged"), [], 0, 3) == (DONE, None)
    assert decide(snap("closed", approved=True), [], 0, 3) == (CLOSED, None)


def test_unhandled_feedback_wins_over_approval():
    assert decide(snap(approved=True, checks="success", feedback=[F1, F2]), ["review-1"], 1, 3) == (REVISE, F2)


def test_approval_with_passing_or_no_checks_merges():
    assert decide(snap(approved=True, checks="success"), [], 0, 3) == (MERGE, None)
    assert decide(snap(approved=True, checks="none"), [], 0, 3) == (MERGE, None)


def test_stale_approval_waits():
    # PrSnapshot.approved is computed against the current head, so a pre-revision approval arrives as False.
    assert decide(snap(approved=False, checks="success"), [], 1, 3) == (WAIT, None)


def test_approved_but_pending_waits_and_failing_escalates():
    assert decide(snap(approved=True, checks="pending"), [], 0, 3) == (WAIT, None)
    assert decide(snap(approved=True, checks="failure"), [], 0, 3)[0] == ESCALATE


def test_revision_limit_escalates_unless_approved():
    assert decide(snap(feedback=[F1]), [], 3, 3) == (ESCALATE, "revision limit (3) reached")
    assert decide(snap(approved=True, feedback=[F1]), [], 3, 3) == (MERGE, None)


def test_raised_escalation_waits_until_the_head_changes():
    reason = "revision limit (3) reached"
    raised = escalation(reason, snap())
    assert decide(snap(feedback=[F1]), [], 3, 3, raised) == (WAIT, None)
    assert decide(snap(approved=True, checks="failure"), [], 3, 3, raised)[0] == ESCALATE  # a different reason
    assert decide(PrSnapshot(5, "open", "h2", False, "none", [F1]), [], 3, 3, raised) == (ESCALATE, reason)
