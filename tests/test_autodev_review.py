import httpx
import pytest
import respx

from autodev.github import GitHub
from autodev.models import Feedback
from autodev.review import approved, combine_checks, feedback, handled_ids

from autodev_helpers import API, R

MP = "<!-- autodev:"
HEAD = "abc1234def5678"
W = {"alice": "write", "bob": "read", "bot": "write", "admin": "admin"}


def rv(id, login, state, commit=HEAD, body="", at="2026-10-10T01:00:00Z"):
    return {"id": id, "user": {"login": login}, "state": state, "commit_id": commit, "body": body,
            "submitted_at": at}


def cm(id, login, body, at="2026-10-10T02:00:00Z"):
    return {"id": id, "user": {"login": login}, "body": body, "created_at": at}


def ok(reviews=(), comments=(), bot="bot", allow_self=False):
    return approved(HEAD, list(reviews), list(comments), W, bot, allow_self, MP)


def test_approval_must_match_head():
    assert ok([rv(1, "alice", "APPROVED")])
    assert not ok([rv(1, "alice", "APPROVED", commit="0ld")])


def test_approval_needs_write_access_and_a_human():
    assert not ok([rv(1, "bob", "APPROVED")])
    assert not ok([rv(1, "bot", "APPROVED")])
    assert ok([rv(1, "bot", "APPROVED")], allow_self=True)
    assert not ok([rv(1, None, "APPROVED")])  # deleted account


def test_later_changes_requested_supersedes_approval():
    assert not ok([rv(1, "alice", "APPROVED"), rv(2, "alice", "CHANGES_REQUESTED")])
    assert ok([rv(1, "alice", "APPROVED"), rv(2, "alice", "COMMENTED")])


def test_approve_comment_with_sha_prefix():
    assert ok(comments=[cm(1, "alice", "/approve abc1234")])
    assert not ok(comments=[cm(1, "alice", "/approve abc123")])  # too short
    assert not ok(comments=[cm(1, "alice", "/approve 0ld1234")])
    assert not ok(comments=[cm(1, "alice", f"/approve abc1234 {MP} x -->")])  # our own comment
    assert not ok(comments=[cm(1, "bob", "/approve abc1234")])


def test_feedback_from_reviews_and_revise_comments_oldest_first():
    reviews = [rv(3, "alice", "CHANGES_REQUESTED", body="rename foo", at="2026-10-10T01:00:00Z"),
               rv(4, "bob", "CHANGES_REQUESTED", body="ignored: reader")]
    inline = [{"pull_request_review_id": 3, "path": "a.py", "line": 4, "body": "here"},
              {"pull_request_review_id": 99, "path": "b.py", "line": 1, "body": "other review"}]
    comments = [cm(10, "admin", "/revise also update docs", at="2026-10-10T00:30:00Z"),
                cm(11, "alice", "looks good"), cm(12, "bot", f"/revise {MP} x -->")]
    fb = feedback(reviews, inline, comments, W, "bot", False, MP)
    assert fb == [Feedback("comment-10", "also update docs"),
                  Feedback("review-3", "rename foo\n- `a.py` line 4: here")]


def test_handled_ids_come_from_our_markers():
    comments = [cm(1, "bot", "done\n<!-- autodev: round k-2 -->\n<!-- autodev: handled review-3 -->"),
                cm(2, "alice", "<!-- autodev: handled review-9 -->"),  # a person pasting our marker does not count
                cm(3, "bot", "see <!-- autodev: round k-1 --> and handled review-7 -->")]  # not anchored to the prefix
    assert handled_ids(comments, MP, "bot") == {"review-3"}


@pytest.mark.parametrize("runs,statuses,want", [
    ([], [], "none"),
    ([{"status": "completed", "conclusion": "success"}], [{"state": "success"}], "success"),
    ([{"status": "in_progress", "conclusion": None}], [], "pending"),
    ([{"status": "completed", "conclusion": "skipped"}], [{"state": "pending"}], "pending"),
    ([{"status": "completed", "conclusion": "failure"}], [{"state": "pending"}], "failure"),
    ([], [{"state": "error"}], "failure"),
])
def test_combine_checks(runs, statuses, want):
    assert combine_checks(runs, statuses) == want


@pytest.fixture
def gh():
    async def no_sleep(_):
        pass
    return GitHub("o/r", lambda: "tok", writes_per_second=1000, sleep=no_sleep)


@respx.mock
async def test_label_actor_is_latest_labeler(gh):
    respx.get(f"{API}{R}/issues/1/events").mock(return_value=httpx.Response(200, json=[
        {"event": "labeled", "label": {"name": "autodev:go"}, "actor": {"login": "a"}},
        {"event": "unlabeled", "label": {"name": "autodev:go"}, "actor": {"login": "a"}},
        {"event": "labeled", "label": {"name": "autodev:go"}, "actor": {"login": "b"}},
        {"event": "labeled", "label": {"name": "other"}, "actor": {"login": "c"}},
    ]))
    assert await gh.label_actor(1, "autodev:go") == "b"


@respx.mock
async def test_permission_of_stranger_is_none(gh):
    respx.get(f"{API}{R}/collaborators/eve/permission").mock(return_value=httpx.Response(404, json={}))
    assert await gh.permission("eve") == "none"


@respx.mock
async def test_merge_refusal_is_reported_not_raised(gh):
    respx.put(f"{API}{R}/pulls/5/merge").mock(return_value=httpx.Response(405, json={"message": "Not mergeable"}))
    merged, msg = await gh.merge_pr(5, HEAD)
    assert not merged and "405" in msg


@respx.mock
async def test_check_state_reads_both_apis(gh):
    respx.get(f"{API}{R}/commits/{HEAD}/check-runs").mock(return_value=httpx.Response(200, json={
        "check_runs": [{"status": "completed", "conclusion": "success"}]}))
    respx.get(f"{API}{R}/commits/{HEAD}/status").mock(return_value=httpx.Response(200, json={"statuses": []}))
    assert await gh.check_state(HEAD) == "success"
