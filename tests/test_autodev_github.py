import json

import httpx
import pytest
import respx

from autodev.github import GitHub, GitHubError

API = "https://api.github.com"
R = "/repos/o/r"


@pytest.fixture
def gh():
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    g = GitHub("o/r", lambda: "tok", writes_per_second=1000, sleep=fake_sleep)
    g.sleeps = sleeps
    return g


@respx.mock
async def test_secondary_rate_limit_backoff(gh):
    route = respx.post(f"{API}{R}/issues/1/comments").mock(
        side_effect=[
            httpx.Response(403, json={"message": "You have exceeded a secondary rate limit"}),
            httpx.Response(403, headers={"retry-after": "7"}, json={"message": "slow down"}),
            httpx.Response(201, json={"id": 1}),
        ]
    )
    assert (await gh.comment(1, "hi"))["id"] == 1
    assert route.call_count == 3
    assert gh.sleeps[:1] == [60.0] and 7.0 in gh.sleeps
    assert route.calls[0].request.headers["authorization"] == "Bearer tok"


@respx.mock
async def test_primary_rate_limit_waits_for_reset(gh, monkeypatch):
    monkeypatch.setattr("time.time", lambda: 1000.0)
    respx.get(f"{API}{R}/issues/5").mock(
        side_effect=[
            httpx.Response(403, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1030"}),
            httpx.Response(200, json={"number": 5}),
        ]
    )
    assert (await gh.get_issue(5))["number"] == 5
    assert gh.sleeps == [31.0]


@respx.mock
async def test_plain_403_is_an_error(gh):
    respx.get(f"{API}{R}/issues/5").mock(return_value=httpx.Response(403, json={"message": "Resource not accessible"}))
    with pytest.raises(GitHubError) as e:
        await gh.get_issue(5)
    assert e.value.status == 403


@respx.mock
async def test_write_throttle(monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    g = GitHub("o/r", lambda: "t", writes_per_second=1.0, sleep=fake_sleep)
    respx.post(f"{API}{R}/issues/1/comments").mock(return_value=httpx.Response(201, json={}))
    await g.comment(1, "a")
    await g.comment(1, "b")
    assert len(sleeps) == 1 and 0.9 < sleeps[0] <= 1.0


@respx.mock
async def test_label_event_and_actor(gh):
    respx.get(f"{API}{R}/issues/3/events").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"id": 1, "event": "labeled", "label": {"name": "x"}, "actor": {"login": "a"}, "created_at": "t1"},
                {"id": 2, "event": "labeled", "label": {"name": "x"}, "actor": {"login": "b"}, "created_at": "t2"},
                {"id": 3, "event": "labeled", "label": {"name": "y"}, "actor": {"login": "c"}, "created_at": "t3"},
            ],
        )
    )
    assert await gh.label_event(3, "x") == ("b", "2", "t2")
    assert await gh.label_actor(3, "x") == "b"
    assert await gh.label_event(3, "z") == (None, None, None)


@respx.mock
async def test_create_issue(gh):
    route = respx.post(f"{API}{R}/issues").mock(return_value=httpx.Response(201, json={"number": 9}))
    assert (await gh.create_issue("t", "b", ["l"]))["number"] == 9
    assert json.loads(route.calls[0].request.content) == {"title": "t", "body": "b", "labels": ["l"]}


@respx.mock
async def test_find_issue_by_marker(gh):
    route = respx.get(f"{API}{R}/issues").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"number": 1, "body": "nope"},
                {"number": 2, "body": "x <!--m1--> y"},
                {"number": 3, "body": "<!--m1-->"},
                {"number": 4, "body": "<!--m1-->", "pull_request": {}},
            ],
        )
    )
    assert (await gh.find_issue_by_marker("<!--m1-->"))["number"] == 2
    assert route.calls[0].request.url.params["state"] == "all"
    assert await gh.find_issue_by_marker("zzz") is None
    await gh.find_issue_by_marker("<!--m1-->", ["a", "b"])
    assert route.calls[-1].request.url.params["labels"] == "a,b"


@respx.mock
async def test_find_and_edit_comment(gh):
    respx.get(f"{API}{R}/issues/4/comments").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "body": "a"}, {"id": 2, "body": "has MK"}])
    )
    assert (await gh.find_comment(4, "MK"))["id"] == 2
    assert await gh.find_comment(4, "nope") is None
    route = respx.patch(f"{API}{R}/issues/comments/2").mock(return_value=httpx.Response(200, json={"id": 2}))
    assert (await gh.edit_comment(2, "new"))["id"] == 2
    assert json.loads(route.calls[0].request.content) == {"body": "new"}


@respx.mock
async def test_set_status_and_statuses_throttled():
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    g = GitHub("o/r", lambda: "t", writes_per_second=1.0, sleep=fake_sleep)
    route = respx.post(f"{API}{R}/statuses/abc").mock(return_value=httpx.Response(201, json={}))
    await g.set_status("abc", "ctx", "success", "d" * 200, "http://u")
    await g.set_status("abc", "ctx", "failure", "d", None)
    assert json.loads(route.calls[0].request.content) == {
        "state": "success",
        "context": "ctx",
        "description": "d" * 140,
        "target_url": "http://u",
    }
    assert len(sleeps) == 1
    respx.get(f"{API}{R}/commits/abc/status").mock(
        return_value=httpx.Response(200, json={"statuses": [{"context": "c"}]})
    )
    assert await g.statuses("abc") == [{"context": "c"}]


@respx.mock
async def test_dir_sha(gh):
    route = respx.get(f"{API}{R}/contents/a/b").mock(
        return_value=httpx.Response(
            200, json=[{"name": "c", "type": "dir", "sha": "S1"}, {"name": "d", "type": "file", "sha": "S2"}]
        )
    )
    assert await gh.dir_sha("a/b/c", "main") == "S1"
    assert route.calls[0].request.url.params["ref"] == "main"
    assert await gh.dir_sha("a/b/d", "main") is None  # a file, not a dir
    assert await gh.dir_sha("a/b/zz", "main") is None
    respx.get(f"{API}{R}/contents/q").mock(return_value=httpx.Response(404, json={}))
    assert await gh.dir_sha("q/c", "main") is None


@respx.mock
async def test_get_file(gh):
    route = respx.get(f"{API}{R}/contents/a.txt").mock(return_value=httpx.Response(200, content=b"raw"))
    assert await gh.get_file("a.txt", "main") == b"raw"
    assert route.calls[0].request.headers["accept"] == "application/vnd.github.raw"
    respx.get(f"{API}{R}/contents/none").mock(return_value=httpx.Response(404, json={}))
    assert await gh.get_file("none", "main") is None


@respx.mock
async def test_ref_prs_and_files(gh):
    respx.get(f"{API}{R}/git/ref/heads/br").mock(return_value=httpx.Response(200, json={"object": {"sha": "S"}}))
    assert await gh.get_ref_sha("br") == "S"
    route = respx.get(f"{API}{R}/pulls").mock(return_value=httpx.Response(200, json=[{"number": 1}]))
    assert await gh.list_prs() == [{"number": 1}]
    p = route.calls[0].request.url.params
    assert (p["state"], p["sort"], p["direction"], p["per_page"]) == ("all", "updated", "desc", "50")
    f = respx.get(f"{API}{R}/pulls/7/files").mock(
        return_value=httpx.Response(200, json=[{"filename": "snippets/a.py"}])
    )
    assert await gh.pr_files(7) == ["snippets/a.py"]
    assert f.calls[0].request.url.params["per_page"] == "100"


@respx.mock
async def test_ensure_branch_noop_second_time(gh):
    respx.get(f"{API}{R}/git/ref/heads/g").mock(
        side_effect=[httpx.Response(404, json={}), httpx.Response(200, json={"object": {"sha": "c"}})]
    )
    respx.post(f"{API}{R}/git/trees").mock(return_value=httpx.Response(201, json={"sha": "t"}))
    commits = respx.post(f"{API}{R}/git/commits").mock(return_value=httpx.Response(201, json={"sha": "c"}))
    refs = respx.post(f"{API}{R}/git/refs").mock(return_value=httpx.Response(201, json={}))
    assert await gh.ensure_branch("g") is True
    assert await gh.ensure_branch("g") is False
    assert commits.call_count == 1 and refs.call_count == 1


@respx.mock
async def test_upsert_file_skips_existing(gh):
    respx.get(f"{API}{R}/contents/a/f.json").mock(
        side_effect=[httpx.Response(404, json={}), httpx.Response(200, json={"sha": "x"})]
    )
    put = respx.put(f"{API}{R}/contents/a/f.json").mock(return_value=httpx.Response(201, json={}))
    assert await gh.upsert_file("a/f.json", b"{}", "b", "m") is True
    assert await gh.upsert_file("a/f.json", b"{}", "b", "m") is False
    assert put.call_count == 1
    assert json.loads(put.calls[0].request.content) == {"message": "m", "branch": "b", "content": "e30="}


@respx.mock
async def test_conditional_get_replaces_cached_etag_on_change(gh):
    route = respx.get(f"{API}{R}/pulls/5").mock(
        side_effect=[
            httpx.Response(200, json={"n": 1}, headers={"etag": '"a"'}),
            httpx.Response(200, json={"n": 2}, headers={"etag": '"b"'}),
            httpx.Response(304),
        ]
    )
    assert [(await gh.get_pr(5))["n"] for _ in range(3)] == [1, 2, 2]
    assert "if-none-match" not in route.calls[0].request.headers
    assert [c.request.headers["if-none-match"] for c in route.calls[1:]] == ['"a"', '"b"']
