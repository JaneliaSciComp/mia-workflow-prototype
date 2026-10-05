import json

import httpx
import pytest
import respx

from zarr_doctor.github_api import GitHub, GitHubError

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
    route = respx.post(f"{API}{R}/issues/1/comments").mock(side_effect=[
        httpx.Response(403, json={"message": "You have exceeded a secondary rate limit"}),
        httpx.Response(403, headers={"retry-after": "7"}, json={"message": "slow down"}),
        httpx.Response(201, json={"id": 1}),
    ])
    assert (await gh.comment(1, "hi"))["id"] == 1
    assert route.call_count == 3
    assert gh.sleeps[:1] == [60.0] and 7.0 in gh.sleeps
    assert route.calls[0].request.headers["authorization"] == "Bearer tok"


@respx.mock
async def test_primary_rate_limit_waits_for_reset(gh, monkeypatch):
    monkeypatch.setattr("time.time", lambda: 1000.0)
    respx.get(f"{API}{R}/issues/5").mock(side_effect=[
        httpx.Response(403, headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "1030"}),
        httpx.Response(200, json={"number": 5}),
    ])
    assert (await gh.get_issue(5))["number"] == 5
    assert gh.sleeps == [31.0]


@respx.mock
async def test_plain_403_is_an_error(gh):
    respx.get(f"{API}{R}/issues/5").mock(return_value=httpx.Response(403, json={"message": "Resource not accessible"}))
    with pytest.raises(GitHubError) as e:
        await gh.get_issue(5)
    assert e.value.status == 403


@respx.mock
async def test_comment_once_skips_when_marker_present(gh):
    respx.get(f"{API}{R}/issues/3/comments").mock(return_value=httpx.Response(200, json=[
        {"body": "x"}, {"body": "done <!-- zarr-doctor: run=r1 result -->"}]))
    post = respx.post(f"{API}{R}/issues/3/comments").mock(return_value=httpx.Response(201, json={}))
    assert await gh.comment_once(3, "b", "<!-- zarr-doctor: run=r1 result -->") is False
    assert await gh.comment_once(3, "b", "<!-- zarr-doctor: run=r2 result -->") is True
    assert post.call_count == 1


@respx.mock
async def test_upsert_file_idempotent(gh):
    exists = respx.get(f"{API}{R}/contents/results/1/r1/stats.json").mock(side_effect=[
        httpx.Response(404), httpx.Response(200, json={"sha": "x"})])
    put = respx.put(f"{API}{R}/contents/results/1/r1/stats.json").mock(return_value=httpx.Response(201, json={}))
    assert await gh.upsert_file("results/1/r1/stats.json", b"{}", "b", "m") is True
    assert await gh.upsert_file("results/1/r1/stats.json", b"{}", "b", "m") is False
    assert put.call_count == 1 and exists.call_count == 2
    sent = json.loads(put.calls[0].request.content)
    assert sent["branch"] == "b" and sent["content"] == "e30="


@respx.mock
async def test_issue_markers_paged_and_skips_prs(gh):
    page1 = [{"number": i, "body": f"<!-- zarr-doctor: path=data/a/c{i}.zarr fingerprint=sha1:{i} -->"} for i in range(100)]
    page2 = [{"number": 200, "body": "no marker"}, {"number": 201, "pull_request": {}, "body": page1[0]["body"]}]
    respx.get(f"{API}{R}/issues").mock(side_effect=[httpx.Response(200, json=page1), httpx.Response(200, json=page2)])
    m = await gh.issue_markers()
    assert len(m) == 100 and m["data/a/c5.zarr"] == (5, "sha1:5")


@respx.mock
async def test_graphql_rate_limited_then_ok(gh):
    respx.post(f"{API}/graphql").mock(side_effect=[
        httpx.Response(200, json={"errors": [{"type": "RATE_LIMITED", "message": "x"}]}),
        httpx.Response(200, json={"data": {"ok": 1}}),
    ])
    assert await gh.graphql("query{ ok }") == {"ok": 1}
    assert gh.sleeps == [60.0]


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
