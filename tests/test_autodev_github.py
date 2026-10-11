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
async def test_write_throttle(monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    g = GitHub("o/r", lambda: "t", writes_per_second=1.0, sleep=fake_sleep)
    respx.post(f"{API}{R}/issues/1/comments").mock(return_value=httpx.Response(201, json={}))
    await g.comment(1, "a")
    await g.comment(1, "b")
    assert len(sleeps) == 1 and 0.9 < sleeps[0] <= 1.0
