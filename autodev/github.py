"""Thin async GitHub client: REST, rate-limit backoff, write throttling, and the calls autodev makes."""

from __future__ import annotations

import asyncio
import collections
import logging
import time
from typing import Any

import httpx

from .auth import token_fn
from .review import combine_checks

log = logging.getLogger(__name__)


class GitHubError(Exception):
    def __init__(self, msg: str, status: int | None = None):
        super().__init__(msg)
        self.status = status


class GitHub:
    def __init__(self, repo: str, token_fn, *, api_url: str = "https://api.github.com",
                 writes_per_second: float = 1.0, creates_per_hour: int = 400, sleep=asyncio.sleep):
        self.repo = repo
        self._token_fn = token_fn
        self._sleep = sleep
        self._min_gap = 1.0 / writes_per_second
        self._creates_per_hour = creates_per_hour
        self._last_write = 0.0
        self._creates: collections.deque[float] = collections.deque()
        self._client = httpx.AsyncClient(base_url=api_url, timeout=30, headers={
            "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})

    @classmethod
    def for_autodev(cls, cfg, tokens=None) -> "GitHub":
        return cls(cfg.github.repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)

    @property
    def tokens(self):
        return self._token_fn

    async def aclose(self) -> None:
        await self._client.aclose()

    # --- transport ---------------------------------------------------------------------------

    async def _throttle(self, create: bool) -> None:
        # ponytail: per-process limits; a second process writing with the same token is not counted.
        wait = self._last_write + self._min_gap - time.monotonic()
        if wait > 0:
            await self._sleep(wait)
        if create:
            while self._creates and self._creates[0] < time.monotonic() - 3600:
                self._creates.popleft()
            if len(self._creates) >= self._creates_per_hour:
                await self._sleep(self._creates[0] + 3600 - time.monotonic())
            self._creates.append(time.monotonic())
        self._last_write = time.monotonic()

    @staticmethod
    def _rate_limit_delay(r: httpx.Response, attempt: int) -> float | None:
        """Seconds to wait if this response is a (primary or secondary) rate limit, else None."""
        if r.status_code not in (403, 429):
            return None
        if "retry-after" in r.headers:
            return float(r.headers["retry-after"])
        if r.headers.get("x-ratelimit-remaining") == "0" and "x-ratelimit-reset" in r.headers:
            return max(1.0, float(r.headers["x-ratelimit-reset"]) - time.time() + 1)
        if r.status_code == 429 or "rate limit" in r.text.lower():
            return min(60.0 * 2**attempt, 900.0)  # GitHub: wait ≥1 min, then back off exponentially
        return None

    async def request(self, method: str, path: str, *, json: Any = None, params: dict | None = None,
                      ok404: bool = False, write: bool | None = None, attempts: int = 8) -> httpx.Response | None:
        if write is None:
            write = method != "GET"
        create = write and method == "POST"
        for attempt in range(attempts):
            if write:
                await self._throttle(create)
            try:
                r = await self._client.request(method, path, json=json, params=params,
                                               headers={"Authorization": f"Bearer {self._token_fn()}"})
            except httpx.TransportError as e:
                log.warning("github %s %s transport error %s (attempt %d)", method, path, e, attempt + 1)
                await self._sleep(min(2**attempt, 60))
                continue
            delay = self._rate_limit_delay(r, attempt)
            if delay is not None:
                log.warning("github rate limited on %s %s; sleeping %.0fs", method, path, delay)
                await self._sleep(delay)
                continue
            if r.status_code >= 500:
                await self._sleep(min(2**attempt, 60))
                continue
            if r.status_code == 404 and ok404:
                return None
            if r.is_error:
                raise GitHubError(f"{method} {path} -> {r.status_code}: {r.text[:500]}", r.status_code)
            if write:
                log.info("github %s %s -> %d", method, path, r.status_code)
            return r
        raise GitHubError(f"{method} {path}: gave up after {attempts} attempts")

    async def _paged(self, path: str, params: dict | None = None):
        page = 1
        while True:
            items = (await self.request("GET", path, params={**(params or {}), "per_page": 100, "page": page})).json()
            for it in items:
                yield it
            if len(items) < 100:
                return
            page += 1

    # --- issues -------------------------------------------------------------------------------

    async def get_issue(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/issues/{number}")).json()

    async def update_issue(self, number: int, **fields) -> dict:
        return (await self.request("PATCH", f"/repos/{self.repo}/issues/{number}", json=fields)).json()

    async def list_issues(self, **params) -> list[dict]:
        return [i async for i in self._paged(f"/repos/{self.repo}/issues", params) if "pull_request" not in i]

    async def add_labels(self, number: int, labels: list[str]) -> None:
        await self.request("POST", f"/repos/{self.repo}/issues/{number}/labels", json={"labels": labels}, write=True)

    async def remove_label(self, number: int, label: str) -> None:
        await self.request("DELETE", f"/repos/{self.repo}/issues/{number}/labels/{label}", ok404=True)

    async def ensure_label(self, name: str, color: str, description: str = "") -> bool:
        """Create the label if missing. Returns True if created."""
        if await self.request("GET", f"/repos/{self.repo}/labels/{name}", ok404=True):
            return False
        await self.request("POST", f"/repos/{self.repo}/labels",
                           json={"name": name, "color": color, "description": description})
        return True

    async def comment(self, number: int, body: str) -> dict:
        return (await self.request("POST", f"/repos/{self.repo}/issues/{number}/comments", json={"body": body})).json()

    async def permission(self, login: str) -> str:
        if login.endswith("[bot]"):  # GitHub may 403/422 the lookup for app bots; a bot never reviews
            return "none"
        r = await self.request("GET", f"/repos/{self.repo}/collaborators/{login}/permission", ok404=True)
        return r.json()["permission"] if r else "none"

    async def label_actor(self, issue: int, label: str) -> str | None:
        """Login of whoever most recently added `label` to the issue."""
        actor = None
        async for e in self._paged(f"/repos/{self.repo}/issues/{issue}/events"):
            if e.get("event") == "labeled" and (e.get("label") or {}).get("name") == label:
                actor = (e.get("actor") or {}).get("login")
        return actor

    async def issue_comments(self, number: int) -> list[dict]:
        return [c async for c in self._paged(f"/repos/{self.repo}/issues/{number}/comments")]

    # --- pull requests ---------------------------------------------------------------------

    async def find_pr(self, branch: str) -> dict | None:
        owner = self.repo.split("/")[0]
        prs = (await self.request("GET", f"/repos/{self.repo}/pulls",
                                  params={"head": f"{owner}:{branch}", "state": "open"})).json()
        return prs[0] if prs else None

    async def create_pr(self, title: str, body: str, head: str, base: str) -> dict:
        return (await self.request("POST", f"/repos/{self.repo}/pulls",
                                   json={"title": title, "body": body, "head": head, "base": base})).json()

    async def get_pr(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/pulls/{number}")).json()

    async def pr_reviews(self, number: int) -> list[dict]:
        return [r async for r in self._paged(f"/repos/{self.repo}/pulls/{number}/reviews")]

    async def pr_review_comments(self, number: int) -> list[dict]:
        return [c async for c in self._paged(f"/repos/{self.repo}/pulls/{number}/comments")]

    async def check_state(self, sha: str) -> str:
        runs = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/check-runs",
                                   params={"per_page": 100})).json()["check_runs"]
        statuses = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/status")).json()["statuses"]
        return combine_checks(runs, statuses)

    async def merge_pr(self, number: int, sha: str) -> tuple[bool, str]:
        """Squash-merge exactly `sha`. GitHub refusing (protection, conflict, moved head) is a result, not an error."""
        try:
            await self.request("PUT", f"/repos/{self.repo}/pulls/{number}/merge",
                               json={"sha": sha, "merge_method": "squash"})
            return True, "merged"
        except GitHubError as e:
            if e.status in (405, 409, 422):
                return False, str(e)
            raise

    async def delete_branch(self, branch: str) -> None:
        try:
            await self.request("DELETE", f"/repos/{self.repo}/git/refs/heads/{branch}", ok404=True)
        except GitHubError as e:
            if e.status != 422:  # already gone
                raise
