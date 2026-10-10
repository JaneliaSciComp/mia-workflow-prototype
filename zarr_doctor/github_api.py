"""Thin async GitHub client: REST + GraphQL, rate-limit backoff, write throttling, idempotent helpers."""

from __future__ import annotations

import asyncio
import base64
import collections
import logging
import os
import time
from typing import Any

import httpx

from .discovery import MARKER_RE

log = logging.getLogger(__name__)


def get_token(cfg) -> str:
    """The one place auth comes from. Swap this for a GitHub App installation token later."""
    token = os.environ.get(cfg.github.token_env)
    if not token:
        raise RuntimeError(f"${cfg.github.token_env} is not set (export it or put it in .env)")
    return token


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
        self._etags: dict[tuple, tuple[str, httpx.Response]] = {}  # (path, params) -> (etag, last 200 response)
        self._creates: collections.deque[float] = collections.deque()
        # Contents API PUTs to one branch race (409 "is at X but expected Y"); serialize them in-process.
        self._commit_lock = asyncio.Lock()
        self._client = httpx.AsyncClient(base_url=api_url, timeout=30, headers={
            "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})

    @classmethod
    def from_config(cls, cfg) -> "GitHub":
        return cls(cfg.github.repo, lambda: get_token(cfg), api_url=cfg.github.api_url,
                   writes_per_second=cfg.discovery.creates_per_second)

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
                      ok404: bool = False, write: bool | None = None, attempts: int = 8,
                      conditional: bool = False) -> httpx.Response | None:
        """`conditional`: GET with If-None-Match; a 304 returns the response cached from the last 200 (free of rate limit)."""
        ckey = (path, tuple(sorted((params or {}).items()))) if conditional and method == "GET" else None
        if write is None:
            write = method != "GET"
        create = write and method == "POST"
        for attempt in range(attempts):
            if write:
                await self._throttle(create)
            headers = {"Authorization": f"Bearer {self._token_fn()}"}
            if ckey in self._etags:
                headers["If-None-Match"] = self._etags[ckey][0]
            try:
                r = await self._client.request(method, path, json=json, params=params, headers=headers)
            except httpx.TransportError as e:
                log.warning("github %s %s transport error %s (attempt %d)", method, path, e, attempt + 1)
                await self._sleep(min(2**attempt, 60))
                continue
            delay = self._rate_limit_delay(r, attempt)
            if delay is not None:
                log.warning("github rate limited on %s %s; sleeping %.0fs", method, path, delay)
                await self._sleep(delay)
                continue
            if r.status_code == 304 and ckey in self._etags:
                return self._etags[ckey][1]
            if r.status_code >= 500:
                await self._sleep(min(2**attempt, 60))
                continue
            if r.status_code == 404 and ok404:
                return None
            if r.is_error:
                raise GitHubError(f"{method} {path} -> {r.status_code}: {r.text[:500]}", r.status_code)
            if write:
                log.info("github %s %s -> %d", method, path, r.status_code)
            if ckey is not None:
                if r.headers.get("etag"):
                    self._etags[ckey] = (r.headers["etag"], r)
                else:
                    self._etags.pop(ckey, None)
            return r
        raise GitHubError(f"{method} {path}: gave up after {attempts} attempts")

    async def graphql(self, query: str, variables: dict | None = None) -> dict:
        is_mutation = query.lstrip().startswith("mutation")
        for attempt in range(8):
            r = await self.request("POST", "/graphql", json={"query": query, "variables": variables or {}},
                                   write=is_mutation)
            body = r.json()
            errors = body.get("errors") or []
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                await self._sleep(min(60.0 * 2**attempt, 900.0))
                continue
            if errors:
                raise GitHubError(f"graphql: {errors}")
            return body["data"]
        raise GitHubError("graphql: rate limited, gave up")

    async def _paged(self, path: str, params: dict | None = None, conditional: bool = False):
        page = 1
        while True:
            items = (await self.request("GET", path, params={**(params or {}), "per_page": 100, "page": page},
                                      conditional=conditional)).json()
            for it in items:
                yield it
            if len(items) < 100:
                return
            page += 1

    # --- issues -------------------------------------------------------------------------------

    async def create_issue(self, title: str, body: str, labels: list[str]) -> dict:
        return (await self.request("POST", f"/repos/{self.repo}/issues",
                                   json={"title": title, "body": body, "labels": labels})).json()

    async def get_issue(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/issues/{number}")).json()

    async def update_issue(self, number: int, **fields) -> dict:
        return (await self.request("PATCH", f"/repos/{self.repo}/issues/{number}", json=fields)).json()

    async def list_issues(self, **params) -> list[dict]:
        return [i async for i in self._paged(f"/repos/{self.repo}/issues", params) if "pull_request" not in i]

    async def issue_markers(self) -> dict[str, tuple[int, str]]:
        """path -> (issue number, fingerprint) for every zarr-doctor issue in the repo."""
        out = {}
        for i in await self.list_issues(state="all"):
            m = MARKER_RE.search(i.get("body") or "")
            if m:
                out.setdefault(m.group(1), (i["number"], m.group(2)))
        return out

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

    async def comment_once(self, number: int, body: str, marker: str) -> bool:
        """Post unless a comment containing `marker` exists. Returns True if posted."""
        async for c in self._paged(f"/repos/{self.repo}/issues/{number}/comments"):
            if marker in (c.get("body") or ""):
                return False
        await self.comment(number, body)
        return True

    # --- contents / branches ------------------------------------------------------------------

    async def ensure_branch(self, branch: str) -> bool:
        """Create `branch` from an orphan commit if missing. Returns True if created."""
        if await self.request("GET", f"/repos/{self.repo}/git/ref/heads/{branch}", ok404=True):
            return False
        tree = (await self.request("POST", f"/repos/{self.repo}/git/trees", json={"tree": [{
            "path": "README.md", "mode": "100644", "type": "blob",
            "content": "Zarr Doctor results (histograms + stats), written by the service.\n"}]})).json()["sha"]
        commit = (await self.request("POST", f"/repos/{self.repo}/git/commits", json={
            "message": "zarr-doctor: initialize results branch", "tree": tree, "parents": []})).json()["sha"]
        await self.request("POST", f"/repos/{self.repo}/git/refs", json={"ref": f"refs/heads/{branch}", "sha": commit})
        return True

    async def file_exists(self, path: str, ref: str) -> bool:
        return await self.request("GET", f"/repos/{self.repo}/contents/{path}", params={"ref": ref}, ok404=True) is not None

    async def upsert_file(self, path: str, content: bytes, branch: str, message: str) -> bool:
        """Commit `content` at `path` unless the file already exists there. Returns True if committed."""
        async with self._commit_lock:
            if await self.file_exists(path, branch):
                return False
            await self.request("PUT", f"/repos/{self.repo}/contents/{path}", json={
                "message": message, "branch": branch, "content": base64.b64encode(content).decode()})
            return True
