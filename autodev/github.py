"""A small async GitHub REST client.

It handles what the REST API needs handled (pagination, rate-limit backoff, retries on transport and 5xx
errors, write throttling) and exposes one method per call autodev and mia-data make. Responses are returned
as the plain dicts GitHub sends; callers pick the fields they need.
"""

from __future__ import annotations

import asyncio
import base64
import collections
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

import httpx

from .auth import token_fn
from .review import combine_checks

if TYPE_CHECKING:
    from .config import Config

log = logging.getLogger(__name__)


class GitHubError(Exception):
    """A request failed for good; `status` is the HTTP status, or None if the request never got a response."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class GitHub:
    """One client per repository. `token_fn` is called for every request, so rotating tokens (GitHub App
    installation tokens) work without restarting the worker."""

    def __init__(
        self,
        repo: str,
        token_fn: Callable[[], str | None],
        *,
        api_url: str = "https://api.github.com",
        writes_per_second: float = 1.0,
        creates_per_hour: int = 400,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.repo = repo
        self._token_fn = token_fn
        self._sleep = sleep
        self._min_gap = 1.0 / writes_per_second
        self._creates_per_hour = creates_per_hour
        self._last_write = 0.0
        self._creates: collections.deque[float] = collections.deque()  # times of recent POSTs, for the hourly cap
        # ETag and last 200 response of each conditional GET, keyed by (path, params). GitHub does not count a
        # 304 against the rate limit, so polling a quiet PR is nearly free. Lives in the worker process.
        # ponytail: unbounded; one entry per polled URL. Bound it if a worker ever polls thousands of PRs.
        self._etags: dict[tuple, tuple[str, httpx.Response]] = {}
        # Contents API PUTs to one branch race (409 "is at X but expected Y"); serialize them in-process.
        self._commit_lock = asyncio.Lock()
        self._client = httpx.AsyncClient(
            base_url=api_url,
            timeout=30,
            headers={"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
        )

    @classmethod
    def for_autodev(cls, cfg: Config, tokens: Callable[[], str | None] | None = None) -> GitHub:
        return cls(cfg.github.repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)

    @property
    def tokens(self) -> Callable[[], str | None]:
        """The token provider, so callers can scrub the current token from text they post."""
        return self._token_fn

    async def aclose(self) -> None:
        await self._client.aclose()

    # --- transport ---------------------------------------------------------------------------

    async def _throttle(self, create: bool) -> None:
        """Space out writes, and cap creations per hour, to stay under GitHub's secondary rate limits."""
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
    def _rate_limit_delay(response: httpx.Response, attempt: int) -> float | None:
        """Seconds to wait if this response is a (primary or secondary) rate limit, else None."""
        if response.status_code not in (403, 429):
            return None
        if "retry-after" in response.headers:
            return float(response.headers["retry-after"])
        if response.headers.get("x-ratelimit-remaining") == "0" and "x-ratelimit-reset" in response.headers:
            return max(1.0, float(response.headers["x-ratelimit-reset"]) - time.time() + 1)
        if response.status_code == 429 or "rate limit" in response.text.lower():
            return min(60.0 * 2**attempt, 900.0)  # GitHub: wait ≥1 min, then back off exponentially
        return None

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict | None = None,
        ok404: bool = False,
        write: bool | None = None,
        attempts: int = 8,
        headers: dict | None = None,
        conditional: bool = False,
    ) -> httpx.Response | None:
        """One API call with retries. With `ok404`, a 404 returns None instead of raising.

        `write` marks a request that counts against the write throttle; it defaults to "not a GET".
        `conditional` (GETs only) sends the ETag of the last 200 for this URL as If-None-Match, and a 304
        answer returns that cached response, so callers never see the 304."""
        if write is None:
            write = method != "GET"
        create = write and method == "POST"
        cache_key = (path, tuple(sorted((params or {}).items()))) if conditional and method == "GET" else None
        for attempt in range(attempts):
            if write:
                await self._throttle(create)
            request_headers = {**(headers or {}), "Authorization": f"Bearer {self._token_fn()}"}
            if cache_key in self._etags:
                request_headers["If-None-Match"] = self._etags[cache_key][0]
            try:
                response = await self._client.request(method, path, json=json, params=params, headers=request_headers)
            except httpx.TransportError as error:
                log.warning("github %s %s transport error %s (attempt %d)", method, path, error, attempt + 1)
                await self._sleep(min(2**attempt, 60))
                continue
            delay = self._rate_limit_delay(response, attempt)
            if delay is not None:
                log.warning("github rate limited on %s %s; sleeping %.0fs", method, path, delay)
                await self._sleep(delay)
                continue
            if response.status_code >= 500:
                await self._sleep(min(2**attempt, 60))
                continue
            if response.status_code == 304 and cache_key in self._etags:
                return self._etags[cache_key][1]
            if response.status_code == 404 and ok404:
                return None
            if response.is_error:
                raise GitHubError(
                    f"{method} {path} -> {response.status_code}: {response.text[:500]}", response.status_code
                )
            if write:
                log.info("github %s %s -> %d", method, path, response.status_code)
            if cache_key is not None:
                if response.headers.get("etag"):
                    self._etags[cache_key] = (response.headers["etag"], response)
                else:
                    self._etags.pop(cache_key, None)
            return response
        raise GitHubError(f"{method} {path}: gave up after {attempts} attempts")

    async def _paged(self, path: str, params: dict | None = None, conditional: bool = False) -> AsyncIterator[dict]:
        """Yield every item of a paginated list endpoint. Each page has its own URL, so its own ETag."""
        page = 1
        while True:
            items = (
                await self.request(
                    "GET", path, params={**(params or {}), "per_page": 100, "page": page}, conditional=conditional
                )
            ).json()
            for item in items:
                yield item
            if len(items) < 100:
                return
            page += 1

    # --- issues -------------------------------------------------------------------------------

    async def get_issue(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/issues/{number}")).json()

    async def update_issue(self, number: int, **fields) -> dict:
        return (await self.request("PATCH", f"/repos/{self.repo}/issues/{number}", json=fields)).json()

    async def list_issues(self, **params) -> list[dict]:
        """Issues matching `params` (GitHub query parameters), excluding pull requests."""
        return [
            issue async for issue in self._paged(f"/repos/{self.repo}/issues", params) if "pull_request" not in issue
        ]

    async def add_labels(self, number: int, labels: list[str]) -> None:
        await self.request("POST", f"/repos/{self.repo}/issues/{number}/labels", json={"labels": labels}, write=True)

    async def remove_label(self, number: int, label: str) -> None:
        await self.request("DELETE", f"/repos/{self.repo}/issues/{number}/labels/{label}", ok404=True)

    async def ensure_label(self, name: str, color: str, description: str = "") -> bool:
        """Create the label if missing. Returns True if created."""
        if await self.request("GET", f"/repos/{self.repo}/labels/{name}", ok404=True):
            return False
        await self.request(
            "POST", f"/repos/{self.repo}/labels", json={"name": name, "color": color, "description": description}
        )
        return True

    async def comment(self, number: int, body: str) -> dict:
        return (await self.request("POST", f"/repos/{self.repo}/issues/{number}/comments", json={"body": body})).json()

    async def permission(self, login: str) -> str:
        """The user's permission on the repo: admin | maintain | write | triage | read | none."""
        if login.endswith("[bot]"):  # GitHub may 403/422 the lookup for app bots; a bot never reviews
            return "none"
        response = await self.request("GET", f"/repos/{self.repo}/collaborators/{login}/permission", ok404=True)
        return response.json()["permission"] if response else "none"

    async def label_event(self, issue: int, label: str) -> tuple[str | None, str | None, str | None]:
        """(actor login, event id, created_at) of the most recent `labeled` event for `label`."""
        found = (None, None, None)
        async for event in self._paged(f"/repos/{self.repo}/issues/{issue}/events"):
            if event.get("event") == "labeled" and (event.get("label") or {}).get("name") == label:
                found = (
                    (event.get("actor") or {}).get("login"),
                    str(event["id"]) if "id" in event else None,
                    event.get("created_at"),
                )
        return found

    async def label_actor(self, issue: int, label: str) -> str | None:
        """Login of whoever most recently added `label` to the issue."""
        return (await self.label_event(issue, label))[0]

    async def create_issue(self, title: str, body: str, labels: list[str]) -> dict:
        return (
            await self.request(
                "POST", f"/repos/{self.repo}/issues", json={"title": title, "body": body, "labels": labels}
            )
        ).json()

    async def find_issue_by_marker(self, marker: str, labels: list[str] | None = None) -> dict | None:
        """First issue (open or closed) whose body contains `marker`. Callers cache the result."""
        params = {"state": "all", **({"labels": ",".join(labels)} if labels else {})}
        return next((issue for issue in await self.list_issues(**params) if marker in (issue.get("body") or "")), None)

    async def find_comment(self, number: int, marker: str) -> dict | None:
        return next(
            (comment for comment in await self.issue_comments(number) if marker in (comment.get("body") or "")), None
        )

    async def edit_comment(self, comment_id: int, body: str) -> dict:
        return (
            await self.request("PATCH", f"/repos/{self.repo}/issues/comments/{comment_id}", json={"body": body})
        ).json()

    async def issue_comments(self, number: int) -> list[dict]:
        return [
            comment async for comment in self._paged(f"/repos/{self.repo}/issues/{number}/comments", conditional=True)
        ]

    # --- pull requests ---------------------------------------------------------------------

    async def find_pr(self, branch: str) -> dict | None:
        """The open PR whose head is `branch` in this repo, if any."""
        owner = self.repo.split("/")[0]
        prs = (
            await self.request(
                "GET", f"/repos/{self.repo}/pulls", params={"head": f"{owner}:{branch}", "state": "open"}
            )
        ).json()
        return prs[0] if prs else None

    async def create_pr(self, title: str, body: str, head: str, base: str) -> dict:
        return (
            await self.request(
                "POST", f"/repos/{self.repo}/pulls", json={"title": title, "body": body, "head": head, "base": base}
            )
        ).json()

    async def get_pr(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/pulls/{number}", conditional=True)).json()

    async def pr_reviews(self, number: int) -> list[dict]:
        return [review async for review in self._paged(f"/repos/{self.repo}/pulls/{number}/reviews", conditional=True)]

    async def pr_review_comments(self, number: int) -> list[dict]:
        """The inline (per-line) comments of the PR's reviews."""
        return [
            comment async for comment in self._paged(f"/repos/{self.repo}/pulls/{number}/comments", conditional=True)
        ]

    async def check_state(self, sha: str, required: list[str] = ()) -> str:
        """Overall CI state of a commit (success | pending | failure | none), combining check runs and statuses."""
        runs = (
            await self.request(
                "GET", f"/repos/{self.repo}/commits/{sha}/check-runs", params={"per_page": 100}, conditional=True
            )
        ).json()["check_runs"]
        statuses = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/status", conditional=True)).json()[
            "statuses"
        ]
        return combine_checks(runs, statuses, required)

    async def merge_pr(self, number: int, sha: str) -> tuple[bool, str]:
        """Squash-merge exactly `sha`. GitHub refusing (protection, conflict, moved head) is a result, not an error."""
        try:
            await self.request(
                "PUT", f"/repos/{self.repo}/pulls/{number}/merge", json={"sha": sha, "merge_method": "squash"}
            )
            return True, "merged"
        except GitHubError as error:
            if error.status in (405, 409, 422):
                return False, str(error)
            raise

    async def delete_branch(self, branch: str) -> None:
        try:
            await self.request("DELETE", f"/repos/{self.repo}/git/refs/heads/{branch}", ok404=True)
        except GitHubError as error:
            if error.status != 422:  # 422: already gone
                raise

    async def list_prs(self, state: str = "all", per_page: int = 50) -> list[dict]:
        """The most recently updated PRs, newest first (one page only)."""
        return (
            await self.request(
                "GET",
                f"/repos/{self.repo}/pulls",
                params={"state": state, "sort": "updated", "direction": "desc", "per_page": per_page},
            )
        ).json()

    async def pr_files(self, number: int) -> list[str]:
        """Paths changed by the PR (first 100)."""
        response = await self.request("GET", f"/repos/{self.repo}/pulls/{number}/files", params={"per_page": 100})
        return [entry["filename"] for entry in response.json()]

    # --- statuses, refs, contents ------------------------------------------------------------

    async def set_status(self, sha: str, context: str, state: str, description: str, target_url: str | None) -> None:
        """Post a commit status (the classic kind, not a check run)."""
        await self.request(
            "POST",
            f"/repos/{self.repo}/statuses/{sha}",
            json={"state": state, "context": context, "description": description[:140], "target_url": target_url},
        )

    async def statuses(self, sha: str) -> list[dict]:
        return (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/status")).json()["statuses"]

    async def get_ref_sha(self, branch: str) -> str:
        return (await self.request("GET", f"/repos/{self.repo}/git/ref/heads/{branch}")).json()["object"]["sha"]

    async def dir_sha(self, path: str, ref: str) -> str | None:
        """Tree SHA of directory `path` at `ref`, from its parent's listing. None if absent."""
        parent, _, name = path.rstrip("/").rpartition("/")
        response = await self.request("GET", f"/repos/{self.repo}/contents/{parent}", params={"ref": ref}, ok404=True)
        if not response:
            return None
        return next(
            (entry["sha"] for entry in response.json() if entry["name"] == name and entry["type"] == "dir"), None
        )

    async def get_file(self, path: str, ref: str) -> bytes | None:
        """The raw bytes of a file at `ref`, or None if it does not exist."""
        response = await self.request(
            "GET",
            f"/repos/{self.repo}/contents/{path}",
            params={"ref": ref},
            ok404=True,
            headers={"Accept": "application/vnd.github.raw"},
        )
        return response.content if response else None

    async def ensure_branch(self, branch: str) -> bool:
        """Create `branch` from an orphan commit if missing. Returns True if created."""
        if await self.request("GET", f"/repos/{self.repo}/git/ref/heads/{branch}", ok404=True):
            return False
        tree = (
            await self.request(
                "POST",
                f"/repos/{self.repo}/git/trees",
                json={
                    "tree": [
                        {
                            "path": "README.md",
                            "mode": "100644",
                            "type": "blob",
                            "content": "mia-data galleries, written by the worker.\n",
                        }
                    ]
                },
            )
        ).json()["sha"]
        commit = (
            await self.request(
                "POST",
                f"/repos/{self.repo}/git/commits",
                json={"message": "mia-data: initialize galleries branch", "tree": tree, "parents": []},
            )
        ).json()["sha"]
        await self.request("POST", f"/repos/{self.repo}/git/refs", json={"ref": f"refs/heads/{branch}", "sha": commit})
        return True

    async def file_exists(self, path: str, ref: str) -> bool:
        return (
            await self.request("GET", f"/repos/{self.repo}/contents/{path}", params={"ref": ref}, ok404=True)
            is not None
        )

    async def upsert_file(self, path: str, content: bytes, branch: str, message: str) -> bool:
        """Commit `content` at `path` unless the file already exists there. Returns True if committed."""
        async with self._commit_lock:
            if await self.file_exists(path, branch):
                return False
            await self.request(
                "PUT",
                f"/repos/{self.repo}/contents/{path}",
                json={"message": message, "branch": branch, "content": base64.b64encode(content).decode()},
            )
            return True
