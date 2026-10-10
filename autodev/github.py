"""The GitHub calls autodev adds to zarr-doctor's client (which brings rate limits, throttling, comment_once)."""

from __future__ import annotations

from zarr_doctor.github_api import GitHub as _Base
from zarr_doctor.github_api import GitHubError

from .auth import token_fn
from .review import combine_checks

__all__ = ["GitHub", "GitHubError"]


class GitHub(_Base):
    @classmethod
    def for_autodev(cls, cfg, tokens=None) -> "GitHub":
        return cls(cfg.github.repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)

    @property
    def tokens(self):
        return self._token_fn

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
        return [c async for c in self._paged(f"/repos/{self.repo}/issues/{number}/comments", conditional=True)]

    async def find_pr(self, branch: str) -> dict | None:
        owner = self.repo.split("/")[0]
        prs = (await self.request("GET", f"/repos/{self.repo}/pulls",
                                  params={"head": f"{owner}:{branch}", "state": "open"})).json()
        return prs[0] if prs else None

    async def create_pr(self, title: str, body: str, head: str, base: str) -> dict:
        return (await self.request("POST", f"/repos/{self.repo}/pulls",
                                   json={"title": title, "body": body, "head": head, "base": base})).json()

    async def get_pr(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/pulls/{number}",
                                   conditional=True)).json()

    async def pr_reviews(self, number: int) -> list[dict]:
        return [r async for r in self._paged(f"/repos/{self.repo}/pulls/{number}/reviews", conditional=True)]

    async def pr_review_comments(self, number: int) -> list[dict]:
        return [c async for c in self._paged(f"/repos/{self.repo}/pulls/{number}/comments", conditional=True)]

    async def check_state(self, sha: str) -> str:
        runs = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/check-runs",
                                   params={"per_page": 100}, conditional=True)).json()["check_runs"]
        statuses = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/status", conditional=True)).json()["statuses"]
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
