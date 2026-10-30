"""Shared helpers for mia_data tests."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mia_data.config import Config


def make_cfg(tmp_path: Path, **overrides) -> Config:
    harness = tmp_path / "harness"
    harness.mkdir(exist_ok=True)
    data = {
        "github": {"data_repo": "o/data", "snippets_repo": "o/snippets", "catalog_repo": "o/catalog"},
        "paths": {
            "state_dir": str(tmp_path / "state"),
            "work_root": str(tmp_path / "work"),
            "storage_root": str(tmp_path / "storage"),
            "local_root": str(tmp_path / "local"),
        },
        "harness": {"dir": str(harness)},
    }
    for k, v in overrides.items():
        data[k] = {**data[k], **v} if isinstance(v, dict) and isinstance(data.get(k), dict) else v
    return Config(**data)


class FakeGH:
    """The GitHub calls the activities make, in memory."""

    def __init__(self, repo, files=None, issues=None, refs=None, prs=None):
        self.repo, self.files, self.issues, self.refs = repo, files or {}, issues or {}, refs or {}
        self.prs = prs or {}  # PR number -> heads, one per get_pr call (the last one repeats)
        self.comments, self.created, self.gets, self.puts, self.removed, self.posted = {}, [], [], [], [], []

    def tokens(self):
        return "tok"

    async def get_file(self, path, ref):
        self.gets.append((path, ref))
        return self.files.get((path, ref))

    async def request(self, method, path, params=None):
        tree = [{"type": "blob", "path": p} for p, _ in self.files]
        return SimpleNamespace(json=lambda: {"tree": tree})

    async def get_ref_sha(self, branch):
        return self.refs[branch]

    async def get_issue(self, n):
        return self.issues[n]

    async def list_issues(self, **params):
        return list(self.issues.values())

    async def find_issue_by_marker(self, marker, labels=None):
        return next((i for i in self.issues.values() if marker in (i.get("body") or "")), None)

    async def add_labels(self, n, labels):
        self.issues[n]["labels"] += [{"name": x} for x in labels]

    async def edit_comment(self, cid, body):
        next(c for cs in self.comments.values() for c in cs if c["id"] == cid)["body"] = body

    async def create_issue(self, title, body, labels):
        n = 100 + len(self.issues)
        self.created.append((title, body, labels))
        self.issues[n] = {"number": n, "body": body, "labels": [{"name": x} for x in labels], "user": {"login": "bot"}}
        return self.issues[n]

    async def issue_comments(self, n):
        return self.comments.get(n, [])

    async def permission(self, login):
        return getattr(self, "permissions", {}).get(login, "none")

    async def comment(self, n, body, login="bot"):
        cs = self.comments.setdefault(n, [])
        cs.append(
            c := {
                "id": 1000 * n + len(cs),
                "body": body,
                "user": {"login": login},
                "html_url": f"https://github.com/{self.repo}/issues/{n}#c{len(cs)}",
            }
        )
        return c

    async def get_pr(self, n):
        heads = self.prs[n]
        return {"number": n, "head": {"sha": heads.pop(0) if len(heads) > 1 else heads[0]}}

    async def set_status(self, sha, context, state, description, target_url):
        self.posted.append((sha, context, state, description, target_url))

    async def remove_label(self, n, name):
        self.removed.append((n, name))
        self.issues[n]["labels"] = [x for x in self.issues[n]["labels"] if x["name"] != name]

    async def ensure_branch(self, branch):
        pass

    async def upsert_file(self, path, content, branch, message):
        if path in self.files:
            return False
        self.files[path] = content
        self.puts.append(path)
        return True
