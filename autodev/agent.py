"""One agent round = one csub job running Claude Code in a sandbox, in a workspace that holds no credentials."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from pathlib import Path
from typing import Callable

from . import jobs
from .jobs import read_untrusted
from .models import JobOutcome, JobRef, Workspace, check_key

PROMPT = """You are implementing GitHub issue #{number} in {repo}. The repository is checked out in the current
directory on branch `{branch}` at commit {base_sha}.

# Issue: {title}

{body}
{discussion}{revision}
# How to work

- Read CLAUDE.md and README.md first, if present, and follow the repository's conventions.
- Make the change with tests. Run the test suite (`pixi run test` if the repo uses pixi) and make it pass.
- Commit your work with `git commit` (clear messages; several commits are fine). Do not push: you have no
  GitHub access, and your commits are collected when you exit.
- Do not modify anything under: {protected}.
- Finish with a short summary of what you changed and how you tested it. It is posted on the pull request.
- If the issue is unclear or cannot be done, make no commits and explain why in your summary.
"""


def render_prompt(cfg, issue: dict, comments: list[dict], branch: str, base_sha: str, pr: int | None,
                  feedback: str | None) -> str:
    human = [c for c in comments if cfg.marker_prefix not in (c.get("body") or "")]
    discussion = ("\n# Discussion on the issue\n\n" + "\n\n".join(
        f"**{(c.get('user') or {}).get('login')}**: {c.get('body')}" for c in human) + "\n") if human else ""
    revision = (f"\n# Requested changes\n\nPull request #{pr} has your earlier work on this branch. "
                f"A reviewer asked for:\n\n{feedback}\n") if feedback else ""
    return PROMPT.format(number=issue["number"], repo=cfg.github.repo, branch=branch, base_sha=base_sha,
                         title=issue.get("title") or "", body=issue.get("body") or "(no description)",
                         discussion=discussion, revision=revision,
                         protected=", ".join(cfg.github.protected_paths) or "(nothing)")


def job_script(cfg, base_sha: str) -> str:
    """Runs inside the csub sandbox with cwd = the round directory."""
    q = shlex.quote
    path = f'export PATH={":".join(q(p) for p in cfg.agent.path_prepend)}:"$PATH"\n' if cfg.agent.path_prepend else ""
    return f"""set -u
{path}cd repo || exit 1
claude -p "$(cat ../prompt.md)" --model {q(cfg.agent.model)} --output-format json \\
  --allowedTools "Bash,Read,Edit,Write,Glob,Grep" > ../result.json 2> ../claude.err
rc=$?
git bundle create -q ../result.bundle {q(base_sha)}..HEAD 2>> ../claude.err
echo "$rc" > ../exit_code
exit "$rc"
"""


def read_result(d: Path, limit: int = 20000) -> tuple[str, float | None]:
    """The agent's summary and cost from result.json. Untrusted: any shape may be there."""
    try:
        data = json.loads(read_untrusted(d, "result.json", 1 << 20) or "")
        text, cost = str(data.get("result") or ""), data.get("total_cost_usd")
    except (ValueError, AttributeError, RecursionError):
        return "(the agent left no readable result.json)", None
    if len(text) > limit:
        text = text[:limit] + "\n\n…(truncated)"
    return text or "(empty summary)", cost if isinstance(cost, (int, float)) else None


def log_tail(d: Path, n: int = 40) -> str:
    d = Path(d)
    parts = []
    for rel in ["claude.err", *(str(p.relative_to(d)) for p in sorted((d / ".csub" / "jobs").glob("*/stderr"))[-3:])]:  # newest few: bounds memory
        if text := (read_untrusted(d, rel, 1 << 16, tail=True) or "").strip():
            parts.append(f"== {rel} ==\n" + "\n".join(text.splitlines()[-n:]))
    return "\n".join(parts)[-5000:]


def csub_client(cfg):
    return jobs.csub_client(cfg.deployment)


class AgentJobs:
    def __init__(self, cfg, client_factory=None, sleep=asyncio.sleep, poll_s: float = 60.0):
        self.cfg = cfg
        self.jobs = jobs.CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs",
                                  client_factory or (lambda: csub_client(cfg)), sleep, poll_s)

    async def submit(self, issue: int, key: str, rnd: int, ws: Workspace) -> JobRef:
        """Submit once per (issue, key, round); a retry returns the same job."""
        a = self.cfg.agent
        job_key = f"issue-{issue}-{check_key(key)}-r{rnd}"
        spec = lambda: jobs.JobSpec(  # noqa: E731  built lazily: env is read only when no id file exists
            name=f"autodev-{issue}-r{rnd}",  # csub rejects names containing the username (autodev-<you>); session = deployment
            command=job_script(self.cfg, ws.base_sha), workspace=ws.dir, walltime_min=a.walltime_min, cpus=a.cpus,
            mem_mb=a.mem_mb, allow_hosts=list(a.allow_hosts), claude=True,
            ro_mounts=[m.removesuffix(":ro") for m in a.extra_mounts],  # Config guarantees ":ro"
            env={"ANTHROPIC_API_KEY": os.environ[a.api_key_env]} if a.api_key_env else {})
        return JobRef(await self.jobs.submit(job_key, spec), a.walltime_min)

    async def wait(self, job_id: str, ws: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome:
        return await self.jobs.wait(job_id, ws.dir, heartbeat)
