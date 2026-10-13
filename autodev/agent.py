"""One agent round: the prompt Claude Code gets, the script that runs it in the sandbox, and how its
output is read back.

The workspace the job sees holds no credentials. The agent commits locally; the script bundles the
commits and the worker pushes them (see gitops.py).
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import jobs
from .jobs import read_untrusted
from .models import JobOutcome, JobRef, Workspace, check_key

if TYPE_CHECKING:
    from .config import Config

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


def render_prompt(cfg: Config, issue: dict, comments: list[dict], branch: str, base_sha: str, pr: int | None,
                  feedback: str | None) -> str:
    """Fill the prompt template with the issue, the human discussion on it, and any reviewer feedback.

    `issue` and `comments` are GitHub API dicts; the bot's own comments (carrying our marker) are left out."""
    human = [comment for comment in comments if cfg.marker_prefix not in (comment.get("body") or "")]
    discussion = ("\n# Discussion on the issue\n\n" + "\n\n".join(
        f"**{(comment.get('user') or {}).get('login')}**: {comment.get('body')}" for comment in human) + "\n"
        if human else "")
    revision = (f"\n# Requested changes\n\nPull request #{pr} has your earlier work on this branch. "
                f"A reviewer asked for:\n\n{feedback}\n") if feedback else ""
    return PROMPT.format(number=issue["number"], repo=cfg.github.repo, branch=branch, base_sha=base_sha,
                         title=issue.get("title") or "", body=issue.get("body") or "(no description)",
                         discussion=discussion, revision=revision,
                         protected=", ".join(cfg.github.protected_paths) or "(nothing)")


def job_script(cfg: Config, base_sha: str) -> str:
    """The shell script the csub job runs, with cwd = the round directory.

    It runs Claude Code on the prompt, bundles whatever was committed since `base_sha`, and records the
    exit code for the worker to read."""
    quote = shlex.quote
    path = (f'export PATH={":".join(quote(entry) for entry in cfg.agent.path_prepend)}:"$PATH"\n'
            if cfg.agent.path_prepend else "")
    budget = f" --max-budget-usd {cfg.agent.max_budget_usd}" if cfg.agent.max_budget_usd is not None else ""
    return f"""set -u
{path}cd repo || exit 1
claude -p "$(cat ../prompt.md)" --model {quote(cfg.agent.model)} --output-format json{budget} \\
  --allowedTools "Bash,Read,Edit,Write,Glob,Grep" > ../result.json 2> ../claude.err
rc=$?
git bundle create -q ../result.bundle {quote(base_sha)}..HEAD 2>> ../claude.err
echo "$rc" > ../exit_code
exit "$rc"
"""


def read_result(round_dir: Path, limit: int = 20000) -> tuple[str, float | None]:
    """The agent's summary and cost from result.json. Untrusted: any shape may be there."""
    try:
        data = json.loads(read_untrusted(round_dir, "result.json", 1 << 20) or "")
        text, cost = str(data.get("result") or ""), data.get("total_cost_usd")
    except (ValueError, AttributeError, RecursionError):
        return "(the agent left no readable result.json)", None
    if len(text) > limit:
        text = text[:limit] + "\n\n…(truncated)"
    return text or "(empty summary)", cost if isinstance(cost, (int, float)) else None


def log_tail(round_dir: Path, lines: int = 40) -> str:
    """The last `lines` of claude.err and of the newest few csub job stderr files, for failure reports."""
    round_dir = Path(round_dir)
    parts = []
    job_logs = sorted((round_dir / ".csub" / "jobs").glob("*/stderr"))[-3:]  # newest few: bounds memory
    for rel in ["claude.err", *(str(path.relative_to(round_dir)) for path in job_logs)]:
        if text := (read_untrusted(round_dir, rel, 1 << 16, tail=True) or "").strip():
            parts.append(f"== {rel} ==\n" + "\n".join(text.splitlines()[-lines:]))
    return "\n".join(parts)[-5000:]


def csub_client(cfg: Config) -> Any:
    return jobs.csub_client(cfg.deployment)


class AgentJobs:
    """Submits and waits on agent jobs for one deployment, keyed by (issue, key, round)."""

    def __init__(self, cfg: Config, client_factory: Callable[[], Any] | None = None,
                 sleep: Callable[[float], Any] = asyncio.sleep, poll_s: float = 60.0):
        self.cfg = cfg
        self.jobs = jobs.CsubJobs(cfg.deployment, cfg.paths.state_dir / "jobs",
                                  client_factory or (lambda: csub_client(cfg)), sleep, poll_s)

    async def submit(self, issue: int, key: str, round_number: int, workspace: Workspace) -> JobRef:
        """Submit once per (issue, key, round); a retry returns the same job."""
        agent_cfg = self.cfg.agent
        job_key = f"issue-{issue}-{check_key(key)}-r{round_number}"

        def build_spec() -> jobs.JobSpec:  # built lazily: the API key env var is read only when no id file exists
            return jobs.JobSpec(
                name=f"autodev-{issue}-r{round_number}",  # csub rejects names containing the username; session = deployment
                command=job_script(self.cfg, workspace.base_sha), workspace=workspace.dir,
                walltime_min=agent_cfg.walltime_min, cpus=agent_cfg.cpus, mem_mb=agent_cfg.mem_mb,
                allow_hosts=list(agent_cfg.allow_hosts), agent="claude",
                ro_mounts=[mount.removesuffix(":ro") for mount in agent_cfg.extra_mounts],  # Config guarantees ":ro"
                env={"ANTHROPIC_API_KEY": os.environ[agent_cfg.api_key_env]} if agent_cfg.api_key_env else {})

        return JobRef(await self.jobs.submit(job_key, build_spec), agent_cfg.walltime_min)

    async def wait(self, job_id: str, workspace: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome:
        return await self.jobs.wait(job_id, workspace.dir, heartbeat)
