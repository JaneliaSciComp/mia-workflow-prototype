"""One agent round = one csub job running Claude Code in a sandbox, in a workspace that holds no credentials."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from pathlib import Path
from typing import Callable

from .models import JobOutcome, JobRef, Workspace

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
{path}cd repo
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
        data = json.loads((Path(d) / "result.json").read_text())
        text, cost = str(data.get("result") or ""), data.get("total_cost_usd")
    except (OSError, ValueError, AttributeError):
        return "(the agent left no readable result.json)", None
    if len(text) > limit:
        text = text[:limit] + "\n\n…(truncated)"
    return text or "(empty summary)", cost if isinstance(cost, (int, float)) else None


def log_tail(d: Path, n: int = 40) -> str:
    d = Path(d)
    parts = []
    for p in [d / "claude.err", *sorted((d / ".csub" / "jobs").glob("*/stderr"))]:
        if p.is_file() and (text := p.read_text(errors="replace").strip()):
            parts.append(f"== {p.relative_to(d)} ==\n" + "\n".join(text.splitlines()[-n:]))
    return "\n".join(parts)[-5000:]


def csub_client(cfg):
    import csub

    return csub.Client(csub.load_client_config().with_(session=cfg.deployment))


def _mount(spec: str) -> dict[str, str]:
    path, _, mode = spec.rpartition(":")
    return {"path": path, "mode": mode} if mode in ("ro", "rw") else {"path": spec, "mode": "ro"}


class AgentJobs:
    def __init__(self, cfg, client_factory=None, sleep=asyncio.sleep, poll_s: float = 60.0):
        self.cfg = cfg
        self._factory = client_factory or (lambda: csub_client(cfg))
        self._client = None
        self._sleep, self._poll_s = sleep, poll_s

    @property
    def client(self):
        if self._client is None:
            self._client = self._factory()
        return self._client

    def _id_file(self, issue: int, key: str, rnd: int) -> Path:
        return self.cfg.paths.state_dir / "jobs" / f"issue-{issue}-{key}-r{rnd}.id"

    async def submit(self, issue: int, key: str, rnd: int, ws: Workspace) -> JobRef:
        """Submit once per (issue, key, round); a retry returns the same job."""
        a = self.cfg.agent
        f = self._id_file(issue, key, rnd)
        if f.exists():
            return JobRef(f.read_text().strip(), a.walltime_min)
        name = f"{self.cfg.deployment}-{issue}-r{rnd}"
        found = [s for s in await asyncio.to_thread(self.client.status) if s.name == name]
        if found:  # submitted, then the worker died before writing the id file
            job_id = found[0].job_id
        else:
            spec = {"command": job_script(self.cfg, ws.base_sha), "shell": True, "cwd": ws.dir, "name": name,
                    "cpus": a.cpus, "mem_mb": a.mem_mb, "walltime_min": a.walltime_min,
                    "allow_hosts": list(a.allow_hosts), "claude": True, "session": self.cfg.deployment,
                    "mounts": [{"path": ws.dir, "mode": "rw"}, *(_mount(m) for m in a.extra_mounts)]}
            if a.api_key_env:
                spec["env"] = {"ANTHROPIC_API_KEY": os.environ[a.api_key_env]}
            job_id = (await asyncio.to_thread(self.client.submit, spec)).job_id
        f.parent.mkdir(parents=True, exist_ok=True)
        tmp = f.with_suffix(".tmp")
        tmp.write_text(job_id)
        tmp.replace(f)
        return JobRef(job_id, a.walltime_min)

    async def wait(self, job_id: str, ws: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome:
        # ponytail: one csub status call per job per poll; batch across jobs if many run at once.
        while True:
            st = next(iter(await asyncio.to_thread(self.client.status, [job_id])), None)
            state = st.state if st else "UNKNOWN"
            heartbeat(state)
            if st is None or st.terminal:
                break
            await self._sleep(self._poll_s)
        code_file = Path(ws.dir) / "exit_code"
        code = int(code_file.read_text().strip()) if code_file.is_file() and code_file.read_text().strip().isdigit() else None
        if state == "UNKNOWN" and code is not None:  # LSF forgot the job, but the script ran to the end
            state = "DONE" if code == 0 else "EXIT"
        exit_code = st.exit_code if st and st.exit_code is not None else code
        return JobOutcome(job_id, state, exit_code)
