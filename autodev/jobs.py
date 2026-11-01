"""Running shell scripts as csub (LSF sandbox) jobs, and reading what they leave behind.

`CsubJobs` submits a job at most once per key, remembering the job id in a file so a worker restart or a
Temporal retry reattaches to the running job instead of starting a second one. `read_untrusted` is how
every file written by a sandboxed job is read back.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mia_core.untrusted import read_untrusted as read_untrusted

from .models import JobOutcome

if TYPE_CHECKING:
    import csub

JOB_KEY_RE = re.compile(r"[A-Za-z0-9._-]{1,120}")


@dataclass
class JobSpec:
    """Everything csub needs to run one job."""

    name: str  # csub job name (no username in it)
    command: str  # shell script; runs with cwd = workspace
    workspace: str  # rw mount + cwd; unique per job (the cwd-lookup key)
    walltime_min: int
    cpus: int = 1
    mem_mb: int = 4000
    allow_hosts: list[str] = field(default_factory=list)
    ro_mounts: list[str] = field(default_factory=list)  # absolute paths
    rw_mounts: list[str] = field(default_factory=list)  # absolute paths, besides the workspace
    agent: str | None = None  # csub launcher agent mode, e.g. "claude"; gated by the policy [agents] table
    env: dict[str, str] = field(default_factory=dict)


def csub_client(session: str) -> csub.Client:
    """A csub client bound to `session`, which groups a deployment's jobs on the broker."""
    import csub
    from csub.client import load_client_config

    return csub.Client(load_client_config().with_(session=session))


class CsubJobs:
    """Submit-once job runner. Job ids are remembered as `<key>.id` files in `id_dir`."""

    def __init__(
        self,
        session: str,
        id_dir: Path,
        client_factory: Callable[[], Any] | None = None,
        sleep: Callable[[float], Any] = asyncio.sleep,
        poll_s: float = 60.0,
    ):
        self.session, self.id_dir = session, Path(id_dir)
        self._factory = client_factory or (lambda: csub_client(session))
        self._client = None
        self._sleep, self._poll_s = sleep, poll_s

    @property
    def client(self) -> Any:
        """The csub client, created on first use."""
        if self._client is None:
            self._client = self._factory()
        return self._client

    async def submit(self, key: str, spec: JobSpec | Callable[[], JobSpec]) -> str:
        """Submit at most once per key. Returns the job id.

        The id file is checked first; failing that, the broker is asked for a job already running in the
        spec's workspace (submitted before a crash, but never recorded). A callable `spec` is only built
        when the id file is missing, so an expensive or environment-dependent spec is not built on reattach."""
        if not JOB_KEY_RE.fullmatch(key):
            raise ValueError(f"invalid job key {key!r}: want {JOB_KEY_RE.pattern}")
        id_file = self.id_dir / f"{key}.id"
        if id_file.exists():
            return id_file.read_text().strip()
        spec = spec() if callable(spec) else spec
        workspace = os.path.realpath(spec.workspace)
        running = [
            status
            for status in await asyncio.to_thread(self.client.status)
            if os.path.realpath(getattr(status, "cwd", None) or "/nonexistent") == workspace
        ]
        if running:  # same workspace (unique per job): submitted, then the worker died before writing the id file
            job_id = running[0].job_id
        else:
            request = {
                "command": spec.command,
                "shell": True,
                "cwd": spec.workspace,
                "name": spec.name,
                "cpus": spec.cpus,
                "mem_mb": spec.mem_mb,
                "walltime_min": spec.walltime_min,
                "allow_hosts": list(spec.allow_hosts),
                "agent": spec.agent,
                "session": self.session,
                "mounts": [
                    {"path": spec.workspace, "mode": "rw"},
                    *({"path": mount, "mode": "rw"} for mount in spec.rw_mounts),
                    *({"path": mount, "mode": "ro"} for mount in spec.ro_mounts),
                ],
            }
            if spec.env:
                request["env"] = dict(spec.env)
            job_id = (await asyncio.to_thread(self.client.submit, request)).job_id
        id_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = id_file.with_suffix(".tmp")
        tmp.write_text(job_id)
        tmp.replace(id_file)
        return job_id

    async def wait(self, job_id: str, workspace: str, heartbeat: Callable[[str], None]) -> JobOutcome:
        """Poll until the job is finished (or forgotten by LSF), calling `heartbeat` with each state seen.

        The script writes its exit code to `exit_code` in the workspace; that file settles the outcome when
        LSF has already forgotten the job."""
        # ponytail: one csub status call per job per poll; batch across jobs if many run at once.
        while True:
            status = next(iter(await asyncio.to_thread(self.client.status, [job_id])), None)
            state = status.state if status else "UNKNOWN"
            heartbeat(state)
            if status is None or status.terminal:
                break
            await self._sleep(self._poll_s)
        raw = (read_untrusted(Path(workspace), "exit_code", 64) or "").strip()
        try:
            file_code = int(raw) if raw.isascii() else None
        except ValueError:
            file_code = None
        if state == "UNKNOWN" and file_code is not None:  # LSF forgot the job, but the script ran to the end
            state = "DONE" if file_code == 0 else "EXIT"
        exit_code = status.exit_code if status and status.exit_code is not None else file_code
        return JobOutcome(job_id, state, exit_code)
