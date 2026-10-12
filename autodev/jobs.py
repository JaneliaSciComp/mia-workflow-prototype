"""Run one shell script as a csub job, submitted at most once per key, and wait for it."""

from __future__ import annotations

import asyncio
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .models import JobOutcome

JOB_KEY_RE = re.compile(r"[A-Za-z0-9._-]{1,120}")


@dataclass
class JobSpec:
    name: str                      # csub job name (no username in it)
    command: str                   # shell script; runs with cwd = workspace
    workspace: str                 # rw mount + cwd; unique per job (the cwd-lookup key)
    walltime_min: int
    cpus: int = 1
    mem_mb: int = 4000
    allow_hosts: list[str] = field(default_factory=list)
    ro_mounts: list[str] = field(default_factory=list)   # absolute paths
    rw_mounts: list[str] = field(default_factory=list)   # absolute paths, besides the workspace
    agent: str | None = None  # csub launcher agent mode, e.g. "claude"; gated by the policy [agents] table
    env: dict[str, str] = field(default_factory=dict)


def read_untrusted(d: Path, rel: str, limit: int, tail: bool = False, binary: bool = False) -> str | bytes | None:
    """Read a file a sandboxed job could have tampered with: no symlinks, no FIFOs, bounded size. Text (str) unless
    `binary`."""
    try:
        root = Path(d).resolve()
        p = root / rel
        if not p.resolve().is_relative_to(root):
            return None
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                return None
            # the opened file itself (not the path we checked earlier) must live inside the job dir
            if not Path(os.path.realpath(f"/proc/self/fd/{fd}")).is_relative_to(root):
                return None
            if tail:
                os.lseek(fd, max(0, st.st_size - limit), os.SEEK_SET)
            data = os.read(fd, limit)
            return data if binary else data.decode(errors="replace")
        finally:
            os.close(fd)
    except (OSError, RuntimeError):  # RuntimeError: symlink loop in resolve()
        return None


def csub_client(session: str):
    import csub
    from csub.client import load_client_config

    return csub.Client(load_client_config().with_(session=session))


class CsubJobs:
    def __init__(self, session: str, id_dir: Path, client_factory=None, sleep=asyncio.sleep, poll_s: float = 60.0):
        self.session, self.id_dir = session, Path(id_dir)
        self._factory = client_factory or (lambda: csub_client(session))
        self._client = None
        self._sleep, self._poll_s = sleep, poll_s

    @property
    def client(self):
        if self._client is None:
            self._client = self._factory()
        return self._client

    async def submit(self, key: str, spec: JobSpec | Callable[[], JobSpec]) -> str:
        """Submit at most once per key: id file -> cwd lookup -> submit. Returns the job id.

        A callable spec is built only when the id file is missing (it holds the workspace the cwd lookup needs)."""
        if not JOB_KEY_RE.fullmatch(key):
            raise ValueError(f"invalid job key {key!r}: want {JOB_KEY_RE.pattern}")
        f = self.id_dir / f"{key}.id"
        if f.exists():
            return f.read_text().strip()
        spec = spec() if callable(spec) else spec
        found = [s for s in await asyncio.to_thread(self.client.status) if os.path.realpath(getattr(s, "cwd", None) or "/nonexistent") == os.path.realpath(spec.workspace)]
        if found:  # same workspace (unique per job): submitted, then the worker died before writing the id file
            job_id = found[0].job_id
        else:
            req = {"command": spec.command, "shell": True, "cwd": spec.workspace, "name": spec.name,
                   "cpus": spec.cpus, "mem_mb": spec.mem_mb, "walltime_min": spec.walltime_min,
                   "allow_hosts": list(spec.allow_hosts), "agent": spec.agent, "session": self.session,
                   "mounts": [{"path": spec.workspace, "mode": "rw"}, *({"path": m, "mode": "rw"} for m in spec.rw_mounts),
                              *({"path": m, "mode": "ro"} for m in spec.ro_mounts)]}
            if spec.env:
                req["env"] = dict(spec.env)
            job_id = (await asyncio.to_thread(self.client.submit, req)).job_id
        f.parent.mkdir(parents=True, exist_ok=True)
        tmp = f.with_suffix(".tmp")
        tmp.write_text(job_id)
        tmp.replace(f)
        return job_id

    async def wait(self, job_id: str, workspace: str, heartbeat: Callable[[str], None]) -> JobOutcome:
        # ponytail: one csub status call per job per poll; batch across jobs if many run at once.
        while True:
            st = next(iter(await asyncio.to_thread(self.client.status, [job_id])), None)
            state = st.state if st else "UNKNOWN"
            heartbeat(state)
            if st is None or st.terminal:
                break
            await self._sleep(self._poll_s)
        raw = (read_untrusted(Path(workspace), "exit_code", 64) or "").strip()
        try:
            code = int(raw) if raw.isascii() else None
        except ValueError:
            code = None
        if state == "UNKNOWN" and code is not None:  # LSF forgot the job, but the script ran to the end
            state = "DONE" if code == 0 else "EXIT"
        exit_code = st.exit_code if st and st.exit_code is not None else code
        return JobOutcome(job_id, state, exit_code)
