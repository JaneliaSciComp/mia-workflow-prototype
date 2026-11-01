"""The helpers for output a sandboxed job wrote: bounded reads, neutralizing, defensive parsing, atomic writes."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Literal, overload


@overload
def read_untrusted(
    job_dir: Path, rel: str, limit: int, tail: bool = ..., binary: Literal[False] = ...
) -> str | None: ...
@overload
def read_untrusted(job_dir: Path, rel: str, limit: int, tail: bool = ..., *, binary: Literal[True]) -> bytes | None: ...
def read_untrusted(job_dir: Path, rel: str, limit: int, tail: bool = False, binary: bool = False) -> str | bytes | None:
    """Read a file a sandboxed job could have tampered with. Returns None for anything suspicious.

    No symlinks, no FIFOs, nothing outside `job_dir`, and at most `limit` bytes (the last `limit` with
    `tail`). Returns text unless `binary`."""
    try:
        root = Path(job_dir).resolve()
        path = root / rel
        if not path.resolve().is_relative_to(root):
            return None
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                return None
            # the opened file itself (not the path we checked earlier) must live inside the job dir
            if not Path(os.path.realpath(f"/proc/self/fd/{fd}")).is_relative_to(root):
                return None
            if tail:
                os.lseek(fd, max(0, info.st_size - limit), os.SEEK_SET)
            data = os.read(fd, limit)
            return data if binary else data.decode(errors="replace")
        finally:
            os.close(fd)
    except (OSError, RuntimeError):  # RuntimeError: symlink loop in resolve()
        return None


# ponytail: known secret shapes plus our own token (see neutralize); not a general secret scanner.
SECRET_RE = re.compile(r"sk-ant-[\w-]+|gh[pousr]_\w+|github_pat_\w+|eyJ[\w-]+\.[\w-]+\.[\w-]+")


def neutralize(text: str, token: str | None = None) -> str:
    """Make agent-written text safe to post on GitHub.

    It must not be able to forge our `<!-- autodev: ... -->` markers, nor leak a credential that a
    prompt-injected agent printed (e.g. from ~/.claude/.credentials.json)."""
    if token:
        text = text.replace(token, "[redacted]")
    return SECRET_RE.sub("[redacted]", text).replace("<!--", "&lt;!--")


def write_atomic(path: Path, data: str | bytes) -> None:
    """Write via a temp file and rename, so a crash never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(data.encode() if isinstance(data, str) else data)
    os.replace(tmp, path)


def text(value: Any, limit: int = 2000) -> str:
    """A job-reported value as a bounded string ("" for None)."""
    return "" if value is None else str(value)[:limit]


def texts(value: Any, limit: int = 20) -> list[str]:
    """A job-reported value as a bounded list of bounded strings (a scalar becomes a one-item list)."""
    items = value if isinstance(value, list) else [] if value is None else [value]
    return [text(item, 500) for item in items[:limit]]


def loads(text: str | bytes | None) -> Any:
    """json.loads that returns None instead of raising."""
    try:
        return json.loads(text or "null")
    except (ValueError, RecursionError):
        return None
