"""Lock files on host-local disk: one JSON file per (kind, key), taken with O_EXCL, kept alive by heartbeats."""

from __future__ import annotations

import contextlib
import json
import os
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,200}")


@dataclass
class Holder:
    """Who holds a lock, since when, and whether its heartbeat has lapsed."""

    kind: str
    key: str
    owner: str
    taken_at: float
    heartbeat_at: float
    ttl_s: float
    stale: bool


class LockDir:
    """Locks under `<root>/<kind>/<key>`; `root` must be host-local disk (O_EXCL is not safe on NFS)."""

    def __init__(self, root: Path, now: Callable[[], float] = time.time) -> None:
        self.root = root
        self.now = now

    def path(self, kind: str, key: str) -> Path:
        """The lock file for (kind, key); both names are validated so neither can escape the root."""
        for name in (kind, key):
            if not NAME_RE.fullmatch(name):
                raise ValueError(f"lock name {name!r} must match {NAME_RE.pattern}")
        return self.root / kind / key

    def take(self, kind: str, key: str, owner: str, ttl_s: float) -> bool:
        """Create the lock file; a stale one is renamed aside first. False when someone else holds it."""
        path = self.path(kind, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                holder = self.holder(kind, key)
                if holder is None:
                    continue  # released between our create and our read
                if not holder.stale:
                    return False
                with contextlib.suppress(FileNotFoundError):  # a second taker may have renamed it already
                    os.replace(path, path.with_name(f"{key}.stale-{int(self.now())}"))
                continue
            now = self.now()
            with os.fdopen(fd, "w") as handle:
                handle.write(json.dumps({"owner": owner, "taken_at": now, "heartbeat_at": now, "ttl_s": ttl_s}))
            return True
        return False

    def holder(self, kind: str, key: str) -> Holder | None:
        """The current holder, None when there is no lock file; an unreadable file is a stale holder."""
        try:
            text = self.path(kind, key).read_text()
        except FileNotFoundError:
            return None
        try:
            data = json.loads(text)
            owner, taken_at = str(data["owner"]), float(data["taken_at"])
            heartbeat_at, ttl_s = float(data["heartbeat_at"]), float(data["ttl_s"])
        except (ValueError, KeyError, TypeError):
            return Holder(kind, key, "", 0, 0, 0, True)
        return Holder(kind, key, owner, taken_at, heartbeat_at, ttl_s, heartbeat_at + ttl_s < self.now())

    def heartbeat(self, kind: str, key: str, owner: str) -> bool:
        """Refresh the heartbeat; False when the lock is gone or someone else holds it."""
        holder = self.holder(kind, key)
        if holder is None or holder.owner != owner:
            return False
        path = self.path(kind, key)
        data = {"owner": owner, "taken_at": holder.taken_at, "heartbeat_at": self.now(), "ttl_s": holder.ttl_s}
        temporary = path.with_name(f"{key}.tmp-{os.getpid()}")
        temporary.write_text(json.dumps(data))
        os.replace(temporary, path)
        return True

    def release(self, kind: str, key: str, owner: str) -> bool:
        """Remove the lock file; False when the lock is gone or someone else holds it."""
        holder = self.holder(kind, key)
        if holder is None or holder.owner != owner:
            return False
        with contextlib.suppress(FileNotFoundError):
            self.path(kind, key).unlink()
        return True

    def holders(self) -> list[Holder]:
        """Every lock file under the root (renamed-aside stale copies and temporaries excluded), by kind then key."""
        found = []
        if self.root.is_dir():
            for kind_dir in sorted(p for p in self.root.iterdir() if p.is_dir() and NAME_RE.fullmatch(p.name)):
                for lock_file in sorted(kind_dir.iterdir()):
                    if (
                        ".stale-" in lock_file.name
                        or ".tmp-" in lock_file.name
                        or not NAME_RE.fullmatch(lock_file.name)
                    ):
                        continue
                    holder = self.holder(kind_dir.name, lock_file.name)
                    if holder is not None:
                        found.append(holder)
        return found

    @contextlib.contextmanager
    def hold(
        self, kind: str, key: str, owner: str, ttl_s: float, timeout_s: float, poll_s: float = 2.0
    ) -> Iterator[None]:
        """Block until the lock is taken (TimeoutError after `timeout_s`); release it on exit."""
        deadline = self.now() + timeout_s
        while not self.take(kind, key, owner, ttl_s):
            current = self.holder(kind, key)
            if self.now() >= deadline:
                raise TimeoutError(f"{kind}/{key} held by {current.owner if current else '?'}")
            time.sleep(poll_s)
        try:
            yield
        finally:
            self.release(kind, key, owner)
