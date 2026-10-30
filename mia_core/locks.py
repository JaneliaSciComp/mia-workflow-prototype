"""Lock files on host-local disk: one JSON file per (kind, key), taken with O_EXCL, kept alive by heartbeats."""

from __future__ import annotations

import contextlib
import itertools
import json
import logging
import os
import re
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)
_COUNTER = itertools.count()  # shared by every LockDir in the process, so temporary names never collide
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

    def _unique(self, path: Path, label: str) -> Path:
        """A sibling name of `path` nobody else uses (pid plus a counter); dot-prefixed, so never listed as a lock."""
        return path.with_name(f".{path.name}.{label}-{os.getpid()}-{next(_COUNTER)}")

    def _read(self, path: Path, kind: str, key: str) -> Holder | None:
        """The holder a lock file at `path` records; an unreadable file is a stale holder, a missing one None."""
        try:
            data = json.loads(path.read_text())
            owner, taken_at = str(data["owner"]), float(data["taken_at"])
            heartbeat_at, ttl_s = float(data["heartbeat_at"]), float(data["ttl_s"])
        except FileNotFoundError:
            return None
        except (ValueError, KeyError, TypeError):  # includes a non-UTF-8 file
            return Holder(kind, key, "", 0, 0, 0, True)
        return Holder(kind, key, owner, taken_at, heartbeat_at, ttl_s, heartbeat_at + ttl_s < self.now())

    def take(self, kind: str, key: str, owner: str, ttl_s: float) -> bool:
        """Create the lock file; a stale one is renamed aside first. False when someone else holds it; True, with
        a fresh heartbeat, when `owner` already holds it (a retried take must not wait on itself).

        The file is written complete under a temporary name and hard-linked into place, so it never exists
        empty; a taker that renames aside a lock it did not observe stale puts it back and loses."""
        path = self.path(kind, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self.now()
        temporary = self._unique(path, "new")
        temporary.write_text(json.dumps({"owner": owner, "taken_at": now, "heartbeat_at": now, "ttl_s": ttl_s}))
        try:
            for _ in range(2):
                try:
                    os.link(temporary, path)
                    return True
                except FileExistsError:
                    pass
                observed = self.holder(kind, key)
                if observed is None:
                    continue  # released between our link and our read
                if not observed.stale:
                    return observed.owner == owner and self.heartbeat(kind, key, owner)
                aside = self._unique(path, "aside")
                try:
                    os.replace(path, aside)
                except FileNotFoundError:
                    continue
                moved = self._read(aside, kind, key)
                if moved is None or (moved.owner, moved.heartbeat_at) != (observed.owner, observed.heartbeat_at):
                    # another taker got in first and we moved its live lock: put it back and lose
                    # ponytail: a third taker that links in between our replace and this link-back wins, and the
                    # moved lock's owner then holds a lock whose file is gone (it learns at its next heartbeat);
                    # three takers within microseconds of a stale lock. Upgrade: renameat2(RENAME_NOREPLACE).
                    with contextlib.suppress(FileExistsError):
                        os.link(aside, path)
                    aside.unlink(missing_ok=True)
                    return False
                for name in (f"{key}.stale-{int(now)}", f"{key}.stale-{int(now)}-{os.getpid()}-{next(_COUNTER)}"):
                    try:
                        os.link(aside, path.with_name(name))  # link, not rename: never overwrites earlier evidence
                        break
                    except FileExistsError:
                        continue
                aside.unlink(missing_ok=True)
            return False
        finally:
            temporary.unlink(missing_ok=True)

    def holder(self, kind: str, key: str) -> Holder | None:
        """The current holder, None when there is no lock file; an unreadable file is a stale holder."""
        return self._read(self.path(kind, key), kind, key)

    def heartbeat(self, kind: str, key: str, owner: str) -> bool:
        """Refresh the heartbeat; False when the lock is gone or someone else holds it."""
        holder = self.holder(kind, key)
        if holder is None or holder.owner != owner:
            return False
        path = self.path(kind, key)
        data = {"owner": owner, "taken_at": holder.taken_at, "heartbeat_at": self.now(), "ttl_s": holder.ttl_s}
        temporary = self._unique(path, "hb")
        temporary.write_text(json.dumps(data))
        # ponytail: a takeover between this check and the replace lets a lapsed owner overwrite the new lock for one
        # heartbeat; the owner has stopped heartbeating for a whole ttl by then. Upgrade: link-based swap.
        current = self.holder(kind, key)
        if current is None or current.owner != owner:
            temporary.unlink(missing_ok=True)
            return False
        os.replace(temporary, path)
        return True

    def release(self, kind: str, key: str, owner: str) -> bool:
        """Remove the lock file; False when the lock is gone or someone else holds it (it is put back)."""
        path = self.path(kind, key)
        claimed = self._unique(path, "rel")
        try:
            os.replace(path, claimed)  # takes the file out of play before looking at whose it is
        except FileNotFoundError:
            return False
        holder = self._read(claimed, kind, key)
        if holder is not None and holder.owner == owner:
            claimed.unlink()
            return True
        with contextlib.suppress(FileExistsError):
            os.link(claimed, path)
        claimed.unlink(missing_ok=True)
        return False

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
        """Block until the lock is taken (TimeoutError after `timeout_s`); release it on exit. Does not heartbeat."""
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


def take_or_holder(locks: LockDir, kind: str, key: str, owner: str, ttl_s: float) -> tuple[bool, str]:
    """(True, "") when `owner` took the lock, else (False, the holder's owner string; "?" when it was released
    between the take and the look): the body of both workers' take_lock activities."""
    if locks.take(kind, key, owner, ttl_s):
        return True, ""
    holder = locks.holder(kind, key)
    return False, holder.owner if holder else "?"


def release_or_log(locks: LockDir, kind: str, key: str, owner: str) -> None:
    """Release the lock if `owner` still holds it; a lock that lapsed or changed hands is logged and left."""
    if not locks.release(kind, key, owner):
        log.warning("%s: the lock %s/%s was no longer held at release", owner, kind, key)


def heartbeat_through(locks: LockDir, lock: Sequence[str], inner: Callable[[str], None]) -> Callable[[str], None]:
    """A job-wait heartbeat callback that calls `inner(state)`, then refreshes `lock` ([kind, key, owner]).

    A lock that is gone or someone else's is logged once, not raised: the job runs on either way."""
    kind, key, owner = lock
    lost = False

    def beat(state: str) -> None:
        nonlocal lost
        inner(state)
        if not locks.heartbeat(kind, key, owner) and not lost:
            lost = True
            log.warning("%s lost the lock %s/%s: it lapsed or someone else holds it", owner, kind, key)

    return beat
