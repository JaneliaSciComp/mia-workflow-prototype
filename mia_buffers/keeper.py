"""The cache keeper's rules: which asset rows are free to evict, how many to evict, which builds are stale, and
`evict_path`, the only delete under the roots. The `keep` activity does the I/O around them."""

from __future__ import annotations

import logging
import shutil
import time
from collections.abc import Callable
from pathlib import Path

log = logging.getLogger(__name__)
BUILDING = ("fetching", "staging")  # states a job is writing under


def _bytes(data: dict) -> float:
    value = data.get("bytes")
    return value if isinstance(value, int | float) and not isinstance(value, bool) else 0


def _last_used(data: dict) -> float:
    return data.get("last_used") or 0  # the registry sets it at a row's first scan; none at all is never used


def live_bytes(assets: list[dict]) -> float:
    """What the rows occupy on disk: the bytes of every row that is not missing or evicted."""
    return sum(
        _bytes(data) for row in assets if (data := row.get("data") or {}).get("state") not in ("missing", "evicted")
    )


def free_assets(assets: list[dict], *, hold_s: float, now: float, in_progress: set[str], kind: str) -> list[dict]:
    """The rows nothing needs: no buffer references them, they are `ready`, unused for `hold_s`, and (raw) no
    dataset of their record is being prepared. Least recently used first."""
    free = [
        row
        for row in assets
        if not row.get("refs")
        and (data := row.get("data") or {}).get("state") == "ready"
        and _last_used(data) + hold_s < now
        and not (kind == "raw" and row.get("key") in in_progress)
    ]
    return sorted(free, key=lambda row: _last_used(row["data"]))


def plan_evictions(
    assets: list[dict],
    *,
    quota_bytes: float,
    headroom_bytes: float,
    hold_s: float,
    now: float,
    in_progress: set[str],
    kind: str,
) -> list[dict]:
    """The free rows to evict, oldest first, until the kind is under its quota minus the headroom; none when the
    quota is 0 (unlimited)."""
    if quota_bytes <= 0:
        return []
    remaining, target, picked = live_bytes(assets), quota_bytes - headroom_bytes, []
    for row in free_assets(assets, hold_s=hold_s, now=now, in_progress=in_progress, kind=kind):
        if remaining <= target:
            break
        picked.append(row)
        remaining -= _bytes(row["data"])
    return picked


def evict_path(path: Path, roots: tuple[Path, ...]) -> bool:
    """Remove the directory `path` when it resolves strictly under one of `roots`. False (logged, nothing touched)
    when it is a symlink (it would delete another asset's content), not under a root, or gone.

    The directory is first renamed to a `.building-evicted-*` sibling, so a removal that fails half way leaves
    nothing at `path` for a scan to find and reuse (a staged one is then swept by the `.building-*` cleanup);
    that failure raises OSError, and the caller must treat the asset as damaged."""
    if path.is_symlink():
        log.warning("refusing to remove %s: a symlink", path)
        return False
    resolved = path.resolve()
    if not any(resolved != root.resolve() and resolved.is_relative_to(root.resolve()) for root in roots):
        log.warning("refusing to remove %s: not under %s", path, ", ".join(map(str, roots)))
        return False
    if not resolved.is_dir():
        log.warning("not removing %s: no such directory", path)
        return False
    aside = resolved.with_name(f".building-evicted-{resolved.name}-{time.time_ns()}")
    resolved.rename(aside)
    shutil.rmtree(aside)
    return True


def stale_builds(assets: list[dict], finished: Callable[[str], bool]) -> list[tuple[str, str, str]]:
    """(kind, key, new state) for each row still `fetching`/`staging` under a job that has ended: a raw cache
    becomes `partial` (a fetch resumes it), a staged artifact `missing`."""
    return [
        (row["kind"], row["key"], "partial" if row["kind"] == "raw" else "missing")
        for row in assets
        if (data := row.get("data") or {}).get("state") in BUILDING and data.get("job") and finished(str(data["job"]))
    ]
