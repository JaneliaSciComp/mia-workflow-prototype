"""The poller: one scheduled pass that turns GitHub state and dropped files into workflow signals.

- a `buffer:go` / `buffer:approve` label on a buffer issue becomes a signal (mia-data's `deliver_label`, which
  starts the workflow if needed and removes the label only once the signal is accepted);
- for issues in `buffer:sourcing` or `buffer:ready`, a dataset waiting for a manual drop whose `incoming/`
  directory holds a file gets a `dropped` signal once that directory's listing has been stable for a whole poll
  interval (a file still being copied grows between passes), and once per change of it;
- for the same issues, a dataset waiting on mia-data gets a `dataset_update` signal when its folder is prepared
  under storage_root (`binding`) or its dataset issue is labelled `data:ineligible` (`ineligible`), once each.

Runs as the `poll` activity and uses the activity object's clients and helpers.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from mia_data.poll import deliver_label
from mia_data.publish import prepared_row

from .config import HUMAN_LABELS
from .models import BufferState

if TYPE_CHECKING:
    from .activities import Activities

log = logging.getLogger(__name__)


async def run_poll(activities: Activities) -> list[str]:
    """One pass over the issue repo's labelled issues. Returns a log of what was done."""
    cfg, gh = activities.cfg, activities.gh_issues
    done: list[str] = []
    for kind in HUMAN_LABELS:
        label = cfg.label(kind)
        for issue in await gh.list_issues(labels=label, state="open"):
            try:
                if await deliver_label(
                    activities, kind, label, issue, workflow="MiaBuffer", make_state=BufferState, guard=None
                ):
                    done.append(f"{kind} #{issue['number']}")
            except Exception:
                log.exception("poll: issue #%s %s failed", issue["number"], label)
    ineligible: dict[str, set[int]] = {}  # the datasets repo's ineligible issues, listed once per pass when needed
    for stage in ("sourcing", "ready"):
        for issue in await gh.list_issues(labels=cfg.label(stage), state="open"):
            try:
                done += await notice_drops(activities, issue["number"])
                done += await notice_datasets(activities, issue["number"], ineligible)
            except Exception:
                log.exception("poll: issue #%s drop or dataset signals failed", issue["number"])
    if done:
        log.info("poll: %s", ", ".join(done))
    return done


def _incoming_signature(directory) -> tuple | None:
    """(name, size, mtime) of every regular file under incoming/, sorted, or None when there is none."""
    try:
        entries = list(os.scandir(directory))
    except OSError:
        return None
    files = sorted(
        (entry.name, entry.stat().st_size, entry.stat().st_mtime_ns)
        for entry in entries
        if entry.is_file(follow_symlinks=False)
    )
    return tuple(files) or None


async def _buffer_state(activities: Activities, number: int) -> tuple[Any, BufferState | None]:
    """The issue's workflow handle and its queried state, or None when no workflow exists for it."""
    from temporalio.service import RPCError, RPCStatusCode

    handle = activities.temporal.get_workflow_handle(activities.cfg.workflow_id(number))
    try:
        return handle, await handle.query("status", result_type=BufferState)
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise
        return handle, None


async def _ineligible_issues(activities: Activities, cache: dict[str, set[int]]) -> set[int]:
    """Numbers of the datasets repo's issues labelled ineligible, from two list calls per poll pass (never one
    request per waiting dataset: a pool of hundreds would exhaust the App's rate limit)."""
    if "numbers" not in cache:
        numbers: set[int] = set()
        for label in ("data:ineligible", "licence-excluded"):
            issues = await activities.gh_datasets.list_issues(labels=label, state="all")
            numbers |= {int(issue["number"]) for issue in issues}
        cache["numbers"] = numbers
    return cache["numbers"]


async def notice_datasets(activities: Activities, number: int, ineligible: dict[str, set[int]]) -> list[str]:
    """Signal `dataset_update(record, outcome)` for every dataset of issue `number` waiting on mia-data whose
    binding is now published, or whose dataset issue became ineligible; each outcome once per process.
    `ineligible` is the pass's cache for the datasets repo's ineligible issue numbers."""
    handle, state = await _buffer_state(activities, number)
    if state is None:
        return []
    cfg, done = activities.cfg, []
    for dataset in state.datasets:
        if dataset.status != "waiting-dataset":
            continue
        outcome = None
        if dataset.folder and prepared_row(cfg.paths.storage_root / dataset.folder) is not None:  # spec §4.2
            outcome = "binding"
        elif dataset.issue is not None and dataset.issue in await _ineligible_issues(activities, ineligible):
            outcome = "ineligible"
        key = (number, dataset.record, outcome or "")
        if outcome is None or key in activities._signalled:
            continue
        await handle.signal("dataset_update", args=[dataset.record, outcome])
        activities._signalled.add(key)
        done.append(f"dataset #{number} {dataset.record} {outcome}")
    return done


async def notice_drops(activities: Activities, number: int) -> list[str]:
    """Signal `dropped(record)` for every waiting dataset of issue `number` whose incoming/ changed."""
    handle, state = await _buffer_state(activities, number)
    if state is None:
        return []
    cfg, done = activities.cfg, []
    for dataset in state.datasets:
        if dataset.status != "waiting-drop":
            continue
        key = (number, dataset.record)
        signature = _incoming_signature(cfg.paths.raw_root / dataset.record / "incoming")
        if signature is None or activities._dropped.get(key) == signature:
            continue
        if activities._seen_listing.get(key) != signature:  # first sight, or still changing: a copy in progress
            activities._seen_listing[key] = signature
            continue
        await handle.signal("dropped", dataset.record)  # unchanged for a whole poll interval: stable
        activities._dropped[key] = signature
        done.append(f"dropped #{number} {dataset.record}")
    return done
