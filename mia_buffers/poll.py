"""The poller: one scheduled pass that turns GitHub state and dropped files into workflow signals.

- a `buffer:go` / `buffer:approve` label on a buffer issue becomes a signal (mia-data's `deliver_label`, which
  starts the workflow if needed and removes the label only once the signal is accepted);
- for issues in `buffer:sourcing` or `buffer:ready`, a dataset waiting for a manual drop whose `incoming/`
  directory holds a file gets a `dropped` signal once that directory's listing has been stable for a whole poll
  interval (a file still being copied grows between passes), and once per change of it;
- for the same issues, a dataset waiting on mia-data gets a `dataset_update` signal when its binding appears
  under storage_root (`binding`) or its dataset issue is labelled `data:ineligible` (`ineligible`), once each.

Runs as the `poll` activity and uses the activity object's clients and helpers.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from mia_data.poll import deliver_label

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
    for stage in ("sourcing", "ready"):
        for issue in await gh.list_issues(labels=cfg.label(stage), state="open"):
            try:
                done += await notice_drops(activities, issue["number"])
                done += await notice_datasets(activities, issue["number"])
            except Exception:
                log.exception("poll: issue #%s drops failed", issue["number"])
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


async def notice_datasets(activities: Activities, number: int) -> list[str]:
    """Signal `dataset_update(record, outcome)` for every dataset of issue `number` waiting on mia-data whose
    binding is now published, or whose dataset issue became ineligible; each outcome once per process."""
    handle, state = await _buffer_state(activities, number)
    if state is None:
        return []
    cfg, done = activities.cfg, []
    for dataset in state.datasets:
        if dataset.status != "waiting-dataset":
            continue
        outcome = None
        if dataset.folder and (cfg.paths.storage_root / dataset.folder / "binding.yaml").is_file():
            outcome = "binding"
        elif dataset.issue is not None:
            labels = {
                label["name"] for label in (await activities.gh_datasets.get_issue(dataset.issue)).get("labels", [])
            }
            if {"data:ineligible", "licence-excluded"} & labels:
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
