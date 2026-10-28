"""Rendering mia-buffers' issue comments: the plan, a manual-drop request, the staged board, the ready notice."""

from __future__ import annotations

from collections.abc import Sequence

from mia_buffers.models import StagedSummary
from mia_data.status import render_links, safe_text

COMMENT_LIMIT = 60_000  # GitHub allows 65,536 characters; leave room for the marker and links


def _in_pool(row: dict) -> bool:
    return bool(row.get("admitted")) and bool((row.get("fit") or {}).get("ok"))


def _voxel(row: dict) -> str:
    voxel = row.get("voxel_size_nm") or {}
    cell = "×".join(f"{voxel[axis]:g}" for axis in ("y", "x") if voxel.get(axis) is not None)
    return f"{cell} (assumed)" if cell and row.get("assumed") else cell


def render_plan_comment(plan: dict, link: str | None, dropped: dict[str, str] | None = None) -> str:
    """The plan as an issue comment: errors, warnings, the pool as a table (cut at COMMENT_LIMIT), a link to
    the browsable copy of plan.md and plan.json. `dropped` (record id -> reason) lists datasets that left the
    pool during sourcing; they are kept out of the table and the count."""
    rows = plan.get("rows") or []
    dropped = dropped or {}
    pool = [row for row in rows if _in_pool(row) and row.get("id") not in dropped]
    head = [
        f"**Plan** for `{safe_text(plan.get('spec_name'))}` · spec `{safe_text(str(plan.get('spec_hash'))[:12])}` · "
        f"catalog `{safe_text(str(plan.get('catalog_ref') or 'unpinned')[:12])}`",
        "",
        f"{len(pool)} in the pool of {plan.get('n_seen', len(rows))} seen.",  # older plan.json: every record is a row
        "",
    ]
    if plan.get("errors"):
        head += ["**Errors**", "", *(f"- {safe_text(error)}" for error in plan["errors"]), ""]
    if plan.get("warnings"):
        head += ["**Warnings**", "", *(f"- {safe_text(warning)}" for warning in plan["warnings"]), ""]
    if dropped:
        head += ["**Dropped**", "", *(f"- `{safe_text(rid)}`: {safe_text(why)}" for rid, why in dropped.items()), ""]
    tail = render_links({"Full plan": link}) if link else ""
    table = ["| id | modality | voxel nm | weight | splits | notes |", "|---|---|---|---|---|---|"]
    budget = COMMENT_LIMIT - len("\n".join(head)) - len(tail) - 200
    truncated = False
    for row in pool:
        splits = " / ".join(f"{name} {count}" for name, count in (row.get("splits") or {}).items())
        line = (
            f"| `{safe_text(row.get('id'))}` | {safe_text(row.get('modality_group') or '')} | {_voxel(row)} | "
            f"{float(row.get('weight') or 0):.3f} | {splits} | {safe_text('; '.join(row.get('notes') or []))} |"
        )
        if budget - len(line) - 1 < 0:
            truncated = True
            break
        table.append(line)
        budget -= len(line) + 1
    if truncated:
        table.append(f"\n(truncated: {len(table) - 2} of {len(pool)} pool rows shown; the full plan is linked below)")
    return "\n".join([*head, *table, "", tail]).rstrip() + "\n"


def render_drop_comment(
    record: str, incoming_link: str | None, files: list[list[str]], missing: list[str] | None = None
) -> str:
    """Ask a human to place a dataset's files: one line per file, `name` ← url."""
    lines = [
        f"`{safe_text(record)}` cannot be downloaded by a job"
        + (": these files are still missing" if missing else "; please place its files under the raw cache's")
        + (" `incoming/` directory:" if not missing else ":"),
        "",
    ]
    wanted = set(missing) if missing else None  # None: list every file
    lines += [
        f"- `incoming/{safe_text(name)}` ← {safe_text(url)}" for url, name in files if wanted is None or name in wanted
    ]
    lines += [
        "",
        "Copy each file to a temporary name first and `mv` it into `incoming/` when the copy is complete: the poller "
        "acts once the directory has been unchanged for a whole poll interval. Plain files only (no symlinks); the "
        "worker verifies and stages them.",
        "",
    ]
    return "\n".join(lines) + render_links({"Incoming": incoming_link})


def render_staged(
    datasets: list[tuple[str, str, str]],
    groups: dict[str, tuple[int, int]],
    min_fraction: float,
    summary: StagedSummary,
) -> str:
    """The staged gate's comment (spec §15): every dataset's outcome (`(record, status, reason)`, a failure with its
    error), each weighting group's servable/total against `min_fraction` with the short ones marked, volumes per
    split, bytes on disk, the datasets still waiting, and what to do next."""
    served = sum(1 for _, status, _ in datasets if status == "servable")
    lines, budget = [], COMMENT_LIMIT - 2_000  # room for the head, the groups, the summary and the call to action
    for record, status, reason in datasets:
        cut = reason if len(reason) <= 300 else reason[:300] + "…"
        line = f"| `{safe_text(record)}` | {safe_text(status)} | {safe_text(cut)} |"
        if budget - len(line) - 1 < 0:
            lines.append(
                f"\n(truncated: {len(lines)} of {len(datasets)} datasets shown; the board on the web UI has them all)"
            )
            break
        lines.append(line)
        budget -= len(line) + 1
    rows = "\n".join(lines)
    marks = ", ".join(
        f"`{safe_text(group)}` {servable}/{total}" + (" **below**" if servable / total < min_fraction else "")
        for group, (servable, total) in sorted(groups.items())
    )
    splits = ", ".join(f"{safe_text(split)} {count}" for split, count in summary.servable.items())
    waiting = f"\nWaiting: {', '.join(f'`{safe_text(r)}`' for r in summary.waiting)}." if summary.waiting else ""
    return (
        f"**Staged:** {served} of {len(datasets)} datasets servable.\n\n| dataset | outcome | detail |\n|---|---|---|\n"
        f"{rows}\n\nWeighting groups against ready.min_fraction {min_fraction:g}: {marks}.\n"
        f"Volumes servable per split: {splits or 'none'}. On disk: {summary.bytes / 1e6:.1f} MB.{waiting}\n\n"
        "Next: approve with `buffer:approve` to serve it as it is, or edit the spec and add `buffer:go` again."
    )


def render_ready(
    buffer_id: str, service_url: str, servable: list[str], short: Sequence[str] = (), min_fraction: float = 1.0
) -> str:
    """The ready notice: the buffer id, the service and the dataset cell from docs/development.md; `short` names
    the weighting groups (as "group servable/total") the approve accepted below `min_fraction`."""
    datasets = "\n".join(f"- `{safe_text(record)}`" for record in servable)
    accepted = (
        f"below ready.min_fraction {min_fraction:g}: {', '.join(safe_text(g) for g in short)}, approved\n\n"
        if short
        else ""
    )
    return (
        f"**Buffer:** `{safe_text(buffer_id)}` at {safe_text(service_url)}\n\nServable now:\n{datasets}\n\n{accepted}"
        "```python\nfrom mia_buffers.client import BufferDataset\n\n"
        f'URL, TENSORIZERS = "{safe_text(service_url)}", "<mia-snippets checkout>"  # or export MIA_BUFFERS_TENSORIZERS\n'
        f'train_ds = BufferDataset(URL, "{safe_text(buffer_id)}", "train", n_samples=4000, tensorizers=TENSORIZERS)\n'
        f'val_ds = BufferDataset(URL, "{safe_text(buffer_id)}", "val", tensorizers=TENSORIZERS)\n```\n'
    )
