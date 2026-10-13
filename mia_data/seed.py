"""`mia-data seed`: create one issue in the data repo for every dataset the catalog has downloaded.

The issue body starts with a hidden marker naming the record id, which is how every later step finds
the record for an issue, and it carries labels for the licence, modality and size so humans can pick
datasets to `go`.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from . import catalog
from .status import safe_text

if TYPE_CHECKING:
    from autodev.github import GitHub

    from .config import Config

log = logging.getLogger(__name__)
MARKER = "<!-- mia-data:record={} -->"
MARKER_RE = re.compile(r"<!-- mia-data:record=(\S+) -->")  # matched only at the start of a body

ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,120}")


def render_issue(
    record: dict, entry: dict, licences: list[str], repo: str = "", path: str = "", record_id: str | None = None
) -> tuple[str, str, list[str]]:
    """Build (title, body, labels) for a dataset issue.

    `record` is the catalog's YAML record, `entry` its downloads-state entry (dataset_dir, record_commit,
    bytes_on_disk, downloaders), `repo`/`path` locate the record on GitHub for links."""
    record_id = record_id or record["id"]
    imaging, spdx = record.get("imaging") or {}, (record.get("license") or {}).get("spdx")
    arrays = (record.get("technical") or {}).get("arrays") or []
    family = (imaging.get("modality") or ["unknown"])[0]
    labels = ["data:new" if spdx in licences else "data:ineligible", f"modality:{family}"]
    if spdx not in licences:
        labels.append("licence-excluded")
    if any(array["role"] == "label" for array in arrays):
        labels.append("has-labels")

    def yx_size(array: dict) -> tuple:
        """The (y, x) extent of an array, from its axes when known, else its last two dimensions."""
        shape, axes = array["shape"], array.get("axes") or ""
        if {"y", "x"} <= set(axes) and len(axes) == len(shape):
            return shape[axes.index("y")], shape[axes.index("x")]
        return tuple(shape[-2:])

    raws = [array for array in arrays if array["role"] == "raw" and array.get("shape")]
    if not any(len(array["shape"]) >= 2 and min(yx_size(array)) >= 256 for array in raws):
        labels.append("maybe-small")
    base_url = f"https://github.com/{repo}/blob"
    voxel = imaging.get("voxel_size_nm") or {}
    data = record.get("data") or {}
    urls = data.get("download_url") or []
    lines = [
        MARKER.format(record_id),
        "",
        f"- Record: [main]({base_url}/main/{path}), "
        f"[at `{entry['record_commit'][:12]}`]({base_url}/{entry['record_commit']}/{path})",
        f"- Licence: {safe_text(spdx)}",
        f"- Modality: {safe_text(', '.join(imaging.get('modality') or []))}",
        f"- Organism: {safe_text(', '.join(imaging.get('organism') or []))}",
        f"- Voxel size (nm): {safe_text(f'x={voxel.get("x")} y={voxel.get("y")} z={voxel.get("z")}')}",
        f"- Data size: {entry.get('bytes_on_disk')} bytes",
        f"- Source: {safe_text(', '.join([urls] if isinstance(urls, str) else urls))}",
        f"- Downloaded by: {', '.join(entry['downloaders'])}",
        f"- Dataset dir: `{safe_text(entry['dataset_dir'])}`",
        "",
        "| role | axes | shape | dtype |",
        "|---|---|---|---|",
        *(
            f"| {safe_text(array['role'])} | {safe_text(array.get('axes'))} | {safe_text(array.get('shape'))} | "
            f"{safe_text(array.get('dtype'))} |"
            for array in arrays
        ),
    ]
    if spdx not in licences:
        lines += [
            "",
            f"**Ineligible:** licence `{safe_text(spdx)}` is not in `crops.licences` ({', '.join(licences)}); "
            "`go` is refused.",
        ]
    title = f"{record_id}: {safe_text(record.get('short_name') or record.get('title'))}"
    return title, "\n".join(lines) + "\n", labels


async def seed(cfg: Config, gh_data: GitHub, gh_catalog: GitHub, dry_run: bool = False) -> list[str]:
    """Create an issue in the data repo for each downloaded dataset that has none yet.

    `gh_data` is the GitHub client for the data repo (where the issues go) and `gh_catalog` the one for the
    catalog repo (where the records and download state are read). Returns the record ids created (or that
    would be, with `dry_run`)."""
    existing = {
        match.group(1)
        for issue in await gh_data.list_issues(state="all")
        if (match := MARKER_RE.match(issue.get("body") or ""))
    }
    paths = await catalog.record_paths(gh_catalog)
    created = []
    for record_id, entry in sorted((await catalog.downloaded(gh_catalog)).items()):
        if record_id in existing:
            continue
        if not ID_RE.fullmatch(record_id):
            log.warning("seed: bad record id %r; skipping", record_id)
            continue
        if record_id not in paths:
            log.warning("seed: %s is downloaded but not on main; skipping", record_id)
            continue
        record = await catalog.record(gh_catalog, paths[record_id], "main")
        if record.get("id") not in (None, record_id):  # the file name is the trusted id; the field must agree
            log.warning("seed: %s has id %r inside; skipping", record_id, record["id"])
            continue
        title, body, labels = render_issue(
            record, entry, cfg.crops.licences, gh_catalog.repo, paths[record_id], record_id
        )
        print(f"{'would create' if dry_run else 'creating'} {title} {labels}", flush=True)
        if not dry_run:
            await gh_data.create_issue(title, body, labels)
        created.append(record_id)
    return created
