"""`mia-data seed`: create one issue in the data repo for every dataset the catalog has downloaded.

The issue body starts with a hidden marker naming the record id, which is how every later step finds
the record for an issue, and it carries the modality and whether labels ship so humans can pick
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
    record: dict, entry: dict, repo: str = "", path: str = "", record_id: str | None = None
) -> tuple[str, str, list[str]]:
    """Build (title, body, labels) for a dataset issue.

    `record` is the catalog's YAML record, `entry` its downloads-state entry (dataset_dir, record_commit,
    bytes_on_disk, downloaders) or just `{"record_commit": sha}` for a record nobody downloaded (a buffer asks
    for it; its jobs fetch on demand), `repo`/`path` locate the record on GitHub for links."""
    record_id = record_id or record["id"]
    imaging, spdx = record.get("imaging") or {}, (record.get("license") or {}).get("spdx")
    arrays = (record.get("technical") or {}).get("arrays") or []
    family = (imaging.get("modality") or ["unknown"])[0]
    # no licence or size verdict here: who may use a dataset is each buffer spec's decision (spec §12)
    labels = ["data:new", f"modality:{family}"]
    if any(array["role"] == "label" for array in arrays):
        labels.append("has-labels")
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
        f"- Data size: {entry.get('bytes_on_disk', 'unknown')} bytes",
        f"- Source: {safe_text(', '.join([urls] if isinstance(urls, str) else urls))}",
        f"- Downloaded by: {', '.join(entry.get('downloaders') or []) or 'none (jobs fetch on demand)'}",
        *([f"- Dataset dir: `{safe_text(entry['dataset_dir'])}`"] if entry.get("dataset_dir") else []),
        "",
        "| role | axes | shape | dtype |",
        "|---|---|---|---|",
        *(
            f"| {safe_text(array['role'])} | {safe_text(array.get('axes'))} | {safe_text(array.get('shape'))} | "
            f"{safe_text(array.get('dtype'))} |"
            for array in arrays
        ),
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
        title, body, labels = render_issue(record, entry, gh_catalog.repo, paths[record_id], record_id)
        print(f"{'would create' if dry_run else 'creating'} {title} {labels}", flush=True)
        if not dry_run:
            await gh_data.create_issue(title, body, labels)
        created.append(record_id)
    return created
