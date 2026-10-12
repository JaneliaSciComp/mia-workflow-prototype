"""`mia-data seed`: one data-repo issue per downloaded dataset (spec §7.1)."""

from __future__ import annotations

import logging
import re

from . import catalog

log = logging.getLogger(__name__)
MARKER = "<!-- mia-data:record={} -->"
MARKER_RE = re.compile(r"<!-- mia-data:record=(\S+) -->")


def render_issue(rec: dict, entry: dict, licences: list[str], repo: str = "", path: str = "") -> tuple[str, str, list[str]]:
    id_, img, spdx = rec["id"], rec.get("imaging") or {}, (rec.get("license") or {}).get("spdx")
    arrays = (rec.get("technical") or {}).get("arrays") or []
    family = (img.get("modality") or ["unknown"])[0]
    labels = ["data:new" if spdx in licences else "data:ineligible", f"modality:{family}"]
    if spdx not in licences:
        labels.append("licence-excluded")
    if any(a["role"] == "label" for a in arrays):
        labels.append("has-labels")

    def yx(a):
        s, ax = a["shape"], a.get("axes") or ""
        return (s[ax.index("y")], s[ax.index("x")]) if {"y", "x"} <= set(ax) and len(ax) == len(s) else tuple(s[-2:])
    raws = [a for a in arrays if a["role"] == "raw" and a.get("shape")]
    if not any(len(a["shape"]) >= 2 and min(yx(a)) >= 256 for a in raws):
        labels.append("maybe-small")
    base = f"https://github.com/{repo}/blob"
    vox = img.get("voxel_size_nm") or {}
    data = rec.get("data") or {}
    urls = data.get("download_url") or []
    lines = [MARKER.format(id_), "",
             f"- Record: [main]({base}/main/{path}), [at `{entry['record_commit'][:12]}`]({base}/{entry['record_commit']}/{path})",
             f"- Licence: {spdx}", f"- Modality: {', '.join(img.get('modality') or [])}",
             f"- Organism: {', '.join(img.get('organism') or [])}",
             f"- Voxel size (nm): x={vox.get('x')} y={vox.get('y')} z={vox.get('z')}",
             f"- Data size: {entry.get('bytes_on_disk')} bytes",
             f"- Source: {', '.join([urls] if isinstance(urls, str) else urls)}",
             f"- Downloaded by: {', '.join(entry['downloaders'])}", f"- Dataset dir: `{entry['dataset_dir']}`", "",
             "| role | axes | shape | dtype |", "|---|---|---|---|",
             *(f"| {a['role']} | {a.get('axes')} | {a.get('shape')} | {a.get('dtype')} |" for a in arrays)]
    if spdx not in licences:
        lines += ["", f"**Ineligible:** licence `{spdx}` is not in `crops.licences` ({', '.join(licences)}); `go` is refused."]
    return f"{id_}: {rec.get('short_name') or rec.get('title')}", "\n".join(lines) + "\n", labels


async def seed(cfg, gh_data, gh_catalog, dry_run: bool = False) -> list[str]:
    """Create the missing issues; returns the record ids created (or that would be, with dry_run)."""
    have = set(MARKER_RE.findall("\n".join(i.get("body") or "" for i in await gh_data.list_issues(state="all"))))
    paths = await catalog.record_paths(gh_catalog)
    made = []
    for id_, entry in sorted((await catalog.downloaded(gh_catalog)).items()):
        if id_ in have:
            continue
        if id_ not in paths:
            log.warning("seed: %s is downloaded but not on main; skipping", id_)
            continue
        rec = await catalog.record(gh_catalog, paths[id_], "main")
        title, body, labels = render_issue(rec, entry, cfg.crops.licences, gh_catalog.repo, paths[id_])
        print(f"{'would create' if dry_run else 'creating'} {title} {labels}", flush=True)
        if not dry_run:
            await gh_data.create_issue(title, body, labels)
        made.append(id_)
    return made
