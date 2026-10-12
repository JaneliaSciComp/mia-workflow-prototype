"""Issue comments: the in-place status comment, per-stage notes and crop tables (spec §4, §7.4, §7.5)."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote

from autodev.activities import neutralize

MARKER = "<!-- mia-data:status -->"


def _c(x) -> str:
    """Record or agent text is data: no forged markers, no table/line breaks."""
    return " ".join(str(x).split()).replace("<!--", "&lt;!--").replace("|", "\\|")


def render_stage(stage: str, detail: str = "") -> str:
    return f"**Stage:** `data:{_c(stage)}`" + (f"\n\n{neutralize(detail)}" if detail else "") + "\n"


def render_crops_table(rows: list[dict]) -> str:
    def cap(r):
        p, b = r.get("plane") or {}, r.get("box") or {}
        return _c(neutralize(f"{r['crop_id']} · z={p.get('z')} c={p.get('c')} · ({b.get('y')}, {b.get('x')})"))
    lines = ["| raw | overlay | caption |", "|---|---|---|",
             *(f"| ![raw]({quote(r['raw_url'], safe=':/?=&%')}) | ![overlay]({quote(r['overlay_url'], safe=':/?=&%')}) | {cap(r)} |"
               for r in rows)]
    table = "\n".join(lines) + "\n"
    return f"<details><summary>{len(rows)} crops</summary>\n\n{table}\n</details>\n" if len(rows) > 4 else table


def render_status(state, links: dict) -> str:
    """links: optional "pr" (URL), "crops" (a render_crops_table result), "totals" (text)."""
    p = Path(state.binding) if state.binding else None
    by = neutralize(p.read_text()) if p and p.is_file() else None
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", by or "")), default=0))
    pr = f"[#{state.pr}]({quote(links['pr'], safe=':/')})" if state.pr and links.get("pr") else (f"#{state.pr}" if state.pr else "(none)")
    out = [MARKER, f"**Stage:** `data:{_c(state.stage)}`", "",
           f"**Binding** (sha256 `{_c(state.binding_hash)}`):" if by else "**Binding:** (none)"]
    if by:
        out += [f"{fence}yaml", by.rstrip("\n"), fence]
    out += ["", f"**Snippet PR:** {pr}"]
    if links.get("crops"):
        out += ["", "**Latest crops:**", "", neutralize(links["crops"])]
    if links.get("totals"):
        out += ["", f"**Totals:** {neutralize(links['totals'])}"]
    return "\n".join(out) + "\n"
