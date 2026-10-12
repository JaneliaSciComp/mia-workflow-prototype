"""Issue comments: one new comment per stage and result, never edited (spec §4, §7.4, §7.5)."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote

from autodev.activities import neutralize


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


def render_binding(path: str | None, digest: str | None) -> str:
    """The binding YAML in a fence longer than any backtick run inside it."""
    p = Path(path) if path else None
    if not (p and p.is_file()):
        return "**Binding:** (none)\n"
    by = neutralize(p.read_text())
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", by)), default=0))
    return f"**Binding** (sha256 `{_c(digest)}`):\n{fence}yaml\n{by.rstrip(chr(10))}\n{fence}\n"


def render_links(links: dict[str, str | None]) -> str:
    """`**Folders:** [name](url) · …` for the links that have a URL; empty when none has."""
    items = [f"[{_c(k)}]({quote(v, safe=':/?=&%')})" for k, v in links.items() if v]
    return f"**Folders:** {' · '.join(items)}\n" if items else ""
