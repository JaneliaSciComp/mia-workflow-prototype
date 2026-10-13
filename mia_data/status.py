"""Rendering the pieces of mia-data's issue comments.

Every stage change and every result is a new comment, never an edit, so the issue reads as a log. All
text that came from a record or a job goes through `safe_text` or `neutralize` first.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from autodev.activities import neutralize


def safe_text(value: Any) -> str:
    """Record or agent text as a single markdown-safe line: no forged markers, no table or line breaks."""
    return " ".join(str(value).split()).replace("<!--", "&lt;!--").replace("|", "\\|")


def render_stage(stage: str, detail: str = "") -> str:
    return f"**Stage:** `data:{safe_text(stage)}`" + (f"\n\n{neutralize(detail)}" if detail else "") + "\n"


def render_crops_table(rows: list[dict]) -> str:
    """A markdown table of crop thumbnails (raw, overlay, caption), folded into <details> when it is long.

    Each row has crop_id, plane, box, raw_url and overlay_url (see Activities.publish_gallery)."""

    def caption(row: dict) -> str:
        plane, box = row.get("plane") or {}, row.get("box") or {}
        return safe_text(
            neutralize(f"{row['crop_id']} · z={plane.get('z')} c={plane.get('c')} · ({box.get('y')}, {box.get('x')})")
        )

    lines = [
        "| raw | overlay | caption |",
        "|---|---|---|",
        *(
            f"| ![raw]({quote(row['raw_url'], safe=':/?=&%')}) | ![overlay]({quote(row['overlay_url'], safe=':/?=&%')}) "
            f"| {caption(row)} |"
            for row in rows
        ),
    ]
    table = "\n".join(lines) + "\n"
    return f"<details><summary>{len(rows)} crops</summary>\n\n{table}\n</details>\n" if len(rows) > 4 else table


def render_binding(path: str | None, digest: str | None) -> str:
    """The binding YAML in a fence longer than any backtick run inside it."""
    binding_file = Path(path) if path else None
    if not (binding_file and binding_file.is_file()):
        return "**Binding:** (none)\n"
    text = neutralize(binding_file.read_text())
    fence = "`" * max(3, 1 + max(map(len, re.findall(r"`+", text)), default=0))
    return f"**Binding** (sha256 `{safe_text(digest)}`):\n{fence}yaml\n{text.rstrip(chr(10))}\n{fence}\n"


def render_links(links: dict[str, str | None]) -> str:
    """`**Folders:** [name](url) · …` for the links that have a URL; empty when none has."""
    items = [f"[{safe_text(name)}]({quote(url, safe=':/?=&%')})" for name, url in links.items() if url]
    return f"**Folders:** {' · '.join(items)}\n" if items else ""
