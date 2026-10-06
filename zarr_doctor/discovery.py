"""Walk $LMD_ROOT/data, extract metadata from zarr JSON only (never chunk data), create issues."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import numpy as np

from . import __version__

log = logging.getLogger(__name__)

MARKER_RE = re.compile(r"<!-- zarr-doctor: path=(\S+) fingerprint=(\S+) -->")
VOCAB = ("class", "segmentation_type", "provenance", "proofreading_status", "coverage")
TRAITS = ("zarr2", "raw-float32", "raw-float64", "labels-int64", "labels-uint64",
          "chunks-not-128", "no-zstd", "no-voxel-size", "no-labels")


# --- metadata -------------------------------------------------------------------------------

def _node(d: Path) -> tuple[int, dict, dict | None] | None:
    """(zarr_format, attributes, array_metadata_or_None) for a zarr node directory, else None."""
    if (d / "zarr.json").is_file():
        m = json.loads((d / "zarr.json").read_text())
        return 3, m.get("attributes", {}), m if m.get("node_type") == "array" else None
    attrs = json.loads((d / ".zattrs").read_text()) if (d / ".zattrs").is_file() else {}
    if (d / ".zarray").is_file():
        return 2, attrs, json.loads((d / ".zarray").read_text())
    if (d / ".zgroup").is_file():
        return 2, attrs, None
    return None


def _codec(c: dict) -> str:
    name, conf = c.get("name") or c.get("id"), c.get("configuration", c)
    if name == "sharding_indexed":
        return "sharding[" + ", ".join(_codec(x) for x in conf.get("codecs", [])) + "]"
    if name == "blosc":
        return f"blosc({conf.get('cname')}, {conf.get('clevel')})"
    return f"{name}({conf['level']})" if "level" in conf else name


def _array_summary(fmt: int, meta: dict) -> dict:
    if fmt == 3:
        outer = meta["chunk_grid"]["configuration"]["chunk_shape"]
        codecs = meta.get("codecs", [])
        shard = next((c for c in codecs if c["name"] == "sharding_indexed"), None)
        dtype = meta["data_type"] if isinstance(meta["data_type"], str) else json.dumps(meta["data_type"])
        return {"shape": meta["shape"], "dtype": dtype,
                "chunks": shard["configuration"]["chunk_shape"] if shard else outer,
                "shards": outer if shard else None,
                "codecs": ", ".join(_codec(c) for c in codecs if c["name"] not in ("bytes", "transpose"))}
    comps = [*(meta.get("filters") or []), *([meta["compressor"]] if meta.get("compressor") else [])]
    return {"shape": meta["shape"], "dtype": np.dtype(meta["dtype"]).name, "chunks": meta["chunks"],
            "shards": None, "codecs": ", ".join(_codec(c) for c in comps) or "none"}


def _multiscale(attrs: dict) -> dict | None:
    ms = attrs.get("ome", {}).get("multiscales") or attrs.get("multiscales")
    return ms[0] if ms else None


def _voxel(ms: dict | None) -> tuple[list | None, list | None]:
    if not ms or not ms.get("datasets"):
        return None, None
    scale = next((t["scale"] for t in ms["datasets"][0].get("coordinateTransformations", [])
                  if t.get("type") == "scale"), None)
    units = [a.get("unit") for a in ms.get("axes", []) if isinstance(a, dict)] or None
    return scale, units


def extract(vol: Path) -> dict | None:
    """Metadata for one volume, or None if the directory is not a zarr group containing raw/."""
    group = _node(vol)
    raw = _node(vol / "raw") if group else None
    if not group or not raw:
        return None
    ms = _multiscale(raw[1]) or _multiscale(group[1])
    level = "s0"
    if ms and ms.get("datasets"):
        level = ms["datasets"][0]["path"].removeprefix("raw/")
    arr = _node(vol / "raw" / level)
    if not arr or arr[2] is None:
        raise ValueError(f"raw/{level} is not a zarr array")
    voxel, units = _voxel(ms)
    levels = len(ms["datasets"]) if ms else sum(1 for d in (vol / "raw").iterdir() if d.is_dir() and _node(d))
    axes = [a.get("name") for a in ms.get("axes", []) if isinstance(a, dict)] if ms else []
    labels = []
    if (vol / "labels").is_dir():
        for ld in sorted(p for p in (vol / "labels").iterdir() if p.is_dir()):
            ln = _node(ld)
            if not ln:
                continue
            lms = _multiscale(ln[1])
            lpath = lms["datasets"][0]["path"] if lms and lms.get("datasets") else "s0"
            la = _node(ld / lpath)
            vocab = {k: ln[1].get(k, ln[1].get("label_class") if k == "class" else None) for k in VOCAB}
            labels.append({"name": ld.name, "dtype": _array_summary(la[0], la[2])["dtype"] if la and la[2] else None,
                           "vocab": {k: v for k, v in vocab.items() if v is not None}})
    return {"zarr_format": arr[0], "level": level, **_array_summary(arr[0], arr[2]),
            "axes": axes, "voxel_size": voxel, "units": units, "scale_levels": levels, "labels": labels}


def fingerprint(meta: dict) -> str:
    return "sha1:" + hashlib.sha1(json.dumps(meta, sort_keys=True).encode()).hexdigest()


def traits(meta: dict) -> list[str]:
    t = []
    if meta["zarr_format"] == 2:
        t.append("zarr2")
    if meta["dtype"] in ("float32", "float64"):
        t.append(f"raw-{meta['dtype']}")
    for dt in ("int64", "uint64"):
        if any(lb["dtype"] == dt for lb in meta["labels"]):
            t.append(f"labels-{dt}")
    if any(c != 128 for c in meta["chunks"][-3:]):
        t.append("chunks-not-128")
    if "zstd" not in meta["codecs"]:
        t.append("no-zstd")
    if not meta["voxel_size"]:
        t.append("no-voxel-size")
    if not meta["labels"]:
        t.append("no-labels")
    return t


def parse_dataset(name: str) -> tuple[str, str] | None:
    """(modality, organism) from '{modality}-{organism}-{dataset}'."""
    parts = name.split("-", 2)
    return (parts[0], parts[1]) if len(parts) == 3 and all(parts) else None


def labels_for(rel: str, meta: dict) -> list[str]:
    ds = rel.split("/")[1]
    mo = parse_dataset(ds)
    return [f"trait:{t}" for t in traits(meta)] + ([f"modality:{mo[0]}", f"organism:{mo[1]}"] if mo else [])


def title_for(rel: str) -> str:
    _, ds, crop = rel.split("/")
    return f"[{ds}] {crop.removesuffix('.zarr')}" if parse_dataset(ds) else f"{ds}/{crop}"


def _x(v) -> str:
    return " × ".join(f"{x:g}" if isinstance(x, float) else str(x) for x in v)


def render_body(rel: str, meta: dict, fp: str, now: datetime | None = None) -> str:
    _, ds, crop = rel.split("/")
    axes = f" ({', '.join(meta['axes'])})" if meta["axes"] else ""
    unit = {"nanometer": "nm", "micrometer": "µm"}.get((meta["units"] or [None])[0], (meta["units"] or [""])[0] or "")
    voxel = f"{_x(meta['voxel_size'])} {unit}".strip() if meta["voxel_size"] else "—"
    chunks = _x(meta["chunks"]) + (f" (shards {_x(meta['shards'])})" if meta["shards"] else "")
    labels = ", ".join(f"`{lb['name']}` ({lb['dtype']})" for lb in meta["labels"]) or "—"
    complete = sum(1 for lb in meta["labels"] if len(lb["vocab"]) == len(VOCAB))
    tr = ", ".join(traits(meta)) or "none"
    now = now or datetime.now(timezone.utc)
    return f"""### {ds} / {crop}

| field | value |
|---|---|
| path | `{rel}` |
| zarr format | v{meta['zarr_format']} |
| raw/{meta['level']} shape | {_x(meta['shape'])}{axes} |
| raw dtype | {meta['dtype']} |
| raw chunks | {chunks} |
| codecs | {meta['codecs']} |
| voxel size | {voxel} |
| scale levels | {meta['scale_levels']} |
| labels | {labels} |
| label vocab complete | {complete} of {len(meta['labels'])} |

**Traits:** {tr}

_Discovered {now.isoformat(timespec='seconds')} by zarr-doctor {__version__}._

<!-- zarr-doctor: path={rel} fingerprint={fp} -->
"""


# --- walk + create --------------------------------------------------------------------------

def iter_candidates(lmd_root: Path):
    """Every directory directly under a dataset directory."""
    for ds in sorted(p for p in (lmd_root / "data").iterdir() if p.is_dir()):
        yield from sorted(p for p in ds.iterdir() if p.is_dir())


@dataclass
class Summary:
    found: int = 0
    existing: int = 0
    created: int = 0
    updated: int = 0
    deferred: int = 0
    skipped: int = 0
    errors: int = 0


async def discover(cfg, *, gh=None, state, set_stage=None, max_new: int = 10, dry_run: bool = False,
                   pattern: str | None = None, out=print) -> Summary:
    """See spec §5. In dry-run mode gh/set_stage are unused and nothing is written."""
    s = Summary()
    if not dry_run:
        # One paged pass over the repo's issues reconciles SQLite with GitHub (covers first run and cache misses).
        for path, (num, fp) in (await gh.issue_markers()).items():
            if state.get_volume(path) is None:
                state.upsert_volume(path, num, fp)
    for vol in iter_candidates(cfg.lmd_root):
        rel = vol.relative_to(cfg.lmd_root).as_posix()
        if pattern and not fnmatch.fnmatch(rel, pattern):
            continue
        try:
            meta = extract(vol)
        except Exception as e:
            s.errors += 1
            out(f"ERROR   {rel}: {type(e).__name__}: {e}")
            continue
        if meta is None:
            s.skipped += 1
            log.info("skip %s: not a zarr group with raw/", rel)
            continue
        s.found += 1
        fp = fingerprint(meta)
        known = state.get_volume(rel)
        if dry_run:
            status = "known" if known and known[1] == fp else "changed" if known else "new"
            out(f"{status:7} {rel}  v{meta['zarr_format']} {meta['dtype']} {_x(meta['shape'])} "
                f"chunks {_x(meta['chunks'])}  [{', '.join(traits(meta))}]")
            s.existing += bool(known)
            s.created += not known
            continue
        if known:
            s.existing += 1
            if known[1] != fp:
                issue = known[0]
                await gh.update_issue(issue, body=render_body(rel, meta, fp))
                await gh.comment(issue, f"Metadata changed: fingerprint `{known[1]}` → `{fp}`. Issue body updated.")
                state.upsert_volume(rel, issue, fp)
                s.updated += 1
            continue
        if s.created >= max_new:
            s.deferred += 1
            continue
        issue = (await gh.create_issue(title_for(rel), render_body(rel, meta, fp), labels_for(rel, meta)))["number"]
        state.upsert_volume(rel, issue, fp)  # record immediately: a crash after this never duplicates
        s.created += 1
        out(f"created #{issue} {rel}")
        if set_stage:
            await set_stage(issue, "discovered")
    return s
