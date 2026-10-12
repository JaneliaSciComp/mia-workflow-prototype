"""Binding checks and job hosts (spec §5.4, §9). Worker side: plain dicts, never the harness."""

from __future__ import annotations

import fnmatch
import re
from urllib.parse import urlsplit

from autodev.config import is_github_host

LABEL_KEY_RE = re.compile(r"[a-z_]+-[a-z0-9_]+-(instance|semantic|binary|points)")


def https(url: str) -> str:
    """s3://bucket/key -> https://bucket.s3.amazonaws.com/key; anything else unchanged."""
    if url.startswith("s3://"):
        bucket, _, key = url[5:].partition("/")
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    return url


def _host(url: str) -> str:
    """Lowercased host of a URL (or `<url>::<member>`); "" for a relative path."""
    return urlsplit(https(url).split("::", 1)[0]).hostname or ""


def record_urls(record: dict) -> list[str]:
    data, tech = record.get("data") or {}, record.get("technical") or {}
    d = data.get("download_url") or []
    return [*([d] if isinstance(d, str) else d), *((tech.get("sample") or {}).get("urls") or []),
            *(a["path_pattern"] for a in tech.get("arrays") or [] if a.get("path_pattern"))]


def check_binding(b: dict, record: dict, catalog_names: set[str], *, pins) -> list[str]:
    """Errors in an agent-written binding (empty = ok). Rewrites s3:// URLs to https in place. `pins` (a
    models.Pins) names the record: the record's own `id` field is not trusted (see seed)."""
    if not isinstance(b, dict):
        return ["binding is not a mapping"]
    errs = [f"{k} {b.get(k)!r} is not {want!r}" for k, want in (("record", pins.record), ("record_commit", pins.record_commit))
            if b.get(k) != want]
    raw, labels = b.get("raw"), b.get("labels") or []
    if not (isinstance(raw, list) and raw and isinstance(labels, list)):
        return errs + ["raw must be a non-empty list and labels a list"]
    allowed = {h for u in record_urls(record) if (h := _host(u))}
    for where, arrs in (("raw", raw), ("labels", labels)):
        for i, a in enumerate(arrs):
            p = f"{where}[{i}]"
            if not (isinstance(a, dict) and isinstance(a.get("urls"), list) and a["urls"]):
                errs.append(f"{p}: needs a non-empty urls list")
                continue
            if not (isinstance(a.get("snippet"), str) and a["snippet"] in catalog_names):
                errs.append(f"{p}: unknown snippet {a.get('snippet')!r}")
            for j, u in enumerate(a["urls"]):
                if not (isinstance(u, str) and (u := https(u)).startswith("https://")):
                    errs.append(f"{p}: {u!r} is not an https URL")
                    continue
                a["urls"][j] = u
                if (h := _host(u)) not in allowed:
                    errs.append(f"{p}: host {h!r} is not one of the record's hosts {sorted(allowed)}")
            if where == "labels":
                if not (isinstance(a.get("label_key"), str) and LABEL_KEY_RE.fullmatch(a["label_key"])):
                    errs.append(f"{p}: bad label_key {a.get('label_key')!r}")
                if not (type(a.get("pairs_with")) is int and 0 <= a["pairs_with"] < len(raw)):
                    errs.append(f"{p}: pairs_with {a.get('pairs_with')!r} is not an index into raw")
    return errs


def hosts(urls: list[str], cfg) -> list[str]:
    """Exact hosts a job may reach: the URLs' hosts, their configured redirect targets, and the package hosts."""
    hs = {h for u in urls if (h := _host(u))}
    redirects = {k.lower(): v for k, v in cfg.network.redirect_hosts.items()}
    out = sorted(hs | {r.lower() for h in hs for r in redirects.get(h, [])} | {p.lower() for p in cfg.network.package_hosts})
    if bad := [h for h in out if is_github_host(h)]:
        raise ValueError(f"GitHub hosts {bad} must never be granted to a job")
    return out


def missing_hosts(hosts: list[str], patterns: list[str]) -> list[str]:
    """Hosts no `csub probe` allowed_hosts pattern matches (csub/broker/resolve.py: check_hosts)."""
    return [h for h in hosts if not any(h == p or fnmatch.fnmatchcase(h, p) for p in patterns)]
