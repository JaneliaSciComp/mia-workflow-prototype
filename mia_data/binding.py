"""Checking agent-written bindings, and deciding which hosts a job may reach.

A binding is the YAML the match agent writes: which snippet loads which URL of the dataset. This module
checks it on the worker side as plain dicts, without the harness, and derives the job's network allowlist
from the URLs it names.
"""

from __future__ import annotations

import fnmatch
import re
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from autodev.config import is_github_host

if TYPE_CHECKING:
    from .config import Config
    from .models import Pins

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
    """Every URL a catalog record names: download URLs, sample URLs and array path patterns."""
    data, technical = record.get("data") or {}, record.get("technical") or {}
    download = data.get("download_url") or []
    return [
        *([download] if isinstance(download, str) else download),
        *((technical.get("sample") or {}).get("urls") or []),
        *(array["path_pattern"] for array in technical.get("arrays") or [] if array.get("path_pattern")),
    ]


def check_binding(binding: dict, record: dict, catalog_names: set[str], *, pins: Pins) -> list[str]:
    """Errors in an agent-written binding (empty = ok). Rewrites s3:// URLs to https in place.

    The binding must name the pinned record and commit, use only snippets from the catalog, and only URLs
    on hosts the record itself names. `pins` is the trusted source of the record id: the record's own `id`
    field is not trusted (see seed)."""
    if not isinstance(binding, dict):
        return ["binding is not a mapping"]
    errors = [
        f"{key} {binding.get(key)!r} is not {want!r}"
        for key, want in (("record", pins.record), ("record_commit", pins.record_commit))
        if binding.get(key) != want
    ]
    raw, labels = binding.get("raw"), binding.get("labels") or []
    if not (isinstance(raw, list) and raw and isinstance(labels, list)):
        return errors + ["raw must be a non-empty list and labels a list"]
    allowed_hosts = {host for url in record_urls(record) if (host := _host(url))}
    for section, arrays in (("raw", raw), ("labels", labels)):
        for index, array in enumerate(arrays):
            location = f"{section}[{index}]"
            if not (isinstance(array, dict) and isinstance(array.get("urls"), list) and array["urls"]):
                errors.append(f"{location}: needs a non-empty urls list")
                continue
            if not (isinstance(array.get("snippet"), str) and array["snippet"] in catalog_names):
                errors.append(f"{location}: unknown snippet {array.get('snippet')!r}")
            for url_index, url in enumerate(array["urls"]):
                if not (isinstance(url, str) and (url := https(url)).startswith("https://")):
                    errors.append(f"{location}: {url!r} is not an https URL")
                    continue
                array["urls"][url_index] = url
                if (host := _host(url)) not in allowed_hosts:
                    errors.append(f"{location}: host {host!r} is not one of the record's hosts {sorted(allowed_hosts)}")
            if section == "labels":
                if not (isinstance(array.get("label_key"), str) and LABEL_KEY_RE.fullmatch(array["label_key"])):
                    errors.append(f"{location}: bad label_key {array.get('label_key')!r}")
                if not (type(array.get("pairs_with")) is int and 0 <= array["pairs_with"] < len(raw)):
                    errors.append(f"{location}: pairs_with {array.get('pairs_with')!r} is not an index into raw")
    return errors


def hosts(urls: list[str], cfg: Config) -> list[str]:
    """Exact hosts a job may reach: the URLs' hosts, their configured redirect targets, and the package hosts."""
    url_hosts = {host for url in urls if (host := _host(url))}
    redirects = {host.lower(): targets for host, targets in cfg.network.redirect_hosts.items()}
    allowed = sorted(
        url_hosts
        | {target.lower() for host in url_hosts for target in redirects.get(host, [])}
        | {host.lower() for host in cfg.network.package_hosts}
    )
    if bad := [host for host in allowed if is_github_host(host)]:
        raise ValueError(f"GitHub hosts {bad} must never be granted to a job")
    return allowed


def missing_hosts(hosts: list[str], patterns: list[str]) -> list[str]:
    """Hosts no `csub probe` allowed_hosts pattern matches (csub/broker/resolve.py: check_hosts)."""
    return [
        host for host in hosts if not any(host == pattern or fnmatch.fnmatchcase(host, pattern) for pattern in patterns)
    ]
