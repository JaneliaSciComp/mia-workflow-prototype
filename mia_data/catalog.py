"""Read mia-agentic-search: the downloaded-state files (branch `state`) and dataset records (branch `main`)."""

from __future__ import annotations

import json

import yaml

FILES = {"state/downloads.json": "tensorswitch", "state/downloads-native.json": "native"}


async def downloaded(gh_catalog) -> dict[str, dict]:
    """Union of both `downloaded` maps. Newest `at` wins; `downloaders` lists every downloader that has the id."""
    out: dict[str, dict] = {}
    for path, name in FILES.items():
        raw = await gh_catalog.get_file(path, "state")
        for id_, e in (json.loads(raw)["downloaded"] if raw else {}).items():
            prev = out.get(id_)
            names = [*(prev["downloaders"] if prev else []), name]
            out[id_] = {**(e if not prev or e.get("at", "") > prev.get("at", "") else prev), "downloaders": names}
    return out


async def record_paths(gh_catalog) -> dict[str, str]:
    tree = (await gh_catalog.request("GET", f"/repos/{gh_catalog.repo}/git/trees/main", params={"recursive": 1})).json()
    return {p.rsplit("/", 1)[-1][:-5]: p for t in tree["tree"] if t["type"] == "blob"
            and (p := t["path"]).startswith("datasets/") and p.endswith(".yaml")}


async def record(gh_catalog, path: str, ref: str) -> dict:
    raw = await gh_catalog.get_file(path, ref)
    if raw is None:
        raise FileNotFoundError(f"{gh_catalog.repo}:{path}@{ref}")
    return yaml.safe_load(raw)
