"""Read mia-agentic-search: the downloaded-state files (branch `state`) and dataset records (branch `main`)."""

from __future__ import annotations

import json
import re

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


# §6.3 Placement: folder names as mia-agentic-search's tools/miao_layout.py@c25d216 builds them (FAMILIES from
# tools/readiness.py@8e9d14f). ponytail: copied tables; re-sync by hand when upstream adds an organism or modality.
FAMILIES = {
    "em": {"FIB-SEM", "SBF-SEM", "ssTEM", "ssSEM", "TEM", "cryo-EM", "cryo-ET"},
    "lm": {"confocal", "spinning-disk", "widefield", "light-sheet", "two-photon", "super-resolution", "brightfield",
           "histology-WSI", "phase-contrast", "DIC", "fluorescence-other"},
    "xray": {"X-ray"},
    "exm": {"expansion"},
}
ORGANISM_SHORT = {
    "Homo sapiens": "human", "Mus musculus": "mouse", "Rattus norvegicus": "rat",
    "Drosophila melanogaster": "fly", "Caenorhabditis elegans": "worm", "Danio rerio": "zebrafish",
    "Platynereis dumerilii": "platynereis", "Arabidopsis thaliana": "arabidopsis", "Gallus gallus": "chicken",
    "Macaca mulatta": "macaque", "Saccharomyces cerevisiae": "yeast", "Escherichia coli": "ecoli",
    "Trypanosoma brucei": "trypanosoma", "Xenopus laevis": "xenopus", "Sus scrofa": "pig",
    "Bos taurus": "cow", "Chlamydomonas reinhardtii": "chlamydomonas", "Tribolium castaneum": "tribolium",
    "Schmidtea mediterranea": "planaria", "Vibrio cholerae": "vibrio",
}


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def dataset_folder(rec: dict, record_id: str) -> str:
    """`{modality family}-{organism}-{name}`; ValueError where miao_layout would stop (family or organism unclear).
    `record_id` is the trusted id (the record's own `id` field is not, see seed)."""
    im = rec.get("imaging") or {}
    mods, orgs = im.get("modality") or [], im.get("organism") or []
    fams = [{f for m in ms for f, members in FAMILIES.items() if m in members} for ms in (mods[:1], mods)]
    fam = next((next(iter(f)) for f in fams if len(f) == 1), None)
    if fam is None or len(orgs) != 1 or not _slug(str(orgs[0])):
        raise ValueError(f"{record_id}: no dataset folder: modality {mods} / organism {orgs} unclear (one of each needed)")
    words = record_id.split("-")
    if len(words) > 2 and re.fullmatch(r"\d+", words[1]):  # accession prefix: zenodo-7142003-...
        words = words[2:]
    modality = {_slug(m).replace("-", "") for fam_ in FAMILIES.values() for m in fam_} | {"em", "lm", "sem", "tem", "cryoet"}
    name = _slug("-".join(w for w in words if w not in modality) or record_id)
    return f"{fam}-{ORGANISM_SHORT.get(orgs[0]) or _slug(str(orgs[0]))}-{name}"
