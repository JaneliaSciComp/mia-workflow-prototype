"""Projects v2 lookups/mutations and the stage abstraction shared by project and label modes."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

STAGE_LABELS = {"run": "zd:run", "running": "zd:running", "done": "zd:done", "failed": "zd:failed"}

_FIELDS = """projectV2(number:$n){ id fields(first:50){ nodes{
  ... on ProjectV2SingleSelectField { id name options{ id name } } } } }"""
_RESOLVE = f"""query($login:String!, $n:Int!){{ repositoryOwner(login:$login){{
  ... on Organization {{ {_FIELDS} }} ... on User {{ {_FIELDS} }} }} }}"""
_ITEMS = """query($p:ID!, $cursor:String, $field:String!){ node(id:$p){ ... on ProjectV2 {
  items(first:100, after:$cursor){ pageInfo{ hasNextPage endCursor } nodes{ id
    fieldValueByName(name:$field){ ... on ProjectV2ItemFieldSingleSelectValue { name } }
    content{ ... on Issue { number repository { nameWithOwner } } } } } } } }"""
_ISSUE_ITEMS = """query($owner:String!, $name:String!, $num:Int!){ repository(owner:$owner, name:$name){
  issue(number:$num){ id projectItems(first:50){ nodes{ id project{ id } } } } } }"""
_ADD = """mutation($p:ID!, $c:ID!){ addProjectV2ItemById(input:{projectId:$p, contentId:$c}){ item{ id } } }"""
_SET = """mutation($p:ID!, $i:ID!, $f:ID!, $o:String!){ updateProjectV2ItemFieldValue(
  input:{projectId:$p, itemId:$i, fieldId:$f, value:{singleSelectOptionId:$o}}){ projectV2Item{ id } } }"""


@dataclass
class ProjectIds:
    project_id: str
    field_id: str
    options: dict[str, str]  # option name -> id


async def resolve(gh, cfg, state=None, refresh: bool = False) -> ProjectIds:
    """Project/field/option IDs (cached in SQLite; they don't change). Raises if anything is missing."""
    key = f"project:{cfg.owner}/{cfg.trigger.project_number}"
    if state and not refresh and (cached := state.get(key)):
        return ProjectIds(**json.loads(cached))
    data = await gh.graphql(_RESOLVE, {"login": cfg.owner, "n": cfg.trigger.project_number})
    proj = (data.get("repositoryOwner") or {}).get("projectV2")
    if not proj:
        raise RuntimeError(f"project #{cfg.trigger.project_number} not found for {cfg.owner}")
    field = next((f for f in proj["fields"]["nodes"] if f.get("name") == cfg.trigger.status_field), None)
    if not field:
        raise RuntimeError(f"single-select field {cfg.trigger.status_field!r} not found in project")
    options = {o["name"]: o["id"] for o in field["options"]}
    missing = [s for s in cfg.trigger.statuses.values() if s not in options]
    if missing:
        raise RuntimeError(f"status options missing from project: {missing}")
    ids = ProjectIds(proj["id"], field["id"], options)
    if state:
        state.set(key, json.dumps(asdict(ids)))
    return ids


async def issues_with_status(gh, cfg, ids: ProjectIds, status: str) -> list[int]:
    out, cursor = [], None
    while True:
        data = await gh.graphql(_ITEMS, {"p": ids.project_id, "cursor": cursor, "field": cfg.trigger.status_field})
        items = data["node"]["items"]
        for it in items["nodes"]:
            c = it.get("content") or {}
            if (it.get("fieldValueByName") or {}).get("name") == status and c.get("number") \
                    and c.get("repository", {}).get("nameWithOwner", "").lower() == cfg.github.repo.lower():
                out.append(c["number"])
        if not items["pageInfo"]["hasNextPage"]:
            return out
        cursor = items["pageInfo"]["endCursor"]


async def item_for_issue(gh, cfg, ids: ProjectIds, number: int) -> str:
    """Project item id for the issue, adding the issue to the project if needed."""
    data = await gh.graphql(_ISSUE_ITEMS, {"owner": cfg.owner, "name": cfg.repo_name, "num": number})
    issue = data["repository"]["issue"]
    for it in issue["projectItems"]["nodes"]:
        if it["project"]["id"] == ids.project_id:
            return it["id"]
    return (await gh.graphql(_ADD, {"p": ids.project_id, "c": issue["id"]}))["addProjectV2ItemById"]["item"]["id"]


async def set_stage(cfg, gh, state, number: int, stage: str) -> None:
    """Move an issue to a stage: discovered | running | done | failed (idempotent)."""
    if cfg.trigger.mode == "project":
        ids = await resolve(gh, cfg, state)
        item = await item_for_issue(gh, cfg, ids, number)
        await gh.graphql(_SET, {"p": ids.project_id, "i": item, "f": ids.field_id,
                                "o": ids.options[cfg.trigger.statuses[stage]]})
        return
    if stage == "discovered":
        return
    await gh.add_labels(number, [STAGE_LABELS[stage]])
    for key, label in STAGE_LABELS.items():
        if key != stage:
            await gh.remove_label(number, label)
