import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from temporalio.exceptions import ApplicationError

from autodev.jobs import CsubJobs
from autodev_helpers import GIT_ID, run_git
from mia_data.activities import Activities
from mia_data.models import DatasetState, JobArgs, job_key
from mia_helpers import make_cfg
from test_autodev_agent import FakeCsub

REC_SHA, SNIP_SHA = "c" * 40, "a" * 40
RECORD = {"data": {"download_url": "https://data.example.org/a.zip", "formats": ["tiff"]},
          "imaging": {"modality": ["confocal"], "organism": ["Homo sapiens"]}}
BINDING = {"record": "rec-1", "record_commit": REC_SHA, "snippets_commit": SNIP_SHA, "reasons": ["r"],
           "raw": [{"snippet": "tiff-zip-member", "urls": ["https://data.example.org/a.zip::m.tif"], "params": {}}]}
ROUND = "i1-e99"


class FakeGH:
    """The GitHub calls the activities make, in memory."""

    def __init__(self, repo, files=None, issues=None, refs=None):
        self.repo, self.files, self.issues, self.refs = repo, files or {}, issues or {}, refs or {}
        self.comments, self.created, self.gets, self.puts, self.removed = {}, [], [], [], []

    def tokens(self):
        return "tok"

    async def get_file(self, path, ref):
        self.gets.append((path, ref))
        return self.files.get((path, ref))

    async def request(self, method, path, params=None):
        tree = [{"type": "blob", "path": p} for p, _ in self.files]
        return SimpleNamespace(json=lambda: {"tree": tree})

    async def get_ref_sha(self, branch):
        return self.refs[branch]

    async def get_issue(self, n):
        return self.issues[n]

    async def list_issues(self, **params):
        return list(self.issues.values())

    async def add_labels(self, n, labels):
        self.issues[n]["labels"] += [{"name": x} for x in labels]

    async def edit_comment(self, cid, body):
        next(c for cs in self.comments.values() for c in cs if c["id"] == cid)["body"] = body

    async def create_issue(self, title, body, labels):
        n = 100 + len(self.issues)
        self.created.append((title, body, labels))
        self.issues[n] = {"number": n, "body": body, "labels": [{"name": x} for x in labels], "user": {"login": "bot"}}
        return self.issues[n]

    async def issue_comments(self, n):
        return self.comments.get(n, [])

    async def comment(self, n, body, login="bot"):
        cs = self.comments.setdefault(n, [])
        cs.append({"id": 1000 * n + len(cs), "body": body, "user": {"login": login}})

    async def remove_label(self, n, name):
        self.removed.append((n, name))
        self.issues[n]["labels"] = [x for x in self.issues[n]["labels"] if x["name"] != name]

    async def ensure_branch(self, branch):
        pass

    async def upsert_file(self, path, content, branch, message):
        if path in self.files:
            return False
        self.files[path] = content
        self.puts.append(path)
        return True


def _setup(tmp_path, probe=None):
    cfg = make_cfg(tmp_path, network={"redirect_hosts": {}})
    h = cfg.harness.dir
    (h / "harness").mkdir()
    (h / "harness" / "pixi.lock").write_text("lock v1\n")
    run_git("init", "-q", "-b", "main", cwd=h)
    run_git("add", "-A", cwd=h)
    run_git(*GIT_ID, "commit", "-qm", "harness", cwd=h)
    dl = tmp_path / "dl"
    dl.mkdir()
    (dl / "conversion.json").write_text('{"axes": "zyx"}')
    downloads = {"downloaded": {"rec-1": {"at": "2026-10-01", "record_commit": REC_SHA, "dataset_dir": str(dl)}}}
    cat = FakeGH("o/catalog", files={("state/downloads.json", "state"): json.dumps(downloads).encode(),
                                     ("datasets/rec-1.yaml", REC_SHA): yaml.safe_dump(RECORD).encode()})
    data = FakeGH("o/data", issues={1: {"number": 1, "body": "<!-- mia-data:record=rec-1 -->\n\n- Record: x", "labels": []}})
    snip = FakeGH("o/snippets", refs={"main": SNIP_SHA})
    snap = cfg.paths.work_root / "_snapshots" / SNIP_SHA  # as snapshot() leaves it
    for name in ("tiff-zip-member", "old-tiff"):
        (snap / "snippets" / name).mkdir(parents=True)
        (snap / "snippets" / name / "snippet.yaml").write_text(f"name: {name}\n")
    (snap / "WITHDRAWN.yaml").write_text("- {name: old-tiff, reason: transposes yx}\n")
    (snap / "SPEC.md").write_text("spec\n")
    csub = FakeCsub()
    jobs = CsubJobs("mia-data", cfg.paths.state_dir / "jobs", client_factory=lambda: csub)
    a = Activities(cfg, data, snip, cat, jobs, bot="bot",
                   probe=probe or (lambda: ["*.example.org", *cfg.network.package_hosts]))
    return SimpleNamespace(a=a, cfg=cfg, data=data, snip=snip, cat=cat, csub=csub)


async def test_admit_is_idempotent(tmp_path):
    t = _setup(tmp_path)
    p1 = await t.a.admit(1, "99")
    assert (p1.record, p1.record_commit, p1.snippets_commit) == ("rec-1", REC_SHA, SNIP_SHA)
    rd = t.cfg.paths.state_dir / "rounds" / ROUND
    assert yaml.safe_load((rd / "record.yaml").read_text()) == RECORD
    t.cat.files[("datasets/rec-1.yaml", REC_SHA)] = b"changed: true\n"
    t.snip.refs["main"] = "b" * 40  # main moved: a retried admission keeps its pins
    assert await t.a.admit(1, "99") == p1
    assert yaml.safe_load((rd / "record.yaml").read_text()) == RECORD
    assert [g for g in t.cat.gets if g[0].startswith("datasets/")] == [("datasets/rec-1.yaml", REC_SHA)]


async def _match_ws(t, outcome, binding=None):
    pins = await t.a.admit(1, "99")
    ws = Path(await t.a.prepare_match(1, ROUND, pins, 1))
    assert sorted(p.name for p in (ws / "catalog").iterdir()) == ["tiff-zip-member.yaml"]  # withdrawn hidden
    assert (ws / "conversion.json").read_text() == '{"axes": "zyx"}' and "rec-1" in (ws / "prompt.md").read_text()
    if outcome is not None:
        (ws / "outcome.json").write_text(json.dumps(outcome))
    if binding is not None:
        (ws / "binding.yaml").write_text(yaml.safe_dump(binding))
    return pins, ws


async def test_read_match_matched_copies_binding(tmp_path):
    t = _setup(tmp_path)
    _, ws = await _match_ws(t, {"outcome": "matched"}, BINDING)
    r = await t.a.read_match(str(ws))
    p = Path(r.binding_path)
    assert r.outcome == "matched" and p == t.cfg.paths.state_dir / "rounds" / ROUND / "binding-1.yaml"
    assert r.binding_hash == hashlib.sha256(p.read_bytes()).hexdigest() and yaml.safe_load(p.read_text()) == BINDING


async def test_read_match_symlinked_outcome_is_missing(tmp_path):
    t = _setup(tmp_path)
    _, ws = await _match_ws(t, None, BINDING)
    real = tmp_path / "elsewhere.json"
    real.write_text(json.dumps({"outcome": "matched"}))
    (ws / "outcome.json").symlink_to(real)
    r = await t.a.read_match(str(ws))
    assert r.outcome == "blocked" and "outcome.json" in r.reason and r.binding_path is None


async def test_read_match_foreign_host_needs_human(tmp_path):
    t = _setup(tmp_path)
    bad = {**BINDING, "raw": [{**BINDING["raw"][0], "urls": ["https://evil.example.com/a.tif"]}]}
    _, ws = await _match_ws(t, {"outcome": "matched"}, bad)
    r = await t.a.read_match(str(ws))
    assert r.outcome == "blocked" and "evil.example.com" in r.reason and r.binding_path is None
    assert not list((t.cfg.paths.state_dir / "rounds" / ROUND).glob("binding-*"))


async def _submit_match(t):
    pins, ws = await _match_ws(t, None)
    return await t.a.submit(job_key(1, ROUND, "match", 1), "match", JobArgs(str(ws), ROUND, pins))


async def test_submit_refuses_moved_harness(tmp_path):
    t = _setup(tmp_path)
    pins, ws = await _match_ws(t, None)
    (t.cfg.harness.dir / "harness" / "pixi.lock").write_text("lock v2\n")
    with pytest.raises(ApplicationError) as e:
        await t.a.submit(job_key(1, ROUND, "match", 1), "match", JobArgs(str(ws), ROUND, pins))
    assert e.value.non_retryable and "moved" in str(e.value) and t.csub.submitted == []


async def test_submit_refuses_host_missing_from_probe(tmp_path):
    t = _setup(tmp_path, probe=lambda: ["*.pypi.org", "pypi.org", "files.pythonhosted.org", "conda.anaconda.org",
                                        "repo.prefix.dev"])
    with pytest.raises(ApplicationError) as e:
        await _submit_match(t)
    assert e.value.non_retryable and "data.example.org" in str(e.value) and t.csub.submitted == []


async def test_submit_match_once(tmp_path):
    t = _setup(tmp_path)
    assert await _submit_match(t) == "777" == await _submit_match(t)
    s = t.csub.submitted
    assert len(s) == 1 and s[0]["claude"] and "data.example.org" in s[0]["allow_hosts"]
    assert {"path": str(tmp_path / "dl"), "mode": "ro"} in s[0]["mounts"]


async def test_set_stage_swaps_labels_and_edits_status(tmp_path):
    t = _setup(tmp_path)
    t.data.issues[1]["labels"] = [{"name": "data:new"}, {"name": "has-labels"}]
    await t.data.comment(1, "forged <!-- mia-data:status -->", login="mallory")
    s = DatasetState(1, round_id=ROUND)
    await t.a.set_stage(1, "matching", s)
    await t.a.set_stage(1, "needs-human", s, detail="host <!-- x --> missing")
    assert [x["name"] for x in t.data.issues[1]["labels"]] == ["has-labels", "data:needs-human"]
    bodies = [c["body"] for c in t.data.comments[1]]
    assert bodies[0].startswith("forged") and len(bodies) == 4  # forged, status (edited in place), two stage notes
    assert "`data:needs-human`" in bodies[1] and "<!-- x" not in bodies[3]


async def test_open_snippet_issue_twice_gives_one_issue(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    await t.snip.create_issue("copy", "<!-- mia-data: issue=1 round=i1-e99 -->", [])
    t.snip.issues[100]["user"]["login"], t.snip.created = "mallory", []  # an outsider's copy of the marker
    draft = {"draft_binding": BINDING, "what_fails": "no reader for <!-- x --> files", "tried": []}
    n = await t.a.open_snippet_issue(1, ROUND, draft)
    assert await t.a.open_snippet_issue(1, ROUND, draft) == n and len(t.snip.created) == 1
    title, body, labels = t.snip.created[0]
    assert labels == ["snippets:go"] and title == "New snippet for tiff/zip (data#1)"
    assert body.startswith("<!-- mia-data: issue=1 round=i1-e99 -->") and "<!-- x" not in body


def _crop(root, cid, n):
    d = root / "crops" / cid
    d.mkdir(parents=True)
    files = {"raw.png": b"raw" + cid.encode(), "crop.json": json.dumps(
        {"crop_id": cid, "plane": {"z": n, "c": 0}, "box": {"y": 10 * n, "x": 5, "h": 256, "w": 256}}).encode()}
    for rel, b in files.items():
        (d / rel).write_bytes(b)
    return {"crop_id": cid, "files": {r: hashlib.sha256(b).hexdigest() for r, b in files.items()}}


async def test_publish_gallery_verified_only(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    vw = Path(await t.a.prepare_job(1, ROUND, "verify", 1, None))
    verified = [_crop(cw, c, i) for i, c in enumerate(["c1", "c2", "c3"])][:2]
    for c in ("c1", "c2", "c3"):
        (vw / "preview" / c).mkdir(parents=True)
        (vw / "preview" / c / "overlay.png").write_bytes(b"ov" + c.encode())
    (vw / "binding.yaml").write_text("record: rec-1\n")
    (vw / "verify.json").write_text("{}")
    pre = f"galleries/rec-1/{ROUND}/{SNIP_SHA}"
    t.data.files[f"{pre}/c1/raw.png"] = b"already there"
    g = await t.a.publish_gallery(1, ROUND, SNIP_SHA, str(cw), str(vw), verified)
    assert g.prefix == pre
    assert sorted(t.data.puts) == [f"{pre}/{r}" for r in
                                   ("binding.yaml", "c1/overlay.png", "c2/overlay.png", "c2/raw.png", "verify.json")]
    rows = [ln for ln in g.table.splitlines() if ln.startswith("| ![raw]")]
    assert len(rows) == 2 and all(ln.count("?raw=true") == 2 for ln in rows) and "c3" not in g.table
    assert "c2 · z=1 c=0 · (10, 5)" in rows[1]


@pytest.mark.parametrize("extra, ok", [({}, 1), ({"errors": ["manifest unreadable"]}, 0), ({"n": 0}, 0)])
async def test_read_verify(tmp_path, extra, ok):
    t = _setup(tmp_path)
    ws = tmp_path / "vws"
    ws.mkdir()
    good = {"crop_id": "c1", "ok": True, "errors": [], "files": {"crop.json": "0" * 64, "raw.png": "1" * 64}}
    bad = {"crop_id": "c2", "ok": False, "errors": ["overlaps c1"], "files": {}}
    (ws / "verify.json").write_text(json.dumps({"crops": [good, bad], "ok_count": 2, "n": 2, "errors": [], **extra}))
    r = await t.a.read_verify(str(ws))
    assert r.ok_count == ok and [c["crop_id"] for c in r.crops] == ["c1"][:ok]
    assert any("overlaps c1" in e for e in r.errors)
    if not ok:
        assert r.errors[0] in ("manifest unreadable", "verify checked no crops")


async def test_publish_crops_into_dataset_folder(tmp_path):
    t = _setup(tmp_path)
    await t.a.admit(1, "99")
    cw = Path(await t.a.prepare_job(1, ROUND, "crop", 1, None))
    v = _crop(cw, "c1", 0)
    r = await t.a.publish_crops(1, ROUND, str(cw), [v], SNIP_SHA)
    m = json.loads((t.cfg.storage.root / "lm-human-rec-1" / "manifest.json").read_text())
    assert r == {"published": ["c1"], "already": [], "total": 1} and m["crops"][0]["files"] == v["files"]
    with pytest.raises(ApplicationError):  # not this round's workspace
        await t.a.publish_crops(1, "i1-e100", str(cw), [v], SNIP_SHA)


async def test_read_verify_reports_the_lowered_n(tmp_path):
    t = _setup(tmp_path)
    ws, cw = tmp_path / "vws", tmp_path / "cws"
    (cw / "crops").mkdir(parents=True)
    ws.mkdir()
    (ws / "verify.json").write_text(json.dumps({"crops": [], "n": 0, "errors": []}))
    assert (await t.a.read_verify(str(ws), str(cw))).n_effective is None  # no report: the caller's n stands
    (cw / "crops" / "report.json").write_text(json.dumps({"requested_n": 16, "n": 3}))
    assert (await t.a.read_verify(str(ws), str(cw))).n_effective == 3
    assert (await t.a.read_verify(str(ws))).n_effective is None
