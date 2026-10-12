import json
from pathlib import Path

import yaml

from mia_data.cli import run_test_pr
from test_autodev_agent import FakeCsub
from test_mia_activities import BINDING, REC_SHA, SNIP_SHA, _crop, _setup

PR, H2 = 7, "b" * 40


class JobCsub(FakeCsub):
    """Runs nothing; writes what a crop or verify job would into its workspace, and logs the submission."""

    def __init__(self, log, verify_ok=True):
        super().__init__()
        self.log, self.verify_ok, self.files = log, verify_ok, None

    def submit(self, spec):
        ws, kind = Path(spec["cwd"]), spec["name"].rsplit("-", 1)[1]
        self.log.append(("job", kind))
        if kind == "crop":
            self.files = _crop(ws, "c1", 0)["files"]
        else:
            (ws / "preview" / "c1").mkdir(parents=True)
            (ws / "preview" / "c1" / "overlay.png").write_bytes(b"ov")
            (ws / "verify.json").write_text(json.dumps({"n": 1, "errors": [], "crops": [
                {"crop_id": "c1", "ok": self.verify_ok, "errors": [] if self.verify_ok else ["overlay empty"],
                 "files": self.files}]}))
        return super().submit(spec)


def _run(tmp_path, heads=(SNIP_SHA,), binding=BINDING, verify_ok=True):
    log = []
    t = _setup(tmp_path, csub=JobCsub(log, verify_ok))
    t.snip.prs, t.snip.posted = {PR: list(heads)}, log
    (b := tmp_path / "binding.yaml").write_text(yaml.safe_dump(binding))
    return t, log, b


def _events(log):
    return [e[1] if e[0] == "job" else (e[0], e[2]) for e in log]


async def test_test_pr_passes(tmp_path, capsys):
    t, log, b = _run(tmp_path)
    assert await run_test_pr(t.a, PR, b, "rec-1") == 0
    assert _events(log) == [(SNIP_SHA, "pending"), "crop", "verify", (SNIP_SHA, "success")]
    [c] = t.snip.comments[PR]
    assert log[-1][4] == c["html_url"] and f"<!-- mia-data: test-pr {SNIP_SHA} -->" in c["body"]
    assert "1 of 1 crops verified" in c["body"] and "?raw=true" in c["body"]
    pre = f"galleries/_test-pr/rec-1/{PR}/{SNIP_SHA}"
    assert sorted(t.data.puts) == [f"{pre}/{r}" for r in ("binding.yaml", "c1/overlay.png", "c1/raw.png", "verify.json")]
    crop = t.csub.submitted[0]
    assert f"--n 4 --seed test-pr-{PR}-{SNIP_SHA}" in crop["command"]
    assert crop["cwd"].startswith(str(t.cfg.paths.work_root / "test-pr" / str(PR) / SNIP_SHA))
    assert all(m["mode"] == "ro" for m in crop["mounts"] if "_cache" in m["path"])  # a PR head is not trusted
    assert "moved" not in capsys.readouterr().out
    c["body"] = f"stale <!-- mia-data: test-pr {SNIP_SHA} -->"
    assert await run_test_pr(t.a, PR, b, "rec-1") == 0 and len(t.csub.submitted) == 2  # a rerun reattaches
    assert len(t.snip.comments[PR]) == 1 and "1 of 1 crops verified" in c["body"]  # and edits the comment
    b.write_text(yaml.safe_dump({**BINDING, "reasons": ["other"]}))
    n = len(log)
    assert await run_test_pr(t.a, PR, b, "rec-1") == 1  # ... so another binding on this head is refused,
    assert len(log) == n and "another binding" in capsys.readouterr().out  # leaving the status alone


async def test_test_pr_stores_the_head_as_snippets_commit(tmp_path):
    t, log, b = _run(tmp_path, heads=(SNIP_SHA,), binding={**BINDING, "snippets_commit": "f" * 40})
    assert await run_test_pr(t.a, PR, b, "rec-1") == 0
    rd = t.cfg.paths.state_dir / "rounds" / f"i0-e{PR}-{SNIP_SHA[:7]}"
    assert yaml.safe_load((rd / "binding.yaml").read_text()) == BINDING  # BINDING's snippets_commit is the head
    assert await run_test_pr(t.a, PR, b, "rec-1") == 0  # the same file again is a rerun, not another binding


async def test_test_pr_verify_fails(tmp_path):
    t, log, b = _run(tmp_path, verify_ok=False)
    assert await run_test_pr(t.a, PR, b, "rec-1") == 1
    assert _events(log) == [(SNIP_SHA, "pending"), "crop", "verify", (SNIP_SHA, "failure")]
    assert "overlay empty" in t.snip.comments[PR][0]["body"] and t.data.puts == []


async def test_test_pr_head_moved(tmp_path, capsys):
    t, log, b = _run(tmp_path, heads=(SNIP_SHA, H2))
    assert await run_test_pr(t.a, PR, b, "rec-1") == 0
    assert {e[0] for e in log if e[0] != "job"} == {SNIP_SHA}
    assert f"moved to {H2}" in capsys.readouterr().out


async def test_test_pr_bad_binding_runs_nothing(tmp_path, capsys):
    t, log, b = _run(tmp_path, binding={**BINDING, "record_commit": "d" * 40})
    assert await run_test_pr(t.a, PR, b, "rec-1") == 1
    assert _events(log) == [(SNIP_SHA, "pending"), (SNIP_SHA, "failure")] and t.csub.submitted == []
    assert REC_SHA in capsys.readouterr().out
