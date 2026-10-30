import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import yaml

from mia_buffers import cli
from mia_buffers.cli import catalog_head, load_records, main

FIXTURES = Path(__file__).parent / "fixtures" / "buffers"
CATALOG = FIXTURES / "catalog"
BCSS = "github-bcss-breast-cancer-semantic-segmentation"


def write_spec(tmp_path: Path, **edits) -> Path:
    data = yaml.safe_load((FIXTURES / "spec.yaml").read_text())
    for dotted, value in edits.items():
        node = data
        *parents, leaf = dotted.split(".")
        for key in parents:
            node = node[key]
        node[leaf] = value
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def spec_2d(tmp_path: Path) -> Path:
    return write_spec(
        tmp_path,
        **{
            "select.where.dims": {"in": ["2D"]},
            "select.where.annotations.source": "any",
            "select.exclude.ids": ["synthetic-em-2d-1ch"],
        },
    )


def test_plan_prints_markdown_and_writes_json(tmp_path, capsys):
    out = tmp_path / "plan.json"
    code = main(
        ["plan", str(spec_2d(tmp_path)), "--catalog-dir", str(CATALOG), "--allow-ref-mismatch", "--json", str(out)]
    )
    assert code == 0 and f"| {BCSS} |" in capsys.readouterr().out
    assert json.loads(out.read_text())["catalog_ref"] is None


def test_load_records_is_sorted_and_relative():
    paths = [path for path, _ in load_records(CATALOG)]
    assert paths == sorted(paths) and paths[0].startswith("datasets/") and len(paths) == 6


def test_catalog_head_none_outside_git(tmp_path):
    assert catalog_head(tmp_path) is None


def test_ref_mismatch_refused(tmp_path, capsys):
    repo = tmp_path / "catalog"
    (repo / "datasets").mkdir(parents=True)
    (repo / "datasets" / "x.yaml").write_text("id: x\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "x"], check=True
    )
    head = catalog_head(repo)
    assert head is not None
    code = main(["plan", str(spec_2d(tmp_path)), "--catalog-dir", str(repo)])
    err = capsys.readouterr().err
    assert code == 1 and head in err and "ab6a013d5da39c80651ddc845915d034b2eae82a" in err


def test_invalid_spec_exit_1(tmp_path, capsys):
    path = write_spec(tmp_path, **{"sampling.weigthing": {}})
    assert main(["plan", str(path), "--catalog-dir", str(CATALOG), "--allow-ref-mismatch"]) == 1
    assert "weigthing" in capsys.readouterr().err


def test_plan_errors_exit_2(tmp_path, capsys):
    path = write_spec(tmp_path, **{"select.where.annotations.source": "any"})
    assert main(["plan", str(path), "--catalog-dir", str(CATALOG), "--allow-ref-mismatch"]) == 2
    assert "## Errors" in capsys.readouterr().out


def test_unreadable_yaml_becomes_a_record(tmp_path):
    (tmp_path / "datasets").mkdir()
    (tmp_path / "datasets" / "broken.yaml").write_text("id: [unclosed\n")
    (tmp_path / "datasets" / "scalar.yaml").write_text("42\n")
    loaded = dict(load_records(tmp_path))
    assert loaded["datasets/broken.yaml"]["id"] == "broken" and "_unreadable" in loaded["datasets/broken.yaml"]
    assert loaded["datasets/scalar.yaml"]["_unreadable"].startswith("not a mapping")


def test_help_lists_commands(capsys):
    import pytest

    with pytest.raises(SystemExit) as error:
        main(["--help"])
    assert error.value.code == 0
    out = capsys.readouterr().out
    for command in ("plan", "serve", "register", "worker", "schedule", "init-labels"):
        assert command in out


def test_init_labels_creates_buffer_labels(tmp_path, monkeypatch, capsys):
    from test_buffers_config import make_cfg

    from mia_buffers import cli

    created = []

    class FakeGitHub:
        def __init__(self, repo):
            self.repo = repo

        async def ensure_label(self, name, color, description=""):
            created.append((self.repo, name, color))
            return True

        async def aclose(self):
            pass

    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(cli, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(cli, "_gh", lambda cfg_, repo, tokens=None: FakeGitHub(repo))
    assert main(["init-labels"]) == 0
    names = [name for _, name, _ in created]
    assert len(names) == 18 and {"buffer:new", "buffer:ready", "buffer:go", "buffer:approve"} <= set(names)
    assert {"buffer:keep", "buffer:retire", "buffer:plan", "buffer:staged"} <= set(names)
    assert {"prep:go", "prep:requested", "prep:done", "prep:needs-human"} <= set(names) and "prep:new" not in names
    assert all(repo == "o/buffers" for repo, _, _ in created) and "created buffer:go" in capsys.readouterr().out


class _CheckGitHub:
    def __init__(self, repo):
        self.repo = repo

    async def request(self, method, path):
        if self.repo == "o/catalog":
            raise RuntimeError("404")
        return SimpleNamespace(json=lambda: {"full_name": self.repo})

    async def aclose(self):
        pass


async def _temporal_down(cfg):
    raise RuntimeError("down")


def test_check_reports_every_row_and_fails_on_the_bad_ones(tmp_path, monkeypatch, capsys):
    from test_buffers_config import make_cfg

    cfg = make_cfg(
        tmp_path,
        paths={"local_root": str(tmp_path / "local")},
        service={"url": "http://svc:1", "token_file": str(tmp_path / "tok")},
    )
    for d in ("state", "work", "raw", "staged", "storage"):
        (tmp_path / d).mkdir(exist_ok=True)
    (tmp_path / "tok").write_text("t")
    (tmp_path / "tok").chmod(0o600)
    tdir = cfg.harness.dir / "tensorizers" / cfg.sourcing.tensorizer
    tdir.mkdir(parents=True)
    (tdir / "tensorizer.yaml").write_text("name: x\n")
    python = cfg.harness.dir / "harness" / ".pixi" / "envs" / "default" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text('#!/bin/sh\n[ "$3" = fetch ] && [ "$4" = --help ]\n')  # a harness that has `fetch`
    python.chmod(0o755)

    async def login(cfg_):
        return "bot"

    monkeypatch.setattr(cli, "bot_login", login)  # no GitHub: the rows' own fakes answer
    monkeypatch.setattr(cli, "load_config", lambda path=None, service=None: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    monkeypatch.setattr(cli, "_gh", lambda cfg_, repo, tokens=None: _CheckGitHub(repo))
    monkeypatch.setattr(cli, "_client", _temporal_down)
    monkeypatch.setattr(cli, "harness_pin", lambda cfg_: ("a" * 40, "b" * 64))
    monkeypatch.setattr(cli, "require_local_dir", lambda p: p.mkdir(parents=True, exist_ok=True))
    monkeypatch.setattr(cli.httpx, "get", lambda url, timeout: SimpleNamespace(status_code=404))
    assert cli.main(["check"]) == 1
    rows = capsys.readouterr().out.rstrip("\n").split("\n")
    assert [r[:3] for r in rows] == ["ok ", "ok ", "ok ", "BAD", "BAD", "ok ", "ok ", "ok ", "ok ", "ok ", "ok "]
    assert rows[3].startswith("BAD  github catalog") and "down" in rows[4]
    assert rows[9].startswith("ok   harness fetch") and rows[10].startswith("ok   buffer-api")
    assert "http://svc:1" in rows[10]
    python.write_text("#!/bin/sh\necho 'invalid choice: fetch' >&2; exit 2\n")  # a checkout from before the fetch
    assert cli.main(["check"]) == 1
    rows = capsys.readouterr().out.rstrip("\n").split("\n")
    assert rows[9].startswith("BAD  harness fetch") and "exited 2: invalid choice: fetch" in rows[9]


def test_serve_takes_config_only(capsys):
    import pytest

    with pytest.raises(SystemExit):
        main(["serve", "--help"])
    out = capsys.readouterr().out
    assert "--config" in out and "--port" not in out


def _sync_cfg(tmp_path, monkeypatch):
    from test_buffers_config import make_cfg

    from mia_core.registry import Registry

    (tmp_path / "tok").write_text("t")
    (tmp_path / "tok").chmod(0o600)
    cfg = make_cfg(tmp_path, service={"url": "http://svc:1", "token_file": str(tmp_path / "tok")})
    monkeypatch.setattr(cli, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    return Registry


def test_catalog_sync_loads_the_snapshot_into_the_registry(tmp_path, monkeypatch, capsys):
    from mia_helpers import FakeGH as BaseGH

    class FakeGH(BaseGH):
        async def aclose(self):
            pass

    registry = _sync_cfg(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cli, "catalog_snapshot", lambda cfg_, ref, tokens: Path(__file__).parent / "fixtures/buffers/catalog"
    )
    monkeypatch.setattr(cli, "_gh", lambda cfg_, repo: FakeGH(repo, refs={"main": "c" * 40}))
    sent = []
    monkeypatch.setattr(registry, "put_records", lambda self, ref, records: sent.append((ref, len(records))) or {})
    assert cli.main(["catalog", "sync"]) == 0 and sent == [("c" * 40, 6)]
    assert cli.main(["catalog", "sync", "--ref", "d" * 40]) == 0 and sent[-1][0] == "d" * 40
    assert "synced" in capsys.readouterr().out


def test_datasets_sync_reports_counts_and_409(tmp_path, monkeypatch, capsys):
    from mia_core.registry import RegistryError

    registry = _sync_cfg(tmp_path, monkeypatch)
    monkeypatch.setattr(registry, "sync_datasets", lambda self: {"prepared": 2, "demoted": 0})
    assert cli.main(["datasets", "sync"]) == 0
    out = capsys.readouterr().out
    assert "2" in out and "prepared" in out and "demoted" in out

    def busy(self):
        raise RegistryError(409, "a datasets sync is running")

    monkeypatch.setattr(registry, "sync_datasets", busy)
    assert cli.main(["datasets", "sync"]) == 1
    assert "try again" in capsys.readouterr().err


def test_retire_prints_the_result_and_fails_on_a_registry_error(tmp_path, monkeypatch, capsys):
    from mia_core.registry import RegistryError

    registry = _sync_cfg(tmp_path, monkeypatch)
    calls, events = [], []
    monkeypatch.setattr(registry, "retire", lambda self, buffer: calls.append(buffer) or {"refs": 2, "draws": 5})
    monkeypatch.setattr(registry, "event", lambda self, buffer, kind, **fields: events.append((buffer, kind, fields)))
    assert cli.main(["retire", "i7-e11"]) == 0 and calls == ["i7-e11"]
    assert events == [("i7-e11", "retired", {"detail": "by hand; archive none"})]  # next/split now answer 410
    assert '"refs": 2' in capsys.readouterr().out

    def unknown(self, buffer):
        raise RegistryError(404, "unknown buffer zz")

    monkeypatch.setattr(registry, "retire", unknown)
    assert cli.main(["retire", "zz"]) == 1 and "unknown buffer zz" in capsys.readouterr().err
    assert len(events) == 1  # no event for a retire that failed


def test_schedule_applies_the_poll_and_the_keeper(tmp_path, monkeypatch, capsys):
    _sync_cfg(tmp_path, monkeypatch)
    applied = []

    async def fake_client(cfg_):
        return None

    async def fake_apply(client, schedule_id, schedule):
        applied.append((schedule_id, schedule.spec.intervals[0].every, schedule.policy.overlap.name))
        return "created"

    monkeypatch.setattr(cli, "_client", fake_client)
    monkeypatch.setattr(cli, "apply_schedule", fake_apply)
    assert cli.main(["schedule", "apply"]) == 0
    assert [(name, every.total_seconds(), overlap) for name, every, overlap in applied] == [
        ("mia-buffers-poll", 120.0, "SKIP"),
        ("mia-buffers-keeper", 1800.0, "SKIP"),
    ]
    assert capsys.readouterr().out.splitlines() == [
        "created schedule mia-buffers-poll",
        "created schedule mia-buffers-keeper",
    ]


def test_assets_sync_runs_one_keeper_pass(tmp_path, monkeypatch, capsys):
    from mia_buffers.activities import Activities
    from mia_buffers.models import KeeperReport

    class FakeGH:
        async def aclose(self):
            pass

    _sync_cfg(tmp_path, monkeypatch)
    passes = []

    async def keep(self, dry_run=False):
        passes.append(dry_run)
        return KeeperReport(scanned=3, evicted=["staged a"], errors=[] if dry_run else ["sync datasets: down"])

    async def login(cfg_):
        return "bot"

    monkeypatch.setattr(cli, "_github_clients", lambda cfg_: (FakeGH(), FakeGH(), FakeGH()))
    monkeypatch.setattr(cli, "_gh", lambda cfg_, repo: FakeGH())
    monkeypatch.setattr(cli, "bot_login", login)
    monkeypatch.setattr(Activities, "keep", keep)
    assert cli.main(["assets", "sync", "--dry-run"]) == 0  # a dry run anywhere
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "scanned: 3" and "evicted: ['staged a']" in out and "errors: []" in out
    assert cli.main(["assets", "sync"]) == 2 and passes == [True]  # no lock directory here: not the workers' host
    err = capsys.readouterr().err
    assert "refusing to evict on" in err and "`mia up`" in err and "local/locks does not exist" in err
    (tmp_path / "local" / "locks").mkdir(parents=True)
    assert cli.main(["assets", "sync"]) == 1 and passes == [True, False]  # a failed step is exit 1
