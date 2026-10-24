import json
import subprocess
from pathlib import Path

import yaml

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
    assert len(names) == 10 and {"buffer:new", "buffer:ready", "buffer:go", "buffer:approve"} <= set(names)
    assert all(repo == "o/buffers" for repo, _, _ in created) and "created buffer:go" in capsys.readouterr().out
