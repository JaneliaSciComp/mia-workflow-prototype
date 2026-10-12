import json
from types import SimpleNamespace

from mia_data import cli
from mia_data.cli import DATA_HOSTS, main
from mia_helpers import make_cfg


def _run(monkeypatch, tmp_path, argv, **cfg_over):
    cfg = make_cfg(tmp_path, **cfg_over)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    return cfg, main(argv)


def _storage(cfg, withdrawn=None):
    for ds, crops in {"ds1": [("a", "tiff-zip"), ("b", "other")], "ds2": [("c", "tiff-zip")]}.items():
        for cid, snip in crops:
            d = cfg.storage.root / ds / cid
            d.mkdir(parents=True)
            (d / "crop.json").write_text(json.dumps({"crop_id": cid, "snippets": [{"name": snip, "commit": "1"}]}))
        (cfg.storage.root / ds / "manifest.json").write_text(json.dumps({"crops": [
            {"crop_id": c, "snippets": [{"name": n}], "withdrawn": False} for c, n in crops]}))


def test_crops_by_snippet(monkeypatch, tmp_path, capsys):
    cfg = make_cfg(tmp_path)
    _storage(cfg)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["crops-by-snippet", "tiff-zip"]) == 0
    assert capsys.readouterr().out.split("\n")[:-1] == ["ds1 a false", "ds2 c false"]
    assert main(["crops-by-snippet", "nope"]) == 0 and capsys.readouterr().out == ""


def test_reindex_marks_withdrawn(monkeypatch, tmp_path, capsys):
    cfg = make_cfg(tmp_path)
    _storage(cfg)
    (cfg.harness.dir / "WITHDRAWN.yaml").write_text("- name: tiff-zip\n  reason: bad\n")
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["reindex", "ds2"]) == 0
    assert capsys.readouterr().out == "ds2: 1 crop(s)\n"
    assert json.loads((cfg.storage.root / "ds2/manifest.json").read_text())["crops"][0]["withdrawn"] is True
    assert json.loads((cfg.storage.root / "ds1/manifest.json").read_text())["crops"][0]["withdrawn"] is False
    assert main(["reindex"]) == 0
    assert capsys.readouterr().out == "ds1: 2 crop(s)\nds2: 1 crop(s)\n"


class _GH:
    def __init__(self, repo, *a, **k):
        self.repo = repo

    async def request(self, method, path):
        if self.repo == "o/snippets":
            raise RuntimeError("404")
        return SimpleNamespace(json=lambda: {"full_name": self.repo})

    async def aclose(self):
        pass


def _fake_env(monkeypatch, hosts, env_allow=("MIA_HARNESS_*",), roots=("/",)):
    async def client(cfg):
        return SimpleNamespace(workflow_service=SimpleNamespace(describe_namespace=_aw))

    async def _aw(req):
        return None

    async def login(cfg):
        return "bot"

    probe = SimpleNamespace(allow_claude=True, allowed_hosts=hosts, user="u", env_allow=list(env_allow),
                            allowed_roots=list(roots))
    monkeypatch.setattr(cli, "GitHub", _GH)
    monkeypatch.setattr(cli, "_client", client)
    monkeypatch.setattr(cli, "bot_login", login)
    monkeypatch.setattr("autodev.jobs.csub_client", lambda s: SimpleNamespace(probe=lambda: probe))
    monkeypatch.setattr("mia_data.jobspec.harness_pin", lambda cfg: ("c" * 40, "d" * 64))


def _harness_bin(cfg, mode=0o755):
    b = cfg.harness.dir / "harness" / ".pixi" / "envs" / "default" / "bin" / "harness"
    b.parent.mkdir(parents=True)
    b.write_text("#!/bin/sh\n")
    b.chmod(mode)


def test_check_reports_every_row_and_fails_on_one(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"])
    for d in ("state", "work", "storage"):
        (tmp_path / d).mkdir()
    cfg = make_cfg(tmp_path)
    _harness_bin(cfg)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    out = capsys.readouterr().out.split("\n")
    assert [l[:3] for l in out[:-1]] == ["ok ", "BAD", "ok ", "ok ", "ok ", "ok ", "ok "]
    assert "github snippets" in out[1] and "RuntimeError" in out[1]


def test_check_flags_unallowed_data_host(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, [h for h in DATA_HOSTS if h != "zenodo.org"] + ["pypi.org", "files.pythonhosted.org",
                                                                          "conda.anaconda.org", "repo.prefix.dev"])
    monkeypatch.setattr(_GH, "request", lambda self, m, p: _ok(self))
    for d in ("state", "work", "storage"):
        (tmp_path / d).mkdir()
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    assert "host not allowed: zenodo.org" in capsys.readouterr().out


async def _ok(gh):
    return SimpleNamespace(json=lambda: {"full_name": gh.repo})


def test_check_wants_an_installed_harness_env(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"])
    cfg = make_cfg(tmp_path)
    _harness_bin(cfg, 0o644)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    pin = next(l for l in capsys.readouterr().out.split("\n") if "harness pin" in l)
    assert pin.startswith("BAD") and "cd harness && pixi install" in pin


def test_check_flags_csub_env_roots_and_home(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"], env_allow=["MIA_HARNESS_COMMIT"], roots=[str(tmp_path / "work")])
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg = make_cfg(tmp_path)
    monkeypatch.setattr(cli, "load_config", lambda _: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    assert main(["check"]) == 1
    row = next(l for l in capsys.readouterr().out.split("\n") if "csub broker" in l)
    assert row.startswith("BAD") and "MIA_HARNESS_JOB_BYTES" in row and "MIA_HARNESS_COMMIT," not in row
    assert 'add "MIA_HARNESS_*" to env_allow' in row
    assert f'add "{cfg.harness.dir}", "{cfg.storage.root}" to allowed_roots' in row
    assert f"home directory {tmp_path.resolve()}" in row and str(cfg.harness.dir) in row.split("home directory")[1]
