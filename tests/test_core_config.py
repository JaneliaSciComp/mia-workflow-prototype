import copy

import pytest
import yaml

from mia_core.config import MANIFEST_ONLY, load_service_config, service_config

SHARED = {
    "sandbox_dir": "../agentic-sandbox/scripts",
    "temporal": {"managed": True, "state_dir": "~/.local/state/temporal", "address": "h:7233", "poll_minutes": 3},
    "github": {"auth": "token", "catalog_repo": "o/c", "snippets_repo": "o/s"},
    "paths": {"state_root": "~/.local/state/mia", "work_root": "/w", "storage_root": "/s"},
    "limits": {"job_bytes": 5e9},
    "services": {
        "mia-data": {"app": "mia-data", "github": {"data_repo": "o/d"}, "agent": {"model": "sonnet"}},
        "snippets": {"app": "autodev", "github": {"repo": "o/snips"}},
    },
}


def test_merge_overlays_a_service_section_one_level_deep():
    cfg = service_config(SHARED, "mia-data")
    assert cfg["github"] == {"auth": "token", "catalog_repo": "o/c", "snippets_repo": "o/s", "data_repo": "o/d"}
    assert cfg["agent"] == {"model": "sonnet"} and cfg["limits"] == {"job_bytes": 5e9}
    assert cfg["deployment"] == "mia-data"
    assert not MANIFEST_ONLY & cfg.keys() and "managed" not in cfg["temporal"] and "state_dir" not in cfg["temporal"]
    assert cfg["temporal"]["address"] == "h:7233"


def test_state_dir_comes_from_state_root_and_the_service_name():
    cfg = service_config(SHARED, "snippets")
    assert cfg["paths"]["state_dir"] == "~/.local/state/mia/snippets" and "state_root" not in cfg["paths"]
    explicit = {**SHARED, "paths": {**SHARED["paths"], "state_dir": "/x"}}
    assert service_config(explicit, "snippets")["paths"]["state_dir"] == "/x"


def test_a_service_does_not_see_another_services_sections():
    assert "agent" not in service_config(SHARED, "snippets")


def test_service_block_keys_are_sections():
    bad = {**SHARED, "services": {"x": {"app": "autodev", "deployment": "y"}}}
    with pytest.raises(ValueError, match=r"services\.x\.deployment"):
        service_config(bad, "x")


@pytest.mark.parametrize("services, message", [({}, "no service 'x'"), ({"x": {"github": {}}}, "needs app")])
def test_missing_service_or_app(services, message):
    with pytest.raises(ValueError, match=message):
        service_config({**SHARED, "services": services}, "x")


@pytest.mark.parametrize(
    "override, message",
    [
        ({"github": "foo"}, "github: the shared section must be a mapping"),
        ({"paths": {**SHARED["paths"], "state_root": None}}, "paths.state_root must be a path string"),
    ],
)
def test_malformed_shared_sections_raise_value_error(override, message):
    with pytest.raises(ValueError, match=message):
        service_config({**SHARED, **override}, "mia-data")


def test_the_input_is_not_mutated():
    before = copy.deepcopy(SHARED)
    service_config(SHARED, "mia-data")
    assert SHARED == before


def test_load_service_config_reads_mia_config_env(tmp_path, monkeypatch):
    (tmp_path / "mia.yaml").write_text(yaml.safe_dump(SHARED))
    monkeypatch.setenv("MIA_CONFIG", str(tmp_path / "mia.yaml"))
    assert load_service_config("mia-data")["deployment"] == "mia-data"
    with pytest.raises(SystemExit, match="no service 'nope'"):
        load_service_config("nope")
    with pytest.raises(SystemExit, match="not found"):
        load_service_config("mia-data", tmp_path / "missing.yaml")


@pytest.mark.parametrize(
    "service, module, attr, expected",
    [
        ("autodev", "autodev.config", "github.repo", "JaneliaSciComp/mia-workflow-prototype"),
        ("snippets", "autodev.config", "github.repo", "JaneliaSciComp/mia-snippets"),
        ("mia-data", "mia_data.config", "github.data_repo", "JaneliaSciComp/mia-workflow-data"),
        ("mia-buffers", "mia_buffers.config", "github.buffers_repo", "JaneliaSciComp/mia-buffers"),
    ],
)
def test_example_mia_yaml_loads_every_service(service, module, attr, expected, monkeypatch):
    import importlib

    from mia_core.config import PROJECT_DIR

    for name in ("TEMPORAL_ADDRESS", "AUTODEV_DEPLOYMENT", "MIA_SERVICE"):
        monkeypatch.delenv(name, raising=False)
    cfg = importlib.import_module(module).load_config(PROJECT_DIR / "mia.yaml.example", service=service)
    section, field = attr.split(".")
    assert getattr(getattr(cfg, section), field) == expected
    assert cfg.deployment == service and cfg.paths.state_dir.name == service
    labels = {"autodev": "autodev:go", "snippets": "snippets:go", "mia-data": "data:go", "mia-buffers": "buffer:go"}
    assert cfg.label("go") == labels[service]


def test_service_token_file_is_absolute(tmp_path):
    from tests.mia_helpers import make_cfg

    cfg = make_cfg(tmp_path, service={"url": "http://svc.example.org", "token_file": "~/x"})
    assert cfg.service.token_file.is_absolute()
