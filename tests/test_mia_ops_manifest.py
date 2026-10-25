import pytest
from pydantic import ValidationError

from autodev.config import PROJECT_DIR
from mia_ops.manifest import Manifest, load_manifest

SERVICES = {
    "autodev": {"app": "autodev", "config": "autodev.yaml"},
    "mia-data": {"app": "mia-data", "config": "mia_data.yaml"},
}


@pytest.mark.xfail(reason="Task 5 teaches the manifest the new service blocks", strict=True)
def test_example_manifest_loads():
    m = load_manifest(PROJECT_DIR / "mia.yaml.example")
    assert list(m.services) == ["autodev", "snippets", "mia-data"] and m.temporal.managed is True
    assert m.services["snippets"].app == "autodev"


def test_omitted_temporal_section_means_external():
    assert Manifest(services=SERVICES).temporal.managed is False


def test_paths_resolve_against_the_repo():
    m = Manifest(services=SERVICES)
    assert m.services["autodev"].config == PROJECT_DIR / "autodev.yaml"
    assert m.sandbox_dir == (PROJECT_DIR / "../agentic-sandbox/scripts").resolve()
    assert m.temporal.state_dir.is_absolute() and "~" not in str(m.temporal.state_dir)


@pytest.mark.parametrize(
    "services, message",
    [
        ({}, "at least one"),
        ({"temporal": {"app": "autodev", "config": "a.yaml"}}, "implicit"),
        ({"Bad.Name": {"app": "autodev", "config": "a.yaml"}}, "Bad.Name"),
        ({"aa": {"app": "nope", "config": "a.yaml"}}, "nope"),
        ({"aa": {"app": "autodev", "config": "x.yaml"}, "bb": {"app": "autodev", "config": "x.yaml"}}, "same config"),
    ],
)
def test_rejects(services, message):
    with pytest.raises(ValidationError, match=message):
        Manifest(services=services)


def test_load_manifest_missing_file_exits(tmp_path):
    with pytest.raises(SystemExit, match="not found"):
        load_manifest(tmp_path / "mia.yaml")


def test_load_manifest_invalid_file_exits(tmp_path):
    (tmp_path / "mia.yaml").write_text("services: {}\n")
    with pytest.raises(SystemExit, match="at least one"):
        load_manifest(tmp_path / "mia.yaml")


def test_load_manifest_env_default(tmp_path, monkeypatch):
    p = tmp_path / "m.yaml"
    p.write_text("services:\n  autodev: {app: autodev, config: autodev.yaml}\n")
    monkeypatch.setenv("MIA_MANIFEST", str(p))
    assert list(load_manifest().services) == ["autodev"]
