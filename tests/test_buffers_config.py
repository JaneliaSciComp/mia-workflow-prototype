"""mia_buffers.config: mia_buffers.yaml, mia-data's sections plus the buffer roots, the service and sourcing knobs."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from mia_buffers.config import HUMAN_LABELS, STAGES, Config, load_config


def make_cfg(tmp_path: Path, **overrides) -> Config:
    harness = tmp_path / "harness"
    harness.mkdir(exist_ok=True)
    data = {
        "github": {"buffers_repo": "o/buffers", "snippets_repo": "o/snippets", "catalog_repo": "o/catalog"},
        "paths": {
            "state_dir": str(tmp_path / "state"),
            "work_root": str(tmp_path / "work"),
            "storage_root": str(tmp_path / "storage"),
            "raw_root": str(tmp_path / "raw"),
            "staged_root": str(tmp_path / "staged"),
        },
        "harness": {"dir": str(harness)},
        "service": {"url": "http://127.0.0.1:8765"},
    }
    for key, value in overrides.items():
        data[key] = {**data[key], **value} if isinstance(value, dict) and isinstance(data.get(key), dict) else value
    return Config(**data)


def test_labels_ids_and_markers(tmp_path):
    cfg = make_cfg(tmp_path)
    assert cfg.label("plan-review") == "buffer:plan-review"
    assert cfg.workflow_id(7) == "mia-buffers/o/buffers/issue-7"
    assert cfg.marker("x") == "<!-- mia-buffers: x -->"
    assert cfg.github.buffers_repo == "o/buffers" and cfg.github.data_repo == "o/buffers"
    assert (
        cfg.deployment == "mia-buffers" and cfg.task_queue == "mia-buffers" and cfg.await_queue == "mia-buffers-await"
    )
    assert cfg.temporal.namespace == "mia-buffers"
    assert set(STAGES) == {"new", "planning", "plan-review", "sourcing", "ready", "idle", "retired", "needs-human"}
    assert set(HUMAN_LABELS) == {"go", "approve"}


def test_roots_accept_path_and_url_form(tmp_path):
    cfg = make_cfg(tmp_path, paths={"raw_root": {"path": str(tmp_path / "raw"), "url": "https://b/raw"}})
    assert cfg.paths.raw_root == (tmp_path / "raw").resolve() and cfg.paths.raw_root_url == "https://b/raw"
    assert cfg.paths.staged_root == (tmp_path / "staged").resolve() and cfg.paths.staged_root_url is None
    assert cfg.paths.link(cfg.paths.raw_root / "r 1") == "https://b/raw/r%201"
    assert cfg.paths.link(cfg.paths.staged_root) is None
    assert cfg.sourcing.parallel == 4 and cfg.sourcing.tensorizer == "muvit-2d"
    assert (cfg.sourcing.val_size, cfg.sourcing.test_size) == (256, 256)
    assert cfg.service.url == "http://127.0.0.1:8765"


def test_data_repo_key_is_refused(tmp_path):
    with pytest.raises(ValidationError, match="buffers_repo"):  # only buffers_repo names the issue repo
        make_cfg(
            tmp_path, github={"data_repo": "o/x", "snippets_repo": "o/s", "catalog_repo": "o/c", "buffers_repo": None}
        )


@pytest.mark.parametrize("override", [{"sourcing": {"parallel": 0}}, {"sourcing": {"val_size": 0}}])
def test_bad_sourcing_values_are_refused(tmp_path, override):
    with pytest.raises(ValidationError):
        make_cfg(tmp_path, **override)


def test_load_config_reads_env_path(tmp_path, monkeypatch):
    (tmp_path / "harness").mkdir()
    path = tmp_path / "b.yaml"
    path.write_text(
        "github: {buffers_repo: o/b, snippets_repo: o/s, catalog_repo: o/c}\n"
        f"paths: {{state_dir: {tmp_path}/state, work_root: {tmp_path}/work, storage_root: {tmp_path}/store,"
        f" raw_root: {tmp_path}/raw, staged_root: {tmp_path}/staged}}\n"
        f"harness: {{dir: {tmp_path}/harness}}\nservice: {{url: http://svc:1}}\n"
    )
    monkeypatch.setenv("MIA_BUFFERS_CONFIG", str(path))
    monkeypatch.setenv("MIA_BUFFERS_TEMPORAL_VIA_PROXY", "true")
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    cfg = load_config()
    assert cfg.service.url == "http://svc:1" and cfg.temporal.via_https_proxy is True


def test_example_config_loads(monkeypatch):
    from autodev.config import PROJECT_DIR

    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    cfg = load_config(PROJECT_DIR / "mia_buffers.yaml.example")
    assert cfg.github.buffers_repo == "JaneliaSciComp/mia-buffers" and cfg.label("go") == "buffer:go"
