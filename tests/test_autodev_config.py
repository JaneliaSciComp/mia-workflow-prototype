from pathlib import Path

import pytest
import yaml
from autodev_helpers import make_cfg

from autodev.config import PROJECT_DIR, load_config

EXAMPLE = PROJECT_DIR / "mia.yaml.example"


def test_example_loads_and_derives_names(monkeypatch):
    monkeypatch.delenv("AUTODEV_DEPLOYMENT", raising=False)
    cfg = load_config(EXAMPLE, service="autodev")
    assert cfg.github.auth == "token"
    assert cfg.label("go") == "autodev:go"
    assert cfg.branch(12) == "autodev/issue-12"
    assert cfg.workflow_id(12) == "autodev/JaneliaSciComp/mia-workflow-prototype/issue-12"
    assert cfg.marker("handled review-3") == "<!-- autodev: handled review-3 -->"
    assert cfg.marker_prefix == "<!-- autodev:"
    assert cfg.git_url == "https://github.com/JaneliaSciComp/mia-workflow-prototype.git"
    assert cfg.repo_slug == "JaneliaSciComp__mia-workflow-prototype"


def test_deployment_override_isolates_a_personal_instance(monkeypatch):
    monkeypatch.setenv("AUTODEV_DEPLOYMENT", "autodev-kr")
    cfg = load_config(EXAMPLE, service="autodev")
    assert cfg.task_queue == "autodev-kr"
    assert cfg.label("go") == "autodev-kr:go"
    assert cfg.branch(3) == "autodev-kr/issue-3"


def test_app_auth_requires_key_settings(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"github": {"repo": "o/r", "auth": "app"}, "paths": {"work_root": str(tmp_path)}}))
    with pytest.raises(SystemExit, match="app_id"):
        load_config(p)


def test_auto_merge_needs_required_checks(tmp_path):
    with pytest.raises(ValueError, match="auto_merge needs github.required_checks"):
        make_cfg(tmp_path, github={"auto_merge": True})
    assert make_cfg(tmp_path, github={"auto_merge": True, "required_checks": ["mia-data/crops"]}).github.auto_merge


def test_deployment_name_is_restricted(tmp_path):
    with pytest.raises(ValueError, match="deployment"):
        make_cfg(tmp_path, deployment="Auto Dev")


@pytest.mark.parametrize(
    "agent",
    [
        {"allow_hosts": ["github.com"]},
        {"allow_hosts": [" api.github.com"]},
        {"allow_hosts": ["github.com "]},
        {"extra_mounts": ["tools:ro"]},
        {"extra_mounts": ["~/tools:ro"]},
        {"allow_hosts": ["api.github.com"]},
        {"allow_hosts": ["Raw.GithubUserContent.com."]},
        {"extra_mounts": ["/tools"]},
        {"extra_mounts": ["/tools:rw"]},
        {"extra_mounts": ["{state}:ro"]},
        {"extra_mounts": ["{state}/sub:ro"]},
        {"extra_mounts": ["{tmp}:ro"]},
        {"extra_mounts": ["{tmp}/tok:ro"]},
    ],
)
def test_agent_constraints_rejected(tmp_path, agent):
    sub = {"state": str(tmp_path / "state"), "tmp": str(tmp_path)}
    agent = {k: [v.format(**sub) for v in vs] for k, vs in agent.items()}
    with pytest.raises(ValueError):
        make_cfg(tmp_path, github={"token_file": str(tmp_path / "tok")}, agent=agent)


def test_agent_constraints_allow_safe_values(tmp_path):
    make_cfg(tmp_path, agent={"allow_hosts": ["pypi.org", "notgithub.com"], "extra_mounts": ["/tools:ro"]})


def test_paths_are_canonical(tmp_path):
    """csub rejects non-canonical mounts, and the round dir is a mount: resolve symlinks once, in config."""
    for name in ("work", "state", "tools"):
        (tmp_path / f"real-{name}").mkdir()
        (tmp_path / f"link-{name}").symlink_to(tmp_path / f"real-{name}")
    cfg = make_cfg(
        tmp_path,
        agent={"extra_mounts": [f"{tmp_path}/link-tools/../link-tools:ro"]},
        paths={"state_dir": str(tmp_path / "link-state"), "work_root": str(tmp_path / "link-work")},
    )
    assert cfg.paths.work_root == (tmp_path / "real-work").resolve()
    assert cfg.paths.state_dir == (tmp_path / "real-state").resolve()
    assert cfg.agent.extra_mounts == [f"{(tmp_path / 'real-tools').resolve()}:ro"]


def test_env_override_into_an_empty_section(tmp_path, monkeypatch):
    p = tmp_path / "c.yaml"
    p.write_text(f"github: {{repo: o/r}}\npaths: {{work_root: {tmp_path}}}\ntemporal:\n")
    monkeypatch.setenv("TEMPORAL_ADDRESS", "example:7233")
    assert load_config(p).temporal.address == "example:7233"


def test_trusted_bots_take_the_app_slug_and_add_the_bot_suffix(tmp_path):
    assert make_cfg(tmp_path, github={"trusted_bots": ["x[bot]", "y"]}).github.trusted_bots == ["x[bot]", "y[bot]"]
    with pytest.raises(ValueError, match="trusted_bots"):
        make_cfg(tmp_path, github={"trusted_bots": ["bad slug!"]})


def test_max_budget_must_be_positive(tmp_path):
    with pytest.raises(ValueError, match="max_budget_usd"):
        make_cfg(tmp_path, agent={"max_budget_usd": 0})


MIA_YAML = """
temporal: {address: h:7233}
github: {auth: token}
paths: {state_root: ~/mia-state, work_root: /w}
services:
  snippets: {app: autodev, github: {repo: o/snips}, agent: {model: sonnet}}
  mia-data: {app: mia-data, github: {data_repo: o/d}}
"""


def test_load_config_from_a_mia_yaml_service(tmp_path, monkeypatch):
    (tmp_path / "mia.yaml").write_text(MIA_YAML)
    monkeypatch.setenv("MIA_SERVICE", "snippets")
    monkeypatch.setenv("MIA_CONFIG", str(tmp_path / "mia.yaml"))
    monkeypatch.delenv("AUTODEV_DEPLOYMENT", raising=False)
    monkeypatch.setenv("TEMPORAL_ADDRESS", "env:1")
    cfg = load_config()
    assert cfg.deployment == "snippets" and cfg.github.repo == "o/snips" and cfg.temporal.address == "env:1"
    assert cfg.paths.state_dir == (Path("~/mia-state").expanduser() / "snippets").resolve()


def test_state_dir_from_state_root_is_expanded(tmp_path, monkeypatch):
    (tmp_path / "mia.yaml").write_text(MIA_YAML.replace("~/mia-state", "rel/state"))
    monkeypatch.chdir(tmp_path)
    cfg = load_config(tmp_path / "mia.yaml", service="snippets")
    assert cfg.paths.state_dir.is_absolute() and cfg.paths.state_dir.name == "snippets"
