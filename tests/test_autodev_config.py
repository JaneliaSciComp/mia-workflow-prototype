import pytest
import yaml

from autodev.config import PROJECT_DIR, load_config

from autodev_helpers import make_cfg

EXAMPLE = PROJECT_DIR / "autodev.yaml.example"


def test_example_loads_and_derives_names(monkeypatch):
    monkeypatch.delenv("AUTODEV_DEPLOYMENT", raising=False)
    cfg = load_config(EXAMPLE)
    assert cfg.github.auth == "token"
    assert cfg.label("go") == "autodev:go"
    assert cfg.branch(12) == "autodev/issue-12"
    assert cfg.workflow_id(12) == "autodev/krokicki/mia-workflow-prototype/issue-12"
    assert cfg.marker("handled review-3") == "<!-- autodev: handled review-3 -->"
    assert cfg.marker_prefix == "<!-- autodev:"
    assert cfg.git_url == "https://github.com/krokicki/mia-workflow-prototype.git"
    assert cfg.repo_slug == "krokicki__mia-workflow-prototype"


def test_deployment_override_isolates_a_personal_instance(monkeypatch):
    monkeypatch.setenv("AUTODEV_DEPLOYMENT", "autodev-kr")
    cfg = load_config(EXAMPLE)
    assert cfg.task_queue == "autodev-kr"
    assert cfg.label("go") == "autodev-kr:go"
    assert cfg.branch(3) == "autodev-kr/issue-3"


def test_app_auth_requires_key_settings(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump({"github": {"repo": "o/r", "auth": "app"}, "paths": {"work_root": str(tmp_path)}}))
    with pytest.raises(SystemExit, match="app_id"):
        load_config(p)


def test_deployment_name_is_restricted(tmp_path):
    with pytest.raises(ValueError, match="deployment"):
        make_cfg(tmp_path, deployment="Auto Dev")


@pytest.mark.parametrize("agent", [
    {"allow_hosts": ["github.com"]}, {"allow_hosts": ["api.github.com"]},
    {"allow_hosts": ["Raw.GithubUserContent.com."]}, {"extra_mounts": ["/tools"]},
    {"extra_mounts": ["/tools:rw"]}, {"extra_mounts": ["{state}:ro"]}, {"extra_mounts": ["{state}/sub:ro"]},
    {"extra_mounts": ["{tmp}:ro"]}, {"extra_mounts": ["{tmp}/tok:ro"]},
])
def test_agent_constraints_rejected(tmp_path, agent):
    sub = {"state": str(tmp_path / "state"), "tmp": str(tmp_path)}
    agent = {k: [v.format(**sub) for v in vs] for k, vs in agent.items()}
    with pytest.raises(ValueError):
        make_cfg(tmp_path, github={"token_file": str(tmp_path / "tok")}, agent=agent)


def test_agent_constraints_allow_safe_values(tmp_path):
    make_cfg(tmp_path, agent={"allow_hosts": ["pypi.org", "notgithub.com"], "extra_mounts": ["/tools:ro"]})
