import pytest
from mia_helpers import make_cfg
from pydantic import ValidationError

from mia_data.config import PROJECT_DIR, load_config


def test_example_config_loads(monkeypatch):
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    c = load_config(PROJECT_DIR / "mia_data.yaml.example")
    assert c.task_queue == "mia-data" and c.await_queue == "mia-data-await"
    assert c.label("go") == "data:go" and c.workflow_id(7) == "mia-data/JaneliaSciComp/mia-workflow-data/issue-7"
    assert c.marker("x") == "<!-- mia-data: x -->"


def test_defaults(tmp_path):
    c = make_cfg(tmp_path)
    assert c.crops.licences[0] == "CC0-1.0" and c.gallery.branch == "galleries"


@pytest.mark.parametrize(
    "override",
    [
        {"paths": {"state_dir": "{tmp}/work/s"}},
        {"network": {"package_hosts": ["api.github.com"]}},
        {"network": {"redirect_hosts": {"a.example": ["*.b.example"]}}},
        {"limits": {"max_pr_revisions": 0}},
        {"limits": {"max_stage_attempts": 2}},  # retries aren't implemented
        {"crops": {"licences": []}},
    ],
)
def test_rejected(tmp_path, override):
    override = {
        k: {a: (b.replace("{tmp}", str(tmp_path)) if isinstance(b, str) else b) for a, b in v.items()}
        for k, v in override.items()
    }
    with pytest.raises(ValidationError):
        make_cfg(tmp_path, **override)


@pytest.mark.parametrize(
    "key, value", [("allow_hosts", ["x.example.org"]), ("extra_mounts", ["/data:ro"]), ("api_key_env", "KEY")]
)
def test_rejects_autodev_only_agent_settings(tmp_path, key, value):
    with pytest.raises(ValidationError, match=f"agent.{key} is not used by mia-data"):
        make_cfg(tmp_path, agent={key: value})


def test_via_proxy_env_override(monkeypatch):
    monkeypatch.setenv("MIA_DATA_TEMPORAL_VIA_PROXY", "1")
    assert load_config(PROJECT_DIR / "mia_data.yaml.example").temporal.via_https_proxy is True


def test_roots_take_a_path_or_a_path_with_browse_url(tmp_path):
    c = make_cfg(
        tmp_path,
        paths={
            "work_root": {"path": str(tmp_path / "w"), "url": "https://fg.example/browse/w/"},
            "storage_root": str(tmp_path / "crops"),
        },
    )
    assert c.paths.work_root == (tmp_path / "w").resolve() and c.paths.storage_root == (tmp_path / "crops").resolve()
    assert c.paths.link(c.paths.work_root / "issue-5" / "round x") == "https://fg.example/browse/w/issue-5/round%20x"
    assert c.paths.link(c.paths.work_root) == "https://fg.example/browse/w"
    assert c.paths.link(c.paths.storage_root / "ds") is None  # no url for that root
    assert c.paths.link(tmp_path / "elsewhere") is None
    assert not hasattr(c, "storage")


def test_trusted_bots_must_be_app_logins(tmp_path):
    with pytest.raises(ValidationError, match="trusted_bots"):
        make_cfg(tmp_path, github={"trusted_bots": ["alice"]})
    assert make_cfg(tmp_path, github={"trusted_bots": ["mia-bot[bot]"]}).github.trusted_bots == ["mia-bot[bot]"]
