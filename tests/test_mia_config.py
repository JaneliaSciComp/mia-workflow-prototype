import pytest
from mia_helpers import make_cfg
from pydantic import ValidationError

from mia_data.config import PROJECT_DIR, load_config


def test_defaults(tmp_path):
    c = make_cfg(tmp_path)
    assert c.crops.size == 256 and c.gallery.branch == "galleries"


@pytest.mark.parametrize(
    "override",
    [
        {"paths": {"state_dir": "{tmp}/work/s"}},
        {"network": {"package_hosts": ["api.github.com"]}},
        {"network": {"redirect_hosts": {"a.example": ["*.b.example"]}}},
        {"limits": {"max_pr_revisions": 0}},
        {"limits": {"max_stage_attempts": 2}},  # retries aren't implemented
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
    assert load_config(PROJECT_DIR / "mia.yaml.example", service="mia-data").temporal.via_https_proxy is True


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


def test_trusted_bots_take_the_app_slug_and_add_the_bot_suffix(tmp_path):
    cfg = make_cfg(tmp_path, github={"trusted_bots": ["mia-bot", "other[bot]"]})
    assert cfg.github.trusted_bots == ["mia-bot[bot]", "other[bot]"]
    with pytest.raises(ValidationError, match="trusted_bots"):
        make_cfg(tmp_path, github={"trusted_bots": ["bad slug!"]})


def test_load_config_by_service_name(tmp_path, monkeypatch):
    (tmp_path / "harness").mkdir()
    (tmp_path / "mia.yaml").write_text(
        "github: {auth: token, catalog_repo: o/c, snippets_repo: o/s}\n"
        f"paths: {{state_root: {tmp_path}/state, work_root: {tmp_path}/w, storage_root: {tmp_path}/s, local_root: {tmp_path}/local}}\n"
        f"harness: {{dir: {tmp_path}/harness}}\nservice: {{url: http://svc:1, token_file: {tmp_path}/tok}}\n"
        "services:\n  mia-data: {app: mia-data, github: {data_repo: o/d}}\n"
    )
    cfg = load_config(tmp_path / "mia.yaml", service="mia-data")
    assert cfg.github.data_repo == "o/d" and cfg.paths.state_dir == tmp_path / "state/mia-data"
    assert cfg.paths.local_root == tmp_path / "local" and cfg.service.token_file == tmp_path / "tok"


@pytest.mark.parametrize("block, namespace", [("", "mia-data"), (", temporal: {namespace: ns}", "ns")])
def test_namespace_defaults_to_the_app_under_a_shared_temporal_section(tmp_path, monkeypatch, block, namespace):
    (tmp_path / "harness").mkdir()
    (tmp_path / "mia.yaml").write_text(
        "github: {auth: token, catalog_repo: o/c, snippets_repo: o/s}\ntemporal: {address: t:7233}\n"
        f"paths: {{state_root: {tmp_path}/state, work_root: {tmp_path}/w, storage_root: {tmp_path}/s}}\n"
        f"harness: {{dir: {tmp_path}/harness}}\n"
        f"services:\n  mia-data: {{app: mia-data, github: {{data_repo: o/d}}{block}}}\n"
    )
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    cfg = load_config(tmp_path / "mia.yaml", service="mia-data")
    assert cfg.temporal.namespace == namespace and cfg.temporal.address == "t:7233"


def test_local_root_and_service_are_optional(tmp_path):
    cfg = make_cfg(tmp_path, paths={"local_root": None})
    assert cfg.paths.local_root is None and cfg.service is None


def test_shared_agent_hosts_still_refused_by_mia_data(tmp_path):
    with pytest.raises(ValidationError, match="agent.allow_hosts is not used by mia-data"):
        make_cfg(tmp_path, agent={"allow_hosts": ["pypi.org"]})


def test_evaluate_is_off_by_default(tmp_path):
    assert make_cfg(tmp_path).evaluate.enabled is False
