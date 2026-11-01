# Prep and buffering, milestone A: `mia_core`, one `mia.yaml`, host-local root and worker token — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every deployment loads its configuration from one `mia.yaml` through a new `mia_core` package, `mia up`
knows the host-local root and the worker token the later milestones use, and the helpers both applications share
have one public home.

**Architecture:** `mia_core/` is a leaf package (imports no application package) holding the shared config
models, the `mia.yaml` loader that merges shared sections with a service's block, the filesystem checks, and the
untrusted-output helpers. Each application's pydantic `Config` is unchanged in shape; only its `load_config`
learns to take a service name. `mia_ops` reads the same file, builds each service's config from it, and mounts
the lock directory and the token file into the workers. The old per-app files keep loading for one release.

**Tech Stack:** Python 3.12, pydantic 2, PyYAML, stdlib (`os`, `stat`, `secrets`); pytest, ruff, mypy via pixi.

**Spec:** `notes/specs/2026-10-25-prep-and-buffering.md` v0.2, §1.7, §1.11, §1.12, §8, §9, §10 row A.
Deviation from §10 row A: `mia_core/registry.py` and `mia_core/locks.py` are created by milestones B and C,
where their first callers are; this milestone creates `config.py`, `fs.py` and `untrusted.py`.

## Global Constraints

- Work on branch `training-buffers`, one commit per task, no PR (the user's convention for this work).
- `pixi run fmt`, `pixi run lint`, `pixi run typecheck`, `pixi run test` clean after every task; no new
  `type: ignore` or `noqa` without a reason beside it. `pixi run` works in this checkout again.
- Line length 120; descriptive names; a one-line docstring on every new module; `# ponytail:` on deliberate
  shortcuts.
- No real paths or hostnames in tracked files: `git diff | grep -n 'rokickik\|/nrs/'` is empty before each commit.
- `models.py` dataclasses are wire format; this milestone changes none of them.
- Import direction: `mia_core` imports nothing from `autodev`, `mia_data`, `mia_buffers`, `mia_ops`; everything
  may import `mia_core`; `mia_data → autodev`, `mia_buffers → mia_data, autodev`, never the reverse.
- Configuration values from the spec: `MIA_SERVICE` and `MIA_CONFIG` environment variables; `paths.state_root`
  gives `paths.state_dir = <state_root>/<service name>`; `paths.local_root` must be on a local filesystem;
  `service.token_file` must be a regular file with mode `0600`; `deployment` = the service name.

## Review Focus

1. A scalar key inside a service block (`services.mia-data.deployment: x`) must fail with a message naming the
   key, not land silently or crash in pydantic (Task 1, `test_service_block_keys_are_sections`).
2. A shared `agent` section carrying autodev's `allow_hosts` must still be refused by mia-data's own config
   check, so the example keeps `agent` per service (Task 2, `test_shared_agent_hosts_still_refused_by_mia_data`).
3. A `local_root` under an automount that is not mounted yet must be classified from the real mount, not from
   `autofs` at the top (Task 4, `test_mount_type_stats_the_path_first` and the `autofs` case).
4. A token file that is group- or world-readable must be refused by `mia up` and by `check` (Task 4,
   `test_require_secret_file_rejects_loose_modes`; Task 5, `test_up_refuses_a_loose_token_file`).
5. `state_root` written with `~` or relative must still yield an absolute `state_dir` per service (Task 2,
   `test_state_dir_from_state_root_is_expanded`).

---

### Task 1: `mia_core/config.py`: shared models and the `mia.yaml` merge

**Files:**
- Create: `mia_core/__init__.py`, `mia_core/config.py`
- Modify: `pyproject.toml` (`packages`, `typecheck` task), `autodev/config.py` (import `PROJECT_DIR`,
  `TemporalCfg`, `load_dotenv` from `mia_core.config` and re-export them)
- Test: `tests/test_core_config.py`

**Interfaces:**
- Produces:
  - `mia_core.config.PROJECT_DIR: Path` (moved from `autodev.config`, same value)
  - `mia_core.config.TemporalCfg` (moved, same fields: `address`, `namespace`, `via_https_proxy`, `poll_minutes`)
  - `mia_core.config.ServiceCfg(BaseModel)`: `url: str`, `token_file: Path | None = None` (moved from
    `mia_buffers.config`, with the new field)
  - `mia_core.config.load_dotenv(path: Path = PROJECT_DIR / ".env") -> None` (moved)
  - `mia_core.config.manifest_path(path: str | os.PathLike | None = None) -> Path`: `path`, else `$MIA_CONFIG`,
    else `PROJECT_DIR / "mia.yaml"`
  - `mia_core.config.read_manifest(path: Path) -> dict`: `yaml.safe_load`; `sys.exit` with a one-line message
    when the file is missing, not YAML, or not a mapping (`prog` is `mia`)
  - `mia_core.config.service_config(manifest: dict, name: str) -> dict`: the merge (below); raises `ValueError`
  - `mia_core.config.load_service_config(name: str, path: str | os.PathLike | None = None) -> dict`:
    `service_config(read_manifest(manifest_path(path)), name)`, turning `ValueError` into `sys.exit`
  - `mia_core.config.MANIFEST_ONLY = frozenset({"sandbox_dir", "services"})` and
    `TEMPORAL_MANIFEST_ONLY = frozenset({"managed", "state_dir"})`

The merge, `service_config(manifest, name)`:
1. `services = manifest.get("services")`; not a mapping or `name` missing → `ValueError(f"no service {name!r} in
   mia.yaml; services: {sorted(services or [])}")`.
2. `block = services[name]`; not a mapping, or without a string `app` → `ValueError(f"services.{name}: needs
   app: autodev | mia-data | mia-buffers")`.
3. `result = copy.deepcopy({k: v for k, v in manifest.items() if k not in MANIFEST_ONLY})`; if
   `result.get("temporal")` is a mapping, drop `TEMPORAL_MANIFEST_ONLY` keys from it.
4. For each `key, value` in `block` except `app` and `config` (legacy, Task 5): `value` not a mapping →
   `ValueError(f"services.{name}.{key}: a service block holds sections; {key!r} is not a mapping")`; else
   `result[key] = {**(result.get(key) or {}), **value}` (one level deep, by design).
5. `result["deployment"] = name`.
6. `paths = result.get("paths")`; if a mapping with `state_root`: pop it and, when `state_dir` is absent, set
   `paths["state_dir"] = str(Path(state_root) / name)` (no expansion here; the app's config expands).
7. Return `result`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_core_config.py
import pytest
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

def test_the_input_is_not_mutated():
    import copy
    before = copy.deepcopy(SHARED)
    service_config(SHARED, "mia-data")
    assert SHARED == before

def test_load_service_config_reads_mia_config_env(tmp_path, monkeypatch):
    import yaml
    (tmp_path / "mia.yaml").write_text(yaml.safe_dump(SHARED))
    monkeypatch.setenv("MIA_CONFIG", str(tmp_path / "mia.yaml"))
    assert load_service_config("mia-data")["deployment"] == "mia-data"
    with pytest.raises(SystemExit, match="no service 'nope'"):
        load_service_config("nope")
    with pytest.raises(SystemExit, match="not found"):
        load_service_config("mia-data", tmp_path / "missing.yaml")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_core_config.py -q`
Expected: `ModuleNotFoundError: No module named 'mia_core'`

- [ ] **Step 3: Create the package and move the shared pieces**

`mia_core/__init__.py`: one docstring line ("What every MIA application shares: configuration, filesystem
checks, untrusted-output helpers. Imports no application package."). `mia_core/config.py` with the interfaces
above. In `autodev/config.py` replace the definitions of `PROJECT_DIR`, `TemporalCfg`, `load_dotenv` with
`from mia_core.config import PROJECT_DIR as PROJECT_DIR, TemporalCfg as TemporalCfg, load_dotenv as load_dotenv`
(the `as` form is ruff's explicit re-export; every current importer keeps working). In `pyproject.toml`: add
`"mia_core"` to `[tool.hatch.build.targets.wheel] packages` and to the `typecheck` task.

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run pytest tests/test_core_config.py tests/test_autodev_config.py -q && pixi run lint && pixi run typecheck`
Expected: all pass; mypy covers `mia_core`.

- [ ] **Step 5: Commit**

```bash
git add mia_core pyproject.toml autodev/config.py tests/test_core_config.py
git commit -m "mia_core: shared config models and the mia.yaml service merge"
```

---

### Task 2: Application loaders take a service name

**Files:**
- Modify: `autodev/config.py` (`load_config`), `mia_data/config.py` (`load_config`, `PathsCfg`, `Config`,
  `_resolve_paths`), `mia_buffers/config.py` (`ServiceCfg` import, `load_config` passes `service` through)
- Test: `tests/test_autodev_config.py`, `tests/test_mia_config.py`, `tests/test_buffers_config.py`

**Interfaces:**
- Consumes: Task 1's `load_service_config`, `ServiceCfg`, `manifest_path`.
- Produces:
  - `autodev.config.load_config(path=None, *, service: str | None = None) -> Config`
  - `mia_data.config.load_config(path=None, *, cls=Config, env_var=..., default_name=..., prog=...,
    overrides=..., service: str | None = None) -> Config`
  - `mia_buffers.config.load_config(path=None, *, service: str | None = None) -> Config`
  - In all three: `service = service or os.environ.get("MIA_SERVICE")`; when set, `data =
    load_service_config(service, path)` and the config "source" named in error messages is
    `manifest_path(path)` plus `(service <name>)`; when unset, the existing per-app file path logic runs
    unchanged. Environment overrides (`TEMPORAL_ADDRESS`, `AUTODEV_DEPLOYMENT`, `<APP>_TEMPORAL_VIA_PROXY`) apply
    after the merge in both cases.
  - `mia_data.config.PathsCfg.local_root: Path | None = None` (expanded and resolved in `_resolve_paths`;
    `local_root` is **not** in `ROOTS`, it takes no `url`)
  - `mia_data.config.Config.service: ServiceCfg | None = None`; `mia_buffers.config.Config.service: ServiceCfg`
    (required, as today); `_resolve_paths` also expands `service.token_file` when present.
  - `mia_buffers.config.ServiceCfg` is removed; the module imports it from `mia_core.config`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_autodev_config.py`:

```python
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
```

Add to `tests/test_mia_config.py`:

```python
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

def test_local_root_and_service_are_optional(tmp_path):
    cfg = make_cfg(tmp_path)
    assert cfg.paths.local_root is None and cfg.service is None

def test_shared_agent_hosts_still_refused_by_mia_data(tmp_path):
    with pytest.raises(ValidationError, match="agent.allow_hosts is not used by mia-data"):
        make_cfg(tmp_path, agent={"allow_hosts": ["pypi.org"]})
```

Add to `tests/test_buffers_config.py`:

```python
def test_load_config_by_service_name(tmp_path, monkeypatch):
    (tmp_path / "harness").mkdir()
    (tmp_path / "mia.yaml").write_text(
        "github: {auth: token, catalog_repo: o/c, snippets_repo: o/s}\n"
        f"paths: {{state_root: {tmp_path}/state, work_root: {tmp_path}/w, storage_root: {tmp_path}/s,"
        f" staged_root: {tmp_path}/staged, local_root: {tmp_path}/local}}\n"
        f"harness: {{dir: {tmp_path}/harness}}\nservice: {{url: http://svc:1, token_file: {tmp_path}/tok}}\n"
        "services:\n  mia-buffers: {app: mia-buffers, github: {buffers_repo: o/b, datasets_repo: o/d}}\n"
    )
    monkeypatch.setenv("MIA_SERVICE", "mia-buffers")
    monkeypatch.delenv("TEMPORAL_ADDRESS", raising=False)
    cfg = load_config(tmp_path / "mia.yaml")
    assert cfg.github.buffers_repo == "o/b" and cfg.deployment == "mia-buffers"
    assert cfg.paths.raw_root == tmp_path / "w/_cache" and cfg.service.token_file == tmp_path / "tok"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_autodev_config.py tests/test_mia_config.py tests/test_buffers_config.py -q`
Expected: the new tests fail (`TypeError: unexpected keyword 'service'`, `AttributeError: local_root`, and a
`ValidationError` on `token_file` for `ServiceCfg`).

- [ ] **Step 3: Implement the three loaders and the two new fields**

In `mia_data.config.load_config`, the branch is the only new logic:

```python
service = service or os.environ.get("MIA_SERVICE")
if service:
    source = manifest_path(path or os.environ.get("MIA_CONFIG"))
    data = load_service_config(service, source)
else:
    ...existing path lookup and read...
```

Keep the `sys.exit(f"{prog}: invalid config {source}...")` message; add ` (service {service})` when set. The
`mia_buffers.config.load_config` wrapper forwards `service=`. `autodev.config.load_config` gets the same branch
(its `prog` is `autodev`).

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run test && pixi run lint && pixi run typecheck`
Expected: all pass (the existing per-file loading tests are unchanged and still pass).

- [ ] **Step 5: Commit**

```bash
git add autodev/config.py mia_data/config.py mia_buffers/config.py tests/test_autodev_config.py tests/test_mia_config.py tests/test_buffers_config.py
git commit -m "config: every application loads its service block from mia.yaml when MIA_SERVICE is set; paths.local_root and service.token_file"
```

---

### Task 3: One example file

**Files:**
- Create: `mia.yaml.example` (replace the current content)
- Delete: `autodev.yaml.example`, `mia_data.yaml.example`, `mia_buffers.yaml.example`
- Modify: `.gitignore` (drop the `!autodev.yaml.example` line), `tests/test_autodev_config.py`
  (`test_example_loads_and_derives_names`, `test_deployment_override_isolates_a_personal_instance`),
  `tests/test_mia_config.py::test_example_config_loads`, `tests/test_buffers_config.py::test_example_config_loads`,
  `tests/test_mia_ops_manifest.py::test_example_manifest_loads` (moves to Task 5's shape; here it only needs to
  keep passing: see Step 3)

**Interfaces:**
- Consumes: Task 2's `load_config(path, service=...)`.
- Produces: the file every later task and document refers to.

- [ ] **Step 1: Write the failing test**

Replace the three per-app "example loads" tests with one parametrized test in `tests/test_core_config.py`:

```python
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
    assert cfg.label("go") == {"autodev": "autodev:go", "snippets": "snippets:go", "mia-data": "data:go", "mia-buffers": "buffer:go"}[service]
```

In `tests/test_autodev_config.py` the two tests that load `EXAMPLE` switch to
`load_config(PROJECT_DIR / "mia.yaml.example", service="autodev")`; keep their other assertions. Delete
`test_example_config_loads` from `tests/test_mia_config.py` and `tests/test_buffers_config.py`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `pixi run pytest tests/test_core_config.py -q -k example`
Expected: FAIL, `no service 'autodev' in mia.yaml` (the current example has `services:` with `config:` entries,
which the merge ignores, so the shared sections are missing).

- [ ] **Step 3: Write `mia.yaml.example`**

The spec's §8 example, completed with every key the four configs need and the comments the three old files
carried that still apply. Placeholders only (`/shared/TBD/...`, `https://fileglancer.example.org/...`,
`host.example.org`). Sections: `sandbox_dir`, `temporal` (`managed`, `state_dir`, `address`, `namespace` is per
service, `poll_minutes`), `github` (`auth: token`, `token_env`, `token_file: null`, `app_id`/`installation_id`/
`private_key_path: null`, `catalog_repo`, `snippets_repo`; `trusted_bots` stays under `services.mia-data.github`
with a comment saying why it is not shared), `paths` (`state_root`, `local_root: /scratch/mia  # host-local`,
`work_root`, `storage_root`, `raw_root` commented as defaulting to `<work_root>/_cache`, `staged_root`),
`harness`, `limits`, `network`, `service` (`url`, `token_file`), and `services` with the four blocks of §8 where
`agent`, `crops`, `gallery`, `sourcing`, `protected_paths`, `temporal.namespace` and the repo keys live.
`temporal.namespace` per service: `autodev: default`, `snippets: default`, `mia-data: mia-data`, `mia-buffers:
mia-buffers`. Until Task 5 lands, `tests/test_mia_ops_manifest.py::test_example_manifest_loads` must keep
passing: `Manifest` today requires `config` per service, so this task marks that one test
`@pytest.mark.xfail(reason="Task 5 teaches the manifest the new service blocks", strict=True)`.

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run test && pixi run lint`
Expected: all pass (one xfail).

- [ ] **Step 5: Commit**

```bash
git rm autodev.yaml.example mia_data.yaml.example mia_buffers.yaml.example
git add mia.yaml.example .gitignore tests/test_core_config.py tests/test_autodev_config.py tests/test_mia_config.py tests/test_buffers_config.py tests/test_mia_ops_manifest.py
git commit -m "config: mia.yaml.example is the one example file"
```

---

### Task 4: `mia_core/fs.py`: local-filesystem and secret-file checks

**Files:**
- Create: `mia_core/fs.py`
- Test: `tests/test_core_fs.py`

**Interfaces:**
- Produces:
  - `mia_core.fs.NETWORK_FS = frozenset({"nfs", "nfs4", "cifs", "smb3", "smbfs", "lustre", "gpfs", "beegfs",
    "ceph", "glusterfs", "fuse.sshfs", "autofs"})`
  - `mia_core.fs.mount_type(path: Path, mounts: str | None = None) -> str`: the filesystem type of the mount that
    holds `path`. `path.resolve()` first, then `os.stat` it when it exists (an automount is triggered by the
    stat), then read `mounts` (the text of `/proc/self/mounts` when `None`) and take the entry whose mount
    point is the longest prefix of the path (`/` matches everything). Mount points in that file have spaces
    escaped as `\040`; unescape them.
  - `mia_core.fs.is_network_fs(path: Path, mounts: str | None = None) -> bool`
  - `mia_core.fs.require_local_dir(path: Path) -> None`: `mkdir(parents=True, exist_ok=True)`, then raise
    `RuntimeError(f"{path} is on {fstype}, a network filesystem; it must be host-local disk")` when
    `is_network_fs`.
  - `mia_core.fs.require_secret_file(path: Path) -> None`: raise `RuntimeError` naming the path when it is
    missing, not a regular file, a symlink, or `stat.S_IMODE(mode) != 0o600` (message: `"... must be a regular
    file with mode 600 (chmod 600)"`).
  - Modules that call these import the names (`from mia_core.fs import require_local_dir, require_secret_file`)
    rather than the module, so Tasks 5 and 6 can replace them per test.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_core_fs.py
import os
import pytest
from pathlib import Path
from mia_core import fs

MOUNTS = (
    "/dev/root / xfs rw 0 0\n"
    "auto /groups autofs rw 0 0\n"
    "auto /netfs autofs rw 0 0\n"
    "host:/vol /netfs/lab nfs rw 0 0\n"
    "/dev/sdb /mnt/my\\040disk ext4 rw 0 0\n"
)

@pytest.mark.parametrize(
    "path, fstype",
    [("/scratch/mia", "xfs"), ("/netfs/lab/mia", "nfs"), ("/netfs/other", "autofs"), ("/groups/x/y", "autofs"), ("/mnt/my disk/a", "ext4")],
)
def test_mount_type_takes_the_longest_mount_point(path, fstype):
    assert fs.mount_type(Path(path), MOUNTS) == fstype

@pytest.mark.parametrize("path, network", [("/scratch/mia", False), ("/netfs/lab/mia", True), ("/groups/x", True)])
def test_is_network_fs(path, network):
    assert fs.is_network_fs(Path(path), MOUNTS) is network

def test_mount_type_stats_the_path_first(tmp_path, monkeypatch):
    seen = []
    real = os.stat
    monkeypatch.setattr(fs.os, "stat", lambda p, *a, **k: (seen.append(Path(p)), real(p, *a, **k))[1])
    fs.mount_type(tmp_path, MOUNTS)
    assert tmp_path.resolve() in seen

def test_require_local_dir_creates_and_accepts_local(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "mount_type", lambda p, mounts=None: "xfs")
    fs.require_local_dir(tmp_path / "locks")
    assert (tmp_path / "locks").is_dir()

def test_require_local_dir_rejects_network(tmp_path, monkeypatch):
    monkeypatch.setattr(fs, "mount_type", lambda p, mounts=None: "nfs")
    with pytest.raises(RuntimeError, match="nfs, a network filesystem"):
        fs.require_local_dir(tmp_path / "locks")

@pytest.mark.parametrize("mode", [0o644, 0o640, 0o660, 0o700])
def test_require_secret_file_rejects_loose_modes(tmp_path, mode):
    tok = tmp_path / "tok"
    tok.write_text("x")
    tok.chmod(mode)
    with pytest.raises(RuntimeError, match="mode 600"):
        fs.require_secret_file(tok)

def test_require_secret_file_accepts_600_and_rejects_missing_or_symlink(tmp_path):
    tok = tmp_path / "tok"
    tok.write_text("x")
    tok.chmod(0o600)
    fs.require_secret_file(tok)
    with pytest.raises(RuntimeError):
        fs.require_secret_file(tmp_path / "missing")
    link = tmp_path / "link"
    link.symlink_to(tok)
    with pytest.raises(RuntimeError):
        fs.require_secret_file(link)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_core_fs.py -q`
Expected: `ImportError: cannot import name 'fs'`

- [ ] **Step 3: Implement `mia_core/fs.py`** with the interfaces above. `mount_type` reads
`Path("/proc/self/mounts").read_text()` when `mounts is None`; parse fields split on spaces, `mount_point =
fields[1].replace("\\040", " ")`, `fstype = fields[2]`; the chosen entry is the one with the longest
`mount_point` such that `path == mount_point or path.is_relative_to(mount_point)`.

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run pytest tests/test_core_fs.py -q && pixi run lint && pixi run typecheck`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mia_core/fs.py tests/test_core_fs.py
git commit -m "mia_core.fs: which mount holds a path, host-local root and secret-file checks"
```

---

### Task 5: `mia_ops` reads `mia.yaml` as the configuration; local root, lock directory and token

**Files:**
- Modify: `mia_ops/manifest.py`, `mia_ops/services.py`, `mia_ops/cli.py`
- Test: `tests/test_mia_ops_manifest.py`, `tests/test_mia_ops_services.py`, `tests/test_mia_ops_cli.py`

**Interfaces:**
- Consumes: Task 1 (`read_manifest`, `manifest_path`, `service_config`), Task 2 (`load_config(path,
  service=)`), Task 4 (`require_local_dir`, `require_secret_file`).
- Produces:
  - `mia_ops.manifest.ServiceCfg`: `model_config = ConfigDict(extra="allow")`; `app: Literal[...]`;
    `config: Path | None = None` (legacy per-app file; resolved against `PROJECT_DIR` when set)
  - `mia_ops.manifest.TemporalCfg`: `extra="allow"` (the shared `address`, `poll_minutes` pass through to the
    applications; the manifest reads only `managed` and `state_dir`)
  - `mia_ops.manifest.Manifest.path: Path | None = None` (set by `load_manifest`); the duplicate-config check
    applies to services that set `config`
  - `mia_ops.manifest.load_manifest(path=None) -> Manifest`: `manifest_path(path or $MIA_MANIFEST)` (the old
    variable kept as a fallback for one release), `read_manifest`, `Manifest(**data, path=path)`
  - `mia_ops.services.App.load_config(path: Path, service: str | None = None) -> Any`
  - `mia_ops.services.App.config_cls_name` is not needed; keep going through `load_config`
  - `mia_ops.services.Service.legacy: bool` (`config` was set); `Service.config_path: Path` (the legacy file,
    else the manifest's path: the file the worker must be able to read); `Service.env() -> dict[str, str]`:
    legacy → `{app.config_env: str(config_path)}`, else `{"MIA_SERVICE": name, "MIA_CONFIG": str(config_path)}`
  - `mia_ops.services.Stack.__init__`: builds each service's config with `app.load_config(svc_config_path,
    service=None if legacy else name)`; prints `"{name}: services.{name}.config is deprecated; move its
    sections into mia.yaml"` once per legacy service
  - `mia_ops.services.App.extra_mounts(cfg)`: for `mia_data` and `mia_buffers`, append
    `("--rw", cfg.paths.local_root / "locks")` when `cfg.paths.local_root` is set
  - `mia_ops.services.service_up`: before starting anything, for `mia-data` and `mia-buffers` apps:
    `cfg.paths.local_root is None` → `sys.exit(f"{svc.name}: set paths.local_root in mia.yaml (host-local disk
    for the lock directory and the service state)")`; `require_local_dir(local_root)` and `mkdir` of
    `local_root / "locks"` and `local_root / "service"`, a `RuntimeError` → `sys.exit(f"{svc.name}: {error}")`.
    For every app whose `cfg.service` has a `token_file`: `require_secret_file` → `sys.exit` on error; mount it
    `--ro` beside the GitHub secret. For `mia-buffers`, a missing `service.token_file` → `sys.exit(f"{svc.name}:
    set service.token_file in mia.yaml and run `mia init-token`")`. The worker's `env ...` prefix and the
    `schedule apply` subprocess use `svc.env()`.
  - `mia_ops.cli.cmd_exec`: `env = {**os.environ, **svc.env()}`
  - `mia_ops.cli.cmd_init_token(stack, args) -> int`: for each distinct `cfg.service.token_file` over the
    services: if absent, write `secrets.token_hex(32)` through `os.open(path, O_WRONLY | O_CREAT | O_EXCL,
    0o600)` and print `"wrote {path}"`; if present, print `"{path} exists"` and warn when its mode is not `600`.
    Registered as `init-token` with help "create the worker token file named by service.token_file".

- [ ] **Step 1: Write the failing tests**

`tests/test_mia_ops_manifest.py`: remove the xfail from `test_example_manifest_loads` and assert
`list(m.services) == ["autodev", "snippets", "mia-data", "mia-buffers"]`, `m.services["mia-data"].config is None`,
`m.path == PROJECT_DIR / "mia.yaml.example"`. Add:

```python
def test_service_blocks_carry_sections_and_legacy_config_is_optional():
    m = Manifest(services={"a": {"app": "autodev", "github": {"repo": "o/r"}}, "b": {"app": "mia-data", "config": "x.yaml"}})
    assert m.services["a"].model_extra == {"github": {"repo": "o/r"}} and m.services["b"].config == PROJECT_DIR / "x.yaml"

def test_load_manifest_prefers_mia_config_env(tmp_path, monkeypatch):
    p = tmp_path / "m.yaml"
    p.write_text("services:\n  autodev: {app: autodev, config: autodev.yaml}\n")
    monkeypatch.setenv("MIA_CONFIG", str(p))
    monkeypatch.delenv("MIA_MANIFEST", raising=False)
    assert load_manifest().path == p
```

`tests/test_mia_ops_services.py`: add a builder for the new form and the tests below.

```python
def write_mia_yaml(tmp_path, address="localhost:7233", token_mode=0o600, local_root=None):
    """mia.yaml in the new shape; `local_root=""` leaves the key out."""
    (tmp_path / "token").write_text("t" * 64)
    (tmp_path / "token").chmod(token_mode)
    (tmp_path / "harness").mkdir(exist_ok=True)
    local = tmp_path / "local" if local_root is None else local_root
    paths = (f"state_root: {tmp_path}/state, work_root: {tmp_path}/w, storage_root: {tmp_path}/store, "
             f"staged_root: {tmp_path}/staged" + (f", local_root: {local}" if local else ""))
    (tmp_path / "mia.yaml").write_text(
        f"temporal: {{managed: true, state_dir: {tmp_path}/temporal, address: {address}}}\n"
        f"github: {{auth: token, token_file: {tmp_path}/token, catalog_repo: o/c, snippets_repo: o/s}}\n"
        f"paths: {{{paths}}}\nharness: {{dir: {tmp_path}/harness}}\n"
        f"service: {{url: http://buffer-host.example.org:8765, token_file: {tmp_path}/token}}\n"
        "services:\n"
        "  autodev: {app: autodev, github: {repo: o/r}, temporal: {namespace: autodev}}\n"
        "  snippets: {app: autodev, github: {repo: o/s2}}\n"
        "  mia-data: {app: mia-data, github: {data_repo: o/d}, temporal: {namespace: mia-data}}\n"
        "  mia-buffers: {app: mia-buffers, github: {buffers_repo: o/b, datasets_repo: o/d}, temporal: {namespace: mia-buffers}}\n"
    )
    return tmp_path / "mia.yaml"

def make_stack_v2(tmp_path, **kw):
    from mia_ops.manifest import load_manifest
    manifest = load_manifest(write_mia_yaml(tmp_path, **kw))
    manifest.sandbox_dir = tmp_path / "sandbox"
    return Stack(manifest)
```

`services.py` imports the two checks by name (`from mia_core.fs import require_local_dir, require_secret_file`)
so tests can replace them; the `fake` fixture sets `services.require_local_dir` to
`lambda p: p.mkdir(parents=True, exist_ok=True)` (the real one reads `/proc/self/mounts`), and the one test that
needs a refusal replaces it again.

```python
def test_stack_builds_each_service_from_its_block(tmp_path, fake):
    s = make_stack_v2(tmp_path)
    assert [svc.legacy for svc in s.services.values()] == [False] * 4
    assert s.services["mia-data"].cfg.github.data_repo == "o/d" and s.services["mia-data"].state == tmp_path / "state/mia-data"
    assert s.services["snippets"].env() == {"MIA_SERVICE": "snippets", "MIA_CONFIG": str(tmp_path / "mia.yaml")}

def test_legacy_config_files_still_load_with_a_deprecation_line(tmp_path, fake, capsys):
    s = make_stack(tmp_path)  # the existing per-file builder
    assert s.services["mia-data"].legacy and s.services["mia-data"].env() == {"MIA_DATA_CONFIG": f"{tmp_path}/mia_data.yaml"}
    assert "services.mia-data.config is deprecated" in capsys.readouterr().out

def test_up_passes_the_service_name_and_mounts_locks_and_token(tmp_path, fake):
    s = make_stack_v2(tmp_path)
    services.service_up(s.services["mia-data"], s, Temporal(s))
    worker = worker_command(fake, "mia-data")
    for piece in (f"MIA_SERVICE=mia-data MIA_CONFIG={tmp_path}/mia.yaml", f"--ro {tmp_path}/mia.yaml",
                  f"--rw {tmp_path}/local/locks", f"--ro {tmp_path}/token", "-m mia_data.cli worker"):
        assert piece in worker, piece
    assert "MIA_DATA_CONFIG" not in worker
    assert (tmp_path / "local/locks").is_dir() and (tmp_path / "local/service").is_dir()
    ((argv, kw),) = schedule_calls(fake)
    assert kw["env"]["MIA_SERVICE"] == "mia-data"

def test_up_refuses_a_network_local_root(tmp_path, fake, monkeypatch):
    s = make_stack_v2(tmp_path)

    def on_nfs(path):
        raise RuntimeError(f"{path} is on nfs, a network filesystem; it must be host-local disk")

    monkeypatch.setattr(services, "require_local_dir", on_nfs)
    with pytest.raises(SystemExit, match="network filesystem"):
        services.service_up(s.services["mia-buffers"], s, Temporal(s))
    assert not tmux_calls(fake, "new-session")

def test_up_refuses_a_missing_local_root_for_data_and_buffers(tmp_path, fake):
    s = make_stack_v2(tmp_path, local_root="")  # the builder omits the key when empty
    with pytest.raises(SystemExit, match="paths.local_root"):
        services.service_up(s.services["mia-data"], s, Temporal(s))
    services.service_up(s.services["autodev"], s, Temporal(s))  # autodev needs none

def test_up_refuses_a_loose_token_file(tmp_path, fake):
    s = make_stack_v2(tmp_path, token_mode=0o644)
    with pytest.raises(SystemExit, match="mode 600"):
        services.service_up(s.services["mia-buffers"], s, Temporal(s))

def test_mia_buffers_app_mounts_include_the_lock_directory(tmp_path, fake):
    s = make_stack_v2(tmp_path)
    cfg = s.services["mia-buffers"].cfg
    assert ("--rw", cfg.paths.local_root / "locks") in services.APPS["mia-buffers"].extra_mounts(cfg)
```

`schedule_calls` must also match `mia_buffers.cli`. `tests/test_mia_ops_cli.py`: add

```python
def test_exec_passes_the_service_name(tmp_path, fake, monkeypatch):
    s = make_stack_v2(tmp_path)
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    assert cli.main(["exec", "mia-buffers", "plan", "--help"]) == 0
    argv, kw = fake.calls[-1]
    assert argv[1:3] == ["-m", "mia_buffers.cli"] and kw["env"]["MIA_SERVICE"] == "mia-buffers"

def test_init_token_writes_once_with_mode_600(tmp_path, fake, monkeypatch, capsys):
    s = make_stack_v2(tmp_path)
    (tmp_path / "token").unlink()
    monkeypatch.setattr(cli, "load_manifest", lambda _: s.manifest)
    assert cli.main(["init-token"]) == 0
    assert oct((tmp_path / "token").stat().st_mode & 0o777) == "0o600" and len((tmp_path / "token").read_text()) == 64
    assert cli.main(["init-token"]) == 0 and "exists" in capsys.readouterr().out
```

(`Stack` loads configs at construction, so `make_stack_v2` must create the token before `Stack(...)`; the
`init-token` test removes it afterwards.) `test_help_lists_commands` gains `"init-token"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_mia_ops_manifest.py tests/test_mia_ops_services.py tests/test_mia_ops_cli.py -q`
Expected: the new tests fail; `test_example_manifest_loads` fails on `config` being required.

- [ ] **Step 3: Implement** the interfaces above in the three `mia_ops` modules. In `service_up`, the order is:
prerequisites and loopback check as today → `local_root` and token checks → directories → Temporal → sessions.
The token file is mounted with `"--ro", token_file` right after `svc.secret`; skip the duplicate when it is the
same path as the secret.

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run test && pixi run lint && pixi run typecheck`
Expected: all pass, no xfail left.

- [ ] **Step 5: Commit**

```bash
git add mia_ops tests/test_mia_ops_manifest.py tests/test_mia_ops_services.py tests/test_mia_ops_cli.py
git commit -m "mia_ops: mia.yaml is the configuration; MIA_SERVICE for workers and exec; host-local root with the lock directory, the worker token, mia init-token"
```

---

### Task 6: `mia-buffers check`, and the local root and token in `mia-data check`

**Files:**
- Modify: `mia_buffers/cli.py` (new `cmd_check`, registered with `--config` like `worker`), `mia_data/cli.py`
  (`cmd_check`: two conditional rows)
- Test: `tests/test_buffers_cli.py`, `tests/test_mia_cli.py`

**Interfaces:**
- Consumes: `autodev.cli.run_checks`, `_client`; `mia_data.cli._gh`; `mia_data.jobspec.harness_pin`;
  Task 4's `require_local_dir`, `require_secret_file`.
- Produces: `mia_buffers.cli.cmd_check(cfg: Config) -> int`, rows in this order:
  `github buffers`, `github datasets`, `github snippets`, `github catalog` (each `GET /repos/<repo>`),
  `temporal` (describe namespace, as mia-data), `directories` (write probe in `state_dir`, `work_root`,
  `raw_root`, `staged_root`; `os.access(storage_root, R_OK)`), `local root` (`require_local_dir(local_root)`,
  then a write probe in `local_root / "locks"`; BAD with "set paths.local_root" when `None`), `service token`
  (`require_secret_file(service.token_file)`; BAD with "set service.token_file" when `None`), `harness`
  (`harness_pin(cfg)` and `harness.dir / "tensorizers" / sourcing.tensorizer / "tensorizer.yaml"` exists),
  `buffer-api` (`httpx.get(f"{service.url}/buffers/_check_/status", timeout=10)`; ok when the status is 404,
  the answer of a reachable service for an unknown buffer; the message is the URL).
  csub is not checked here: the policy and broker are the same session the mia-data check already covers
  (`# ponytail:` comment saying so).
  In `mia_data.cli.cmd_check`: append `("local root", ...)` when `cfg.paths.local_root` is set and
  `("service token", ...)` when `cfg.service and cfg.service.token_file`, with the same bodies; the existing
  seven rows and their order are unchanged, so the current tests keep passing.

`mia_buffers/cli.py` gets these module-level imports so the test can replace them by name: `import httpx`,
`from autodev.cli import run_checks` (beside `_client`), `from mia_core.fs import require_local_dir,
require_secret_file`, `from mia_data.jobspec import harness_pin`. `mia_data/cli.py` imports the two `mia_core.fs`
names the same way. `run_checks` prints `f"{'ok ' if ok else 'BAD'}  {name:16} {detail}"`, so a row starts with
`"ok   "` or `"BAD  "` followed by the name.

- [ ] **Step 1: Write the failing tests**

`tests/test_buffers_cli.py`:

```python
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
    cfg = make_cfg(tmp_path, paths={"local_root": str(tmp_path / "local")}, service={"url": "http://svc:1", "token_file": str(tmp_path / "tok")})
    for d in ("state", "work", "raw", "staged", "storage"):
        (tmp_path / d).mkdir(exist_ok=True)
    (tmp_path / "tok").write_text("t")
    (tmp_path / "tok").chmod(0o600)
    tdir = cfg.harness.dir / "tensorizers" / cfg.sourcing.tensorizer
    tdir.mkdir(parents=True)
    (tdir / "tensorizer.yaml").write_text("name: x\n")
    monkeypatch.setattr(cli, "load_config", lambda path=None, service=None: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    monkeypatch.setattr(cli, "_gh", lambda cfg_, repo, tokens=None: _CheckGitHub(repo))
    monkeypatch.setattr(cli, "_client", _temporal_down)
    monkeypatch.setattr(cli, "harness_pin", lambda cfg_: ("a" * 40, "b" * 64))
    monkeypatch.setattr(cli, "require_local_dir", lambda p: p.mkdir(parents=True, exist_ok=True))
    monkeypatch.setattr(cli.httpx, "get", lambda url, timeout: SimpleNamespace(status_code=404))
    assert cli.main(["check"]) == 1
    rows = capsys.readouterr().out.rstrip("\n").split("\n")
    assert [r[:3] for r in rows] == ["ok ", "ok ", "ok ", "BAD", "BAD", "ok ", "ok ", "ok ", "ok ", "ok "]
    assert rows[3].startswith("BAD  github catalog") and "down" in rows[4]
    assert rows[9].startswith("ok   buffer-api") and "http://svc:1" in rows[9]
```

`tests/test_mia_cli.py`:

```python
def test_check_adds_local_root_and_token_rows_when_configured(monkeypatch, tmp_path, capsys):
    _fake_env(monkeypatch, ["*"])
    for d in ("state", "work", "storage"):
        (tmp_path / d).mkdir()
    (tmp_path / "tok").write_text("t"); (tmp_path / "tok").chmod(0o600)
    cfg = make_cfg(tmp_path, paths={"local_root": str(tmp_path / "local")}, service={"url": "http://svc:1", "token_file": str(tmp_path / "tok")})
    _harness_bin(cfg)
    monkeypatch.setattr(cli, "load_config", lambda path=None, service=None: cfg)
    monkeypatch.setattr(cli, "_setup_logging", lambda *a: None)
    monkeypatch.setattr(cli, "require_local_dir", lambda p: p.mkdir(parents=True, exist_ok=True))
    main(["check"])
    out = capsys.readouterr().out.rstrip("\n").split("\n")
    assert len(out) == 9 and out[7].startswith("ok   local root") and out[8].startswith("ok   service token")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_buffers_cli.py tests/test_mia_cli.py -q -k check`
Expected: the buffers test fails on `argparse` (no `check` command); the mia-data test fails on the row count.

- [ ] **Step 3: Implement** `cmd_check` in `mia_buffers/cli.py` and the two rows in `mia_data/cli.py`.
Register `check` in `mia_buffers.cli.main` in the loop that creates `worker`, `schedule`, `init-labels`, with
help "verify GitHub repos, Temporal, directories, the local root, the token, the harness and buffer-api".

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run test && pixi run lint && pixi run typecheck`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mia_buffers/cli.py mia_data/cli.py tests/test_buffers_cli.py tests/test_mia_cli.py
git commit -m "check: mia-buffers check; mia-data check covers the local root and the worker token"
```

---

### Task 7: `mia_core/untrusted.py`: one home for the untrusted-output helpers

**Files:**
- Create: `mia_core/untrusted.py`
- Modify: `autodev/jobs.py` (`read_untrusted` becomes a re-export), `autodev/activities.py` (`neutralize`,
  `SECRET_RE` become re-exports), `mia_data/activities.py` (`write_atomic`, `_text`, `_texts`, `_loads` become
  imports with their old names), `mia_data/gallery.py`, `mia_data/publish.py`, `mia_data/status.py`,
  `mia_buffers/activities.py` (import from `mia_core.untrusted` under the public names)
- Test: `tests/test_core_untrusted.py`

**Interfaces:**
- Produces, in `mia_core.untrusted`, moved verbatim (bodies unchanged) under these names:
  `read_untrusted` (with its two `@overload`s), `SECRET_RE`, `neutralize(text: str, token: str | None = None) ->
  str`, `write_atomic(path: Path, data: str | bytes) -> None`, `loads(text: str | bytes | None) -> Any`,
  `text(value: Any, limit: int = 2000) -> str`, `texts(value: Any, limit: int = 20) -> list[str]`.
  `_small_dict`, `_job_json`, `_fenced` stay in `mia_data.activities` (no second user).
- Re-exports: `autodev/jobs.py`: `from mia_core.untrusted import read_untrusted as read_untrusted`;
  `autodev/activities.py`: `from mia_core.untrusted import SECRET_RE as SECRET_RE, neutralize as neutralize`;
  `mia_data/activities.py`: `from mia_core.untrusted import loads as _loads, text as _text, texts as _texts,
  write_atomic as write_atomic` (the module's own callers and `tests/test_mia_activities.py` keep their names).
  `mia_buffers/activities.py` imports `loads, text, texts, write_atomic, neutralize, read_untrusted` from
  `mia_core.untrusted` and its call sites use the public names.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_core_untrusted.py
from pathlib import Path
from mia_core.untrusted import loads, neutralize, read_untrusted, text, texts, write_atomic

def test_moved_helpers_behave_as_before(tmp_path):
    assert neutralize("a <!-- x --> ghp_abcdef", token=None) == "a &lt;!-- x --> [redacted]"
    assert loads("[1]") == [1] and loads("nope") is None and loads(None) is None
    assert text(None) == "" and text("x" * 10, 3) == "xxx" and texts("a") == ["a"] and texts(None) == []
    write_atomic(tmp_path / "d" / "f", "hi")
    assert (tmp_path / "d" / "f").read_text() == "hi" and not list((tmp_path / "d").glob(".*tmp"))
    (tmp_path / "job").mkdir(); (tmp_path / "job" / "out.json").write_text("{}")
    assert read_untrusted(tmp_path / "job", "out.json", 100) == "{}"
    (tmp_path / "job" / "link").symlink_to(tmp_path / "job" / "out.json")
    assert read_untrusted(tmp_path / "job", "link", 100) is None

def test_old_import_sites_still_work():
    from autodev.activities import neutralize as a
    from autodev.jobs import read_untrusted as b
    from mia_data.activities import _loads, write_atomic as c
    import mia_core.untrusted as core
    assert a is core.neutralize and b is core.read_untrusted and c is core.write_atomic and _loads is core.loads
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pixi run pytest tests/test_core_untrusted.py -q`
Expected: `ModuleNotFoundError: No module named 'mia_core.untrusted'`

- [ ] **Step 3: Move the functions** and rewrite the import lines listed above. `grep -rn "_loads\|_text(\|_texts(\|read_untrusted\|neutralize\|write_atomic" mia_buffers autodev mia_data` afterwards shows no definition outside `mia_core/untrusted.py` and no underscore name in `mia_buffers`.

- [ ] **Step 4: Run the tests and the four checks**

Run: `pixi run test && pixi run lint && pixi run typecheck`
Expected: PASS (the Temporal replay tests included: no workflow command changed).

- [ ] **Step 5: Commit**

```bash
git add mia_core/untrusted.py autodev/jobs.py autodev/activities.py mia_data/activities.py mia_data/gallery.py mia_data/publish.py mia_data/status.py mia_buffers/activities.py tests/test_core_untrusted.py
git commit -m "mia_core.untrusted: read_untrusted, neutralize, write_atomic and the bounded readers in one place"
```

---

### Task 8: Documentation and the final check

**Files:**
- Modify: `AGENTS.md`, `docs/development.md`, `docs/autodev-local-setup.md`, `docs/mia-data-workflow.md`,
  `docs/github-app.md`, `notes/autodev-runbook.md`

No code. The exact changes:

- `AGENTS.md` first paragraph: "Three Python packages" → four, adding `mia_core/` (shared config, filesystem
  checks, untrusted-output helpers; imports no application package; every package may import it). Add to
  "Rules that will bite you": "**One config file.** Deployments read their block of `mia.yaml` (`MIA_SERVICE`,
  `MIA_CONFIG`); the per-app YAML files are deprecated. `paths.local_root` must be host-local disk."
- `docs/development.md` Layout table: a `mia_core/` row before `autodev/`; the `autodev/` row loses "the
  Temporal connection ... (cli.py)" nothing, but its parenthetical about config moves to `mia_core`. Rule "No
  real paths": name `mia.yaml`. Section "Running a buffer from an issue": "The deployment reads
  `mia_buffers.yaml` (copy the example; ...)" → "The deployment reads its `mia-buffers` block of `mia.yaml`
  (`mia.yaml.example`; `buffers_repo` ...)". Section "Datasets without a binding": "`mia_buffers.yaml` names
  mia-data's issue repo" → "the `mia-buffers` block names ..."; "mia-data's own `mia_data.yaml` lists" → "the
  `mia-data` block lists". Add after "Setup": a short "Configuration" paragraph: one file, shared sections,
  per-service blocks, `MIA_SERVICE=<name> <app> check` by hand, `mia exec <service> check` through `mia`,
  `paths.local_root`, `service.token_file` and `mia init-token`.
- `docs/autodev-local-setup.md`: steps 2–4 (`cp autodev.yaml.example autodev.yaml`, "Edit `autodev.yaml`",
  `github.token_file ... in autodev.yaml`, "`mia.yaml` lists one service, your `autodev.yaml`") → `cp
  mia.yaml.example mia.yaml`, keep the `autodev` service block, edit `services.autodev.github.repo` and the
  shared `github`/`paths`; `agent.allow_hosts` lives under `services.autodev.agent`.
- `docs/mia-data-workflow.md` "Operating it": "Config is `mia_data.yaml` (see `mia_data.yaml.example`)" → "Config
  is the `mia-data` block of `mia.yaml` (see `mia.yaml.example`)".
- `docs/github-app.md` line "In `autodev.yaml`:" → "In the shared `github` section of `mia.yaml`:".
- `notes/autodev-runbook.md` steps 2 and the mia-data step 1: the same substitution; the personal-instance
  advice becomes "name the service `autodev-<you>` in `services`" (the deployment is the service name).

- [ ] **Step 1: Make the edits above**

- [ ] **Step 2: Verify**

Run: `grep -rn 'yaml.example\|_CONFIG\b' --include='*.md' docs AGENTS.md notes/autodev-runbook.md`
Expected: only `mia.yaml.example` and `MIA_CONFIG` remain.

Run: `pixi run fmt && pixi run lint && pixi run typecheck && pixi run test && git diff | grep -n 'rokickik\|/nrs/'`
Expected: four clean runs; the grep prints nothing.

- [ ] **Step 3: Commit**

```bash
git add AGENTS.md docs notes/autodev-runbook.md
git commit -m "docs: one mia.yaml, mia_core, the host-local root and the worker token"
```

---

## Done when (spec §10 row A)

Both deployments start from one file (`make_stack_v2` in the tests; by hand, `mia up` on a host with a real
`mia.yaml`), `mia exec mia-data check` and `mia exec mia-buffers check` pass against the live services, the old
per-app files still load with a deprecation line, and `mia up` refuses a `local_root` on NFS. The live `mia up`
is the one step this plan cannot test; run it once after Task 8 and note the result in the commit message of a
follow-up if anything needed changing.
