# autodev: issue → agent PR → human approval → merge — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A service that picks up GitHub issues a human has labelled `autodev:go` and runs Claude Code in a sandboxed LSF job to implement each one. It opens a PR and runs revision rounds when reviewers ask for changes. It merges only after a human with write access approves the current head commit.

**Architecture:**
- **Workflows.** One long-lived Temporal `IssueWorkflow` per issue: a thin state machine of activity calls and durable timers. A scheduled `PollWorkflow` starts these workflows.
- **Agent jobs.** Each agent round is one csub job (`claude: true`) that gets only its own workspace, a fresh clone with no credentials. It hands back a git bundle.
- **The trusted worker.** It owns the GitHub token and a bare mirror. It is the only thing that pushes, opens PRs, comments, or merges.
- **Identity.** Nothing is tied to a person. The OS account that runs the processes (Temporal worker, csub broker) plus `autodev.yaml` decide who the system acts as: a GitHub App on a service account, or your own token for testing.

**Tech Stack:**
- Python 3.12 (pixi), Temporal Python SDK 1.34, httpx, pydantic 2
- `cryptography` for GitHub App JWTs
- csub client library; Claude Code CLI inside csub jobs; agentic-sandbox (`sandbox-run.sh`) for the worker
- pytest, pytest-asyncio, respx

**Spec:** [`notes/agentic-dev-infrastructure.md`](../agentic-dev-infrastructure.md) gives the design context: why Temporal, the trust boundaries, and what is borrowed from where. The "Design decisions" section below is the spec for this basic loop.

## Design decisions (the spec for this plan)

1. **Trigger.** A human adds the label `<deployment>:go` to an open issue. The poller starts a workflow only if whoever added that label has `write`, `maintain` or `admin` permission. Otherwise it removes the label and says why in a comment.
2. **Rounds.** Round 1 starts from `base_branch` and force-pushes `<deployment>/issue-<n>`. Revision rounds start from the PR head and must fast-forward.
3. **Agent job.**
   - `csub submit` with `claude: true`, `shell: true`, cwd and the only read-write mount set to the round directory, and `allow_hosts` from config. The config must not list GitHub hosts.
   - The job runs `claude -p` in `repo/` and writes `result.json`, `claude.err` and `exit_code` next to it.
   - It then runs `git bundle create ../result.bundle <base>..HEAD`.
4. **Trust boundary.** The worker never runs git inside an agent workspace after the job has started. It reads `result.bundle`, `result.json`, `exit_code` and the logs as data only.
   - Workspaces are cloned from the mirror with `--no-hardlinks`.
   - Every git command the worker runs has `-c core.hooksPath=/dev/null`.
   - The GitHub token reaches git only through `GIT_CONFIG_*` environment variables. It never appears in argv or in any file.
   - Commits that touch `protected_paths` (default `.github/`) are refused.
5. **PR loop.** Every `poll_minutes` the workflow observes its PR and calls the pure function `decide()`:
   - merged → done
   - closed → stop
   - unhandled feedback and rounds left → revise
   - approved on the current head, with checks not failing → merge
   - approved but checks failing, or revision limit reached → escalation comment (posted once)
   - otherwise → wait
6. **Approval.** Either an `APPROVED` review on the head SHA, if it is that reviewer's latest review, or a comment `/approve <sha prefix ≥7>`. The author must have write access and must not be the bot login, except under `auth: token`, where you are the bot and are testing.
7. **Feedback.** Either a `CHANGES_REQUESTED` review (its body plus its inline comments) or a comment `/revise <text>`, from a human with write access.
   - The summary comment posted after each round carries `<!-- <deployment>: handled <feedback-id> -->`.
   - Feedback already handled in an earlier workflow run is therefore never redone.
8. **Merge.** Squash merge pinned to the approved SHA (`sha=` on the merge call). Then delete the branch and close the issue.
9. **Failure.** Any round or activity failure posts the error plus a log tail and sets `<deployment>:needs-human`, and the workflow ends. Re-adding `:go` starts a new run, which resumes an existing open PR instead of redoing it.
10. **Identity (all from config and the OS account):**

| | run as yourself (testing) | group service |
|---|---|---|
| GitHub | `auth: token`: a personal token in `$GITHUB_TOKEN` / `.env`, or `github.token_file` (needed inside the sandbox) | `auth: app`: GitHub App installation token minted from a private key file |
| LSF / Claude | your csub broker, your Claude login (csub `--claude`) | csub broker installed under the service account; its Claude login, or `agent.api_key_env` |
| Temporal | local dev server started by `scripts/autodev-up.sh` | same script under the service account (a shared server later) |
| Names | `deployment: autodev-<you>` | `deployment: autodev` |

   `deployment` sets the label prefix, the Temporal task queue, workflow IDs, the branch prefix, the csub session and comment markers. A personal instance and the service can therefore share one repo without touching each other's issues.

## Before you start (prerequisites, not tasks)

- [ ] **P1. csub with Claude support.** The `claude` request field must be on csub `main`: the PR "Let a job run Claude Code" (branches `claude-jobs` / `deploy-all`). Check:
  `cd ../csub && git log origin/main --oneline | grep -i "run Claude Code"` prints a commit.
  Until it is merged, Task 1's pixi dependency must point at the branch that has it.
- [ ] **P2. Broker policy** of the account you'll test with (`~/.config/csub/broker.toml`):
  - `allow_claude = true`
  - `allowed_hosts` includes `api.anthropic.com`, `claude.ai`, `platform.claude.com`, plus the hosts in `agent.allow_hosts`
  - `allowed_roots` contains `paths.work_root`

  Check: `csub probe`.
- [ ] **P3. Claude login** for that account: `claude auth login`. Check: `csub submit --claude --walltime 5 --wait -- claude -p "Reply OK" --model sonnet` prints `OK`.
- [ ] **P4. GitHub token** with Contents, Pull requests and Issues read/write on the target repo, saved for the sandboxed worker with `gh auth token > ~/.config/autodev/token && chmod 600 ~/.config/autodev/token` (or a fine-grained PAT in that file). `GITHUB_TOKEN` in `.env` also works for host-side commands, but the sandbox masks `.env`.
- [ ] **P5. Branch.** `git checkout main && git checkout -b autodev`.

## Global Constraints

- All new code lives in the package `autodev/` beside `zarr_doctor/`. Reuse `zarr_doctor.github_api.GitHub` (subclass it) and `zarr_doctor.config.load_dotenv` / `PROJECT_DIR`; don't copy them.
- Tests live in `tests/test_autodev_*.py`; shared helpers go in `tests/autodev_helpers.py`. File basenames must not clash with zarr-doctor's tests. Run everything with `pixi run test`.
- No secret may appear in argv, in any file under `paths.work_root`, in a git config file, or in a comment. The only exception is `ANTHROPIC_API_KEY` in the job env when `agent.api_key_env` is set.
- Agent jobs go through csub only, with `claude: true`. Their read-write mount is their own round directory only. `agent.allow_hosts` never includes `github.com` or `api.github.com`.
- The worker pushes only to `<deployment>/issue-<n>`. It never pushes to `base_branch`.
- Workflow code does no I/O and imports activities by name only. Any change to `autodev/workflows.py` must keep `test_replay_after_log_change` passing.
- Poll a csub job's status at most once a minute (cluster etiquette).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- `csub` is a **pixi-only** dependency. Never add it to `[project].dependencies`: an unrelated PyPI package could resolve under that name.

## Review Focus

1. **A `:go` label added by someone without write access** (for example a drive-by issue author on a public repo, i.e. prompt injection) must not start an agent. Pinned in Task 8 `test_poll_ignores_label_from_reader`.
2. **An approval of an older head SHA.** A reviewer approved, then the agent pushed a revision; this must not merge the new head. Pinned in Task 3 `test_approval_must_match_head` and Task 4 `test_stale_approval_waits`.
3. **Agent-controlled files** (workspace `.git/config` and hooks, hardlinked objects, `exit_code`, `result.json`, the bundle) must never execute on the trusted side, alter the mirror, or reach a credential. Pinned in Task 5 `test_workspace_is_independent_of_mirror` and `test_workspace_hooks_never_run`, and Task 6 `test_read_result_tolerates_garbage`.
4. **A worker restart mid-job, or LSF forgetting a finished job** (`UNKNOWN`), must not submit a duplicate job and must still finish the round. Pinned in Task 6 `test_submit_is_idempotent`, `test_submit_reuses_job_found_by_name` and `test_unknown_state_uses_exit_code_file`, and Task 7 `test_worker_death_mid_await_does_not_resubmit`.
5. **Feedback handled in an earlier run, or before continue-as-new,** must not trigger another round. Pinned in Task 8 `test_observe_skips_handled_feedback` and Task 7 `test_continue_as_new_keeps_handled_feedback`.

---

## File structure

| file | responsibility |
|---|---|
| `autodev/__init__.py` | package docstring |
| `autodev/config.py` | `Config` (pydantic), `load_config`, derived names (`label`, `branch`, `workflow_id`, `marker`), `STAGES` |
| `autodev/auth.py` | token function for `token` / `app` auth, GitHub App JWT, `bot_login` |
| `autodev/review.py` | pure readers: `approved`, `feedback`, `handled_ids`, `combine_checks`, `WRITE_PERMISSIONS` |
| `autodev/github.py` | `GitHub` subclass with PR, permission, label-actor, checks and merge calls |
| `autodev/models.py` | dataclasses passed between workflow and activities |
| `autodev/decide.py` | pure `decide()`: the next action for an open PR |
| `autodev/gitops.py` | trusted git: `Mirror` (sync, workspace, publish), `git()` helper, errors |
| `autodev/agent.py` | prompt and job-script rendering, `read_result`, `log_tail`, `AgentJobs` (csub submit/wait), `csub_client` |
| `autodev/workflows.py` | `IssueWorkflow`, `PollWorkflow` |
| `autodev/activities.py` | `Activities`: glue between workflows and the modules above; comment templates |
| `autodev/cli.py` | `autodev check / init-labels / worker / schedule apply` |
| `autodev.yaml.example` | annotated config |
| `scripts/autodev-up.sh` | tmux: Temporal dev server, host-side csub broker, worker inside `sandbox-run.sh` |
| `notes/autodev-runbook.md` | running as yourself, service-account setup, how humans drive it |
| `tests/autodev_helpers.py` | `make_cfg`, `make_origin`, `agent_commit`, `fake_gh`, constants |

---

### Task 1: Package, config and derived names

**Files:**
- Create: `autodev/__init__.py`, `autodev/config.py`, `autodev.yaml.example`, `tests/autodev_helpers.py`, `tests/test_autodev_config.py`
- Modify: `pyproject.toml`, `.gitignore`

**Interfaces:**
- Produces:
  - `Config` with fields `.deployment`, `.github` (`repo, base_branch, auth, token_env, token_file, app_id, installation_id, private_key_path, api_url, git_url, protected_paths`), `.temporal` (`address, namespace, via_https_proxy, poll_minutes`), `.paths` (`state_dir, work_root`) and `.agent` (`model, walltime_min, cpus, mem_mb, allow_hosts, extra_mounts, path_prepend, max_revisions, api_key_env`)
  - methods and properties `label(stage) -> str`, `branch(issue) -> str`, `workflow_id(issue) -> str`, `marker(text) -> str`, `marker_prefix`, `task_queue`, `git_url`, `repo_slug`
  - `STAGES: dict[str, tuple[str, str]]` (stage → (color, description))
  - `load_config(path=None) -> Config`
  - test helper `make_cfg(tmp_path, **overrides) -> Config`

- [ ] **Step 1: Write the failing test**

`tests/autodev_helpers.py`:
```python
"""Shared helpers for autodev tests."""

from __future__ import annotations

from pathlib import Path

from autodev.config import Config

API = "https://api.github.com"
R = "/repos/o/r"


def make_cfg(tmp_path: Path, github: dict | None = None, agent: dict | None = None, **top) -> Config:
    return Config(**{
        "deployment": "autodev",
        "github": {"repo": "o/r", **(github or {})},
        "paths": {"state_dir": str(tmp_path / "state"), "work_root": str(tmp_path / "work")},
        "agent": agent or {},
        **top,
    })
```

`tests/test_autodev_config.py`:
```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev'`

- [ ] **Step 3: Write minimal implementation**

`autodev/__init__.py`:
```python
"""autodev: GitHub issue -> Claude Code in a sandboxed LSF job -> PR -> human approval -> merge."""
```

`autodev/config.py`:
```python
"""Config: autodev.yaml + environment overrides. Secrets come only from env / ./.env and key files."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ValidationError, model_validator

from zarr_doctor.config import PROJECT_DIR, load_dotenv

__all__ = ["PROJECT_DIR", "STAGES", "Config", "load_config"]

_ENV_OVERRIDES = {
    "AUTODEV_DEPLOYMENT": ("deployment",),
    "TEMPORAL_ADDRESS": ("temporal", "address"),
    "AUTODEV_TEMPORAL_VIA_PROXY": ("temporal", "via_https_proxy"),
}

# stage -> (label color, description). `go` is set by humans; the rest by autodev.
STAGES = {
    "go": ("0e8a16", "Ask autodev to implement this issue"),
    "working": ("fbca04", "autodev: agent job running"),
    "review": ("1d76db", "autodev: PR open, waiting for review"),
    "needs-human": ("d93f0b", "autodev: stopped, needs a human"),
    "done": ("5319e7", "autodev: merged"),
}


class GitHubCfg(BaseModel):
    repo: str
    base_branch: str = "main"
    auth: Literal["token", "app"] = "token"  # token: run as yourself; app: GitHub App (service)
    token_env: str = "GITHUB_TOKEN"
    token_file: Path | None = None  # used when $token_env is unset; needed inside the sandbox, which masks .env
    app_id: int | None = None
    installation_id: int | None = None
    private_key_path: Path | None = None
    api_url: str = "https://api.github.com"
    git_url: str | None = None  # default https://github.com/<repo>.git; tests use a local path
    protected_paths: list[str] = [".github/"]


class TemporalCfg(BaseModel):
    address: str = "localhost:7233"
    namespace: str = "default"
    via_https_proxy: bool = False  # inside agentic-sandbox: reach Temporal through the allowlist proxy
    poll_minutes: int = 2


class PathsCfg(BaseModel):
    state_dir: Path = Path("~/.local/state/autodev")  # trusted: mirror, job ids, logs; never mounted into jobs
    work_root: Path  # per-round workspaces; must be under the csub policy's allowed_roots


class AgentCfg(BaseModel):
    model: str = "sonnet"
    walltime_min: int = 60
    cpus: int = 4
    mem_mb: int = 16000
    allow_hosts: list[str] = []  # never GitHub: the agent must not be able to push
    extra_mounts: list[str] = []  # "PATH:ro" or "PATH:rw"
    path_prepend: list[str] = []  # dirs put first on PATH inside the job (e.g. where pixi lives)
    max_revisions: int = 3
    api_key_env: str | None = None  # bill the API with this env var's key instead of the broker account's login


class Config(BaseModel):
    deployment: str = "autodev"
    github: GitHubCfg
    temporal: TemporalCfg = TemporalCfg()
    paths: PathsCfg
    agent: AgentCfg = AgentCfg()

    @model_validator(mode="after")
    def _finish(self) -> "Config":
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,30}", self.deployment):
            raise ValueError(f"deployment {self.deployment!r}: use 2-31 chars of a-z, 0-9, '-'")
        g = self.github
        if g.auth == "app" and not (g.app_id and g.installation_id and g.private_key_path):
            raise ValueError("github.auth=app needs github.app_id, installation_id and private_key_path")
        self.paths.state_dir = self.paths.state_dir.expanduser()
        self.paths.work_root = self.paths.work_root.expanduser()
        for attr in ("private_key_path", "token_file"):
            if getattr(g, attr):
                setattr(g, attr, getattr(g, attr).expanduser())
        return self

    @property
    def task_queue(self) -> str:
        return self.deployment

    def label(self, stage: str) -> str:
        return f"{self.deployment}:{stage}"

    def branch(self, issue: int) -> str:
        return f"{self.deployment}/issue-{issue}"

    def workflow_id(self, issue: int) -> str:
        return f"{self.deployment}/{self.github.repo}/issue-{issue}"

    @property
    def marker_prefix(self) -> str:
        return f"<!-- {self.deployment}:"

    def marker(self, text: str) -> str:
        return f"<!-- {self.deployment}: {text} -->"

    @property
    def git_url(self) -> str:
        return self.github.git_url or f"https://github.com/{self.github.repo}.git"

    @property
    def repo_slug(self) -> str:
        return self.github.repo.replace("/", "__")


def load_config(path: str | os.PathLike | None = None) -> Config:
    """Load config; exits with a readable message if it is missing or invalid."""
    load_dotenv()
    path = Path(path or os.environ.get("AUTODEV_CONFIG") or PROJECT_DIR / "autodev.yaml")
    if not path.is_file():
        sys.exit(f"autodev: config not found at {path} (copy autodev.yaml.example, or set $AUTODEV_CONFIG)")
    data = yaml.safe_load(path.read_text()) or {}
    for env, keys in _ENV_OVERRIDES.items():
        if os.environ.get(env):
            d = data
            for k in keys[:-1]:
                d = d.setdefault(k, {})
            d[keys[-1]] = os.environ[env]
    try:
        return Config(**data)
    except ValidationError as e:
        sys.exit(f"autodev: invalid config {path}:\n{e}")
```

`autodev.yaml.example`:
```yaml
# Copy to autodev.yaml (git-ignored), or point $AUTODEV_CONFIG at a copy. Secrets come from env / .env / key files.
# Who autodev acts as = the OS account running it + the github.auth settings below.
deployment: autodev          # prefix for labels, task queue, workflow ids, branches; personal test: autodev-<you>
github:
  repo: krokicki/mia-workflow-prototype
  base_branch: main
  auth: token                # token: $GITHUB_TOKEN (run as yourself) | app: GitHub App (service account)
  token_env: GITHUB_TOKEN
  token_file: null           # auth: token, when $GITHUB_TOKEN is unset; scripts/autodev-up.sh needs it
                             # (the sandbox masks .env): gh auth token > ~/.config/autodev/token; chmod 600 it
  app_id: null               # auth: app
  installation_id: null      # auth: app
  private_key_path: null     # auth: app; PEM readable only by the service account
  protected_paths: [".github/"]
temporal:
  address: localhost:7233    # $TEMPORAL_ADDRESS overrides
  namespace: default
  poll_minutes: 2
paths:
  state_dir: ~/.local/state/autodev        # trusted; never mounted into agent jobs
  work_root: /nrs/scicompsoft/autodev/work # agent workspaces; must be inside the csub policy's allowed_roots
agent:
  model: sonnet
  walltime_min: 60           # <= 60 lands in the short queue
  cpus: 4
  mem_mb: 16000
  allow_hosts: [pypi.org, files.pythonhosted.org, conda.anaconda.org, prefix.dev, repo.prefix.dev]
  extra_mounts: []           # e.g. ["/nrs/scicompsoft/autodev/tools:ro"]
  path_prepend: []           # e.g. ["/nrs/scicompsoft/autodev/tools/bin"] (a pixi binary for the agent's tests)
  max_revisions: 3
  api_key_env: null          # e.g. ANTHROPIC_API_KEY (service account billing the API instead of a login)
```

`pyproject.toml`: make these edits.
```toml
# [project] dependencies: add
    "cryptography",

# [project.scripts]: add
autodev = "autodev.cli:main"

# [tool.hatch.build.targets.wheel]
packages = ["zarr_doctor", "autodev"]

# [tool.pixi.dependencies]: add
cryptography = "*"

# [tool.pixi.pypi-dependencies]: add (pixi only; see Global Constraints)
csub = { git = "https://github.com/saalfeldlab/csub", branch = "main" }

# [tool.pixi.tasks]: add
ad = "autodev"
```

`.gitignore`: add a line `autodev.yaml`.

Then run `pixi install` and confirm `pixi run python -c "import csub, cryptography"` succeeds.

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_config.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add autodev/__init__.py autodev/config.py autodev.yaml.example tests/autodev_helpers.py tests/test_autodev_config.py pyproject.toml pixi.lock .gitignore
git commit -m "autodev: package, config and per-deployment names

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: GitHub identity (personal token or GitHub App)

**Files:**
- Create: `autodev/auth.py`, `tests/test_autodev_auth.py`

**Interfaces:**
- Consumes: `Config` (Task 1).
- Produces:
  - `token_fn(cfg) -> Callable[[], str]`: a sync callable returning a current token (`$token_env`, then `token_file`, or App tokens).
  - `app_jwt(app_id: int, pem: bytes, now: float | None = None) -> str`
  - `AppTokens(app_id, installation_id, key_path, api_url=..., clock=time.time, http=None)`: callable.
  - `async bot_login(cfg) -> str`: the login autodev's comments and PRs appear under.

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_auth.py`:
```python
import base64
import json

import httpx
import pytest
import respx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from autodev.auth import AppTokens, app_jwt, bot_login, token_fn

from autodev_helpers import API, make_cfg


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


@pytest.fixture(scope="module")
def key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _pem(key) -> bytes:
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def test_app_jwt_is_signed_and_backdated(key):
    tok = app_jwt(42, _pem(key), now=1000)
    h, p, s = tok.split(".")
    key.public_key().verify(_unb64(s), f"{h}.{p}".encode(), padding.PKCS1v15(), hashes.SHA256())
    assert json.loads(_unb64(h)) == {"alg": "RS256", "typ": "JWT"}
    assert json.loads(_unb64(p)) == {"iat": 940, "exp": 1540, "iss": "42"}


@respx.mock
def test_app_tokens_are_cached_until_near_expiry(key, tmp_path):
    kp = tmp_path / "app.pem"
    kp.write_bytes(_pem(key))
    route = respx.post(f"{API}/app/installations/7/access_tokens").mock(side_effect=[
        httpx.Response(201, json={"token": "t1", "expires_at": "2026-10-10T12:00:00Z"}),
        httpx.Response(201, json={"token": "t2", "expires_at": "2026-10-10T13:00:00Z"}),
    ])
    now = [1791633600.0 - 3600]  # 2026-10-10T11:00:00Z
    tokens = AppTokens(42, 7, kp, clock=lambda: now[0])
    assert tokens() == "t1" and tokens() == "t1" and route.call_count == 1
    now[0] = 1791633600.0 - 200  # within 5 minutes of expiry
    assert tokens() == "t2" and route.call_count == 2
    assert route.calls[0].request.headers["authorization"].startswith("Bearer ey")


def test_token_mode_reads_env_then_file(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "pat")
    assert token_fn(make_cfg(tmp_path))() == "pat"
    monkeypatch.delenv("GITHUB_TOKEN")
    with pytest.raises(RuntimeError, match="GITHUB_TOKEN"):
        token_fn(make_cfg(tmp_path))()
    (tmp_path / "tok").write_text("from-file\n")
    assert token_fn(make_cfg(tmp_path, github={"token_file": str(tmp_path / "tok")}))() == "from-file"


@respx.mock
async def test_bot_login_token_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "pat")
    respx.get(f"{API}/user").mock(return_value=httpx.Response(200, json={"login": "me"}))
    assert await bot_login(make_cfg(tmp_path)) == "me"


@respx.mock
async def test_bot_login_app_mode(tmp_path, key):
    kp = tmp_path / "app.pem"
    kp.write_bytes(_pem(key))
    respx.get(f"{API}/app").mock(return_value=httpx.Response(200, json={"slug": "autodev-bot"}))
    cfg = make_cfg(tmp_path, github={"auth": "app", "app_id": 42, "installation_id": 7, "private_key_path": str(kp)})
    assert await bot_login(cfg) == "autodev-bot[bot]"
```

Check the timestamp constant before relying on it: `python -c "import datetime;print(datetime.datetime(2026,10,10,12,tzinfo=datetime.timezone.utc).timestamp())"` should print `1791633600.0`. If it doesn't, use the printed value.

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_auth.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.auth'`

- [ ] **Step 3: Write minimal implementation**

`autodev/auth.py`:
```python
"""Who autodev is on GitHub. token: a personal token (run as yourself). app: a GitHub App installation (service)."""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

_HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def app_jwt(app_id: int, pem: bytes, now: float | None = None) -> str:
    """RS256 JWT for GitHub App auth: valid 9 minutes, iat backdated 60 s for clock drift."""
    now = int(time.time() if now is None else now)
    header = _b64(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64(json.dumps({"iat": now - 60, "exp": now + 540, "iss": str(app_id)}).encode())
    key = serialization.load_pem_private_key(pem, password=None)
    sig = key.sign(f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{payload}.{_b64(sig)}"


class AppTokens:
    """Installation tokens, minted on demand and reused until 5 minutes before they expire."""

    def __init__(self, app_id: int, installation_id: int, key_path: Path, api_url: str = "https://api.github.com",
                 clock: Callable[[], float] = time.time, http: httpx.Client | None = None):
        self._app_id, self._inst, self._key_path, self._api = app_id, installation_id, Path(key_path), api_url
        self._clock = clock
        self._http = http or httpx.Client(timeout=30)
        self._token: str | None = None
        self._expires = 0.0
        self._lock = threading.Lock()

    def __call__(self) -> str:
        with self._lock:
            if self._token is None or self._clock() > self._expires - 300:
                # ponytail: a sync HTTP call about once an hour from inside the event loop; fine at this rate.
                jwt = app_jwt(self._app_id, self._key_path.read_bytes(), now=self._clock())
                r = self._http.post(f"{self._api}/app/installations/{self._inst}/access_tokens",
                                    headers={**_HEADERS, "Authorization": f"Bearer {jwt}"})
                r.raise_for_status()
                body = r.json()
                self._token = body["token"]
                self._expires = datetime.fromisoformat(body["expires_at"]).timestamp()
            return self._token


def token_fn(cfg) -> Callable[[], str]:
    g = cfg.github
    if g.auth == "app":
        return AppTokens(g.app_id, g.installation_id, g.private_key_path, g.api_url)

    def from_env_or_file() -> str:
        if tok := os.environ.get(g.token_env):
            return tok
        if g.token_file and g.token_file.is_file():
            return g.token_file.read_text().strip()
        raise RuntimeError(f"no GitHub token: set ${g.token_env} (or put it in .env), or github.token_file")

    return from_env_or_file


async def bot_login(cfg) -> str:
    """The login autodev's own comments, commits and PRs appear under."""
    g = cfg.github
    async with httpx.AsyncClient(base_url=g.api_url, timeout=30, headers=_HEADERS) as c:
        if g.auth == "token":
            r = await c.get("/user", headers={"Authorization": f"Bearer {token_fn(cfg)()}"})
            r.raise_for_status()
            return r.json()["login"]
        jwt = app_jwt(g.app_id, g.private_key_path.read_bytes())
        r = await c.get("/app", headers={"Authorization": f"Bearer {jwt}"})
        r.raise_for_status()
        return f"{r.json()['slug']}[bot]"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_auth.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add autodev/auth.py tests/test_autodev_auth.py
git commit -m "autodev: GitHub identity from a personal token or a GitHub App

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Reading PRs: approvals, feedback, checks; GitHub PR calls

**Files:**
- Create: `autodev/models.py` (only `Feedback` for now; Task 4 adds the rest), `autodev/review.py`, `autodev/github.py`, `tests/test_autodev_review.py`

**Interfaces:**
- Consumes: `token_fn` (Task 2); `zarr_doctor.github_api.GitHub`, which provides `request`, `_paged`, `comment_once`, `get_issue`, `update_issue`, `list_issues`, `add_labels`, `remove_label` and `ensure_label`.
- Produces:
  - `models.Feedback(id: str, text: str)`
  - `review.WRITE_PERMISSIONS: set[str]`
  - `review.approved(head_sha, reviews, comments, perms, bot, allow_self, marker_prefix) -> bool`
  - `review.feedback(reviews, review_comments, comments, perms, bot, allow_self, marker_prefix) -> list[Feedback]`
  - `review.handled_ids(comments, marker_prefix) -> set[str]`
  - `review.combine_checks(check_runs, statuses) -> str` returning one of `success | pending | failure | none`
  - `github.GitHub`: a subclass with `for_autodev(cfg, tokens=None)`, `tokens`, `permission(login) -> str`, `label_actor(issue, label) -> str | None`, `issue_comments(n) -> list[dict]`, `find_pr(branch) -> dict | None`, `create_pr(title, body, head, base) -> dict`, `get_pr(n) -> dict`, `pr_reviews(n)`, `pr_review_comments(n)`, `check_state(sha) -> str`, `merge_pr(n, sha) -> tuple[bool, str]` and `delete_branch(branch) -> None`

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_review.py`:
```python
import httpx
import pytest
import respx

from autodev.github import GitHub
from autodev.models import Feedback
from autodev.review import approved, combine_checks, feedback, handled_ids

from autodev_helpers import API, R

MP = "<!-- autodev:"
HEAD = "abc1234def5678"
W = {"alice": "write", "bob": "read", "bot": "write", "admin": "admin"}


def rv(id, login, state, commit=HEAD, body="", at="2026-10-10T01:00:00Z"):
    return {"id": id, "user": {"login": login}, "state": state, "commit_id": commit, "body": body,
            "submitted_at": at}


def cm(id, login, body, at="2026-10-10T02:00:00Z"):
    return {"id": id, "user": {"login": login}, "body": body, "created_at": at}


def ok(reviews=(), comments=(), bot="bot", allow_self=False):
    return approved(HEAD, list(reviews), list(comments), W, bot, allow_self, MP)


def test_approval_must_match_head():
    assert ok([rv(1, "alice", "APPROVED")])
    assert not ok([rv(1, "alice", "APPROVED", commit="0ld")])


def test_approval_needs_write_access_and_a_human():
    assert not ok([rv(1, "bob", "APPROVED")])
    assert not ok([rv(1, "bot", "APPROVED")])
    assert ok([rv(1, "bot", "APPROVED")], allow_self=True)
    assert not ok([rv(1, None, "APPROVED")])  # deleted account


def test_later_changes_requested_supersedes_approval():
    assert not ok([rv(1, "alice", "APPROVED"), rv(2, "alice", "CHANGES_REQUESTED")])
    assert ok([rv(1, "alice", "APPROVED"), rv(2, "alice", "COMMENTED")])


def test_approve_comment_with_sha_prefix():
    assert ok(comments=[cm(1, "alice", "/approve abc1234")])
    assert not ok(comments=[cm(1, "alice", "/approve abc123")])  # too short
    assert not ok(comments=[cm(1, "alice", "/approve 0ld1234")])
    assert not ok(comments=[cm(1, "alice", f"/approve abc1234 {MP} x -->")])  # our own comment
    assert not ok(comments=[cm(1, "bob", "/approve abc1234")])


def test_feedback_from_reviews_and_revise_comments_oldest_first():
    reviews = [rv(3, "alice", "CHANGES_REQUESTED", body="rename foo", at="2026-10-10T01:00:00Z"),
               rv(4, "bob", "CHANGES_REQUESTED", body="ignored: reader")]
    inline = [{"pull_request_review_id": 3, "path": "a.py", "line": 4, "body": "here"},
              {"pull_request_review_id": 99, "path": "b.py", "line": 1, "body": "other review"}]
    comments = [cm(10, "admin", "/revise also update docs", at="2026-10-10T00:30:00Z"),
                cm(11, "alice", "looks good"), cm(12, "bot", f"/revise {MP} x -->")]
    fb = feedback(reviews, inline, comments, W, "bot", False, MP)
    assert fb == [Feedback("comment-10", "also update docs"),
                  Feedback("review-3", "rename foo\n- `a.py` line 4: here")]


def test_handled_ids_come_from_our_markers():
    comments = [cm(1, "bot", "done\n<!-- autodev: round k-2 -->\n<!-- autodev: handled review-3 -->"),
                cm(2, "alice", "<!-- autodev: handled review-9 -->")]  # a person pasting our marker also counts
    assert handled_ids(comments, MP) >= {"review-3"}


@pytest.mark.parametrize("runs,statuses,want", [
    ([], [], "none"),
    ([{"status": "completed", "conclusion": "success"}], [{"state": "success"}], "success"),
    ([{"status": "in_progress", "conclusion": None}], [], "pending"),
    ([{"status": "completed", "conclusion": "skipped"}], [{"state": "pending"}], "pending"),
    ([{"status": "completed", "conclusion": "failure"}], [{"state": "pending"}], "failure"),
    ([], [{"state": "error"}], "failure"),
])
def test_combine_checks(runs, statuses, want):
    assert combine_checks(runs, statuses) == want


@pytest.fixture
def gh():
    async def no_sleep(_):
        pass
    return GitHub("o/r", lambda: "tok", writes_per_second=1000, sleep=no_sleep)


@respx.mock
async def test_label_actor_is_latest_labeler(gh):
    respx.get(f"{API}{R}/issues/1/events").mock(return_value=httpx.Response(200, json=[
        {"event": "labeled", "label": {"name": "autodev:go"}, "actor": {"login": "a"}},
        {"event": "unlabeled", "label": {"name": "autodev:go"}, "actor": {"login": "a"}},
        {"event": "labeled", "label": {"name": "autodev:go"}, "actor": {"login": "b"}},
        {"event": "labeled", "label": {"name": "other"}, "actor": {"login": "c"}},
    ]))
    assert await gh.label_actor(1, "autodev:go") == "b"


@respx.mock
async def test_permission_of_stranger_is_none(gh):
    respx.get(f"{API}{R}/collaborators/eve/permission").mock(return_value=httpx.Response(404, json={}))
    assert await gh.permission("eve") == "none"


@respx.mock
async def test_merge_refusal_is_reported_not_raised(gh):
    respx.put(f"{API}{R}/pulls/5/merge").mock(return_value=httpx.Response(405, json={"message": "Not mergeable"}))
    merged, msg = await gh.merge_pr(5, HEAD)
    assert not merged and "405" in msg


@respx.mock
async def test_check_state_reads_both_apis(gh):
    respx.get(f"{API}{R}/commits/{HEAD}/check-runs").mock(return_value=httpx.Response(200, json={
        "check_runs": [{"status": "completed", "conclusion": "success"}]}))
    respx.get(f"{API}{R}/commits/{HEAD}/status").mock(return_value=httpx.Response(200, json={"statuses": []}))
    assert await gh.check_state(HEAD) == "success"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_review.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.github'`

- [ ] **Step 3: Write minimal implementation**

`autodev/models.py` (first version):
```python
"""Dataclasses shared by workflows and activities (plain, so Temporal serializes them as JSON)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Feedback:
    id: str  # "review-<id>" | "comment-<id>"
    text: str
```

`autodev/review.py`:
```python
"""Pure readers of GitHub PR data: who approved which commit, what change requests are pending, how CI looks."""

from __future__ import annotations

import re

from .models import Feedback

WRITE_PERMISSIONS = {"admin", "maintain", "write"}
APPROVE_RE = re.compile(r"^/approve\s+([0-9a-f]{7,40})\b", re.M)
REVISE_RE = re.compile(r"^/revise\b(.*)", re.M | re.S)
_HANDLED_RE = re.compile(r"handled (\S+) -->")
_FAILED = {"failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"}


def _login(x: dict) -> str | None:
    return (x.get("user") or {}).get("login")


def _human(login: str | None, perms: dict[str, str], bot: str, allow_self: bool) -> bool:
    """A person with write access. `allow_self`: under a personal token you are also the bot."""
    return bool(login) and perms.get(login) in WRITE_PERMISSIONS and (allow_self or login != bot)


def _ours(body: str | None, marker_prefix: str) -> bool:
    return marker_prefix in (body or "")


def approved(head_sha: str, reviews: list[dict], comments: list[dict], perms: dict[str, str], bot: str,
             allow_self: bool, marker_prefix: str) -> bool:
    latest: dict[str, dict] = {}
    for r in reviews:  # GitHub returns reviews oldest first; a reviewer's latest verdict counts
        if r.get("state") in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED") and _login(r):
            latest[_login(r)] = r
    if any(r["state"] == "APPROVED" and r.get("commit_id") == head_sha and _human(login, perms, bot, allow_self)
           for login, r in latest.items()):
        return True
    for c in comments:
        if _ours(c.get("body"), marker_prefix):
            continue
        m = APPROVE_RE.search(c.get("body") or "")
        if m and head_sha.startswith(m.group(1)) and _human(_login(c), perms, bot, allow_self):
            return True
    return False


def feedback(reviews: list[dict], review_comments: list[dict], comments: list[dict], perms: dict[str, str],
             bot: str, allow_self: bool, marker_prefix: str) -> list[Feedback]:
    """Change requests from people with write access, oldest first."""
    items: list[tuple[str, Feedback]] = []
    for r in reviews:
        if r.get("state") != "CHANGES_REQUESTED" or not _human(_login(r), perms, bot, allow_self):
            continue
        inline = [f"- `{c.get('path')}` line {c.get('line') or c.get('original_line')}: {c.get('body')}"
                  for c in review_comments if c.get("pull_request_review_id") == r["id"]]
        text = "\n".join(filter(None, [r.get("body") or "", *inline])) or "(changes requested, no details)"
        items.append((r.get("submitted_at") or "", Feedback(f"review-{r['id']}", text)))
    for c in comments:
        if _ours(c.get("body"), marker_prefix):
            continue
        m = REVISE_RE.search(c.get("body") or "")
        if m and _human(_login(c), perms, bot, allow_self):
            items.append((c.get("created_at") or "", Feedback(f"comment-{c['id']}", m.group(1).strip() or "(no details)")))
    return [f for _, f in sorted(items, key=lambda t: t[0])]


def handled_ids(comments: list[dict], marker_prefix: str) -> set[str]:
    """Feedback ids that a round already addressed, from the markers in our summary comments."""
    out: set[str] = set()
    for c in comments:
        body = c.get("body") or ""
        if _ours(body, marker_prefix):
            out.update(_HANDLED_RE.findall(body))
    return out


def combine_checks(check_runs: list[dict], statuses: list[dict]) -> str:
    states = ["pending" if r.get("status") != "completed" else
              "failure" if r.get("conclusion") in _FAILED else "success" for r in check_runs]
    states += [{"success": "success", "pending": "pending"}.get(s.get("state"), "failure") for s in statuses]
    if not states:
        return "none"
    return next((s for s in ("failure", "pending") if s in states), "success")
```

The `handled_ids` test asserts `>=` on purpose. A human comment containing our marker also counts, but only people with repo access can comment that way, and the worst case is that one request gets skipped. That is acceptable.

`autodev/github.py`:
```python
"""The GitHub calls autodev adds to zarr-doctor's client (which brings rate limits, throttling, comment_once)."""

from __future__ import annotations

from zarr_doctor.github_api import GitHub as _Base
from zarr_doctor.github_api import GitHubError

from .auth import token_fn
from .review import combine_checks

__all__ = ["GitHub", "GitHubError"]


class GitHub(_Base):
    @classmethod
    def for_autodev(cls, cfg, tokens=None) -> "GitHub":
        return cls(cfg.github.repo, tokens or token_fn(cfg), api_url=cfg.github.api_url)

    @property
    def tokens(self):
        return self._token_fn

    async def permission(self, login: str) -> str:
        r = await self.request("GET", f"/repos/{self.repo}/collaborators/{login}/permission", ok404=True)
        return r.json()["permission"] if r else "none"

    async def label_actor(self, issue: int, label: str) -> str | None:
        """Login of whoever most recently added `label` to the issue."""
        actor = None
        async for e in self._paged(f"/repos/{self.repo}/issues/{issue}/events"):
            if e.get("event") == "labeled" and (e.get("label") or {}).get("name") == label:
                actor = (e.get("actor") or {}).get("login")
        return actor

    async def issue_comments(self, number: int) -> list[dict]:
        return [c async for c in self._paged(f"/repos/{self.repo}/issues/{number}/comments")]

    async def find_pr(self, branch: str) -> dict | None:
        owner = self.repo.split("/")[0]
        prs = (await self.request("GET", f"/repos/{self.repo}/pulls",
                                  params={"head": f"{owner}:{branch}", "state": "open"})).json()
        return prs[0] if prs else None

    async def create_pr(self, title: str, body: str, head: str, base: str) -> dict:
        return (await self.request("POST", f"/repos/{self.repo}/pulls",
                                   json={"title": title, "body": body, "head": head, "base": base})).json()

    async def get_pr(self, number: int) -> dict:
        return (await self.request("GET", f"/repos/{self.repo}/pulls/{number}")).json()

    async def pr_reviews(self, number: int) -> list[dict]:
        return [r async for r in self._paged(f"/repos/{self.repo}/pulls/{number}/reviews")]

    async def pr_review_comments(self, number: int) -> list[dict]:
        return [c async for c in self._paged(f"/repos/{self.repo}/pulls/{number}/comments")]

    async def check_state(self, sha: str) -> str:
        runs = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/check-runs",
                                   params={"per_page": 100})).json()["check_runs"]
        statuses = (await self.request("GET", f"/repos/{self.repo}/commits/{sha}/status")).json()["statuses"]
        return combine_checks(runs, statuses)

    async def merge_pr(self, number: int, sha: str) -> tuple[bool, str]:
        """Squash-merge exactly `sha`. GitHub refusing (protection, conflict, moved head) is a result, not an error."""
        try:
            await self.request("PUT", f"/repos/{self.repo}/pulls/{number}/merge",
                               json={"sha": sha, "merge_method": "squash"})
            return True, "merged"
        except GitHubError as e:
            if e.status in (405, 409, 422):
                return False, str(e)
            raise

    async def delete_branch(self, branch: str) -> None:
        try:
            await self.request("DELETE", f"/repos/{self.repo}/git/refs/heads/{branch}", ok404=True)
        except GitHubError as e:
            if e.status != 422:  # already gone
                raise
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_review.py -v`
Expected: 16 passed (6 parametrized checks + 10 others)

- [ ] **Step 5: Commit**

```bash
git add autodev/models.py autodev/review.py autodev/github.py tests/test_autodev_review.py
git commit -m "autodev: read approvals, change requests and checks from PRs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Workflow data and the `decide()` policy

**Files:**
- Modify: `autodev/models.py` (add the remaining dataclasses)
- Create: `autodev/decide.py`, `tests/test_autodev_decide.py`

**Interfaces:**
- Consumes: `Feedback` (Task 3).
- Produces, all in `models.py` (full listing below):
  - `IssueInput`, `StageInput`, `CommentInput`, `RoundInput`, `Workspace`, `SubmitInput`, `JobRef`, `AwaitInput`, `JobOutcome`
  - `PublishInput`, `PrRef`, `ObserveInput`, `PrSnapshot`, `MergeInput`, `MergeResult`, `FailureInput`, `Status`
- Also produces, in `decide.py`: the constants `DONE, CLOSED, MERGE, REVISE, ESCALATE, WAIT`, and `decide(snap: PrSnapshot, handled: list[str], revisions: int, max_revisions: int) -> tuple[str, Feedback | str | None]`. The detail is a `Feedback` for `REVISE` and a reason `str` for `ESCALATE`.

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_decide.py`:
```python
from autodev.decide import CLOSED, DONE, ESCALATE, MERGE, REVISE, WAIT, decide
from autodev.models import Feedback, PrSnapshot

F1, F2 = Feedback("review-1", "a"), Feedback("comment-2", "b")


def snap(state="open", approved=False, checks="none", feedback=()):
    return PrSnapshot(5, state, "h", approved, checks, list(feedback))


def test_terminal_states():
    assert decide(snap("merged"), [], 0, 3) == (DONE, None)
    assert decide(snap("closed", approved=True), [], 0, 3) == (CLOSED, None)


def test_unhandled_feedback_wins_over_approval():
    assert decide(snap(approved=True, checks="success", feedback=[F1, F2]), ["review-1"], 1, 3) == (REVISE, F2)


def test_approval_with_passing_or_no_checks_merges():
    assert decide(snap(approved=True, checks="success"), [], 0, 3) == (MERGE, None)
    assert decide(snap(approved=True, checks="none"), [], 0, 3) == (MERGE, None)


def test_stale_approval_waits():
    # PrSnapshot.approved is computed against the current head, so a pre-revision approval arrives as False.
    assert decide(snap(approved=False, checks="success"), [], 1, 3) == (WAIT, None)


def test_approved_but_pending_waits_and_failing_escalates():
    assert decide(snap(approved=True, checks="pending"), [], 0, 3) == (WAIT, None)
    assert decide(snap(approved=True, checks="failure"), [], 0, 3)[0] == ESCALATE


def test_revision_limit_escalates_unless_approved():
    assert decide(snap(feedback=[F1]), [], 3, 3) == (ESCALATE, "revision limit (3) reached")
    assert decide(snap(approved=True, feedback=[F1]), [], 3, 3) == (MERGE, None)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_decide.py -v`
Expected: FAIL with `ImportError: cannot import name 'PrSnapshot'`

- [ ] **Step 3: Write minimal implementation**

`autodev/models.py` (full file):
```python
"""Dataclasses shared by workflows and activities (plain, so Temporal serializes them as JSON)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Feedback:
    id: str  # "review-<id>" | "comment-<id>"
    text: str


@dataclass
class IssueInput:
    issue: int
    max_revisions: int = 3
    poll_minutes: int = 2
    key: str = ""  # set on first execution; names this attempt's workspaces, job ids and refs
    round: int = 0
    pr: int | None = None
    revisions: int = 0
    handled: list[str] = field(default_factory=list)  # carried across continue_as_new


@dataclass
class StageInput:
    issue: int
    stage: str | None  # a key of config.STAGES, or None to remove every stage label


@dataclass
class CommentInput:
    number: int  # issue or PR
    body: str
    marker: str  # posted at most once per marker


@dataclass
class RoundInput:
    issue: int
    key: str
    round: int
    pr: int | None
    feedback: str | None


@dataclass
class Workspace:
    dir: str  # round directory: prompt.md, repo/, result.*, exit_code
    base_sha: str
    branch: str


@dataclass
class SubmitInput:
    issue: int
    key: str
    round: int
    workspace: Workspace


@dataclass
class JobRef:
    job_id: str
    walltime_min: int


@dataclass
class AwaitInput:
    job_id: str
    workspace: Workspace


@dataclass
class JobOutcome:
    job_id: str
    state: str  # csub/LSF: DONE | EXIT | UNKNOWN
    exit_code: int | None = None


@dataclass
class PublishInput:
    issue: int
    key: str
    round: int
    workspace: Workspace
    pr: int | None
    feedback_id: str | None = None


@dataclass
class PrRef:
    pr: int | None
    head_sha: str | None


@dataclass
class ObserveInput:
    pr: int


@dataclass
class PrSnapshot:
    pr: int
    state: str  # open | closed | merged
    head_sha: str
    approved: bool  # a qualifying human approval of head_sha exists
    checks: str  # success | pending | failure | none
    feedback: list[Feedback] = field(default_factory=list)  # not yet handled per GitHub markers


@dataclass
class MergeInput:
    issue: int
    pr: int
    sha: str


@dataclass
class MergeResult:
    merged: bool
    message: str


@dataclass
class FailureInput:
    issue: int
    key: str
    round: int
    error: str
    workspace: Workspace | None = None


@dataclass
class Status:
    issue: int
    state: str
    round: int
    pr: int | None
    job_id: str | None
    last_error: str | None
```

`autodev/decide.py`:
```python
"""What to do next with an open PR. Pure, so the workflow can call it and tests can table it."""

from __future__ import annotations

from .models import Feedback, PrSnapshot

DONE, CLOSED, MERGE, REVISE, ESCALATE, WAIT = "DONE", "CLOSED", "MERGE", "REVISE", "ESCALATE", "WAIT"


def decide(snap: PrSnapshot, handled: list[str], revisions: int,
           max_revisions: int) -> tuple[str, Feedback | str | None]:
    if snap.state == "merged":
        return DONE, None
    if snap.state == "closed":
        return CLOSED, None
    pending = [f for f in snap.feedback if f.id not in handled]
    if pending and revisions < max_revisions:
        return REVISE, pending[0]
    if snap.approved:
        if snap.checks in ("success", "none"):
            return MERGE, None
        if snap.checks == "failure":
            return ESCALATE, "approved, but checks are failing"
    if pending:
        return ESCALATE, f"revision limit ({max_revisions}) reached"
    return WAIT, None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_decide.py tests/test_autodev_review.py -v`
Expected: all passed

- [ ] **Step 5: Commit**

```bash
git add autodev/models.py autodev/decide.py tests/test_autodev_decide.py
git commit -m "autodev: workflow dataclasses and the pure decide() policy

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Trusted git: mirror, workspaces, publishing a bundle

**Files:**
- Create: `autodev/gitops.py`, `tests/test_autodev_gitops.py`
- Modify: `tests/autodev_helpers.py` (add `run_git`, `make_origin`, `agent_commit`)

**Interfaces:**
- Produces:
  - `GitError(RuntimeError)`, and `ProtectedPathError(GitError)`
  - `git(*args, cwd=None, token=None) -> str`
  - `Mirror(path: Path, url: str, tokens: Callable[[], str | None] = lambda: None)`
  - `Mirror.sync() -> None`
  - `Mirror.workspace(dest: Path, start: str, branch: str, agent_url: str) -> str`: returns the base sha.
  - `Mirror.publish(bundle: Path, base_sha: str, branch: str, ref: str, protected: list[str], force: bool) -> str | None`: returns the new head sha, or `None` when the agent made no commits.
  - test helpers `make_origin(tmp_path) -> Path` and `agent_commit(repo: Path, path: str, text: str, base_sha: str) -> str`

- [ ] **Step 1: Write the failing test**

Append to `tests/autodev_helpers.py`:
```python
import subprocess

GIT_ID = ["-c", "user.name=t", "-c", "user.email=t@t"]


def run_git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def make_origin(tmp_path: Path) -> Path:
    """A bare 'GitHub' repo with main = README.md + .github/workflows/ci.yml."""
    src = tmp_path / "src"
    (src / ".github" / "workflows").mkdir(parents=True)
    (src / "README.md").write_text("hi\n")
    (src / ".github" / "workflows" / "ci.yml").write_text("on: push\n")
    run_git("init", "-q", "-b", "main", cwd=src)
    run_git("add", "-A", cwd=src)
    run_git(*GIT_ID, "commit", "-qm", "init", cwd=src)
    origin = tmp_path / "origin.git"
    run_git("clone", "-q", "--bare", str(src), str(origin), cwd=tmp_path)
    return origin


def agent_commit(repo: Path, path: str, text: str, base_sha: str) -> str:
    """What an agent job does: commit, then bundle base..HEAD next to the repo. Returns the new HEAD."""
    p = repo / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    run_git("add", "-A", cwd=repo)
    run_git("commit", "-qm", f"edit {path}", cwd=repo)
    run_git("bundle", "create", "-q", str(repo.parent / "result.bundle"), f"{base_sha}..HEAD", cwd=repo)
    return run_git("rev-parse", "HEAD", cwd=repo)
```

`tests/test_autodev_gitops.py`:
```python
import pytest

from autodev.gitops import GitError, Mirror, ProtectedPathError

from autodev_helpers import agent_commit, make_origin, run_git

URL = "https://github.com/o/r.git"


@pytest.fixture
def setup(tmp_path):
    origin = make_origin(tmp_path)
    m = Mirror(tmp_path / "state" / "mirror.git", str(origin))
    m.sync()
    ws = tmp_path / "work" / "round-1"
    base = m.workspace(ws, "main", "autodev/issue-7", URL)
    return origin, m, ws, base


def test_round_trip_pushes_agent_commits(setup):
    origin, m, ws, base = setup
    assert run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=ws / "repo") == "autodev/issue-7"
    assert run_git("remote", "get-url", "origin", cwd=ws / "repo") == URL
    head = agent_commit(ws / "repo", "feature.txt", "x\n", base)
    got = m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [".github/"], force=True)
    assert got == head
    assert run_git("rev-parse", "refs/heads/autodev/issue-7", cwd=origin) == head


def test_no_bundle_means_no_commits(setup):
    origin, m, ws, base = setup
    assert m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [], force=True) is None


def test_protected_path_refused_and_nothing_pushed(setup):
    origin, m, ws, base = setup
    agent_commit(ws / "repo", ".github/workflows/ci.yml", "on: [push, pull_request]\n", base)
    with pytest.raises(ProtectedPathError, match="ci.yml"):
        m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [".github/"], force=True)
    with pytest.raises(Exception):
        run_git("rev-parse", "--verify", "refs/heads/autodev/issue-7", cwd=origin)


def test_commits_must_descend_from_base(setup):
    origin, m, ws, base = setup
    repo = ws / "repo"
    run_git("checkout", "-q", "--orphan", "evil", cwd=repo)
    (repo / "x.txt").write_text("x")
    run_git("add", "-A", cwd=repo)
    run_git("commit", "-qm", "orphan", cwd=repo)
    run_git("bundle", "create", "-q", str(ws / "result.bundle"), "HEAD", cwd=repo)
    with pytest.raises(GitError, match="descend"):
        m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [], force=True)


def test_workspace_is_independent_of_mirror(setup):
    _, m, ws, _ = setup
    objects = [p for p in (ws / "repo" / ".git" / "objects").rglob("*") if p.is_file()]
    assert objects and all(p.stat().st_nlink == 1 for p in objects)
    assert not (ws / "repo" / ".git" / "objects" / "info" / "alternates").exists()


def test_workspace_hooks_never_run(setup, tmp_path):
    origin, m, ws, base = setup
    sentinel = tmp_path / "pwned"
    hook = f"#!/bin/sh\ntouch {sentinel}\n"
    for name in ("pre-push", "post-checkout", "reference-transaction", "post-merge"):
        h = ws / "repo" / ".git" / "hooks" / name
        h.write_text(hook)
        h.chmod(0o755)
    run_git("config", "core.hooksPath", str(ws / "repo" / ".git" / "hooks"), cwd=ws / "repo")
    agent_commit(ws / "repo", "f.txt", "y", base)
    sentinel.unlink(missing_ok=True)  # agent_commit itself ran the agent's own hooks inside the "job"
    m.sync()
    m.publish(ws / "result.bundle", base, "autodev/issue-7", "refs/autodev/7/k-r1", [], force=True)
    assert not sentinel.exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_gitops.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.gitops'`

- [ ] **Step 3: Write minimal implementation**

`autodev/gitops.py`:
```python
"""Git on the trusted side. The worker owns a bare mirror that no job ever sees. Each agent round gets a fresh
clone of it (no hardlinks back into the mirror) and hands back a bundle, which is only ever read as data."""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
from pathlib import Path
from typing import Callable


class GitError(RuntimeError):
    pass


class ProtectedPathError(GitError):
    pass


def _env(token: str | None) -> dict[str, str]:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    if token:
        cred = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        # Via env only: never in argv (visible in ps) or a config file (left on disk).
        env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                   GIT_CONFIG_VALUE_0=f"AUTHORIZATION: basic {cred}")
    return env


def git(*args: str, cwd: Path | None = None, token: str | None = None) -> str:
    r = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", *args], cwd=cwd, env=_env(token),
                       capture_output=True, text=True)
    if r.returncode:
        raise GitError(f"git {' '.join(args)}: {r.stderr.strip()[-2000:]}")
    return r.stdout.strip()


class Mirror:
    def __init__(self, path: Path, url: str, tokens: Callable[[], str | None] = lambda: None):
        self.path, self.url, self.tokens = Path(path), url, tokens

    def sync(self) -> None:
        if not (self.path / "HEAD").exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            git("clone", "-q", "--bare", self.url, str(self.path), token=self.tokens())
        git("fetch", "-q", "--prune", "origin", "+refs/heads/*:refs/heads/*", cwd=self.path, token=self.tokens())

    def workspace(self, dest: Path, start: str, branch: str, agent_url: str) -> str:
        """Fresh clone of branch `start` at dest/repo, checked out as `branch`. Returns the base sha."""
        repo = Path(dest) / "repo"
        if repo.exists():  # a retried prepare; nothing has run in it yet
            shutil.rmtree(repo)
        repo.parent.mkdir(parents=True, exist_ok=True)
        git("clone", "-q", "--no-hardlinks", "--branch", start, str(self.path), str(repo))
        git("checkout", "-q", "-B", branch, cwd=repo)
        git("remote", "set-url", "origin", agent_url, cwd=repo)
        git("config", "user.name", "autodev", cwd=repo)
        git("config", "user.email", "autodev@localhost", cwd=repo)
        return git("rev-parse", "HEAD", cwd=repo)

    def publish(self, bundle: Path, base_sha: str, branch: str, ref: str, protected: list[str],
                force: bool) -> str | None:
        """Import the agent's bundle and push it to `branch`. Returns the new head, or None if no commits."""
        if not Path(bundle).is_file():
            return None
        git("fetch", "-q", str(bundle), f"+HEAD:{ref}", cwd=self.path)
        new = git("rev-parse", ref, cwd=self.path)
        if new == base_sha:
            return None
        try:
            git("merge-base", "--is-ancestor", base_sha, new, cwd=self.path)
        except GitError:
            raise GitError(f"agent commits do not descend from {base_sha[:12]}") from None
        changed = git("diff", "--name-only", base_sha, new, cwd=self.path).splitlines()
        if bad := [p for p in changed if any(p.startswith(pre) for pre in protected)]:
            raise ProtectedPathError(f"agent changed protected paths: {', '.join(bad[:10])}")
        git("push", "-q", "origin", f"{'+' if force else ''}{new}:refs/heads/{branch}", cwd=self.path,
            token=self.tokens())
        git("update-ref", f"refs/heads/{branch}", new, cwd=self.path)
        return new
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_gitops.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add autodev/gitops.py tests/test_autodev_gitops.py tests/autodev_helpers.py
git commit -m "autodev: trusted git mirror, credential-free workspaces, bundle publishing

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The agent round: prompt, job script, csub submit and wait

**Files:**
- Create: `autodev/agent.py`, `tests/test_autodev_agent.py`

**Interfaces:**
- Consumes: `Config` (Task 1); `Workspace`, `JobRef` and `JobOutcome` (Task 4); `Mirror.workspace` and test helpers (Task 5).
- Produces:
  - `render_prompt(cfg, issue: dict, comments: list[dict], branch: str, base_sha: str, pr: int | None, feedback: str | None) -> str`
  - `job_script(cfg, base_sha: str) -> str`
  - `read_result(d: Path, limit: int = 20000) -> tuple[str, float | None]`
  - `log_tail(d: Path, n: int = 40) -> str`
  - `csub_client(cfg)`
  - `AgentJobs(cfg, client_factory=None, sleep=asyncio.sleep, poll_s=60.0)`, with:
    - `async submit(issue, key, rnd, ws: Workspace) -> JobRef`
    - `async wait(job_id, ws: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome`

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_agent.py`:
```python
import json
import subprocess
from types import SimpleNamespace

from autodev.agent import AgentJobs, job_script, log_tail, read_result, render_prompt
from autodev.gitops import Mirror
from autodev.models import Workspace

from autodev_helpers import make_cfg, make_origin, run_git

ISSUE = {"number": 7, "title": "Add feature", "body": "Please add feature.txt"}


def test_prompt_has_issue_and_human_discussion_only(tmp_path):
    cfg = make_cfg(tmp_path)
    comments = [{"user": {"login": "alice"}, "body": "use lowercase"},
                {"user": {"login": "bot"}, "body": "Started <!-- autodev: started k-1 -->"}]
    p = render_prompt(cfg, ISSUE, comments, "autodev/issue-7", "abc", None, None)
    assert "#7 in o/r" in p and "Add feature" in p and "Please add feature.txt" in p
    assert "use lowercase" in p and "Started" not in p and "Requested changes" not in p
    p2 = render_prompt(cfg, ISSUE, [], "autodev/issue-7", "abc", 5, "rename it")
    assert "Pull request #5" in p2 and "rename it" in p2


def test_job_script_runs_claude_and_bundles(tmp_path):
    """Run the real script with a fake `claude` on PATH, against a real clone."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "claude"
    fake.write_text('#!/bin/sh\necho x > feature.txt\ngit add feature.txt\ngit commit -qm "add feature"\n'
                    'echo \'{"type":"result","result":"Added feature.txt","total_cost_usd":0.12}\'\n')
    fake.chmod(0o755)
    cfg = make_cfg(tmp_path, agent={"path_prepend": [str(bin_dir)], "model": "it's-quoted"})
    m = Mirror(tmp_path / "mirror.git", str(make_origin(tmp_path)))
    m.sync()
    d = tmp_path / "round-1"
    base = m.workspace(d, "main", "autodev/issue-7", "https://example.invalid/r.git")
    (d / "prompt.md").write_text("do it")
    script = job_script(cfg, base)
    r = subprocess.run(["bash", "-c", script], cwd=d)
    assert r.returncode == 0
    assert (d / "exit_code").read_text().strip() == "0"
    assert read_result(d) == ("Added feature.txt", 0.12)
    assert run_git("bundle", "list-heads", str(d / "result.bundle"), cwd=d)


def test_read_result_tolerates_garbage(tmp_path):
    assert read_result(tmp_path)[0].startswith("(the agent left no readable result.json")
    (tmp_path / "result.json").write_text("[1, 2]")
    assert read_result(tmp_path)[1] is None
    (tmp_path / "result.json").write_text(json.dumps({"result": "x" * 50000, "total_cost_usd": "free"}))
    text, cost = read_result(tmp_path)
    assert len(text) < 20100 and text.endswith("(truncated)") and cost is None


def test_log_tail_reads_claude_and_job_stderr(tmp_path):
    (tmp_path / "claude.err").write_text("\n".join(f"line {i}" for i in range(100)))
    (tmp_path / ".csub" / "jobs" / "9").mkdir(parents=True)
    (tmp_path / ".csub" / "jobs" / "9" / "stderr").write_text("bwrap: oops\n")
    t = log_tail(tmp_path, n=5)
    assert "line 99" in t and "line 90" not in t and "bwrap: oops" in t


class FakeCsub:
    def __init__(self, states=("DONE",), existing=()):
        self.submitted, self.states, self.existing = [], list(states), list(existing)

    def submit(self, spec):
        self.submitted.append(spec)
        return SimpleNamespace(job_id="777", walltime_min=spec["walltime_min"])

    def status(self, ids=None):
        if ids is None:
            return self.existing
        st = self.states.pop(0) if len(self.states) > 1 else self.states[0]
        return [SimpleNamespace(job_id=ids[0], state=st, exit_code=0 if st == "DONE" else None, name=None,
                                terminal=st in ("DONE", "EXIT", "UNKNOWN"))]


def _jobs(tmp_path, fake, **agent):
    sleeps = []

    async def sleep(s):
        sleeps.append(s)

    j = AgentJobs(make_cfg(tmp_path, agent=agent), client_factory=lambda: fake, sleep=sleep)
    j.sleeps = sleeps
    return j


async def test_submit_is_idempotent_and_scoped(tmp_path):
    fake = FakeCsub()
    j = _jobs(tmp_path, fake, extra_mounts=["/tools:ro"], allow_hosts=["pypi.org"])
    ws = Workspace(str(tmp_path / "work" / "r1"), "abc123", "autodev/issue-7")
    ref = await j.submit(7, "k", 1, ws)
    assert ref.job_id == "777" and await j.submit(7, "k", 1, ws) == ref
    assert len(fake.submitted) == 1
    spec = fake.submitted[0]
    assert spec["claude"] is True and spec["shell"] is True and spec["cwd"] == ws.dir
    assert spec["mounts"] == [{"path": ws.dir, "mode": "rw"}, {"path": "/tools", "mode": "ro"}]
    assert spec["allow_hosts"] == ["pypi.org"] and spec["name"] == "autodev-7-r1" and "env" not in spec


async def test_submit_reuses_job_found_by_name(tmp_path):
    fake = FakeCsub(existing=[SimpleNamespace(job_id="555", name="autodev-7-r1", state="RUN")])
    j = _jobs(tmp_path, fake)
    ref = await j.submit(7, "k", 1, Workspace(str(tmp_path), "abc", "b"))
    assert ref.job_id == "555" and fake.submitted == []


async def test_api_key_mode_passes_key_in_job_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SVC_KEY", "sk-1")
    fake = FakeCsub()
    await _jobs(tmp_path, fake, api_key_env="SVC_KEY").submit(7, "k", 1, Workspace(str(tmp_path), "a", "b"))
    assert fake.submitted[0]["env"] == {"ANTHROPIC_API_KEY": "sk-1"}


async def test_wait_heartbeats_until_terminal(tmp_path):
    fake = FakeCsub(states=["PEND", "RUN", "DONE"])
    j = _jobs(tmp_path, fake)
    beats = []
    out = await j.wait("777", Workspace(str(tmp_path), "a", "b"), beats.append)
    assert (out.state, out.exit_code) == ("DONE", 0)
    assert beats == ["PEND", "RUN", "DONE"] and j.sleeps == [60.0, 60.0]


async def test_unknown_state_uses_exit_code_file(tmp_path):
    (tmp_path / "exit_code").write_text("0\n")
    out = await _jobs(tmp_path, FakeCsub(states=["UNKNOWN"])).wait("777", Workspace(str(tmp_path), "a", "b"),
                                                                    lambda s: None)
    assert (out.state, out.exit_code) == ("DONE", 0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_agent.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.agent'`

- [ ] **Step 3: Write minimal implementation**

`autodev/agent.py`:
```python
"""One agent round = one csub job running Claude Code in a sandbox, in a workspace that holds no credentials."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
from pathlib import Path
from typing import Callable

from .models import JobOutcome, JobRef, Workspace

PROMPT = """You are implementing GitHub issue #{number} in {repo}. The repository is checked out in the current
directory on branch `{branch}` at commit {base_sha}.

# Issue: {title}

{body}
{discussion}{revision}
# How to work

- Read CLAUDE.md and README.md first, if present, and follow the repository's conventions.
- Make the change with tests. Run the test suite (`pixi run test` if the repo uses pixi) and make it pass.
- Commit your work with `git commit` (clear messages; several commits are fine). Do not push: you have no
  GitHub access, and your commits are collected when you exit.
- Do not modify anything under: {protected}.
- Finish with a short summary of what you changed and how you tested it. It is posted on the pull request.
- If the issue is unclear or cannot be done, make no commits and explain why in your summary.
"""


def render_prompt(cfg, issue: dict, comments: list[dict], branch: str, base_sha: str, pr: int | None,
                  feedback: str | None) -> str:
    human = [c for c in comments if cfg.marker_prefix not in (c.get("body") or "")]
    discussion = ("\n# Discussion on the issue\n\n" + "\n\n".join(
        f"**{(c.get('user') or {}).get('login')}**: {c.get('body')}" for c in human) + "\n") if human else ""
    revision = (f"\n# Requested changes\n\nPull request #{pr} has your earlier work on this branch. "
                f"A reviewer asked for:\n\n{feedback}\n") if feedback else ""
    return PROMPT.format(number=issue["number"], repo=cfg.github.repo, branch=branch, base_sha=base_sha,
                         title=issue.get("title") or "", body=issue.get("body") or "(no description)",
                         discussion=discussion, revision=revision,
                         protected=", ".join(cfg.github.protected_paths) or "(nothing)")


def job_script(cfg, base_sha: str) -> str:
    """Runs inside the csub sandbox with cwd = the round directory."""
    q = shlex.quote
    path = f'export PATH={":".join(q(p) for p in cfg.agent.path_prepend)}:"$PATH"\n' if cfg.agent.path_prepend else ""
    return f"""set -u
{path}cd repo
claude -p "$(cat ../prompt.md)" --model {q(cfg.agent.model)} --output-format json \\
  --allowedTools "Bash,Read,Edit,Write,Glob,Grep" > ../result.json 2> ../claude.err
rc=$?
git bundle create -q ../result.bundle {q(base_sha)}..HEAD 2>> ../claude.err
echo "$rc" > ../exit_code
exit "$rc"
"""


def read_result(d: Path, limit: int = 20000) -> tuple[str, float | None]:
    """The agent's summary and cost from result.json. Untrusted: any shape may be there."""
    try:
        data = json.loads((Path(d) / "result.json").read_text())
        text, cost = str(data.get("result") or ""), data.get("total_cost_usd")
    except (OSError, ValueError, AttributeError):
        return "(the agent left no readable result.json)", None
    if len(text) > limit:
        text = text[:limit] + "\n\n…(truncated)"
    return text or "(empty summary)", cost if isinstance(cost, (int, float)) else None


def log_tail(d: Path, n: int = 40) -> str:
    d = Path(d)
    parts = []
    for p in [d / "claude.err", *sorted((d / ".csub" / "jobs").glob("*/stderr"))]:
        if p.is_file() and (text := p.read_text(errors="replace").strip()):
            parts.append(f"== {p.relative_to(d)} ==\n" + "\n".join(text.splitlines()[-n:]))
    return "\n".join(parts)[-5000:]


def csub_client(cfg):
    import csub

    return csub.Client(csub.load_client_config().with_(session=cfg.deployment))


def _mount(spec: str) -> dict[str, str]:
    path, _, mode = spec.rpartition(":")
    return {"path": path, "mode": mode} if mode in ("ro", "rw") else {"path": spec, "mode": "ro"}


class AgentJobs:
    def __init__(self, cfg, client_factory=None, sleep=asyncio.sleep, poll_s: float = 60.0):
        self.cfg = cfg
        self._factory = client_factory or (lambda: csub_client(cfg))
        self._client = None
        self._sleep, self._poll_s = sleep, poll_s

    @property
    def client(self):
        if self._client is None:
            self._client = self._factory()
        return self._client

    def _id_file(self, issue: int, key: str, rnd: int) -> Path:
        return self.cfg.paths.state_dir / "jobs" / f"issue-{issue}-{key}-r{rnd}.id"

    async def submit(self, issue: int, key: str, rnd: int, ws: Workspace) -> JobRef:
        """Submit once per (issue, key, round); a retry returns the same job."""
        a = self.cfg.agent
        f = self._id_file(issue, key, rnd)
        if f.exists():
            return JobRef(f.read_text().strip(), a.walltime_min)
        name = f"{self.cfg.deployment}-{issue}-r{rnd}"
        found = [s for s in await asyncio.to_thread(self.client.status) if s.name == name]
        if found:  # submitted, then the worker died before writing the id file
            job_id = found[0].job_id
        else:
            spec = {"command": job_script(self.cfg, ws.base_sha), "shell": True, "cwd": ws.dir, "name": name,
                    "cpus": a.cpus, "mem_mb": a.mem_mb, "walltime_min": a.walltime_min,
                    "allow_hosts": list(a.allow_hosts), "claude": True, "session": self.cfg.deployment,
                    "mounts": [{"path": ws.dir, "mode": "rw"}, *(_mount(m) for m in a.extra_mounts)]}
            if a.api_key_env:
                spec["env"] = {"ANTHROPIC_API_KEY": os.environ[a.api_key_env]}
            job_id = (await asyncio.to_thread(self.client.submit, spec)).job_id
        f.parent.mkdir(parents=True, exist_ok=True)
        tmp = f.with_suffix(".tmp")
        tmp.write_text(job_id)
        tmp.replace(f)
        return JobRef(job_id, a.walltime_min)

    async def wait(self, job_id: str, ws: Workspace, heartbeat: Callable[[str], None]) -> JobOutcome:
        # ponytail: one csub status call per job per poll; batch across jobs if many run at once.
        while True:
            st = next(iter(await asyncio.to_thread(self.client.status, [job_id])), None)
            state = st.state if st else "UNKNOWN"
            heartbeat(state)
            if st is None or st.terminal:
                break
            await self._sleep(self._poll_s)
        code_file = Path(ws.dir) / "exit_code"
        code = int(code_file.read_text().strip()) if code_file.is_file() and code_file.read_text().strip().isdigit() else None
        if state == "UNKNOWN" and code is not None:  # LSF forgot the job, but the script ran to the end
            state = "DONE" if code == 0 else "EXIT"
        exit_code = st.exit_code if st and st.exit_code is not None else code
        return JobOutcome(job_id, state, exit_code)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_agent.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add autodev/agent.py tests/test_autodev_agent.py
git commit -m "autodev: agent rounds as csub Claude jobs (prompt, script, idempotent submit, wait)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: `IssueWorkflow` and `PollWorkflow`

**Files:**
- Create: `autodev/workflows.py`, `tests/test_autodev_workflows.py`

**Interfaces:**
- Consumes: models and `decide` (Task 4). Activities are called by name, with these names and signatures (Task 8 implements them):

| name | argument → result | queue |
|---|---|---|
| `set_stage` | `StageInput` → `None` | main |
| `comment` | `CommentInput` → `None` | main |
| `find_pr` | `int` (issue) → `PrRef` | main |
| `prepare_round` | `RoundInput` → `Workspace` | main |
| `submit_agent` | `SubmitInput` → `JobRef` | main |
| `await_agent` | `AwaitInput` → `JobOutcome` | `<task_queue>-await` |
| `publish_round` | `PublishInput` → `PrRef` | main |
| `observe_pr` | `ObserveInput` → `PrSnapshot` | main |
| `merge_pr` | `MergeInput` → `MergeResult` | main |
| `report_failure` | `FailureInput` → `None` | main |
| `poll_issues` | none → `list[int]` | main |

- Produces:
  - `IssueWorkflow.execute(IssueInput) -> str`, returning `"merged"`, `"closed"` or `"failed"`
  - the query `IssueWorkflow.status -> Status`
  - `PollWorkflow.execute() -> list[int]`
  - the module constants `ROUND_DONE_LOG` and `MAX_POLLS_PER_EXECUTION`

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_workflows.py`:
```python
"""IssueWorkflow on Temporal's time-skipping server, with every activity mocked."""

import uuid

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from autodev import workflows
from autodev.models import (AwaitInput, CommentInput, FailureInput, Feedback, IssueInput, JobOutcome, JobRef,
                            MergeInput, MergeResult, ObserveInput, PrRef, PrSnapshot, PublishInput, RoundInput,
                            StageInput, SubmitInput, Workspace)
from autodev.workflows import IssueWorkflow


@pytest.fixture(scope="session")
async def env():
    e = await WorkflowEnvironment.start_time_skipping()
    yield e
    await e.shutdown()


def snap(state="open", sha="h1", approved=False, checks="none", feedback=()):
    return PrSnapshot(5, state, sha, approved, checks, list(feedback))


class Mocks:
    def __init__(self, snaps, existing_pr=None, job_state="DONE", exit_code=0, publish_pr=5, await_failures=0,
                 merge_ok=True):
        self.calls: list[tuple[str, object]] = []
        self.snaps = list(snaps)
        self.existing_pr, self.job_state, self.exit_code = existing_pr, job_state, exit_code
        self.publish_pr, self.await_failures, self.merge_ok = publish_pr, await_failures, merge_ok

    def acts(self):
        @activity.defn(name="set_stage")
        async def set_stage(a: StageInput) -> None:
            self.calls.append(("set_stage", a.stage))

        @activity.defn(name="comment")
        async def comment(a: CommentInput) -> None:
            self.calls.append(("comment", a))

        @activity.defn(name="find_pr")
        async def find_pr(issue: int) -> PrRef:
            self.calls.append(("find_pr", issue))
            return PrRef(self.existing_pr, "h0" if self.existing_pr else None)

        @activity.defn(name="prepare_round")
        async def prepare(a: RoundInput) -> Workspace:
            self.calls.append(("prepare", a))
            return Workspace(f"/w/round-{a.round}", "base", "autodev/issue-7")

        @activity.defn(name="submit_agent")
        async def submit(a: SubmitInput) -> JobRef:
            self.calls.append(("submit", a))
            return JobRef(f"job-{a.round}", 60)

        @activity.defn(name="publish_round")
        async def publish(a: PublishInput) -> PrRef:
            self.calls.append(("publish", a))
            if a.pr is None:
                return PrRef(self.publish_pr, "h1" if self.publish_pr else None)
            return PrRef(a.pr, f"h{a.round}")

        @activity.defn(name="observe_pr")
        async def observe(a: ObserveInput) -> PrSnapshot:
            self.calls.append(("observe", a.pr))
            return self.snaps.pop(0) if len(self.snaps) > 1 else self.snaps[0]

        @activity.defn(name="merge_pr")
        async def merge(a: MergeInput) -> MergeResult:
            self.calls.append(("merge", a))
            return MergeResult(self.merge_ok, "merged" if self.merge_ok else "405 not mergeable")

        @activity.defn(name="report_failure")
        async def report(a: FailureInput) -> None:
            self.calls.append(("report_failure", a))

        return [set_stage, comment, find_pr, prepare, submit, publish, observe, merge, report]

    def await_act(self):
        @activity.defn(name="await_agent")
        async def await_agent(a: AwaitInput) -> JobOutcome:
            self.calls.append(("await", a.job_id))
            if self.await_failures:
                self.await_failures -= 1
                raise ApplicationError("worker died mid-await")
            return JobOutcome(a.job_id, self.job_state, self.exit_code)

        return [await_agent]

    def names(self):
        return [c[0] for c in self.calls]

    def of(self, name):
        return [c[1] for c in self.calls if c[0] == name]


async def start(env, m, inp=None, runner=None):
    tq = f"tq-{uuid.uuid4()}"
    kw = {"workflow_runner": runner} if runner else {}
    main = Worker(env.client, task_queue=tq, workflows=[IssueWorkflow], activities=m.acts(), **kw)
    waits = Worker(env.client, task_queue=tq + "-await", activities=m.await_act())
    async with main, waits:
        h = await env.client.start_workflow(IssueWorkflow.execute, inp or IssueInput(7), id=f"wf-{uuid.uuid4()}",
                                            task_queue=tq)
        return await h.result(), h


async def run(env, m, inp=None, runner=None):
    return (await start(env, m, inp, runner))[0]


async def test_issue_to_merge(env):
    m = Mocks([snap(), snap(approved=True, checks="success")])
    assert await run(env, m) == "merged"
    assert m.of("set_stage") == ["working", "review", "done"]
    assert [r.round for r in m.of("prepare")] == [1] and m.of("prepare")[0].pr is None
    assert m.of("merge")[0].sha == "h1" and m.names().count("observe") == 2
    assert any("job-1" in c.body and c.number == 7 for c in m.of("comment"))


async def test_existing_pr_is_resumed_not_redone(env):
    m = Mocks([snap(approved=True)], existing_pr=9)
    assert await run(env, m) == "merged"
    assert "prepare" not in m.names() and m.of("observe") == [9]


async def test_no_commits_reports_failure(env):
    m = Mocks([snap()], publish_pr=None)
    assert await run(env, m) == "failed"
    assert "no commits" in m.of("report_failure")[0].error and "observe" not in m.names()


async def test_failed_job_reports_and_does_not_publish(env):
    m = Mocks([snap()], job_state="EXIT", exit_code=1)
    assert await run(env, m) == "failed"
    f = m.of("report_failure")[0]
    assert "job-1" in f.error and "EXIT" in f.error and f.workspace.dir == "/w/round-1"
    assert "publish" not in m.names()


async def test_worker_death_mid_await_does_not_resubmit(env):
    m = Mocks([snap(state="merged")], await_failures=1)
    assert await run(env, m) == "merged"
    assert m.names().count("submit") == 1 and m.names().count("await") == 2


async def test_feedback_revised_once(env):
    fb = Feedback("review-1", "rename foo")
    m = Mocks([snap(feedback=[fb]), snap(sha="h2", feedback=[fb]), snap(sha="h2", approved=True)])
    assert await run(env, m) == "merged"
    rounds = m.of("prepare")
    assert [r.round for r in rounds] == [1, 2]
    assert rounds[1].feedback == "rename foo" and rounds[1].pr == 5
    assert m.of("publish")[1].feedback_id == "review-1" and m.of("merge")[0].sha == "h2"


async def test_revision_limit_escalates_but_approval_still_merges(env):
    f1, f2 = Feedback("review-1", "a"), Feedback("comment-2", "b")
    m = Mocks([snap(feedback=[f1]), snap(feedback=[f1, f2]), snap(feedback=[f1, f2], approved=True)])
    assert await run(env, m, IssueInput(7, max_revisions=1)) == "merged"
    assert len(m.of("prepare")) == 2
    assert any("revision limit" in c.body for c in m.of("comment"))


async def test_closed_pr_stops(env):
    m = Mocks([snap(state="closed")])
    assert await run(env, m) == "closed"
    assert m.of("set_stage")[-1] is None and "merge" not in m.names()


async def test_merge_failure_comments_then_external_merge_finishes(env):
    m = Mocks([snap(approved=True), snap(state="merged")], merge_ok=False)
    assert await run(env, m) == "merged"
    assert any("merging failed" in c.body for c in m.of("comment"))


async def test_continue_as_new_keeps_handled_feedback(env, monkeypatch):
    monkeypatch.setattr(workflows, "MAX_POLLS_PER_EXECUTION", 2)
    fb = Feedback("review-1", "x")
    m = Mocks([snap(feedback=[fb])] + [snap(sha="h2", feedback=[fb])] * 3 + [snap(sha="h2", approved=True)])
    assert await run(env, m, runner=UnsandboxedWorkflowRunner()) == "merged"
    assert len(m.of("prepare")) == 2


async def test_replay_after_log_change(env, monkeypatch):
    _, h = await start(env, Mocks([snap(approved=True)]))
    history = await h.fetch_history()
    replayer = Replayer(workflows=[IssueWorkflow], workflow_runner=UnsandboxedWorkflowRunner())
    await replayer.replay_workflow(history)
    monkeypatch.setattr(workflows, "ROUND_DONE_LOG", "issue #%d, round %d: %s -> %s")
    assert (await replayer.replay_workflow(history, raise_on_replay_failure=False)).replay_failure is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_workflows.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.workflows'`

- [ ] **Step 3: Write minimal implementation**

`autodev/workflows.py`:
```python
"""Temporal workflows. No I/O here; activities are called by name (see activities.py)."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from .decide import CLOSED, DONE, ESCALATE, MERGE, REVISE, decide
    from .models import (AwaitInput, CommentInput, FailureInput, Feedback, IssueInput, JobOutcome, JobRef,
                         MergeInput, MergeResult, ObserveInput, PrRef, PrSnapshot, PublishInput, RoundInput,
                         StageInput, Status, SubmitInput, Workspace)

ROUND_DONE_LOG = "issue #%d round %d: job %s ended %s"
MAX_POLLS_PER_EXECUTION = 500
QUEUE_SLACK = timedelta(hours=12)  # ponytail: covers LSF pending time; use schedule-to-start metrics if it bites


class RoundFailed(Exception):
    pass


def _err(e: BaseException) -> str:
    while isinstance(e, ActivityError) and e.cause:
        e = e.cause
    return str(e) or type(e).__name__


@workflow.defn
class IssueWorkflow:
    @workflow.init
    def __init__(self, inp: IssueInput) -> None:
        self.s = inp
        self.state = "STARTING"
        self.job_id: str | None = None
        self.last_error: str | None = None
        self.workspace: Workspace | None = None

    @workflow.query(name="status")
    def status(self) -> Status:
        return Status(self.s.issue, self.state, self.s.round, self.s.pr, self.job_id, self.last_error)

    async def _act(self, name: str, arg, seconds: int = 60, attempts: int = 5, result_type=None):
        return await workflow.execute_activity(
            name, arg, start_to_close_timeout=timedelta(seconds=seconds),
            retry_policy=RetryPolicy(maximum_attempts=attempts), result_type=result_type)

    async def _comment(self, number: int, body: str, marker: str) -> None:
        await self._act("comment", CommentInput(number, body, marker))

    @workflow.run
    async def execute(self, inp: IssueInput) -> str:
        s = self.s
        s.key = s.key or workflow.info().run_id[:8]
        try:
            if s.pr is None:
                s.pr = (await self._act("find_pr", s.issue, result_type=PrRef)).pr
            if s.pr is None:
                await self._act("set_stage", StageInput(s.issue, "working"))
                res = await self._round(None)
                if res.pr is None:
                    raise RoundFailed("the agent made no commits; its summary is above")
                s.pr = res.pr
            await self._act("set_stage", StageInput(s.issue, "review"))
            return await self._watch()
        except (ActivityError, RoundFailed) as e:
            self.state, self.last_error = "FAILED", _err(e)
            await self._act("report_failure", FailureInput(s.issue, s.key, s.round, self.last_error, self.workspace))
            return "failed"

    async def _round(self, fb: Feedback | None) -> PrRef:
        s = self.s
        s.round += 1
        self.state, self.job_id = "PREPARING", None
        self.workspace = ws = await self._act(
            "prepare_round", RoundInput(s.issue, s.key, s.round, s.pr, fb.text if fb else None),
            seconds=600, attempts=3, result_type=Workspace)
        job = await self._act("submit_agent", SubmitInput(s.issue, s.key, s.round, ws), attempts=3,
                              result_type=JobRef)
        self.state, self.job_id = "RUNNING", job.job_id
        await self._comment(s.pr or s.issue, f"Round {s.round} started: agent job `{job.job_id}`.",
                            f"started {s.key}-{s.round}")
        out = await workflow.execute_activity(
            "await_agent", AwaitInput(job.job_id, ws), result_type=JobOutcome,
            task_queue=workflow.info().task_queue + "-await",
            start_to_close_timeout=timedelta(minutes=job.walltime_min) + QUEUE_SLACK,
            heartbeat_timeout=timedelta(minutes=3), retry_policy=RetryPolicy(maximum_attempts=3))
        workflow.logger.info(ROUND_DONE_LOG, s.issue, s.round, out.job_id, out.state)
        if out.state != "DONE" or out.exit_code not in (0, None):
            raise RoundFailed(f"agent job {out.job_id} ended {out.state} (exit {out.exit_code})")
        self.state = "PUBLISHING"
        return await self._act("publish_round", PublishInput(s.issue, s.key, s.round, ws, s.pr, fb.id if fb else None),
                               seconds=300, attempts=3, result_type=PrRef)

    async def _watch(self) -> str:
        s = self.s
        for _ in range(MAX_POLLS_PER_EXECUTION):
            self.state = "REVIEW"
            snap = await self._act("observe_pr", ObserveInput(s.pr), result_type=PrSnapshot)
            action, detail = decide(snap, s.handled, s.revisions, s.max_revisions)
            if action == DONE:
                await self._act("set_stage", StageInput(s.issue, "done"))
                return "merged"
            if action == CLOSED:
                await self._act("set_stage", StageInput(s.issue, None))
                return "closed"
            if action == MERGE:
                r = await self._act("merge_pr", MergeInput(s.issue, s.pr, snap.head_sha), result_type=MergeResult)
                if r.merged:
                    await self._act("set_stage", StageInput(s.issue, "done"))
                    return "merged"
                await self._comment(s.pr, f"Approved, but merging failed: {r.message}", f"merge-failed {snap.head_sha}")
            elif action == REVISE:
                s.handled.append(detail.id)
                s.revisions += 1
                await self._act("set_stage", StageInput(s.issue, "working"))
                await self._round(detail)
                await self._act("set_stage", StageInput(s.issue, "review"))
                continue
            elif action == ESCALATE:
                await self._comment(s.pr, f"Needs a human: {detail}.", f"escalate {detail} {snap.head_sha}")
            await workflow.sleep(timedelta(minutes=s.poll_minutes))
            if workflow.info().is_continue_as_new_suggested():
                break
        workflow.continue_as_new(s)


@workflow.defn
class PollWorkflow:
    @workflow.run
    async def execute(self) -> list[int]:
        return await workflow.execute_activity(
            "poll_issues", start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=1), result_type=list[int])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_workflows.py -v`
Expected: 11 passed. Also run `pixi run test` to confirm zarr-doctor's 36 tests still pass.

- [ ] **Step 5: Commit**

```bash
git add autodev/workflows.py tests/test_autodev_workflows.py
git commit -m "autodev: IssueWorkflow (round, watch, revise, merge) and PollWorkflow

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Activities (wiring, comment text, label gate)

**Files:**
- Create: `autodev/activities.py`, `tests/test_autodev_activities.py`

**Interfaces:**
- Consumes:
  - `Config` and `STAGES` (Task 1)
  - `GitHub` (Task 3); `review.approved`, `review.feedback`, `review.handled_ids` and `review.WRITE_PERMISSIONS` (Task 3)
  - models (Task 4)
  - `Mirror` and `ProtectedPathError` (Task 5)
  - `AgentJobs`, `render_prompt`, `read_result` and `log_tail` (Task 6)
  - `IssueWorkflow` (Task 7)
- Produces: `Activities(cfg, gh, mirror, jobs, bot, client=None)` with `.all() -> list`. It covers the main-queue activities in Task 7's table plus `poll_issues`; `await_agent` is separate, for the `-await` queue.

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_activities.py`:
```python
import json

import httpx
import pytest
import respx
from temporalio.common import WorkflowIDConflictPolicy

from autodev.activities import Activities
from autodev.github import GitHub
from autodev.gitops import Mirror
from autodev.models import Feedback, ObserveInput, PublishInput, RoundInput

from autodev_helpers import API, R, agent_commit, make_cfg, make_origin, run_git


class FakeTemporal:
    def __init__(self):
        self.started = []

    async def start_workflow(self, fn, arg, **kw):
        self.started.append((arg, kw))


@pytest.fixture
def gh():
    async def no_sleep(_):
        pass
    return GitHub("o/r", lambda: "tok", writes_per_second=1000, sleep=no_sleep)


def acts(tmp_path, gh, client=None, origin=None):
    cfg = make_cfg(tmp_path, github={"git_url": str(origin or tmp_path / "none.git")})
    mirror = Mirror(tmp_path / "state" / "mirror.git", cfg.git_url)
    return Activities(cfg, gh, mirror, jobs=None, bot="bot", client=client)


@respx.mock
async def test_poll_ignores_label_from_reader(tmp_path, gh):
    respx.get(f"{API}{R}/issues").mock(return_value=httpx.Response(200, json=[{"number": 1}, {"number": 2}]))
    for n, who in ((1, "alice"), (2, "mallory")):
        respx.get(f"{API}{R}/issues/{n}/events").mock(return_value=httpx.Response(200, json=[
            {"event": "labeled", "label": {"name": "autodev:go"}, "actor": {"login": who}}]))
    respx.get(f"{API}{R}/collaborators/alice/permission").mock(return_value=httpx.Response(200, json={"permission": "write"}))
    respx.get(f"{API}{R}/collaborators/mallory/permission").mock(return_value=httpx.Response(200, json={"permission": "read"}))
    unlabel = respx.delete(url__regex=r".*/issues/2/labels/.*").mock(return_value=httpx.Response(200, json=[]))
    respx.get(f"{API}{R}/issues/2/comments").mock(return_value=httpx.Response(200, json=[]))
    note = respx.post(f"{API}{R}/issues/2/comments").mock(return_value=httpx.Response(201, json={"id": 1}))
    temporal = FakeTemporal()
    assert await acts(tmp_path, gh, temporal).poll_issues() == [1]
    arg, kw = temporal.started[0]
    assert arg.issue == 1 and kw["id"] == "autodev/o/r/issue-1"
    assert kw["id_conflict_policy"] == WorkflowIDConflictPolicy.USE_EXISTING and kw["task_queue"] == "autodev"
    assert unlabel.called and "mallory" in json.loads(note.calls[0].request.content)["body"]


@respx.mock
async def test_first_round_publishes_and_opens_pr(tmp_path, gh):
    origin = make_origin(tmp_path)
    a = acts(tmp_path, gh, origin=origin)
    respx.get(f"{API}{R}/issues/7").mock(return_value=httpx.Response(200, json={
        "number": 7, "title": "Add feature", "body": "please", "labels": []}))
    respx.get(f"{API}{R}/issues/7/comments").mock(return_value=httpx.Response(200, json=[]))
    ws = await a.prepare_round(RoundInput(7, "k", 1, None, None))
    d = tmp_path / "work" / "o__r" / "issue-7" / "k" / "round-1"
    assert ws.dir == str(d) and "Add feature" in (d / "prompt.md").read_text()
    head = agent_commit(d / "repo", "f.txt", "x", ws.base_sha)
    (d / "result.json").write_text(json.dumps({"result": "did it " + "y" * 70000}))
    respx.get(f"{API}{R}/pulls").mock(return_value=httpx.Response(200, json=[]))
    create = respx.post(f"{API}{R}/pulls").mock(return_value=httpx.Response(201, json={"number": 5}))
    respx.get(f"{API}{R}/issues/5/comments").mock(return_value=httpx.Response(200, json=[]))
    summary = respx.post(f"{API}{R}/issues/5/comments").mock(return_value=httpx.Response(201, json={"id": 2}))
    ref = await a.publish_round(PublishInput(7, "k", 1, ws, None, None))
    assert (ref.pr, ref.head_sha) == (5, head)
    assert run_git("rev-parse", "refs/heads/autodev/issue-7", cwd=origin) == head
    pr = json.loads(create.calls[0].request.content)
    assert pr["head"] == "autodev/issue-7" and pr["base"] == "main" and "Closes #7" in pr["body"]
    body = json.loads(summary.calls[0].request.content)["body"]
    assert len(body) < 65536 and "<!-- autodev: round k-1 -->" in body


@respx.mock
async def test_observe_skips_handled_feedback(tmp_path, gh):
    head = "abc1234def"
    respx.get(f"{API}{R}/pulls/5").mock(return_value=httpx.Response(200, json={
        "state": "open", "merged": False, "head": {"sha": head}}))
    respx.get(f"{API}{R}/pulls/5/reviews").mock(return_value=httpx.Response(200, json=[
        {"id": 3, "user": {"login": "alice"}, "state": "CHANGES_REQUESTED", "commit_id": head, "body": "rename",
         "submitted_at": "2026-10-10T01:00:00Z"}]))
    respx.get(f"{API}{R}/pulls/5/comments").mock(return_value=httpx.Response(200, json=[]))
    respx.get(f"{API}{R}/issues/5/comments").mock(return_value=httpx.Response(200, json=[
        {"id": 9, "user": {"login": "bot"}, "body": "round 2\n<!-- autodev: handled review-3 -->",
         "created_at": "2026-10-10T01:30:00Z"},
        {"id": 10, "user": {"login": "alice"}, "body": "/revise also docs", "created_at": "2026-10-10T02:00:00Z"}]))
    for who in ("alice", "bot"):
        respx.get(f"{API}{R}/collaborators/{who}/permission").mock(
            return_value=httpx.Response(200, json={"permission": "write"}))
    respx.get(f"{API}{R}/commits/{head}/check-runs").mock(return_value=httpx.Response(200, json={"check_runs": []}))
    respx.get(f"{API}{R}/commits/{head}/status").mock(return_value=httpx.Response(200, json={"statuses": []}))
    snap = await acts(tmp_path, gh).observe_pr(ObserveInput(5))
    assert snap.feedback == [Feedback("comment-10", "also docs")]
    assert (snap.state, snap.head_sha, snap.approved, snap.checks) == ("open", head, False, "none")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_activities.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.activities'`

- [ ] **Step 3: Write minimal implementation**

`autodev/activities.py`:
```python
"""Temporal activities: the only code that touches GitHub, git, csub and the filesystem."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from temporalio import activity
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError

from .agent import log_tail, read_result, render_prompt
from .config import STAGES
from .gitops import ProtectedPathError
from .models import (AwaitInput, CommentInput, FailureInput, IssueInput, JobOutcome, JobRef, MergeInput,
                     MergeResult, ObserveInput, PrRef, PrSnapshot, PublishInput, RoundInput, StageInput,
                     SubmitInput, Workspace)
from .review import WRITE_PERMISSIONS, approved, feedback, handled_ids
from .workflows import IssueWorkflow

log = logging.getLogger(__name__)


# --- comment templates ------------------------------------------------------------------------

def render_pr_body(cfg, issue: int) -> str:
    return f"""Closes #{issue}

Implemented by **{cfg.deployment}**: Claude Code running in a sandboxed job on the cluster. The agent's own
summary is the first comment.

- **Merge:** approve this PR, or comment `/approve <head sha>` (needed when running under a personal token).
  {cfg.deployment} merges once an approval matches the current head and checks are not failing.
- **Request changes:** submit a review with *Request changes*, or comment `/revise <what to change>`.
  Up to {cfg.agent.max_revisions} revision rounds.
- **Stop:** close this PR.

{cfg.marker("pr")}"""


def render_summary(cfg, rnd: int, branch: str, head: str | None, summary: str, cost: float | None) -> str:
    what = f"pushed `{head[:12]}` to `{branch}`" if head else "made no commits"
    cost_s = f" · ${cost:.2f}" if cost is not None else ""
    return f"### {cfg.deployment}: round {rnd} {what}{cost_s}\n\n{summary}"


def render_failure(cfg, error: str, tail: str) -> str:
    return f"""### {cfg.deployment}: stopped

**Error:** {error}

````
{tail.replace("````", "'''") or "(no log output)"}
````

Fix the cause (or edit the issue), then add the `{cfg.label("go")}` label to try again. An open PR is picked up
where it left off; close it first to start over."""


class Activities:
    def __init__(self, cfg, gh, mirror, jobs, bot: str, client=None):
        self.cfg, self.gh, self.mirror, self.jobs, self.bot, self.client = cfg, gh, mirror, jobs, bot, client

    def all(self) -> list:
        return [self.set_stage, self.comment, self.find_pr, self.prepare_round, self.submit_agent,
                self.publish_round, self.observe_pr, self.merge_pr, self.report_failure, self.poll_issues]

    def _round_dir(self, issue: int, key: str, rnd: int) -> Path:
        return self.cfg.paths.work_root / self.cfg.repo_slug / f"issue-{issue}" / key / f"round-{rnd}"

    @activity.defn(name="set_stage")
    async def set_stage(self, a: StageInput) -> None:
        want = self.cfg.label(a.stage) if a.stage else None
        current = {lb["name"] for lb in (await self.gh.get_issue(a.issue)).get("labels", [])}
        for stage in STAGES:
            name = self.cfg.label(stage)
            if name in current and name != want:
                await self.gh.remove_label(a.issue, name)
        if want and want not in current:
            await self.gh.add_labels(a.issue, [want])

    @activity.defn(name="comment")
    async def comment(self, a: CommentInput) -> None:
        marker = self.cfg.marker(a.marker)
        await self.gh.comment_once(a.number, f"{a.body}\n\n{marker}", marker)

    @activity.defn(name="find_pr")
    async def find_pr(self, issue: int) -> PrRef:
        pr = await self.gh.find_pr(self.cfg.branch(issue))
        return PrRef(pr["number"], pr["head"]["sha"]) if pr else PrRef(None, None)

    @activity.defn(name="prepare_round")
    async def prepare_round(self, a: RoundInput) -> Workspace:
        d = self._round_dir(a.issue, a.key, a.round)
        branch = self.cfg.branch(a.issue)
        start = branch if a.pr else self.cfg.github.base_branch
        await asyncio.to_thread(self.mirror.sync)
        base = await asyncio.to_thread(self.mirror.workspace, d, start, branch, self.cfg.git_url)
        issue = await self.gh.get_issue(a.issue)
        comments = await self.gh.issue_comments(a.issue)
        (d / "prompt.md").write_text(render_prompt(self.cfg, issue, comments, branch, base, a.pr, a.feedback))
        return Workspace(str(d), base, branch)

    @activity.defn(name="submit_agent")
    async def submit_agent(self, a: SubmitInput) -> JobRef:
        return await self.jobs.submit(a.issue, a.key, a.round, a.workspace)

    @activity.defn(name="await_agent")
    async def await_agent(self, a: AwaitInput) -> JobOutcome:
        return await self.jobs.wait(a.job_id, a.workspace, activity.heartbeat)

    @activity.defn(name="publish_round")
    async def publish_round(self, a: PublishInput) -> PrRef:
        ws = Path(a.workspace.dir)
        summary, cost = read_result(ws)
        try:
            head = await asyncio.to_thread(
                self.mirror.publish, ws / "result.bundle", a.workspace.base_sha, a.workspace.branch,
                f"refs/autodev/issue-{a.issue}/{a.key}-r{a.round}", self.cfg.github.protected_paths,
                a.pr is None)  # round 1 owns the branch; revisions must fast-forward
        except ProtectedPathError as e:
            raise ApplicationError(str(e), non_retryable=True) from e
        pr = a.pr
        if head and pr is None:
            existing = await self.gh.find_pr(a.workspace.branch)
            if existing:
                pr = existing["number"]
            else:
                issue = await self.gh.get_issue(a.issue)
                pr = (await self.gh.create_pr(f"{issue['title']} (#{a.issue})", render_pr_body(self.cfg, a.issue),
                                              a.workspace.branch, self.cfg.github.base_branch))["number"]
        markers = [f"round {a.key}-{a.round}"] + ([f"handled {a.feedback_id}"] if a.feedback_id else [])
        body = render_summary(self.cfg, a.round, a.workspace.branch, head, summary, cost)
        body += "\n\n" + "\n".join(self.cfg.marker(m) for m in markers)
        await self.gh.comment_once(pr or a.issue, body, self.cfg.marker(markers[0]))
        return PrRef(pr, head)

    @activity.defn(name="observe_pr")
    async def observe_pr(self, a: ObserveInput) -> PrSnapshot:
        pr = await self.gh.get_pr(a.pr)
        head = pr["head"]["sha"]
        if pr.get("merged") or pr["state"] != "open":
            return PrSnapshot(a.pr, "merged" if pr.get("merged") else pr["state"], head, False, "none")
        reviews = await self.gh.pr_reviews(a.pr)
        inline = await self.gh.pr_review_comments(a.pr)
        comments = await self.gh.issue_comments(a.pr)
        logins = {(x.get("user") or {}).get("login") for x in [*reviews, *comments]} - {None}
        # ponytail: one permission lookup per participant per poll; cache if the rate limit ever matters.
        perms = {login: await self.gh.permission(login) for login in logins}
        mp, allow_self = self.cfg.marker_prefix, self.cfg.github.auth == "token"
        done = handled_ids(comments, mp)
        fb = [f for f in feedback(reviews, inline, comments, perms, self.bot, allow_self, mp) if f.id not in done]
        return PrSnapshot(a.pr, "open", head, approved(head, reviews, comments, perms, self.bot, allow_self, mp),
                          await self.gh.check_state(head), fb)

    @activity.defn(name="merge_pr")
    async def merge_pr(self, a: MergeInput) -> MergeResult:
        merged, msg = await self.gh.merge_pr(a.pr, a.sha)
        if not merged and (await self.gh.get_pr(a.pr)).get("merged"):  # a retry after a merge that went through
            merged, msg = True, "merged"
        if merged:
            await self.gh.delete_branch(self.cfg.branch(a.issue))
            await self.gh.update_issue(a.issue, state="closed", state_reason="completed")
        return MergeResult(merged, msg)

    @activity.defn(name="report_failure")
    async def report_failure(self, a: FailureInput) -> None:
        tail = log_tail(Path(a.workspace.dir)) if a.workspace else ""
        marker = self.cfg.marker(f"failed {a.key}-{a.round}")
        await self.gh.comment_once(a.issue, f"{render_failure(self.cfg, a.error, tail)}\n\n{marker}", marker)
        await self.set_stage(StageInput(a.issue, "needs-human"))

    @activity.defn(name="poll_issues")
    async def poll_issues(self) -> list[int]:
        go = self.cfg.label("go")
        started = []
        for i in await self.gh.list_issues(labels=go, state="open"):
            n = i["number"]
            actor = await self.gh.label_actor(n, go)
            if not actor or await self.gh.permission(actor) not in WRITE_PERMISSIONS:
                await self.gh.remove_label(n, go)
                marker = self.cfg.marker(f"ignored-label {actor}")
                await self.gh.comment_once(n, f"Ignoring `{go}`: it was added by `{actor}`, who does not have write "
                                              f"access to this repository.\n\n{marker}", marker)
                continue
            await self.client.start_workflow(
                IssueWorkflow.execute, IssueInput(n, self.cfg.agent.max_revisions, self.cfg.temporal.poll_minutes),
                id=self.cfg.workflow_id(n), task_queue=self.cfg.task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING)
            started.append(n)
        if started:
            log.info("poll: started/kept workflows for %s", started)
        return started
```

`set_stage` is called directly from `report_failure` as a plain coroutine. That works because `@activity.defn` leaves the method callable.

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_activities.py -v`
Expected: 3 passed. Then run `pixi run test`: everything passes.

- [ ] **Step 5: Commit**

```bash
git add autodev/activities.py tests/test_autodev_activities.py
git commit -m "autodev: activities, comment templates and the write-access label gate

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: CLI and the sandboxed run script

**Files:**
- Create: `autodev/cli.py`, `scripts/autodev-up.sh`, `tests/test_autodev_cli.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - the commands `autodev check`, `autodev init-labels`, `autodev worker` and `autodev schedule apply`
  - `proxy_target(environ) -> str | None`
  - `scripts/autodev-up.sh`

- [ ] **Step 1: Write the failing test**

`tests/test_autodev_cli.py`:
```python
import pytest

from autodev.cli import main, proxy_target


def test_proxy_target_from_sandbox_env():
    assert proxy_target({"https_proxy": "http://127.0.0.1:41234"}) == "127.0.0.1:41234"
    assert proxy_target({"HTTPS_PROXY": "http://127.0.0.1:9"}) == "127.0.0.1:9"
    assert proxy_target({}) is None


def test_help_lists_commands(capsys):
    with pytest.raises(SystemExit) as e:
        main(["--help"])
    assert e.value.code == 0
    out = capsys.readouterr().out
    for cmd in ("check", "init-labels", "worker", "schedule"):
        assert cmd in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run pytest tests/test_autodev_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autodev.cli'`

- [ ] **Step 3: Write minimal implementation**

`autodev/cli.py`:
```python
"""autodev command line."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from datetime import timedelta
from urllib.parse import urlsplit

from .config import STAGES, load_config

log = logging.getLogger("autodev")


def _setup_logging(cfg, name: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if name:
        logs = cfg.paths.state_dir / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(logs / f"{name}.log"))
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def proxy_target(environ) -> str | None:
    url = environ.get("https_proxy") or environ.get("HTTPS_PROXY")
    return urlsplit(url).netloc if url else None


async def _client(cfg):
    from temporalio.client import Client
    from temporalio.service import HttpConnectProxyConfig

    proxy = None
    if cfg.temporal.via_https_proxy:  # inside agentic-sandbox the only way out is the allowlist proxy
        if not (target := proxy_target(os.environ)):
            sys.exit("autodev: temporal.via_https_proxy is set but $https_proxy is not (run under sandbox-run.sh --allow)")
        proxy = HttpConnectProxyConfig(target_host=target)
    return await asyncio.wait_for(Client.connect(cfg.temporal.address, namespace=cfg.temporal.namespace,
                                                 http_connect_proxy_config=proxy), 10)


async def cmd_check(cfg, args) -> int:
    from .agent import csub_client
    from .auth import bot_login
    from .github import GitHub

    rows: list[tuple[str, bool, str]] = []

    async def row(name, fn):
        try:
            rows.append((name, True, str(await fn())))
        except Exception as e:  # report every check, then fail
            rows.append((name, False, f"{type(e).__name__}: {e}"))

    gh = GitHub.for_autodev(cfg)

    async def repo():
        r = (await gh.request("GET", f"/repos/{cfg.github.repo}")).json()
        return f"{r['full_name']} (default branch {r['default_branch']})"

    async def csub_ok():
        p = await asyncio.to_thread(csub_client(cfg).probe)
        need = set(cfg.agent.allow_hosts) | {"api.anthropic.com", "claude.ai", "platform.claude.com"}
        problems = [*([] if p.allow_claude else ["allow_claude is false"]),
                    *(f"host not allowed: {h}" for h in sorted(need - set(p.allowed_hosts))),
                    *([] if any(str(cfg.paths.work_root).startswith(r) for r in p.allowed_roots)
                      else [f"work_root {cfg.paths.work_root} not under allowed_roots {p.allowed_roots}"])]
        if problems:
            raise RuntimeError("; ".join(problems))
        return f"broker user {p.user}, sandbox {p.sandbox}"

    async def temporal():
        c = await _client(cfg)
        return f"{cfg.temporal.address} ns={cfg.temporal.namespace} ({c.identity})"

    async def dirs():
        for d in (cfg.paths.state_dir, cfg.paths.work_root):
            d.mkdir(parents=True, exist_ok=True)
            (d / ".write-test").touch()
            (d / ".write-test").unlink()
        return f"{cfg.paths.state_dir}, {cfg.paths.work_root}"

    await row("github identity", lambda: bot_login(cfg))
    await row("github repo", repo)
    await row("temporal", temporal)
    await row("csub broker", csub_ok)
    await row("directories", dirs)
    await gh.aclose()
    for name, ok, detail in rows:
        print(f"{'ok ' if ok else 'BAD'}  {name:16} {detail}")
    return 0 if all(ok for _, ok, _ in rows) else 1


async def cmd_init_labels(cfg, args) -> int:
    from .github import GitHub

    gh = GitHub.for_autodev(cfg)
    for stage, (color, desc) in STAGES.items():
        if await gh.ensure_label(cfg.label(stage), color, desc):
            print(f"created {cfg.label(stage)}")
    await gh.aclose()
    return 0


async def cmd_worker(cfg, args) -> int:
    from temporalio.worker import Worker

    from .activities import Activities
    from .agent import AgentJobs
    from .auth import bot_login, token_fn
    from .github import GitHub
    from .gitops import Mirror
    from .workflows import IssueWorkflow, PollWorkflow

    client = await _client(cfg)
    tokens = token_fn(cfg)
    gh = GitHub.for_autodev(cfg, tokens)
    bot = await bot_login(cfg)
    mirror = Mirror(cfg.paths.state_dir / "mirror" / f"{cfg.repo_slug}.git", cfg.git_url, tokens)
    acts = Activities(cfg, gh, mirror, AgentJobs(cfg), bot, client)
    tq = cfg.task_queue
    main = Worker(client, task_queue=tq, workflows=[IssueWorkflow, PollWorkflow], activities=acts.all(),
                  max_concurrent_activities=8)
    waits = Worker(client, task_queue=f"{tq}-await", activities=[acts.await_agent], max_concurrent_activities=20)
    task = asyncio.current_task()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    log.info("worker up as %s: %s ns=%s queues %s, %s-await", bot, cfg.temporal.address, cfg.temporal.namespace, tq, tq)
    try:
        async with main, waits:
            await asyncio.Event().wait()
    except asyncio.CancelledError:
        log.info("worker shutting down")
    finally:
        await gh.aclose()
    return 0


async def cmd_schedule(cfg, args) -> int:
    from temporalio.client import (Schedule, ScheduleActionStartWorkflow, ScheduleAlreadyRunningError,
                                   ScheduleIntervalSpec, ScheduleOverlapPolicy, SchedulePolicy, ScheduleSpec,
                                   ScheduleUpdate)

    from .workflows import PollWorkflow

    client = await _client(cfg)
    sid = f"{cfg.deployment}-poll"
    sched = Schedule(
        action=ScheduleActionStartWorkflow(PollWorkflow.execute, id=f"{cfg.deployment}-poll-run", task_queue=cfg.task_queue),
        spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=cfg.temporal.poll_minutes))]),
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP))
    try:
        await client.create_schedule(sid, sched)
        print(f"created schedule {sid}")
    except ScheduleAlreadyRunningError:
        await client.get_schedule_handle(sid).update(lambda _: ScheduleUpdate(schedule=sched))
        print(f"updated schedule {sid}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="autodev", description=__doc__)
    p.add_argument("--config", help="path to autodev.yaml (default: $AUTODEV_CONFIG or ./autodev.yaml)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check", help="verify GitHub identity, Temporal, csub policy and directories")
    sub.add_parser("init-labels", help="create the stage labels")
    sub.add_parser("worker", help="run the Temporal worker")
    s = sub.add_parser("schedule", help="manage the poll Schedule")
    s.add_argument("action", choices=["apply"])
    args = p.parse_args(argv)
    cfg = load_config(args.config)
    _setup_logging(cfg, args.cmd if args.cmd == "worker" else None)
    cmd = {"check": cmd_check, "init-labels": cmd_init_labels, "worker": cmd_worker, "schedule": cmd_schedule}[args.cmd]
    return asyncio.run(cmd(cfg, args))


if __name__ == "__main__":
    sys.exit(main())
```

`scripts/autodev-up.sh`:
```bash
#!/usr/bin/env bash
# Run autodev under tmux: Temporal dev server and csub broker on the host, the worker inside agentic-sandbox.
# Identity = the account running this + autodev.yaml: the same script serves a personal test instance and
# the service account. Attach: tmux attach -t <deployment>    Stop: tmux kill-session -t <deployment>
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$PWD
PY=$REPO/.pixi/envs/default/bin/python
SANDBOX=${SANDBOX:-$REPO/../agentic-sandbox/scripts}
[ -x "$PY" ] || { echo "run 'pixi install' first"; exit 1; }
[ -x "$SANDBOX/sandbox-run.sh" ] || { echo "agentic-sandbox not found at $SANDBOX (set \$SANDBOX)"; exit 1; }

eval "$("$PY" - <<'EOF'
import os
import shlex
from autodev.config import PROJECT_DIR, load_config
c = load_config()
g = c.github
secret = g.private_key_path if g.auth == "app" else g.token_file
for k, v in {"DEPLOY": c.deployment, "STATE": c.paths.state_dir, "WORK": c.paths.work_root, "SECRET": secret or "",
             "CFG": os.environ.get("AUTODEV_CONFIG") or PROJECT_DIR / "autodev.yaml"}.items():
    print(f"{k}={shlex.quote(str(v))}")
EOF
)"
# The sandbox masks .env and tmux passes no env from this shell, so the worker's GitHub secret must be a file.
[ -n "$SECRET" ] || { echo "set github.token_file (auth: token) or github.private_key_path (auth: app)"; exit 1; }
RUN=/scratch/$USER/$DEPLOY   # node-local: the csub broker's unix socket
mkdir -p "$STATE/logs" "$STATE/bin" "$WORK" "$RUN"

TEMPORAL=$(command -v temporal || echo "$STATE/bin/temporal")
if [ ! -x "$TEMPORAL" ]; then
  curl -sSL "https://temporal.download/cli/archive/latest?platform=linux&arch=amd64" | tar xz -C "$STATE/bin" temporal
fi
if tmux has-session -t "$DEPLOY" 2>/dev/null; then echo "already running: tmux attach -t $DEPLOY"; exit 0; fi

# Temporal gRPC and UI stay on this host's loopback; the sandboxed worker reaches them via the allowlist proxy.
tmux new-session -d -s "$DEPLOY" -n temporal \
  "'$TEMPORAL' server start-dev --db-filename '$STATE/temporal.db' 2>&1 | tee -a '$STATE/logs/temporal.log'"
until "$TEMPORAL" operator cluster health >/dev/null 2>&1; do sleep 1; done
tmux new-window -t "$DEPLOY" -n broker "csub-broker --serve '$RUN/csub.sock' 2>&1 | tee -a '$STATE/logs/broker.log'"

# cwd = $STATE, which sandbox-run.sh binds read-write; the repo (code + pixi env) is read-only.
tmux new-window -t "$DEPLOY" -n worker "cd '$STATE' && '$SANDBOX/sandbox-run.sh' --ro '$REPO' --ro '$CFG' \
  --ro '$SECRET' --rw '$WORK' --rw '$RUN' --allow github.com --allow api.github.com --allow localhost -- \
  env AUTODEV_CONFIG='$CFG' CSUB_TRANSPORT=unix CSUB_SOCKET='$RUN/csub.sock' AUTODEV_TEMPORAL_VIA_PROXY=1 \
  '$PY' -m autodev.cli worker; read -p 'worker exited; press enter'"
"$PY" -m autodev.cli schedule apply

cat <<MSG
autodev '$DEPLOY' running in tmux (windows: temporal, broker, worker).
  attach:      tmux attach -t $DEPLOY
  logs:        tail -f $STATE/logs/worker.log
  Temporal UI: http://localhost:8233 (ssh -L 8233:localhost:8233 $(hostname -f))
  trigger:     add the label '$DEPLOY:go' to an issue
MSG
```

Make it executable: `chmod +x scripts/autodev-up.sh`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run pytest tests/test_autodev_cli.py -v && bash -n scripts/autodev-up.sh`
Expected: 2 passed; `bash -n` prints nothing.

- [ ] **Step 5: Commit**

```bash
git add autodev/cli.py scripts/autodev-up.sh tests/test_autodev_cli.py
git commit -m "autodev: CLI (check, init-labels, worker, schedule) and sandboxed run script

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Runbook and an end-to-end run as yourself

**Files:**
- Create: `notes/autodev-runbook.md`
- Modify: `README.md` (add one line linking the runbook under a new "autodev" heading)

**Interfaces:**
- Consumes: the whole system. Produces the runbook plus a verified live run.

- [ ] **Step 1: Write the runbook**

`notes/autodev-runbook.md`:
````markdown
# autodev runbook

autodev turns a labelled GitHub issue into a PR written by Claude Code in a sandboxed LSF job, revises it on
request, and merges it after a human approves. Design: [agentic-dev-infrastructure.md](agentic-dev-infrastructure.md).

## How people use it

| you do | autodev does |
|---|---|
| add `autodev:go` to an issue (needs write access) | runs an agent round, opens PR `autodev/issue-N`, labels the issue `autodev:review` |
| review with *Request changes*, or comment `/revise <what>` | runs another round on the same branch (up to `max_revisions`) |
| approve the PR (or comment `/approve <sha>` under a personal token) | squash-merges that exact commit, closes the issue |
| close the PR | stops; re-add `autodev:go` to start over |
| sees `autodev:needs-human` | read the failure comment, fix, re-add `autodev:go` (an open PR is resumed) |

## Run it as yourself (testing)

1. Prerequisites P1–P4 in [the plan](plans/2026-10-10-autodev-basic-loop.md#before-you-start-prerequisites-not-tasks).
2. `cp autodev.yaml.example autodev.yaml` and set `deployment: autodev-<you>` (keeps your labels, branches and
   workflows apart from the service and from colleagues), `paths.work_root` under your csub `allowed_roots`.
3. `gh auth token > ~/.config/autodev/token && chmod 600 ~/.config/autodev/token`, and set
   `github.token_file: ~/.config/autodev/token` (the sandbox masks `.env`).
4. `pixi run autodev check` — every row `ok`.
5. `pixi run autodev init-labels`, then `scripts/autodev-up.sh`.
6. Open a small issue ("add a `--version` flag"), add `autodev-<you>:go`, watch `tmux attach -t autodev-<you>`.

Under a personal token you are both the bot and the reviewer, so GitHub won't let you *approve* your own PR:
comment `/approve <first 7+ chars of the head sha>` instead.

## Run it as the group service

1. **Service account** on the cluster with rootless podman / bwrap set up (agentic-sandbox README), a csub
   broker installed for it (csub `deploy/INSTALL.md`), and an LSF billing group in its broker policy.
2. **Broker policy** (`~svc/.config/csub/broker.toml`): `allow_claude = true`; `allowed_hosts` = the three
   Anthropic hosts + `agent.allow_hosts`; `allowed_roots` contains `paths.work_root`; `env_allow` has
   `ANTHROPIC_*` if using `agent.api_key_env`.
3. **Claude** for the service account: `claude auth login` as that account (a team seat), or an API key in its
   environment and `agent.api_key_env: ANTHROPIC_API_KEY`.
4. **GitHub App** owned by the org, installed on the repo, permissions: Contents RW, Pull requests RW, Issues
   RW, Checks R, Commit statuses R, Metadata R. **Not** Workflows, **not** Administration. Private key at
   `~svc/.config/autodev/app.pem` (mode 600); `github.auth: app`, `app_id`, `installation_id`,
   `private_key_path`.
5. **Branch protection / ruleset on `main`**: 1 approving review required, stale approvals dismissed on push,
   the App not in the bypass list. This is the real gate; autodev's own approval check is a pre-check.
   Add CODEOWNERS for `autodev/`, `scripts/` and `.github/` so changes to the gates need a named human.
6. As the service account: `pixi run autodev check`, `pixi run autodev init-labels`, `scripts/autodev-up.sh`.

## Known gaps (deliberate, for later)

- No cancel command: `temporal workflow terminate -w <id>` stops a workflow but leaves its job; `csub kill <job>`.
- The agent's tests need tools inside the job (e.g. pixi on `path_prepend` + `extra_mounts`, conda/pypi in
  `allow_hosts`); nothing installs them for you.
- CI failures after approval only escalate; there is no automatic fix-CI round.
- Claude sessions are not resumed across rounds; each round starts fresh from the PR diff + feedback.
- Temporal is a dev server under the running account; a shared production server is a later step.
````

`README.md`: append
```markdown

## autodev

An agentic issue → PR → approval → merge loop built on the same Temporal stack: see
[`notes/autodev-runbook.md`](notes/autodev-runbook.md).
```

- [ ] **Step 2: Run the full suite**

Run: `pixi run test`
Expected: all zarr-doctor and autodev tests pass, with no warnings about un-awaited coroutines.

- [ ] **Step 3: Live run as yourself (manual; record the results)**

```bash
cp autodev.yaml.example autodev.yaml         # deployment: autodev-$USER; work_root under allowed_roots; token_file
pixi run autodev check                       # every row ok
pixi run autodev init-labels
scripts/autodev-up.sh
gh issue create --title "autodev smoke: add CONTRIBUTORS.md" --body "Create CONTRIBUTORS.md listing 'autodev'." \
  --label "autodev-$USER:go"
```
Expected, within roughly 2 + walltime minutes:
- The issue gets `autodev-$USER:working` and a "Round 1 started: agent job `<id>`" comment.
- A PR `autodev-$USER/issue-N` appears containing `CONTRIBUTORS.md`, with the agent's summary as its first comment, and the issue moves to `:review`.

Then:
- Comment `/revise also mention the README` → a round-2 summary comment carrying a `handled comment-<id>` marker, and a new head commit.
- Comment `/approve <head sha>` → the PR is squash-merged, the branch is deleted, the issue is closed and labelled `:done`.

If the worker can't reach Temporal from inside the sandbox (the `localhost` CONNECT through the allowlist proxy), stop and report it; don't work around it. The fallback is a decision for the user: run the worker without the sandbox for the test, or move Temporal to a host the proxy can reach.

- [ ] **Step 4: Commit**

```bash
git add notes/autodev-runbook.md README.md
git commit -m "autodev: runbook (personal and service-account setup) and README pointer

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## What this plan deliberately leaves out

- **Fixing CI automatically.** An approved PR with failing checks only escalates.
- **Resuming Claude sessions across rounds.** csub's `--claude` config directory is thrown away after each job.
- **Webhooks**, plus a cancel command and a status command.
- **Worker Versioning, and the replay test as a required GitHub check.** Add both before the system starts editing `autodev/workflows.py` itself.
- **Release and deploy gates.** They come with the `ReleaseWorkflow` from the design note.

Each is a separate plan once this loop has run against real issues.
