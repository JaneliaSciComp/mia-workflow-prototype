"""Building the csub job specs for the four job kinds (match, smoke, crop, verify), plus the two things
every job needs: a read-only snapshot of mia-snippets at a commit, and the harness pin.

The record and bindings live under `paths.state_dir`, which is never mounted into a job: the caller copies
them into the job's workspace first, and the builders take their names relative to it (`record.yaml`,
`binding.yaml`). The snippet snapshot, the harness snapshot, the harness environment, the dataset cache and
any extra input are mounted read-only; only the workspace (and the dataset cache, for jobs that run reviewed
code) is writable.

A round carries its own harness: its source is a snapshot of `harness/src` at the admitted commit, taken from
the `harness.dir` checkout, and its environment is that checkout's pixi env, pinned by the sha256 of
`pixi.lock`. Commits in `harness.dir` therefore never touch an in-flight round; only a changed `pixi.lock`
does, and every builder then refuses (PinMoved).
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any

import yaml

from autodev.gitops import GitError, Mirror, git
from autodev.jobs import JobSpec

from .binding import hosts, missing_hosts, record_urls
from .seed import ID_RE

if TYPE_CHECKING:
    from .config import Config
    from .models import JobArgs, Pins

SHA_RE = re.compile(r"[0-9a-f]{40}")
MATCH_TOOLS = "Bash(harness smoke:*),Read,Write,Glob,Grep"
HARNESS_ENV = (
    "MIA_HARNESS_JOB_BYTES",
    "MIA_HARNESS_COMMIT",
    "MIA_HARNESS_ENV_DIGEST",
    "MIA_HARNESS_ALLOWED_HOSTS",
    "MIA_HARNESS_CACHE_DIR",
    "MIA_HARNESS_CACHE_WRITABLE",
)  # every job's env; csub's env_allow must cover it
quote = shlex.quote
BINDING_FILE = "binding.yaml"  # the round's checked binding, copied into the workspace by prepare_job
RECORD_FILE = "record.yaml"  # the dataset record, copied into every workspace


class PinMoved(RuntimeError):
    """The harness environment changed since the round was admitted. Not retryable: the round goes to needs-human."""


class HostsNotAllowed(RuntimeError):
    """A job needs hosts the csub policy doesn't allow. Not retryable: the fix is a broker.toml edit."""

    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__(
            f"hosts not allowed by the csub policy: {', '.join(missing)} "
            "(add them to broker.toml, and to network.redirect_hosts if they are redirect targets)"
        )


def _snapshot_dir(cfg: Config, sha: str) -> Path:
    return cfg.paths.work_root / "_snapshots" / sha


def snapshot(cfg: Config, sha: str, tokens: Callable[[], str | None] = lambda: None) -> Path:
    """`snippets/`, `WITHDRAWN.yaml` and `SPEC.md` of mia-snippets at `sha`, extracted once and read-only, at
    `work_root/_snapshots/<sha>`. The bare mirror in `state_dir` is fetched only when it lacks `sha`."""
    if not SHA_RE.fullmatch(sha):
        raise ValueError(f"not a full commit SHA: {sha!r}")
    dest = _snapshot_dir(cfg, sha)
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest.with_name(f".{sha}.lock"), "w") as lock:  # one fetch + extract per SHA at a time
        fcntl.flock(lock, fcntl.LOCK_EX)
        if dest.exists():
            return dest
        github = cfg.github
        mirror = Mirror(
            cfg.paths.state_dir / "mirror" / "mia-snippets.git",
            github.snippets_git_url or f"https://github.com/{github.snippets_repo}.git",
            tokens,
        )
        if (
            not (mirror.path / "HEAD").exists()
            or subprocess.run(
                ["git", "-C", str(mirror.path), "cat-file", "-e", f"{sha}^{{commit}}"], capture_output=True
            ).returncode
        ):
            mirror.sync()
        tmp = dest.with_name(f".{sha}.tmp")
        if tmp.exists():  # left by a crash; read-only, like a finished one
            subprocess.run(["chmod", "-R", "u+w", str(tmp)], check=True)
            shutil.rmtree(tmp)
        tmp.mkdir()
        # no LFS smudge (or any global/system filter) from a PR head's .gitattributes
        archive = subprocess.run(
            [
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "filter.lfs.smudge=",
                "-c",
                "filter.lfs.process=",
                "-c",
                "filter.lfs.required=false",
                "-C",
                str(mirror.path),
                "archive",
                sha,
                "snippets",
                "WITHDRAWN.yaml",
                "SPEC.md",
            ],
            capture_output=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"},
        )
        if archive.returncode:
            raise GitError(f"git archive {sha}: {archive.stderr.decode(errors='replace').strip()[-2000:]}")
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=archive.stdout, check=True)
        subprocess.run(["chmod", "-R", "a-w", str(tmp)], check=True)
        os.rename(tmp, dest)
        return dest


def _env_digest(cfg: Config) -> str:
    return hashlib.sha256((cfg.harness.dir / "harness" / "pixi.lock").read_bytes()).hexdigest()


def harness_pin(cfg: Config) -> tuple[str, str]:
    """(commit, env digest) of the `harness.dir` checkout: HEAD and the sha256 of harness/pixi.lock."""
    return git("rev-parse", "HEAD", cwd=cfg.harness.dir), _env_digest(cfg)


def _harness_snapshot_dir(cfg: Config, commit: str) -> Path:
    return cfg.paths.work_root / "_harness" / commit


def harness_snapshot(cfg: Config, commit: str) -> Path:
    """`harness/src` of the `harness.dir` checkout at `commit`, extracted once and read-only, at
    `work_root/_harness/<commit>/src`. Taken at admission, so later commits in the checkout don't reach the round."""
    if not SHA_RE.fullmatch(commit):
        raise ValueError(f"not a full commit SHA: {commit!r}")
    dest = _harness_snapshot_dir(cfg, commit)
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest.with_name(f".{commit}.lock"), "w") as lock:  # one extract per commit at a time
        fcntl.flock(lock, fcntl.LOCK_EX)
        if dest.exists():
            return dest
        tmp = dest.with_name(f".{commit}.tmp")
        if tmp.exists():  # left by a crash; read-only, like a finished one
            subprocess.run(["chmod", "-R", "u+w", str(tmp)], check=True)
            shutil.rmtree(tmp)
        tmp.mkdir()
        archive = subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-C", str(cfg.harness.dir), "archive", commit, "harness/src"],
            capture_output=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"},
        )
        if archive.returncode:
            raise GitError(
                f"git archive {commit} harness/src in {cfg.harness.dir}: "
                f"{archive.stderr.decode(errors='replace').strip()[-2000:]}"
            )
        subprocess.run(["tar", "-x", "--strip-components=1", "-C", str(tmp)], input=archive.stdout, check=True)
        subprocess.run(["chmod", "-R", "a-w", str(tmp)], check=True)
        os.rename(tmp, dest)
        return dest


def assert_pin(cfg: Config, pins: Pins) -> None:
    """Raise PinMoved unless the harness environment is still the one the round was admitted with.

    Only the `pixi.lock` digest is compared: the harness commit is served by its snapshot, so HEAD may move."""
    current = _env_digest(cfg)
    if current != pins.env_digest:
        raise PinMoved(
            f"harness environment in {cfg.harness.dir} changed since admission: pixi.lock "
            f"{pins.env_digest[:12]} -> {current[:12]}"
        )


def check_hosts_allowed(spec: JobSpec, patterns: list[str]) -> None:
    """Refuse a job before submission if `csub probe`'s allowed_hosts patterns miss any of its hosts."""
    if missing := missing_hosts(spec.allow_hosts, patterns):
        raise HostsNotAllowed(missing)


def match_prompt(pins: Pins) -> str:
    """The match agent's prompt (prompts/match.md) with the round's pins filled in."""
    return Template(files("mia_data").joinpath("prompts/match.md").read_text()).substitute(
        record=pins.record, record_commit=pins.record_commit, snippets_commit=pins.snippets_commit
    )


def harness_env(cfg: Config) -> Path:
    """The harness environment's python. Run directly: `pixi run` wants the install lock on the prefix, and
    `harness.dir` is mounted read-only."""
    return cfg.harness.dir / "harness" / ".pixi" / "envs" / "default" / "bin" / "python"


def _harness(cfg: Config, pins: Pins) -> str:
    """The `harness` command for a round: the pinned environment's python running the snapshot's source.

    PYTHONPATH precedes the env's editable install of the checkout's own source; -P keeps the job's cwd off
    sys.path."""
    src = _harness_snapshot_dir(cfg, pins.harness_commit) / "src"
    return f"env PYTHONPATH={quote(str(src))} {quote(str(harness_env(cfg)))} -P -c " + quote(
        "import sys; from mia_harness.cli import main; sys.exit(main())"
    )


def _binding(workspace: Path, rel: str) -> tuple[str, list[str]]:
    """(record id, URLs) of a binding the worker has already checked (check_binding)."""
    binding = yaml.safe_load(Path(workspace, rel).read_text())
    return binding.get("record"), [
        url for array in (binding.get("raw") or []) + (binding.get("labels") or []) for url in array.get("urls") or []
    ]


def _parent(file: Path | None) -> Path | None:
    """The directory to mount for an input file (csub mounts directories only)."""
    return Path(file).parent if file else None


def _spec(
    cfg: Config,
    kind: str,
    workspace: Path,
    pins: Pins,
    body: str,
    allow_hosts: list[str],
    ro_inputs: list[Path | None],
    record: str | None = None,
    writable: bool = False,
    **kwargs: Any,
) -> JobSpec:
    """The JobSpec shared by every kind: harness env vars, mounts, and the wrapper that records the exit code.

    `record` enables the per-dataset download cache (read-write when `writable`); `ro_inputs` are extra
    read-only mounts (None entries are skipped)."""
    assert_pin(cfg, pins)  # the admitted environment, or no job
    harness_src = harness_snapshot(cfg, pins.harness_commit)  # the admitted source (made at admission; idempotent)
    env = {
        "MIA_HARNESS_JOB_BYTES": str(int(cfg.limits.job_bytes)),
        "MIA_HARNESS_COMMIT": pins.harness_commit,
        "MIA_HARNESS_ENV_DIGEST": pins.env_digest,
        "MIA_HARNESS_ALLOWED_HOSTS": ",".join(allow_hosts),
        "MIA_HARNESS_CACHE_DIR": "",
        "MIA_HARNESS_CACHE_WRITABLE": "0",
    }
    ro_mounts = [str(cfg.harness.dir), str(harness_src), *(str(path) for path in ro_inputs if path)]
    rw_mounts: list[str] = []
    if record is not None:  # the dataset cache
        if not (isinstance(record, str) and ID_RE.fullmatch(record)):
            raise ValueError(f"bad record id {record!r}")
        cache = cfg.paths.work_root / "_cache" / record
        cache.mkdir(parents=True, exist_ok=True)  # a read-only mount must exist
        (rw_mounts if writable else ro_mounts).append(str(cache))
        env.update(MIA_HARNESS_CACHE_DIR=str(cache), MIA_HARNESS_CACHE_WRITABLE="1" if writable else "0")
    # every job calls `harness`: a wrapper in the workspace, first on PATH, runs the round's pinned harness
    path_entries = ":".join([*(quote(entry) for entry in cfg.agent.path_prepend), '"$PATH"'])
    prelude = (
        f"mkdir -p bin && printf '#!/bin/sh\\nexec %s \"$@\"\\n' {quote(_harness(cfg, pins))} > bin/harness && "
        f'chmod +x bin/harness\nexport PATH="$PWD/bin":{path_entries}\n'
    )
    # ponytail: plain jobs take JobSpec's 1 cpu / 4000 MB; add limits.job_cpus / job_mem_mb if crops need more.
    return JobSpec(
        name=f"{cfg.deployment}-{kind}",
        command=f'set -u\n{prelude}{body}\nrc=$?\necho "$rc" > exit_code\nexit "$rc"\n',
        workspace=str(workspace),
        walltime_min=cfg.limits.job_walltime_min,
        allow_hosts=allow_hosts,
        ro_mounts=ro_mounts,
        rw_mounts=rw_mounts,
        env=env,
        **kwargs,
    )


def match_job(cfg: Config, args: JobArgs) -> JobSpec:
    """The match agent: Claude Code with the harness's `smoke` command as its only tool besides reading.

    The caller has written `prompt.md` (match_prompt), `record.yaml`, `downloads.json`, any `conversion.json`
    / `manifest.json`, `catalog/` and `SPEC.md` into the workspace, and made the snapshot of
    `pins.snippets_commit`. Anthropic hosts come from the csub policy's [agents] table, never from here."""
    workspace, pins = Path(args.workspace), args.pins
    record = yaml.safe_load((workspace / RECORD_FILE).read_text()) or {}
    snapshot_dir = _snapshot_dir(cfg, pins.snippets_commit)
    body = (
        f"ln -sfn {quote(str(snapshot_dir))} snapshot && \\\n"
        f'claude -p "$(cat prompt.md)" --model {quote(cfg.agent.model)} '
        f"--max-budget-usd {cfg.limits.agent_max_budget_usd} --output-format json \\\n"
        f'  --allowedTools "{MATCH_TOOLS}" > claude.json 2> claude.err'
    )
    return _spec(
        cfg,
        "match",
        workspace,
        pins,
        body,
        hosts(record_urls(record), cfg),
        [snapshot_dir],
        record=pins.record,
        agent="claude",
        cpus=cfg.agent.cpus,
        mem_mb=cfg.agent.mem_mb,
    )


def smoke_job(cfg: Config, args: JobArgs, snapshot: Path) -> JobSpec:
    """The worker's own smoke of a binding, so the agent's claim is checked. Reviewed code: cache read-write."""
    workspace = Path(args.workspace)
    record, urls = _binding(workspace, BINDING_FILE)
    body = f"harness smoke {BINDING_FILE} --snippets {quote(str(snapshot))} --record {RECORD_FILE} --out smoke.json"
    return _spec(cfg, "smoke", workspace, args.pins, body, hosts(urls, cfg), [snapshot], record=record, writable=True)


def crop_job(cfg: Config, args: JobArgs, snapshot: Path, existing: Path | None) -> JobSpec:
    """`harness crop` into `<workspace>/crops`, `args.n_crops` of them from `args.seed`.

    `args.trusted` (a matched or merged SHA, or production) mounts the dataset cache read-write; a PR head gets
    it read-only and keeps a private cache. `existing` is a published manifest.json to avoid duplicating."""
    workspace = Path(args.workspace)
    record, urls = _binding(workspace, BINDING_FILE)
    body = (
        f"harness crop {BINDING_FILE} --snippets {quote(str(snapshot))} --record {RECORD_FILE} "
        f"--n {int(args.n_crops)} --seed {quote(args.seed)} --round {quote(args.round_id)} "
        f"--batch {int(args.batch)} --out crops" + (f" --existing {quote(str(existing))}" if existing else "")
    )
    return _spec(
        cfg,
        "crop",
        workspace,
        args.pins,
        body,
        hosts(urls, cfg),
        [snapshot, _parent(existing)],
        record=record,
        writable=args.trusted,
    )


def verify_job(cfg: Config, args: JobArgs, crops_dir: Path, existing: Path | None) -> JobSpec:
    """`harness verify` then `harness preview` on a crop job's output, mounted read-only. Loads no snippet
    code: no network, no dataset cache."""
    crops = quote(str(crops_dir))
    body = (
        f"harness verify {crops} --binding {BINDING_FILE}"
        + (f" --existing {quote(str(existing))}" if existing else "")
        + f" --out verify.json; v=$?\nharness preview {crops} --out preview && (exit $v)"
    )  # overlays even if a crop fails
    return _spec(cfg, "verify", Path(args.workspace), args.pins, body, [], [crops_dir, _parent(existing)])
