"""Building the csub job specs for the four job kinds (match, smoke, crop, verify), plus the two things
every job needs: a read-only snapshot of mia-snippets at a commit, and the harness pin.

The record and bindings live under `paths.state_dir`, which is never mounted into a job: the caller copies
them into the job's workspace first, and the builders take their names relative to it (`record.yaml`,
`binding.yaml`). The snapshot, the harness checkout, the dataset cache and any extra input are mounted
read-only; only the workspace (and the dataset cache, for jobs that run reviewed code) is writable. Every
builder refuses (PinMoved) when the harness checkout is no longer the round's admitted pin.
"""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shlex
import shutil
import subprocess
from importlib.resources import files
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Any, Callable

import yaml

from autodev.gitops import GitError, Mirror, git
from autodev.jobs import JobSpec

from .binding import hosts, missing_hosts, record_urls
from .seed import ID_RE

if TYPE_CHECKING:
    from .config import Config
    from .models import Pins

SHA_RE = re.compile(r"[0-9a-f]{40}")
MATCH_TOOLS = "Bash(harness smoke:*),Read,Write,Glob,Grep"
HARNESS_ENV = ("MIA_HARNESS_JOB_BYTES", "MIA_HARNESS_COMMIT", "MIA_HARNESS_ENV_DIGEST", "MIA_HARNESS_ALLOWED_HOSTS",
               "MIA_HARNESS_CACHE_DIR", "MIA_HARNESS_CACHE_WRITABLE")  # every job's env; csub's env_allow must cover it
quote = shlex.quote


class PinMoved(RuntimeError):
    """The harness checkout moved since the round was admitted. Not retryable: the round goes to needs-human."""


class HostsNotAllowed(RuntimeError):
    """A job needs hosts the csub policy doesn't allow. Not retryable: the fix is a broker.toml edit."""

    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__(f"hosts not allowed by the csub policy: {', '.join(missing)} "
                         "(add them to broker.toml, and to network.redirect_hosts if they are redirect targets)")


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
        mirror = Mirror(cfg.paths.state_dir / "mirror" / "mia-snippets.git",
                        github.snippets_git_url or f"https://github.com/{github.snippets_repo}.git", tokens)
        if not (mirror.path / "HEAD").exists() or subprocess.run(
                ["git", "-C", str(mirror.path), "cat-file", "-e", f"{sha}^{{commit}}"], capture_output=True).returncode:
            mirror.sync()
        tmp = dest.with_name(f".{sha}.tmp")
        if tmp.exists():  # left by a crash; read-only, like a finished one
            subprocess.run(["chmod", "-R", "u+w", str(tmp)], check=True)
            shutil.rmtree(tmp)
        tmp.mkdir()
        # no LFS smudge (or any global/system filter) from a PR head's .gitattributes
        archive = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "filter.lfs.smudge=",
                                  "-c", "filter.lfs.process=", "-c", "filter.lfs.required=false", "-C", str(mirror.path),
                                  "archive", sha, "snippets", "WITHDRAWN.yaml", "SPEC.md"], capture_output=True,
                                 env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"})
        if archive.returncode:
            raise GitError(f"git archive {sha}: {archive.stderr.decode(errors='replace').strip()[-2000:]}")
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=archive.stdout, check=True)
        subprocess.run(["chmod", "-R", "a-w", str(tmp)], check=True)
        os.rename(tmp, dest)
        return dest


def harness_pin(cfg: Config) -> tuple[str, str]:
    """(commit, env digest) of the `harness.dir` checkout: HEAD and the sha256 of harness/pixi.lock."""
    harness_dir = cfg.harness.dir
    return (git("rev-parse", "HEAD", cwd=harness_dir),
            hashlib.sha256((harness_dir / "harness" / "pixi.lock").read_bytes()).hexdigest())


def assert_pin(cfg: Config, pins: Pins) -> None:
    """Raise PinMoved unless the harness checkout is still exactly what the round was admitted with."""
    current = harness_pin(cfg)
    if current != (pins.harness_commit, pins.env_digest):
        raise PinMoved(f"harness checkout {cfg.harness.dir} moved since admission: commit {pins.harness_commit[:12]} "
                       f"-> {current[0][:12]}, pixi.lock {pins.env_digest[:12]} -> {current[1][:12]}")


def check_hosts_allowed(spec: JobSpec, patterns: list[str]) -> None:
    """Refuse a job before submission if `csub probe`'s allowed_hosts patterns miss any of its hosts."""
    if missing := missing_hosts(spec.allow_hosts, patterns):
        raise HostsNotAllowed(missing)


def match_prompt(pins: Pins) -> str:
    """The match agent's prompt (prompts/match.md) with the round's pins filled in."""
    return Template(files("mia_data").joinpath("prompts/match.md").read_text()).substitute(
        record=pins.record, record_commit=pins.record_commit, snippets_commit=pins.snippets_commit)


def harness_bin(cfg: Config) -> Path:
    """The harness environment's entry point. Run directly: `pixi run` wants the install lock on the prefix, and
    `harness.dir` is mounted read-only."""
    return cfg.harness.dir / "harness" / ".pixi" / "envs" / "default" / "bin" / "harness"


def _harness(cfg: Config) -> str:
    return quote(str(harness_bin(cfg)))


def _binding(workspace: Path, rel: str) -> tuple[str, list[str]]:
    """(record id, URLs) of a binding the worker has already checked (check_binding)."""
    binding = yaml.safe_load(Path(workspace, rel).read_text())
    return binding.get("record"), [url for array in (binding.get("raw") or []) + (binding.get("labels") or [])
                                   for url in array.get("urls") or []]


def _parent(file: Path | None) -> Path | None:
    """The directory to mount for an input file (csub mounts directories only)."""
    return Path(file).parent if file else None


def _spec(cfg: Config, kind: str, workspace: Path, pins: Pins, body: str, allow_hosts: list[str],
          ro_inputs: list[Path | None], record: str | None = None, writable: bool = False, **kwargs: Any) -> JobSpec:
    """The JobSpec shared by every kind: harness env vars, mounts, and the wrapper that records the exit code.

    `record` enables the per-dataset download cache (read-write when `writable`); `ro_inputs` are extra
    read-only mounts (None entries are skipped)."""
    assert_pin(cfg, pins)  # the admitted harness, or no job
    env = {"MIA_HARNESS_JOB_BYTES": str(int(cfg.limits.job_bytes)), "MIA_HARNESS_COMMIT": pins.harness_commit,
           "MIA_HARNESS_ENV_DIGEST": pins.env_digest, "MIA_HARNESS_ALLOWED_HOSTS": ",".join(allow_hosts),
           "MIA_HARNESS_CACHE_DIR": "", "MIA_HARNESS_CACHE_WRITABLE": "0"}
    ro_mounts = [str(cfg.harness.dir), *(str(path) for path in ro_inputs if path)]
    rw_mounts: list[str] = []
    if record is not None:  # the dataset cache
        if not (isinstance(record, str) and ID_RE.fullmatch(record)):
            raise ValueError(f"bad record id {record!r}")
        cache = cfg.paths.work_root / "_cache" / record
        cache.mkdir(parents=True, exist_ok=True)  # a read-only mount must exist
        (rw_mounts if writable else ro_mounts).append(str(cache))
        env.update(MIA_HARNESS_CACHE_DIR=str(cache), MIA_HARNESS_CACHE_WRITABLE="1" if writable else "0")
    path = (f'export PATH={":".join(quote(entry) for entry in cfg.agent.path_prepend)}:"$PATH"\n'
            if cfg.agent.path_prepend else "")
    # ponytail: plain jobs take JobSpec's 1 cpu / 4000 MB; add limits.job_cpus / job_mem_mb if crops need more.
    return JobSpec(name=f"{cfg.deployment}-{kind}",
                   command=f'set -u\n{path}{body}\nrc=$?\necho "$rc" > exit_code\nexit "$rc"\n',
                   workspace=str(workspace), walltime_min=cfg.limits.job_walltime_min, allow_hosts=allow_hosts,
                   ro_mounts=ro_mounts, rw_mounts=rw_mounts, env=env, **kwargs)


def match_job(cfg: Config, workspace: Path, pins: Pins, record_path: str = "record.yaml") -> JobSpec:
    """The match agent: Claude Code with the harness's `smoke` command as its only tool besides reading.

    The caller has written `prompt.md` (match_prompt), `record.yaml`, `downloads.json`, any `conversion.json`
    / `manifest.json`, `catalog/` and `SPEC.md` into the workspace, and made the snapshot of
    `pins.snippets_commit`. Anthropic hosts come from the csub policy's [agents] table, never from here."""
    record = yaml.safe_load(Path(workspace, record_path).read_text()) or {}
    snapshot_dir = _snapshot_dir(cfg, pins.snippets_commit)
    body = (f"ln -sfn {quote(str(snapshot_dir))} snapshot && mkdir -p bin && \\\n"
            f"printf '#!/bin/sh\\nexec %s \"$@\"\\n' {quote(_harness(cfg))} > bin/harness && chmod +x bin/harness && \\\n"
            f'PATH="$PWD/bin:$PATH" claude -p "$(cat prompt.md)" --model {quote(cfg.agent.model)} '
            f"--max-budget-usd {cfg.limits.agent_max_budget_usd} --output-format json \\\n"
            f'  --allowedTools "{MATCH_TOOLS}" > claude.json 2> claude.err')
    return _spec(cfg, "match", workspace, pins, body, hosts(record_urls(record), cfg), [snapshot_dir],
                 record=pins.record, agent="claude", cpus=cfg.agent.cpus, mem_mb=cfg.agent.mem_mb)


def smoke_job(cfg: Config, workspace: Path, pins: Pins, binding_path: str, snapshot: Path,
              record_path: str) -> JobSpec:
    """The worker's own smoke of a binding, so the agent's claim is checked. Reviewed code: cache read-write."""
    record, urls = _binding(workspace, binding_path)
    body = (f"{_harness(cfg)} smoke {quote(binding_path)} --snippets {quote(str(snapshot))} "
            f"--record {quote(record_path)} --out smoke.json")
    return _spec(cfg, "smoke", workspace, pins, body, hosts(urls, cfg), [snapshot], record=record, writable=True)


def crop_job(cfg: Config, workspace: Path, pins: Pins, binding_path: str, snapshot: Path, record_path: str,
             n_crops: int, seed: str, round_id: str, batch: int, existing: Path | None, trusted: bool) -> JobSpec:
    """`harness crop` into `<workspace>/crops`.

    `trusted` (a matched or merged SHA, or production) mounts the dataset cache read-write; a PR head gets it
    read-only and keeps a private cache. `existing` is a published manifest.json to avoid duplicating."""
    record, urls = _binding(workspace, binding_path)
    body = (f"{_harness(cfg)} crop {quote(binding_path)} --snippets {quote(str(snapshot))} --record {quote(record_path)} "
            f"--n {int(n_crops)} --seed {quote(seed)} --round {quote(round_id)} --batch {int(batch)} --out crops"
            + (f" --existing {quote(str(existing))}" if existing else ""))
    return _spec(cfg, "crop", workspace, pins, body, hosts(urls, cfg), [snapshot, _parent(existing)], record=record,
                 writable=trusted)


def verify_job(cfg: Config, workspace: Path, pins: Pins, crops_dir: Path, binding_path: str,
               existing: Path | None) -> JobSpec:
    """`harness verify` then `harness preview` on a crop job's output, mounted read-only. Loads no snippet
    code: no network, no dataset cache."""
    harness, crops = _harness(cfg), quote(str(crops_dir))
    body = (f"{harness} verify {crops} --binding {quote(binding_path)}"
            + (f" --existing {quote(str(existing))}" if existing else "")
            + f" --out verify.json; v=$?\n{harness} preview {crops} --out preview && (exit $v)")  # overlays even if a crop fails
    return _spec(cfg, "verify", workspace, pins, body, [], [crops_dir, _parent(existing)])
