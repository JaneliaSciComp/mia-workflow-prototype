"""Job specs (match, smoke, crop, verify), snippet snapshots at a SHA, and the harness pin (spec §6, §7.2, §8.1, §9).

The record and bindings live under `paths.state_dir`, which is never mounted into a job: the caller copies them
into the job's workspace first, and the builders take their names relative to it (`record.yaml`, `binding.yaml`).
The snippet snapshot, the harness snapshot, the harness environment, the dataset cache and any extra input are
mounted read-only; only the workspace (and the dataset cache, for jobs that run reviewed code) is writable.

A round carries its own harness (§6): its source is a snapshot of `harness/src` at the admitted commit, taken from
the `harness.dir` checkout, and its environment is that checkout's pixi env, pinned by the sha256 of `pixi.lock`.
Commits in `harness.dir` therefore never touch an in-flight round; only a changed `pixi.lock` does, and every
builder then refuses (PinMoved)."""

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

import yaml

from autodev.gitops import GitError, Mirror, git
from autodev.jobs import JobSpec

from .binding import hosts, missing_hosts, record_urls
from .seed import ID_RE

SHA_RE = re.compile(r"[0-9a-f]{40}")
MATCH_TOOLS = "Bash(harness smoke:*),Read,Write,Glob,Grep"
HARNESS_ENV = ("MIA_HARNESS_JOB_BYTES", "MIA_HARNESS_COMMIT", "MIA_HARNESS_ENV_DIGEST", "MIA_HARNESS_ALLOWED_HOSTS",
               "MIA_HARNESS_CACHE_DIR", "MIA_HARNESS_CACHE_WRITABLE")  # every job's env; csub's env_allow must cover it
q = shlex.quote


class PinMoved(RuntimeError):
    """The harness environment changed since the round was admitted. Not retryable: the round goes to needs-human."""


class HostsNotAllowed(RuntimeError):
    """A job needs hosts the csub policy doesn't allow. Not retryable: the fix is a broker.toml edit (§9)."""

    def __init__(self, missing: list[str]):
        self.missing = missing
        super().__init__(f"hosts not allowed by the csub policy: {', '.join(missing)} "
                         "(add them to broker.toml, and to network.redirect_hosts if they are redirect targets)")


def _snap_dir(cfg, sha: str) -> Path:
    return cfg.paths.work_root / "_snapshots" / sha


def snapshot(cfg, sha: str, tokens=lambda: None) -> Path:
    """`snippets/`, `WITHDRAWN.yaml` and `SPEC.md` of mia-snippets at `sha`, extracted once and read-only, at
    `work_root/_snapshots/<sha>`. The bare mirror in `state_dir` is fetched only when it lacks `sha`."""
    if not SHA_RE.fullmatch(sha):
        raise ValueError(f"not a full commit SHA: {sha!r}")
    dest = _snap_dir(cfg, sha)
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest.with_name(f".{sha}.lock"), "w") as lock:  # one fetch + extract per SHA at a time
        fcntl.flock(lock, fcntl.LOCK_EX)
        if dest.exists():
            return dest
        g = cfg.github
        m = Mirror(cfg.paths.state_dir / "mirror" / "mia-snippets.git",
                   g.snippets_git_url or f"https://github.com/{g.snippets_repo}.git", tokens)
        if not (m.path / "HEAD").exists() or subprocess.run(
                ["git", "-C", str(m.path), "cat-file", "-e", f"{sha}^{{commit}}"], capture_output=True).returncode:
            m.sync()
        tmp = dest.with_name(f".{sha}.tmp")
        if tmp.exists():  # left by a crash; read-only, like a finished one
            subprocess.run(["chmod", "-R", "u+w", str(tmp)], check=True)
            shutil.rmtree(tmp)
        tmp.mkdir()
        # no LFS smudge (or any global/system filter) from a PR head's .gitattributes
        r = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-c", "filter.lfs.smudge=",
                            "-c", "filter.lfs.process=", "-c", "filter.lfs.required=false", "-C", str(m.path),
                            "archive", sha, "snippets", "WITHDRAWN.yaml", "SPEC.md"], capture_output=True,
                           env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"})
        if r.returncode:
            raise GitError(f"git archive {sha}: {r.stderr.decode(errors='replace').strip()[-2000:]}")
        subprocess.run(["tar", "-x", "-C", str(tmp)], input=r.stdout, check=True)
        subprocess.run(["chmod", "-R", "a-w", str(tmp)], check=True)
        os.rename(tmp, dest)
        return dest


def _env_digest(cfg) -> str:
    return hashlib.sha256((cfg.harness.dir / "harness" / "pixi.lock").read_bytes()).hexdigest()


def harness_pin(cfg) -> tuple[str, str]:
    """(commit, env digest) of the `harness.dir` checkout: HEAD and the sha256 of harness/pixi.lock."""
    return git("rev-parse", "HEAD", cwd=cfg.harness.dir), _env_digest(cfg)


def _harness_snap_dir(cfg, commit: str) -> Path:
    return cfg.paths.work_root / "_harness" / commit


def harness_snapshot(cfg, commit: str) -> Path:
    """`harness/src` of the `harness.dir` checkout at `commit`, extracted once and read-only, at
    `work_root/_harness/<commit>/src`. Taken at admission, so later commits in the checkout don't reach the round."""
    if not SHA_RE.fullmatch(commit):
        raise ValueError(f"not a full commit SHA: {commit!r}")
    dest = _harness_snap_dir(cfg, commit)
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest.with_name(f".{commit}.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if dest.exists():
            return dest
        tmp = dest.with_name(f".{commit}.tmp")
        if tmp.exists():
            subprocess.run(["chmod", "-R", "u+w", str(tmp)], check=True)
            shutil.rmtree(tmp)
        tmp.mkdir()
        r = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(cfg.harness.dir), "archive", commit,
                            "harness/src"], capture_output=True,
                           env={**os.environ, "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"})
        if r.returncode:
            raise GitError(f"git archive {commit} harness/src in {cfg.harness.dir}: "
                           f"{r.stderr.decode(errors='replace').strip()[-2000:]}")
        subprocess.run(["tar", "-x", "--strip-components=1", "-C", str(tmp)], input=r.stdout, check=True)
        subprocess.run(["chmod", "-R", "a-w", str(tmp)], check=True)
        os.rename(tmp, dest)
        return dest


def assert_pin(cfg, pins) -> None:
    """The admitted environment, or no job. The harness commit is served by its snapshot, so HEAD may move."""
    now = _env_digest(cfg)
    if now != pins.env_digest:
        raise PinMoved(f"harness environment in {cfg.harness.dir} changed since admission: pixi.lock "
                       f"{pins.env_digest[:12]} -> {now[:12]}")


def check_hosts_allowed(spec: JobSpec, patterns: list[str]) -> None:
    """Refuse a job before submission if `csub probe`'s allowed_hosts patterns miss any of its hosts."""
    if missing := missing_hosts(spec.allow_hosts, patterns):
        raise HostsNotAllowed(missing)


def match_prompt(pins) -> str:
    return Template(files("mia_data").joinpath("prompts/match.md").read_text()).substitute(
        record=pins.record, record_commit=pins.record_commit, snippets_commit=pins.snippets_commit)


def harness_env(cfg) -> Path:
    """The harness environment's python. Run directly: `pixi run` wants the install lock on the prefix, and
    `harness.dir` is mounted read-only."""
    return cfg.harness.dir / "harness" / ".pixi" / "envs" / "default" / "bin" / "python"


def _harness(cfg, pins) -> str:
    """The `harness` command for a round: the pinned environment's python running the snapshot's source. -P keeps
    the job's cwd off sys.path; PYTHONPATH precedes the env's editable install of the checkout's own source."""
    src = _harness_snap_dir(cfg, pins.harness_commit) / "src"
    return (f"env PYTHONPATH={q(str(src))} {q(str(harness_env(cfg)))} -P -c "
            + q("import sys; from mia_harness.cli import main; sys.exit(main())"))


def _binding(ws, rel: str) -> tuple[str, list[str]]:
    """(record id, URLs) of a binding the worker has already checked (check_binding)."""
    b = yaml.safe_load(Path(ws, rel).read_text())
    return b.get("record"), [u for a in (b.get("raw") or []) + (b.get("labels") or []) for u in a.get("urls") or []]


def _parent(f):
    """The directory to mount for an input file (csub mounts directories only)."""
    return Path(f).parent if f else None


def _spec(cfg, kind: str, ws, pins, body: str, allow: list[str], ro: list, record: str | None = None,
          writable: bool = False, **kw) -> JobSpec:
    assert_pin(cfg, pins)  # the admitted environment, or no job
    hsnap = harness_snapshot(cfg, pins.harness_commit)  # the admitted source (made at admission; idempotent)
    env = {"MIA_HARNESS_JOB_BYTES": str(int(cfg.limits.job_bytes)), "MIA_HARNESS_COMMIT": pins.harness_commit,
           "MIA_HARNESS_ENV_DIGEST": pins.env_digest, "MIA_HARNESS_ALLOWED_HOSTS": ",".join(allow),
           "MIA_HARNESS_CACHE_DIR": "", "MIA_HARNESS_CACHE_WRITABLE": "0"}
    ro, rw = [str(cfg.harness.dir), str(hsnap), *(str(p) for p in ro if p)], []
    if record is not None:  # the dataset cache, §6.6
        if not (isinstance(record, str) and ID_RE.fullmatch(record)):
            raise ValueError(f"bad record id {record!r}")
        cache = cfg.paths.work_root / "_cache" / record
        cache.mkdir(parents=True, exist_ok=True)  # a read-only mount must exist
        (rw if writable else ro).append(str(cache))
        env.update(MIA_HARNESS_CACHE_DIR=str(cache), MIA_HARNESS_CACHE_WRITABLE="1" if writable else "0")
    # every job calls `harness`: a wrapper in the workspace, first on PATH, runs the round's pinned harness
    pre = (f"mkdir -p bin && printf '#!/bin/sh\\nexec %s \"$@\"\\n' {q(_harness(cfg, pins))} > bin/harness && "
           f"chmod +x bin/harness\nexport PATH=\"$PWD/bin\":{':'.join([*(q(p) for p in cfg.agent.path_prepend), '"$PATH"'])}\n")
    # ponytail: plain jobs take JobSpec's 1 cpu / 4000 MB; add limits.job_cpus / job_mem_mb if crops need more.
    return JobSpec(name=f"{cfg.deployment}-{kind}", command=f'set -u\n{pre}{body}\nrc=$?\necho "$rc" > exit_code\nexit "$rc"\n',
                   workspace=str(ws), walltime_min=cfg.limits.job_walltime_min, allow_hosts=allow, ro_mounts=ro,
                   rw_mounts=rw, env=env, **kw)


def match_job(cfg, ws, pins, record_path: str = "record.yaml") -> JobSpec:
    """The match agent (§7.2). The caller has written `prompt.md` (match_prompt), `record.yaml`, `downloads.json`,
    any `conversion.json` / `manifest.json`, `catalog/` and `SPEC.md` into `ws`, and made the snapshot of
    `pins.snippets_commit`. Anthropic hosts come from the csub policy's [agents] table, never from here."""
    rec = yaml.safe_load(Path(ws, record_path).read_text()) or {}
    snap = _snap_dir(cfg, pins.snippets_commit)
    body = (f"ln -sfn {q(str(snap))} snapshot && \\\n"
            f'claude -p "$(cat prompt.md)" --model {q(cfg.agent.model)} '
            f"--max-budget-usd {cfg.limits.agent_max_budget_usd} --output-format json \\\n"
            f'  --allowedTools "{MATCH_TOOLS}" > claude.json 2> claude.err')
    return _spec(cfg, "match", ws, pins, body, hosts(record_urls(rec), cfg), [snap],
                 record=pins.record, agent="claude", cpus=cfg.agent.cpus, mem_mb=cfg.agent.mem_mb)


def smoke_job(cfg, ws, pins, binding_path: str, snapshot, record_path: str) -> JobSpec:
    """The worker's own smoke of a binding, so the agent's claim is checked (§7.2). Reviewed code: cache rw."""
    record, urls = _binding(ws, binding_path)
    body = f"harness smoke {q(binding_path)} --snippets {q(str(snapshot))} --record {q(record_path)} --out smoke.json"
    return _spec(cfg, "smoke", ws, pins, body, hosts(urls, cfg), [snapshot], record=record, writable=True)


def crop_job(cfg, ws, pins, binding_path: str, snapshot, record_path: str, n: int, seed: str, round_id: str, batch: int,
             existing, trusted: bool) -> JobSpec:
    """`harness crop` into `ws/crops` (§7.4, §7.5). `trusted` (a matched or merged SHA, or production) mounts the
    dataset cache read-write; a PR head gets it read-only and keeps a private cache (§6.6)."""
    record, urls = _binding(ws, binding_path)
    body = (f"harness crop {q(binding_path)} --snippets {q(str(snapshot))} --record {q(record_path)} "
            f"--n {int(n)} --seed {q(seed)} --round {q(round_id)} --batch {int(batch)} --out crops"
            + (f" --existing {q(str(existing))}" if existing else ""))
    return _spec(cfg, "crop", ws, pins, body, hosts(urls, cfg), [snapshot, _parent(existing)], record=record,
                 writable=trusted)


def verify_job(cfg, ws, pins, crops_dir, binding_path: str, existing) -> JobSpec:
    """`harness verify` then `harness preview` on a crop job's output, mounted read-only (§6.4, §6.7). Loads no
    snippet code: no network, no dataset cache."""
    d = q(str(crops_dir))
    body = (f"harness verify {d} --binding {q(binding_path)}" + (f" --existing {q(str(existing))}" if existing else "")
            + f" --out verify.json; v=$?\nharness preview {d} --out preview && (exit $v)")  # overlays even if a crop fails
    return _spec(cfg, "verify", ws, pins, body, [], [crops_dir, _parent(existing)])
