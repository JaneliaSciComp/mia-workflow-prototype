"""Job specs (match, smoke, crop, verify), snippet snapshots at a SHA, and the harness pin (spec §6, §7.2, §8.1, §9).

The record and bindings live under `paths.state_dir`, which is never mounted into a job: the caller copies them
into the job's workspace first, and the builders take their names relative to it (`record.yaml`, `binding.yaml`).
The snapshot, the harness checkout, the dataset cache and any extra input are mounted read-only; only the workspace
(and the dataset cache, for jobs that run reviewed code) is writable."""

from __future__ import annotations

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

from autodev.gitops import Mirror, git
from autodev.jobs import JobSpec

from .binding import hosts, missing_hosts, record_urls
from .seed import ID_RE

SHA_RE = re.compile(r"[0-9a-f]{40}")
MATCH_TOOLS = "Bash(harness smoke:*),Bash(pixi run:*),Read,Write,Glob,Grep"
q = shlex.quote


class PinMoved(RuntimeError):
    """The harness checkout moved since the round was admitted. Not retryable: the round goes to needs-human."""


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
    tmp.mkdir(parents=True)
    tar = subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(m.path), "archive", sha,
                          "snippets", "WITHDRAWN.yaml", "SPEC.md"], check=True, capture_output=True).stdout
    subprocess.run(["tar", "-x", "-C", str(tmp)], input=tar, check=True)
    subprocess.run(["chmod", "-R", "a-w", str(tmp)], check=True)
    os.rename(tmp, dest)
    return dest


def harness_pin(cfg) -> tuple[str, str]:
    """(commit, env digest) of the `harness.dir` checkout: HEAD and the sha256 of harness/pixi.lock."""
    h = cfg.harness.dir
    return git("rev-parse", "HEAD", cwd=h), hashlib.sha256((h / "harness" / "pixi.lock").read_bytes()).hexdigest()


def assert_pin(cfg, pins) -> None:
    now = harness_pin(cfg)
    if now != (pins.harness_commit, pins.env_digest):
        raise PinMoved(f"harness checkout {cfg.harness.dir} moved since admission: commit {pins.harness_commit[:12]} "
                       f"-> {now[0][:12]}, pixi.lock {pins.env_digest[:12]} -> {now[1][:12]}")


def check_hosts_allowed(spec: JobSpec, patterns: list[str]) -> None:
    """Refuse a job before submission if `csub probe`'s allowed_hosts patterns miss any of its hosts."""
    if missing := missing_hosts(spec.allow_hosts, patterns):
        raise HostsNotAllowed(missing)


def match_prompt(pins) -> str:
    return Template(files("mia_data").joinpath("prompts/match.md").read_text()).substitute(
        record=pins.record, record_commit=pins.record_commit, snippets_commit=pins.snippets_commit)


def _harness(cfg) -> str:
    return f"pixi run --manifest-path {q(str(cfg.harness.dir / 'harness' / 'pixi.toml'))} harness"


def _binding(ws, rel: str) -> tuple[str, list[str]]:
    """(record id, URLs) of a binding the worker has already checked (check_binding)."""
    b = yaml.safe_load(Path(ws, rel).read_text())
    return b.get("record"), [u for a in (b.get("raw") or []) + (b.get("labels") or []) for u in a.get("urls") or []]


def _spec(cfg, kind: str, ws, body: str, allow: list[str], pin: tuple[str, str], ro: list, record: str | None = None,
          writable: bool = False, **kw) -> JobSpec:
    env = {"MIA_HARNESS_JOB_BYTES": str(int(cfg.limits.job_bytes)), "MIA_HARNESS_COMMIT": pin[0],
           "MIA_HARNESS_ENV_DIGEST": pin[1], "MIA_HARNESS_ALLOWED_HOSTS": ",".join(allow),
           "MIA_HARNESS_CACHE_DIR": "", "MIA_HARNESS_CACHE_WRITABLE": "0"}
    ro, rw = [str(cfg.harness.dir), *(str(p) for p in ro if p)], []
    if record is not None:  # the dataset cache, §6.6
        if not (isinstance(record, str) and ID_RE.fullmatch(record)):
            raise ValueError(f"bad record id {record!r}")
        cache = cfg.paths.work_root / "_cache" / record
        cache.mkdir(parents=True, exist_ok=True)  # a read-only mount must exist
        (rw if writable else ro).append(str(cache))
        env.update(MIA_HARNESS_CACHE_DIR=str(cache), MIA_HARNESS_CACHE_WRITABLE="1" if writable else "0")
    path = f'export PATH={":".join(q(p) for p in cfg.agent.path_prepend)}:"$PATH"\n' if cfg.agent.path_prepend else ""
    # ponytail: plain jobs take JobSpec's 1 cpu / 4000 MB; add limits.job_cpus / job_mem_mb if crops need more.
    return JobSpec(name=f"{cfg.deployment}-{kind}", command=f'set -u\n{path}{body}\nrc=$?\necho "$rc" > exit_code\nexit "$rc"\n',
                   workspace=str(ws), walltime_min=cfg.limits.job_walltime_min, allow_hosts=allow, ro_mounts=ro,
                   rw_mounts=rw, env=env, **kw)


def match_job(cfg, ws, pins, record_path: str = "record.yaml", dl_dir=None) -> JobSpec:
    """The match agent (§7.2). The caller has written `prompt.md` (match_prompt), `record.yaml`, `downloads.json`,
    any `conversion.json` / `manifest.json`, `catalog/` and `SPEC.md` into `ws`, and made the snapshot of
    `pins.snippets_commit`; `dl_dir` (the downloader's dataset_dir) is mounted read-only. Anthropic hosts come from
    the csub policy's [agents] table, never from here."""
    rec = yaml.safe_load(Path(ws, record_path).read_text()) or {}
    snap = _snap_dir(cfg, pins.snippets_commit)
    body = (f"ln -sfn {q(str(snap))} snapshot && mkdir -p bin && \\\n"
            f"printf '#!/bin/sh\\nexec %s \"$@\"\\n' {q(_harness(cfg))} > bin/harness && chmod +x bin/harness && \\\n"
            f'PATH="$PWD/bin:$PATH" claude -p "$(cat prompt.md)" --model {q(cfg.agent.model)} '
            f"--max-budget-usd {cfg.limits.agent_max_budget_usd} --output-format json \\\n"
            f'  --allowedTools "{MATCH_TOOLS}" > claude.json 2> claude.err')
    return _spec(cfg, "match", ws, body, hosts(record_urls(rec), cfg), (pins.harness_commit, pins.env_digest),
                 [snap, dl_dir], record=pins.record, claude=True, cpus=cfg.agent.cpus, mem_mb=cfg.agent.mem_mb)


def smoke_job(cfg, ws, binding_path: str, snapshot, record_path: str) -> JobSpec:
    """The worker's own smoke of a binding, so the agent's claim is checked (§7.2). Reviewed code: cache rw."""
    record, urls = _binding(ws, binding_path)
    body = f"{_harness(cfg)} smoke {q(binding_path)} --snippets {q(str(snapshot))} --record {q(record_path)} --out smoke.json"
    return _spec(cfg, "smoke", ws, body, hosts(urls, cfg), harness_pin(cfg), [snapshot], record=record, writable=True)


def crop_job(cfg, ws, binding_path: str, snapshot, record_path: str, n: int, seed: str, round_id: str, batch: int,
             existing, trusted: bool) -> JobSpec:
    """`harness crop` into `ws/crops` (§7.4, §7.5). `trusted` (a matched or merged SHA, or production) mounts the
    dataset cache read-write; a PR head gets it read-only and keeps a private cache (§6.6)."""
    record, urls = _binding(ws, binding_path)
    body = (f"{_harness(cfg)} crop {q(binding_path)} --snippets {q(str(snapshot))} --record {q(record_path)} "
            f"--n {int(n)} --seed {q(seed)} --round {q(round_id)} --batch {int(batch)} --out crops"
            + (f" --existing {q(str(existing))}" if existing else ""))
    return _spec(cfg, "crop", ws, body, hosts(urls, cfg), harness_pin(cfg), [snapshot, existing], record=record,
                 writable=trusted)


def verify_job(cfg, ws, crops_dir, binding_path: str, existing) -> JobSpec:
    """`harness verify` then `harness preview` on a crop job's output, mounted read-only (§6.4, §6.7). Loads no
    snippet code: no network, no dataset cache."""
    h, d = _harness(cfg), q(str(crops_dir))
    body = (f"{h} verify {d} --binding {q(binding_path)}" + (f" --existing {q(str(existing))}" if existing else "")
            + f" --out verify.json && \\\n{h} preview {d} --out preview")
    return _spec(cfg, "verify", ws, body, [], harness_pin(cfg), [crops_dir, existing])
