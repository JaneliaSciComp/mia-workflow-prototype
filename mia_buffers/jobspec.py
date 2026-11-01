"""The buffer worker's job specs, `fetch` and `stage`, plus the catalog snapshot a plan is built from.

A fetch job runs the mia-snippets harness's `fetch` on a dataset's binding: every file into the raw cache, mounted
read-write, under the fetch byte and walltime limits. A stage job runs the harness's `stage` on the binding with
the dataset's staged directory as its output; after a fetch phase the cache is mounted read-only and the job runs
offline, otherwise (the reader snippets are reviewed code, as in mia-data's trusted crop job) the cache is
read-write and the harness fetches what is missing first.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING

from autodev.jobs import JobSpec
from mia_data.jobspec import BINDING_FILE, RECORD_FILE, _spec, archive_snapshot, quote

if TYPE_CHECKING:
    from collections.abc import Callable

    from .config import Config
    from .models import FetchJobArgs, StageJobArgs


def levels_key(patch: int, levels: list[int]) -> str:
    """The staged layout's directory key: a pyramid depends on the spec's patch and levels (spec §2)."""
    return f"p{int(patch)}-l{'-'.join(str(int(level)) for level in levels)}"


def artifact_id(
    binding_hash: str, levels_key: str, voxel_size_nm: dict[str, float] | None, harness_commit: str, env_digest: str
) -> str:
    """A staged pyramid's id: 16 hex of the sha256 over every input that changes its bytes, so a pyramid on disk is
    an immutable artifact a later round can reuse only when all of them match."""
    voxel = {axis: float(size) for axis, size in sorted((voxel_size_nm or {}).items())}
    payload = json.dumps([binding_hash, levels_key, voxel, harness_commit, env_digest], separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def catalog_snapshot(cfg: Config, ref: str, tokens: Callable[[], str | None] = lambda: None) -> Path:
    """`datasets/` of the catalog repo at commit `ref`, read-only, at work_root/_catalog/<ref>."""
    github = cfg.github
    return archive_snapshot(
        cfg,
        mirror="catalog.git",
        git_url=getattr(github, "catalog_git_url", None) or f"https://github.com/{github.catalog_repo}.git",
        sha=ref,
        paths=["datasets"],
        dest=cfg.paths.work_root / "_catalog" / ref,
        tokens=tokens,
    )


def stage_job(cfg: Config, args: StageJobArgs, snapshot: Path) -> JobSpec:
    """`harness stage` on the workspace's binding: hosts exactly as given (none for a dropped dataset or an
    offline stage), the raw cache read-write unless `args.cache_writable` is False, the staged directory
    read-write."""
    if not args.out:
        raise ValueError("StageJobArgs.out is empty: the submit activity sets it to the staged directory")
    body = (
        f"harness stage {BINDING_FILE} --snippets {quote(str(snapshot))} --record {RECORD_FILE} "
        f"--levels {','.join(str(int(level)) for level in args.levels)} --patch {int(args.patch)} "
        f"--out {quote(args.out)}"
    )
    if args.voxel_size_nm:
        axes = [axis for axis in ("z", "y", "x") if axis in args.voxel_size_nm]
        body += " --voxel-size-nm " + ",".join(str(float(args.voxel_size_nm[axis])) for axis in axes)
    spec = _spec(cfg, "stage", Path(args.workspace), args.pins, body, list(args.hosts), [snapshot])
    cache = str(cfg.paths.raw_root / args.record)
    (spec.rw_mounts if args.cache_writable else spec.ro_mounts).append(cache)
    spec.rw_mounts.append(args.out)
    spec.env.update(MIA_HARNESS_CACHE_DIR=cache, MIA_HARNESS_CACHE_WRITABLE="1" if args.cache_writable else "0")
    return spec


def fetch_job(cfg: Config, args: FetchJobArgs, snapshot: Path) -> JobSpec:
    """`harness fetch` on the workspace's binding: every file into the raw cache (read-write), hosts as given,
    under `limits.fetch_bytes` and `limits.fetch_walltime_min`; the report is fetch.json in the workspace."""
    body = f"harness fetch {BINDING_FILE} --record {RECORD_FILE} --out fetch.json"
    spec = _spec(cfg, "fetch", Path(args.workspace), args.pins, body, list(args.hosts), [snapshot])
    cache = str(cfg.paths.raw_root / args.record)
    spec.rw_mounts.append(cache)
    spec.walltime_min = cfg.limits.fetch_walltime_min
    spec.env.update(
        MIA_HARNESS_CACHE_DIR=cache,
        MIA_HARNESS_CACHE_WRITABLE="1",
        MIA_HARNESS_JOB_BYTES=str(int(cfg.limits.fetch_bytes)),
    )
    return spec
