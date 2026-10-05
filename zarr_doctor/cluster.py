"""py-cluster-api wrapper: idempotent submit, restart-safe await, cancel. One instance per worker process."""

from __future__ import annotations

import asyncio
import logging
import shlex
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from cluster_api import JobMonitor, JobStatus, ResourceSpec, create_executor

from .config import PROJECT_DIR
from .models import JobOutcome

log = logging.getLogger(__name__)


class Cluster:
    def __init__(self, cfg, executor=None):
        self.cfg = cfg
        cp = Path(cfg.cluster.config_path)
        self.executor = executor or create_executor(
            profile=cfg.cluster.profile, config_path=str(cp if cp.is_absolute() else PROJECT_DIR / cp),
            job_name_prefix=cfg.cluster.job_name_prefix)
        self._monitor: JobMonitor | None = None

    def run_dir(self, issue: int, run_id: str) -> Path:
        return self.cfg.results_dir / str(issue) / run_id

    def job_name(self, issue: int, run_id: str) -> str:
        return f"{self.cfg.cluster.job_name_prefix}-{issue}-{run_id}"

    async def _ensure_monitor(self) -> JobMonitor:
        if self._monitor is None:
            self._monitor = JobMonitor(self.executor)
            await self._monitor.start()
        return self._monitor

    async def close(self) -> None:
        if self._monitor:
            await self._monitor.stop()
            self._monitor = None

    async def find_job(self, name: str) -> str | None:
        """Job id of a live job with this full name (LSF reconnect), if any."""
        try:
            recovered = await self.executor.reconnect()
        except NotImplementedError:  # local executor
            return None
        live = [j for j in self.executor.jobs.values() if not j.is_terminal]
        return next((j.job_id for j in [*recovered, *live] if j.name == name), None)

    async def submit(self, issue: int, rel_path: str, run_id: str) -> str:
        """Submit the histogram job once per (issue, run_id); retries return the same job id."""
        rd = self.run_dir(issue, run_id)
        rd.mkdir(parents=True, exist_ok=True)
        id_file = rd / "job_id"
        if id_file.exists():
            return id_file.read_text().strip()
        name = self.job_name(issue, run_id)
        if job_id := await self.find_job(name):
            log.info("issue #%d run %s: found existing job %s (%s)", issue, run_id, job_id, name)
        else:
            h = self.cfg.histogram
            command = shlex.join(["python", "-m", "zarr_doctor.jobs.histogram",
                                  "--zarr", str(self.cfg.lmd_root / rel_path), "--out", str(rd),
                                  "--level", h.level, "--max-chunks", str(h.max_chunks)])
            pixi = shutil.which("pixi") or "pixi"
            prologue = [f"cd {shlex.quote(str(PROJECT_DIR))}",
                        f'eval "$({shlex.quote(pixi)} shell-hook --manifest-path {shlex.quote(str(PROJECT_DIR / "pyproject.toml"))})"']
            r = self.cfg.cluster.resources
            rec = await self.executor.submit(
                command=command, name=f"{issue}-{run_id}",  # executor prepends job_name_prefix
                resources=ResourceSpec(cpus=r.get("cpus"), memory=r.get("memory"), walltime=r.get("walltime"),
                                       queue=r.get("queue"), work_dir=str(rd),
                                       stdout_path=str(rd / "lsf.out"), stderr_path=str(rd / "lsf.err")),
                prologue=prologue, env={"ZD_ISSUE": str(issue), "ZD_RUN_ID": run_id},
                inherit_env=False, login_shell=True)
            job_id = rec.job_id
            log.info("issue #%d run %s: submitted job %s (%s)", issue, run_id, job_id, name)
        tmp = id_file.with_suffix(".tmp")
        tmp.write_text(job_id)
        tmp.replace(id_file)
        return job_id

    async def wait(self, issue: int, run_id: str, job_id: str, on_poll: Callable[[str], None],
                   interval: float = 30.0) -> JobOutcome:
        """Block until the job is terminal, calling on_poll(status) at least every `interval` seconds."""
        monitor = await self._ensure_monitor()
        rec = self.executor.jobs.get(job_id)
        if rec is None:  # submitted by a previous worker process
            rec = self.executor.track(job_id, status=JobStatus.RUNNING)
            # ponytail: private field; seeds zombie detection so a job the scheduler forgot fails after
            # zombie_timeout_minutes instead of hanging until the activity timeout.
            rec._last_seen = datetime.now(timezone.utc)
        while not rec.is_terminal:
            on_poll(rec.status.value)
            try:
                await asyncio.wait_for(monitor.wait_for(rec), timeout=interval)
            except asyncio.TimeoutError:
                pass
        rd = self.run_dir(issue, run_id)
        status = rec.status.value
        if rec.metadata.get("zombie") and (rd / "stats.json").exists() and (rd / "histogram.png").exists():
            status = JobStatus.DONE.value  # scheduler forgot the job, but it finished its work
        runtime = (rec.finish_time - rec.start_time).total_seconds() if rec.finish_time and rec.start_time else None
        return JobOutcome(job_id=job_id, status=status, exit_code=rec.exit_code, max_mem=rec.max_mem,
                          runtime_s=runtime, log_path=str(rd / "lsf.err"))

    async def cancel(self, job_id: str) -> None:
        await self.executor.cancel(job_id)

    def log_tail(self, issue: int, run_id: str, n: int = 40) -> str:
        rd = self.run_dir(issue, run_id)
        for name in ("lsf.err", "lsf.out"):
            p = rd / name
            if p.exists() and (text := p.read_text(errors="replace").strip()):
                return "\n".join(text.splitlines()[-n:])
        return "(no log output)"
