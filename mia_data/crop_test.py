"""Judging a test-crop run, and running one outside Temporal.

`judge` is the single definition of "the test crops passed", shared by the workflow and the `test-pr`
command. `run_crop_test` calls the activity methods in the same order as MiaDataDataset._crop but
synchronously, for `test-pr`, which has no worker. The orchestration exists twice because the workflow
cannot await activity methods directly; what a result means lives once, in `judge`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .models import GalleryResult, JobArgs, JobDone, Pins, VerifyResult, job_key

if TYPE_CHECKING:
    from .activities import Activities


def job_succeeded(job: JobDone) -> bool:
    return job.state == "DONE" and job.exit_code in (0, None)


def describe_failure(kind: str, job: JobDone) -> str:
    return f"{kind} job {job.job_id} ended {job.state} (exit {job.exit_code})"


def judge(verify: VerifyResult, jobs: list[JobDone]) -> tuple[bool, list[str]]:
    """(passed, descriptions of failed jobs): passed when both jobs succeeded and every test crop verified."""
    failed = [
        describe_failure(kind, job)
        for kind, job in zip(("crop", "verify"), jobs, strict=False)
        if not job_succeeded(job)
    ]
    return not failed and verify.ok_count > 0 and not verify.errors, failed


async def run_crop_test(  # noqa: PLR0913  keyword-only; the workflow passes the round's state through
    activities: Activities,
    *,
    issue: int,
    round_id: str,
    pins: Pins,
    sha: str,
    binding_path: str,
    seed: str,
    trusted: bool,
) -> tuple[VerifyResult, GalleryResult | None, list[JobDone]]:
    """crop (`crops.test_per_round`, batch 0) → verify (+ preview) → read_verify → publish_gallery, in the
    round's crop-<sha> / verify-<sha> workspaces, waiting for each job in turn.

    Returns (verify result, gallery or None, the jobs) so the caller can `judge` them; an uncertain crop job
    skips verify."""

    async def run_job(kind: str, **job_args: Any) -> tuple[str, JobDone]:
        workspace = await activities.prepare_job(issue, round_id, f"{kind}-{sha}", 1, binding_path)
        job_id = await activities.submit(
            job_key(issue, round_id, f"{kind}-{sha}", 1), kind, JobArgs(workspace, round_id, pins, **job_args)
        )
        outcome = await activities.jobs.wait(job_id, workspace, lambda _: None)
        return workspace, JobDone(outcome.job_id, outcome.state, outcome.exit_code)

    crop_workspace, crop_job = await run_job(
        "crop", sha=sha, n_crops=activities.cfg.crops.test_per_round, seed=seed, trusted=trusted
    )
    if crop_job.state not in ("DONE", "EXIT"):
        return VerifyResult(0, 0, [f"crop job {crop_job.job_id} ended {crop_job.state}"], []), None, [crop_job]
    verify_workspace, verify_job = await run_job("verify", crop_workspace=crop_workspace)
    verify = await activities.read_verify(verify_workspace, crop_workspace)
    gallery = (
        await activities.publish_gallery(issue, round_id, sha, crop_workspace, verify_workspace, verify.crops)
        if verify.crops
        else None
    )
    return verify, gallery, [crop_job, verify_job]
