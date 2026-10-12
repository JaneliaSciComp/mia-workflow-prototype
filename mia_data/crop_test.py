"""§7.4 test crops: `judge` (pure, shared with MiaDataDataset) and `run_crop_test`, the activity methods in
MiaDataDataset._crop's order without Temporal (used by `test-pr`). The orchestration is twice because the workflow
can't await activity methods directly and `test-pr` has no worker; what a result means lives once, in `judge`."""

from __future__ import annotations

from .models import GalleryResult, JobArgs, JobDone, Pins, VerifyResult, job_key


def _ok(d: JobDone) -> bool:
    return d.state == "DONE" and d.exit_code in (0, None)


def _failed(kind: str, d: JobDone) -> str:
    return f"{kind} job {d.job_id} ended {d.state} (exit {d.exit_code})"


def judge(v: VerifyResult, jobs: list[JobDone]) -> tuple[bool, list[str]]:
    """(passed, failed jobs): both jobs succeeded and every test crop verified."""
    bad = [_failed(k, j) for k, j in zip(("crop", "verify"), jobs) if not _ok(j)]
    return not bad and v.ok_count > 0 and not v.errors, bad


async def run_crop_test(acts, *, issue: int, round_id: str, pins: Pins, sha: str, binding_path: str, seed: str,
                        trusted: bool) -> tuple[VerifyResult, GalleryResult | None, list[JobDone]]:
    """crop (`crops.test_per_round`, batch 0) → verify (+ preview) → read_verify → publish_gallery of the verified
    crops, in workspaces crop-<sha> / verify-<sha> of the round, waiting synchronously. The jobs are returned, so
    the caller judges them; an uncertain crop job skips verify."""
    async def job(kind: str, **kw) -> tuple[str, JobDone]:
        ws = await acts.prepare_job(issue, round_id, f"{kind}-{sha}", 1, binding_path)
        jid = await acts.submit(job_key(issue, round_id, f"{kind}-{sha}", 1), kind, JobArgs(ws, round_id, pins, **kw))
        o = await acts.jobs.wait(jid, ws, lambda _: None)
        return ws, JobDone(o.job_id, o.state, o.exit_code)

    cw, c = await job("crop", sha=sha, n=acts.cfg.crops.test_per_round, seed=seed, trusted=trusted)
    if c.state not in ("DONE", "EXIT"):
        return VerifyResult(0, 0, [f"crop job {c.job_id} ended {c.state}"], []), None, [c]
    vw, d = await job("verify", crop_ws=cw)
    v = await acts.read_verify(vw, cw)
    return v, await acts.publish_gallery(issue, round_id, sha, cw, vw, v.crops) if v.crops else None, [c, d]
