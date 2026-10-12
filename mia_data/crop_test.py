"""§7.4 test crops without Temporal: the activity methods in MiaDataDataset._crop's order (used by `test-pr`)."""

from __future__ import annotations

from .models import GalleryResult, JobArgs, JobDone, Pins, VerifyResult, job_key


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
