"""Replay a recorded workflow history against the *current* workflow code.

    temporal workflow show -w zarr-doctor/issue-9 --output json > /tmp/h.json
    pixi run python scripts/replay_history.py /tmp/h.json

Exit 0 = the current code is compatible with that history (safe to deploy for it);
non-zero = nondeterminism: the code would make different decisions than it made before.
"""

import asyncio
import sys
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from zarr_doctor.workflows import IssueWorkflow, PollWorkflow


async def main(path: str) -> int:
    history = WorkflowHistory.from_json("replayed", Path(path).read_text())
    result = await Replayer(workflows=[IssueWorkflow, PollWorkflow]).replay_workflow(
        history, raise_on_replay_failure=False)
    if result.replay_failure:
        print(f"NONDETERMINISTIC: {result.replay_failure}")
        return 1
    print("ok: current code replays this history deterministically")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1])))
