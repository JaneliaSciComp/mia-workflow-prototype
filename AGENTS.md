# Working in this repository

Three Python packages: `autodev/` (GitHub issue → PR by Claude Code) and `mia_data/` (dataset → published crops),
both Temporal workflows with activities that talk to GitHub and the LSF cluster via csub, and `mia_buffers/` (training
buffers: the `MiaBuffer` workflow, the offline planner, the buffer-api service and its client dataset). `mia_data` imports from `autodev`, `mia_buffers` from both, never the reverse. Full guide: [`docs/development.md`](docs/development.md).

## Before you push

```
pixi run fmt        # format + safe autofixes
pixi run lint       # ruff check + format check
pixi run typecheck  # mypy
pixi run test       # pytest, incl. Temporal replay tests; needs no GitHub, LSF or Temporal server
```

CI runs lint, typecheck and test. Keep them clean; do not add `type: ignore` or `noqa` without a reason.

## Rules that will bite you

- **Activity result dataclasses in `models.py` are wire format.** They live in recorded Temporal histories. Add
  fields with defaults; never rename or remove one. (`VerifyResult.n` keeps its name for this reason.)
- **Workflow code is deterministic.** If `test_replay_*` fails, your change alters the commands a workflow issues
  and will break in-flight runs. Guard with `workflow.patched()` and re-record the fixture (see the guide).
- **No real paths or hostnames in tracked files.** `git diff | grep -n 'rokickik\|/nrs/'` must be empty.
- **Output of sandboxed jobs is untrusted.** Read it with `read_untrusted`, reduce it to a small typed result,
  `neutralize` it before posting to GitHub.

## Style

Descriptive names (no single-letter attributes), a plain-language line on each module, class and non-obvious
method, type hints on object parameters, line length 120. Mark deliberate shortcuts with a `# ponytail:` comment
naming the ceiling and the upgrade path.
