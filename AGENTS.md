# Working in this repository

Four Python packages: `mia_core/` (what every package shares: the `mia.yaml` loader, filesystem checks,
untrusted-output helpers), `autodev/` (GitHub issue → PR by Claude Code) and `mia_data/` (dataset → published crops),
both Temporal workflows with activities that talk to GitHub and the LSF cluster via csub, and `mia_buffers/` (training
buffers: the `MiaBuffer` and `MiaPrep` workflows, the planner, the cache keeper, the buffer-api service with its ledger,
registry and read-only page, and the `BufferDataset` client). `mia_data` imports from `autodev`, `mia_buffers` from
both, never the reverse; every package may import `mia_core`, which imports none of them. Full guide:
[`docs/development.md`](docs/development.md); what each part is for: the [README](README.md).

How the pieces meet: mia-data prepares a dataset (approved binding, reviewed crops) and writes its row to buffer-api's
registry; a buffer plans over prepared rows only, fetches raw files into the shared raw cache, stages one **sparse
OME-Zarr 0.6 pyramid per volume** (only the chunks of the planned samples; provenance under `attributes.mia`; the
stage job runs `harness stage` then `harness check-stage`), registers the volumes, and buffer-api serves planned
samples by weight and split. The harness (writer, reader, planner, tensorizer) lives in mia-snippets at `harness.dir`;
a round pins its HEAD, so the harness deploys first. Dated specs and plans are under `docs/specs/` and `docs/plans/`;
`notes/` is local scratch and is not tracked.

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
- **One config file.** Deployments read their block of `mia.yaml` (`MIA_SERVICE`, `MIA_CONFIG`); the per-app YAML
  files are deprecated. `paths.local_root` and a managed `temporal.state_dir` must be host-local disk.
- **Worker routes of buffer-api take the bearer token.** Every PUT/POST/DELETE other than the consumer routes needs
  `Authorization: Bearer <service.token_file>`; use `mia_core.registry.Registry`, never a bare httpx call.
- **One writer per raw cache.** No job writes a record's raw cache unless its worker holds
  `<local_root>/locks/raw/<record>` (`mia_core.locks`; TTL 900 s, heartbeat with every `await_job` poll). Hand-run
  harness commands bypass it.
- **Only the keeper deletes.** The buffers worker's `MiaCacheKeeper` is the only code that removes anything under
  `raw_root` or `staged_root` (and takes the record's lock first); buffer-api never deletes.
- **A staged artifact is published by the worker's rename and never written again.** Stage jobs write
  `.building-<artifact>-<job key>`; a rebuild lands under the same id through the same rename.
- **An artifact id is a recipe hash.** Binding, levels key, voxel size, harness commit, env digest and plan digest.
  Changing how the harness derives samples (`plan.py`) changes plan digests, so every volume restages; a buffer that
  re-registers a volume under a new artifact loses its frozen eval lists by design.
- **A buffer never asks mia-data for a dataset; a `prep:go` issue does.** `MiaBuffer` fails an unprepared record;
  `MiaPrep` opens the `data:go` issues and follows them through the registry.
- **The page is read-only and holds no token.** Its only actions are GitHub links; a route the page needs is a read
  route (`tests/test_buffers_ui.py` enforces it).
- **Output of sandboxed jobs is untrusted.** Read it with `read_untrusted`, reduce it to a small typed result,
  `neutralize` it before posting to GitHub.

## Style

Descriptive names (no single-letter attributes), a plain-language line on each module, class and non-obvious
method, type hints on object parameters, line length 120. Mark deliberate shortcuts with a `# ponytail:` comment
naming the ceiling and the upgrade path.
