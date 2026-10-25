# Developing in this repository

How to change the code here without breaking what is running. The README explains what autodev and mia-data do;
this page is about working on them. Agents start from [`AGENTS.md`](../AGENTS.md), which is the short version.

## Setup

Everything runs through [pixi](https://pixi.sh). `pixi install` creates the environment (Python 3.12, Temporal
SDK, httpx, pydantic, csub, and the test and lint tools). No GitHub token, LSF or Temporal server is needed for the
checks below; they are only needed to run a deployment ([`docs/autodev-local-setup.md`](autodev-local-setup.md)).

## The four checks

Run all of these before pushing. CI runs the last three in this order on every pull request and push to main.

| command | what it does |
|---|---|
| `pixi run fmt` | formats with ruff and applies its safe autofixes |
| `pixi run lint` | `ruff check` plus a formatting check; fails on drift |
| `pixi run typecheck` | mypy over `autodev/`, `mia_data/`, `mia_ops/` and `mia_buffers/` with `check_untyped_defs` |
| `pixi run test` | pytest over `tests/`, including the Temporal replay tests |

Configuration for all four is in `pyproject.toml`. Keep them clean rather than suppressing: there are no
`type: ignore` or `noqa` comments in the source today, and a new one needs a reason next to it.

## Layout

| path | |
|---|---|
| `autodev/` | the issue → PR loop. Also home to the pieces mia-data reuses: the GitHub client (`github.py`), auth (`auth.py`), the csub job runner (`jobs.py`), git operations (`gitops.py`), and the Temporal connection and logging (`cli.py`) |
| `mia_data/` | the dataset → crops loop. Imports from `autodev`, never the other way round |
| `mia_buffers/` | training buffers: the `MiaBuffer` workflow (a buffer issue → plan → approval → sourcing → ready) and its poller, `mia-buffers plan` (spec + catalog checkout → data pool, fit, weights, channel recipes), `mia-buffers serve` (buffer-api, the one sampler and SQLite ledger), `mia-buffers register` (plan + staged volumes → a buffer) and `mia_buffers.client.BufferDataset` (MuViT dataset over a buffer). Imports from `mia_data` and `autodev`, never the other way round. Design: `notes/specs/2026-10-21-training-buffers.md`; walkthroughs below |
| `tests/` | one `test_<package>_<module>.py` per module; `autodev_helpers.py` and `mia_helpers.py` build configs and fake repos; `fixtures/` holds recorded workflow histories and sample catalog data |
| `docs/` | setup guides, the mia-data workflow overview, this page |
| `notes/` | design: `mia-crops-spec.md` (the mia-data spec), `autodev-runbook.md` (operations and known gaps), `agentic-dev-infrastructure.md` (why), `specs/` and `plans/` (dated, historical; they describe the code as it was when written) |
| `mia_ops/` | `pixi run mia`: the services manifest (`mia.yaml`), the Temporal and app-service recipes under tmux and agentic-sandbox, and `mia exec` for app commands |

Within each package the split is the same: `workflows.py` (deterministic Temporal code, no I/O), `activities.py`
(everything that touches GitHub, the filesystem or the cluster), `models.py` (the dataclasses that cross between
them), `config.py` (pydantic models for the YAML), `cli.py`.

## Rules that are not obvious from the code

**Activity results are wire format.** The dataclasses in `models.py` that activities *return* (`PrSnapshot`,
`VerifyResult`, `MatchResult`, ...) are serialized into Temporal workflow histories. Renaming or removing a field
breaks replay of every in-flight workflow and fails the replay tests. Add fields with defaults; never rename.
Activity *inputs* (`JobArgs`, `StageInput`, ...) are not replayed, so they can change when worker and workflow are
deployed together. `VerifyResult.n` is the standing example of a name kept for this reason.

**Workflow code must stay deterministic, and the replay tests enforce it.** `tests/fixtures/*history.json` are
real histories recorded from earlier versions of the workflows. If a change alters the sequence of commands a
workflow issues, `test_replay_*` fails. When the change is intentional, guard the new behaviour with
`workflow.patched()` so old runs keep their old path, then re-record:

```
AUTODEV_RECORD_HISTORY=1 pixi run pytest tests/test_autodev_workflows.py -k recorded_history
MIA_DATA_RECORD_HISTORY=1 pixi run pytest tests/test_mia_workflows.py -k replay
MIA_BUFFERS_RECORD_HISTORY=1 pixi run pytest tests/test_buffers_workflows.py -k replay
```

**Keep the workflows thin.** Decisions that only need data in hand (`decide.py`, `crop_test.judge`) are plain
functions, tested directly. Everything with a side effect is an activity, called by name so the workflow module
never imports the activity implementations.

**No real paths or hostnames in tracked files.** Configs use placeholders; the real `autodev*.yaml` and
`mia_data.yaml` are git-ignored. Before committing, `git diff | grep -n 'rokickik\|/nrs/'` should find nothing.

**Anything a sandboxed job wrote is untrusted.** Read it through `read_untrusted` (size-limited, no symlinks),
reduce it to a small typed result before it enters workflow history, and run it through `neutralize` before
posting it to GitHub.

**Shared helpers take a `Deployment`, not a `Config`.** autodev's auth, Temporal connection, logging and ops
helpers are annotated with the `Deployment` protocol in `autodev/config.py`, which both packages' configs satisfy.
If a helper needs a new config field, add it to the protocol and to both configs.

## Style

- Descriptive names everywhere except loop variables; no single-letter instance attributes.
- A one-line module docstring saying what the module is for, a sentence or two per class, and a short plain-language
  note on any method whose purpose is not clear from its name. Not full docstrings: this is not a public API.
- Type hints on parameters that take objects (`Config`, `GitHub`, `Pins`, ...). Import them under `TYPE_CHECKING`
  when a runtime import would create a cycle.
- Line length 120, enforced by the formatter. Long-line errors are ignored by ruff; keep lines readable by eye.
- A deliberate shortcut with a known ceiling gets a `# ponytail:` comment naming the ceiling and the upgrade path.
  `grep -rn "ponytail:" autodev mia_data` lists them.

## Pull requests

Branch from `main`, keep one topic per PR, and let CI run. Both deployments poll GitHub, so a PR that changes
activity behaviour should say in its description whether in-flight workflows are affected and what happens to
them on deploy. The autodev bot opens PRs here too; review them like any other.

## Serving a buffer (milestone 2, local mode)

Everything here is hand-driven; the `MiaBuffer` workflow (next section) runs the same steps from a buffer issue.

1. **Plan.** `mia-buffers plan notes/specs/buffer-2d-gray.example.yaml --catalog-dir <catalog checkout> --json plan.json`
   writes the data pool with a weight, a voxel size (documented or assumed) and a channel recipe per record.
2. **Stage** each dataset you want to serve with the mia-snippets harness (`harness stage`, see that repo's
   `SPEC.md` §7) into `<staged root>/<record id>/`, which then holds `stage.json` and one `v<i>.zarr` per volume.
3. **Serve.** `mia-buffers serve --state-dir <state> --port 8765` runs buffer-api on the local host; the ledger is
   `<state>/buffers.sqlite`. It holds no credentials and never reads pixels.
4. **Register.** `mia-buffers register plan.json <spec> --staged-root <staged root> --url http://host:8765
   --buffer <id> [--val-size 256 --test-size 256]` creates the buffer and registers every pooled, staged volume;
   planes are split train/val/test by the spec's fractions (split key = volume plus plane). Unstaged records are
   listed as skipped, volumes the coarsest crop does not fit as refused.
5. **Consume.** In an environment with the harness importable (the mia-snippets `harness/` pixi env, with this
   repository on `PYTHONPATH`), plus torch and muvit for training, the demo notebook's dataset cell becomes

   ```python
   from mia_buffers.client import BufferDataset

   URL, TENSORIZERS = "http://host:8765", "<mia-snippets checkout>"  # or export MIA_BUFFERS_TENSORIZERS
   train_ds = BufferDataset(URL, "<id>", "train", n_samples=4000, tensorizers=TENSORIZERS)
   val_ds = BufferDataset(URL, "<id>", "val", tensorizers=TENSORIZERS)
   ```

   and its `DataLoader` and `MuViTMAE2d(in_channels=train_ds.n_channels, levels=train_ds.levels, ...)` cells run
   unchanged. Train is a stream of fresh crops (`idx` is ignored, `len` is `n_samples`); val and test replay a
   frozen list that is identical every epoch. Each DataLoader worker registers its own consumer.
   `GET /buffers/<id>/status` shows servable volumes per split, draws per volume and consumers.

## Running a buffer from an issue (milestone 3)

The deployment reads `mia_buffers.yaml` (copy the example; `buffers_repo` is the issue repo, `storage_root` is
mia-data's, `raw_root` may be mia-data's `<work_root>/_cache` so downloads are shared). `mia.yaml` lists it as
`app: mia-buffers`; by hand: `mia-buffers init-labels`, `mia-buffers serve`, `mia-buffers worker`,
`mia-buffers schedule apply`.

1. **The issue.** Its body holds the spec in exactly one ```` ```yaml ```` fence; the title is free. A human adds
   `buffer:go`. The poller (every `temporal.poll_minutes`) turns the label into a signal and removes it; only a
   person with write access counts.
2. **Planning.** The worker pins the round (`state_dir/rounds/<round>/`: `spec.yaml`, `pins.json`), archives the
   catalog at `catalog.ref` under `work_root/_catalog/<ref>/` and builds the plan; `plan.md` and `plan.json` land
   in the round directory and in the browsable `work_root/issue-<n>/<round>/`. The plan is posted (pool table,
   cut at 60k characters) and the issue moves to `buffer:plan-review`; an invalid plan, or no `tensorizers/<name>`
   in the harness checkout, goes to `buffer:needs-human`.
3. **Approval.** `buffer:approve` newer than the plan comment starts sourcing; an older one is refused with a
   comment, and a new `buffer:go` (after editing the spec) plans again.
4. **Sourcing.** The buffer is created in buffer-api (id = round id, `i<issue>-e<event>`). Per dataset, in
   parallel (`sourcing.parallel` stage jobs at a time): the binding is read from
   `<storage_root>/<dataset folder>/binding.yaml`; a stage job runs the harness's `stage` with the raw cache
   `raw_root/<record>/` mounted read-write and the binding's hosts allowed, writing the pyramid to
   `staged_root/<levels key>/<record>/` (`p256-l1-2-4` for patch 256, levels 1,2,4); then every volume is
   registered. A dataset whose hosts are not allowed to jobs, or whose binding uses `manual://` URLs, waits
   instead: the issue gets a comment listing `incoming/<name>` ← url pairs, a human places the files under
   `raw_root/<record>/incoming/`, the poller notices and the worker verifies them (regular, non-empty files),
   moves them into the cache under the harness's key and lists them in `raw_root/<record>/files.json`, which the
   harness reads instead of a HEAD request. A dataset with no binding under `storage_root` is handed to mia-data
   (next section). Each dataset ends `servable`, `failed` or `dropped`, with a comment.
5. **Ready.** When every weighting group's servable share reaches the spec's `ready.min_fraction` (default 1.0),
   the issue moves to `buffer:ready` with the buffer id and the `BufferDataset` cell to paste; the remaining
   datasets keep sourcing and join as they finish. If the threshold is not met after
   `sourcing.dataset_timeout_days` (default 14) the issue goes `buffer:needs-human` naming the groups below it.

Not in milestones 3 and 4: no tensorizer develop path; `idle` and `retired` are labels only (milestone 5).

## Datasets without a binding, and normalisation statistics (milestone 4)

**Operator setup.** `mia_buffers.yaml` names mia-data's issue repo as `github.datasets_repo`. mia-data's own
`mia_data.yaml` lists the buffer worker's bot login in `github.trusted_bots` (`<app slug>[bot]`; both deployments
run as the same GitHub App, so it is mia-data's own login) so its poller accepts a `data:go` the buffer adds. mia-data
no longer needs the catalog's downloads state: a record that is on the catalog's `main` is admitted with
`record_commit` = main's head when no downloads entry exists, and its jobs fetch through the harness cache as before.

**The hand-off.** When `read_binding` finds no `<storage_root>/<dataset folder>/binding.yaml`, the worker's
`nudge_dataset` looks the record up in the datasets repo by mia-data's marker (`<!-- mia-data:record=<id> -->`),
opens the issue if there is none (mia-data's usual body, pinned to the buffer's catalog ref) and adds `data:go`
unless the issue is already in progress (`data:matching … cropping`), `data:done` or `data:ineligible`. The dataset
sits in `waiting-dataset` with a comment on the buffer issue naming the dataset issue. mia-data runs its normal
round there: match, smoke, test crops, a human's `data:approve`, production crops, and at `publish_crops` it now
also copies the approved binding to `<dataset folder>/binding.yaml` and records its sha256 in `manifest.json`
(`"binding": {"sha256": …}`, `null` when none). The buffer poller watches that file and the dataset issue's
labels and signals `dataset_update(record, "binding" | "ineligible")`; a binding resumes the normal path (drop or
stage job), an ineligible dataset is **dropped**: it leaves the plan and the ready denominator, the buffer issue
gets a comment and the plan is re-posted with a **Dropped** list. The same happens to every dataset still
`waiting-dataset` or `waiting-drop` when `sourcing.dataset_timeout_days` fires; if the remaining groups then meet
`ready.min_fraction` the issue goes `buffer:ready`, else `buffer:needs-human` naming the groups that were below.
A dataset issue that is `data:done` without a published binding (a run from before this change) is not relabelled:
the comment says to add `data:go` on it by hand, and the timeout drops it otherwise.

**Statistics.** `harness stage` records, per raw volume and channel, a sorted sample of up to 4,096 level-0
values from 8 random patch boxes (`stage.json` → `volumes[].intensity_sample`). At registration, for
`sampling.normalize.scope: volume`, each volume body carries `normalize: {lo: [per channel], hi: [per channel]}`
at the spec's percentile range from its own sample; for `scope: dataset` the samples of the record's volumes are
pooled first; for `scope: patch` (the default) the field is `null`. Descriptors carry the volume's `normalize`,
`BufferDataset` passes it to the tensorizer as `stats` (with `scope` from the buffer config), and `muvit-2d` then
scales by those bounds instead of the patch's own percentiles (`meta.normalize_scope` says which). A staged
directory from before this change has no sample: with a non-`patch` scope its volumes are refused at
registration with a reason that says to re-stage.
