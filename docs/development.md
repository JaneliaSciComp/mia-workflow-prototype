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
| `pixi run typecheck` | mypy over `autodev/` and `mia_data/` with `check_untyped_defs` |
| `pixi run test` | pytest over `tests/`, including the Temporal replay tests |

Configuration for all four is in `pyproject.toml`. Keep them clean rather than suppressing: there are no
`type: ignore` or `noqa` comments in the source today, and a new one needs a reason next to it.

## Layout

| path | |
|---|---|
| `autodev/` | the issue → PR loop. Also home to the pieces mia-data reuses: the GitHub client (`github.py`), auth (`auth.py`), the csub job runner (`jobs.py`), git operations (`gitops.py`), and the Temporal connection and logging (`cli.py`) |
| `mia_data/` | the dataset → crops loop. Imports from `autodev`, never the other way round |
| `mia_buffers/` | training buffers: `mia-buffers plan` (spec + catalog checkout → data pool, fit, weights, channel recipes), `mia-buffers serve` (buffer-api, the one sampler and SQLite ledger), `mia-buffers register` (plan + staged volumes → a buffer) and `mia_buffers.client.BufferDataset` (MuViT dataset over a buffer). Imports from `mia_data` and `autodev`, never the other way round. Design: `notes/specs/2026-10-21-training-buffers.md`; serving walkthrough below |
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

Everything here is hand-driven; the worker of milestone 3 will run the same steps from a buffer issue.

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

   URL, TENSORIZERS = "http://host:8765", "<mia-snippets checkout>"   # or export MIA_BUFFERS_TENSORIZERS
   train_ds = BufferDataset(URL, "<id>", "train", n_samples=4000, tensorizers=TENSORIZERS)
   val_ds = BufferDataset(URL, "<id>", "val", tensorizers=TENSORIZERS)
   ```

   and its `DataLoader` and `MuViTMAE2d(in_channels=train_ds.n_channels, levels=train_ds.levels, ...)` cells run
   unchanged. Train is a stream of fresh crops (`idx` is ignored, `len` is `n_samples`); val and test replay a
   frozen list that is identical every epoch. Each DataLoader worker registers its own consumer.
   `GET /buffers/<id>/status` shows servable volumes per split, draws per volume and consumers.
