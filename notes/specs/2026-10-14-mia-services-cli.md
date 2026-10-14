# `mia`: one command for every service

Status: draft for review. Replaces `scripts/autodev-up.sh`, `scripts/mia-data-up.sh` and the `up|down|restart|ps|attach|logs`
subcommands of `autodev` and `mia-data` with a single compose-style lifecycle command.

## Problem

Three deployments run on a host today: `autodev`, `snippets` (autodev with a second config) and `mia-data`. They are
started three different ways (`pixi run ad up`, `AUTODEV_CONFIG=autodev-snippets.yaml pixi run ad up`, `pixi run md up`),
by two bash scripts that duplicate each other, and the Temporal dev server belongs to whichever autodev session
happened to start it, so `ad down` can take mia-data's Temporal away. There is no way to say "restart everything".

## Goal

```
pixi run mia up        [SERVICE...]     # start all (Temporal first), or the named ones
pixi run mia down      [SERVICE...]     # stop all (Temporal last), or the named ones
pixi run mia restart   [SERVICE...]     # down, then up, same selection
pixi run mia ps        [SERVICE...]     # one line per service: running / stopped / external, windows alive/dead
pixi run mia logs      SERVICE [worker|broker] [-n N] [-f]
pixi run mia attach    SERVICE
pixi run mia exec      SERVICE ARGS...  # run the service's app CLI with its config, e.g. `mia exec snippets check`
```

`pixi run ad` and `pixi run md` keep only app commands (`check`, `init-labels`, `worker`, `schedule`, `seed`,
`test-pr`, ...). Lifecycle lives in `mia` alone.

## Services

A gitignored manifest, `mia.yaml`, next to the app configs (shipped as `mia.yaml.example`):

```yaml
sandbox_dir: ../agentic-sandbox/scripts      # replaces $SANDBOX; relative to the repo
temporal:
  state_dir: ~/.local/state/temporal          # temporal.db, logs/, and bin/temporal if the CLI must be downloaded
services:
  autodev:  {app: autodev,  config: autodev.yaml}
  snippets: {app: autodev,  config: autodev-snippets.yaml}
  mia-data: {app: mia-data, config: mia_data.yaml}
```

- The **service name** is the tmux session name and what you type on the command line. It is independent of the
  config's `deployment` field (which still prefixes labels, queues and workflow IDs).
- `app` selects the per-app recipe (below). `config` is relative to the repo; it is loaded with that app's
  `load_config`, so all existing validation applies.
- **`temporal` is an implicit service**, always present, never listed. Its address comes from the app configs'
  `temporal.address`; `mia` refuses to start if the listed services disagree on it.
- Unknown service names, unknown apps, or a missing manifest are errors with a one-line message.

## Behaviour

**Ordering.** `up` with no arguments starts `temporal`, then the services in manifest order. `up X` starts
`temporal` if it is not healthy, then X. `down` with no arguments stops services in reverse order, then `temporal`.
`down X` stops only X and never touches `temporal`; `down temporal` is explicit and allowed. `restart` is `down`
then `up` with the same selection, so `restart autodev` leaves Temporal and the other workers alone.

**Temporal service.** One tmux session `temporal` running `temporal server start-dev --db-filename
<state_dir>/temporal.db`, log at `<state_dir>/logs/temporal.log`. The binary is `temporal` on PATH, else downloaded
once into `<state_dir>/bin`. Health is `temporal operator cluster health`. If Temporal is healthy at the configured
address but there is no `temporal` tmux session (started by hand, or by the old scripts), `ps` shows it as
`external`, `up` reuses it, and `down` leaves it alone.

**App service start** (ported from the two scripts, one recipe parameterised per app):

1. Load the config; resolve state dir, work root, secret file, config path, namespace. Keep the existing refusals
   (autodev: `agent.api_key_env` unsupported; both: secret must be a file; no single quotes in paths).
2. Check prerequisites: `.pixi/envs/default/bin/{python,csub-broker}`, `sandbox_dir/sandbox-run.sh`, `tmux`.
3. Create `state/logs`, `state-host/logs`, work root, mia-data's storage root, and `/scratch/$USER/<service>` for
   the broker socket.
4. Ensure Temporal is healthy (start the `temporal` service if needed) and the namespace exists, waiting for it to
   become visible as today.
5. If the tmux session already exists: re-apply the schedule, print "already running", exit 0 (today's repair path).
6. Otherwise `tmux new-session -s <service> -n broker` running `csub-broker --serve <run>/csub.sock`, then
   `new-window -n worker` running `sandbox-run.sh` with the app's mounts and env, then `<app> schedule apply`.

Per-app differences are a small table, not two scripts:

| | autodev | mia-data |
|---|---|---|
| module | `autodev.cli` | `mia_data.cli` |
| config env var | `AUTODEV_CONFIG` | `MIA_DATA_CONFIG` |
| proxy flag | `AUTODEV_TEMPORAL_VIA_PROXY=1` | `MIA_DATA_TEMPORAL_VIA_PROXY=1` |
| extra mounts | none | `--ro harness.dir`, `--rw paths.storage_root` |

Everything else (`--ro repo`, `--ro config`, `--ro secret`, `--rw work`, `--rw run`, the GitHub and localhost
allowlist, `CSUB_TRANSPORT=unix`, `CSUB_SOCKET`) is shared.

**ps / logs / attach** keep the semantics of today's `autodev/ops.py` (exact-match tmux session, `tail -F`, log
paths under `state/logs` and `state-host/logs`), generalised over the selected services. `ps` exits 1 if any
selected service is stopped.

**exec** runs `python -m <module> ARGS...` with the service's config env var set. It exists so `snippets` no
longer needs the `AUTODEV_CONFIG=... pixi run ad` incantation; it is five lines.

## Code

- New package `mia_ops/` (console script `mia`, pixi task `mia = "mia"`):
  - `manifest.py`: pydantic model for `mia.yaml`, the app table, service -> loaded config.
  - `tmux.py`: the helpers now in `autodev/ops.py` (`is_running`, session kill/list, `log_files`), moved.
  - `services.py`: temporal + app start recipes, ordering, the command functions.
  - `cli.py`: argparse, `main`.
- Delete `scripts/autodev-up.sh`, `scripts/mia-data-up.sh`, `autodev/ops.py`, and the six lifecycle subparsers plus
  the `OPS` dispatch in `autodev/cli.py` and `mia_data/cli.py`. Add `mia_ops` to the hatch wheel packages and to
  `pixi run typecheck`.
- `mia.yaml` joins `.gitignore`; `mia.yaml.example` is tracked with placeholder paths only.

## Tests

Unit tests only, with the monkeypatched `subprocess.run` pattern from `tests/test_autodev_ops.py` (which moves to
`tests/test_mia_ops.py`):

- manifest: loads, rejects unknown app / missing config / disagreeing Temporal addresses.
- ordering: `up` all -> temporal first; `down` all -> temporal last; `down autodev` issues no temporal command.
- recipes: the tmux command lines for an autodev and a mia-data service contain the expected mounts and env vars;
  the already-running path only re-applies the schedule.
- `ps` exit code and `external` state for Temporal.

Then one manual run on a submit host: `mia up`, `mia ps`, `mia restart snippets`, `mia down`.

## Docs to update

`README.md` (layout table, `scripts/` row goes away), `docs/autodev-local-setup.md`, `docs/github-app.md`,
`docs/mia-data-workflow.md`, `notes/autodev-runbook.md`, the header comment in `autodev-snippets.yaml`, the
`token_file` comment in `autodev.yaml.example`.

## Migration

The Temporal database moves from `<autodev state>-host/temporal.db` to `temporal.state_dir`. To keep running
workflows, either copy the file once before the first `mia up`, or point `temporal.state_dir` at the old `-host`
directory. A fresh database is also acceptable on a dev host: the poll schedule is re-applied and open issues are
picked up again on the next poll.

## Out of scope

One broker shared by all services, one tmux session for everything, an aggregated `mia check`, health watching or
auto-restart, systemd units. Each is a separate change if it ever earns its keep.
