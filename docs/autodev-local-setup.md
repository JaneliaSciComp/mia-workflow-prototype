# Setting up autodev for local development

This sets up a personal autodev instance that acts as you, on your own repo or a test repo. It's for developing and
testing autodev. For the shared service account, see "Run it as the group service" in
[`notes/autodev-runbook.md`](../notes/autodev-runbook.md). Back to the [README](../README.md).

## What you're starting

`scripts/autodev-up.sh` starts three things in one tmux session named after your `deployment`:

| tmux window | runs | where |
|---|---|---|
| `temporal` | Temporal dev server (gRPC `localhost:7233`, UI `:8233`) | host |
| `broker` | `csub-broker`, applying your `~/.config/csub/broker.toml` policy | host |
| `worker` | `autodev worker`, wrapped in `agentic-sandbox` | sandbox |

The worker polls GitHub, drives the workflows, and asks the broker to submit Claude Code jobs to LSF.

## Prerequisites

Run these from a Janelia cluster submit host.

1. **pixi** on your PATH, and **tmux**.
2. **agentic-sandbox** checked out next to this repo (`../agentic-sandbox`), or set `$SANDBOX` to its `scripts`
   directory. It needs rootless podman or bwrap set up, per its README.
3. **csub with Claude support.** The pixi env pins csub to `krokicki/csub` branch `deploy-all`, so `pixi install`
   provides it, including `csub-broker`.
4. **Broker policy** in `~/.config/csub/broker.toml`:
   - `allow_claude = true`
   - `allowed_hosts` includes `api.anthropic.com`, `claude.ai`, `platform.claude.com`, plus everything in
     `agent.allow_hosts` from `autodev.yaml`
   - `allowed_roots` contains your `paths.work_root`

   Check with `csub probe`.
5. **Claude login** for your account: `claude auth login`. Check:
   `csub submit --claude --walltime 5 --wait -- claude -p "Reply OK" --model sonnet` prints `OK`.
6. **A GitHub repo** you can write to, to hold test issues. The default in the example config is this repo.

## Steps

### 1. Install

```bash
pixi install
```

### 2. Configure

```bash
cp autodev.yaml.example autodev.yaml    # git-ignored
```

Edit `autodev.yaml`:

- `deployment: autodev-<you>`. This prefixes your labels, task queue, workflow IDs, branches and tmux session, which
  keeps your instance apart from the service and from colleagues.
- `github.repo`: the repo to watch.
- `paths.work_root`: agent workspaces, which must be inside your broker's `allowed_roots`.
- `paths.state_dir`: the worker's logs and state. Keep it apart from `work_root`. `work_root` is bound read-write
  into the sandbox, so it must not be an ancestor of `state_dir`, and `<state_dir>-host` (the Temporal binary and
  database) must stay outside every writable bind.

### 3. GitHub token

```bash
mkdir -p ~/.config/autodev
gh auth token > ~/.config/autodev/token && chmod 600 ~/.config/autodev/token
```

Or save a fine-grained PAT there with Contents, Pull requests and Issues read/write, and Checks and Commit statuses
read. Then set `github.token_file: ~/.config/autodev/token` in `autodev.yaml`. The sandboxed worker reads the token
from this file because the sandbox masks `.env`. Host-side commands such as `autodev check` also accept `.env` or
`$GITHUB_TOKEN`.

### 4. Check, create labels, start

```bash
pixi run autodev check          # the `temporal` row is BAD until step 5 starts the dev server; fix any other BAD first
pixi run autodev init-labels    # creates the autodev-<you>:* labels (idempotent)
scripts/autodev-up.sh           # tmux session autodev-<you>; applies the poll schedule
pixi run autodev check          # now every row should say ok
```

### 5. Try it

Open a small issue in the repo (for example "add a `--version` flag"), add the label `autodev-<you>:go`, and watch:

```bash
tmux attach -t autodev-<you>    # windows: temporal, broker, worker
```

The Temporal UI is at `http://localhost:8233`. From your laptop use
`ssh -L 8233:localhost:8233 <submit-host>`. Workflows are named per issue.

autodev opens PR `autodev-<you>/issue-N` and labels the issue `autodev-<you>:review`. Because you are both the bot
and the reviewer, GitHub won't let you approve your own PR. Comment `/approve <first 7+ chars of the head sha>`
instead. Only `/approve` comments posted after the bot's latest round summary count. Use `/revise <what>` or a
*Request changes* review to run another round.

## Day to day

```bash
pixi run ad ps                      # status
pixi run ad logs [worker|temporal|broker] [-n N] [-f]
pixi run ad attach
pixi run ad restart                 # after changing code or config
pixi run ad down
pixi run test                       # unit tests; no GitHub, LSF or Temporal server needed
```

Logs: worker logs are in `<state_dir>/logs`. The Temporal and broker logs are in `<state_dir>-host/logs`.

## Gotchas

- **Workflow code is replay-sensitive.** Changing `autodev/workflows.py` or `autodev/decide.py` can break in-flight
  workflows. `test_replay_recorded_history` catches it. If the change is intentional, guard it with
  `workflow.patched()` and re-record the history (see the runbook's known gaps).
- **Agent tests need tools in the job.** For example pixi needs `path_prepend` and `extra_mounts`, and package
  hosts need to be in `agent.allow_hosts`. Nothing installs them for you.
- **The Temporal dev server is unauthenticated.** Anyone with a login on the host can signal or terminate
  workflows, so don't run this on a shared host.
- **`agent.api_key_env` is refused** by `autodev-up.sh` for now. Use a Claude login.
- **No cancel command.** `temporal workflow terminate -w <id>` stops a workflow but leaves its job running. Also
  run `csub kill <job>`.
