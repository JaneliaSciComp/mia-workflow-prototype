# autodev runbook

autodev turns a labelled GitHub issue into a PR written by Claude Code in a sandboxed LSF job, revises it on
request, and merges it after a human approves. Design: [agentic-dev-infrastructure.md](agentic-dev-infrastructure.md).

## How people use it

| you do | autodev does |
|---|---|
| add `autodev:go` to an issue (needs write access) | runs an agent round, opens PR `autodev/issue-N`, labels the issue `autodev:review` |
| review with *Request changes*, or comment `/revise <what>` (write access) | runs another round on the same branch (up to `max_revisions`) |
| approve the PR (or comment `/approve <sha>` under a personal token, after the latest round's summary) | squash-merges that exact commit, closes the issue |
| close the PR | stops; re-add `autodev:go` to start over (closing the *issue* does not stop a running workflow) |
| sees `autodev:needs-human` | read the failure comment, fix, re-add `autodev:go` (an open PR is resumed) |

Trust boundaries:
- Only people with write access can trigger (`:go`), approve, or request changes, and only their comments reach
  the agent's prompt. The issue body is included as-is, so edits to it after labelling are a known ceiling.
- Agent commits touching `.github/` are refused.
- Permanent failures stop the round immediately; transient GitHub/csub/git errors retry for up to 6 h.
- Agent summaries and log tails are posted with secret-shaped strings (`sk-ant-…`, `ghp_…`, JWTs, the worker's
  own GitHub token) replaced by `[redacted]`; that is a pattern filter, not a guarantee.
- The Temporal dev server's gRPC port and web UI are unauthenticated: anyone with a login on the host can start,
  signal or terminate workflows. Run autodev on a host you don't share with untrusted users.

## Run it as yourself (testing)

1. Prerequisites P1-P4 in [the plan](plans/2026-10-10-autodev-basic-loop.md#before-you-start-prerequisites-not-tasks).
   csub is pinned to `krokicki/csub` branch `deploy-all` (has the `claude` request field); `pixi install` gets it.
2. `cp autodev.yaml.example autodev.yaml` and set `deployment: autodev-<you>` (keeps your labels, branches and
   workflows apart from the service and from colleagues), `paths.work_root` under your csub `allowed_roots`.
   Keep `work_root` and `paths.state_dir` apart: `work_root` must not be an ancestor of `state_dir`, because
   `work_root` is bound read-write into the sandbox and `<state_dir>-host` (the Temporal binary and database the
   host runs) must stay outside every writable bind.
3. `gh auth token > ~/.config/autodev/token && chmod 600 ~/.config/autodev/token` (or a fine-grained PAT with
   Contents, Pull requests and Issues read/write, and Checks and Commit statuses read), and set
   `github.token_file: ~/.config/autodev/token`. The sandboxed worker reads the token from this file (the sandbox
   masks `.env`); host-side commands like `autodev check` also accept `.env` / `$GITHUB_TOKEN`.
4. `pixi run autodev check`: the `temporal` row is `BAD` until step 5 has started the dev server; fix any other
   `BAD` row first.
5. `pixi run autodev init-labels`, then `scripts/autodev-up.sh`. It starts, in tmux, a Temporal dev server and the
   pixi env's `csub-broker` (reading your `~/.config/csub/broker.toml` policy) on the host, and the worker inside
   the sandbox. Re-run `pixi run autodev check`: now every row `ok`.
6. Open a small issue ("add a `--version` flag"), add `autodev-<you>:go`, watch `tmux attach -t autodev-<you>`.

Manage the deployment (name and paths come from `autodev.yaml`): `pixi run ad ps` (status), `ad logs [worker|temporal|broker]`
(`-n N`, `-f` to follow), `ad attach`, `ad down`, `ad restart`, `ad up` (runs `scripts/autodev-up.sh`).

Files: host-only things (Temporal CLI binary, `temporal.db`, temporal/broker logs) live in `<state_dir>-host`,
which the sandbox never binds; worker logs are in `<state_dir>/logs`.

Under a personal token you are both the bot and the reviewer, so GitHub won't let you *approve* your own PR:
comment `/approve <first 7+ chars of the head sha>` instead. Only `/approve` comments posted after the bot's latest
round summary count, so a short prefix approved earlier cannot approve a later head.

## Run it as the group service

1. **Service account** on the cluster with rootless podman / bwrap set up (agentic-sandbox README), a csub
   broker installed for it (csub `deploy/INSTALL.md`), and an LSF billing group in its broker policy.
2. **Broker policy** (`~svc/.config/csub/broker.toml`): `allow_claude = true`; `allowed_hosts` = the three
   Anthropic hosts + `agent.allow_hosts`; `allowed_roots` contains `paths.work_root`. Config rejects GitHub
   hosts in `agent.allow_hosts`, and `agent.extra_mounts` must be absolute and end in `:ro`.
3. **Claude** for the service account: `claude auth login` as that account (a team seat).
   `agent.api_key_env` is refused by `autodev-up.sh` for now.
4. **GitHub App** owned by the org, installed on the repo, permissions: Contents RW, Pull requests RW, Issues
   RW, Checks R, Commit statuses R, Metadata R. **Not** Workflows, **not** Administration. Private key at
   `~svc/.config/autodev/app.pem` (mode 600); `github.auth: app`, `app_id`, `installation_id`,
   `private_key_path`.
5. **Branch protection / ruleset on `main`**: 1 approving review required, stale approvals dismissed on push,
   the App not in the bypass list. This is the real gate; autodev's own approval check is a pre-check.
   Add `.github/CODEOWNERS` (inside `.github/`, so agents cannot edit it) covering `autodev/`, `scripts/` and
   `.github/`, so changes to the gates need a named human.
6. As the service account: `pixi run autodev init-labels`, `scripts/autodev-up.sh`, then `pixi run autodev check`
   (the `temporal` row needs the dev server that `autodev-up.sh` starts).

## Known gaps (deliberate, for later)

- No cancel command: `temporal workflow terminate -w <id>` stops a workflow but leaves its job; `csub kill <job>`.
- The agent's tests need tools inside the job (e.g. pixi on `path_prepend` + `extra_mounts`, conda/pypi in
  `allow_hosts`); nothing installs them for you.
- CI failures after approval only escalate; there is no automatic fix-CI round.
- Claude sessions are not resumed across rounds; each round starts fresh from the PR diff + feedback.
- Temporal is a dev server under the running account; a shared production server is a later step.
- `autodev/workflows.py` and `autodev/decide.py` are workflow code: changing their decisions or command order
  breaks in-flight workflows on replay. `test_replay_recorded_history` replays a recorded run
  (`tests/fixtures/autodev_issue_history.json`) and fails on such a change. If the change is intentional, guard it
  with `workflow.patched()` (there is no Worker Versioning yet) so runs in flight keep their old path, then
  re-record: `AUTODEV_RECORD_HISTORY=1 pixi run pytest tests/test_autodev_workflows.py -k recorded_history`.
- `[bot]` logins are treated as having no permission (never looked up), so no bot can trigger, approve or revise.
- The live end-to-end run (issue, PR, `/revise`, `/approve`) has not been done yet.
