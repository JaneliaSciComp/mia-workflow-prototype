# autodev runbook

autodev turns a labelled GitHub issue into a PR written by Claude Code in a sandboxed LSF job, revises it on
request, and merges it after a human approves. Design: [agentic-dev-infrastructure.md](agentic-dev-infrastructure.md).

## How people use it

| you do | autodev does |
|---|---|
| add `autodev:go` to an issue (needs write access) | runs an agent round, opens PR `autodev/issue-N`, labels the issue `autodev:review` |
| review with *Request changes*, or comment `/revise <what>` (write access) | runs another round on the same branch (up to `max_revisions`) |
| approve the PR (or comment `/approve <sha>` under a personal token) | squash-merges that exact commit, closes the issue |
| close the PR | stops; re-add `autodev:go` to start over |
| sees `autodev:needs-human` | read the failure comment, fix, re-add `autodev:go` (an open PR is resumed) |

Trust boundaries:
- Only people with write access can trigger (`:go`), approve, or request changes, and only their comments reach
  the agent's prompt. The issue body is included as-is, so edits to it after labelling are a known ceiling.
- Agent commits touching `.github/` are refused.
- Permanent failures stop the round immediately; transient GitHub/csub/git errors retry for up to 6 h.

## Run it as yourself (testing)

1. Prerequisites P1-P4 in [the plan](plans/2026-10-10-autodev-basic-loop.md#before-you-start-prerequisites-not-tasks).
   csub is pinned to `krokicki/csub` branch `deploy-all` (has the `claude` request field); `pixi install` gets it.
2. `cp autodev.yaml.example autodev.yaml` and set `deployment: autodev-<you>` (keeps your labels, branches and
   workflows apart from the service and from colleagues), `paths.work_root` under your csub `allowed_roots`.
   Keep `work_root` and `paths.state_dir` apart: `work_root` must not be an ancestor of `state_dir`.
3. `gh auth token > ~/.config/autodev/token && chmod 600 ~/.config/autodev/token`, and set
   `github.token_file: ~/.config/autodev/token`. The sandboxed worker reads the token from this file (the sandbox
   masks `.env`); host-side commands like `autodev check` also accept `.env` / `$GITHUB_TOKEN`.
4. `pixi run autodev check`: every row `ok`.
5. `pixi run autodev init-labels`, then `scripts/autodev-up.sh`. It starts, in tmux, a Temporal dev server and the
   pixi env's `csub-broker` (reading your `~/.config/csub/broker.toml` policy) on the host, and the worker inside
   the sandbox.
6. Open a small issue ("add a `--version` flag"), add `autodev-<you>:go`, watch `tmux attach -t autodev-<you>`.

Files: host-only things (Temporal CLI binary, `temporal.db`, temporal/broker logs) live in `<state_dir>-host`,
which the sandbox never binds; worker logs are in `<state_dir>/logs`.

Under a personal token you are both the bot and the reviewer, so GitHub won't let you *approve* your own PR:
comment `/approve <first 7+ chars of the head sha>` instead.

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
   Add CODEOWNERS for `autodev/`, `scripts/` and `.github/` so changes to the gates need a named human.
6. As the service account: `pixi run autodev check`, `pixi run autodev init-labels`, `scripts/autodev-up.sh`.

## Known gaps (deliberate, for later)

- No cancel command: `temporal workflow terminate -w <id>` stops a workflow but leaves its job; `csub kill <job>`.
- The agent's tests need tools inside the job (e.g. pixi on `path_prepend` + `extra_mounts`, conda/pypi in
  `allow_hosts`); nothing installs them for you.
- CI failures after approval only escalate; there is no automatic fix-CI round.
- Claude sessions are not resumed across rounds; each round starts fresh from the PR diff + feedback.
- Temporal is a dev server under the running account; a shared production server is a later step.
- `autodev/workflows.py` and `autodev/decide.py` are workflow code: changing their decisions or command order
  breaks in-flight workflows on replay. Keep `test_replay_after_log_change` green; there is no Worker Versioning yet.
- In GitHub App mode the permission lookup for the bot's own `<slug>[bot]` login is unverified against the real API.
- The live end-to-end run (issue, PR, `/revise`, `/approve`) has not been done yet.
