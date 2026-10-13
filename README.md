# MIA agentic workflows (prototype)

A prototype for running agentic workflows for MIA on the Janelia cluster, built on Temporal for orchestration, LSF
for compute, and GitHub issues as the user interface. Two workflows so far: **autodev** turns a labelled GitHub
issue into a PR written by Claude Code, revises it on request, and merges it after human approval; **mia-data**
turns a dataset in the MIA catalog into verified, published image crops, with a human approving every binding.
Setup: [`docs/autodev-local-setup.md`](docs/autodev-local-setup.md). Running it under a bot identity:
[`docs/github-app.md`](docs/github-app.md).

## autodev

autodev is an issue → PR → review → merge loop. A person with write access adds the `autodev:go` label to an issue.
autodev runs Claude Code on the cluster, which implements the issue and opens a PR. The reviewer can request changes
(a review, or a `/revise <what>` comment) and autodev runs another round on the same branch. When a human approves,
autodev squash-merges that exact commit and closes the issue. Anything it can't handle gets the
`autodev:needs-human` label and a comment explaining why.

Humans stay in control. Only people with write access can trigger, approve or steer it, agent commits touching
`.github/` are refused, and branch protection on `main` is the real merge gate. Details, trust boundaries and
known gaps: [`notes/autodev-runbook.md`](notes/autodev-runbook.md). Design rationale:
[`notes/agentic-dev-infrastructure.md`](notes/agentic-dev-infrastructure.md).

## mia-data

mia-data is a dataset → binding → test crops → human approval → production crops loop. Each dataset that
[mia-agentic-search](https://github.com/AI-HHMI/mia-agentic-search) has downloaded gets an issue in
[mia-workflow-data](https://github.com/JaneliaSciComp/mia-workflow-data). On `data:go`, a Claude agent in a cluster
sandbox matches the dataset to a reader snippet from [mia-snippets](https://github.com/JaneliaSciComp/mia-snippets)
and writes a binding; plain harness jobs check it, cut four test crops and show them in the issue. A human approves
with `data:approve` (or, for a new snippet, by reviewing its PR, which autodev writes), and a plain job extracts
the production set, verifies it and publishes it to shared storage and a `galleries` branch. Every stage is a label
and a new comment with links to the folders involved. The whole flow, with a diagram and the component for each
step: [`docs/mia-data-workflow.md`](docs/mia-data-workflow.md). Design: [`notes/mia-crops-spec.md`](notes/mia-crops-spec.md).

## Why durable workflows

These workflows mostly wait: for a reviewer, for CI, for an approval, for a cluster job, often for days. Each issue
gets one long-lived [Temporal](https://temporal.io) workflow that holds its state and receives GitHub events as
signals. That gives us:

- **Survival of restarts.** If a worker or host dies mid-run, the workflow resumes where it was. Job submission is
  idempotent and polling reattaches to the same LSF job, so nothing is run twice.
- **Cheap waiting.** A workflow waiting on a human costs nothing while idle, and durable timers replace cron-style
  bookkeeping.
- **Retries instead of glue code.** Transient GitHub, git and cluster errors are retried by policy; permanent ones
  stop the round and escalate to a human.
- **One object per issue/PR.** Workflow IDs make duplicates impossible, and the Temporal UI shows exactly what
  happened in each run, which is the main debugging surface.

The tradeoff is that workflow code must be deterministic, so changing it can break in-flight runs. Workflows are
kept thin (behaviour lives in activities), and a replay test against a recorded history guards the rest. See the
runbook's known gaps.

## How agents run on the cluster

Agents are untrusted code driven by text from issues, so they never run on the host directly.

- The **orchestrator** (Temporal dev server and worker) runs inside [agentic-sandbox](https://github.com/JaneliaSciComp/agentic-sandbox),
  which gives it read-only access to the code and config, read-write access only to its own state and work
  directories, and a network allowlist (GitHub and Temporal only).
- Each agent round is submitted to LSF as a job through [csub](https://github.com/krokicki/csub) (`csub submit --claude`).
  The job runs Claude Code in its own sandbox, bound read-write to just its round directory, with a network allowlist
  limited to Anthropic and package hosts. It never gets GitHub access or the worker's token.
- A **csub broker** on the host, running with the user's policy (`broker.toml`: allowed hosts, allowed roots,
  `allow_claude`), is what actually submits the jobs. The sandboxed worker can only ask it.
- The **worker**, not the agent, does all GitHub and git pushes, and pushes only to the deployment's own
  `autodev/issue-N` branches.

## Repository layout

| path | |
|---|---|
| `autodev/` | autodev: workflows, activities, CLI; GitHub client and csub job runner shared with mia-data |
| `mia_data/` | mia-data: workflows, activities, harness job specs, CLI (`pixi run md`) |
| `docs/` | setup guides and the [mia-data workflow overview](docs/mia-data-workflow.md) |
| `notes/` | design notes, runbook, specs |
| `scripts/` | `autodev-up.sh`, `mia-data-up.sh` |
| `tests/` | unit tests (`pixi run test`); no GitHub, LSF or Temporal server needed. `pixi run lint` runs ruff |
