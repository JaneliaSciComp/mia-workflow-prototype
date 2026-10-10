# MIA agentic workflows (prototype)

A prototype for running agentic workflows for MIA on the Janelia cluster. It has two components, built on the
same stack (Temporal for orchestration, LSF for compute, GitHub issues as the user interface):

| component | what it does | setup |
|---|---|---|
| [**autodev**](#autodev) | turns a labelled GitHub issue into a PR written by Claude Code, revises it on request, merges it after human approval | [`docs/autodev-local-setup.md`](docs/autodev-local-setup.md) |
| [**zarr-doctor**](#zarr-doctor) | one GitHub issue per zarr volume; moving it to **Run** submits an LSF job that histograms the volume and posts the result | [`docs/zarr-doctor.md`](docs/zarr-doctor.md) |

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

## zarr-doctor

zarr-doctor is the simpler first workflow, and it doesn't use an LLM. It keeps one GitHub issue per zarr volume in
the LMD store. Moving an issue to **Run** submits an LSF job that histograms the volume, and the PNG and stats are
posted back to the issue. There is one durable workflow per issue. It served as the proving ground for the GitHub
client, labels/Projects logic and LSF job handling that autodev reuses. Setup, commands and findings:
[`docs/zarr-doctor.md`](docs/zarr-doctor.md). Spec: [`notes/zarr-doctor-spec.md`](notes/zarr-doctor-spec.md).

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
| `autodev/`, `zarr_doctor/` | the two packages (workflows, activities, CLI) |
| `docs/` | setup guides |
| `notes/` | design notes, runbook, specs |
| `scripts/` | `autodev-up.sh`, `run-all.sh`, replay tooling |
| `tests/` | unit tests (`pixi run test`); no GitHub, LSF or Temporal server needed |
