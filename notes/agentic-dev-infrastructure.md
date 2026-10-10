# Infrastructure for an autonomous issue-to-PR system

The goal is a system that reads issues from this GitHub repo, implements them as PRs, and keeps working each PR
until a human approves it and it merges. All development runs as Claude Code on the Janelia cluster:

- the orchestrator runs inside [agentic-sandbox](../../agentic-sandbox),
- it submits Claude Code jobs through [csub](../../csub) (`csub submit --claude`, gated by the broker's
  `allow_claude` policy),
- humans approve at checkpoints before merge, release and deploy,
- and eventually the system develops itself.

This note picks the orchestration layer, rules on Pydantic AI, and lists existing systems worth borrowing from.
Existing systems were surveyed on 2026-10-10.

**Summary.**
- **Use Temporal.** The workload is mostly *waiting*: for review, CI, human approval and release gates, often for
  days. One long-lived workflow per issue that receives GitHub events as signals is exactly Temporal's entity
  pattern. In Prefect it would be reruns from the top plus hand-written deduplication.
- **Don't build on Pydantic AI.** Its durable execution makes *its own* agent loop durable. Here the agent loop is
  Claude Code inside one LSF job, so one coarse Temporal activity per job is the right granularity.
- **Nothing off the shelf fits.** No self-hosted system does issue → PR → babysit → merge → release, and none knows
  about LSF. Several have parts worth borrowing (section 4).

---

## 1. Temporal vs Prefect for this workload

Background: [`temporal-vs-prefect.md`](https://github.com/krokicki/mia-workflow-prototype/blob/prefect/notes/temporal-vs-prefect.md) (on the `prefect` branch), sections 2 and 3.

| need | Temporal | Prefect |
|---|---|---|
| One object per issue/PR that lives for weeks and waits on humans | Entity workflow, Workflow ID `agent/issue-<n>`, durable timers, free while idle | No per-key object. `wait_for_input` suspends and **reruns the flow from the top** on resume |
| GitHub events ("review submitted", "CI failed", "/approve") delivered to that object | Signals, signal-with-start, race-free via Workflow ID uniqueness | New flow runs plus a check-then-act duplicate guard |
| Claude Code jobs on LSF that run for hours and can die | Activity with heartbeats. On worker death it is redispatched and `await_job` reattaches to the same job | Crash is detected by a heartbeat automation; the flow reruns; every step must be idempotent |
| Human checkpoints (approve plan / merge / release / deploy) | `wait_condition` on a signal, state in plain variables | Pause/suspend (rerun on resume), or state kept externally |
| Session continuity across review rounds | Claude session ID kept in workflow state, `--resume` on the next round | Must be persisted externally |
| Operator UI with per-run logs | Weaker: Temporal UI shows history, not logs | Stronger |

Prefect's UI advantage matters less here, because GitHub already is the UI: a sticky progress comment per PR,
check runs, labels and Projects. The Temporal UI covers debugging.

Reuse from `main` (the Temporal branch of zarr-doctor):
- the GitHub client (rate limits, throttling, `comment_once`, `upsert_file`),
- labels and Projects v2 stage logic,
- the `py-cluster-api` wrapper with idempotent submit and restart-safe `await_job`, which needs to move onto csub,
- the poll workflow and schedule,
- `scripts/replay_history.py` and the replay tests.

### The self-development risk, and how to contain it

A system that edits its own workflow code collides with Temporal's determinism rules. If an in-flight PR workflow
replays against changed code, it fails with a nondeterminism error. Mitigations:

1. **Keep workflows thin.** A workflow is a state machine of signals, timers and activity calls. Behaviour lives in
   activities, prompts and skills, which can change freely between runs.
2. **Use Worker Versioning (pinned).** In-flight workflows finish on the build they started on, and new issues
   start on the new build.
3. **Make the replay test a required check.** Any PR that touches `workflows.py` must replay recorded production
   histories cleanly. That turns "the agent changed the orchestrator" into a gate it passes or fails mechanically.
4. **Use CODEOWNERS on orchestrator, policy and workflow files.** Changes to the gates themselves always need a
   specific human.

## 2. Pydantic AI

[Pydantic AI](https://github.com/pydantic/pydantic-ai) (MIT, v2.55) has durable-execution capabilities:
`TemporalDurability`, `PrefectDurability`, plus DBOS, Restate and others. The older `TemporalAgent` and
`PrefectAgent` are deprecated. These turn each model request, tool call and MCP call of a *Pydantic AI* agent into
an activity or task.

- **The coding step:** no fit. Claude Code runs its own loop inside the sandbox. Durability per tool call would
  mean reimplementing Claude Code, or reaching into its internals (see the Temporal plugin in section 4).
- **Small orchestrator-side LLM calls:** a clean fit. These are calls like "triage this issue", "classify these
  review comments", or "draft release notes", which want typed outputs. But `claude -p --output-format json`
  covers them without a second agent framework.
- **Decision:** don't adopt now. Revisit if orchestrator-side LLM calls grow beyond a handful.

## 3. Proposed shape

```
PollWorkflow (Temporal Schedule; GraphQL + ETag polling)
  └─ signal-with-start ─▶ IssueWorkflow(agent/issue-<n>)
        triage ─▶ [plan gate: human /approve-plan, optional]
        implement ─▶ activity: csub submit --claude (sandboxed Claude Code on LSF)
                  ─▶ trusted worker pushes agent/issue-<n>, opens PR
        loop:
          observe PR (conflicts, CI, review threads, mergeability)
          ─▶ action ∈ {WAIT, FIX_CODE, MARK_READY, ESCALATE, MERGE}
          FIX_CODE ─▶ activity: csub --claude, resume same Claude session
        human /approve <sha> (required status check) ─▶ merge queue ─▶ done
ReleaseWorkflow
  wait for human signal ─▶ tag/release ─▶ deploy job behind a GitHub environment with required reviewers
```

**Trust boundaries:**
- **The sandboxed Claude job holds no GitHub credentials.** It gets a checkout and returns a diff or commits.
- **The trusted worker owns all GitHub writes.** It pushes only to `agent/*` branches, through a GitHub App whose
  token cannot approve or merge.
- **Branch protection does the gating.** Rulesets require a human review, dismiss stale approvals on push, and
  require status checks, including the replay test and the approval check.
- **The decision is deterministic code.** The LLM only executes `FIX_CODE`. Picking the next action is workflow
  logic, so it is testable and replayable.

**Event ingress:**
- **Polling is required anyway.** GitHub sends no event when main moves and a PR becomes conflicted, and polling
  already exists in zarr-doctor.
- **If latency matters,** add a self-hosted Actions runner inside Janelia that signals Temporal on issue and PR
  events (it only makes outbound connections), or poll the GitHub App deliveries API.
- **Skip smee.io:** it has no delivery guarantees.

## 4. Existing systems: what to take from each

### Worth borrowing

| project | license / lang | what it is | take |
|---|---|---|---|
| [jonathanong/pr-shepherd](https://github.com/jonathanong/pr-shepherd) | MIT / TS | CLI/MCP server. One batched GraphQL poll returns exactly one next action: `WAIT/MARK_READY/FIX_CODE/MERGE/CANCEL/ESCALATE` | **The PR state machine.** Port the action enum and its edge cases into workflow code: outdated/resolved/minimized threads, CI log excerpts, mergeability. Young project (few stars), so port the logic rather than depend on it. |
| [mblode/agent-skills `pr-babysitter`](https://github.com/mblode/agent-skills) | MIT / Claude skill | Poll loop: conflicts, then CI, then comments, then readiness; speaks only on change | **Phase order and prompts.** Its `mergeStateStatus` table, rebase/lockfile conflict playbook, comment-triage plan template, list of legitimate reasons to ignore a comment, and pre-commit verification gate |
| [anthropics/claude-code-action](https://github.com/anthropics/claude-code-action) | MIT / TS | GitHub Action running Claude Code on issue/PR triggers | **`agent-approval-check/`**: a status check requiring N human `/approve <head-sha>` comments, stale after a new push, with the approver's write access verified (Anthropic reports running this gate internally). Also `examples/ci-failure-auto-fix.yml` (fetch failed job logs → prompt), `src/create-prompt` (issue/PR context template), and the sticky progress-checkbox comment |
| [anthropics/claude-agent-sdk-python](https://github.com/anthropics/claude-agent-sdk-python) | MIT / Py | Python wrapper over the Claude Code CLI | **Run it inside the job instead of raw `claude -p`.** Session IDs enable resume across review rounds; hooks give an audit log and command blocking; the `can_use_tool` callback; in-process MCP tools such as `request_human_approval` |
| [gabe4coding/agentic-temporal-wf](https://github.com/gabe4coding/agentic-temporal-wf) | none / Py | Proof of concept: Temporal + Claude Agent SDK PR autofix | **The trust boundary.** The sandbox gets an opaque run token, not a PAT or API key. A trusted broker does the side effects; `push_changes` is the only one, and it needs approval. No license, so take the idea, not the code. |
| [GitHub Copilot coding agent](https://docs.github.com/en/copilot/concepts/agents/coding-agent/about-coding-agent) | closed | Hosted issue-to-PR agent | **Gate design:** pushes only to `copilot/*`; separate secrets environment; egress allowlist; CI on its PRs needs a human "Approve and run"; the person who requested the PR cannot approve it |
| [openai/codex-action](https://github.com/openai/codex-action) | Apache-2.0 / JS | `codex exec` in Actions | `docs/security.md`: keep the model key out of the agent process with a proxy. Same principle as csub's broker. |
| [temporal-community/temporal-ai-agent](https://github.com/temporal-community/temporal-ai-agent) | MIT / Py | Chat agent as a long-lived Temporal workflow | **The entity-workflow pattern** for agents: signals for human input, tools as activities, continue-as-new for long histories |
| [The-PR-Agent/pr-agent](https://github.com/The-PR-Agent/pr-agent) | MIT (per API; was AGPL, unverified) / Py | Self-hosted review bot (`/review`, `/improve`) | **Review-side prompts.** Optionally a second-opinion reviewer whose comments feed the `FIX_CODE` loop |
| [Darwin Gödel Machine](https://github.com/jennyzzt/dgm), [SICA](https://github.com/MaximeRobeyns/self_improving_coding_agent), [HGM](https://github.com/metauto-ai/HGM) | Apache / MIT / Apache, Py | Research: an agent edits its own scaffold, scored on SWE-bench/Polyglot, with an archive of variants | **The idea for "develops itself":** changes to the agent's own prompts and workflow must pass a frozen eval set before merge, and the archive of previous variants allows rollback. Research code, inactive. |

### Reference only, or skip

| project | why |
|---|---|
| [Temporal Claude Agent SDK plugin](https://github.com/temporalio/ai-integrations/pull/33) | Unmerged community PR. Makes every Claude Code tool call a Temporal activity, in a ~4k-line runner that depends on CLI internals. Too fine-grained when one LSF job equals one `claude` run. Read its notes on resume pitfalls and on not re-running non-idempotent Bash calls. |
| Temporal + OpenAI Agents SDK (`temporalio.contrib.openai_agents`) | Only relevant if Claude Code is replaced |
| [OpenHands](https://github.com/OpenHands/OpenHands) / [software-agent-sdk](https://github.com/OpenHands/software-agent-sdk) | The old `openhands-resolver` was archived in Nov 2024. No current equivalent found beyond SDK examples. Its review-thread GraphQL snippets (`resolveReviewThread`) are worth a look. |
| [SWE-agent](https://github.com/SWE-agent/SWE-agent) / [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) | Research agent loops that Claude Code replaces. Possibly useful later as a benchmark harness (trajectory format, SWE-bench runs). |
| [Kodus](https://github.com/kodustech/kodus-ai) | Review bot with a rules-as-files idea. AGPL-3.0, so don't copy code. |
| [Sweep](https://github.com/sweepai/sweep), [Aider](https://github.com/Aider-AI/aider) bots, [claude-hub](https://github.com/claude-did-this/claude-hub) | Stale, pivoted, or no license |
| Claude Code `/autofix-pr` | Hosted and closed. Its policy is still worth copying: clear failure → fix and push; ambiguous or architectural → ask; duplicate → note. |
| [CSCS coding-agents guide](https://docs.cscs.ch/guides/coding-agents/) | The only HPC-side guidance found. It warns that sandbox limits don't carry over to scheduler jobs an agent submits, which is the problem csub solves. |

### GitHub primitives to lean on

- **Rulesets / branch protection:** required human review, dismiss stale approvals on push, required status
  checks (approval check, replay test, tests), bot restricted to `agent/*`.
- **Merge queue:** CI must also run on `merge_group`. It removes most rebase churn, so the agent handles only
  real conflicts.
- **Environments with required reviewers:** the release and deploy gates. Custom deployment protection rules
  would let Temporal act as the gate, but they are in public preview and need Enterprise for private repos.
- **Check runs:** rich per-PR status with "re-run" buttons; GitHub App only.

## 5. Unverified

- How long GitHub keeps webhook deliveries for the deliveries API (believed to be about 3 days).
- PR-Agent's current license.
- Whether Anthropic officially documents `/autofix-pr` (only third-party sources found).
- Whether OpenHands has a current resolver successor.
