# Principles for durable agent systems

These are the design principles we take from studying how others are running autonomous agents on Temporal. They apply
to anything that puts a long-running, non-deterministic worker (an LLM, a coding agent, a cluster job)
under a durable orchestrator, including our issue-to-PR pipeline.

## The core idea

An agent being autonomous is not the problem. What matters is that everything the orchestrator
*decides* from is **recorded**, so that recovering from a crash replays the record instead of asking
the model again. Temporal gives us the record. It does not give us safe side effects, session
recovery, memory management, or policy. Those are our job.

## 1. Know what "durable" covers

Durability applies to the *record*, not to the world. Keep three things apart:

- **Execution state**: what has completed and what is pending. The orchestrator makes this durable.
- **Conversation context**: what the next model call needs to see. We must carry it explicitly.
- **External session state**: files, browsers, REPLs, subprocesses. Nothing restores these for us.

When someone says "it's durable," ask which of the three they mean.

## 2. Put the retry boundary where you want to stop re-doing work

Whatever sits inside one retryable unit gets redone from the start after a failure. Finer units
(one LLM call, one tool call) give finer recovery but cost more bookkeeping. If another runtime owns
the state, such as Claude Code in a cluster job, treat it as one coarse unit. Make that boundary
smaller only if the runtime can resume itself.

Corollary: on recovery, reattach to the existing job or workspace first. Never blindly submit a second one.

## 3. Retrying is a business decision

A crash between "the effect happened" and "we recorded it" is unavoidable, and the orchestrator can't
close that gap. For each kind of action, decide what a repeat means:

| Action | Typical contract |
|---|---|
| Read | Retry freely |
| LLM call | Retry, accept the extra cost, cap attempts and spend |
| Submit a job | Reattach or deduplicate by ID before retrying |
| Irreversible command (send, delete) | Needs an idempotency key, or reconciliation after an unknown outcome |

"Exactly once" only comes from the destination enforcing it. Unlimited retries with no wall-clock or
spend budget turns a provider outage into an open-ended stall.

## 4. Freeze what drives decisions

Settings that choose which commands run (feature flags, retry policy, routing) are part of the run's
state. Capture them when the run starts so a config change can't reinterpret an old run.

Freezing inputs doesn't freeze code. Changing orchestrator logic still needs versioning and replay
tests. Keep *mutable* authority (credentials, permissions) at the point of use, so a saved binding
never becomes permanent permission.

## 5. Long-lived is not unbounded

Budget these separately, because each fails differently:

- event history length
- size of any single payload
- size of state carried across a restart
- model context window
- fan-out of delegated work

Trimming the model's context doesn't shrink recorded history. Fail loudly when a limit is hit
rather than silently starting over. Summarizing loses detail, so keep the original evidence in
artifacts and pass references.

## 6. Stop is a protocol, not a flag

"Nothing new will start" and "everything started has finished" are different guarantees. A clean stop
needs to:

- close admission first
- account for every started child, including detached ones
- acknowledge only once the transition has actually completed
- reject stale commands (revisions, idempotency keys)

Draining is cooperative and can be slow. If you need an emergency brake, build a separate kill path
and plan to reconcile the partial effects it leaves.

## 7. Keep the live view separate from the record

What the UI shows *now* (streamed tokens, progress) is not what has *committed*. Don't put token
events into history. If a user must be able to reconnect and see everything, persist a separate
ordered stream.

## 8. Match the tool to the job

Durable orchestration fits work where waiting dominates: research, triage, review loops, approvals,
bounded teams of agents. It is a poor fit when you need:

- instant restore of a live process on another machine
- immediate halt of all effects
- exactly-once effects without help from the destination
- very high-frequency cheap actions, where bookkeeping outweighs the work
- an agent rewriting its own orchestrator mid-run

For those, add machinery (persistent workspaces, kill switches, outboxes) or use a coarser durable
milestone around a faster local loop.

## Decision rule

- We own the agent loop and can serialize its state: orchestrate **per turn**.
- Another runtime owns the state: orchestrate **around** it.

Either way, evaluate session durability, effect safety, stop latency, and history growth on their own
terms. Workflow durability doesn't answer them.

## Test the gaps, not the happy path

The failures that matter live between steps. Kill the worker after submitting a job but before it's
acknowledged, after the agent writes its result, and during a version change. Check that progress
survives and that nothing external happened twice.
