# mia-data handoff: what was built on 2026-10-11/12 and what you need to do to try it

Branch `mia-crops-poc` in this repo (26 commits after `42c1c19`), and a new local repo at
`<harness.dir>` (your mia-snippets checkout) (`main`, 24 commits, **not on GitHub**). Everything follows `notes/mia-crops-spec.md`
(now v0.9) and `notes/plans/2026-10-11-mia-crops-poc.md`. Nothing was pushed, no GitHub repos were created, and
no broker policy was edited. Each task was implemented by one agent and reviewed by another; every Important
finding was fixed and re-reviewed; both repos then had a whole-branch review and one fix wave. The rulings I made
on your behalf are listed at the end: read them, they are the decisions you did not get to make.

## What exists

**This repo** (`pixi run test`: 318 passed, including replay tests on two recorded workflow histories)
- `autodev/jobs.py`: generic csub job runner split out of `agent.py` (submit-once by key, cwd reattach,
  `rw_mounts`). `autodev/github.py`: statuses, tree SHAs, raw files, issues/comments by marker, zarr-doctor's
  `ensure_branch`/`upsert_file` restored. `autodev/config.py`: `github.trusted_bots`, `github.required_checks`,
  `agent.max_budget_usd`. `scripts/autodev-up.sh` reuses a running Temporal server and ensures its namespace.
- `mia_data/`: config, models, catalog reader, seed, binding/host checks, status comments, galleries branch,
  idempotent publication, job specs + snippet snapshots + match prompt, activities, `MiaDataDataset` and
  `MiaDataPoll` (match → test → review → approve → crop → publish, and the develop path through autodev PRs),
  `crop_test.py` (shared judging), CLI.
- CLI (`pixi run md …`): `check`, `init-labels`, `seed [--dry-run]`, `worker`, `schedule apply`,
  `test-pr <pr> --binding <file> --record <id>`, `crops-by-snippet <name>`, `reindex [dataset]`.
- `scripts/mia-data-up.sh`, `mia_data.yaml.example`, and a "mia-data" section in `notes/autodev-runbook.md`.

**`<harness.dir>` (your mia-snippets checkout)** (`cd harness && pixi run test`: 122; `pixi run ci-test`: 14)
- `SPEC.md`, `harness/` (`smoke`, `crop`, `verify`, `preview`; `io` with range reads, dataset cache, byte cap,
  host checks on every redirect hop), `ci/check_pr.py` + `.github/workflows/check.yml` (runs the checker from
  `origin/main`, not the PR), `harness/allowlist.txt`, `ci/maintainers.txt` (= `krokicki`).
- Snippets `tiff-zip-member` (bbbc034) and `mrc-http` (czcdp-10003), each with an example binding. Both pass
  `harness smoke` against the real data from this host: bbbc034 reads 52×1024×1024 raw and label (46.5 MB,
  deflated zip members fetched whole); czcdp-10003 reads a 450×928×928 float32 tomogram fetching 1 MiB of 1.9 GB.

## What you need to do before anything runs end to end

1. **Repos.** Create `JaneliaSciComp/mia-workflow-data` **with an initial commit** (the galleries branch is
   created through the trees API, which 409s on an empty repo) and `JaneliaSciComp/mia-snippets`; then
   `cd <harness.dir> && git remote add origin … && git push -u origin main` (push `main` before opening
   any PR: CI runs the checker from `origin/main`). Install the autodev GitHub App on both with Issues RW,
   Pull requests RW, Contents RW (both), Commit statuses RW, Checks R, Metadata R. Branch protection settings are
   in the mia-snippets README; apply them after the first `test-pr` has posted a `mia-data/crops` status.
2. **Shared directories** visible from the worker host and compute nodes, none under `$HOME`:
   `paths.state_dir`, `paths.work_root`, `storage.root`, and a mia-snippets checkout for `harness.dir`
   (`cd harness && pixi install` there; jobs call `harness/.pixi/envs/default/bin/harness` directly).
3. **Broker policy** (`~/.config/csub/broker.toml` of the service account): `allowed_roots` covering
   `harness.dir`, `work_root`, `storage.root`; `env_allow` with `MIA_HARNESS_*`; `[agents] claude = [...]`
   enabled; the data hosts and their redirect targets in `allowed_hosts`. `pixi run md check` prints exactly
   what is missing. Run the redirect probe from the worker host (plan P3) and put the hops into
   `network.redirect_hosts`.
4. **Config.** `cp mia_data.yaml.example mia_data.yaml`, fill it in, then run
   `CSUB_TRANSPORT=unix CSUB_SOCKET=/scratch/$USER/mia-data/csub.sock pixi run md check` until every row is `ok`
   (the broker must be running for the csub row: start it with step 5 first if needed).
5. **Bring up.** `pixi run ad up` (Temporal dev server + autodev), then `scripts/mia-data-up.sh` (broker,
   sandboxed worker, schedule). `pixi run md init-labels`; `pixi run md seed --dry-run`; `pixi run md seed`.
6. **First real check.** Open the two snippet PRs (or merge them by review; they are human-authored), then
   `pixi run md test-pr <pr> --binding snippets/tiff-zip-member/examples/bbbc034-hipsc-nuclei-3d.yaml --record bbbc034-hipsc-nuclei-3d`.
   The PR comment should show four crops with nucleus overlays inline. This is also the first check that
   `?raw=true` images render in the private repo, as zarr-doctor's did.
7. **Milestone 3.** Add `data:go` to the bbbc034 issue: expect `matching → testing → review` with the crops in
   the issue; add `data:approve`; expect `cropping → done` with 16 crops. Then add `data:go` again after `done`:
   the second round must submit a crop job with `--existing` (exercises the manifest-directory mount fix).
8. **Not built, by design:** Tasks 20, 22, 23 of the plan (the real runs), `autodev-snippets.yaml` for the
   `snippets` autodev deployment (write it from the plan's Task 22 list when you get there).

## Known gaps that can wait (from the reviews)
- Runtime of the two up-scripts (tmux, sandbox mounts, server reuse) is unverified on a real host.
- The match agent's `--allowedTools` string in `claude -p` mode and the Claude Code login inside the csub job
  were not exercised.
- `check_pr.py` static check is lint, not a sandbox: aliasing, `getattr`, `__builtins__` bypass it.
- Harness: 206 responses now check the `Content-Range` start; zlib inflate has no `max_length`; volume weighting
  with mixed access modes is per group, not per crop; directory stores (zarr/N5) would record no validators and
  fail verify closed (no such snippet exists yet).
- Worker: `prepare_*` writes into a reused workspace follow symlinks; `submit` probes/snapshots before the reattach
  path; `_test_head`'s error-status window does not cover its trailing GitHub calls (fails closed, status stays
  pending); the poll activity has no heartbeat; "rounds never overlap" is not enforced after an await timeout;
  the merge-commit test report is not posted on the PR; `modality:<label>` uses the record's first modality value,
  not a family.
- Tests to add: "h1 passed, h2 merged with an equal tree → M must be tested"; develop-path continue-as-new
  (verified by a scratch test only); approve arriving during `testing`/`cropping`.
- The plan file still shows the old `pixi run --manifest-path` job invocation (historical; the code and spec are
  updated).

## Rulings made on your behalf (in order; what it costs if wrong)
1. Two implementer tracks ran in parallel, one per repo. Cost: none.
2. Nothing that needs org repos, the App installation, the service-account policy or a Temporal server was
   executed; those steps are the checklist above. Cost: one morning of setup.
3. `GitHub.label_event` returns `(actor, event id, created_at)`; the approve gate needs the timestamp. Cost: a
   3-tuple unpack if autodev ever calls it directly.
4. `mia-snippets` lives at `<harness.dir>` (your mia-snippets checkout) and was never pushed. Cost: you push it.
5. Subagent commits keep their own harness attribution trailer. Cost: none.
6. Task 11's real-data smokes ran on this host directly, not through csub. Cost: the csub path is first exercised
   by you.
7. `CsubJobs.submit` builds the job spec after the id-file check but before the cwd lookup; the uncovered case is a
   worker misconfiguration inside a crash window. Cost: one `needs-human` if it ever happens.
8. Task 4 (scripts) ran last, after the CLI existed. Cost: none.
9. A zip-member cache hit in a new job may read the zip's central-directory tail once (CRC check). Cost: ≤1 MiB
   per zip per job against a 5 GB cap.
10. `check_binding` takes `pins`; `publish` copies only verify-listed files; the manifest temp name is unique;
    `crop.json` carries `snippets: [{name, commit}]`. Cost: none, these tighten the spec.
11. Crop checks overlap before content. Cost: only which rejection bucket a doubly-bad candidate lands in.
12. All job builders take `pins` and assert the harness pin; snapshots are flock-serialised per SHA; the match
    agent's tools no longer include `Bash(pixi run:*)`. Cost: none.
13. Test crops stay in the job workspace under `work_root`; the galleries branch is the reviewed artefact (spec
    §7.4 updated to v0.9). Cost: test crops vanish when `work_root` is cleaned.
14. A `data:go` arriving while the issue is in `review` starts a new round (spec §7.4 "re-trigger"); a `go`
    queued while jobs run is refused as busy. Cost: an abandoned test gallery on an accidental re-trigger.
15. Production verify is judged by its report and the half rule, not its exit code (harness verify exits 1 when
    any crop fails). Cost: none.
16. The equal-tree merge shortcut counts only a passing test of the merged head itself (§9); otherwise the merge
    commit is tested. Cost: one extra crop test when a human merges a different head than the last tested one.
17. On the final review's Critical finding, jobs now invoke the harness environment's entry point directly
    instead of `pixi run` (which cannot take its install lock on a read-only mount). Cost: `pixi install` must
    have been run in `harness.dir` before the first job; `md check` verifies it.
