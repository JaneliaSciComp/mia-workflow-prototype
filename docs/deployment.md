# Deploying to production

How the services run outside a developer's tmux: Temporal in Kubernetes, the workers and csub brokers on a
cluster-attached host as a service account, under systemd. This page is the plan; the parts marked **todo** are
not built yet. Developer setup is [`autodev-local-setup.md`](autodev-local-setup.md); the service-account checklist
(GitHub App, branch protection, broker policy) is in [`notes/autodev-runbook.md`](../notes/autodev-runbook.md).
Back to the [README](../README.md).

## The split

Temporal is only orchestration state and can live anywhere the workers can reach. Everything else has to run
*as the cluster account* on a host that sees the cluster:

- **Shared filesystem, read-write, as the right uid.** The worker creates job workspaces under `work_root`, syncs a
  git mirror in `state_dir`, copies records into workspaces, reads job output back with `read_untrusted`, and
  mia-data publishes into `storage_root`. All of it must be visible to compute nodes and owned by the account that
  runs the LSF jobs.
- **Claude credentials and the broker policy are the account's.** Jobs run `claude` with the broker account's
  credentials; `~/.config/csub/broker.toml`, `~/.csub` and the LSF project belong to that account.
- **`bsub`.** csub has an `ssh` transport (`csub relay`) that would let a worker elsewhere reach a broker on the
  submit host, so submission alone is not the blocker; the filesystem is. A worker in Kubernetes would need the
  cluster filesystems mounted in the pod with a matching uid, which is a question for clusteradmins and a lot of
  coupling for no gain.

| component | where | notes |
|---|---|---|
| Temporal server, UI, Postgres | Kubernetes | one cluster; one namespace per deployment (`autodev`, `snippets`, `mia-data`); TLS on the frontend; UI behind SSO |
| csub broker + sandboxed worker, one pair per service | cluster-attached host, service account | systemd user units with linger, `Restart=always`, logs in journald |
| agent and harness jobs | LSF via csub | unchanged |
| configs and secrets | the service account's home | `~svc/.config/mia/{mia,autodev,autodev-snippets,mia_data}.yaml`, the App PEM, token files; `$MIA_MANIFEST` names the manifest |
| code | a tagged checkout at a fixed path, with a `current` symlink | the sandbox binds it read-only; deploy = `pixi install` in the new checkout, flip the symlink, `mia restart <service>` |

### Which host

Ask the HPC team for a small VM that is an LSF client with `/groups` and `/nrs` mounted. The submit host is
documented as one core and 16 GB shared by all users, for lightweight orchestration scripts; the worker is
lightweight when idle, but mirror syncs and snapshot extraction are bursts, and a shared host is where the
"unauthenticated dev server" gotcha came from. Fallback if no VM is possible: run each service as a long-running
LSF job (csub's "agent as a csub job" mode provides the sandbox and the broker socket). Temporal makes the 48-hour
queue limit harmless, since a restarted worker reattaches to running jobs, but the billing meter runs the whole
time, so it is a fallback, not the plan.

### mia.yaml stays

`mia.yaml` is the per-host manifest: which services run here, where agentic-sandbox is, whether `mia` runs
Temporal. Production uses it with `temporal.managed: false`, so `mia up` only verifies the cluster answers and
never starts or stops it. The service configs keep their shape; only `temporal.address` and the new TLS fields
change.

## Changes needed (todo)

None of these touch `workflows.py`, so there is no replay risk.

1. **systemd backend for `mia_ops`.** A `runner: systemd` key in `mia.yaml` (default `tmux`). `mia systemd
   install` writes `~/.config/systemd/user/mia-<service>-broker.service` and `-worker.service` from the same
   `Stack`/`Service` objects `services.py` uses for tmux; `up`, `down`, `restart`, `ps`, `logs` and `attach` then
   dispatch to `systemctl --user` and `journalctl --user`. The broker socket moves from `/scratch/$USER` (node-local,
   periodically cleaned) to `$XDG_RUNTIME_DIR`, which exists once `loginctl enable-linger svc` is set.
2. **Temporal TLS and auth.** `TemporalCfg` gains `tls: bool = False` and either `client_cert`/`client_key` paths
   (mTLS) or `api_key_file`, all with defaults, so the `Deployment` protocol and every existing config stay valid.
   `_client` in `autodev/cli.py` passes them to `Client.connect`; the `temporal` CLI calls in `mia_ops/services.py`
   pass the matching `--tls-*` flags. HTTP CONNECT through the sandbox proxy (`via_https_proxy`) works unchanged.
3. **Namespaces are an admin step.** In production `ensure_namespace` checks that the namespace exists and fails
   with a message instead of creating it; creation needs an operator credential the worker should not hold.
4. **Agent billing from a file.** Production should not depend on an interactive OAuth login for the service
   account. Add `agent.api_key_file`, bind it read-only into the job and set `ANTHROPIC_API_KEY` from it (the
   broker policy's `env_allow` must cover it), and drop the `mia up` refusal of `api_key_env`.
5. **State on backed-up storage.** `state_dir` is the durable record on the cluster side (job ids, mirrors,
   logs); put it under `/groups`, which is backed up, not under `/nrs`. The Temporal database is the other half;
   back up Postgres in Kubernetes.
6. **Release procedure.** Tag, `git clone` into `releases/<tag>`, `pixi install`, `pixi run mia exec <service>
   check` against production config, flip `current`, `mia restart <service>`. A worker restart mid-round is safe:
   job submission is idempotent and `wait` reattaches by job id. Changes to `workflows.py` or `decide.py` still
   need `workflow.patched()` for runs in flight ([`development.md`](development.md)).

## Open questions for the HPC team

- A cluster-attached VM for the service account, or permission to run lingering user services on the submit host.
- Whether the Kubernetes Temporal frontend is reachable from that host, and who issues its client certificates.
- Rootless bwrap (or podman) for the service account on the VM, since the worker runs inside agentic-sandbox.
