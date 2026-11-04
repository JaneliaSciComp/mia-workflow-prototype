# Evolution of self-hosted Temporal server operations (2020 – Oct 2026)

Method note: server and Helm release dates below come from the GitHub Releases API (`published_at`) for temporalio/temporal and temporalio/helm-charts, pulled 2026-10-07. Release-note quotes come from the release bodies at `https://github.com/temporalio/temporal/releases/tag/<tag>`. Some `.0` releases have no published GitHub release; the first published patch is used instead and marked as such.

## Dated timeline (master)

### Takeaway
Self-hosting got much simpler in steps. 1.20 (Feb 2023) dropped the Elasticsearch requirement for advanced visibility. 1.24 (May 2024) removed standard and Cassandra visibility. 1.27 (Feb 2025) made Nexus GA. 1.30 (Mar 2026) slimmed the Docker images and deprecated auto-setup. Helm chart 1.0.0 (Apr 2026) removed the bundled databases and stabilized the values format. 1.31 (Apr 2026) made Worker Deployments GA. Some things have not changed: the one-minor-at-a-time upgrade rule, the fixed shard count, the lack of a supported operator, and Cloud-only features such as API keys.

### Cited Findings
| Date | Version | Ops-relevant change |
|---|---|---|
| 2020-03-19 | helm-charts initial commit | Chart created; the README disclaimer was reworded in Oct 2020 from "against prod" to "for development". — [helm-charts git log](https://github.com/temporalio/helm-charts/commits/main/README.md) |
| 2020-09-30 | Server v1.0.0 | First GA. Releases point to the `temporalio/auto-setup` image. — [v1.0.0](https://github.com/temporalio/temporal/releases/tag/v1.0.0) |
| 2020-11-11 | v1.3.0 | Metrics config moved to a `global.metrics` section (one Prometheus listener per process). — [v1.3.0](https://github.com/temporalio/temporal/releases/tag/v1.3.0) |
| 2020-12-09 | v1.4.0 | ClaimMapper and Authorizer plugin interfaces added; "Authorization for JWT tokens (#986)". — [v1.4.0](https://github.com/temporalio/temporal/releases/tag/v1.4.0) |
| 2020-12-23 / 2021-01-22 | v1.5.0 / v1.6.0 | Kafka deprecated for visibility; internal queues used instead. That removed one dependency from ES-based setups. — [v1.5.0](https://github.com/temporalio/temporal/releases/tag/v1.5.0), [v1.6.0](https://github.com/temporalio/temporal/releases/tag/v1.6.0) |
| ~Feb 2021 | v1.7.0 | Elasticsearch v7 supported. GitHub shows `published_at` 2021-10-29, probably from a later edit; the helm chart tag v1.7.0 is dated 2021-02-17. — [v1.7.0](https://github.com/temporalio/temporal/releases/tag/v1.7.0) |
| 2021-03-24 | v1.8.0 | Config parameters for choosing the authorizer and claim mapper without custom builds ("Add config parameters for selecting authorizer and claim mapper (#1291)"). — [v1.8.0](https://github.com/temporalio/temporal/releases/tag/v1.8.0) |
| 2021-08-31 | v1.12.0 | Elasticsearch v7 becomes the default. — [v1.12.0](https://github.com/temporalio/temporal/releases/tag/v1.12.0) |
| 2021-10-29 | v1.13.0 | SQLite persistence driver contributed by Jacob LeGrone (DataDog); "still under development". — [v1.13.0](https://github.com/temporalio/temporal/releases/tag/v1.13.0) |
| 2021-12-14 | v1.14.0 | "Temporal now officially supports MySQL 8 starting from version 8.0.19 and PostgreSQL 13 starting from version 13.5." — [v1.14.0](https://github.com/temporalio/temporal/releases/tag/v1.14.0) |
| 2022-01-20 | helm README | "**We do not recommend using Helm for managing Temporal deployments in production**. Rather, we recommend it for templating/generating manifests for Temporal's internal services only." — [helm-charts commit 91517a4](https://github.com/temporalio/helm-charts/commit/91517a4) |
| 2022-06-21 | v1.17.0 | SQLite moved to pure Go, so no CGO is needed; the release binary runs a local dev server with `--env development-sqlite`. — [v1.17.0](https://github.com/temporalio/temporal/releases/tag/v1.17.0) |
| 2022-09-19 | v1.18.0 | Elasticsearch v6 removed; ES v8 added. — [v1.18.0](https://github.com/temporalio/temporal/releases/tag/v1.18.0) |
| 2023-02-17 | **v1.20.0** | **Advanced visibility on SQL, in beta:** "MySQL 8.0.17+, PostgreSQL 12+, and SQLite". Requires the `mysql8`/`postgres12` plugins. MySQL 5.7 and PG 9.6–11 to be removed by v1.21. New **internal-frontend** role simplifies mTLS and auth for the system worker. Default authorizer changes. — [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0) |
| 2023-04-24 | helm README | Wording softened: "The only portions of the helm chart that are production ready are the parts that configure and manage Temporal Server itself—not Cassandra, Elasticsearch, Prometheus, or Grafana." — [helm-charts commit 5a39608](https://github.com/temporalio/helm-charts/commit/5a39608) |
| 2023-06-23 | v1.21.0 | Standard visibility and Cassandra-as-visibility deprecated, removal planned for 1.23. Dual (secondary) visibility added for migrations. Worker Versioning "preview" (Build IDs/version sets). Breaking JWT change: system role becomes `temporal-system:<role>`. The 1.21.0 MySQL visibility upgrade script had a bug; use 1.21.2. — [v1.21.0](https://github.com/temporalio/temporal/releases/tag/v1.21.0) |
| 2023-09-05 | v1.22.0 | Default authorizer refactored to use method metadata. — [v1.22.0](https://github.com/temporalio/temporal/releases/tag/v1.22.0) |
| 2024-03-22 | v1.23.0 | "Standard visibility will be deprecated in the next release v1.24.0 … this is the last minor version to support them"; the `LIKE` operator is dropped. — [v1.23.0](https://github.com/temporalio/temporal/releases/tag/v1.23.0) |
| 2024-04 | Temporalite | Repo archived (last push 2024-04-03): "An experimental distribution of Temporal that runs as a single process". Replaced by the CLI dev server. — [temporalio/temporalite](https://github.com/temporalio/temporalite) |
| 2024-05-31 | **v1.24.0** | "Standard visibility is no longer supported in this version." The main store plugin must also change to `mysql8`/`postgres12`. 1.24.0 itself had a SQL visibility bug, so skip to a later patch. admin-tools image released separately. — [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0) |
| 2024-09-09 | v1.25.0 | (Routine release; nothing notable for ops found in the notes.) — [v1.25.0](https://github.com/temporalio/temporal/releases/tag/v1.25.0) |
| 2024-12-23 | v1.26.2 (no 1.26.0 published) | First Deployment APIs (Worker Versioning pre-release). "Nexus … still in public preview … but has now been enabled by default." — [v1.26.2](https://github.com/temporalio/temporal/releases/tag/v1.26.2) |
| 2025-02-26 | **v1.27.1** (1.27.0 was a prerelease; "DO NOT use it if you are using SQL-based … persistence") | **"Nexus is now GA with a stable server API."** Visibility Scan APIs deprecated. — [v1.27.1](https://github.com/temporalio/temporal/releases/tag/v1.27.1) |
| 2025-06-27 | v1.28.0 | Worker Deployment / Versioning APIs move to **Public Preview** ("Production usage is encouraged"). 1.27.x Worker Deployments must be deleted before upgrading. statsd moves to OTel as Tally deprecation begins. — [v1.28.0](https://github.com/temporalio/temporal/releases/tag/v1.28.0) |
| 2025-10-03 | v1.29.0 | Task-queue fairness pre-release; `UpdateTaskQueueConfig` (rate limits); eager workflow start on by default. Announces upcoming Docker image breaking changes. tctl deprecated. — [v1.29.0](https://github.com/temporalio/temporal/releases/tag/v1.29.0) |
| 2025-12-15 → 2026-03-26 | helm temporal-1.0.0-rc.1/2/3 | The "V1 (#810)" commit (2025-12-18) removes the sub-charts. — [helm-charts releases](https://github.com/temporalio/helm-charts/releases) |
| 2026-03-02 | **v1.30.1** (no 1.30.0 published) | Slim Docker images: bash, curl, cqlsh, tctl, and psql/mysql clients removed. `dockerize` replaced by built-in sprig templating (`TEMPORAL_SERVER_CONFIG_FILE_PATH`). **`temporalio/auto-setup` deprecated**: "For local development, we recommend using the CLI dev server." **Helm chart must be ≥ `temporal-0.73.2`.** OpenSearch 2+ supported for visibility. Cross-namespace commands now off by default. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1) |
| 2026-04-08 | **helm temporal-1.0.0** | Stable chart; no database sub-charts. Blog published 2026-04-21. — [helm-charts releases](https://github.com/temporalio/helm-charts/releases), [blog](https://temporal.io/blog/an-important-milestone-for-temporals-helm-charts) |
| 2026-04-29 | **v1.31.0** | Schema bumps: MySQL/PG core v1.19, visibility v1.14; ES v14. **Worker Deployment APIs GA.** Versioning V1/V2 sunset. **Principal Attribution** (who-did-what in history). `passwordCommand` for AWS RDS / GCP Cloud SQL IAM auth. Task-queue priority/fairness GA. Cassandra 5 (≥5.0.4). Serverless Workers preview. — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0) |
| 2026-07 / 08 | helm temporal-proxy 0.1.0–0.2.1 | New proxy chart ("handles routing between clusters, namespace translation, encryption"). — [helm-charts releases](https://github.com/temporalio/helm-charts/releases) |
| 2026-09-11 | **v1.32.0** | Standalone Activities GA. Unified visibility query converter on by default; this is a breaking type-strictness change. Admin search-attribute APIs deprecated in favor of the operator API and CLI. Final release with legacy Worker Versioning, which is removed in v1.33. — [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0) |
| 2026-09-12 | helm temporal-1.7.0 | Latest chart as of research date. — [helm-charts releases](https://github.com/temporalio/helm-charts/releases) |

Release cadence, from the dates above: about one minor every 1–2 months in 2020–21 (v1.0–v1.14). It slowed to about one every 3–4 months in 2022–23, then every 3–6 months from 2024 to 2026 (1.23 Mar-24, 1.24 May-24, 1.25 Sep-24, 1.26 Dec-24, 1.27 Feb-25, 1.28 Jun-25, 1.29 Oct-25, 1.30 Mar-26, 1.31 Apr-26, 1.32 Sep-26). Patch releases are frequent, often for security fixes such as Go updates and CVE-2026-5724 in 1.30.6/1.31.2. — [releases](https://github.com/temporalio/temporal/releases)

### Inferences
- Since 1.24 (May 2024), a "Postgres-only" Temporal is fully supported for both persistence and advanced visibility. The core "you need Cassandra + Elasticsearch" problem of 2020–2022 is gone for small and medium deployments.
- A few `.0` releases were pulled or skipped because of SQL bugs (1.21.0 MySQL, 1.24.0 SQL visibility, 1.27.0 SQL persistence; 1.26.0 and 1.30.0 have no published GitHub release). Waiting for the first patch release is a sensible ops policy.

### Gaps
- Why v1.26.0/1.26.1 and v1.30.0 have no published GitHub release (possibly withdrawn or released only as tags) is not verified.
- v1.0.0 → v1.2.x and 1.11, 1.17 `.0` dates exist only as patches/tags in the API listing; 1.17.0 is dated 2022-06-21 per its notes.

## Visibility: SQL advanced visibility, Elasticsearch need, standard visibility deprecation

### Takeaway
SQL advanced visibility arrived in 1.20 (Feb 2023, beta). Standard visibility was deprecated in 1.21 and removed in 1.24 (May 2024). Cassandra can no longer be a visibility store. The docs still steer anything beyond small scale toward Elasticsearch or OpenSearch.

### Cited Findings
- 1.20.0: "Advanced visibility features are now supported using one of the supported SQL databases (MySQL 8.0.17+, PostgreSQL 12+, and SQLite)". It was labeled "(beta testing)". Users switch `sql.pluginName` to `mysql8`/`postgres12` and recreate custom search attributes. — [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0)
- 1.21.0: "Standard Visibility is entering a deprecation process … expect to remove it in v1.23.0" and "Cassandra, as a Visibility store, will be deprecated together with Standard Visibility". Worker Versioning preview "requires Advanced Visibility". — [v1.21.0](https://github.com/temporalio/temporal/releases/tag/v1.21.0)
- 1.23.0 pushed the removal to 1.24. 1.24.0: "standard visibility is no longer supported in this version." — [v1.23.0](https://github.com/temporalio/temporal/releases/tag/v1.23.0), [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0)
- Current docs support matrix: Elasticsearch v7 (server 1.7+) or v8 (1.18+); OpenSearch 2+ (1.30.1+); MySQL 8.0.17+ (1.20+); PostgreSQL 12+ (1.20+); SQLite 3.31.0+ (1.20+); Cassandra "Not supported … Deprecated in … v1.21, removed in … v1.24". — [docs: Visibility](https://docs.temporal.io/self-hosted-guide/visibility)
- Docs: Elasticsearch/OpenSearch "Recommended for any setup that spawns more than a few Workflow Executions." — [docs: Visibility](https://docs.temporal.io/self-hosted-guide/visibility)
- Dual visibility: "Temporal Server v1.21 introduced support for a secondary Visibility store … useful for migrating your Visibility store database." — [docs: Visibility](https://docs.temporal.io/self-hosted-guide/visibility)
- 1.30.1 added OpenSearch 2+ as a visibility store; the new unified query converter was experimental and off by default. 1.32.0 turned it on by default, which is a breaking change: type mismatches and empty-string Text filters now error. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1), [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0)
- The Helm chart README lists visibility on MySQL, PostgreSQL, or Elasticsearch; Cassandra cannot be used for visibility. — [helm-charts README](https://github.com/temporalio/helm-charts)

### Inferences
- Elasticsearch is no longer required for any feature: custom search attributes, list filters, Schedules, and Worker Versioning all work on SQL visibility. It is now a scale decision. The docs' "more than a few Workflow Executions" wording is vague, so the scale cutoff is undocumented.
- One small hazard remains: the SQL and ES query engines behaved differently enough that a unified converter was needed (1.30–1.32). Visibility query behavior changed in 1.32.

### Gaps
- No primary source found for current per-type custom search attribute limits on SQL vs ES. Earlier docs listed per-type caps for SQL; these were not re-verified.
- No official throughput number found for when SQL visibility stops being enough.

## Persistence: Postgres/MySQL vs Cassandra maturity, supported versions

### Takeaway
SQL (MySQL 8, PostgreSQL 12+) has been a first-class main store since early on and the only fully-featured option since 1.24. Cassandra remains supported for the main store only (Cassandra 5 since 1.31). The docs give no clear threshold for when Cassandra is needed.

### Cited Findings
- v1.14.0 (Dec 2021): official support for "MySQL 8 starting from version 8.0.19 and PostgreSQL 13 starting from version 13.5". — [v1.14.0](https://github.com/temporalio/temporal/releases/tag/v1.14.0)
- v1.20.0 deprecation heads-up: MySQL 5.7 and PostgreSQL 9.6–11 to be removed by v1.21. 1.24 requires the main store plugin `mysql8`/`postgres12`. — [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0), [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0)
- The docs upgrade page still says "PostgreSQL (v9.6+, with v12+ support for advanced visibility features)" and "MySQL (v5.7+ …)". That looks stale and contradicts the release-note deprecations. — [docs: Upgrade server](https://docs.temporal.io/self-hosted-guide/upgrade-server)
- v1.31.0: Cassandra 5 support; "Cassandra 5 versions earlier than `5.0.4` are not supported." Also `passwordCommand` for IAM-auth to AWS RDS and GCP Cloud SQL. The helm README notes this works for "Temporal server v1.31+, **SQL datastores only**". — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0), [helm-charts README](https://github.com/temporalio/helm-charts)
- 1.30.5 / 1.31.1 patches include PostgreSQL schema fixes and pgx simple-protocol duration encoding, so the pgx driver path is still being refined. — [releases](https://github.com/temporalio/temporal/releases)
- Until chart 1.0.0, the Helm chart bundled Apache Cassandra by default. — [Temporal blog, 2026-04-21](https://temporal.io/blog/an-important-milestone-for-temporals-helm-charts)

### Inferences
- The default has moved from Cassandra to SQL in practice: the chart no longer defaults to Cassandra, and Cassandra cannot host visibility. A Cassandra deployment therefore needs a second store (SQL or ES) for visibility. For a small team, Postgres alone is the simplest supported setup.

### Gaps
- No current official doc found stating when Cassandra is needed (e.g., a throughput or shard threshold), or a maximum tested Postgres major version (e.g., PG 16/17).

## Helm chart history, 1.0.0, compatibility, operators

### Takeaway
The chart started in March 2020 as a dev-only chart bundling Cassandra, ES, Prometheus, and Grafana, with an explicit "not for production" stance through 2022. The 2023 wording said only the Temporal parts were production-ready. 1.0.0 (2026-04-08) removed all sub-charts, adopted the raw server config format, and promised stability. There is still no official operator; the community alexandrevilain/temporal-operator lags behind server releases.

### Cited Findings
- Old README (2022-01-20): "**We do not recommend using Helm for managing Temporal deployments in production**." From 2023-04-24: "The only portions of the helm chart that are production ready are the parts that configure and manage Temporal Server itself—not Cassandra, Elasticsearch, Prometheus, or Grafana." — [commit 91517a4](https://github.com/temporalio/helm-charts/commit/91517a4), [commit 5a39608](https://github.com/temporalio/helm-charts/commit/5a39608)
- Chart tags followed server versions (v0.21.1 → v1.22.4, 2020–Jan 2024), then switched to independent `temporal-0.33.0` (2024-01-30) and later. — [helm-charts releases](https://github.com/temporalio/helm-charts/releases)
- 1.0.0: rc.1 on 2025-12-15, rc.2 on 2026-01-14, rc.3 on 2026-03-26, GA on **2026-04-08**. Then 1.1.0 (04-14), 1.2.0 (04-29), 1.3/1.4 (06-26), 1.5 (07-01), 1.6 (07-13), 1.7.0 (09-12). — [GitHub API, helm-charts releases](https://github.com/temporalio/helm-charts/releases)
- Blog (Tom Wheeler, 2026-04-21): the chart "began as a side project … six years ago", with more than 400 PRs from about 150 contributors (two-thirds non-employees). 1.0.0 is "not expected to introduce breaking changes for the releases that follow in this series". Sub-charts removed for databases (Cassandra was the default) and for ES, Prometheus, and Grafana. Other changes: config moved to `server.config.persistence.datastores`, `server.internal-frontend` naming, `imagePullSecrets` is now an array. New features: configurable readiness probes, deployment strategies, `minReadySeconds`, `server.config.logLevel`, AWS IAM auth and TLS for ES/OpenSearch, and read-only filesystem. — [Temporal blog](https://temporal.io/blog/an-important-milestone-for-temporals-helm-charts)
- UPGRADING.md: "The chart **does not install any database sub-charts** … You must provide your own persistence". Persistence "follows the **raw Temporal server config format**". `createDatabase` / `manageSchema` flags control schema jobs. `shims.dockerize` / `shims.elasticsearchTool` give compatibility with images older than 1.30. — [UPGRADING.md](https://github.com/temporalio/helm-charts/blob/main/UPGRADING.md)
- README: schema Job uses Helm hooks (`pre-install`, `pre-upgrade`) by default (`useHelmHooks: false` disables). Recommends credentials in an externally managed `Secret` (e.g., External Secrets). "Not supported: running newer binaries with an older schema. Supported: downgrading binaries." — [helm-charts README](https://github.com/temporalio/helm-charts)
- Compatibility: v1.30.1 notes say "you must now use a minimum Helm chart version of `temporal-0.73.2`" (0.73.2 was released 2026-03-02). The brief's "≥0.73.1" figure conflicts with this primary source; 0.73.1 was released 2025-12-20. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1)
- New `temporal-proxy` chart (0.1.0 on 2026-07-22, 0.2.1 on 2026-08-14) for "routing between clusters, namespace translation, encryption". — [helm-charts releases](https://github.com/temporalio/helm-charts/releases)
- Community operator alexandrevilain/temporal-operator: latest releases are v0.21.0 (2025-04-05) and v0.22.0 (2025-10-31). The README matrix says v0.21.x supports "Temporal v1.20.x to v1.25.x" (K8s 1.27–1.31). Features include cert-manager mTLS, cluster upgrades, and namespace management. No official Temporal endorsement; repo not archived (last push 2026-03-29). — [temporal-operator](https://github.com/alexandrevilain/temporal-operator), GitHub API

### Inferences
- The chart is now a thin, honest deployer for Temporal services. That is easier to reason about, but you must bring and run the databases yourself (e.g., CloudNativePG or RDS for Postgres).
- The operator's documented support stops well behind current server (1.32). Using it for 1.30+ (new image layout, no dockerize) is risky.

### Gaps
- Could not confirm whether v0.22.0 of the operator added 1.26+ support; only the README matrix was read.
- No official Temporal Kubernetes operator was found.

## Upgrades: one-minor rule, schema tooling, auto-setup deprecation

### Takeaway
The rule is unchanged and still enforced in the docs: upgrade one minor at a time, to the latest patch first. Schema migrations are a manual step with `temporal-sql-tool`, `temporal-cassandra-tool`, or `temporal-elasticsearch-tool`; the Helm chart's pre-upgrade hook can automate this. auto-setup was deprecated in 1.30 (Mar 2026). Several minors (1.24, 1.28, 1.30, 1.31, 1.32/1.33) carry breaking ops steps.

### Cited Findings
- "Temporal Server should be upgraded sequentially, one minor version at a time." Upgrade "to the highest available patch version of your current minor version" first. "Temporal Server guarantees backward compatibility between two consecutive minor versions"; there is "no guarantee … between _any_ two non-consecutive versions". Allow about 10 minutes per version for History to load shards. — [docs: Upgrade server](https://docs.temporal.io/self-hosted-guide/upgrade-server)
- Production checklist repeats: "upgrading sequentially, not skipping any minor versions, although you can skip patch versions". — [docs: Production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist)
- Tools: temporal-sql-tool (PG/MySQL), temporal-cassandra-tool, temporal-elasticsearch-tool. — [docs: Upgrade server](https://docs.temporal.io/self-hosted-guide/upgrade-server). `temporal-elasticsearch-tool` was added to admin-tools in 1.30.1. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1)
- 1.31.0 requires schema updates before upgrading: core MySQL/PG v1.19, SQLite v1.11; visibility ES v14, MySQL/PG v1.14. — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0)
- 1.30.1: "The following images are deprecated and will no longer receive updates: `temporalio/auto-setup` … For local development, we recommend using the CLI dev server." The base-* images were also deprecated. Images were slimmed for security, and dockerize was replaced by embedded sprig templating. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1)
- 1.20.0 notes: "For SQLite users, we do not support schema updates." — [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0)
- Examples of version-specific upgrade traps: 1.24 needs the visibility config and plugin rename; 1.28 needs 1.27.x Worker Deployments deleted first; 1.30.1 rollback must target 1.29.3 or newer if `SetWorkerDeploymentCurrentVersion` was used; 1.33 will remove legacy versioning, so pinned legacy workflows must be drained first. — [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0), [v1.28.0](https://github.com/temporalio/temporal/releases/tag/v1.28.0), [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1), [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0)

### Inferences
- A team that falls behind (e.g., 1.22 → 1.32) faces about 10 sequential hops, each with its own schema bump and release-note review. Regular upgrades are mandatory operational work, not optional.

### Gaps
- No official support window or LTS policy was found. Patch backports appeared for 1.29/1.30/1.31 concurrently in 2026, which suggests about 3 supported minors, but this is inferred.

## Security in OSS

### Takeaway
OSS has mTLS, pluggable ClaimMapper/Authorizer with a default JWT mapper (namespace-scoped permissions), the internal-frontend role (1.20), UI OIDC SSO, and Principal Attribution (1.31). The default is still a no-op authorizer that allows every request. No built-in API-key or user management was found in OSS.

### Cited Findings
- mTLS for internode and client traffic is supported. "If you do **not** explicitly configure an Authorizer, Temporal uses the default noopAuthorizer. This default allows **every** API request". The default JWT ClaimMapper uses permissions of the form "namespace1:write". UI SSO is configured via `TEMPORAL_AUTH_ENABLED`, `TEMPORAL_AUTH_CLIENT_ID`, `..._CLIENT_SECRET`, `..._PROVIDER_URL`, and `..._CALLBACK_URL`. The security page does not mention API keys. — [docs: Security](https://docs.temporal.io/self-hosted-guide/security)
- History: ClaimMapper/Authorizer added in 1.4.0 (Dec 2020); config-selectable in 1.8.0 (Mar 2021); internal-frontend in 1.20.0 (Feb 2023), described as "makes the system worker service and frontend work like the other 'internode' services"; JWT system role changed to `temporal-system:<role>` in 1.21.0 (breaking). — [v1.4.0](https://github.com/temporalio/temporal/releases/tag/v1.4.0), [v1.8.0](https://github.com/temporalio/temporal/releases/tag/v1.8.0), [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0), [v1.21.0](https://github.com/temporalio/temporal/releases/tag/v1.21.0)
- 1.31.0 Principal Attribution: "a server-computed, immutable `Principal` field to workflow history events". The default Authorizer fills it from the JWT `sub` claim. Enabled with `system.enablePrincipalAttribution`. — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0)
- 1.30.1 changed `system.enableCrossNamespaceCommands` default from true to false. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1)
- 1.31.2 / 1.30.6 added a streaming authorizer for replication to mitigate CVE-2026-5724. — [releases](https://github.com/temporalio/temporal/releases)

### Inferences
- A secure OSS deployment still means operating your own IdP-issued JWTs (or a custom ClaimMapper) plus certificate management. Principal Attribution is the nearest OSS feature to an audit trail; it is per-workflow history, not an account-level audit log.

### Gaps
- No primary source confirms or denies API keys in OSS. The security doc is silent, which suggests they are absent.

## Dynamic config, numHistoryShards, small-cluster defaults

### Takeaway
numHistoryShards is still fixed at cluster creation. Dynamic config remains the main tuning and feature-flag surface; many new features ship behind flags and later become default-on.

### Cited Findings
- "You set Shard capacity, and often overall Temporal Service throughput, at build time and can't adjust it later." "Temporal server throughput is often limited by the number of Shards configured." — [docs: Production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist)
- The Helm UPGRADING example uses `numHistoryShards: 512`. — [UPGRADING.md](https://github.com/temporalio/helm-charts/blob/main/UPGRADING.md)
- Feature flags gated by dynamic config, e.g. `system.enableDeployments` (1.27), `matching.enableFairness` (1.29), `system.visibilityEnableUnifiedQueryConverter`, `activity.enableStandalone`, `system.enablePrincipalAttribution`, and `matching.newUseMatcher` (1.31). — [v1.27.1](https://github.com/temporalio/temporal/releases/tag/v1.27.1), [v1.29.0](https://github.com/temporalio/temporal/releases/tag/v1.29.0), [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0), [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0)
- 1.29.0 added a per-task-queue `UpdateTaskQueueConfig` RPS API, configurable via the CLI. — [v1.29.0](https://github.com/temporalio/temporal/releases/tag/v1.29.0)

### Inferences
- The fixed shard count is still a design-time decision with no migration path short of a new cluster (or multi-cluster replication).

### Gaps
- No current official small-cluster numHistoryShards recommendation (e.g., 128/512) was found in the checklist page.

## Observability

### Takeaway
Prometheus metrics have been built in since 1.0, configured under `global.metrics` since 1.3. The official dashboards live in temporalio/dashboards. The checklist names a short set of key metrics. Helm no longer bundles Prometheus or Grafana but supports a ServiceMonitor.

### Cited Findings
- 1.3.0 moved metrics to `global.metrics` ("one Prometheus listener … per process"). 1.17 added `service_error_with_type` and `persistence_error_with_type`. 1.29 deprecated `activity_e2e_latency`. 1.28 started the Tally→OTel migration for statsd. — [v1.3.0](https://github.com/temporalio/temporal/releases/tag/v1.3.0), [v1.17.0](https://github.com/temporalio/temporal/releases/tag/v1.17.0), [v1.29.0](https://github.com/temporalio/temporal/releases/tag/v1.29.0), [v1.28.0](https://github.com/temporalio/temporal/releases/tag/v1.28.0)
- Key metrics to monitor: `service_requests`, `service_errors`, `service_latency`; `persistence_requests`, `persistence_errors`, `persistence_latency`; `workflow_success`, `workflow_failed`, `workflow_timeout`, `workflow_terminate`, `workflow_cancel`. — [docs: Production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist)
- temporalio/dashboards repo (created 2020-06-05, last push 2026-08-17) has `server`, `sdk`, `cloud`, and `misc` folders. — [temporalio/dashboards](https://github.com/temporalio/dashboards)
- Chart 1.0 removed the Prometheus and Grafana sub-charts. ServiceMonitor support via `server.metrics.serviceMonitor.enabled`. — [UPGRADING.md](https://github.com/temporalio/helm-charts/blob/main/UPGRADING.md)
- 1.32 removed the `tasks_expired` metric (replaced by `tasks_dropped`), so metric names keep changing between minors. — [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0)

### Gaps
- No official alerting-rule set (Prometheus alert rules with thresholds) was found.

## Docs: self-hosted guide and production checklist

### Takeaway
The self-hosted guide (docs.temporal.io/self-hosted-guide/*) now has dedicated pages for deployment, upgrade, visibility, security, and a production checklist. The guide is still conceptual and partly stale: the upgrade page lists PG 9.6+ / MySQL 5.7+.

### Cited Findings
- The production checklist covers shards, load testing (Omes), a "99.99%" availability target for mission-critical deployments, key metrics, and the upgrade policy. — [docs: Production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist)
- Stale version info on the upgrade page (PG 9.6+, MySQL 5.7+) contradicts the 1.20/1.24 release notes. — [docs: Upgrade server](https://docs.temporal.io/self-hosted-guide/upgrade-server), [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0)

### Gaps
- The date and scope of the self-hosted guide rewrite were not established; no announcement was found within this research budget.

## Single-binary / small deployments

### Takeaway
There is no supported single-binary production mode. The server binary has run with SQLite since 1.17 (`--env development-sqlite`), but that is for development. Temporalite, the experimental single process, was archived in 2024 in favor of `temporal server start-dev` (CLI). auto-setup, the old "quick start" container, was deprecated in 1.30.

### Cited Findings
- 1.17.0: pure-Go SQLite, "you can use binaries attached to this release to run local server for development". — [v1.17.0](https://github.com/temporalio/temporal/releases/tag/v1.17.0)
- SQLite: "we do not support schema updates. … you'll need to reset your database from scratch." — [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0)
- Temporalite archived: "An experimental distribution of Temporal that runs as a single process". — [temporalio/temporalite](https://github.com/temporalio/temporalite)
- auto-setup deprecated; "For local development, we recommend using the CLI dev server." Docker Compose examples moved to temporalio/samples-server. — [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1)

### Inferences
- The minimal production footprint is still: the server (all four roles can run in one process) plus a real SQL database plus your own schema management. SQLite cannot be upgraded, which rules it out for production.

### Gaps
- No doc found that explicitly blesses running all services in one process in production (it is technically possible).

## Worker-side ops features (Worker Versioning/Deployments, Nexus, others)

### Takeaway
Worker Versioning took about three years and three API generations: preview in 1.21 (Jun 2023), rules in 1.24, Deployments in 1.26/1.27, Public Preview in 1.28 (Jun 2025), and **GA in 1.31.0 (2026-04-29)**. Legacy APIs are removed in 1.33. Nexus went GA in 1.27 (Feb 2025). Recent ops-friendly additions are priority/fairness GA (1.31), Standalone Activities GA (1.32), and a Serverless Workers preview (1.31).

### Cited Findings
- Worker Versioning (preview) in 1.21.0, requiring advanced visibility and dynamic config flags. — [v1.21.0](https://github.com/temporalio/temporal/releases/tag/v1.21.0)
- 1.24 deprecated version sets in favor of versioning rules. — [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0)
- 1.28.0: "Worker Versioning APIs graduated into Public Preview … Production usage is encouraged". — [v1.28.0](https://github.com/temporalio/temporal/releases/tag/v1.28.0)
- 1.31.0: "Worker Deployment APIs are now fully GA … users can rely on the signature and behavior consistency going forward." — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0)
- 1.32.0: final release with the deprecated implementations; "Complete removal is now planned for server `v1.33`". — [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0)
- Nexus: enabled by default in 1.26 (still public preview); "Nexus is now GA with a stable server API" in 1.27 (1.27.1, 2025-02-26). — [v1.26.2](https://github.com/temporalio/temporal/releases/tag/v1.26.2), [v1.27.1](https://github.com/temporalio/temporal/releases/tag/v1.27.1)
- 1.31.0: Task Queue Priority/Fairness GA (new matcher default on; revert with `matching.newUseMatcher: false`). Serverless Workers preview using a Worker Controller for AWS Lambda. — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0)
- 1.32.0: Standalone Activities GA, Activity Eager Execution on by default, and Serverless Workers support for AWS Bedrock AgentCore. — [v1.32.0](https://github.com/temporalio/temporal/releases/tag/v1.32.0)

### Gaps
- Not verified whether Serverless Workers in OSS needs any Cloud component.

## Cloud-only capabilities that matter for ops

### Takeaway
API keys with RBAC, account-level audit logs, and managed same-region, multi-region, and multi-cloud replication with SLAs are Cloud features. OSS has asynchronous multi-cluster replication that you configure and operate yourself (the new temporal-proxy chart helps). Evidence here is weaker, partly from a search summary.

### Cited Findings
- Temporal Cloud offers API key authentication with RBAC, Audit Logging (via Kinesis, and for GCP customers), and replication options: same-region (across cells), multi-region, and multi-cloud. Self-hosted offers asynchronous Multi-Cluster Replication from active to passive clusters. This comes from a search-engine summary of docs pages, not directly fetched; treat it as moderately reliable. — [docs: Cloud overview](https://docs.temporal.io/cloud/overview), [docs: Security controls](https://docs.temporal.io/best-practices/security-controls.md)
- The OSS security page does not mention API keys. — [docs: Security](https://docs.temporal.io/self-hosted-guide/security)
- The OSS `temporal-proxy` chart (Jul 2026) handles "routing between clusters, namespace translation, encryption". — [helm-charts releases](https://github.com/temporalio/helm-charts/releases)

### Inferences
- For a single-site internal deployment, the Cloud-only gaps that matter most are user and API-key management plus audit logs, not multi-region. OSS teams fill these gaps with an IdP, JWTs, and a custom ClaimMapper.

### Gaps
- Not directly verified: SCIM, SAML, or the Cloud metrics endpoint in OSS; Cloud's SLA numbers; whether the 1.31 Principal Attribution feature is pitched as an OSS audit-log substitute.

## Pain-point scorecard (Oct 2026)

### Takeaway
Resolved: Elasticsearch requirement, Cassandra requirement, Kafka, unstable Helm values, auto-setup confusion, Worker Versioning instability. Mitigated: secrets/IAM DB auth, Helm schema jobs, image attack surface, security plumbing. Still present: sequential upgrades with schema bumps, fixed shards, no official operator, no supported single-binary production mode, no OSS API keys or audit log, and docs drift.

### Cited Findings
- Resolved: SQL advanced visibility (1.20), with standard visibility removed (1.24). — [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0), [v1.24.0](https://github.com/temporalio/temporal/releases/tag/v1.24.0)
- Resolved: Kafka dependency removed (1.5/1.6). — [v1.6.0](https://github.com/temporalio/temporal/releases/tag/v1.6.0)
- Resolved: Helm stability and the bundled-dependency confusion (chart 1.0.0, 2026-04-08). — [blog](https://temporal.io/blog/an-important-milestone-for-temporals-helm-charts)
- Resolved: Worker Versioning churn (GA 1.31). — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0)
- Mitigated: IAM DB auth (`passwordCommand`, 1.31), Helm `existingSecret`, slim images (1.30), internal-frontend (1.20), Principal Attribution (1.31). — [v1.31.0](https://github.com/temporalio/temporal/releases/tag/v1.31.0), [v1.30.1](https://github.com/temporalio/temporal/releases/tag/v1.30.1), [v1.20.0](https://github.com/temporalio/temporal/releases/tag/v1.20.0)
- Still present: the one-minor-at-a-time rule and immutable shards. — [docs: Upgrade](https://docs.temporal.io/self-hosted-guide/upgrade-server), [docs: Production checklist](https://docs.temporal.io/self-hosted-guide/production-checklist)
- Still present: no official operator; the community operator only documents support through 1.25. — [temporal-operator](https://github.com/alexandrevilain/temporal-operator)

### Inferences
- Since 2024, the friction has moved from heavy dependencies to keeping up with releases: frequent minors, each with schema bumps, breaking flags, and occasionally a bad `.0`.

### Gaps
- Not quantified: how much ops time an upgrade typically takes in practice; this needs community or user reports, which are out of scope here.
