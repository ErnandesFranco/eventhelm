# EventHelm

EventHelm is an open-source event-streaming operations cockpit for Kafka, broker-local collectors, and built-in advisor agents.

It is designed for platform teams that want safe self-service, cluster visibility, operational guardrails, and a path toward GitOps-style Kafka governance.

## What Exists Now

- A TypeScript API that connects to Kafka through KafkaJS.
- A React console for command overview, live topology, topics, records, consumer groups, collectors, audit, and advisor agents.
- Postgres persistence for cluster configs, audit events, broker collector state, rebalance plans, and advisor-agent run history.
- One broker-local collector per broker in the Docker lab, reporting disk, partition byte, and host pressure telemetry.
- Topic configuration inspection and reviewed updates for common mutable topic configs.
- Disk-aware partition rebalance planning from broker collector telemetry.
- Consumer group lag summaries and topic/partition offset drill-downs.
- Reviewed consumer group offset resets with live Kafka bounds, stale-preview protection, and audit events.
- Rules-based advisor agents for UX, security, SRE, governance, and maintainership.
- A ten-broker Kafka lab in Docker Compose.

## Quick Start

```bash
npm install
npm run compose:up
```

Then open:

- Console: http://localhost:15173
- API health: http://localhost:18080/health
- Postgres: `localhost:15432`

The Docker lab exposes Kafka brokers only on localhost:

- `localhost:19092`
- `localhost:29092`
- `localhost:39092`
- `localhost:49092`
- `localhost:59092`
- `localhost:60092`
- `localhost:61092`
- `localhost:62092`
- `localhost:63092`
- `localhost:64092`

## Advisor Agents

EventHelm ships with deterministic advisor agents before it grows into model-backed automation:

- **Navigator**: UX and workflow quality.
- **Sentinel**: security posture and unsafe defaults.
- **Operator**: collector freshness and operational health.
- **Steward**: topic governance and policy hygiene.
- **Scribe**: project readiness, docs, and release quality.

Current routes:

- `GET /api/agents`
- `GET /api/clusters/:clusterId/agents`
- `GET /api/clusters/:clusterId/agents/runs`
- `GET /api/clusters/:clusterId/agents/runs/:runId`
- `POST /api/clusters/:clusterId/agents/run`

Every sweep returns a run ID, severity summary, per-agent scores, and finding evidence. Sweeps include sanitized cluster registry data, cluster change reviews, rebalance plan history, in-flight rebalance execution state, collector coverage, audit activity, and security settings. `GET /api/clusters/:clusterId/agents` returns an ephemeral read-side sweep; `POST /api/clusters/:clusterId/agents/run` persists a manual sweep. In Postgres mode, persisted runs are stored in `agent_runs` and findings are indexed in `agent_findings` so operators can review posture drift over time.

## Cluster Registry

EventHelm bootstraps clusters from `EVENTHELM_CLUSTERS_JSON` and stores them in Postgres when persistence is enabled:

- `GET /api/clusters` returns safe cluster metadata, including brokers, source, and whether SASL is configured.
- `POST /api/clusters` and `DELETE /api/clusters/:clusterId` are break-glass direct mutation routes. They require `EVENTHELM_ENABLE_CLUSTER_BREAKGLASS=true` and `cluster:breakglass` or `admin` scope.
- `GET /api/clusters/reviews` lists retained cluster change reviews.
- `POST /api/clusters/reviews` creates an upsert or delete review with sanitized current/proposed metadata.
- `POST /api/clusters/reviews/:reviewId/approve`, `/reject`, and `/apply` move a reviewed cluster change through decision and execution.
- Apply rejects stale reviews when the cluster registry state changed after the review was created.
- Cluster read responses and audit records do not expose SASL passwords.
- SASL registrations can use `passwordEnv` to reference an API-process environment variable instead of storing a raw password.
- In token auth mode, inline SASL passwords are rejected; use `passwordEnv` for Kafka credentials.
- The console Clusters view can inspect, select, and submit reviewed registrations or removals for API-managed clusters without exposing credentials.

## Consumer Lag

EventHelm calculates consumer lag with Kafka committed offsets and topic log-end offsets:

- `GET /api/clusters/:clusterId/consumer-groups` includes lag totals, topic count, partition count, and unknown offsets per group.
- `GET /api/clusters/:clusterId/consumer-groups/:groupId/lag` returns topic and partition offset details for drill-downs.
- The Operator advisor flags consumer groups with active lag or unknown committed offsets.

## Topic Configs

EventHelm reads topic configs directly from Kafka and supports reviewed updates for an allowlist of common mutable configs:

- `GET /api/clusters/:clusterId/topics/:topic/config` returns config values, source, default status, sensitivity, and read-only flags.
- `POST /api/clusters/:clusterId/topics/:topic/config/preview` returns effective changes, warnings, and a review token.
- `POST /api/clusters/:clusterId/topics/:topic/config/apply` requires the review token and write confirmation headers.
- Execution preserves existing dynamic topic overrides, validates with Kafka, applies the reviewed changes, and verifies the requested values are visible before returning success.
- Current editable configs are `cleanup.policy`, `retention.ms`, `delete.retention.ms`, `min.insync.replicas`, `max.message.bytes`, and `segment.bytes`.

## Offset Reset

EventHelm treats consumer offset resets as a reviewed operation:

- `POST /api/clusters/:clusterId/consumer-groups/:groupId/offset-reset/preview` returns current offsets, low/high log bounds, proposed offsets, warnings, projected lag, and a review token.
- `POST /api/clusters/:clusterId/consumer-groups/:groupId/offset-reset/execute` requires the reviewed token and write confirmation headers.
- Execution recomputes the preview and rejects stale tokens when live offsets, group state, or requested targets changed.
- KafkaJS requires the consumer group to be stopped; EventHelm surfaces running groups as non-executable previews.
- Successful resets are recorded as `consumer.offset_reset` audit events.

## Partition Rebalance

EventHelm includes a disk-pressure rebalance planner:

- Collectors can mount broker log data at `BROKER_DATA_PATH` and report disk pressure.
- Collectors scan broker log directories and report per-partition byte sizes for movement estimates.
- Collectors also report host CPU count, load averages, memory pressure, and uptime.
- Collector heartbeat and snapshot writes ignore stale `observedAt` samples so older telemetry cannot overwrite fresher broker state.
- `POST /api/clusters/:clusterId/rebalance/plan` generates a Kafka reassignment payload.
- `GET /api/clusters/:clusterId/rebalance/plans` lists retained plan summaries.
- `GET /api/clusters/:clusterId/rebalance/plans/:planId` returns the full stored plan for review or reload.
- `GET /api/clusters/:clusterId/rebalance/plans/:planId/preflight` checks the approved plan against live Kafka state and collector freshness.
- `GET /api/clusters/:clusterId/rebalance/status` reports active Kafka partition reassignments.
- `POST /api/clusters/:clusterId/rebalance/plans/:planId/approve` and `/reject` record a reviewed decision.
- Generated plans are persisted with a plan ID before operators can apply them.
- The planner preserves replication factor, skips under-replicated partitions, prefers follower replica moves before leader moves, and scores targets by projected disk usage.
- Execution requires an approved stored plan.
- Execution is rejected while Kafka reports an active partition reassignment.
- Execution runs the same preflight gate and rejects stale plans, degraded planned partitions, missing live brokers, missing movement byte estimates, missing or stale broker disk telemetry, inactive execution switches, and current replica placement drift before calling Kafka.
- Execution moves approved plans to `executing`; EventHelm marks them `executed` only after Kafka reports no active reassignment and the proposed replica placement is visible.
- Execution is locked unless `EVENTHELM_ENABLE_REBALANCE_EXECUTION=true` is set.

## Audit

The audit ledger supports investigation filters:

- `GET /api/audit` returns recent events.
- Query parameters include `clusterId`, `actor`, `action`, `resourceType`, `resourceName`, `query`, and `limit`.
- The console Audit view includes search, action, actor, and resource filters for operator triage.

## Security Status

This repository is still an early local-development platform. The Docker lab is intentionally easy to run, not production hardened.

Current protections:

- Host-published Docker ports bind to `127.0.0.1`.
- Collectors support a shared `EVENTHELM_COLLECTOR_TOKEN`.
- In token auth mode, `EVENTHELM_COLLECTOR_TOKEN` is required for collector heartbeat and snapshot writes.
- The API supports `EVENTHELM_AUTH_MODE=token` with `EVENTHELM_API_TOKEN`.
- The API also supports scoped tokens through `EVENTHELM_API_TOKENS_JSON`; scopes include `read`, `cluster:write`, `cluster:breakglass`, `topic:write`, `message:write`, `consumer:write`, `rebalance:plan`, `rebalance:review`, `rebalance:execute`, `agent:run`, `write`, and `admin`.
- In token mode, audit actors and write-rate principals are derived from the authenticated token instead of caller-supplied actor headers.
- In token mode, cluster-review and rebalance workflows enforce separation of duties between requester/planner, reviewer, and apply/execution actors.
- In token mode, read auth is required by default unless `EVENTHELM_REQUIRE_READ_AUTH=false` is set.
- Mutating endpoints support per-actor/per-scope write rate limits with `EVENTHELM_WRITE_RATE_LIMIT_PER_MINUTE`.
- Cluster configs, audit events, collector state, rebalance plans, and advisor-agent runs are persisted in Postgres in the Docker lab.
- The API tracks applied database migrations in `schema_migrations` and exposes schema status from `/health`.
- Mutating requests support explicit confirmation headers.
- Topic config updates require preview tokens and post-apply verification.
- Consumer offset resets require preview tokens and reject stale live state.
- Direct cluster registry mutations are locked by default with `EVENTHELM_ENABLE_CLUSTER_BREAKGLASS=false`; use the cluster change review queue for normal operations.
- Partition reassignment execution is locked by default with `EVENTHELM_ENABLE_REBALANCE_EXECUTION=false`.
- Rebalance execution also requires a reviewed approval decision and passing live preflight on the stored plan.

Still required before shared or production use:

- OIDC/JWT user auth and per-user RBAC integration.
- Retention policies and backup guidance for persisted control-plane state.
- Approval workflows for production mutations.
- TLS/SASL examples and external secret references.
- Distributed rate limits and per-user quotas.

## Local Development

Run the API and web app against an existing Kafka cluster:

```bash
cp .env.example .env
npm install
npm run dev
```

## Project Gates

The GitHub Actions workflow runs on pushes to `main` and all pull requests:

- `npm ci`
- `npm test`
- `npm run typecheck --workspaces`
- `npm run build --workspaces`
- `docker compose config --quiet`

## Project Layout

```text
apps/api        Control plane API
apps/web        React operations console
apps/collector  Broker-local collector agent
docs/           Architecture notes and roadmap
```

## License

Apache-2.0.
