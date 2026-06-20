# EventHelm

EventHelm is an open-source event-streaming operations cockpit for Kafka, broker-local collectors, and built-in advisor agents.

It is designed for platform teams that want safe self-service, cluster visibility, operational guardrails, and a path toward GitOps-style Kafka governance.

## What Exists Now

- A TypeScript API that connects to Kafka through KafkaJS.
- A React console for command overview, live topology, topics, records, consumer groups, collectors, audit, and advisor agents.
- One broker-local collector per broker in the Docker lab.
- Disk-aware partition rebalance planning from broker collector telemetry.
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
- `POST /api/clusters/:clusterId/agents/run`

## Partition Rebalance

EventHelm includes a disk-pressure rebalance planner:

- Collectors can mount broker log data at `BROKER_DATA_PATH` and report disk pressure.
- `POST /api/clusters/:clusterId/rebalance/plan` generates a Kafka reassignment payload.
- The planner preserves replication factor, skips under-replicated partitions, and prefers follower replica moves before leader moves.
- Execution is locked unless `EVENTHELM_ENABLE_REBALANCE_EXECUTION=true` is set.

The current planner balances replica placement against broker disk pressure. Per-partition byte sizing will be added through broker log-dir/JMX collection.

## Security Status

This repository is still an early local-development platform. The Docker lab is intentionally easy to run, not production hardened.

Current protections:

- Host-published Docker ports bind to `127.0.0.1`.
- Collectors support a shared `EVENTHELM_COLLECTOR_TOKEN`.
- The API supports `EVENTHELM_AUTH_MODE=token` with `EVENTHELM_API_TOKEN`.
- Mutating requests support explicit confirmation headers.
- Partition reassignment execution is locked by default with `EVENTHELM_ENABLE_REBALANCE_EXECUTION=false`.

Still required before shared or production use:

- OIDC/JWT user auth and RBAC.
- Persistent audit logs and collector state.
- Approval workflows for production mutations.
- TLS/SASL examples and secret references.
- Rate limits and per-user quotas.

## Local Development

Run the API and web app against an existing Kafka cluster:

```bash
cp .env.example .env
npm install
npm run dev
```

## Project Layout

```text
apps/api        Control plane API
apps/web        React operations console
apps/collector  Broker-local collector agent
docs/           Architecture notes and roadmap
```

## License

Apache-2.0.
