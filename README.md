# Brokara

Brokara is an open-source Kafka operations cockpit with broker-local collectors and built-in advisor agents.

It is designed for platform teams that want safe self-service, cluster visibility, operational guardrails, and a path toward GitOps-style Kafka governance.

## What Exists Now

- A TypeScript API that connects to Kafka through KafkaJS.
- A React console for cluster overview, topics, messages, consumer groups, collectors, audit, and advisor agents.
- One broker-local collector per broker in the Docker lab.
- Rules-based advisor agents for UX, security, SRE, governance, and maintainership.
- A three-broker Kafka lab in Docker Compose.

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

## Advisor Agents

Brokara ships with deterministic advisor agents before it grows into model-backed automation:

- **Navigator**: UX and workflow quality.
- **Sentinel**: security posture and unsafe defaults.
- **Operator**: collector freshness and operational health.
- **Steward**: topic governance and policy hygiene.
- **Scribe**: project readiness, docs, and release quality.

Current routes:

- `GET /api/agents`
- `GET /api/clusters/:clusterId/agents`
- `POST /api/clusters/:clusterId/agents/run`

## Security Status

This repository is still an early local-development platform. The Docker lab is intentionally easy to run, not production hardened.

Current protections:

- Host-published Docker ports bind to `127.0.0.1`.
- Collectors support a shared `BROKARA_COLLECTOR_TOKEN`.
- The API supports `BROKARA_AUTH_MODE=token` with `BROKARA_API_TOKEN`.
- Mutating requests support explicit confirmation headers.

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
