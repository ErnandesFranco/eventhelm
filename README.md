# Open Kafka Control Plane

Open Kafka Control Plane is an open-source platform for managing Kafka clusters with a broker-local collector model.

The first slice includes:

- A TypeScript API that connects to Kafka through KafkaJS.
- A React console for clusters, topics, consumer groups, messages, collectors, and audit events.
- A broker collector agent that heartbeats and pushes cluster snapshots to the control plane.
- A three-broker local Kafka lab in Docker Compose with one collector per broker.

## Quick Start

```bash
npm install
npm run compose:up
```

Then open:

- Web console: http://localhost:15173
- API health: http://localhost:18080/health

The Docker lab exposes Kafka brokers on:

- `localhost:19092`
- `localhost:29092`
- `localhost:39092`

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
apps/web        React management console
apps/collector  Broker-local collector agent
docs/           Architecture notes and roadmap
```

## Collector Model

Collectors are designed to run next to brokers. In Docker Compose this is modeled as one collector container per broker. In Kubernetes this maps naturally to sidecars or DaemonSets. For bare metal or VM-based Kafka clusters, the same agent can run as a systemd service on each broker host.

The API is intentionally push-based so broker networks do not have to expose collector ports back to the control plane.

## License

Apache-2.0.
