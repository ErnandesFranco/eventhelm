# Security Policy

EventHelm is early-stage software. Treat the current Docker Compose stack as a local lab only.

## Current Security Posture

- API token mode is available with `EVENTHELM_AUTH_MODE=token` and `EVENTHELM_API_TOKEN`.
- Collector shared-token mode is available with `EVENTHELM_COLLECTOR_TOKEN`.
- Docker Compose binds host ports to `127.0.0.1`.
- Audit events are currently in-memory and not durable.

## Not Ready Yet

Do not expose EventHelm to shared or production networks until these are implemented:

- OIDC/JWT authentication.
- Role-based access control.
- Persistent audit logs.
- Collector enrollment and rotation.
- Rate limiting and request quotas.
- Approval gates for production mutations.

## Reporting Issues

Please open a private security advisory on GitHub when available, or create a minimal public issue without exploit details.
