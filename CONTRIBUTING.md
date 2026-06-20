# Contributing

Thanks for helping shape EventHelm.

## Development

```bash
npm install
npm run typecheck --workspaces
npm run build --workspaces
npm run compose:up
```

## Priorities

The project currently needs help with:

- Postgres persistence for configured clusters and production deployment metadata.
- Versioned database migrations and retention policies for persisted control-plane state.
- Approval queues for safe mutation workflows.
- OIDC/RBAC and policy checks.
- Collector JMX metrics.
- UX polish for safe mutation workflows.

Keep pull requests focused and include the checks you ran.
