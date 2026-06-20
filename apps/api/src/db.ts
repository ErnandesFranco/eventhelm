import pg from "pg";
import { getDatabaseUrl } from "./config.js";

const { Pool } = pg;

const databaseUrl = getDatabaseUrl();
const pool = databaseUrl
  ? new Pool({
      connectionString: databaseUrl,
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000
    })
  : undefined;

export function persistenceMode(): "postgres" | "memory" {
  return pool ? "postgres" : "memory";
}

export async function initDatabase() {
  if (!pool) {
    return;
  }

  await pool.query(`
    create table if not exists audit_events (
      id text primary key,
      actor text not null,
      action text not null,
      cluster_id text,
      resource_type text,
      resource_name text,
      details jsonb,
      created_at timestamptz not null default now()
    );

    create index if not exists audit_events_created_at_idx on audit_events (created_at desc);
    create index if not exists audit_events_cluster_id_idx on audit_events (cluster_id);

    create table if not exists collector_states (
      collector_id text primary key,
      cluster_id text not null,
      broker_id text not null,
      heartbeat jsonb not null,
      last_snapshot jsonb,
      observed_at timestamptz not null,
      updated_at timestamptz not null default now()
    );

    create index if not exists collector_states_cluster_id_idx on collector_states (cluster_id);
    create index if not exists collector_states_observed_at_idx on collector_states (observed_at desc);
  `);
}

export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(text: string, values: unknown[] = []) {
  if (!pool) {
    throw new Error("Postgres is not configured for this EventHelm process.");
  }
  return pool.query<T>(text, values);
}

export async function closeDatabase() {
  await pool?.end();
}
