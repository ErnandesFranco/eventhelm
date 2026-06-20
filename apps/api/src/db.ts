import { createHash } from "node:crypto";
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

type DatabaseMigration = {
  id: string;
  name: string;
  sql: string;
};

type AppliedMigrationRow = {
  id: string;
  checksum: string;
};

const migrations: DatabaseMigration[] = [
  {
    id: "001_control_plane_foundation",
    name: "Control-plane persistence foundation",
    sql: `
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

      create table if not exists rebalance_plans (
        id text primary key,
        cluster_id text not null,
        actor text not null,
        status text not null,
        plan jsonb not null,
        created_at timestamptz not null default now(),
        executed_at timestamptz
      );

      create index if not exists rebalance_plans_cluster_id_idx on rebalance_plans (cluster_id);
      create index if not exists rebalance_plans_created_at_idx on rebalance_plans (created_at desc);
      create index if not exists rebalance_plans_status_idx on rebalance_plans (status);

      create table if not exists agent_runs (
        id text primary key,
        cluster_id text not null,
        actor text not null,
        trigger text not null,
        run jsonb not null,
        summary jsonb not null,
        generated_at timestamptz not null,
        created_at timestamptz not null default now()
      );

      create index if not exists agent_runs_cluster_id_created_at_idx on agent_runs (cluster_id, created_at desc);
      create index if not exists agent_runs_trigger_idx on agent_runs (trigger);

      create table if not exists agent_findings (
        id text primary key,
        run_id text not null references agent_runs(id) on delete cascade,
        cluster_id text not null,
        agent_id text not null,
        severity text not null,
        resource_type text,
        resource_name text,
        finding jsonb not null,
        created_at timestamptz not null default now()
      );

      create index if not exists agent_findings_run_id_idx on agent_findings (run_id);
      create index if not exists agent_findings_cluster_severity_idx on agent_findings (cluster_id, severity);
      create index if not exists agent_findings_resource_idx on agent_findings (resource_type, resource_name);
    `
  }
];

export function persistenceMode(): "postgres" | "memory" {
  return pool ? "postgres" : "memory";
}

export async function initDatabase() {
  if (!pool) {
    return;
  }

  await ensureMigrationTable();
  for (const migration of migrations) {
    await applyMigration(migration);
  }
}

export function listDatabaseMigrations() {
  return migrations.map((migration) => ({
    id: migration.id,
    name: migration.name,
    checksum: checksumMigration(migration)
  }));
}

export async function databaseSchemaStatus() {
  if (!pool) {
    return {
      mode: "memory" as const,
      latestMigration: undefined,
      appliedMigrations: 0,
      pendingMigrations: 0
    };
  }

  const applied = await listAppliedMigrationIds();
  return {
    mode: "postgres" as const,
    latestMigration: migrations.at(-1)?.id,
    appliedMigrations: applied.length,
    pendingMigrations: Math.max(0, migrations.length - applied.length)
  };
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

async function ensureMigrationTable() {
  await pool?.query(`
    create table if not exists schema_migrations (
      id text primary key,
      name text not null,
      checksum text not null,
      applied_at timestamptz not null default now()
    )
  `);
}

async function applyMigration(migration: DatabaseMigration) {
  const checksum = checksumMigration(migration);
  const applied = await pool?.query<AppliedMigrationRow>("select id, checksum from schema_migrations where id = $1", [
    migration.id
  ]);
  const appliedRow = applied?.rows[0];

  if (appliedRow) {
    if (appliedRow.checksum !== checksum) {
      throw new Error(
        `Database migration ${migration.id} checksum changed after it was applied. Create a new migration instead of editing history.`
      );
    }
    return;
  }

  const client = await pool?.connect();
  if (!client) {
    return;
  }

  try {
    await client.query("begin");
    await client.query(migration.sql);
    await client.query(
      `insert into schema_migrations (id, name, checksum)
       values ($1, $2, $3)`,
      [migration.id, migration.name, checksum]
    );
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}

async function listAppliedMigrationIds(): Promise<string[]> {
  const result = await query<{ id: string }>("select id from schema_migrations order by id asc");
  return result.rows.map((row) => row.id);
}

function checksumMigration(migration: DatabaseMigration): string {
  return createHash("sha256").update(migration.sql.trim()).digest("hex");
}
