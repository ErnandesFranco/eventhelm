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
  },
  {
    id: "002_cluster_registry",
    name: "Persisted Kafka cluster registry",
    sql: `
      create table if not exists cluster_configs (
        id text primary key,
        name text not null,
        brokers text[] not null,
        ssl boolean not null default false,
        sasl jsonb,
        source text not null,
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now()
      );

      create index if not exists cluster_configs_source_idx on cluster_configs (source);
      create index if not exists cluster_configs_updated_at_idx on cluster_configs (updated_at desc);
    `
  },
  {
    id: "003_rebalance_plan_reviews",
    name: "Rebalance plan review decisions",
    sql: `
      alter table rebalance_plans
        add column if not exists reviewed_by text,
        add column if not exists reviewed_at timestamptz,
        add column if not exists review_comment text;

      create index if not exists rebalance_plans_reviewed_at_idx on rebalance_plans (reviewed_at desc);
    `
  },
  {
    id: "004_cluster_change_reviews",
    name: "Cluster registry change reviews",
    sql: `
      create table if not exists cluster_change_reviews (
        id text primary key,
        cluster_id text not null,
        action text not null,
        status text not null,
        actor text not null,
        request jsonb not null,
        current_cluster jsonb,
        proposed_cluster jsonb,
        warnings text[] not null default '{}',
        created_at timestamptz not null default now(),
        reviewed_by text,
        reviewed_at timestamptz,
        review_comment text,
        applied_by text,
        applied_at timestamptz
      );

      create index if not exists cluster_change_reviews_cluster_id_idx on cluster_change_reviews (cluster_id);
      create index if not exists cluster_change_reviews_status_idx on cluster_change_reviews (status);
      create index if not exists cluster_change_reviews_created_at_idx on cluster_change_reviews (created_at desc);
    `
  },
  {
    id: "005_rebalance_execution_lifecycle",
    name: "Rebalance execution lifecycle",
    sql: `
      alter table rebalance_plans
        add column if not exists execution_started_at timestamptz,
        add column if not exists execution_started_by text;

      update rebalance_plans
      set execution_started_at = executed_at
      where status = 'executed'
        and executed_at is not null
        and execution_started_at is null;

      create index if not exists rebalance_plans_execution_started_at_idx on rebalance_plans (execution_started_at desc);
      create unique index if not exists rebalance_plans_one_executing_per_cluster_idx
        on rebalance_plans (cluster_id)
        where status = 'executing';
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
