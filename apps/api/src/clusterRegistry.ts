import { persistenceMode, query } from "./db.js";
import type { ClusterConfig } from "./types.js";

export type ClusterSource = "environment" | "api";

export type ClusterRecord = ClusterConfig & {
  source: ClusterSource;
  createdAt: string;
  updatedAt: string;
};

export type PublicCluster = {
  id: string;
  name: string;
  brokers: string[];
  ssl?: boolean;
  saslConfigured: boolean;
  saslPasswordSource?: "inline" | "environment";
  source: ClusterSource;
  createdAt?: string;
  updatedAt?: string;
};

type ClusterRow = {
  id: string;
  name: string;
  brokers: string[];
  ssl: boolean;
  sasl: ClusterConfig["sasl"] | null;
  source: ClusterSource;
  created_at: Date;
  updated_at: Date;
};

const memoryClusters = new Map<string, ClusterRecord>();

export async function initializeClusterRegistry(seedClusters: ClusterConfig[]): Promise<ClusterRecord[]> {
  if (persistenceMode() === "postgres") {
    for (const cluster of seedClusters) {
      await upsertClusterConfig(cluster, "environment");
    }
    return listClusterConfigs();
  }

  memoryClusters.clear();
  for (const cluster of seedClusters) {
    await upsertClusterConfig(cluster, "environment");
  }
  return listClusterConfigs();
}

export async function listClusterConfigs(): Promise<ClusterRecord[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterRow>(
      `select id, name, brokers, ssl, sasl, source, created_at, updated_at
       from cluster_configs
       order by name asc`
    );
    return result.rows.map(rowToClusterRecord);
  }

  return [...memoryClusters.values()].sort((left, right) => left.name.localeCompare(right.name));
}

export async function upsertClusterConfig(cluster: ClusterConfig, source: ClusterSource): Promise<ClusterRecord> {
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterRow>(
      `insert into cluster_configs (id, name, brokers, ssl, sasl, source, updated_at)
       values ($1, $2, $3, $4, $5, $6, now())
       on conflict (id) do update set
        name = excluded.name,
        brokers = excluded.brokers,
        ssl = excluded.ssl,
        sasl = excluded.sasl,
        source = excluded.source,
        updated_at = now()
       returning id, name, brokers, ssl, sasl, source, created_at, updated_at`,
      [cluster.id, cluster.name, cluster.brokers, cluster.ssl ?? false, cluster.sasl ?? null, source]
    );
    return rowToClusterRecord(result.rows[0]);
  }

  const now = new Date().toISOString();
  const existing = memoryClusters.get(cluster.id);
  const record: ClusterRecord = {
    ...cluster,
    source,
    createdAt: existing?.createdAt ?? now,
    updatedAt: now
  };
  memoryClusters.set(cluster.id, record);
  return record;
}

export async function deleteClusterConfig(clusterId: string): Promise<ClusterRecord | undefined> {
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterRow>(
      `delete from cluster_configs
       where id = $1
       returning id, name, brokers, ssl, sasl, source, created_at, updated_at`,
      [clusterId]
    );
    return result.rows[0] ? rowToClusterRecord(result.rows[0]) : undefined;
  }

  const record = memoryClusters.get(clusterId);
  memoryClusters.delete(clusterId);
  return record;
}

export function toPublicCluster(cluster: ClusterConfig | ClusterRecord): PublicCluster {
  const record = cluster as Partial<ClusterRecord>;
  return {
    id: cluster.id,
    name: cluster.name,
    brokers: cluster.brokers,
    ssl: cluster.ssl,
    saslConfigured: Boolean(cluster.sasl),
    saslPasswordSource: cluster.sasl ? saslPasswordSource(cluster.sasl) : undefined,
    source: record.source ?? "environment",
    createdAt: record.createdAt,
    updatedAt: record.updatedAt
  };
}

export function sanitizeClusterConfig(cluster: ClusterConfig) {
  return {
    id: cluster.id,
    name: cluster.name,
    brokers: cluster.brokers,
    ssl: cluster.ssl ?? false,
    saslConfigured: Boolean(cluster.sasl),
    saslPasswordSource: cluster.sasl ? saslPasswordSource(cluster.sasl) : undefined
  };
}

function saslPasswordSource(sasl: NonNullable<ClusterConfig["sasl"]>): "inline" | "environment" {
  return sasl.passwordEnv ? "environment" : "inline";
}

function rowToClusterRecord(row: ClusterRow): ClusterRecord {
  return {
    id: row.id,
    name: row.name,
    brokers: row.brokers,
    ssl: row.ssl,
    sasl: row.sasl ?? undefined,
    source: row.source,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString()
  };
}
