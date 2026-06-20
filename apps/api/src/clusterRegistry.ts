import { nanoid } from "nanoid";
import { persistenceMode, query } from "./db.js";
import type { ClusterConfig, SecurityMode } from "./types.js";

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

export type ClusterChangeAction = "upsert" | "delete";
export type ClusterChangeReviewStatus = "pending" | "approved" | "rejected" | "applied";

export type ClusterChangeReviewRequest =
  | {
      action: "upsert";
      cluster: ClusterConfig;
    }
  | {
      action: "delete";
      clusterId: string;
    };

export type PublicClusterChangeReview = {
  id: string;
  clusterId: string;
  action: ClusterChangeAction;
  status: ClusterChangeReviewStatus;
  actor: string;
  request: {
    action: ClusterChangeAction;
    clusterId: string;
    cluster?: ReturnType<typeof sanitizeClusterConfig>;
  };
  current?: PublicCluster;
  proposed?: PublicCluster;
  warnings: string[];
  createdAt: string;
  reviewedBy?: string;
  reviewedAt?: string;
  reviewComment?: string;
  appliedBy?: string;
  appliedAt?: string;
};

type ClusterChangeReviewRecord = Omit<PublicClusterChangeReview, "request"> & {
  request: ClusterChangeReviewRequest;
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

type ClusterChangeReviewRow = {
  id: string;
  cluster_id: string;
  action: ClusterChangeAction;
  status: ClusterChangeReviewStatus;
  actor: string;
  request: ClusterChangeReviewRequest;
  current_cluster: PublicCluster | null;
  proposed_cluster: PublicCluster | null;
  warnings: string[];
  created_at: Date;
  reviewed_by?: string;
  reviewed_at?: Date;
  review_comment?: string;
  applied_by?: string;
  applied_at?: Date;
};

const memoryClusters = new Map<string, ClusterRecord>();
const memoryClusterChangeReviews = new Map<string, ClusterChangeReviewRecord>();

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

export async function createClusterChangeReview(
  request: ClusterChangeReviewRequest,
  actor: string,
  existingClusters: ClusterRecord[]
): Promise<PublicClusterChangeReview> {
  const now = new Date().toISOString();
  const clusterId = request.action === "upsert" ? request.cluster.id : request.clusterId;
  const currentRecord = existingClusters.find((cluster) => cluster.id === clusterId);
  const current = currentRecord ? toPublicCluster(currentRecord) : undefined;
  const proposed = request.action === "upsert" ? proposedPublicCluster(request.cluster, currentRecord, now) : undefined;
  const warnings = clusterReviewWarnings(request, current);
  const record: ClusterChangeReviewRecord = {
    id: nanoid(),
    clusterId,
    action: request.action,
    status: "pending",
    actor,
    request,
    current,
    proposed,
    warnings,
    createdAt: now
  };

  if (persistenceMode() === "postgres") {
    const result = await query<ClusterChangeReviewRow>(
      `insert into cluster_change_reviews
        (id, cluster_id, action, status, actor, request, current_cluster, proposed_cluster, warnings, created_at)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       returning id, cluster_id, action, status, actor, request, current_cluster, proposed_cluster, warnings, created_at,
        reviewed_by, reviewed_at, review_comment, applied_by, applied_at`,
      [
        record.id,
        record.clusterId,
        record.action,
        record.status,
        record.actor,
        record.request,
        record.current ?? null,
        record.proposed ?? null,
        record.warnings,
        record.createdAt
      ]
    );
    return toPublicClusterChangeReview(rowToClusterChangeReviewRecord(result.rows[0]));
  }

  memoryClusterChangeReviews.set(record.id, record);
  return toPublicClusterChangeReview(record);
}

export async function listClusterChangeReviews(limit = 25): Promise<PublicClusterChangeReview[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterChangeReviewRow>(
      `select id, cluster_id, action, status, actor, request, current_cluster, proposed_cluster, warnings, created_at,
        reviewed_by, reviewed_at, review_comment, applied_by, applied_at
       from cluster_change_reviews
       order by created_at desc
       limit $1`,
      [limit]
    );
    return result.rows.map(rowToClusterChangeReviewRecord).map(toPublicClusterChangeReview);
  }

  return [...memoryClusterChangeReviews.values()]
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
    .slice(0, limit)
    .map(toPublicClusterChangeReview);
}

export async function getClusterChangeReview(reviewId: string): Promise<ClusterChangeReviewRecord | undefined> {
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterChangeReviewRow>(
      `select id, cluster_id, action, status, actor, request, current_cluster, proposed_cluster, warnings, created_at,
        reviewed_by, reviewed_at, review_comment, applied_by, applied_at
       from cluster_change_reviews
       where id = $1`,
      [reviewId]
    );
    return result.rows[0] ? rowToClusterChangeReviewRecord(result.rows[0]) : undefined;
  }

  return memoryClusterChangeReviews.get(reviewId);
}

export async function markClusterChangeReview(
  reviewId: string,
  status: Extract<ClusterChangeReviewStatus, "approved" | "rejected">,
  actor: string,
  comment?: string
): Promise<PublicClusterChangeReview | undefined> {
  const reviewedAt = new Date().toISOString();
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterChangeReviewRow>(
      `update cluster_change_reviews
       set status = $2,
           reviewed_by = $3,
           reviewed_at = $4,
           review_comment = $5
       where id = $1
       returning id, cluster_id, action, status, actor, request, current_cluster, proposed_cluster, warnings, created_at,
        reviewed_by, reviewed_at, review_comment, applied_by, applied_at`,
      [reviewId, status, actor, reviewedAt, comment ?? null]
    );
    return result.rows[0] ? toPublicClusterChangeReview(rowToClusterChangeReviewRecord(result.rows[0])) : undefined;
  }

  const record = memoryClusterChangeReviews.get(reviewId);
  if (!record) {
    return undefined;
  }
  const next: ClusterChangeReviewRecord = {
    ...record,
    status,
    reviewedBy: actor,
    reviewedAt,
    reviewComment: comment
  };
  memoryClusterChangeReviews.set(reviewId, next);
  return toPublicClusterChangeReview(next);
}

export async function markClusterChangeReviewApplied(reviewId: string, actor: string): Promise<PublicClusterChangeReview | undefined> {
  const appliedAt = new Date().toISOString();
  if (persistenceMode() === "postgres") {
    const result = await query<ClusterChangeReviewRow>(
      `update cluster_change_reviews
       set status = 'applied',
           applied_by = $2,
           applied_at = $3
       where id = $1
       returning id, cluster_id, action, status, actor, request, current_cluster, proposed_cluster, warnings, created_at,
        reviewed_by, reviewed_at, review_comment, applied_by, applied_at`,
      [reviewId, actor, appliedAt]
    );
    return result.rows[0] ? toPublicClusterChangeReview(rowToClusterChangeReviewRecord(result.rows[0])) : undefined;
  }

  const record = memoryClusterChangeReviews.get(reviewId);
  if (!record) {
    return undefined;
  }
  const next: ClusterChangeReviewRecord = {
    ...record,
    status: "applied",
    appliedBy: actor,
    appliedAt
  };
  memoryClusterChangeReviews.set(reviewId, next);
  return toPublicClusterChangeReview(next);
}

export function clusterChangeReviewStateDrift(review: ClusterChangeReviewRecord, existingClusters: ClusterRecord[]): string[] {
  const currentRecord = existingClusters.find((cluster) => cluster.id === review.clusterId);
  const current = currentRecord ? toPublicCluster(currentRecord) : undefined;
  if (sameReviewedCluster(review.current, current)) {
    return [];
  }

  if (!review.current && current) {
    return ["Cluster was created after this review was requested; create a new review from the current registry state."];
  }
  if (review.current && !current) {
    return ["Cluster was removed after this review was requested; create a new review from the current registry state."];
  }
  return ["Cluster registry state changed after this review was requested; create a new review from the current registry state."];
}

export function clusterSecretPolicyViolation(cluster: ClusterConfig, authMode: SecurityMode): string | undefined {
  if (authMode === "token" && cluster.sasl?.password && !cluster.sasl.passwordEnv) {
    return "Inline Kafka SASL passwords are not accepted in token auth mode. Use sasl.passwordEnv backed by an API-process secret.";
  }
  return undefined;
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

function toPublicClusterChangeReview(record: ClusterChangeReviewRecord): PublicClusterChangeReview {
  return {
    id: record.id,
    clusterId: record.clusterId,
    action: record.action,
    status: record.status,
    actor: record.actor,
    request:
      record.request.action === "upsert"
        ? {
            action: "upsert",
            clusterId: record.clusterId,
            cluster: sanitizeClusterConfig(record.request.cluster)
          }
        : {
            action: "delete",
            clusterId: record.clusterId
          },
    current: record.current,
    proposed: record.proposed,
    warnings: record.warnings,
    createdAt: record.createdAt,
    reviewedBy: record.reviewedBy,
    reviewedAt: record.reviewedAt,
    reviewComment: record.reviewComment,
    appliedBy: record.appliedBy,
    appliedAt: record.appliedAt
  };
}

function proposedPublicCluster(cluster: ClusterConfig, current: ClusterRecord | undefined, now: string): PublicCluster {
  return toPublicCluster({
    ...cluster,
    source: "api",
    createdAt: current?.createdAt ?? now,
    updatedAt: now
  });
}

function clusterReviewWarnings(request: ClusterChangeReviewRequest, current: PublicCluster | undefined) {
  const warnings: string[] = [];
  if (request.action === "upsert") {
    if (current?.source === "environment") {
      warnings.push("Applying this review will replace an environment-managed cluster with an API-managed registry entry.");
    }
    if (request.cluster.sasl?.password && !request.cluster.sasl.passwordEnv) {
      warnings.push("Inline SASL passwords are stored in the control-plane database; prefer passwordEnv for production clusters.");
    }
  }

  if (request.action === "delete") {
    if (!current) {
      warnings.push("Cluster does not exist yet; apply will be rejected unless the registry changes before review.");
    } else if (current.source === "environment") {
      warnings.push("Environment-managed clusters cannot be deleted through the API.");
    }
  }

  return warnings;
}

function sameReviewedCluster(left: PublicCluster | undefined, right: PublicCluster | undefined) {
  return stableClusterFingerprint(left) === stableClusterFingerprint(right);
}

function stableClusterFingerprint(cluster: PublicCluster | undefined) {
  if (!cluster) {
    return "missing";
  }
  return JSON.stringify({
    id: cluster.id,
    name: cluster.name,
    brokers: cluster.brokers,
    ssl: cluster.ssl ?? false,
    saslConfigured: cluster.saslConfigured,
    saslPasswordSource: cluster.saslPasswordSource,
    source: cluster.source
  });
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

function rowToClusterChangeReviewRecord(row: ClusterChangeReviewRow): ClusterChangeReviewRecord {
  return {
    id: row.id,
    clusterId: row.cluster_id,
    action: row.action,
    status: row.status,
    actor: row.actor,
    request: row.request,
    current: row.current_cluster ?? undefined,
    proposed: row.proposed_cluster ?? undefined,
    warnings: row.warnings,
    createdAt: row.created_at.toISOString(),
    reviewedBy: row.reviewed_by,
    reviewedAt: row.reviewed_at?.toISOString(),
    reviewComment: row.review_comment,
    appliedBy: row.applied_by,
    appliedAt: row.applied_at?.toISOString()
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
