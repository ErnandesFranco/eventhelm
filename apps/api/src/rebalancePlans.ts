import { persistenceMode, query } from "./db.js";
import type { RebalancePlan, RebalancePlanRecord, RebalancePlanStatus, RebalancePlanSummaryRecord } from "./types.js";

type RebalancePlanRow = {
  id: string;
  cluster_id: string;
  actor: string;
  status: RebalancePlanStatus;
  plan: RebalancePlan;
  created_at: Date;
  execution_started_at?: Date;
  execution_started_by?: string;
  executed_at?: Date;
  reviewed_by?: string;
  reviewed_at?: Date;
  review_comment?: string;
};

const plans = new Map<string, RebalancePlanRecord>();

export async function saveRebalancePlan(plan: RebalancePlan, actor: string): Promise<RebalancePlanRecord> {
  const record: RebalancePlanRecord = {
    id: plan.id,
    clusterId: plan.clusterId,
    actor,
    status: "planned",
    plan,
    createdAt: plan.generatedAt
  };

  if (persistenceMode() === "postgres") {
    await query(
      `insert into rebalance_plans (id, cluster_id, actor, status, plan, created_at)
       values ($1, $2, $3, $4, $5, $6)
       on conflict (id) do update set
        actor = excluded.actor,
        status = excluded.status,
        plan = excluded.plan`,
      [record.id, record.clusterId, record.actor, record.status, record.plan, record.createdAt]
    );
    return record;
  }

  plans.set(record.id, record);
  return record;
}

export async function getRebalancePlan(planId: string): Promise<RebalancePlanRecord | undefined> {
  if (persistenceMode() === "postgres") {
    const result = await query<RebalancePlanRow>(
      `select id, cluster_id, actor, status, plan, created_at, execution_started_at, execution_started_by, executed_at, reviewed_by, reviewed_at, review_comment
       from rebalance_plans
       where id = $1`,
      [planId]
    );
    return result.rows[0] ? rowToRecord(result.rows[0]) : undefined;
  }

  return plans.get(planId);
}

export async function listRebalancePlans(clusterId: string, limit = 25): Promise<RebalancePlanSummaryRecord[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<RebalancePlanRow>(
      `select id, cluster_id, actor, status, plan, created_at, execution_started_at, execution_started_by, executed_at, reviewed_by, reviewed_at, review_comment
       from rebalance_plans
       where cluster_id = $1
       order by created_at desc
       limit $2`,
      [clusterId, limit]
    );
    return result.rows.map((row) => toSummary(rowToRecord(row)));
  }

  return [...plans.values()]
    .filter((record) => record.clusterId === clusterId)
    .sort((left, right) => new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime())
    .slice(0, limit)
    .map(toSummary);
}

export async function markRebalancePlanReviewed(
  planId: string,
  status: Extract<RebalancePlanStatus, "approved" | "rejected">,
  actor: string,
  comment?: string
): Promise<RebalancePlanRecord | undefined> {
  const reviewedAt = new Date().toISOString();

  if (persistenceMode() === "postgres") {
    const result = await query<RebalancePlanRow>(
      `update rebalance_plans
       set status = $2,
           reviewed_by = $3,
           reviewed_at = $4,
           review_comment = $5
       where id = $1
       returning id, cluster_id, actor, status, plan, created_at, execution_started_at, execution_started_by, executed_at, reviewed_by, reviewed_at, review_comment`,
      [planId, status, actor, reviewedAt, comment ?? null]
    );
    return result.rows[0] ? rowToRecord(result.rows[0]) : undefined;
  }

  const record = plans.get(planId);
  if (!record) {
    return undefined;
  }
  const next: RebalancePlanRecord = {
    ...record,
    status,
    reviewedBy: actor,
    reviewedAt,
    reviewComment: comment
  };
  plans.set(planId, next);
  return next;
}

export async function listExecutingRebalancePlans(clusterId: string): Promise<RebalancePlanRecord[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<RebalancePlanRow>(
      `select id, cluster_id, actor, status, plan, created_at, execution_started_at, execution_started_by, executed_at, reviewed_by, reviewed_at, review_comment
       from rebalance_plans
       where cluster_id = $1
         and status = 'executing'
       order by execution_started_at asc nulls last, created_at asc`,
      [clusterId]
    );
    return result.rows.map(rowToRecord);
  }

  return [...plans.values()]
    .filter((record) => record.clusterId === clusterId && record.status === "executing")
    .sort((left, right) => new Date(left.executionStartedAt ?? left.createdAt).getTime() - new Date(right.executionStartedAt ?? right.createdAt).getTime());
}

export async function markRebalancePlanExecutionStarted(planId: string, actor: string): Promise<RebalancePlanRecord | undefined> {
  const executionStartedAt = new Date().toISOString();

  if (persistenceMode() === "postgres") {
    try {
      const result = await query<RebalancePlanRow>(
        `update rebalance_plans
         set status = 'executing',
             execution_started_at = $2,
             execution_started_by = $3
         where id = $1
           and status = 'approved'
           and not exists (
             select 1
             from rebalance_plans active
             where active.cluster_id = rebalance_plans.cluster_id
               and active.status = 'executing'
               and active.id <> rebalance_plans.id
           )
         returning id, cluster_id, actor, status, plan, created_at, execution_started_at, execution_started_by, executed_at, reviewed_by, reviewed_at, review_comment`,
        [planId, executionStartedAt, actor]
      );
      return result.rows[0] ? rowToRecord(result.rows[0]) : undefined;
    } catch (caught) {
      if ((caught as { code?: string }).code === "23505") {
        return undefined;
      }
      throw caught;
    }
  }

  const record = plans.get(planId);
  if (!record || record.status !== "approved") {
    return undefined;
  }
  const hasExecutingPlan = [...plans.values()].some((candidate) => candidate.clusterId === record.clusterId && candidate.status === "executing" && candidate.id !== record.id);
  if (hasExecutingPlan) {
    return undefined;
  }
  const next: RebalancePlanRecord = {
    ...record,
    status: "executing",
    executionStartedAt,
    executionStartedBy: actor
  };
  plans.set(planId, next);
  return next;
}

export async function releaseRebalancePlanExecution(planId: string): Promise<void> {
  if (persistenceMode() === "postgres") {
    await query(
      `update rebalance_plans
       set status = 'approved',
           execution_started_at = null,
           execution_started_by = null
       where id = $1
         and status = 'executing'`,
      [planId]
    );
    return;
  }

  const record = plans.get(planId);
  if (record?.status === "executing") {
    plans.set(planId, {
      ...record,
      status: "approved",
      executionStartedAt: undefined,
      executionStartedBy: undefined
    });
  }
}

export async function markRebalancePlanExecuted(planId: string): Promise<RebalancePlanRecord | undefined> {
  const executedAt = new Date().toISOString();

  if (persistenceMode() === "postgres") {
    const result = await query<RebalancePlanRow>(
      `update rebalance_plans
       set status = 'executed',
           execution_started_at = coalesce(execution_started_at, $2),
           executed_at = $2
       where id = $1
       returning id, cluster_id, actor, status, plan, created_at, execution_started_at, execution_started_by, executed_at, reviewed_by, reviewed_at, review_comment`,
      [planId, executedAt]
    );
    return result.rows[0] ? rowToRecord(result.rows[0]) : undefined;
  }

  const record = plans.get(planId);
  if (record) {
    const next: RebalancePlanRecord = {
      ...record,
      status: "executed",
      executionStartedAt: record.executionStartedAt ?? executedAt,
      executedAt
    };
    plans.set(planId, next);
    return next;
  }
  return undefined;
}

function rowToRecord(row: RebalancePlanRow): RebalancePlanRecord {
  return {
    id: row.id,
    clusterId: row.cluster_id,
    actor: row.actor,
    status: row.status,
    plan: row.plan,
    createdAt: row.created_at.toISOString(),
    executionStartedAt: row.execution_started_at?.toISOString(),
    executionStartedBy: row.execution_started_by,
    executedAt: row.executed_at?.toISOString(),
    reviewedBy: row.reviewed_by,
    reviewedAt: row.reviewed_at?.toISOString(),
    reviewComment: row.review_comment
  };
}

export function toSummary(record: RebalancePlanRecord): RebalancePlanSummaryRecord {
  return {
    id: record.id,
    clusterId: record.clusterId,
    actor: record.actor,
    status: record.status,
    createdAt: record.createdAt,
    executionStartedAt: record.executionStartedAt,
    executionStartedBy: record.executionStartedBy,
    executedAt: record.executedAt,
    reviewedBy: record.reviewedBy,
    reviewedAt: record.reviewedAt,
    reviewComment: record.reviewComment,
    strategy: record.plan.strategy,
    executable: record.plan.executable,
    executionBlockedReason: record.plan.executionBlockedReason,
    summary: record.plan.summary,
    warnings: record.plan.warnings
  };
}
