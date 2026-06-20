import { persistenceMode, query } from "./db.js";
import type { RebalancePlan } from "./types.js";

type RebalancePlanStatus = "planned" | "executed";

type RebalancePlanRecord = {
  id: string;
  clusterId: string;
  actor: string;
  status: RebalancePlanStatus;
  plan: RebalancePlan;
  createdAt: string;
  executedAt?: string;
};

type RebalancePlanRow = {
  id: string;
  cluster_id: string;
  actor: string;
  status: RebalancePlanStatus;
  plan: RebalancePlan;
  created_at: Date;
  executed_at?: Date;
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
      `select id, cluster_id, actor, status, plan, created_at, executed_at
       from rebalance_plans
       where id = $1`,
      [planId]
    );
    return result.rows[0] ? rowToRecord(result.rows[0]) : undefined;
  }

  return plans.get(planId);
}

export async function markRebalancePlanExecuted(planId: string): Promise<void> {
  const executedAt = new Date().toISOString();

  if (persistenceMode() === "postgres") {
    await query(
      `update rebalance_plans
       set status = 'executed',
           executed_at = $2
       where id = $1`,
      [planId, executedAt]
    );
    return;
  }

  const record = plans.get(planId);
  if (record) {
    plans.set(planId, {
      ...record,
      status: "executed",
      executedAt
    });
  }
}

function rowToRecord(row: RebalancePlanRow): RebalancePlanRecord {
  return {
    id: row.id,
    clusterId: row.cluster_id,
    actor: row.actor,
    status: row.status,
    plan: row.plan,
    createdAt: row.created_at.toISOString(),
    executedAt: row.executed_at?.toISOString()
  };
}
