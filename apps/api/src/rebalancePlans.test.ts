import test from "node:test";
import assert from "node:assert/strict";
import {
  getRebalancePlan,
  listRebalancePlans,
  markRebalancePlanExecuted,
  markRebalancePlanReviewed,
  saveRebalancePlan
} from "./rebalancePlans.js";
import type { RebalancePlan } from "./types.js";

test("rebalance plan history lists summaries and preserves full plan lookup", async () => {
  const plan = rebalancePlan("history-plan-a");
  await saveRebalancePlan(plan, "history-test");

  const history = await listRebalancePlans("history-cluster", 10);
  const stored = await getRebalancePlan(plan.id);

  assert.equal(history[0]?.id, plan.id);
  assert.equal(history[0]?.actor, "history-test");
  assert.equal(history[0]?.summary.movements, 1);
  assert.equal(stored?.plan.reassignment.partitions[0]?.topic, "orders.created");

  const approved = await markRebalancePlanReviewed(plan.id, "approved", "reviewer-a", "looks balanced");
  assert.equal(approved?.status, "approved");
  assert.equal(approved?.reviewedBy, "reviewer-a");
  assert.equal(approved?.reviewComment, "looks balanced");

  await markRebalancePlanExecuted(plan.id);
  const executed = await listRebalancePlans("history-cluster", 10);
  assert.equal(executed[0]?.status, "executed");
  assert.equal(typeof executed[0]?.executedAt, "string");

  const rejectedPlan = rebalancePlan("history-plan-b");
  await saveRebalancePlan(rejectedPlan, "history-test");
  const rejected = await markRebalancePlanReviewed(rejectedPlan.id, "rejected", "reviewer-b");
  assert.equal(rejected?.status, "rejected");
  assert.equal(rejected?.reviewedBy, "reviewer-b");
});

function rebalancePlan(id: string): RebalancePlan {
  const generatedAt = new Date().toISOString();
  return {
    id,
    clusterId: "history-cluster",
    generatedAt,
    strategy: "disk-pressure",
    executable: true,
    brokerPressure: [
      {
        brokerId: 1,
        replicaCount: 3,
        leaderCount: 1
      }
    ],
    summary: {
      movements: 1,
      partitionsEvaluated: 3,
      sourceBrokerIds: [1],
      targetBrokerIds: [2],
      estimatedBytesMoved: 1024
    },
    movements: [
      {
        topic: "orders.created",
        partition: 0,
        sourceBrokerId: 1,
        targetBrokerId: 2,
        currentReplicas: [1, 2, 3],
        proposedReplicas: [2, 3, 4],
        leaderMove: false,
        estimatedSizeBytes: 1024,
        reason: "test"
      }
    ],
    reassignment: {
      version: 1,
      partitions: [
        {
          topic: "orders.created",
          partition: 0,
          replicas: [2, 3, 4],
          log_dirs: ["any", "any", "any"]
        }
      ]
    },
    kafkaJsRequest: [
      {
        topic: "orders.created",
        partitionAssignment: [
          {
            partition: 0,
            replicas: [2, 3, 4]
          }
        ]
      }
    ],
    warnings: []
  };
}
