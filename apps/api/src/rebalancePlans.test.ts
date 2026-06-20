import test from "node:test";
import assert from "node:assert/strict";
import {
  getRebalancePlan,
  listRebalancePlans,
  markRebalancePlanExecuted,
  markRebalancePlanExecutionStarted,
  markRebalancePlanReviewed,
  releaseRebalancePlanExecution,
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

  const executing = await markRebalancePlanExecutionStarted(plan.id, "operator-a");
  assert.equal(executing?.status, "executing");
  assert.equal(executing?.executionStartedBy, "operator-a");
  assert.equal(typeof executing?.executionStartedAt, "string");

  const concurrentPlan = rebalancePlan("history-plan-concurrent");
  await saveRebalancePlan(concurrentPlan, "history-test");
  await markRebalancePlanReviewed(concurrentPlan.id, "approved", "reviewer-a");
  const blockedConcurrentExecution = await markRebalancePlanExecutionStarted(concurrentPlan.id, "operator-b");
  assert.equal(blockedConcurrentExecution, undefined);

  await releaseRebalancePlanExecution(plan.id);
  const released = await getRebalancePlan(plan.id);
  assert.equal(released?.status, "approved");
  assert.equal(released?.executionStartedAt, undefined);

  const executingAgain = await markRebalancePlanExecutionStarted(plan.id, "operator-a");
  assert.equal(executingAgain?.status, "executing");

  const completed = await markRebalancePlanExecuted(plan.id);
  assert.equal(completed?.status, "executed");
  const executed = await listRebalancePlans("history-cluster", 10);
  const executedPlan = executed.find((record) => record.id === plan.id);
  assert.equal(executedPlan?.status, "executed");
  assert.equal(typeof executedPlan?.executionStartedAt, "string");
  assert.equal(typeof executedPlan?.executedAt, "string");

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
