import assert from "node:assert/strict";
import test from "node:test";
import { toRebalanceExecutionStatus } from "./kafka.js";

test("toRebalanceExecutionStatus filters active partition reassignments", () => {
  const status = toRebalanceExecutionStatus(
    "status-cluster",
    [
      {
        topic: "orders.created",
        partitions: [
          {
            partitionIndex: 2,
            replicas: [1, 2, 4],
            addingReplicas: [4],
            removingReplicas: [3]
          },
          {
            partitionIndex: 0,
            replicas: [1, 2, 3],
            addingReplicas: [],
            removingReplicas: []
          }
        ]
      },
      {
        topic: "billing.events",
        partitions: [
          {
            partitionIndex: 1,
            replicas: [2, 3, 5],
            addingReplicas: [5]
          }
        ]
      }
    ],
    "2026-06-20T18:00:00.000Z"
  );

  assert.equal(status.active, true);
  assert.equal(status.activePartitionCount, 2);
  assert.equal(status.activeTopicCount, 2);
  assert.deepEqual(
    status.reassignments.map((assignment) => `${assignment.topic}:${assignment.partition}`),
    ["billing.events:1", "orders.created:2"]
  );
  assert.deepEqual(status.reassignments[0]?.removingReplicas, []);
});

