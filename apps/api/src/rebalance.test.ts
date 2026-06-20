import test from "node:test";
import assert from "node:assert/strict";
import { buildDiskRebalancePlan } from "./rebalance.js";
import type { CollectorState, DiskTelemetry, PartitionPlacement } from "./types.js";

const brokers = [
  { nodeId: 1, host: "kafka-1", port: 9092 },
  { nodeId: 2, host: "kafka-2", port: 9092 },
  { nodeId: 3, host: "kafka-3", port: 9092 },
  { nodeId: 4, host: "kafka-4", port: 9092 }
];

const placements: PartitionPlacement[] = [
  placement("orders.created", 0, 2, [1, 2, 3]),
  placement("orders.created", 1, 2, [1, 2, 3]),
  placement("orders.created", 2, 1, [1, 2, 3])
];

test("buildDiskRebalancePlan uses collector partition bytes for estimates", () => {
  const plan = buildDiskRebalancePlan({
    clusterId: "local",
    brokers,
    collectors: [
      collector(1, 90, [
        ["orders.created", 0, 50_000],
        ["orders.created", 1, 100_000],
        ["orders.created", 2, 200_000]
      ]),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
    placements,
    input: {
      maxMovements: 2,
      includeInternal: false,
      sourceBrokerId: 1,
      targetBrokerIds: [4],
      highWatermarkPercent: 95,
      minBrokerGapPercent: 5,
      executionEnabled: true
    }
  });

  assert.equal(typeof plan.id, "string");
  assert.equal(plan.movements.length, 2);
  assert.equal(plan.movements[0]?.topic, "orders.created");
  assert.equal(plan.movements[0]?.partition, 1);
  assert.equal(plan.movements[0]?.estimatedSizeBytes, 100_000);
  assert.equal(plan.movements[1]?.partition, 0);
  assert.equal(plan.movements[1]?.estimatedSizeBytes, 50_000);
  assert.equal(plan.summary.estimatedBytesMoved, 150_000);
  assert.deepEqual(plan.summary.targetBrokerIds, [4]);
  assert.equal(plan.warnings.length, 0);
});

test("buildDiskRebalancePlan warns when partition byte telemetry is missing", () => {
  const plan = buildDiskRebalancePlan({
    clusterId: "local",
    brokers,
    collectors: [collector(1, 90), collector(2, 42), collector(3, 40), collector(4, 10)],
    placements,
    input: {
      maxMovements: 1,
      includeInternal: false,
      sourceBrokerId: 1,
      targetBrokerIds: [4],
      highWatermarkPercent: 95,
      minBrokerGapPercent: 5,
      executionEnabled: true
    }
  });

  assert.equal(plan.movements.length, 1);
  assert.equal(plan.movements[0]?.estimatedSizeBytes, undefined);
  assert.equal(plan.summary.estimatedBytesMoved, undefined);
  assert.ok(plan.warnings.some((warning) => warning.includes("Partition byte telemetry is missing")));
});

function placement(topic: string, partition: number, leader: number, replicas: number[]): PartitionPlacement {
  return {
    topic,
    partition,
    leader,
    replicas,
    isr: replicas,
    offlineReplicas: [],
    isInternal: topic.startsWith("__")
  };
}

function collector(
  brokerId: number,
  usedPercent: number,
  partitions: Array<[topic: string, partition: number, sizeBytes: number]> = []
): CollectorState {
  const observedAt = new Date().toISOString();
  return {
    heartbeat: {
      collectorId: `local-broker-${brokerId}`,
      clusterId: "local",
      brokerId: String(brokerId),
      hostname: `broker-${brokerId}`,
      version: "test",
      startedAt: observedAt,
      observedAt
    },
    lastSnapshot: {
      collectorId: `local-broker-${brokerId}`,
      clusterId: "local",
      brokerId: String(brokerId),
      hostname: `broker-${brokerId}`,
      version: "test",
      startedAt: observedAt,
      observedAt,
      brokerCount: brokers.length,
      topicCount: 1,
      disk: disk(usedPercent),
      partitions: partitions.map(([topic, partition, sizeBytes]) => ({
        topic,
        partition,
        sizeBytes,
        logDir: `/broker-data/${topic}-${partition}`
      })),
      brokers
    }
  };
}

function disk(usedPercent: number): DiskTelemetry {
  const totalBytes = 1_000_000;
  const usedBytes = Math.round(totalBytes * (usedPercent / 100));
  return {
    path: "/broker-data",
    totalBytes,
    usedBytes,
    freeBytes: totalBytes - usedBytes,
    usedPercent,
    pressure: usedPercent >= 85 ? "high" : "normal",
    sampledAt: new Date().toISOString()
  };
}
