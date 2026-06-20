import test from "node:test";
import assert from "node:assert/strict";
import { buildDiskRebalancePlan, buildRebalancePreflight, evaluateRebalanceExecutionCompletion } from "./rebalance.js";
import type { CollectorState, DiskTelemetry, PartitionPlacement, RebalanceExecutionStatus, RebalancePlan, RebalancePlanRecord } from "./types.js";

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
  assert.equal(plan.executable, false);
  assert.ok(plan.executionBlockedReason?.includes("broker-local byte estimate"));
  assert.equal(plan.summary.estimatedBytesMoved, undefined);
  assert.ok(plan.warnings.some((warning) => warning.includes("Partition byte telemetry is missing")));
});

test("buildDiskRebalancePlan refuses target brokers without disk telemetry", () => {
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
      collector(4, 10, [], { disk: false })
    ],
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

  assert.equal(plan.movements.length, 0);
  assert.equal(plan.executable, false);
  assert.ok(plan.warnings.some((warning) => warning.includes("Requested target broker 4 does not have fresh disk telemetry")));
});

test("buildDiskRebalancePlan refuses stale requested source telemetry", () => {
  const now = new Date("2026-06-20T12:10:00.000Z");
  const staleSample = "2026-06-20T12:00:00.000Z";
  const plan = buildDiskRebalancePlan({
    clusterId: "local",
    brokers,
    collectors: [
      collector(1, 90, [["orders.created", 0, 50_000]], { sampledAt: staleSample }),
      collector(2, 42, [], { sampledAt: now.toISOString() }),
      collector(3, 40, [], { sampledAt: now.toISOString() }),
      collector(4, 10, [], { sampledAt: now.toISOString() })
    ],
    placements,
    input: {
      maxMovements: 1,
      includeInternal: false,
      sourceBrokerId: 1,
      targetBrokerIds: [4],
      highWatermarkPercent: 95,
      minBrokerGapPercent: 5,
      executionEnabled: true
    },
    now,
    collectorMaxAgeMs: 5 * 60 * 1000
  });

  assert.equal(plan.movements.length, 0);
  assert.equal(plan.executable, false);
  assert.ok(plan.warnings.some((warning) => warning.includes("Disk telemetry is stale or invalid for broker 1")));
  assert.ok(plan.warnings.some((warning) => warning.includes("Requested source broker 1 does not have fresh disk telemetry")));
});

test("buildDiskRebalancePlan ignores stale partition byte telemetry", () => {
  const now = new Date("2026-06-20T12:10:00.000Z");
  const staleSnapshot = "2026-06-20T12:00:00.000Z";
  const plan = buildDiskRebalancePlan({
    clusterId: "local",
    brokers,
    collectors: [
      collector(1, 90, [["orders.created", 0, 50_000]], {
        sampledAt: staleSnapshot,
        diskSampledAt: now.toISOString()
      }),
      collector(2, 42, [], { sampledAt: now.toISOString() }),
      collector(3, 40, [], { sampledAt: now.toISOString() }),
      collector(4, 10, [], { sampledAt: now.toISOString() })
    ],
    placements,
    input: {
      maxMovements: 1,
      includeInternal: false,
      sourceBrokerId: 1,
      targetBrokerIds: [4],
      highWatermarkPercent: 95,
      minBrokerGapPercent: 5,
      executionEnabled: true
    },
    now,
    collectorMaxAgeMs: 5 * 60 * 1000
  });

  assert.equal(plan.movements.length, 1);
  assert.equal(plan.movements[0]?.estimatedSizeBytes, undefined);
  assert.equal(plan.executable, false);
  assert.ok(plan.warnings.some((warning) => warning.includes("Partition byte telemetry is stale or invalid for broker 1")));
});

test("buildRebalancePreflight passes an approved current plan with fresh collector telemetry", () => {
  const plan = planFixture();
  const preflight = buildRebalancePreflight({
    planRecord: planRecord(plan, "approved"),
    executionEnabled: true,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: placements,
    collectors: [
      collector(1, 90),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
    now: new Date()
  });

  assert.equal(preflight.executable, true);
  assert.equal(preflight.blockedReasons.length, 0);
  assert.equal(preflight.checks.every((check) => check.status !== "fail"), true);
});

test("buildRebalancePreflight fails when reviewed replica placement is stale", () => {
  const plan = planFixture();
  const preflight = buildRebalancePreflight({
    planRecord: planRecord(plan, "approved"),
    executionEnabled: true,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: [
      placement("orders.created", 0, 2, [1, 2, 3]),
      placement("orders.created", 1, 2, [2, 1, 3]),
      placement("orders.created", 2, 1, [1, 2, 3])
    ],
    collectors: [
      collector(1, 90),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
    now: new Date()
  });

  assert.equal(preflight.executable, false);
  assert.equal(preflight.staleMovementCount, 1);
  assert.equal(preflight.checks.find((check) => check.id === "placement-current")?.status, "fail");
});

test("buildRebalancePreflight fails when planned partitions are degraded", () => {
  const plan = planFixture();
  const movement = plan.movements[0];
  assert.ok(movement);
  const preflight = buildRebalancePreflight({
    planRecord: planRecord(plan, "approved"),
    executionEnabled: true,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: placements.map((candidate) =>
      candidate.topic === movement.topic && candidate.partition === movement.partition
        ? {
            ...candidate,
            isr: candidate.replicas.slice(0, -1),
            offlineReplicas: [candidate.replicas.at(-1) ?? candidate.replicas[0] ?? 0]
          }
        : candidate
    ),
    collectors: [
      collector(1, 90),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
    now: new Date()
  });

  assert.equal(preflight.executable, false);
  assert.equal(preflight.degradedMovementCount, 1);
  assert.equal(preflight.checks.find((check) => check.id === "partition-health")?.status, "fail");
});

test("buildRebalancePreflight fails when planned brokers lack disk telemetry", () => {
  const plan = planFixture();
  const preflight = buildRebalancePreflight({
    planRecord: planRecord(plan, "approved"),
    executionEnabled: true,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: placements,
    collectors: [
      collector(1, 90),
      collector(2, 42),
      collector(3, 40)
    ],
    now: new Date()
  });

  assert.equal(preflight.executable, false);
  assert.deepEqual(preflight.missingTelemetryBrokerIds, [4]);
  assert.equal(preflight.checks.find((check) => check.id === "collector-coverage")?.status, "fail");
});

test("buildRebalancePreflight fails when planned brokers leave live metadata", () => {
  const plan = planFixture();
  const preflight = buildRebalancePreflight({
    planRecord: planRecord(plan, "approved"),
    executionEnabled: true,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: placements,
    brokers: brokers.filter((broker) => broker.nodeId !== 4),
    collectors: [
      collector(1, 90),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
    now: new Date()
  });

  assert.equal(preflight.executable, false);
  assert.deepEqual(preflight.missingBrokerIds, [4]);
  assert.equal(preflight.checks.find((check) => check.id === "broker-membership")?.status, "fail");
});

test("buildRebalancePreflight fails when planned movements lack byte estimates", () => {
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
  const preflight = buildRebalancePreflight({
    planRecord: planRecord(plan, "approved"),
    executionEnabled: true,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: placements,
    collectors: [
      collector(1, 90),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
    now: new Date()
  });

  assert.equal(plan.movements[0]?.estimatedSizeBytes, undefined);
  assert.equal(preflight.executable, false);
  assert.equal(preflight.unknownSizeMovementCount, 1);
  assert.equal(preflight.checks.find((check) => check.id === "movement-size-coverage")?.status, "fail");
});

test("rebalance execution completion requires inactive Kafka status and proposed placement", () => {
  const plan = planFixture();
  const movement = plan.movements[0];
  assert.ok(movement);

  const pending = evaluateRebalanceExecutionCompletion({
    plan,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: placements
  });
  assert.equal(pending.complete, false);
  assert.equal(pending.incompleteMovementCount, 1);

  const completedPlacements = placements.map((candidate) =>
    candidate.topic === movement.topic && candidate.partition === movement.partition
      ? placement(candidate.topic, candidate.partition, candidate.leader, movement.proposedReplicas)
      : candidate
  );
  const active = evaluateRebalanceExecutionCompletion({
    plan,
    reassignmentStatus: activeReassignmentStatus(movement.topic, movement.partition),
    currentPlacements: completedPlacements
  });
  assert.equal(active.complete, false);
  assert.equal(active.active, true);

  const completed = evaluateRebalanceExecutionCompletion({
    plan,
    reassignmentStatus: inactiveReassignmentStatus(),
    currentPlacements: completedPlacements
  });
  assert.equal(completed.complete, true);
  assert.equal(completed.incompleteMovementCount, 0);
});

function planFixture(): RebalancePlan {
  return buildDiskRebalancePlan({
    clusterId: "local",
    brokers,
    collectors: [
      collector(1, 90, [
        ["orders.created", 0, 50_000],
        ["orders.created", 1, 100_000]
      ]),
      collector(2, 42),
      collector(3, 40),
      collector(4, 10)
    ],
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
}

function planRecord(plan: RebalancePlan, status: RebalancePlanRecord["status"]): RebalancePlanRecord {
  return {
    id: plan.id,
    clusterId: plan.clusterId,
    actor: "test",
    status,
    createdAt: plan.generatedAt,
    reviewedBy: status === "approved" ? "reviewer" : undefined,
    reviewedAt: status === "approved" ? new Date().toISOString() : undefined,
    plan
  };
}

function inactiveReassignmentStatus(): RebalanceExecutionStatus {
  return {
    clusterId: "local",
    checkedAt: new Date().toISOString(),
    active: false,
    activeTopicCount: 0,
    activePartitionCount: 0,
    reassignments: []
  };
}

function activeReassignmentStatus(topic: string, partition: number): RebalanceExecutionStatus {
  return {
    clusterId: "local",
    checkedAt: new Date().toISOString(),
    active: true,
    activeTopicCount: 1,
    activePartitionCount: 1,
    reassignments: [
      {
        topic,
        partition,
        replicas: [1, 2, 3, 4],
        addingReplicas: [4],
        removingReplicas: [1]
      }
    ]
  };
}

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
  partitions: Array<[topic: string, partition: number, sizeBytes: number]> = [],
  options: { disk?: boolean; sampledAt?: string; diskSampledAt?: string } = {}
): CollectorState {
  const observedAt = options.sampledAt ?? new Date().toISOString();
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
      disk: options.disk === false ? undefined : disk(usedPercent, options.diskSampledAt ?? observedAt),
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

function disk(usedPercent: number, sampledAt = new Date().toISOString()): DiskTelemetry {
  const totalBytes = 1_000_000;
  const usedBytes = Math.round(totalBytes * (usedPercent / 100));
  return {
    path: "/broker-data",
    totalBytes,
    usedBytes,
    freeBytes: totalBytes - usedBytes,
    usedPercent,
    pressure: usedPercent >= 85 ? "high" : "normal",
    sampledAt
  };
}
