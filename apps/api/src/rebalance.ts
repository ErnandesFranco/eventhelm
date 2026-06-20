import { nanoid } from "nanoid";
import type {
  CollectorState,
  PartitionPlacement,
  RebalanceExecutionStatus,
  RebalanceMovement,
  RebalancePlan,
  RebalancePlanRecord,
  RebalancePreflight,
  RebalancePreflightCheck
} from "./types.js";

export const REBALANCE_COLLECTOR_MAX_AGE_MS = 5 * 60 * 1000;

export type RebalancePlanInput = {
  maxMovements: number;
  includeInternal: boolean;
  sourceBrokerId?: number;
  targetBrokerIds?: number[];
  highWatermarkPercent: number;
  minBrokerGapPercent: number;
  executionEnabled: boolean;
};

type Broker = {
  nodeId: number;
  host: string;
  port: number;
};

type BrokerPressure = RebalancePlan["brokerPressure"][number];

export function buildDiskRebalancePlan({
  clusterId,
  brokers,
  collectors,
  placements,
  input,
  now = new Date(),
  collectorMaxAgeMs = REBALANCE_COLLECTOR_MAX_AGE_MS
}: {
  clusterId: string;
  brokers: Broker[];
  collectors: CollectorState[];
  placements: PartitionPlacement[];
  input: RebalancePlanInput;
  now?: Date;
  collectorMaxAgeMs?: number;
}): RebalancePlan {
  const warnings = new Set<string>();
  const pressure = buildBrokerPressure(brokers, collectors, placements);
  const brokersWithDisk = pressure.filter((broker) => broker.disk);
  const liveBrokerIds = new Set(brokers.map((broker) => broker.nodeId));
  const staleDiskBrokerIds = brokersWithDisk
    .filter((broker) => !hasFreshDiskTelemetry(broker, now, collectorMaxAgeMs))
    .map((broker) => broker.brokerId)
    .sort((left, right) => left - right);
  const freshDiskBrokerIds = new Set(
    brokersWithDisk
      .filter((broker) => hasFreshDiskTelemetry(broker, now, collectorMaxAgeMs))
      .map((broker) => broker.brokerId)
  );
  const partitionSizes = buildPartitionSizeIndex(collectors);
  const eligiblePlacements = placements.filter((placement) => input.includeInternal || !placement.isInternal);
  const sizedPlacements = eligiblePlacements.filter((placement) => partitionSizes.has(partitionKey(placement.topic, placement.partition)));
  const requestedTargetIds = input.targetBrokerIds ?? [];

  if (brokersWithDisk.length < brokers.length) {
    warnings.add("Disk telemetry is missing for at least one broker. Run a collector with BROKER_DATA_PATH on every broker host.");
  }

  if (staleDiskBrokerIds.length > 0) {
    warnings.add(
      `Disk telemetry is stale or invalid for broker${staleDiskBrokerIds.length === 1 ? "" : "s"} ${staleDiskBrokerIds.join(", ")}.`
    );
  }

  if (input.sourceBrokerId !== undefined && !liveBrokerIds.has(input.sourceBrokerId)) {
    warnings.add(`Requested source broker ${input.sourceBrokerId} is not present in live Kafka metadata.`);
  }

  if (input.sourceBrokerId !== undefined && liveBrokerIds.has(input.sourceBrokerId) && !freshDiskBrokerIds.has(input.sourceBrokerId)) {
    warnings.add(`Requested source broker ${input.sourceBrokerId} does not have fresh disk telemetry.`);
  }

  const missingTargetIds = requestedTargetIds.filter((brokerId) => !liveBrokerIds.has(brokerId));
  if (missingTargetIds.length > 0) {
    warnings.add(
      `Requested target broker${missingTargetIds.length === 1 ? "" : "s"} ${missingTargetIds.join(", ")} ${
        missingTargetIds.length === 1 ? "is" : "are"
      } not present in live Kafka metadata.`
    );
  }

  const targetIdsWithoutFreshDisk = requestedTargetIds.filter((brokerId) => liveBrokerIds.has(brokerId) && !freshDiskBrokerIds.has(brokerId));
  if (targetIdsWithoutFreshDisk.length > 0) {
    warnings.add(
      `Requested target broker${targetIdsWithoutFreshDisk.length === 1 ? "" : "s"} ${targetIdsWithoutFreshDisk.join(", ")} ${
        targetIdsWithoutFreshDisk.length === 1 ? "does" : "do"
      } not have fresh disk telemetry.`
    );
  }

  if (sizedPlacements.length === 0) {
    warnings.add("Partition byte telemetry is missing. Collectors need broker log-dir access to estimate movement size.");
  } else if (sizedPlacements.length < eligiblePlacements.length) {
    warnings.add(`${eligiblePlacements.length - sizedPlacements.length} eligible partitions are missing byte telemetry.`);
  }

  const skippedUnderReplicated = placements.filter((placement) => placement.isr.length < placement.replicas.length).length;
  if (skippedUnderReplicated > 0) {
    warnings.add(`${skippedUnderReplicated} under-replicated partitions were skipped.`);
  }

  const projectedReplicaCount = new Map(pressure.map((broker) => [broker.brokerId, broker.replicaCount]));
  const projectedUsedBytes = new Map(
    pressure
      .filter((broker) => broker.disk)
      .map((broker) => [broker.brokerId, broker.disk?.usedBytes ?? 0])
  );
  const movements: RebalanceMovement[] = [];
  const sourceBrokerIds = chooseSources(pressure, input, freshDiskBrokerIds);

  if (sourceBrokerIds.length === 0) {
    warnings.add("No broker is above the disk high-water mark or imbalance threshold.");
  }

  for (const sourceBrokerId of sourceBrokerIds) {
    const candidates = placements
      .filter((placement) => input.includeInternal || !placement.isInternal)
      .filter((placement) => placement.replicas.includes(sourceBrokerId))
      .filter((placement) => placement.isr.length === placement.replicas.length)
      .filter((placement) => !movements.some((movement) => movement.topic === placement.topic && movement.partition === placement.partition))
      .sort(
        (left, right) =>
          Number(left.leader === sourceBrokerId) - Number(right.leader === sourceBrokerId) ||
          (partitionSize(partitionSizes, right) ?? 0) - (partitionSize(partitionSizes, left) ?? 0)
      );

    for (const placement of candidates) {
      if (movements.length >= input.maxMovements) {
        break;
      }

      const estimatedSizeBytes = partitionSize(partitionSizes, placement);
      const targetBroker = chooseTarget({
        sourceBrokerId,
        currentReplicas: placement.replicas,
        estimatedSizeBytes,
        pressure,
        projectedReplicaCount,
        projectedUsedBytes,
        highWatermarkPercent: input.highWatermarkPercent,
        allowedTargets: input.targetBrokerIds,
        freshDiskBrokerIds
      });

      if (!targetBroker) {
        continue;
      }

      const proposedReplicas = placement.replicas.map((replica) => (replica === sourceBrokerId ? targetBroker.brokerId : replica));
      movements.push({
        topic: placement.topic,
        partition: placement.partition,
        sourceBrokerId,
        targetBrokerId: targetBroker.brokerId,
        currentReplicas: placement.replicas,
        proposedReplicas,
        estimatedSizeBytes,
        leaderMove: placement.leader === sourceBrokerId,
        reason: rebalanceReason(sourceBrokerId, targetBroker.brokerId, pressure, estimatedSizeBytes)
      });

      projectedReplicaCount.set(sourceBrokerId, Math.max(0, (projectedReplicaCount.get(sourceBrokerId) ?? 0) - 1));
      projectedReplicaCount.set(targetBroker.brokerId, (projectedReplicaCount.get(targetBroker.brokerId) ?? 0) + 1);
      if (estimatedSizeBytes !== undefined) {
        projectedUsedBytes.set(sourceBrokerId, Math.max(0, (projectedUsedBytes.get(sourceBrokerId) ?? 0) - estimatedSizeBytes));
        projectedUsedBytes.set(targetBroker.brokerId, (projectedUsedBytes.get(targetBroker.brokerId) ?? 0) + estimatedSizeBytes);
      }
    }
  }

  if (sourceBrokerIds.length > 0 && movements.length === 0) {
    warnings.add("No safe movement candidate was found. Eligible targets may already host replicas for the selected partitions.");
  }

  const reassignmentPartitions = movements.map((movement) => ({
    topic: movement.topic,
    partition: movement.partition,
    replicas: movement.proposedReplicas,
    log_dirs: movement.proposedReplicas.map(() => "any")
  }));

  const kafkaJsRequest = [...groupAssignments(movements).values()];
  const usedPercents = brokersWithDisk.map((broker) => broker.disk?.usedPercent).filter((value): value is number => typeof value === "number");
  const targetBrokerIds = [...new Set(movements.map((movement) => movement.targetBrokerId))].sort((left, right) => left - right);
  const movedSourceIds = [...new Set(movements.map((movement) => movement.sourceBrokerId))].sort((left, right) => left - right);
  const knownMovementSizes = movements
    .map((movement) => movement.estimatedSizeBytes)
    .filter((value): value is number => typeof value === "number");

  if (movements.length > 0 && knownMovementSizes.length < movements.length) {
    warnings.add("Some planned movements do not have byte estimates because their source collector did not report log-dir size.");
  }

  const executionBlockers = [
    input.executionEnabled
      ? undefined
      : "Rebalance execution is locked. Set EVENTHELM_ENABLE_REBALANCE_EXECUTION=true after approvals and RBAC are configured.",
    movements.length === 0 ? "No partition movements were generated." : undefined,
    movements.length > 0 && knownMovementSizes.length < movements.length
      ? "Every movement must have a broker-local byte estimate before execution."
      : undefined
  ].filter((reason): reason is string => Boolean(reason));
  const executionBlockedReason = executionBlockers.length > 0 ? executionBlockers.join(" ") : undefined;

  return {
    id: nanoid(),
    clusterId,
    generatedAt: new Date().toISOString(),
    strategy: "disk-pressure",
    executable: executionBlockers.length === 0,
    executionBlockedReason,
    brokerPressure: pressure,
    summary: {
      movements: movements.length,
      partitionsEvaluated: placements.length,
      sourceBrokerIds: movedSourceIds,
      targetBrokerIds,
      maxUsedPercent: usedPercents.length > 0 ? Math.max(...usedPercents) : undefined,
      minUsedPercent: usedPercents.length > 0 ? Math.min(...usedPercents) : undefined,
      estimatedBytesMoved: knownMovementSizes.length > 0 ? knownMovementSizes.reduce((total, size) => total + size, 0) : undefined
    },
    movements,
    reassignment: {
      version: 1,
      partitions: reassignmentPartitions
    },
    kafkaJsRequest,
    warnings: [...warnings]
  };
}

export function buildRebalancePreflight({
  planRecord,
  executionEnabled,
  reassignmentStatus,
  currentPlacements,
  brokers,
  collectors,
  now = new Date(),
  collectorMaxAgeMs = REBALANCE_COLLECTOR_MAX_AGE_MS
}: {
  planRecord: RebalancePlanRecord;
  executionEnabled: boolean;
  reassignmentStatus: RebalanceExecutionStatus;
  currentPlacements: PartitionPlacement[];
  brokers?: Broker[];
  collectors: CollectorState[];
  now?: Date;
  collectorMaxAgeMs?: number;
}): RebalancePreflight {
  const plan = planRecord.plan;
  const staleMovements = staleRebalanceMovements(plan, currentPlacements);
  const degradedMovements = degradedRebalanceMovements(plan, currentPlacements);
  const unknownSizeMovements = plan.movements.filter((movement) => movement.estimatedSizeBytes === undefined);
  const plannedBrokerIds = plannedMovementBrokerIds(plan);
  const liveBrokerIds = brokers ? new Set(brokers.map((broker) => broker.nodeId)) : undefined;
  const missingBrokerIds = liveBrokerIds ? plannedBrokerIds.filter((brokerId) => !liveBrokerIds.has(brokerId)) : [];
  const collectorByBroker = collectorsByBrokerId(collectors);
  const missingTelemetryBrokerIds = plannedBrokerIds.filter((brokerId) => !collectorByBroker.get(brokerId)?.lastSnapshot?.disk);
  const staleTelemetryBrokerIds = plannedBrokerIds.filter((brokerId) => {
    const sampledAt = collectorByBroker.get(brokerId)?.lastSnapshot?.disk?.sampledAt;
    if (!sampledAt) {
      return false;
    }
    const sampledAtMs = Date.parse(sampledAt);
    return !Number.isFinite(sampledAtMs) || now.getTime() - sampledAtMs > collectorMaxAgeMs;
  });
  const checks: RebalancePreflightCheck[] = [
    {
      id: "execution-enabled",
      label: "Execution switch",
      status: executionEnabled ? "pass" : "fail",
      detail: executionEnabled
        ? "EVENTHELM_ENABLE_REBALANCE_EXECUTION is enabled for this API process."
        : "Execution is disabled. Set EVENTHELM_ENABLE_REBALANCE_EXECUTION=true only after deployment RBAC and runbooks are ready."
    },
    {
      id: "plan-status",
      label: "Review decision",
      status: planRecord.status === "approved" ? "pass" : "fail",
      detail:
        planRecord.status === "approved"
          ? `Plan was approved by ${planRecord.reviewedBy ?? "an operator"}.`
          : `Plan status is ${planRecord.status}; approve the stored plan before execution.`,
      evidence: {
        reviewedBy: planRecord.reviewedBy,
        reviewedAt: planRecord.reviewedAt
      }
    },
    {
      id: "plan-movements",
      label: "Plan shape",
      status: plan.movements.length > 0 ? "pass" : "fail",
      detail:
        plan.movements.length > 0
          ? `${plan.movements.length} partition movement${plan.movements.length === 1 ? "" : "s"} ready for Kafka reassignment.`
          : "Plan has no partition movements; regenerate it before execution.",
      evidence: {
        movements: plan.movements.length,
        generatedAt: plan.generatedAt
      }
    },
    {
      id: "generated-executable",
      label: "Generated execution flag",
      status: plan.executable ? "pass" : "fail",
      detail: plan.executable
        ? "The stored plan was generated while execution was available."
        : plan.executionBlockedReason ?? "Stored plan is not executable; regenerate it before execution.",
      evidence: {
        executable: plan.executable
      }
    },
    {
      id: "active-reassignment",
      label: "Kafka reassignment activity",
      status: reassignmentStatus.active ? "fail" : "pass",
      detail: reassignmentStatus.active
        ? `Kafka reports ${reassignmentStatus.activePartitionCount} active partition reassignment${
            reassignmentStatus.activePartitionCount === 1 ? "" : "s"
          }.`
        : "Kafka reports no active partition reassignments.",
      evidence: {
        activeTopicCount: reassignmentStatus.activeTopicCount,
        activePartitionCount: reassignmentStatus.activePartitionCount
      }
    },
    {
      id: "placement-current",
      label: "Reviewed placement still current",
      status: staleMovements.length > 0 ? "fail" : "pass",
      detail:
        staleMovements.length > 0
          ? `${staleMovements.length} planned partition${staleMovements.length === 1 ? "" : "s"} no longer match the reviewed replica assignments.`
          : "Current Kafka replica placement still matches the reviewed plan.",
      evidence: {
        staleMovements: staleMovements.slice(0, 5).map((movement) => `${movement.topic}:${movement.partition}`)
      }
    },
    {
      id: "partition-health",
      label: "Partition health",
      status: degradedMovements.length > 0 ? "fail" : "pass",
      detail:
        degradedMovements.length > 0
          ? `${degradedMovements.length} planned partition${
              degradedMovements.length === 1 ? " is" : "s are"
            } under-replicated or have offline replicas.`
          : "Every planned partition is fully replicated with no offline replicas.",
      evidence: {
        degradedMovements: degradedMovements.slice(0, 5).map((movement) => `${movement.topic}:${movement.partition}`)
      }
    },
    {
      id: "broker-membership",
      label: "Broker membership",
      status: missingBrokerIds.length > 0 ? "fail" : "pass",
      detail:
        missingBrokerIds.length > 0
          ? `Broker${missingBrokerIds.length === 1 ? "" : "s"} ${missingBrokerIds.join(", ")} in the plan are not present in live Kafka metadata.`
          : "Every source and target broker in the plan is present in live Kafka metadata.",
      evidence: {
        brokerIds: plannedBrokerIds,
        missingBrokerIds
      }
    },
    {
      id: "movement-size-coverage",
      label: "Movement size coverage",
      status: unknownSizeMovements.length > 0 ? "fail" : "pass",
      detail:
        unknownSizeMovements.length > 0
          ? `${unknownSizeMovements.length} planned movement${unknownSizeMovements.length === 1 ? "" : "s"} lack broker-local byte estimates.`
          : "Every planned movement has a broker-local byte estimate.",
      evidence: {
        unknownSizeMovements: unknownSizeMovements.slice(0, 5).map((movement) => `${movement.topic}:${movement.partition}`)
      }
    },
    {
      id: "collector-coverage",
      label: "Collector disk coverage",
      status: missingTelemetryBrokerIds.length > 0 ? "fail" : "pass",
      detail:
        missingTelemetryBrokerIds.length > 0
          ? `Missing broker-local disk telemetry for broker${missingTelemetryBrokerIds.length === 1 ? "" : "s"} ${missingTelemetryBrokerIds.join(", ")}.`
          : plannedBrokerIds.length > 0
            ? "Every source and target broker has collector disk telemetry."
            : "Plan has no movement brokers that require collector disk telemetry.",
      evidence: {
        brokerIds: plannedBrokerIds,
        missingTelemetryBrokerIds
      }
    },
    {
      id: "collector-freshness",
      label: "Collector freshness",
      status: staleTelemetryBrokerIds.length > 0 ? "fail" : "pass",
      detail:
        staleTelemetryBrokerIds.length > 0
          ? `Disk telemetry is stale for broker${staleTelemetryBrokerIds.length === 1 ? "" : "s"} ${staleTelemetryBrokerIds.join(", ")}.`
          : "Collector disk samples for planned movement brokers are fresh.",
      evidence: {
        maxAgeSeconds: Math.round(collectorMaxAgeMs / 1000),
        staleTelemetryBrokerIds
      }
    },
    {
      id: "planner-warnings",
      label: "Planner warnings",
      status: plan.warnings.length > 0 ? "warn" : "pass",
      detail: plan.warnings.length > 0 ? plan.warnings.join(" ") : "Planner returned no warnings.",
      evidence: {
        warningCount: plan.warnings.length
      }
    }
  ];
  const failedChecks = checks.filter((check) => check.status === "fail");
  const warningChecks = checks.filter((check) => check.status === "warn");

  return {
    planId: plan.id,
    clusterId: plan.clusterId,
    checkedAt: now.toISOString(),
    executable: failedChecks.length === 0,
    blockedReasons: failedChecks.map((check) => check.detail),
    warnings: warningChecks.map((check) => check.detail),
    staleMovementCount: staleMovements.length,
    degradedMovementCount: degradedMovements.length,
    unknownSizeMovementCount: unknownSizeMovements.length,
    missingBrokerIds,
    missingTelemetryBrokerIds,
    staleTelemetryBrokerIds,
    checks
  };
}

export function staleRebalanceMovements(plan: RebalancePlan, placements: PartitionPlacement[]): RebalanceMovement[] {
  const placementByKey = new Map(placements.map((placement) => [partitionKey(placement.topic, placement.partition), placement]));
  return plan.movements.filter((movement) => {
    const placement = placementByKey.get(partitionKey(movement.topic, movement.partition));
    return !placement || !sameReplicas(placement.replicas, movement.currentReplicas);
  });
}

export function degradedRebalanceMovements(plan: RebalancePlan, placements: PartitionPlacement[]): RebalanceMovement[] {
  const placementByKey = new Map(placements.map((placement) => [partitionKey(placement.topic, placement.partition), placement]));
  return plan.movements.filter((movement) => {
    const placement = placementByKey.get(partitionKey(movement.topic, movement.partition));
    return Boolean(placement && (placement.isr.length < placement.replicas.length || placement.offlineReplicas.length > 0));
  });
}

export function evaluateRebalanceExecutionCompletion({
  plan,
  reassignmentStatus,
  currentPlacements
}: {
  plan: RebalancePlan;
  reassignmentStatus: RebalanceExecutionStatus;
  currentPlacements: PartitionPlacement[];
}) {
  const placementByKey = new Map(currentPlacements.map((placement) => [partitionKey(placement.topic, placement.partition), placement]));
  const incompleteMovements = plan.movements.filter((movement) => {
    const placement = placementByKey.get(partitionKey(movement.topic, movement.partition));
    return !placement || !sameReplicas(placement.replicas, movement.proposedReplicas);
  });
  const blockedReasons = [
    reassignmentStatus.active
      ? `Kafka still reports ${reassignmentStatus.activePartitionCount} active partition reassignment${
          reassignmentStatus.activePartitionCount === 1 ? "" : "s"
        }.`
      : undefined,
    incompleteMovements.length > 0
      ? `${incompleteMovements.length} planned partition movement${
          incompleteMovements.length === 1 ? "" : "s"
        } have not reached the proposed replica placement.`
      : undefined
  ].filter((reason): reason is string => Boolean(reason));

  return {
    complete: !reassignmentStatus.active && incompleteMovements.length === 0,
    active: reassignmentStatus.active,
    incompleteMovementCount: incompleteMovements.length,
    incompleteMovements: incompleteMovements.slice(0, 10).map((movement) => `${movement.topic}:${movement.partition}`),
    blockedReasons
  };
}

function buildBrokerPressure(brokers: Broker[], collectors: CollectorState[], placements: PartitionPlacement[]): BrokerPressure[] {
  const collectorEntries: Array<[number, CollectorState]> = [];
  for (const collector of collectors) {
    const brokerId = Number(collector.heartbeat.brokerId);
    if (Number.isFinite(brokerId)) {
      collectorEntries.push([brokerId, collector]);
    }
  }
  const collectorByBroker = new Map<number, CollectorState>(collectorEntries);

  return brokers
    .map((broker) => {
      const collector = collectorByBroker.get(broker.nodeId);
      return {
        brokerId: broker.nodeId,
        host: broker.host,
        port: broker.port,
        replicaCount: placements.filter((placement) => placement.replicas.includes(broker.nodeId)).length,
        leaderCount: placements.filter((placement) => placement.leader === broker.nodeId).length,
        logBytes: collector?.lastSnapshot?.partitions?.reduce((total, partition) => total + partition.sizeBytes, 0),
        disk: collector?.lastSnapshot?.disk
      };
    })
    .sort((left, right) => left.brokerId - right.brokerId);
}

function chooseSources(pressure: BrokerPressure[], input: RebalancePlanInput, freshDiskBrokerIds: Set<number>): number[] {
  if (input.sourceBrokerId !== undefined) {
    return freshDiskBrokerIds.has(input.sourceBrokerId) ? [input.sourceBrokerId] : [];
  }

  const withDisk = pressure.filter((broker) => freshDiskBrokerIds.has(broker.brokerId));
  const overloaded = withDisk
    .filter((broker) => (broker.disk?.usedPercent ?? 0) >= input.highWatermarkPercent)
    .sort((left, right) => (right.disk?.usedPercent ?? 0) - (left.disk?.usedPercent ?? 0));

  if (overloaded.length > 0) {
    return overloaded.map((broker) => broker.brokerId);
  }

  if (withDisk.length >= 2) {
    const sorted = [...withDisk].sort((left, right) => (right.disk?.usedPercent ?? 0) - (left.disk?.usedPercent ?? 0));
    const gap = (sorted[0]?.disk?.usedPercent ?? 0) - (sorted[sorted.length - 1]?.disk?.usedPercent ?? 0);
    if (gap >= input.minBrokerGapPercent && sorted[0]) {
      return [sorted[0].brokerId];
    }
  }

  return [];
}

function chooseTarget({
  sourceBrokerId,
  currentReplicas,
  estimatedSizeBytes,
  pressure,
  projectedReplicaCount,
  projectedUsedBytes,
  highWatermarkPercent,
  allowedTargets,
  freshDiskBrokerIds
}: {
  sourceBrokerId: number;
  currentReplicas: number[];
  estimatedSizeBytes?: number;
  pressure: BrokerPressure[];
  projectedReplicaCount: Map<number, number>;
  projectedUsedBytes: Map<number, number>;
  highWatermarkPercent: number;
  allowedTargets?: number[];
  freshDiskBrokerIds: Set<number>;
}): BrokerPressure | undefined {
  return pressure
    .filter((broker) => broker.brokerId !== sourceBrokerId)
    .filter((broker) => freshDiskBrokerIds.has(broker.brokerId))
    .filter((broker) => !currentReplicas.includes(broker.brokerId))
    .filter((broker) => !allowedTargets || allowedTargets.includes(broker.brokerId))
    .filter((broker) => {
      const projectedPercent = projectedTargetUsedPercent(broker, estimatedSizeBytes, projectedUsedBytes);
      return projectedPercent === undefined || projectedPercent < highWatermarkPercent;
    })
    .sort(
      (left, right) =>
        targetScore(left, projectedReplicaCount, projectedUsedBytes, estimatedSizeBytes) -
        targetScore(right, projectedReplicaCount, projectedUsedBytes, estimatedSizeBytes)
    )[0];
}

function targetScore(
  broker: BrokerPressure,
  projectedReplicaCount: Map<number, number>,
  projectedUsedBytes: Map<number, number>,
  estimatedSizeBytes?: number
) {
  const projectedPercent = projectedTargetUsedPercent(broker, estimatedSizeBytes, projectedUsedBytes);
  const diskScore = projectedPercent ?? broker.disk?.usedPercent ?? 50;
  return diskScore + (projectedReplicaCount.get(broker.brokerId) ?? broker.replicaCount) * 0.2;
}

function projectedTargetUsedPercent(
  broker: BrokerPressure,
  estimatedSizeBytes: number | undefined,
  projectedUsedBytes: Map<number, number>
) {
  if (estimatedSizeBytes === undefined || !broker.disk || broker.disk.totalBytes <= 0) {
    return undefined;
  }
  return ((projectedUsedBytes.get(broker.brokerId) ?? broker.disk.usedBytes) + estimatedSizeBytes) / broker.disk.totalBytes * 100;
}

function hasFreshDiskTelemetry(broker: BrokerPressure, now: Date, collectorMaxAgeMs: number) {
  if (!broker.disk || broker.disk.totalBytes <= 0) {
    return false;
  }
  const sampledAtMs = Date.parse(broker.disk.sampledAt);
  return Number.isFinite(sampledAtMs) && now.getTime() - sampledAtMs <= collectorMaxAgeMs;
}

function rebalanceReason(sourceBrokerId: number, targetBrokerId: number, pressure: BrokerPressure[], estimatedSizeBytes?: number) {
  const source = pressure.find((broker) => broker.brokerId === sourceBrokerId);
  const target = pressure.find((broker) => broker.brokerId === targetBrokerId);
  const sourceDisk = source?.disk ? `${source.disk.usedPercent.toFixed(1)}%` : "unknown";
  const targetDisk = target?.disk ? `${target.disk.usedPercent.toFixed(1)}%` : "unknown";
  const size = estimatedSizeBytes !== undefined ? ` Estimated movement size: ${formatBytes(estimatedSizeBytes)}.` : "";
  return `Move a replica from broker ${sourceBrokerId} (${sourceDisk} used) to broker ${targetBrokerId} (${targetDisk} used).${size}`;
}

function groupAssignments(movements: RebalanceMovement[]) {
  const grouped = new Map<string, RebalancePlan["kafkaJsRequest"][number]>();
  for (const movement of movements) {
    const existing = grouped.get(movement.topic) ?? {
      topic: movement.topic,
      partitionAssignment: []
    };
    existing.partitionAssignment.push({
      partition: movement.partition,
      replicas: movement.proposedReplicas
    });
    grouped.set(movement.topic, existing);
  }
  return grouped;
}

function buildPartitionSizeIndex(collectors: CollectorState[]) {
  const sizes = new Map<string, number>();
  for (const collector of collectors) {
    for (const partition of collector.lastSnapshot?.partitions ?? []) {
      const key = partitionKey(partition.topic, partition.partition);
      const current = sizes.get(key);
      if (current === undefined || partition.sizeBytes > current) {
        sizes.set(key, partition.sizeBytes);
      }
    }
  }
  return sizes;
}

function partitionSize(sizes: Map<string, number>, placement: PartitionPlacement) {
  return sizes.get(partitionKey(placement.topic, placement.partition));
}

function plannedMovementBrokerIds(plan: RebalancePlan) {
  return [...new Set(plan.movements.flatMap((movement) => [movement.sourceBrokerId, movement.targetBrokerId]))].sort((left, right) => left - right);
}

function collectorsByBrokerId(collectors: CollectorState[]) {
  const byBrokerId = new Map<number, CollectorState>();
  for (const collector of collectors) {
    const brokerId = Number(collector.heartbeat.brokerId);
    if (Number.isFinite(brokerId)) {
      byBrokerId.set(brokerId, collector);
    }
  }
  return byBrokerId;
}

function partitionKey(topic: string, partition: number) {
  return `${topic}\u0000${partition}`;
}

function sameReplicas(left: number[], right: number[]) {
  return left.length === right.length && left.every((replica, index) => replica === right[index]);
}

function formatBytes(value: number) {
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let next = value;
  let unit = 0;
  while (next >= 1024 && unit < units.length - 1) {
    next /= 1024;
    unit += 1;
  }
  return `${next.toFixed(next >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}
