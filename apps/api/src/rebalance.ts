import { nanoid } from "nanoid";
import type { CollectorState, PartitionPlacement, RebalanceMovement, RebalancePlan } from "./types.js";

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
  input
}: {
  clusterId: string;
  brokers: Broker[];
  collectors: CollectorState[];
  placements: PartitionPlacement[];
  input: RebalancePlanInput;
}): RebalancePlan {
  const warnings = new Set<string>();
  const pressure = buildBrokerPressure(brokers, collectors, placements);
  const brokersWithDisk = pressure.filter((broker) => broker.disk);
  const partitionSizes = buildPartitionSizeIndex(collectors);
  const eligiblePlacements = placements.filter((placement) => input.includeInternal || !placement.isInternal);
  const sizedPlacements = eligiblePlacements.filter((placement) => partitionSizes.has(partitionKey(placement.topic, placement.partition)));

  if (brokersWithDisk.length < brokers.length) {
    warnings.add("Disk telemetry is missing for at least one broker. Run a collector with BROKER_DATA_PATH on every broker host.");
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
  const sourceBrokerIds = chooseSources(pressure, input);

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
        allowedTargets: input.targetBrokerIds
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

  const executionBlockedReason = input.executionEnabled
    ? movements.length === 0
      ? "No partition movements were generated."
      : undefined
    : "Rebalance execution is locked. Set EVENTHELM_ENABLE_REBALANCE_EXECUTION=true after approvals and RBAC are configured.";

  return {
    id: nanoid(),
    clusterId,
    generatedAt: new Date().toISOString(),
    strategy: "disk-pressure",
    executable: input.executionEnabled && movements.length > 0,
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

function chooseSources(pressure: BrokerPressure[], input: RebalancePlanInput): number[] {
  if (input.sourceBrokerId !== undefined) {
    return [input.sourceBrokerId];
  }

  const withDisk = pressure.filter((broker) => broker.disk);
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
  allowedTargets
}: {
  sourceBrokerId: number;
  currentReplicas: number[];
  estimatedSizeBytes?: number;
  pressure: BrokerPressure[];
  projectedReplicaCount: Map<number, number>;
  projectedUsedBytes: Map<number, number>;
  highWatermarkPercent: number;
  allowedTargets?: number[];
}): BrokerPressure | undefined {
  return pressure
    .filter((broker) => broker.brokerId !== sourceBrokerId)
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

function partitionKey(topic: string, partition: number) {
  return `${topic}\u0000${partition}`;
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
