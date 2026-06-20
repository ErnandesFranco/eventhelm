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

  if (brokersWithDisk.length < brokers.length) {
    warnings.add("Disk telemetry is missing for at least one broker. Run a collector with BROKER_DATA_PATH on every broker host.");
  }

  warnings.add("Partition byte size is not available yet. This plan balances replica placement against broker disk pressure, not exact bytes.");

  const skippedUnderReplicated = placements.filter((placement) => placement.isr.length < placement.replicas.length).length;
  if (skippedUnderReplicated > 0) {
    warnings.add(`${skippedUnderReplicated} under-replicated partitions were skipped.`);
  }

  const projectedReplicaCount = new Map(pressure.map((broker) => [broker.brokerId, broker.replicaCount]));
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
      .sort((left, right) => Number(left.leader === sourceBrokerId) - Number(right.leader === sourceBrokerId));

    for (const placement of candidates) {
      if (movements.length >= input.maxMovements) {
        break;
      }

      const targetBroker = chooseTarget({
        sourceBrokerId,
        currentReplicas: placement.replicas,
        pressure,
        projectedReplicaCount,
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
        leaderMove: placement.leader === sourceBrokerId,
        reason: rebalanceReason(sourceBrokerId, targetBroker.brokerId, pressure)
      });

      projectedReplicaCount.set(sourceBrokerId, Math.max(0, (projectedReplicaCount.get(sourceBrokerId) ?? 0) - 1));
      projectedReplicaCount.set(targetBroker.brokerId, (projectedReplicaCount.get(targetBroker.brokerId) ?? 0) + 1);
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
  const executionBlockedReason = input.executionEnabled
    ? movements.length === 0
      ? "No partition movements were generated."
      : undefined
    : "Rebalance execution is locked. Set EVENTHELM_ENABLE_REBALANCE_EXECUTION=true after approvals and RBAC are configured.";

  return {
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
      estimatedBytesMoved: undefined
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
  pressure,
  projectedReplicaCount,
  allowedTargets
}: {
  sourceBrokerId: number;
  currentReplicas: number[];
  pressure: BrokerPressure[];
  projectedReplicaCount: Map<number, number>;
  allowedTargets?: number[];
}): BrokerPressure | undefined {
  return pressure
    .filter((broker) => broker.brokerId !== sourceBrokerId)
    .filter((broker) => !currentReplicas.includes(broker.brokerId))
    .filter((broker) => !allowedTargets || allowedTargets.includes(broker.brokerId))
    .sort((left, right) => targetScore(left, projectedReplicaCount) - targetScore(right, projectedReplicaCount))[0];
}

function targetScore(broker: BrokerPressure, projectedReplicaCount: Map<number, number>) {
  const diskScore = broker.disk?.usedPercent ?? 50;
  return diskScore + (projectedReplicaCount.get(broker.brokerId) ?? broker.replicaCount) * 0.2;
}

function rebalanceReason(sourceBrokerId: number, targetBrokerId: number, pressure: BrokerPressure[]) {
  const source = pressure.find((broker) => broker.brokerId === sourceBrokerId);
  const target = pressure.find((broker) => broker.brokerId === targetBrokerId);
  const sourceDisk = source?.disk ? `${source.disk.usedPercent.toFixed(1)}%` : "unknown";
  const targetDisk = target?.disk ? `${target.disk.usedPercent.toFixed(1)}%` : "unknown";
  return `Move a replica from broker ${sourceBrokerId} (${sourceDisk} used) to broker ${targetBrokerId} (${targetDisk} used).`;
}

function groupAssignments(movements: RebalanceMovement[]) {
  const grouped = new Map< string, RebalancePlan["kafkaJsRequest"][number]>();
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
