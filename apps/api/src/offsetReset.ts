import { createHash } from "node:crypto";
import type { ConsumerOffsetResetPreview, ConsumerOffsetResetRequest } from "./types.js";

type OffsetResetGroup = {
  groupId: string;
  protocolType: string;
  state?: string;
  members: unknown[];
};

type CommittedTopicOffsets = {
  topic: string;
  partitions: Array<{
    partition: number;
    offset: string;
    metadata?: string | null;
  }>;
};

type TopicLogOffset = {
  partition: number;
  high: string;
  low: string;
};

type BuildOffsetResetPreviewInput = {
  group: OffsetResetGroup;
  request: ConsumerOffsetResetRequest;
  committedTopic: CommittedTopicOffsets;
  logOffsets: TopicLogOffset[];
  generatedAt?: string;
};

export function buildOffsetResetPreview(input: BuildOffsetResetPreviewInput): ConsumerOffsetResetPreview {
  const request = normalizeRequest(input.request);
  const committedByPartition = new Map(input.committedTopic.partitions.map((partition) => [partition.partition, partition]));
  const logByPartition = new Map(input.logOffsets.map((partition) => [partition.partition, partition]));
  const selectedPartitions = request.partitions ?? input.logOffsets.map((partition) => partition.partition).sort((left, right) => left - right);

  const partitions = selectedPartitions.map((partitionId) => {
    const committed = committedByPartition.get(partitionId);
    const logOffset = logByPartition.get(partitionId);
    const lowOffset = logOffset?.low ?? "0";
    const logEndOffset = logOffset?.high ?? "0";
    const low = parseOffsetBigInt(lowOffset);
    const high = parseOffsetBigInt(logEndOffset);
    const current = parseOffsetBigInt(committed?.offset);
    const proposedOffset = proposedOffsetFor(request, lowOffset, logEndOffset);
    const proposed = parseOffsetBigInt(proposedOffset);
    let blockedReason: string | undefined;

    if (!logOffset || low === undefined || high === undefined) {
      blockedReason = "Partition offset bounds are unavailable.";
    } else if (proposed === undefined) {
      blockedReason = "Proposed offset is invalid.";
    } else if (proposed < low || proposed > high) {
      blockedReason = `Proposed offset must be between ${lowOffset} and ${logEndOffset}.`;
    }

    const lagBefore = current === undefined || high === undefined ? undefined : positiveDifference(high, current);
    const lagAfter = proposed === undefined || high === undefined ? undefined : positiveDifference(high, proposed);
    const delta = current === undefined || proposed === undefined ? undefined : proposed - current;

    return {
      partition: partitionId,
      currentOffset: current === undefined ? undefined : committed?.offset,
      lowOffset,
      logEndOffset,
      proposedOffset,
      lagBefore: lagBefore?.toString(),
      lagAfter: lagAfter?.toString(),
      delta: delta?.toString(),
      blockedReason
    };
  });

  const warnings = warningsFor(input.group, partitions);
  const executable =
    partitions.length > 0 &&
    partitions.every((partition) => !partition.blockedReason) &&
    isResettableGroupState(input.group.state);

  const previewWithoutToken = {
    groupId: input.group.groupId,
    generatedAt: input.generatedAt ?? new Date().toISOString(),
    state: input.group.state,
    members: input.group.members.length,
    protocolType: input.group.protocolType,
    request,
    executable,
    reviewToken: "",
    warnings,
    summary: {
      partitions: partitions.length,
      executablePartitions: partitions.filter((partition) => !partition.blockedReason).length,
      lagBefore: sumOffsets(partitions.map((partition) => partition.lagBefore)).toString(),
      lagAfter: sumOffsets(partitions.map((partition) => partition.lagAfter)).toString(),
      messagesSkipped: sumOffsets(
        partitions.map((partition) => {
          const delta = parseSignedBigInt(partition.delta);
          return delta !== undefined && delta > 0n ? delta.toString() : undefined;
        })
      ).toString(),
      messagesToReplay: sumOffsets(
        partitions.map((partition) => {
          const delta = parseSignedBigInt(partition.delta);
          return delta !== undefined && delta < 0n ? (-delta).toString() : undefined;
        })
      ).toString()
    },
    topics: [
      {
        topic: request.topic,
        partitions
      }
    ]
  };

  return {
    ...previewWithoutToken,
    reviewToken: hashPreview(previewWithoutToken)
  };
}

function normalizeRequest(request: ConsumerOffsetResetRequest): ConsumerOffsetResetRequest {
  const partitions = request.partitions
    ? [...new Set(request.partitions)].sort((left, right) => left - right)
    : undefined;
  return {
    topic: request.topic,
    mode: request.mode,
    partitions,
    offset: request.mode === "absolute" ? String(request.offset ?? "") : undefined
  };
}

function proposedOffsetFor(request: ConsumerOffsetResetRequest, lowOffset: string, logEndOffset: string) {
  if (request.mode === "earliest") {
    return lowOffset;
  }
  if (request.mode === "latest") {
    return logEndOffset;
  }
  return String(request.offset ?? "");
}

function warningsFor(group: OffsetResetGroup, partitions: Array<{ currentOffset?: string; blockedReason?: string }>) {
  const warnings: string[] = [];
  if (!isResettableGroupState(group.state)) {
    warnings.push(`Consumer group must be stopped before offsets can be reset. Current state: ${group.state ?? "unknown"}.`);
  }
  if (partitions.length === 0) {
    warnings.push("No partitions were found for the requested topic.");
  }
  if (partitions.some((partition) => partition.currentOffset === undefined)) {
    warnings.push("Some partitions have no committed offset; execution will create an explicit committed offset.");
  }
  if (partitions.some((partition) => partition.blockedReason)) {
    warnings.push("One or more partitions cannot be reset with the requested target.");
  }
  return warnings;
}

function isResettableGroupState(state?: string) {
  return state === "Empty" || state === "Dead";
}

function parseOffsetBigInt(offset?: string): bigint | undefined {
  if (!offset || !/^-?\d+$/.test(offset)) {
    return undefined;
  }
  const parsed = BigInt(offset);
  return parsed < 0n ? undefined : parsed;
}

function parseSignedBigInt(offset?: string): bigint | undefined {
  if (!offset || !/^-?\d+$/.test(offset)) {
    return undefined;
  }
  return BigInt(offset);
}

function positiveDifference(left: bigint, right: bigint) {
  return left > right ? left - right : 0n;
}

function sumOffsets(offsets: Array<string | undefined>) {
  return offsets.reduce((total, offset) => total + (parseOffsetBigInt(offset) ?? 0n), 0n);
}

function hashPreview(preview: Omit<ConsumerOffsetResetPreview, "reviewToken">) {
  const tokenPayload = {
    groupId: preview.groupId,
    state: preview.state,
    members: preview.members,
    protocolType: preview.protocolType,
    request: preview.request,
    executable: preview.executable,
    summary: preview.summary,
    topics: preview.topics
  };
  return createHash("sha256").update(JSON.stringify(tokenPayload)).digest("hex");
}
