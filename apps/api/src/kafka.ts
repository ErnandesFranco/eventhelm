import { Kafka, logLevel } from "kafkajs";
import { buildOffsetResetPreview } from "./offsetReset.js";
import type {
  ClusterConfig,
  ConsumerGroupLag,
  ConsumerGroupSummary,
  ConsumerOffsetResetPreview,
  ConsumerOffsetResetRequest,
  PartitionPlacement,
  RebalancePlan,
  TopicSummary
} from "./types.js";

type KafkaAdmin = ReturnType<ReturnType<typeof createKafka>["admin"]>;
type GroupDescription = {
  groupId: string;
  protocolType: string;
  state?: string;
  members: unknown[];
};

function createKafka(cluster: ClusterConfig): Kafka {
  return new Kafka({
    clientId: "eventhelm-api",
    brokers: cluster.brokers,
    ssl: cluster.ssl,
    sasl: toKafkaSasl(cluster.sasl),
    logLevel: logLevel.ERROR
  });
}

function toKafkaSasl(sasl: ClusterConfig["sasl"]) {
  if (!sasl) {
    return undefined;
  }

  switch (sasl.mechanism) {
    case "plain":
      return { mechanism: "plain" as const, username: sasl.username, password: sasl.password };
    case "scram-sha-256":
      return { mechanism: "scram-sha-256" as const, username: sasl.username, password: sasl.password };
    case "scram-sha-512":
      return { mechanism: "scram-sha-512" as const, username: sasl.username, password: sasl.password };
  }
}

export async function describeCluster(cluster: ClusterConfig) {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    return await admin.describeCluster();
  } finally {
    await admin.disconnect();
  }
}

export async function listTopics(cluster: ClusterConfig): Promise<TopicSummary[]> {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    const metadata = await admin.fetchTopicMetadata();
    return metadata.topics
      .map((topic) => {
        const replicas = topic.partitions[0]?.replicas.length ?? 0;
        return {
          name: topic.name,
          partitions: topic.partitions.length,
          replicas,
          isInternal: topic.name.startsWith("__")
        };
      })
      .sort((left, right) => left.name.localeCompare(right.name));
  } finally {
    await admin.disconnect();
  }
}

export async function listPartitionPlacements(cluster: ClusterConfig, includeInternal = false): Promise<PartitionPlacement[]> {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    const metadata = await admin.fetchTopicMetadata();
    return metadata.topics
      .filter((topic) => includeInternal || !topic.name.startsWith("__"))
      .flatMap((topic) =>
        topic.partitions.map((partition) => ({
          topic: topic.name,
          partition: partition.partitionId,
          leader: partition.leader,
          replicas: partition.replicas,
          isr: partition.isr,
          offlineReplicas: partition.offlineReplicas ?? [],
          isInternal: topic.name.startsWith("__")
        }))
      )
      .sort((left, right) => left.topic.localeCompare(right.topic) || left.partition - right.partition);
  } finally {
    await admin.disconnect();
  }
}

export async function alterPartitionAssignments(cluster: ClusterConfig, topics: RebalancePlan["kafkaJsRequest"]) {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    await admin.alterPartitionReassignments({ topics, timeout: 10_000 });
  } finally {
    await admin.disconnect();
  }
}

export async function createTopic(
  cluster: ClusterConfig,
  input: {
    name: string;
    partitions: number;
    replicationFactor: number;
    retentionMs?: number;
    cleanupPolicy?: "delete" | "compact";
  }
) {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    const configEntries = [
      input.retentionMs ? { name: "retention.ms", value: String(input.retentionMs) } : undefined,
      input.cleanupPolicy ? { name: "cleanup.policy", value: input.cleanupPolicy } : undefined
    ].filter(Boolean) as Array<{ name: string; value: string }>;

    try {
      const created = await admin.createTopics({
        waitForLeaders: false,
        topics: [
          {
            topic: input.name,
            numPartitions: input.partitions,
            replicationFactor: input.replicationFactor,
            configEntries
          }
        ]
      });
      await waitForTopic(admin, input.name);
      return created;
    } catch (error) {
      if (isKafkaMetadataRace(error) && (await topicExists(admin, input.name))) {
        return true;
      }
      throw error;
    }
  } finally {
    await admin.disconnect();
  }
}

async function waitForTopic(admin: KafkaAdmin, topic: string) {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    if (await topicExists(admin, topic)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

async function topicExists(admin: KafkaAdmin, topic: string) {
  try {
    const topics = await admin.listTopics();
    return topics.includes(topic);
  } catch (error) {
    if (isKafkaMetadataRace(error)) {
      return false;
    }
    throw error;
  }
}

function isKafkaMetadataRace(error: unknown) {
  return typeof error === "object" && error !== null && "code" in error && (error as { code: unknown }).code === 3;
}

export async function listConsumerGroups(cluster: ClusterConfig): Promise<ConsumerGroupSummary[]> {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    const listed = await admin.listGroups();
    if (listed.groups.length === 0) {
      return [];
    }

    const described = await admin.describeGroups(listed.groups.map((group) => group.groupId));
    const summaries = await mapWithConcurrency(described.groups as GroupDescription[], 4, async (group) => {
      const lag = await buildConsumerGroupLag(admin, group);
      return {
        groupId: group.groupId,
        protocolType: group.protocolType,
        state: group.state,
        members: group.members.length,
        lag: {
          total: lag.totalLag,
          topics: lag.topics.length,
          partitions: lag.topics.reduce((total, topic) => total + topic.partitions.length, 0),
          unknownOffsets: lag.unknownOffsets
        }
      };
    });

    return summaries.sort((left, right) => left.groupId.localeCompare(right.groupId));
  } finally {
    await admin.disconnect();
  }
}

export async function describeConsumerGroupLag(cluster: ClusterConfig, groupId: string): Promise<ConsumerGroupLag> {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    const described = await admin.describeGroups([groupId]);
    const group = described.groups.find((candidate) => candidate.groupId === groupId) as GroupDescription | undefined;
    if (!group) {
      throw new Error(`Consumer group '${groupId}' was not found`);
    }
    return buildConsumerGroupLag(admin, group);
  } finally {
    await admin.disconnect();
  }
}

export async function previewConsumerGroupOffsetReset(
  cluster: ClusterConfig,
  groupId: string,
  request: ConsumerOffsetResetRequest
): Promise<ConsumerOffsetResetPreview> {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    return buildConsumerGroupOffsetResetPreview(admin, groupId, request);
  } finally {
    await admin.disconnect();
  }
}

export async function executeConsumerGroupOffsetReset(
  cluster: ClusterConfig,
  groupId: string,
  request: ConsumerOffsetResetRequest,
  reviewToken: string
): Promise<ConsumerOffsetResetPreview> {
  const admin = createKafka(cluster).admin();
  await admin.connect();
  try {
    const preview = await buildConsumerGroupOffsetResetPreview(admin, groupId, request);
    if (preview.reviewToken !== reviewToken) {
      throw new Error("Offset reset preview is stale. Generate a fresh preview before executing.");
    }
    if (!preview.executable) {
      throw new Error(preview.warnings[0] ?? "Offset reset preview is not executable.");
    }

    for (const topic of preview.topics) {
      await admin.setOffsets({
        groupId,
        topic: topic.topic,
        partitions: topic.partitions.map((partition) => ({
          partition: partition.partition,
          offset: partition.proposedOffset
        }))
      });
    }

    return preview;
  } finally {
    await admin.disconnect();
  }
}

async function buildConsumerGroupOffsetResetPreview(
  admin: KafkaAdmin,
  groupId: string,
  request: ConsumerOffsetResetRequest
) {
  const described = await admin.describeGroups([groupId]);
  const group = described.groups.find((candidate) => candidate.groupId === groupId) as GroupDescription | undefined;
  if (!group) {
    throw new Error(`Consumer group '${groupId}' was not found`);
  }

  const [committedTopic] = await admin.fetchOffsets({ groupId, topics: [request.topic] });
  const logOffsets = await admin.fetchTopicOffsets(request.topic);

  return buildOffsetResetPreview({
    group,
    request,
    committedTopic: committedTopic ?? { topic: request.topic, partitions: [] },
    logOffsets
  });
}

async function buildConsumerGroupLag(admin: KafkaAdmin, group: GroupDescription): Promise<ConsumerGroupLag> {
  const committedOffsets = await admin.fetchOffsets({ groupId: group.groupId });
  const topics = await Promise.all(
    committedOffsets
      .filter((topic) => topic.partitions.length > 0)
      .map(async (topicOffsets) => {
        const logOffsets = new Map(
          (await admin.fetchTopicOffsets(topicOffsets.topic)).map((partition) => [partition.partition, partition])
        );
        const partitions = topicOffsets.partitions
          .map((partition) => {
            const logOffset = logOffsets.get(partition.partition);
            const currentOffset = parseOffset(partition.offset);
            const logEndOffset = logOffset?.high ?? "0";
            const logEnd = parseOffset(logEndOffset);
            const lag =
              currentOffset === undefined || logEnd === undefined ? undefined : Math.max(0, logEnd - currentOffset);

            return {
              partition: partition.partition,
              currentOffset: currentOffset === undefined ? undefined : partition.offset,
              logEndOffset,
              lowOffset: logOffset?.low ?? "0",
              lag,
              metadata: partition.metadata
            };
          })
          .sort((left, right) => left.partition - right.partition);

        return {
          topic: topicOffsets.topic,
          totalLag: partitions.reduce((total, partition) => total + (partition.lag ?? 0), 0),
          partitions
        };
      })
  );

  return {
    groupId: group.groupId,
    generatedAt: new Date().toISOString(),
    state: group.state,
    members: group.members.length,
    protocolType: group.protocolType,
    totalLag: topics.reduce((total, topic) => total + topic.totalLag, 0),
    unknownOffsets: topics.reduce(
      (total, topic) => total + topic.partitions.filter((partition) => partition.lag === undefined).length,
      0
    ),
    topics: topics.sort((left, right) => right.totalLag - left.totalLag || left.topic.localeCompare(right.topic))
  };
}

function parseOffset(offset: string, fallback?: number): number | undefined {
  const parsed = Number(offset);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    return fallback;
  }
  return parsed;
}

async function mapWithConcurrency<T, R>(items: T[], concurrency: number, mapper: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = [];
  let nextIndex = 0;

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, async () => {
      while (nextIndex < items.length) {
        const index = nextIndex;
        nextIndex += 1;
        results[index] = await mapper(items[index]);
      }
    })
  );

  return results;
}

export async function produceMessage(
  cluster: ClusterConfig,
  input: {
    topic: string;
    key?: string;
    value: string;
    headers?: Record<string, string>;
  }
) {
  const producer = createKafka(cluster).producer();
  await producer.connect();
  try {
    return await producer.send({
      topic: input.topic,
      messages: [
        {
          key: input.key,
          value: input.value,
          headers: input.headers
        }
      ]
    });
  } finally {
    await producer.disconnect();
  }
}

export async function browseMessages(
  cluster: ClusterConfig,
  input: {
    topic: string;
    limit: number;
    fromBeginning: boolean;
    timeoutMs: number;
  }
) {
  const kafka = createKafka(cluster);
  const consumer = kafka.consumer({
    groupId: `eventhelm-browser-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    allowAutoTopicCreation: false
  });
  const messages: Array<{
    topic: string;
    partition: number;
    offset: string;
    key?: string;
    value?: string;
    timestamp: string;
  }> = [];

  let resolved = false;
  let resolveDone: () => void = () => undefined;
  const done = new Promise<void>((resolve) => {
    resolveDone = () => {
      if (!resolved) {
        resolved = true;
        resolve();
      }
    };
  });

  await consumer.connect();
  try {
    await consumer.subscribe({ topic: input.topic, fromBeginning: input.fromBeginning });
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        messages.push({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString("utf8"),
          value: message.value?.toString("utf8"),
          timestamp: new Date(Number(message.timestamp)).toISOString()
        });

        if (messages.length >= input.limit) {
          resolveDone();
        }
      }
    });

    const timeout = setTimeout(resolveDone, input.timeoutMs);
    await done;
    clearTimeout(timeout);
    await consumer.stop();
    return messages;
  } finally {
    await consumer.disconnect();
  }
}
