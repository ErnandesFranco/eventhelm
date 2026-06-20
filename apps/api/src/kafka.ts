import { Kafka, logLevel } from "kafkajs";
import type { ClusterConfig, ConsumerGroupSummary, TopicSummary } from "./types.js";

function createKafka(cluster: ClusterConfig): Kafka {
  return new Kafka({
    clientId: "okcp-api",
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

async function waitForTopic(admin: ReturnType<ReturnType<typeof createKafka>["admin"]>, topic: string) {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    if (await topicExists(admin, topic)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
}

async function topicExists(admin: ReturnType<ReturnType<typeof createKafka>["admin"]>, topic: string) {
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
    return described.groups
      .map((group) => ({
        groupId: group.groupId,
        protocolType: group.protocolType,
        state: group.state,
        members: group.members.length
      }))
      .sort((left, right) => left.groupId.localeCompare(right.groupId));
  } finally {
    await admin.disconnect();
  }
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
    groupId: `okcp-browser-${Date.now()}-${Math.random().toString(16).slice(2)}`,
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
