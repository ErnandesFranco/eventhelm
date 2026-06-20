import cors from "@fastify/cors";
import Fastify from "fastify";
import { z } from "zod";
import { recordAudit, listAuditEvents } from "./audit.js";
import { upsertHeartbeat, upsertSnapshot, listCollectors } from "./collectors.js";
import { getPort, loadClusters } from "./config.js";
import {
  browseMessages,
  createTopic,
  describeCluster,
  listConsumerGroups,
  listTopics,
  produceMessage
} from "./kafka.js";

const clusters = loadClusters();
const app = Fastify({
  logger: true
});

await app.register(cors, {
  origin: true
});

function getCluster(clusterId: string) {
  const cluster = clusters.find((candidate) => candidate.id === clusterId);
  if (!cluster) {
    const error = new Error(`Cluster '${clusterId}' is not configured`);
    (error as Error & { statusCode: number }).statusCode = 404;
    throw error;
  }
  return cluster;
}

app.get("/health", async () => ({
  ok: true,
  service: "okcp-api",
  timestamp: new Date().toISOString()
}));

app.get("/api/clusters", async () =>
  clusters.map((cluster) => ({
    id: cluster.id,
    name: cluster.name,
    brokers: cluster.brokers
  }))
);

app.get("/api/clusters/:clusterId/overview", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const cluster = getCluster(params.clusterId);
  const [description, topics, groups] = await Promise.all([
    describeCluster(cluster),
    listTopics(cluster),
    listConsumerGroups(cluster)
  ]);

  return {
    clusterId: cluster.id,
    clusterName: cluster.name,
    kafkaClusterId: description.clusterId,
    controllerId: description.controller,
    brokerCount: description.brokers.length,
    topicCount: topics.filter((topic) => !topic.isInternal).length,
    internalTopicCount: topics.filter((topic) => topic.isInternal).length,
    consumerGroupCount: groups.length,
    brokers: description.brokers,
    collectors: listCollectors().filter((collector) => collector.heartbeat.clusterId === cluster.id)
  };
});

app.get("/api/clusters/:clusterId/topics", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return listTopics(getCluster(params.clusterId));
});

app.post("/api/clusters/:clusterId/topics", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      name: z.string().min(1),
      partitions: z.coerce.number().int().min(1).max(1000),
      replicationFactor: z.coerce.number().int().min(1).max(10),
      retentionMs: z.coerce.number().int().positive().optional(),
      cleanupPolicy: z.enum(["delete", "compact"]).optional()
    })
    .parse(request.body);

  const created = await createTopic(getCluster(params.clusterId), body);
  recordAudit({
    actor: "system",
    action: "topic.create",
    clusterId: params.clusterId,
    resourceType: "topic",
    resourceName: body.name,
    details: body
  });

  return { created };
});

app.get("/api/clusters/:clusterId/consumer-groups", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return listConsumerGroups(getCluster(params.clusterId));
});

app.post("/api/clusters/:clusterId/messages/produce", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      topic: z.string().min(1),
      key: z.string().optional(),
      value: z.string().min(1),
      headers: z.record(z.string()).optional()
    })
    .parse(request.body);

  const result = await produceMessage(getCluster(params.clusterId), body);
  recordAudit({
    actor: "system",
    action: "message.produce",
    clusterId: params.clusterId,
    resourceType: "topic",
    resourceName: body.topic,
    details: { key: body.key, size: body.value.length }
  });

  return { result };
});

app.get("/api/clusters/:clusterId/messages", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const query = z
    .object({
      topic: z.string().min(1),
      limit: z.coerce.number().int().min(1).max(100).default(25),
      fromBeginning: z.coerce.boolean().default(true),
      timeoutMs: z.coerce.number().int().min(500).max(10000).default(3000)
    })
    .parse(request.query);

  return browseMessages(getCluster(params.clusterId), query);
});

app.get("/api/collectors", async () => listCollectors());

app.post("/api/collectors/heartbeat", async (request) => {
  const body = z
    .object({
      collectorId: z.string().min(1),
      clusterId: z.string().min(1),
      brokerId: z.string().min(1),
      hostname: z.string().min(1),
      version: z.string().min(1),
      startedAt: z.string().min(1),
      observedAt: z.string().min(1)
    })
    .parse(request.body);

  return upsertHeartbeat(body);
});

app.post("/api/collectors/snapshot", async (request) => {
  const body = z
    .object({
      collectorId: z.string().min(1),
      clusterId: z.string().min(1),
      brokerId: z.string().min(1),
      hostname: z.string().min(1),
      version: z.string().min(1),
      startedAt: z.string().min(1),
      observedAt: z.string().min(1),
      brokerCount: z.number(),
      topicCount: z.number(),
      controllerId: z.number().optional(),
      kafkaClusterId: z.string().optional(),
      brokers: z.array(
        z.object({
          nodeId: z.number(),
          host: z.string(),
          port: z.number()
        })
      )
    })
    .parse(request.body);

  return upsertSnapshot(body);
});

app.get("/api/audit", async () => listAuditEvents());

const port = getPort();
await app.listen({ host: "0.0.0.0", port });
