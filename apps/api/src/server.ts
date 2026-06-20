import cors from "@fastify/cors";
import Fastify from "fastify";
import { z } from "zod";
import { listAdvisorAgents, runAdvisorAgents } from "./agents.js";
import { recordAudit, listAuditEvents } from "./audit.js";
import { upsertHeartbeat, upsertSnapshot, listCollectors } from "./collectors.js";
import { getCorsOrigin, getPort, getSecurityStatus, isRebalanceExecutionEnabled, loadClusters } from "./config.js";
import { closeDatabase, initDatabase, persistenceMode } from "./db.js";
import {
  alterPartitionAssignments,
  browseMessages,
  createTopic,
  describeCluster,
  listConsumerGroups,
  listPartitionPlacements,
  listTopics,
  produceMessage
} from "./kafka.js";
import { buildDiskRebalancePlan } from "./rebalance.js";
import { actorFromRequest, assertCollectorAllowed, assertWriteAllowed } from "./security.js";

const clusters = loadClusters();
const app = Fastify({
  logger: true
});

await initDatabase();

await app.register(cors, {
  origin: getCorsOrigin()
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

function badRequest(message: string) {
  const error = new Error(message);
  (error as Error & { statusCode: number }).statusCode = 400;
  return error;
}

function forbidden(message: string) {
  const error = new Error(message);
  (error as Error & { statusCode: number }).statusCode = 403;
  return error;
}

app.get("/health", async () => ({
  ok: true,
  service: "eventhelm-api",
  persistence: persistenceMode(),
  timestamp: new Date().toISOString()
}));

app.get("/api/security/status", async () => getSecurityStatus());

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
    collectors: (await listCollectors()).filter((collector) => collector.heartbeat.clusterId === cluster.id)
  };
});

app.get("/api/clusters/:clusterId/topics", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return listTopics(getCluster(params.clusterId));
});

app.post("/api/clusters/:clusterId/topics", async (request) => {
  assertWriteAllowed(request);
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      name: z.string().min(1),
      partitions: z.coerce.number().int().min(1).max(200),
      replicationFactor: z.coerce.number().int().min(1).max(10),
      retentionMs: z.coerce.number().int().positive().optional(),
      cleanupPolicy: z.enum(["delete", "compact"]).optional()
    })
    .parse(request.body);

  if (body.name.startsWith("__")) {
    throw badRequest("EventHelm will not create internal Kafka topics.");
  }

  if (!/^[a-z0-9]+([._-][a-z0-9]+)+$/.test(body.name)) {
    throw badRequest("Topic names must use lowercase domain.event style segments.");
  }

  const cluster = getCluster(params.clusterId);
  const description = await describeCluster(cluster);
  if (body.replicationFactor > description.brokers.length) {
    throw badRequest(`Replication factor ${body.replicationFactor} exceeds broker count ${description.brokers.length}.`);
  }

  const created = await createTopic(cluster, body);
  await recordAudit({
    actor: actorFromRequest(request),
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
  assertWriteAllowed(request);
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      topic: z.string().min(1),
      key: z.string().optional(),
      value: z.string().min(1).max(1_000_000),
      headers: z.record(z.string()).optional()
    })
    .parse(request.body);

  if (body.topic.startsWith("__")) {
    throw badRequest("EventHelm will not produce messages to internal Kafka topics.");
  }

  const result = await produceMessage(getCluster(params.clusterId), body);
  await recordAudit({
    actor: actorFromRequest(request),
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

app.post("/api/clusters/:clusterId/rebalance/plan", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      maxMovements: z.coerce.number().int().min(1).max(100).default(12),
      includeInternal: z.boolean().default(false),
      sourceBrokerId: z.coerce.number().int().nonnegative().optional(),
      targetBrokerIds: z.array(z.coerce.number().int().nonnegative()).optional(),
      highWatermarkPercent: z.coerce.number().min(50).max(99).default(85),
      minBrokerGapPercent: z.coerce.number().min(1).max(60).default(10)
    })
    .parse(request.body ?? {});

  const cluster = getCluster(params.clusterId);
  const [description, placements] = await Promise.all([
    describeCluster(cluster),
    listPartitionPlacements(cluster, body.includeInternal)
  ]);
  const plan = buildDiskRebalancePlan({
    clusterId: params.clusterId,
    brokers: description.brokers,
    collectors: (await listCollectors()).filter((collector) => collector.heartbeat.clusterId === params.clusterId),
    placements,
    input: {
      ...body,
      executionEnabled: isRebalanceExecutionEnabled()
    }
  });

  await recordAudit({
    actor: actorFromRequest(request),
    action: "rebalance.plan",
    clusterId: params.clusterId,
    resourceType: "cluster",
    resourceName: params.clusterId,
    details: {
      movements: plan.summary.movements,
      sources: plan.summary.sourceBrokerIds,
      targets: plan.summary.targetBrokerIds
    }
  });

  return plan;
});

app.post("/api/clusters/:clusterId/rebalance/execute", async (request) => {
  assertWriteAllowed(request);
  if (!isRebalanceExecutionEnabled()) {
    throw forbidden("Rebalance execution is locked. Set EVENTHELM_ENABLE_REBALANCE_EXECUTION=true after approvals and RBAC are configured.");
  }

  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      topics: z.array(
        z.object({
          topic: z.string().min(1),
          partitionAssignment: z.array(
            z.object({
              partition: z.number().int().nonnegative(),
              replicas: z.array(z.number().int().nonnegative()).min(1)
            })
          )
        })
      )
    })
    .parse(request.body);

  await alterPartitionAssignments(getCluster(params.clusterId), body.topics);
  await recordAudit({
    actor: actorFromRequest(request),
    action: "rebalance.execute",
    clusterId: params.clusterId,
    resourceType: "cluster",
    resourceName: params.clusterId,
    details: { topics: body.topics.length }
  });

  return { accepted: true };
});

app.get("/api/collectors", async () => listCollectors());

app.post("/api/collectors/heartbeat", async (request) => {
  assertCollectorAllowed(request);
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
  assertCollectorAllowed(request);
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
      disk: z
        .object({
          path: z.string().min(1),
          totalBytes: z.number().nonnegative(),
          freeBytes: z.number().nonnegative(),
          usedBytes: z.number().nonnegative(),
          usedPercent: z.number().min(0).max(100),
          pressure: z.enum(["normal", "watch", "high", "critical"]),
          sampledAt: z.string().min(1)
        })
        .optional(),
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

app.get("/api/agents", async () => listAdvisorAgents());

app.get("/api/clusters/:clusterId/agents", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return buildAgentRun(params.clusterId);
});

app.post("/api/clusters/:clusterId/agents/run", async (request) => {
  assertWriteAllowed(request);
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const run = await buildAgentRun(params.clusterId);
  await recordAudit({
    actor: actorFromRequest(request),
    action: "agents.run",
    clusterId: params.clusterId,
    resourceType: "agent",
    details: { findingCount: run.findings.length }
  });
  return run;
});

async function buildAgentRun(clusterId: string) {
  const cluster = getCluster(clusterId);
  const [description, topics, consumerGroups] = await Promise.all([
    describeCluster(cluster),
    listTopics(cluster),
    listConsumerGroups(cluster)
  ]);

  return runAdvisorAgents({
    clusterId,
    brokerCount: description.brokers.length,
    topics,
    consumerGroups,
    collectors: (await listCollectors()).filter((collector) => collector.heartbeat.clusterId === clusterId),
    auditEvents: (await listAuditEvents()).filter((event) => !event.clusterId || event.clusterId === clusterId),
    security: getSecurityStatus(),
    persistenceMode: persistenceMode()
  });
}

const port = getPort();
await app.listen({ host: "0.0.0.0", port });

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void closeDatabase().finally(() => process.exit(0));
  });
}
