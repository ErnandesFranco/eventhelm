import cors from "@fastify/cors";
import Fastify from "fastify";
import { z } from "zod";
import { getAgentRun, listAgentRuns, saveAgentRun } from "./agentRuns.js";
import { listAdvisorAgents, runAdvisorAgents } from "./agents.js";
import { recordAudit, listAuditEvents } from "./audit.js";
import {
  clusterChangeReviewStateDrift,
  clusterSecretPolicyViolation,
  createClusterChangeReview,
  deleteClusterConfig,
  getClusterChangeReview,
  initializeClusterRegistry,
  listClusterChangeReviews,
  listClusterConfigs,
  markClusterChangeReview,
  markClusterChangeReviewApplied,
  sanitizeClusterConfig,
  toPublicCluster,
  upsertClusterConfig
} from "./clusterRegistry.js";
import type { ClusterRecord } from "./clusterRegistry.js";
import { upsertHeartbeat, upsertSnapshot, listCollectors } from "./collectors.js";
import {
  clusterSchema,
  getCorsOrigin,
  getPort,
  getSecurityStatus,
  isClusterBreakglassEnabled,
  isRebalanceExecutionEnabled,
  loadClusters
} from "./config.js";
import { closeDatabase, databaseSchemaStatus, initDatabase, persistenceMode } from "./db.js";
import {
  alterPartitionAssignments,
  applyTopicConfigUpdate,
  browseMessages,
  createTopic,
  describeConsumerGroupLag,
  describeCluster,
  describePartitionReassignments,
  describeTopicConfig,
  executeConsumerGroupOffsetReset,
  listConsumerGroups,
  listPartitionPlacements,
  listTopics,
  previewConsumerGroupOffsetReset,
  previewTopicConfigUpdate,
  produceMessage
} from "./kafka.js";
import { buildDiskRebalancePlan, buildRebalancePreflight } from "./rebalance.js";
import {
  getRebalancePlan,
  listRebalancePlans,
  markRebalancePlanExecuted,
  markRebalancePlanReviewed,
  saveRebalancePlan
} from "./rebalancePlans.js";
import { actorFromRequest, assertCollectorAllowed, assertReadAllowed, assertWriteAllowed } from "./security.js";
import type { ClusterConfig, RebalancePlanRecord } from "./types.js";

let clusters: ClusterRecord[] = [];
const app = Fastify({
  logger: true
});

await initDatabase();
clusters = await initializeClusterRegistry(loadClusters());

await app.register(cors, {
  origin: getCorsOrigin()
});

app.addHook("onRequest", async (request) => {
  if (request.method === "GET" && request.url.startsWith("/api/")) {
    assertReadAllowed(request);
  }
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

function assertClusterBreakglassAllowed(request: Parameters<typeof assertWriteAllowed>[0]) {
  if (!isClusterBreakglassEnabled()) {
    throw forbidden("Direct cluster registry mutation is disabled. Use cluster change reviews or enable EVENTHELM_ENABLE_CLUSTER_BREAKGLASS=true.");
  }
  assertWriteAllowed(request, "cluster:breakglass");
}

function assertClusterSecretPolicy(cluster: ClusterConfig) {
  const violation = clusterSecretPolicyViolation(cluster, getSecurityStatus().authMode);
  if (violation) {
    throw badRequest(violation);
  }
}

async function buildStoredRebalancePreflight(clusterId: string, storedPlan: RebalancePlanRecord) {
  const cluster = getCluster(clusterId);
  const [reassignmentStatus, currentPlacements, collectors] = await Promise.all([
    describePartitionReassignments(cluster),
    listPartitionPlacements(cluster, true),
    listCollectors()
  ]);
  return buildRebalancePreflight({
    planRecord: storedPlan,
    executionEnabled: isRebalanceExecutionEnabled(),
    reassignmentStatus,
    currentPlacements,
    collectors: collectors.filter((collector) => collector.heartbeat.clusterId === clusterId)
  });
}

const offsetResetRequestSchema = z
  .discriminatedUnion("mode", [
    z.object({
      mode: z.literal("earliest"),
      topic: z.string().min(1),
      partitions: z.array(z.coerce.number().int().nonnegative()).optional()
    }),
    z.object({
      mode: z.literal("latest"),
      topic: z.string().min(1),
      partitions: z.array(z.coerce.number().int().nonnegative()).optional()
    }),
    z.object({
      mode: z.literal("absolute"),
      topic: z.string().min(1),
      partitions: z.array(z.coerce.number().int().nonnegative()).optional(),
      offset: z
        .union([z.string(), z.number()])
        .transform(String)
        .refine((offset) => /^\d+$/.test(offset), "Absolute offset must be a non-negative integer.")
    })
  ])
  .superRefine((body, context) => {
    if (body.topic.startsWith("__")) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "EventHelm will not reset offsets for internal Kafka topics.",
        path: ["topic"]
      });
    }
  });

const topicConfigUpdateSchema = z.object({
  configs: z
    .array(
      z.object({
        name: z.string().min(1),
        value: z.string().min(1)
      })
    )
    .min(1)
    .max(12)
});

app.get("/health", async () => ({
  ok: true,
  service: "eventhelm-api",
  persistence: persistenceMode(),
  database: await databaseSchemaStatus(),
  timestamp: new Date().toISOString()
}));

app.get("/api/security/status", async () => getSecurityStatus());

app.get("/api/clusters", async () => clusters.map(toPublicCluster));

app.get("/api/clusters/reviews", async (request) => {
  const query = z
    .object({
      limit: z.coerce.number().int().min(1).max(100).default(25)
    })
    .parse(request.query);
  return listClusterChangeReviews(query.limit);
});

app.post("/api/clusters/reviews", async (request) => {
  assertWriteAllowed(request, "cluster:write");
  const body = z
    .discriminatedUnion("action", [
      z.object({
        action: z.literal("upsert"),
        cluster: clusterSchema.extend({
          id: z.string().regex(/^[a-z0-9][a-z0-9-]{1,62}$/, "Cluster IDs must be lowercase alphanumeric slugs.")
        })
      }),
      z.object({
        action: z.literal("delete"),
        clusterId: z.string().min(1)
      })
    ])
    .parse(request.body);
  if (body.action === "upsert") {
    assertClusterSecretPolicy(body.cluster);
  }
  const actor = actorFromRequest(request);
  const review = await createClusterChangeReview(body, actor, clusters);
  await recordAudit({
    actor,
    action: "cluster.review_create",
    clusterId: review.clusterId,
    resourceType: "cluster",
    resourceName: review.clusterId,
    details: {
      reviewId: review.id,
      action: review.action,
      warnings: review.warnings,
      request: review.request
    }
  });
  return review;
});

app.post("/api/clusters/reviews/:reviewId/approve", async (request) => {
  assertWriteAllowed(request, "cluster:write");
  const params = z.object({ reviewId: z.string().min(1) }).parse(request.params);
  const body = z.object({ comment: z.string().max(500).optional() }).parse(request.body ?? {});
  const review = await getClusterChangeReview(params.reviewId);
  if (!review) {
    throw badRequest("Cluster change review was not found.");
  }
  if (review.status !== "pending") {
    throw badRequest("Only pending cluster change reviews can be approved.");
  }
  const actor = actorFromRequest(request);
  const approved = await markClusterChangeReview(review.id, "approved", actor, body.comment);
  await recordAudit({
    actor,
    action: "cluster.review_approve",
    clusterId: review.clusterId,
    resourceType: "cluster",
    resourceName: review.clusterId,
    details: { reviewId: review.id, action: review.action, comment: body.comment }
  });
  return approved;
});

app.post("/api/clusters/reviews/:reviewId/reject", async (request) => {
  assertWriteAllowed(request, "cluster:write");
  const params = z.object({ reviewId: z.string().min(1) }).parse(request.params);
  const body = z.object({ comment: z.string().max(500).optional() }).parse(request.body ?? {});
  const review = await getClusterChangeReview(params.reviewId);
  if (!review) {
    throw badRequest("Cluster change review was not found.");
  }
  if (review.status !== "pending") {
    throw badRequest("Only pending cluster change reviews can be rejected.");
  }
  const actor = actorFromRequest(request);
  const rejected = await markClusterChangeReview(review.id, "rejected", actor, body.comment);
  await recordAudit({
    actor,
    action: "cluster.review_reject",
    clusterId: review.clusterId,
    resourceType: "cluster",
    resourceName: review.clusterId,
    details: { reviewId: review.id, action: review.action, comment: body.comment }
  });
  return rejected;
});

app.post("/api/clusters/reviews/:reviewId/apply", async (request) => {
  assertWriteAllowed(request, "cluster:write");
  const params = z.object({ reviewId: z.string().min(1) }).parse(request.params);
  const review = await getClusterChangeReview(params.reviewId);
  if (!review) {
    throw badRequest("Cluster change review was not found.");
  }
  if (review.status !== "approved") {
    throw badRequest("Cluster change review must be approved before apply.");
  }
  const driftReasons = clusterChangeReviewStateDrift(review, clusters);
  if (driftReasons.length > 0) {
    throw badRequest(`Cluster change review preflight failed. ${driftReasons.join(" ")}`);
  }

  const actor = actorFromRequest(request);
  if (review.request.action === "upsert") {
    const saved = await upsertClusterConfig(review.request.cluster, "api");
    clusters = await listClusterConfigs();
    const applied = await markClusterChangeReviewApplied(review.id, actor);
    await recordAudit({
      actor,
      action: "cluster.review_apply",
      clusterId: saved.id,
      resourceType: "cluster",
      resourceName: saved.id,
      details: {
        reviewId: review.id,
        action: review.action,
        cluster: sanitizeClusterConfig(saved)
      }
    });
    return { applied: true, review: applied, cluster: toPublicCluster(saved) };
  }

  const existing = clusters.find((cluster) => cluster.id === review.clusterId);
  if (!existing) {
    throw badRequest("Cluster was not found.");
  }
  if ((existing as { source?: string }).source === "environment") {
    throw forbidden("Environment-managed clusters cannot be deleted through the API.");
  }
  const deleted = await deleteClusterConfig(review.clusterId);
  if (!deleted) {
    throw badRequest("Cluster was not found.");
  }
  clusters = await listClusterConfigs();
  const applied = await markClusterChangeReviewApplied(review.id, actor);
  await recordAudit({
    actor,
    action: "cluster.review_apply",
    clusterId: deleted.id,
    resourceType: "cluster",
    resourceName: deleted.id,
    details: {
      reviewId: review.id,
      action: review.action,
      cluster: sanitizeClusterConfig(deleted)
    }
  });
  return { applied: true, review: applied, cluster: toPublicCluster(deleted) };
});

app.post("/api/clusters", async (request) => {
  assertClusterBreakglassAllowed(request);
  const body = clusterSchema
    .extend({
      id: z.string().regex(/^[a-z0-9][a-z0-9-]{1,62}$/, "Cluster IDs must be lowercase alphanumeric slugs.")
    })
    .parse(request.body);
  assertClusterSecretPolicy(body);
  const actor = actorFromRequest(request);
  const saved = await upsertClusterConfig(body, "api");
  clusters = await listClusterConfigs();

  await recordAudit({
    actor,
    action: "cluster.upsert",
    clusterId: saved.id,
    resourceType: "cluster",
    resourceName: saved.id,
    details: sanitizeClusterConfig(saved)
  });

  return toPublicCluster(saved);
});

app.delete("/api/clusters/:clusterId", async (request) => {
  assertClusterBreakglassAllowed(request);
  const params = z.object({ clusterId: z.string().min(1) }).parse(request.params);
  const existing = clusters.find((cluster) => cluster.id === params.clusterId);
  if (!existing) {
    throw badRequest("Cluster was not found.");
  }
  if ((existing as { source?: string }).source === "environment") {
    throw forbidden("Environment-managed clusters cannot be deleted through the API.");
  }

  const deleted = await deleteClusterConfig(params.clusterId);
  if (!deleted) {
    throw badRequest("Cluster was not found.");
  }
  clusters = await listClusterConfigs();

  await recordAudit({
    actor: actorFromRequest(request),
    action: "cluster.delete",
    clusterId: deleted.id,
    resourceType: "cluster",
    resourceName: deleted.id,
    details: sanitizeClusterConfig(deleted)
  });

  return { deleted: true, cluster: toPublicCluster(deleted) };
});

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

app.get("/api/clusters/:clusterId/topics/:topic/config", async (request) => {
  const params = z.object({ clusterId: z.string(), topic: z.string().min(1) }).parse(request.params);
  return describeTopicConfig(getCluster(params.clusterId), params.topic);
});

app.post("/api/clusters/:clusterId/topics", async (request) => {
  assertWriteAllowed(request, "topic:write");
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

app.post("/api/clusters/:clusterId/topics/:topic/config/preview", async (request) => {
  assertReadAllowed(request);
  const params = z.object({ clusterId: z.string(), topic: z.string().min(1) }).parse(request.params);
  if (params.topic.startsWith("__")) {
    throw badRequest("EventHelm will not alter internal Kafka topic configs.");
  }
  const body = topicConfigUpdateSchema.parse(request.body);
  return previewTopicConfigUpdate(getCluster(params.clusterId), params.topic, body);
});

app.post("/api/clusters/:clusterId/topics/:topic/config/apply", async (request) => {
  assertWriteAllowed(request, "topic:write");
  const params = z.object({ clusterId: z.string(), topic: z.string().min(1) }).parse(request.params);
  if (params.topic.startsWith("__")) {
    throw badRequest("EventHelm will not alter internal Kafka topic configs.");
  }
  const body = topicConfigUpdateSchema
    .and(
      z.object({
        reviewToken: z.string().length(64)
      })
    )
    .parse(request.body);

  try {
    const preview = await applyTopicConfigUpdate(getCluster(params.clusterId), params.topic, body, body.reviewToken);
    await recordAudit({
      actor: actorFromRequest(request),
      action: "topic.config_update",
      clusterId: params.clusterId,
      resourceType: "topic",
      resourceName: params.topic,
      details: {
        reviewToken: preview.reviewToken,
        changes: preview.changes.map((change) => ({
          name: change.name,
          from: change.currentValue,
          to: change.newValue
        }))
      }
    });

    return {
      accepted: true,
      reviewToken: preview.reviewToken,
      changes: preview.changes
    };
  } catch (error) {
    throw badRequest(error instanceof Error ? error.message : String(error));
  }
});

app.get("/api/clusters/:clusterId/consumer-groups", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return listConsumerGroups(getCluster(params.clusterId));
});

app.get("/api/clusters/:clusterId/consumer-groups/:groupId/lag", async (request) => {
  const params = z.object({ clusterId: z.string(), groupId: z.string().min(1) }).parse(request.params);
  return describeConsumerGroupLag(getCluster(params.clusterId), params.groupId);
});

app.post("/api/clusters/:clusterId/consumer-groups/:groupId/offset-reset/preview", async (request) => {
  assertReadAllowed(request);
  const params = z.object({ clusterId: z.string(), groupId: z.string().min(1) }).parse(request.params);
  const body = offsetResetRequestSchema.parse(request.body);
  return previewConsumerGroupOffsetReset(getCluster(params.clusterId), params.groupId, body);
});

app.post("/api/clusters/:clusterId/consumer-groups/:groupId/offset-reset/execute", async (request) => {
  assertWriteAllowed(request, "consumer:write");
  const params = z.object({ clusterId: z.string(), groupId: z.string().min(1) }).parse(request.params);
  const body = offsetResetRequestSchema
    .and(
      z.object({
        reviewToken: z.string().length(64)
      })
    )
    .parse(request.body);

  try {
    const preview = await executeConsumerGroupOffsetReset(getCluster(params.clusterId), params.groupId, body, body.reviewToken);
    await recordAudit({
      actor: actorFromRequest(request),
      action: "consumer.offset_reset",
      clusterId: params.clusterId,
      resourceType: "consumer-group",
      resourceName: params.groupId,
      details: {
        topic: preview.request.topic,
        mode: preview.request.mode,
        partitions: preview.summary.partitions,
        messagesSkipped: preview.summary.messagesSkipped,
        messagesToReplay: preview.summary.messagesToReplay,
        reviewToken: preview.reviewToken
      }
    });

    return {
      accepted: true,
      reviewToken: preview.reviewToken,
      summary: preview.summary
    };
  } catch (error) {
    throw badRequest(error instanceof Error ? error.message : String(error));
  }
});

app.post("/api/clusters/:clusterId/messages/produce", async (request) => {
  assertWriteAllowed(request, "message:write");
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

app.get("/api/clusters/:clusterId/rebalance/plans", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  getCluster(params.clusterId);
  const query = z
    .object({
      limit: z.coerce.number().int().min(1).max(100).default(25)
    })
    .parse(request.query);
  return listRebalancePlans(params.clusterId, query.limit);
});

app.get("/api/clusters/:clusterId/rebalance/plans/:planId", async (request) => {
  const params = z.object({ clusterId: z.string(), planId: z.string().min(1) }).parse(request.params);
  getCluster(params.clusterId);
  const storedPlan = await getRebalancePlan(params.planId);
  if (!storedPlan || storedPlan.clusterId !== params.clusterId) {
    throw badRequest("Rebalance plan was not found for this cluster.");
  }
  return storedPlan;
});

app.get("/api/clusters/:clusterId/rebalance/plans/:planId/preflight", async (request) => {
  const params = z.object({ clusterId: z.string(), planId: z.string().min(1) }).parse(request.params);
  const storedPlan = await getRebalancePlan(params.planId);
  if (!storedPlan || storedPlan.clusterId !== params.clusterId) {
    throw badRequest("Rebalance plan was not found for this cluster.");
  }
  return buildStoredRebalancePreflight(params.clusterId, storedPlan);
});

app.post("/api/clusters/:clusterId/rebalance/plan", async (request) => {
  assertWriteAllowed(request, "rebalance:plan");
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
  const actor = actorFromRequest(request);
  await saveRebalancePlan(plan, actor);

  await recordAudit({
    actor,
    action: "rebalance.plan",
    clusterId: params.clusterId,
    resourceType: "cluster",
    resourceName: params.clusterId,
    details: {
      planId: plan.id,
      movements: plan.summary.movements,
      sources: plan.summary.sourceBrokerIds,
      targets: plan.summary.targetBrokerIds
    }
  });

  return plan;
});

app.get("/api/clusters/:clusterId/rebalance/status", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return describePartitionReassignments(getCluster(params.clusterId));
});

app.post("/api/clusters/:clusterId/rebalance/plans/:planId/approve", async (request) => {
  assertWriteAllowed(request, "rebalance:review");
  const params = z.object({ clusterId: z.string(), planId: z.string().min(1) }).parse(request.params);
  const body = z
    .object({
      comment: z.string().max(500).optional()
    })
    .parse(request.body ?? {});
  const storedPlan = await getRebalancePlan(params.planId);
  if (!storedPlan || storedPlan.clusterId !== params.clusterId) {
    throw badRequest("Rebalance plan was not found for this cluster.");
  }
  if (storedPlan.status === "executed") {
    throw badRequest("Executed rebalance plans cannot be reviewed again.");
  }

  const actor = actorFromRequest(request);
  const reviewed = await markRebalancePlanReviewed(storedPlan.id, "approved", actor, body.comment);
  await recordAudit({
    actor,
    action: "rebalance.approve",
    clusterId: params.clusterId,
    resourceType: "cluster",
    resourceName: params.clusterId,
    details: {
      planId: storedPlan.id,
      movements: storedPlan.plan.summary.movements,
      comment: body.comment
    }
  });
  return reviewed;
});

app.post("/api/clusters/:clusterId/rebalance/plans/:planId/reject", async (request) => {
  assertWriteAllowed(request, "rebalance:review");
  const params = z.object({ clusterId: z.string(), planId: z.string().min(1) }).parse(request.params);
  const body = z
    .object({
      comment: z.string().max(500).optional()
    })
    .parse(request.body ?? {});
  const storedPlan = await getRebalancePlan(params.planId);
  if (!storedPlan || storedPlan.clusterId !== params.clusterId) {
    throw badRequest("Rebalance plan was not found for this cluster.");
  }
  if (storedPlan.status === "executed") {
    throw badRequest("Executed rebalance plans cannot be reviewed again.");
  }

  const actor = actorFromRequest(request);
  const reviewed = await markRebalancePlanReviewed(storedPlan.id, "rejected", actor, body.comment);
  await recordAudit({
    actor,
    action: "rebalance.reject",
    clusterId: params.clusterId,
    resourceType: "cluster",
    resourceName: params.clusterId,
    details: {
      planId: storedPlan.id,
      movements: storedPlan.plan.summary.movements,
      comment: body.comment
    }
  });
  return reviewed;
});

app.post("/api/clusters/:clusterId/rebalance/execute", async (request) => {
  assertWriteAllowed(request, "rebalance:execute");
  if (!isRebalanceExecutionEnabled()) {
    throw forbidden("Rebalance execution is locked. Set EVENTHELM_ENABLE_REBALANCE_EXECUTION=true after approvals and RBAC are configured.");
  }

  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const body = z
    .object({
      planId: z.string().min(1)
    })
    .parse(request.body);

  const storedPlan = await getRebalancePlan(body.planId);
  if (!storedPlan || storedPlan.clusterId !== params.clusterId) {
    throw badRequest("Rebalance plan was not found for this cluster.");
  }
  if (storedPlan.status === "executed") {
    throw badRequest("Rebalance plan has already been executed.");
  }
  if (storedPlan.status !== "approved") {
    throw badRequest("Rebalance plan must be approved before execution.");
  }

  const cluster = getCluster(params.clusterId);
  const preflight = await buildStoredRebalancePreflight(params.clusterId, storedPlan);
  if (!preflight.executable) {
    throw badRequest(`Rebalance preflight failed. ${preflight.blockedReasons.join(" ")}`);
  }

  await alterPartitionAssignments(cluster, storedPlan.plan.kafkaJsRequest);
  await markRebalancePlanExecuted(storedPlan.id);
  await recordAudit({
    actor: actorFromRequest(request),
    action: "rebalance.execute",
    clusterId: params.clusterId,
    resourceType: "cluster",
    resourceName: params.clusterId,
    details: {
      planId: storedPlan.id,
      topics: storedPlan.plan.kafkaJsRequest.length,
      movements: storedPlan.plan.summary.movements,
      estimatedBytesMoved: storedPlan.plan.summary.estimatedBytesMoved
    }
  });

  return { accepted: true, planId: storedPlan.id };
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
      host: z
        .object({
          cpuCount: z.number().int().positive(),
          loadAverage1m: z.number().nonnegative(),
          loadAverage5m: z.number().nonnegative(),
          loadAverage15m: z.number().nonnegative(),
          totalMemoryBytes: z.number().nonnegative(),
          freeMemoryBytes: z.number().nonnegative(),
          usedMemoryBytes: z.number().nonnegative(),
          usedMemoryPercent: z.number().min(0).max(100),
          memoryPressure: z.enum(["normal", "watch", "high", "critical"]),
          uptimeSeconds: z.number().nonnegative(),
          sampledAt: z.string().min(1)
        })
        .optional(),
      partitions: z
        .array(
          z.object({
            topic: z.string().min(1),
            partition: z.number().int().nonnegative(),
            sizeBytes: z.number().nonnegative(),
            logDir: z.string().min(1)
          })
        )
        .default([]),
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

app.get("/api/audit", async (request) => {
  const query = z
    .object({
      clusterId: z.string().optional(),
      actor: z.string().optional(),
      action: z.string().optional(),
      resourceType: z.string().optional(),
      resourceName: z.string().optional(),
      query: z.string().optional(),
      limit: z.coerce.number().int().min(1).max(500).default(250)
    })
    .parse(request.query);
  return listAuditEvents(query);
});

app.get("/api/agents", async () => listAdvisorAgents());

app.get("/api/clusters/:clusterId/agents/runs", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  getCluster(params.clusterId);
  const query = z
    .object({
      limit: z.coerce.number().int().min(1).max(100).default(25)
    })
    .parse(request.query);
  return listAgentRuns(params.clusterId, query.limit);
});

app.get("/api/clusters/:clusterId/agents/runs/:runId", async (request) => {
  const params = z.object({ clusterId: z.string(), runId: z.string().min(1) }).parse(request.params);
  getCluster(params.clusterId);
  const run = await getAgentRun(params.runId);
  if (!run || run.clusterId !== params.clusterId) {
    throw badRequest("Agent run was not found for this cluster.");
  }
  return run;
});

app.get("/api/clusters/:clusterId/agents", async (request) => {
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  return buildAgentRunPayload(params.clusterId, "system", "automatic");
});

app.post("/api/clusters/:clusterId/agents/run", async (request) => {
  assertWriteAllowed(request, "agent:run");
  const params = z.object({ clusterId: z.string() }).parse(request.params);
  const actor = actorFromRequest(request);
  const run = await saveAgentRun(await buildAgentRunPayload(params.clusterId, actor, "manual"), actor, "manual");
  await recordAudit({
    actor,
    action: "agents.run",
    clusterId: params.clusterId,
    resourceType: "agent",
    resourceName: run.id,
    details: { runId: run.id, findingCount: run.findings.length, summary: run.summary }
  });
  return run;
});

async function buildAgentRunPayload(clusterId: string, actor: string, trigger: "automatic" | "manual") {
  const cluster = getCluster(clusterId);
  const [description, topics, consumerGroups] = await Promise.all([
    describeCluster(cluster),
    listTopics(cluster),
    listConsumerGroups(cluster)
  ]);

  return runAdvisorAgents(
    {
      clusterId,
      brokerCount: description.brokers.length,
      topics,
      consumerGroups,
      collectors: (await listCollectors()).filter((collector) => collector.heartbeat.clusterId === clusterId),
      auditEvents: (await listAuditEvents()).filter((event) => !event.clusterId || event.clusterId === clusterId),
      clusters: clusters.map(toPublicCluster),
      clusterChangeReviews: (await listClusterChangeReviews(100)).filter((review) => review.clusterId === clusterId),
      rebalancePlans: await listRebalancePlans(clusterId, 100),
      security: getSecurityStatus(),
      persistenceMode: persistenceMode()
    },
    { actor, trigger }
  );
}

const port = getPort();
await app.listen({ host: "0.0.0.0", port });

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void closeDatabase().finally(() => process.exit(0));
  });
}
