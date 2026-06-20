import test from "node:test";
import assert from "node:assert/strict";
import { runAdvisorAgents } from "./agents.js";
import { listAgentRuns, saveAgentRun } from "./agentRuns.js";

test("advisor sweeps include durable run metadata and severity summary", async () => {
  const run = runAdvisorAgents(
    {
      clusterId: "agent-test",
      brokerCount: 2,
      topics: [],
      consumerGroups: [],
      collectors: [],
      auditEvents: [],
      clusters: [
        {
          id: "agent-test",
          name: "Agent Test",
          brokers: ["localhost:19092"],
          saslConfigured: true,
          saslPasswordSource: "inline",
          source: "api"
        }
      ],
      clusterChangeReviews: [],
      rebalancePlans: [],
      security: {
        authMode: "dev",
        apiTokenConfigured: false,
        apiTokenCount: 0,
        configuredScopes: [],
        collectorTokenConfigured: false,
        corsOrigin: "*",
        readAuthRequired: false,
        writeConfirmationRequired: true,
        writeRateLimitPerMinute: 0
      },
      persistenceMode: "memory"
    },
    { actor: "test-suite", trigger: "manual" }
  );

  assert.equal(typeof run.id, "string");
  assert.equal(run.actor, "test-suite");
  assert.equal(run.trigger, "manual");
  assert.equal(run.summary.findings, run.findings.length);
  assert.ok(run.summary.score < 100);
  assert.ok(run.summary.high > 0);
  assert.ok(run.findings.some((finding) => finding.id === "sentinel-kafka-credentials-are-stored-inline"));

  const saved = await saveAgentRun(run, "test-suite", "manual");
  const history = await listAgentRuns("agent-test", 5);

  assert.equal(saved.id, run.id);
  assert.equal(history[0]?.id, run.id);
  assert.equal(history[0]?.summary.findings, run.findings.length);
  assert.equal(history[0]?.findingsPreview[0]?.id, run.findings[0]?.id);
});

test("advisor sweeps flag review queues and direct cluster mutations", () => {
  const generatedAt = new Date().toISOString();
  const run = runAdvisorAgents({
    clusterId: "agent-test",
    brokerCount: 3,
    topics: [],
    consumerGroups: [],
    collectors: [],
    auditEvents: [
      {
        id: "audit-direct-cluster",
        actor: "legacy-script",
        action: "cluster.upsert",
        clusterId: "agent-test",
        resourceType: "cluster",
        resourceName: "agent-test",
        createdAt: generatedAt
      }
    ],
    clusters: [],
    clusterChangeReviews: [
      {
        id: "review-inline-1",
        clusterId: "agent-test",
        action: "upsert",
        status: "pending",
        actor: "operator",
        request: {
          action: "upsert",
          clusterId: "agent-test"
        },
        warnings: ["Inline SASL passwords are stored in the control-plane database."],
        createdAt: generatedAt
      }
    ],
    rebalancePlans: [
      {
        id: "rebalance-plan-1",
        clusterId: "agent-test",
        actor: "operator",
        status: "planned",
        createdAt: generatedAt,
        strategy: "disk-pressure",
        executable: false,
        executionBlockedReason: "Execution is locked.",
        summary: {
          movements: 2,
          partitionsEvaluated: 10,
          sourceBrokerIds: [1],
          targetBrokerIds: [3],
          estimatedBytesMoved: 10_000
        },
        warnings: []
      },
      {
        id: "rebalance-plan-2",
        clusterId: "agent-test",
        actor: "operator",
        status: "approved",
        createdAt: generatedAt,
        reviewedBy: "reviewer",
        reviewedAt: generatedAt,
        strategy: "disk-pressure",
        executable: false,
        executionBlockedReason: "Execution is locked.",
        summary: {
          movements: 1,
          partitionsEvaluated: 10,
          sourceBrokerIds: [2],
          targetBrokerIds: [3],
          estimatedBytesMoved: 5_000
        },
        warnings: []
      }
    ],
    security: {
      authMode: "token",
      apiTokenConfigured: true,
      apiTokenCount: 1,
      configuredScopes: ["read", "agent:run"],
      collectorTokenConfigured: true,
      corsOrigin: "http://localhost:15173",
      readAuthRequired: true,
      writeConfirmationRequired: true,
      writeRateLimitPerMinute: 60
    },
    persistenceMode: "postgres"
  });

  assert.ok(run.findings.some((finding) => finding.id === "navigator-cluster-change-reviews-are-waiting"));
  assert.ok(run.findings.some((finding) => finding.id === "navigator-rebalance-plans-need-an-operator-decision"));
  assert.ok(run.findings.some((finding) => finding.id === "sentinel-cluster-review-contains-inline-credentials"));
  assert.ok(run.findings.some((finding) => finding.id === "sentinel-direct-cluster-registry-mutations-detected"));
  assert.ok(run.findings.some((finding) => finding.id === "operator-approved-rebalance-plans-are-waiting"));
});
