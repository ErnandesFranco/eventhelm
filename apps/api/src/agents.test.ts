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
      security: {
        authMode: "dev",
        apiTokenConfigured: false,
        apiTokenCount: 0,
        configuredScopes: [],
        collectorTokenConfigured: false,
        corsOrigin: "*",
        readAuthRequired: false,
        writeConfirmationRequired: true
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
