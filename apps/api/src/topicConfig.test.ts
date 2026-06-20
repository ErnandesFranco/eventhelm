import test from "node:test";
import assert from "node:assert/strict";
import { buildTopicConfigUpdatePlan, toTopicConfig } from "./topicConfig.js";

const current = toTopicConfig("orders.created", [
  entry("cleanup.policy", "delete", true),
  entry("retention.ms", "604800000", true),
  entry("min.insync.replicas", "2", false),
  entry("segment.bytes", "1073741824", true)
]);

test("buildTopicConfigUpdatePlan preserves existing dynamic config entries", () => {
  const plan = buildTopicConfigUpdatePlan(current, {
    configs: [{ name: "retention.ms", value: "86400000" }]
  });

  assert.equal(plan.preview.executable, true);
  assert.equal(plan.preview.changes.length, 1);
  assert.deepEqual(plan.configEntries, [
    { name: "min.insync.replicas", value: "2" },
    { name: "retention.ms", value: "86400000" }
  ]);
  assert.match(plan.preview.reviewToken, /^[a-f0-9]{64}$/);
});

test("buildTopicConfigUpdatePlan blocks unsupported config names", () => {
  const plan = buildTopicConfigUpdatePlan(current, {
    configs: [{ name: "unclean.leader.election.enable", value: "true" }]
  });

  assert.equal(plan.preview.executable, false);
  assert.equal(plan.preview.changes[0]?.blockedReason, "Config is not in the EventHelm editable allowlist.");
});

test("buildTopicConfigUpdatePlan validates cleanup policy values", () => {
  const plan = buildTopicConfigUpdatePlan(current, {
    configs: [{ name: "cleanup.policy", value: "destroy" }]
  });

  assert.equal(plan.preview.executable, false);
  assert.match(plan.preview.changes[0]?.blockedReason ?? "", /Cleanup policy/);
});

function entry(name: string, value: string, isDefault: boolean) {
  return {
    name,
    value,
    isDefault,
    source: isDefault ? 5 : 1,
    isSensitive: false,
    readOnly: false
  };
}
