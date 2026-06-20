import test from "node:test";
import assert from "node:assert/strict";
import { buildOffsetResetPreview } from "./offsetReset.js";

const group = {
  groupId: "orders-worker",
  protocolType: "consumer",
  state: "Empty",
  members: []
};

const committedTopic = {
  topic: "orders.created",
  partitions: [
    { partition: 0, offset: "10" },
    { partition: 1, offset: "50" }
  ]
};

const logOffsets = [
  { partition: 0, low: "3", high: "30" },
  { partition: 1, low: "8", high: "75" }
];

test("buildOffsetResetPreview creates a reviewed latest-offset reset", () => {
  const preview = buildOffsetResetPreview({
    group,
    request: {
      topic: "orders.created",
      mode: "latest"
    },
    committedTopic,
    logOffsets,
    generatedAt: "2026-06-20T00:00:00.000Z"
  });

  assert.equal(preview.executable, true);
  assert.equal(preview.summary.partitions, 2);
  assert.equal(preview.summary.lagBefore, "45");
  assert.equal(preview.summary.lagAfter, "0");
  assert.equal(preview.summary.messagesSkipped, "45");
  assert.equal(preview.summary.messagesToReplay, "0");
  assert.equal(preview.topics[0]?.partitions[0]?.proposedOffset, "30");
  assert.match(preview.reviewToken, /^[a-f0-9]{64}$/);
});

test("buildOffsetResetPreview blocks absolute offsets outside the log range", () => {
  const preview = buildOffsetResetPreview({
    group,
    request: {
      topic: "orders.created",
      mode: "absolute",
      offset: "100"
    },
    committedTopic,
    logOffsets,
    generatedAt: "2026-06-20T00:00:00.000Z"
  });

  assert.equal(preview.executable, false);
  assert.equal(preview.summary.executablePartitions, 0);
  assert.ok(preview.topics[0]?.partitions.every((partition) => partition.blockedReason));
  assert.ok(preview.warnings.some((warning) => warning.includes("cannot be reset")));
});

test("buildOffsetResetPreview blocks running consumer groups", () => {
  const preview = buildOffsetResetPreview({
    group: {
      ...group,
      state: "Stable",
      members: [{ memberId: "consumer-1" }]
    },
    request: {
      topic: "orders.created",
      mode: "earliest",
      partitions: [1]
    },
    committedTopic,
    logOffsets,
    generatedAt: "2026-06-20T00:00:00.000Z"
  });

  assert.equal(preview.executable, false);
  assert.equal(preview.summary.partitions, 1);
  assert.equal(preview.summary.messagesToReplay, "42");
  assert.ok(preview.warnings.some((warning) => warning.includes("must be stopped")));
});
