import assert from "node:assert/strict";
import test from "node:test";
import { listCollectors, upsertHeartbeat, upsertSnapshot } from "./collectors.js";
import type { CollectorHeartbeat, CollectorSnapshot } from "./types.js";

test("collector heartbeats ignore stale observed timestamps", async () => {
  const collectorId = `collector-heartbeat-${Date.now()}`;
  const fresh = heartbeat(collectorId, "2026-01-01T00:05:00.000Z");
  const stale = heartbeat(collectorId, "2026-01-01T00:01:00.000Z");

  await upsertHeartbeat(fresh);
  const returned = await upsertHeartbeat(stale);
  const stored = (await listCollectors()).find((collector) => collector.heartbeat.collectorId === collectorId);

  assert.equal(returned.heartbeat.observedAt, fresh.observedAt);
  assert.equal(stored?.heartbeat.observedAt, fresh.observedAt);
});

test("collector snapshots ignore stale observed timestamps", async () => {
  const collectorId = `collector-snapshot-${Date.now()}`;
  const fresh = snapshot(collectorId, "2026-01-01T00:05:00.000Z", 42);
  const stale = snapshot(collectorId, "2026-01-01T00:01:00.000Z", 99);

  await upsertSnapshot(fresh);
  const returned = await upsertSnapshot(stale);
  const stored = (await listCollectors()).find((collector) => collector.heartbeat.collectorId === collectorId);

  assert.equal(returned.heartbeat.observedAt, fresh.observedAt);
  assert.equal(returned.lastSnapshot?.disk?.usedPercent, fresh.disk?.usedPercent);
  assert.equal(stored?.lastSnapshot?.disk?.usedPercent, fresh.disk?.usedPercent);
});

function heartbeat(collectorId: string, observedAt: string): CollectorHeartbeat {
  return {
    collectorId,
    clusterId: "local",
    brokerId: "1",
    hostname: "broker-1",
    version: "test",
    startedAt: "2026-01-01T00:00:00.000Z",
    observedAt
  };
}

function snapshot(collectorId: string, observedAt: string, usedPercent: number): CollectorSnapshot {
  return {
    ...heartbeat(collectorId, observedAt),
    brokerCount: 1,
    topicCount: 1,
    disk: {
      path: "/kafka-logs",
      totalBytes: 100,
      freeBytes: 100 - usedPercent,
      usedBytes: usedPercent,
      usedPercent,
      pressure: "normal",
      sampledAt: observedAt
    },
    brokers: [
      {
        nodeId: 1,
        host: "kafka-1",
        port: 9092
      }
    ]
  };
}
