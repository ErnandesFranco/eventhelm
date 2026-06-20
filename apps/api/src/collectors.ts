import type { CollectorHeartbeat, CollectorSnapshot, CollectorState } from "./types.js";

const collectors = new Map<string, CollectorState>();

export function upsertHeartbeat(heartbeat: CollectorHeartbeat): CollectorState {
  const existing = collectors.get(heartbeat.collectorId);
  const next: CollectorState = {
    heartbeat,
    lastSnapshot: existing?.lastSnapshot
  };
  collectors.set(heartbeat.collectorId, next);
  return next;
}

export function upsertSnapshot(snapshot: CollectorSnapshot): CollectorState {
  const next: CollectorState = {
    heartbeat: snapshot,
    lastSnapshot: snapshot
  };
  collectors.set(snapshot.collectorId, next);
  return next;
}

export function listCollectors(): CollectorState[] {
  return [...collectors.values()].sort((left, right) =>
    left.heartbeat.collectorId.localeCompare(right.heartbeat.collectorId)
  );
}
