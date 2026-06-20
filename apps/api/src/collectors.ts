import { persistenceMode, query } from "./db.js";
import type { CollectorHeartbeat, CollectorSnapshot, CollectorState } from "./types.js";

const collectors = new Map<string, CollectorState>();

type CollectorRow = {
  collector_id: string;
  heartbeat: CollectorHeartbeat;
  last_snapshot?: CollectorSnapshot;
};

export async function upsertHeartbeat(heartbeat: CollectorHeartbeat): Promise<CollectorState> {
  if (persistenceMode() === "postgres") {
    const result = await query<CollectorRow>(
      `insert into collector_states
        (collector_id, cluster_id, broker_id, heartbeat, observed_at, updated_at)
       values ($1, $2, $3, $4, $5, now())
       on conflict (collector_id) do update set
        cluster_id = excluded.cluster_id,
        broker_id = excluded.broker_id,
        heartbeat = excluded.heartbeat,
        observed_at = excluded.observed_at,
        updated_at = now()
       where collector_states.observed_at <= excluded.observed_at
       returning collector_id, heartbeat, last_snapshot`,
      [heartbeat.collectorId, heartbeat.clusterId, heartbeat.brokerId, heartbeat, heartbeat.observedAt]
    );
    return result.rows[0] ? rowToCollectorState(result.rows[0]) : getCollectorState(heartbeat.collectorId);
  }

  const existing = collectors.get(heartbeat.collectorId);
  if (existing && Date.parse(existing.heartbeat.observedAt) > Date.parse(heartbeat.observedAt)) {
    return existing;
  }
  const next: CollectorState = {
    heartbeat,
    lastSnapshot: existing?.lastSnapshot
  };
  collectors.set(heartbeat.collectorId, next);
  return next;
}

export async function upsertSnapshot(snapshot: CollectorSnapshot): Promise<CollectorState> {
  if (persistenceMode() === "postgres") {
    const result = await query<CollectorRow>(
      `insert into collector_states
        (collector_id, cluster_id, broker_id, heartbeat, last_snapshot, observed_at, updated_at)
       values ($1, $2, $3, $4, $5, $6, now())
       on conflict (collector_id) do update set
        cluster_id = excluded.cluster_id,
        broker_id = excluded.broker_id,
        heartbeat = excluded.heartbeat,
        last_snapshot = excluded.last_snapshot,
        observed_at = excluded.observed_at,
        updated_at = now()
       where collector_states.observed_at <= excluded.observed_at
       returning collector_id, heartbeat, last_snapshot`,
      [snapshot.collectorId, snapshot.clusterId, snapshot.brokerId, heartbeatFromSnapshot(snapshot), snapshot, snapshot.observedAt]
    );
    return result.rows[0] ? rowToCollectorState(result.rows[0]) : getCollectorState(snapshot.collectorId);
  }

  const existing = collectors.get(snapshot.collectorId);
  if (existing && Date.parse(existing.heartbeat.observedAt) > Date.parse(snapshot.observedAt)) {
    return existing;
  }
  const next: CollectorState = {
    heartbeat: heartbeatFromSnapshot(snapshot),
    lastSnapshot: snapshot
  };
  collectors.set(snapshot.collectorId, next);
  return next;
}

export async function listCollectors(): Promise<CollectorState[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<CollectorRow>(
      `select collector_id, heartbeat, last_snapshot
       from collector_states
       order by collector_id asc`
    );
    return result.rows.map(rowToCollectorState);
  }

  return [...collectors.values()].sort((left, right) =>
    left.heartbeat.collectorId.localeCompare(right.heartbeat.collectorId)
  );
}

async function getCollectorState(collectorId: string): Promise<CollectorState> {
  if (persistenceMode() === "postgres") {
    const result = await query<CollectorRow>(
      `select collector_id, heartbeat, last_snapshot
       from collector_states
       where collector_id = $1`,
      [collectorId]
    );
    if (result.rows[0]) {
      return rowToCollectorState(result.rows[0]);
    }
  }
  const state = collectors.get(collectorId);
  if (state) {
    return state;
  }
  throw new Error(`Collector '${collectorId}' was not found.`);
}

function heartbeatFromSnapshot(snapshot: CollectorSnapshot): CollectorHeartbeat {
  return {
    collectorId: snapshot.collectorId,
    clusterId: snapshot.clusterId,
    brokerId: snapshot.brokerId,
    hostname: snapshot.hostname,
    version: snapshot.version,
    startedAt: snapshot.startedAt,
    observedAt: snapshot.observedAt
  };
}

function rowToCollectorState(row: CollectorRow): CollectorState {
  return {
    heartbeat: row.heartbeat,
    lastSnapshot: row.last_snapshot
  };
}
