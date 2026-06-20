import { nanoid } from "nanoid";
import { persistenceMode, query } from "./db.js";
import type { AuditEvent } from "./types.js";

const auditEvents: AuditEvent[] = [];

export type AuditEventFilters = {
  clusterId?: string;
  actor?: string;
  action?: string;
  resourceType?: string;
  resourceName?: string;
  query?: string;
  limit?: number;
};

type AuditRow = {
  id: string;
  actor: string;
  action: string;
  cluster_id?: string;
  resource_type?: string;
  resource_name?: string;
  details?: Record<string, unknown>;
  created_at: Date;
};

export async function recordAudit(event: Omit<AuditEvent, "id" | "createdAt">): Promise<AuditEvent> {
  const created: AuditEvent = {
    ...event,
    id: nanoid(),
    createdAt: new Date().toISOString()
  };

  if (persistenceMode() === "postgres") {
    await query(
      `insert into audit_events
        (id, actor, action, cluster_id, resource_type, resource_name, details, created_at)
       values ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [
        created.id,
        created.actor,
        created.action,
        created.clusterId,
        created.resourceType,
        created.resourceName,
        created.details ?? null,
        created.createdAt
      ]
    );
    return created;
  }

  auditEvents.unshift(created);
  auditEvents.splice(250);
  return created;
}

export async function listAuditEvents(filters: AuditEventFilters = {}): Promise<AuditEvent[]> {
  const limit = filters.limit ?? 250;
  if (persistenceMode() === "postgres") {
    const values: unknown[] = [];
    const clauses: string[] = [];

    addWhere(clauses, values, "cluster_id", filters.clusterId);
    addWhere(clauses, values, "actor", filters.actor);
    addWhere(clauses, values, "action", filters.action);
    addWhere(clauses, values, "resource_type", filters.resourceType);
    addWhere(clauses, values, "resource_name", filters.resourceName);

    if (filters.query) {
      values.push(`%${filters.query}%`);
      const index = values.length;
      clauses.push(
        `(actor ilike $${index}
          or action ilike $${index}
          or resource_type ilike $${index}
          or resource_name ilike $${index}
          or coalesce(details::text, '') ilike $${index})`
      );
    }

    values.push(limit);
    const limitIndex = values.length;
    const result = await query<AuditRow>(
      `select id, actor, action, cluster_id, resource_type, resource_name, details, created_at
       from audit_events
       ${clauses.length > 0 ? `where ${clauses.join(" and ")}` : ""}
       order by created_at desc
       limit $${limitIndex}`,
      values
    );
    return result.rows.map((row) => ({
      id: row.id,
      actor: row.actor,
      action: row.action,
      clusterId: row.cluster_id,
      resourceType: row.resource_type,
      resourceName: row.resource_name,
      details: row.details,
      createdAt: row.created_at.toISOString()
    }));
  }

  return auditEvents.filter((event) => matchesFilters(event, filters)).slice(0, limit);
}

function addWhere(clauses: string[], values: unknown[], column: string, value?: string) {
  if (!value) {
    return;
  }
  values.push(value);
  clauses.push(`${column} = $${values.length}`);
}

function matchesFilters(event: AuditEvent, filters: AuditEventFilters) {
  if (filters.clusterId && event.clusterId !== filters.clusterId) {
    return false;
  }
  if (filters.actor && event.actor !== filters.actor) {
    return false;
  }
  if (filters.action && event.action !== filters.action) {
    return false;
  }
  if (filters.resourceType && event.resourceType !== filters.resourceType) {
    return false;
  }
  if (filters.resourceName && event.resourceName !== filters.resourceName) {
    return false;
  }
  if (filters.query) {
    return `${event.actor} ${event.action} ${event.resourceType ?? ""} ${event.resourceName ?? ""} ${JSON.stringify(event.details ?? {})}`
      .toLowerCase()
      .includes(filters.query.toLowerCase());
  }
  return true;
}
