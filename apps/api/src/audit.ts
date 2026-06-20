import { nanoid } from "nanoid";
import { persistenceMode, query } from "./db.js";
import type { AuditEvent } from "./types.js";

const auditEvents: AuditEvent[] = [];

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

export async function listAuditEvents(): Promise<AuditEvent[]> {
  if (persistenceMode() === "postgres") {
    const result = await query<AuditRow>(
      `select id, actor, action, cluster_id, resource_type, resource_name, details, created_at
       from audit_events
       order by created_at desc
       limit 250`
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

  return auditEvents;
}
