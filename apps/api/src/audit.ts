import { nanoid } from "nanoid";
import type { AuditEvent } from "./types.js";

const auditEvents: AuditEvent[] = [];

export function recordAudit(event: Omit<AuditEvent, "id" | "createdAt">): AuditEvent {
  const created: AuditEvent = {
    ...event,
    id: nanoid(),
    createdAt: new Date().toISOString()
  };

  auditEvents.unshift(created);
  auditEvents.splice(250);
  return created;
}

export function listAuditEvents(): AuditEvent[] {
  return auditEvents;
}
