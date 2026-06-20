import test from "node:test";
import assert from "node:assert/strict";
import { listAuditEvents, recordAudit } from "./audit.js";

test("audit filters narrow memory audit events", async () => {
  await recordAudit({
    actor: "audit-filter-a",
    action: "topic.create",
    clusterId: "audit-filter-cluster",
    resourceType: "topic",
    resourceName: "orders.created",
    details: { partitions: 3, reviewId: "review-search-target" }
  });
  await recordAudit({
    actor: "audit-filter-b",
    action: "cluster.upsert",
    clusterId: "audit-filter-cluster",
    resourceType: "cluster",
    resourceName: "analytics",
    details: { brokers: 3 }
  });

  const byActor = await listAuditEvents({ actor: "audit-filter-a" });
  const byAction = await listAuditEvents({ action: "cluster.upsert" });
  const byQuery = await listAuditEvents({ query: "orders" });
  const byDetails = await listAuditEvents({ query: "review-search-target" });

  assert.ok(byActor.every((event) => event.actor === "audit-filter-a"));
  assert.ok(byAction.some((event) => event.resourceName === "analytics"));
  assert.ok(byQuery.some((event) => event.resourceName === "orders.created"));
  assert.ok(byDetails.some((event) => event.details?.reviewId === "review-search-target"));
});
