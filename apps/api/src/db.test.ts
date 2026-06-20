import test from "node:test";
import assert from "node:assert/strict";
import { listDatabaseMigrations } from "./db.js";

test("database migrations are ordered, unique, and checksumed", () => {
  const migrations = listDatabaseMigrations();
  const ids = migrations.map((migration) => migration.id);

  assert.ok(migrations.length > 0);
  assert.deepEqual(ids, [...ids].sort());
  assert.equal(new Set(ids).size, ids.length);
  assert.equal(migrations[0]?.id, "001_control_plane_foundation");

  for (const migration of migrations) {
    assert.match(migration.checksum, /^[a-f0-9]{64}$/);
    assert.ok(migration.name.length > 0);
  }
});
