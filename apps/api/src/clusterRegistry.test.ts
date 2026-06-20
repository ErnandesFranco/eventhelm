import test from "node:test";
import assert from "node:assert/strict";
import {
  deleteClusterConfig,
  initializeClusterRegistry,
  listClusterConfigs,
  toPublicCluster,
  upsertClusterConfig
} from "./clusterRegistry.js";

test("cluster registry bootstraps env clusters and masks public credentials", async () => {
  await initializeClusterRegistry([
    {
      id: "registry-test-local",
      name: "Registry Test Local",
      brokers: ["localhost:19092"]
    }
  ]);

  await upsertClusterConfig(
    {
      id: "registry-test-secure",
      name: "Registry Test Secure",
      brokers: ["kafka.example:9093"],
      ssl: true,
      sasl: {
        mechanism: "scram-sha-512",
        username: "eventhelm",
        password: "do-not-expose"
      }
    },
    "api"
  );
  await upsertClusterConfig(
    {
      id: "registry-test-secret-ref",
      name: "Registry Test Secret Ref",
      brokers: ["kafka-secret.example:9093"],
      ssl: true,
      sasl: {
        mechanism: "scram-sha-512",
        username: "eventhelm",
        passwordEnv: "EVENTHELM_TEST_KAFKA_PASSWORD"
      }
    },
    "api"
  );

  const clusters = await listClusterConfigs();
  const secure = clusters.find((cluster) => cluster.id === "registry-test-secure");
  const publicSecure = secure ? toPublicCluster(secure) : undefined;
  const secretRef = clusters.find((cluster) => cluster.id === "registry-test-secret-ref");
  const publicSecretRef = secretRef ? toPublicCluster(secretRef) : undefined;

  assert.ok(clusters.some((cluster) => cluster.id === "registry-test-local" && cluster.source === "environment"));
  assert.equal(secure?.source, "api");
  assert.equal(publicSecure?.saslConfigured, true);
  assert.equal(publicSecure?.saslPasswordSource, "inline");
  assert.equal(JSON.stringify(publicSecure).includes("do-not-expose"), false);
  assert.equal(publicSecretRef?.saslConfigured, true);
  assert.equal(publicSecretRef?.saslPasswordSource, "environment");
  assert.equal(JSON.stringify(publicSecretRef).includes("EVENTHELM_TEST_KAFKA_PASSWORD"), false);

  const deleted = await deleteClusterConfig("registry-test-secure");
  await deleteClusterConfig("registry-test-secret-ref");
  const afterDelete = await listClusterConfigs();

  assert.equal(deleted?.id, "registry-test-secure");
  assert.equal(afterDelete.some((cluster) => cluster.id === "registry-test-secure"), false);
  assert.equal(afterDelete.some((cluster) => cluster.id === "registry-test-secret-ref"), false);
});
