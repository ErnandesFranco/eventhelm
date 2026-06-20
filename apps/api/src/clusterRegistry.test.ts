import test from "node:test";
import assert from "node:assert/strict";
import {
  clusterChangeReviewStateDrift,
  clusterSecretPolicyViolation,
  createClusterChangeReview,
  deleteClusterConfig,
  getClusterChangeReview,
  initializeClusterRegistry,
  listClusterChangeReviews,
  listClusterConfigs,
  markClusterChangeReview,
  markClusterChangeReviewApplied,
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

test("cluster change reviews preserve sanitized public metadata through review lifecycle", async () => {
  await initializeClusterRegistry([]);
  const review = await createClusterChangeReview(
    {
      action: "upsert",
      cluster: {
        id: "review-test-secure",
        name: "Review Test Secure",
        brokers: ["kafka-review.example:9093"],
        ssl: true,
        sasl: {
          mechanism: "scram-sha-512",
          username: "eventhelm",
          password: "do-not-expose"
        }
      }
    },
    "review-author",
    await listClusterConfigs()
  );

  assert.equal(review.status, "pending");
  assert.equal(review.request.cluster?.saslConfigured, true);
  assert.equal(JSON.stringify(review).includes("do-not-expose"), false);
  assert.ok(review.warnings.some((warning) => warning.includes("Inline SASL passwords")));

  const approved = await markClusterChangeReview(review.id, "approved", "reviewer-a", "ship it");
  const internal = await getClusterChangeReview(review.id);
  const listed = await listClusterChangeReviews(5);

  assert.equal(approved?.status, "approved");
  assert.equal(approved?.reviewedBy, "reviewer-a");
  assert.equal(internal?.request.action, "upsert");
  assert.equal(listed[0]?.id, review.id);

  const applied = await markClusterChangeReviewApplied(review.id, "applier-a");
  assert.equal(applied?.status, "applied");
  assert.equal(applied?.appliedBy, "applier-a");
});

test("cluster change review drift detects registry changes before apply", async () => {
  await initializeClusterRegistry([]);
  const review = await createClusterChangeReview(
    {
      action: "upsert",
      cluster: {
        id: "review-drift-cluster",
        name: "Review Drift Cluster",
        brokers: ["kafka-a.example:9092"]
      }
    },
    "review-author",
    await listClusterConfigs()
  );
  const internal = await getClusterChangeReview(review.id);

  assert.ok(internal);
  assert.deepEqual(clusterChangeReviewStateDrift(internal, await listClusterConfigs()), []);

  await upsertClusterConfig(
    {
      id: "review-drift-cluster",
      name: "Review Drift Cluster Outside Review",
      brokers: ["kafka-b.example:9092"]
    },
    "api"
  );

  assert.match(clusterChangeReviewStateDrift(internal, await listClusterConfigs())[0] ?? "", /created after this review/);
});

test("cluster secret policy rejects inline SASL passwords in token mode", () => {
  const inlineCluster = {
    id: "secret-policy-inline",
    name: "Secret Policy Inline",
    brokers: ["kafka.example:9093"],
    sasl: {
      mechanism: "scram-sha-512" as const,
      username: "eventhelm",
      password: "do-not-store"
    }
  };
  const envCluster = {
    ...inlineCluster,
    sasl: {
      mechanism: "scram-sha-512" as const,
      username: "eventhelm",
      passwordEnv: "EVENTHELM_KAFKA_PASSWORD"
    }
  };

  assert.match(clusterSecretPolicyViolation(inlineCluster, "token") ?? "", /Inline Kafka SASL passwords/);
  assert.equal(clusterSecretPolicyViolation(envCluster, "token"), undefined);
  assert.equal(clusterSecretPolicyViolation(inlineCluster, "dev"), undefined);
});
