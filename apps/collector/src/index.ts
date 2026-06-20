import os from "node:os";
import { Kafka, logLevel } from "kafkajs";

const version = "0.1.0";
const startedAt = new Date().toISOString();

const controlPlaneUrl = requiredEnv("CONTROL_PLANE_URL");
const clusterId = requiredEnv("CLUSTER_ID");
const brokerId = requiredEnv("BROKER_ID");
const collectorId = process.env.COLLECTOR_ID ?? `${clusterId}-broker-${brokerId}`;
const collectorToken = process.env.BROKARA_COLLECTOR_TOKEN;
const kafkaBrokers = requiredEnv("KAFKA_BROKERS")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);
const intervalMs = Number(process.env.COLLECTOR_INTERVAL_MS ?? 10000);
let stopping = false;

const kafka = new Kafka({
  clientId: `brokara-collector-${collectorId}`,
  brokers: kafkaBrokers,
  logLevel: logLevel.ERROR
});

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function basePayload() {
  return {
    collectorId,
    clusterId,
    brokerId,
    hostname: os.hostname(),
    version,
    startedAt,
    observedAt: new Date().toISOString()
  };
}

async function postJson(path: string, body: unknown) {
  const headers: Record<string, string> = {
    "content-type": "application/json"
  };

  if (collectorToken) {
    headers["x-brokara-collector-token"] = collectorToken;
  }

  const response = await fetch(`${controlPlaneUrl}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body)
  });

  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}: ${await response.text()}`);
  }
}

async function sendHeartbeat() {
  await postJson("/api/collectors/heartbeat", basePayload());
}

async function sendSnapshot() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const [cluster, topics] = await Promise.all([admin.describeCluster(), admin.listTopics()]);
    await postJson("/api/collectors/snapshot", {
      ...basePayload(),
      brokerCount: cluster.brokers.length,
      topicCount: topics.filter((topic) => !topic.startsWith("__")).length,
      controllerId: cluster.controller,
      kafkaClusterId: cluster.clusterId,
      brokers: cluster.brokers.map((broker) => ({
        nodeId: broker.nodeId,
        host: broker.host,
        port: broker.port
      }))
    });
  } finally {
    await admin.disconnect();
  }
}

async function tick() {
  try {
    await sendHeartbeat();
    await sendSnapshot();
    console.log(`[collector:${collectorId}] snapshot sent`);
  } catch (error) {
    console.error(`[collector:${collectorId}] ${error instanceof Error ? error.message : String(error)}`);
  }
}

console.log(`[collector:${collectorId}] starting for cluster=${clusterId} broker=${brokerId}`);
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    stopping = true;
  });
}

await loop();

async function loop() {
  while (!stopping) {
    await tick();
    await new Promise((resolve) => setTimeout(resolve, intervalMs + Math.floor(Math.random() * 1000)));
  }
}
