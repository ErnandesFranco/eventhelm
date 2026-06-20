import os from "node:os";
import path from "node:path";
import { readdir, stat, statfs } from "node:fs/promises";
import { Kafka, logLevel } from "kafkajs";

const version = "0.1.0";
const startedAt = new Date().toISOString();

const controlPlaneUrl = requiredEnv("CONTROL_PLANE_URL");
const clusterId = requiredEnv("CLUSTER_ID");
const brokerId = requiredEnv("BROKER_ID");
const collectorId = process.env.COLLECTOR_ID ?? `${clusterId}-broker-${brokerId}`;
const collectorToken = process.env.EVENTHELM_COLLECTOR_TOKEN ?? process.env.BROKARA_COLLECTOR_TOKEN;
const brokerDataPath = process.env.BROKER_DATA_PATH;
const kafkaBrokers = requiredEnv("KAFKA_BROKERS")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);
const intervalMs = Number(process.env.COLLECTOR_INTERVAL_MS ?? 10000);
let stopping = false;

const kafka = new Kafka({
  clientId: `eventhelm-collector-${collectorId}`,
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
    headers["x-eventhelm-collector-token"] = collectorToken;
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
    const [disk, partitions] = brokerDataPath
      ? await Promise.all([readDiskTelemetry(brokerDataPath), readPartitionLogSizes(brokerDataPath)])
      : [undefined, []];

    await postJson("/api/collectors/snapshot", {
      ...basePayload(),
      brokerCount: cluster.brokers.length,
      topicCount: topics.filter((topic) => !topic.startsWith("__")).length,
      controllerId: cluster.controller,
      kafkaClusterId: cluster.clusterId,
      disk,
      partitions,
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

async function readPartitionLogSizes(root: string) {
  const entries = await readdir(root, { withFileTypes: true });
  const sizes = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory())
      .map(async (entry) => {
        const partition = parsePartitionDirectory(entry.name);
        if (!partition) {
          return undefined;
        }

        const logDir = path.join(root, entry.name);
        try {
          return {
            ...partition,
            sizeBytes: await directorySize(logDir),
            logDir
          };
        } catch (error) {
          console.warn(`[collector:${collectorId}] could not size ${logDir}: ${error instanceof Error ? error.message : String(error)}`);
          return undefined;
        }
      })
  );

  return sizes.filter((size): size is NonNullable<typeof size> => Boolean(size));
}

function parsePartitionDirectory(name: string) {
  const match = /^(.*)-(\d+)$/.exec(name);
  if (!match) {
    return undefined;
  }
  return {
    topic: match[1],
    partition: Number(match[2])
  };
}

async function directorySize(directory: string): Promise<number> {
  const entries = await readdir(directory, { withFileTypes: true });
  let total = 0;
  for (const entry of entries) {
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      total += await directorySize(fullPath);
    } else if (entry.isFile()) {
      total += (await stat(fullPath)).size;
    }
  }
  return total;
}

async function readDiskTelemetry(path: string) {
  const stats = await statfs(path);
  const totalBytes = Number(stats.blocks) * Number(stats.bsize);
  const freeBytes = Number(stats.bavail) * Number(stats.bsize);
  const usedBytes = Math.max(0, totalBytes - freeBytes);
  const usedPercent = totalBytes > 0 ? (usedBytes / totalBytes) * 100 : 0;
  return {
    path,
    totalBytes,
    freeBytes,
    usedBytes,
    usedPercent: Number(usedPercent.toFixed(2)),
    pressure: diskPressure(usedPercent),
    sampledAt: new Date().toISOString()
  };
}

function diskPressure(usedPercent: number) {
  if (usedPercent >= 92) {
    return "critical";
  }
  if (usedPercent >= 85) {
    return "high";
  }
  if (usedPercent >= 75) {
    return "watch";
  }
  return "normal";
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
