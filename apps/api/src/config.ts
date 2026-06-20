import { z } from "zod";
import type { ClusterConfig, SecurityMode, SecurityStatus } from "./types.js";

const clusterSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  brokers: z.array(z.string().min(1)).min(1),
  ssl: z.boolean().optional(),
  sasl: z
    .object({
      mechanism: z.enum(["plain", "scram-sha-256", "scram-sha-512"]),
      username: z.string(),
      password: z.string()
    })
    .optional()
});

const defaultClusters: ClusterConfig[] = [
  {
    id: "local",
    name: "Local Kafka Lab",
    brokers: ["localhost:19092", "localhost:29092", "localhost:39092"]
  }
];

export function loadClusters(): ClusterConfig[] {
  const raw = process.env.BROKARA_CLUSTERS_JSON ?? process.env.PLATFORM_CLUSTERS_JSON;
  if (!raw) {
    return defaultClusters;
  }

  const parsed = z.array(clusterSchema).safeParse(JSON.parse(raw));
  if (!parsed.success) {
    throw new Error(`Invalid BROKARA_CLUSTERS_JSON: ${parsed.error.message}`);
  }

  return parsed.data;
}

export function getPort(): number {
  return Number(process.env.API_PORT ?? 18080);
}

export function getSecurityStatus(): SecurityStatus {
  const authMode = (process.env.BROKARA_AUTH_MODE ?? "dev") as SecurityMode;
  return {
    authMode,
    apiTokenConfigured: Boolean(process.env.BROKARA_API_TOKEN),
    collectorTokenConfigured: Boolean(process.env.BROKARA_COLLECTOR_TOKEN),
    corsOrigin: process.env.BROKARA_CORS_ORIGIN ?? "*",
    writeConfirmationRequired: process.env.BROKARA_REQUIRE_WRITE_CONFIRMATION === "true"
  };
}

export function getApiToken(): string | undefined {
  return process.env.BROKARA_API_TOKEN;
}

export function getCollectorToken(): string | undefined {
  return process.env.BROKARA_COLLECTOR_TOKEN;
}

export function getCorsOrigin(): string | true {
  return process.env.BROKARA_CORS_ORIGIN ?? true;
}
