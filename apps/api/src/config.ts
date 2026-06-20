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

function env(primary: string, fallback?: string): string | undefined {
  return process.env[primary] ?? (fallback ? process.env[fallback] : undefined);
}

export function loadClusters(): ClusterConfig[] {
  const raw = env("EVENTHELM_CLUSTERS_JSON", "BROKARA_CLUSTERS_JSON") ?? process.env.PLATFORM_CLUSTERS_JSON;
  if (!raw) {
    return defaultClusters;
  }

  const parsed = z.array(clusterSchema).safeParse(JSON.parse(raw));
  if (!parsed.success) {
    throw new Error(`Invalid EVENTHELM_CLUSTERS_JSON: ${parsed.error.message}`);
  }

  return parsed.data;
}

export function getPort(): number {
  return Number(process.env.API_PORT ?? 18080);
}

export function getSecurityStatus(): SecurityStatus {
  const authMode = (env("EVENTHELM_AUTH_MODE", "BROKARA_AUTH_MODE") ?? "dev") as SecurityMode;
  return {
    authMode,
    apiTokenConfigured: Boolean(env("EVENTHELM_API_TOKEN", "BROKARA_API_TOKEN")),
    collectorTokenConfigured: Boolean(env("EVENTHELM_COLLECTOR_TOKEN", "BROKARA_COLLECTOR_TOKEN")),
    corsOrigin: env("EVENTHELM_CORS_ORIGIN", "BROKARA_CORS_ORIGIN") ?? "*",
    writeConfirmationRequired: env("EVENTHELM_REQUIRE_WRITE_CONFIRMATION", "BROKARA_REQUIRE_WRITE_CONFIRMATION") === "true"
  };
}

export function getApiToken(): string | undefined {
  return env("EVENTHELM_API_TOKEN", "BROKARA_API_TOKEN");
}

export function getCollectorToken(): string | undefined {
  return env("EVENTHELM_COLLECTOR_TOKEN", "BROKARA_COLLECTOR_TOKEN");
}

export function getCorsOrigin(): string | true {
  return env("EVENTHELM_CORS_ORIGIN", "BROKARA_CORS_ORIGIN") ?? true;
}

export function isRebalanceExecutionEnabled(): boolean {
  return env("EVENTHELM_ENABLE_REBALANCE_EXECUTION", "BROKARA_ENABLE_REBALANCE_EXECUTION") === "true";
}
