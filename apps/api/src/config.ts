import { z } from "zod";
import type { ApiTokenConfig, AuthScope, ClusterConfig, SecurityMode, SecurityStatus } from "./types.js";

export const authScopes = [
  "read",
  "write",
  "admin",
  "cluster:write",
  "topic:write",
  "message:write",
  "consumer:write",
  "rebalance:plan",
  "rebalance:review",
  "rebalance:execute",
  "agent:run"
] as const satisfies readonly AuthScope[];

const authScopeSchema = z.enum(authScopes);
const apiTokenConfigSchema = z.object({
  token: z.string().min(1),
  actor: z.string().min(1).optional(),
  scopes: z.array(authScopeSchema).min(1)
});

export const clusterSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  brokers: z.array(z.string().min(1)).min(1),
  ssl: z.boolean().optional(),
  sasl: z
    .object({
      mechanism: z.enum(["plain", "scram-sha-256", "scram-sha-512"]),
      username: z.string(),
      password: z.string().optional(),
      passwordEnv: z.string().min(1).optional()
    })
    .refine((sasl) => Boolean(sasl.password || sasl.passwordEnv), "SASL requires password or passwordEnv")
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
  const corsOrigin = env("EVENTHELM_CORS_ORIGIN", "BROKARA_CORS_ORIGIN") ?? "*";
  const apiTokens = getApiTokens();
  return {
    authMode,
    apiTokenConfigured: apiTokens.length > 0,
    apiTokenCount: apiTokens.length,
    configuredScopes: [...new Set(apiTokens.flatMap((token) => token.scopes))].sort(),
    collectorTokenConfigured: Boolean(env("EVENTHELM_COLLECTOR_TOKEN", "BROKARA_COLLECTOR_TOKEN")),
    corsOrigin,
    readAuthRequired: isReadAuthRequired(),
    writeConfirmationRequired: env("EVENTHELM_REQUIRE_WRITE_CONFIRMATION", "BROKARA_REQUIRE_WRITE_CONFIRMATION") === "true",
    writeRateLimitPerMinute: getWriteRateLimitPerMinute()
  };
}

export function getApiToken(): string | undefined {
  return env("EVENTHELM_API_TOKEN", "BROKARA_API_TOKEN");
}

export function getApiTokens(): ApiTokenConfig[] {
  const tokens: ApiTokenConfig[] = [];
  const legacyToken = getApiToken();
  if (legacyToken) {
    tokens.push({ token: legacyToken, actor: "api-token", scopes: ["admin"] });
  }

  const rawTokens = env("EVENTHELM_API_TOKENS_JSON", "BROKARA_API_TOKENS_JSON");
  if (rawTokens) {
    const parsed = z.array(apiTokenConfigSchema).safeParse(JSON.parse(rawTokens));
    if (!parsed.success) {
      throw new Error(`Invalid EVENTHELM_API_TOKENS_JSON: ${parsed.error.message}`);
    }
    tokens.push(...parsed.data);
  }

  return tokens;
}

export function getCollectorToken(): string | undefined {
  return env("EVENTHELM_COLLECTOR_TOKEN", "BROKARA_COLLECTOR_TOKEN");
}

export function isReadAuthRequired(): boolean {
  const configured = env("EVENTHELM_REQUIRE_READ_AUTH", "BROKARA_REQUIRE_READ_AUTH");
  if (configured !== undefined) {
    return configured === "true";
  }
  return (env("EVENTHELM_AUTH_MODE", "BROKARA_AUTH_MODE") ?? "dev") === "token";
}

export function getWriteRateLimitPerMinute(): number {
  const raw = env("EVENTHELM_WRITE_RATE_LIMIT_PER_MINUTE", "BROKARA_WRITE_RATE_LIMIT_PER_MINUTE");
  if (!raw) {
    return 0;
  }

  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error("EVENTHELM_WRITE_RATE_LIMIT_PER_MINUTE must be a non-negative integer.");
  }
  return parsed;
}

export function getCorsOrigin(): string | string[] | true {
  const corsOrigin = env("EVENTHELM_CORS_ORIGIN", "BROKARA_CORS_ORIGIN");
  if (!corsOrigin) {
    return true;
  }

  const origins = corsOrigin
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);

  return origins.length > 1 ? origins : origins[0] ?? true;
}

export function isRebalanceExecutionEnabled(): boolean {
  return env("EVENTHELM_ENABLE_REBALANCE_EXECUTION", "BROKARA_ENABLE_REBALANCE_EXECUTION") === "true";
}

export function getDatabaseUrl(): string | undefined {
  return env("EVENTHELM_DATABASE_URL", "DATABASE_URL");
}
