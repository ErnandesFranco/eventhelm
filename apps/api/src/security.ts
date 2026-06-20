import type { FastifyRequest } from "fastify";
import { createHash } from "node:crypto";
import { getApiTokens, getCollectorToken, getSecurityStatus, getWriteRateLimitPerMinute } from "./config.js";
import type { ApiTokenConfig, AuthScope } from "./types.js";

const writeRateBuckets = new Map<string, number[]>();

function unauthorized(message: string) {
  const error = new Error(message);
  (error as Error & { statusCode: number }).statusCode = 401;
  return error;
}

function preconditionRequired(message: string) {
  const error = new Error(message);
  (error as Error & { statusCode: number }).statusCode = 428;
  return error;
}

function forbidden(message: string) {
  const error = new Error(message);
  (error as Error & { statusCode: number }).statusCode = 403;
  return error;
}

function tooManyRequests(message: string) {
  const error = new Error(message);
  (error as Error & { statusCode: number }).statusCode = 429;
  return error;
}

function bearerToken(request: FastifyRequest): string | undefined {
  const authorization = request.headers.authorization;
  if (!authorization?.startsWith("Bearer ")) {
    return undefined;
  }
  return authorization.slice("Bearer ".length);
}

function tokenConfigFromRequest(request: FastifyRequest): ApiTokenConfig | undefined {
  const token = bearerToken(request);
  if (!token) {
    return undefined;
  }
  return getApiTokens().find((candidate) => candidate.token === token);
}

function hasScope(config: ApiTokenConfig, scope: AuthScope) {
  const writeScopeAllowed = scope !== "read" && scope !== "cluster:breakglass" && config.scopes.includes("write");
  return config.scopes.includes("admin") || config.scopes.includes(scope) || writeScopeAllowed;
}

function requireToken(request: FastifyRequest): ApiTokenConfig {
  const tokenConfig = tokenConfigFromRequest(request);
  if (!tokenConfig) {
    throw unauthorized("A valid EventHelm API bearer token is required.");
  }
  return tokenConfig;
}

export function actorFromRequest(request: FastifyRequest): string {
  const security = getSecurityStatus();
  const tokenConfig = tokenConfigFromRequest(request);
  if (security.authMode === "token" && tokenConfig) {
    return tokenConfig.actor ?? "api-token";
  }

  const actor = request.headers["x-eventhelm-actor"] ?? request.headers["x-brokara-actor"];
  if (Array.isArray(actor)) {
    return actor[0] ?? tokenConfig?.actor ?? "system";
  }
  return actor ?? tokenConfig?.actor ?? "system";
}

export function assertReadAllowed(request: FastifyRequest) {
  const security = getSecurityStatus();
  if (security.authMode !== "token" || !security.readAuthRequired) {
    return;
  }

  const tokenConfig = requireToken(request);
  if (!hasScope(tokenConfig, "read")) {
    throw forbidden("This EventHelm API token is missing the read scope.");
  }
}

export function assertWriteAllowed(request: FastifyRequest, scope: AuthScope = "write") {
  const security = getSecurityStatus();
  if (security.authMode === "token") {
    const tokenConfig = requireToken(request);
    if (!hasScope(tokenConfig, scope)) {
      throw forbidden(`This EventHelm API token is missing the ${scope} scope.`);
    }
  }

  const confirmed = request.headers["x-eventhelm-confirm"] ?? request.headers["x-brokara-confirm"];
  if (security.writeConfirmationRequired && confirmed !== "true") {
    throw preconditionRequired("Write confirmation is required for this action.");
  }

  assertWriteRateLimit(request, scope);
}

export function assertSeparatedActor(
  actor: string,
  blockedActors: Array<{ role: string; actor?: string }>,
  action: string
) {
  if (getSecurityStatus().authMode !== "token") {
    return;
  }

  const conflict = blockedActors.find((candidate) => candidate.actor && candidate.actor === actor);
  if (conflict) {
    throw forbidden(`${action} requires separation of duties; ${conflict.role} '${actor}' cannot perform this step.`);
  }
}

function assertWriteRateLimit(request: FastifyRequest, scope: AuthScope) {
  const limit = getWriteRateLimitPerMinute();
  if (limit <= 0) {
    return;
  }

  const now = Date.now();
  const windowStart = now - 60_000;
  const key = `${rateLimitPrincipal(request)}\u0000${scope}`;
  const bucket = (writeRateBuckets.get(key) ?? []).filter((timestamp) => timestamp >= windowStart);
  if (bucket.length >= limit) {
    writeRateBuckets.set(key, bucket);
    throw tooManyRequests(`Write rate limit exceeded for principal '${actorFromRequest(request)}' and scope '${scope}'.`);
  }

  bucket.push(now);
  writeRateBuckets.set(key, bucket);
}

function rateLimitPrincipal(request: FastifyRequest) {
  const security = getSecurityStatus();
  const token = bearerToken(request);
  if (security.authMode === "token" && token) {
    return `token:${createHash("sha256").update(token).digest("hex").slice(0, 16)}`;
  }
  return `actor:${actorFromRequest(request)}`;
}

export function assertCollectorAllowed(request: FastifyRequest) {
  const security = getSecurityStatus();
  const token = getCollectorToken();
  if (!token) {
    if (security.authMode === "token") {
      throw unauthorized("EVENTHELM_COLLECTOR_TOKEN is required when EVENTHELM_AUTH_MODE=token.");
    }
    return;
  }

  const collectorToken = request.headers["x-eventhelm-collector-token"] ?? request.headers["x-brokara-collector-token"];
  if (collectorToken !== token) {
    throw unauthorized("A valid EventHelm collector token is required.");
  }
}
