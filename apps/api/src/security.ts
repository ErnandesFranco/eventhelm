import type { FastifyRequest } from "fastify";
import { getApiTokens, getCollectorToken, getSecurityStatus } from "./config.js";
import type { ApiTokenConfig, AuthScope } from "./types.js";

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
  return config.scopes.includes("admin") || config.scopes.includes(scope) || (scope !== "read" && config.scopes.includes("write"));
}

function requireToken(request: FastifyRequest): ApiTokenConfig {
  const tokenConfig = tokenConfigFromRequest(request);
  if (!tokenConfig) {
    throw unauthorized("A valid EventHelm API bearer token is required.");
  }
  return tokenConfig;
}

export function actorFromRequest(request: FastifyRequest): string {
  const actor = request.headers["x-eventhelm-actor"] ?? request.headers["x-brokara-actor"];
  if (Array.isArray(actor)) {
    return actor[0] ?? tokenConfigFromRequest(request)?.actor ?? "system";
  }
  return actor ?? tokenConfigFromRequest(request)?.actor ?? "system";
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
}

export function assertCollectorAllowed(request: FastifyRequest) {
  const token = getCollectorToken();
  if (!token) {
    return;
  }

  const collectorToken = request.headers["x-eventhelm-collector-token"] ?? request.headers["x-brokara-collector-token"];
  if (collectorToken !== token) {
    throw unauthorized("A valid EventHelm collector token is required.");
  }
}
