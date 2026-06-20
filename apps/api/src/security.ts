import type { FastifyRequest } from "fastify";
import { getApiToken, getCollectorToken, getSecurityStatus } from "./config.js";

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

function bearerToken(request: FastifyRequest): string | undefined {
  const authorization = request.headers.authorization;
  if (!authorization?.startsWith("Bearer ")) {
    return undefined;
  }
  return authorization.slice("Bearer ".length);
}

export function actorFromRequest(request: FastifyRequest): string {
  const actor = request.headers["x-eventhelm-actor"] ?? request.headers["x-brokara-actor"];
  return Array.isArray(actor) ? actor[0] ?? "system" : actor ?? "system";
}

export function assertWriteAllowed(request: FastifyRequest) {
  const security = getSecurityStatus();
  if (security.authMode === "token" && bearerToken(request) !== getApiToken()) {
    throw unauthorized("A valid EventHelm API bearer token is required.");
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
