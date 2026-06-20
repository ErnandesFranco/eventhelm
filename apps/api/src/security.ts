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
  const actor = request.headers["x-brokara-actor"];
  return Array.isArray(actor) ? actor[0] ?? "system" : actor ?? "system";
}

export function assertWriteAllowed(request: FastifyRequest) {
  const security = getSecurityStatus();
  if (security.authMode === "token" && bearerToken(request) !== getApiToken()) {
    throw unauthorized("A valid Brokara API bearer token is required.");
  }

  if (security.writeConfirmationRequired && request.headers["x-brokara-confirm"] !== "true") {
    throw preconditionRequired("Write confirmation is required for this action.");
  }
}

export function assertCollectorAllowed(request: FastifyRequest) {
  const token = getCollectorToken();
  if (!token) {
    return;
  }

  if (request.headers["x-brokara-collector-token"] !== token) {
    throw unauthorized("A valid Brokara collector token is required.");
  }
}
