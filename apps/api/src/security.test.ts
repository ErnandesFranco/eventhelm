import assert from "node:assert/strict";
import test from "node:test";
import type { FastifyRequest } from "fastify";
import { actorFromRequest, assertReadAllowed, assertWriteAllowed } from "./security.js";

const managedEnvKeys = [
  "EVENTHELM_AUTH_MODE",
  "EVENTHELM_API_TOKEN",
  "EVENTHELM_API_TOKENS_JSON",
  "EVENTHELM_REQUIRE_READ_AUTH",
  "EVENTHELM_REQUIRE_WRITE_CONFIRMATION",
  "EVENTHELM_WRITE_RATE_LIMIT_PER_MINUTE"
];

test("scoped API tokens enforce write scope boundaries", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_REQUIRE_WRITE_CONFIRMATION: "true",
      EVENTHELM_API_TOKENS_JSON: JSON.stringify([
        { token: "topic-token", actor: "topic-operator", scopes: ["read", "topic:write"] }
      ])
    },
    () => {
      const request = requestWithToken("topic-token", true);

      assert.doesNotThrow(() => assertWriteAllowed(request, "topic:write"));
      const error = captureError(() => assertWriteAllowed(request, "cluster:write"));
      assert.equal(error.statusCode, 403);
      assert.equal(actorFromRequest(request), "topic-operator");
    }
  );
});

test("cluster breakglass scope is not granted by generic write", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_REQUIRE_WRITE_CONFIRMATION: "true",
      EVENTHELM_API_TOKENS_JSON: JSON.stringify([
        { token: "write-token", actor: "writer", scopes: ["read", "write"] },
        { token: "breakglass-token", actor: "breakglass", scopes: ["read", "cluster:breakglass"] }
      ])
    },
    () => {
      const genericWrite = requestWithToken("write-token", true);
      const breakglass = requestWithToken("breakglass-token", true);

      assert.doesNotThrow(() => assertWriteAllowed(genericWrite, "cluster:write"));
      const error = captureError(() => assertWriteAllowed(genericWrite, "cluster:breakglass"));
      assert.equal(error.statusCode, 403);
      assert.doesNotThrow(() => assertWriteAllowed(breakglass, "cluster:breakglass"));
    }
  );
});

test("token mode ignores caller supplied actor headers", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_API_TOKENS_JSON: JSON.stringify([
        { token: "actor-token", actor: "configured-operator", scopes: ["read", "topic:write"] }
      ])
    },
    () => {
      assert.equal(actorFromRequest(requestWithToken("actor-token", false, "spoofed-operator")), "configured-operator");
    }
  );
});

test("read auth requires read scope in token mode", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_API_TOKENS_JSON: JSON.stringify([
        { token: "read-token", scopes: ["read"] },
        { token: "write-token", scopes: ["topic:write"] }
      ])
    },
    () => {
      assert.doesNotThrow(() => assertReadAllowed(requestWithToken("read-token")));
      const error = captureError(() => assertReadAllowed(requestWithToken("write-token")));
      assert.equal(error.statusCode, 403);
    }
  );
});

test("legacy API token keeps admin access", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_API_TOKEN: "legacy-admin",
      EVENTHELM_REQUIRE_WRITE_CONFIRMATION: "true"
    },
    () => {
      const request = requestWithToken("legacy-admin", true);
      assert.doesNotThrow(() => assertReadAllowed(request));
      assert.doesNotThrow(() => assertWriteAllowed(request, "rebalance:execute"));
    }
  );
});

test("write rate limit is enforced per actor and scope", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_API_TOKENS_JSON: JSON.stringify([
        { token: "limited-token", actor: "limited-operator", scopes: ["read", "topic:write"] }
      ]),
      EVENTHELM_WRITE_RATE_LIMIT_PER_MINUTE: "2"
    },
    () => {
      const request = requestWithToken("limited-token");
      assert.doesNotThrow(() => assertWriteAllowed(request, "topic:write"));
      assert.doesNotThrow(() => assertWriteAllowed(request, "topic:write"));
      const error = captureError(() => assertWriteAllowed(request, "topic:write"));
      assert.equal(error.statusCode, 429);
    }
  );
});

test("write rate limit follows token when actor headers change", () => {
  withEnv(
    {
      EVENTHELM_AUTH_MODE: "token",
      EVENTHELM_API_TOKENS_JSON: JSON.stringify([
        { token: "limited-spoof-token", actor: "limited-operator", scopes: ["read", "topic:write"] }
      ]),
      EVENTHELM_WRITE_RATE_LIMIT_PER_MINUTE: "2"
    },
    () => {
      assert.doesNotThrow(() => assertWriteAllowed(requestWithToken("limited-spoof-token", false, "first-header"), "topic:write"));
      assert.doesNotThrow(() => assertWriteAllowed(requestWithToken("limited-spoof-token", false, "second-header"), "topic:write"));
      const error = captureError(() => assertWriteAllowed(requestWithToken("limited-spoof-token", false, "third-header"), "topic:write"));
      assert.equal(error.statusCode, 429);
    }
  );
});

function requestWithToken(token: string, confirmed = false, actor?: string): FastifyRequest {
  return {
    headers: {
      authorization: `Bearer ${token}`,
      ...(actor ? { "x-eventhelm-actor": actor } : {}),
      ...(confirmed ? { "x-eventhelm-confirm": "true" } : {})
    }
  } as unknown as FastifyRequest;
}

function captureError(run: () => void): Error & { statusCode?: number } {
  try {
    run();
  } catch (error) {
    return error as Error & { statusCode?: number };
  }
  throw new Error("Expected function to throw.");
}

function withEnv(env: Record<string, string>, run: () => void) {
  const original = new Map(managedEnvKeys.map((key) => [key, process.env[key]]));
  for (const key of managedEnvKeys) {
    delete process.env[key];
  }
  Object.assign(process.env, env);
  try {
    run();
  } finally {
    for (const key of managedEnvKeys) {
      const value = original.get(key);
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
}
