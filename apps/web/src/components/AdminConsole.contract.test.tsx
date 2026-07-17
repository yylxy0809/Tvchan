import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  AdminRequestError,
  fetchAdminOpsStatus,
  isAdminAuthFailure,
} from "../api/adminRuntimeConfig";

test("admin console includes editable iWencai key metadata", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  assert.match(source, /<span>API Key<\/span>/);
  assert.match(source, /autoComplete="new-password"/);
  assert.match(source, /nextWencaiPriority/);
  assert.match(source, /api_keys: current\.api_keys\.filter/);
});

test("admin console exposes lifecycle observer health, backlog, and watermark", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  const apiSource = readFileSync(new URL("../api/adminRuntimeConfig.ts", import.meta.url), "utf8");
  assert.match(source, /fetchAdminOpsStatus/);
  assert.match(source, /Lifecycle observer/);
  assert.match(source, /pending/);
  assert.match(source, /processing/);
  assert.match(source, /failed/);
  assert.match(source, /dead_letter/);
  assert.match(source, /oldest_backlog_at/);
  assert.match(source, /observer_watermark/);
  assert.match(source, /heartbeat_age_seconds/);
  assert.match(source, /heartbeat_stale_after_seconds/);
  assert.match(source, /expected_observer_name/);
  assert.match(source, /unavailable/);
  assert.match(source, /degraded/);
  assert.match(apiSource, /"unavailable" \| "degraded" \| "healthy"/);
  assert.match(apiSource, /expected_observer_name/);
  assert.match(apiSource, /heartbeat_age_seconds/);
  assert.match(apiSource, /heartbeat_stale_after_seconds/);
});

test("admin console passes its authenticated admin token to durable token CRUD", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  assert.match(source, /listAdminTokens\(adminToken\)/);
  assert.match(source, /createAdminToken\(adminToken,/);
  assert.match(source, /disableAdminToken\(adminToken, id\)/);
  assert.match(source, /deleteAdminToken\(adminToken, id\)/);
});

test("admin runtime requests preserve auth status without leaking the token", async () => {
  const token = "admin-secret-token";
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () =>
    new Response(JSON.stringify({ detail: `expired ${token}` }), {
      status: 401,
      statusText: "Unauthorized",
      headers: { "Content-Type": "application/json" },
    });
  try {
    await assert.rejects(fetchAdminOpsStatus(token), (error: unknown) => {
      assert.ok(error instanceof AdminRequestError);
      assert.equal(error.status, 401);
      assert.doesNotMatch(error.message, new RegExp(token));
      return true;
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("admin auth failures logout instead of fabricating observer health", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  const appSource = readFileSync(new URL("../App.tsx", import.meta.url), "utf8");

  assert.match(source, /onAuthenticationFailure/);
  assert.match(source, /handleAuthenticationFailure\(nextError\)/);
  assert.match(source, /isAdminAuthFailure\(error\)/);
  assert.match(source, /onAuthenticationFailure\(\);\s*return true;/);
  assert.match(appSource, /onAuthenticationFailure=\{handleLogout\}/);
  assert.match(appSource, /clearSavedSessionMeta\(\)/);
  assert.equal(isAdminAuthFailure(new AdminRequestError(401, "expired")), true);
  assert.equal(isAdminAuthFailure(new AdminRequestError(403, "forbidden")), true);
  assert.equal(isAdminAuthFailure(new AdminRequestError(500, "server error")), false);
});

test("non-auth ops failures remain locally degraded", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");

  assert.match(source, /reason: "request_failed"/);
  assert.match(source, /status: "degraded"/);
  assert.match(source, /status: "unavailable"/);
});
