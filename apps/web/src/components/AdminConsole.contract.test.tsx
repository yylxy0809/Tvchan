import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  AdminRequestError,
  fetchAdminOpsStatus,
  fetchModuleCExecution,
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

test("module C execution fetch uses the authenticated read-only endpoint", async () => {
  const originalFetch = globalThis.fetch;
  let requestedUrl = "";
  let authorization = "";
  globalThis.fetch = async (input, init) => {
    requestedUrl = String(input);
    authorization = String((init?.headers as Record<string, string>)?.Authorization ?? "");
    return new Response(JSON.stringify({
      observed_at: "2026-07-18T00:00:00Z",
      readonly: true,
      running_parent_batches: 0,
      running_child_batches: 0,
      running_tasks: 0,
      batch: null,
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  };
  try {
    const result = await fetchModuleCExecution("admin-token", 42);
    assert.equal(result.readonly, true);
    assert.match(requestedUrl, /\/api\/v1\/admin\/ops\/module-c\/execution\?batch_id=42$/);
    assert.equal(authorization, "Bearer admin-token");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("admin console renders read-only Module C execution evidence without control actions", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  const apiSource = readFileSync(new URL("../api/adminRuntimeConfig.ts", import.meta.url), "utf8");

  for (const label of [
    "Batch and tasks",
    "Retry and leases",
    "Strict provenance",
    "Freshness and catalog drift",
  ]) assert.match(source, new RegExp(label));
  for (const field of [
    "retryable_failed", "exhausted_failed", "expired_leases", "max_attempts",
    "canonical_audit_run_id", "audit_evidence_sha256", "audit_checkpoint_sha256",
    "freshness_contract_sha256", "expected_closed_watermarks", "actual_checkpoint_watermarks",
    "catalog_generation_id", "catalog_control_revision", "catalog_revision_matches", "drift_reasons",
    "frozen_config_matches", "execution_identity_matches", "live_universe_matches",
    "catalog_manifest_matches", "future_scopes",
  ]) {
    assert.match(source, new RegExp(field));
    assert.match(apiSource, new RegExp(field));
  }
  assert.match(source, /title=\{value\}/);
  assert.doesNotMatch(source, /handle(?:Start|Retry|Activate)ModuleC/);
});

test("Module C request failure preserves the last snapshot and auth failure returns first", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  assert.match(source, /if \(handleAuthenticationFailure\(nextError\)\) return;/);
  assert.match(source, /setModuleCExecutionState\(\(current\) => \(\{[\s\S]*\.\.\.current,[\s\S]*stale: true,[\s\S]*reason: "request_failed"/);
  assert.match(source, /showing the last successful snapshot/);
});
