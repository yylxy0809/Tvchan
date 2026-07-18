import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import {
  type AdminOpsStatus,
  AdminRequestError,
  fetchAdminOpsStatus,
  fetchModuleCExecution,
  handleAdminAuthenticationFailure,
  isAdminAuthFailure,
} from "../api/adminRuntimeConfig";
import {
  ModuleCBatchSelector,
  ModuleCSelectionEvidenceCard,
  runLatestAdminOpsStatusRequest,
} from "./AdminConsole";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

const HEALTHY_OPS_STATUS: AdminOpsStatus = {
  status: "ok",
  lifecycle_observer: {
    status: "healthy",
    deployed: true,
    expected_observer_name: "observer-v1",
  },
};

const DEGRADED_OPS_STATUS: AdminOpsStatus = {
  status: "degraded",
  lifecycle_observer: {
    status: "degraded",
    deployed: true,
    expected_observer_name: "observer-v1",
    reason: "heartbeat_stale",
  },
};

const PASS_SELECTION = {
  status: "pass" as const,
  contract_version: "module-c-canary-selection-v2",
  manifest_sha256: "a".repeat(64),
  source_build_id: "11111111-1111-1111-1111-111111111111",
  activity_basis: "pinned-audit-5f-rows-per-1d-session-v1",
  board_counts: { main_board: 5, chinext: 5, star: 5, bj: 5 },
  boundary_counts: {
    main_board: { lower: 2, middle: 1, upper: 2 },
    chinext: { lower: 2, middle: 1, upper: 2 },
    star: { lower: 2, middle: 1, upper: 2 },
    bj: { lower: 2, middle: 1, upper: 2 },
  },
  contract_matches: true,
  hash_matches: true,
  source_matches: true,
  quotas_match: true,
  active_universe_matches: true,
  drift_reasons: [],
};

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

test("a stale observer success cannot replace the latest completed status", async () => {
  const epoch = { current: 0 };
  const lifecycle = { active: true, generation: 1 };
  const first = deferred<AdminOpsStatus>();
  const second = deferred<AdminOpsStatus>();
  let rendered: AdminOpsStatus | null = null;
  const request = (load: () => Promise<AdminOpsStatus>) =>
    runLatestAdminOpsStatusRequest(epoch, lifecycle, {
      load,
      apply: (status) => { rendered = status; },
      degrade: () => { rendered = DEGRADED_OPS_STATUS; },
      handleAuthenticationFailure: () => false,
    });

  const olderRequest = request(() => first.promise);
  const latestRequest = request(() => second.promise);
  second.resolve(DEGRADED_OPS_STATUS);
  await latestRequest;
  first.resolve(HEALTHY_OPS_STATUS);
  await olderRequest;

  assert.equal(rendered, DEGRADED_OPS_STATUS);
});

test("a stale observer 5xx cannot degrade a newer successful status", async () => {
  const epoch = { current: 0 };
  const lifecycle = { active: true, generation: 1 };
  const first = deferred<AdminOpsStatus>();
  const second = deferred<AdminOpsStatus>();
  let rendered: AdminOpsStatus | null = null;
  let degradations = 0;
  const request = (load: () => Promise<AdminOpsStatus>) =>
    runLatestAdminOpsStatusRequest(epoch, lifecycle, {
      load,
      apply: (status) => { rendered = status; },
      degrade: () => {
        degradations += 1;
        rendered = DEGRADED_OPS_STATUS;
      },
      handleAuthenticationFailure: () => false,
    });

  const olderRequest = request(() => first.promise);
  const latestRequest = request(() => second.promise);
  second.resolve(HEALTHY_OPS_STATUS);
  await latestRequest;
  first.reject(new AdminRequestError(503, "service unavailable"));
  await olderRequest;

  assert.equal(rendered, HEALTHY_OPS_STATUS);
  assert.equal(degradations, 0);
});

test("a stale observer authentication failure still triggers logout", async () => {
  const epoch = { current: 0 };
  const lifecycle = { active: true, generation: 1 };
  const first = deferred<AdminOpsStatus>();
  const second = deferred<AdminOpsStatus>();
  let authenticationFailures = 0;
  let rendered: AdminOpsStatus | null = null;
  const request = (load: () => Promise<AdminOpsStatus>) =>
    runLatestAdminOpsStatusRequest(epoch, lifecycle, {
      load,
      apply: (status) => { rendered = status; },
      degrade: () => { rendered = DEGRADED_OPS_STATUS; },
      handleAuthenticationFailure: (error) =>
        handleAdminAuthenticationFailure(error, () => {
          authenticationFailures += 1;
        }),
    });

  const olderRequest = request(() => first.promise);
  const latestRequest = request(() => second.promise);
  second.resolve(HEALTHY_OPS_STATUS);
  await latestRequest;
  first.reject(new AdminRequestError(401, "expired"));
  await olderRequest;

  assert.equal(rendered, HEALTHY_OPS_STATUS);
  assert.equal(authenticationFailures, 1);
});

test("an observer response from a cleaned-up lifecycle cannot affect the next setup", async () => {
  const epoch = { current: 0 };
  const lifecycle = { active: true, generation: 1 };
  const first = deferred<AdminOpsStatus>();
  const second = deferred<AdminOpsStatus>();
  let authenticationFailures = 0;
  let applications = 0;
  let degradations = 0;
  let rendered: AdminOpsStatus | null = null;
  const request = (load: () => Promise<AdminOpsStatus>) =>
    runLatestAdminOpsStatusRequest(epoch, lifecycle, {
      load,
      apply: (status) => {
        applications += 1;
        rendered = status;
      },
      degrade: () => {
        degradations += 1;
        rendered = DEGRADED_OPS_STATUS;
      },
      handleAuthenticationFailure: (error) =>
        handleAdminAuthenticationFailure(error, () => {
          authenticationFailures += 1;
        }),
    });

  const oldLifecycleRequest = request(() => first.promise);
  lifecycle.active = false;
  epoch.current += 1;
  lifecycle.generation += 1;
  lifecycle.active = true;
  const newLifecycleRequest = request(() => second.promise);
  second.resolve(HEALTHY_OPS_STATUS);
  await newLifecycleRequest;
  first.reject(new AdminRequestError(401, "expired old lifecycle"));
  await oldLifecycleRequest;

  assert.equal(rendered, HEALTHY_OPS_STATUS);
  assert.equal(applications, 1);
  assert.equal(degradations, 0);
  assert.equal(authenticationFailures, 0);

  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  assert.match(source, /\+\+opsStatusLifecycle\.current\.generation/);
  assert.match(source, /opsStatusLifecycle\.current\.active = false/);
  assert.match(source, /opsStatusRequestEpoch\.current \+= 1/);
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

test("admin auth rejection preserves status when the response body is unreadable", async () => {
  const token = "admin-secret-token";
  const originalFetch = globalThis.fetch;
  try {
    for (const status of [401, 403]) {
      globalThis.fetch = async () => ({
        ok: false,
        status,
        statusText: "Rejected",
        text: async () => {
          throw new TypeError(`unreadable body for ${token}`);
        },
      }) as unknown as Response;

      await assert.rejects(fetchAdminOpsStatus(token), (error: unknown) => {
        assert.ok(error instanceof AdminRequestError);
        assert.equal(error.status, status);
        assert.equal(isAdminAuthFailure(error), true);
        assert.doesNotMatch(error.message, new RegExp(token));
        return true;
      });
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("admin auth failures logout instead of fabricating observer health", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  const appSource = readFileSync(new URL("../App.tsx", import.meta.url), "utf8");

  assert.match(source, /onAuthenticationFailure/);
  assert.match(source, /handleAuthenticationFailure\(nextError\)/);
  assert.match(source, /handleAdminAuthenticationFailure\(error, onAuthenticationFailure\)/);
  assert.match(appSource, /onAuthenticationFailure=\{handleLogout\}/);
  assert.match(appSource, /clearSavedSessionMeta\(\)/);
  assert.equal(isAdminAuthFailure(new AdminRequestError(401, "expired")), true);
  assert.equal(isAdminAuthFailure(new AdminRequestError(403, "forbidden")), true);
  assert.equal(isAdminAuthFailure(new AdminRequestError(500, "server error")), false);
});

test("admin authentication routing invokes only the correct failure path", () => {
  for (const status of [401, 403]) {
    let authenticationFailures = 0;
    assert.equal(
      handleAdminAuthenticationFailure(
        new AdminRequestError(status, "expired"),
        () => { authenticationFailures += 1; },
      ),
      true,
    );
    assert.equal(authenticationFailures, 1);
  }

  let authenticationFailures = 0;
  assert.equal(
    handleAdminAuthenticationFailure(
      new AdminRequestError(500, "server error"),
      () => { authenticationFailures += 1; },
    ),
    false,
  );
  assert.equal(authenticationFailures, 0);
});

test("token CRUD and feature mutations route auth failures through logout", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  for (const call of [
    "listAdminTokens(adminToken)",
    "saveRuntimeFeatureConfig(adminToken, next)",
    "createAdminToken(adminToken,",
    "disableAdminToken(adminToken, id)",
    "deleteAdminToken(adminToken, id)",
  ]) {
    const callIndex = source.indexOf(call);
    assert.ok(callIndex >= 0, `${call} must remain wired to the durable backend`);
    const catchIndex = source.indexOf("catch (nextError)", callIndex);
    const nextFunctionIndex = source.indexOf("\n  async function ", callIndex + call.length);
    const authFailureIndex = source.indexOf(
      "if (handleAuthenticationFailure(nextError)) return;",
      catchIndex,
    );
    assert.ok(catchIndex > callIndex, `${call} must handle request failures`);
    assert.ok(
      authFailureIndex > catchIndex &&
        (nextFunctionIndex < 0 || authFailureIndex < nextFunctionIndex),
      `${call} must clear the session on 401/403`,
    );
  }
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
    authorization = new Headers(init?.headers).get("Authorization") ?? "";
    return new Response(JSON.stringify({
      observed_at: "2026-07-18T00:00:00Z",
      readonly: true,
      running_parent_batches: 0,
      running_child_batches: 0,
      running_tasks: 0,
      running_batch_ids: ["9007199254740993", "42"],
      batch: null,
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  };
  try {
    const result = await fetchModuleCExecution("admin-token", "9007199254740993");
    assert.equal(result.readonly, true);
    assert.deepEqual(result.running_batch_ids, ["9007199254740993", "42"]);
    assert.match(
      requestedUrl,
      /\/api\/v1\/admin\/ops\/module-c\/execution\?batch_id=9007199254740993$/,
    );
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
    "audit_gate_pass",
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
  assert.match(source, /aria-label="Module C batch view"/);
  assert.match(source, /moduleCRequestEpoch\.current/);
  assert.match(source, /fetchModuleCExecution\(adminToken, batchId\)/);
  assert.match(source, /running_batch_ids/);
});

test("Module C batch selector preserves bigint IDs and a completed selected batch", () => {
  const html = renderToStaticMarkup(
    <ModuleCBatchSelector
      runningBatchIds={["9007199254740993", "42"]}
      selectedBatchId="9007199254740995"
      onSelect={() => undefined}
    />,
  );

  assert.match(html, /Automatic \(newest running or latest\)/);
  assert.match(html, /Selected terminal #9007199254740995/);
  assert.match(html, /Running #9007199254740993/);
  assert.match(html, /value="9007199254740993"/);
  assert.doesNotMatch(html, /9007199254740992/);
});

test("canary selection card renders a passing v2 contract and exact quotas read-only", () => {
  const html = renderToStaticMarkup(
    <ModuleCSelectionEvidenceCard selection={PASS_SELECTION} />,
  );

  assert.match(html, /Canary selection v2/);
  assert.match(html, /role="status"/);
  assert.match(html, /selection gate: pass/);
  assert.match(html, /module-c-canary-selection-v2/);
  assert.match(html, new RegExp(`title="${"a".repeat(64)}"`));
  assert.match(html, /11111111-1111-1111-1111-111111111111/);
  assert.match(html, /main board: 5 \/ 5/);
  assert.match(html, /ChiNext: 5 \/ 5/);
  assert.match(html, /STAR: 5 \/ 5/);
  assert.match(html, /Beijing: 5 \/ 5/);
  assert.match(html, /lower 2 \/ 2, middle 1 \/ 1, upper 2 \/ 2/);
  assert.match(html, /contract matches: yes/);
  assert.match(html, /hash matches: yes/);
  assert.match(html, /source matches: yes/);
  assert.match(html, /quotas match: yes/);
  assert.match(html, /active universe matches: yes/);
  assert.doesNotMatch(html, /<button|<form|<input/);
});

test("canary selection card makes failed and unavailable evidence explicit", () => {
  const failed = renderToStaticMarkup(
    <ModuleCSelectionEvidenceCard
      selection={{
        ...PASS_SELECTION,
        status: "failed",
        quotas_match: false,
        drift_reasons: ["selection_board_quota_drift"],
      }}
    />,
  );
  const unavailable = renderToStaticMarkup(
    <ModuleCSelectionEvidenceCard
      selection={{
        ...PASS_SELECTION,
        status: "unavailable",
        contract_version: null,
        manifest_sha256: null,
        source_build_id: null,
        activity_basis: null,
        board_counts: {},
        boundary_counts: {},
        contract_matches: null,
        hash_matches: null,
        source_matches: null,
        quotas_match: null,
        active_universe_matches: null,
        drift_reasons: ["canary_selection_unavailable"],
      }}
    />,
  );

  assert.match(failed, /role="alert"/);
  assert.match(failed, /selection gate: failed/);
  assert.match(failed, /quotas match: no/);
  assert.match(failed, /selection_board_quota_drift/);
  assert.match(unavailable, /role="alert"/);
  assert.match(unavailable, /selection gate: unavailable/);
  assert.match(unavailable, /canary_selection_unavailable/);
  assert.doesNotMatch(unavailable, /selection gate: pass/);
});

test("Module C request failure preserves the last snapshot and auth failure returns first", () => {
  const source = readFileSync(new URL("./AdminConsole.tsx", import.meta.url), "utf8");
  assert.match(source, /if \(handleAuthenticationFailure\(nextError\)\) return;/);
  assert.match(source, /setModuleCExecutionState\(\(current\) => \(\{[\s\S]*\.\.\.current,[\s\S]*stale: true,[\s\S]*reason: "request_failed"/);
  assert.match(source, /showing the last successful snapshot/);
});
