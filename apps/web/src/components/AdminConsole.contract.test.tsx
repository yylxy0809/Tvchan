import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

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
  assert.match(source, /expected_observer_name/);
  assert.match(source, /unavailable/);
  assert.match(source, /degraded/);
  assert.match(apiSource, /"unavailable" \| "degraded" \| "healthy"/);
  assert.match(apiSource, /expected_observer_name/);
});
