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
