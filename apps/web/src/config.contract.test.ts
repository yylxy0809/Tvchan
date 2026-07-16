import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

test("authenticated API clients prefer the persisted login token", () => {
  const source = readFileSync(new URL("./config.ts", import.meta.url), "utf8");

  const loginTokenRead = source.indexOf(
    "window.localStorage.getItem(LOGIN_TOKEN_STORAGE_KEY)",
  );
  const legacyTokenRead = source.indexOf(
    "window.localStorage.getItem(API_TOKEN_STORAGE_KEY)",
  );

  assert.ok(loginTokenRead >= 0, "getApiToken must read the authenticated session token");
  assert.ok(
    legacyTokenRead > loginTokenRead,
    "the authenticated session token must take precedence over the legacy API token",
  );
});
