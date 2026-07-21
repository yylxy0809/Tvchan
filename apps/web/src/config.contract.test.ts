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

test("frontend configuration cannot mint authenticated sessions", () => {
  const source = readFileSync(new URL("./config.ts", import.meta.url), "utf8");

  assert.doesNotMatch(source, /frontendAdminToken/);
  assert.doesNotMatch(source, /VITE_FRONTEND_ADMIN_TOKEN/);
  assert.doesNotMatch(source, /isFrontendAdminToken/);
  assert.doesNotMatch(source, /isFrontendLoginToken/);
});

test("gateway keeps entry configuration uncached and versioned runtime assets immutable", () => {
  const source = readFileSync(new URL("../../../deploy/nginx.tv.conf", import.meta.url), "utf8");

  assert.match(source, /location = \/app-config\.js[\s\S]*no-store/);
  assert.match(source, /location = \/index\.html[\s\S]*no-store/);
  assert.match(source, /location \/assets\/[\s\S]*max-age=31536000, immutable/);
  assert.match(source, /location \/charting_library\/[\s\S]*max-age=31536000, immutable/);
});
