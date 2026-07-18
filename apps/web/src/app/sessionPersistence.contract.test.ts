import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  clearSavedSessionMeta,
  loadSavedToken,
  persistSession,
} from "./sessionPersistence";

const originalWindow = globalThis.window;

function installWindow(localStorage: Storage): void {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { localStorage },
  });
}

function restoreWindow(): void {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: originalWindow,
  });
}

test("legacy API tokens are only login input and are never promoted to saved sessions", (t) => {
  t.after(restoreWindow);
  const writes: string[] = [];
  const removals: string[] = [];
  installWindow({
    getItem(key: string) {
      return key === "tv-a-share-api-token" ? "tv_forged_legacy" : null;
    },
    setItem(key: string) { writes.push(key); },
    removeItem(key: string) { removals.push(key); },
    clear() {},
    key() { return null; },
    length: 0,
  });

  assert.equal(loadSavedToken(), "");
  assert.deepEqual(writes, []);
  assert.deepEqual(removals, []);
});

test("persisted sessions store only the backend-validated credential", (t) => {
  t.after(restoreWindow);
  const writes: Array<[string, string]> = [];
  const removals: string[] = [];
  installWindow({
    getItem() { return null; },
    setItem(key: string, value: string) { writes.push([key, value]); },
    removeItem(key: string) { removals.push(key); },
    clear() {},
    key() { return null; },
    length: 0,
  });

  persistSession({ token: "server-validated", role: "admin", displayName: "Admin" });
  assert.deepEqual(writes, [["tv-a-share-login-token", "server-validated"]]);
  assert.deepEqual(removals.sort(), [
    "tv-a-share-display-name",
    "tv-a-share-token-label",
    "tv-a-share-user-role",
  ]);

  clearSavedSessionMeta();
  assert.ok(removals.includes("tv-a-share-login-token"));
});

test("App restores a saved credential through the backend before creating a session", () => {
  const appSource = readFileSync(new URL("../App.tsx", import.meta.url), "utf8");
  const persistenceSource = readFileSync(
    new URL("./sessionPersistence.ts", import.meta.url),
    "utf8",
  );
  const loginPageSource = readFileSync(
    new URL("../components/LoginPage.tsx", import.meta.url),
    "utf8",
  );

  assert.doesNotMatch(appSource, /loadSavedSession/);
  assert.match(appSource, /loginWithToken\(initialLoginToken\)/);
  assert.match(appSource, /useState<AuthSession \| null>\(null\)/);
  assert.match(appSource, /restoringSession/);
  assert.match(appSource, /setLoginHint\(""\)/);
  assert.match(appSource, /initialToken=\{loginHint\}/);
  assert.doesNotMatch(persistenceSource, /loadSavedRole/);
  assert.doesNotMatch(persistenceSource, /isFrontendAdminToken/);
  assert.match(appSource, /onAuthenticationFailure=\{handleLogout\}/);
  assert.match(loginPageSource, /isCredentialRejection\(nextError\)/);
  assert.match(loginPageSource, /setToken\(""\)/);
  assert.match(loginPageSource, /onAuthenticationFailure\?\.\(\)/);
  assert.match(loginPageSource, /fence\.isCurrent\(attempt\)/);
  assert.match(loginPageSource, /attemptFence\.current = fence/);
  assert.match(loginPageSource, /return \(\) => fence\.dispose\(\)/);
});
