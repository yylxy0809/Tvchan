import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  clearSavedSessionMeta,
  loadSavedToken,
  persistSession,
} from "./sessionPersistence";
import { SessionAuthorityFence } from "../auth/sessionAuthorityFence";

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

test("App resets chart transports at authentication session boundaries", () => {
  const appSource = readFileSync(new URL("../App.tsx", import.meta.url), "utf8");
  const restore = appSource.slice(
    appSource.indexOf("useEffect(() =>"),
    appSource.indexOf("function handleAuthenticated"),
  );
  const authenticated = appSource.slice(
    appSource.indexOf("function handleAuthenticated"),
    appSource.indexOf("function handleLogout"),
  );
  const logout = appSource.slice(
    appSource.indexOf("function handleLogout"),
    appSource.indexOf("if (restoringSession)"),
  );

  assert.match(appSource, /import \{ chartDataManager \} from "\.\/api\/chartDataManager"/);
  assert.match(
    restore,
    /chartDataManager\.resetSession\(\);[\s\S]*loginWithToken\(initialLoginToken\)/,
  );
  assert.match(authenticated, /chartDataManager\.resetSession\(\)/);
  assert.match(logout, /chartDataManager\.resetSession\(\)/);
});

test("a prior admin generation cannot clear a newly authenticated session", () => {
  const fence = new SessionAuthorityFence();
  let sessionToken: string | null = "admin-a";
  let persistedToken: string | null = "admin-a";
  let transportResets = 0;
  const generationA = fence.activate();
  const logout = () => {
    fence.invalidate();
    sessionToken = null;
    persistedToken = null;
    transportResets += 1;
  };

  logout();
  sessionToken = "admin-b";
  persistedToken = "admin-b";
  const generationB = fence.activate();
  const resetsBeforeLateFailure = transportResets;

  assert.equal(fence.runIfCurrent(generationA, logout), false);
  assert.equal(sessionToken, "admin-b");
  assert.equal(persistedToken, "admin-b");
  assert.equal(transportResets, resetsBeforeLateFailure);

  assert.equal(fence.runIfCurrent(generationB, logout), true);
  assert.equal(sessionToken, null);
  assert.equal(persistedToken, null);
  assert.equal(transportResets, resetsBeforeLateFailure + 1);
});

test("session generation, not token identity, fences a same-token relogin", () => {
  const fence = new SessionAuthorityFence();
  const firstGeneration = fence.activate();
  fence.invalidate();
  const secondGeneration = fence.activate();
  let activeToken: string | null = "same-admin-token";

  assert.notEqual(firstGeneration, secondGeneration);
  assert.equal(
    fence.runIfCurrent(firstGeneration, () => { activeToken = null; }),
    false,
  );
  assert.equal(activeToken, "same-admin-token");
  assert.equal(
    fence.runIfCurrent(secondGeneration, () => { activeToken = null; }),
    true,
  );
  assert.equal(activeToken, null);
});

test("App binds Admin auth failures to the current session authority generation", () => {
  const appSource = readFileSync(new URL("../App.tsx", import.meta.url), "utf8");
  const authenticated = appSource.slice(
    appSource.indexOf("function handleAuthenticated"),
    appSource.indexOf("function handleLogout"),
  );
  const logout = appSource.slice(
    appSource.indexOf("function handleLogout"),
    appSource.indexOf("if (restoringSession)"),
  );

  assert.match(appSource, /new SessionAuthorityFence\(\)/);
  assert.match(authenticated, /sessionAuthority\.activate\(\)/);
  assert.match(logout, /sessionAuthority\.invalidate\(\)/);
  assert.match(appSource, /sessionAuthority\.runIfCurrent\(generation, handleLogout\)/);
  assert.match(
    appSource,
    /onAuthenticationFailure=\{\(\) => handleSessionAuthenticationFailure\(sessionGeneration\)\}/,
  );
});
