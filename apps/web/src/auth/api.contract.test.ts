import assert from "node:assert/strict";
import test from "node:test";

import {
  AuthenticationError,
  createAdminToken,
  deleteAdminToken,
  disableAdminToken,
  isCredentialRejection,
  listAdminTokens,
  loginWithToken,
} from "./api";
import { buildAdminHeaders } from "../api/adminRequest";
import { LoginAttemptFence } from "./loginAttemptFence";

type FetchCall = {
  input: string;
  init?: RequestInit;
};

const originalFetch = globalThis.fetch;
const originalWindow = globalThis.window;

function installWindow(localStorage: Storage): void {
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { localStorage },
  });
}

function restoreGlobals(): void {
  globalThis.fetch = originalFetch;
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: originalWindow,
  });
}

function responseJson(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

test("admin token CRUD sends the explicit admin token to the durable backend", async (t) => {
  t.after(restoreGlobals);
  const storageReads: string[] = [];
  installWindow({
    getItem(key: string) {
      storageReads.push(key);
      return key === "tv-a-share-user-role" ? "admin" : null;
    },
    setItem() {
      throw new Error("durable CRUD must not write localStorage");
    },
    removeItem() {
      throw new Error("durable CRUD must not remove localStorage entries");
    },
    clear() {},
    key() { return null; },
    length: 0,
  });

  const calls: FetchCall[] = [];
  globalThis.fetch = async (input, init) => {
    calls.push({ input: String(input), init });
    const method = init?.method ?? "GET";
    if (method === "DELETE") {
      return new Response(null, { status: 204 });
    }
    if (method === "POST" && String(input).endsWith("/disable")) {
      return responseJson({ id: 7, label: "reader", role: "user", is_active: false });
    }
    if (method === "POST") {
      return responseJson({
        id: 7,
        label: "reader",
        display_name: "Reader",
        role: "user",
        is_active: true,
        token: "issued-once",
      }, 201);
    }
    return responseJson({
      items: [{ id: 7, label: "reader", role: "user", is_active: true }],
    });
  };

  const adminToken = " durable-admin-token ";
  assert.equal((await listAdminTokens(adminToken))[0]?.id, 7);
  assert.equal((await createAdminToken(adminToken, {
    label: "reader",
    display_name: "Reader",
  })).token, "issued-once");
  assert.equal((await disableAdminToken(adminToken, 7)).is_active, false);
  await deleteAdminToken(adminToken, 7);

  assert.deepEqual(calls.map((call) => call.init?.method ?? "GET"), [
    "GET", "POST", "POST", "DELETE",
  ]);
  assert.deepEqual(
    calls.map((call) => new Headers(call.init?.headers).get("Authorization")),
    Array(4).fill("Bearer durable-admin-token"),
  );
  assert.deepEqual(
    calls.map((call) => new URL(call.input).pathname),
    [
      "/api/v1/admin/tokens",
      "/api/v1/admin/tokens",
      "/api/v1/admin/tokens/7/disable",
      "/api/v1/admin/tokens/7",
    ],
  );
  assert.deepEqual(storageReads, []);
});

test("admin token CRUD does not fall back to localStorage on network or 404 failures", async (t) => {
  t.after(restoreGlobals);
  let localStorageAccesses = 0;
  installWindow({
    getItem() {
      localStorageAccesses += 1;
      return JSON.stringify([
        { id: 99, label: "fake", role: "user", is_active: true, token: "fake" },
      ]);
    },
    setItem() { localStorageAccesses += 1; },
    removeItem() { localStorageAccesses += 1; },
    clear() {},
    key() { return null; },
    length: 0,
  });

  globalThis.fetch = async () => {
    throw new TypeError("network unavailable");
  };
  await assert.rejects(listAdminTokens("admin"), /network unavailable/);

  globalThis.fetch = async () => responseJson({ detail: "Not Found" }, 404);
  await assert.rejects(
    createAdminToken("admin", { label: "must-not-be-local" }),
    /Not Found/,
  );
  await assert.rejects(disableAdminToken("admin", 99), /Not Found/);
  await assert.rejects(deleteAdminToken("admin", 99), /Not Found/);
  assert.equal(localStorageAccesses, 0);
});

test("admin request headers preserve standard HeadersInit forms without overriding auth", () => {
  const record = buildAdminHeaders("canonical-admin", {
    "X-Record": "kept",
    Authorization: "Bearer attacker",
  });
  const headers = buildAdminHeaders(
    "canonical-admin",
    new Headers({ "X-Headers": "kept", "Content-Type": "text/plain" }),
  );
  const tuples = buildAdminHeaders("canonical-admin", [["X-Tuple", "kept"]]);

  assert.equal(record.get("x-record"), "kept");
  assert.equal(record.get("authorization"), "Bearer canonical-admin");
  assert.equal(headers.get("x-headers"), "kept");
  assert.equal(headers.get("content-type"), "text/plain");
  assert.equal(tuples.get("x-tuple"), "kept");
  assert.equal(tuples.get("content-type"), "application/json");
});

test("login always uses the durable backend even when localStorage contains a forged token", async (t) => {
  t.after(restoreGlobals);
  installWindow({
    getItem(key: string) {
      if (key === "tv-a-share-local-issued-tokens") {
        return JSON.stringify([
          { id: 1, label: "forged", role: "user", is_active: true, token: "forged-token" },
        ]);
      }
      return null;
    },
    setItem() {},
    removeItem() {},
    clear() {},
    key() { return null; },
    length: 0,
  });

  const calls: FetchCall[] = [];
  globalThis.fetch = async (input, init) => {
    calls.push({ input: String(input), init });
    return responseJson({
      valid: true,
      role: "admin",
      display_name: "Server Admin",
      label: "durable",
    });
  };

  const session = await loginWithToken(" forged-token ");

  assert.equal(session.role, "admin");
  assert.equal(session.displayName, "Server Admin");
  assert.equal(calls.length, 1);
  assert.match(calls[0]!.input, /\/api\/v1\/auth\/login$/);
  assert.equal(new Headers(calls[0]!.init?.headers).get("Authorization"), "Bearer forged-token");
  assert.deepEqual(JSON.parse(String(calls[0]!.init?.body)), { token: "forged-token" });
});

test("login failures never probe public health or fabricate a session", async (t) => {
  t.after(restoreGlobals);
  installWindow({
    getItem() { return null; },
    setItem() {},
    removeItem() {},
    clear() {},
    key() { return null; },
    length: 0,
  });

  for (const status of [401, 403, 404, 405, 500, 503]) {
    const calls: string[] = [];
    globalThis.fetch = async (input) => {
      calls.push(String(input));
      return responseJson({ detail: `login failed for super-secret-${status}` }, status);
    };
    await assert.rejects(
      loginWithToken(`super-secret-${status}`),
      (error: unknown) => {
        assert.ok(error instanceof AuthenticationError);
        assert.equal(error.status, status);
        assert.equal(isCredentialRejection(error), status === 401 || status === 403);
        assert.doesNotMatch(error.message, new RegExp(`super-secret-${status}`));
        return true;
      },
    );
    assert.equal(calls.length, 1);
    assert.match(calls[0]!, /\/api\/v1\/auth\/login$/);
  }

  const calls: string[] = [];
  globalThis.fetch = async (input) => {
    calls.push(String(input));
    throw new TypeError("network unavailable");
  };
  await assert.rejects(loginWithToken("network-token"), /Authentication service unavailable/);
  assert.equal(calls.length, 1);
  assert.match(calls[0]!, /\/api\/v1\/auth\/login$/);
});

test("credential rejection keeps its status when the response body is unreadable", async (t) => {
  t.after(restoreGlobals);
  installWindow({
    getItem() { return null; },
    setItem() {},
    removeItem() {},
    clear() {},
    key() { return null; },
    length: 0,
  });

  for (const status of [401, 403]) {
    globalThis.fetch = async () => ({
      ok: false,
      status,
      statusText: "Rejected",
      text: async () => {
        throw new TypeError("body stream unavailable for body-secret");
      },
    }) as unknown as Response;

    await assert.rejects(
      loginWithToken("credential-secret"),
      (error: unknown) => {
        assert.ok(error instanceof AuthenticationError);
        assert.equal(error.status, status);
        assert.doesNotMatch(error.message, /credential-secret|body-secret/);
        return true;
      },
    );
  }
});

test("login accepts only authoritative user or admin roles", async (t) => {
  t.after(restoreGlobals);
  installWindow({
    getItem() { return null; },
    setItem() {},
    removeItem() {},
    clear() {},
    key() { return null; },
    length: 0,
  });

  for (const valid of [false, undefined, "false"]) {
    globalThis.fetch = async () => responseJson({ valid, role: "user" });
    await assert.rejects(
      loginWithToken("invalid-token"),
      (error: unknown) => error instanceof AuthenticationError && error.status === 403,
    );
  }

  for (const role of [undefined, null, "owner", "ADMINISTRATOR"]) {
    globalThis.fetch = async () => responseJson({ valid: true, role });
    await assert.rejects(loginWithToken("contract-drift"), /invalid role/i);
  }
});

test("login attempt fence makes the latest attempt win and invalidates unmounted work", () => {
  const fence = new LoginAttemptFence();
  const first = fence.begin();
  const second = fence.begin();

  assert.equal(fence.isCurrent(first), false);
  assert.equal(fence.isCurrent(second), true);

  fence.dispose();
  assert.equal(fence.isCurrent(second), false);
});

test("login aborts a stalled request after one second", async (t) => {
  t.after(restoreGlobals);
  installWindow({
    getItem() { return null; }, setItem() {}, removeItem() {}, clear() {}, key() { return null; }, length: 0,
  });
  globalThis.fetch = async (_input, init) => {
    const signal = init?.signal;
    assert.ok(signal);
    return new Promise<Response>((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
  };

  const startedAt = Date.now();
  await assert.rejects(loginWithToken("deadline-token"), /Authentication service unavailable/);
  assert.ok(Date.now() - startedAt >= 900);
  assert.ok(Date.now() - startedAt < 1_300);
});
