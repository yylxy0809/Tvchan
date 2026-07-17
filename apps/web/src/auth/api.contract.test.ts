import assert from "node:assert/strict";
import test from "node:test";

import {
  createAdminToken,
  deleteAdminToken,
  disableAdminToken,
  listAdminTokens,
} from "./api";
import { buildAdminHeaders } from "../api/adminRequest";

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
