import { apiUrl, API_TOKEN_STORAGE_KEY, isFrontendAdminToken } from "../config";

export type UserRole = "user" | "admin";

export type AuthSession = {
  token: string;
  role: UserRole;
  displayName?: string | null;
  label?: string | null;
};

export type AdminToken = {
  id: number;
  label: string;
  display_name?: string | null;
  role: UserRole | string;
  is_active: boolean;
  created_at?: string;
  updated_at?: string;
  disabled_at?: string | null;
  last_used_at?: string | null;
  token?: string;
};

type LoginResponse = {
  valid: boolean;
  role?: string | null;
  display_name?: string | null;
  label?: string | null;
};

type AdminTokenListResponse = {
  items?: AdminToken[];
};

const LOCAL_TOKEN_STORAGE_KEY = "tv-a-share-local-issued-tokens";
const ROLE_STORAGE_KEY = "tv-a-share-user-role";

export async function loginWithToken(token: string): Promise<AuthSession> {
  const normalized = token.trim();
  if (!normalized) {
    throw new Error("Please enter an access token.");
  }

  if (isFrontendAdminToken(normalized)) {
    return {
      token: normalized,
      role: "admin",
      displayName: "Administrator",
      label: "frontend-admin",
    };
  }

  const localToken = readLocalTokens().find(
    (item) => item.is_active && item.token === normalized,
  );
  if (localToken) {
    return {
      token: normalized,
      role: "user",
      displayName: localToken.display_name,
      label: localToken.label,
    };
  }

  try {
    const response = await fetch(apiUrl("/api/v1/auth/login"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${normalized}`,
      },
      body: JSON.stringify({ token: normalized }),
    });
    if (response.status === 404 || response.status === 405) {
      return loginViaHealthFallback(normalized);
    }
    if (!response.ok) {
      throw new Error(await readResponseError(response));
    }
    const data = (await response.json()) as LoginResponse;
    if (!data.valid) {
      throw new Error("Invalid access token.");
    }
    return {
      token: normalized,
      role: normalizeRole(data.role),
      displayName: data.display_name,
      label: data.label,
    };
  } catch (error) {
    if (isNetworkFailure(error)) {
      return loginViaHealthFallback(normalized);
    }
    throw error;
  }
}

export async function listAdminTokens(): Promise<AdminToken[]> {
  if (useFrontendTokenStore()) {
    return readLocalTokens().map(stripPlainToken);
  }
  try {
    const data = await requestAdmin<AdminTokenListResponse | AdminToken[]>(
      "/api/v1/admin/tokens",
    );
    return Array.isArray(data) ? data : data.items ?? [];
  } catch (error) {
    if (!shouldUseLocalStoreFallback(error)) {
      throw error;
    }
    return readLocalTokens().map(stripPlainToken);
  }
}

export async function createAdminToken(input: {
  label: string;
  display_name?: string | null;
}): Promise<AdminToken> {
  if (!useFrontendTokenStore()) {
    try {
      return await requestAdmin<AdminToken>("/api/v1/admin/tokens", {
        method: "POST",
        body: JSON.stringify(input),
      });
    } catch (error) {
      if (!shouldUseLocalStoreFallback(error)) {
        throw error;
      }
    }
  }

  const now = new Date().toISOString();
  const created: AdminToken = {
    id: Date.now(),
    label: input.label,
    display_name: input.display_name,
    role: "user",
    is_active: true,
    created_at: now,
    updated_at: now,
    token: createLocalToken(),
  };
  writeLocalTokens([created, ...readLocalTokens()]);
  return created;
}

export async function disableAdminToken(id: number): Promise<AdminToken> {
  if (!useFrontendTokenStore()) {
    try {
      return await requestAdmin<AdminToken>(
        `/api/v1/admin/tokens/${encodeURIComponent(String(id))}/disable`,
        {
          method: "POST",
        },
      );
    } catch (error) {
      if (!shouldUseLocalStoreFallback(error)) {
        throw error;
      }
    }
  }

  const tokens = readLocalTokens();
  const now = new Date().toISOString();
  const next = tokens.map((item) =>
    item.id === id
      ? { ...item, is_active: false, disabled_at: now, updated_at: now }
      : item,
  );
  writeLocalTokens(next);
  const updated = next.find((item) => item.id === id);
  if (!updated) {
    throw new Error("Token not found.");
  }
  return stripPlainToken(updated);
}

export async function deleteAdminToken(id: number): Promise<void> {
  if (!useFrontendTokenStore()) {
    try {
      await requestAdmin(`/api/v1/admin/tokens/${encodeURIComponent(String(id))}`, {
        method: "DELETE",
      });
      return;
    } catch (error) {
      if (!shouldUseLocalStoreFallback(error)) {
        throw error;
      }
    }
  }
  writeLocalTokens(readLocalTokens().filter((item) => item.id !== id));
}

async function loginViaHealthFallback(token: string): Promise<AuthSession> {
  const response = await fetch(apiUrl("/api/v1/health"), {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
  if (!response.ok) {
    throw new Error(await readResponseError(response));
  }
  return {
    token,
    role: isFrontendAdminToken(token) ? "admin" : "user",
    displayName: "Legacy API token",
    label: "legacy",
  };
}

async function requestAdmin<T = unknown>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${getStoredToken()}`,
      ...init.headers,
    },
  });
  if (!response.ok) {
    throw new Error(await readResponseError(response));
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json() as Promise<T>;
}

function useFrontendTokenStore(): boolean {
  try {
    return window.localStorage.getItem(ROLE_STORAGE_KEY) === "admin";
  } catch {
    return false;
  }
}

function readLocalTokens(): AdminToken[] {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const raw = window.localStorage.getItem(LOCAL_TOKEN_STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.filter(isAdminToken);
  } catch {
    return [];
  }
}

function writeLocalTokens(tokens: AdminToken[]) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(LOCAL_TOKEN_STORAGE_KEY, JSON.stringify(tokens));
  } catch {
    // Local token management is a temporary frontend-only store.
  }
}

function stripPlainToken(token: AdminToken): AdminToken {
  const { token: _token, ...rest } = token;
  return rest;
}

function isAdminToken(value: unknown): value is AdminToken {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Partial<AdminToken>;
  return (
    typeof record.id === "number" &&
    typeof record.label === "string" &&
    typeof record.is_active === "boolean"
  );
}

function createLocalToken(): string {
  const bytes = new Uint8Array(18);
  crypto.getRandomValues(bytes);
  return `tv_${Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

function getStoredToken(): string {
  try {
    return window.localStorage.getItem(API_TOKEN_STORAGE_KEY) ?? "";
  } catch {
    return "";
  }
}

function normalizeRole(value: unknown): UserRole {
  return String(value).toLowerCase() === "admin" ? "admin" : "user";
}

async function readResponseError(response: Response): Promise<string> {
  const text = await response.text();
  if (!text) {
    return `${response.status} ${response.statusText}`;
  }
  try {
    const data = JSON.parse(text) as { detail?: unknown; message?: unknown };
    return String(data.detail ?? data.message ?? text);
  } catch {
    return `${response.status} ${response.statusText}: ${text}`;
  }
}

function isNetworkFailure(error: unknown): boolean {
  return error instanceof TypeError;
}

function shouldUseLocalStoreFallback(error: unknown): boolean {
  if (error instanceof TypeError) {
    return true;
  }
  if (!(error instanceof Error)) {
    return false;
  }
  return /^404\b/.test(error.message) || /^405\b/.test(error.message);
}
