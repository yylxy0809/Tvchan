import type { AuthSession } from "../auth/api";
import {
  API_TOKEN_STORAGE_KEY,
  isFrontendAdminToken,
  isFrontendLoginToken,
  LOGIN_TOKEN_STORAGE_KEY,
} from "../config";

const ROLE_STORAGE_KEY = "tv-a-share-user-role";
const DISPLAY_NAME_STORAGE_KEY = "tv-a-share-display-name";
const LABEL_STORAGE_KEY = "tv-a-share-token-label";

export function loadSavedSession(): AuthSession | null {
  const token = loadSavedToken();
  if (!token) {
    return null;
  }
  return {
    token,
    role: loadSavedRole(token),
    displayName: readStorage(DISPLAY_NAME_STORAGE_KEY),
    label: readStorage(LABEL_STORAGE_KEY),
  };
}

export function persistSession(session: AuthSession): void {
  writeStorage(LOGIN_TOKEN_STORAGE_KEY, session.token);
  clearLegacyLoginToken();
  writeStorage(ROLE_STORAGE_KEY, session.role);
  writeStorage(DISPLAY_NAME_STORAGE_KEY, session.displayName ?? "");
  writeStorage(LABEL_STORAGE_KEY, session.label ?? "");
}

export function loadSavedToken(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    const loginToken = window.localStorage.getItem(LOGIN_TOKEN_STORAGE_KEY)?.trim();
    if (loginToken) {
      return loginToken;
    }
    const legacyToken = window.localStorage.getItem(API_TOKEN_STORAGE_KEY)?.trim() ?? "";
    if (isFrontendLoginToken(legacyToken)) {
      window.localStorage.setItem(LOGIN_TOKEN_STORAGE_KEY, legacyToken);
      window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
      return legacyToken;
    }
    return "";
  } catch {
    return "";
  }
}

export function clearSavedSessionMeta(): void {
  removeStorage(LOGIN_TOKEN_STORAGE_KEY);
  removeStorage(ROLE_STORAGE_KEY);
  removeStorage(DISPLAY_NAME_STORAGE_KEY);
  removeStorage(LABEL_STORAGE_KEY);
}

export function readStorage(key: string): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return window.localStorage.getItem(key) ?? "";
  } catch {
    return "";
  }
}

export function writeStorage(key: string, value: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    if (value) {
      window.localStorage.setItem(key, value);
    } else {
      window.localStorage.removeItem(key);
    }
  } catch {
    // Storage failures should not block login or chart usage.
  }
}

function loadSavedRole(token: string): AuthSession["role"] {
  const stored = readStorage(ROLE_STORAGE_KEY);
  if (stored === "admin") {
    return "admin";
  }
  if (stored === "user") {
    return "user";
  }
  return isFrontendAdminToken(token) ? "admin" : "user";
}

function removeStorage(key: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(key);
  } catch {
    // Ignore storage failures.
  }
}

function clearLegacyLoginToken(): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    const legacyToken = window.localStorage.getItem(API_TOKEN_STORAGE_KEY)?.trim() ?? "";
    if (isFrontendLoginToken(legacyToken)) {
      window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
    }
  } catch {
    // Storage cleanup is best effort only.
  }
}
