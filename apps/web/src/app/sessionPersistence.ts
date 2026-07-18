import type { AuthSession } from "../auth/api";
import { LOGIN_TOKEN_STORAGE_KEY } from "../config";

const ROLE_STORAGE_KEY = "tv-a-share-user-role";
const DISPLAY_NAME_STORAGE_KEY = "tv-a-share-display-name";
const LABEL_STORAGE_KEY = "tv-a-share-token-label";

export function persistSession(session: AuthSession): void {
  writeStorage(LOGIN_TOKEN_STORAGE_KEY, session.token);
  removeStorage(ROLE_STORAGE_KEY);
  removeStorage(DISPLAY_NAME_STORAGE_KEY);
  removeStorage(LABEL_STORAGE_KEY);
}

export function loadSavedToken(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return window.localStorage.getItem(LOGIN_TOKEN_STORAGE_KEY)?.trim() ?? "";
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
