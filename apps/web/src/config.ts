type RuntimeAppConfig = {
  apiBaseUrl?: string;
  apiToken?: string;
  chanStudy?: boolean | string;
  chartV2Fallback?: boolean | string;
  chartDataTransport?: string;
  frontendAdminToken?: string;
  tvDebug?: boolean | string;
};

declare global {
  interface Window {
    __TV_APP_CONFIG__?: RuntimeAppConfig;
  }
}

const runtimeConfig =
  typeof window !== "undefined" ? window.__TV_APP_CONFIG__ ?? {} : {};
const queryParams =
  typeof window !== "undefined" ? new URLSearchParams(window.location.search) : undefined;
const viteEnv = import.meta.env ?? {};

const configuredApiBaseUrl =
  queryParams?.get("apiBaseUrl") ??
  runtimeConfig.apiBaseUrl ??
  viteEnv.VITE_API_BASE_URL ??
  "http://127.0.0.1:8001";

export const API_BASE_URL =
  normalizeApiBaseUrl(configuredApiBaseUrl);

export const API_BASE_MODE = API_BASE_URL ? "absolute" : "same-origin";

export const API_TOKEN_STORAGE_KEY = "tv-a-share-api-token";
export const LOGIN_TOKEN_STORAGE_KEY = "tv-a-share-login-token";

export const FRONTEND_ADMIN_TOKEN =
  runtimeConfig.frontendAdminToken ??
  viteEnv.VITE_FRONTEND_ADMIN_TOKEN ??
  "";

export const DEFAULT_API_TOKEN =
  runtimeConfig.apiToken ??
  viteEnv.VITE_API_TOKEN ??
  (API_BASE_URL ? "dev-local-token" : "");

export const API_TOKEN = DEFAULT_API_TOKEN;

export const TRADINGVIEW_DEBUG =
  readBoolean(queryParams?.get("tvDebug")) ??
  readBoolean(viteEnv.VITE_TV_DEBUG) ??
  readBoolean(runtimeConfig.tvDebug) ??
  false;

export const CHAN_STUDY_ENABLED =
  readBoolean(queryParams?.get("chanStudy")) ??
  readBoolean(viteEnv.VITE_CHAN_STUDY_ENABLED) ??
  readBoolean(runtimeConfig.chanStudy) ??
  true;

export type ChartDataTransport = "http" | "websocket" | "auto";

export const CHART_DATA_TRANSPORT = readTransport(
  queryParams?.get("dataTransport") ??
    viteEnv.VITE_CHART_DATA_TRANSPORT ??
    runtimeConfig.chartDataTransport ??
    "http",
);

export const ALLOW_CHART_V2_FALLBACK =
  readBoolean(queryParams?.get("chartV2Fallback")) ??
  readBoolean(viteEnv.VITE_ALLOW_CHART_V2_FALLBACK) ??
  readBoolean(runtimeConfig.chartV2Fallback) ??
  false;

export function apiUrl(path: string): string {
  const normalizedPath = normalizePath(path);
  if (!API_BASE_URL) {
    return normalizedPath;
  }
  return new URL(normalizedPath, ensureTrailingSlash(API_BASE_URL)).toString();
}

export function webSocketUrl(path: string): string {
  const normalizedPath = normalizePath(path);
  const base =
    API_BASE_URL ||
    (typeof window !== "undefined" ? window.location.origin : "http://127.0.0.1:8001");
  const url = new URL(normalizedPath, ensureTrailingSlash(base));
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

export function getApiToken(): string {
  if (typeof window === "undefined") {
    return DEFAULT_API_TOKEN;
  }
  try {
    const saved = window.localStorage.getItem(API_TOKEN_STORAGE_KEY);
    const normalized = saved?.trim() ?? "";
    if (!normalized || isFrontendLoginToken(normalized)) {
      return DEFAULT_API_TOKEN;
    }
    return normalized;
  } catch {
    return DEFAULT_API_TOKEN;
  }
}

export function saveApiToken(token: string): string {
  const normalized = token.trim();
  if (typeof window !== "undefined") {
    try {
      if (normalized) {
        window.localStorage.setItem(API_TOKEN_STORAGE_KEY, normalized);
      } else {
        window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
      }
    } catch {
      // Storage failures should not prevent in-memory login state.
    }
  }
  return normalized;
}

export function clearApiToken(): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
  } catch {
    // Ignore storage failures.
  }
}

function readBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return undefined;
}

function readTransport(value: unknown): ChartDataTransport {
  if (typeof value !== "string") {
    return "http";
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "websocket" || normalized === "ws") {
    return "websocket";
  }
  if (normalized === "auto") {
    return "auto";
  }
  return "http";
}

export function isFrontendAdminToken(value: string): boolean {
  return Boolean(FRONTEND_ADMIN_TOKEN) && value === FRONTEND_ADMIN_TOKEN;
}

export function isFrontendLoginToken(value: string): boolean {
  return isFrontendAdminToken(value) || value.startsWith("tv_");
}

function normalizeApiBaseUrl(value: unknown): string {
  if (typeof value !== "string") {
    return "http://127.0.0.1:8001";
  }
  const trimmed = value.trim();
  const normalized = trimmed.toLowerCase();
  if (["", "/", "same", "same-origin", "origin"].includes(normalized)) {
    return "";
  }
  return trimmed.replace(/\/+$/, "");
}

function normalizePath(path: string): string {
  return path.startsWith("/") ? path : `/${path}`;
}

function ensureTrailingSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}
