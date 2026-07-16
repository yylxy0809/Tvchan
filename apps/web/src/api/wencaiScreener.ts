import { apiUrl, getApiToken } from "../config";
import type { WatchlistItem } from "./watchlistStore";

export type WencaiScreenerRow = WatchlistItem & {
  code: string;
  price: number | null;
  changePercent: number | null;
  buySignal: string;
  technicalShape: string;
  reason: string;
  highBreakReason: string;
  raw?: Record<string, unknown>;
};

export type WencaiScreenerResponse = {
  query: string;
  total: number;
  page: number;
  pageSize: number;
  source: string;
  fetchedAt: string;
  conditions: string[];
  suggestions: string[];
  rows: WencaiScreenerRow[];
};

type ApiWencaiItem = {
  symbol?: unknown;
  code?: unknown;
  exchange?: unknown;
  name?: unknown;
  price?: unknown;
  change_percent?: unknown;
  buy_signal?: unknown;
  technical_shape?: unknown;
  reason?: unknown;
  high_break_reason?: unknown;
  raw?: unknown;
};

type ApiWencaiResponse = {
  query?: unknown;
  total?: unknown;
  page?: unknown;
  page_size?: unknown;
  source?: unknown;
  fetched_at?: unknown;
  conditions?: unknown;
  suggestions?: unknown;
  items?: unknown;
};

export async function queryWencaiScreener(
  query: string,
  page = 1,
  pageSize = 50,
): Promise<WencaiScreenerResponse> {
  const params = new URLSearchParams({
    q: query.trim(),
    page: String(page),
    page_size: String(pageSize),
  });
  const response = await fetch(apiUrl(`/api/v1/screener/wencai?${params.toString()}`), {
    headers: {
      Authorization: `Bearer ${getApiToken()}`,
    },
  });
  if (!response.ok) {
    const detail = await readResponseError(response);
    throw new Error(detail);
  }
  return normalizeWencaiResponse(await response.json());
}

function normalizeWencaiResponse(payload: unknown): WencaiScreenerResponse {
  const root = asRecord(payload) as ApiWencaiResponse | null;
  const items = Array.isArray(root?.items) ? root.items : [];
  return {
    query: readString(root?.query),
    total: readNumber(root?.total) ?? items.length,
    page: readNumber(root?.page) ?? 1,
    pageSize: readNumber(root?.page_size) ?? 50,
    source: readString(root?.source) || "wencai",
    fetchedAt: readString(root?.fetched_at),
    conditions: readStringArray(root?.conditions),
    suggestions: readStringArray(root?.suggestions),
    rows: items.map(normalizeWencaiRow).filter((row): row is WencaiScreenerRow => row !== null),
  };
}

function normalizeWencaiRow(value: unknown): WencaiScreenerRow | null {
  const row = asRecord(value) as ApiWencaiItem | null;
  if (!row) {
    return null;
  }
  const symbol = readString(row.symbol);
  const code = readString(row.code);
  const exchange = readString(row.exchange) || exchangeFromCode(code);
  if (!symbol && !code) {
    return null;
  }
  return {
    symbol: symbol || `${code}.${exchange}`,
    code,
    name: readString(row.name) || code,
    exchange,
    price: readNumber(row.price),
    changePercent: readNumber(row.change_percent),
    buySignal: readString(row.buy_signal),
    technicalShape: readString(row.technical_shape),
    reason: readString(row.reason),
    highBreakReason: readString(row.high_break_reason),
    raw: asRecord(row.raw) ?? undefined,
  };
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

function readString(value: unknown): string {
  return typeof value === "string" ? value.trim() : value == null ? "" : String(value);
}

function readStringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.map(readString).filter(Boolean)
    : [];
}

function readNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value !== "string") {
    return null;
  }
  const parsed = Number(value.replace(/[% ,]/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function exchangeFromCode(code: string): string {
  if (/^(4|8|920)/.test(code)) {
    return "BJ";
  }
  return /^(6|9)/.test(code) ? "SH" : "SZ";
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}
