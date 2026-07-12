import type { ChartTheme } from "../tradingview/widget";
import { INTERVAL_BY_TIMEFRAME } from "../tradingview/time";
import { readStorage, writeStorage } from "./sessionPersistence";

const CHART_THEME_STORAGE_KEY = "tv-a-share-chart-theme";

export function loadSavedChartTheme(): ChartTheme {
  const value = readStorage(CHART_THEME_STORAGE_KEY);
  return value === "light" ? "light" : "dark";
}

export function saveChartTheme(theme: ChartTheme): void {
  writeStorage(CHART_THEME_STORAGE_KEY, theme);
}

export function readRemoteChartTheme(value: unknown): ChartTheme | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const theme = (value as { theme?: unknown }).theme;
  return theme === "light" || theme === "dark" ? theme : null;
}

export function readRemoteChartLayout(
  value: unknown,
): { symbol?: string; timeframe?: string } | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as { symbol?: unknown; timeframe?: unknown };
  const symbol = normalizeChartSymbol(record.symbol, "");
  const timeframe =
    typeof record.timeframe === "string" && record.timeframe in INTERVAL_BY_TIMEFRAME
      ? record.timeframe
      : "";
  if (!symbol && !timeframe) {
    return null;
  }
  return {
    ...(symbol ? { symbol } : {}),
    ...(timeframe ? { timeframe } : {}),
  };
}

export function hasInitialChartSymbolQuery(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  return Boolean(new URLSearchParams(window.location.search).get("symbol")?.trim());
}

export function normalizeChartSymbol(value: unknown, fallback: string): string {
  if (typeof value !== "string" || !value.trim()) {
    return fallback;
  }
  const normalized = value.trim().toUpperCase();
  const match = normalized.match(/^(\d{6})(?:\.(SH|SZ|BJ))?$/);
  if (match) {
    const code = match[1];
    if (/^(4|8|920)/.test(code)) {
      return `${code}.BJ`;
    }
    if (/^6\d{5}$/.test(code)) {
      return `${code}.SH`;
    }
    if (/^[03]\d{5}$/.test(code)) {
      return `${code}.SZ`;
    }
  }
  if (/^(4|8|920)/.test(normalized)) {
    return `${normalized}.BJ`;
  }
  if (/^6\d{5}$/.test(normalized)) {
    return `${normalized}.SH`;
  }
  if (/^[03]\d{5}$/.test(normalized)) {
    return `${normalized}.SZ`;
  }
  return normalized;
}
