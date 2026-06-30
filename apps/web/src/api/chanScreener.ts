import { apiUrl, getApiToken } from "../config";
import type { WatchlistItem } from "./watchlistStore";

export type ChanScreenerCondition = {
  level: string;
  kind: string;
  direction: "up" | "down" | null;
  value: string | null;
  raw: string;
};

export type ChanScreenerMarket = {
  price: number | null;
  change_percent: number | null;
  industry: string | null;
  fund_net_inflow: number | null;
  latest_bar_time: number | null;
};

export type ChanScreenerRow = WatchlistItem & {
  code: string;
  trend_status: Record<string, string | null>;
  stroke_states: Record<string, string | null>;
  segment_states: Record<string, string | null>;
  market: ChanScreenerMarket;
};

export type ChanScreenerResponse = {
  query: string;
  mode: string;
  parser: "llm" | "rules" | string;
  parser_error: string | null;
  conditions: ChanScreenerCondition[];
  unsupported: string[];
  items: ChanScreenerRow[];
};

export async function queryChanScreener(
  query: string,
  limit = 100,
  mode: "current" | "confirmed" | "predictive" = "current",
): Promise<ChanScreenerResponse> {
  const params = new URLSearchParams({
    q: query,
    mode,
    limit: String(limit),
  });
  const response = await fetch(apiUrl(`/api/v1/screener/chan?${params.toString()}`), {
    headers: {
      Authorization: `Bearer ${getApiToken()}`,
    },
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${detail}`);
  }
  return response.json() as Promise<ChanScreenerResponse>;
}
