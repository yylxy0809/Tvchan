import { getApiToken, webSocketUrl } from "../config";

export type RealtimeStatus = "idle" | "connecting" | "open" | "closed" | "error";

export type RealtimeMessage = {
  type: string;
  seq?: number;
  symbol?: string;
  timeframe?: string;
  snapshot_version?: string;
  bar?: unknown;
};

export function createChartSocket(): WebSocket {
  return createSocket("/ws/v2/chart");
}

function createSocket(path: string): WebSocket {
  const url = new URL(webSocketUrl(path));
  url.searchParams.set("token", getApiToken());
  return new WebSocket(url.toString());
}
