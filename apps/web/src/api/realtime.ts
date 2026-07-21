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

export function createRealtimeSocket(): WebSocket {
  return createSocket("/ws/v1/realtime");
}

function createSocket(path: string): WebSocket {
  const url = new URL(webSocketUrl(path));
  return new WebSocket(url.toString(), [encodeBearerProtocol(getApiToken())]);
}

function encodeBearerProtocol(token: string): string {
  const bytes = new TextEncoder().encode(token);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  const encoded = btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `tvchan.bearer.${encoded}`;
}
