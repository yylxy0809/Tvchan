import { apiUrl, getApiToken } from "../config";

export interface HistoryBar {
  time?: number | string;
  timestamp?: number | string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
  [key: string]: unknown;
}

export interface HistoryExportRequest {
  bars: HistoryBar[];
  metadata?: Record<string, unknown>;
  chunk_size_bytes?: number;
}

export interface HistoryExportChunk {
  index: number;
  href: string;
  size_bytes: number;
  sha256: string;
  compression: "gzip";
}

export interface HistoryExportManifest {
  request_id: string;
  created_at: string;
  format: "json";
  compression: "gzip";
  bar_count: number;
  metadata: Record<string, unknown>;
  uncompressed_size_bytes: number;
  compressed_size_bytes: number;
  chunk_count: number;
  chunks: HistoryExportChunk[];
}

export async function createHistoryExport(
  request: HistoryExportRequest,
): Promise<HistoryExportManifest> {
  const response = await fetch(apiUrl("/api/v1/history/export"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${getApiToken()}`,
    },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    throw new Error(`History export failed: ${response.status}`);
  }
  return response.json();
}

export async function fetchHistoryExportChunk(
  requestId: string,
  index: number,
): Promise<Blob> {
  const response = await fetch(
    apiUrl(`/api/v1/history/export/${requestId}/chunks/${index}`),
    {
      headers: {
        Authorization: `Bearer ${getApiToken()}`,
      },
    },
  );
  if (!response.ok) {
    throw new Error(`History export chunk fetch failed: ${response.status}`);
  }
  return response.blob();
}
