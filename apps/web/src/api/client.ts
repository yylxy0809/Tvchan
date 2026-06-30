import { apiUrl, getApiToken } from "../config";

export type ApiSymbol = {
  symbol: string;
  code: string;
  exchange: string;
  name: string;
  asset_type: string;
};

export type ApiBar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount: number | null;
  complete: boolean;
  revision: number;
};

export type BarsResponse = {
  symbol: string;
  timeframe: string;
  bars: ApiBar[];
};

export type ChanPoint = {
  time?: number;
  price: number;
  base_ts?: number | null;
  base_seq?: number | null;
};

export type ChanStroke = {
  id: string;
  level: string;
  mode: string;
  start: ChanPoint;
  end: ChanPoint;
  begin_base_ts?: number | null;
  end_base_ts?: number | null;
  begin_base_seq?: number | null;
  end_base_seq?: number | null;
  direction: string;
  confirmed: boolean;
};

export type ChanCenter = {
  id: string;
  level: string;
  mode: string;
  start_time: number;
  end_time: number;
  begin_base_ts?: number | null;
  end_base_ts?: number | null;
  begin_base_seq?: number | null;
  end_base_seq?: number | null;
  low: number;
  high: number;
  confirmed: boolean;
};

export type ChanSignal = {
  id: string;
  level: string;
  mode: string;
  time: number;
  base_ts?: number | null;
  base_seq?: number | null;
  price: number;
  signal_type: string;
  side?: string | null;
  bsp_type?: string | null;
  features?: Record<string, unknown>;
  confirmed: boolean;
};

export type ChanChannel = {
  id: string;
  level: string;
  mode: string;
  time: number;
  base_ts?: number | null;
  base_seq?: number | null;
  upper: number;
  lower: number;
  period?: number | null;
  confirmed: boolean;
};

export type ChanOverlayResponse = {
  symbol: string;
  chart_timeframe: string;
  levels: string[];
  modes: string[];
  snapshot_version: string;
  base_timeframe: string;
  base_ts_semantics: string;
  engine: string;
  requested_bar_count: number;
  bars_by_level: Record<string, number>;
  strokes: ChanStroke[];
  segments: ChanStroke[];
  centers: ChanCenter[];
  signals: ChanSignal[];
  channels: ChanChannel[];
};

export type ChartBundleSchemaVersion =
  | "chart-bundle.v3"
  | "chart-bundle.v2"
  | "chart-window.v1"
  | "frontend-chart-bundle.v2";

export type ApiChartBundleResponse = {
  schema_version: ChartBundleSchemaVersion;
  snapshot_id: string;
  snapshot_version?: string;
  symbol: string;
  chart_timeframe: string;
  base_timeframe?: string;
  bar_time_semantics?: string;
  analysis_levels?: string[];
  range: {
    from?: number | null;
    to?: number | null;
    limit: number;
  };
  bars: ApiBar[];
  chan: ChanOverlayResponse;
  source_watermarks?: Record<string, unknown>;
  warnings?: string[];
};

export type ApiChartWindowResponse = ApiChartBundleResponse;

export const DEFAULT_CHAN_LEVELS = ["5f", "30f", "1d"] as const;
export const DEFAULT_CHAN_MODES = ["confirmed", "predictive"] as const;

function headers(): HeadersInit {
  return {
    Authorization: `Bearer ${getApiToken()}`,
  };
}

async function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(apiUrl(path), {
    headers: headers(),
    signal,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${detail}`);
  }
  return response.json() as Promise<T>;
}

export async function getHealth(): Promise<Record<string, unknown>> {
  const response = await fetch(apiUrl("/api/v1/health"));
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<Record<string, unknown>>;
}

export async function searchSymbols(keyword: string): Promise<ApiSymbol[]> {
  const params = new URLSearchParams({ keyword, limit: "20" });
  const response = await getJson<{ items: ApiSymbol[] }>(
    `/api/v1/symbols?${params.toString()}`,
  );
  return response.items;
}

export async function getBars(
  symbol: string,
  timeframe: string,
  limit = 300,
  from?: number,
  to?: number,
  signal?: AbortSignal,
): Promise<BarsResponse> {
  const params = new URLSearchParams({
    symbol,
    timeframe,
    limit: String(limit),
  });
  if (from !== undefined) params.set("from", new Date(from * 1000).toISOString());
  if (to !== undefined) params.set("to", new Date(to * 1000).toISOString());
  return getJson<BarsResponse>(`/api/v1/bars?${params.toString()}`, signal);
}

export async function getChartBundle(
  symbol: string,
  timeframe: string,
  limit = 300,
  from?: number,
  to?: number,
  signal?: AbortSignal,
  levels: readonly string[] = DEFAULT_CHAN_LEVELS,
  modes: readonly string[] = DEFAULT_CHAN_MODES,
): Promise<ApiChartWindowResponse> {
  const v3Path = chartBundlePath(
    "v3",
    symbol,
    timeframe,
    limit,
    from,
    to,
    levels,
    modes,
  );
  try {
    const v3Bundle = normalizeChartBundleForFrontend(
      await getJson<unknown>(v3Path, signal),
    );
    if (v3Bundle) {
      return v3Bundle;
    }
  } catch (error) {
    if (isAbortError(error)) {
      throw error;
    }
  }

  const v2Path = chartBundlePath(
    "v2",
    symbol,
    timeframe,
    limit,
    from,
    to,
    levels,
    modes,
  );
  const v2Bundle = normalizeChartBundleForFrontend(
    await getJson<unknown>(v2Path, signal),
  );
  if (!v2Bundle) {
    throw new Error("Invalid chart bundle response");
  }
  return v2Bundle;
}

export async function getChartWindow(
  symbol: string,
  timeframe: string,
  limit = 300,
  from?: number,
  to?: number,
  signal?: AbortSignal,
  levels: readonly string[] = DEFAULT_CHAN_LEVELS,
  modes: readonly string[] = DEFAULT_CHAN_MODES,
): Promise<ApiChartWindowResponse> {
  return getChartBundle(symbol, timeframe, limit, from, to, signal, levels, modes);
}

export function normalizeChartBundleForFrontend(
  payload: unknown,
): ApiChartBundleResponse | null {
  const bundle = asRecord(payload);
  if (!bundle) {
    return null;
  }
  const bars = Array.isArray(bundle.bars) ? (bundle.bars as ApiBar[]) : null;
  const rawChan = asRecord(bundle.chan);
  if (!bars || !rawChan) {
    return null;
  }

  const symbol = readString(bundle.symbol) ?? readString(rawChan.symbol);
  const chartTimeframe =
    readString(bundle.chart_timeframe) ?? readString(rawChan.chart_timeframe);
  if (!symbol || !chartTimeframe) {
    return null;
  }

  const snapshotId = readString(bundle.snapshot_id) ?? "";
  const snapshotVersion =
    readString(bundle.snapshot_version) ??
    readString(rawChan.snapshot_version) ??
    snapshotId;
  const baseTimeframe =
    readString(bundle.base_timeframe) ??
    readString(rawChan.base_timeframe) ??
    "5f";
  const baseTsSemantics =
    readString(bundle.bar_time_semantics) ??
    readString(rawChan.base_ts_semantics) ??
    "bar_end";
  const analysisLevels =
    readStringArray(bundle.analysis_levels) ??
    readStringArray(rawChan.levels) ??
    [...DEFAULT_CHAN_LEVELS];
  const chan = normalizeChanOverlayForFrontend(rawChan, {
    symbol,
    chartTimeframe,
    snapshotVersion,
    baseTimeframe,
    baseTsSemantics,
    analysisLevels,
    requestedBarCount: bars.length,
  });
  if (!chan) {
    return null;
  }

  return {
    ...(bundle as Partial<ApiChartBundleResponse>),
    schema_version: readSchemaVersion(bundle.schema_version),
    snapshot_id: snapshotId || createFallbackSnapshotId(symbol, chartTimeframe, bars, chan),
    snapshot_version: snapshotVersion,
    symbol,
    chart_timeframe: chartTimeframe,
    base_timeframe: baseTimeframe,
    bar_time_semantics: baseTsSemantics,
    analysis_levels: analysisLevels,
    range: normalizeRange(bundle.range, bars.length),
    bars,
    chan,
    source_watermarks: asRecord(bundle.source_watermarks) ?? undefined,
    warnings: readStringArray(bundle.warnings) ?? undefined,
  };
}

type ChanNormalizeContext = {
  symbol: string;
  chartTimeframe: string;
  snapshotVersion: string;
  baseTimeframe: string;
  baseTsSemantics: string;
  analysisLevels: string[];
  requestedBarCount: number;
};

function chartBundlePath(
  version: "v2" | "v3",
  symbol: string,
  timeframe: string,
  limit: number,
  from: number | undefined,
  to: number | undefined,
  levels: readonly string[],
  modes: readonly string[],
): string {
  const params = new URLSearchParams({
    symbol,
    timeframe,
    limit: String(limit),
  });
  if (version === "v2") {
    params.set("levels", levels.join(","));
    params.set("modes", modes.join(","));
  }
  if (from !== undefined) params.set("from", new Date(from * 1000).toISOString());
  if (to !== undefined) params.set("to", new Date(to * 1000).toISOString());
  return `/api/${version}/chart/bundle?${params.toString()}`;
}

function normalizeChanOverlayForFrontend(
  rawChan: Record<string, unknown>,
  context: ChanNormalizeContext,
): ChanOverlayResponse | null {
  const nestedLevels = asRecord(rawChan.levels);
  const flatStrokes = Array.isArray(rawChan.strokes);
  if (nestedLevels) {
    return normalizeNestedChanOverlay(rawChan, nestedLevels, context);
  }
  if (flatStrokes) {
    return normalizeFlatChanOverlay(rawChan, context);
  }
  return null;
}

function normalizeNestedChanOverlay(
  rawChan: Record<string, unknown>,
  nestedLevels: Record<string, unknown>,
  context: ChanNormalizeContext,
): ChanOverlayResponse {
  const strokes: ChanStroke[] = [];
  const segments: ChanStroke[] = [];
  const centers: ChanCenter[] = [];
  const signals: ChanSignal[] = [];
  const channels: ChanChannel[] = [];
  const barsByLevel: Record<string, number> = {};

  for (const level of context.analysisLevels) {
    const data = asRecord(nestedLevels[level]) ?? {};
    barsByLevel[level] = readNumber(data.bar_count) ?? 0;
    strokes.push(...normalizeStrokeArray(data.strokes, level));
    segments.push(...normalizeStrokeArray(data.segments, level));
    centers.push(...normalizeCenterArray(data.centers, level));
    signals.push(...normalizeSignalArray(data.signals, level));
    channels.push(...normalizeChannelArray(data.channels, level));
  }

  return buildChanOverlay(rawChan, context, {
    levels: context.analysisLevels,
    modes: collectChanModes(strokes, segments, centers, signals, channels),
    barsByLevel,
    strokes,
    segments,
    centers,
    signals,
    channels,
  });
}

function normalizeFlatChanOverlay(
  rawChan: Record<string, unknown>,
  context: ChanNormalizeContext,
): ChanOverlayResponse {
  const levels = readStringArray(rawChan.levels) ?? context.analysisLevels;
  return buildChanOverlay(rawChan, context, {
    levels,
    modes: readStringArray(rawChan.modes) ?? undefined,
    barsByLevel: normalizeBarsByLevel(rawChan.bars_by_level, levels),
    strokes: normalizeStrokeArray(rawChan.strokes, ""),
    segments: normalizeStrokeArray(rawChan.segments, ""),
    centers: normalizeCenterArray(rawChan.centers, ""),
    signals: normalizeSignalArray(rawChan.signals, ""),
    channels: normalizeChannelArray(rawChan.channels, ""),
  });
}

function buildChanOverlay(
  rawChan: Record<string, unknown>,
  context: ChanNormalizeContext,
  parts: {
    levels: string[];
    modes?: string[];
    barsByLevel: Record<string, number>;
    strokes: ChanStroke[];
    segments: ChanStroke[];
    centers: ChanCenter[];
    signals: ChanSignal[];
    channels: ChanChannel[];
  },
): ChanOverlayResponse {
  return {
    ...(rawChan as Partial<ChanOverlayResponse>),
    symbol: readString(rawChan.symbol) ?? context.symbol,
    chart_timeframe: readString(rawChan.chart_timeframe) ?? context.chartTimeframe,
    levels: parts.levels,
    modes: parts.modes?.length ? parts.modes : [...DEFAULT_CHAN_MODES],
    snapshot_version:
      readString(rawChan.snapshot_version) ?? context.snapshotVersion,
    base_timeframe: readString(rawChan.base_timeframe) ?? context.baseTimeframe,
    base_ts_semantics:
      readString(rawChan.base_ts_semantics) ?? context.baseTsSemantics,
    engine: readString(rawChan.engine) ?? "unknown",
    requested_bar_count:
      readNumber(rawChan.requested_bar_count) ?? context.requestedBarCount,
    bars_by_level: parts.barsByLevel,
    strokes: parts.strokes,
    segments: parts.segments,
    centers: parts.centers,
    signals: parts.signals,
    channels: parts.channels,
  };
}

function normalizeStrokeArray(value: unknown, fallbackLevel: string): ChanStroke[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => normalizeStroke(item, fallbackLevel))
    .filter((item): item is ChanStroke => item !== null);
}

function normalizeStroke(value: unknown, fallbackLevel: string): ChanStroke | null {
  const item = asRecord(value);
  if (!item) {
    return null;
  }
  const start = asRecord(item.start) ?? {};
  const end = asRecord(item.end) ?? {};
  const beginBaseTs = readNumber(item.begin_base_ts) ?? readNumber(start.base_ts) ?? readNumber(start.time);
  const endBaseTs = readNumber(item.end_base_ts) ?? readNumber(end.base_ts) ?? readNumber(end.time);
  const startPrice = readNumber(start.price);
  const endPrice = readNumber(end.price);
  if (beginBaseTs === null || endBaseTs === null || startPrice === null || endPrice === null) {
    return null;
  }
  return {
    ...(item as Partial<ChanStroke>),
    id: readString(item.id) ?? `${fallbackLevel}:${beginBaseTs}:${endBaseTs}`,
    level: readString(item.level) ?? fallbackLevel,
    mode: readString(item.mode) ?? "confirmed",
    start: {
      ...(start as Partial<ChanPoint>),
      time: readNumber(start.time) ?? beginBaseTs,
      price: startPrice,
      base_ts: beginBaseTs,
      base_seq: readNumber(start.base_seq),
    },
    end: {
      ...(end as Partial<ChanPoint>),
      time: readNumber(end.time) ?? endBaseTs,
      price: endPrice,
      base_ts: endBaseTs,
      base_seq: readNumber(end.base_seq),
    },
    begin_base_ts: beginBaseTs,
    end_base_ts: endBaseTs,
    begin_base_seq: readNumber(item.begin_base_seq),
    end_base_seq: readNumber(item.end_base_seq),
    direction: readString(item.direction) ?? "",
    confirmed: readBoolean(item.confirmed) ?? true,
  };
}

function normalizeCenterArray(value: unknown, fallbackLevel: string): ChanCenter[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => normalizeCenter(item, fallbackLevel))
    .filter((item): item is ChanCenter => item !== null);
}

function normalizeCenter(value: unknown, fallbackLevel: string): ChanCenter | null {
  const item = asRecord(value);
  if (!item) {
    return null;
  }
  const beginBaseTs = readNumber(item.begin_base_ts) ?? readNumber(item.start_time);
  const endBaseTs = readNumber(item.end_base_ts) ?? readNumber(item.end_time);
  const low = readNumber(item.low);
  const high = readNumber(item.high);
  if (beginBaseTs === null || endBaseTs === null || low === null || high === null) {
    return null;
  }
  return {
    ...(item as Partial<ChanCenter>),
    id: readString(item.id) ?? `${fallbackLevel}:center:${beginBaseTs}:${endBaseTs}`,
    level: readString(item.level) ?? fallbackLevel,
    mode: readString(item.mode) ?? "confirmed",
    start_time: readNumber(item.start_time) ?? beginBaseTs,
    end_time: readNumber(item.end_time) ?? endBaseTs,
    begin_base_ts: beginBaseTs,
    end_base_ts: endBaseTs,
    begin_base_seq: readNumber(item.begin_base_seq),
    end_base_seq: readNumber(item.end_base_seq),
    low,
    high,
    confirmed: readBoolean(item.confirmed) ?? true,
  };
}

function normalizeSignalArray(value: unknown, fallbackLevel: string): ChanSignal[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => normalizeSignal(item, fallbackLevel))
    .filter((item): item is ChanSignal => item !== null);
}

function normalizeSignal(value: unknown, fallbackLevel: string): ChanSignal | null {
  const item = asRecord(value);
  if (!item) {
    return null;
  }
  const baseTs = readNumber(item.base_ts) ?? readNumber(item.time);
  const price = readNumber(item.price);
  if (baseTs === null || price === null) {
    return null;
  }
  return {
    ...(item as Partial<ChanSignal>),
    id: readString(item.id) ?? `${fallbackLevel}:signal:${baseTs}:${price}`,
    level: readString(item.level) ?? fallbackLevel,
    mode: readString(item.mode) ?? "confirmed",
    time: readNumber(item.time) ?? baseTs,
    base_ts: baseTs,
    base_seq: readNumber(item.base_seq),
    price,
    signal_type:
      readString(item.signal_type) ?? readString(item.signal_key) ?? "",
    side: readString(item.side),
    bsp_type: readString(item.bsp_type),
    features: asRecord(item.features) ?? undefined,
    confirmed: readBoolean(item.confirmed) ?? true,
  };
}

function normalizeChannelArray(value: unknown, fallbackLevel: string): ChanChannel[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => normalizeChannel(item, fallbackLevel))
    .filter((item): item is ChanChannel => item !== null);
}

function normalizeChannel(value: unknown, fallbackLevel: string): ChanChannel | null {
  const item = asRecord(value);
  if (!item) {
    return null;
  }
  const baseTs = readNumber(item.base_ts) ?? readNumber(item.time);
  const upper = readNumber(item.upper);
  const lower = readNumber(item.lower);
  if (baseTs === null || upper === null || lower === null) {
    return null;
  }
  return {
    ...(item as Partial<ChanChannel>),
    id: readString(item.id) ?? `${fallbackLevel}:channel:${baseTs}`,
    level: readString(item.level) ?? fallbackLevel,
    mode: readString(item.mode) ?? "confirmed",
    time: readNumber(item.time) ?? baseTs,
    base_ts: baseTs,
    base_seq: readNumber(item.base_seq),
    upper,
    lower,
    period: readNumber(item.period),
    confirmed: readBoolean(item.confirmed) ?? true,
  };
}

function collectChanModes(
  ...groups: Array<Array<{ mode: string }>>
): string[] {
  const modes = new Set<string>();
  for (const group of groups) {
    for (const item of group) {
      if (item.mode) {
        modes.add(item.mode);
      }
    }
  }
  return [...modes];
}

function normalizeBarsByLevel(
  value: unknown,
  levels: readonly string[],
): Record<string, number> {
  const record = asRecord(value);
  return Object.fromEntries(
    levels.map((level) => [level, readNumber(record?.[level]) ?? 0]),
  );
}

function normalizeRange(value: unknown, fallbackLimit: number): ApiChartBundleResponse["range"] {
  const range = asRecord(value) ?? {};
  return {
    from: readNumber(range.from),
    to: readNumber(range.to),
    limit: readNumber(range.limit) ?? fallbackLimit,
  };
}

function readSchemaVersion(value: unknown): ChartBundleSchemaVersion {
  const schema = readString(value);
  if (
    schema === "chart-bundle.v3" ||
    schema === "chart-bundle.v2" ||
    schema === "chart-window.v1" ||
    schema === "frontend-chart-bundle.v2"
  ) {
    return schema;
  }
  return "chart-bundle.v2";
}

function createFallbackSnapshotId(
  symbol: string,
  timeframe: string,
  bars: ApiBar[],
  chan: ChanOverlayResponse,
): string {
  const first = bars[0]?.time ?? "";
  const last = bars[bars.length - 1]?.time ?? "";
  return [
    symbol,
    timeframe,
    chan.snapshot_version,
    first,
    last,
    bars.length,
  ].join("|");
}

function readStringArray(value: unknown): string[] | null {
  if (!Array.isArray(value)) {
    return null;
  }
  return value.filter((item): item is string => typeof item === "string");
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function readNumber(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function readBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}
