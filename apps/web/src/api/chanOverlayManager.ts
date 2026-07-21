import { getChanOverlay, type ChanCenter, type ChanChannel, type ChanOverlayResponse, type ChanSignal, type ChanStroke } from "./client";
import type { ChanMode } from "../tradingview/overlaySettings";
import type { ChanRealtimeOverlayState } from "./chanRealtimeOverlayBridge";

export type ChanOverlayRange = { from: number; to: number };
type TimedBar = Readonly<{ time: number }>;
type FetchOverlay = (symbol: string, timeframe: string, limit: number, from?: number, to?: number, signal?: AbortSignal, levels?: readonly string[], modes?: readonly string[]) => Promise<ChanOverlayResponse>;
export type ChanOverlayContextRequest = ChanOverlayRange & { symbol: string; timeframe: string; modes: ChanMode[] };
type OverlayRequest = ChanOverlayContextRequest & { onPaint(overlay: ChanOverlayResponse): void; onError(error: Error): void };
type Consumer = OverlayRequest & { key: string; generation: number; settled?: boolean };
type InFlight = { range: ChanOverlayRange; controller: AbortController; consumers: Set<Consumer> };
type CacheEntry = { overlay: ChanOverlayResponse; ranges: ChanOverlayRange[]; retained?: ChanOverlayRange; lastUsed: number };

const MAX_CACHE_ENTRIES = 12;
const MAX_RANGES_PER_ENTRY = 24;

export function chanLevelsForTimeframe(timeframe: string): readonly string[] {
  switch (timeframe) {
    case "5f": case "15f": return ["5f", "30f", "1d"];
    case "30f": case "1h": return ["30f", "1d"];
    case "1d": return ["1d", "1w"];
    case "1w": return ["1w", "1m"];
    case "1m": return ["1m"];
    default: return [];
  }
}

export function clampOverlayRangeToBars(
  range: ChanOverlayRange,
  bars: readonly TimedBar[],
): ChanOverlayRange | null {
  const first = bars[0]?.time;
  const last = bars[bars.length - 1]?.time;
  if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
  const clamped = {
    from: Math.max(range.from, first),
    to: Math.min(range.to, last),
  };
  return clamped.from <= clamped.to ? clamped : null;
}

export class ChanOverlayManager {
  private readonly fetchOverlay: FetchOverlay;
  private readonly cache = new Map<string, CacheEntry>();
  private readonly flights = new Map<string, InFlight[]>();
  private timer: ReturnType<typeof setTimeout> | undefined;
  private pending: OverlayRequest | undefined;
  private generation = 0;
  private activeKey: string | undefined;
  private disposed = false;

  constructor(fetchOverlay: FetchOverlay = getChanOverlay) { this.fetchOverlay = fetchOverlay; }

  async fetchFresh(request: ChanOverlayContextRequest, signal?: AbortSignal): Promise<ChanOverlayResponse> {
    const overlay = await this.fetchOverlay(request.symbol, request.timeframe, rangeLength(request), request.from, request.to, signal, chanLevelsForTimeframe(request.timeframe), request.modes);
    return this.hydrateHttp(overlay, request, request);
  }

  hydrateHttp(overlay: ChanOverlayResponse, range: ChanOverlayRange, context?: Pick<ChanOverlayContextRequest, "symbol" | "timeframe" | "modes">): ChanOverlayResponse {
    const validationError = validateChanOverlayResponse(overlay);
    if (validationError) throw new Error(`Invalid Chan overlay response: ${validationError}`);
    const modes = normalizeModes((context?.modes ?? overlay.modes) as ChanMode[]);
    const key = cacheKey(context?.symbol ?? overlay.symbol, context?.timeframe ?? overlay.chart_timeframe, modes);
    const previous = this.cache.get(key);
    const next: CacheEntry = {
      overlay: previous ? mergeOverlays(previous.overlay, overlay, range) : overlay,
      ranges: mergeRanges(previous?.ranges ?? [], range),
      retained: previous?.retained,
      lastUsed: Date.now(),
    };
    if (next.retained) {
      next.overlay = pruneOverlay(next.overlay, next.retained);
      next.ranges = clipRanges(next.ranges, next.retained);
    }
    this.cache.set(key, next);
    this.evict();
    return next.overlay;
  }

  applyRealtime(state: ChanRealtimeOverlayState): ChanOverlayResponse | null {
    const modes = normalizeModes(state.modes as ChanMode[]);
    const key = cacheKey(state.symbol, state.chartTimeframe, modes);
    const previous = this.cache.get(key);
    if (!previous) return null;
    const realtime: ChanOverlayResponse = {
      ...previous.overlay,
      symbol: state.symbol,
      chart_timeframe: state.chartTimeframe,
      modes: [...state.modes],
      snapshot_version: state.snapshotVersion,
      strokes: state.objects.strokes as ChanStroke[],
      segments: state.objects.segments as ChanStroke[],
      centers: state.objects.centers as ChanCenter[],
      signals: state.objects.signals as ChanSignal[],
      channels: state.objects.channels as ChanChannel[],
    };
    const overlay = mergeOverlays(previous.overlay, realtime, state.range);
    previous.overlay = overlay;
    previous.ranges = mergeRanges(previous.ranges, state.range);
    previous.lastUsed = Date.now();
    return overlay;
  }

  request(request: OverlayRequest): () => void {
    const modes = normalizeModes(request.modes);
    if (this.disposed || request.to < request.from || !modes.length || !chanLevelsForTimeframe(request.timeframe).length) return () => {};
    const pending = { ...request, modes };
    this.pending = pending;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = setTimeout(() => { this.timer = undefined; const next = this.pending; this.pending = undefined; if (next) this.consume(next); }, 150);
    return () => this.cancelPending(pending);
  }

  // Keep two visible-window widths on each side so panning retains continuity without unbounded growth.
  retain(symbol: string, timeframe: string, modes: ChanMode[], visible: ChanOverlayRange): void {
    const key = cacheKey(symbol, timeframe, normalizeModes(modes));
    const entry = this.cache.get(key);
    if (!entry) return;
    const guard = Math.max(1, visible.to - visible.from + 1) * 2;
    const retained = { from: visible.from - guard, to: visible.to + guard };
    entry.retained = retained;
    entry.overlay = pruneOverlay(entry.overlay, retained);
    entry.ranges = clipRanges(entry.ranges, retained);
  }

  switchContext(): void {
    this.generation += 1;
    for (const flights of this.flights.values()) for (const flight of flights) flight.controller.abort();
    this.flights.clear();
  }

  dispose(): void {
    this.disposed = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.pending = undefined;
    this.switchContext();
  }

  private consume(request: OverlayRequest): void {
    if (this.disposed) return;
    const key = cacheKey(request.symbol, request.timeframe, request.modes);
    if (this.activeKey !== undefined && this.activeKey !== key) this.switchContext();
    this.activeKey = key;
    const consumer: Consumer = { ...request, key, generation: this.generation };
    this.attachConsumer(consumer);
  }

  private attachConsumer(consumer: Consumer): void {
    const { key } = consumer;
    const entry = this.cache.get(key);
    if (entry && rangeCovered(entry.ranges, consumer)) { consumer.settled = true; entry.lastUsed = Date.now(); consumer.onPaint(entry.overlay); return; }
    const flights = this.flights.get(key) ?? [];
    const coveredFlight = flights.find((flight) => rangeContains(flight.range, consumer));
    if (coveredFlight) { coveredFlight.consumers.add(consumer); return; }
    const occupied = [...(entry?.ranges ?? []), ...flights.map((flight) => flight.range)];
    const missing = missingRanges(occupied, consumer);
    if (!missing.length) {
      // The requested range is jointly covered by active flights. Subscribe to
      // every overlapping flight instead of polling until one happens to finish.
      for (const flight of flights) if (overlaps(flight.range, consumer)) flight.consumers.add(consumer);
      return;
    }
    for (const range of missing) this.startFlight(key, consumer, range);
  }

  private startFlight(key: string, consumer: Consumer, range: ChanOverlayRange): void {
    const controller = new AbortController();
    const flight: InFlight = { range, controller, consumers: new Set([consumer]) };
    const flights = this.flights.get(key) ?? [];
    flights.push(flight); this.flights.set(key, flights);
    void this.fetchOverlay(consumer.symbol, consumer.timeframe, rangeLength(range), range.from, range.to, controller.signal, chanLevelsForTimeframe(consumer.timeframe), consumer.modes)
      .then((overlay) => {
        if (controller.signal.aborted || this.disposed) return;
        this.hydrateHttp(overlay, range, consumer);
        // Remove this completed interval before calculating gaps for its consumers.
        this.removeFlight(key, flight);
        for (const item of flight.consumers) queueMicrotask(() => this.waitForCoverage(item));
      })
      .catch((error) => {
        if (isAbortError(error)) return;
        const failure = error instanceof Error ? error : new Error(String(error));
        for (const item of [...flight.consumers]) this.failConsumer(item, failure);
      })
      .finally(() => this.removeFlight(key, flight));
  }

  private waitForCoverage(consumer: Consumer): void {
    if (consumer.settled || consumer.generation !== this.generation || this.disposed) return;
    const entry = this.cache.get(consumer.key);
    if (entry && rangeCovered(entry.ranges, consumer)) { consumer.settled = true; entry.lastUsed = Date.now(); consumer.onPaint(entry.overlay); return; }
    this.attachConsumer(consumer);
  }

  private failConsumer(consumer: Consumer, error: Error): void {
    if (consumer.settled) return;
    consumer.settled = true;
    for (const flights of this.flights.values()) for (const flight of flights) {
      if (!flight.consumers.delete(consumer)) continue;
      if (!flight.consumers.size) flight.controller.abort();
    }
    consumer.onError(error);
  }

  private cancelPending(request: OverlayRequest): void {
    if (this.pending === request) this.pending = undefined;
    for (const flights of this.flights.values()) for (const flight of flights) {
      for (const consumer of flight.consumers) if (consumer.onPaint === request.onPaint) flight.consumers.delete(consumer);
      if (!flight.consumers.size) flight.controller.abort();
    }
  }

  private removeFlight(key: string, flight: InFlight): void {
    const current = this.flights.get(key)?.filter((item) => item !== flight) ?? [];
    if (current.length) this.flights.set(key, current); else this.flights.delete(key);
  }

  private evict(): void {
    if (this.cache.size <= MAX_CACHE_ENTRIES) return;
    for (const [key] of [...this.cache.entries()].sort((a, b) => a[1].lastUsed - b[1].lastUsed).slice(0, this.cache.size - MAX_CACHE_ENTRIES)) this.cache.delete(key);
  }
}

function normalizeModes(modes: ChanMode[]): ChanMode[] { return [...new Set(modes.filter((mode): mode is ChanMode => mode === "confirmed" || mode === "predictive"))].sort(); }
function cacheKey(symbol: string, timeframe: string, modes: ChanMode[]): string { return `${symbol.toUpperCase()}|${timeframe}|${modes.join(",")}`; }
function rangeLength(range: ChanOverlayRange): number {
  // The range uses epoch seconds; the API limit is a bar count.
  // Keep the estimate bounded because visible windows include overnight gaps.
  return Math.min(5000, Math.max(1, Math.floor(range.to - range.from + 1)));
}
function overlaps(a: ChanOverlayRange, b: ChanOverlayRange): boolean { return a.from <= b.to && b.from <= a.to; }
function rangeContains(a: ChanOverlayRange, b: ChanOverlayRange): boolean { return a.from <= b.from && a.to >= b.to; }
function rangeCovered(ranges: ChanOverlayRange[], wanted: ChanOverlayRange): boolean { return ranges.some((range) => rangeContains(range, wanted)); }
function clipRanges(ranges: ChanOverlayRange[], retained: ChanOverlayRange): ChanOverlayRange[] { return ranges.map((range) => ({ from: Math.max(range.from, retained.from), to: Math.min(range.to, retained.to) })).filter((range) => range.from <= range.to); }
function missingRanges(ranges: ChanOverlayRange[], wanted: ChanOverlayRange): ChanOverlayRange[] { let cursor = wanted.from; const result: ChanOverlayRange[] = []; for (const range of mergeRanges([], ...ranges)) { if (range.to < cursor) continue; if (range.from > cursor) result.push({ from: cursor, to: Math.min(wanted.to, range.from - 1) }); cursor = Math.max(cursor, range.to + 1); if (cursor > wanted.to) break; } if (cursor <= wanted.to) result.push({ from: cursor, to: wanted.to }); return result; }
function mergeRanges(ranges: ChanOverlayRange[], ...next: ChanOverlayRange[]): ChanOverlayRange[] { const sorted = [...ranges, ...next].sort((a, b) => a.from - b.from); const result: ChanOverlayRange[] = []; for (const range of sorted) { const last = result[result.length - 1]; if (last && range.from <= last.to + 1) last.to = Math.max(last.to, range.to); else result.push({ ...range }); } return result.slice(-MAX_RANGES_PER_ENTRY); }

function pruneOverlay(overlay: ChanOverlayResponse, retained: ChanOverlayRange): ChanOverlayResponse {
  const keepLines = (items: ChanStroke[]) => retainBoundary(items, retained, (item) => Number(item.start.base_ts ?? item.start.time), (item) => Number(item.end.base_ts ?? item.end.time), (item) => `${item.level}|${item.mode}`);
  return { ...overlay, strokes: keepLines(overlay.strokes), segments: keepLines(overlay.segments), centers: overlay.centers.filter((item) => Number(item.end_base_ts ?? item.end_time) >= retained.from && Number(item.begin_base_ts ?? item.start_time) <= retained.to), signals: overlay.signals.filter((item) => Number(item.base_ts ?? item.time) >= retained.from && Number(item.base_ts ?? item.time) <= retained.to), channels: overlay.channels.filter((item) => Number(item.base_ts ?? item.time) >= retained.from && Number(item.base_ts ?? item.time) <= retained.to) };
}
function retainBoundary(items: ChanStroke[], retained: ChanOverlayRange, start: (item: ChanStroke) => number, end: (item: ChanStroke) => number, group: (item: ChanStroke) => string): ChanStroke[] { const groups = new Map<string, ChanStroke[]>(); for (const item of items) { const key = group(item); const values = groups.get(key) ?? []; values.push(item); groups.set(key, values); } const result: ChanStroke[] = []; for (const groupItems of groups.values()) { const inside = groupItems.filter((item) => end(item) >= retained.from && start(item) <= retained.to); const before = groupItems.filter((item) => end(item) < retained.from).slice(-1)[0]; const after = groupItems.find((item) => start(item) > retained.to); result.push(...inside, ...(before ? [before] : []), ...(after ? [after] : [])); } return result.sort(strokeOrder); }
function mergeOverlays(previous: ChanOverlayResponse, incoming: ChanOverlayResponse, authoritativeRange?: ChanOverlayRange): ChanOverlayResponse {
  const scopes = { levels: new Set(incoming.levels), modes: new Set(incoming.modes) };
  return {
    ...incoming,
    levels: [...new Set([...previous.levels, ...incoming.levels])],
    modes: [...new Set([...previous.modes, ...incoming.modes])],
    bars_by_level: { ...previous.bars_by_level, ...incoming.bars_by_level },
    strokes: reconcileById(previous.strokes, incoming.strokes, authoritativeRange, scopes, lineOwnedByRange, strokeOrder),
    segments: reconcileById(previous.segments, incoming.segments, authoritativeRange, scopes, lineOwnedByRange, strokeOrder),
    centers: reconcileById(previous.centers, incoming.centers, authoritativeRange, scopes, centerOwnedByRange, centerOrder),
    signals: reconcileById(previous.signals, incoming.signals, authoritativeRange, scopes, pointOwnedByRange, signalOrder),
    channels: reconcileById(previous.channels, incoming.channels, authoritativeRange, scopes, pointOwnedByRange, channelOrder),
  };
}
type OverlayScoped = { id: string; level: string; mode: string };
function reconcileById<T extends OverlayScoped>(previous: T[], incoming: T[], range: ChanOverlayRange | undefined, scopes: { levels: Set<string>; modes: Set<string> }, owned: (item: T, range: ChanOverlayRange) => boolean, compare: (a: T, b: T) => number): T[] {
  const incomingIds = new Set(incoming.map((item) => item.id));
  const retained = range
    ? previous.filter((item) => incomingIds.has(item.id) || !scopes.levels.has(item.level) || !scopes.modes.has(item.mode) || !owned(item, range))
    : previous;
  return mergeById(retained, incoming, (item) => item.id, compare);
}
function lineOwnedByRange(item: ChanStroke, range: ChanOverlayRange): boolean { return Number(item.end.base_ts ?? item.end_base_ts ?? item.end.time) >= range.from && Number(item.start.base_ts ?? item.begin_base_ts ?? item.start.time) <= range.to; }
function centerOwnedByRange(item: ChanCenter, range: ChanOverlayRange): boolean { return Number(item.end_base_ts ?? item.end_time) >= range.from && Number(item.begin_base_ts ?? item.start_time) <= range.to; }
function pointOwnedByRange(item: ChanSignal | ChanChannel, range: ChanOverlayRange): boolean { const time = Number(item.base_ts ?? item.time); return time >= range.from && time <= range.to; }
function mergeById<T>(previous: T[], incoming: T[], id: (item: T) => string, compare: (a: T, b: T) => number): T[] { const merged = new Map(previous.map((item) => [id(item), item])); for (const item of incoming) merged.set(id(item), item); return [...merged.values()].sort(compare); }
function strokeOrder(a: ChanStroke, b: ChanStroke): number { return Number(a.start.base_ts ?? a.start.time ?? 0) - Number(b.start.base_ts ?? b.start.time ?? 0) || a.id.localeCompare(b.id); }
function centerOrder(a: ChanCenter, b: ChanCenter): number { return a.start_time - b.start_time || a.id.localeCompare(b.id); }
function signalOrder(a: ChanSignal, b: ChanSignal): number { return a.time - b.time || a.id.localeCompare(b.id); }
function channelOrder(a: ChanChannel, b: ChanChannel): number { return a.time - b.time || a.id.localeCompare(b.id); }
function isAbortError(error: unknown): boolean { return error instanceof DOMException ? error.name === "AbortError" : error instanceof Error && error.name === "AbortError"; }

export function validateChanOverlayResponse(value: unknown): string | null {
  if (!isRecord(value)) return "overlay must be an object";
  for (const key of ["symbol", "chart_timeframe", "snapshot_version", "base_timeframe", "engine"] as const) {
    if (typeof value[key] !== "string" || value[key].length === 0) return `invalid ${key}`;
  }
  if (value.base_ts_semantics !== "bar_end") return "invalid base_ts_semantics";
  if (!strictInteger(value.requested_bar_count) || value.requested_bar_count < 0) return "invalid requested_bar_count";
  const allowedLevels = new Set(["5f", "30f", "1d", "1w", "1m"]);
  const allowedModes = new Set(["confirmed", "predictive"]);
  if (!Array.isArray(value.levels) || value.levels.some((item) => typeof item !== "string" || !allowedLevels.has(item))) return "invalid levels";
  if (!Array.isArray(value.modes) || value.modes.some((item) => typeof item !== "string" || !allowedModes.has(item))) return "invalid modes";
  const barsByLevel = value.bars_by_level;
  if (!isRecord(barsByLevel)) return "invalid bars_by_level";
  for (const [level, count] of Object.entries(barsByLevel)) {
    if (!allowedLevels.has(level) || !strictInteger(count) || count < 0) return `invalid bars_by_level.${level}`;
  }
  if (value.levels.some((level) => !(level in barsByLevel))) return "missing bars_by_level entry";
  const { strokes, segments, centers, signals, channels } = value;
  if (!Array.isArray(strokes) || !Array.isArray(segments) || !Array.isArray(centers) || !Array.isArray(signals) || !Array.isArray(channels)) return "invalid overlay collections";
  if (![...strokes, ...segments].every(validLine)) return "invalid line collection";
  if (!centers.every(validCenter)) return "invalid centers";
  if (!signals.every(validSignal)) return "invalid signals";
  if (!channels.every(validChannel)) return "invalid channels";
  return null;
}

function validScoped(value: unknown): value is Record<string, unknown> {
  return isRecord(value)
    && typeof value.id === "string" && value.id.length > 0
    && typeof value.level === "string" && ["5f", "30f", "1d", "1w", "1m"].includes(value.level)
    && typeof value.mode === "string" && ["confirmed", "predictive"].includes(value.mode)
    && typeof value.confirmed === "boolean";
}
function validLine(value: unknown): boolean {
  if (!validScoped(value) || !isRecord(value.start) || !isRecord(value.end)) return false;
  return typeof value.direction === "string" && value.direction.length > 0
    && optionalInteger(value.seq, true)
    && optionalInteger(value.begin_base_ts, true) && optionalInteger(value.end_base_ts, true)
    && optionalInteger(value.begin_base_seq, true) && optionalInteger(value.end_base_seq, true)
    && optionalInteger(value.start_time, false) && optionalInteger(value.end_time, false)
    && optionalInteger(value.index, false) && optionalInteger(value.start_index, false) && optionalInteger(value.end_index, false)
    && optionalNumber(value.start_price, false) && optionalNumber(value.end_price, false)
    && optionalNumber(value.strength, false)
    && validChanPoint(value.start) && validChanPoint(value.end)
    && strictNumber(value.start.price) && strictNumber(value.end.price)
    && firstStrictNumber(value.start.base_ts, value.begin_base_ts, value.start.time)
    && firstStrictNumber(value.end.base_ts, value.end_base_ts, value.end.time)
    && nullableAliasesAgree(value.start.base_ts, value.begin_base_ts, value.start.time)
    && nullableAliasesAgree(value.end.base_ts, value.end_base_ts, value.end.time)
    && nullableAliasesAgree(value.start.base_seq, value.begin_base_seq)
    && nullableAliasesAgree(value.end.base_seq, value.end_base_seq)
    && nonNullAliasesAgree(value.start.price, value.start_price)
    && nonNullAliasesAgree(value.end.price, value.end_price);
}
function validCenter(value: unknown): boolean {
  return validScoped(value)
    && optionalInteger(value.seq, true)
    && strictInteger(value.start_time) && strictInteger(value.end_time)
    && optionalInteger(value.begin_base_ts, true) && optionalInteger(value.end_base_ts, true)
    && optionalInteger(value.begin_base_seq, true) && optionalInteger(value.end_base_seq, true)
    && optionalInteger(value.index, false) && optionalNumber(value.strength, false)
    && firstStrictNumber(value.begin_base_ts, value.start_time)
    && firstStrictNumber(value.end_base_ts, value.end_time)
    && nullableAliasesAgree(value.begin_base_ts, value.start_time)
    && nullableAliasesAgree(value.end_base_ts, value.end_time)
    && strictNumber(value.high) && strictNumber(value.low);
}
function validSignal(value: unknown): boolean {
  return validScoped(value)
    && optionalInteger(value.seq, true)
    && strictInteger(value.time)
    && optionalInteger(value.base_ts, true) && optionalInteger(value.base_seq, true)
    && optionalInteger(value.index, false) && optionalNumber(value.strength, false)
    && firstStrictNumber(value.base_ts, value.time)
    && nullableAliasesAgree(value.base_ts, value.time)
    && strictNumber(value.price)
    && typeof value.signal_type === "string" && value.signal_type.length > 0
    && optionalNullableString(value.side)
    && optionalNullableString(value.bsp_type)
    && optionalString(value.signal_key)
    && optionalBoolean(value.is_sure);
}
function validChannel(value: unknown): boolean {
  return validScoped(value)
    && strictInteger(value.time)
    && optionalInteger(value.base_ts, true) && optionalInteger(value.base_seq, true)
    && optionalInteger(value.period, true) && optionalInteger(value.index, false)
    && optionalNumber(value.strength, false)
    && firstStrictNumber(value.base_ts, value.time)
    && nullableAliasesAgree(value.base_ts, value.time)
    && strictNumber(value.upper) && strictNumber(value.lower);
}
function validChanPoint(value: Record<string, unknown>): boolean {
  return strictNumber(value.price)
    && optionalInteger(value.time, false)
    && optionalInteger(value.base_ts, true)
    && optionalInteger(value.base_seq, true)
    && optionalInteger(value.index, false)
    && optionalNumber(value.strength, false);
}
function optionalInteger(value: unknown, nullable: boolean): boolean {
  return value === undefined || (nullable && value === null) || strictInteger(value);
}
function optionalNumber(value: unknown, nullable: boolean): boolean {
  return value === undefined || (nullable && value === null) || strictNumber(value);
}
function nullableAliasesAgree(...values: unknown[]): boolean {
  const present = values.filter((value) => value !== undefined && value !== null);
  return present.length < 2 || present.every((value) => value === present[0]);
}
function nonNullAliasesAgree(...values: unknown[]): boolean {
  const present = values.filter((value) => value !== undefined);
  return !present.includes(null) && (present.length < 2 || present.every((value) => value === present[0]));
}
function optionalString(value: unknown): boolean { return value === undefined || (typeof value === "string" && value.length > 0); }
function optionalNullableString(value: unknown): boolean { return value === undefined || value === null || (typeof value === "string" && value.length > 0); }
function optionalBoolean(value: unknown): boolean { return value === undefined || typeof value === "boolean"; }
function firstStrictNumber(...values: unknown[]): boolean {
  const value = values.find((item) => item !== undefined && item !== null);
  return strictNumber(value);
}
function strictNumber(value: unknown): value is number { return typeof value === "number" && Number.isFinite(value); }
function strictInteger(value: unknown): value is number { return strictNumber(value) && Number.isInteger(value); }
function isRecord(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null; }

export const __CHAN_OVERLAY_MANAGER_TESTING__ = { mergeOverlays, rangeCovered, missingRanges, pruneOverlay, clipRanges };
