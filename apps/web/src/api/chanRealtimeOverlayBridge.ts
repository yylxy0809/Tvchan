export const CHAN_REALTIME_SCHEMA_VERSION = "chan-event.v1" as const;

export type ChanRealtimeRange = Readonly<{ from: number; to: number }>;
export type ChanRealtimeObject = Readonly<{ id: string; [field: string]: unknown }>;
export type ChanRealtimeGroup = "strokes" | "segments" | "centers" | "signals" | "channels";

export type ChanRealtimeObjects = Readonly<
  Record<ChanRealtimeGroup, readonly ChanRealtimeObject[]>
>;
export type ChanRealtimeDeletes = Readonly<
  Record<ChanRealtimeGroup, readonly string[]>
>;

type ChanOverlayEventBase = Readonly<{
  type: "chan_overlay";
  schema_version: typeof CHAN_REALTIME_SCHEMA_VERSION;
  id: string;
  symbol: string;
  chart_timeframe: string;
  modes: readonly string[];
  snapshot_version: string;
  sequence: number;
  range: ChanRealtimeRange;
  upserts: ChanRealtimeObjects;
  deletes: ChanRealtimeDeletes;
}>;

export type ChanOverlaySnapshotEvent = ChanOverlayEventBase &
  Readonly<{ kind: "snapshot"; base_version: null }>;

export type ChanOverlayDeltaEvent = ChanOverlayEventBase &
  Readonly<{ kind: "delta"; base_version: string }>;

export type ChanOverlayEvent =
  | ChanOverlaySnapshotEvent
  | ChanOverlayDeltaEvent;

export type ChanResyncRequiredEvent = Readonly<{
  type: "chan_resync_required";
  schema_version: typeof CHAN_REALTIME_SCHEMA_VERSION;
  id: string;
  symbol: string;
  chart_timeframe: string;
  modes: readonly string[];
  sequence: number;
  range: ChanRealtimeRange;
  reason: "source_sequence_gap" | "source_version_mismatch";
  source_event_id: string;
  source_sequence: number;
}>;

export type ChanRealtimeEnvelope = ChanOverlayEvent | ChanResyncRequiredEvent;
export type ChanRealtimeTransportStatus = "connected" | "disconnected" | "replayed";

export type ChanRealtimeContext = Readonly<{
  symbol: string;
  chartTimeframe: string;
  modes: readonly string[];
}>;

export type ChanHttpOverlayHydration = ChanRealtimeContext &
  Readonly<{
    snapshotVersion: string;
    range: ChanRealtimeRange;
    objects: ChanRealtimeObjects;
  }>;

export type ChanRealtimeOverlayState = Readonly<{
  key: string;
  symbol: string;
  chartTimeframe: string;
  modes: readonly string[];
  snapshotVersion: string;
  sequence: number;
  range: ChanRealtimeRange;
  objects: ChanRealtimeObjects;
}>;

export type ChanHttpResyncInstruction = Readonly<{
  type: "http_overlay_resync";
  key: string;
  symbol: string;
  chartTimeframe: string;
  modes: readonly string[];
  range: ChanRealtimeRange;
  reason: "sequence_gap" | "base_version_mismatch" | "server_resync_required";
}>;

export type ChanRealtimeApplyResult =
  | Readonly<{ status: "applied"; state: ChanRealtimeOverlayState }>
  | Readonly<{
      status: "ignored";
      reason: "duplicate_or_older" | "resync_pending" | "malformed";
      state?: ChanRealtimeOverlayState;
    }>
  | Readonly<{
      status: "resync";
      instruction: ChanHttpResyncInstruction;
      state?: ChanRealtimeOverlayState;
    }>;

type MutableObjects = Record<ChanRealtimeGroup, Map<string, ChanRealtimeObject>>;
type Entry = {
  key: string;
  symbol: string;
  chartTimeframe: string;
  modes: string[];
  lastSeenSequence: number;
  snapshotVersion?: string;
  committedSequence?: number;
  range?: ChanRealtimeRange;
  objects: MutableObjects;
  resyncPending: boolean;
};

const GROUPS: readonly ChanRealtimeGroup[] = [
  "strokes",
  "segments",
  "centers",
  "signals",
  "channels",
];
const MODES = new Set(["confirmed", "predictive"]);
const OVERLAY_KEYS = [
  "type",
  "schema_version",
  "kind",
  "id",
  "symbol",
  "chart_timeframe",
  "modes",
  "snapshot_version",
  "base_version",
  "sequence",
  "range",
  "upserts",
  "deletes",
] as const;
const RESYNC_KEYS = [
  "type",
  "schema_version",
  "id",
  "symbol",
  "chart_timeframe",
  "modes",
  "sequence",
  "range",
  "reason",
  "source_event_id",
  "source_sequence",
] as const;

export class ChanRealtimeOverlayBridge {
  private readonly entries = new Map<string, Entry>();

  hydrateHttp(hydration: ChanHttpOverlayHydration): ChanRealtimeOverlayState {
    const key = contextKey(hydration);
    const existing = this.entries.get(key);
    const entry: Entry = existing ?? {
      key,
      symbol: hydration.symbol.toUpperCase(),
      chartTimeframe: hydration.chartTimeframe,
      modes: normalizeModes(hydration.modes),
      lastSeenSequence: 0,
      objects: emptyObjects(),
      resyncPending: false,
    };
    entry.objects = objectsFromSnapshot(hydration.objects, emptyDeletes());
    entry.snapshotVersion = hydration.snapshotVersion;
    entry.committedSequence = entry.lastSeenSequence;
    entry.range = cloneRange(hydration.range);
    entry.resyncPending = false;
    this.entries.set(key, entry);
    return stateFromEntry(entry);
  }

  resetTransportEpoch(context: ChanRealtimeContext): ChanRealtimeOverlayState | undefined {
    const entry = this.entries.get(contextKey(context));
    if (!entry) return undefined;
    entry.lastSeenSequence = 0;
    entry.committedSequence = 0;
    entry.resyncPending = false;
    return entry.snapshotVersion === undefined ? undefined : stateFromEntry(entry);
  }

  apply(value: unknown): ChanRealtimeApplyResult {
    const event = readEnvelope(value);
    if (event === undefined) {
      return { status: "ignored", reason: "malformed" };
    }
    const entry = this.entryFor(event);
    if (event.sequence <= entry.lastSeenSequence) {
      return this.ignored(entry, "duplicate_or_older");
    }

    if (
      entry.lastSeenSequence > 0 &&
      event.sequence !== entry.lastSeenSequence + 1
    ) {
      entry.lastSeenSequence = event.sequence;
      return this.requestResync(entry, event.range, "sequence_gap");
    }
    entry.lastSeenSequence = event.sequence;

    if (event.type === "chan_resync_required") {
      return this.requestResync(entry, event.range, "server_resync_required");
    }

    if (event.kind === "snapshot") {
      entry.objects = objectsFromSnapshot(event.upserts, event.deletes);
      entry.snapshotVersion = event.snapshot_version;
      entry.committedSequence = event.sequence;
      entry.range = cloneRange(event.range);
      entry.resyncPending = false;
      return { status: "applied", state: stateFromEntry(entry) };
    }

    if (entry.resyncPending) {
      return this.ignored(entry, "resync_pending");
    }
    if (
      entry.snapshotVersion === undefined ||
      event.base_version !== entry.snapshotVersion
    ) {
      return this.requestResync(entry, event.range, "base_version_mismatch");
    }

    applyChanges(entry.objects, event.upserts, event.deletes);
    entry.snapshotVersion = event.snapshot_version;
    entry.committedSequence = event.sequence;
    entry.range = cloneRange(event.range);
    return { status: "applied", state: stateFromEntry(entry) };
  }

  getState(context: ChanRealtimeContext): ChanRealtimeOverlayState | undefined {
    const entry = this.entries.get(contextKey(context));
    return entry?.snapshotVersion === undefined ? undefined : stateFromEntry(entry);
  }

  unsubscribe(context: ChanRealtimeContext): boolean {
    return this.entries.delete(contextKey(context));
  }

  reset(): void {
    this.entries.clear();
  }

  private entryFor(event: ChanRealtimeEnvelope): Entry {
    const context = contextFromEvent(event);
    const key = contextKey(context);
    const existing = this.entries.get(key);
    if (existing) return existing;
    const entry: Entry = {
      key,
      symbol: context.symbol.toUpperCase(),
      chartTimeframe: context.chartTimeframe,
      modes: normalizeModes(context.modes),
      lastSeenSequence: 0,
      objects: emptyObjects(),
      resyncPending: false,
    };
    this.entries.set(key, entry);
    return entry;
  }

  private requestResync(
    entry: Entry,
    range: ChanRealtimeRange,
    reason: ChanHttpResyncInstruction["reason"],
  ): ChanRealtimeApplyResult {
    if (entry.resyncPending) return this.ignored(entry, "resync_pending");
    entry.resyncPending = true;
    return {
      status: "resync",
      instruction: {
        type: "http_overlay_resync",
        key: entry.key,
        symbol: entry.symbol,
        chartTimeframe: entry.chartTimeframe,
        modes: [...entry.modes],
        range: cloneRange(range),
        reason,
      },
      state:
        entry.snapshotVersion === undefined ? undefined : stateFromEntry(entry),
    };
  }

  private ignored(
    entry: Entry,
    reason: "duplicate_or_older" | "resync_pending",
  ): ChanRealtimeApplyResult {
    return {
      status: "ignored",
      reason,
      state:
        entry.snapshotVersion === undefined ? undefined : stateFromEntry(entry),
    };
  }
}

export class ChanRealtimePollingGate {
  private timer: unknown;

  constructor(
    private readonly poll: () => void,
    private readonly intervalMs = 3_000,
    private readonly schedule: (callback: () => void, delay: number) => unknown =
      (callback, delay) => globalThis.setInterval(callback, delay),
    private readonly cancel: (timer: unknown) => void =
      (timer) => globalThis.clearInterval(timer as number),
  ) {}

  update(status: ChanRealtimeTransportStatus): void {
    if (status !== "disconnected") {
      this.stop();
      return;
    }
    if (this.timer !== undefined) return;
    this.timer = this.schedule(this.poll, this.intervalMs);
  }

  dispose(): void {
    this.stop();
  }

  private stop(): void {
    if (this.timer === undefined) return;
    this.cancel(this.timer);
    this.timer = undefined;
  }
}

export function chanRealtimeContextKey(context: ChanRealtimeContext): string {
  return contextKey(context);
}

function contextFromEvent(event: ChanRealtimeEnvelope): ChanRealtimeContext {
  return {
    symbol: event.symbol,
    chartTimeframe: event.chart_timeframe,
    modes: event.modes,
  };
}

function contextKey(context: ChanRealtimeContext): string {
  return JSON.stringify([
    context.symbol.toUpperCase(),
    context.chartTimeframe,
    normalizeModes(context.modes),
  ]);
}

function normalizeModes(modes: readonly string[]): string[] {
  return [...new Set(modes)].sort();
}

function emptyObjects(): MutableObjects {
  return {
    strokes: new Map(),
    segments: new Map(),
    centers: new Map(),
    signals: new Map(),
    channels: new Map(),
  };
}

function emptyDeletes(): ChanRealtimeDeletes {
  return { strokes: [], segments: [], centers: [], signals: [], channels: [] };
}

function objectsFromSnapshot(
  upserts: ChanRealtimeObjects,
  deletes: ChanRealtimeDeletes,
): MutableObjects {
  const objects = emptyObjects();
  applyChanges(objects, upserts, deletes);
  return objects;
}

function applyChanges(
  objects: MutableObjects,
  upserts: ChanRealtimeObjects,
  deletes: ChanRealtimeDeletes,
): void {
  for (const group of GROUPS) {
    for (const item of upserts[group]) {
      objects[group].set(item.id, item);
    }
    for (const id of deletes[group]) objects[group].delete(id);
  }
}

function stateFromEntry(entry: Entry): ChanRealtimeOverlayState {
  if (
    entry.snapshotVersion === undefined ||
    entry.committedSequence === undefined ||
    entry.range === undefined
  ) {
    throw new Error("Chan realtime entry has no committed overlay");
  }
  return {
    key: entry.key,
    symbol: entry.symbol,
    chartTimeframe: entry.chartTimeframe,
    modes: [...entry.modes],
    snapshotVersion: entry.snapshotVersion,
    sequence: entry.committedSequence,
    range: cloneRange(entry.range),
    objects: {
      strokes: [...entry.objects.strokes.values()],
      segments: [...entry.objects.segments.values()],
      centers: [...entry.objects.centers.values()],
      signals: [...entry.objects.signals.values()],
      channels: [...entry.objects.channels.values()],
    },
  };
}

function cloneRange(range: ChanRealtimeRange): ChanRealtimeRange {
  return { from: range.from, to: range.to };
}

function readEnvelope(value: unknown): ChanRealtimeEnvelope | undefined {
  if (!isRecord(value)) return undefined;
  if (value.type === "chan_overlay") {
    return isOverlayEvent(value) ? value : undefined;
  }
  if (value.type === "chan_resync_required") {
    return isResyncEvent(value) ? value : undefined;
  }
  return undefined;
}

function isOverlayEvent(value: Record<string, unknown>): value is ChanOverlayEvent {
  const upserts = value.upserts;
  const deletes = value.deletes;
  if (
    !hasExactKeys(value, OVERLAY_KEYS) ||
    value.schema_version !== CHAN_REALTIME_SCHEMA_VERSION ||
    (value.kind !== "snapshot" && value.kind !== "delta") ||
    !hasValidContext(value) ||
    !isNonEmptyString(value.snapshot_version) ||
    !isPositiveInteger(value.sequence) ||
    !isRange(value.range) ||
    !isUpserts(upserts) ||
    !isDeletes(deletes)
  ) {
    return false;
  }
  if (value.kind === "snapshot") return value.base_version === null;
  return (
    isNonEmptyString(value.base_version) &&
    value.base_version !== value.snapshot_version &&
    hasRealChanges(upserts, deletes)
  );
}

function hasRealChanges(
  upserts: ChanRealtimeObjects,
  deletes: ChanRealtimeDeletes,
): boolean {
  return GROUPS.some(
    (group) => upserts[group].length > 0 || deletes[group].length > 0,
  );
}

function isResyncEvent(
  value: Record<string, unknown>,
): value is ChanResyncRequiredEvent {
  return (
    hasExactKeys(value, RESYNC_KEYS) &&
    value.schema_version === CHAN_REALTIME_SCHEMA_VERSION &&
    hasValidContext(value) &&
    isPositiveInteger(value.sequence) &&
    isRange(value.range) &&
    (value.reason === "source_sequence_gap" ||
      value.reason === "source_version_mismatch") &&
    isNonEmptyString(value.source_event_id) &&
    isPositiveInteger(value.source_sequence)
  );
}

function hasValidContext(value: Record<string, unknown>): boolean {
  return (
    isNonEmptyString(value.id) &&
    isNonEmptyString(value.symbol) &&
    isNonEmptyString(value.chart_timeframe) &&
    isModes(value.modes)
  );
}

function isModes(value: unknown): value is string[] {
  if (!Array.isArray(value) || value.length === 0) return false;
  if (
    !value.every((mode) => typeof mode === "string" && MODES.has(mode))
  ) {
    return false;
  }
  return new Set(value).size === value.length;
}

function isRange(value: unknown): value is ChanRealtimeRange {
  return (
    isRecord(value) &&
    hasExactKeys(value, ["from", "to"]) &&
    Number.isInteger(value.from) &&
    Number.isInteger(value.to) &&
    (value.from as number) <= (value.to as number)
  );
}

function isUpserts(value: unknown): value is ChanRealtimeObjects {
  if (!isChangeGroups(value)) return false;
  for (const group of GROUPS) {
    const items = value[group];
    if (!Array.isArray(items)) return false;
    const ids = new Set<string>();
    for (const item of items) {
      if (!isRecord(item) || !isNonEmptyString(item.id) || ids.has(item.id)) {
        return false;
      }
      ids.add(item.id);
    }
  }
  return true;
}

function isDeletes(value: unknown): value is ChanRealtimeDeletes {
  if (!isChangeGroups(value)) return false;
  for (const group of GROUPS) {
    const ids = value[group];
    if (
      !Array.isArray(ids) ||
      !ids.every(isNonEmptyString) ||
      new Set(ids).size !== ids.length
    ) {
      return false;
    }
  }
  return true;
}

function isChangeGroups(
  value: unknown,
): value is Record<ChanRealtimeGroup, unknown> {
  return isRecord(value) && hasExactKeys(value, GROUPS);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function isPositiveInteger(value: unknown): value is number {
  return Number.isInteger(value) && (value as number) > 0;
}

function hasExactKeys(
  value: Record<string, unknown>,
  expected: readonly string[],
): boolean {
  const keys = Object.keys(value);
  return (
    keys.length === expected.length &&
    expected.every((key) => Object.prototype.hasOwnProperty.call(value, key))
  );
}
