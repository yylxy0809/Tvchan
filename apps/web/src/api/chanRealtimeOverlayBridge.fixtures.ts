import type {
  ChanOverlayEvent,
  ChanRealtimeEnvelope,
  ChanResyncRequiredEvent,
} from "./chanRealtimeOverlayBridge";

const emptyDeletes = {
  strokes: [],
  segments: [],
  centers: [],
  signals: [],
  channels: [],
} as const;

export const CHAN_SNAPSHOT_FIXTURE = {
  type: "chan_overlay",
  schema_version: "chan-event.v1",
  kind: "snapshot",
  id: "active-chart",
  symbol: "000001.SZ",
  chart_timeframe: "5f",
  modes: ["confirmed", "predictive"],
  snapshot_version: "000001.SZ:module-c:5f:confirmed:77:committed-v77",
  base_version: null,
  sequence: 1,
  range: { from: 1_780_000_000, to: 1_780_001_200 },
  upserts: {
    strokes: [
      {
        id: "stroke-a",
        level: "5f",
        mode: "confirmed",
        start: { time: 1_780_000_000, price: 10 },
        end: { time: 1_780_000_300, price: 10.5 },
        direction: "up",
        confirmed: true,
      },
    ],
    segments: [],
    centers: [],
    signals: [],
    channels: [
      { id: "channel-a", level: "5f", mode: "confirmed", time: 1_780_000_300, upper: 11, lower: 9, confirmed: true },
    ],
  },
  deletes: emptyDeletes,
} as const satisfies ChanOverlayEvent;

export const CHAN_DELTA_FIXTURE = {
  type: "chan_overlay",
  schema_version: "chan-event.v1",
  kind: "delta",
  id: "active-chart",
  symbol: "000001.SZ",
  chart_timeframe: "5f",
  modes: ["confirmed", "predictive"],
  snapshot_version: "000001.SZ:module-c:5f:confirmed:78:committed-v78",
  base_version: CHAN_SNAPSHOT_FIXTURE.snapshot_version,
  sequence: 2,
  range: CHAN_SNAPSHOT_FIXTURE.range,
  upserts: {
    strokes: [
      {
        id: "stroke-b",
        level: "5f",
        mode: "confirmed",
        start: { time: 1_780_000_300, price: 10.5 },
        end: { time: 1_780_000_600, price: 10.2 },
        direction: "down",
        confirmed: true,
      },
    ],
    segments: [],
    centers: [],
    signals: [],
    channels: [
      { id: "channel-b", level: "5f", mode: "confirmed", time: 1_780_000_600, upper: 12, lower: 10, confirmed: true },
    ],
  },
  deletes: {
    ...emptyDeletes,
    strokes: ["stroke-a"],
    channels: ["channel-a"],
  },
} as const satisfies ChanOverlayEvent;

export const CHAN_EMPTY_DELTA_FIXTURE = {
  ...CHAN_DELTA_FIXTURE,
  upserts: {
    strokes: [],
    segments: [],
    centers: [],
    signals: [],
    channels: [],
  },
  deletes: emptyDeletes,
} as const satisfies ChanOverlayEvent;

export const CHAN_RESYNC_REQUIRED_FIXTURE = {
  type: "chan_resync_required",
  schema_version: "chan-event.v1",
  id: "active-chart",
  symbol: "000001.SZ",
  chart_timeframe: "5f",
  modes: ["confirmed", "predictive"],
  sequence: 3,
  range: CHAN_SNAPSHOT_FIXTURE.range,
  reason: "source_sequence_gap",
  source_event_id: "000001.SZ:5f:confirmed:80:committed-v80",
  source_sequence: 5,
} as const satisfies ChanResyncRequiredEvent;

export const CHAN_REALTIME_ROUND_TRIP_FIXTURES: readonly ChanRealtimeEnvelope[] = [
  CHAN_SNAPSHOT_FIXTURE,
  CHAN_DELTA_FIXTURE,
  CHAN_RESYNC_REQUIRED_FIXTURE,
];

export const MALFORMED_EVENT_FIXTURES: readonly unknown[] = [
  null,
  [],
  "chan_overlay",
  7,
  {},
  { ...CHAN_SNAPSHOT_FIXTURE, type: 7 },
  { ...CHAN_SNAPSHOT_FIXTURE, symbol: null },
  { ...CHAN_SNAPSHOT_FIXTURE, chart_timeframe: [] },
  { ...CHAN_SNAPSHOT_FIXTURE, modes: "confirmed" },
  { ...CHAN_SNAPSHOT_FIXTURE, modes: ["confirmed", 7] },
  { ...CHAN_SNAPSHOT_FIXTURE, modes: ["confirmed", "confirmed"] },
  { ...CHAN_SNAPSHOT_FIXTURE, sequence: "999" },
  { ...CHAN_SNAPSHOT_FIXTURE, snapshot_version: 7 },
  { ...CHAN_SNAPSHOT_FIXTURE, range: null },
  { ...CHAN_SNAPSHOT_FIXTURE, range: { from: 20, to: 10 } },
  { ...CHAN_SNAPSHOT_FIXTURE, upserts: null },
  { ...CHAN_SNAPSHOT_FIXTURE, upserts: [] },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    upserts: { strokes: [], segments: [], centers: [] },
  },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    upserts: {
      ...CHAN_SNAPSHOT_FIXTURE.upserts,
      extras: [],
    },
  },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    upserts: { ...CHAN_SNAPSHOT_FIXTURE.upserts, strokes: {} },
  },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    upserts: { ...CHAN_SNAPSHOT_FIXTURE.upserts, strokes: [null] },
  },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    upserts: { ...CHAN_SNAPSHOT_FIXTURE.upserts, strokes: [{ id: 7 }] },
  },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    upserts: {
      ...CHAN_SNAPSHOT_FIXTURE.upserts,
      strokes: [{ id: "duplicate" }, { id: "duplicate" }],
    },
  },
  { ...CHAN_SNAPSHOT_FIXTURE, deletes: null },
  {
    ...CHAN_SNAPSHOT_FIXTURE,
    deletes: { ...CHAN_SNAPSHOT_FIXTURE.deletes, strokes: [7] },
  },
  { ...CHAN_DELTA_FIXTURE, base_version: null, sequence: 999 },
  CHAN_EMPTY_DELTA_FIXTURE,
  {
    ...CHAN_EMPTY_DELTA_FIXTURE,
    upserts: { strokes: [], segments: [], centers: [] },
  },
  {
    ...CHAN_EMPTY_DELTA_FIXTURE,
    deletes: { strokes: [], segments: [], signals: [] },
  },
  {
    ...CHAN_DELTA_FIXTURE,
    base_version: CHAN_DELTA_FIXTURE.snapshot_version,
    sequence: 999,
  },
  {
    ...CHAN_DELTA_FIXTURE,
    deletes: { ...CHAN_DELTA_FIXTURE.deletes, centers: {} },
    sequence: 999,
  },
  { ...CHAN_RESYNC_REQUIRED_FIXTURE, reason: "unknown_gap", sequence: 999 },
  { ...CHAN_RESYNC_REQUIRED_FIXTURE, source_event_id: null, sequence: 999 },
  { ...CHAN_RESYNC_REQUIRED_FIXTURE, source_sequence: "5", sequence: 999 },
  { ...CHAN_RESYNC_REQUIRED_FIXTURE, extra: true, sequence: 999 },
];
