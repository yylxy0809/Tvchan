import test from "node:test";
import assert from "node:assert/strict";

import type { ApiBar, ChanOverlayResponse } from "../api/client";
import {
  __CHAN_STUDY_TESTING__,
  clearChanStudyOverlay,
  setChanStudyOverlay,
} from "./chanStudy";
import {
  __CHAN_WIDGET_RENDER_TESTING__,
  __CHAN_WIDGET_TESTING__,
  renderChanOverlay,
} from "./widget";
import { createDefaultChanOverlaySettings } from "./overlaySettings";
import { studyInputItemsFromSettings } from "./chanStudySettings";

const {
  applyPivotBreak,
  buildBspMapForView,
  buildChannelMapForView,
  buildStrokePointsForView,
  findPivotOverlap,
  lineToRawStroke,
  mapChanTimeToViewTime,
  projectChanPointToViewTime,
} = __CHAN_STUDY_TESTING__;

function ms(value: string): number {
  return Date.parse(value);
}

test("maps a canonical 5f point onto the containing 30f bar end time", () => {
  const viewBarTimes = [
    ms("2026-06-10T10:00:00.000Z"),
    ms("2026-06-10T10:30:00.000Z"),
    ms("2026-06-10T11:00:00.000Z"),
  ];

  const mapped = mapChanTimeToViewTime(
    ms("2026-06-10T10:07:00.000Z"),
    "30",
    viewBarTimes,
  );

  assert.equal(mapped, ms("2026-06-10T10:30:00.000Z"));
});

test("stroke raw cache ignores conflicting legacy flat endpoint fields", () => {
  const raw = lineToRawStroke({
    id: "stroke-a",
    level: "5f",
    mode: "confirmed",
    direction: "up",
    confirmed: true,
    start: {
      time: 100,
      base_ts: 100,
      price: 10,
    },
    end: {
      time: 400,
      base_ts: 400,
      price: 12,
    },
    begin_base_ts: 100,
    end_base_ts: 400,
    start_time: 200,
    start_price: 20,
    end_time: 500,
    end_price: 25,
  } as never);

  assert.deepEqual(raw, {
    seq: 0,
    startTime: 100_000,
    startPrice: 10,
    endTime: 400_000,
    endPrice: 12,
    direction: "up",
  });
});

test("shared stroke endpoint keeps the later stroke start semantics", () => {
  const shared = ms("2026-06-10T10:30:00.000Z");
  const visible = [shared];
  const { map } = buildStrokePointsForView(
    [
      {
        seq: 1,
        startTime: ms("2026-06-10T10:00:00.000Z"),
        startPrice: 10.0,
        endTime: shared,
        endPrice: 10.8,
        direction: "up",
      },
      {
        seq: 2,
        startTime: shared,
        startPrice: 10.8,
        endTime: ms("2026-06-10T11:00:00.000Z"),
        endPrice: 10.2,
        direction: "down",
      },
    ],
    "30",
    visible,
  );

  assert.deepEqual(map.get(shared), {
    price: 10.8,
    dir: "down",
  });
});

test("center lookup requires full visible-bar containment and injects a break on center switch", () => {
  const pivots = [
    {
      key: "pivot-a",
      startTime: 100,
      endTime: 200,
      high: 12,
      low: 10,
    },
    {
      key: "pivot-b",
      startTime: 260,
      endTime: 340,
      high: 13,
      low: 11,
    },
  ];
  const starts = pivots.map((item) => item.startTime);
  const context: { _pivotBreakState?: Record<string, { key: string | null; skip: number }> } = {};

  assert.equal(findPivotOverlap(pivots, starts, 90, 110), null);

  const firstHit = findPivotOverlap(pivots, starts, 110, 120);
  assert.equal(firstHit?.key, "pivot-a");
  assert.equal(applyPivotBreak("5f", firstHit, context as never), null);
  assert.equal(applyPivotBreak("5f", firstHit, context as never), null);
  assert.equal(applyPivotBreak("5f", firstHit, context as never)?.key, "pivot-a");

  const secondHit = findPivotOverlap(pivots, starts, 280, 300);
  assert.equal(secondHit?.key, "pivot-b");
  assert.equal(applyPivotBreak("5f", secondHit, context as never), null);
  assert.equal(applyPivotBreak("5f", secondHit, context as never), null);
  assert.equal(applyPivotBreak("5f", secondHit, context as never)?.key, "pivot-b");
});

test("buy and sell signal projection keeps side separation on the visible bar timeline", () => {
  const visible = [
    ms("2026-06-10T10:30:00.000Z"),
    ms("2026-06-10T11:00:00.000Z"),
  ];
  const signals = [
    {
      time: ms("2026-06-10T10:07:00.000Z"),
      price: 10.1,
      bspType: "1",
      side: "buy" as const,
    },
    {
      time: ms("2026-06-10T10:37:00.000Z"),
      price: 10.9,
      bspType: "2",
      side: "sell" as const,
    },
  ];

  const buy = buildBspMapForView(signals, "buy", "30", visible);
  const sell = buildBspMapForView(signals, "sell", "30", visible);

  assert.deepEqual([...buy.entries()], [
    [
      ms("2026-06-10T10:30:00.000Z"),
      { price: 10.1, bspType: "1" },
    ],
  ]);
  assert.deepEqual([...sell.entries()], [
    [
      ms("2026-06-10T11:00:00.000Z"),
      { price: 10.9, bspType: "2" },
    ],
  ]);
});

test("falls back to A-share session-aware intraday bar end times when visible bars are unavailable", () => {
  const mappedMorning = mapChanTimeToViewTime(
    ms("2026-06-08T02:10:00.000Z"),
    "30",
    [],
  );
  const mappedAfternoon = mapChanTimeToViewTime(
    ms("2026-06-08T05:35:00.000Z"),
    "60",
    [],
  );

  assert.equal(mappedMorning, ms("2026-06-08T02:30:00.000Z"));
  assert.equal(mappedAfternoon, ms("2026-06-08T06:00:00.000Z"));
});

test("higher-level endpoint projection uses the last equal-price chart extreme", () => {
  const bars = [
    { time: 100, open: 9, high: 10, low: 8, close: 9, volume: 1, amount: null, complete: true, revision: 1 },
    { time: 200, open: 9, high: 10, low: 8, close: 9, volume: 1, amount: null, complete: true, revision: 1 },
    { time: 300, open: 9, high: 9, low: 8, close: 9, volume: 1, amount: null, complete: true, revision: 1 },
  ];
  assert.equal(projectChanPointToViewTime(300_000, 10, "30f", "5", bars.map((bar) => bar.time * 1000), bars), 200_000);
});

test("daily bar-end projection on 30f never searches into the following day", () => {
  const bars = [
    { time: 86_000, open: 9, high: 10, low: 8, close: 9, volume: 1, amount: null, complete: true, revision: 1 },
    { time: 86_200, open: 9, high: 10, low: 8, close: 9, volume: 1, amount: null, complete: true, revision: 1 },
    { time: 87_000, open: 9, high: 10, low: 8, close: 9, volume: 1, amount: null, complete: true, revision: 1 },
  ];
  assert.equal(projectChanPointToViewTime(86_400_000, 10, "1d", "30", bars.map((bar) => bar.time * 1000), bars), 86_200_000);
});

test("fallback monthly projection uses calendar bounds and the last equal-price extreme", () => {
  const seconds = (value: string) => Date.parse(value) / 1000;
  const bars: ApiBar[] = [
    apiBar(seconds("2026-01-31T07:00:00Z"), 12, 8),
    apiBar(seconds("2026-02-10T07:00:00Z"), 12, 8),
    apiBar(seconds("2026-02-20T07:00:00Z"), 12, 7),
    apiBar(seconds("2026-03-01T07:00:00Z"), 12, 6),
  ];
  const projected = __CHAN_WIDGET_TESTING__.projectOverlayPointToChartTime(
    seconds("2026-02-28T07:00:00Z"),
    12,
    "1m",
    "5f",
    bars,
  );
  assert.equal(projected, seconds("2026-02-20T07:00:00Z"));
});

test("fallback projection requires the native interval extreme and keeps same-level time exact", () => {
  const bars = [apiBar(100, 10, 8), apiBar(200, 11, 8), apiBar(300, 10, 7)];
  assert.equal(
    __CHAN_WIDGET_TESTING__.projectOverlayPointToChartTime(300, 10, "30f", "5f", bars),
    300,
  );
  assert.equal(
    __CHAN_WIDGET_TESTING__.projectOverlayPointToChartTime(300, 10, "5f", "5f", bars),
    300,
  );
});

test("incremental overlay update preserves untouched level caches by reference", () => {
  clearChanStudyOverlay();
  const bars = [apiBar(100, 12, 8), apiBar(200, 13, 9)];
  const initial = studyOverlay(12);
  setChanStudyOverlay(initial, bars);
  const before = __CHAN_STUDY_TESTING__.getActiveStudyState();
  const untouchedRaw = before.rawLevels["30f"];
  const untouchedCache = before.levels["30f"];

  setChanStudyOverlay(studyOverlay(12.5), bars);
  const after = __CHAN_STUDY_TESTING__.getActiveStudyState();
  assert.notEqual(after.levels["5f"], before.levels["5f"]);
  assert.equal(after.rawLevels["30f"], untouchedRaw);
  assert.equal(after.levels["30f"], untouchedCache);
  clearChanStudyOverlay();
});

test("Shanghai calendar projection handles UTC day, week, and month rollovers", () => {
  const project = (end: string, level: string, bars: ApiBar[]) => projectChanPointToViewTime(
    Date.parse(end),
    12,
    level,
    "30",
    bars.map((bar) => bar.time * 1000),
    bars,
  );
  const bar = (value: string, high = 12) => apiBar(Date.parse(value) / 1000, high, 8);

  assert.equal(project("2026-06-10T16:00:00Z", "1d", [bar("2026-06-10T07:00:00Z"), bar("2026-06-10T17:00:00Z")]), Date.parse("2026-06-10T07:00:00Z"));
  assert.equal(project("2026-06-14T16:00:00Z", "1w", [bar("2026-06-12T07:00:00Z"), bar("2026-06-15T07:00:00Z")]), Date.parse("2026-06-12T07:00:00Z"));
  assert.equal(project("2026-06-30T16:00:00Z", "1m", [bar("2026-06-29T07:00:00Z"), bar("2026-07-01T07:00:00Z")]), Date.parse("2026-06-29T07:00:00Z"));
});

test("D/W/M missing-extreme, signal, and channel fallbacks use Shanghai calendar keys", () => {
  const point = Date.parse("2026-06-10T17:00:00Z");
  const bars = [apiBar(point / 1000, 11, 9)];
  const expectedDay = Date.UTC(2026, 5, 11);
  const expectedWeek = Date.UTC(2026, 5, 8);
  const expectedMonth = Date.UTC(2026, 5, 1);

  assert.equal(projectChanPointToViewTime(point, 12, "1w", "D", [], bars), expectedDay);
  assert.equal(projectChanPointToViewTime(point, 12, "1m", "W", [], bars), expectedWeek);
  assert.equal(projectChanPointToViewTime(point, 12, "1w", "M", [], bars), expectedMonth);

  const signalMap = buildBspMapForView([{ time: point, price: 12, bspType: "1", side: "buy" }], "buy", "D", []);
  assert.deepEqual([...signalMap.keys()], [expectedDay]);
  const channelMap = buildChannelMapForView([{ time: point, upper: 13, lower: 8 }], "W", []);
  assert.deepEqual(channelMap.times, [expectedWeek]);
});

test("same stable drawing ID with changed endpoint content replaces the fallback object", async () => {
  const removed: Array<string | number> = [];
  let created = 0;
  const chart = {
    createMultipointShape: async () => `shape-${++created}`,
    removeEntity: (id: string | number) => removed.push(id),
  };
  const widget = {};
  const settings = createDefaultChanOverlaySettings();
  const first = studyOverlay(12).strokes[0];
  const changed = { ...first, end: { ...first.end, price: 12.5 } };
  await __CHAN_WIDGET_RENDER_TESTING__.reconcileStrokeDrawings(widget as never, chart as never, [first], settings, "5f", [], () => true);
  await __CHAN_WIDGET_RENDER_TESTING__.reconcileStrokeDrawings(widget as never, chart as never, [changed], settings, "5f", [], () => true);
  assert.equal(created, 2);
  assert.deepEqual(removed, ["shape-1"]);
});

test("mid-render stale fence leaves Pine state and study objects untouched", async () => {
  const originalWindow = globalThis.window;
  Object.assign(globalThis, { window: globalThis });
  try {
    let ready: (() => void) | undefined;
    let creates = 0;
    let current = true;
    const chart = { createStudy: () => { creates += 1; return "study"; } };
    const widget = {
      onChartReady: (callback: () => void) => { ready = callback; },
      activeChart: () => chart,
    };
    const before = __CHAN_STUDY_TESTING__.getActiveStudyState();
    const rendering = __CHAN_WIDGET_RENDER_TESTING__.renderChanStudy(
      widget as never,
      studyOverlay(12),
      createDefaultChanOverlaySettings(),
      [],
      () => current,
    );
    await Promise.resolve();
    current = false;
    ready?.();
    assert.equal(await rendering, false);
    assert.equal(creates, 0);
    assert.equal(__CHAN_STUDY_TESTING__.getActiveStudyState(), before);
  } finally {
    Object.assign(globalThis, { window: originalWindow });
  }
});

test("a changed overlay dataset recreates the Pine study to recalculate plots", async () => {
  const originalWindow = globalThis.window;
  const originalSetInterval = globalThis.setInterval;
  const originalClearInterval = globalThis.clearInterval;
  const activeStudies = new Set<string>();
  const removed: string[] = [];
  let creates = 0;
  const settings = createDefaultChanOverlaySettings();
  const study = {
    applyOverrides: () => {},
    getInputValues: () => studyInputItemsFromSettings(settings),
    setUserEditEnabled: () => {},
  };
  const chart = {
    createStudy: async () => {
      const id = `study-${++creates}`;
      activeStudies.add(id);
      return id;
    },
    getStudyById: (id: string) => {
      if (!activeStudies.has(id)) throw new Error("study missing");
      return study;
    },
    removeEntity: (id: string) => {
      removed.push(id);
      activeStudies.delete(id);
    },
  };
  const widget = {
    activeChart: () => chart,
    onChartReady: (callback: () => void) => callback(),
  };
  Object.assign(globalThis, { window: globalThis });
  globalThis.setInterval = (() => 1) as unknown as typeof globalThis.setInterval;
  globalThis.clearInterval = (() => {}) as unknown as typeof globalThis.clearInterval;
  try {
    assert.equal(await __CHAN_WIDGET_RENDER_TESTING__.renderChanStudy(
      widget as never, studyOverlay(12), settings, [],
    ), true);
    assert.equal(await __CHAN_WIDGET_RENDER_TESTING__.renderChanStudy(
      widget as never, studyOverlay(13), settings, [],
    ), true);
    assert.equal(creates, 2);
    assert.deepEqual(removed, ["study-1"]);
  } finally {
    Object.assign(globalThis, {
      window: originalWindow,
      setInterval: originalSetInterval,
      clearInterval: originalClearInterval,
    });
  }
});

test("activeChart probe treats a usable chart as ready when TradingView omits onChartReady", async () => {
  const originalWindow = globalThis.window;
  Object.assign(globalThis, { window: globalThis });
  try {
    const widget = {
      activeChart: () => ({}),
      onChartReady: () => {},
    };
    assert.equal(
      await __CHAN_WIDGET_RENDER_TESTING__.whenChartReady(widget as never, 20),
      true,
    );
  } finally {
    Object.assign(globalThis, { window: originalWindow });
  }
});

test("malformed overlay is handled before chart mutation and retains current drawings", async () => {
  const originalWindow = globalThis.window;
  Object.assign(globalThis, { window: globalThis });
  try {
    let chartAccesses = 0;
    const widget = { activeChart: () => { chartAccesses += 1; throw new Error("must not render"); } };
    const malformed = { ...studyOverlay(12), strokes: [{ id: "bad" }] };
    const validationError = __CHAN_WIDGET_RENDER_TESTING__.validateChanOverlay(malformed);
    assert.notEqual(validationError, null);
    assert.match(validationError!, /line collection/);
    await assert.doesNotReject(renderChanOverlay(widget as never, malformed as never));
    assert.equal(chartAccesses, 0);
  } finally {
    Object.assign(globalThis, { window: originalWindow });
  }
});

function apiBar(time: number, high: number, low: number): ApiBar {
  return { time, open: low, high, low, close: high, volume: 1, amount: null, complete: true, revision: 1 };
}

function studyOverlay(fiveMinuteEndPrice: number): ChanOverlayResponse {
  const stroke = (id: string, level: string, endPrice: number) => ({
    id,
    level,
    mode: "confirmed",
    start: { time: 100, base_ts: 100, price: 10 },
    end: { time: 200, base_ts: 200, price: endPrice },
    direction: "up",
    confirmed: true,
  });
  return {
    symbol: "000001.SZ",
    chart_timeframe: "5f",
    levels: ["5f", "30f"],
    modes: ["confirmed"],
    snapshot_version: String(fiveMinuteEndPrice),
    base_timeframe: "5f",
    base_ts_semantics: "bar_end",
    engine: "test",
    requested_bar_count: 2,
    bars_by_level: { "5f": 2, "30f": 2 },
    strokes: [stroke("5", "5f", fiveMinuteEndPrice), stroke("30", "30f", 13)],
    segments: [],
    centers: [],
    signals: [],
    channels: [],
  };
}
