import assert from "node:assert/strict";
import test from "node:test";

import {
  ChanOverlayManager,
  __CHAN_OVERLAY_MANAGER_TESTING__,
  chanLevelsForTimeframe,
  validateChanOverlayResponse,
} from "./chanOverlayManager";
import type { ChanOverlayResponse } from "./client";
import { __CHAN_WIDGET_RENDER_TESTING__ } from "../tradingview/widget";
import { ChanRealtimeOverlayBridge } from "./chanRealtimeOverlayBridge";
import { CHAN_DELTA_FIXTURE } from "./chanRealtimeOverlayBridge.fixtures";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function overlay(id = "s1"): ChanOverlayResponse {
  return {
    symbol: "000001.SZ", chart_timeframe: "5f", levels: ["5f"], modes: ["confirmed"],
    snapshot_version: "v1", base_timeframe: "5f", base_ts_semantics: "bar_end", engine: "test",
    requested_bar_count: 1, bars_by_level: { "5f": 1 },
    strokes: [{ id, level: "5f", mode: "confirmed", start: { time: 1, price: 1 }, end: { time: 2, price: 2 }, direction: "up", confirmed: true }],
    segments: [], centers: [], signals: [], channels: [],
  };
}

test("uses only the backend display levels for each chart timeframe", () => {
  assert.deepEqual(chanLevelsForTimeframe("5f"), ["5f", "30f", "1d"]);
  assert.deepEqual(chanLevelsForTimeframe("15f"), ["5f", "30f", "1d"]);
  assert.deepEqual(chanLevelsForTimeframe("30f"), ["30f", "1d"]);
  assert.deepEqual(chanLevelsForTimeframe("1h"), ["30f", "1d"]);
  assert.deepEqual(chanLevelsForTimeframe("1d"), ["1d", "1w"]);
  assert.deepEqual(chanLevelsForTimeframe("1w"), ["1w", "1m"]);
  assert.deepEqual(chanLevelsForTimeframe("1m"), ["1m"]);
  assert.deepEqual(chanLevelsForTimeframe("bad"), []);
});

test("debounces rapid ranges and paints only the latest request", async () => {
  let calls = 0;
  const manager = new ChanOverlayManager(async () => { calls += 1; return overlay(); });
  const painted: string[] = [];
  for (let index = 0; index < 5; index += 1) {
    manager.request({ symbol: "000001.SZ", timeframe: "5f", from: index, to: index + 10, modes: ["confirmed"], onPaint: () => painted.push(String(index)), onError: () => assert.fail("unexpected error") });
  }
  await sleep(180);
  assert.equal(calls, 1);
  assert.deepEqual(painted, ["4"]);
  manager.dispose();
});

test("aborts stale symbol requests and reuses a covered cache range", async () => {
  let calls = 0;
  let firstResolve: ((value: ChanOverlayResponse) => void) | undefined;
  const manager = new ChanOverlayManager((_symbol, _timeframe, _limit, _from, _to, signal) => {
    calls += 1;
    if (calls === 1) return new Promise((resolve) => { firstResolve = resolve; signal?.addEventListener("abort", () => undefined); });
    return Promise.resolve(overlay(`s${calls}`));
  });
  const painted: string[] = [];
  const request = (symbol: string) => manager.request({ symbol, timeframe: "5f", from: 1, to: 10, modes: ["confirmed"], onPaint: (value) => painted.push(value.symbol), onError: () => assert.fail("unexpected error") });
  request("000001.SZ"); await sleep(160); request("000002.SZ"); await sleep(160);
  firstResolve?.(overlay("stale")); await sleep(10);
  assert.deepEqual(painted, ["000001.SZ"]);
  request("000002.SZ"); await sleep(160);
  assert.equal(calls, 2);
  manager.dispose();
});

test("merges overlapping responses by authoritative stable id", () => {
  const old = overlay("stable");
  old.strokes[0].end.price = 2;
  const next = overlay("stable");
  next.strokes[0].end.price = 3;
  next.strokes.push({ ...next.strokes[0], id: "later", start: { time: 3, price: 3 }, end: { time: 4, price: 4 } });
  const merged = __CHAN_OVERLAY_MANAGER_TESTING__.mergeOverlays(old, next);
  assert.equal(merged.strokes.length, 2);
  assert.equal(merged.strokes[0].end.price, 3);
});

test("realtime bridge state paints from cache without fetch and preserves HTTP metadata", () => {
  let fetches = 0;
  const manager = new ChanOverlayManager(async () => { fetches += 1; return overlay(); });
  const http = overlay("stroke-a");
  http.modes = ["confirmed", "predictive"];
  http.engine = "http-engine";
  http.strokes[0].start.time = CHAN_DELTA_FIXTURE.range.from;
  http.strokes[0].end.time = CHAN_DELTA_FIXTURE.range.from + 300;
  http.channels = [{ id: "channel-a", level: "5f", mode: "confirmed", time: CHAN_DELTA_FIXTURE.range.from + 300, upper: 11, lower: 9, confirmed: true }];
  manager.hydrateHttp(http, CHAN_DELTA_FIXTURE.range);
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.hydrateHttp({
    symbol: http.symbol,
    chartTimeframe: http.chart_timeframe,
    modes: http.modes,
    snapshotVersion: CHAN_DELTA_FIXTURE.base_version,
    range: CHAN_DELTA_FIXTURE.range,
    objects: { strokes: http.strokes, segments: [], centers: [], signals: [], channels: http.channels },
  });
  const result = bridge.apply(CHAN_DELTA_FIXTURE);
  assert.equal(result.status, "applied");
  if (result.status !== "applied") return;
  const painted = manager.applyRealtime(result.state);
  assert.equal(fetches, 0);
  assert.equal(painted?.engine, "http-engine");
  assert.deepEqual(painted?.channels.map((item) => item.id), ["channel-b"]);
  assert.deepEqual(painted?.strokes.map((item) => item.id), ["stroke-b"]);
});

test("fresh bounded overlay fetch bypasses manager coverage cache", async () => {
  let fetches = 0;
  const manager = new ChanOverlayManager(async () => {
    fetches += 1;
    const value = overlay(`fresh-${fetches}`);
    value.snapshot_version = `v${fetches}`;
    return value;
  });
  const request = { symbol: "000001.SZ", timeframe: "5f", from: 1, to: 10, modes: ["confirmed"] as const };
  await manager.fetchFresh({ ...request, modes: [...request.modes] });
  await manager.fetchFresh({ ...request, modes: [...request.modes] });
  assert.equal(fetches, 2);
});

test("authoritative window removes every omitted intersecting ID and retains wholly outside context", () => {
  const previous = overlay("inside");
  previous.strokes[0].start.time = 10;
  previous.strokes[0].end.time = 20;
  previous.strokes.push(
    { ...previous.strokes[0], id: "crossing", start: { time: 5, price: 1 }, end: { time: 15, price: 2 } },
    { ...previous.strokes[0], id: "outside", start: { time: 30, price: 1 }, end: { time: 40, price: 2 } },
  );
  previous.signals.push({ id: "deleted-signal", level: "5f", mode: "confirmed", time: 12, price: 1, signal_type: "1", confirmed: true });
  const incoming = overlay("replacement");
  incoming.strokes = [];
  incoming.signals = [];
  const merged = __CHAN_OVERLAY_MANAGER_TESTING__.mergeOverlays(previous, incoming, { from: 10, to: 20 });
  assert.deepEqual(merged.strokes.map((item) => item.id), ["outside"]);
  assert.equal(merged.signals.length, 0);
});

test("retention bounds disjoint history while preserving line boundary context", () => {
  const value = overlay("before");
  value.strokes.push(
    { ...value.strokes[0], id: "inside", start: { time: 100, price: 1 }, end: { time: 110, price: 2 } },
    { ...value.strokes[0], id: "after", start: { time: 200, price: 1 }, end: { time: 210, price: 2 } },
  );
  value.signals.push({ id: "old", level: "5f", mode: "confirmed", time: 1, price: 1, signal_type: "1", confirmed: true });
  const pruned = __CHAN_OVERLAY_MANAGER_TESTING__.pruneOverlay(value, { from: 100, to: 110 });
  assert.deepEqual(pruned.strokes.map((item) => item.id), ["before", "inside", "after"]);
  assert.equal(pruned.signals.length, 0);
});

test("retention clips coverage so an evicted subrange cannot be served from cache", () => {
  const ranges = __CHAN_OVERLAY_MANAGER_TESTING__.clipRanges([{ from: 0, to: 100 }], { from: 40, to: 60 });
  assert.deepEqual(ranges, [{ from: 40, to: 60 }]);
  assert.equal(__CHAN_OVERLAY_MANAGER_TESTING__.rangeCovered(ranges, { from: 0, to: 10 }), false);
});

test("fragmented cache fills multiple gaps without recursive completion or duplicate fetches", async () => {
  const pending: Array<{ from: number; resolve: (value: ChanOverlayResponse) => void }> = [];
  let calls = 0;
  let defer = false;
  const manager = new ChanOverlayManager((_symbol, _timeframe, _limit, from) => {
    calls += 1;
    if (!defer) return Promise.resolve(overlay(`seed-${from}`));
    return new Promise((resolve) => pending.push({ from: from ?? 0, resolve }));
  });
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 4, to: 6, modes: ["confirmed"], onPaint: () => undefined, onError: () => assert.fail("unexpected error") });
  await sleep(170);
  defer = true;
  let paints = 0;
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { paints += 1; }, onError: () => assert.fail("unexpected error") });
  await sleep(170);
  assert.deepEqual(pending.map((item) => item.from).sort((a, b) => a - b), [0, 7]);
  pending.forEach((item) => item.resolve(overlay(`gap-${item.from}`)));
  await sleep(20);
  assert.equal(calls, 3);
  assert.equal(paints, 1);
  manager.dispose();
});

test("two sibling failures notify once and an explicit later request retries", async () => {
  const pending: Array<{
    from: number;
    resolve: (value: ChanOverlayResponse) => void;
    reject: (error: Error) => void;
  }> = [];
  let defer = false;
  let calls = 0;
  const manager = new ChanOverlayManager((_symbol, _timeframe, _limit, from) => {
    calls += 1;
    if (!defer) return Promise.resolve(overlay("seed"));
    return new Promise((resolve, reject) => pending.push({ from: from ?? 0, resolve, reject }));
  });
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 4, to: 6, modes: ["confirmed"], onPaint: () => undefined, onError: () => assert.fail("seed failed") });
  await sleep(170);
  defer = true;
  let paints = 0;
  let errors = 0;
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { paints += 1; }, onError: () => { errors += 1; } });
  await sleep(170);
  assert.equal(pending.length, 2);
  pending[0].reject(new Error("left failed"));
  pending[1].reject(new Error("right failed"));
  await sleep(20);
  assert.equal(errors, 1);
  assert.equal(paints, 0);
  assert.equal(calls, 3);

  pending.length = 0;
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { paints += 1; }, onError: () => assert.fail("retry failed") });
  await sleep(170);
  assert.equal(pending.length, 2);
  pending.forEach((item) => item.resolve(overlay(`retry-${item.from}`)));
  await sleep(20);
  assert.equal(paints, 1);
  manager.dispose();
});

test("terminal failure detaches and aborts an unshared hanging sibling", async () => {
  const pending: Array<{
    from: number;
    signal: AbortSignal;
    reject: (error: Error) => void;
  }> = [];
  let defer = false;
  const manager = new ChanOverlayManager((_symbol, _timeframe, _limit, from, _to, signal) => {
    if (!defer) return Promise.resolve(overlay("seed"));
    return new Promise((_resolve, reject) => {
      const item = { from: from ?? 0, signal: signal!, reject };
      pending.push(item);
      signal!.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
    });
  });
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 4, to: 6, modes: ["confirmed"], onPaint: () => undefined, onError: () => assert.fail("seed failed") });
  await sleep(170);
  defer = true;
  let errors = 0;
  let paints = 0;
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { paints += 1; }, onError: () => { errors += 1; } });
  await sleep(170);
  assert.deepEqual(pending.map((item) => item.from).sort((a, b) => a - b), [0, 7]);
  pending.find((item) => item.from === 0)!.reject(new Error("left failed"));
  await sleep(20);
  assert.equal(errors, 1);
  assert.equal(paints, 0);
  assert.equal(pending.find((item) => item.from === 7)!.signal.aborted, true);
  manager.dispose();
});

test("failed consumer detaches while a shared sibling remains for another consumer", async () => {
  const pending: Array<{
    from: number;
    signal: AbortSignal;
    resolve: (value: ChanOverlayResponse) => void;
    reject: (error: Error) => void;
  }> = [];
  let defer = false;
  const manager = new ChanOverlayManager((_symbol, _timeframe, _limit, from, _to, signal) => {
    if (!defer) return Promise.resolve(overlay("seed"));
    return new Promise((resolve, reject) => pending.push({ from: from ?? 0, signal: signal!, resolve, reject }));
  });
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 4, to: 6, modes: ["confirmed"], onPaint: () => undefined, onError: () => assert.fail("seed failed") });
  await sleep(170);
  defer = true;
  let failedErrors = 0;
  let failedPaints = 0;
  let sharedPaints = 0;
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { failedPaints += 1; }, onError: () => { failedErrors += 1; } });
  await sleep(170);
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 7, to: 10, modes: ["confirmed"], onPaint: () => { sharedPaints += 1; }, onError: () => assert.fail("shared consumer failed") });
  await sleep(170);
  const left = pending.find((item) => item.from === 0)!;
  const right = pending.find((item) => item.from === 7)!;
  left.reject(new Error("left failed"));
  await sleep(20);
  assert.equal(right.signal.aborted, false);
  right.resolve(overlay("right"));
  await sleep(20);
  assert.equal(failedErrors, 1);
  assert.equal(failedPaints, 0);
  assert.equal(sharedPaints, 1);
  manager.dispose();
});

test("overlapping consumers await every delayed flight and release independently", async () => {
  const pending: Array<{ signal: AbortSignal; resolve: (value: ChanOverlayResponse) => void }> = [];
  let defer = false;
  const manager = new ChanOverlayManager((_symbol, _timeframe, _limit, _from, _to, signal) => {
    if (!defer) return Promise.resolve(overlay("seed"));
    return new Promise((resolve) => pending.push({ signal: signal!, resolve }));
  });
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 4, to: 6, modes: ["confirmed"], onPaint: () => undefined, onError: () => assert.fail("unexpected error") });
  await sleep(170);
  defer = true;
  let firstPaints = 0;
  let secondPaints = 0;
  manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { firstPaints += 1; }, onError: () => assert.fail("unexpected error") });
  await sleep(170);
  const releaseSecond = manager.request({ symbol: "000001.SZ", timeframe: "5f", from: 0, to: 10, modes: ["confirmed"], onPaint: () => { secondPaints += 1; }, onError: () => assert.fail("unexpected error") });
  await sleep(170);
  assert.equal(pending.length, 2);
  releaseSecond();
  assert.equal(pending.some((item) => item.signal.aborted), false);
  pending.forEach((item) => item.resolve(overlay("resolved")));
  await sleep(20);
  assert.equal(firstPaints, 1);
  assert.equal(secondPaints, 0);
  manager.dispose();
});

test("malformed fetch never marks coverage and the same range refetches successfully", async () => {
  let calls = 0;
  let errors = 0;
  let paints = 0;
  const manager = new ChanOverlayManager(async () => {
    calls += 1;
    return calls === 1 ? ({ ...overlay(), strokes: null } as never) : overlay("valid");
  });
  const request = () => manager.request({
    symbol: "000001.SZ", timeframe: "5f", from: 1, to: 10, modes: ["confirmed"],
    onPaint: () => { paints += 1; },
    onError: () => { errors += 1; },
  });
  request();
  await sleep(170);
  assert.equal(errors, 1);
  assert.equal(paints, 0);
  request();
  await sleep(170);
  assert.equal(calls, 2);
  assert.equal(paints, 1);
  manager.dispose();
});

test("runtime validator rejects coercible and non-finite numeric values", () => {
  for (const invalid of [null, "", true, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.notEqual(validateChanOverlayResponse({ ...overlay(), requested_bar_count: invalid }), null);
    const value = overlay();
    value.strokes[0].start.price = invalid as never;
    assert.notEqual(validateChanOverlayResponse(value), null);
  }
  assert.notEqual(validateChanOverlayResponse({ ...overlay(), centers: null }), null);
  assert.equal(validateChanOverlayResponse(overlay()), null);
});

test("runtime validator rejects bad aliases and exact level or mode violations", () => {
  const center = { id: "c", level: "5f", mode: "confirmed", start_time: 10, end_time: 20, begin_base_ts: 10, end_base_ts: 20, low: 1, high: 2, confirmed: true };
  const signal = { id: "s", level: "5f", mode: "confirmed", time: 10, base_ts: 10, price: 1, signal_type: "1", confirmed: true };
  const channel = { id: "ch", level: "5f", mode: "confirmed", time: 10, base_ts: 10, upper: 2, lower: 1, confirmed: true };

  assert.notEqual(validateChanOverlayResponse({ ...overlay(), centers: [{ ...center, start_time: "10" }] }), null);
  assert.notEqual(validateChanOverlayResponse({ ...overlay(), signals: [{ ...signal, time: false }] }), null);
  assert.notEqual(validateChanOverlayResponse({ ...overlay(), channels: [{ ...channel, time: "10" }] }), null);
  assert.notEqual(validateChanOverlayResponse({ ...overlay(), signals: [{ ...signal, time: 11 }] }), null);

  const badLevel = overlay();
  badLevel.strokes[0].level = "15f";
  assert.notEqual(validateChanOverlayResponse(badLevel), null);
  const badMode = overlay();
  badMode.strokes[0].mode = "merged";
  assert.notEqual(validateChanOverlayResponse(badMode), null);
  assert.notEqual(validateChanOverlayResponse({ ...overlay(), levels: ["15f"] }), null);
  assert.notEqual(validateChanOverlayResponse({ ...overlay(), modes: ["merged"] }), null);

  const conflictingLine = overlay();
  conflictingLine.strokes[0].start = { time: 2, base_ts: 1, price: 1 };
  assert.notEqual(validateChanOverlayResponse(conflictingLine), null);
  const nullSeq = overlay();
  nullSeq.strokes[0].seq = null;
  assert.equal(validateChanOverlayResponse(nullSeq), null);
});

test("production-shaped nullable fields validate, cache, and pass the renderer gate", async () => {
  const value = productionOverlay();
  assert.equal(validateChanOverlayResponse(value), null);
  assert.equal(__CHAN_WIDGET_RENDER_TESTING__.validateChanOverlay(value), null);

  let paints = 0;
  const manager = new ChanOverlayManager(async () => value);
  manager.request({
    symbol: value.symbol, timeframe: value.chart_timeframe, from: 1, to: 30, modes: ["confirmed"],
    onPaint: (painted) => { paints += 1; assert.equal(painted.strokes[0].seq, null); },
    onError: (error) => assert.fail(error.message),
  });
  await sleep(170);
  assert.equal(paints, 1);
  manager.dispose();
});

test("required fields remain non-null with nullable aliases present", () => {
  const centerNull = productionOverlay();
  centerNull.centers[0].start_time = null as never;
  assert.notEqual(validateChanOverlayResponse(centerNull), null);
  const signalNull = productionOverlay();
  signalNull.signals[0].time = null as never;
  assert.notEqual(validateChanOverlayResponse(signalNull), null);
  const channelNull = productionOverlay();
  channelNull.channels[0].upper = null as never;
  assert.notEqual(validateChanOverlayResponse(channelNull), null);
  const pointNull = productionOverlay();
  pointNull.strokes[0].start.price = null as never;
  assert.notEqual(validateChanOverlayResponse(pointNull), null);
});

function productionOverlay(): ChanOverlayResponse {
  return {
    ...overlay("production-stroke"),
    requested_bar_count: 30,
    strokes: [{
      id: "production-stroke", seq: null, level: "5f", mode: "confirmed",
      start: { time: 1, price: 10, base_ts: null, base_seq: null },
      end: { time: 20, price: 12, base_ts: null, base_seq: null },
      begin_base_ts: null, end_base_ts: null, begin_base_seq: null, end_base_seq: null,
      direction: "up", confirmed: true,
    }],
    centers: [{
      id: "production-center", seq: null, level: "5f", mode: "confirmed",
      start_time: 5, end_time: 15, begin_base_ts: null, end_base_ts: null,
      begin_base_seq: null, end_base_seq: null, low: 10, high: 12, confirmed: true,
    }],
    signals: [{
      id: "production-signal", seq: null, level: "5f", mode: "confirmed",
      time: 10, base_ts: null, base_seq: null, price: 11, signal_type: "1",
      side: null, bsp_type: null, confirmed: true,
    }],
    channels: [{
      id: "production-channel", level: "5f", mode: "confirmed",
      time: 10, base_ts: null, base_seq: null, upper: 13, lower: 9,
      period: null, confirmed: true,
    }],
  };
}
