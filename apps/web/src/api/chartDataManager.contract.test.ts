import assert from "node:assert/strict";
import test from "node:test";

import { ChartDataManager } from "./chartDataManager";

(globalThis as { window?: Window }).window = globalThis as unknown as Window;

type Bar = {
  time: number;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
  amount?: number | null;
  complete?: boolean;
  revision?: number;
};

function response(bars: Bar[]): Response {
  return new Response(
    JSON.stringify({ symbol: "000001.SZ", timeframe: "5f", bars: bars.map((bar) => ({
      open: 1, high: 2, low: 0, close: bar.close ?? 1, volume: 1, amount: null,
      complete: true, revision: 0, ...bar,
    })) }),
    { status: 200, headers: { "content-type": "application/json" } },
  );
}

function installFetch(handler: (url: URL, signal: AbortSignal | undefined) => Promise<Response>): () => void {
  const original = globalThis.fetch;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) =>
    handler(new URL(String(input), "http://localhost"), init?.signal ?? undefined)) as typeof fetch;
  return () => { globalThis.fetch = original; };
}

test("covered ranges are reused and output is ascending, unique, and exclusive of to", async () => {
  let calls = 0;
  const restore = installFetch(async () => {
    calls += 1;
    return response(calls === 1 ? [{ time: 20 }, { time: 10 }, { time: 20, close: 3 }] : []);
  });
  try {
    const manager = new ChartDataManager();
    const first = await manager.getBars({ symbol: "000001.SZ", timeframe: "5f", from: 10, to: 30, limit: 3 });
    const revisit = await manager.getBars({ symbol: "000001.SZ", timeframe: "5f", from: 10, to: 30, limit: 2 });
    assert.deepEqual(first.bars.map((bar) => [bar.time, bar.close]), [[10, 1], [20, 3]]);
    assert.deepEqual(revisit.bars.map((bar) => bar.time), [10, 20]);
    assert.equal(calls, 2);
  } finally { restore(); }
});

test("contained overlapping requests coalesce into one HTTP request", async () => {
  let calls = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const restore = installFetch(async () => {
    calls += 1;
    await gate;
    return response(Array.from({ length: 10 }, (_, index) => ({ time: 120 + index * 5 })));
  });
  try {
    const manager = new ChartDataManager();
    const first = manager.getBars({ symbol: "000002.SZ", timeframe: "5f", from: 100, to: 200, limit: 10 });
    const second = manager.getBars({ symbol: "000002.SZ", timeframe: "5f", from: 120, to: 180, limit: 5 });
    release();
    await Promise.all([first, second]);
    assert.equal(calls, 1);
  } finally { restore(); }
});

test("partial overlaps wait for pending coverage and fetch only the uncovered tail or head", async () => {
  const ranges: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const restore = installFetch(async (url) => {
    ranges.push(`${new Date(url.searchParams.get("from") ?? 0).getTime() / 1000}|${new Date(url.searchParams.get("to") ?? 0).getTime() / 1000}`);
    if (ranges.length === 1) await gate;
    return response(Array.from(
      { length: 10 },
      (_, index) => ({ time: ranges.length === 1 ? 100 + index * 10 : 200 + index * 5 }),
    ));
  });
  try {
    const manager = new ChartDataManager();
    const first = manager.getBars({ symbol: "000008.SZ", timeframe: "5f", from: 100, to: 200, limit: 10 });
    const second = manager.getBars({ symbol: "000008.SZ", timeframe: "5f", from: 150, to: 250, limit: 10 });
    release();
    await Promise.all([first, second]);
    assert.deepEqual(ranges.map((value) => value.split("|").map(Number)), [[100, 200], [200, 250]]);

    ranges.length = 0;
    let releaseReverse!: () => void;
    const reverseGate = new Promise<void>((resolve) => { releaseReverse = resolve; });
    const reverseRestore = installFetch(async (url) => {
      ranges.push(`${new Date(url.searchParams.get("from") ?? 0).getTime() / 1000}|${new Date(url.searchParams.get("to") ?? 0).getTime() / 1000}`);
      if (ranges.length === 1) await reverseGate;
      return response(Array.from(
        { length: 10 },
        (_, index) => ({ time: ranges.length === 1 ? 200 + index * 10 : 150 + index * 5 }),
      ));
    });
    try {
      const reverse = new ChartDataManager();
      const later = reverse.getBars({ symbol: "000009.SZ", timeframe: "5f", from: 200, to: 300, limit: 10 });
      const overlap = reverse.getBars({ symbol: "000009.SZ", timeframe: "5f", from: 150, to: 250, limit: 10 });
      releaseReverse();
      await Promise.all([later, overlap]);
      assert.deepEqual(ranges.map((value) => value.split("|").map(Number)), [[200, 300], [150, 200]]);
    } finally { reverseRestore(); }

    ranges.length = 0;
    let releaseMany!: () => void;
    const manyGate = new Promise<void>((resolve) => { releaseMany = resolve; });
    const manyRestore = installFetch(async (url) => {
      ranges.push(`${new Date(url.searchParams.get("from") ?? 0).getTime() / 1000}|${new Date(url.searchParams.get("to") ?? 0).getTime() / 1000}`);
      await manyGate;
      const from = new Date(url.searchParams.get("from") ?? 0).getTime() / 1000;
      return response(Array.from({ length: 10 }, (_, index) => ({ time: from + index * 5 })));
    });
    try {
      const multiple = new ChartDataManager();
      const left = multiple.getBars({ symbol: "000010.SZ", timeframe: "5f", from: 100, to: 200, limit: 10 });
      const right = multiple.getBars({ symbol: "000010.SZ", timeframe: "5f", from: 200, to: 300, limit: 10 });
      const middle = multiple.getBars({ symbol: "000010.SZ", timeframe: "5f", from: 150, to: 250, limit: 10 });
      releaseMany();
      await Promise.all([left, right, middle]);
      assert.deepEqual(ranges.map((value) => value.split("|").map(Number)), [[100, 200], [200, 300]]);
    } finally { manyRestore(); }
  } finally { restore(); }
});

test("a truncated sparse parent proves only its returned suffix before a child fills the missing prefix", async () => {
  const ranges: Array<[number, number]> = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const restore = installFetch(async (url) => {
    const from = new Date(url.searchParams.get("from") ?? 0).getTime() / 1000;
    const to = new Date(url.searchParams.get("to") ?? 0).getTime() / 1000;
    ranges.push([from, to]);
    if (ranges.length === 1) {
      await gate;
      return response([{ time: 200 }, { time: 250 }]);
    }
    return response([{ time: 175 }]);
  });
  try {
    const manager = new ChartDataManager();
    const parent = manager.getBars({ symbol: "000011.SZ", timeframe: "5f", from: 100, to: 300, limit: 2 });
    const child = manager.getBars({ symbol: "000011.SZ", timeframe: "5f", from: 150, to: 280, limit: 2 });
    release();
    const [, childResult] = await Promise.all([parent, child]);
    assert.deepEqual(ranges, [[100, 300], [150, 200]]);
    assert.deepEqual(childResult.bars.map((bar) => bar.time), [200, 250]);
  } finally { restore(); }
});

test("aborting one consumer does not cancel another, but the final consumer abort cancels HTTP", async () => {
  let aborted = 0;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const restore = installFetch(async (_url, signal) => new Promise<Response>((resolve, reject) => {
    signal?.addEventListener("abort", () => { aborted += 1; reject(new DOMException("aborted", "AbortError")); }, { once: true });
    gate.then(() => resolve(response([{ time: 150 }])));
  }));
  try {
    const manager = new ChartDataManager();
    const one = new AbortController();
    const two = new AbortController();
    const request = { symbol: "000003.SZ", timeframe: "5f", from: 100, to: 200, limit: 10 };
    const first = manager.getBars({ ...request, signal: one.signal });
    const second = manager.getBars({ ...request, signal: two.signal });
    one.abort();
    await assert.rejects(first, { name: "AbortError" });
    assert.equal(aborted, 0);
    release();
    await second;

    const all = new AbortController();
    const final = manager.getBars({ symbol: "000004.SZ", timeframe: "5f", from: 100, to: 200, limit: 10, signal: all.signal });
    all.abort();
    await assert.rejects(final, { name: "AbortError" });
    assert.equal(aborted, 1);
  } finally { restore(); }
});

test("empty holidays do not claim exhaustion, while a left-edge probe does", async () => {
  const restore = installFetch(async () => response([]));
  try {
    const manager = new ChartDataManager();
    const holiday = await manager.getBars({ symbol: "000005.SZ", timeframe: "5f", from: 100, to: 200, limit: 10 });
    const exhausted = await manager.getBars({ symbol: "000006.SZ", timeframe: "5f", from: 0, to: 100, limit: 10 });
    assert.equal(holiday.noData, false);
    assert.equal(exhausted.noData, true);
  } finally { restore(); }
});

test("a covered countBack shortfall fetches the deficit before the earliest cached bar", async () => {
  const limits: string[] = [];
  const ends: number[] = [];
  let call = 0;
  const restore = installFetch(async (url) => {
    limits.push(url.searchParams.get("limit") ?? "");
    ends.push(new Date(url.searchParams.get("to") ?? 0).getTime() / 1000);
    call += 1;
    return response(call === 1 ? [{ time: 10, close: 1 }] : [{ time: 5, close: 2 }, { time: 9, close: 9 }]);
  });
  try {
    const manager = new ChartDataManager();
    await manager.getBars({ symbol: "000007.SZ", timeframe: "5f", from: 10, to: 30, limit: 3 });
    const result = await manager.getBars({ symbol: "000007.SZ", timeframe: "5f", from: 10, to: 30, limit: 3 });
    assert.deepEqual(limits, ["3", "2"]);
    assert.deepEqual(ends, [30, 10]);
    assert.deepEqual(result.bars.map((bar) => [bar.time, bar.close]), [[5, 2], [9, 9], [10, 1]]);
  } finally { restore(); }
});

test("a realtime revision survives completion of an older inflight HTTP response", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const restore = installFetch(async () => {
    await gate;
    return response([{ time: 150 }, { time: 200, close: 1 }]);
  });
  try {
    const manager = new ChartDataManager();
    const pending = manager.getBars({ symbol: "000012.SZ", timeframe: "5f", from: 100, to: 300, limit: 2 });
    manager.handleRealtimeBarUpdate({
      symbol: "000012.SZ",
      timeframe: "5f",
      snapshotVersion: "revision-1",
      bar: { time: 200, open: 8, high: 10, low: 7, close: 9, volume: 99, revision: 1, complete: false },
    });
    release();
    const result = await pending;
    const revised = result.bars.find((bar) => bar.time === 200);
    assert.deepEqual(revised, {
      time: 200, open: 8, high: 10, low: 7, close: 9, volume: 99,
      amount: null, revision: 1, complete: false,
    });
  } finally { restore(); }
});

test("reconnect generation accepts low seq, rejects late old session, and fences stale HTTP", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const restore = installFetch(async () => {
    await gate;
    return response([{ time: 150 }, { time: 200, close: 1 }]);
  });
  try {
    const manager = new ChartDataManager();
    const pending = manager.getBars({ symbol: "000018.SZ", timeframe: "5f", from: 100, to: 300, limit: 2 });
    manager.handleRealtimeBarUpdate({
      symbol: "000018.SZ", timeframe: "5f", snapshotVersion: "bar:old:2:0",
      seq: 99, sessionGeneration: 1,
      bar: { time: 200, open: 20, high: 22, low: 19, close: 21, volume: 200, revision: 2, complete: false },
    });
    assert.equal(manager.beginRealtimeSession("000018.SZ", "5f", 2), true);
    manager.handleRealtimeBarUpdate({
      symbol: "000018.SZ", timeframe: "5f", snapshotVersion: "bar:late-old:2:0",
      seq: 100, sessionGeneration: 1,
      bar: { time: 200, open: 20, high: 24, low: 19, close: 23, volume: 230, revision: 2, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000018.SZ", timeframe: "5f", snapshotVersion: "bar:new:2:0",
      seq: 0, sessionGeneration: 2,
      bar: { time: 200, open: 20, high: 23, low: 19, close: 22, volume: 220, revision: 2, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000018.SZ", timeframe: "5f", snapshotVersion: "bar:new-lower:1:0",
      seq: 1, sessionGeneration: 2,
      bar: { time: 200, open: 10, high: 12, low: 9, close: 11, volume: 110, revision: 1, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000018.SZ", timeframe: "5f", snapshotVersion: "bar:new-higher:3:0",
      seq: 0, sessionGeneration: 2,
      bar: { time: 200, open: 30, high: 33, low: 29, close: 32, volume: 320, revision: 3, complete: false },
    });
    release();
    const result = await pending;
    const latest = result.bars.find((bar) => bar.time === 200);
    assert.equal(latest?.close, 32);
    assert.equal(latest?.volume, 320);
    assert.equal(latest?.revision, 3);
  } finally { restore(); }
});

test("realtime ordering uses revision and websocket seq, never opaque backend snapshot versions", async () => {
  const restore = installFetch(async () => response([{ time: 100 }, { time: 200 }]));
  try {
    const manager = new ChartDataManager();
    await manager.getBars({ symbol: "000014.SZ", timeframe: "5f", from: 1, to: 300, limit: 2 });
    manager.handleRealtimeBarUpdate({
      symbol: "000014.SZ",
      timeframe: "5f",
      snapshotVersion: "bar:000014.SZ:5f:200:20:22:19:21:200:2:0",
      seq: 20,
      bar: { time: 200, open: 20, high: 22, low: 19, close: 21, volume: 200, revision: 2, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000014.SZ",
      timeframe: "5f",
      snapshotVersion: "bar:000014.SZ:5f:200:10:12:9:11:100:1:0",
      seq: 30,
      bar: { time: 200, open: 10, high: 12, low: 9, close: 11, volume: 100, revision: 1, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000014.SZ",
      timeframe: "5f",
      snapshotVersion: "bar:000014.SZ:5f:200:20:23:19:19:210:2:0",
      seq: 19,
      bar: { time: 200, open: 20, high: 23, low: 19, close: 19, volume: 210, revision: 2, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000014.SZ",
      timeframe: "5f",
      snapshotVersion: "bar:000014.SZ:5f:200:20:23:19:22:250:2:0",
      seq: 21,
      bar: { time: 200, open: 20, high: 23, low: 19, close: 22, volume: 250, revision: 2, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000014.SZ",
      timeframe: "5f",
      snapshotVersion: "bar:000014.SZ:5f:200:20:23:19:22:250:2:0",
      seq: 23,
      bar: { time: 200, open: 20, high: 23, low: 19, close: 22, volume: 250, revision: 2, complete: false },
    });
    manager.handleRealtimeBarUpdate({
      symbol: "000014.SZ",
      timeframe: "5f",
      snapshotVersion: "bar:000014.SZ:5f:200:20:24:19:24:260:2:0",
      seq: 22,
      bar: { time: 200, open: 20, high: 24, low: 19, close: 24, volume: 260, revision: 2, complete: false },
    });
    const result = await manager.getBars({ symbol: "000014.SZ", timeframe: "5f", from: 1, to: 300, limit: 2 });
    const latest = result.bars.find((bar) => bar.time === 200);
    assert.equal(latest?.revision, 2);
    assert.equal(latest?.close, 22);
    assert.equal(latest?.volume, 250);
    assert.equal(latest?.complete, false);
  } finally { restore(); }
});

test("trimming to 15000 bars clips coverage so the evicted range refetches", async () => {
  const ranges: Array<[number, number]> = [];
  const restore = installFetch(async (url) => {
    const from = new Date(url.searchParams.get("from") ?? 0).getTime() / 1000;
    const to = new Date(url.searchParams.get("to") ?? 0).getTime() / 1000;
    ranges.push([from, to]);
    if (ranges.length === 1) return response(Array.from({ length: 5_000 }, (_, index) => ({ time: 10_001 + index })));
    if (ranges.length === 2) return response(Array.from({ length: 5_000 }, (_, index) => ({ time: 5_001 + index })));
    if (ranges.length === 3) return response(Array.from({ length: 5_000 }, (_, index) => ({ time: 1 + index })));
    return response([{ time: 1, close: 7 }]);
  });
  try {
    const manager = new ChartDataManager();
    const initial = await manager.getBars({ symbol: "000015.SZ", timeframe: "5f", from: 1, to: 16_000, limit: 15_000 });
    assert.equal(initial.bars.length, 15_000);
    manager.handleRealtimeBarUpdate({
      symbol: "000015.SZ",
      timeframe: "5f",
      snapshotVersion: "tail-1",
      bar: { time: 15_001, open: 2, high: 3, low: 1, close: 2, volume: 2, revision: 1, complete: false },
    });
    const evicted = await manager.getBars({ symbol: "000015.SZ", timeframe: "5f", from: 1, to: 2, limit: 1 });
    assert.deepEqual(ranges, [[1, 16_000], [1, 10_001], [1, 5_001], [1, 2]]);
    assert.deepEqual(evicted.bars.map((bar) => [bar.time, bar.close]), [[1, 7]]);
  } finally { restore(); }
});

test("requests above the HTTP cap paginate backward until countBack is satisfied", async () => {
  const limits: number[] = [];
  const ranges: Array<[number, number]> = [];
  const restore = installFetch(async (url) => {
    const limit = Number(url.searchParams.get("limit"));
    const from = new Date(url.searchParams.get("from") ?? 0).getTime() / 1000;
    const to = new Date(url.searchParams.get("to") ?? 0).getTime() / 1000;
    limits.push(limit);
    ranges.push([from, to]);
    return response(ranges.length === 1
      ? Array.from({ length: 5_000 }, (_, index) => ({ time: 1_001 + index }))
      : Array.from({ length: 1_000 }, (_, index) => ({ time: 1 + index })));
  });
  try {
    const manager = new ChartDataManager();
    const result = await manager.getBars({ symbol: "000013.SZ", timeframe: "5f", from: 1, to: 7_000, limit: 6_000 });
    assert.deepEqual(limits, [5_000, 5_000]);
    assert.deepEqual(ranges, [[1, 7_000], [1, 1_001]]);
    assert.equal(result.bars.length, 6_000);
    assert.equal(result.bars[0]?.time, 1);
    assert.equal(result.bars[result.bars.length - 1]?.time, 6_000);
  } finally { restore(); }
});
