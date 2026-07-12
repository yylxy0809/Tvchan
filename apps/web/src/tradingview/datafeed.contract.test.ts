import assert from "node:assert/strict";
import test from "node:test";

import { ChartDataManager } from "../api/chartDataManager";
import type { ApiBar } from "../api/client";
import { createDatafeed, planHistoryRequest } from "./datafeed";
import { toTradingViewTime } from "./time";

(globalThis as { window?: Window }).window = globalThis as unknown as Window;

test("history planner honors from/to/countBack without fixed prefetch", () => {
  assert.deepEqual(planHistoryRequest({ from: 100, to: 200, countBack: 80 }), {
    from: 100, to: 200, limit: 100, guard: 20,
  });
  assert.deepEqual(planHistoryRequest({ from: 1, to: 2, countBack: 2_000 }), {
    from: 1, to: 2, limit: 2_200, guard: 200,
  });
  assert.deepEqual(planHistoryRequest({ from: 1, to: 2, countBack: 6_000 }), {
    from: 1, to: 2, limit: 6_200, guard: 200,
  });
});

test("identical newer seq reaches manager fence so delayed differing seq cannot paint", async () => {
  const manager = new ChartDataManager();
  let emit!: (event: {
    symbol: string;
    timeframe: string;
    snapshotVersion?: string;
    seq?: number;
    sessionGeneration?: number;
    bar: ApiBar;
  }) => void;
  manager.getBars = async (request) => ({
    symbol: request.symbol,
    timeframe: request.timeframe,
    bars: [],
    noData: false,
  });
  manager.subscribeRealtimeBars = async (_request, listener) => {
    emit = listener;
    return () => {};
  };
  const feed = createDatafeed(manager);
  const painted: number[] = [];
  feed.subscribeBars(
    { ticker: "000016.SZ" },
    "5",
    (...args: unknown[]) => painted.push((args[0] as { close: number }).close),
    "seq-fence",
  );
  await new Promise((resolve) => setTimeout(resolve, 0));

  const seq21: ApiBar = {
    time: 200, open: 20, high: 22, low: 19, close: 21, volume: 200,
    amount: null, revision: 2, complete: false,
  };
  emit({
    symbol: "000016.SZ",
    timeframe: "5f",
    snapshotVersion: "bar:000016.SZ:5f:200:20:22:19:21:200:2:0",
    seq: 21,
    sessionGeneration: 1,
    bar: seq21,
  });
  emit({
    symbol: "000016.SZ",
    timeframe: "5f",
    snapshotVersion: "bar:000016.SZ:5f:200:20:22:19:21:200:2:0",
    seq: 23,
    sessionGeneration: 1,
    bar: seq21,
  });
  emit({
    symbol: "000016.SZ",
    timeframe: "5f",
    snapshotVersion: "bar:000016.SZ:5f:200:20:23:19:22:250:2:0",
    seq: 22,
    sessionGeneration: 1,
    bar: { ...seq21, high: 23, close: 22, volume: 250 },
  });

  assert.deepEqual(painted, [21]);
  feed.unsubscribeBars("seq-fence");
});

test("new realtime session accepts restarted seq and rejects late old-session paint", async () => {
  const manager = new ChartDataManager();
  let activate!: (generation: number) => void;
  let emit!: (event: {
    symbol: string;
    timeframe: string;
    snapshotVersion?: string;
    seq?: number;
    sessionGeneration?: number;
    bar: ApiBar;
  }) => void;
  manager.getBars = async (request) => ({
    symbol: request.symbol,
    timeframe: request.timeframe,
    bars: [],
    noData: false,
  });
  manager.subscribeRealtimeBars = async (_request, listener, sessionListener) => {
    emit = listener;
    activate = sessionListener;
    return () => {};
  };
  const feed = createDatafeed(manager);
  const painted: number[] = [];
  feed.subscribeBars(
    { ticker: "000017.SZ" },
    "5",
    (...args: unknown[]) => painted.push((args[0] as { close: number }).close),
    "reconnect-fence",
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  const bar: ApiBar = {
    time: 300, open: 30, high: 32, low: 29, close: 31, volume: 300,
    amount: null, revision: 2, complete: false,
  };
  activate(1);
  emit({
    symbol: "000017.SZ", timeframe: "5f", snapshotVersion: "bar:old:2:0",
    seq: 99, sessionGeneration: 1, bar,
  });
  activate(2);
  emit({
    symbol: "000017.SZ", timeframe: "5f", snapshotVersion: "bar:late-old:2:0",
    seq: 100, sessionGeneration: 1, bar: { ...bar, close: 33 },
  });
  emit({
    symbol: "000017.SZ", timeframe: "5f", snapshotVersion: "bar:new:2:0",
    seq: 0, sessionGeneration: 2, bar: { ...bar, close: 32 },
  });
  assert.deepEqual(painted, [31, 32]);
  feed.unsubscribeBars("reconnect-fence");
});

test("switching history context aborts obsolete HTTP work", async () => {
  const original = globalThis.fetch;
  let oldAborted = false;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    if (!String(input).includes("ABORT.SZ")) {
      return Promise.resolve(new Response(JSON.stringify({ symbol: "NEXT.SZ", timeframe: "5f", bars: [] }), { status: 200 }));
    }
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => {
        oldAborted = true;
        reject(new DOMException("aborted", "AbortError"));
      }, { once: true });
    });
  }) as typeof fetch;
  try {
    const feed = createDatafeed();
    const painted: string[] = [];
    const errors: string[] = [];
    feed.getBars({ ticker: "ABORT.SZ" }, "5", { from: 1, to: 2, countBack: 1 }, () => painted.push("old"), (...args) => errors.push(String(args[0])));
    feed.getBars({ ticker: "NEXT.SZ" }, "5", { from: 1, to: 2, countBack: 1 }, () => painted.push("next"), (...args) => errors.push(String(args[0])));
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(oldAborted, true);
    assert.deepEqual(painted, ["next"]);
    assert.deepEqual(errors, []);
  } finally { globalThis.fetch = original; }
});

test("a late response for a previous symbol cannot paint history", async () => {
  const original = globalThis.fetch;
  let releaseOld!: () => void;
  const old = new Promise<Response>((resolve) => { releaseOld = () => resolve(new Response(JSON.stringify({ symbol: "OLD.SZ", timeframe: "5f", bars: [] }), { status: 200 })); });
  globalThis.fetch = ((input: RequestInfo | URL) => String(input).includes("OLD.SZ")
    ? old
    : Promise.resolve(new Response(JSON.stringify({ symbol: "NEW.SZ", timeframe: "5f", bars: [] }), { status: 200 }))) as typeof fetch;
  try {
    const feed = createDatafeed();
    const painted: string[] = [];
    const failHistory = (...args: unknown[]) => assert.fail(String(args[0] ?? "history error"));
    feed.getBars({ ticker: "OLD.SZ" }, "5", { from: 1, to: 2, countBack: 1 }, () => painted.push("old"), failHistory);
    feed.getBars({ ticker: "NEW.SZ" }, "5", { from: 1, to: 2, countBack: 1 }, () => painted.push("new"), failHistory);
    releaseOld();
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(painted, ["new"]);
  } finally { globalThis.fetch = original; }
});

test("OLD to NEW to OLD keeps the first OLD request fenced by context epoch", async () => {
  const original = globalThis.fetch;
  let releaseFirstOld!: () => void;
  const firstOld = new Promise<Response>((resolve) => { releaseFirstOld = () => resolve(new Response(JSON.stringify({ symbol: "OLD.SZ", timeframe: "5f", bars: [] }), { status: 200 })); });
  let oldCalls = 0;
  globalThis.fetch = ((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("OLD.SZ")) return oldCalls++ === 0 ? firstOld : Promise.resolve(new Response(JSON.stringify({ symbol: "OLD.SZ", timeframe: "5f", bars: [] }), { status: 200 }));
    return Promise.resolve(new Response(JSON.stringify({ symbol: "NEW.SZ", timeframe: "5f", bars: [] }), { status: 200 }));
  }) as typeof fetch;
  try {
    const feed = createDatafeed();
    const painted: string[] = [];
    const failHistory = (...args: unknown[]) => assert.fail(String(args[0] ?? "history error"));
    feed.getBars({ ticker: "OLD.SZ" }, "5", { from: 1, to: 2, countBack: 1 }, () => painted.push("old-first"), failHistory);
    feed.getBars({ ticker: "NEW.SZ" }, "5", { from: 1, to: 2, countBack: 1 }, () => painted.push("new"), failHistory);
    feed.getBars({ ticker: "OLD.SZ" }, "5", { from: 3, to: 4, countBack: 1 }, () => painted.push("old-current"), failHistory);
    releaseFirstOld();
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.deepEqual(painted.sort(), ["old-current"]);
  } finally { globalThis.fetch = original; }
});

test("A to B to A never joins the aborted first A flight and completes the current callback", async () => {
  const original = globalThis.fetch;
  let aCalls = 0;
  globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (!url.includes("RACE-A.SZ")) {
      return Promise.resolve(new Response(JSON.stringify({ symbol: "RACE-B.SZ", timeframe: "5f", bars: [] }), { status: 200 }));
    }
    aCalls += 1;
    if (aCalls === 2) {
      return Promise.resolve(new Response(JSON.stringify({ symbol: "RACE-A.SZ", timeframe: "5f", bars: [] }), { status: 200 }));
    }
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => {
        queueMicrotask(() => reject(new DOMException("aborted", "AbortError")));
      }, { once: true });
    });
  }) as typeof fetch;
  try {
    const feed = createDatafeed();
    const callbacks: Array<[string, boolean]> = [];
    const errors: string[] = [];
    const history = (label: string) => (...args: unknown[]) => {
      const meta = args[1] as { noData?: boolean } | undefined;
      callbacks.push([label, meta?.noData === true]);
    };
    const error = (...args: unknown[]) => errors.push(String(args[0]));
    const params = { from: 1, to: 2, countBack: 1 };
    feed.getBars({ ticker: "RACE-A.SZ" }, "5", params, history("a-old"), error);
    feed.getBars({ ticker: "RACE-B.SZ" }, "5", params, history("b"), error);
    feed.getBars({ ticker: "RACE-A.SZ" }, "5", params, history("a-current"), error);
    await new Promise((resolve) => setTimeout(resolve, 0));
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(aCalls, 2);
    assert.deepEqual(callbacks, [["a-current", false]]);
    assert.deepEqual(errors, []);
  } finally { globalThis.fetch = original; }
});

test("resolved A-share symbols expose Shanghai trading sessions", async () => {
  const feed = createDatafeed();
  const info = await new Promise<Record<string, unknown>>((resolve, reject) => {
    feed.resolveSymbol("000001.SZ", (...args) => resolve(args[0] as Record<string, unknown>), (...args) => reject(args[0]));
  });
  assert.equal(info.session, "0930-1130,1300-1500");
  assert.equal(info.timezone, "Asia/Shanghai");
});

test("TradingView timestamps preserve intraday, daily, weekly, and monthly rules", () => {
  const close = Date.UTC(2026, 6, 10, 7); // 15:00 Asia/Shanghai on Friday.
  assert.equal(toTradingViewTime(close / 1000, "5"), close);
  assert.equal(toTradingViewTime(close / 1000, "D"), Date.UTC(2026, 6, 10));
  assert.equal(toTradingViewTime(close / 1000, "W"), Date.UTC(2026, 6, 6));
  assert.equal(toTradingViewTime(close / 1000, "M"), Date.UTC(2026, 6, 1));
});
