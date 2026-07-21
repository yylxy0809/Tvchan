import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { ChartDataManager, type RealtimeFeedback } from "./chartDataManager";
import { ChanRealtimeOverlayBridge } from "./chanRealtimeOverlayBridge";
import { CHAN_DELTA_FIXTURE, CHAN_SNAPSHOT_FIXTURE } from "./chanRealtimeOverlayBridge.fixtures";
import { installAsyncSubscription } from "../components/ChartWorkspace";

(globalThis as { window?: Window }).window = globalThis as unknown as Window;

class FakeWebSocket {
  static readonly OPEN = 1;
  static instances: FakeWebSocket[] = [];
  static failFirstConnection = false;
  readyState = 0;
  sent: unknown[] = [];
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;

  constructor(readonly url: string) {
    FakeWebSocket.instances.push(this);
    const shouldFail = FakeWebSocket.failFirstConnection && FakeWebSocket.instances.length === 1;
    queueMicrotask(() => {
      if (this.readyState !== 0) return;
      if (shouldFail) {
        this.onerror?.();
        return;
      }
      this.readyState = FakeWebSocket.OPEN;
      this.onopen?.();
    });
  }

  send(payload: string): void { this.sent.push(JSON.parse(payload)); }
  close(): void { this.readyState = 3; this.onclose?.(); }
  receive(payload: unknown): void { this.onmessage?.({ data: JSON.stringify(payload) }); }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function installTokenStorage(): Storage {
  const values = new Map<string, string>();
  const storage = {
    getItem(key: string) { return values.get(key) ?? null; },
    setItem(key: string, value: string) { values.set(key, value); },
    removeItem(key: string) { values.delete(key); },
    clear() { values.clear(); },
    key(index: number) { return [...values.keys()][index] ?? null; },
    get length() { return values.size; },
  } satisfies Storage;
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: storage,
  });
  return storage;
}

test("bounded Chan subscription forwards envelopes, reports replay, and unsubscribes", async () => {
  const original = globalThis.WebSocket;
  let releaseSubscription: (() => void) | null = null;
  (globalThis as { WebSocket?: typeof WebSocket }).WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  FakeWebSocket.instances = [];
  try {
    const manager = new ChartDataManager();
    const messages: unknown[] = [];
    const applied: string[] = [];
    const statuses: string[] = [];
    const bridge = new ChanRealtimeOverlayBridge();
    const context = { symbol: "000001.SZ", chartTimeframe: "5f", modes: ["confirmed", "predictive"] };
    const release = await manager.subscribeChanOverlay({
      symbol: "000001.SZ", timeframe: "5f", levels: ["5f", "30f", "1d"],
      modes: ["confirmed", "predictive"], from: 100, to: 200, limit: 300,
    }, (message) => {
      messages.push(message);
      const result = bridge.apply(message);
      if (result.status === "applied") applied.push(result.state.snapshotVersion);
    }, (status) => {
      statuses.push(status);
      if (status === "replayed") bridge.resetTransportEpoch(context);
    });
    releaseSubscription = release;
    await sleep(0);
    const first = FakeWebSocket.instances[0];
    assert.deepEqual(first.sent, [{
      type: "subscribe_chan", id: "chan:000001.SZ:5f:300:100:200:5f,30f,1d:confirmed,predictive",
      symbol: "000001.SZ", timeframe: "5f", levels: ["5f", "30f", "1d"],
      modes: ["confirmed", "predictive"], from: 100, to: 200, limit: 300,
    }]);
    assert.deepEqual(statuses, ["disconnected", "connected", "replayed"]);
    const subscriptionId = (first.sent[0] as { id: string }).id;
    const snapshot = { ...CHAN_SNAPSHOT_FIXTURE, id: subscriptionId };
    first.receive(snapshot);
    first.receive({ ...CHAN_DELTA_FIXTURE, id: subscriptionId });
    first.receive({ type: "chart_bundle_snapshot", id: subscriptionId, bundle: {} });
    assert.deepEqual(messages, [snapshot, { ...CHAN_DELTA_FIXTURE, id: subscriptionId }]);

    first.close();
    await sleep(1_100);
    const second = FakeWebSocket.instances[1];
    assert.ok(second);
    assert.equal(statuses.includes("disconnected"), true);
    assert.equal(statuses[statuses.length - 1], "replayed");
    assert.equal((second.sent[0] as { type?: string }).type, "subscribe_chan");
    const replaySnapshot = { ...snapshot, sequence: 1, snapshot_version: "replay-v1" };
    second.receive(replaySnapshot);
    assert.equal(applied[applied.length - 1], "replay-v1");
    const statusCount = statuses.length;
    const messageCount = messages.length;
    first.receive({ ...replaySnapshot, snapshot_version: "stale-old-socket" });
    first.onclose?.();
    await sleep(10);
    assert.equal(messages.length, messageCount);
    assert.equal(statuses.length, statusCount);
    assert.equal(FakeWebSocket.instances.length, 2);
    release();
    releaseSubscription = null;
    assert.deepEqual(second.sent[second.sent.length - 1], {
      type: "unsubscribe_chan",
      id: "chan:000001.SZ:5f:300:100:200:5f,30f,1d:confirmed,predictive",
    });
  } finally {
    releaseSubscription?.();
    globalThis.WebSocket = original;
  }
});

test("active Chan subscription source has no bundle or legacy snapshot handling", () => {
  const source = readFileSync(new URL("./chartDataManager.ts", import.meta.url), "utf8");
  const active = source.slice(
    source.indexOf("async subscribeChanOverlay"),
    source.indexOf("async subscribeRealtimeBars"),
  );
  assert.doesNotMatch(active, /bundle|chan_snapshot|chan_delta|subscribe_chart_bundle/);
});

test("session reset closes old-token sockets before a new login subscribes", async (t) => {
  const original = globalThis.WebSocket;
  const localStorageDescriptor = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  const storage = installTokenStorage();
  (globalThis as { WebSocket?: typeof WebSocket }).WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  FakeWebSocket.instances = [];
  const manager = new ChartDataManager();
  t.after(() => {
    manager.resetSession();
    globalThis.WebSocket = original;
    if (localStorageDescriptor) {
      Object.defineProperty(globalThis, "localStorage", localStorageDescriptor);
    } else {
      Reflect.deleteProperty(globalThis, "localStorage");
    }
  });

  storage.setItem("tv-a-share-login-token", "token-a");
  const releaseA = await manager.subscribeChanOverlay({
    symbol: "000001.SZ", timeframe: "5f", levels: ["5f", "30f", "1d"],
    modes: ["confirmed", "predictive"], from: 100, to: 200, limit: 300,
  }, () => {}, () => {});
  await sleep(0);
  const first = FakeWebSocket.instances[0];
  assert.equal(new URL(first.url).searchParams.get("token"), "token-a");

  manager.resetSession();
  assert.equal(first.readyState, 3);
  storage.setItem("tv-a-share-login-token", "token-b");
  const releaseB = await manager.subscribeChanOverlay({
    symbol: "000002.SZ", timeframe: "5f", levels: ["5f", "30f", "1d"],
    modes: ["confirmed", "predictive"], from: 100, to: 200, limit: 300,
  }, () => {}, () => {});
  await sleep(0);
  const second = FakeWebSocket.instances[1];
  assert.ok(second);
  assert.equal(new URL(second.url).searchParams.get("token"), "token-b");
  await sleep(1_100);
  assert.equal(FakeWebSocket.instances.length, 2);

  releaseA();
  releaseB();
});

test("session reset closes chart and realtime sockets still connecting", async (t) => {
  const original = globalThis.WebSocket;
  (globalThis as { WebSocket?: typeof WebSocket }).WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  FakeWebSocket.instances = [];
  const manager = new ChartDataManager();
  t.after(() => {
    manager.resetSession();
    globalThis.WebSocket = original;
  });

  const chartSubscription = manager.subscribeChanOverlay({
    symbol: "000001.SZ", timeframe: "5f", levels: ["5f", "30f", "1d"],
    modes: ["confirmed", "predictive"], from: 100, to: 200, limit: 300,
  }, () => {}, () => {});
  const realtimeSubscription = manager.subscribeRealtimeBars({
    symbol: "000001.SZ", timeframe: "5f",
  }, () => {}, () => {});
  const [chartSocket, realtimeSocket] = FakeWebSocket.instances;

  manager.resetSession();

  assert.equal(chartSocket.readyState, 3);
  assert.equal(realtimeSocket.readyState, 3);
  (await chartSubscription)();
  (await realtimeSubscription)();
});

test("an open realtime socket error reconnects and fences late messages", async (t) => {
  const original = globalThis.WebSocket;
  (globalThis as { WebSocket?: typeof WebSocket }).WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  FakeWebSocket.instances = [];
  const manager = new ChartDataManager();
  t.after(() => {
    manager.resetSession();
    globalThis.WebSocket = original;
  });
  const messages: unknown[] = [];
  const release = await manager.subscribeRealtimeBars({
    symbol: "000001.SZ", timeframe: "5f",
  }, (message) => messages.push(message), () => {});
  const first = FakeWebSocket.instances[0];

  first.onerror?.();
  first.receive({
    type: "bar_update", symbol: "000001.SZ", timeframe: "5f", bar: { close: 1 },
  });
  await sleep(1_100);

  assert.equal(messages.length, 0);
  assert.equal(first.readyState, 3);
  assert.equal(FakeWebSocket.instances.length, 2);
  assert.deepEqual(FakeWebSocket.instances[1].sent[0], {
    type: "subscribe", id: "bar:000001.SZ:5f", symbol: "000001.SZ", timeframes: ["5f"],
  });
  release();
});

test("an initial realtime connection failure retains the subscription and retries after 250ms", async (t) => {
  const original = globalThis.WebSocket;
  (globalThis as { WebSocket?: typeof WebSocket }).WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  FakeWebSocket.instances = [];
  FakeWebSocket.failFirstConnection = true;
  const manager = new ChartDataManager();
  const feedback: RealtimeFeedback[] = [];
  const releaseFeedback = manager.subscribeRealtimeFeedback((event) => feedback.push(event));
  t.after(() => {
    manager.resetSession();
    releaseFeedback();
    FakeWebSocket.failFirstConnection = false;
    globalThis.WebSocket = original;
  });

  const release = await manager.subscribeRealtimeBars({
    symbol: "000001.SZ", timeframe: "5f",
  }, () => {}, () => {});

  assert.equal(FakeWebSocket.instances.length, 1);
  await sleep(300);
  assert.equal(FakeWebSocket.instances.length, 2);
  assert.deepEqual(FakeWebSocket.instances[1].sent[0], {
    type: "subscribe", id: "bar:000001.SZ:5f", symbol: "000001.SZ", timeframes: ["5f"],
  });
  assert.equal(feedback.some((event) => event.state === "connecting"), true);
  assert.equal(feedback.some((event) => event.state === "degraded"), true);
  assert.equal(feedback[feedback.length - 1]?.state, "connecting");
  FakeWebSocket.instances[1].receive({ type: "subscribed", id: "bar:000001.SZ:5f" });
  assert.equal(feedback[feedback.length - 1]?.channels.bars.state, "live");
  assert.equal(feedback[feedback.length - 1]?.channels.chan.state, "connecting");
  release();
});

test("realtime feedback preserves bars and Chan state until each channel recovers", async (t) => {
  const original = globalThis.WebSocket;
  (globalThis as { WebSocket?: typeof WebSocket }).WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  FakeWebSocket.instances = [];
  const manager = new ChartDataManager();
  const feedback: RealtimeFeedback[] = [];
  const releaseFeedback = manager.subscribeRealtimeFeedback((event) => feedback.push(event));
  t.after(() => {
    manager.resetSession();
    releaseFeedback();
    globalThis.WebSocket = original;
  });

  const releaseBars = await manager.subscribeRealtimeBars({
    symbol: "000001.SZ", timeframe: "5f",
  }, () => {}, () => {});
  const releaseChan = await manager.subscribeChanOverlay({
    symbol: "000001.SZ", timeframe: "5f", levels: ["5f", "30f", "1d"],
    modes: ["confirmed", "predictive"], from: 100, to: 200, limit: 300,
  }, () => {}, () => {});
  await sleep(0);

  const [barSocket, chanSocket] = FakeWebSocket.instances;
  assert.equal(feedback[feedback.length - 1]?.state, "connecting");
  barSocket.receive({ type: "subscribed", id: "bar:000001.SZ:5f" });
  assert.equal(feedback[feedback.length - 1]?.channels.bars.state, "live");
  assert.equal(feedback[feedback.length - 1]?.channels.chan.state, "connecting");

  const chanSubscriptionId = (chanSocket.sent[0] as { id: string }).id;
  chanSocket.receive({ type: "chan_subscribed", id: chanSubscriptionId });
  assert.equal(feedback[feedback.length - 1]?.state, "live");

  chanSocket.receive({ type: "chan_resync_required", id: chanSubscriptionId });
  assert.equal(feedback[feedback.length - 1]?.state, "degraded");
  assert.deepEqual(feedback[feedback.length - 1]?.degradedChannels, ["chan"]);
  assert.equal(feedback[feedback.length - 1]?.channels.bars.state, "live");
  assert.equal(feedback[feedback.length - 1]?.channels.chan.state, "degraded");

  barSocket.receive({
    type: "bar_update",
    symbol: "000001.SZ",
    timeframe: "5f",
    bar: { time: 100, open: 1, high: 2, low: 1, close: 2, volume: 10, revision: 1, complete: false },
  });
  assert.deepEqual(feedback[feedback.length - 1]?.degradedChannels, ["chan"]);

  manager.markChanOverlayLive();
  assert.equal(feedback[feedback.length - 1]?.state, "live");
  assert.deepEqual(feedback[feedback.length - 1]?.degradedChannels, []);
  releaseBars();
  releaseChan();
});

test("workspace renders independent bars and Chan realtime feedback", () => {
  const source = readFileSync(new URL("../components/ChartWorkspace.tsx", import.meta.url), "utf8");
  assert.match(source, /realtimeFeedback\.channels\.bars\.state/);
  assert.match(source, /realtimeFeedback\.channels\.chan\.state/);
  assert.match(source, /data-degraded-channels=\{realtimeFeedback\.degradedChannels\.join\(","\)\}/);
});

test("late widget subscription is disposed after StrictMode-style cleanup", async () => {
  let resolve!: (dispose: () => void) => void;
  let current = true;
  let disposed = 0;
  let assigned = 0;
  const pending = installAsyncSubscription(
    () => new Promise((next) => { resolve = next; }),
    () => current,
    () => { assigned += 1; },
  );
  current = false;
  resolve(() => { disposed += 1; });
  assert.equal(await pending, false);
  assert.equal(disposed, 1);
  assert.equal(assigned, 0);
});
