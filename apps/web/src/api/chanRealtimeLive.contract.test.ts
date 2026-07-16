import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { ChartDataManager } from "./chartDataManager";
import { ChanRealtimeOverlayBridge } from "./chanRealtimeOverlayBridge";
import { CHAN_DELTA_FIXTURE, CHAN_SNAPSHOT_FIXTURE } from "./chanRealtimeOverlayBridge.fixtures";
import { installAsyncSubscription } from "../components/ChartWorkspace";

(globalThis as { window?: Window }).window = globalThis as unknown as Window;

class FakeWebSocket {
  static readonly OPEN = 1;
  static instances: FakeWebSocket[] = [];
  readyState = 0;
  sent: unknown[] = [];
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;

  constructor(_url: string) {
    FakeWebSocket.instances.push(this);
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN;
      this.onopen?.();
    });
  }

  send(payload: string): void { this.sent.push(JSON.parse(payload)); }
  close(): void { this.readyState = 3; this.onclose?.(); }
  receive(payload: unknown): void { this.onmessage?.({ data: JSON.stringify(payload) }); }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
