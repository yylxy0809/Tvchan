import assert from "node:assert/strict";
import test from "node:test";

import {
  ChanRealtimeOverlayBridge,
  ChanRealtimePollingGate,
  chanRealtimeContextKey,
  type ChanOverlayEvent,
  type ChanRealtimeContext,
  type ChanRealtimeEnvelope,
} from "./chanRealtimeOverlayBridge";
import {
  CHAN_DELTA_FIXTURE,
  CHAN_EMPTY_DELTA_FIXTURE,
  CHAN_REALTIME_ROUND_TRIP_FIXTURES,
  CHAN_RESYNC_REQUIRED_FIXTURE,
  CHAN_SNAPSHOT_FIXTURE,
  MALFORMED_EVENT_FIXTURES,
} from "./chanRealtimeOverlayBridge.fixtures";

const context: ChanRealtimeContext = {
  symbol: "000001.SZ",
  chartTimeframe: "5f",
  modes: ["confirmed", "predictive"],
};

test("backend envelopes survive a typed JSON round trip", () => {
  const roundTripped = JSON.parse(
    JSON.stringify(CHAN_REALTIME_ROUND_TRIP_FIXTURES),
  ) as ChanRealtimeEnvelope[];
  assert.deepEqual(roundTripped, CHAN_REALTIME_ROUND_TRIP_FIXTURES);
  assert.deepEqual(
    roundTripped.map((event) => event.type),
    ["chan_overlay", "chan_overlay", "chan_resync_required"],
  );
});

test("snapshot and matching delta commit stable-id changes", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  const snapshot = bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  assert.equal(snapshot.status, "applied");
  assert.deepEqual(
    bridge.getState(context)?.objects.strokes.map((item) => item.id),
    ["stroke-a"],
  );

  const delta = bridge.apply(CHAN_DELTA_FIXTURE);
  assert.equal(delta.status, "applied");
  const state = bridge.getState(context);
  assert.equal(state?.snapshotVersion, CHAN_DELTA_FIXTURE.snapshot_version);
  assert.equal(state?.sequence, 2);
  assert.deepEqual(
    state?.objects.strokes.map((item) => item.id),
    ["stroke-b"],
  );
  assert.deepEqual(state?.objects.channels.map((item) => item.id), ["channel-b"]);
});

test("channels add, update, and delete by stable id", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  bridge.apply(CHAN_DELTA_FIXTURE);
  const updated = bridge.apply({
    ...CHAN_DELTA_FIXTURE,
    sequence: 3,
    base_version: CHAN_DELTA_FIXTURE.snapshot_version,
    snapshot_version: "channels-v3",
    upserts: {
      strokes: [], segments: [], centers: [], signals: [],
      channels: [{ ...CHAN_DELTA_FIXTURE.upserts.channels[0], upper: 13 }],
    },
    deletes: { strokes: [], segments: [], centers: [], signals: [], channels: [] },
  });
  assert.equal(updated.status, "applied");
  assert.equal(bridge.getState(context)?.objects.channels[0]?.upper, 13);
  const removed = bridge.apply({
    ...CHAN_DELTA_FIXTURE,
    sequence: 4,
    base_version: "channels-v3",
    snapshot_version: "channels-v4",
    upserts: { strokes: [], segments: [], centers: [], signals: [], channels: [] },
    deletes: { strokes: [], segments: [], centers: [], signals: [], channels: ["channel-b"] },
  });
  assert.equal(removed.status, "applied");
  assert.deepEqual(bridge.getState(context)?.objects.channels, []);
});

test("HTTP hydration establishes version without resetting observed WS sequence", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  bridge.apply(CHAN_RESYNC_REQUIRED_FIXTURE);
  const hydrated = bridge.hydrateHttp({
    ...context,
    snapshotVersion: "http-v3",
    range: CHAN_SNAPSHOT_FIXTURE.range,
    objects: CHAN_SNAPSHOT_FIXTURE.upserts,
  });
  assert.equal(hydrated.snapshotVersion, "http-v3");
  assert.equal(hydrated.sequence, 3);

  const next = bridge.apply({
    ...CHAN_DELTA_FIXTURE,
    sequence: 4,
    base_version: "http-v3",
    snapshot_version: "ws-v4",
  });
  assert.equal(next.status, "applied");
  assert.equal(bridge.getState(context)?.snapshotVersion, "ws-v4");
});

test("duplicate and older events are ignored", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  bridge.apply(CHAN_DELTA_FIXTURE);

  assert.deepEqual(bridge.apply(CHAN_DELTA_FIXTURE), {
    status: "ignored",
    reason: "duplicate_or_older",
    state: bridge.getState(context),
  });
  assert.equal(bridge.apply(CHAN_SNAPSHOT_FIXTURE).status, "ignored");
  assert.equal(bridge.getState(context)?.sequence, 2);
});

test("sequence gap returns one bounded resync and retains last valid overlay", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  const gap = {
    ...CHAN_DELTA_FIXTURE,
    sequence: 4,
  } as const satisfies ChanOverlayEvent;

  const first = bridge.apply(gap);
  assert.equal(first.status, "resync");
  if (first.status !== "resync") return;
  assert.deepEqual(first.instruction, {
    type: "http_overlay_resync",
    key: chanRealtimeContextKey(context),
    symbol: "000001.SZ",
    chartTimeframe: "5f",
    modes: ["confirmed", "predictive"],
    range: CHAN_SNAPSHOT_FIXTURE.range,
    reason: "sequence_gap",
  });
  assert.equal(first.state?.snapshotVersion, CHAN_SNAPSHOT_FIXTURE.snapshot_version);
  assert.deepEqual(
    first.state?.objects.strokes.map((item) => item.id),
    ["stroke-a"],
  );

  const second = bridge.apply({ ...gap, sequence: 5 });
  assert.equal(second.status, "ignored");
  assert.equal(second.status === "ignored" && second.reason, "resync_pending");
});

test("base mismatch requests one resync without clearing committed state", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  const mismatch = {
    ...CHAN_DELTA_FIXTURE,
    base_version: "not-the-cached-version",
  } as const satisfies ChanOverlayEvent;

  const result = bridge.apply(mismatch);
  assert.equal(result.status, "resync");
  assert.equal(
    result.status === "resync" && result.instruction.reason,
    "base_version_mismatch",
  );
  assert.equal(
    bridge.getState(context)?.snapshotVersion,
    CHAN_SNAPSHOT_FIXTURE.snapshot_version,
  );
  assert.deepEqual(
    bridge.getState(context)?.objects.strokes.map((item) => item.id),
    ["stroke-a"],
  );
});

test("empty delta is malformed and cannot advance committed state", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  const before = bridge.getState(context);

  assert.deepEqual(bridge.apply(CHAN_EMPTY_DELTA_FIXTURE), {
    status: "ignored",
    reason: "malformed",
  });
  assert.deepEqual(bridge.getState(context), before);

  const validAtSameSequence = bridge.apply(CHAN_DELTA_FIXTURE);
  assert.equal(validAtSameSequence.status, "applied");
  assert.equal(bridge.getState(context)?.sequence, 2);
  assert.equal(
    bridge.getState(context)?.snapshotVersion,
    CHAN_DELTA_FIXTURE.snapshot_version,
  );
});

test("upsert-only and delete-only deltas remain producer-valid changes", () => {
  const upsertBridge = new ChanRealtimeOverlayBridge();
  upsertBridge.apply(CHAN_SNAPSHOT_FIXTURE);
  const upsertOnly = {
    ...CHAN_DELTA_FIXTURE,
    deletes: {
      strokes: [],
      segments: [],
      centers: [],
      signals: [],
      channels: [],
    },
  } as const satisfies ChanOverlayEvent;
  assert.equal(upsertBridge.apply(upsertOnly).status, "applied");
  assert.deepEqual(
    upsertBridge.getState(context)?.objects.strokes.map((item) => item.id),
    ["stroke-a", "stroke-b"],
  );

  const deleteBridge = new ChanRealtimeOverlayBridge();
  deleteBridge.apply(CHAN_SNAPSHOT_FIXTURE);
  const deleteOnly = {
    ...CHAN_DELTA_FIXTURE,
    upserts: {
      strokes: [],
      segments: [],
      centers: [],
      signals: [],
      channels: [],
    },
  } as const satisfies ChanOverlayEvent;
  assert.equal(deleteBridge.apply(deleteOnly).status, "applied");
  assert.deepEqual(deleteBridge.getState(context)?.objects.strokes, []);
});

test("server resync instruction is deduplicated and next snapshot heals state", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  bridge.apply(CHAN_DELTA_FIXTURE);
  const resync = bridge.apply(CHAN_RESYNC_REQUIRED_FIXTURE);
  assert.equal(resync.status, "resync");
  assert.equal(
    resync.status === "resync" && resync.instruction.reason,
    "server_resync_required",
  );

  const repeated = bridge.apply({
    ...CHAN_RESYNC_REQUIRED_FIXTURE,
    sequence: 4,
  });
  assert.equal(repeated.status, "ignored");

  const healed = bridge.apply({
    ...CHAN_SNAPSHOT_FIXTURE,
    sequence: 5,
    snapshot_version: "healed-v5",
    upserts: {
      ...CHAN_SNAPSHOT_FIXTURE.upserts,
      strokes: [{ id: "healed-stroke" }],
    },
  });
  assert.equal(healed.status, "applied");
  assert.equal(bridge.getState(context)?.snapshotVersion, "healed-v5");
});

test("state keys normalize symbol and mode ordering but isolate timeframes", () => {
  assert.equal(
    chanRealtimeContextKey(context),
    chanRealtimeContextKey({
      symbol: "000001.sz",
      chartTimeframe: "5f",
      modes: ["predictive", "confirmed", "confirmed"],
    }),
  );
  assert.notEqual(
    chanRealtimeContextKey(context),
    chanRealtimeContextKey({ ...context, chartTimeframe: "30f" }),
  );
});

test("unsubscribe and reset remove all realtime state", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  assert.equal(bridge.unsubscribe(context), true);
  assert.equal(bridge.getState(context), undefined);
  assert.equal(bridge.unsubscribe(context), false);

  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  bridge.reset();
  assert.equal(bridge.getState(context), undefined);
});

test("disconnected polling starts once and stops on connect or replay", () => {
  let callback: (() => void) | undefined;
  const delays: number[] = [];
  const cancelled: unknown[] = [];
  let polls = 0;
  const gate = new ChanRealtimePollingGate(
    () => { polls += 1; },
    3_000,
    (next, delay) => { callback = next; delays.push(delay); return `timer-${delays.length}`; },
    (timer) => cancelled.push(timer),
  );
  gate.update("disconnected");
  gate.update("disconnected");
  callback?.();
  assert.deepEqual(delays, [3_000]);
  assert.equal(polls, 1);
  gate.update("connected");
  assert.deepEqual(cancelled, ["timer-1"]);
  gate.update("disconnected");
  gate.update("replayed");
  assert.deepEqual(delays, [3_000, 3_000]);
  assert.deepEqual(cancelled, ["timer-1", "timer-2"]);
  gate.dispose();
});

test("malformed untrusted events never throw, mutate state, or request resync", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  bridge.apply(CHAN_SNAPSHOT_FIXTURE);
  const before = bridge.getState(context);

  for (const malformed of MALFORMED_EVENT_FIXTURES) {
    assert.doesNotThrow(() => bridge.apply(malformed));
    assert.deepEqual(bridge.apply(malformed), {
      status: "ignored",
      reason: "malformed",
    });
    assert.deepEqual(bridge.getState(context), before);
  }

  const validDelta = bridge.apply(CHAN_DELTA_FIXTURE);
  assert.equal(validDelta.status, "applied");
  assert.equal(bridge.getState(context)?.sequence, 2);
});

test("malformed event for a new context creates no state", () => {
  const bridge = new ChanRealtimeOverlayBridge();
  const malformed = {
    ...CHAN_SNAPSHOT_FIXTURE,
    symbol: "600000.SH",
    sequence: 1,
    upserts: { strokes: null, segments: [], centers: [], signals: [] },
  };
  assert.deepEqual(bridge.apply(malformed), {
    status: "ignored",
    reason: "malformed",
  });
  assert.equal(
    bridge.getState({
      symbol: "600000.SH",
      chartTimeframe: "5f",
      modes: ["confirmed", "predictive"],
    }),
    undefined,
  );
});
