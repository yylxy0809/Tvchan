import test from "node:test";
import assert from "node:assert/strict";

import { __CHAN_STUDY_TESTING__ } from "./chanStudy";

const {
  applyPivotBreak,
  buildBspMapForView,
  buildStrokePointsForView,
  findPivotOverlap,
  lineToRawStroke,
  mapChanTimeToViewTime,
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
        startTime: ms("2026-06-10T10:00:00.000Z"),
        startPrice: 10.0,
        endTime: shared,
        endPrice: 10.8,
        direction: "up",
      },
      {
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
