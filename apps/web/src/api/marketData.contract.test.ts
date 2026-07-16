import assert from "node:assert/strict";
import test from "node:test";

import { buildMarketQuoteFromDailyBars } from "./marketData";

test("quote uses previous daily close and latest daily totals", () => {
  const quote = buildMarketQuoteFromDailyBars("000001.SZ", null, [
    {
      time: 100,
      open: 10,
      high: 10.3,
      low: 9.9,
      close: 10.2,
      volume: 1_000,
      amount: 10_100,
      complete: true,
      revision: 0,
    },
    {
      time: 200,
      open: 10.3,
      high: 10.6,
      low: 10.1,
      close: 10.5,
      volume: 2_000,
      amount: 20_800,
      complete: true,
      revision: 0,
    },
  ]);

  assert.equal(quote.previousClose, 10.2);
  assert.equal(quote.price, 10.5);
  assert.equal(quote.change, 0.3);
  assert.equal(quote.changePercent, 2.94);
  assert.equal(quote.volume, 2_000);
  assert.equal(quote.amount, 20_800);
});
