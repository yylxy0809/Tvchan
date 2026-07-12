import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { StrongestTodayPanel } from "./StrongestTodayPanel";

test("renders unavailable and stale strength from the market sidebar snapshot", () => {
  const unavailable = renderToStaticMarkup(<StrongestTodayPanel marketSnapshot={{ strength: { freshness: "unavailable", score: null, leaders: [], themes: [], source: "normalized_snapshot" } }} />);
  const stale = renderToStaticMarkup(<StrongestTodayPanel marketSnapshot={{ strength: {
    freshness: "stale",
    score: 88,
    leaders: [{ name: "Ping An Bank", changePercent: 2.18 }],
    themes: [{ name: "Bank", changePercent: 1.25, mainNetInflowWan: 12500 }],
    source: "westock_data",
  } }} />);

  assert.match(unavailable, /Unavailable/);
  assert.match(stale, /Stale/);
  assert.match(stale, /88/);
  assert.match(stale, /Ping An Bank/);
  assert.match(stale, /\+2\.18%/);
  assert.match(stale, /\+1\.25%/);
  assert.match(stale, /\+1\.25亿/);
});
