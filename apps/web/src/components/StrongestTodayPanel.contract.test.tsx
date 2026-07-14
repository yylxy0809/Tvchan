import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { StrongestTodayPanel } from "./StrongestTodayPanel";

test("labels Notte strength data as AnythingAPI", () => {
  const html = renderToStaticMarkup(<StrongestTodayPanel marketSnapshot={{ strength: {
    freshness: "fresh", score: 88, leaders: [], themes: [], source: "notte",
    asOf: "2026-07-12T09:30:00+08:00", tradingDate: "2026-07-12",
  } }} />);

  assert.match(html, /AnythingAPI/);
  assert.doesNotMatch(html, /强势标的/);
  assert.doesNotMatch(html, /市场主题/);
});

test("renders unavailable and stale strength from the market sidebar snapshot", () => {
  const unavailable = renderToStaticMarkup(<StrongestTodayPanel marketSnapshot={{ strength: { freshness: "unavailable", score: null, leaders: [], themes: [], source: "iwencai", asOf: "2026-07-12T09:30:00+08:00", tradingDate: "2026-07-12" } }} />);
  const stale = renderToStaticMarkup(<StrongestTodayPanel marketSnapshot={{ strength: {
    freshness: "stale",
    score: 88,
    leaders: [{ name: "Ping An Bank", changePercent: 2.18 }],
    themes: [{ name: "Bank", changePercent: 1.25, mainNetInflowWan: 12500 }],
    source: "iwencai",
    asOf: "2026-07-12T09:30:00+08:00",
    tradingDate: "2026-07-12",
  } }} />);

  assert.doesNotMatch(unavailable, /强度评分/);
  assert.match(stale, /Stale/);
  assert.match(stale, /88/);
  assert.match(stale, /Ping An Bank/);
  assert.match(stale, /\+2\.18%/);
  assert.match(stale, /\+1\.25%/);
  assert.match(stale, /\+1\.25亿/);
});

test("does not render empty strength blocks", () => {
  const source = readFileSync(new URL("./StrongestTodayPanel.tsx", import.meta.url), "utf8");
  assert.doesNotMatch(source, /function EmptyState/);
});

test("keeps theme change percent beside the theme name", () => {
  const html = renderToStaticMarkup(<StrongestTodayPanel marketSnapshot={{ strength: {
    freshness: "fresh",
    score: null,
    leaders: [],
    themes: [{ name: "玻纤制造", changePercent: 7.8, mainNetInflowWan: 165200 }],
    source: "iwencai",
    asOf: "2026-07-14T09:30:00+08:00",
    tradingDate: "2026-07-14",
  } }} />);

  assert.match(
    html,
    /tv-strength-row-head[^>]*><strong>玻纤制造<\/strong><em[^>]*>\+7\.80%<\/em><\/div><small>主力净流入 \+16\.52亿<\/small>/,
  );
});
