import assert from "node:assert/strict";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { StockNewsPanel } from "./StockNewsPanel";

test("renders compact linked news with related symbol performance", () => {
  const html = renderToStaticMarkup(
    <StockNewsPanel
      activeSymbol="000001.SZ"
      feed={{
        symbol: "000001.SZ",
        source: "iwencai_news_search",
        asOf: "2026-07-12T09:30:00+08:00",
        stockNews: [{
          id: "news-1",
          title: "Compact headline",
          source: "Exchange Media",
          time: "2026-07-12T09:00:00+08:00",
          summary: "This body must not be rendered",
          url: "https://example.com/news-1",
          relatedSymbols: [{ symbol: "000001.SZ", changePercent: 1.25 }],
        }],
        globalNews: [],
      }}
    />,
  );

  assert.match(html, /href="https:\/\/example\.com\/news-1"/);
  assert.match(html, /target="_blank"/);
  assert.match(html, /Compact headline/);
  assert.match(html, /Exchange Media/);
  assert.match(html, /000001\.SZ/);
  assert.match(html, /\+1\.25%/);
  assert.doesNotMatch(html, /This body must not be rendered/);
});
