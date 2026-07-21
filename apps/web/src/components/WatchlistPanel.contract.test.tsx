import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { WatchlistPanel } from "./WatchlistPanel";

test("labels Notte profiles as AnythingAPI", () => {
  const html = renderToStaticMarkup(
    <WatchlistPanel
      activeSymbol="000001.SZ"
      onSelectSymbol={() => undefined}
      onWatchlistSymbolsChange={() => undefined}
      quotes={{}}
      profile={{
        symbol: "000001.SZ", name: "Ping An", exchange: "SZ", code: "000001", assetType: "stock",
        latestPrice: null, dayChangePercent: null, volume: null, amount: null, sector: null, concepts: [],
        marketCap: null, peRatio: null, turnoverRate: null, fundFlow: { net: null, main: null, retail: null },
        chanStrokeStates: [], strategySignals: [], source: "notte", freshness: "fresh",
        asOf: "2026-07-12T09:30:00+08:00", tradingDate: "2026-07-12",
      }}
      sidebarStatus={{ state: "ready" }}
    />,
  );

  assert.match(html, /AnythingAPI \/ 2026-07-12T09:30:00\+08:00 \/ Fresh/);
  assert.doesNotMatch(html, /板块与概念/);
  assert.doesNotMatch(html, /估值与活跃度/);
  assert.doesNotMatch(html, /资金流向/);
  assert.doesNotMatch(html, /上游未返回/);
  assert.match(html, /缠论状态/);
  assert.match(html, /策略信号/);
});

test("renders parser failures as an unavailable profile error", () => {
  const html = renderToStaticMarkup(
    <WatchlistPanel
      activeSymbol="000001.SZ"
      onSelectSymbol={() => undefined}
      onWatchlistSymbolsChange={() => undefined}
      quotes={{}}
      profile={null}
      sidebarStatus={{ state: "error", message: "quote must be an object" }}
    />,
  );

  assert.match(html, /iWencai data unavailable: quote must be an object/);
  assert.match(html, /iWencai \/ Unavailable/);
  assert.match(html, /role="alert"/);
});

test("renders Chan stroke status with Chinese labels", () => {
  const html = renderToStaticMarkup(
    <WatchlistPanel
      activeSymbol="000001.SZ"
      onSelectSymbol={() => undefined}
      onWatchlistSymbolsChange={() => undefined}
      quotes={{}}
      profile={{
        symbol: "000001.SZ", name: "平安银行", exchange: "SZ", code: "000001", assetType: "stock",
        latestPrice: null, dayChangePercent: null, volume: null, amount: null, sector: null, concepts: [],
        marketCap: null, peRatio: null, turnoverRate: null, fundFlow: { net: null, main: null, retail: null },
        chanStrokeStates: [
          {
            level: "5f", label: "5f stroke", direction: "unknown", stateLabel: "5f unavailable",
            mode: "predictive", modeLabel: "Predictive", confirmed: false, anchorTime: null, anchorPrice: null,
          },
          {
            level: "1d", label: "1d stroke", direction: "up", stateLabel: "Up",
            mode: "confirmed", modeLabel: "Confirmed", confirmed: true, anchorTime: null, anchorPrice: null,
          },
        ],
        strategySignals: [], source: "iwencai", freshness: "fresh",
        asOf: "2026-07-12T09:30:00+08:00", tradingDate: "2026-07-12",
      }}
      sidebarStatus={{ state: "ready" }}
    />,
  );

  assert.match(html, /5分钟笔/);
  assert.match(html, /暂无数据/);
  assert.match(html, /预测/);
  assert.match(html, /日线笔/);
  assert.match(html, /向上/);
  assert.match(html, /已确认/);
  assert.doesNotMatch(html, /5f stroke|5f unavailable|Predictive|Confirmed|>Up</);
});

test("renders stable strategy lifecycle identity, status, and event time", () => {
  const html = renderToStaticMarkup(
    <WatchlistPanel
      activeSymbol="000001.SZ"
      onSelectSymbol={() => undefined}
      onWatchlistSymbolsChange={() => undefined}
      quotes={{}}
      profile={{
        symbol: "000001.SZ", name: "平安银行", exchange: "SZ", code: "000001", assetType: "stock",
        latestPrice: null, dayChangePercent: null, volume: null, amount: null, sector: null, concepts: [],
        marketCap: null, peRatio: null, turnoverRate: null, fundFlow: { net: null, main: null, retail: null },
        chanStrokeStates: [], strategySignals: [{
          key: "b2", eventId: "evt-stable-42", label: "二买", value: "confirmed", tone: "up", source: "local_db",
          eventType: "strategy_lifecycle", status: "active", sourceLevel: "1d", sourceSignalType: "b2",
          sourceSignalSide: "buy", pointTime: "2026-07-21T09:35:00+08:00",
          firstSeenTime: "2026-07-21T09:36:00+08:00", confirmTime: "2026-07-21T09:37:00+08:00",
          disappearTime: null, sourceSnapshotVersion: "snapshot-9", confidenceScore: 0.9, strengthScore: 0.8,
        }], source: "iwencai", freshness: "fresh", asOf: "2026-07-21T09:37:00+08:00", tradingDate: "2026-07-21",
      }}
      sidebarStatus={{ state: "ready" }}
    />,
  );

  assert.match(html, /evt-stable-42/);
  assert.match(html, /active/);
  assert.match(html, /2026-07-21T09:37:00\+08:00/);
});
