import assert from "node:assert/strict";
import test from "node:test";
import { createHttpMarketSidebarTransport, parseMarketSidebarBootstrap, parseMarketSidebarEvent, toWireContext, type MarketSidebarTransport, type SidebarContext } from "./marketSidebar";
import { chartDataManager } from "./chartDataManager";
import { MarketSidebarStore } from "./marketSidebarStore";

class FixtureTransport implements MarketSidebarTransport {
  bootstrapCalls = 0;
  contexts: SidebarContext[] = [];
  listener: (event: unknown) => boolean = () => false;
  response: unknown;

  constructor(symbol = "000001.SZ") { this.response = backendBootstrap(symbol, 0); }
  async bootstrap(): Promise<unknown> { this.bootstrapCalls += 1; return this.response; }
  setContext(context: SidebarContext): void { this.contexts.push(structuredClone(context)); }
  subscribe(listener: (event: unknown) => boolean): () => void { this.listener = listener; return () => undefined; }
  push(event: unknown): void { this.listener(event); }
}

test("maps the real nested bootstrap and unavailable quote placeholders", () => {
  const snapshot = parseMarketSidebarBootstrap(backendBootstrap("000001.SZ", 18));
  assert.equal(snapshot.profileBySymbol["000001.SZ"].name, "Profile 000001.SZ");
  assert.equal(snapshot.profileBySymbol["000001.SZ"].latestPrice, 10);
  assert.equal(snapshot.profileBySymbol["000001.SZ"].turnoverRate, 3.2);
  assert.equal(snapshot.profileBySymbol["000001.SZ"].fundFlow.net, 1000);
  assert.deepEqual(snapshot.strength, {
    score: 88,
    leaders: [{ name: "Ping An Bank", changePercent: 2.18 }],
    themes: [{ name: "Bank", changePercent: 1.25, mainNetInflowWan: 12500 }],
    source: "westock_data",
    freshness: "live",
  });
  assert.equal(snapshot.profileBySymbol["000001.SZ"].fundFlow.main, 1200);
  assert.deepEqual(snapshot.profileBySymbol["000001.SZ"].concepts, [{ name: "Bank", changePercent: null }]);
  assert.deepEqual(snapshot.quotesBySymbol["600000.SH"], {
    symbol: "600000.SH", name: "600000.SH", exchange: "SH", price: null,
    previousClose: null, change: null, changePercent: null, volume: null,
    amount: null, time: null, source: "placeholder",
  });
  assert.equal(snapshot.newsBySymbol["000001.SZ"].stockNews[0].id, "news-1");
  assert.deepEqual(snapshot.newsBySymbol["000001.SZ"].stockNews[0], {
    id: "news-1",
    title: "Notice",
    source: "Exchange Media",
    time: "2026-07-12T09:00:00+08:00",
    summary: "Fact",
    url: "https://example.com/news-1",
    tags: ["filing"],
    relatedSymbols: [{ symbol: "000001.SZ", changePercent: 2 }],
  });
});

test("wire context retains watchlist identity and revision", () => {
  assert.deepEqual(toWireContext({ chartSymbol: "A", chartEpoch: 3, watchlistId: "favorites", watchlistSymbols: ["B"], watchlistRevision: 9 }), {
    chart_symbol: "A", chart_epoch: 3, watchlist_id: "favorites", watchlist_symbols: ["B"], watchlist_revision: 9,
  });
});

test("uses one initial bootstrap and keeps watchlist revisions independent", async () => {
  const transport = new FixtureTransport();
  const store = new MarketSidebarStore(transport, "000001.SZ", ["600000.SH"]);
  await store.start();
  await store.start();
  store.setWatchlistSymbols(["600000.SH", "000002.SZ"]);
  assert.equal(transport.bootstrapCalls, 1);
  assert.equal(store.getSnapshot().context.chartEpoch, 0);
  assert.equal(store.getSnapshot().context.watchlistRevision, 8);
});

test("late bootstrap cannot roll back a newer watchlist revision", async () => {
  let resolveBootstrap: (value: unknown) => void = () => undefined;
  const transport = new FixtureTransport();
  transport.response = new Promise((resolve) => { resolveBootstrap = resolve; });
  const store = new MarketSidebarStore(transport, "000001.SZ", ["600000.SH"]);

  const started = store.start();
  store.setWatchlistSymbols(["000002.SZ"]);
  const stale = backendBootstrap("000001.SZ", 0) as { context: Record<string, unknown> };
  stale.context.watchlist_revision = 0;
  resolveBootstrap(stale);
  await started;

  assert.equal(store.getSnapshot().context.watchlistRevision, 1);
  assert.deepEqual(store.getSnapshot().context.watchlistSymbols, ["000002.SZ"]);
});

test("parses the server sidebar event context and payload fields", () => {
  const quotes = parseMarketSidebarEvent({ type: "watchlist_quote_delta", ...wireEventContext("000001.SZ", 2, 11), quotes: { "600000.SH": { source: "normalized_snapshot", freshness: "unavailable" } } });
  assert.equal(quotes.type, "watchlist_quote_delta");
  if (quotes.type === "watchlist_quote_delta") {
    assert.equal(quotes.quotes[0].symbol, "600000.SH");
    assert.equal(quotes.streamId, "stream-a");
    assert.equal(quotes.watchlistRevision, 7);
  }
  assert.equal(parseMarketSidebarEvent({ type: "strength_delta", ...wireEventContext("000001.SZ", 2, 12), strength: { items: [] } }).type, "strength_delta");
  assert.equal(parseMarketSidebarEvent(newsEvent("000001.SZ", 2, 13)).type, "news_delta");
});

test("watchlist identity fence rejects in-flight events from the previous revision", async () => {
  const transport = new FixtureTransport();
  const store = new MarketSidebarStore(transport, "000001.SZ", ["600000.SH"]);
  await store.start();
  transport.push(profileEvent("000001.SZ", 0, 1));
  store.setWatchlistSymbols(["000002.SZ"]);
  transport.push(quoteEvent("000001.SZ", 0, 2, 7, "600000.SH", 99));
  assert.equal(store.getSnapshot().quotesBySymbol["600000.SH"]?.price, null);
  transport.push(quoteEvent("000001.SZ", 0, 2, 8, "000002.SZ", 12));
  assert.equal(store.getSnapshot().quotesBySymbol["000002.SZ"].price, 12);
});

test("A -> B -> A resets each epoch cursor at 1 and rejects delayed old epochs", async () => {
  const transport = new FixtureTransport();
  const store = new MarketSidebarStore(transport, "000001.SZ");
  await store.start();
  const initialNews = store.getSnapshot().newsBySymbol["000001.SZ"];
  store.confirmChartSymbol("600000.SH");
  store.confirmChartSymbol("000001.SZ");
  transport.push(profileEvent("600000.SH", 1, 1));
  transport.push(newsEvent("000001.SZ", 0, 2));
  assert.equal(store.getSnapshot().profileBySymbol["600000.SH"], undefined);
  assert.equal(store.getSnapshot().newsBySymbol["000001.SZ"], initialNews);
  transport.push(profileEvent("000001.SZ", 2, 1));
  transport.push(newsEvent("000001.SZ", 2, 2));
  assert.equal(store.getSnapshot().profileBySymbol["000001.SZ"].name, "Profile 000001.SZ");
  assert.equal(store.getSnapshot().newsBySymbol["000001.SZ"].stockNews[0].id, "news-1");
});

test("resync adopts the event snapshot and cursor without a second bootstrap", async () => {
  const transport = new FixtureTransport();
  const store = new MarketSidebarStore(transport, "000001.SZ");
  await store.start();
  const event = resyncEvent("000001.SZ", 0, 11, "stream-a", backendBootstrap("000001.SZ", 0, 20));
  transport.push(event);
  transport.push(event);
  assert.equal(transport.bootstrapCalls, 1);
  assert.equal(store.getSnapshot().sequence, 11);
  assert.equal(store.getSnapshot().snapshotVersion, 11);
});

test("runtime schema still fences mismatched profiles and malformed epochs", () => {
  const mismatch = backendBootstrap("000001.SZ", 0) as Record<string, unknown>;
  mismatch.active_symbol_profile = nestedProfile("600000.SH");
  assert.throws(() => parseMarketSidebarBootstrap(mismatch), /does not match/);
  const malformed = backendBootstrap("000001.SZ", 0) as { context: Record<string, unknown> };
  malformed.context.chart_epoch = "0";
  assert.throws(() => parseMarketSidebarBootstrap(malformed), /must be an integer/);
});

test("bar and sidebar share one realtime socket, replay, deliver, and unsubscribe", async () => {
  const originalWebSocket = globalThis.WebSocket;
  const originalFetch = globalThis.fetch;
  (globalThis as { window?: Window }).window = globalThis as unknown as Window;
  globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
  globalThis.fetch = (async () => new Response(JSON.stringify(backendBootstrap("000001.SZ", 0)), { status: 200, headers: { "Content-Type": "application/json" } })) as unknown as typeof fetch;
  FakeWebSocket.instances = [];
  let releaseBars: (() => void) | null = null;
  try {
    releaseBars = await chartDataManager.subscribeRealtimeBars(
      { symbol: "000001.SZ", timeframe: "5f" },
      () => undefined,
      () => undefined,
    );
    const store = new MarketSidebarStore(createHttpMarketSidebarTransport("token"), "000001.SZ", ["600000.SH"]);
    await store.start();
    await sleep(0);
    assert.equal(FakeWebSocket.instances.length, 1);
    const first = FakeWebSocket.instances[0];
    assert.deepEqual(first.sent.map((item) => (item as { type: string }).type).sort(), ["set_sidebar_context", "subscribe"]);
    const sidebarSet = first.sent.find((item) => (item as { type?: string }).type === "set_sidebar_context") as Record<string, unknown>;
    assert.equal(sidebarSet.subscription_id, "right-sidebar");
    assert.equal(sidebarSet.after_sequence, 0);
    assert.equal(sidebarSet.snapshot_version, 0);
    first.receive(profileEvent("000001.SZ", 0, 3, "connection-a"));
    assert.equal(store.getSnapshot().profileBySymbol["000001.SZ"].name, "Profile 000001.SZ");

    first.close();
    await sleep(1_100);
    const second = FakeWebSocket.instances[1];
    assert.ok(second);
    assert.deepEqual(second.sent.map((item) => (item as { type: string }).type).sort(), ["set_sidebar_context", "subscribe"]);
    const replay = second.sent.find((item) => (item as { type?: string }).type === "set_sidebar_context") as Record<string, unknown>;
    assert.equal(replay.after_sequence, 3);
    assert.equal(replay.snapshot_version, 3);
    second.receive(resyncEvent("000001.SZ", 0, 1, "connection-b", backendBootstrap("000001.SZ", 0, 1)));
    assert.equal(store.getSnapshot().sequence, 1);
    second.receive(profileEvent("000001.SZ", 0, 4, "connection-a"));
    assert.equal(store.getSnapshot().sequence, 1);
    const recovered = second.sent[second.sent.length - 1] as Record<string, unknown>;
    assert.equal(recovered.after_sequence, 1);
    assert.equal(recovered.snapshot_version, 1);

    store.dispose();
    releaseBars();
    releaseBars = null;
    assert.deepEqual(second.sent.slice(-2), [
      { type: "unsubscribe", id: "right-sidebar" },
      { type: "unsubscribe", id: "bar:000001.SZ:5f" },
    ]);
  } finally {
    releaseBars?.();
    globalThis.WebSocket = originalWebSocket;
    globalThis.fetch = originalFetch;
  }
});

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
    queueMicrotask(() => { this.readyState = FakeWebSocket.OPEN; this.onopen?.(); });
  }
  send(payload: string): void { this.sent.push(JSON.parse(payload)); }
  close(): void { this.readyState = 3; this.onclose?.(); }
  receive(payload: unknown): void { this.onmessage?.({ data: JSON.stringify(payload) }); }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function backendBootstrap(symbol: string, epoch: number, sequence = 10): unknown {
  return {
    context: { chart_symbol: symbol, chart_epoch: epoch, watchlist_id: "default", watchlist_revision: 7 },
    watchlist_quotes: { "600000.SH": { source: "normalized_snapshot", freshness: "unavailable" } },
    active_symbol_profile: nestedProfile(symbol), strongest_preview: { score: 88, leaders: ["000001.SZ"], themes: ["Bank"], leader_details: [{ name: "Ping An Bank", change_percent: 2.18 }], theme_details: [{ name: "Bank", change_percent: 1.25, main_net_inflow_wan: 12500 }], source: "westock_data", freshness: "live" },
    news_preview: backendNews(symbol, epoch), snapshot_version: sequence, sequence,
  };
}

function nestedProfile(symbol: string) {
  return {
    symbol,
    quote: { symbol, price: 10, change: 0.2, change_percent: 2, volume: 100, amount: 1000, source: "westock_data", freshness: "live" },
    identity: { symbol, name: `Profile ${symbol}`, exchange: symbol.slice(-2), industry: "Bank", source: "westock_data", freshness: "live" },
    valuation: { market_cap: 100000, pe_ratio: 6, turnover_rate: 3.2, source: "westock_data", freshness: "live" },
    capital_flow: { net_inflow: 1000, main_net_inflow: 1200, small_net_inflow: -200, source: "westock_data", freshness: "live" },
    themes: ["Bank"], chan_state: { source: "canonical_module_c", freshness: "unavailable" }, strategy_signals: [],
  };
}

function backendNews(symbol: string, epoch: number) {
  return { symbol, chart_epoch: epoch, status: "fresh", source: "iwencai_news_search", as_of: "2026-07-12T09:30:00+08:00", items: [{ event_id: "news-1", title: "Notice", source: "Exchange", published_at: "2026-07-12T09:00:00+08:00", fact_summary: "Fact", impact_tags: ["filing"], sources: [{ name: "Exchange Media", url: "https://example.com/news-1" }], related_symbols: [{ symbol, change_percent: 2 }] }] };
}

function wireEventContext(symbol: string, epoch: number, sequence: number, streamId = "stream-a", watchlistRevision = 7) {
  return { subscription_id: "right-sidebar", stream_id: streamId, chart_symbol: symbol, chart_epoch: epoch, watchlist_id: "default", watchlist_revision: watchlistRevision, sequence, snapshot_version: sequence };
}

function profileEvent(symbol: string, epoch: number, sequence: number, streamId = "stream-a") {
  return { type: "active_profile_delta", ...wireEventContext(symbol, epoch, sequence, streamId), profile: nestedProfile(symbol) };
}

function newsEvent(symbol: string, epoch: number, sequence: number, streamId = "stream-a") {
  return { type: "news_delta", ...wireEventContext(symbol, epoch, sequence, streamId), source: "iwencai_news_search", news: backendNews(symbol, epoch) };
}

function quoteEvent(symbol: string, epoch: number, sequence: number, watchlistRevision: number, quoteSymbol: string, price: number) {
  return { type: "watchlist_quote_delta", ...wireEventContext(symbol, epoch, sequence, "stream-a", watchlistRevision), quotes: { [quoteSymbol]: { symbol: quoteSymbol, price, freshness: "live" } } };
}

function resyncEvent(symbol: string, epoch: number, sequence: number, streamId: string, snapshot: unknown) {
  return { type: "sidebar_resync_required", ...wireEventContext(symbol, epoch, sequence, streamId), reason: "cursor_mismatch", snapshot };
}
