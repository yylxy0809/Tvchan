import { apiUrl } from "../config";
import { chartDataManager, type RealtimeSidebarContext } from "./chartDataManager";
import type { MarketQuote, SymbolProfile } from "./marketData";
import type { NewsItem, StockNewsFeed } from "./marketContracts";

export type SidebarContext = {
  chartSymbol: string;
  chartEpoch: number;
  watchlistId: string;
  watchlistSymbols: string[];
  watchlistRevision: number;
};

export type MarketSidebarSnapshot = {
  context: SidebarContext;
  quotesBySymbol: Record<string, MarketQuote>;
  profileBySymbol: Record<string, SymbolProfile>;
  newsBySymbol: Record<string, StockNewsFeed>;
  strength?: MarketStrengthSnapshot;
  snapshotVersion: number;
  sequence: number;
};

export type MarketSidebarEvent =
  | (MarketSidebarEventContext & { type: "watchlist_quote_delta"; quotes: MarketQuote[] })
  | (MarketSidebarEventContext & { type: "active_profile_delta"; profile: SymbolProfile })
  | (MarketSidebarEventContext & { type: "news_delta"; feed: StockNewsFeed })
  | (MarketSidebarEventContext & { type: "strength_delta"; strength: MarketStrengthSnapshot })
  | (MarketSidebarEventContext & { type: "sidebar_resync_required"; snapshot: MarketSidebarSnapshot });

type MarketSidebarEventContext = {
  subscriptionId: string;
  streamId: string;
  chartSymbol: string;
  chartEpoch: number;
  watchlistId: string;
  watchlistRevision: number;
  sequence: number;
  snapshotVersion: number;
};

export type MarketStrengthSnapshot = {
  score: number | null;
  leaders: Array<{ name: string; changePercent: number | null }>;
  themes: Array<{ name: string; changePercent: number | null; mainNetInflowWan: number | null }>;
  source: string;
  freshness: "live" | "delayed" | "stale" | "unavailable";
};

export interface MarketSidebarTransport {
  bootstrap(context: SidebarContext): Promise<unknown>;
  setContext(context: SidebarContext): void;
  subscribe(listener: (event: unknown) => boolean): () => void;
}

export function createHttpMarketSidebarTransport(token: string): MarketSidebarTransport {
  let context: SidebarContext | null = null;
  let listener: (event: unknown) => boolean = () => false;
  let release: (() => void) | null = null;
  let subscriptionPending: Promise<void> | null = null;
  let disposed = false;
  let cursor = { sequence: 0, snapshotVersion: 0, epoch: -1, streamId: "" };

  const realtimeContext = (): RealtimeSidebarContext => {
    if (!context) throw new Error("Sidebar context is not initialized");
    return {
      subscriptionId: "right-sidebar",
      chartSymbol: context.chartSymbol,
      chartEpoch: context.chartEpoch,
      watchlistId: context.watchlistId,
      watchlistRevision: context.watchlistRevision,
      watchlistSymbols: context.watchlistSymbols,
      channels: ["watchlist_quotes", "active_profile", "strength", "news"],
      afterSequence: cursor.epoch === context.chartEpoch ? cursor.sequence : 0,
      snapshotVersion: cursor.epoch === context.chartEpoch ? cursor.snapshotVersion : 0,
    };
  };

  const receive = (event: unknown) => {
    if (listener(event) && event && typeof event === "object" && context) {
      const wire = event as { sequence?: unknown; snapshot_version?: unknown; chart_epoch?: unknown };
      const streamId = optionalString((wire as { stream_id?: unknown }).stream_id);
      if (streamId && Number.isSafeInteger(wire.sequence) && Number.isSafeInteger(wire.snapshot_version)) {
        cursor = { sequence: wire.sequence as number, snapshotVersion: wire.snapshot_version as number, epoch: Number.isSafeInteger(wire.chart_epoch) ? wire.chart_epoch as number : context.chartEpoch, streamId };
      }
    }
    // ChartDataManager observes messages before this transport. Restore its replay
    // cursor to the last accepted sidebar event rather than a stale wire message.
    if (context) chartDataManager.updateRealtimeSidebarContext(realtimeContext());
  };

  const ensureSubscription = () => {
    if (!context || release || subscriptionPending || disposed) return;
    const requestedContext = realtimeContext();
    subscriptionPending = chartDataManager.subscribeRealtimeSidebar(requestedContext, receive)
      .then((dispose) => {
        subscriptionPending = null;
        if (disposed) {
          dispose();
          return;
        }
        release = dispose;
        const latestContext = realtimeContext();
        if (JSON.stringify(latestContext) !== JSON.stringify(requestedContext)) {
          chartDataManager.updateRealtimeSidebarContext(latestContext);
        }
      })
      .catch(() => { subscriptionPending = null; });
  };

  return {
    async bootstrap(nextContext) {
      context = { ...nextContext };
      if (cursor.epoch !== nextContext.chartEpoch) cursor = { sequence: 0, snapshotVersion: 0, epoch: nextContext.chartEpoch, streamId: "" };
      const response = await fetch(apiUrl("/api/v3/market/sidebar/bootstrap"), {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify(toWireContext(nextContext)),
      });
      if (!response.ok) throw new Error(`Sidebar bootstrap failed (${response.status})`);
      const payload = await response.json() as unknown;
      ensureSubscription();
      return payload;
    },
    setContext(nextContext) {
      context = { ...nextContext };
      if (cursor.epoch !== nextContext.chartEpoch) cursor = { sequence: 0, snapshotVersion: 0, epoch: nextContext.chartEpoch, streamId: "" };
      if (release) chartDataManager.updateRealtimeSidebarContext(realtimeContext());
      else ensureSubscription();
    },
    subscribe(nextListener) {
      listener = nextListener;
      ensureSubscription();
      return () => {
        disposed = true;
        listener = () => false;
        release?.();
        release = null;
      };
    },
  };
}

export function parseMarketSidebarBootstrap(value: unknown): MarketSidebarSnapshot {
  const root = record(value, "bootstrap");
  const wireContext = record(root.context, "bootstrap.context");
  const chartSymbol = string(wireContext.chart_symbol, "context.chart_symbol").toUpperCase();
  const chartEpoch = integer(wireContext.chart_epoch, "context.chart_epoch");
  const profile = parseProfile(root.active_symbol_profile);
  if (profile.symbol.toUpperCase() !== chartSymbol) {
    throw new Error("active_symbol_profile does not match chart context");
  }
  const quotes = record(root.watchlist_quotes, "watchlist_quotes");
  const quotesBySymbol: Record<string, MarketQuote> = {};
  for (const [symbol, quote] of Object.entries(quotes)) {
    const parsed = parseQuote(quote, symbol);
    quotesBySymbol[symbol.toUpperCase()] = parsed;
  }
  const newsBySymbol: Record<string, StockNewsFeed> = {};
  if (root.news_preview !== undefined) {
    const feed = parseNews(root.news_preview, chartSymbol, chartEpoch);
    newsBySymbol[chartSymbol] = feed;
  }
  return {
    context: {
      chartSymbol,
      chartEpoch,
      watchlistId: string(wireContext.watchlist_id, "context.watchlist_id"),
      watchlistSymbols: Object.keys(quotesBySymbol),
      watchlistRevision: integer(wireContext.watchlist_revision, "context.watchlist_revision"),
    },
    quotesBySymbol,
    profileBySymbol: { [chartSymbol]: profile },
    newsBySymbol,
    strength: parseStrength(root.strongest_preview),
    snapshotVersion: integer(root.snapshot_version, "snapshot_version"),
    sequence: integer(root.sequence, "sequence"),
  };
}

export function parseMarketSidebarEvent(value: unknown): MarketSidebarEvent {
  const root = record(value, "event");
  const type = string(root.type, "event.type");
  const sequence = integer(root.sequence, "event.sequence");
  const subscriptionId = string(root.subscription_id, "event.subscription_id");
  const streamId = string(root.stream_id, "event.stream_id");
  const chartSymbol = string(root.chart_symbol, "event.chart_symbol").toUpperCase();
  const chartEpoch = integer(root.chart_epoch, "event.chart_epoch");
  const watchlistId = string(root.watchlist_id, "event.watchlist_id");
  const watchlistRevision = integer(root.watchlist_revision, "event.watchlist_revision");
  const snapshotVersion = integer(root.snapshot_version, "event.snapshot_version");
  const eventContext = { subscriptionId, streamId, chartSymbol, chartEpoch, watchlistId, watchlistRevision, sequence, snapshotVersion };
  if (type === "watchlist_quote_delta") {
    const quotes = record(root.quotes, "event.quotes");
    return { type, ...eventContext, quotes: Object.entries(quotes).map(([symbol, quote]) => parseQuote(quote, symbol)) };
  }
  if (type === "strength_delta") return { type, ...eventContext, strength: parseStrength(root.strength) };
  if (type === "active_profile_delta") return { type, ...eventContext, profile: parseProfile(root.profile) };
  if (type === "news_delta") return { type, ...eventContext, feed: parseNews(root.news, chartSymbol, chartEpoch) };
  if (type === "sidebar_resync_required") {
    const snapshot = parseMarketSidebarBootstrap(root.snapshot);
    return { type, ...eventContext, snapshot };
  }
  throw new Error(`Unsupported sidebar event: ${type}`);
}

export function toWireContext(context: SidebarContext) {
  return {
    chart_symbol: context.chartSymbol,
    chart_epoch: context.chartEpoch,
    watchlist_id: context.watchlistId,
    watchlist_symbols: context.watchlistSymbols,
    watchlist_revision: context.watchlistRevision,
  };
}

function parseQuote(value: unknown, symbolHint?: string): MarketQuote {
  const item = record(value, "quote");
  const symbol = optionalString(item.symbol) ?? symbolHint?.toUpperCase();
  if (!symbol) throw new Error("quote.symbol must be a string when no map key is available");
  const unavailable = item.freshness === "unavailable";
  return {
    symbol,
    name: optionalString(item.name) ?? symbol,
    exchange: optionalString(item.exchange) ?? inferExchange(symbol),
    price: readNullableNumber(item.price, "quote.price"),
    previousClose: readNullableNumber(item.previous_close ?? item.previousClose, "quote.previous_close"),
    change: readNullableNumber(item.change, "quote.change"),
    changePercent: readNullableNumber(item.change_percent ?? item.changePercent, "quote.change_percent"),
    volume: readNullableNumber(item.volume, "quote.volume"),
    amount: readNullableNumber(item.amount, "quote.amount"),
    time: readNullableNumber(item.time, "quote.time"),
    source: unavailable ? "placeholder" : "bundle-adapter",
  };
}

function parseProfile(value: unknown): SymbolProfile {
  const item = record(value, "profile");
  const symbol = string(item.symbol, "profile.symbol").toUpperCase();
  const quote = parseQuote(item.quote, symbol);
  const identity = record(item.identity, "profile.identity");
  const valuation = record(item.valuation, "profile.valuation");
  const capitalFlow = record(item.capital_flow, "profile.capital_flow");
  const themes = Array.isArray(item.themes) ? item.themes.map(parseTheme).filter((theme): theme is { name: string; changePercent: number | null } => theme !== null) : [];
  const industry = optionalString(identity.industry);
  return {
    symbol,
    name: optionalString(identity.name) ?? quote.name,
    exchange: optionalString(identity.exchange) ?? quote.exchange,
    code: symbol.split(".")[0],
    assetType: optionalString(identity.asset_type) ?? "stock",
    latestPrice: quote.price,
    dayChangePercent: quote.changePercent,
    volume: quote.volume,
    amount: quote.amount,
    sector: industry ? { name: industry, changePercent: null } : null,
    concepts: themes,
    marketCap: readNullableNumber(valuation.market_cap, "profile.valuation.market_cap"),
    peRatio: readNullableNumber(valuation.pe_ratio, "profile.valuation.pe_ratio"),
    turnoverRate: readNullableNumber(valuation.turnover_rate, "profile.valuation.turnover_rate"),
    fundFlow: {
      net: readNullableNumber(capitalFlow.net_inflow, "profile.capital_flow.net_inflow"),
      main: readNullableNumber(capitalFlow.main_net_inflow, "profile.capital_flow.main_net_inflow"),
      retail: readNullableNumber(capitalFlow.small_net_inflow, "profile.capital_flow.small_net_inflow"),
    },
    chanStrokeStates: [],
    strategySignals: Array.isArray(item.strategy_signals) ? item.strategy_signals as SymbolProfile["strategySignals"] : [],
    dataSource: `${optionalString(identity.source) ?? "normalized_snapshot"}/${optionalString(identity.freshness) ?? "unavailable"}`,
  };
}

function parseStrength(value: unknown): MarketStrengthSnapshot {
  const item = record(value, "strength");
  const freshness = optionalString(item.freshness) ?? "unavailable";
  if (freshness !== "live" && freshness !== "delayed" && freshness !== "stale" && freshness !== "unavailable") {
    throw new Error("strength.freshness is invalid");
  }
  return {
    score: readNullableNumber(item.score, "strength.score"),
    leaders: parseStrengthLeaders(item),
    themes: parseStrengthThemes(item),
    source: optionalString(item.source) ?? "normalized_snapshot",
    freshness,
  };
}

function parseStrengthLeaders(item: Record<string, unknown>): MarketStrengthSnapshot["leaders"] {
  if (Array.isArray(item.leader_details)) {
    return item.leader_details.map((value, index) => {
      const leader = record(value, `strength.leader_details[${index}]`);
      return {
        name: string(leader.name, `strength.leader_details[${index}].name`),
        changePercent: readNullableNumber(leader.change_percent, `strength.leader_details[${index}].change_percent`),
      };
    });
  }
  return readStrings(item.leaders, "strength.leaders").map((name) => ({ name, changePercent: null }));
}

function parseStrengthThemes(item: Record<string, unknown>): MarketStrengthSnapshot["themes"] {
  if (Array.isArray(item.theme_details)) {
    return item.theme_details.map((value, index) => {
      const theme = record(value, `strength.theme_details[${index}]`);
      return {
        name: string(theme.name, `strength.theme_details[${index}].name`),
        changePercent: readNullableNumber(theme.change_percent, `strength.theme_details[${index}].change_percent`),
        mainNetInflowWan: readNullableNumber(theme.main_net_inflow_wan, `strength.theme_details[${index}].main_net_inflow_wan`),
      };
    });
  }
  return readStrings(item.themes, "strength.themes").map((name) => ({ name, changePercent: null, mainNetInflowWan: null }));
}

function parseNews(value: unknown, symbol: string, epoch: number): StockNewsFeed {
  const item = record(value, "news");
  const itemSymbol = string(item.symbol, "news.symbol").toUpperCase();
  if (itemSymbol !== symbol || integer(item.chart_epoch, "news.chart_epoch") !== epoch) throw new Error("news context mismatch");
  if (!Array.isArray(item.items)) throw new Error("news.items must be an array");
  const items = item.items.map((value, index): NewsItem => {
    const news = record(value, `news.items[${index}]`);
    const sources = Array.isArray(news.sources)
      ? news.sources.map((source) => record(source, `news.items[${index}].sources`))
      : [];
    const primarySource = sources.find((source) => optionalString(source.name) || optionalString(source.url));
    const relatedSymbols = Array.isArray(news.related_symbols)
      ? news.related_symbols.map((value, relatedIndex) => {
        const related = record(value, `news.items[${index}].related_symbols[${relatedIndex}]`);
        return {
          symbol: string(related.symbol, `news.items[${index}].related_symbols[${relatedIndex}].symbol`).toUpperCase(),
          changePercent: readNullableNumber(related.change_percent, `news.items[${index}].related_symbols[${relatedIndex}].change_percent`),
        };
      })
      : [];
    return {
      id: string(news.event_id ?? news.id, `news.items[${index}].event_id`),
      title: string(news.title, `news.items[${index}].title`),
      source: optionalString(primarySource?.name) ?? string(news.source, `news.items[${index}].source`),
      time: string(news.published_at ?? news.time, `news.items[${index}].published_at`),
      ...(typeof (news.fact_summary ?? news.summary) === "string" ? { summary: (news.fact_summary ?? news.summary) as string } : {}),
      ...(optionalString(primarySource?.url) || optionalString(news.url) ? { url: optionalString(primarySource?.url) ?? optionalString(news.url) } : {}),
      ...(Array.isArray(news.impact_tags) && news.impact_tags.every((tag) => typeof tag === "string") ? { tags: news.impact_tags as string[] } : {}),
      ...(relatedSymbols.length > 0 ? { relatedSymbols } : {}),
    };
  });
  const status = optionalString(item.status) ?? "unavailable";
  return { symbol, source: optionalString(item.source) ?? "iwencai_news_search", asOf: optionalString(item.as_of) ?? "", stale: status === "stale" || status === "unavailable", warnings: status === "unavailable" ? ["news unavailable"] : undefined, stockNews: items, globalNews: [] };
}

function parseTheme(value: unknown): { name: string; changePercent: number | null } | null {
  if (typeof value === "string" && value) return { name: value, changePercent: null };
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const item = value as Record<string, unknown>;
  const name = optionalString(item.name);
  return name ? { name, changePercent: readNullableNumber(item.change_percent, "theme.change_percent") } : null;
}

function readStrings(value: unknown, label: string): string[] {
  if (value === undefined) return [];
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    throw new Error(`${label} must be an array of strings`);
  }
  return value;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as Record<string, unknown>;
}
function string(value: unknown, label: string): string {
  if (typeof value !== "string" || !value) throw new Error(`${label} must be a string`);
  return value;
}
function integer(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value)) throw new Error(`${label} must be an integer`);
  return value as number;
}
function readNullableNumber(value: unknown, label: string): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`${label} must be a finite number or null`);
  return value;
}
function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}
function inferExchange(symbol: string): string {
  const suffix = symbol.split(".")[1];
  return suffix?.toUpperCase() ?? "";
}
