import { chartDataManager } from "./chartDataManager";
import {
  type ApiBar,
  type ApiSymbol,
  type ChanStroke,
  getBars,
  searchSymbols,
} from "./client";

export type SymbolSearchResult = ApiSymbol;

export type MarketQuote = {
  symbol: string;
  name: string;
  exchange: string;
  price: number | null;
  previousClose: number | null;
  change: number | null;
  changePercent: number | null;
  volume: number | null;
  amount: number | null;
  time: number | null;
  source: "bundle-adapter" | "placeholder";
};

export type SymbolProfile = {
  symbol: string;
  name: string;
  exchange: string;
  code: string;
  assetType: string;
  latestPrice: number | null;
  dayChangePercent: number | null;
  volume: number | null;
  amount: number | null;
  sector: ProfileTheme | null;
  concepts: ProfileTheme[];
  marketCap: number | null;
  peRatio: number | null;
  turnoverRate: number | null;
  fundFlow: {
    net: number | null;
    main: number | null;
    retail: number | null;
  };
  chanStrokeStates: ChanStrokeState[];
  strategySignals: StrategySignal[];
  dataSource: string;
};

export type ProfileTheme = {
  name: string;
  changePercent: number | null;
};

export type ChanStrokeLevel = "1d" | "30f" | "5f";

export type ChanStrokeState = {
  level: ChanStrokeLevel;
  label: string;
  direction: "up" | "down" | "unknown";
  stateLabel: string;
  mode: "confirmed" | "predictive" | null;
  modeLabel: string;
  confirmed: boolean | null;
  anchorTime: number | null;
  anchorPrice: number | null;
};

export type StrategySignal = {
  key: string;
  label: string;
  value: string;
  tone: "up" | "down" | "neutral";
  source: "chan-overlay" | "czsc-V230701" | "czsc-pending";
};

export type SymbolProfileMarketFields = {
  industry: string | null;
  fundNetInflow: number | null;
};

const DEFAULT_TIMEFRAME = "5f";
const WATCHLIST_CHAN_LEVELS: readonly ChanStrokeLevel[] = ["1d", "30f", "5f"] as const;

export async function searchSymbolCatalog(
  keyword: string,
): Promise<SymbolSearchResult[]> {
  const normalized = keyword.trim();
  if (!normalized) {
    return [];
  }
  return searchSymbols(normalized);
}

export async function getMarketQuote(
  symbol: ApiSymbol | string,
  _timeframe = DEFAULT_TIMEFRAME,
  signal?: AbortSignal,
): Promise<MarketQuote> {
  const symbolInfo = typeof symbol === "string" ? null : symbol;
  const symbolCode = typeof symbol === "string" ? symbol : symbol.symbol;
  try {
    const response = await getBars(
      symbolCode,
      "1d",
      2,
      undefined,
      undefined,
      signal,
    );
    return buildMarketQuoteFromDailyBars(symbolCode, symbolInfo, response.bars);
  } catch {
    return createPlaceholderQuote(symbolCode, symbolInfo);
  }
}

export function buildMarketQuoteFromDailyBars(
  symbol: string,
  info: ApiSymbol | null,
  bars: ApiBar[],
): MarketQuote {
  const last = bars[bars.length - 1];
  if (!last) {
    return createPlaceholderQuote(symbol, info);
  }
  const previous = bars[bars.length - 2];
  const previousClose = previous?.close ?? last.open ?? null;
  const change = previousClose !== null ? roundPrice(last.close - previousClose) : null;
  return {
    symbol,
    name: info?.name ?? symbol,
    exchange: info?.exchange ?? inferExchange(symbol),
    price: last.close,
    previousClose,
    change,
    changePercent:
      previousClose && previousClose !== 0
        ? roundPercent(((last.close - previousClose) / previousClose) * 100)
        : null,
    volume: last.volume ?? null,
    amount: last.amount ?? null,
    time: last.time ?? null,
    source: "bundle-adapter",
  };
}

export async function getSymbolProfile(
  symbol: ApiSymbol | string,
  timeframe = DEFAULT_TIMEFRAME,
  signal?: AbortSignal,
): Promise<SymbolProfile> {
  const info = typeof symbol === "string" ? await resolveSymbolInfo(symbol) : symbol;
  const [quote, intelligence] = await Promise.all([
    getMarketQuote(info ?? symbol, timeframe, signal),
    getSymbolChanIntelligence(
      typeof symbol === "string" ? symbol : symbol.symbol,
      signal,
    ),
  ]);
  const enrichment = buildProfileEnrichment(
    quote.symbol,
    info?.name ?? quote.name,
  );
  return {
    symbol: quote.symbol,
    name: info?.name ?? quote.name,
    exchange: info?.exchange ?? quote.exchange,
    code: info?.code ?? quote.symbol.split(".")[0],
    assetType: info?.asset_type ?? "stock",
    latestPrice: quote.price,
    dayChangePercent: quote.changePercent,
    volume: quote.volume,
    amount: quote.amount,
    sector: enrichment.sector,
    concepts: enrichment.concepts,
    marketCap: enrichment.marketCap,
    peRatio: enrichment.peRatio,
    turnoverRate: enrichment.turnoverRate,
    fundFlow: enrichment.fundFlow,
    chanStrokeStates: intelligence.chanStrokeStates,
    strategySignals: intelligence.strategySignals,
    dataSource:
      quote.source === "bundle-adapter"
        ? "chart bars latest close"
        : "placeholder until quote API is available",
  };
}

export async function getSymbolProfileMarketFields(
  symbol: ApiSymbol | string,
  fallbackName?: string,
): Promise<SymbolProfileMarketFields> {
  const info =
    typeof symbol === "string"
      ? fallbackName
        ? null
        : await resolveSymbolInfo(symbol)
      : symbol;
  const symbolCode = typeof symbol === "string" ? symbol : symbol.symbol;
  const enrichment = buildProfileEnrichment(
    symbolCode,
    info?.name ?? fallbackName ?? symbolCode,
  );
  return {
    industry: enrichment.sector?.name ?? null,
    fundNetInflow: enrichment.fundFlow.net,
  };
}

async function getSymbolChanIntelligence(
  symbol: string,
  signal?: AbortSignal,
): Promise<{
  chanStrokeStates: ChanStrokeState[];
  strategySignals: StrategySignal[];
}> {
  const [windowResult, intradaySignal] = await Promise.allSettled([
    chartDataManager.getChanOverlay({
      symbol,
      timeframe: DEFAULT_TIMEFRAME,
      limit: 600,
      levels: WATCHLIST_CHAN_LEVELS,
      modes: ["confirmed", "predictive"],
      signal,
    }),
    getCurrentTradingDayIntradaySignal(symbol, signal),
  ]);
  const chanStrokeStates =
    windowResult.status === "fulfilled"
      ? WATCHLIST_CHAN_LEVELS.map((level) =>
          deriveCurrentStrokeState(windowResult.value.strokes, level),
        )
      : WATCHLIST_CHAN_LEVELS.map(createUnknownStrokeState);
  return {
    chanStrokeStates,
    strategySignals: buildStrategySignals(
      chanStrokeStates,
      intradaySignal.status === "fulfilled" ? intradaySignal.value : null,
    ),
  };
}

function deriveCurrentStrokeState(
  strokes: ChanStroke[],
  level: ChanStrokeLevel,
): ChanStrokeState {
  const current = strokes
    .filter((stroke) => stroke.level === level)
    .sort(compareStrokePriority)[0];
  if (!current) {
    return createUnknownStrokeState(level);
  }
  const direction = normalizeStrokeDirection(current);
  const mode = normalizeStrokeMode(current);
  return {
    level,
    label: strokeLevelLabel(level),
    direction,
    stateLabel: strokeDirectionLabel(direction),
    mode,
    modeLabel: strokeModeLabel(mode),
    confirmed: current.confirmed,
    anchorTime: current.end.base_ts ?? current.end.time ?? null,
    anchorPrice: current.end.price ?? null,
  };
}

function buildStrategySignals(
  states: ChanStrokeState[],
  intradaySignal: StrategySignal | null,
): StrategySignal[] {
  const directionSummary = states
    .map((state) => `${state.label}${state.stateLabel}`)
    .join(" / ");
  const signals: StrategySignal[] = [
    {
      key: "chan-stroke-summary",
      label: "三级别笔状态",
      value: directionSummary,
      tone: summarizeStrokeTone(states),
      source: "chan-overlay",
    },
  ];
  if (intradaySignal) {
    signals.push(intradaySignal);
  } else {
    signals.push({
      key: "czsc-hook-pending",
      label: "CZSC信号",
      value: "待接入指定信号函数",
      tone: "neutral",
      source: "czsc-pending",
    });
  }
  return signals;
}

async function getCurrentTradingDayIntradaySignal(
  symbol: string,
  signal?: AbortSignal,
): Promise<StrategySignal> {
  const bars30f = await chartDataManager.getBars({
    symbol,
    timeframe: "30f",
    limit: 200,
    signal,
  });
  return deriveIntradayStructureSignal(bars30f.bars);
}

function deriveIntradayStructureSignal(
  bars30f: Array<{
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
  }>,
): StrategySignal {
  const groups = groupBarsByChinaDate(bars30f);
  const sortedDays = Array.from(groups.keys()).sort();
  const targetDay = sortedDays[sortedDays.length - 1];
  if (!targetDay) {
    return {
      key: "czsc-intraday-v230701",
      label: "日内分类",
      value: "其他",
      tone: "neutral",
      source: "czsc-V230701",
    };
  }

  const bars = groups.get(targetDay) ?? [];
  if (bars.length <= 4 || bars.length > 8) {
    return {
      key: "czsc-intraday-v230701",
      label: "日内分类",
      value: "其他",
      tone: "neutral",
      source: "czsc-V230701",
    };
  }
  const dir = bars[bars.length - 1].close > bars[0].open ? "上涨" : "下跌";
  const zsList: typeof bars[] = [];
  for (let i = 0; i < bars.length - 2; i += 1) {
    const b1 = bars[i];
    const b2 = bars[i + 1];
    const b3 = bars[i + 2];
    if (Math.min(b1.high, b2.high, b3.high) > Math.max(b1.low, b2.low, b3.low)) {
      zsList.push([b1, b2, b3]);
    }
  }

  let value = "其他";
  if (zsList.length === 0) {
    value = `无中枢${dir}`;
  } else if (zsList.length >= 2) {
    const zs1 = zsList[0];
    const zs2 = zsList[zsList.length - 1];
    const zs1High = Math.max(...zs1.map((bar) => bar.high));
    const zs1Low = Math.min(...zs1.map((bar) => bar.low));
    const zs2High = Math.max(...zs2.map((bar) => bar.high));
    const zs2Low = Math.min(...zs2.map((bar) => bar.low));
    if (dir === "上涨" && zs1High < zs2Low) {
      value = `双中枢${dir}`;
    } else if (dir === "下跌" && zs1Low > zs2High) {
      value = `双中枢${dir}`;
    }
  }

  if (value === "其他") {
    const highFirst =
      Math.max(bars[0].high, bars[1].high, bars[2].high) ===
      Math.max(...bars.map((bar) => bar.high));
    const lowFirst =
      Math.min(bars[0].low, bars[1].low, bars[2].low) ===
      Math.min(...bars.map((bar) => bar.low));
    if (highFirst && !lowFirst) {
      value = "弱平衡市";
    } else if (lowFirst && !highFirst) {
      value = "强平衡市";
    } else {
      value = "转折平衡市";
    }
  }

  return {
    key: "czsc-intraday-v230701",
    label: "日内分类",
    value,
    tone: intradaySignalTone(value),
    source: "czsc-V230701",
  };
}

function groupBarsByChinaDate<T extends { time: number }>(
  bars: T[],
): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const bar of bars) {
    const key = toChinaDateKey(bar.time);
    const items = groups.get(key);
    if (items) {
      items.push(bar);
    } else {
      groups.set(key, [bar]);
    }
  }
  return groups;
}

function toChinaDateKey(unixSeconds: number): string {
  const date = new Date((unixSeconds + 8 * 3600) * 1000);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function intradaySignalTone(value: string): StrategySignal["tone"] {
  if (value.includes("强平衡市") || value.includes("双中枢上涨") || value.includes("无中枢上涨")) {
    return "up";
  }
  if (value.includes("弱平衡市") || value.includes("双中枢下跌") || value.includes("无中枢下跌")) {
    return "down";
  }
  return "neutral";
}

function summarizeStrokeTone(
  states: ChanStrokeState[],
): StrategySignal["tone"] {
  const directions = states
    .map((state) => state.direction)
    .filter((direction) => direction !== "unknown");
  if (directions.length === 0) {
    return "neutral";
  }
  if (directions.every((direction) => direction === "up")) {
    return "up";
  }
  if (directions.every((direction) => direction === "down")) {
    return "down";
  }
  return "neutral";
}

function compareStrokePriority(a: ChanStroke, b: ChanStroke): number {
  return (
    compareNumber(b.end.time ?? 0, a.end.time ?? 0) ||
    compareNumber(strokeModeRank(b), strokeModeRank(a)) ||
    compareNumber(Number(b.confirmed), Number(a.confirmed)) ||
    b.id.localeCompare(a.id)
  );
}

function compareNumber(left: number, right: number): number {
  if (left === right) {
    return 0;
  }
  return left > right ? 1 : -1;
}

function strokeModeRank(stroke: ChanStroke): number {
  return normalizeStrokeMode(stroke) === "predictive" ? 2 : 1;
}

function normalizeStrokeMode(
  stroke: Pick<ChanStroke, "mode" | "confirmed">,
): "confirmed" | "predictive" {
  if (stroke.mode === "predictive" || !stroke.confirmed) {
    return "predictive";
  }
  return "confirmed";
}

function normalizeStrokeDirection(
  stroke: Pick<ChanStroke, "direction" | "start" | "end">,
): ChanStrokeState["direction"] {
  if (stroke.direction === "up" || stroke.direction === "down") {
    return stroke.direction;
  }
  if (stroke.end.price > stroke.start.price) {
    return "up";
  }
  if (stroke.end.price < stroke.start.price) {
    return "down";
  }
  return "unknown";
}

function createUnknownStrokeState(level: ChanStrokeLevel): ChanStrokeState {
  return {
    level,
    label: strokeLevelLabel(level),
    direction: "unknown",
    stateLabel: "--",
    mode: null,
    modeLabel: "--",
    confirmed: null,
    anchorTime: null,
    anchorPrice: null,
  };
}

function strokeLevelLabel(level: ChanStrokeLevel): string {
  switch (level) {
    case "1d":
      return "日线笔";
    case "30f":
      return "30f笔";
    case "5f":
      return "5f笔";
    default:
      return level;
  }
}

function strokeDirectionLabel(
  direction: ChanStrokeState["direction"],
): string {
  switch (direction) {
    case "up":
      return "上";
    case "down":
      return "下";
    default:
      return "--";
  }
}

function strokeModeLabel(
  mode: ChanStrokeState["mode"],
): string {
  switch (mode) {
    case "predictive":
      return "构建中";
    case "confirmed":
      return "完成";
    default:
      return "--";
  }
}

function buildProfileEnrichment(symbol: string, name: string) {
  const known = KNOWN_PROFILE_ENRICHMENT[symbol.toUpperCase()];
  const sector = known?.sector ?? inferSectorFromName(name);
  return {
    sector,
    concepts: known?.concepts ?? inferConceptsFromName(name),
    marketCap: known?.marketCap ?? null,
    peRatio: known?.peRatio ?? null,
    turnoverRate: known?.turnoverRate ?? null,
    fundFlow: known?.fundFlow ?? {
      net: null,
      main: null,
      retail: null,
    },
  };
}

const KNOWN_PROFILE_ENRICHMENT: Record<
  string,
  {
    sector: ProfileTheme | null;
    concepts: ProfileTheme[];
    marketCap: number | null;
    peRatio: number | null;
    turnoverRate: number | null;
    fundFlow: {
      net: number | null;
      main: number | null;
      retail: number | null;
    };
  }
> = {
  "000001.SZ": {
    sector: { name: "银行", changePercent: null },
    concepts: [
      { name: "深股通", changePercent: null },
      { name: "融资融券", changePercent: null },
      { name: "破净股", changePercent: null },
    ],
    marketCap: null,
    peRatio: null,
    turnoverRate: null,
    fundFlow: {
      net: null,
      main: null,
      retail: null,
    },
  },
};

function inferSectorFromName(name: string): ProfileTheme | null {
  if (/银行|bank/i.test(name)) {
    return { name: "银行", changePercent: null };
  }
  if (/证券|券商/i.test(name)) {
    return { name: "证券", changePercent: null };
  }
  if (/医药|生物/i.test(name)) {
    return { name: "医药生物", changePercent: null };
  }
  if (/新能源|光伏|锂/i.test(name)) {
    return { name: "电力设备", changePercent: null };
  }
  return null;
}

function inferConceptsFromName(name: string): ProfileTheme[] {
  if (/银行|bank/i.test(name)) {
    return [
      { name: "融资融券", changePercent: null },
      { name: "沪深300", changePercent: null },
    ];
  }
  return [];
}

async function resolveSymbolInfo(symbol: string): Promise<ApiSymbol | null> {
  try {
    const results = await searchSymbols(symbol);
    return (
      results.find((item) => item.symbol.toUpperCase() === symbol.toUpperCase()) ??
      null
    );
  } catch {
    return null;
  }
}

function createPlaceholderQuote(
  symbol: string,
  info?: ApiSymbol | null,
): MarketQuote {
  return {
    symbol,
    name: info?.name ?? symbol,
    exchange: info?.exchange ?? inferExchange(symbol),
    price: null,
    previousClose: null,
    change: null,
    changePercent: null,
    volume: null,
    amount: null,
    time: null,
    source: "placeholder",
  };
}

function inferExchange(symbol: string): string {
  const upper = symbol.toUpperCase();
  if (upper.endsWith(".SH")) {
    return "SH";
  }
  if (upper.endsWith(".SZ")) {
    return "SZ";
  }
  if (upper.endsWith(".BJ")) {
    return "BJ";
  }
  return "";
}

function roundPrice(value: number): number {
  return Math.round(value * 1000) / 1000;
}

function roundPercent(value: number): number {
  return Math.round(value * 100) / 100;
}
