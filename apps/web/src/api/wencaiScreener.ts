import type { WatchlistItem } from "./watchlistStore";

export type WencaiScreenerRow = WatchlistItem & {
  code: string;
  price: number;
  changePercent: number;
  buySignal: string;
  technicalShape: string;
  reason: string;
  highBreakReason: string;
};

export type WencaiScreenerResponse = {
  query: string;
  total: number;
  conditions: string[];
  suggestions: string[];
  rows: WencaiScreenerRow[];
};

const MOCK_ROWS: WencaiScreenerRow[] = [
  {
    symbol: "600063.SH",
    code: "600063",
    name: "皖维高新",
    exchange: "SH",
    price: 8.46,
    changePercent: -0.24,
    buySignal: "月线BOLL突破上轨；月线CCI买入信号",
    technicalShape: "阳线；放量；旗形",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600110.SH",
    code: "600110",
    name: "诺德股份",
    exchange: "SH",
    price: 17.57,
    changePercent: 2.03,
    buySignal: "周线BOLL突破上轨；月线RSI金叉",
    technicalShape: "价升量涨；阳线",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600114.SH",
    code: "600114",
    name: "东睦股份",
    exchange: "SH",
    price: 43.85,
    changePercent: 1.41,
    buySignal: "月线SKDJ金叉；周线CCI买入信号",
    technicalShape: "阳线；缩量；强中选强",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600176.SH",
    code: "600176",
    name: "中国巨石",
    exchange: "SH",
    price: 53.5,
    changePercent: -2.01,
    buySignal: "月线SKDJ金叉；周线RSI金叉",
    technicalShape: "阳线；放量",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600186.SH",
    code: "600186",
    name: "莲花控股",
    exchange: "SH",
    price: 14.28,
    changePercent: -2.59,
    buySignal: "月线SKDJ金叉；周线CCI买入信号",
    technicalShape: "强中选强；放量；阴线",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600206.SH",
    code: "600206",
    name: "有研新材",
    exchange: "SH",
    price: 42.51,
    changePercent: 6.84,
    buySignal: "月线BOLL突破上轨；周线SKDJ金叉",
    technicalShape: "价升量涨；阳线；放量",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600226.SH",
    code: "600226",
    name: "亨通股份",
    exchange: "SH",
    price: 9.76,
    changePercent: 10.03,
    buySignal: "月线RSI金叉；周线DMI金叉",
    technicalShape: "价升量涨；阳线；放量",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600246.SH",
    code: "600246",
    name: "万通发展",
    exchange: "SH",
    price: 18.65,
    changePercent: 5.91,
    buySignal: "RSI金叉；CCI买入信号",
    technicalShape: "强中选强；阳线；放量",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600259.SH",
    code: "600259",
    name: "中稀有色",
    exchange: "SH",
    price: 116.28,
    changePercent: 10.0,
    buySignal: "月线BOLL突破上轨；周线RSI金叉",
    technicalShape: "阳线；放量；价升量缩",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600345.SH",
    code: "600345",
    name: "长江通信",
    exchange: "SH",
    price: 64.78,
    changePercent: 7.02,
    buySignal: "月线SKDJ金叉；周线CCI买入信号",
    technicalShape: "强中选强；阳线；放量",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600353.SH",
    code: "600353",
    name: "旭光电子",
    exchange: "SH",
    price: 42.01,
    changePercent: 10.0,
    buySignal: "月线BOLL突破上轨",
    technicalShape: "价升量涨；强中选强；阳线",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
  {
    symbol: "600366.SH",
    code: "600366",
    name: "宁波韵升",
    exchange: "SH",
    price: 14.91,
    changePercent: 5.89,
    buySignal: "BOLL突破上轨；周线SKDJ金叉",
    technicalShape: "阳线；放量",
    reason: "5日线15日线60日线多头排列",
    highBreakReason: "最高价后复权创新高",
  },
];

export async function queryWencaiScreener(
  query: string,
): Promise<WencaiScreenerResponse> {
  const normalized = query.trim();
  await delay(220);
  const rows = normalized ? MOCK_ROWS : [];
  return {
    query: normalized,
    total: rows.length,
    conditions: normalized
      ? splitConditions(normalized)
      : ["请输入自然语言选股条件"],
    suggestions: ["主板", "成交量>1000万股", "MACD零上金叉", "振幅>3%"],
    rows,
  };
}

function splitConditions(query: string): string[] {
  return query
    .split(/[，,、]/)
    .map((item) => item.trim())
    .filter(Boolean)
    .slice(0, 6);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
