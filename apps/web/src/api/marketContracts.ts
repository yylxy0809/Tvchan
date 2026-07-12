export type DataFreshness = {
  source: string;
  asOf: string;
  stale?: boolean;
  warnings?: string[];
};

export type RealtimeQuote = DataFreshness & {
  symbol: string;
  name: string;
  price: number | null;
  change: number | null;
  changePercent: number | null;
  volume: number | null;
  amount: number | null;
  turnoverRate: number | null;
  peTtm: number | null;
  marketCap: number | null;
};

export type BoardLeader = {
  symbol: string;
  name: string;
  price: number | null;
  changePercent: number | null;
  reason?: string;
};

export type BoardConcept = {
  id: string;
  name: string;
  type: "sector" | "concept";
  changePercent: number | null;
  netInflow: number | null;
  leaders: BoardLeader[];
};

export type CapitalFlow = {
  mainNetInflow: number | null;
  superLargeNetInflow: number | null;
  largeNetInflow: number | null;
  mediumNetInflow: number | null;
  smallNetInflow: number | null;
};

export type StatementRow = {
  period: string;
  values: Record<string, number | string | null>;
};

export type AnnouncementItem = {
  id: string;
  title: string;
  time: string;
  type: string;
  url?: string;
};

export type StockProfileDetail = DataFreshness & {
  symbol: string;
  name: string;
  companyName: string | null;
  mainBusiness: string | null;
  listingDate: string | null;
  industry: string | null;
  area: string | null;
  marketCap: number | null;
  floatMarketCap: number | null;
  peTtm: number | null;
  pb: number | null;
  turnoverRate: number | null;
  sectors: BoardConcept[];
  concepts: BoardConcept[];
  capitalFlow: CapitalFlow;
  financials: {
    income: StatementRow[];
    balance: StatementRow[];
    cashflow: StatementRow[];
  };
  announcements: AnnouncementItem[];
};

export type NewsItem = {
  id: string;
  title: string;
  source: string;
  time: string;
  summary?: string;
  url?: string;
  tags?: string[];
  relatedSymbols?: Array<{
    symbol: string;
    changePercent: number | null;
  }>;
};

export type StockNewsFeed = DataFreshness & {
  symbol: string;
  stockNews: NewsItem[];
  globalNews: NewsItem[];
};

export type StrongStock = {
  symbol: string;
  name: string;
  price: number | null;
  changePercent: number | null;
  theme: string;
  reason: string;
  netInflow: number | null;
};

export type StrongTheme = {
  name: string;
  changePercent: number | null;
  leaders: BoardLeader[];
  netInflow: number | null;
};

export type DragonTigerItem = {
  symbol: string;
  name: string;
  reason: string;
  netBuy: number | null;
};

export type UnlockItem = {
  symbol: string;
  name: string;
  date: string;
  marketValue: number | null;
};

export type IndustryCompareItem = {
  industry: string;
  changePercent: number | null;
  netInflow: number | null;
  strongCount: number;
};

export type StrongestToday = DataFreshness & {
  hotStocks: StrongStock[];
  themes: StrongTheme[];
  northbound: {
    netInflow: number | null;
    shNetInflow: number | null;
    szNetInflow: number | null;
  };
  dragonTiger: DragonTigerItem[];
  marketDragonTiger: DragonTigerItem[];
  unlocks: UnlockItem[];
  industryCompare: IndustryCompareItem[];
};

const MOCK_AS_OF = "2026-06-20T15:00:00+08:00";

export async function getMockStockNews(symbol: string): Promise<StockNewsFeed> {
  return {
    symbol,
    source: "mock:eastmoney-contract",
    asOf: MOCK_AS_OF,
    stockNews: [
      {
        id: "stock-news-1",
        title: "平安银行零售业务结构继续优化",
        source: "东方财富",
        time: "14:32",
        summary: "关注净息差、资产质量和零售客户经营效率的边际变化。",
        tags: ["银行", "财报跟踪"],
      },
      {
        id: "stock-news-2",
        title: "银行板块午后资金回流，低估值品种活跃",
        source: "东方财富",
        time: "13:48",
        summary: "板块资金呈现分化，股份行与城商行表现强于大盘。",
        tags: ["板块资金", "低估值"],
      },
    ],
    globalNews: [
      {
        id: "global-news-1",
        title: "亚太主要市场收盘涨跌互现，金融权重波动收窄",
        source: "全球资讯",
        time: "15:01",
        tags: ["全球市场"],
      },
      {
        id: "global-news-2",
        title: "离岸人民币窄幅波动，市场等待新的宏观数据",
        source: "全球资讯",
        time: "11:23",
        tags: ["汇率", "宏观"],
      },
    ],
  };
}

export async function getMockStrongestToday(): Promise<StrongestToday> {
  return {
    source: "mock:a-stock-data-contract",
    asOf: MOCK_AS_OF,
    hotStocks: [
      {
        symbol: "600519.SH",
        name: "贵州茅台",
        price: 1518.2,
        changePercent: 2.18,
        theme: "消费权重",
        reason: "白酒权重放量修复，机构资金回补",
        netInflow: 328000000,
      },
      {
        symbol: "300750.SZ",
        name: "宁德时代",
        price: 214.6,
        changePercent: 3.42,
        theme: "新能源",
        reason: "储能订单预期升温，板块强度提升",
        netInflow: 412000000,
      },
      {
        symbol: "000001.SZ",
        name: "平安银行",
        price: 10.93,
        changePercent: -0.91,
        theme: "银行",
        reason: "低估值金融权重维持活跃",
        netInflow: -36000000,
      },
    ],
    themes: [
      {
        name: "新能源",
        changePercent: 2.36,
        netInflow: 1260000000,
        leaders: [
          { symbol: "300750.SZ", name: "宁德时代", price: 214.6, changePercent: 3.42 },
          { symbol: "002594.SZ", name: "比亚迪", price: 267.1, changePercent: 2.06 },
        ],
      },
      {
        name: "银行",
        changePercent: -0.28,
        netInflow: -180000000,
        leaders: [
          { symbol: "000001.SZ", name: "平安银行", price: 10.93, changePercent: -0.91 },
          { symbol: "600036.SH", name: "招商银行", price: 34.7, changePercent: 0.38 },
        ],
      },
    ],
    northbound: {
      netInflow: 890000000,
      shNetInflow: 320000000,
      szNetInflow: 570000000,
    },
    dragonTiger: [
      { symbol: "300750.SZ", name: "宁德时代", reason: "机构净买入", netBuy: 168000000 },
      { symbol: "002475.SZ", name: "立讯精密", reason: "深股通活跃", netBuy: 84000000 },
    ],
    marketDragonTiger: [
      { symbol: "600519.SH", name: "贵州茅台", reason: "沪股通买入", netBuy: 126000000 },
    ],
    unlocks: [
      { symbol: "688981.SH", name: "中芯国际", date: "2026-06-24", marketValue: 520000000 },
    ],
    industryCompare: [
      { industry: "电力设备", changePercent: 1.86, netInflow: 960000000, strongCount: 18 },
      { industry: "银行", changePercent: -0.28, netInflow: -180000000, strongCount: 6 },
      { industry: "医药生物", changePercent: 0.42, netInflow: 210000000, strongCount: 9 },
    ],
  };
}
