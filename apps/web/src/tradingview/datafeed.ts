import { chartDataManager } from "../api/chartDataManager";
import { getBars as getBarsHttp, searchSymbols } from "../api/client";
import { patchTvDebug, recordTvDebug } from "./debug";
import { TIMEFRAME_BY_INTERVAL, toTradingViewTime } from "./time";

type Callback = (...args: unknown[]) => void;
type TvBar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

const RESOLUTIONS = ["5", "15", "30", "60", "D", "W", "M"];
const REALTIME_POLL_INTERVAL_MS = 3_000;
const MAX_BARS_PER_REQUEST = 5_000;
const FIRST_LOAD_PREFETCH_BARS: Record<string, number> = {
  "5f": 3000,
  "15f": 2400,
  "30f": 2000,
  "1h": 1600,
  "1d": 1200,
  "1w": 800,
  "1m": 600,
};
const PAN_PREFETCH_BARS: Record<string, number> = {
  "5f": 1800,
  "15f": 1200,
  "30f": 1000,
  "1h": 800,
  "1d": 600,
  "1w": 400,
  "1m": 300,
};

const realtimeSubscriptions = new Map<
  string,
  {
    timer: number;
    latestSignature: string;
    symbol: string;
    timeframe: string;
    resolution: string;
  }
>();

export function createDatafeed() {
  return {
    onReady(callback: Callback) {
      setTimeout(() => {
        recordTvDebug("datafeed.onReady");
        callback({
          supports_search: true,
          supports_group_request: false,
          supports_marks: false,
          supports_timescale_marks: false,
          supports_time: true,
          supported_resolutions: RESOLUTIONS,
        });
      }, 0);
    },

    getServerTime(callback: Callback) {
      const serverTime = Math.floor(Date.now() / 1000);
      patchTvDebug("datafeed", { serverTime });
      callback(serverTime);
    },

    searchSymbols(
      userInput: string,
      _exchange: string,
      _symbolType: string,
      onResult: Callback,
    ) {
      searchSymbols(userInput)
        .then((items) => {
          recordTvDebug("datafeed.searchSymbols", { userInput, count: items.length });
          onResult(
            items.map((item) => ({
              symbol: item.symbol,
              full_name: item.symbol,
              description: item.name,
              exchange: item.exchange,
              ticker: item.symbol,
              type: "stock",
            })),
          );
        })
        .catch(() => onResult([]));
    },

    resolveSymbol(
      symbolName: string,
      onResolve: Callback,
      onError: Callback,
    ) {
      const symbol = symbolName.toUpperCase();
      window.setTimeout(() => {
        recordTvDebug("datafeed.resolveSymbol", { symbol });
        const exchange = symbol.endsWith(".SH") ? "SH" : "SZ";
        onResolve({
          ticker: symbol,
          name: symbol,
          short_name: symbol,
          full_name: symbol,
          description: symbol,
          type: "stock",
          session: "24x7",
          session_display: "24x7",
          timezone: "Asia/Shanghai",
          exchange,
          listed_exchange: exchange,
          minmov: 1,
          pricescale: 1000,
          format: "price",
          has_intraday: true,
          has_daily: true,
          has_weekly_and_monthly: true,
          intraday_multipliers: ["5", "15", "30", "60"],
          daily_multipliers: ["1"],
          weekly_multipliers: ["1"],
          monthly_multipliers: ["1"],
          supported_resolutions: RESOLUTIONS,
          volume_precision: 0,
          data_status: "streaming",
        });
      }, 0);
      void onError;
    },

    getBars(
      symbolInfo: { ticker?: string; name?: string },
      resolution: string,
      periodParams: {
        from: number;
        to: number;
        countBack?: number;
        firstDataRequest?: boolean;
      },
      onHistory: Callback,
      onError: Callback,
    ) {
      const symbol = symbolInfo.ticker ?? symbolInfo.name ?? "";
      const timeframe = TIMEFRAME_BY_INTERVAL[resolution] ?? "5f";
      const sessionRange = chartDataManager.getSessionBarRange(symbol, timeframe);
      const panDirection = inferPanDirection(periodParams, sessionRange);
      const prefetchLimit = periodParams.firstDataRequest
        ? firstLoadPrefetchBars(timeframe)
        : panPrefetchBars(timeframe, panDirection);
      const requestedLimit = Math.min(
        MAX_BARS_PER_REQUEST,
        Math.max(
          periodParams.countBack ?? 300,
          prefetchLimit,
        ),
      );
      // TradingView's from/to window can fall on non-trading gaps while panning.
      // Query by countBack + to so the backend can return the nearest prior bars.
      const requestFrom = undefined;
      recordTvDebug("datafeed.getBars.request", {
        symbol,
        resolution,
        timeframe,
        from: requestFrom,
        to: periodParams.to,
        countBack: periodParams.countBack,
        requestedLimit,
        panDirection,
        sessionRange,
        firstDataRequest: periodParams.firstDataRequest,
      });
      getBarsHttp(
        symbol,
        timeframe,
        requestedLimit,
        requestFrom,
        periodParams.to,
      )
        .then((response) => {
          const bars = response.bars.map((bar) => toTvBar(bar, resolution));
          const firstBar = response.bars[0];
          const lastBar = response.bars[response.bars.length - 1];
          if (response.bars.length > 0) {
            chartDataManager.publishHistoryWindow({
              source: "tradingview-datafeed",
              symbol,
              timeframe,
              resolution,
              requestedFrom: requestFrom,
              requestedTo: periodParams.to,
              from: firstBar?.time ?? periodParams.from,
              to: lastBar?.time ?? periodParams.to,
              limit: Math.max(response.bars.length, requestedLimit),
              bars: response.bars,
              first: firstBar?.time,
              last: lastBar?.time,
            });
          } else {
            recordTvDebug("datafeed.getBars.empty", {
              symbol,
              resolution,
              timeframe,
              from: requestFrom,
              to: periodParams.to,
              requestedLimit,
              panDirection,
            });
          }
          patchTvDebug("datafeed", {
            lastBarsRequest: {
              symbol,
              resolution,
              timeframe,
              from: requestFrom,
              to: periodParams.to,
              requestedLimit,
              panDirection,
              count: bars.length,
              first: bars[0]?.time,
              last: bars[bars.length - 1]?.time,
              transport: "http-bars",
            },
          });
          recordTvDebug("datafeed.getBars.response", {
            symbol,
            resolution,
            count: bars.length,
            transport: "http-bars",
          });
          onHistory(bars, { noData: bars.length === 0 });
        })
        .catch((error) => {
          const message = String(error);
          recordTvDebug("datafeed.getBars.error", message);
          if (
            timeframe === "5f" &&
            message.includes("Chan service requires 5f base bars for recursive analysis")
          ) {
            onHistory([], { noData: true });
            return;
          }
          onError(message);
        });
    },

    subscribeBars(
      symbolInfo: { ticker?: string; name?: string },
      resolution: string,
      onRealtime: Callback,
      subscriberUid: string,
    ) {
      const symbol = symbolInfo.ticker ?? symbolInfo.name ?? "";
      const timeframe = TIMEFRAME_BY_INTERVAL[resolution] ?? "5f";
      const subscription = {
        timer: 0,
        latestSignature: "",
        symbol,
        timeframe,
        resolution,
      };

      const poll = async () => {
        try {
          const response = await getBarsHttp(symbol, timeframe, 2);
          const bar = response.bars[response.bars.length - 1];
          if (!bar) {
            return;
          }
          const signature = [
            bar.time,
            bar.revision,
            bar.close,
            bar.volume,
            bar.complete ? 1 : 0,
          ].join(":");
          if (signature === subscription.latestSignature) {
            return;
          }
          subscription.latestSignature = signature;
          chartDataManager.handleRealtimeBarUpdate({
            symbol: response.symbol.toUpperCase(),
            timeframe: response.timeframe,
            bar,
          });
          recordTvDebug("datafeed.subscribeBars.tick", {
            symbol,
            timeframe,
            subscriberUid,
            transport: "http-bars",
          });
          onRealtime(toTvBar(bar, resolution));
        } catch (error) {
          recordTvDebug("datafeed.subscribeBars.error", String(error));
        }
      };

      recordTvDebug("datafeed.subscribeBars.open", { symbol, timeframe, subscriberUid });
      void poll();
      subscription.timer = window.setInterval(() => {
        void poll();
      }, REALTIME_POLL_INTERVAL_MS);
      realtimeSubscriptions.set(subscriberUid, subscription);
    },

    unsubscribeBars(subscriberUid: string) {
      const subscription = realtimeSubscriptions.get(subscriberUid);
      if (!subscription) {
        return;
      }
      window.clearInterval(subscription.timer);
      realtimeSubscriptions.delete(subscriberUid);
    },
  };
}

function toTvBar(
  bar: {
    time: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
  },
  resolution: string,
): TvBar {
  return {
    time: toTradingViewTime(bar.time, resolution),
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    volume: bar.volume,
  };
}

type SessionRange = { from?: number; to?: number; count: number } | null;
type PanDirection = "initial" | "older" | "newer" | "overlap";

function inferPanDirection(
  periodParams: { from: number; to: number; firstDataRequest?: boolean },
  sessionRange: SessionRange,
): PanDirection {
  if (periodParams.firstDataRequest || !sessionRange || sessionRange.count === 0) {
    return "initial";
  }
  if (sessionRange.from !== undefined && periodParams.to < sessionRange.from) {
    return "older";
  }
  if (sessionRange.to !== undefined && periodParams.from > sessionRange.to) {
    return "newer";
  }
  if (sessionRange.to !== undefined && periodParams.to < sessionRange.to) {
    return "older";
  }
  if (sessionRange.from !== undefined && periodParams.from > sessionRange.from) {
    return "newer";
  }
  return "overlap";
}

function firstLoadPrefetchBars(timeframe: string): number {
  return FIRST_LOAD_PREFETCH_BARS[timeframe] ?? 1200;
}

function panPrefetchBars(timeframe: string, direction: PanDirection): number {
  const base = PAN_PREFETCH_BARS[timeframe] ?? 600;
  if (direction === "older") {
    return Math.min(MAX_BARS_PER_REQUEST, Math.round(base * 1.5));
  }
  if (direction === "newer") {
    return Math.max(300, Math.round(base * 0.75));
  }
  return base;
}
