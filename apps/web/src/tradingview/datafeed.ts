import { chartDataManager, type ChartDataManager } from "../api/chartDataManager";
import { searchSymbols } from "../api/client";
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
const MAX_DIRECTIONAL_GUARD_BARS = 200;

const realtimeSubscriptions = new Map<
  string,
  {
    timer: number;
    unsubscribe?: () => void;
    closed: boolean;
    symbol: string;
    timeframe: string;
    resolution: string;
  }
>();

export function createDatafeed(manager: ChartDataManager = chartDataManager) {
  let activeHistoryContext = "";
  let activeHistoryEpoch = 0;
  let historyAbortController: AbortController | null = null;

  const resetHistoryContext = () => {
    historyAbortController?.abort();
    historyAbortController = null;
    activeHistoryContext = "";
    activeHistoryEpoch += 1;
  };

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
        const exchange = symbol.endsWith(".BJ") ? "BJ" : symbol.endsWith(".SH") ? "SH" : "SZ";
        onResolve({
          ticker: symbol,
          name: symbol,
          short_name: symbol,
          full_name: symbol,
          description: symbol,
          type: "stock",
          session: "0930-1130,1300-1500",
          session_display: "0930-1130,1300-1500",
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
      const request = planHistoryRequest(periodParams);
      const context = `${symbol.toUpperCase()}|${timeframe}`;
      if (context !== activeHistoryContext) {
        historyAbortController?.abort();
        historyAbortController = new AbortController();
        activeHistoryContext = context;
        activeHistoryEpoch += 1;
      }
      historyAbortController ??= new AbortController();
      const requestEpoch = activeHistoryEpoch;
      const signal = historyAbortController.signal;
      recordTvDebug("datafeed.getBars.request", {
        symbol,
        resolution,
        timeframe,
        from: request.from,
        to: request.to,
        countBack: periodParams.countBack,
        requestedLimit: request.limit,
        guard: request.guard,
        firstDataRequest: periodParams.firstDataRequest,
      });
      manager.getBars({ symbol, timeframe, ...request, signal })
        .then((response) => {
          if (context !== activeHistoryContext || requestEpoch !== activeHistoryEpoch) {
            recordTvDebug("datafeed.getBars.stale", { symbol, timeframe });
            return;
          }
          const bars = response.bars.map((bar) => toTvBar(bar, resolution));
          const firstBar = response.bars[0];
          const lastBar = response.bars[response.bars.length - 1];
          if (response.bars.length > 0) {
            manager.publishHistoryWindow({
              source: "tradingview-datafeed",
              symbol,
              timeframe,
              resolution,
              requestedFrom: request.from,
              requestedTo: request.to,
              from: firstBar?.time ?? periodParams.from,
              to: lastBar?.time ?? periodParams.to,
              limit: Math.max(response.bars.length, request.limit),
              bars: response.bars,
              first: firstBar?.time,
              last: lastBar?.time,
            });
          } else {
            recordTvDebug("datafeed.getBars.empty", {
              symbol,
              resolution,
              timeframe,
              from: request.from,
              to: request.to,
              requestedLimit: request.limit,
            });
          }
          patchTvDebug("datafeed", {
            lastBarsRequest: {
              symbol,
              resolution,
              timeframe,
              from: request.from,
              to: request.to,
              requestedLimit: request.limit,
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
          onHistory(bars, { noData: response.noData === true });
        })
        .catch((error) => {
          if (
            signal.aborted ||
            context !== activeHistoryContext ||
            requestEpoch !== activeHistoryEpoch
          ) {
            recordTvDebug("datafeed.getBars.aborted", { symbol, timeframe });
            return;
          }
          const message = String(error);
          recordTvDebug("datafeed.getBars.error", message);
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
        unsubscribe: undefined as (() => void) | undefined,
        closed: false,
        symbol,
        timeframe,
        resolution,
      };

      const acceptBar = (
        bar: {
          time: number;
          open: number;
          high: number;
          low: number;
          close: number;
          volume: number;
          revision?: number;
          complete?: boolean;
        },
        responseSymbol: string,
        responseTimeframe: string,
        snapshotVersion?: string,
        seq?: number,
        sessionGeneration?: number,
      ) => {
        const accepted = manager.handleRealtimeBarUpdate({
          symbol: responseSymbol.toUpperCase(),
          timeframe: responseTimeframe,
          snapshotVersion,
          seq,
          sessionGeneration,
          bar,
        });
        if (!accepted) {
          return;
        }
        recordTvDebug("datafeed.subscribeBars.tick", {
          symbol,
          timeframe,
          subscriberUid,
          transport: manager.transportMode,
        });
        onRealtime(toTvBar(bar, resolution));
      };

      const poll = async () => {
        try {
          const response = await manager.getBars({
            symbol,
            timeframe,
            limit: 2,
          });
          const bar = response.bars[response.bars.length - 1];
          if (!bar) {
            return;
          }
          acceptBar(bar, response.symbol, response.timeframe);
        } catch (error) {
          recordTvDebug("datafeed.subscribeBars.error", String(error));
        }
      };

      const startPollingFallback = () => {
        if (subscription.closed || subscription.timer) {
          return;
        }
        void poll();
        subscription.timer = window.setInterval(() => {
          void poll();
        }, REALTIME_POLL_INTERVAL_MS);
      };

      recordTvDebug("datafeed.subscribeBars.open", { symbol, timeframe, subscriberUid });
      realtimeSubscriptions.set(subscriberUid, subscription);
      void poll();
      void manager.subscribeRealtimeBars(
        {
          symbol,
          timeframe,
        },
        (event) => {
          acceptBar(
            event.bar,
            event.symbol,
            event.timeframe,
            event.snapshotVersion,
            event.seq,
            event.sessionGeneration,
          );
        },
        (generation) => {
          manager.beginRealtimeSession(symbol, timeframe, generation);
        },
      )
        .then((unsubscribe) => {
          if (subscription.closed) {
            unsubscribe();
            return;
          }
          subscription.unsubscribe = unsubscribe;
        })
        .catch((error) => {
          recordTvDebug("datafeed.subscribeBars.realtimeFallback", String(error));
          startPollingFallback();
        });
    },

    unsubscribeBars(subscriberUid: string) {
      const subscription = realtimeSubscriptions.get(subscriberUid);
      if (!subscription) {
        return;
      }
      subscription.closed = true;
      subscription.unsubscribe?.();
      if (subscription.timer) {
        window.clearInterval(subscription.timer);
      }
      realtimeSubscriptions.delete(subscriberUid);
      if (`${subscription.symbol.toUpperCase()}|${subscription.timeframe}` === activeHistoryContext) {
        resetHistoryContext();
      }
    },

    resetCache() {
      resetHistoryContext();
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

export function planHistoryRequest(periodParams: {
  from: number;
  to: number;
  countBack?: number;
  firstDataRequest?: boolean;
}): { from?: number; to: number; limit: number; guard: number } {
  const countBack = Math.max(1, periodParams.countBack ?? 1);
  const guard = Math.min(MAX_DIRECTIONAL_GUARD_BARS, Math.ceil(countBack * 0.25));
  return {
    // On a symbol/timeframe switch, one end-anchored countBack query avoids
    // waiting for a ranged response followed by a second deficit request.
    from: periodParams.firstDataRequest ? undefined : periodParams.from,
    to: periodParams.to,
    limit: countBack + guard,
    guard,
  };
}
