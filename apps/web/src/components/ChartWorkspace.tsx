import { Moon, Sun } from "lucide-react";
import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import { chartDataManager, type RealtimeFeedback } from "../api/chartDataManager";
import { createHttpMarketSidebarTransport } from "../api/marketSidebar";
import { MarketSidebarStore } from "../api/marketSidebarStore";
import { ChanOverlayManager, chanLevelsForTimeframe } from "../api/chanOverlayManager";
import { ChanRealtimeOverlayBridge, ChanRealtimePollingGate, type ChanRealtimeContext } from "../api/chanRealtimeOverlayBridge";
import { getBars, type ApiBar, type ChanOverlayResponse } from "../api/client";
import { listUserSettings, saveUserSetting } from "../api/userSettings";
import { readWatchlistGroups } from "../api/watchlistStore";
import type { AuthSession } from "../auth/api";
import { TRADINGVIEW_DEBUG } from "../config";
import {
  hasInitialChartSymbolQuery,
  loadSavedChartTheme,
  normalizeChartSymbol,
  readRemoteChartLayout,
  readRemoteChartTheme,
  saveChartTheme,
} from "../app/chartPreferences";
import {
  applyChanStudySettings,
  createTradingViewWidget,
  getWidgetSymbol,
  getWidgetTimeframe,
  renderChanOverlay,
  setTradingViewTheme,
  setWidgetSymbol,
  setWidgetTimeframe,
  subscribeChanStudySettingsChanges,
  subscribeWidgetSymbolChanges,
  subscribeWidgetVisibleRangeChanges,
  type ChartTheme,
  type TradingViewWidget,
} from "../tradingview/widget";
import {
  createDefaultChanOverlaySettings,
  mergeChanOverlaySettings,
  type ChanOverlaySettings,
} from "../tradingview/overlaySettings";
import { recordTvDebug } from "../tradingview/debug";
import { RightSidebar } from "./RightSidebar";
import { ScreenerDock } from "./ScreenerDock";

const DEFAULT_SYMBOL = "000001.SZ";
const DEFAULT_TIMEFRAME = "5f";
const DEFAULT_BAR_WINDOW_SIZE = 300;

type ChartMode = "loading" | "tradingview" | "fallback";

type ChartWorkspaceProps = {
  session: AuthSession;
  onOpenAdmin(): void;
  onLogout(): void;
};

function loadInitialChartSymbol(): string {
  if (typeof window === "undefined") {
    return DEFAULT_SYMBOL;
  }
  const raw = new URLSearchParams(window.location.search).get("symbol")?.trim();
  return normalizeChartSymbol(raw, DEFAULT_SYMBOL);
}

export async function installAsyncSubscription(
  create: () => Promise<() => void>,
  isCurrent: () => boolean,
  assign: (dispose: () => void) => void,
): Promise<boolean> {
  const dispose = await create();
  if (!isCurrent()) {
    dispose();
    return false;
  }
  assign(dispose);
  return true;
}

export function ChartWorkspace({
  session,
  onOpenAdmin,
  onLogout,
}: ChartWorkspaceProps) {
  const widgetContainerId = useRef(`tv-widget-${Math.random().toString(36).slice(2)}`);
  const widgetRef = useRef<TradingViewWidget | null>(null);
  const [chartTheme, setChartTheme] = useState<ChartTheme>(loadSavedChartTheme);
  const chartThemeRef = useRef<ChartTheme>(chartTheme);
  const remoteThemeReadyRef = useRef(false);
  const suppressNextThemeSyncRef = useRef(false);
  const remoteLayoutReadyRef = useRef(false);
  const [chanOverlaySettings, setChanOverlaySettings] = useState<ChanOverlaySettings>(
    createDefaultChanOverlaySettings,
  );
  const chanOverlaySettingsRef = useRef<ChanOverlaySettings>(chanOverlaySettings);
  const remoteIndicatorSettingsReadyRef = useRef(false);
  const suppressNextIndicatorSettingsSyncRef = useRef(false);
  const [chartMode, setChartMode] = useState<ChartMode>("loading");
  const [realtimeFeedback, setRealtimeFeedback] = useState<RealtimeFeedback>({ state: "connecting", channel: "bars" });
  const [feedbackNow, setFeedbackNow] = useState(Date.now);
  const [bars, setBars] = useState<ApiBar[]>([]);
  const [currentSymbol, setCurrentSymbol] = useState(loadInitialChartSymbol);
  const [confirmedChartSymbol, setConfirmedChartSymbol] = useState(loadInitialChartSymbol);
  const [currentTimeframe, setCurrentTimeframe] = useState(DEFAULT_TIMEFRAME);
  const [rightSidebarCollapseSignal, setRightSidebarCollapseSignal] = useState(0);
  const currentSymbolRef = useRef(currentSymbol);
  const confirmedChartSymbolRef = useRef(confirmedChartSymbol);
  const currentTimeframeRef = useRef(currentTimeframe);
  const marketSidebarStoreRef = useRef<MarketSidebarStore | null>(null);
  if (!marketSidebarStoreRef.current) {
    marketSidebarStoreRef.current = new MarketSidebarStore(
      createHttpMarketSidebarTransport(session.token),
      confirmedChartSymbol,
      readWatchlistGroups().flatMap((group) => group.items.map((item) => item.symbol)),
    );
  }
  const marketSidebarStore = marketSidebarStoreRef.current;
  const marketSnapshot = useSyncExternalStore(
    marketSidebarStore.subscribe,
    marketSidebarStore.getSnapshot,
    marketSidebarStore.getSnapshot,
  );

  useEffect(() => {
    const unsubscribe = chartDataManager.subscribeRealtimeFeedback(setRealtimeFeedback);
    const timer = window.setInterval(() => setFeedbackNow(Date.now()), 250);
    return () => {
      unsubscribe();
      window.clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    void marketSidebarStore.start();
    return () => marketSidebarStore.dispose();
  }, [marketSidebarStore]);

  const confirmChartSymbol = useCallback((symbol: string) => {
    const normalized = symbol.toUpperCase();
    if (confirmedChartSymbolRef.current !== normalized) {
      confirmedChartSymbolRef.current = normalized;
      setConfirmedChartSymbol(normalized);
    }
    marketSidebarStore.confirmChartSymbol(normalized);
  }, [marketSidebarStore]);

  const handleWatchlistSymbolsChange = useCallback((symbols: string[]) => {
    marketSidebarStore.setWatchlistSymbols(symbols);
  }, [marketSidebarStore]);

  useEffect(() => {
    let cancelled = false;
    remoteThemeReadyRef.current = false;
    remoteLayoutReadyRef.current = false;
    remoteIndicatorSettingsReadyRef.current = false;
    void listUserSettings(session.token)
      .then((settings) => {
        if (cancelled) {
          return;
        }
        const valueOf = (bucket: "theme" | "layout" | "indicatorSettings") =>
          settings.find((item) => item.bucket === bucket)?.value;
        const remoteTheme = readRemoteChartTheme(valueOf("theme"));
        if (remoteTheme && remoteTheme !== chartThemeRef.current) {
          suppressNextThemeSyncRef.current = true;
          setChartTheme(remoteTheme);
        }
        const remoteLayout = readRemoteChartLayout(valueOf("layout"));
        if (remoteLayout) {
          const remoteSymbol = remoteLayout.symbol;
          const remoteTimeframe = remoteLayout.timeframe;
          if (!hasInitialChartSymbolQuery() && remoteSymbol) {
            setCurrentSymbol((previous) =>
              previous === remoteSymbol ? previous : remoteSymbol,
            );
            currentSymbolRef.current = remoteSymbol;
          }
          if (remoteTimeframe) {
            setCurrentTimeframe((previous) =>
              previous === remoteTimeframe ? previous : remoteTimeframe,
            );
            currentTimeframeRef.current = remoteTimeframe;
          }
        }
        const remoteIndicatorSettings = valueOf("indicatorSettings");
        if (remoteIndicatorSettings) {
          const merged = mergeChanOverlaySettings(remoteIndicatorSettings);
          suppressNextIndicatorSettingsSyncRef.current = true;
          chanOverlaySettingsRef.current = merged;
          setChanOverlaySettings(merged);
        }
        remoteThemeReadyRef.current = true;
        remoteLayoutReadyRef.current = true;
        remoteIndicatorSettingsReadyRef.current = true;
      })
      .catch(() => {
        if (!cancelled) {
          remoteThemeReadyRef.current = true;
          remoteLayoutReadyRef.current = true;
          remoteIndicatorSettingsReadyRef.current = true;
        }
      });
    return () => {
      cancelled = true;
    };
  }, [session.token]);

  useEffect(() => {
    chartThemeRef.current = chartTheme;
    saveChartTheme(chartTheme);
    document.body.dataset.appTheme = chartTheme;
    void setTradingViewTheme(widgetRef.current, chartTheme);
    if (suppressNextThemeSyncRef.current) {
      suppressNextThemeSyncRef.current = false;
      return;
    }
    if (remoteThemeReadyRef.current) {
      void saveUserSetting(session.token, "theme", { theme: chartTheme }).catch(() => {
        // Local theme changes should remain usable if server-side settings fail.
      });
    }
  }, [chartTheme, session.token]);

  useEffect(() => {
    currentSymbolRef.current = currentSymbol;
  }, [currentSymbol]);

  useEffect(() => {
    currentTimeframeRef.current = currentTimeframe;
  }, [currentTimeframe]);

  useEffect(() => {
    if (!remoteLayoutReadyRef.current) {
      return;
    }
    void saveUserSetting(session.token, "layout", {
      symbol: currentSymbol.toUpperCase(),
      timeframe: currentTimeframe,
    }).catch(() => {
      // Layout is recoverable from the current chart if remote persistence fails.
    });
  }, [currentSymbol, currentTimeframe, session.token]);

  useEffect(() => {
    chanOverlaySettingsRef.current = chanOverlaySettings;
    if (widgetRef.current) {
      void applyChanStudySettings(widgetRef.current, chanOverlaySettings);
    }
    if (suppressNextIndicatorSettingsSyncRef.current) {
      suppressNextIndicatorSettingsSyncRef.current = false;
      return;
    }
    if (remoteIndicatorSettingsReadyRef.current) {
      void saveUserSetting(session.token, "indicatorSettings", chanOverlaySettings).catch(() => {
        // Indicator settings should still apply locally if remote persistence fails.
      });
    }
  }, [chanOverlaySettings, session.token]);

  useEffect(() => {
    let cancelled = false;
    let overlayVersion = 0;
    let disposeSymbolSubscription: (() => void) | null = null;
    let disposeVisibleRangeSubscription: (() => void) | null = null;
    let disposeStudySettingsSubscription: (() => void) | null = null;
    let releaseOverlayRequest: (() => void) | null = null;
    const overlayManager = new ChanOverlayManager();
    const realtimeBridge = new ChanRealtimeOverlayBridge();
    let activeOverlay: {
      generation: number;
      context: ChanRealtimeContext;
      from: number;
      to: number;
      limit: number;
      bars: ApiBar[];
      releaseSocket: (() => void) | null;
      refreshController: AbortController | null;
      refreshPromise: Promise<void> | null;
      pollingGate: ChanRealtimePollingGate | null;
    } | null = null;
    let latestHistoryWindow:
      | {
          symbol: string;
          timeframe: string;
          limit: number;
          from?: number;
          to?: number;
          bars: ApiBar[];
        }
      | null = null;
    const teardownActiveOverlay = () => {
      const active = activeOverlay;
      if (!active) return;
      active.releaseSocket?.();
      active.refreshController?.abort();
      active.pollingGate?.dispose();
      realtimeBridge.unsubscribe(active.context);
      overlayManager.switchContext();
      activeOverlay = null;
    };
    const requestOverlay = (symbol: string, timeframe: string, from: number, to: number, chartBars: ApiBar[]) => {
      const widget = widgetRef.current;
      if (!widget || to < from) return;
      const modes = (["confirmed", "predictive"] as const).filter(
        (mode) => chanOverlaySettingsRef.current.modes[mode],
      );
      const context: ChanRealtimeContext = { symbol: symbol.toUpperCase(), chartTimeframe: timeframe, modes };
      if (activeOverlay && activeOverlay.context.symbol === context.symbol
        && activeOverlay.context.chartTimeframe === context.chartTimeframe
        && activeOverlay.context.modes.join(",") === modes.join(",")
        && activeOverlay.from === from && activeOverlay.to === to) {
        activeOverlay.bars = chartBars;
        return;
      }
      releaseOverlayRequest?.();
      releaseOverlayRequest = null;
      teardownActiveOverlay();
      const requestVersion = ++overlayVersion;
      const session: NonNullable<typeof activeOverlay> = {
        generation: requestVersion,
        context,
        from,
        to,
        limit: Math.max(DEFAULT_BAR_WINDOW_SIZE, chartBars.length),
        bars: chartBars,
        releaseSocket: null,
        refreshController: null,
        refreshPromise: null,
        pollingGate: null,
      };
      activeOverlay = session;
      const isCurrent = () => !cancelled && activeOverlay === session
        && requestVersion === overlayVersion && widgetRef.current === widget;
      const paint = (overlay: ChanOverlayResponse) => {
        if (!isCurrent()) return;
        void renderChanOverlay(widget, overlay, chanOverlaySettingsRef.current, {
          chartBars: session.bars,
          isCurrent,
        });
      };
      const hydrateBridge = (overlay: ChanOverlayResponse) => realtimeBridge.hydrateHttp({
        ...context,
        snapshotVersion: overlay.snapshot_version,
        range: { from: session.from, to: session.to },
        objects: {
          strokes: overlay.strokes,
          segments: overlay.segments,
          centers: overlay.centers,
          signals: overlay.signals,
          channels: overlay.channels,
        },
      });
      const refreshHttp = (source: "resync" | "poll") => {
        if (!isCurrent() || session.refreshPromise) return session.refreshPromise;
        const controller = new AbortController();
        session.refreshController = controller;
        const promise = overlayManager.fetchFresh({
          symbol: context.symbol,
          timeframe: context.chartTimeframe,
          from: session.from,
          to: session.to,
          modes: [...modes],
        }, controller.signal).then((overlay) => {
          if (!isCurrent() || controller.signal.aborted) return;
          hydrateBridge(overlay);
          paint(overlay);
        }).catch((error) => {
          if (!controller.signal.aborted && isCurrent()) {
            recordTvDebug(`chan.overlay.${source}.error`, {
              symbol: context.symbol,
              timeframe: context.chartTimeframe,
              message: error instanceof Error ? error.message : String(error),
            });
          }
        }).finally(() => {
          if (session.refreshController === controller) session.refreshController = null;
          if (session.refreshPromise === promise) session.refreshPromise = null;
        });
        session.refreshPromise = promise;
        return promise;
      };
      session.pollingGate = new ChanRealtimePollingGate(() => {
        if (isCurrent()) void refreshHttp("poll");
      });
      const subscribeRealtime = () => {
        void chartDataManager.subscribeChanOverlay({
          symbol: context.symbol,
          timeframe: context.chartTimeframe,
          levels: chanLevelsForTimeframe(context.chartTimeframe),
          modes,
          from: session.from,
          to: session.to,
          limit: session.limit,
        }, (message) => {
          if (!isCurrent()) return;
          const result = realtimeBridge.apply(message);
          if (result.status === "applied") {
            const overlay = overlayManager.applyRealtime(result.state);
            if (overlay) paint(overlay);
          } else if (result.status === "resync") {
            void refreshHttp("resync");
          }
        }, (status) => {
          if (!isCurrent()) return;
          if (status === "replayed") realtimeBridge.resetTransportEpoch(context);
          recordTvDebug("chan.overlay.transport", { status, symbol: context.symbol, timeframe: context.chartTimeframe });
          session.pollingGate?.update(status);
        }).then((release) => {
          if (!isCurrent()) release();
          else session.releaseSocket = release;
        }).catch((error) => {
          if (!isCurrent()) return;
          session.pollingGate?.update("disconnected");
          recordTvDebug("chan.overlay.subscribe.error", String(error));
        });
      };
      overlayManager.retain(symbol, timeframe, [...modes], { from, to });
      releaseOverlayRequest = overlayManager.request({
        symbol,
        timeframe,
        from,
        to,
        modes: [...modes],
        onPaint: (overlay) => {
          if (!isCurrent()) return;
          hydrateBridge(overlay);
          paint(overlay);
          if (!session.releaseSocket) subscribeRealtime();
        },
        onError: (error) => recordTvDebug("chan.overlay.error", { symbol, timeframe, message: error.message }),
      });
    };
    const unsubscribeHistory = chartDataManager.subscribeHistoryWindows((event) => {
      if (cancelled || event.bars.length === 0) {
        return;
      }
      const nextSymbol = event.symbol.toUpperCase();
      currentSymbolRef.current = nextSymbol;
      currentTimeframeRef.current = event.timeframe;
      setCurrentSymbol((previous) => (previous === nextSymbol ? previous : nextSymbol));
      confirmChartSymbol(nextSymbol);
      setCurrentTimeframe((previous) => (previous === event.timeframe ? previous : event.timeframe));
      latestHistoryWindow = {
        symbol: event.symbol,
        timeframe: event.timeframe,
        limit: Math.max(event.limit, event.bars.length, DEFAULT_BAR_WINDOW_SIZE),
        from: event.bars.length > 0 ? event.from : undefined,
        to: event.bars.length > 0 ? event.to : undefined,
        bars: event.bars,
      };
      const widget = widgetRef.current;
      if (!widget) {
        return;
      }
      const from = event.bars[0]?.time;
      const to = event.bars[event.bars.length - 1]?.time;
      if (from !== undefined && to !== undefined) requestOverlay(event.symbol, event.timeframe, from, to, event.bars);
    });
    const initialSymbol = currentSymbolRef.current;
    const initialTimeframe = currentTimeframeRef.current;
    setChartMode("loading");
    setBars([]);

    createTradingViewWidget(
      widgetContainerId.current,
      initialSymbol,
      initialTimeframe,
      chartThemeRef.current,
    )
      .then(async (widget) => {
        if (!widget) {
          if (!cancelled) {
            setChartMode("fallback");
            void refreshFallbackBars(initialSymbol, setBars);
          }
          return;
        }
        if (cancelled) {
          widget.remove();
          return;
        }
        widgetRef.current = widget;
        const widgetIsCurrent = () => !cancelled && widgetRef.current === widget;
        const widgetSymbol = getWidgetSymbol(widget);
        if (widgetSymbol) {
          currentSymbolRef.current = widgetSymbol;
          setCurrentSymbol((previous) => (previous === widgetSymbol ? previous : widgetSymbol));
          confirmChartSymbol(widgetSymbol);
        }
        if (!await installAsyncSubscription(
          () => subscribeWidgetSymbolChanges(widget, (nextSymbol) => {
            if (activeOverlay && activeOverlay.context.symbol !== nextSymbol.toUpperCase()) teardownActiveOverlay();
            currentSymbolRef.current = nextSymbol;
            setCurrentSymbol((previous) => (previous === nextSymbol ? previous : nextSymbol));
            confirmChartSymbol(nextSymbol);
          }),
          widgetIsCurrent,
          (dispose) => { disposeSymbolSubscription = dispose; },
        )) return;
        if (!await installAsyncSubscription(
          () => subscribeWidgetVisibleRangeChanges(widget, (range) => {
            const timeframe = getWidgetTimeframe(widget) ?? currentTimeframeRef.current;
            const symbol = getWidgetSymbol(widget) ?? currentSymbolRef.current;
            const bars = latestHistoryWindow?.symbol.toUpperCase() === symbol.toUpperCase()
              && latestHistoryWindow.timeframe === timeframe
              ? latestHistoryWindow.bars
              : [];
            requestOverlay(symbol, timeframe, Math.floor(range.from), Math.floor(range.to), bars);
          }),
          widgetIsCurrent,
          (dispose) => { disposeVisibleRangeSubscription = dispose; },
        )) return;
        if (!widgetIsCurrent()) return;
        disposeStudySettingsSubscription = subscribeChanStudySettingsChanges(
          widget,
          (nextSettings) => {
            chanOverlaySettingsRef.current = nextSettings;
            setChanOverlaySettings(nextSettings);
            const active = activeOverlay;
            if (active) requestOverlay(active.context.symbol, active.context.chartTimeframe, active.from, active.to, active.bars);
          },
        );
        setChartMode("tradingview");
        const initialWindow =
          latestHistoryWindow &&
          latestHistoryWindow.symbol.toUpperCase() === initialSymbol.toUpperCase() &&
          latestHistoryWindow.timeframe === initialTimeframe
            ? latestHistoryWindow
            : null;
        const initialBars = initialWindow?.bars ?? [];
        const from = initialBars[0]?.time;
        const to = initialBars[initialBars.length - 1]?.time;
        if (from !== undefined && to !== undefined) {
          requestOverlay(initialSymbol, initialTimeframe, from, to, initialBars);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setChartMode("fallback");
          void refreshFallbackBars(initialSymbol, setBars);
        }
      });

    return () => {
      cancelled = true;
      const current = widgetRef.current;
      widgetRef.current = null;
      disposeSymbolSubscription?.();
      disposeVisibleRangeSubscription?.();
      disposeStudySettingsSubscription?.();
      releaseOverlayRequest?.();
      teardownActiveOverlay();
      current?.remove();
      overlayManager.dispose();
      unsubscribeHistory();
    };
  }, [session.token]);

  useEffect(() => {
    const widget = widgetRef.current;
    if (!widget) {
      return;
    }
    const nextSymbol = currentSymbol.toUpperCase();
    if (getWidgetSymbol(widget) === nextSymbol) {
      return;
    }
    void setWidgetSymbol(widget, nextSymbol, currentTimeframeRef.current);
  }, [currentSymbol]);

  useEffect(() => {
    const widget = widgetRef.current;
    if (!widget) {
      return;
    }
    if (getWidgetTimeframe(widget) === currentTimeframe) {
      return;
    }
    void setWidgetTimeframe(widget, currentTimeframe);
  }, [currentTimeframe]);

  function handleSelectSymbol(symbol: string) {
    const normalized = symbol.toUpperCase();
    currentSymbolRef.current = normalized;
    // The TradingView symbol callback confirms the chart switch before the
    // sidebar changes context; clicking a watchlist row is only a request.
    setCurrentSymbol(normalized);
  }

  function toggleChartTheme() {
    setChartTheme((current) => (current === "dark" ? "light" : "dark"));
  }

  function collapseRightSidebarForScreener() {
    setRightSidebarCollapseSignal((current) => current + 1);
  }

  return (
    <section className="chart-workspace" aria-label="TradingView chart">
      <div className="chart-frame">
        <div id={widgetContainerId.current} className="tv-container" />
        <button
          type="button"
          className="chart-theme-toggle"
          title={chartTheme === "dark" ? "切换到白色主题" : "切换到黑色主题"}
          aria-label={chartTheme === "dark" ? "切换到白色主题" : "切换到黑色主题"}
          onClick={toggleChartTheme}
        >
          {chartTheme === "dark" ? <Sun size={17} /> : <Moon size={17} />}
        </button>
        <div className="chart-realtime-status" data-state={realtimeFeedback.state} role="status" aria-live="polite">
          <strong>{realtimeStateLabel(realtimeFeedback.state)}</strong>
          <span>{realtimeFeedback.channel === "bars" ? "K线" : "缠论"}</span>
          <span>最近事件 {formatRealtimeAge(realtimeFeedback.lastEventAt, feedbackNow)}</span>
        </div>
        {chartMode === "loading" ? (
          <div className="chart-loading">Loading TradingView</div>
        ) : null}
        {chartMode === "fallback" ? <FallbackChart bars={bars} /> : null}
      </div>
      <ScreenerDock
        onSelectSymbol={handleSelectSymbol}
        onOpenPanel={collapseRightSidebarForScreener}
        authToken={session.token}
      />
      <RightSidebar
        activeSymbol={confirmedChartSymbol}
        timeframe={currentTimeframe}
        collapseSignal={rightSidebarCollapseSignal}
        onSelectSymbol={handleSelectSymbol}
        marketSnapshot={marketSnapshot}
        onWatchlistSymbolsChange={handleWatchlistSymbolsChange}
        authToken={session.token}
        isAdmin={session.role === "admin"}
        onOpenAdmin={onOpenAdmin}
        onLogout={onLogout}
      />
    </section>
  );
}

function realtimeStateLabel(state: RealtimeFeedback["state"]): string {
  if (state === "live") return "实时";
  if (state === "degraded") return "已降级";
  return "连接中";
}

function formatRealtimeAge(lastEventAt: number | undefined, now: number): string {
  if (lastEventAt === undefined) return "--";
  return `${Math.max(0, now - lastEventAt)}ms`;
}

async function refreshFallbackBars(
  symbol: string,
  setBars: (bars: ApiBar[]) => void,
) {
  try {
    const response = await getBars(symbol, DEFAULT_TIMEFRAME, DEFAULT_BAR_WINDOW_SIZE);
    setBars(response.bars);
  } catch {
    setBars([]);
  }
}

function FallbackChart({ bars }: { bars: ApiBar[] }) {
  const width = 920;
  const height = 360;
  const visible = bars.slice(-100);
  const lows = visible.map((bar) => bar.low);
  const highs = visible.map((bar) => bar.high);
  const rawMin = lows.length > 0 ? Math.min(...lows) : 0;
  const rawMax = highs.length > 0 ? Math.max(...highs) : 1;
  const rawSpan = Math.max(rawMax - rawMin, 0.01);
  const min = rawMin - rawSpan * 0.08;
  const span = rawSpan * 1.16;
  const candleWidth = width / Math.max(visible.length, 1);

  return (
    <div className="fallback-chart">
      {bars.length === 0 ? (
        <div className="chart-loading">TradingView library unavailable</div>
      ) : null}
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Fallback K-line chart">
        {visible.map((bar, index) => {
          const x = index * candleWidth + candleWidth / 2;
          const yHigh = height - ((bar.high - min) / span) * height;
          const yLow = height - ((bar.low - min) / span) * height;
          const yOpen = height - ((bar.open - min) / span) * height;
          const yClose = height - ((bar.close - min) / span) * height;
          const up = bar.close >= bar.open;
          return (
            <g key={`${bar.time}-${index}`} data-up={up}>
              <line x1={x} x2={x} y1={yHigh} y2={yLow} />
              <rect
                x={x - Math.max(2, candleWidth * 0.28)}
                y={Math.min(yOpen, yClose)}
                width={Math.max(4, candleWidth * 0.56)}
                height={Math.max(2, Math.abs(yClose - yOpen))}
              />
            </g>
          );
        })}
      </svg>
    </div>
  );
}
