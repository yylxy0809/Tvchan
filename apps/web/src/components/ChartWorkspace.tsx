import { useEffect, useRef, useState } from "react";
import { chartDataManager } from "../api/chartDataManager";
import { DEFAULT_CHAN_LEVELS, type ApiBar } from "../api/client";
import { listUserSettings, saveUserSetting } from "../api/userSettings";
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
const MAX_CHART_BUNDLE_REQUEST_BARS = 5_000;
const CHAN_RENDER_LEVELS = DEFAULT_CHAN_LEVELS;

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
  const [bars, setBars] = useState<ApiBar[]>([]);
  const [currentSymbol, setCurrentSymbol] = useState(loadInitialChartSymbol);
  const [currentTimeframe, setCurrentTimeframe] = useState(DEFAULT_TIMEFRAME);
  const currentSymbolRef = useRef(currentSymbol);
  const currentTimeframeRef = useRef(currentTimeframe);

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
    let disposeStudySettingsSubscription: (() => void) | null = null;
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
    let chanSnapshotSubscriptionKey = "";
    let disposeChanSnapshotSubscription: (() => void) | null = null;
    const syncChanSnapshotSubscription = (windowRequest: {
      symbol: string;
      timeframe: string;
      limit: number;
      from?: number;
      to?: number;
    }) => {
      const nextKey = [
        windowRequest.symbol.toUpperCase(),
        windowRequest.timeframe,
        windowRequest.limit,
        windowRequest.from ?? "",
        windowRequest.to ?? "",
      ].join("|");
      if (nextKey === chanSnapshotSubscriptionKey) {
        return;
      }
      disposeChanSnapshotSubscription?.();
      disposeChanSnapshotSubscription = null;
      chanSnapshotSubscriptionKey = nextKey;
      void chartDataManager
        .subscribeChanSnapshots({
          symbol: windowRequest.symbol,
          timeframe: windowRequest.timeframe,
          limit: Math.min(windowRequest.limit, MAX_CHART_BUNDLE_REQUEST_BARS),
          from: windowRequest.from,
          to: windowRequest.to,
          levels: CHAN_RENDER_LEVELS,
        })
        .then((dispose) => {
          if (cancelled || chanSnapshotSubscriptionKey !== nextKey) {
            dispose();
            return;
          }
          disposeChanSnapshotSubscription = dispose;
        })
        .catch(() => {
          if (chanSnapshotSubscriptionKey === nextKey) {
            chanSnapshotSubscriptionKey = "";
          }
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
      syncChanSnapshotSubscription({
        symbol: event.symbol,
        timeframe: event.timeframe,
        limit: latestHistoryWindow.limit,
        from: latestHistoryWindow.from,
        to: latestHistoryWindow.to,
      });
      const requestVersion = ++overlayVersion;
      void renderCurrentChanOverlay({
        widget,
        symbol: event.symbol,
        timeframe: event.timeframe,
        limit: latestHistoryWindow.limit,
        from: latestHistoryWindow.from,
        to: latestHistoryWindow.to,
        settings: chanOverlaySettingsRef.current,
        chartBars: event.bars,
        isCurrent: () =>
          !cancelled &&
          requestVersion === overlayVersion &&
          widgetRef.current === widget &&
          currentSymbolRef.current === nextSymbol &&
          (!getWidgetTimeframe(widget) || getWidgetTimeframe(widget) === event.timeframe),
      });
    });
    const unsubscribeSnapshots = chartDataManager.subscribeSnapshotUpdates((event) => {
      if (event.source !== "realtime") {
        return;
      }
      const widget = widgetRef.current;
      if (!widget || cancelled) {
        return;
      }
      const activeTimeframe = getWidgetTimeframe(widget) ?? DEFAULT_TIMEFRAME;
      if (event.timeframe !== activeTimeframe && event.timeframe !== latestHistoryWindow?.timeframe) {
        return;
      }
      if (
        !latestHistoryWindow ||
        latestHistoryWindow.symbol.toUpperCase() !== event.symbol.toUpperCase() ||
        latestHistoryWindow.timeframe !== activeTimeframe
      ) {
        return;
      }
      const requestVersion = ++overlayVersion;
      void renderCurrentChanOverlay({
        widget,
        symbol: event.symbol,
        timeframe: activeTimeframe,
        limit: latestHistoryWindow.limit,
        from: latestHistoryWindow.from,
        to: latestHistoryWindow.to,
        settings: chanOverlaySettingsRef.current,
        chartBars: latestHistoryWindow.bars,
        isCurrent: () =>
          !cancelled &&
          requestVersion === overlayVersion &&
          widgetRef.current === widget &&
          currentSymbolRef.current === event.symbol.toUpperCase() &&
          (!getWidgetTimeframe(widget) || getWidgetTimeframe(widget) === activeTimeframe),
      });
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
      { onToggleTheme: toggleChartTheme },
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
        disposeSymbolSubscription = await subscribeWidgetSymbolChanges(widget, (nextSymbol) => {
          currentSymbolRef.current = nextSymbol;
          setCurrentSymbol((previous) => (previous === nextSymbol ? previous : nextSymbol));
        });
        disposeStudySettingsSubscription = subscribeChanStudySettingsChanges(
          widget,
          (nextSettings) => {
            chanOverlaySettingsRef.current = nextSettings;
            setChanOverlaySettings(nextSettings);
          },
        );
        setChartMode("tradingview");
        const initialWindow =
          latestHistoryWindow &&
          latestHistoryWindow.symbol.toUpperCase() === initialSymbol.toUpperCase() &&
          latestHistoryWindow.timeframe === initialTimeframe
            ? latestHistoryWindow
            : null;
        const requestVersion = ++overlayVersion;
        void renderCurrentChanOverlay({
          widget,
          symbol: initialSymbol,
          timeframe: initialTimeframe,
          limit: initialWindow?.limit ?? DEFAULT_BAR_WINDOW_SIZE,
          from: initialWindow?.from,
          to: initialWindow?.to,
          settings: chanOverlaySettingsRef.current,
          chartBars: initialWindow?.bars,
          isCurrent: () =>
            !cancelled &&
            requestVersion === overlayVersion &&
            widgetRef.current === widget &&
            (!getWidgetTimeframe(widget) || getWidgetTimeframe(widget) === initialTimeframe),
        });
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
      disposeStudySettingsSubscription?.();
      current?.remove();
      disposeChanSnapshotSubscription?.();
      unsubscribeHistory();
      unsubscribeSnapshots();
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
    setCurrentSymbol(symbol.toUpperCase());
  }

  function toggleChartTheme() {
    setChartTheme((current) => (current === "dark" ? "light" : "dark"));
  }

  return (
    <section className="chart-workspace" aria-label="TradingView chart">
      <div className="chart-frame">
        <div id={widgetContainerId.current} className="tv-container" />
        {chartMode === "loading" ? (
          <div className="chart-loading">Loading TradingView</div>
        ) : null}
        {chartMode === "fallback" ? <FallbackChart bars={bars} /> : null}
      </div>
      <ScreenerDock onSelectSymbol={handleSelectSymbol} />
      <RightSidebar
        activeSymbol={currentSymbol}
        timeframe={currentTimeframe}
        onSelectSymbol={handleSelectSymbol}
        authToken={session.token}
        isAdmin={session.role === "admin"}
        onOpenAdmin={onOpenAdmin}
        onLogout={onLogout}
      />
    </section>
  );
}

async function refreshFallbackBars(
  symbol: string,
  setBars: (bars: ApiBar[]) => void,
) {
  try {
    const response = await chartDataManager.getChartWindow({
      symbol,
      timeframe: DEFAULT_TIMEFRAME,
      limit: DEFAULT_BAR_WINDOW_SIZE,
    });
    setBars(response.bars);
  } catch {
    setBars([]);
  }
}

async function renderCurrentChanOverlay({
  widget,
  symbol,
  timeframe,
  limit,
  from,
  to,
  settings,
  chartBars = [],
  isCurrent,
}: {
  widget: TradingViewWidget;
  symbol: string;
  timeframe: string;
  limit: number;
  from?: number;
  to?: number;
  settings: ReturnType<typeof createDefaultChanOverlaySettings>;
  chartBars?: ApiBar[];
  isCurrent(): boolean;
}) {
  try {
    recordTvDebug("chan.renderCurrent.request", {
      symbol,
      timeframe,
      limit: Math.min(limit, MAX_CHART_BUNDLE_REQUEST_BARS),
      from,
      to,
      chartBars: chartBars.length,
    });
    const window = await chartDataManager.getChartWindow({
      symbol,
      timeframe,
      limit: Math.min(limit, MAX_CHART_BUNDLE_REQUEST_BARS),
      from,
      to,
      levels: CHAN_RENDER_LEVELS,
    });
    const projectedBars = chartBars.length > 0 ? chartBars : window.bars;
    const chan = window.chan;
    if (!isCurrent()) {
      recordTvDebug("chan.renderCurrent.stale", {
        symbol,
        timeframe,
        snapshotVersion: chan.snapshot_version,
      });
      return;
    }
    recordTvDebug("chan.renderCurrent.response", {
      symbol,
      timeframe,
      snapshotVersion: chan.snapshot_version,
      strokes: chan.strokes.length,
      segments: chan.segments.length,
      centers: chan.centers.length,
      signals: chan.signals.length,
      chartBars: projectedBars.length,
    });
    await renderChanOverlay(widget, chan, settings, {
      isCurrent,
      chartBars: projectedBars,
    });
  } catch (error) {
    recordTvDebug("chan.renderCurrent.error", {
      symbol,
      timeframe,
      message: error instanceof Error ? error.message : String(error),
    });
    if (TRADINGVIEW_DEBUG) {
      console.warn("[chan-render-current-failed]", error);
    }
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
