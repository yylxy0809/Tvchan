import type { ApiBar, ChanOverlayResponse } from "../api/client";
import { CHAN_STUDY_ENABLED, TRADINGVIEW_DEBUG } from "../config";
import {
  buildChanStudyOverrides,
  CHAN_STUDY_DESCRIPTION,
  clearChanStudyOverlay,
  createChanCustomIndicators,
  getChanStudyFallbackCenters,
  setChanStudyOverlay,
} from "./chanStudy";
import {
  chanOverlaySettingsToStudyInputs,
  studyInputItemsFromSettings,
  studyInputValuesToOverlaySettings,
} from "./chanStudySettings";
import { getChanLineStyle } from "./chanStyles";
import { createDatafeed } from "./datafeed";
import { patchTvDebug, recordTvDebug } from "./debug";
import {
  createDefaultChanOverlaySettings,
  type ChanOverlaySettings,
} from "./overlaySettings";
import { INTERVAL_BY_TIMEFRAME, timeframeFromTradingViewInterval, toTradingViewTime } from "./time";

type ShapeId = string | number;
type StudyInputValue = string | number | boolean;

type StudyInputValueItem = {
  id: string;
  value: StudyInputValue;
};

type TradingViewStudy = {
  getInputValues(): StudyInputValueItem[];
  setInputValues(values: StudyInputValueItem[]): void;
  applyOverrides?(overrides: Record<string, unknown>): void;
  setUserEditEnabled?(enabled: boolean): void;
};

type ShapePoint = {
  time: number;
  price: number;
};

type TradingViewChart = {
  dataReady(callback?: () => void): boolean;
  resolution?(): string;
  symbol?(): string;
  setSymbol?(symbol: string, interval?: string, callback?: () => void): void;
  setResolution?(resolution: string, callback?: () => void): void;
  onSymbolChanged?(): {
    subscribe?(context: unknown, callback: (symbolInfo?: { ticker?: string }) => void): void;
    unsubscribe?(context: unknown, callback: (symbolInfo?: { ticker?: string }) => void): void;
    unsubscribeAll?(context?: unknown): void;
  };
  createStudy(
    name: string,
    forceOverlay?: boolean,
    lock?: boolean,
    inputs?: Record<string, unknown>,
    overrides?: Record<string, unknown>,
  ): Promise<ShapeId | null> | ShapeId | null;
  getStudyById(id: ShapeId): TradingViewStudy;
  showPropertiesDialog(id: ShapeId): void;
  createMultipointShape(
    points: ShapePoint[],
    options: Record<string, unknown>,
  ): Promise<ShapeId>;
  createShape(point: ShapePoint, options: Record<string, unknown>): Promise<ShapeId>;
  removeEntity(id: ShapeId): void;
};

export type TradingViewWidget = {
  onChartReady(callback: () => void): void;
  headerReady?(): Promise<void>;
  createButton?(options?: {
    align?: "left" | "right";
    useTradingViewStyle?: boolean;
  }): HTMLElement;
  activeChart(): TradingViewChart;
  changeTheme?(theme: "Dark" | "Light"): Promise<void> | void;
  remove(): void;
};

export type ChartTheme = "dark" | "light";

declare global {
  interface Window {
    TradingView?: {
      widget: new (options: Record<string, unknown>) => TradingViewWidget;
    };
    __TV_CHART_WIDGET__?: TradingViewWidget;
    __CHAN_OVERLAY_RENDER__?: Record<string, unknown>;
  }
}

const SCRIPT_CANDIDATES = [
  "/charting_library/charting_library.js",
  "/charting_library/charting_library.standalone.js",
];
const DISABLED_PERSISTENCE_FEATURES = [
  "use_localstorage_for_settings",
  "save_chart_properties_to_local_storage",
  "header_saveload",
  "study_templates",
];

const overlayShapeIds = new WeakMap<TradingViewWidget, ShapeId[]>();
const studyIds = new WeakMap<TradingViewWidget, ShapeId>();
const studyDatasetKeys = new WeakMap<TradingViewWidget, string>();
const studySettingsSyncTimers = new WeakMap<TradingViewWidget, number>();
const studyInputSignatures = new WeakMap<TradingViewWidget, string>();
const studySettingsChangeHandlers = new WeakMap<
  TradingViewWidget,
  (settings: ChanOverlaySettings) => void
>();
const themeButtons = new WeakMap<TradingViewWidget, HTMLElement>();

export async function createTradingViewWidget(
  containerId: string,
  symbol: string,
  timeframe: string,
  theme: ChartTheme = "dark",
  options: { onToggleTheme?: () => void } = {},
): Promise<TradingViewWidget | null> {
  const container = document.getElementById(containerId);
  if (container) {
    container.innerHTML = "";
    container.dataset.tvWidgetPhase = "loading-script";
  }

  recordTvDebug("widget.loadScript.start");
  const loaded = await ensureTradingViewScript();
  if (!loaded || !window.TradingView?.widget) {
    recordTvDebug("widget.loadScript.failed");
    setChartDataset({
      tvWidgetPhase: "script-failed",
    });
    return null;
  }

  const interval = INTERVAL_BY_TIMEFRAME[timeframe] ?? "5";
  recordTvDebug("widget.create", { symbol, timeframe, interval });
  const enabledFeatures = [
    "iframe_loading_same_origin",
    "items_favoriting",
    "widgetbar_tabs",
    "multiple_watchlists",
    "watchlist_import_export",
    "watchlist_context_menu",
    "support_multicharts",
    "show_right_widgets_panel_by_default",
    ...(TRADINGVIEW_DEBUG ? ["charting_library_debug_mode"] : []),
  ];
  const widgetOptions: Record<string, unknown> = {
    symbol,
    interval,
    container: containerId,
    library_path: "/charting_library/",
    locale: "zh",
    timezone: "Asia/Shanghai",
    autosize: true,
    datafeed: createDatafeed(),
    enabled_features: enabledFeatures,
    disabled_features: DISABLED_PERSISTENCE_FEATURES,
    load_last_chart: false,
    debug: TRADINGVIEW_DEBUG,
    theme: toTradingViewTheme(theme),
  };
  if (CHAN_STUDY_ENABLED) {
    widgetOptions.custom_indicators_getter = createChanCustomIndicators;
  }
  const widget = new window.TradingView.widget(widgetOptions);

  window.__TV_CHART_WIDGET__ = widget;
  setChartDataset({
    tvWidgetPhase: "created",
    tvSymbol: symbol,
    tvTimeframe: timeframe,
    tvInterval: interval,
  });
  const ready = await whenChartReady(widget, 10000);
  setChartDataset({
    tvWidgetPhase: ready ? "ready" : "ready-timeout",
    tvReady: String(ready),
  });
  if (options.onToggleTheme) {
    await installThemeButton(widget, theme, options.onToggleTheme);
  }
  patchTvDebug("widget", {
    ready,
    symbol,
    timeframe,
    interval,
    iframeLoading: "same-origin",
    tradingViewDebug: TRADINGVIEW_DEBUG,
    chanStudyEnabled: CHAN_STUDY_ENABLED,
    disabledPersistenceFeatures: DISABLED_PERSISTENCE_FEATURES,
  });
  return widget;
}

export async function setTradingViewTheme(
  widget: TradingViewWidget | null,
  theme: ChartTheme,
): Promise<void> {
  if (!widget) {
    return;
  }
  await widget.changeTheme?.(toTradingViewTheme(theme));
  updateTradingViewThemeButton(widget, theme);
}

export function updateTradingViewThemeButton(
  widget: TradingViewWidget | null,
  theme: ChartTheme,
): void {
  const button = widget ? themeButtons.get(widget) : undefined;
  if (!button) {
    return;
  }
  button.textContent = theme === "dark" ? "白色" : "黑色";
  button.setAttribute(
    "title",
    theme === "dark" ? "切换到白色主题" : "切换到黑色主题",
  );
  button.setAttribute(
    "aria-label",
    theme === "dark" ? "切换到白色主题" : "切换到黑色主题",
  );
}

export async function clearChanOverlay(widget: TradingViewWidget): Promise<void> {
  stopChanStudySettingsSync(widget);
  const chart = await getActiveChart(widget);
  if (!chart) {
    return;
  }
  clearChanStudyOverlay();
  const studyId = studyIds.get(widget);
  if (studyId !== undefined) {
    chart.removeEntity(studyId);
    studyIds.delete(widget);
    studyDatasetKeys.delete(widget);
  }
  for (const id of overlayShapeIds.get(widget) ?? []) {
    chart.removeEntity(id);
  }
  overlayShapeIds.set(widget, []);
}

export function getWidgetTimeframe(widget: TradingViewWidget | null): string | null {
  try {
    const interval = widget?.activeChart?.().resolution?.();
    return timeframeFromTradingViewInterval(interval);
  } catch {
    return null;
  }
}

export function getWidgetSymbol(widget: TradingViewWidget | null): string | null {
  try {
    const symbol = widget?.activeChart?.().symbol?.();
    return typeof symbol === "string" && symbol.trim() ? symbol.toUpperCase() : null;
  } catch {
    return null;
  }
}

export async function setWidgetSymbol(
  widget: TradingViewWidget | null,
  symbol: string,
  timeframe?: string | null,
): Promise<void> {
  if (!widget) {
    return;
  }
  const chart = await getActiveChart(widget);
  if (!chart?.setSymbol) {
    return;
  }
  const nextSymbol = symbol.toUpperCase();
  if (chart.symbol?.().toUpperCase() === nextSymbol) {
    return;
  }
  const interval = timeframe ? (INTERVAL_BY_TIMEFRAME[timeframe] ?? timeframe) : undefined;
  await new Promise<void>((resolve) => {
    let completed = false;
    const finish = () => {
      if (completed) {
        return;
      }
      completed = true;
      resolve();
    };
    const timeout = window.setTimeout(finish, 2000);
    try {
      chart.setSymbol?.(nextSymbol, interval, () => {
        window.clearTimeout(timeout);
        finish();
      });
    } catch {
      window.clearTimeout(timeout);
      finish();
    }
  });
}

export async function setWidgetTimeframe(
  widget: TradingViewWidget | null,
  timeframe: string,
): Promise<void> {
  if (!widget) {
    return;
  }
  const chart = await getActiveChart(widget);
  if (!chart?.setResolution) {
    return;
  }
  const interval = INTERVAL_BY_TIMEFRAME[timeframe] ?? timeframe;
  if (chart.resolution?.() === interval) {
    return;
  }
  await new Promise<void>((resolve) => {
    let completed = false;
    const finish = () => {
      if (completed) {
        return;
      }
      completed = true;
      resolve();
    };
    const timeout = window.setTimeout(finish, 1500);
    try {
      chart.setResolution?.(interval, () => {
        window.clearTimeout(timeout);
        finish();
      });
    } catch {
      window.clearTimeout(timeout);
      finish();
    }
  });
}

export async function subscribeWidgetSymbolChanges(
  widget: TradingViewWidget | null,
  onSymbolChange: (symbol: string) => void,
): Promise<() => void> {
  if (!widget) {
    return () => {};
  }
  const chart = await getActiveChart(widget);
  const subscription = chart?.onSymbolChanged?.();
  if (!subscription?.subscribe) {
    return () => {};
  }
  const handler = (symbolInfo?: { ticker?: string }) => {
    const symbol = symbolInfo?.ticker ?? chart?.symbol?.() ?? "";
    if (symbol) {
      onSymbolChange(symbol.toUpperCase());
    }
  };
  subscription.subscribe(null, handler);
  return () => {
    try {
      subscription.unsubscribe?.(null, handler);
    } catch {
      subscription.unsubscribeAll?.();
    }
  };
}

export async function renderChanOverlay(
  widget: TradingViewWidget,
  overlay: ChanOverlayResponse,
  settings: ChanOverlaySettings = createDefaultChanOverlaySettings(),
  options: { isCurrent?: () => boolean; chartBars?: ApiBar[] } = {},
): Promise<void> {
  const isCurrent = options.isCurrent ?? (() => true);
  const activeTimeframe = getWidgetTimeframe(widget);
  if (!isCurrent() || (activeTimeframe && overlay.chart_timeframe !== activeTimeframe)) {
    if (TRADINGVIEW_DEBUG) {
      console.info("[chan-overlay-skip-stale]", {
        overlayTimeframe: overlay.chart_timeframe,
        activeTimeframe,
      });
    }
    return;
  }
  const startedState = {
    phase: "started",
    engine: overlay.engine,
    chartTimeframe: overlay.chart_timeframe,
    levels: overlay.levels,
    strokes: overlay.strokes.length,
    segments: overlay.segments.length,
    centers: overlay.centers.length,
    signals: overlay.signals.length,
  };
  window.__CHAN_OVERLAY_RENDER__ = {
    ...startedState,
  };
  setChanRenderDataset(startedState);
  const chart = await getActiveChart(widget);
  if (!chart) {
    window.__CHAN_OVERLAY_RENDER__ = {
      ...window.__CHAN_OVERLAY_RENDER__,
      phase: "chart-unavailable",
    };
    setChanRenderDataset({
      ...startedState,
      phase: "chart-unavailable",
    });
    return;
  }
  const dataReady = await waitForChartData(widget, 10000);
  if (!isCurrent()) {
    return;
  }
  await clearDrawings(widget, chart);
  const createdIds: ShapeId[] = [];
  const cleanupCreated = () => {
    for (const id of createdIds) {
      chart.removeEntity(id);
    }
  };
  const studyReady = await renderChanStudy(widget, overlay, settings, options.chartBars ?? []);
  if (!isCurrent()) {
    cleanupCreated();
    return;
  }
  const strokeDrawings: ChanOverlayResponse["strokes"] = [];
  const segmentDrawings: ChanOverlayResponse["segments"] = [];
  const centerDrawings = studyReady ? getChanStudyFallbackCenters() : overlay.centers;
  const renderState = {
    engine: overlay.engine,
    chartTimeframe: overlay.chart_timeframe,
    requestedBarCount: overlay.requested_bar_count,
    barsByLevel: overlay.bars_by_level,
    levels: overlay.levels,
    strokes: overlay.strokes.length,
    segments: overlay.segments.length,
    centers: overlay.centers.length,
    strokeDrawings: strokeDrawings.length,
    segmentDrawings: segmentDrawings.length,
    centerDrawings: centerDrawings.length,
    dataReady,
    pineStrokes: overlay.strokes.length,
    pineSegments: overlay.segments.length,
    pineCenters: overlay.centers.length - centerDrawings.length,
    pineSignals: overlay.signals.length,
    lineRenderer: "pinejs",
    centerRenderer: studyReady ? "pinejs" : "drawings",
    signalRenderer: "pinejs",
    signals: overlay.signals.length,
    studyReady,
    phase: "rendered",
  };
  window.__CHAN_OVERLAY_RENDER__ = renderState;
  setChanRenderDataset(renderState);
  patchTvDebug("overlay", renderState);
  if (TRADINGVIEW_DEBUG) {
    console.info("[chan-overlay-render]", renderState);
  }

  for (const center of centerDrawings) {
    if (!isCurrent()) {
      cleanupCreated();
      return;
    }
    createdIds.push(...(await drawCenter(chart, center, settings, overlay.chart_timeframe)));
  }

  if (!isCurrent()) {
    cleanupCreated();
    return;
  }
  overlayShapeIds.set(widget, createdIds);
  const completedState = {
    ...renderState,
    actualDrawings: createdIds.length,
    actualDrawingIds: createdIds.slice(0, 12).map(String),
    phase: "rendered",
  };
  window.__CHAN_OVERLAY_RENDER__ = completedState;
  setChanRenderDataset(completedState);
  patchTvDebug("overlay", completedState);
}

export async function waitForChartData(
  widget: TradingViewWidget,
  timeoutMs = 10000,
): Promise<boolean> {
  const chart = await getActiveChart(widget);
  if (!chart) {
    return false;
  }
  if (chart.dataReady()) {
    return true;
  }
  return new Promise((resolve) => {
    const timeout = window.setTimeout(() => resolve(false), timeoutMs);
    chart.dataReady(() => {
      window.clearTimeout(timeout);
      resolve(true);
    });
  });
}

async function ensureChanStudy(
  widget: TradingViewWidget,
  settings: ChanOverlaySettings,
): Promise<ShapeId | undefined> {
  const existingId = studyIds.get(widget);
  if (existingId !== undefined) {
    return existingId;
  }
  const chart = await getActiveChart(widget);
  if (!chart) {
    return undefined;
  }
  const id = await chart.createStudy(
    CHAN_STUDY_DESCRIPTION,
    true,
    false,
    chanOverlaySettingsToStudyInputs(settings),
    buildChanStudyOverrides(settings),
  );
  if (id !== null) {
    studyIds.set(widget, id);
    const study = chart.getStudyById(id);
    study.applyOverrides?.(buildChanStudyOverrides(settings));
    study.setUserEditEnabled?.(true);
    return id;
  }
  return undefined;
}

function readCurrentChanStudySettings(
  chart: TradingViewChart,
  studyId: ShapeId,
  fallback: ChanOverlaySettings,
): ChanOverlaySettings {
  try {
    return studyInputValuesToOverlaySettings(
      chart.getStudyById(studyId).getInputValues(),
      fallback,
    );
  } catch {
    return fallback;
  }
}

function studyInputSignature(values: StudyInputValueItem[]): string {
  return JSON.stringify(values.map((item) => [item.id, item.value]));
}

function stopChanStudySettingsSync(widget: TradingViewWidget): void {
  const timer = studySettingsSyncTimers.get(widget);
  if (timer !== undefined) {
    window.clearInterval(timer);
    studySettingsSyncTimers.delete(widget);
  }
  studyInputSignatures.delete(widget);
}

function startChanStudySettingsSync(
  widget: TradingViewWidget,
  fallback: ChanOverlaySettings,
): void {
  if (studySettingsSyncTimers.has(widget)) {
    return;
  }

  const sync = () => {
    try {
      const chart = widget.activeChart();
      const studyId = studyIds.get(widget);
      if (studyId === undefined) {
        stopChanStudySettingsSync(widget);
        return;
      }
      const study = chart.getStudyById(studyId);
      const inputValues = study.getInputValues();
      const signature = studyInputSignature(inputValues);
      if (studyInputSignatures.get(widget) === signature) {
        return;
      }
      const currentSettings = studyInputValuesToOverlaySettings(inputValues, fallback);
      study.applyOverrides?.(buildChanStudyOverrides(currentSettings));
      studyInputSignatures.set(widget, signature);
      studySettingsChangeHandlers.get(widget)?.(currentSettings);
    } catch {
      stopChanStudySettingsSync(widget);
    }
  };

  try {
    const chart = widget.activeChart();
    const studyId = studyIds.get(widget);
    if (studyId !== undefined) {
      studyInputSignatures.set(
        widget,
        studyInputSignature(chart.getStudyById(studyId).getInputValues()),
      );
    }
  } catch {
    // The next timer tick will stop itself if the study is unavailable.
  }
  studySettingsSyncTimers.set(widget, window.setInterval(sync, 1000));
}

async function renderChanStudy(
  widget: TradingViewWidget,
  overlay: ChanOverlayResponse,
  settings: ChanOverlaySettings,
  chartBars: ApiBar[] = [],
): Promise<boolean> {
  if (!CHAN_STUDY_ENABLED) {
    clearChanStudyOverlay();
    return false;
  }
  try {
    const chart = await getActiveChart(widget);
    if (!chart) {
      return false;
    }
    setChanStudyOverlay(overlay, chartBars);
    const chartBarsFirst = chartBars[0]?.time ?? "";
    const chartBarsLast = chartBars[chartBars.length - 1]?.time ?? "";
    const datasetKey = [
      overlay.symbol.toUpperCase(),
      overlay.chart_timeframe,
      overlay.snapshot_version,
      overlay.strokes.length,
      overlay.segments.length,
      overlay.centers.length,
      overlay.signals.length,
      overlay.channels.length,
      chartBarsFirst,
      chartBarsLast,
    ].join("|");
    const existingId = studyIds.get(widget);
    const existingDatasetKey = studyDatasetKeys.get(widget);
    const effectiveSettings = existingId === undefined
      ? settings
      : readCurrentChanStudySettings(chart, existingId, settings);
    if (existingId !== undefined && existingDatasetKey !== datasetKey) {
      chart.removeEntity(existingId);
      studyIds.delete(widget);
      studyDatasetKeys.delete(widget);
    }
    const studyId = await ensureChanStudy(widget, effectiveSettings);
    if (studyId !== undefined) {
      studyDatasetKeys.set(widget, datasetKey);
      const study = chart.getStudyById(studyId);
      const currentSettings = readCurrentChanStudySettings(chart, studyId, effectiveSettings);
      study.applyOverrides?.(buildChanStudyOverrides(currentSettings));
      studyInputSignatures.set(widget, studyInputSignature(study.getInputValues()));
      startChanStudySettingsSync(widget, currentSettings);
    }
    patchTvDebug("chanStudy", {
      ready: studyId !== undefined,
      studyId: studyId ?? null,
      datasetKey,
      reused: existingId !== undefined && existingDatasetKey === datasetKey,
      settingsPreserved: existingId !== undefined,
    });
    return studyId !== undefined;
  } catch (error) {
    patchTvDebug("chanStudy", {
      ready: false,
      error: error instanceof Error ? error.message : String(error),
    });
    if (TRADINGVIEW_DEBUG) {
      console.warn("[chan-study-render-failed]", error);
    }
    clearChanStudyOverlay();
    return false;
  }
}

export async function openChanStudySettings(widget: TradingViewWidget): Promise<void> {
  const chart = await getActiveChart(widget);
  const studyId = studyIds.get(widget);
  if (!chart || studyId === undefined) {
    return;
  }
  chart.showPropertiesDialog(studyId);
}

export function subscribeChanStudySettingsChanges(
  widget: TradingViewWidget,
  onSettingsChange: (settings: ChanOverlaySettings) => void,
): () => void {
  studySettingsChangeHandlers.set(widget, onSettingsChange);
  return () => {
    if (studySettingsChangeHandlers.get(widget) === onSettingsChange) {
      studySettingsChangeHandlers.delete(widget);
    }
  };
}

export async function readChanStudySettings(
  widget: TradingViewWidget,
  fallback: ChanOverlaySettings,
): Promise<ChanOverlaySettings | null> {
  const chart = await getActiveChart(widget);
  const studyId = studyIds.get(widget);
  if (!chart || studyId === undefined) {
    return null;
  }
  try {
    return readCurrentChanStudySettings(chart, studyId, fallback);
  } catch {
    return null;
  }
}

export async function applyChanStudySettings(
  widget: TradingViewWidget,
  settings: ChanOverlaySettings,
): Promise<void> {
  const chart = await getActiveChart(widget);
  const studyId = studyIds.get(widget);
  if (!chart || studyId === undefined) {
    return;
  }
  const study = chart.getStudyById(studyId);
  study.setInputValues(studyInputItemsFromSettings(settings));
  study.applyOverrides?.(buildChanStudyOverrides(settings));
  studyInputSignatures.set(widget, studyInputSignature(study.getInputValues()));
}

async function clearDrawings(widget: TradingViewWidget, chart?: TradingViewChart): Promise<void> {
  const activeChart = chart ?? (await getActiveChart(widget));
  if (!activeChart) {
    return;
  }
  for (const id of overlayShapeIds.get(widget) ?? []) {
    activeChart.removeEntity(id);
  }
  overlayShapeIds.set(widget, []);
}

async function drawCenter(
  chart: TradingViewChart,
  center: ChanOverlayResponse["centers"][number],
  settings: ChanOverlaySettings,
  timeframe = "5f",
): Promise<ShapeId[]> {
  const style = getChanLineStyle(
    settings.styles,
    center.level,
    "center",
    center.confirmed,
    settings.lineStyles,
  );
  const common = {
    shape: "trend_line",
    lock: true,
    disableSelection: true,
    disableSave: true,
    disableUndo: true,
    overrides: trendLineOverrides(style),
  };

  const lowLine = await chart.createMultipointShape(
    [
      { time: mapOverlayTimeToChartTime(center.start_time, timeframe), price: center.low },
      { time: mapOverlayTimeToChartTime(center.end_time, timeframe), price: center.low },
    ],
    {
      ...common,
      text: `${center.level} center low`,
    },
  );
  const highLine = await chart.createMultipointShape(
    [
      { time: mapOverlayTimeToChartTime(center.start_time, timeframe), price: center.high },
      { time: mapOverlayTimeToChartTime(center.end_time, timeframe), price: center.high },
    ],
    {
      ...common,
      text: `${center.level} center high`,
    },
  );

  return [lowLine, highLine];
}

function trendLineOverrides(style: {
  color: string;
  linewidth: number;
  linestyle: number;
}): Record<string, unknown> {
  return {
    color: style.color,
    linecolor: style.color,
    linewidth: style.linewidth,
    linestyle: style.linestyle,
    textcolor: style.color,
    extendLeft: false,
    extendRight: false,
    showAngle: false,
    showBarsRange: false,
    showDateTimeRange: false,
    showDistance: false,
    showLabel: false,
    showMiddlePoint: false,
    showPercentPriceRange: false,
    showPipsPriceRange: false,
    showPriceLabels: false,
    showPriceRange: false,
    "linetooltrendline.linecolor": style.color,
    "linetooltrendline.linewidth": style.linewidth,
    "linetooltrendline.linestyle": style.linestyle,
    "linetooltrendline.textcolor": style.color,
    "linetooltrendline.extendLeft": false,
    "linetooltrendline.extendRight": false,
    "linetooltrendline.showAngle": false,
    "linetooltrendline.showBarsRange": false,
    "linetooltrendline.showDateTimeRange": false,
    "linetooltrendline.showDistance": false,
    "linetooltrendline.showLabel": false,
    "linetooltrendline.showMiddlePoint": false,
    "linetooltrendline.showPercentPriceRange": false,
    "linetooltrendline.showPipsPriceRange": false,
    "linetooltrendline.showPriceLabels": false,
    "linetooltrendline.showPriceRange": false,
  };
}

function mapOverlayTimeToChartTime(epochSeconds: number, timeframe: string): number {
  const interval = INTERVAL_BY_TIMEFRAME[timeframe] ?? timeframe;
  const normalized = interval.toUpperCase();
  const minuteResolution = Number.parseInt(normalized, 10);
  if (Number.isFinite(minuteResolution) && minuteResolution > 0) {
    const rawMs = epochSeconds * 1000;
    const intervalMs = minuteResolution * 60 * 1000;
    return Math.floor(rawMs / intervalMs) * intervalMs / 1000;
  }
  return toTradingViewTime(epochSeconds, interval) / 1000;
}

async function ensureTradingViewScript(): Promise<boolean> {
  if (window.TradingView?.widget) {
    return true;
  }
  for (const src of SCRIPT_CANDIDATES) {
    const ok = await loadScript(src);
    if (ok && window.TradingView?.widget) {
      recordTvDebug("widget.script.loaded", { src });
      return true;
    }
    recordTvDebug("widget.script.failed", { src });
  }
  return false;
}

function whenChartReady(widget: TradingViewWidget, timeoutMs = 10000): Promise<boolean> {
  return new Promise((resolve) => {
    const timeout = window.setTimeout(() => resolve(false), timeoutMs);
    widget.onChartReady(() => {
      window.clearTimeout(timeout);
      resolve(true);
    });
  });
}

async function installThemeButton(
  widget: TradingViewWidget,
  theme: ChartTheme,
  onToggleTheme: () => void,
): Promise<void> {
  if (!widget.headerReady || !widget.createButton) {
    return;
  }
  try {
    await widget.headerReady();
    let button: HTMLElement;
    try {
      button = widget.createButton({
        align: "left",
        useTradingViewStyle: true,
      });
    } catch {
      button = widget.createButton();
    }
    button.classList.add("tv-chart-theme-header-button");
    button.addEventListener("click", onToggleTheme);
    themeButtons.set(widget, button);
    updateTradingViewThemeButton(widget, theme);
  } catch {
    // Header widgets are optional; failing here must not break chart loading.
  }
}

async function getActiveChart(widget: TradingViewWidget): Promise<TradingViewChart | null> {
  try {
    await whenChartReady(widget, 1500);
    return widget.activeChart();
  } catch {
    return null;
  }
}

function loadScript(src: string): Promise<boolean> {
  return new Promise((resolve) => {
    const existing = document.querySelector(`script[src="${src}"]`);
    if (existing) {
      resolve(true);
      return;
    }
    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.onload = () => resolve(true);
    script.onerror = () => resolve(false);
    document.head.appendChild(script);
  });
}

function setChanRenderDataset(state: Record<string, unknown>): void {
  const dataset = getChartDataset();
  if (!dataset) {
    return;
  }
  dataset.chanRenderPhase = String(state.phase ?? "");
  dataset.chanEngine = String(state.engine ?? "");
  dataset.chanChartTimeframe = String(state.chartTimeframe ?? "");
  dataset.chanStudyReady = String(state.studyReady ?? "");
  dataset.chanStrokes = String(state.strokes ?? "");
  dataset.chanSegments = String(state.segments ?? "");
  dataset.chanCenters = String(state.centers ?? "");
  dataset.chanSignals = String(state.signals ?? "");
  dataset.chanLevels = Array.isArray(state.levels)
    ? state.levels.join(",")
    : String(state.levels ?? "");
  dataset.chanStrokeDrawings = String(state.strokeDrawings ?? "");
  dataset.chanSegmentDrawings = String(state.segmentDrawings ?? "");
  dataset.chanCenterDrawings = String(state.centerDrawings ?? "");
  dataset.chanActualDrawings = String(state.actualDrawings ?? "");
  dataset.chanDataReady = String(state.dataReady ?? "");
  dataset.chanPineStrokes = String(state.pineStrokes ?? "");
  dataset.chanPineSegments = String(state.pineSegments ?? "");
  dataset.chanPineCenters = String(state.pineCenters ?? "");
  dataset.chanRequestedBarCount = String(state.requestedBarCount ?? "");
  dataset.chanLineRenderer = String(state.lineRenderer ?? "");
  dataset.chanCenterRenderer = String(state.centerRenderer ?? "");
}

function setChartDataset(values: Record<string, string>): void {
  const dataset = getChartDataset();
  if (!dataset) {
    return;
  }
  Object.assign(dataset, values);
}

function getChartDataset(): DOMStringMap | null {
  return document.querySelector<HTMLElement>(".tv-container")?.dataset ?? null;
}

function toTradingViewTheme(theme: ChartTheme): "Dark" | "Light" {
  return theme === "light" ? "Light" : "Dark";
}
