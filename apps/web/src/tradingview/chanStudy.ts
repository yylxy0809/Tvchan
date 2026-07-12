import type {
  ApiBar,
  ChanChannel,
  ChanCenter,
  ChanOverlayResponse,
  ChanSignal,
  ChanStroke,
} from "../api/client";
import {
  getChanSignalStyle,
  readLineStyle,
  type ResolvedChanSignalStyle,
} from "./chanStyles";
import {
  CHAN_STUDY_INPUTS,
  DEFAULT_CHAN_STUDY_INPUTS,
  studyInputGetterToDisplaySettings,
  studyInputGetterToOverlaySettings,
  type ChanStudyDisplaySettings,
} from "./chanStudySettings";
import {
  createDefaultChanOverlaySettings,
  type ChanLevel,
  type ChanOverlaySettings,
} from "./overlaySettings";
import { INTERVAL_BY_TIMEFRAME, toTradingViewTime } from "./time";

type PineJS = {
  Std: {
    interval?: (context: PineContext) => string;
    ticker?: (context: PineContext) => string;
    period: (context: PineContext) => string;
    time: (context: PineContext, period: string) => number;
  };
};

type PineContext = {
  symbol: {
    time: number;
    resolution?: string;
  };
};

type PineStudyContext = {
  _context: PineContext;
  _input?: (index: number) => string | number | boolean;
  _lastTime?: number | null;
  _lastResolution?: string | null;
  _pivotBreakState?: Record<string, { key: string | null; skip: number }>;
  main(context: PineContext, input: (index: number) => string | number | boolean): number[];
};

type SignalSide = "buy" | "sell";
type SignalVariant = "1" | "2" | "2s" | "3" | "other";
type ModeKey = "confirmed" | "predictive" | "merged";

type TimedLinePoint = {
  price: number;
  dir: string;
};

type TimedSignalPoint = {
  price: number;
  bspType: string;
};

type RawStrokePoint = {
  seq: number;
  level?: string;
  startTime: number;
  startPrice: number;
  endTime: number;
  endPrice: number;
  direction: string;
};

type RawSignalPoint = {
  time: number;
  price: number;
  bspType: string;
  side: SignalSide | null;
};

type RawChannelPoint = {
  time: number;
  upper: number;
  lower: number;
};

type RawCenterInterval = {
  key: string;
  startTime: number;
  endTime: number;
  high: number;
  low: number;
};

type RawLevelModeData = {
  strokes: RawStrokePoint[];
  centers: RawCenterInterval[];
  signals: RawSignalPoint[];
  channels: RawChannelPoint[];
};

type CanonicalLineLike = ChanStroke & {
  seq?: number | null;
  is_sure?: boolean;
};

type CanonicalSignalLike = ChanSignal & {
  bsp_type?: string | null;
  signal_key?: string;
  is_buy?: boolean;
  side?: string | null;
  is_sure?: boolean;
};

type PivotInterval = {
  key: string;
  startTime: number;
  endTime: number;
  high: number;
  low: number;
};

type LevelCache = {
  strokePoints: Map<number, TimedLinePoint>;
  strokeTimes: number[];
  pivots: PivotInterval[];
  pivotStarts: number[];
  channelPoints: Map<number, RawChannelPoint>;
  channelTimes: number[];
  bspBuy: Map<number, TimedSignalPoint>;
  bspBuyTimes: number[];
  bspSell: Map<number, TimedSignalPoint>;
  bspSellTimes: number[];
};

type ChanStudyState = {
  resolution: string;
  viewBarTimes: number[];
  viewBars: ApiBar[];
  rawLevels: Record<ChanLevel, Record<ModeKey, RawLevelModeData>>;
  levels: Record<ChanLevel, Record<ModeKey, LevelCache>>;
  fallbackCenters: ChanCenter[];
};

type MainLinePlot = {
  id: string;
  title: string;
  level: ChanLevel;
};

type CenterPlot = {
  id: string;
  title: string;
  level: ChanLevel;
  side: "high" | "low";
};

type SignalPlot = {
  id: string;
  title: string;
  level: ChanLevel;
  side: SignalSide;
  variant: SignalVariant;
};

type ChannelPlot = {
  id: string;
  title: string;
  level: ChanLevel;
  side: "upper" | "lower";
};

type PlotDefinition = MainLinePlot | CenterPlot | SignalPlot | ChannelPlot;

const LEVELS: ChanLevel[] = ["5f", "30f", "1d", "1w", "1m"];
const MODE_KEYS: ModeKey[] = ["confirmed", "predictive", "merged"];
const SIGNAL_VARIANTS: SignalVariant[] = ["1", "2", "2s", "3"];
const VALUES_PER_LEVEL = 14;
const STUDY_ID = "ChanOverlay@tv-basicstudies-1";
const STUDY_NAME = "Chan Overlay";
const STUDY_SHORT = "Chan";
export const CHAN_STUDY_DESCRIPTION = STUDY_SHORT;
const DAY_MS = 24 * 60 * 60 * 1000;
const TRANSPARENT = "rgba(0,0,0,0)";
const NAN_VALUES = Array.from({ length: LEVELS.length * VALUES_PER_LEVEL }, () => Number.NaN);

const MAIN_LINE_PLOTS: MainLinePlot[] = [
  { id: "bi_val", title: "5f\u7b14", level: "5f" },
  { id: "seg_val", title: "30f\u7b14", level: "30f" },
  { id: "ss_val", title: "\u65e5\u7ebf\u7b14", level: "1d" },
  { id: "week_val", title: "周线笔", level: "1w" },
  { id: "month_val", title: "月线笔", level: "1m" },
];

const CENTER_PLOTS: CenterPlot[] = [
  { id: "zs5_hi", title: "5f\u4e2d\u67a2\u4e0a\u8f68", level: "5f", side: "high" },
  { id: "zs5_lo", title: "5f\u4e2d\u67a2\u4e0b\u8f68", level: "5f", side: "low" },
  { id: "zs30_hi", title: "30f\u4e2d\u67a2\u4e0a\u8f68", level: "30f", side: "high" },
  { id: "zs30_lo", title: "30f\u4e2d\u67a2\u4e0b\u8f68", level: "30f", side: "low" },
  { id: "zsd_hi", title: "\u65e5\u7ebf\u4e2d\u67a2\u4e0a\u8f68", level: "1d", side: "high" },
  { id: "zsd_lo", title: "\u65e5\u7ebf\u4e2d\u67a2\u4e0b\u8f68", level: "1d", side: "low" },
  { id: "zsw_hi", title: "周线中枢上轨", level: "1w", side: "high" },
  { id: "zsw_lo", title: "周线中枢下轨", level: "1w", side: "low" },
  { id: "zsm_hi", title: "月线中枢上轨", level: "1m", side: "high" },
  { id: "zsm_lo", title: "月线中枢下轨", level: "1m", side: "low" },
];

const SIGNAL_PLOTS: SignalPlot[] = LEVELS.flatMap((level) => [
  signalPlot(level, "buy", "1"),
  signalPlot(level, "buy", "2"),
  signalPlot(level, "buy", "2s"),
  signalPlot(level, "buy", "3"),
  signalPlot(level, "sell", "1"),
  signalPlot(level, "sell", "2"),
  signalPlot(level, "sell", "2s"),
  signalPlot(level, "sell", "3"),
]);

const CHANNEL_PLOTS: ChannelPlot[] = [
  { id: "ch5_hi", title: "5f plot_channel \u4e0a\u8f68", level: "5f", side: "upper" },
  { id: "ch5_lo", title: "5f plot_channel \u4e0b\u8f68", level: "5f", side: "lower" },
  { id: "ch30_hi", title: "30f plot_channel \u4e0a\u8f68", level: "30f", side: "upper" },
  { id: "ch30_lo", title: "30f plot_channel \u4e0b\u8f68", level: "30f", side: "lower" },
  { id: "chd_hi", title: "\u65e5\u7ebf plot_channel \u4e0a\u8f68", level: "1d", side: "upper" },
  { id: "chd_lo", title: "\u65e5\u7ebf plot_channel \u4e0b\u8f68", level: "1d", side: "lower" },
  { id: "chw_hi", title: "周线 plot_channel 上轨", level: "1w", side: "upper" },
  { id: "chw_lo", title: "周线 plot_channel 下轨", level: "1w", side: "lower" },
  { id: "chm_hi", title: "月线 plot_channel 上轨", level: "1m", side: "upper" },
  { id: "chm_lo", title: "月线 plot_channel 下轨", level: "1m", side: "lower" },
];

const PLOTS: PlotDefinition[] = [...MAIN_LINE_PLOTS, ...CENTER_PLOTS, ...CHANNEL_PLOTS, ...SIGNAL_PLOTS];
const PLOT_TITLE_BY_ID = new Map(PLOTS.map((plot) => [plot.id, plot.title]));
const PALETTE_ID_BY_LEVEL: Record<ChanLevel, string> = {
  "5f": "pal_bi",
  "30f": "pal_seg",
  "1d": "pal_ss",
  "1w": "pal_w",
  "1m": "pal_m",
};
const FILL_ID_BY_LEVEL: Record<ChanLevel, string> = {
  "5f": "fill5",
  "30f": "fill30",
  "1d": "filld",
  "1w": "fillw",
  "1m": "fillm",
};

const EMPTY_LEVEL_CACHE: LevelCache = {
  strokePoints: new Map<number, TimedLinePoint>(),
  strokeTimes: [],
  pivots: [],
  pivotStarts: [],
  channelPoints: new Map<number, RawChannelPoint>(),
  channelTimes: [],
  bspBuy: new Map<number, TimedSignalPoint>(),
  bspBuyTimes: [],
  bspSell: new Map<number, TimedSignalPoint>(),
  bspSellTimes: [],
};

const EMPTY_STATE: ChanStudyState = {
  resolution: "5",
  viewBarTimes: [],
  viewBars: [],
  rawLevels: {
    "5f": createEmptyRawLevelState(),
    "30f": createEmptyRawLevelState(),
    "1d": createEmptyRawLevelState(),
    "1w": createEmptyRawLevelState(),
    "1m": createEmptyRawLevelState(),
  },
  levels: {
    "5f": createEmptyLevelState(),
    "30f": createEmptyLevelState(),
    "1d": createEmptyLevelState(),
    "1w": createEmptyLevelState(),
    "1m": createEmptyLevelState(),
  },
  fallbackCenters: [],
};

let activeStudyState: ChanStudyState = EMPTY_STATE;

export function setChanStudyOverlay(overlay: ChanOverlayResponse, chartBars: ApiBar[] = []): void {
  activeStudyState = updateStudyState(activeStudyState, overlay, chartBars);
}

export function clearChanStudyOverlay(): void {
  activeStudyState = EMPTY_STATE;
}

export function getChanStudyFallbackCenters(): ChanCenter[] {
  return activeStudyState.fallbackCenters;
}

export function buildChanStudyOverrides(
  settings: ChanOverlaySettings = createDefaultChanOverlaySettings(),
): Record<string, unknown> {
  const overrides: Record<string, unknown> = {};
  const activeMode = resolveActiveModeKey(settings);
  const lineStyle = readLineStyle(
    activeMode === "predictive"
      ? settings.lineStyles.predictive
      : settings.lineStyles.confirmed,
    0,
  );

  for (const plot of MAIN_LINE_PLOTS) {
    const style = settings.styles[plot.level].stroke;
    const path = plotOverrideName(plot.id);
    overrides[`${path}.color`] = style.color;
    overrides[`${path}.linestyle`] = lineStyle;
    overrides[`${path}.linewidth`] = style.linewidth;
    overrides[`${path}.transparency`] = 0;
  }

  for (const plot of CENTER_PLOTS) {
    const style = settings.styles[plot.level].center;
    const path = plotOverrideName(plot.id);
    overrides[`${path}.color`] = style.color;
    overrides[`${path}.linestyle`] = lineStyle;
    overrides[`${path}.linewidth`] = style.linewidth;
    overrides[`${path}.transparency`] = style.transparency;
    overrides[`${path}.display`] = 0;
  }

  for (const level of LEVELS) {
    const fillId = FILL_ID_BY_LEVEL[level];
    const style = settings.styles[level].center;
    const visible = settings.levels[level] && settings.parts.centers;
    overrides[`filledAreasStyle.${fillId}.color`] = style.color;
    overrides[`filledAreasStyle.${fillId}.transparency`] = style.transparency;
    overrides[`filledAreasStyle.${fillId}.visible`] = visible;
  }

  for (const plot of CHANNEL_PLOTS) {
    const style = settings.styles[plot.level].channel;
    const path = plotOverrideName(plot.id);
    overrides[`${path}.color`] = style.color;
    overrides[`${path}.linestyle`] = 0;
    overrides[`${path}.linewidth`] = style.linewidth;
    overrides[`${path}.transparency`] = 0;
  }

  for (const plot of SIGNAL_PLOTS) {
    const signalStyle = getChanSignalStyle(
      settings.styles,
      plot.level,
      plot.side === "buy" ? "B" : "S",
    );
    const path = plotOverrideName(plot.id);
    overrides[`${path}.color`] = signalStyle.color;
    overrides[`${path}.textColor`] = signalStyle.textColor;
  }

  return overrides;
}

export function createChanCustomIndicators(PineJS: PineJS) {
  return Promise.resolve([
    {
      name: STUDY_SHORT,
      metainfo: {
        _metainfoVersion: 52,
        id: STUDY_ID,
        scriptIdPart: "",
        name: STUDY_SHORT,
        description: STUDY_SHORT,
        shortDescription: STUDY_SHORT,
        isTVScript: false,
        isTVScriptStub: false,
        is_price_study: true,
        is_hidden_study: false,
        isCustomIndicator: true,
        linkedToSeries: true,
        format: { type: "inherit" },
        inputs: CHAN_STUDY_INPUTS,
        plots: [
          { id: "bi_val", type: "line" },
          { id: "bi_col", type: "colorer", target: "bi_val", palette: PALETTE_ID_BY_LEVEL["5f"] },
          { id: "zs5_hi", type: "line" },
          { id: "zs5_lo", type: "line" },
          { id: "ch5_hi", type: "line" },
          { id: "ch5_lo", type: "line" },
          { id: "bsp5_1b", type: "shapes" },
          { id: "bsp5_2b", type: "shapes" },
          { id: "bsp5_2sb", type: "shapes" },
          { id: "bsp5_3b", type: "shapes" },
          { id: "bsp5_1s", type: "shapes" },
          { id: "bsp5_2s", type: "shapes" },
          { id: "bsp5_2ss", type: "shapes" },
          { id: "bsp5_3s", type: "shapes" },
          { id: "seg_val", type: "line" },
          { id: "seg_col", type: "colorer", target: "seg_val", palette: PALETTE_ID_BY_LEVEL["30f"] },
          { id: "zs30_hi", type: "line" },
          { id: "zs30_lo", type: "line" },
          { id: "ch30_hi", type: "line" },
          { id: "ch30_lo", type: "line" },
          { id: "bsp30_1b", type: "shapes" },
          { id: "bsp30_2b", type: "shapes" },
          { id: "bsp30_2sb", type: "shapes" },
          { id: "bsp30_3b", type: "shapes" },
          { id: "bsp30_1s", type: "shapes" },
          { id: "bsp30_2s", type: "shapes" },
          { id: "bsp30_2ss", type: "shapes" },
          { id: "bsp30_3s", type: "shapes" },
          { id: "ss_val", type: "line" },
          { id: "ss_col", type: "colorer", target: "ss_val", palette: PALETTE_ID_BY_LEVEL["1d"] },
          { id: "zsd_hi", type: "line" },
          { id: "zsd_lo", type: "line" },
          { id: "chd_hi", type: "line" },
          { id: "chd_lo", type: "line" },
          { id: "bspd_1b", type: "shapes" },
          { id: "bspd_2b", type: "shapes" },
          { id: "bspd_2sb", type: "shapes" },
          { id: "bspd_3b", type: "shapes" },
          { id: "bspd_1s", type: "shapes" },
          { id: "bspd_2s", type: "shapes" },
          { id: "bspd_2ss", type: "shapes" },
          { id: "bspd_3s", type: "shapes" },
          { id: "week_val", type: "line" },
          { id: "week_col", type: "colorer", target: "week_val", palette: PALETTE_ID_BY_LEVEL["1w"] },
          { id: "zsw_hi", type: "line" },
          { id: "zsw_lo", type: "line" },
          { id: "chw_hi", type: "line" },
          { id: "chw_lo", type: "line" },
          { id: "bspw_1b", type: "shapes" },
          { id: "bspw_2b", type: "shapes" },
          { id: "bspw_2sb", type: "shapes" },
          { id: "bspw_3b", type: "shapes" },
          { id: "bspw_1s", type: "shapes" },
          { id: "bspw_2s", type: "shapes" },
          { id: "bspw_2ss", type: "shapes" },
          { id: "bspw_3s", type: "shapes" },
          { id: "month_val", type: "line" },
          { id: "month_col", type: "colorer", target: "month_val", palette: PALETTE_ID_BY_LEVEL["1m"] },
          { id: "zsm_hi", type: "line" },
          { id: "zsm_lo", type: "line" },
          { id: "chm_hi", type: "line" },
          { id: "chm_lo", type: "line" },
          { id: "bspm_1b", type: "shapes" },
          { id: "bspm_2b", type: "shapes" },
          { id: "bspm_2sb", type: "shapes" },
          { id: "bspm_3b", type: "shapes" },
          { id: "bspm_1s", type: "shapes" },
          { id: "bspm_2s", type: "shapes" },
          { id: "bspm_2ss", type: "shapes" },
          { id: "bspm_3s", type: "shapes" },
        ],
        palettes: {
          pal_bi: {
            colors: [{ name: "5f-up" }, { name: "5f-down" }],
            valToIndex: { 0: 0, 1: 1 },
          },
          pal_seg: {
            colors: [{ name: "30f-up" }, { name: "30f-down" }],
            valToIndex: { 0: 0, 1: 1 },
          },
          pal_ss: {
            colors: [{ name: "1d-up" }, { name: "1d-down" }],
            valToIndex: { 0: 0, 1: 1 },
          },
          pal_w: {
            colors: [{ name: "1w-up" }, { name: "1w-down" }],
            valToIndex: { 0: 0, 1: 1 },
          },
          pal_m: {
            colors: [{ name: "1m-up" }, { name: "1m-down" }],
            valToIndex: { 0: 0, 1: 1 },
          },
        },
        filledAreas: [
          { id: "fill5", objAId: "zs5_hi", objBId: "zs5_lo", type: "plot_plot", title: "5f涓灑" },
          { id: "fill30", objAId: "zs30_hi", objBId: "zs30_lo", type: "plot_plot", title: "30f涓灑" },
          { id: "filld", objAId: "zsd_hi", objBId: "zsd_lo", type: "plot_plot", title: "鏃ョ嚎涓灑" },
          { id: "fillw", objAId: "zsw_hi", objBId: "zsw_lo", type: "plot_plot", title: "周线中枢" },
          { id: "fillm", objAId: "zsm_hi", objBId: "zsm_lo", type: "plot_plot", title: "月线中枢" },
        ],
        defaults: {
          inputs: DEFAULT_CHAN_STUDY_INPUTS,
          styles: {
            bi_val: buildLineDefaults("5f"),
            zs5_hi: buildCenterDefaults("5f"),
            zs5_lo: buildCenterDefaults("5f"),
            ch5_hi: buildChannelDefaults("5f"),
            ch5_lo: buildChannelDefaults("5f"),
            bsp5_1b: buildSignalDefaults("5f", "buy", "1"),
            bsp5_2b: buildSignalDefaults("5f", "buy", "2"),
            bsp5_2sb: buildSignalDefaults("5f", "buy", "2s"),
            bsp5_3b: buildSignalDefaults("5f", "buy", "3"),
            bsp5_1s: buildSignalDefaults("5f", "sell", "1"),
            bsp5_2s: buildSignalDefaults("5f", "sell", "2"),
            bsp5_2ss: buildSignalDefaults("5f", "sell", "2s"),
            bsp5_3s: buildSignalDefaults("5f", "sell", "3"),
            seg_val: buildLineDefaults("30f"),
            zs30_hi: buildCenterDefaults("30f"),
            zs30_lo: buildCenterDefaults("30f"),
            ch30_hi: buildChannelDefaults("30f"),
            ch30_lo: buildChannelDefaults("30f"),
            bsp30_1b: buildSignalDefaults("30f", "buy", "1"),
            bsp30_2b: buildSignalDefaults("30f", "buy", "2"),
            bsp30_2sb: buildSignalDefaults("30f", "buy", "2s"),
            bsp30_3b: buildSignalDefaults("30f", "buy", "3"),
            bsp30_1s: buildSignalDefaults("30f", "sell", "1"),
            bsp30_2s: buildSignalDefaults("30f", "sell", "2"),
            bsp30_2ss: buildSignalDefaults("30f", "sell", "2s"),
            bsp30_3s: buildSignalDefaults("30f", "sell", "3"),
            ss_val: buildLineDefaults("1d"),
            zsd_hi: buildCenterDefaults("1d"),
            zsd_lo: buildCenterDefaults("1d"),
            chd_hi: buildChannelDefaults("1d"),
            chd_lo: buildChannelDefaults("1d"),
            bspd_1b: buildSignalDefaults("1d", "buy", "1"),
            bspd_2b: buildSignalDefaults("1d", "buy", "2"),
            bspd_2sb: buildSignalDefaults("1d", "buy", "2s"),
            bspd_3b: buildSignalDefaults("1d", "buy", "3"),
            bspd_1s: buildSignalDefaults("1d", "sell", "1"),
            bspd_2s: buildSignalDefaults("1d", "sell", "2"),
            bspd_2ss: buildSignalDefaults("1d", "sell", "2s"),
            bspd_3s: buildSignalDefaults("1d", "sell", "3"),
            week_val: buildLineDefaults("1w"),
            zsw_hi: buildCenterDefaults("1w"),
            zsw_lo: buildCenterDefaults("1w"),
            chw_hi: buildChannelDefaults("1w"),
            chw_lo: buildChannelDefaults("1w"),
            bspw_1b: buildSignalDefaults("1w", "buy", "1"),
            bspw_2b: buildSignalDefaults("1w", "buy", "2"),
            bspw_2sb: buildSignalDefaults("1w", "buy", "2s"),
            bspw_3b: buildSignalDefaults("1w", "buy", "3"),
            bspw_1s: buildSignalDefaults("1w", "sell", "1"),
            bspw_2s: buildSignalDefaults("1w", "sell", "2"),
            bspw_2ss: buildSignalDefaults("1w", "sell", "2s"),
            bspw_3s: buildSignalDefaults("1w", "sell", "3"),
            month_val: buildLineDefaults("1m"),
            zsm_hi: buildCenterDefaults("1m"),
            zsm_lo: buildCenterDefaults("1m"),
            chm_hi: buildChannelDefaults("1m"),
            chm_lo: buildChannelDefaults("1m"),
            bspm_1b: buildSignalDefaults("1m", "buy", "1"),
            bspm_2b: buildSignalDefaults("1m", "buy", "2"),
            bspm_2sb: buildSignalDefaults("1m", "buy", "2s"),
            bspm_3b: buildSignalDefaults("1m", "buy", "3"),
            bspm_1s: buildSignalDefaults("1m", "sell", "1"),
            bspm_2s: buildSignalDefaults("1m", "sell", "2"),
            bspm_2ss: buildSignalDefaults("1m", "sell", "2s"),
            bspm_3s: buildSignalDefaults("1m", "sell", "3"),
          },
          filledAreasStyle: {
            fill5: buildFillDefaults("5f"),
            fill30: buildFillDefaults("30f"),
            filld: buildFillDefaults("1d"),
            fillw: buildFillDefaults("1w"),
            fillm: buildFillDefaults("1m"),
          },
          palettes: {
            pal_bi: buildPaletteDefaults("5f"),
            pal_seg: buildPaletteDefaults("30f"),
            pal_ss: buildPaletteDefaults("1d"),
            pal_w: buildPaletteDefaults("1w"),
            pal_m: buildPaletteDefaults("1m"),
          },
        },
        styles: {
          bi_val: { title: plotOverrideName("bi_val"), joinPoints: true, histogramBase: 0 },
          zs5_hi: { title: plotOverrideName("zs5_hi"), joinPoints: false, histogramBase: 0 },
          zs5_lo: { title: plotOverrideName("zs5_lo"), joinPoints: false, histogramBase: 0 },
          ch5_hi: { title: plotOverrideName("ch5_hi"), joinPoints: true, histogramBase: 0 },
          ch5_lo: { title: plotOverrideName("ch5_lo"), joinPoints: true, histogramBase: 0 },
          bsp5_1b: buildSignalMeta("bsp5_1b"),
          bsp5_2b: buildSignalMeta("bsp5_2b"),
          bsp5_2sb: buildSignalMeta("bsp5_2sb"),
          bsp5_3b: buildSignalMeta("bsp5_3b"),
          bsp5_1s: buildSignalMeta("bsp5_1s"),
          bsp5_2s: buildSignalMeta("bsp5_2s"),
          bsp5_2ss: buildSignalMeta("bsp5_2ss"),
          bsp5_3s: buildSignalMeta("bsp5_3s"),
          seg_val: { title: plotOverrideName("seg_val"), joinPoints: true, histogramBase: 0 },
          zs30_hi: { title: plotOverrideName("zs30_hi"), joinPoints: false, histogramBase: 0 },
          zs30_lo: { title: plotOverrideName("zs30_lo"), joinPoints: false, histogramBase: 0 },
          ch30_hi: { title: plotOverrideName("ch30_hi"), joinPoints: true, histogramBase: 0 },
          ch30_lo: { title: plotOverrideName("ch30_lo"), joinPoints: true, histogramBase: 0 },
          bsp30_1b: buildSignalMeta("bsp30_1b"),
          bsp30_2b: buildSignalMeta("bsp30_2b"),
          bsp30_2sb: buildSignalMeta("bsp30_2sb"),
          bsp30_3b: buildSignalMeta("bsp30_3b"),
          bsp30_1s: buildSignalMeta("bsp30_1s"),
          bsp30_2s: buildSignalMeta("bsp30_2s"),
          bsp30_2ss: buildSignalMeta("bsp30_2ss"),
          bsp30_3s: buildSignalMeta("bsp30_3s"),
          ss_val: { title: plotOverrideName("ss_val"), joinPoints: true, histogramBase: 0 },
          zsd_hi: { title: plotOverrideName("zsd_hi"), joinPoints: false, histogramBase: 0 },
          zsd_lo: { title: plotOverrideName("zsd_lo"), joinPoints: false, histogramBase: 0 },
          chd_hi: { title: plotOverrideName("chd_hi"), joinPoints: true, histogramBase: 0 },
          chd_lo: { title: plotOverrideName("chd_lo"), joinPoints: true, histogramBase: 0 },
          bspd_1b: buildSignalMeta("bspd_1b"),
          bspd_2b: buildSignalMeta("bspd_2b"),
          bspd_2sb: buildSignalMeta("bspd_2sb"),
          bspd_3b: buildSignalMeta("bspd_3b"),
          bspd_1s: buildSignalMeta("bspd_1s"),
          bspd_2s: buildSignalMeta("bspd_2s"),
          bspd_2ss: buildSignalMeta("bspd_2ss"),
          bspd_3s: buildSignalMeta("bspd_3s"),
          week_val: { title: plotOverrideName("week_val"), joinPoints: true, histogramBase: 0 },
          zsw_hi: { title: plotOverrideName("zsw_hi"), joinPoints: false, histogramBase: 0 },
          zsw_lo: { title: plotOverrideName("zsw_lo"), joinPoints: false, histogramBase: 0 },
          chw_hi: { title: plotOverrideName("chw_hi"), joinPoints: true, histogramBase: 0 },
          chw_lo: { title: plotOverrideName("chw_lo"), joinPoints: true, histogramBase: 0 },
          bspw_1b: buildSignalMeta("bspw_1b"),
          bspw_2b: buildSignalMeta("bspw_2b"),
          bspw_2sb: buildSignalMeta("bspw_2sb"),
          bspw_3b: buildSignalMeta("bspw_3b"),
          bspw_1s: buildSignalMeta("bspw_1s"),
          bspw_2s: buildSignalMeta("bspw_2s"),
          bspw_2ss: buildSignalMeta("bspw_2ss"),
          bspw_3s: buildSignalMeta("bspw_3s"),
          month_val: { title: plotOverrideName("month_val"), joinPoints: true, histogramBase: 0 },
          zsm_hi: { title: plotOverrideName("zsm_hi"), joinPoints: false, histogramBase: 0 },
          zsm_lo: { title: plotOverrideName("zsm_lo"), joinPoints: false, histogramBase: 0 },
          chm_hi: { title: plotOverrideName("chm_hi"), joinPoints: true, histogramBase: 0 },
          chm_lo: { title: plotOverrideName("chm_lo"), joinPoints: true, histogramBase: 0 },
          bspm_1b: buildSignalMeta("bspm_1b"),
          bspm_2b: buildSignalMeta("bspm_2b"),
          bspm_2sb: buildSignalMeta("bspm_2sb"),
          bspm_3b: buildSignalMeta("bspm_3b"),
          bspm_1s: buildSignalMeta("bspm_1s"),
          bspm_2s: buildSignalMeta("bspm_2s"),
          bspm_2ss: buildSignalMeta("bspm_2ss"),
          bspm_3s: buildSignalMeta("bspm_3s"),
        },
      },
      constructor: function (this: PineStudyContext) {
        this.main = function (
          context: PineContext,
          input: (index: number) => string | number | boolean,
        ) {
          this._context = context;
          this._input = input;
          this._pivotBreakState = this._pivotBreakState ?? {};

          const settings = studyInputGetterToOverlaySettings(input);
          const displaySettings = studyInputGetterToDisplaySettings(input);
          const modeKey = resolveActiveModeKey(settings);
          const resolution = getResolutionStr(context, PineJS);
          const time = normalizePineBarTime(resolveCurrentBarTime(context, PineJS));

          if (!Number.isFinite(time)) {
            return [...NAN_VALUES];
          }

          if (this._lastResolution !== resolution) {
            this._lastResolution = resolution;
            this._lastTime = null;
            this._pivotBreakState = {};
          }

          const barMs = resolutionToSeconds(resolution) * 1000;
          let previousTime: number = Number.isFinite(this._lastTime ?? Number.NaN)
            ? (this._lastTime as number)
            : Number.NaN;
          if (!Number.isFinite(previousTime) || previousTime >= time) {
            previousTime = time - barMs;
          }
          this._lastTime = time;

          const rangeStart = previousTime;
          const rangeEnd = time;
          const values = [...NAN_VALUES];

          applyMainLevel(
            values,
            0,
            "5f",
            settings,
            displaySettings,
            modeKey,
            rangeStart,
            rangeEnd,
            this,
            resolution,
          );
          applyMainLevel(
            values,
            14,
            "30f",
            settings,
            displaySettings,
            modeKey,
            rangeStart,
            rangeEnd,
            this,
            resolution,
          );
          applyMainLevel(
            values,
            28,
            "1d",
            settings,
            displaySettings,
            modeKey,
            rangeStart,
            rangeEnd,
            this,
            resolution,
          );
          applyMainLevel(
            values,
            42,
            "1w",
            settings,
            displaySettings,
            modeKey,
            rangeStart,
            rangeEnd,
            this,
            resolution,
          );
          applyMainLevel(
            values,
            56,
            "1m",
            settings,
            displaySettings,
            modeKey,
            rangeStart,
            rangeEnd,
            this,
            resolution,
          );

          return values;
        };
      },
    },
  ]);
}

function applyMainLevel(
  values: number[],
  baseIndex: number,
  level: ChanLevel,
  settings: ChanOverlaySettings,
  displaySettings: ChanStudyDisplaySettings,
  modeKey: ModeKey,
  rangeStart: number,
  rangeEnd: number,
  context: PineStudyContext,
  resolution: string,
): void {
  if (!settings.levels[level]) {
    return;
  }

  ensureStudyStateResolution(resolution);
  const levelCache = activeStudyState.levels[level][modeKey];
  const showMainLine = settings.parts.strokes || settings.parts.segments;
  if (showMainLine) {
    const point = lastPointInRange(levelCache.strokeTimes, levelCache.strokePoints, rangeStart, rangeEnd);
    if (point) {
      values[baseIndex] = point.price;
      values[baseIndex + 1] = point.dir === "up" ? 0 : 1;
    }
  }

  if (settings.parts.centers) {
    const pivot = applyPivotBreak(
      level,
      findPivotOverlap(levelCache.pivots, levelCache.pivotStarts, rangeStart, rangeEnd),
      context,
    );
    if (pivot) {
      values[baseIndex + 2] = pivot.high;
      values[baseIndex + 3] = pivot.low;
    }
  } else {
    clearPivotBreak(level, context);
  }

  if (settings.parts.channels) {
    const channel = lastPointInRange(
      levelCache.channelTimes,
      levelCache.channelPoints,
      rangeStart,
      rangeEnd,
    );
    if (channel) {
      values[baseIndex + 4] = channel.upper;
      values[baseIndex + 5] = channel.lower;
    }
  }

  if (!settings.parts.signals || !shouldShowSignalLevel(level, resolution)) {
    return;
  }

  const buyPoint = lastPointInRange(
    levelCache.bspBuyTimes,
    levelCache.bspBuy,
    rangeStart,
    rangeEnd,
  );
  if (buyPoint) {
    const variant = parseSignalVariant(buyPoint.bspType);
    if (variant && shouldShowSignalVariant("buy", variant, displaySettings)) {
      values[baseIndex + 6 + signalVariantOffset("buy", variant)] = 1;
    }
  }

  const sellPoint = lastPointInRange(
    levelCache.bspSellTimes,
    levelCache.bspSell,
    rangeStart,
    rangeEnd,
  );
  if (sellPoint) {
    const variant = parseSignalVariant(sellPoint.bspType);
    if (variant && shouldShowSignalVariant("sell", variant, displaySettings)) {
      values[baseIndex + 6 + signalVariantOffset("sell", variant)] = 1;
    }
  }
}

function buildStudyState(
  overlay: ChanOverlayResponse,
  chartBars: ApiBar[] = [],
): ChanStudyState {
  const resolution = INTERVAL_BY_TIMEFRAME[overlay.chart_timeframe] ?? "5";
  const rawLevels = buildRawLevelState(overlay);
  const viewBarTimes = buildViewBarTimes(chartBars, resolution);

  return {
    resolution,
    viewBarTimes,
    viewBars: chartBars,
    rawLevels,
    levels: rebuildLevelCaches(rawLevels, resolution, viewBarTimes, chartBars),
    fallbackCenters: [],
  };
}

function updateStudyState(
  previous: ChanStudyState,
  overlay: ChanOverlayResponse,
  chartBars: ApiBar[],
): ChanStudyState {
  const resolution = INTERVAL_BY_TIMEFRAME[overlay.chart_timeframe] ?? "5";
  const viewBarTimes = buildViewBarTimes(chartBars, resolution);
  if (
    previous === EMPTY_STATE
    || previous.resolution !== resolution
    || !chartBarsEqual(previous.viewBars, chartBars)
  ) {
    return buildStudyState(overlay, chartBars);
  }

  const incomingRaw = buildRawLevelState(overlay);
  const rawLevels = { ...previous.rawLevels };
  const levels = { ...previous.levels };
  for (const level of LEVELS) {
    let levelChanged = false;
    const nextRawLevel = { ...previous.rawLevels[level] };
    const nextLevel = { ...previous.levels[level] };
    for (const mode of MODE_KEYS) {
      if (rawLevelModeEqual(previous.rawLevels[level][mode], incomingRaw[level][mode])) {
        continue;
      }
      levelChanged = true;
      nextRawLevel[mode] = incomingRaw[level][mode];
      nextLevel[mode] = buildLevelCacheFromRaw(
        incomingRaw[level][mode],
        resolution,
        viewBarTimes,
        chartBars,
      );
    }
    if (levelChanged) {
      rawLevels[level] = nextRawLevel;
      levels[level] = nextLevel;
    }
  }
  return { ...previous, resolution, viewBarTimes, viewBars: chartBars, rawLevels, levels };
}

function chartBarsEqual(left: ApiBar[], right: ApiBar[]): boolean {
  if (left === right) return true;
  if (left.length !== right.length) return false;
  return left.every((bar, index) => {
    const other = right[index];
    return other !== undefined
      && bar.time === other.time
      && bar.high === other.high
      && bar.low === other.low
      && bar.revision === other.revision;
  });
}

function rawLevelModeEqual(left: RawLevelModeData, right: RawLevelModeData): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function ensureStudyStateResolution(resolution: string): void {
  if (activeStudyState.resolution === resolution) {
    return;
  }
  const nextViewBarTimes = activeStudyState.viewBarTimes.length > 0
    ? activeStudyState.viewBarTimes
    : [];
  activeStudyState = {
    ...activeStudyState,
    resolution,
    viewBarTimes: nextViewBarTimes,
    levels: rebuildLevelCaches(
      activeStudyState.rawLevels,
      resolution,
      nextViewBarTimes,
      activeStudyState.viewBars,
    ),
  };
}

function buildRawLevelState(
  overlay: ChanOverlayResponse,
): ChanStudyState["rawLevels"] {
  const rawLevels: ChanStudyState["rawLevels"] = {
    "5f": createEmptyRawLevelState(),
    "30f": createEmptyRawLevelState(),
    "1d": createEmptyRawLevelState(),
    "1w": createEmptyRawLevelState(),
    "1m": createEmptyRawLevelState(),
  };

  for (const level of LEVELS) {
    rawLevels[level] = {
      confirmed: buildRawLevelModeData(overlay, level, "confirmed"),
      predictive: buildRawLevelModeData(overlay, level, "predictive"),
      merged: buildRawLevelModeData(overlay, level, "merged"),
    };
  }

  return rawLevels;
}

function buildRawLevelModeData(
  overlay: ChanOverlayResponse,
  level: ChanLevel,
  modeKey: ModeKey,
): RawLevelModeData {
  return {
    strokes: overlay.strokes
      .filter((item) => normalizeLevel(item.level) === level)
      .filter((item) => modeMatches(item.mode, resolveConfirmed(item), modeKey))
      .map(lineToRawStroke)
      .filter((item): item is RawStrokePoint => item !== null)
      .map((item) => ({ ...item, level }))
      .sort(compareRawStrokes),
    centers: overlay.centers
      .filter((item) => normalizeLevel(item.level) === level)
      .filter((item) => modeMatches(item.mode, item.confirmed, modeKey))
      .map(centerToRawCenter)
      .filter((item): item is RawCenterInterval => item !== null),
    signals: overlay.signals
      .filter((item) => normalizeLevel(item.level) === level)
      .filter((item) => modeMatches(item.mode, resolveConfirmed(item), modeKey))
      .map(signalToRawSignal)
      .filter((item): item is RawSignalPoint => item !== null),
    channels: overlay.channels
      .filter((item) => normalizeLevel(item.level) === level)
      .filter((item) => modeMatches(item.mode, resolveConfirmed(item), modeKey))
      .map(channelToRawChannel)
      .filter((item): item is RawChannelPoint => item !== null),
  };
}

function lineToRawStroke(item: CanonicalLineLike): RawStrokePoint | null {
  const startTime = numberFromUnknown(
    item.start?.base_ts ?? item.begin_base_ts ?? item.start?.time,
  );
  const endTime = numberFromUnknown(
    item.end?.base_ts ?? item.end_base_ts ?? item.end?.time,
  );
  const startPrice = numberFromUnknown(item.start?.price);
  const endPrice = numberFromUnknown(item.end?.price);
  if (
    startTime === null
    || endTime === null
    || startPrice === null
    || endPrice === null
  ) {
    return null;
  }
  return {
    seq: numberFromUnknown(item.seq) ?? 0,
    startTime: startTime * 1000,
    startPrice,
    endTime: endTime * 1000,
    endPrice,
    direction: String(item.direction ?? ""),
  };
}

function compareRawStrokes(left: RawStrokePoint, right: RawStrokePoint): number {
  if (left.seq !== right.seq) {
    return left.seq - right.seq;
  }
  if (left.startTime !== right.startTime) {
    return left.startTime - right.startTime;
  }
  return left.endTime - right.endTime;
}

function centerToRawCenter(item: ChanCenter): RawCenterInterval | null {
  const startTime = numberFromUnknown(item.begin_base_ts ?? item.start_time);
  const endTime = numberFromUnknown(item.end_base_ts ?? item.end_time);
  if (startTime === null || endTime === null) {
    return null;
  }
  return {
    key: `${item.id}:${startTime}:${endTime}`,
    startTime: startTime * 1000,
    endTime: endTime * 1000,
    high: item.high,
    low: item.low,
  };
}

function signalToRawSignal(item: CanonicalSignalLike): RawSignalPoint | null {
  const time = numberFromUnknown(item.base_ts ?? item.time);
  const price = numberFromUnknown(item.price);
  if (time === null || price === null) {
    return null;
  }
  const bspType = String(item.signal_key ?? item.bsp_type ?? item.signal_type ?? "");
  return {
    time: time * 1000,
    price,
    bspType,
    side: resolveSignalSideFromItem(item),
  };
}

function channelToRawChannel(item: ChanChannel): RawChannelPoint | null {
  const time = numberFromUnknown(item.base_ts ?? item.time);
  const upper = numberFromUnknown(item.upper);
  const lower = numberFromUnknown(item.lower);
  if (time === null || upper === null || lower === null) {
    return null;
  }
  return {
    time: time * 1000,
    upper,
    lower,
  };
}

function resolveConfirmed(item: { confirmed?: boolean; is_sure?: boolean }): boolean {
  if (typeof item.confirmed === "boolean") {
    return item.confirmed;
  }
  if (typeof item.is_sure === "boolean") {
    return item.is_sure;
  }
  return true;
}

function numberFromUnknown(value: unknown): number | null {
  const result = Number(value);
  return Number.isFinite(result) ? result : null;
}

function rebuildLevelCaches(
  rawLevels: ChanStudyState["rawLevels"],
  resolution: string,
  viewBarTimes: number[],
  viewBars: ApiBar[],
): ChanStudyState["levels"] {
  const levels: ChanStudyState["levels"] = {
    "5f": createEmptyLevelState(),
    "30f": createEmptyLevelState(),
    "1d": createEmptyLevelState(),
    "1w": createEmptyLevelState(),
    "1m": createEmptyLevelState(),
  };

  for (const level of LEVELS) {
    levels[level] = {
      confirmed: buildLevelCacheFromRaw(rawLevels[level].confirmed, resolution, viewBarTimes, viewBars),
      predictive: buildLevelCacheFromRaw(rawLevels[level].predictive, resolution, viewBarTimes, viewBars),
      merged: buildLevelCacheFromRaw(rawLevels[level].merged, resolution, viewBarTimes, viewBars),
    };
  }

  return levels;
}

function buildLevelCacheFromRaw(
  levelData: RawLevelModeData,
  resolution: string,
  viewBarTimes: number[],
  viewBars: ApiBar[],
): LevelCache {
  const stroke = buildStrokePointsForView(levelData.strokes, resolution, viewBarTimes, viewBars);
  const pivots = sortedPivotsForView(levelData.centers, resolution, viewBarTimes);
  const channel = buildChannelMapForView(levelData.channels, resolution, viewBarTimes);
  const bspBuy = buildBspMapForView(levelData.signals, "buy", resolution, viewBarTimes);
  const bspSell = buildBspMapForView(levelData.signals, "sell", resolution, viewBarTimes);

  return {
    strokePoints: stroke.map,
    strokeTimes: stroke.times,
    pivots: pivots.pivots,
    pivotStarts: pivots.starts,
    channelPoints: channel.map,
    channelTimes: channel.times,
    bspBuy,
    bspBuyTimes: [...bspBuy.keys()].sort((left, right) => left - right),
    bspSell,
    bspSellTimes: [...bspSell.keys()].sort((left, right) => left - right),
  };
}

function buildChannelMapForView(
  channels: RawChannelPoint[],
  resolution: string,
  viewBarTimes: number[],
): { map: Map<number, RawChannelPoint>; times: number[] } {
  const map = new Map<number, RawChannelPoint>();
  for (const channel of channels) {
    const time = normalizeChanPointTimeForResolution(
      channel.time,
      resolution,
      viewBarTimes,
    );
    if (!Number.isFinite(time)) {
      continue;
    }
    map.set(time, {
      time,
      upper: channel.upper,
      lower: channel.lower,
    });
  }
  return {
    map,
    times: [...map.keys()].sort((left, right) => left - right),
  };
}

function buildStrokePointsForView(
  strokes: RawStrokePoint[],
  resolution: string,
  viewBarTimes: number[],
  viewBars: ApiBar[] = [],
): { map: Map<number, TimedLinePoint>; times: number[] } {
  const map = new Map<number, TimedLinePoint>();
  if (!strokes.length) {
    return { map, times: [] };
  }

  for (const stroke of strokes) {
    const startTime = projectChanPointToViewTime(stroke.startTime, stroke.startPrice, stroke.level, resolution, viewBarTimes, viewBars);
    if (Number.isFinite(startTime)) {
      map.set(startTime, {
        price: stroke.startPrice,
        dir: stroke.direction,
      });
    }
    const endTime = projectChanPointToViewTime(stroke.endTime, stroke.endPrice, stroke.level, resolution, viewBarTimes, viewBars);
    if (Number.isFinite(endTime)) {
      map.set(endTime, {
        price: stroke.endPrice,
        dir: stroke.direction,
      });
    }
  }

  return {
    map,
    times: [...map.keys()].sort((left, right) => left - right),
  };
}

function sortedPivotsForView(
  centers: RawCenterInterval[],
  resolution: string,
  viewBarTimes: number[],
): { pivots: PivotInterval[]; starts: number[] } {
  const pivots = centers
    .map((center) => ({
      key: center.key,
      startTime: normalizeChanPointTimeForResolution(
        center.startTime,
        resolution,
        viewBarTimes,
      ),
      endTime: normalizeChanPointTimeForResolution(
        center.endTime,
        resolution,
        viewBarTimes,
      ),
      high: center.high,
      low: center.low,
    }))
    .filter((item) => Number.isFinite(item.startTime) && Number.isFinite(item.endTime))
    .sort((left, right) => left.startTime - right.startTime);

  return {
    starts: pivots.map((item) => item.startTime),
    pivots,
  };
}

function buildBspMapForView(
  signals: RawSignalPoint[],
  side: SignalSide,
  resolution: string,
  viewBarTimes: number[],
): Map<number, TimedSignalPoint> {
  const map = new Map<number, TimedSignalPoint>();

  for (const signal of signals) {
    if (signal.side !== side) {
      continue;
    }
    const time = normalizeChanPointTimeForResolution(
      signal.time,
      resolution,
      viewBarTimes,
    );
    if (!Number.isFinite(time)) {
      continue;
    }
    map.set(time, {
      price: signal.price,
      bspType: signal.bspType,
    });
  }

  return map;
}

function lastPointInRange<T>(
  sortedTimes: number[],
  pointMap: Map<number, T>,
  rangeStart: number,
  rangeEnd: number,
): T | null {
  if (!sortedTimes.length) {
    return null;
  }
  const index = upperBound(sortedTimes, rangeEnd) - 1;
  if (index < 0) {
    return null;
  }
  const pointTime = sortedTimes[index];
  if (pointTime > rangeStart && pointTime <= rangeEnd) {
    return pointMap.get(pointTime) ?? null;
  }
  return null;
}

function findPivotOverlap(
  pivots: PivotInterval[],
  starts: number[],
  rangeStart: number,
  rangeEnd: number,
): PivotInterval | null {
  if (!pivots.length || starts.length !== pivots.length) {
    return null;
  }
  const index = upperBound(starts, rangeEnd) - 1;
  if (index < 0) {
    return null;
  }

  const containsFullBar = (pivot: PivotInterval | undefined): pivot is PivotInterval => {
    if (!pivot) {
      return false;
    }
    return pivot.startTime <= rangeStart && rangeEnd <= pivot.endTime;
  };

  const candidate = pivots[index];
  if (containsFullBar(candidate)) {
    return candidate;
  }
  const previous = pivots[index - 1];
  if (containsFullBar(previous)) {
    return previous;
  }
  return null;
}

function applyPivotBreak(
  level: ChanLevel,
  pivot: PivotInterval | null,
  context: PineStudyContext,
): PivotInterval | null {
  const state = context._pivotBreakState ?? {};
  context._pivotBreakState = state;
  const current = state[level] ?? { key: null, skip: 0 };

  if (!pivot) {
    state[level] = { key: null, skip: 0 };
    return null;
  }

  if (current.key !== pivot.key) {
    state[level] = { key: pivot.key, skip: 1 };
    return null;
  }

  if (current.skip > 0) {
    state[level] = { key: current.key, skip: current.skip - 1 };
    return null;
  }

  state[level] = current;
  return pivot;
}

function clearPivotBreak(level: ChanLevel, context: PineStudyContext): void {
  if (!context._pivotBreakState) {
    return;
  }
  context._pivotBreakState[level] = { key: null, skip: 0 };
}

function modeMatches(mode: string, confirmed: boolean, modeKey: ModeKey): boolean {
  const normalized = normalizeMode(mode, confirmed);
  if (modeKey === "merged") {
    return true;
  }
  return normalized === modeKey;
}

function resolveActiveModeKey(settings: ChanOverlaySettings): ModeKey {
  if (settings.modes.confirmed && settings.modes.predictive) {
    return "merged";
  }
  if (settings.modes.predictive) {
    return "predictive";
  }
  return "confirmed";
}

function shouldShowSignalLevel(level: ChanLevel, resolution: string): boolean {
  void level;
  void resolution;
  return true;
}

function shouldShowSignalPlot(
  plot: SignalPlot,
  displaySettings: ChanStudyDisplaySettings,
): boolean {
  return displaySettings.signalSides[plot.side]
    && displaySettings.signalVariants[plot.variant];
}

function signalPlotsForLevel(level: ChanLevel): SignalPlot[] {
  return SIGNAL_PLOTS.filter((plot) => plot.level === level);
}

function detectSignalSide(signalType: string): SignalSide | null {
  const normalized = signalType.trim().toUpperCase();
  if (!normalized) {
    return null;
  }
  if (normalized.startsWith("B") || signalType.includes("\u4e70")) {
    return "buy";
  }
  if (normalized.startsWith("S") || signalType.includes("\u5356")) {
    return "sell";
  }
  return null;
}

function extractSignalVariants(signalType: string): SignalVariant[] {
  const normalized = signalType.trim().toUpperCase();
  const variants = new Set<SignalVariant>();

  if (normalized.includes("2S") || signalType.includes("2s\u7c7b")) {
    variants.add("2s");
  }
  if (normalized.includes("1") || signalType.includes("1\u7c7b")) {
    variants.add("1");
  }
  if (!normalized.includes("2S") && (normalized.includes("2") || signalType.includes("2\u7c7b"))) {
    variants.add("2");
  }
  if (normalized.includes("3") || signalType.includes("3\u7c7b")) {
    variants.add("3");
  }

  return variants.size ? [...variants] : ["other"];
}

function shouldShowSignalVariant(
  side: SignalSide,
  variant: SignalVariant,
  displaySettings: ChanStudyDisplaySettings,
): boolean {
  return displaySettings.signalSides[side] && displaySettings.signalVariants[variant];
}

function signalVariantOffset(side: SignalSide, variant: SignalVariant): number {
  const base = side === "buy" ? 0 : 4;
  switch (variant) {
    case "1":
      return base;
    case "2":
      return base + 1;
    case "2s":
      return base + 2;
    case "3":
      return base + 3;
    default:
      return base;
  }
}

function parseSignalVariant(signalType: string): SignalVariant | null {
  const normalized = signalType.trim().toUpperCase();
  const match = normalized.match(/(?:^|[^0-9])([123])([A-Z])?/);
  if (!match) {
    return null;
  }
  const major = Number.parseInt(match[1], 10);
  const suffix = (match[2] ?? "").toLowerCase();
  if (major === 1) {
    return "1";
  }
  if (major === 2) {
    return suffix === "s" ? "2s" : "2";
  }
  if (major === 3) {
    return "3";
  }
  return null;
}

function resolveSignalSide(signalType: string): SignalSide | null {
  const normalized = signalType.trim().toUpperCase();
  if (!normalized) {
    return null;
  }
  if (
    normalized.startsWith("B")
    || signalType.includes("\u4e70")
  ) {
    return "buy";
  }
  if (
    normalized.startsWith("S")
    || signalType.includes("\u5356")
  ) {
    return "sell";
  }
  return null;
}

function resolveSignalSideFromItem(item: CanonicalSignalLike): SignalSide | null {
  if (typeof item.is_buy === "boolean") {
    return item.is_buy ? "buy" : "sell";
  }
  if (typeof item.side === "string") {
    const normalizedSide = item.side.trim().toLowerCase();
    if (normalizedSide === "buy" || normalizedSide === "b") {
      return "buy";
    }
    if (normalizedSide === "sell" || normalizedSide === "s") {
      return "sell";
    }
  }
  return resolveSignalSide(String(item.bsp_type ?? item.signal_type ?? ""));
}

function buildLineDefaults(level: ChanLevel): Record<string, unknown> {
  const style = defaultLineStyle(level);
  return {
    color: style.color,
    linestyle: 0,
    linewidth: style.linewidth,
    plottype: 0,
    visible: true,
    trackPrice: false,
    transparency: 0,
  };
}

function buildCenterDefaults(level: ChanLevel): Record<string, unknown> {
  const style = defaultCenterStyle(level);
  return {
    color: style.color,
    linestyle: 0,
    linewidth: style.linewidth,
    plottype: 0,
    visible: true,
    trackPrice: false,
    transparency: 0,
  };
}

function buildChannelDefaults(level: ChanLevel): Record<string, unknown> {
  const style = settingsLine(level, "channel");
  return {
    color: style.color,
    linestyle: 0,
    linewidth: style.linewidth,
    plottype: 0,
    visible: true,
    trackPrice: false,
    transparency: 0,
  };
}

function buildSignalDefaults(
  level: ChanLevel,
  side: SignalSide,
  variant: SignalVariant,
): Record<string, unknown> {
  const style = defaultSignalStyle(level, side);
  return {
    color: TRANSPARENT,
    textColor: style.textColor,
    plottype: side === "buy" ? "shape_label_up" : "shape_label_down",
    location: side === "buy" ? "BelowBar" : "AboveBar",
    text: signalText(level, side, variant),
    visible: true,
    linewidth: 1,
    linestyle: 0,
    trackPrice: false,
    transparency: 0,
  };
}

function buildFillDefaults(level: ChanLevel): Record<string, unknown> {
  if (level === "5f") {
    return { color: "rgba(242,193,78,0.12)", visible: true, transparency: 80 };
  }
  if (level === "30f") {
    return { color: "rgba(46,196,182,0.12)", visible: true, transparency: 80 };
  }
  if (level === "1d") {
    return { color: "rgba(240,101,149,0.12)", visible: true, transparency: 80 };
  }
  if (level === "1w") {
    return { color: "rgba(255,146,43,0.12)", visible: true, transparency: 80 };
  }
  return { color: "rgba(76,110,245,0.12)", visible: true, transparency: 80 };
}

function buildPaletteDefaults(level: ChanLevel): Record<string, unknown> {
  const style = defaultLineStyle(level);
  return {
    colors: [
      { color: style.color, width: style.linewidth, style: 0 },
      { color: style.color, width: style.linewidth, style: 0 },
    ],
  };
}

function buildSignalMeta(plotId: string): Record<string, unknown> {
  const plot = SIGNAL_PLOTS.find((item) => item.id === plotId);
  return {
    isHidden: false,
    title: plotOverrideName(plotId),
    text: plot ? signalText(plot.level, plot.side, plot.variant) : "",
    location: plot?.side === "buy" ? "BelowBar" : "AboveBar",
    histogramBase: 0,
    joinPoints: false,
  };
}

function defaultLineStyle(level: ChanLevel): { color: string; linewidth: number } {
  return settingsLine(level, "stroke");
}

function defaultCenterStyle(level: ChanLevel): { color: string; linewidth: number } {
  return settingsLine(level, "center");
}

function defaultSignalStyle(level: ChanLevel, side: SignalSide): ResolvedChanSignalStyle {
  return getChanSignalStyle(
    createDefaultChanOverlaySettings().styles,
    level,
    side === "buy" ? "B" : "S",
  );
}

function settingsLine(
  level: ChanLevel,
  part: "stroke" | "segment" | "center" | "channel",
): { color: string; linewidth: number } {
  const settings = createDefaultChanOverlaySettings().styles[level];
  const style = settings[part];
  return {
    color: style.color,
    linewidth: style.linewidth,
  };
}

function signalPlot(level: ChanLevel, side: SignalSide, variant: SignalVariant): SignalPlot {
  return {
    id: signalPlotId(level, side, variant),
    title: signalText(level, side, variant),
    level,
    side,
    variant,
  };
}

function signalPlotId(level: ChanLevel, side: SignalSide, variant: SignalVariant): string {
  const prefixByLevel: Record<ChanLevel, string> = {
    "5f": "bsp5",
    "30f": "bsp30",
    "1d": "bspd",
    "1w": "bspw",
    "1m": "bspm",
  };
  const prefix = prefixByLevel[level];
  const suffix = side === "buy"
    ? variant === "2s"
      ? "2sb"
      : `${variant}b`
    : variant === "2s"
      ? "2ss"
      : `${variant}s`;
  return `${prefix}_${suffix}`;
}

function signalText(level: ChanLevel, side: SignalSide, variant: SignalVariant): string {
  const levelTextByLevel: Record<ChanLevel, string> = {
    "5f": "5f",
    "30f": "30f",
    "1d": "\u65e5\u7ebf",
    "1w": "周线",
    "1m": "月线",
  };
  const levelText = levelTextByLevel[level];
  const variantText = variant === "2s" ? "2s\u7c7b" : `${variant}\u7c7b`;
  const sideText = side === "buy" ? "\u4e70" : "\u5356";
  return `${levelText}${variantText}${sideText}`;
}

function plotOverrideName(plotId: string): string {
  return PLOT_TITLE_BY_ID.get(plotId) ?? plotId;
}

function createEmptyLevelState(): Record<ModeKey, LevelCache> {
  return {
    confirmed: cloneEmptyLevelCache(),
    predictive: cloneEmptyLevelCache(),
    merged: cloneEmptyLevelCache(),
  };
}

function createEmptyRawLevelState(): Record<ModeKey, RawLevelModeData> {
  return {
    confirmed: cloneEmptyRawLevelModeData(),
    predictive: cloneEmptyRawLevelModeData(),
    merged: cloneEmptyRawLevelModeData(),
  };
}

function cloneEmptyLevelCache(): LevelCache {
  return {
    strokePoints: new Map<number, TimedLinePoint>(EMPTY_LEVEL_CACHE.strokePoints),
    strokeTimes: [...EMPTY_LEVEL_CACHE.strokeTimes],
    pivots: [...EMPTY_LEVEL_CACHE.pivots],
    pivotStarts: [...EMPTY_LEVEL_CACHE.pivotStarts],
    bspBuy: new Map<number, TimedSignalPoint>(EMPTY_LEVEL_CACHE.bspBuy),
    bspBuyTimes: [...EMPTY_LEVEL_CACHE.bspBuyTimes],
    bspSell: new Map<number, TimedSignalPoint>(EMPTY_LEVEL_CACHE.bspSell),
    bspSellTimes: [...EMPTY_LEVEL_CACHE.bspSellTimes],
    channelPoints: new Map<number, RawChannelPoint>(EMPTY_LEVEL_CACHE.channelPoints),
    channelTimes: [...EMPTY_LEVEL_CACHE.channelTimes],
  };
}

function cloneEmptyRawLevelModeData(): RawLevelModeData {
  return {
    strokes: [],
    centers: [],
    signals: [],
    channels: [],
  };
}

function normalizeLevel(level: string): ChanLevel {
  const normalized = level.trim().toLowerCase();
  if (normalized === "30f") {
    return "30f";
  }
  if (normalized === "1d" || normalized === "d" || normalized === "daily" || normalized === "day") {
    return "1d";
  }
  if (normalized === "1w" || normalized === "w" || normalized === "weekly" || normalized === "week") {
    return "1w";
  }
  if (normalized === "1m" || normalized === "m" || normalized === "monthly" || normalized === "month") {
    return "1m";
  }
  return "5f";
}

function normalizeMode(mode: string, confirmed: boolean): ModeKey {
  const normalized = mode.trim().toLowerCase();
  if (normalized === "predictive" || confirmed === false) {
    return "predictive";
  }
  return "confirmed";
}

function getResolutionStr(context: PineContext, PineJS: PineJS): string {
  try {
    const interval = PineJS.Std.interval?.(context);
    if (typeof interval === "string" && interval.trim()) {
      return interval.trim();
    }
  } catch {
    // ignore
  }
  try {
    const period = PineJS.Std.period(context);
    if (typeof period === "string" && period.trim()) {
      return period.trim();
    }
  } catch {
    // ignore
  }
  const resolution = context.symbol.resolution;
  if (typeof resolution === "string" && resolution.trim()) {
    return resolution.trim();
  }
  return activeStudyState.resolution || "5";
}

function resolveCurrentBarTime(context: PineContext, PineJS: PineJS): number {
  if (Number.isFinite(context.symbol.time)) {
    return context.symbol.time;
  }
  try {
    return PineJS.Std.time(context, PineJS.Std.period(context));
  } catch {
    return Number.NaN;
  }
}

function normalizePineBarTime(value: number): number {
  if (!Number.isFinite(value)) {
    return Number.NaN;
  }
  return Math.abs(value) < 1_000_000_000_000 ? value * 1000 : value;
}

function resolutionToSeconds(resolution: string): number {
  if (!resolution) {
    return 5 * 60;
  }
  const normalized = resolution.trim().toUpperCase();
  if (/^\d+$/.test(normalized)) {
    return Number.parseInt(normalized, 10) * 60;
  }
  if (normalized === "D" || normalized === "1D") {
    return 24 * 60 * 60;
  }
  if (normalized === "W" || normalized === "1W") {
    return 7 * 24 * 60 * 60;
  }
  if (normalized === "M" || normalized === "1M") {
    return 30 * 24 * 60 * 60;
  }
  const match = normalized.match(/^(\d+)(D|W|M)$/);
  if (match) {
    const count = Number.parseInt(match[1], 10) || 1;
    if (match[2] === "D") {
      return count * 24 * 60 * 60;
    }
    if (match[2] === "W") {
      return count * 7 * 24 * 60 * 60;
    }
    if (match[2] === "M") {
      return count * 30 * 24 * 60 * 60;
    }
  }
  return 5 * 60;
}

function buildViewBarTimes(chartBars: ApiBar[], resolution: string): number[] {
  if (!Array.isArray(chartBars) || chartBars.length === 0) {
    return [];
  }
  return [...new Set(
    chartBars
      .map((bar) => toTradingViewTime(bar.time, resolution))
      .filter((value) => Number.isFinite(value)),
  )].sort((left, right) => left - right);
}

function mapChanTimeToViewTime(
  chanTimeMs: number,
  resolution: string,
  viewBarTimes: number[] = [],
): number {
  if (!Number.isFinite(chanTimeMs)) {
    return chanTimeMs;
  }
  const normalized = resolution.trim().toUpperCase();

  if (/^\d+$/.test(normalized)) {
    const mappedBarTime = mapChanTimeToLoadedIntradayBar(
      chanTimeMs,
      resolution,
      viewBarTimes,
    );
    if (Number.isFinite(mappedBarTime)) {
      return mappedBarTime;
    }
    const minutes = Number.parseInt(normalized, 10);
    if (minutes > 0) {
      const mappedSessionEnd = mapChanTimeToSessionBarEnd(chanTimeMs, minutes);
      if (Number.isFinite(mappedSessionEnd)) {
        return mappedSessionEnd;
      }
    }
  }

  if (["D", "1D", "W", "1W", "M", "1M"].includes(normalized)) {
    return toTradingViewTime(chanTimeMs / 1000, normalized);
  }

  return toTradingViewTime(chanTimeMs / 1000, resolution);
}

function mapChanTimeToLoadedIntradayBar(
  chanTimeMs: number,
  resolution: string,
  viewBarTimes: number[],
): number {
  if (!viewBarTimes.length) {
    return Number.NaN;
  }
  const index = lowerBound(viewBarTimes, chanTimeMs);
  if (index < 0 || index >= viewBarTimes.length) {
    return Number.NaN;
  }
  const end = viewBarTimes[index];
  const fallbackStart = end - resolutionToSeconds(resolution) * 1000;
  const start = index > 0 ? viewBarTimes[index - 1] : fallbackStart;
  return chanTimeMs > start && chanTimeMs <= end ? end : Number.NaN;
}

function mapChanTimeToSessionBarEnd(chanTimeMs: number, minutes: number): number {
  if (!Number.isFinite(chanTimeMs) || minutes <= 0) {
    return Number.NaN;
  }

  const shanghaiMs = chanTimeMs + 8 * 60 * 60 * 1000;
  const date = new Date(shanghaiMs);
  const totalMinutes = date.getUTCHours() * 60 + date.getUTCMinutes();

  const morningStart = 9 * 60 + 30;
  const morningEnd = 11 * 60 + 30;
  const afternoonStart = 13 * 60;
  const afternoonEnd = 15 * 60;

  let sessionStart: number | null = null;
  let sessionEnd: number | null = null;

  if (totalMinutes > morningStart && totalMinutes <= morningEnd) {
    sessionStart = morningStart;
    sessionEnd = morningEnd;
  } else if (totalMinutes > afternoonStart && totalMinutes <= afternoonEnd) {
    sessionStart = afternoonStart;
    sessionEnd = afternoonEnd;
  } else {
    return Number.NaN;
  }

  const elapsed = totalMinutes - sessionStart;
  const alignedElapsed = Math.min(
    Math.ceil(elapsed / minutes) * minutes,
    sessionEnd - sessionStart,
  );
  const alignedTotalMinutes = sessionStart + alignedElapsed;
  const utcMinutes = alignedTotalMinutes - 8 * 60;
  const alignedHours = Math.floor(utcMinutes / 60);
  const alignedMins = utcMinutes % 60;

  return Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
    alignedHours,
    alignedMins,
    0,
    0,
  );
}

function normalizeChanPointTimeForResolution(
  chanTimeMs: number,
  resolution: string,
  viewBarTimes: number[] = [],
): number {
  return mapChanTimeToViewTime(chanTimeMs, resolution, viewBarTimes);
}

// Higher-level endpoints land on the last chart bar that owns the reported extreme.
export function projectChanPointToViewTime(
  chanTimeMs: number,
  price: number,
  level: string | undefined,
  resolution: string,
  viewBarTimes: number[] = [],
  viewBars: ApiBar[] = [],
): number {
  const sameLevel = !level || (INTERVAL_BY_TIMEFRAME[level] ?? level).toUpperCase() === resolution.toUpperCase();
  if (sameLevel) {
    return ["D", "1D", "W", "1W", "M", "1M"].includes(resolution.toUpperCase())
      ? toTradingViewTime(chanTimeMs / 1000, resolution)
      : chanTimeMs;
  }
  const intervalStart = nativeIntervalStartMs(chanTimeMs, level);
  const intervalBars = viewBars.filter((bar) => bar.time * 1000 > intervalStart && bar.time * 1000 <= chanTimeMs);
  if (intervalBars.length) {
    const maxHigh = Math.max(...intervalBars.map((bar) => bar.high));
    const minLow = Math.min(...intervalBars.map((bar) => bar.low));
    const candidates = intervalBars.filter((bar) =>
      (price === maxHigh && bar.high === price) || (price === minLow && bar.low === price),
    );
    if (candidates.length) return toTradingViewTime(candidates[candidates.length - 1].time, resolution);
  }
  return mapChanTimeToViewTime(chanTimeMs, resolution, viewBarTimes);
}

function nativeIntervalStartMs(endMs: number, level: string): number {
  const shanghaiOffset = 8 * 60 * 60 * 1000;
  if (level === "1d" || level === "1w" || level === "1m") {
    const shanghai = new Date(endMs - 1 + shanghaiOffset);
    let day = shanghai.getUTCDate();
    if (level === "1w") {
      day -= (shanghai.getUTCDay() + 6) % 7;
    }
    if (level === "1m") {
      day = 1;
    }
    return Date.UTC(shanghai.getUTCFullYear(), shanghai.getUTCMonth(), day) - shanghaiOffset;
  }
  const secondsByLevel: Record<string, number> = {
    "5f": 5 * 60,
    "15f": 15 * 60,
    "30f": 30 * 60,
    "1h": 60 * 60,
  };
  return endMs - (secondsByLevel[level] ?? 0) * 1000;
}

function lowerBound(values: number[], target: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if (values[middle] < target) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
}

function upperBound(values: number[], target: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if (values[middle] <= target) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
}

export const __CHAN_STUDY_TESTING__ = {
  buildViewBarTimes,
  buildStrokePointsForView,
  lineToRawStroke,
  sortedPivotsForView,
  buildBspMapForView,
  buildChannelMapForView,
  lastPointInRange,
  findPivotOverlap,
  applyPivotBreak,
  mapChanTimeToViewTime,
  mapChanTimeToLoadedIntradayBar,
  normalizeChanPointTimeForResolution,
  projectChanPointToViewTime,
  getActiveStudyState: () => activeStudyState,
};

