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

const LEVELS: ChanLevel[] = ["5f", "30f", "1d"];
const MODE_KEYS: ModeKey[] = ["confirmed", "predictive", "merged"];
const SIGNAL_VARIANTS: SignalVariant[] = ["1", "2", "2s", "3"];
const STUDY_ID = "ChanOverlay@tv-basicstudies-1";
const STUDY_NAME = "Chan Overlay";
const STUDY_SHORT = "Chan";
export const CHAN_STUDY_DESCRIPTION = STUDY_SHORT;
const DAY_MS = 24 * 60 * 60 * 1000;
const TRANSPARENT = "rgba(0,0,0,0)";
const NAN_VALUES = Array.from({ length: 42 }, () => Number.NaN);

const MAIN_LINE_PLOTS: MainLinePlot[] = [
  { id: "bi_val", title: "5f\u7b14", level: "5f" },
  { id: "seg_val", title: "30f\u7ebf\u6bb5", level: "30f" },
  { id: "ss_val", title: "\u65e5\u7ebf\u8d70\u52bf", level: "1d" },
];

const CENTER_PLOTS: CenterPlot[] = [
  { id: "zs5_hi", title: "5f\u4e2d\u67a2\u4e0a\u8f68", level: "5f", side: "high" },
  { id: "zs5_lo", title: "5f\u4e2d\u67a2\u4e0b\u8f68", level: "5f", side: "low" },
  { id: "zs30_hi", title: "30f\u4e2d\u67a2\u4e0a\u8f68", level: "30f", side: "high" },
  { id: "zs30_lo", title: "30f\u4e2d\u67a2\u4e0b\u8f68", level: "30f", side: "low" },
  { id: "zsd_hi", title: "\u65e5\u7ebf\u4e2d\u67a2\u4e0a\u8f68", level: "1d", side: "high" },
  { id: "zsd_lo", title: "\u65e5\u7ebf\u4e2d\u67a2\u4e0b\u8f68", level: "1d", side: "low" },
];

const SIGNAL_PLOTS: SignalPlot[] = [
  signalPlot("5f", "buy", "1"),
  signalPlot("5f", "buy", "2"),
  signalPlot("5f", "buy", "2s"),
  signalPlot("5f", "buy", "3"),
  signalPlot("5f", "sell", "1"),
  signalPlot("5f", "sell", "2"),
  signalPlot("5f", "sell", "2s"),
  signalPlot("5f", "sell", "3"),
  signalPlot("30f", "buy", "1"),
  signalPlot("30f", "buy", "2"),
  signalPlot("30f", "buy", "2s"),
  signalPlot("30f", "buy", "3"),
  signalPlot("30f", "sell", "1"),
  signalPlot("30f", "sell", "2"),
  signalPlot("30f", "sell", "2s"),
  signalPlot("30f", "sell", "3"),
  signalPlot("1d", "buy", "1"),
  signalPlot("1d", "buy", "2"),
  signalPlot("1d", "buy", "2s"),
  signalPlot("1d", "buy", "3"),
  signalPlot("1d", "sell", "1"),
  signalPlot("1d", "sell", "2"),
  signalPlot("1d", "sell", "2s"),
  signalPlot("1d", "sell", "3"),
];

const CHANNEL_PLOTS: ChannelPlot[] = [
  { id: "ch5_hi", title: "5f plot_channel \u4e0a\u8f68", level: "5f", side: "upper" },
  { id: "ch5_lo", title: "5f plot_channel \u4e0b\u8f68", level: "5f", side: "lower" },
  { id: "ch30_hi", title: "30f plot_channel \u4e0a\u8f68", level: "30f", side: "upper" },
  { id: "ch30_lo", title: "30f plot_channel \u4e0b\u8f68", level: "30f", side: "lower" },
  { id: "chd_hi", title: "\u65e5\u7ebf plot_channel \u4e0a\u8f68", level: "1d", side: "upper" },
  { id: "chd_lo", title: "\u65e5\u7ebf plot_channel \u4e0b\u8f68", level: "1d", side: "lower" },
];

const PLOTS: PlotDefinition[] = [...MAIN_LINE_PLOTS, ...CENTER_PLOTS, ...CHANNEL_PLOTS, ...SIGNAL_PLOTS];
const PLOT_TITLE_BY_ID = new Map(PLOTS.map((plot) => [plot.id, plot.title]));
const PALETTE_ID_BY_LEVEL: Record<ChanLevel, string> = {
  "5f": "pal_bi",
  "30f": "pal_seg",
  "1d": "pal_ss",
};
const FILL_ID_BY_LEVEL: Record<ChanLevel, string> = {
  "5f": "fill5",
  "30f": "fill30",
  "1d": "filld",
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
  rawLevels: {
    "5f": createEmptyRawLevelState(),
    "30f": createEmptyRawLevelState(),
    "1d": createEmptyRawLevelState(),
  },
  levels: {
    "5f": createEmptyLevelState(),
    "30f": createEmptyLevelState(),
    "1d": createEmptyLevelState(),
  },
  fallbackCenters: [],
};

let activeStudyState: ChanStudyState = EMPTY_STATE;

export function setChanStudyOverlay(overlay: ChanOverlayResponse, chartBars: ApiBar[] = []): void {
  activeStudyState = buildStudyState(overlay, chartBars);
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
    const style = plot.level === "5f"
      ? settings.styles[plot.level].stroke
      : settings.styles[plot.level].segment;
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
    overrides[`${fillId}.color`] = style.color;
    overrides[`${fillId}.transparency`] = style.transparency;
    overrides[`${fillId}.visible`] = visible;
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
        },
        filledAreas: [
          { id: "fill5", objAId: "zs5_hi", objBId: "zs5_lo", type: "plot_plot", title: "5f涓灑" },
          { id: "fill30", objAId: "zs30_hi", objBId: "zs30_lo", type: "plot_plot", title: "30f涓灑" },
          { id: "filld", objAId: "zsd_hi", objBId: "zsd_lo", type: "plot_plot", title: "鏃ョ嚎涓灑" },
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
          },
          filledAreasStyle: {
            fill5: buildFillDefaults("5f"),
            fill30: buildFillDefaults("30f"),
            filld: buildFillDefaults("1d"),
          },
          palettes: {
            pal_bi: buildPaletteDefaults("5f"),
            pal_seg: buildPaletteDefaults("30f"),
            pal_ss: buildPaletteDefaults("1d"),
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
    const pivot = findPivotOverlap(levelCache.pivots, levelCache.pivotStarts, rangeStart, rangeEnd);
    const center = applyPivotBreak(level, pivot, context);
    if (center) {
      values[baseIndex + 2] = center.high;
      values[baseIndex + 3] = center.low;
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
    rawLevels,
    levels: rebuildLevelCaches(rawLevels, resolution, viewBarTimes),
    fallbackCenters: [],
  };
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
      .filter((item): item is RawStrokePoint => item !== null),
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
    startTime: startTime * 1000,
    startPrice,
    endTime: endTime * 1000,
    endPrice,
    direction: String(item.direction ?? ""),
  };
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
): ChanStudyState["levels"] {
  const levels: ChanStudyState["levels"] = {
    "5f": createEmptyLevelState(),
    "30f": createEmptyLevelState(),
    "1d": createEmptyLevelState(),
  };

  for (const level of LEVELS) {
    levels[level] = {
      confirmed: buildLevelCacheFromRaw(rawLevels[level].confirmed, resolution, viewBarTimes),
      predictive: buildLevelCacheFromRaw(rawLevels[level].predictive, resolution, viewBarTimes),
      merged: buildLevelCacheFromRaw(rawLevels[level].merged, resolution, viewBarTimes),
    };
  }

  return levels;
}

function buildLevelCacheFromRaw(
  levelData: RawLevelModeData,
  resolution: string,
  viewBarTimes: number[],
): LevelCache {
  const stroke = buildStrokePointsForView(levelData.strokes, resolution, viewBarTimes);
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
): { map: Map<number, TimedLinePoint>; times: number[] } {
  const map = new Map<number, TimedLinePoint>();
  if (!strokes.length) {
    return { map, times: [] };
  }

  for (const stroke of strokes) {
    const endTime = normalizeChanPointTimeForResolution(
      stroke.endTime,
      resolution,
      viewBarTimes,
    );
    if (Number.isFinite(endTime)) {
      map.set(endTime, {
        price: stroke.endPrice,
        dir: stroke.direction,
      });
    }
  }

  for (const stroke of strokes) {
    const startTime = normalizeChanPointTimeForResolution(
      stroke.startTime,
      resolution,
      viewBarTimes,
    );
    if (Number.isFinite(startTime)) {
      map.set(startTime, {
        price: stroke.startPrice,
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
  const normalized = resolution.trim().toUpperCase();
  const minuteValue = Number.parseInt(normalized, 10);
  const isMinute30OrAbove = Number.isFinite(minuteValue) && minuteValue >= 30;
  const isHourOrAbove = /^\d+H$/.test(normalized);
  const isDailyOrAbove = normalized === "D"
    || normalized === "1D"
    || normalized === "W"
    || normalized === "1W"
    || normalized === "M"
    || normalized === "1M";

  if (!isMinute30OrAbove && !isHourOrAbove && !isDailyOrAbove) {
    return true;
  }
  return level === "30f" || level === "1d";
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
    visible: false,
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
  return { color: "rgba(240,101,149,0.12)", visible: true, transparency: 80 };
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
  return settingsLine(level, level === "5f" ? "stroke" : "segment");
}

function defaultCenterStyle(level: ChanLevel): { color: string; linewidth: number } {
  return settingsLine(level, "center");
}

function defaultSignalStyle(level: ChanLevel, side: SignalSide): ResolvedChanSignalStyle {
  return getChanSignalStyle(
    {
      "5f": createDefaultChanOverlaySettings().styles["5f"],
      "30f": createDefaultChanOverlaySettings().styles["30f"],
      "1d": createDefaultChanOverlaySettings().styles["1d"],
    },
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
  const prefix = level === "1d" ? "bspd" : level === "30f" ? "bsp30" : "bsp5";
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
  const levelText = level === "1d" ? "\u65e5\u7ebf" : level;
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

  if (normalized === "D" || normalized === "1D") {
    const date = new Date(chanTimeMs);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), 0, 0, 0, 0);
  }

  if (normalized === "W" || normalized === "1W") {
    const date = new Date(chanTimeMs);
    const dayOfWeek = date.getUTCDay();
    const daysFromMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    return Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate() - daysFromMonday,
      0,
      0,
      0,
      0,
    );
  }

  if (normalized === "M" || normalized === "1M") {
    const date = new Date(chanTimeMs);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1, 0, 0, 0, 0);
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
  lastPointInRange,
  findPivotOverlap,
  applyPivotBreak,
  mapChanTimeToViewTime,
  mapChanTimeToLoadedIntradayBar,
  normalizeChanPointTimeForResolution,
};

