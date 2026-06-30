import {
  createDefaultChanOverlaySettings,
  type ChanLevel,
  type ChanOverlaySettings,
} from "./overlaySettings";
import { readLineStyle } from "./chanStyles";

type StudyInputValue = string | number | boolean;

type StudyInputValueItem = {
  id: string;
  value: StudyInputValue;
};

type StudyInputDefinition = {
  id: string;
  name: string;
  type: "bool" | "color" | "integer";
  defval: StudyInputValue;
  min?: number;
  max?: number;
  step?: number;
};

type PineInputGetter = (index: number) => StudyInputValue;

const LEVELS: ChanLevel[] = ["5f", "30f", "1d"];
const SIGNAL_VARIANTS = ["1", "2", "2s", "3", "other"] as const;

export type ChanStudySignalVariant = (typeof SIGNAL_VARIANTS)[number];

export type ChanStudyDisplaySettings = {
  signalSides: Record<"buy" | "sell", boolean>;
  signalVariants: Record<ChanStudySignalVariant, boolean>;
};

const LEVEL_LABELS: Record<ChanLevel, string> = {
  "5f": "5f",
  "30f": "30f",
  "1d": "日线",
};

export const CHAN_STUDY_INPUTS: StudyInputDefinition[] = [
  boolInput("show_5f", "显示 5f 级别", true),
  boolInput("show_30f", "显示 30f 级别", true),
  boolInput("show_1d", "显示日线级别", true),
  boolInput("show_strokes", "显示笔", true),
  boolInput("show_segments", "显示线段", true),
  boolInput("show_centers", "显示中枢", true),
  boolInput("show_signals", "显示买卖点", true),
  boolInput("show_channels", "显示 plot_channel", false),
  boolInput("show_buy_signals", "显示买点标签", true),
  boolInput("show_sell_signals", "显示卖点标签", true),
  boolInput("show_signal_1", "显示1类买卖点", true),
  boolInput("show_signal_2", "显示2类买卖点", true),
  boolInput("show_signal_2s", "显示2s类买卖点", true),
  boolInput("show_signal_3", "显示3类买卖点", true),
  boolInput("show_signal_other", "显示其他买卖点", true),
  boolInput("show_confirmed", "显示已完成走势", true),
  boolInput("show_predictive", "显示构建中走势", true),
  integerInput("confirmed_line_style", "已完成线型 0实线/1点线/2虚线", 0, 2),
  integerInput("predictive_line_style", "构建中线型 0实线/1点线/2虚线", 0, 2),
  ...LEVELS.flatMap((level) => [
    colorInput(`${level}_stroke_color`, `${LEVEL_LABELS[level]} 笔颜色`),
    widthInput(`${level}_stroke_width`, `${LEVEL_LABELS[level]} 笔线宽`),
    colorInput(`${level}_segment_color`, `${LEVEL_LABELS[level]} 线段颜色`),
    widthInput(`${level}_segment_width`, `${LEVEL_LABELS[level]} 线段线宽`),
    colorInput(`${level}_center_color`, `${LEVEL_LABELS[level]} 中枢颜色`),
    widthInput(`${level}_center_width`, `${LEVEL_LABELS[level]} 中枢线宽`),
    integerInput(`${level}_center_transparency`, `${LEVEL_LABELS[level]} 中枢透明度`, 0, 80),
    colorInput(`${level}_channel_color`, `${LEVEL_LABELS[level]} plot_channel 颜色`),
    widthInput(`${level}_channel_width`, `${LEVEL_LABELS[level]} plot_channel 线宽`),
    colorInput(`${level}_buy_color`, `${LEVEL_LABELS[level]} 买点标签背景`),
    colorInput(`${level}_buy_text_color`, `${LEVEL_LABELS[level]} 买点文字`),
    colorInput(`${level}_sell_color`, `${LEVEL_LABELS[level]} 卖点标签背景`),
    colorInput(`${level}_sell_text_color`, `${LEVEL_LABELS[level]} 卖点文字`),
  ]),
];

export const DEFAULT_CHAN_STUDY_INPUTS: Record<string, StudyInputValue> =
  Object.fromEntries(CHAN_STUDY_INPUTS.map((input) => [input.id, input.defval]));

const INPUT_INDEX_BY_ID = new Map(
  CHAN_STUDY_INPUTS.map((input, index) => [input.id, index]),
);

export function chanOverlaySettingsToStudyInputs(
  settings: ChanOverlaySettings,
): Record<string, StudyInputValue> {
  const inputs: Record<string, StudyInputValue> = {
    show_5f: settings.levels["5f"],
    show_30f: settings.levels["30f"],
    show_1d: settings.levels["1d"],
    show_strokes: settings.parts.strokes,
    show_segments: settings.parts.segments,
    show_centers: settings.parts.centers,
    show_signals: settings.parts.signals,
    show_channels: settings.parts.channels,
    show_buy_signals: true,
    show_sell_signals: true,
    show_signal_1: true,
    show_signal_2: true,
    show_signal_2s: true,
    show_signal_3: true,
    show_signal_other: true,
    show_confirmed: settings.modes.confirmed,
    show_predictive: settings.modes.predictive,
    confirmed_line_style: settings.lineStyles.confirmed,
    predictive_line_style: settings.lineStyles.predictive,
  };

  for (const level of LEVELS) {
    const style = settings.styles[level];
    inputs[`${level}_stroke_color`] = style.stroke.color;
    inputs[`${level}_stroke_width`] = style.stroke.linewidth;
    inputs[`${level}_segment_color`] = style.segment.color;
    inputs[`${level}_segment_width`] = style.segment.linewidth;
    inputs[`${level}_center_color`] = style.center.color;
    inputs[`${level}_center_width`] = style.center.linewidth;
    inputs[`${level}_center_transparency`] = style.center.transparency;
    inputs[`${level}_channel_color`] = style.channel.color;
    inputs[`${level}_channel_width`] = style.channel.linewidth;
    inputs[`${level}_buy_color`] = style.signal.buyColor;
    inputs[`${level}_buy_text_color`] = style.signal.buyTextColor;
    inputs[`${level}_sell_color`] = style.signal.sellColor;
    inputs[`${level}_sell_text_color`] = style.signal.sellTextColor;
  }

  return inputs;
}

export function studyInputValuesToOverlaySettings(
  values: StudyInputValueItem[],
  fallback: ChanOverlaySettings = createDefaultChanOverlaySettings(),
): ChanOverlaySettings {
  return studyInputRecordToOverlaySettings(
    Object.fromEntries(values.map((item) => [String(item.id), item.value])),
    fallback,
  );
}

export function studyInputGetterToOverlaySettings(
  getInput: PineInputGetter,
  fallback: ChanOverlaySettings = createDefaultChanOverlaySettings(),
): ChanOverlaySettings {
  const values: Record<string, StudyInputValue> = {};
  for (const input of CHAN_STUDY_INPUTS) {
    const index = INPUT_INDEX_BY_ID.get(input.id);
    values[input.id] = index === undefined ? input.defval : getInput(index);
  }
  return studyInputRecordToOverlaySettings(values, fallback);
}

export function studyInputGetterToDisplaySettings(
  getInput: PineInputGetter,
): ChanStudyDisplaySettings {
  return {
    signalSides: {
      buy: readBooleanById(getInput, "show_buy_signals", true),
      sell: readBooleanById(getInput, "show_sell_signals", true),
    },
    signalVariants: {
      "1": readBooleanById(getInput, "show_signal_1", true),
      "2": readBooleanById(getInput, "show_signal_2", true),
      "2s": readBooleanById(getInput, "show_signal_2s", true),
      "3": readBooleanById(getInput, "show_signal_3", true),
      other: readBooleanById(getInput, "show_signal_other", true),
    },
  };
}

export function studyInputItemsFromSettings(
  settings: ChanOverlaySettings,
): StudyInputValueItem[] {
  const values = chanOverlaySettingsToStudyInputs(settings);
  return CHAN_STUDY_INPUTS.map((input) => ({
    id: input.id,
    value: values[input.id],
  }));
}

function studyInputRecordToOverlaySettings(
  values: Record<string, unknown>,
  fallback: ChanOverlaySettings,
): ChanOverlaySettings {
  return {
    levels: {
      "5f": readBoolean(values.show_5f, fallback.levels["5f"]),
      "30f": readBoolean(values.show_30f, fallback.levels["30f"]),
      "1d": readBoolean(values.show_1d, fallback.levels["1d"]),
    },
    modes: {
      confirmed: readBoolean(values.show_confirmed, fallback.modes.confirmed),
      predictive: readBoolean(values.show_predictive, fallback.modes.predictive),
    },
    parts: {
      strokes: readBoolean(values.show_strokes, fallback.parts.strokes),
      segments: readBoolean(values.show_segments, fallback.parts.segments),
      centers: readBoolean(values.show_centers, fallback.parts.centers),
      signals: readBoolean(values.show_signals, fallback.parts.signals),
      channels: readBoolean(values.show_channels, fallback.parts.channels),
    },
    lineStyles: {
      confirmed: readLineStyle(
        values.confirmed_line_style,
        fallback.lineStyles.confirmed,
      ),
      predictive: readLineStyle(
        values.predictive_line_style,
        fallback.lineStyles.predictive,
      ),
    },
    styles: {
      "5f": readLevelStyle(values, "5f", fallback),
      "30f": readLevelStyle(values, "30f", fallback),
      "1d": readLevelStyle(values, "1d", fallback),
    },
  };
}

function readLevelStyle(
  values: Record<string, unknown>,
  level: ChanLevel,
  fallback: ChanOverlaySettings,
) {
  const base = fallback.styles[level];
  return {
    stroke: {
      color: readColor(values[`${level}_stroke_color`], base.stroke.color),
      linewidth: readInteger(values[`${level}_stroke_width`], base.stroke.linewidth, 1, 8),
    },
    segment: {
      color: readColor(values[`${level}_segment_color`], base.segment.color),
      linewidth: readInteger(values[`${level}_segment_width`], base.segment.linewidth, 1, 8),
    },
    center: {
      color: readColor(values[`${level}_center_color`], base.center.color),
      linewidth: readInteger(values[`${level}_center_width`], base.center.linewidth, 1, 8),
      transparency: readInteger(
        values[`${level}_center_transparency`],
        base.center.transparency,
        0,
        80,
      ),
    },
    channel: {
      color: readColor(values[`${level}_channel_color`], base.channel.color),
      linewidth: readInteger(values[`${level}_channel_width`], base.channel.linewidth, 1, 8),
    },
    signal: {
      buyColor: readColor(values[`${level}_buy_color`], base.signal.buyColor),
      buyTextColor: readColor(
        values[`${level}_buy_text_color`],
        base.signal.buyTextColor,
      ),
      sellColor: readColor(values[`${level}_sell_color`], base.signal.sellColor),
      sellTextColor: readColor(
        values[`${level}_sell_text_color`],
        base.signal.sellTextColor,
      ),
    },
  };
}

function boolInput(id: string, name: string, defval: boolean): StudyInputDefinition {
  return { id, name, type: "bool", defval };
}

function colorInput(id: string, name: string): StudyInputDefinition {
  const defaults = createDefaultChanOverlaySettings();
  return {
    id,
    name,
    type: "color",
    defval: chanOverlaySettingsToStudyInputs(defaults)[id],
  };
}

function widthInput(id: string, name: string): StudyInputDefinition {
  const defaults = createDefaultChanOverlaySettings();
  return {
    id,
    name,
    type: "integer",
    defval: chanOverlaySettingsToStudyInputs(defaults)[id],
    min: 1,
    max: 8,
    step: 1,
  };
}

function integerInput(
  id: string,
  name: string,
  min: number,
  max: number,
): StudyInputDefinition {
  const defaults = createDefaultChanOverlaySettings();
  return {
    id,
    name,
    type: "integer",
    defval: chanOverlaySettingsToStudyInputs(defaults)[id],
    min,
    max,
    step: 1,
  };
}

function readBoolean(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function readBooleanById(
  getInput: PineInputGetter,
  id: string,
  fallback: boolean,
): boolean {
  const index = INPUT_INDEX_BY_ID.get(id);
  return index === undefined ? fallback : readBoolean(getInput(index), fallback);
}

function readColor(value: unknown, fallback: string): string {
  return typeof value === "string" && /^#[0-9a-fA-F]{6}$/.test(value)
    ? value
    : fallback;
}

function readInteger(
  value: unknown,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.round(parsed)));
}
