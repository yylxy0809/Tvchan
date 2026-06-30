import type { ChanLevel } from "./overlaySettings";

export type ChanLineStyleMode = "confirmed" | "predictive";

export type ChanLineStyleByMode = Record<ChanLineStyleMode, number>;

export type ChanLineStyle = {
  color: string;
  linewidth: number;
  transparency?: number;
};

export type ChanSignalStyle = {
  buyColor: string;
  buyTextColor: string;
  sellColor: string;
  sellTextColor: string;
};

export type ChanLevelStyle = {
  stroke: ChanLineStyle;
  segment: ChanLineStyle;
  center: ChanLineStyle & { transparency: number };
  channel: ChanLineStyle;
  signal: ChanSignalStyle;
};

export type ChanStyleSettings = Record<ChanLevel, ChanLevelStyle>;

export type ResolvedChanLineStyle = {
  color: string;
  linewidth: number;
  linestyle: number;
  transparency?: number;
};

export type ResolvedChanSignalStyle = {
  color: string;
  textColor: string;
};

export const SOLID_LINE_STYLE = 0;
export const DASHED_LINE_STYLE = 2;
export const DOTTED_LINE_STYLE = 1;

export const DEFAULT_CHAN_LINE_STYLES: ChanLineStyleByMode = {
  confirmed: SOLID_LINE_STYLE,
  predictive: DASHED_LINE_STYLE,
};

export const DEFAULT_CHAN_STYLE_SETTINGS: ChanStyleSettings = {
  "5f": {
    stroke: { color: "#f2c14e", linewidth: 1 },
    segment: { color: "#f2c14e", linewidth: 2 },
    center: { color: "#f2c14e", linewidth: 1, transparency: 10 },
    channel: { color: "#8ecae6", linewidth: 1 },
    signal: {
      buyColor: "#ffd166",
      buyTextColor: "#20170a",
      sellColor: "#f4a261",
      sellTextColor: "#2a1206",
    },
  },
  "30f": {
    stroke: { color: "#2ec4b6", linewidth: 2 },
    segment: { color: "#2ec4b6", linewidth: 3 },
    center: { color: "#2ec4b6", linewidth: 2, transparency: 8 },
    channel: { color: "#219ebc", linewidth: 1 },
    signal: {
      buyColor: "#52d1c7",
      buyTextColor: "#062522",
      sellColor: "#1d7874",
      sellTextColor: "#f1fffd",
    },
  },
  "1d": {
    stroke: { color: "#f06595", linewidth: 3 },
    segment: { color: "#f06595", linewidth: 4 },
    center: { color: "#f06595", linewidth: 3, transparency: 5 },
    channel: { color: "#ffb703", linewidth: 1 },
    signal: {
      buyColor: "#ff7aa2",
      buyTextColor: "#2f0614",
      sellColor: "#d6336c",
      sellTextColor: "#fff0f5",
    },
  },
};

export function cloneChanStyleSettings(
  settings = DEFAULT_CHAN_STYLE_SETTINGS,
): ChanStyleSettings {
  return {
    "5f": cloneLevelStyle(settings["5f"]),
    "30f": cloneLevelStyle(settings["30f"]),
    "1d": cloneLevelStyle(settings["1d"]),
  };
}

export function mergeChanStyleSettings(value: unknown): ChanStyleSettings {
  const incoming = isRecord(value) ? value : {};
  return {
    "5f": mergeLevelStyle(DEFAULT_CHAN_STYLE_SETTINGS["5f"], incoming["5f"]),
    "30f": mergeLevelStyle(DEFAULT_CHAN_STYLE_SETTINGS["30f"], incoming["30f"]),
    "1d": mergeLevelStyle(DEFAULT_CHAN_STYLE_SETTINGS["1d"], incoming["1d"]),
  };
}

export function getChanLineStyle(
  settings: ChanStyleSettings,
  level: string,
  part: "stroke" | "segment" | "center" | "channel",
  confirmed: boolean,
  lineStyles: ChanLineStyleByMode = DEFAULT_CHAN_LINE_STYLES,
): ResolvedChanLineStyle {
  const levelStyle = settings[normalizeStyleLevel(level)];
  const style = levelStyle[part];
  const mode = confirmed ? "confirmed" : "predictive";
  return {
    color: style.color,
    linewidth: style.linewidth,
    linestyle: readLineStyle(lineStyles[mode], DEFAULT_CHAN_LINE_STYLES[mode]),
    transparency: style.transparency,
  };
}

export function getChanSignalStyle(
  settings: ChanStyleSettings,
  level: string,
  signalType: string,
): ResolvedChanSignalStyle {
  const signal = settings[normalizeStyleLevel(level)].signal;
  if (signalType.startsWith("B")) {
    return {
      color: signal.buyColor,
      textColor: signal.buyTextColor,
    };
  }
  return {
    color: signal.sellColor,
    textColor: signal.sellTextColor,
  };
}

function cloneLevelStyle(style: ChanLevelStyle): ChanLevelStyle {
  return {
    stroke: { ...style.stroke },
    segment: { ...style.segment },
    center: { ...style.center },
    channel: { ...style.channel },
    signal: { ...style.signal },
  };
}

function mergeLevelStyle(defaultStyle: ChanLevelStyle, value: unknown): ChanLevelStyle {
  const incoming = isRecord(value) ? value : {};
  return {
    stroke: mergeLineStyle(defaultStyle.stroke, incoming.stroke),
    segment: mergeLineStyle(defaultStyle.segment, incoming.segment),
    center: mergeCenterStyle(defaultStyle.center, incoming.center),
    channel: mergeLineStyle(defaultStyle.channel, incoming.channel),
    signal: mergeSignalStyle(defaultStyle.signal, incoming.signal),
  };
}

function mergeLineStyle(defaultStyle: ChanLineStyle, value: unknown): ChanLineStyle {
  const incoming = isRecord(value) ? value : {};
  return {
    color: readHexColor(incoming.color, defaultStyle.color),
    linewidth: readInt(incoming.linewidth, defaultStyle.linewidth, 1, 8),
  };
}

function mergeCenterStyle(
  defaultStyle: ChanLineStyle & { transparency: number },
  value: unknown,
): ChanLineStyle & { transparency: number } {
  const incoming = isRecord(value) ? value : {};
  return {
    color: readHexColor(incoming.color, defaultStyle.color),
    linewidth: readInt(incoming.linewidth, defaultStyle.linewidth, 1, 8),
    transparency: readInt(incoming.transparency, defaultStyle.transparency, 0, 80),
  };
}

function mergeSignalStyle(defaultStyle: ChanSignalStyle, value: unknown): ChanSignalStyle {
  const incoming = isRecord(value) ? value : {};
  return {
    buyColor: readHexColor(incoming.buyColor, defaultStyle.buyColor),
    buyTextColor: readHexColor(incoming.buyTextColor, defaultStyle.buyTextColor),
    sellColor: readHexColor(incoming.sellColor, defaultStyle.sellColor),
    sellTextColor: readHexColor(incoming.sellTextColor, defaultStyle.sellTextColor),
  };
}

function normalizeStyleLevel(level: string): ChanLevel {
  if (level === "5f" || level === "30f" || level === "1d") {
    return level;
  }
  return "5f";
}

function readHexColor(value: unknown, fallback: string): string {
  if (typeof value !== "string") {
    return fallback;
  }
  return /^#[0-9a-fA-F]{6}$/.test(value) ? value : fallback;
}

export function readLineStyle(value: unknown, fallback: number): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  if (parsed === SOLID_LINE_STYLE || parsed === DOTTED_LINE_STYLE || parsed === DASHED_LINE_STYLE) {
    return parsed;
  }
  return fallback;
}

function readInt(value: unknown, fallback: number, min: number, max: number): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.round(parsed)));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
