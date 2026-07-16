import type { ChanOverlayResponse } from "../api/client";
import {
  cloneChanStyleSettings,
  DEFAULT_CHAN_LINE_STYLES,
  DEFAULT_CHAN_STYLE_SETTINGS,
  mergeChanStyleSettings,
  readLineStyle,
  type ChanLineStyleByMode,
  type ChanStyleSettings,
} from "./chanStyles";

export type ChanLevel = "5f" | "30f" | "1d" | "1w" | "1m";
export type ChanMode = "confirmed" | "predictive";
export type ChanOverlayPart = "strokes" | "segments" | "centers" | "signals" | "channels";

export type ChanOverlaySettings = {
  levels: Record<ChanLevel, boolean>;
  modes: Record<ChanMode, boolean>;
  parts: Record<ChanOverlayPart, boolean>;
  lineStyles: ChanLineStyleByMode;
  styles: ChanStyleSettings;
};

export const CHAN_LEVELS: ChanLevel[] = ["5f", "30f", "1d", "1w", "1m"];
export const CHAN_MODES: ChanMode[] = ["confirmed", "predictive"];
export const CHAN_PARTS: ChanOverlayPart[] = ["strokes", "segments", "centers", "signals", "channels"];

export const DEFAULT_CHAN_OVERLAY_SETTINGS: ChanOverlaySettings = {
  levels: {
    "5f": true,
    "30f": true,
    "1d": true,
    "1w": true,
    "1m": true,
  },
  modes: {
    confirmed: true,
    predictive: true,
  },
  parts: {
    strokes: true,
    segments: true,
    centers: true,
    signals: true,
    channels: false,
  },
  lineStyles: DEFAULT_CHAN_LINE_STYLES,
  styles: DEFAULT_CHAN_STYLE_SETTINGS,
};

export function createDefaultChanOverlaySettings(): ChanOverlaySettings {
  return {
    levels: { ...DEFAULT_CHAN_OVERLAY_SETTINGS.levels },
    modes: { ...DEFAULT_CHAN_OVERLAY_SETTINGS.modes },
    parts: { ...DEFAULT_CHAN_OVERLAY_SETTINGS.parts },
    lineStyles: { ...DEFAULT_CHAN_OVERLAY_SETTINGS.lineStyles },
    styles: cloneChanStyleSettings(),
  };
}

export function mergeChanOverlaySettings(value: unknown): ChanOverlaySettings {
  const incoming = isRecord(value) ? value : {};
  return {
    levels: {
      "5f": readBoolean(incoming.levels, "5f", true),
      "30f": readBoolean(incoming.levels, "30f", true),
      "1d": readBoolean(incoming.levels, "1d", true),
      "1w": readBoolean(incoming.levels, "1w", true),
      "1m": readBoolean(incoming.levels, "1m", true),
    },
    modes: {
      confirmed: readBoolean(incoming.modes, "confirmed", true),
      predictive: readBoolean(incoming.modes, "predictive", true),
    },
    parts: {
      strokes: readBoolean(incoming.parts, "strokes", true),
      segments: readBoolean(incoming.parts, "segments", true),
      centers: readBoolean(incoming.parts, "centers", true),
      signals: readBoolean(incoming.parts, "signals", true),
      channels: readBoolean(incoming.parts, "channels", false),
    },
    lineStyles: {
      confirmed: readLineStyle(
        readRecordValue(incoming.lineStyles, "confirmed"),
        DEFAULT_CHAN_LINE_STYLES.confirmed,
      ),
      predictive: readLineStyle(
        readRecordValue(incoming.lineStyles, "predictive"),
        DEFAULT_CHAN_LINE_STYLES.predictive,
      ),
    },
    styles: mergeChanStyleSettings(incoming.styles),
  };
}

export function filterChanOverlay(
  overlay: ChanOverlayResponse,
  settings: ChanOverlaySettings,
): ChanOverlayResponse {
  const levelEnabled = (level: string) => Boolean(settings.levels[level as ChanLevel]);
  const modeEnabled = (mode: string) => Boolean(settings.modes[mode as ChanMode]);

  return {
    ...overlay,
    levels: overlay.levels.filter(levelEnabled),
    modes: overlay.modes.filter(modeEnabled),
    bars_by_level: Object.fromEntries(
      overlay.levels
        .filter(levelEnabled)
        .map((level) => [level, overlay.bars_by_level[level] ?? 0]),
    ),
    strokes: settings.parts.strokes
      ? overlay.strokes.filter((item) => levelEnabled(item.level) && modeEnabled(item.mode))
      : [],
    segments: settings.parts.segments
      ? overlay.segments.filter((item) => levelEnabled(item.level) && modeEnabled(item.mode))
      : [],
    centers: settings.parts.centers
      ? overlay.centers.filter((item) => levelEnabled(item.level) && modeEnabled(item.mode))
      : [],
    signals: settings.parts.signals
      ? overlay.signals.filter((item) => levelEnabled(item.level) && modeEnabled(item.mode))
      : [],
    channels: settings.parts.channels
      ? overlay.channels.filter((item) => levelEnabled(item.level) && modeEnabled(item.mode))
      : [],
  };
}

function readBoolean(value: unknown, key: string, fallback: boolean): boolean {
  if (!isRecord(value) || typeof value[key] !== "boolean") {
    return fallback;
  }
  return value[key];
}

function readRecordValue(value: unknown, key: string): unknown {
  return isRecord(value) ? value[key] : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
