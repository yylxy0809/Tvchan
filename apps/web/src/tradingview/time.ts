export const INTERVAL_BY_TIMEFRAME: Record<string, string> = {
  "5f": "5",
  "15f": "15",
  "30f": "30",
  "1h": "60",
  "1d": "D",
  "1w": "W",
  "1m": "M",
};

export const TIMEFRAME_BY_INTERVAL: Record<string, string> = {
  "5": "5f",
  "15": "15f",
  "30": "30f",
  "60": "1h",
  "1D": "1d",
  D: "1d",
  "1W": "1w",
  W: "1w",
  "1M": "1m",
  M: "1m",
};

const DWM_RESOLUTIONS = new Set(["D", "1D", "W", "1W", "M", "1M"]);
const SHANGHAI_OFFSET_MS = 8 * 60 * 60 * 1000;

export function timeframeFromTradingViewInterval(value: unknown): string | null {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }
  const normalized = String(value).toUpperCase();
  return TIMEFRAME_BY_INTERVAL[normalized] ?? TIMEFRAME_BY_INTERVAL[String(value)] ?? null;
}

export function toTradingViewTime(epochSeconds: number, resolution: string): number {
  const ms = epochSeconds * 1000;
  const normalized = resolution.toUpperCase();
  if (!DWM_RESOLUTIONS.has(normalized)) {
    return ms;
  }
  const shanghaiDate = new Date(ms + SHANGHAI_OFFSET_MS);
  const dayStart = Date.UTC(
    shanghaiDate.getUTCFullYear(),
    shanghaiDate.getUTCMonth(),
    shanghaiDate.getUTCDate(),
  );
  if (normalized === "W" || normalized === "1W") {
    const weekday = new Date(dayStart).getUTCDay();
    const daysFromMonday = (weekday + 6) % 7;
    return dayStart - daysFromMonday * 24 * 60 * 60 * 1000;
  }
  if (normalized === "M" || normalized === "1M") {
    return Date.UTC(shanghaiDate.getUTCFullYear(), shanghaiDate.getUTCMonth(), 1);
  }
  return dayStart;
}
