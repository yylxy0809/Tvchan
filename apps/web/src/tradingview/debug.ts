type TvDebugEvent = {
  at: string;
  event: string;
  detail?: unknown;
};

type TvDebugState = {
  events: TvDebugEvent[];
  widget?: Record<string, unknown>;
  datafeed?: Record<string, unknown>;
  overlay?: Record<string, unknown>;
  chanStudy?: Record<string, unknown>;
};

declare global {
  interface Window {
    __TV_DEBUG__?: TvDebugState;
  }
}

const MAX_EVENTS = 120;

export function recordTvDebug(event: string, detail?: unknown): void {
  const state = getDebugState();
  state.events.push({ at: new Date().toISOString(), event, detail });
  if (state.events.length > MAX_EVENTS) {
    state.events.splice(0, state.events.length - MAX_EVENTS);
  }
}

export function patchTvDebug(section: keyof Omit<TvDebugState, "events">, value: Record<string, unknown>): void {
  const state = getDebugState();
  state[section] = {
    ...(state[section] ?? {}),
    ...value,
  };
}

function getDebugState(): TvDebugState {
  if (!window.__TV_DEBUG__) {
    window.__TV_DEBUG__ = { events: [] };
  }
  return window.__TV_DEBUG__;
}
