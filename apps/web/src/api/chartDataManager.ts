import { CHART_DATA_TRANSPORT } from "../config";
import { patchTvDebug, recordTvDebug } from "../tradingview/debug";
import {
  ApiBar,
  ApiChartBundleResponse,
  BarsResponse,
  ChanOverlayResponse,
  DEFAULT_CHAN_LEVELS,
  DEFAULT_CHAN_MODES,
  getBars as getBarsHttp,
  getChanOverlay as getChanOverlayHttp,
  getChartBundle as getChartBundleHttp,
  normalizeChartBundleForFrontend,
} from "./client";
import { createChartSocket, createRealtimeSocket } from "./realtime";

export type ChartWindowRequest = {
  symbol: string;
  timeframe: string;
  limit: number;
  from?: number;
  to?: number;
  levels?: readonly string[];
  modes?: readonly string[];
  signal?: AbortSignal;
};

export type ChartBarsResponse = BarsResponse & {
  /** True only when a request reached the beginning of available history. */
  noData?: boolean;
};

export type ChartWindowResponse = {
  schema_version:
    | ApiChartBundleResponse["schema_version"]
    | "frontend-chart-bundle.v2";
  snapshot_id: string;
  symbol: string;
  chart_timeframe: string;
  range: {
    from?: number | null;
    to?: number | null;
    limit: number;
  };
  bars: ApiBar[];
  chan: ChanOverlayResponse;
  transport: "http" | "websocket";
};

export type ChartHistoryWindowEvent = {
  source: "app" | "tradingview-datafeed" | "realtime";
  symbol: string;
  timeframe: string;
  resolution?: string;
  requestedFrom?: number;
  requestedTo?: number;
  from?: number;
  to?: number;
  limit: number;
  bars: ApiBar[];
  first?: number;
  last?: number;
};

export type SnapshotUpdateEvent = {
  source: "response" | "realtime";
  symbol: string;
  timeframe: string;
  snapshotVersion: string;
};

type CacheRecord<T> = {
  value: T;
  createdAt: number;
};

type WebSocketRequest = Record<string, unknown> & {
  type: string;
  request_id: string;
};

type WebSocketReply = Record<string, unknown> & {
  type?: string;
  request_id?: string;
  error?: string;
  message?: string;
};

type ChanSubscriptionReply = WebSocketReply & {
  id?: string;
  symbol?: string;
  timeframe?: string;
  snapshot_version?: string;
};

export type ChanOverlayTransportStatus = "connected" | "disconnected" | "replayed";

type RealtimeBarReply = {
  type?: string;
  id?: string;
  seq?: number;
  symbol?: string;
  timeframe?: string;
  snapshot_version?: string;
  sessionGeneration?: number;
  bar?: unknown;
};

export type RealtimeSidebarContext = {
  subscriptionId: string;
  chartSymbol: string;
  chartEpoch: number;
  watchlistId: string;
  watchlistRevision: number;
  watchlistSymbols: string[];
  channels: string[];
  afterSequence: number;
  snapshotVersion: number;
};

const CACHE_TTL_MS = 90_000;
const WS_RETRY_COOLDOWN_MS = 30_000;
const WS_REQUEST_TIMEOUT_MS = 8_000;
const SESSION_HISTORY_MAX_BARS = 15_000;
const SESSION_CHAN_MAX_ITEMS = 30_000;
const BARS_HISTORY_MAX_SERIES = 24;
const BARS_HISTORY_MAX_BARS = 15_000;
const BARS_HTTP_MAX_LIMIT = 5_000;

type CoveredInterval = { from?: number; to?: number };
type BarsHistory = {
  bars: ApiBar[];
  covered: CoveredInterval[];
  exhaustedBefore?: number;
  touchedAt: number;
};
type PendingBarsRequest = {
  request: ChartWindowRequest;
  controller: AbortController;
  consumers: number;
  complete: boolean;
  promise: Promise<BarsResponse>;
};
type RealtimeBarState = {
  sessionGeneration: number;
  revision: number;
  seq?: number;
  signature: string;
};

class ChartWebSocketClient {
  private socket: WebSocket | null = null;
  private connectPromise: Promise<WebSocket> | null = null;
  private pending = new Map<
    string,
    {
      resolve: (value: WebSocketReply) => void;
      reject: (error: Error) => void;
      timer: number;
    }
  >();
  private subscriptions = new Map<
    string,
    {
      payload: Record<string, unknown>;
      unsubscribeType: string;
      listeners: Set<(message: unknown) => void>;
      statusListeners: Set<(status: ChanOverlayTransportStatus) => void>;
    }
  >();
  private reconnectTimer: number | null = null;

  async request<T extends WebSocketReply>(
    payload: Omit<WebSocketRequest, "request_id"> & { type: string },
    signal?: AbortSignal,
  ): Promise<T> {
    const requestId = createRequestId();
    const socket = await this.connect();
    if (signal?.aborted) {
      throw new DOMException("Request aborted", "AbortError");
    }
    const message: WebSocketRequest = {
      ...payload,
      type: payload.type,
      request_id: requestId,
    };
    const response = await new Promise<WebSocketReply>((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.pending.delete(requestId);
        reject(new Error(`WebSocket request timed out: ${payload.type}`));
      }, WS_REQUEST_TIMEOUT_MS);
      const abort = () => {
        window.clearTimeout(timer);
        this.pending.delete(requestId);
        reject(new DOMException("Request aborted", "AbortError"));
      };
      if (signal) {
        signal.addEventListener("abort", abort, { once: true });
      }
      this.pending.set(requestId, {
        resolve: (value) => {
          if (signal) {
            signal.removeEventListener("abort", abort);
          }
          resolve(value);
        },
        reject: (error) => {
          if (signal) {
            signal.removeEventListener("abort", abort);
          }
          reject(error);
        },
        timer,
      });
      socket.send(JSON.stringify(message));
    });
    if (response.error) {
      throw new Error(String(response.error));
    }
    return response as T;
  }

  async subscribe(
    subscriptionId: string,
    payload: Record<string, unknown> & { type: string },
    listener: (message: unknown) => void,
    statusListener: (status: ChanOverlayTransportStatus) => void,
  ): Promise<() => void> {
    const normalizedPayload = {
      ...payload,
      id: subscriptionId,
    };
    const existing = this.subscriptions.get(subscriptionId);
    if (existing) {
      existing.payload = normalizedPayload;
      existing.unsubscribeType = unsubscribeTypeFor(payload.type);
      existing.listeners.add(listener);
      existing.statusListeners.add(statusListener);
    } else {
      this.subscriptions.set(subscriptionId, {
        payload: normalizedPayload,
        unsubscribeType: unsubscribeTypeFor(payload.type),
        listeners: new Set([listener]),
        statusListeners: new Set([statusListener]),
      });
    }

    if (this.socket?.readyState === WebSocket.OPEN) {
      const socket = this.socket;
      socket.send(JSON.stringify(normalizedPayload));
      statusListener("connected");
    } else {
      statusListener("disconnected");
      void this.connect().catch(() => {
        statusListener("disconnected");
        this.scheduleReconnect();
      });
    }

    return () => {
      this.removeSubscriptionListener(
        subscriptionId,
        listener,
        statusListener,
        true,
      );
    };
  }

  private async connect(): Promise<WebSocket> {
    if (this.socket?.readyState === WebSocket.OPEN) {
      return this.socket;
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }
    this.connectPromise = new Promise((resolve, reject) => {
      const socket = createChartSocket();
      this.socket = socket;
      const timer = window.setTimeout(() => {
        if (this.socket !== socket) return;
        socket.close();
        reject(new Error("WebSocket chart transport connect timeout"));
      }, 4_000);
      socket.onopen = () => {
        if (this.socket !== socket) return;
        window.clearTimeout(timer);
        this.connectPromise = null;
        if (this.reconnectTimer !== null) {
          window.clearTimeout(this.reconnectTimer);
          this.reconnectTimer = null;
        }
        this.notifyTransport("connected");
        this.replaySubscriptions(socket);
        resolve(socket);
      };
      socket.onerror = () => {
        if (this.socket !== socket) return;
        window.clearTimeout(timer);
        this.socket = null;
        this.connectPromise = null;
        reject(new Error("WebSocket chart transport failed"));
      };
      socket.onclose = () => {
        if (this.socket !== socket) return;
        window.clearTimeout(timer);
        this.socket = null;
        this.connectPromise = null;
        this.rejectPending(new Error("WebSocket chart transport closed"));
        this.notifyTransport("disconnected");
        this.scheduleReconnect();
      };
      socket.onmessage = (event) => {
        if (this.socket !== socket) return;
        this.handleMessage(event.data);
      };
    });
    return this.connectPromise;
  }

  private handleMessage(data: unknown): void {
    let message: ChanSubscriptionReply;
    try {
      message = JSON.parse(String(data)) as ChanSubscriptionReply;
    } catch {
      return;
    }
    if (message.request_id) {
      const pending = this.pending.get(message.request_id);
      if (pending) {
        window.clearTimeout(pending.timer);
        this.pending.delete(message.request_id);
        if (message.type === "error" || message.error) {
          pending.reject(new Error(String(message.error ?? message.message ?? "WebSocket error")));
        } else {
          pending.resolve(message);
        }
      }
    }
    const subscriptionId = typeof message.id === "string" ? message.id : "";
    if (!subscriptionId) {
      return;
    }
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }
    for (const listener of subscription.listeners) {
      listener(message);
    }
  }

  private rejectPending(error: Error): void {
    for (const [requestId, pending] of this.pending) {
      window.clearTimeout(pending.timer);
      pending.reject(error);
      this.pending.delete(requestId);
    }
  }

  private removeSubscriptionListener(
    subscriptionId: string,
    listener: (message: unknown) => void,
    statusListener: (status: ChanOverlayTransportStatus) => void,
    notifyServer: boolean,
  ): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }
    subscription.listeners.delete(listener);
    subscription.statusListeners.delete(statusListener);
    if (subscription.listeners.size > 0) {
      return;
    }
    this.subscriptions.delete(subscriptionId);
    if (!notifyServer || this.socket?.readyState !== WebSocket.OPEN) {
      return;
    }
    this.socket.send(
      JSON.stringify({
        type: subscription.unsubscribeType,
        id: subscriptionId,
      }),
    );
  }

  private replaySubscriptions(socket: WebSocket): void {
    for (const subscription of this.subscriptions.values()) {
      socket.send(JSON.stringify(subscription.payload));
      for (const listener of subscription.statusListeners) listener("replayed");
    }
  }

  private notifyTransport(status: ChanOverlayTransportStatus): void {
    for (const subscription of this.subscriptions.values()) {
      for (const listener of subscription.statusListeners) listener(status);
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null || this.subscriptions.size === 0) {
      return;
    }
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      if (this.socket || this.connectPromise || this.subscriptions.size === 0) {
        return;
      }
      void this.connect().catch(() => {
        this.scheduleReconnect();
      });
    }, 1_000);
  }
}

class RealtimeBarSocketClient {
  private socket: WebSocket | null = null;
  private connectPromise: Promise<WebSocket> | null = null;
  private subscriptions = new Map<
    string,
    {
      symbol: string;
      timeframe: string;
      listeners: Set<(message: RealtimeBarReply) => void>;
      sessionListeners: Set<(generation: number) => void>;
    }
  >();
  private sidebarSubscriptions = new Map<string, {
    context: RealtimeSidebarContext;
    listeners: Set<(message: unknown) => void>;
  }>();
  private reconnectTimer: number | null = null;
  private connectionGeneration = 0;
  private activeGeneration = 0;

  async subscribe(
    subscriptionId: string,
    symbol: string,
    timeframe: string,
    listener: (message: RealtimeBarReply) => void,
    sessionListener: (generation: number) => void,
  ): Promise<() => void> {
    const normalizedSymbol = symbol.toUpperCase();
    const existing = this.subscriptions.get(subscriptionId);
    if (existing) {
      existing.symbol = normalizedSymbol;
      existing.timeframe = timeframe;
      existing.listeners.add(listener);
      existing.sessionListeners.add(sessionListener);
    } else {
      this.subscriptions.set(subscriptionId, {
        symbol: normalizedSymbol,
        timeframe,
        listeners: new Set([listener]),
        sessionListeners: new Set([sessionListener]),
      });
    }
    try {
      const socket = await this.connect();
      this.sendSubscribe(socket, subscriptionId, normalizedSymbol, timeframe);
      if (this.activeGeneration > 0) sessionListener(this.activeGeneration);
    } catch (error) {
      this.removeSubscriptionListener(subscriptionId, listener, sessionListener, false);
      throw error;
    }
    return () => {
      this.removeSubscriptionListener(subscriptionId, listener, sessionListener, true);
    };
  }

  async subscribeSidebar(
    context: RealtimeSidebarContext,
    listener: (message: unknown) => void,
  ): Promise<() => void> {
    const existing = this.sidebarSubscriptions.get(context.subscriptionId);
    if (existing) {
      existing.context = context;
      existing.listeners.add(listener);
    } else {
      this.sidebarSubscriptions.set(context.subscriptionId, { context, listeners: new Set([listener]) });
    }
    try {
      const socket = await this.connect();
      this.sendSidebarContext(socket, context);
    } catch (error) {
      this.removeSidebarListener(context.subscriptionId, listener, false);
      throw error;
    }
    return () => this.removeSidebarListener(context.subscriptionId, listener, true);
  }

  updateSidebarContext(context: RealtimeSidebarContext): void {
    const subscription = this.sidebarSubscriptions.get(context.subscriptionId);
    if (!subscription) return;
    subscription.context = context;
    if (this.socket?.readyState === WebSocket.OPEN) this.sendSidebarContext(this.socket, context);
  }

  private async connect(): Promise<WebSocket> {
    if (this.socket?.readyState === WebSocket.OPEN) {
      return this.socket;
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }
    this.connectPromise = new Promise((resolve, reject) => {
      const socket = createRealtimeSocket();
      let socketGeneration = 0;
      const timer = window.setTimeout(() => {
        socket.close();
        reject(new Error("Realtime bar transport connect timeout"));
      }, 4_000);
      socket.onopen = () => {
        window.clearTimeout(timer);
        this.socket = socket;
        socketGeneration = ++this.connectionGeneration;
        this.activeGeneration = socketGeneration;
        this.connectPromise = null;
        if (this.reconnectTimer !== null) {
          window.clearTimeout(this.reconnectTimer);
          this.reconnectTimer = null;
        }
        this.notifySessionGeneration(socketGeneration);
        if (socketGeneration > 1) this.replaySubscriptions(socket);
        resolve(socket);
      };
      socket.onerror = () => {
        window.clearTimeout(timer);
        this.connectPromise = null;
        reject(new Error("Realtime bar transport failed"));
      };
      socket.onclose = () => {
        window.clearTimeout(timer);
        if (this.socket === socket) {
          this.socket = null;
          this.connectPromise = null;
          this.scheduleReconnect();
        }
      };
      socket.onmessage = (event) => {
        this.handleMessage(event.data, socketGeneration);
      };
    });
    return this.connectPromise;
  }

  private handleMessage(data: unknown, sessionGeneration: number): void {
    let message: RealtimeBarReply & { subscription_id?: string; sequence?: number };
    try {
      message = JSON.parse(String(data)) as RealtimeBarReply;
    } catch {
      return;
    }
    if (message.subscription_id) {
      const subscription = this.sidebarSubscriptions.get(message.subscription_id);
      subscription?.listeners.forEach((listener) => listener(message));
      return;
    }
    if (message.type !== "bar_update" || !message.bar) {
      return;
    }
    message.sessionGeneration = sessionGeneration;
    const symbol = String(message.symbol ?? "").toUpperCase();
    const timeframe = String(message.timeframe ?? "");
    for (const subscription of this.subscriptions.values()) {
      if (subscription.symbol !== symbol || subscription.timeframe !== timeframe) {
        continue;
      }
      for (const listener of subscription.listeners) {
        listener(message);
      }
    }
  }

  private removeSidebarListener(subscriptionId: string, listener: (message: unknown) => void, notifyServer: boolean): void {
    const subscription = this.sidebarSubscriptions.get(subscriptionId);
    if (!subscription) return;
    subscription.listeners.delete(listener);
    if (subscription.listeners.size > 0) return;
    this.sidebarSubscriptions.delete(subscriptionId);
    if (notifyServer && this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify({ type: "unsubscribe", id: subscriptionId }));
    }
  }

  private removeSubscriptionListener(
    subscriptionId: string,
    listener: (message: RealtimeBarReply) => void,
    sessionListener: (generation: number) => void,
    notifyServer: boolean,
  ): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }
    subscription.listeners.delete(listener);
    subscription.sessionListeners.delete(sessionListener);
    if (subscription.listeners.size > 0 || subscription.sessionListeners.size > 0) {
      return;
    }
    this.subscriptions.delete(subscriptionId);
    if (!notifyServer || this.socket?.readyState !== WebSocket.OPEN) {
      return;
    }
    this.socket.send(
      JSON.stringify({
        type: "unsubscribe",
        id: subscriptionId,
      }),
    );
  }

  private replaySubscriptions(socket: WebSocket): void {
    for (const [subscriptionId, subscription] of this.subscriptions) {
      this.sendSubscribe(socket, subscriptionId, subscription.symbol, subscription.timeframe);
    }
    for (const subscription of this.sidebarSubscriptions.values()) {
      this.sendSidebarContext(socket, subscription.context);
    }
  }

  private sendSidebarContext(socket: WebSocket, context: RealtimeSidebarContext): void {
    socket.send(JSON.stringify({
      type: "set_sidebar_context",
      subscription_id: context.subscriptionId,
      chart_symbol: context.chartSymbol,
      chart_epoch: context.chartEpoch,
      watchlist_id: context.watchlistId,
      watchlist_revision: context.watchlistRevision,
      watchlist_symbols: context.watchlistSymbols,
      channels: context.channels,
      after_sequence: context.afterSequence,
      snapshot_version: context.snapshotVersion,
    }));
  }

  private notifySessionGeneration(generation: number): void {
    for (const subscription of this.subscriptions.values()) {
      for (const listener of subscription.sessionListeners) listener(generation);
    }
  }

  private sendSubscribe(
    socket: WebSocket,
    subscriptionId: string,
    symbol: string,
    timeframe: string,
  ): void {
    socket.send(
      JSON.stringify({
        type: "subscribe",
        id: subscriptionId,
        symbol,
        timeframes: [timeframe],
      }),
    );
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null || (this.subscriptions.size === 0 && this.sidebarSubscriptions.size === 0)) {
      return;
    }
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      if (this.socket || this.connectPromise || (this.subscriptions.size === 0 && this.sidebarSubscriptions.size === 0)) {
        return;
      }
      void this.connect().catch(() => {
        this.scheduleReconnect();
      });
    }, 1_000);
  }
}

export class ChartDataManager {
  private barsCache = new Map<string, CacheRecord<BarsResponse>>();
  private chanCache = new Map<string, CacheRecord<ChanOverlayResponse>>();
  private windowCache = new Map<string, CacheRecord<ChartWindowResponse>>();
  private latestSnapshotVersions = new Map<string, string>();
  private latestRealtimeVersions = new Map<string, string>();
  private pendingBars = new Map<string, PendingBarsRequest>();
  private barsHistory = new Map<string, BarsHistory>();
  private realtimeBarStates = new Map<string, Map<number, RealtimeBarState>>();
  private realtimeSessionGenerations = new Map<string, number>();
  private pendingChan = new Map<string, Promise<ChanOverlayResponse>>();
  private pendingWindow = new Map<string, Promise<ChartWindowResponse>>();
  private historyListeners = new Set<(event: ChartHistoryWindowEvent) => void>();
  private sessionHistoryWindows = new Map<string, ChartHistoryWindowEvent>();
  private sessionChanOverlays = new Map<string, ChanOverlayResponse>();
  private snapshotListeners = new Set<(event: SnapshotUpdateEvent) => void>();
  private wsClient = new ChartWebSocketClient();
  private realtimeClient = new RealtimeBarSocketClient();
  private wsDisabledUntil = 0;

  get transportMode(): string {
    return CHART_DATA_TRANSPORT;
  }

  subscribeHistoryWindows(
    listener: (event: ChartHistoryWindowEvent) => void,
  ): () => void {
    this.historyListeners.add(listener);
    return () => {
      this.historyListeners.delete(listener);
    };
  }

  subscribeSnapshotUpdates(
    listener: (event: SnapshotUpdateEvent) => void,
  ): () => void {
    this.snapshotListeners.add(listener);
    return () => {
      this.snapshotListeners.delete(listener);
    };
  }

  publishHistoryWindow(event: ChartHistoryWindowEvent): void {
    const mergedEvent = this.mergeSessionHistoryWindow(event);
    patchTvDebug("datafeed", {
      lastHistoryWindow: {
        source: mergedEvent.source,
        symbol: mergedEvent.symbol,
        timeframe: mergedEvent.timeframe,
        from: mergedEvent.from,
        to: mergedEvent.to,
        limit: mergedEvent.limit,
        count: mergedEvent.bars.length,
      },
      sessionHistoryWindow: {
        symbol: mergedEvent.symbol,
        timeframe: mergedEvent.timeframe,
        from: mergedEvent.from,
        to: mergedEvent.to,
        count: mergedEvent.bars.length,
        max: SESSION_HISTORY_MAX_BARS,
      },
    });
    for (const listener of this.historyListeners) {
      listener(mergedEvent);
    }
  }

  getSessionBarRange(symbol: string, timeframe: string): { from?: number; to?: number; count: number } | null {
    const event = this.sessionHistoryWindows.get(sessionHistoryKey(symbol, timeframe));
    if (!event) {
      return null;
    }
    return {
      from: event.from,
      to: event.to,
      count: event.bars.length,
    };
  }

  private mergeSessionHistoryWindow(event: ChartHistoryWindowEvent): ChartHistoryWindowEvent {
    if (event.bars.length === 0) {
      return event;
    }
    const key = sessionHistoryKey(event.symbol, event.timeframe);
    const previous = this.sessionHistoryWindows.get(key);
    const incomingFirst = event.bars[0]?.time ?? event.from;
    const incomingLast = event.bars[event.bars.length - 1]?.time ?? event.to;
    const keepOlderSide =
      previous !== undefined &&
      incomingLast !== undefined &&
      previous.to !== undefined &&
      incomingLast < previous.to;

    const byTime = new Map<number, ApiBar>();
    for (const bar of previous?.bars ?? []) {
      byTime.set(bar.time, bar);
    }
    for (const bar of event.bars) {
      byTime.set(bar.time, bar);
    }

    let bars = Array.from(byTime.values()).sort((left, right) => left.time - right.time);
    if (bars.length > SESSION_HISTORY_MAX_BARS) {
      bars = keepOlderSide
        ? bars.slice(0, SESSION_HISTORY_MAX_BARS)
        : bars.slice(-SESSION_HISTORY_MAX_BARS);
    }

    const first = bars[0]?.time;
    const last = bars[bars.length - 1]?.time;
    const merged: ChartHistoryWindowEvent = {
      ...event,
      from: first,
      to: last,
      first,
      last,
      limit: Math.max(event.limit, bars.length),
      bars,
    };
    this.sessionHistoryWindows.set(key, merged);
    recordTvDebug("chartData.historyWindow.merge", {
      symbol: event.symbol,
      timeframe: event.timeframe,
      incomingCount: event.bars.length,
      incomingFirst,
      incomingLast,
      previousCount: previous?.bars.length ?? 0,
      mergedCount: bars.length,
      keepOlderSide,
    });
    return merged;
  }

  private mergeSessionChartWindow(response: ChartWindowResponse): ChartWindowResponse {
    const chan = this.mergeSessionChanOverlay(
      response.symbol,
      response.chart_timeframe,
      response.chan,
    );
    patchTvDebug("overlay", {
      sessionChan: {
        symbol: response.symbol,
        timeframe: response.chart_timeframe,
        strokes: chan.strokes.length,
        segments: chan.segments.length,
        centers: chan.centers.length,
        signals: chan.signals.length,
      },
    });
    return {
      ...response,
      chan,
    };
  }

  private mergeSessionChanOverlay(
    symbol: string,
    timeframe: string,
    incoming: ChanOverlayResponse,
  ): ChanOverlayResponse {
    const key = sessionHistoryKey(symbol, timeframe);
    const previous = this.sessionChanOverlays.get(key);
    const merged = previous
      ? scopeChanOverlay(
          mergeChanOverlay(previous, incoming),
          incoming.levels,
          incoming.modes,
        )
      : incoming;
    this.sessionChanOverlays.set(key, merged);
    return merged;
  }

  handleRealtimeBarUpdate(event: {
    symbol: string;
    timeframe: string;
    snapshotVersion?: string;
    seq?: number;
    sessionGeneration?: number;
    bar?: {
      time?: number;
      open?: number;
      high?: number;
      low?: number;
      close?: number;
      volume?: number;
      amount?: number | null;
      revision?: number;
      complete?: boolean;
    };
  }): boolean {
    const symbol = event.symbol.toUpperCase();
    const snapshotVersion =
      event.snapshotVersion ||
      createRealtimeSnapshotVersion(symbol, event.timeframe, event.bar);
    if (!snapshotVersion) {
      return false;
    }
    if (!this.upsertRealtimeBar(
      symbol,
      event.timeframe,
      event.bar,
      event.seq,
      event.sessionGeneration,
    )) {
      return false;
    }
    const key = snapshotVersionKey(symbol, event.timeframe);
    const previous = this.latestRealtimeVersions.get(key);
    if (previous === snapshotVersion) {
      return true;
    }
    this.latestRealtimeVersions.set(key, snapshotVersion);
    this.invalidateSymbolScopedCaches(symbol);
    this.notifySnapshotUpdate({
      source: "realtime",
      symbol,
      timeframe: event.timeframe,
      snapshotVersion,
    });
    return true;
  }

  beginRealtimeSession(symbol: string, timeframe: string, generation: number): boolean {
    const normalizedGeneration = Math.max(0, Math.trunc(generation));
    const key = sessionHistoryKey(symbol, timeframe);
    const current = this.realtimeSessionGenerations.get(key);
    if (current !== undefined && normalizedGeneration < current) return false;
    if (current === undefined || normalizedGeneration > current) {
      this.realtimeSessionGenerations.set(key, normalizedGeneration);
    }
    return true;
  }

  async subscribeChanOverlay(
    request: ChartWindowRequest,
    listener: (message: unknown) => void,
    statusListener: (status: ChanOverlayTransportStatus) => void,
  ): Promise<() => void> {
    if (request.from === undefined || request.to === undefined) {
      throw new Error("Chan realtime subscription requires a bounded from/to range");
    }
    const subscriptionId = createChanSubscriptionId(request);
    return this.wsClient.subscribe(
      subscriptionId,
      {
        type: "subscribe_chan",
        ...requestPayload(request),
      },
      (message) => {
        if (!message || typeof message !== "object") return;
        const type = (message as { type?: unknown }).type;
        if (type === "chan_overlay" || type === "chan_resync_required") {
          listener(message);
        }
      },
      statusListener,
    );
  }

  async subscribeRealtimeBars(
    request: Pick<ChartWindowRequest, "symbol" | "timeframe">,
    listener: (event: {
      symbol: string;
      timeframe: string;
      snapshotVersion?: string;
      seq?: number;
      sessionGeneration?: number;
      bar: ApiBar;
    }) => void,
    sessionListener: (generation: number) => void,
  ): Promise<() => void> {
    const subscriptionId = createRealtimeBarSubscriptionId(request);
    return this.realtimeClient.subscribe(
      subscriptionId,
      request.symbol,
      request.timeframe,
      (message) => {
        if (!isApiBar(message.bar)) {
          return;
        }
        const symbol = String(message.symbol ?? request.symbol).toUpperCase();
        const timeframe = String(message.timeframe ?? request.timeframe);
        listener({
          symbol,
          timeframe,
          snapshotVersion: message.snapshot_version,
          seq: message.seq,
          sessionGeneration: message.sessionGeneration,
          bar: message.bar,
        });
      },
      sessionListener,
    );
  }

  subscribeRealtimeSidebar(
    context: RealtimeSidebarContext,
    listener: (event: unknown) => void,
  ): Promise<() => void> {
    return this.realtimeClient.subscribeSidebar(context, listener);
  }

  updateRealtimeSidebarContext(context: RealtimeSidebarContext): void {
    this.realtimeClient.updateSidebarContext(context);
  }

  async getBars(request: ChartWindowRequest): Promise<ChartBarsResponse> {
    return this.getBarsInternal(request, false);
  }

  private async getBarsInternal(
    request: ChartWindowRequest,
    requireRequestedCoverage: boolean,
  ): Promise<ChartBarsResponse> {
    if (request.signal?.aborted) {
      throw new DOMException("Request aborted", "AbortError");
    }
    const normalized = { ...request, symbol: request.symbol.toUpperCase() };
    const historyKey = sessionHistoryKey(normalized.symbol, normalized.timeframe);
    const cached = this.readHistory(historyKey, normalized, requireRequestedCoverage);
    if (cached) {
      recordTvDebug("chartData.bars.cache", { key: historyKey, count: cached.bars.length });
      return cached;
    }

    const plannedRequest = this.planBarsFetch(historyKey, normalized);
    const networkRequest = {
      ...plannedRequest,
      limit: Math.min(plannedRequest.limit, BARS_HTTP_MAX_LIMIT),
    };
    const pending = this.findPendingBars(networkRequest);
    if (pending) {
      recordTvDebug("chartData.bars.coalesced", { key: historyKey });
      await this.consumePendingBars(pending, normalized, normalized.signal);
      return this.getBarsInternal(normalized, true);
    }
    const overlapping = this.findOverlappingPendingBars(networkRequest);
    if (overlapping.length > 0) {
      return this.waitForPendingCoverage(overlapping, normalized);
    }

    const controller = new AbortController();
    const loadRequest = { ...networkRequest, signal: controller.signal };
    let pendingRequest!: PendingBarsRequest;
    pendingRequest = {
      request: networkRequest,
      controller,
      consumers: 0,
      complete: false,
      promise: this.loadBars(loadRequest)
        .then((response) => {
          this.mergeBarsHistory(historyKey, networkRequest, response.bars);
          return response;
        })
        .finally(() => {
          pendingRequest.complete = true;
          const key = requestKey("bars", networkRequest);
          if (this.pendingBars.get(key) === pendingRequest) {
            this.pendingBars.delete(key);
          }
        }),
    };
    this.pendingBars.set(requestKey("bars", networkRequest), pendingRequest);
    await this.consumePendingBars(pendingRequest, normalized, normalized.signal);
    const loaded = this.responseFromHistory(normalized, []);
    if (!requireRequestedCoverage && loaded.bars.length >= normalized.limit) {
      return loaded;
    }
    return this.getBarsInternal(normalized, requireRequestedCoverage);
  }

  private planBarsFetch(key: string, request: ChartWindowRequest): ChartWindowRequest {
    const history = this.barsHistory.get(key);
    if (!history) {
      return request;
    }
    const uncovered = firstUncoveredRange(history.covered, request.from, request.to);
    if (uncovered) return { ...request, ...uncovered };
    const selected = selectBars(history.bars, request);
    const deficit = request.limit - selected.length;
    const earliest = selected[0]?.time;
    return deficit > 0 && earliest !== undefined
      ? { ...request, from: undefined, to: earliest, limit: deficit }
      : request;
  }

  private findPendingBars(request: ChartWindowRequest): PendingBarsRequest | null {
    for (const pending of this.pendingBars.values()) {
      if (
        !pending.complete &&
        !pending.controller.signal.aborted &&
        pending.request.symbol === request.symbol &&
        pending.request.timeframe === request.timeframe &&
        rangeContains(pending.request.from, pending.request.to, request.from, request.to) &&
        pending.request.limit >= request.limit
      ) {
        return pending;
      }
    }
    return null;
  }

  private findOverlappingPendingBars(request: ChartWindowRequest): PendingBarsRequest[] {
    return [...this.pendingBars.values()].filter(
      (pending) =>
        !pending.complete &&
        !pending.controller.signal.aborted &&
        pending.request.symbol === request.symbol &&
        pending.request.timeframe === request.timeframe &&
        rangesOverlap(pending.request.from, pending.request.to, request.from, request.to),
    );
  }

  private async waitForPendingCoverage(
    pending: PendingBarsRequest[],
    request: ChartWindowRequest,
  ): Promise<ChartBarsResponse> {
    await Promise.all(
      pending.map((item) => this.consumePendingBars(item, request, request.signal)),
    );
    return this.getBarsInternal(request, true);
  }

  private consumePendingBars(
    pending: PendingBarsRequest,
    consumerRequest: ChartWindowRequest,
    signal?: AbortSignal,
  ): Promise<ChartBarsResponse> {
    pending.consumers += 1;
    return new Promise((resolve, reject) => {
      let settled = false;
      const release = () => {
        if (settled) return;
        settled = true;
        pending.consumers -= 1;
        signal?.removeEventListener("abort", abort);
        if (pending.consumers === 0 && !pending.complete) pending.controller.abort();
      };
      const abort = () => {
        release();
        reject(new DOMException("Request aborted", "AbortError"));
      };
      if (signal?.aborted) {
        abort();
        return;
      }
      signal?.addEventListener("abort", abort, { once: true });
      pending.promise.then(
        (response) => {
          if (settled) return;
          release();
          resolve(this.responseFromHistory(consumerRequest, response.bars));
        },
        (error) => {
          if (settled) return;
          release();
          reject(error);
        },
      );
    });
  }

  async getChanOverlay(request: ChartWindowRequest): Promise<ChanOverlayResponse> {
    const key = requestKey("chan", request, this.snapshotHint(request));
    const cached = this.readCache(this.chanCache, key);
    if (cached) {
      recordTvDebug("chartData.chan.cache", { key });
      return cached;
    }
    const pending = this.pendingChan.get(key);
    if (pending) {
      return withAbort(pending, request.signal);
    }
    const promise = this.loadChan(request)
      .then((response) => {
        this.rememberSnapshotVersion(request, response.snapshot_version);
        this.writeCache(
          this.chanCache,
          requestKey("chan", request, response.snapshot_version),
          response,
        );
        return response;
      })
      .finally(() => {
        this.pendingChan.delete(key);
      });
    this.pendingChan.set(key, promise);
    return withAbort(promise, request.signal);
  }

  async getChartWindow(request: ChartWindowRequest): Promise<ChartWindowResponse> {
    const key = requestKey("window", request, this.snapshotHint(request));
    const cached = this.readCache(this.windowCache, key);
    if (cached) {
      recordTvDebug("chartData.window.cache", { key });
      return cached;
    }
    const pending = this.pendingWindow.get(key);
    if (pending) {
      return withAbort(pending, request.signal);
    }
    const promise = this.loadChartWindow(request)
      .then((response) => {
        const mergedResponse = this.mergeSessionChartWindow(response);
        this.rememberSnapshotVersion(request, mergedResponse.chan.snapshot_version);
        this.hydrateBundleCaches(request, mergedResponse);
        return mergedResponse;
      })
      .finally(() => {
        this.pendingWindow.delete(key);
      });
    this.pendingWindow.set(key, promise);
    return withAbort(promise, request.signal);
  }

  async refreshChartWindow(request: ChartWindowRequest): Promise<ChartWindowResponse> {
    const response = await this.loadChartWindow(request);
    const mergedResponse = this.mergeSessionChartWindow(response);
    this.rememberSnapshotVersion(request, mergedResponse.chan.snapshot_version);
    this.hydrateBundleCaches(request, mergedResponse);
    return mergedResponse;
  }

  private async loadBars(request: ChartWindowRequest): Promise<BarsResponse> {
    return getBarsHttp(
      request.symbol,
      request.timeframe,
      request.limit,
      request.from,
      request.to,
      request.signal,
    );
  }

  private readHistory(
    key: string,
    request: ChartWindowRequest,
    requireRequestedCoverage: boolean,
  ): ChartBarsResponse | null {
    const history = this.barsHistory.get(key);
    if (!history) {
      return null;
    }
    const bars = selectBars(history.bars, request);
    const requestedRangeCovered = rangeCovered(history.covered, request.from, request.to);
    if (requireRequestedCoverage && !requestedRangeCovered) {
      return null;
    }
    if (
      bars.length < request.limit &&
      !(bars.length === 0 && requestedRangeCovered) &&
      !isHistoricalExhausted(history, request)
    ) {
      return null;
    }
    history.touchedAt = Date.now();
    return {
      symbol: request.symbol,
      timeframe: request.timeframe,
      bars,
      noData: bars.length === 0 && isHistoricalExhausted(history, request),
    };
  }

  private mergeBarsHistory(
    key: string,
    request: ChartWindowRequest,
    incoming: ApiBar[],
  ): void {
    const previous = this.barsHistory.get(key);
    const responseBars = normalizeBars(incoming).filter(
      (bar) =>
        (request.from === undefined || bar.time >= request.from) &&
        (request.to === undefined || bar.time < request.to),
    );
    const byTime = new Map<number, ApiBar>();
    for (const bar of previous?.bars ?? []) byTime.set(bar.time, bar);
    // A later response is authoritative for OHLCV revisions at the same timestamp.
    const realtimeStates = this.realtimeBarStates.get(key);
    for (const bar of responseBars) {
      const existing = byTime.get(bar.time);
      if (
        existing &&
        realtimeStates?.has(bar.time) &&
        bar.revision <= existing.revision
      ) {
        continue;
      }
      byTime.set(bar.time, bar);
      if (existing && bar.revision > existing.revision) realtimeStates?.delete(bar.time);
    }
    const allBars = Array.from(byTime.values()).sort((left, right) => left.time - right.time);
    const trimmed = allBars.length > BARS_HISTORY_MAX_BARS;
    const incomingLast = responseBars[responseBars.length - 1]?.time;
    const previousFirst = previous?.bars[0]?.time;
    const keepOlderSide =
      trimmed &&
      incomingLast !== undefined &&
      previousFirst !== undefined &&
      incomingLast < previousFirst;
    const bars = trimmed
      ? keepOlderSide
        ? allBars.slice(0, BARS_HISTORY_MAX_BARS)
        : allBars.slice(-BARS_HISTORY_MAX_BARS)
      : allBars;
    if (realtimeStates) {
      const retainedTimes = new Set(bars.map((bar) => bar.time));
      for (const time of realtimeStates.keys()) {
        if (!retainedTimes.has(time)) realtimeStates.delete(time);
      }
      if (realtimeStates.size === 0) this.realtimeBarStates.delete(key);
    }
    const truncated = responseBars.length >= request.limit;
    const earliest = responseBars[0]?.time;
    const provenInterval = truncated && earliest !== undefined
      ? { from: earliest, to: request.to }
      : { from: request.from, to: request.to };
    const exhaustedBefore =
      !truncated && (request.from === undefined || request.from <= 0)
        ? request.to ?? Number.POSITIVE_INFINITY
        : previous?.exhaustedBefore;
    const covered = mergeCoveredIntervals([
      ...(previous?.covered ?? []),
      provenInterval,
    ]);
    this.barsHistory.set(key, {
      bars,
      covered: trimmed ? clipCoverageToRetainedBars(covered, bars) : covered,
      exhaustedBefore: trimmed ? undefined : exhaustedBefore,
      touchedAt: Date.now(),
    });
    this.evictBarsHistory();
    recordTvDebug("chartData.bars.network", {
      symbol: request.symbol,
      timeframe: request.timeframe,
      count: incoming.length,
    });
  }

  private upsertRealtimeBar(
    symbol: string,
    timeframe: string,
    bar: {
      time?: number;
      open?: number;
      high?: number;
      low?: number;
      close?: number;
      volume?: number;
      amount?: number | null;
      revision?: number;
      complete?: boolean;
    } | undefined,
    seq?: number,
    sessionGeneration?: number,
  ): boolean {
    if (
      !bar ||
      typeof bar.time !== "number" ||
      typeof bar.open !== "number" ||
      typeof bar.high !== "number" ||
      typeof bar.low !== "number" ||
      typeof bar.close !== "number" ||
      typeof bar.volume !== "number"
    ) {
      return false;
    }
    const key = sessionHistoryKey(symbol, timeframe);
    const generation = Math.max(0, Math.trunc(sessionGeneration ?? 0));
    const currentGeneration = this.realtimeSessionGenerations.get(key);
    if (currentGeneration !== undefined && generation < currentGeneration) {
      return false;
    }
    if (currentGeneration === undefined || generation > currentGeneration) {
      this.realtimeSessionGenerations.set(key, generation);
    }
    const history = this.barsHistory.get(key);
    const existing = history?.bars.find((item) => item.time === bar.time);
    const revised: ApiBar = {
      time: bar.time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      volume: bar.volume,
      amount: bar.amount ?? existing?.amount ?? null,
      revision: bar.revision ?? existing?.revision ?? 0,
      complete: bar.complete ?? existing?.complete ?? false,
    };
    const states = this.realtimeBarStates.get(key) ?? new Map<number, RealtimeBarState>();
    const previousState = states.get(revised.time);
    const signature = barSignature(revised);
    const sameSession = previousState?.sessionGeneration === generation;
    const knownRevision = Math.max(
      existing?.revision ?? Number.NEGATIVE_INFINITY,
      previousState?.revision ?? Number.NEGATIVE_INFINITY,
    );
    if (revised.revision < knownRevision) {
      return false;
    }
    if (previousState && previousState.sessionGeneration > generation) {
      return false;
    }
    if (previousState && revised.revision === knownRevision) {
      if (!sameSession) {
        if (signature === previousState.signature) {
          states.set(revised.time, {
            sessionGeneration: generation,
            revision: revised.revision,
            seq,
            signature,
          });
          return false;
        }
      } else {
        if (signature === previousState.signature) {
          if (seq !== undefined && (previousState.seq === undefined || seq > previousState.seq)) {
            previousState.seq = seq;
          }
          return false;
        }
        if (previousState.seq !== undefined) {
          if (seq === undefined || seq <= previousState.seq) return false;
        }
      }
    }
    const byTime = new Map((history?.bars ?? []).map((item) => [item.time, item]));
    byTime.set(revised.time, revised);
    const allBars = [...byTime.values()].sort((left, right) => left.time - right.time);
    const trimmed = allBars.length > BARS_HISTORY_MAX_BARS;
    const bars = allBars.slice(-BARS_HISTORY_MAX_BARS);
    this.barsHistory.set(key, {
      bars,
      covered: trimmed
        ? clipCoverageToRetainedBars(history?.covered ?? [], bars)
        : history?.covered ?? [],
      exhaustedBefore: trimmed ? undefined : history?.exhaustedBefore,
      touchedAt: Date.now(),
    });
    states.set(revised.time, {
      sessionGeneration: generation,
      revision: revised.revision,
      seq,
      signature,
    });
    const retainedTimes = new Set(bars.map((item) => item.time));
    for (const time of states.keys()) {
      if (!retainedTimes.has(time)) states.delete(time);
    }
    this.realtimeBarStates.set(key, states);
    this.evictBarsHistory();
    return true;
  }

  private responseFromHistory(
    request: ChartWindowRequest,
    fallbackBars: ApiBar[],
  ): ChartBarsResponse {
    const history = this.barsHistory.get(sessionHistoryKey(request.symbol, request.timeframe));
    const bars = history ? selectBars(history.bars, request) : selectBars(fallbackBars, request);
    return {
      symbol: request.symbol,
      timeframe: request.timeframe,
      bars,
      noData: bars.length === 0 && Boolean(history && isHistoricalExhausted(history, request)),
    };
  }

  private evictBarsHistory(): void {
    if (this.barsHistory.size <= BARS_HISTORY_MAX_SERIES) return;
    const oldest = [...this.barsHistory.entries()]
      .sort(([, left], [, right]) => left.touchedAt - right.touchedAt)
      .slice(0, this.barsHistory.size - BARS_HISTORY_MAX_SERIES);
    for (const [key] of oldest) {
      this.barsHistory.delete(key);
      this.realtimeBarStates.delete(key);
      this.realtimeSessionGenerations.delete(key);
    }
  }

  private async loadChan(request: ChartWindowRequest): Promise<ChanOverlayResponse> {
    const response = await getChanOverlayHttp(
      request.symbol,
      request.timeframe,
      request.limit,
      request.from,
      request.to,
      request.signal,
      normalizedChanLevels(request),
      normalizedChanModes(request),
    );
    const merged = this.mergeSessionChanOverlay(request.symbol, request.timeframe, response);
    return merged;
  }

  private async loadChartWindow(request: ChartWindowRequest): Promise<ChartWindowResponse> {
    if (this.shouldTryWebSocket()) {
      try {
        const response = await this.loadChartWindowViaWebSocket(request);
        recordTvDebug("chartData.window.websocket", { symbol: request.symbol });
        return response;
      } catch (error) {
        this.markWebSocketFailure(error);
        if (CHART_DATA_TRANSPORT === "websocket") {
          throw error;
        }
      }
    }
    const response = await getChartBundleHttp(
      request.symbol,
      request.timeframe,
      request.limit,
      request.from,
      request.to,
      request.signal,
      normalizedChanLevels(request),
      normalizedChanModes(request),
    );
    return {
      ...response,
      transport: "http",
    };
  }

  private async loadChartWindowViaWebSocket(
    request: ChartWindowRequest,
  ): Promise<ChartWindowResponse> {
    const response = await this.wsClient.request<{
      bundle?: unknown;
      window?: unknown;
      bars?: ApiBar[];
      chan?: unknown;
      snapshot_id?: string;
    }>(
      {
        type: "get_chart_bundle",
        ...requestPayload(request),
      },
      request.signal,
    );
    const bundle = response.bundle
      ? normalizeChartBundleForFrontend(response.bundle)
      : null;
    if (bundle) {
      return {
        ...bundle,
        transport: "websocket",
      };
    }
    const windowBundle = response.window
      ? normalizeChartBundleForFrontend(response.window)
      : null;
    if (windowBundle) {
      return {
        ...windowBundle,
        transport: "websocket",
      };
    }
    if (!Array.isArray(response.bars) || !response.chan) {
      throw new Error("Invalid WebSocket chart window response");
    }
    const fallbackBundle = normalizeChartBundleForFrontend({
      schema_version: "frontend-chart-bundle.v2",
      snapshot_id: String(response.snapshot_id ?? ""),
      symbol: request.symbol,
      chart_timeframe: request.timeframe,
      range: {
        from: request.from,
        to: request.to,
        limit: request.limit,
      },
      bars: response.bars,
      chan: response.chan,
    });
    if (!fallbackBundle) {
      throw new Error("Invalid WebSocket chart window response");
    }
    return {
      ...fallbackBundle,
      transport: "websocket",
    };
  }

  private shouldTryWebSocket(): boolean {
    if (CHART_DATA_TRANSPORT === "http") {
      return false;
    }
    if (CHART_DATA_TRANSPORT === "websocket") {
      return true;
    }
    return Date.now() >= this.wsDisabledUntil;
  }

  private markWebSocketFailure(error: unknown): void {
    this.wsDisabledUntil = Date.now() + WS_RETRY_COOLDOWN_MS;
    recordTvDebug("chartData.websocket.unavailable", String(error));
    patchTvDebug("datafeed", {
      chartDataTransportFallback: "http",
      chartDataTransportError: String(error),
    });
  }

  private readCache<T>(cache: Map<string, CacheRecord<T>>, key: string): T | null {
    const record = cache.get(key);
    if (!record) {
      return null;
    }
    if (Date.now() - record.createdAt > CACHE_TTL_MS) {
      cache.delete(key);
      return null;
    }
    return record.value;
  }

  private writeCache<T>(cache: Map<string, CacheRecord<T>>, key: string, value: T): void {
    cache.set(key, { value, createdAt: Date.now() });
  }

  private hydrateBundleCaches(
    request: ChartWindowRequest,
    response: ChartWindowResponse,
  ): void {
    const snapshotVersion = response.chan.snapshot_version;
    this.writeCache(
      this.windowCache,
      requestKey("window", request, snapshotVersion),
      response,
    );
    this.writeCache(
      this.barsCache,
      requestKey("bars", request, snapshotVersion),
      {
        symbol: response.symbol,
        timeframe: response.chart_timeframe,
        bars: response.bars,
      },
    );
    this.writeCache(
      this.chanCache,
      requestKey("chan", request, snapshotVersion),
      response.chan,
    );
  }

  private snapshotHint(request: Pick<ChartWindowRequest, "symbol" | "timeframe">): string {
    return this.latestSnapshotVersions.get(snapshotVersionKey(request.symbol, request.timeframe)) ?? "";
  }

  private rememberSnapshotVersion(
    request: Pick<ChartWindowRequest, "symbol" | "timeframe">,
    snapshotVersion: string,
  ): void {
    this.notePublishedSnapshotVersion(
      request.symbol,
      request.timeframe,
      snapshotVersion,
      "response",
    );
  }

  private notePublishedSnapshotVersion(
    symbol: string,
    timeframe: string,
    snapshotVersion: string,
    source: SnapshotUpdateEvent["source"],
  ): void {
    if (!this.applyPublishedSnapshotVersion(symbol, timeframe, snapshotVersion)) {
      return;
    }
    this.notifySnapshotUpdate({
      source,
      symbol: symbol.toUpperCase(),
      timeframe,
      snapshotVersion,
    });
  }

  private applyPublishedSnapshotVersion(
    symbol: string,
    timeframe: string,
    snapshotVersion: string,
  ): boolean {
    if (!snapshotVersion) {
      return false;
    }
    const key = snapshotVersionKey(symbol, timeframe);
    const previous = this.latestSnapshotVersions.get(key);
    if (previous === snapshotVersion) {
      return false;
    }
    this.latestSnapshotVersions.set(key, snapshotVersion);
    this.invalidateSnapshotScopedCaches(symbol, timeframe);
    return true;
  }

  private invalidateSnapshotScopedCaches(symbol: string, timeframe: string): void {
    const cachePrefix = snapshotScopedCachePrefix(symbol, timeframe);
    for (const cache of [this.barsCache, this.chanCache, this.windowCache]) {
      for (const key of cache.keys()) {
        if (key.includes(cachePrefix)) {
          cache.delete(key);
        }
      }
    }
  }

  private invalidateSymbolScopedCaches(symbol: string): void {
    const symbolPrefix = `|${symbol.toUpperCase()}|`;
    for (const cache of [this.barsCache, this.chanCache, this.windowCache]) {
      for (const key of cache.keys()) {
        if (key.includes(symbolPrefix)) {
          cache.delete(key);
        }
      }
    }
  }

  private notifySnapshotUpdate(event: SnapshotUpdateEvent): void {
    for (const listener of this.snapshotListeners) {
      listener(event);
    }
  }
}

export const chartDataManager = new ChartDataManager();

function mergeChanOverlay(previous: ChanOverlayResponse, incoming: ChanOverlayResponse): ChanOverlayResponse {
  const strokes = mergeByStableKey(previous.strokes, incoming.strokes, strokeKey)
    .sort((left, right) => chanLineTime(left) - chanLineTime(right))
    .slice(-SESSION_CHAN_MAX_ITEMS);
  const segments = mergeByStableKey(previous.segments, incoming.segments, strokeKey)
    .sort((left, right) => chanLineTime(left) - chanLineTime(right))
    .slice(-SESSION_CHAN_MAX_ITEMS);
  const centers = mergeByStableKey(previous.centers, incoming.centers, centerKey)
    .sort((left, right) => left.start_time - right.start_time)
    .slice(-SESSION_CHAN_MAX_ITEMS);
  const signals = mergeByStableKey(previous.signals, incoming.signals, signalKey)
    .sort((left, right) => left.time - right.time)
    .slice(-SESSION_CHAN_MAX_ITEMS);

  return {
    ...incoming,
    levels: uniqueStrings([...previous.levels, ...incoming.levels]),
    modes: uniqueStrings([...previous.modes, ...incoming.modes]),
    snapshot_version: incoming.snapshot_version || previous.snapshot_version,
    requested_bar_count: Math.max(previous.requested_bar_count, incoming.requested_bar_count),
    bars_by_level: mergeBarsByLevel(previous.bars_by_level, incoming.bars_by_level),
    strokes,
    segments,
    centers,
    signals,
  };
}

function scopeChanOverlay(
  overlay: ChanOverlayResponse,
  levels: readonly string[],
  modes: readonly string[],
): ChanOverlayResponse {
  const levelSet = new Set(levels);
  const modeSet = new Set(modes);
  const keep = (item: { level: string; mode: string }) =>
    levelSet.has(item.level) && modeSet.has(item.mode);
  return {
    ...overlay,
    levels: overlay.levels.filter((level) => levelSet.has(level)),
    modes: overlay.modes.filter((mode) => modeSet.has(mode)),
    bars_by_level: Object.fromEntries(
      levels.map((level) => [level, overlay.bars_by_level[level] ?? 0]),
    ),
    strokes: overlay.strokes.filter(keep),
    segments: overlay.segments.filter(keep),
    centers: overlay.centers.filter(keep),
    signals: overlay.signals.filter(keep),
    channels: overlay.channels.filter(keep),
  };
}

function mergeByStableKey<T>(
  previous: T[],
  incoming: T[],
  keyOf: (item: T) => string,
): T[] {
  const byKey = new Map<string, T>();
  for (const item of previous) {
    byKey.set(keyOf(item), item);
  }
  for (const item of incoming) {
    byKey.set(keyOf(item), item);
  }
  return Array.from(byKey.values());
}

function strokeKey(item: ChanOverlayResponse["strokes"][number]): string {
  return [
    item.level,
    item.mode,
    item.begin_base_ts ?? item.start.time ?? "",
    item.end_base_ts ?? item.end.time ?? "",
    item.begin_base_seq ?? "",
    item.end_base_seq ?? "",
    item.start.price,
    item.end.price,
    item.direction,
    item.confirmed ? 1 : 0,
  ].join("|");
}

function centerKey(item: ChanOverlayResponse["centers"][number]): string {
  return [
    item.level,
    item.mode,
    item.begin_base_ts ?? item.start_time,
    item.end_base_ts ?? item.end_time,
    item.begin_base_seq ?? "",
    item.end_base_seq ?? "",
    item.low,
    item.high,
    item.confirmed ? 1 : 0,
  ].join("|");
}

function signalKey(item: ChanOverlayResponse["signals"][number]): string {
  return [
    item.level,
    item.mode,
    item.base_ts ?? item.time,
    item.base_seq ?? "",
    item.price,
    item.signal_type,
    item.confirmed ? 1 : 0,
  ].join("|");
}

function chanLineTime(item: ChanOverlayResponse["strokes"][number]): number {
  return item.begin_base_ts ?? item.start.time ?? 0;
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values));
}

function mergeBarsByLevel(
  previous: Record<string, number>,
  incoming: Record<string, number>,
): Record<string, number> {
  const merged = { ...previous, ...incoming };
  for (const key of Object.keys(previous)) {
    merged[key] = Math.max(previous[key] ?? 0, incoming[key] ?? 0);
  }
  return merged;
}

function requestPayload(request: ChartWindowRequest): Record<string, unknown> {
  return {
    symbol: request.symbol,
    timeframe: request.timeframe,
    limit: request.limit,
    from: request.from,
    to: request.to,
    levels: normalizedChanLevels(request),
    modes: normalizedChanModes(request),
  };
}

function requestKey(prefix: string, request: ChartWindowRequest, snapshotVersion = ""): string {
  return [
    prefix,
    request.symbol.toUpperCase(),
    request.timeframe,
    snapshotVersion,
    request.limit,
    request.from ?? "",
    request.to ?? "",
    normalizedChanLevels(request).join(","),
    normalizedChanModes(request).join(","),
  ].join("|");
}

function normalizedChanLevels(request: Pick<ChartWindowRequest, "levels">): readonly string[] {
  return request.levels && request.levels.length > 0
    ? request.levels
    : DEFAULT_CHAN_LEVELS;
}

function normalizedChanModes(request: Pick<ChartWindowRequest, "modes">): readonly string[] {
  return request.modes && request.modes.length > 0
    ? request.modes
    : DEFAULT_CHAN_MODES;
}

function withAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) {
    return promise;
  }
  if (signal.aborted) {
    return Promise.reject(new DOMException("Request aborted", "AbortError"));
  }
  return new Promise((resolve, reject) => {
    const abort = () => reject(new DOMException("Request aborted", "AbortError"));
    signal.addEventListener("abort", abort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", abort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", abort);
        reject(error);
      },
    );
  });
}

function createRequestId(): string {
  return `req_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function snapshotVersionKey(symbol: string, timeframe: string): string {
  return `${symbol.toUpperCase()}|${timeframe}`;
}

function sessionHistoryKey(symbol: string, timeframe: string): string {
  return `${symbol.toUpperCase()}|${timeframe}`;
}

function selectBars(bars: ApiBar[], request: ChartWindowRequest): ApiBar[] {
  const inRange = bars.filter(
    (bar) =>
      (request.to === undefined || bar.time < request.to),
  );
  return inRange.slice(-request.limit);
}

function normalizeBars(bars: ApiBar[]): ApiBar[] {
  const byTime = new Map<number, ApiBar>();
  for (const bar of bars) byTime.set(bar.time, bar);
  return [...byTime.values()].sort((left, right) => left.time - right.time);
}

function barSignature(bar: ApiBar): string {
  return [
    bar.time,
    bar.open,
    bar.high,
    bar.low,
    bar.close,
    bar.volume,
    bar.amount ?? "",
    bar.revision,
    bar.complete ? 1 : 0,
  ].join(":");
}

function rangeContains(
  outerFrom: number | undefined,
  outerTo: number | undefined,
  innerFrom: number | undefined,
  innerTo: number | undefined,
): boolean {
  return (
    (outerFrom === undefined || (innerFrom !== undefined && outerFrom <= innerFrom)) &&
    (outerTo === undefined || (innerTo !== undefined && outerTo >= innerTo))
  );
}

function rangeCovered(
  intervals: CoveredInterval[],
  from: number | undefined,
  to: number | undefined,
): boolean {
  return intervals.some((interval) => rangeContains(interval.from, interval.to, from, to));
}

function rangesOverlap(
  leftFrom: number | undefined,
  leftTo: number | undefined,
  rightFrom: number | undefined,
  rightTo: number | undefined,
): boolean {
  if (leftTo !== undefined && rightFrom !== undefined && leftTo <= rightFrom) return false;
  if (rightTo !== undefined && leftFrom !== undefined && rightTo <= leftFrom) return false;
  return true;
}

function firstUncoveredRange(
  intervals: CoveredInterval[],
  from: number | undefined,
  to: number | undefined,
): CoveredInterval | null {
  if (from === undefined || to === undefined) return null;
  let cursor = from;
  for (const interval of intervals
    .filter((item): item is { from: number; to: number } => item.from !== undefined && item.to !== undefined)
    .sort((left, right) => left.from - right.from)) {
    if (interval.to <= cursor) continue;
    if (interval.from > cursor) return { from: cursor, to: Math.min(interval.from, to) };
    cursor = Math.max(cursor, interval.to);
    if (cursor >= to) return null;
  }
  return cursor < to ? { from: cursor, to } : null;
}

function mergeCoveredIntervals(intervals: CoveredInterval[]): CoveredInterval[] {
  const finite = intervals.filter(
    (interval): interval is { from: number; to: number } =>
      interval.from !== undefined && interval.to !== undefined,
  );
  const unbounded = intervals.filter(
    (interval) => interval.from === undefined || interval.to === undefined,
  );
  const merged: CoveredInterval[] = [];
  for (const interval of finite.sort((left, right) => left.from - right.from)) {
    const previous = merged[merged.length - 1];
    if (previous?.to !== undefined && interval.from <= previous.to) {
      previous.to = Math.max(previous.to, interval.to);
    } else {
      merged.push({ ...interval });
    }
  }
  return [...unbounded, ...merged].slice(-64);
}

function clipCoverageToRetainedBars(
  intervals: CoveredInterval[],
  bars: ApiBar[],
): CoveredInterval[] {
  const earliest = bars[0]?.time;
  const latestExclusive = bars[bars.length - 1]?.time;
  if (earliest === undefined || latestExclusive === undefined) return [];
  const retainedTo = latestExclusive + 1;
  return mergeCoveredIntervals(
    intervals
      .filter(
        (interval) =>
          (interval.to === undefined || interval.to > earliest) &&
          (interval.from === undefined || interval.from < retainedTo),
      )
      .map((interval) => ({
        from: interval.from === undefined ? earliest : Math.max(interval.from, earliest),
        to: interval.to === undefined ? retainedTo : Math.min(interval.to, retainedTo),
      })),
  );
}

function isHistoricalExhausted(history: BarsHistory, request: ChartWindowRequest): boolean {
  void request;
  return history.exhaustedBefore !== undefined;
}

function snapshotScopedCachePrefix(symbol: string, timeframe: string): string {
  return `|${symbol.toUpperCase()}|${timeframe}|`;
}

function createRealtimeSnapshotVersion(
  symbol: string,
  timeframe: string,
  bar?: {
    time?: number;
    open?: number;
    high?: number;
    low?: number;
    close?: number;
    volume?: number;
    revision?: number;
    complete?: boolean;
  },
): string {
  const time = Number(bar?.time ?? 0);
  if (!Number.isFinite(time) || time <= 0) {
    return "";
  }
  const revision = Number(bar?.revision ?? 0);
  const complete = bar?.complete ? 1 : 0;
  return [
    "rt",
    symbol.toUpperCase(),
    timeframe,
    Math.trunc(time).toString().padStart(12, "0"),
    stableNumber(bar?.open),
    stableNumber(bar?.high),
    stableNumber(bar?.low),
    stableNumber(bar?.close),
    stableNumber(bar?.volume),
    Math.trunc(revision).toString().padStart(6, "0"),
    complete,
  ].join(":");
}

function stableNumber(value: unknown): string {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number.toString() : "0";
}

function unsubscribeTypeFor(type: string): string {
  return type === "subscribe_chan" ? "unsubscribe_chan" : "unsubscribe_chart_bundle";
}

function createChanSubscriptionId(request: ChartWindowRequest): string {
  return [
    "chan",
    request.symbol.toUpperCase(),
    request.timeframe,
    request.limit,
    request.from ?? "",
    request.to ?? "",
    normalizedChanLevels(request).join(","),
    normalizedChanModes(request).join(","),
  ].join(":");
}

function createRealtimeBarSubscriptionId(
  request: Pick<ChartWindowRequest, "symbol" | "timeframe">,
): string {
  return [
    "bar",
    request.symbol.toUpperCase(),
    request.timeframe,
  ].join(":");
}

function isApiBar(value: unknown): value is ApiBar {
  if (!value || typeof value !== "object") {
    return false;
  }
  const bar = value as Partial<ApiBar>;
  return (
    typeof bar.time === "number" &&
    typeof bar.open === "number" &&
    typeof bar.high === "number" &&
    typeof bar.low === "number" &&
    typeof bar.close === "number" &&
    typeof bar.volume === "number"
  );
}
