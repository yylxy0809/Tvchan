import { CHART_DATA_TRANSPORT } from "../config";
import { patchTvDebug, recordTvDebug } from "../tradingview/debug";
import {
  ApiBar,
  ApiChartBundleResponse,
  BarsResponse,
  ChanOverlayResponse,
  DEFAULT_CHAN_LEVELS,
  DEFAULT_CHAN_MODES,
  getChartBundle as getChartBundleHttp,
  normalizeChartBundleForFrontend,
} from "./client";
import { createChartSocket } from "./realtime";

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
  chan?: ChanOverlayResponse;
};

const CACHE_TTL_MS = 90_000;
const WS_RETRY_COOLDOWN_MS = 30_000;
const WS_REQUEST_TIMEOUT_MS = 8_000;
const SESSION_HISTORY_MAX_BARS = 15_000;
const SESSION_CHAN_MAX_ITEMS = 30_000;

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
      listeners: Set<(message: ChanSubscriptionReply) => void>;
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
    listener: (message: ChanSubscriptionReply) => void,
  ): Promise<() => void> {
    const normalizedPayload = {
      ...payload,
      id: subscriptionId,
    };
    const existing = this.subscriptions.get(subscriptionId);
    if (existing) {
      existing.payload = normalizedPayload;
      existing.listeners.add(listener);
    } else {
      this.subscriptions.set(subscriptionId, {
        payload: normalizedPayload,
        listeners: new Set([listener]),
      });
    }

    try {
      const socket = await this.connect();
      socket.send(JSON.stringify(normalizedPayload));
    } catch (error) {
      this.removeSubscriptionListener(subscriptionId, listener, false);
      throw error;
    }

    return () => {
      this.removeSubscriptionListener(subscriptionId, listener, true);
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
      const timer = window.setTimeout(() => {
        socket.close();
        reject(new Error("WebSocket chart transport connect timeout"));
      }, 4_000);
      socket.onopen = () => {
        window.clearTimeout(timer);
        this.socket = socket;
        this.connectPromise = null;
        if (this.reconnectTimer !== null) {
          window.clearTimeout(this.reconnectTimer);
          this.reconnectTimer = null;
        }
        this.replaySubscriptions(socket);
        resolve(socket);
      };
      socket.onerror = () => {
        window.clearTimeout(timer);
        this.connectPromise = null;
        reject(new Error("WebSocket chart transport failed"));
      };
      socket.onclose = () => {
        window.clearTimeout(timer);
        this.socket = null;
        this.connectPromise = null;
        this.rejectPending(new Error("WebSocket chart transport closed"));
        this.scheduleReconnect();
      };
      socket.onmessage = (event) => {
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
    listener: (message: ChanSubscriptionReply) => void,
    notifyServer: boolean,
  ): void {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      return;
    }
    subscription.listeners.delete(listener);
    if (subscription.listeners.size > 0) {
      return;
    }
    this.subscriptions.delete(subscriptionId);
    if (!notifyServer || this.socket?.readyState !== WebSocket.OPEN) {
      return;
    }
    this.socket.send(
      JSON.stringify({
        type: "unsubscribe_chan",
        id: subscriptionId,
      }),
    );
  }

  private replaySubscriptions(socket: WebSocket): void {
    for (const subscription of this.subscriptions.values()) {
      socket.send(JSON.stringify(subscription.payload));
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

class ChartDataManager {
  private barsCache = new Map<string, CacheRecord<BarsResponse>>();
  private chanCache = new Map<string, CacheRecord<ChanOverlayResponse>>();
  private windowCache = new Map<string, CacheRecord<ChartWindowResponse>>();
  private latestSnapshotVersions = new Map<string, string>();
  private latestRealtimeVersions = new Map<string, string>();
  private pendingBars = new Map<string, Promise<BarsResponse>>();
  private pendingChan = new Map<string, Promise<ChanOverlayResponse>>();
  private pendingWindow = new Map<string, Promise<ChartWindowResponse>>();
  private historyListeners = new Set<(event: ChartHistoryWindowEvent) => void>();
  private sessionHistoryWindows = new Map<string, ChartHistoryWindowEvent>();
  private sessionChanOverlays = new Map<string, ChanOverlayResponse>();
  private snapshotListeners = new Set<(event: SnapshotUpdateEvent) => void>();
  private wsClient = new ChartWebSocketClient();
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
    const key = sessionHistoryKey(response.symbol, response.chart_timeframe);
    const previous = this.sessionChanOverlays.get(key);
    const chan = previous ? mergeChanOverlay(previous, response.chan) : response.chan;
    this.sessionChanOverlays.set(key, chan);
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

  handleRealtimeBarUpdate(event: {
    symbol: string;
    timeframe: string;
    snapshotVersion?: string;
    bar?: { time?: number; revision?: number; complete?: boolean };
  }): void {
    const symbol = event.symbol.toUpperCase();
    const snapshotVersion =
      event.snapshotVersion ||
      createRealtimeSnapshotVersion(symbol, event.timeframe, event.bar);
    if (!snapshotVersion) {
      return;
    }
    const key = snapshotVersionKey(symbol, event.timeframe);
    const previous = this.latestRealtimeVersions.get(key);
    if (previous === snapshotVersion) {
      return;
    }
    this.latestRealtimeVersions.set(key, snapshotVersion);
    this.invalidateSymbolScopedCaches(symbol);
    this.notifySnapshotUpdate({
      source: "realtime",
      symbol,
      timeframe: event.timeframe,
      snapshotVersion,
    });
  }

  async subscribeChanSnapshots(request: ChartWindowRequest): Promise<() => void> {
    const subscriptionId = createChanSubscriptionId(request);
    return this.wsClient.subscribe(
      subscriptionId,
      {
        type: "subscribe_chan",
        ...requestPayload(request),
      },
      (message) => {
        this.handleChanSnapshotMessage(request, message);
      },
    );
  }

  async getBars(request: ChartWindowRequest): Promise<BarsResponse> {
    const key = requestKey("bars", request, this.snapshotHint(request));
    const cached = this.readCache(this.barsCache, key);
    if (cached) {
      recordTvDebug("chartData.bars.cache", { key });
      return cached;
    }
    const pending = this.pendingBars.get(key);
    if (pending) {
      return withAbort(pending, request.signal);
    }
    const promise = this.loadBars(request)
      .then((response) => {
        this.writeCache(
          this.barsCache,
          requestKey("bars", request, this.snapshotHint(request)),
          response,
        );
        return response;
      })
      .finally(() => {
        this.pendingBars.delete(key);
      });
    this.pendingBars.set(key, promise);
    return withAbort(promise, request.signal);
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
    const response = await this.loadChartWindow(request);
    return {
      symbol: response.symbol,
      timeframe: response.chart_timeframe,
      bars: response.bars,
    };
  }

  private async loadChan(request: ChartWindowRequest): Promise<ChanOverlayResponse> {
    const response = await this.loadChartWindow(request);
    return response.chan;
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

  private handleChanSnapshotMessage(
    request: ChartWindowRequest,
    message: ChanSubscriptionReply,
  ): void {
    if ((message.type !== "chan_snapshot" && message.type !== "chan_delta") || !message.chan) {
      return;
    }
    const chan = message.chan as ChanOverlayResponse;
    const symbol = String(message.symbol ?? chan.symbol ?? request.symbol).toUpperCase();
    const timeframe = String(message.timeframe ?? chan.chart_timeframe ?? request.timeframe);
    const changed = this.applyPublishedSnapshotVersion(
      symbol,
      timeframe,
      chan.snapshot_version,
    );
    this.writeCache(
      this.chanCache,
      requestKey(
        "chan",
        {
          ...request,
          symbol,
          timeframe,
        },
        chan.snapshot_version,
      ),
      chan,
    );
    if (!changed) {
      return;
    }
    this.notifySnapshotUpdate({
      source: "realtime",
      symbol,
      timeframe,
      snapshotVersion: chan.snapshot_version,
    });
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

function snapshotScopedCachePrefix(symbol: string, timeframe: string): string {
  return `|${symbol.toUpperCase()}|${timeframe}|`;
}

function createRealtimeSnapshotVersion(
  symbol: string,
  timeframe: string,
  bar?: { time?: number; revision?: number; complete?: boolean },
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
    Math.trunc(revision).toString().padStart(6, "0"),
    complete,
  ].join(":");
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
