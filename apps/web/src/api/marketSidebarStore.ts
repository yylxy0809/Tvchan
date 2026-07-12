import { parseMarketSidebarBootstrap, parseMarketSidebarEvent, type MarketSidebarSnapshot, type MarketSidebarTransport, type SidebarContext } from "./marketSidebar";

export class MarketSidebarStore {
  private snapshot: MarketSidebarSnapshot;
  private listeners = new Set<() => void>();
  private started = false;
  private disposeTransport: (() => void) | null = null;
  private streamSequences = new Map<string, number>();
  private activeStreamId: string | null = null;
  private retiredStreamIds = new Set<string>();

  constructor(private transport: MarketSidebarTransport, chartSymbol: string, watchlistSymbols: string[] = []) {
    this.snapshot = emptySnapshot(chartSymbol.toUpperCase());
    this.snapshot.context.watchlistSymbols = Array.from(new Set(watchlistSymbols.map((symbol) => symbol.toUpperCase())));
  }

  getSnapshot = (): MarketSidebarSnapshot => this.snapshot;
  subscribe = (listener: () => void): (() => void) => { this.listeners.add(listener); return () => this.listeners.delete(listener); };

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    this.disposeTransport = this.transport.subscribe((value) => this.acceptEvent(value));
    await this.bootstrap(this.snapshot.context);
  }

  confirmChartSymbol(symbol: string): void {
    const normalized = symbol.toUpperCase();
    this.snapshot = { ...this.snapshot, context: { ...this.snapshot.context, chartSymbol: normalized, chartEpoch: this.snapshot.context.chartEpoch + 1 } };
    this.resetStreamIdentity();
    this.transport.setContext(this.snapshot.context);
    this.emit();
  }

  setWatchlistSymbols(symbols: string[], watchlistId = "default"): void {
    const normalized = Array.from(new Set(symbols.map((symbol) => symbol.toUpperCase())));
    if (watchlistId === this.snapshot.context.watchlistId && normalized.join("|") === this.snapshot.context.watchlistSymbols.join("|")) return;
    this.snapshot = { ...this.snapshot, context: { ...this.snapshot.context, watchlistId, watchlistSymbols: normalized, watchlistRevision: this.snapshot.context.watchlistRevision + 1 } };
    this.transport.setContext(this.snapshot.context);
    this.emit();
  }

  dispose(): void { this.disposeTransport?.(); this.disposeTransport = null; }

  private acceptEvent(value: unknown): boolean {
    let event;
    try { event = parseMarketSidebarEvent(value); } catch { return false; }
    if (!this.fence(event)) return false;
    if (event.type === "sidebar_resync_required" && (
      !this.isCurrent(event.snapshot.context.chartSymbol, event.snapshot.context.chartEpoch)
      || event.snapshot.context.watchlistId !== event.watchlistId
      || event.snapshot.context.watchlistRevision !== event.watchlistRevision
    )) return false;
    if (!this.acceptStreamIdentity(event.streamId, event.type === "sidebar_resync_required")) return false;
    if (event.sequence <= (this.streamSequences.get(event.streamId) ?? 0)) return false;
    if (event.type === "sidebar_resync_required") {
      this.streamSequences.set(event.streamId, event.sequence);
      this.snapshot = { ...event.snapshot, context: { ...this.snapshot.context, watchlistRevision: event.snapshot.context.watchlistRevision }, sequence: event.sequence, snapshotVersion: event.snapshotVersion };
      this.emit();
      return true;
    }
    if (event.type === "watchlist_quote_delta") {
      const quotes = { ...this.snapshot.quotesBySymbol };
      event.quotes.forEach((quote) => { quotes[quote.symbol.toUpperCase()] = quote; });
      this.snapshot = { ...this.snapshot, quotesBySymbol: quotes, sequence: event.sequence, snapshotVersion: event.snapshotVersion };
    } else if (event.type === "strength_delta") {
      this.snapshot = { ...this.snapshot, strength: event.strength, sequence: event.sequence, snapshotVersion: event.snapshotVersion };
    } else if (this.isCurrent(event.chartSymbol, event.chartEpoch)) {
      this.snapshot = event.type === "active_profile_delta"
        ? { ...this.snapshot, profileBySymbol: { ...this.snapshot.profileBySymbol, [event.chartSymbol]: event.profile }, sequence: event.sequence, snapshotVersion: event.snapshotVersion }
        : { ...this.snapshot, newsBySymbol: { ...this.snapshot.newsBySymbol, [event.chartSymbol]: event.feed }, sequence: event.sequence, snapshotVersion: event.snapshotVersion };
    } else return false;
    this.streamSequences.set(event.streamId, event.sequence);
    this.emit();
    return true;
  }

  private fence(event: { subscriptionId: string; chartSymbol: string; chartEpoch: number; watchlistId: string; watchlistRevision: number }): boolean {
    const context = this.snapshot.context;
    return event.subscriptionId === "right-sidebar"
      && event.chartSymbol.toUpperCase() === context.chartSymbol
      && event.chartEpoch === context.chartEpoch
      && event.watchlistId === context.watchlistId
      && event.watchlistRevision === context.watchlistRevision;
  }

  private acceptStreamIdentity(streamId: string, canReplace: boolean): boolean {
    if (this.retiredStreamIds.has(streamId)) return false;
    if (this.activeStreamId === null) {
      this.activeStreamId = streamId;
      return true;
    }
    if (this.activeStreamId === streamId) return true;
    if (!canReplace) return false;
    this.retiredStreamIds.add(this.activeStreamId);
    this.activeStreamId = streamId;
    this.streamSequences.delete(streamId);
    return true;
  }

  private resetStreamIdentity(): void {
    if (this.activeStreamId) this.retiredStreamIds.add(this.activeStreamId);
    this.activeStreamId = null;
  }

  private isCurrent(symbol: string, epoch: number): boolean {
    return symbol.toUpperCase() === this.snapshot.context.chartSymbol && epoch === this.snapshot.context.chartEpoch;
  }
  private async bootstrap(request: SidebarContext): Promise<void> {
    try {
      const incoming = parseMarketSidebarBootstrap(await this.transport.bootstrap(request));
      if (!this.isCurrent(incoming.context.chartSymbol, incoming.context.chartEpoch)) return;
      const current = this.snapshot.context;
      if (request.watchlistId !== current.watchlistId
        || request.watchlistRevision !== current.watchlistRevision
        || request.watchlistSymbols.join("|") !== current.watchlistSymbols.join("|")) return;
      // Bootstrap is a snapshot, not a realtime cursor. The next stream starts at 1.
      this.snapshot = { ...incoming, context: { ...this.snapshot.context, watchlistRevision: incoming.context.watchlistRevision }, sequence: 0, snapshotVersion: 0 };
      this.resetStreamIdentity();
      this.emit();
    } catch {
      // Keep the most recent valid snapshot when bootstrap/resync is unavailable.
    }
  }
  private emit(): void { this.listeners.forEach((listener) => listener()); }
}

function emptySnapshot(chartSymbol: string): MarketSidebarSnapshot {
  return { context: { chartSymbol, chartEpoch: 0, watchlistId: "default", watchlistSymbols: [], watchlistRevision: 0 }, quotesBySymbol: {}, profileBySymbol: {}, newsBySymbol: {}, strength: undefined, snapshotVersion: 0, sequence: 0 };
}
