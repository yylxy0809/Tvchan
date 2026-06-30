import {
  AlertCircle,
  Ban,
  CandlestickChart,
  Copy,
  KeyRound,
  LogOut,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { chartDataManager } from "./api/chartDataManager";
import { DEFAULT_CHAN_LEVELS, type ApiBar } from "./api/client";
import {
  type AdminToken,
  type AuthSession,
  createAdminToken,
  deleteAdminToken,
  disableAdminToken,
  listAdminTokens,
  loginWithToken,
} from "./auth/api";
import {
  API_TOKEN_STORAGE_KEY,
  getApiToken,
  LOGIN_TOKEN_STORAGE_KEY,
  TRADINGVIEW_DEBUG,
} from "./config";
import { RightSidebar } from "./components/RightSidebar";
import { ScreenerDock } from "./components/ScreenerDock";
import {
  createTradingViewWidget,
  getWidgetSymbol,
  getWidgetTimeframe,
  renderChanOverlay,
  setWidgetSymbol,
  setWidgetTimeframe,
  subscribeWidgetSymbolChanges,
  setTradingViewTheme,
  type ChartTheme,
  type TradingViewWidget,
} from "./tradingview/widget";
import { createDefaultChanOverlaySettings } from "./tradingview/overlaySettings";
import { recordTvDebug } from "./tradingview/debug";

const DEFAULT_SYMBOL = "000001.SZ";
const DEFAULT_TIMEFRAME = "5f";
const DEFAULT_BAR_WINDOW_SIZE = 300;
const MAX_CHART_BUNDLE_REQUEST_BARS = 5_000;
const CHAN_RENDER_LEVELS = DEFAULT_CHAN_LEVELS;
const FRONTEND_ADMIN_TOKEN = "Oppo0809*";
const ROLE_STORAGE_KEY = "tv-a-share-user-role";
const DISPLAY_NAME_STORAGE_KEY = "tv-a-share-display-name";
const LABEL_STORAGE_KEY = "tv-a-share-token-label";
const CHART_THEME_STORAGE_KEY = "tv-a-share-chart-theme";

type AppView = "chart" | "admin";
type ChartMode = "loading" | "tradingview" | "fallback";

function loadInitialChartSymbol(): string {
  if (typeof window === "undefined") {
    return DEFAULT_SYMBOL;
  }
  const raw = new URLSearchParams(window.location.search).get("symbol")?.trim();
  if (!raw) {
    return DEFAULT_SYMBOL;
  }
  const normalized = raw.toUpperCase();
  if (normalized.includes(".")) {
    return normalized;
  }
  if (/^6\d{5}$/.test(normalized)) {
    return `${normalized}.SH`;
  }
  if (/^[03]\d{5}$/.test(normalized)) {
    return `${normalized}.SZ`;
  }
  return normalized;
}

export default function App() {
  const [session, setSession] = useState<AuthSession | null>(loadSavedSession);
  const [view, setView] = useState<AppView>("chart");

  function handleAuthenticated(next: AuthSession) {
    persistSession(next);
    setSession(next);
    setView("chart");
  }

  function handleLogout() {
    clearSavedSessionMeta();
    setSession(null);
    setView("chart");
  }

  if (!session) {
    return <LoginPage onAuthenticated={handleAuthenticated} />;
  }

  if (view === "admin" && session.role === "admin") {
    return (
      <main className="terminal-shell">
        <header className="shell-header">
          <div className="brand-mark" aria-label="A-share terminal">
            <CandlestickChart size={19} />
            <span>A-Share Terminal</span>
          </div>
          <div className="shell-actions">
            <nav className="view-tabs" aria-label="Workspace">
              <button type="button" onClick={() => setView("chart")}>
                Chart
              </button>
              <button type="button" data-active="true">
                Admin
              </button>
            </nav>
            <button className="ghost-button" type="button" onClick={handleLogout}>
              <LogOut size={16} />
              <span>Sign out</span>
            </button>
          </div>
        </header>
        <AdminConsole />
      </main>
    );
  }

  return (
    <main className="terminal-shell terminal-shell--chart">
      <ChartWorkspace
        session={session}
        onOpenAdmin={() => setView("admin")}
        onLogout={handleLogout}
      />
    </main>
  );
}

function LoginPage({
  onAuthenticated,
}: {
  onAuthenticated: (session: AuthSession) => void;
}) {
  const [token, setToken] = useState(() => loadSavedToken() || getApiToken());
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const session = await loginWithToken(token);
      onAuthenticated(session);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="login-shell">
      <section className="login-panel" aria-label="Token login">
        <div className="login-brand">
          <div className="brand-emblem">
            <KeyRound size={22} />
          </div>
          <div>
            <p>TradingView Access</p>
            <h1>A-Share Chan Terminal</h1>
          </div>
        </div>

        <form className="login-form" onSubmit={handleSubmit}>
          <label htmlFor="login-token">Access Token</label>
          <input
            id="login-token"
            type="password"
            autoComplete="current-password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            placeholder="Enter your token"
          />
          {error ? (
            <p className="form-error" role="alert">
              <AlertCircle size={15} />
              <span>{error}</span>
            </p>
          ) : null}
          <button className="primary-button" type="submit" disabled={submitting}>
            {submitting ? "Signing in" : "Sign in"}
          </button>
        </form>

        <div className="login-status">
          <span />
          <strong>Secure session</strong>
          <small>Token is stored locally on this browser.</small>
        </div>
      </section>
    </main>
  );
}

function ChartWorkspace({
  session,
  onOpenAdmin,
  onLogout,
}: {
  session: AuthSession;
  onOpenAdmin(): void;
  onLogout(): void;
}) {
  const widgetContainerId = useRef(`tv-widget-${Math.random().toString(36).slice(2)}`);
  const widgetRef = useRef<TradingViewWidget | null>(null);
  const [chartTheme, setChartTheme] = useState<ChartTheme>(loadSavedChartTheme);
  const chartThemeRef = useRef<ChartTheme>(chartTheme);
  const [chartMode, setChartMode] = useState<ChartMode>("loading");
  const [bars, setBars] = useState<ApiBar[]>([]);
  const [currentSymbol, setCurrentSymbol] = useState(loadInitialChartSymbol);
  const [currentTimeframe, setCurrentTimeframe] = useState(DEFAULT_TIMEFRAME);
  const currentSymbolRef = useRef(currentSymbol);
  const currentTimeframeRef = useRef(currentTimeframe);

  useEffect(() => {
    chartThemeRef.current = chartTheme;
    saveChartTheme(chartTheme);
    document.body.dataset.appTheme = chartTheme;
    void setTradingViewTheme(widgetRef.current, chartTheme);
  }, [chartTheme]);

  useEffect(() => {
    currentSymbolRef.current = currentSymbol;
  }, [currentSymbol]);

  useEffect(() => {
    currentTimeframeRef.current = currentTimeframe;
  }, [currentTimeframe]);

  useEffect(() => {
    let cancelled = false;
    let overlayVersion = 0;
    let disposeSymbolSubscription: (() => void) | null = null;
    let latestHistoryWindow:
      | {
          symbol: string;
          timeframe: string;
          limit: number;
          from?: number;
          to?: number;
          bars: ApiBar[];
        }
      | null = null;
    let chanSnapshotSubscriptionKey = "";
    let disposeChanSnapshotSubscription: (() => void) | null = null;
    const overlaySettings = createDefaultChanOverlaySettings();
    const syncChanSnapshotSubscription = (windowRequest: {
      symbol: string;
      timeframe: string;
      limit: number;
      from?: number;
      to?: number;
    }) => {
      const nextKey = [
        windowRequest.symbol.toUpperCase(),
        windowRequest.timeframe,
        windowRequest.limit,
        windowRequest.from ?? "",
        windowRequest.to ?? "",
      ].join("|");
      if (nextKey === chanSnapshotSubscriptionKey) {
        return;
      }
      disposeChanSnapshotSubscription?.();
      disposeChanSnapshotSubscription = null;
      chanSnapshotSubscriptionKey = nextKey;
      void chartDataManager
        .subscribeChanSnapshots({
          symbol: windowRequest.symbol,
          timeframe: windowRequest.timeframe,
          limit: Math.min(windowRequest.limit, MAX_CHART_BUNDLE_REQUEST_BARS),
          from: windowRequest.from,
          to: windowRequest.to,
          levels: CHAN_RENDER_LEVELS,
        })
        .then((dispose) => {
          if (cancelled || chanSnapshotSubscriptionKey !== nextKey) {
            dispose();
            return;
          }
          disposeChanSnapshotSubscription = dispose;
        })
        .catch(() => {
          if (chanSnapshotSubscriptionKey === nextKey) {
            chanSnapshotSubscriptionKey = "";
          }
        });
    };
    const unsubscribeHistory = chartDataManager.subscribeHistoryWindows((event) => {
      if (cancelled) {
        return;
      }
      if (event.bars.length === 0) {
        return;
      }
      const nextSymbol = event.symbol.toUpperCase();
      currentSymbolRef.current = nextSymbol;
      currentTimeframeRef.current = event.timeframe;
      setCurrentSymbol((previous) => (previous === nextSymbol ? previous : nextSymbol));
      setCurrentTimeframe((previous) => (previous === event.timeframe ? previous : event.timeframe));
      latestHistoryWindow = {
        symbol: event.symbol,
        timeframe: event.timeframe,
        limit: Math.max(event.limit, event.bars.length, DEFAULT_BAR_WINDOW_SIZE),
        from: event.bars.length > 0 ? event.from : undefined,
        to: event.bars.length > 0 ? event.to : undefined,
        bars: event.bars,
      };
      const widget = widgetRef.current;
      if (!widget) {
        return;
      }
      syncChanSnapshotSubscription({
        symbol: event.symbol,
        timeframe: event.timeframe,
        limit: latestHistoryWindow.limit,
        from: latestHistoryWindow.from,
        to: latestHistoryWindow.to,
      });
      const requestVersion = ++overlayVersion;
      void renderCurrentChanOverlay({
        widget,
        symbol: event.symbol,
        timeframe: event.timeframe,
        limit: latestHistoryWindow.limit,
        from: latestHistoryWindow.from,
        to: latestHistoryWindow.to,
        settings: overlaySettings,
        chartBars: event.bars,
        isCurrent: () =>
          !cancelled &&
          requestVersion === overlayVersion &&
          widgetRef.current === widget &&
          currentSymbolRef.current === nextSymbol &&
          (!getWidgetTimeframe(widget) || getWidgetTimeframe(widget) === event.timeframe),
        });
    });
    const unsubscribeSnapshots = chartDataManager.subscribeSnapshotUpdates((event) => {
      if (event.source !== "realtime") {
        return;
      }
      const widget = widgetRef.current;
      if (!widget || cancelled) {
        return;
      }
      const activeTimeframe = getWidgetTimeframe(widget) ?? DEFAULT_TIMEFRAME;
      if (event.timeframe !== activeTimeframe && event.timeframe !== latestHistoryWindow?.timeframe) {
        return;
      }
      if (
        !latestHistoryWindow
        || latestHistoryWindow.symbol.toUpperCase() !== event.symbol.toUpperCase()
        || latestHistoryWindow.timeframe !== activeTimeframe
      ) {
        return;
      }
      const requestVersion = ++overlayVersion;
      void renderCurrentChanOverlay({
        widget,
        symbol: event.symbol,
        timeframe: activeTimeframe,
        limit: latestHistoryWindow.limit,
        from: latestHistoryWindow.from,
        to: latestHistoryWindow.to,
        settings: overlaySettings,
        chartBars: latestHistoryWindow.bars,
        isCurrent: () =>
          !cancelled &&
          requestVersion === overlayVersion &&
          widgetRef.current === widget &&
          currentSymbolRef.current === event.symbol.toUpperCase() &&
          (!getWidgetTimeframe(widget) || getWidgetTimeframe(widget) === activeTimeframe),
      });
    });
    const initialSymbol = currentSymbolRef.current;
    const initialTimeframe = currentTimeframeRef.current;
    setChartMode("loading");
    setBars([]);

        createTradingViewWidget(
          widgetContainerId.current,
          initialSymbol,
          initialTimeframe,
          chartThemeRef.current,
          { onToggleTheme: toggleChartTheme },
        )
      .then(async (widget) => {
        if (!widget) {
          if (!cancelled) {
            setChartMode("fallback");
            void refreshFallbackBars(initialSymbol, setBars);
          }
          return;
        }
        if (cancelled) {
          widget.remove();
          return;
        }
        widgetRef.current = widget;
        disposeSymbolSubscription = await subscribeWidgetSymbolChanges(widget, (nextSymbol) => {
          currentSymbolRef.current = nextSymbol;
          setCurrentSymbol((previous) => (previous === nextSymbol ? previous : nextSymbol));
        });
        setChartMode("tradingview");
        const initialWindow =
          latestHistoryWindow &&
          latestHistoryWindow.symbol.toUpperCase() === initialSymbol.toUpperCase() &&
          latestHistoryWindow.timeframe === initialTimeframe
            ? latestHistoryWindow
            : null;
        const requestVersion = ++overlayVersion;
        void renderCurrentChanOverlay({
          widget,
          symbol: initialSymbol,
          timeframe: initialTimeframe,
          limit: initialWindow?.limit ?? DEFAULT_BAR_WINDOW_SIZE,
          from: initialWindow?.from,
          to: initialWindow?.to,
          settings: overlaySettings,
          chartBars: initialWindow?.bars,
          isCurrent: () =>
            !cancelled &&
            requestVersion === overlayVersion &&
            widgetRef.current === widget &&
            (!getWidgetTimeframe(widget) || getWidgetTimeframe(widget) === initialTimeframe),
        });
      })
      .catch(() => {
        if (!cancelled) {
          setChartMode("fallback");
          void refreshFallbackBars(initialSymbol, setBars);
        }
      });

    return () => {
      cancelled = true;
      const current = widgetRef.current;
      widgetRef.current = null;
      disposeSymbolSubscription?.();
      current?.remove();
      disposeChanSnapshotSubscription?.();
      unsubscribeHistory();
      unsubscribeSnapshots();
    };
  }, [session.token]);

  useEffect(() => {
    const widget = widgetRef.current;
    if (!widget) {
      return;
    }
    const nextSymbol = currentSymbol.toUpperCase();
    if (getWidgetSymbol(widget) === nextSymbol) {
      return;
    }
    void setWidgetSymbol(widget, nextSymbol, currentTimeframeRef.current);
  }, [currentSymbol]);

  useEffect(() => {
    const widget = widgetRef.current;
    if (!widget) {
      return;
    }
    if (getWidgetTimeframe(widget) === currentTimeframe) {
      return;
    }
    void setWidgetTimeframe(widget, currentTimeframe);
  }, [currentTimeframe]);

  function handleSelectSymbol(symbol: string) {
    setCurrentSymbol(symbol.toUpperCase());
  }

  function toggleChartTheme() {
    setChartTheme((current) => (current === "dark" ? "light" : "dark"));
  }

  return (
    <section className="chart-workspace" aria-label="TradingView chart">
      <div className="chart-frame">
        <div id={widgetContainerId.current} className="tv-container" />
        {chartMode === "loading" ? (
          <div className="chart-loading">Loading TradingView</div>
        ) : null}
        {chartMode === "fallback" ? <FallbackChart bars={bars} /> : null}
      </div>
      <ScreenerDock onSelectSymbol={handleSelectSymbol} />
      <RightSidebar
        activeSymbol={currentSymbol}
        timeframe={currentTimeframe}
        onSelectSymbol={handleSelectSymbol}
        isAdmin={session.role === "admin"}
        onOpenAdmin={onOpenAdmin}
        onLogout={onLogout}
      />
    </section>
  );
}

function AdminConsole() {
  const [tokens, setTokens] = useState<AdminToken[]>([]);
  const [label, setLabel] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [newToken, setNewToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [mutating, setMutating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void refreshTokens();
  }, []);

  async function refreshTokens() {
    setLoading(true);
    setError(null);
    try {
      setTokens(await listAdminTokens());
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setLoading(false);
    }
  }

  async function handleCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedLabel = label.trim();
    if (!normalizedLabel) {
      setError("Token label is required.");
      return;
    }
    setMutating(true);
    setError(null);
    try {
      const created = await createAdminToken({
        label: normalizedLabel,
        display_name: displayName.trim() || null,
      });
      setTokens((current) => [created, ...current]);
      setNewToken(created.token ?? null);
      setLabel("");
      setDisplayName("");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleDisable(id: number) {
    setMutating(true);
    setError(null);
    try {
      const updated = await disableAdminToken(id);
      setTokens((current) =>
        current.map((item) => (item.id === id ? updated : item)),
      );
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleDelete(id: number) {
    setMutating(true);
    setError(null);
    try {
      await deleteAdminToken(id);
      setTokens((current) => current.filter((item) => item.id !== id));
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function copyNewToken() {
    if (!newToken) {
      return;
    }
    try {
      await navigator.clipboard.writeText(newToken);
    } catch {
      setError("Clipboard is not available. Select and copy the token manually.");
    }
  }

  return (
    <section className="admin-workspace" aria-label="Admin token management">
      <div className="admin-head">
        <div>
          <p className="eyebrow">Admin Console</p>
          <h1>Access Tokens</h1>
        </div>
        <button
          className="ghost-button"
          type="button"
          onClick={() => void refreshTokens()}
          disabled={loading}
        >
          <RefreshCw size={16} />
          <span>Refresh</span>
        </button>
      </div>

      <form className="token-create" onSubmit={handleCreate}>
        <label htmlFor="token-label">Label</label>
        <input
          id="token-label"
          value={label}
          onChange={(event) => setLabel(event.target.value)}
          placeholder="desk-user-01"
        />
        <label htmlFor="token-display-name">Display name</label>
        <input
          id="token-display-name"
          value={displayName}
          onChange={(event) => setDisplayName(event.target.value)}
          placeholder="User name or device"
        />
        <button className="primary-button compact" type="submit" disabled={mutating}>
          <Plus size={16} />
          <span>Create</span>
        </button>
      </form>

      {newToken ? (
        <div className="created-token">
          <div>
            <span>New token</span>
            <code>{newToken}</code>
          </div>
          <button type="button" onClick={() => void copyNewToken()}>
            <Copy size={15} />
            <span>Copy</span>
          </button>
        </div>
      ) : null}

      {error ? (
        <p className="form-error admin-error" role="alert">
          <AlertCircle size={15} />
          <span>{error}</span>
        </p>
      ) : null}

      <div className="token-table" aria-busy={loading}>
        <div className="token-row token-row-head">
          <span>Label</span>
          <span>Display</span>
          <span>Status</span>
          <span>Created</span>
          <span />
        </div>
        {loading ? <div className="empty-row">Loading tokens</div> : null}
        {!loading && tokens.length === 0 ? (
          <div className="empty-row">No user tokens yet.</div>
        ) : null}
        {tokens.map((item) => (
          <div className="token-row" key={item.id}>
            <span className="token-name">{item.label}</span>
            <span>{item.display_name || "--"}</span>
            <span data-state={item.is_active ? "on" : "off"}>
              {item.is_active ? "active" : "disabled"}
            </span>
            <span>{formatDate(item.created_at)}</span>
            <span className="row-actions">
              <button
                type="button"
                title="Disable token"
                onClick={() => void handleDisable(item.id)}
                disabled={mutating || !item.is_active}
              >
                <Ban size={15} />
              </button>
              <button
                type="button"
                title="Delete token"
                onClick={() => void handleDelete(item.id)}
                disabled={mutating}
              >
                <Trash2 size={15} />
              </button>
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

async function refreshFallbackBars(
  symbol: string,
  setBars: (bars: ApiBar[]) => void,
) {
  try {
    const response = await chartDataManager.getChartWindow({
      symbol,
      timeframe: DEFAULT_TIMEFRAME,
      limit: DEFAULT_BAR_WINDOW_SIZE,
    });
    setBars(response.bars);
  } catch {
    setBars([]);
  }
}

async function renderCurrentChanOverlay({
  widget,
  symbol,
  timeframe,
  limit,
  from,
  to,
  settings,
  chartBars = [],
  isCurrent,
}: {
  widget: TradingViewWidget;
  symbol: string;
  timeframe: string;
  limit: number;
  from?: number;
  to?: number;
  settings: ReturnType<typeof createDefaultChanOverlaySettings>;
  chartBars?: ApiBar[];
  isCurrent(): boolean;
}) {
  try {
    recordTvDebug("chan.renderCurrent.request", {
      symbol,
      timeframe,
      limit: Math.min(limit, MAX_CHART_BUNDLE_REQUEST_BARS),
      from,
      to,
      chartBars: chartBars.length,
    });
    const window = await chartDataManager.getChartWindow({
      symbol,
      timeframe,
      limit: Math.min(limit, MAX_CHART_BUNDLE_REQUEST_BARS),
      from,
      to,
      levels: CHAN_RENDER_LEVELS,
    });
    const projectedBars = chartBars.length > 0 ? chartBars : window.bars;
    const chan = window.chan;
    if (!isCurrent()) {
      recordTvDebug("chan.renderCurrent.stale", {
        symbol,
        timeframe,
        snapshotVersion: chan.snapshot_version,
      });
      return;
    }
    recordTvDebug("chan.renderCurrent.response", {
      symbol,
      timeframe,
      snapshotVersion: chan.snapshot_version,
      strokes: chan.strokes.length,
      segments: chan.segments.length,
      centers: chan.centers.length,
      signals: chan.signals.length,
      chartBars: projectedBars.length,
    });
    await renderChanOverlay(widget, chan, settings, {
      isCurrent,
      chartBars: projectedBars,
    });
  } catch (error) {
    recordTvDebug("chan.renderCurrent.error", {
      symbol,
      timeframe,
      message: error instanceof Error ? error.message : String(error),
    });
    if (TRADINGVIEW_DEBUG) {
      console.warn("[chan-render-current-failed]", error);
    }
  }
}

function loadSavedSession(): AuthSession | null {
  const token = loadSavedToken();
  if (!token) {
    return null;
  }
  return {
    token,
    role: loadSavedRole(token),
    displayName: readStorage(DISPLAY_NAME_STORAGE_KEY),
    label: readStorage(LABEL_STORAGE_KEY),
  };
}

function persistSession(session: AuthSession) {
  writeStorage(LOGIN_TOKEN_STORAGE_KEY, session.token);
  clearLegacyLoginToken();
  writeStorage(ROLE_STORAGE_KEY, session.role);
  writeStorage(DISPLAY_NAME_STORAGE_KEY, session.displayName ?? "");
  writeStorage(LABEL_STORAGE_KEY, session.label ?? "");
}

function loadSavedToken(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    const loginToken = window.localStorage.getItem(LOGIN_TOKEN_STORAGE_KEY)?.trim();
    if (loginToken) {
      return loginToken;
    }
    const legacyToken = window.localStorage.getItem(API_TOKEN_STORAGE_KEY)?.trim() ?? "";
    if (isFrontendLoginToken(legacyToken)) {
      window.localStorage.setItem(LOGIN_TOKEN_STORAGE_KEY, legacyToken);
      window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
      return legacyToken;
    }
    return "";
  } catch {
    return "";
  }
}

function loadSavedRole(token: string): AuthSession["role"] {
  const stored = readStorage(ROLE_STORAGE_KEY);
  if (stored === "admin") {
    return "admin";
  }
  if (stored === "user") {
    return "user";
  }
  return token === FRONTEND_ADMIN_TOKEN ? "admin" : "user";
}

function clearSavedSessionMeta() {
  removeStorage(LOGIN_TOKEN_STORAGE_KEY);
  removeStorage(ROLE_STORAGE_KEY);
  removeStorage(DISPLAY_NAME_STORAGE_KEY);
  removeStorage(LABEL_STORAGE_KEY);
}

function loadSavedChartTheme(): ChartTheme {
  const value = readStorage(CHART_THEME_STORAGE_KEY);
  return value === "light" ? "light" : "dark";
}

function saveChartTheme(theme: ChartTheme) {
  writeStorage(CHART_THEME_STORAGE_KEY, theme);
}

function readStorage(key: string): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return window.localStorage.getItem(key) ?? "";
  } catch {
    return "";
  }
}

function writeStorage(key: string, value: string) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    if (value) {
      window.localStorage.setItem(key, value);
    } else {
      window.localStorage.removeItem(key);
    }
  } catch {
    // Storage failures should not block login.
  }
}

function removeStorage(key: string) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(key);
  } catch {
    // Ignore storage failures.
  }
}

function clearLegacyLoginToken() {
  if (typeof window === "undefined") {
    return;
  }
  try {
    const legacyToken = window.localStorage.getItem(API_TOKEN_STORAGE_KEY)?.trim() ?? "";
    if (isFrontendLoginToken(legacyToken)) {
      window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);
    }
  } catch {
    // Storage cleanup is best effort only.
  }
}

function isFrontendLoginToken(value: string): boolean {
  return value === FRONTEND_ADMIN_TOKEN || value.startsWith("tv_");
}

function formatDate(value?: string | null): string {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function FallbackChart({ bars }: { bars: ApiBar[] }) {
  const width = 920;
  const height = 360;
  const visible = bars.slice(-100);
  const lows = visible.map((bar) => bar.low);
  const highs = visible.map((bar) => bar.high);
  const rawMin = lows.length > 0 ? Math.min(...lows) : 0;
  const rawMax = highs.length > 0 ? Math.max(...highs) : 1;
  const rawSpan = Math.max(rawMax - rawMin, 0.01);
  const min = rawMin - rawSpan * 0.08;
  const span = rawSpan * 1.16;
  const candleWidth = width / Math.max(visible.length, 1);

  return (
    <div className="fallback-chart">
      {bars.length === 0 ? (
        <div className="chart-loading">TradingView library unavailable</div>
      ) : null}
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Fallback K-line chart">
        {visible.map((bar, index) => {
          const x = index * candleWidth + candleWidth / 2;
          const yHigh = height - ((bar.high - min) / span) * height;
          const yLow = height - ((bar.low - min) / span) * height;
          const yOpen = height - ((bar.open - min) / span) * height;
          const yClose = height - ((bar.close - min) / span) * height;
          const up = bar.close >= bar.open;
          return (
            <g key={`${bar.time}-${index}`} data-up={up}>
              <line x1={x} x2={x} y1={yHigh} y2={yLow} />
              <rect
                x={x - Math.max(2, candleWidth * 0.28)}
                y={Math.min(yOpen, yClose)}
                width={Math.max(4, candleWidth * 0.56)}
                height={Math.max(2, Math.abs(yClose - yOpen))}
              />
            </g>
          );
        })}
      </svg>
    </div>
  );
}
