import {
  Check,
  ChevronDown,
  ChevronRight,
  Grid2X2,
  MoreHorizontal,
  Plus,
  Search,
  Trash2,
  X,
} from "lucide-react";
import {
  type CSSProperties,
  type FormEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ApiSymbol } from "../api/client";
import { listUserSettings, saveUserSetting } from "../api/userSettings";
import {
  type ChanStrokeState,
  type MarketQuote,
  type ProfileTheme,
  type StrategySignal,
  type SymbolProfile,
  getMarketQuote,
  getSymbolProfile,
  searchSymbolCatalog,
} from "../api/marketData";
import {
  DEFAULT_WATCHLIST_GROUPS,
  FAVORITES_GROUP_ID,
  WATCHLIST_STORAGE_KEY,
  WATCHLIST_UPDATED_EVENT,
  createWatchlistId,
  isFixedWatchlistGroup,
  reviveWatchlistGroups,
  type WatchlistGroup,
  type WatchlistItem,
} from "../api/watchlistStore";
import { useLocalStorageState } from "../hooks/useLocalStorageState";

type Props = {
  activeSymbol: string;
  timeframe: string;
  onSelectSymbol(symbol: string): void;
  authToken?: string;
};

const SPLIT_STORAGE_KEY = "tv-a-share-watchlist-list-height";
const MIN_WATCHLIST_HEIGHT = 142;
const MIN_PROFILE_HEIGHT = 238;

export function WatchlistPanel({
  activeSymbol,
  timeframe,
  onSelectSymbol,
  authToken,
}: Props) {
  const [groups, setGroups] = useLocalStorageState<WatchlistGroup[]>(
    WATCHLIST_STORAGE_KEY,
    DEFAULT_WATCHLIST_GROUPS,
    reviveWatchlistGroups,
  );
  const [activeGroupId, setActiveGroupId] = useState(
    () => groups[0]?.id ?? FAVORITES_GROUP_ID,
  );
  const [expandedGroupIds, setExpandedGroupIds] = useState<string[]>(() =>
    groups.map((group) => group.id),
  );
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<ApiSymbol[]>([]);
  const [searching, setSearching] = useState(false);
  const [editingGroupId, setEditingGroupId] = useState<string | null>(null);
  const [editingName, setEditingName] = useState("");
  const [quotes, setQuotes] = useState<Record<string, MarketQuote>>({});
  const [profile, setProfile] = useState<SymbolProfile | null>(null);
  const [listHeight, setListHeight] = useLocalStorageState<number>(
    SPLIT_STORAGE_KEY,
    356,
    reviveSplitHeight,
  );
  const [resizing, setResizing] = useState(false);
  const contentRef = useRef<HTMLDivElement | null>(null);
  const remoteWatchlistReadyRef = useRef(false);
  const suppressNextWatchlistSyncRef = useRef(false);

  const activeGroup = useMemo(
    () => groups.find((group) => group.id === activeGroupId) ?? groups[0],
    [activeGroupId, groups],
  );

  const watchlistItems = useMemo(() => {
    const items = new Map<string, WatchlistItem>();
    groups.forEach((group) => {
      group.items.forEach((item) => items.set(item.symbol, item));
    });
    return Array.from(items.values());
  }, [groups]);

  const selectedProfileItem = useMemo(() => {
    if (activeSymbol) {
      return (
        watchlistItems.find((item) => item.symbol === activeSymbol) ??
        createProfileItemFromSymbol(activeSymbol)
      );
    }
    return (
      activeGroup?.items[0] ??
      watchlistItems[0]
    );
  }, [activeGroup?.items, activeSymbol, watchlistItems]);

  useEffect(() => {
    if (!authToken) {
      remoteWatchlistReadyRef.current = false;
      return;
    }
    let cancelled = false;
    remoteWatchlistReadyRef.current = false;
    void listUserSettings(authToken)
      .then((settings) => {
        if (cancelled) {
          return;
        }
        const remote = settings.find((item) => item.bucket === "watchlist")?.value;
        const remoteGroups = readRemoteWatchlistGroups(remote);
        remoteWatchlistReadyRef.current = true;
        if (remoteGroups) {
          suppressNextWatchlistSyncRef.current = true;
          setGroups(remoteGroups);
          setActiveGroupId(remoteGroups[0]?.id ?? FAVORITES_GROUP_ID);
          setExpandedGroupIds(remoteGroups.map((group) => group.id));
        } else {
          void saveUserSetting(authToken, "watchlist", { groups }).catch(() => {
            // Local watchlist remains authoritative when server sync fails.
          });
        }
      })
      .catch(() => {
        if (!cancelled) {
          remoteWatchlistReadyRef.current = true;
        }
      });
    return () => {
      cancelled = true;
    };
  }, [authToken, setGroups]);

  useEffect(() => {
    if (!authToken || !remoteWatchlistReadyRef.current) {
      return;
    }
    if (suppressNextWatchlistSyncRef.current) {
      suppressNextWatchlistSyncRef.current = false;
      return;
    }
    void saveUserSetting(authToken, "watchlist", { groups }).catch(() => {
      // Local watchlist remains usable if server-side settings fail.
    });
  }, [authToken, groups]);

  useEffect(() => {
    const handleExternalUpdate = (event: Event) => {
      const detail = (event as CustomEvent<{ groups?: unknown }>).detail;
      if (detail?.groups) {
        const next = reviveWatchlistGroups(detail.groups);
        setGroups(next);
        setActiveGroupId(FAVORITES_GROUP_ID);
        setExpandedGroupIds((current) =>
          current.includes(FAVORITES_GROUP_ID)
            ? current
            : [FAVORITES_GROUP_ID, ...current],
        );
      }
    };
    window.addEventListener(WATCHLIST_UPDATED_EVENT, handleExternalUpdate);
    return () => {
      window.removeEventListener(WATCHLIST_UPDATED_EVENT, handleExternalUpdate);
    };
  }, [setGroups]);

  useEffect(() => {
    if (groups.length === 0) {
      return;
    }
    if (!groups.some((group) => group.id === activeGroupId)) {
      setActiveGroupId(groups[0].id);
    }
  }, [activeGroupId, groups]);

  useEffect(() => {
    setExpandedGroupIds((current) => {
      const validIds = new Set(groups.map((group) => group.id));
      const next = current.filter((id) => validIds.has(id));
      if (next.length === 0 && groups[0]) {
        next.push(groups[0].id);
      }
      return next;
    });
  }, [groups]);

  useEffect(() => {
    const trimmed = query.trim();
    if (!trimmed) {
      setResults([]);
      return;
    }
    const localResults = buildLocalSearchResults(trimmed, groups);
    setResults(localResults);
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      setSearching(true);
      searchSymbolCatalog(trimmed)
        .then((items) => {
          if (!controller.signal.aborted) {
            setResults(mergeSearchResults(items, localResults));
          }
        })
        .catch(() => {
          if (!controller.signal.aborted) {
            setResults(localResults);
          }
        })
        .finally(() => {
          if (!controller.signal.aborted) {
            setSearching(false);
          }
        });
    }, 180);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [groups, query]);

  useEffect(() => {
    if (watchlistItems.length === 0) {
      return;
    }
    const controller = new AbortController();
    watchlistItems.forEach((item) => {
      void getMarketQuote(toApiSymbol(item), timeframe, controller.signal).then(
        (quote) => {
          if (!controller.signal.aborted) {
            setQuotes((current) => ({ ...current, [item.symbol]: quote }));
          }
        },
      );
    });
    return () => controller.abort();
  }, [timeframe, watchlistItems]);

  useEffect(() => {
    if (!selectedProfileItem) {
      setProfile(null);
      return;
    }
    const controller = new AbortController();
    void getSymbolProfile(
      toApiSymbol(selectedProfileItem),
      timeframe,
      controller.signal,
    ).then((nextProfile) => {
      if (!controller.signal.aborted) {
        setProfile(nextProfile);
      }
    });
    return () => controller.abort();
  }, [selectedProfileItem, timeframe]);

  useEffect(() => {
    if (!resizing) {
      return;
    }
    const previousCursor = document.body.style.cursor;
    const previousUserSelect = document.body.style.userSelect;
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";

    const handleMove = (event: PointerEvent) => {
      updateSplitFromClientY(event.clientY);
    };
    const handleDone = () => {
      setResizing(false);
    };
    window.addEventListener("pointermove", handleMove);
    window.addEventListener("pointerup", handleDone, { once: true });
    window.addEventListener("pointercancel", handleDone, { once: true });
    return () => {
      document.body.style.cursor = previousCursor;
      document.body.style.userSelect = previousUserSelect;
      window.removeEventListener("pointermove", handleMove);
      window.removeEventListener("pointerup", handleDone);
      window.removeEventListener("pointercancel", handleDone);
    };
  }, [resizing]);

  function handleCreateGroup() {
    const name = `列表 ${groups.length + 1}`;
    const nextGroup = { id: createWatchlistId("watchlist"), name, items: [] };
    setGroups((current) => [...current, nextGroup]);
    setActiveGroupId(nextGroup.id);
    setExpandedGroupIds((current) => [...current, nextGroup.id]);
  }

  function handleDeleteGroup(groupId: string) {
    if (isFixedWatchlistGroup(groupId)) {
      return;
    }
    setEditingGroupId(null);
    setEditingName("");
    setExpandedGroupIds((current) => current.filter((id) => id !== groupId));
    setGroups((current) => {
      if (current.length <= 1) {
        return current;
      }
      const next = current.filter((group) => group.id !== groupId);
      if (activeGroupId === groupId) {
        setActiveGroupId(next[0]?.id ?? FAVORITES_GROUP_ID);
      }
      return next;
    });
  }

  function handleStartRename(group: WatchlistGroup) {
    if (isFixedWatchlistGroup(group.id)) {
      return;
    }
    setEditingGroupId(group.id);
    setEditingName(group.name);
  }

  function handleRenameCommit(groupId = editingGroupId) {
    if (!groupId) {
      return;
    }
    const name = editingName.trim();
    if (name) {
      setGroups((current) =>
        current.map((group) =>
          group.id === groupId ? { ...group, name } : group,
        ),
      );
    }
    setEditingGroupId(null);
    setEditingName("");
  }

  function handleRenameSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    handleRenameCommit();
  }

  function toggleGroup(groupId: string) {
    setActiveGroupId(groupId);
    setExpandedGroupIds((current) =>
      current.includes(groupId)
        ? current.filter((id) => id !== groupId)
        : [...current, groupId],
    );
  }

  function handleAddSymbolToGroup(symbol: ApiSymbol, groupId: string) {
    const item: WatchlistItem = {
      symbol: symbol.symbol,
      name: symbol.name || symbol.symbol,
      exchange: symbol.exchange,
    };
    setGroups((current) =>
      current.map((group) => {
        if (group.id !== groupId) {
          return group;
        }
        if (group.items.some((existing) => existing.symbol === item.symbol)) {
          return group;
        }
        return { ...group, items: [...group.items, item] };
      }),
    );
    setActiveGroupId(groupId);
    setExpandedGroupIds((current) =>
      current.includes(groupId) ? current : [...current, groupId],
    );
    setQuery("");
    setResults([]);
  }

  function handleRemoveSymbol(groupId: string, symbol: string) {
    setGroups((current) =>
      current.map((group) =>
        group.id === groupId
          ? {
              ...group,
              items: group.items.filter((item) => item.symbol !== symbol),
            }
          : group,
      ),
    );
  }

  function handleSelectSearchResult(symbol: ApiSymbol) {
    const existingGroup = groups.find((group) =>
      group.items.some((item) => item.symbol === symbol.symbol),
    );
    if (existingGroup) {
      setActiveGroupId(existingGroup.id);
      setExpandedGroupIds((current) =>
        current.includes(existingGroup.id)
          ? current
          : [...current, existingGroup.id],
      );
    }
    onSelectSymbol(symbol.symbol);
  }

  function updateSplitFromClientY(clientY: number) {
    const rect = contentRef.current?.getBoundingClientRect();
    if (!rect) {
      return;
    }
    const max = Math.max(MIN_WATCHLIST_HEIGHT, rect.height - MIN_PROFILE_HEIGHT);
    const next = Math.min(max, Math.max(MIN_WATCHLIST_HEIGHT, clientY - rect.top));
    setListHeight(Math.round(next));
  }

  function nudgeSplit(delta: number) {
    const rect = contentRef.current?.getBoundingClientRect();
    const max = rect
      ? Math.max(MIN_WATCHLIST_HEIGHT, rect.height - MIN_PROFILE_HEIGHT)
      : 520;
    setListHeight((current) =>
      Math.round(Math.min(max, Math.max(MIN_WATCHLIST_HEIGHT, current + delta))),
    );
  }

  return (
    <section className="tv-watchlist-panel" aria-label="关注列表">
      <header className="tv-panel-toolbar">
        <button type="button" className="tv-panel-title-button">
          <span>关注列表</span>
          <ChevronDown size={16} />
        </button>
        <div className="tv-panel-actions">
          <button type="button" title="添加标的" onClick={() => setQuery("000001")}>
            <Plus size={19} />
          </button>
          <button type="button" title="新建分组" onClick={handleCreateGroup}>
            <Grid2X2 size={18} />
          </button>
          <button type="button" title="更多">
            <MoreHorizontal size={20} />
          </button>
        </div>
      </header>

      <div className="tv-watchlist-search-zone">
        <div className="tv-watchlist-search">
          <Search size={17} />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="搜索代码或名称"
          />
          {query ? (
            <button type="button" title="清空检索" onClick={() => setQuery("")}>
              <X size={15} />
            </button>
          ) : null}
        </div>

        {query ? (
          <div className="tv-symbol-results" aria-busy={searching}>
            {searching ? (
              <div className="tv-panel-empty compact">检索中</div>
            ) : null}
            {!searching && results.length === 0 ? (
              <div className="tv-panel-empty compact">暂无标的</div>
            ) : null}
            {results.map((result) => (
              <div key={result.symbol} className="tv-symbol-result-row">
                <button
                  type="button"
                  className="tv-symbol-result-main"
                  onClick={() => handleSelectSearchResult(result)}
                >
                  <span>{result.symbol}</span>
                  <strong>{result.name}</strong>
                  <small>{result.exchange}</small>
                </button>
                <div className="tv-symbol-add-options">
                  {groups.map((group) => {
                    const inGroup = group.items.some(
                      (item) => item.symbol === result.symbol,
                    );
                    return (
                      <button
                        type="button"
                        key={group.id}
                        disabled={inGroup}
                        onClick={() => handleAddSymbolToGroup(result, group.id)}
                      >
                        {inGroup ? `已在${group.name}` : `添加到${group.name}`}
                      </button>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        ) : null}
      </div>

      <div
        className="tv-watchlist-content"
        ref={contentRef}
        style={
          {
            "--watchlist-list-height": `${listHeight}px`,
          } as CSSProperties
        }
      >
      <div className="tv-watchlist-groups-list" aria-label="关注列表分组">
        {groups.map((group) => {
          const expanded = expandedGroupIds.includes(group.id);
          const editing = editingGroupId === group.id;
          return (
            <section
              key={group.id}
              className="tv-watchlist-group-section"
              data-active={group.id === activeGroup?.id}
            >
              {editing ? (
                <form
                  className="tv-watchlist-group-edit"
                  onSubmit={handleRenameSubmit}
                >
                  <input
                    autoFocus
                    value={editingName}
                    onBlur={() => handleRenameCommit(group.id)}
                    onChange={(event) => setEditingName(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Escape") {
                        setEditingGroupId(null);
                        setEditingName("");
                      }
                    }}
                  />
                  <button
                    type="submit"
                    title="保存分组名称"
                    onMouseDown={(event) => event.preventDefault()}
                  >
                    <Check size={14} />
                  </button>
                  <button
                    type="button"
                    title="删除分组"
                    disabled={groups.length <= 1 || isFixedWatchlistGroup(group.id)}
                    onMouseDown={(event) => event.preventDefault()}
                    onClick={() => handleDeleteGroup(group.id)}
                  >
                    <Trash2 size={14} />
                  </button>
                </form>
              ) : (
                <button
                  type="button"
                  className="tv-watchlist-group-header"
                  onClick={() => toggleGroup(group.id)}
                  onDoubleClick={() => handleStartRename(group)}
                >
                  {expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
                  <span>{group.name}</span>
                  {isFixedWatchlistGroup(group.id) ? (
                    <em className="tv-watchlist-fixed-label">固定</em>
                  ) : null}
                  <small>{group.items.length}</small>
                </button>
              )}

              {expanded ? (
                <div
                  className="tv-watchlist-group-body"
                  role="table"
                  aria-label={`${group.name}关注列表`}
                >
                  <div className="tv-watchlist-table-head" role="row">
                    <span>代码</span>
                    <span>最新</span>
                    <span>涨跌</span>
                    <span>涨幅</span>
                  </div>
                  {group.items.length === 0 ? (
                    <div className="tv-panel-empty compact">暂无标的</div>
                  ) : null}
                  {group.items.map((item) => {
                    const quote = quotes[item.symbol];
                    const change = quote?.change ?? null;
                    const percent = quote?.changePercent ?? null;
                    const direction =
                      percent === null ? "flat" : percent >= 0 ? "up" : "down";
                    return (
                      <div
                        key={item.symbol}
                        className="tv-watchlist-row"
                        role="row"
                        data-active={item.symbol === activeSymbol}
                      >
                        <button
                          type="button"
                          onClick={() => {
                            setActiveGroupId(group.id);
                            onSelectSymbol(item.symbol);
                          }}
                        >
                          <span>{item.symbol}</span>
                          <strong>{item.name}</strong>
                        </button>
                        <span>{formatPrice(quote?.price)}</span>
                        <span data-direction={direction}>{formatSigned(change)}</span>
                        <span data-direction={direction}>{formatPercent(percent)}</span>
                        <button
                          type="button"
                          title="删除标的"
                          onClick={() => handleRemoveSymbol(group.id, item.symbol)}
                        >
                          <Trash2 size={13} />
                        </button>
                      </div>
                    );
                  })}
                </div>
              ) : null}
            </section>
          );
        })}
      </div>

      <div
        className="tv-watchlist-resizer"
        role="separator"
        aria-label="调整关注列表和标的资料高度"
        aria-orientation="horizontal"
        tabIndex={0}
        data-resizing={resizing}
        onPointerDown={(event) => {
          event.currentTarget.setPointerCapture(event.pointerId);
          setResizing(true);
          updateSplitFromClientY(event.clientY);
        }}
        onKeyDown={(event) => {
          if (event.key === "ArrowUp") {
            event.preventDefault();
            nudgeSplit(-24);
          }
          if (event.key === "ArrowDown") {
            event.preventDefault();
            nudgeSplit(24);
          }
        }}
      >
        <span />
      </div>

      <div className="tv-symbol-card" aria-label="标的资料">
        <div className="tv-symbol-card-head">
          <div>
            <strong>{profile?.symbol ?? activeSymbol}</strong>
            <span>{profile?.name ?? "--"}</span>
          </div>
          <MoreHorizontal size={19} />
        </div>
        <div className="tv-symbol-price-line">
          <strong>{formatPrice(profile?.latestPrice)}</strong>
          <span data-direction={directionOf(profile?.dayChangePercent ?? null)}>
            {formatPercent(profile?.dayChangePercent ?? null)}
          </span>
        </div>
        <div className="tv-symbol-profile-section">
          <h3>交易概况</h3>
          <dl>
            <div>
              <dt>交易所</dt>
              <dd>{profile?.exchange || "--"}</dd>
            </div>
            <div>
              <dt>成交量</dt>
              <dd>{formatNumber(profile?.volume)}</dd>
            </div>
            <div>
              <dt>成交额</dt>
              <dd>{formatMoney(profile?.amount)}</dd>
            </div>
          </dl>
        </div>

        <div className="tv-symbol-profile-section">
          <h3>板块与概念</h3>
          <div className="tv-symbol-theme-list">
            <ThemePill label="板块" theme={profile?.sector ?? null} />
            {profile?.concepts.length ? (
              profile.concepts.slice(0, 4).map((theme) => (
                <ThemePill key={theme.name} label="概念" theme={theme} />
              ))
            ) : (
              <ThemePill label="概念" theme={null} />
            )}
          </div>
        </div>

        <div className="tv-symbol-profile-section">
          <h3>估值与活跃度</h3>
          <dl>
            <div>
              <dt>市值</dt>
              <dd>{formatMarketCap(profile?.marketCap)}</dd>
            </div>
            <div>
              <dt>市盈率</dt>
              <dd>{formatRatio(profile?.peRatio)}</dd>
            </div>
            <div>
              <dt>换手率</dt>
              <dd>{formatPercent(profile?.turnoverRate)}</dd>
            </div>
          </dl>
        </div>

        <div className="tv-symbol-profile-section">
          <h3>资金流向</h3>
          <dl>
            <FundFlowRow label="净流入" value={profile?.fundFlow.net ?? null} />
            <FundFlowRow label="主力净流入" value={profile?.fundFlow.main ?? null} />
            <FundFlowRow label="散户净流入" value={profile?.fundFlow.retail ?? null} />
          </dl>
        </div>
        <div className="tv-symbol-profile-section">
          <h3>缠论状态</h3>
          <div className="tv-chan-state-list">
            {(profile?.chanStrokeStates ?? []).map((state) => (
              <StrokeStateRow key={state.level} state={state} />
            ))}
          </div>
        </div>
        <div className="tv-symbol-profile-section">
          <h3>策略信号</h3>
          <div className="tv-strategy-signal-list">
            {(profile?.strategySignals ?? []).map((item) => (
              <StrategySignalRow key={item.key} signal={item} />
            ))}
          </div>
        </div>
      </div>
      </div>
    </section>
  );
}

function ThemePill({
  label,
  theme,
}: {
  label: string;
  theme: ProfileTheme | null;
}) {
  return (
    <div className="tv-symbol-theme-pill">
      <span>{label}</span>
      <strong>{theme?.name ?? "--"}</strong>
      <em data-direction={directionOf(theme?.changePercent ?? null)}>
        {formatPercent(theme?.changePercent ?? null)}
      </em>
    </div>
  );
}

function FundFlowRow({ label, value }: { label: string; value: number | null }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd data-direction={directionOf(value)}>{formatMoney(value, true)}</dd>
    </div>
  );
}

function StrokeStateRow({ state }: { state: ChanStrokeState }) {
  return (
    <div className="tv-chan-state-pill">
      <span>{state.label}</span>
      <strong data-direction={directionOfStroke(state)}>{state.stateLabel}</strong>
      <em>{state.modeLabel}</em>
    </div>
  );
}

function StrategySignalRow({ signal }: { signal: StrategySignal }) {
  return (
    <div className="tv-strategy-signal-row">
      <span>{signal.label}</span>
      <strong data-direction={directionOfSignalTone(signal.tone)}>
        {signal.value}
      </strong>
    </div>
  );
}

function directionOfStroke(state: ChanStrokeState): "up" | "down" | "flat" {
  if (state.direction === "up") {
    return "up";
  }
  if (state.direction === "down") {
    return "down";
  }
  return "flat";
}

function directionOfSignalTone(
  tone: StrategySignal["tone"],
): "up" | "down" | "flat" {
  if (tone === "up") {
    return "up";
  }
  if (tone === "down") {
    return "down";
  }
  return "flat";
}

function reviveSplitHeight(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed)) {
    return 356;
  }
  return Math.min(620, Math.max(MIN_WATCHLIST_HEIGHT, Math.round(parsed)));
}

function toApiSymbol(item: WatchlistItem): ApiSymbol {
  return {
    symbol: item.symbol,
    code: item.symbol.split(".")[0],
    exchange: item.exchange,
    name: item.name,
    asset_type: "stock",
  };
}

function createProfileItemFromSymbol(symbol: string): WatchlistItem {
  const normalized = symbol.trim().toUpperCase();
  const [code = normalized, suffix] = normalized.split(".");
  return {
    symbol: normalized,
    name: code,
    exchange: suffix ? suffix.toUpperCase() : inferExchangeFromCode(code),
  };
}

function readRemoteWatchlistGroups(value: unknown): WatchlistGroup[] | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const groups = (value as { groups?: unknown }).groups;
  if (!Array.isArray(groups)) {
    return null;
  }
  return reviveWatchlistGroups(groups);
}

function buildLocalSearchResults(
  keyword: string,
  groups: WatchlistGroup[],
): ApiSymbol[] {
  const normalized = keyword.trim().toUpperCase();
  if (!normalized) {
    return [];
  }
  const results: ApiSymbol[] = [];
  const knownItems = groups.flatMap((group) => group.items);
  knownItems.forEach((item) => {
    const haystack = `${item.symbol} ${item.name} ${item.exchange}`.toUpperCase();
    if (haystack.includes(normalized)) {
      results.push(toApiSymbol(item));
    }
  });
  const codeMatch = normalized.match(/^(\d{6})(?:\.(SH|SZ|BJ))?$/);
  if (codeMatch) {
    const code = codeMatch[1];
    const exchange = codeMatch[2] ?? inferExchangeFromCode(code);
    results.push({
      symbol: `${code}.${exchange}`,
      code,
      exchange,
      name: knownItems.find((item) => item.symbol === `${code}.${exchange}`)?.name ?? code,
      asset_type: "stock",
    });
  }
  return mergeSearchResults([], results);
}

function mergeSearchResults(primary: ApiSymbol[], fallback: ApiSymbol[]): ApiSymbol[] {
  const merged = new Map<string, ApiSymbol>();
  [...primary, ...fallback].forEach((item) => {
    if (item?.symbol) {
      merged.set(item.symbol, item);
    }
  });
  return Array.from(merged.values()).slice(0, 20);
}

function inferExchangeFromCode(code: string): string {
  if (/^(4|8|920)/.test(code)) {
    return "BJ";
  }
  if (/^(6|9)/.test(code)) {
    return "SH";
  }
  return "SZ";
}

function directionOf(value?: number | null): "up" | "down" | "flat" {
  if (typeof value !== "number") {
    return "flat";
  }
  return value >= 0 ? "up" : "down";
}

function formatPrice(value?: number | null): string {
  return typeof value === "number" ? value.toFixed(2) : "--";
}

function formatSigned(value?: number | null): string {
  if (typeof value !== "number") {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}`;
}

function formatPercent(value?: number | null): string {
  if (typeof value !== "number") {
    return "--";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatNumber(value?: number | null): string {
  if (typeof value !== "number") {
    return "--";
  }
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatMoney(value?: number | null, signed = false): string {
  if (typeof value !== "number") {
    return "--";
  }
  const abs = Math.abs(value);
  const sign = signed && value > 0 ? "+" : value < 0 ? "-" : "";
  if (abs >= 100_000_000) {
    return `${sign}${formatCompact(abs / 100_000_000)}亿`;
  }
  if (abs >= 10_000) {
    return `${sign}${formatCompact(abs / 10_000)}万`;
  }
  return `${sign}${formatCompact(abs)}`;
}

function formatMarketCap(value?: number | null): string {
  return formatMoney(value);
}

function formatRatio(value?: number | null): string {
  if (typeof value !== "number") {
    return "--";
  }
  return value.toFixed(value >= 100 ? 0 : 2);
}

function formatCompact(value: number): string {
  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: value >= 100 ? 0 : 2,
  }).format(value);
}
