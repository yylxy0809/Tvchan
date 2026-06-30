export type WatchlistItem = {
  symbol: string;
  name: string;
  exchange: string;
};

export type WatchlistGroup = {
  id: string;
  name: string;
  items: WatchlistItem[];
};

export const WATCHLIST_STORAGE_KEY = "tv-a-share-watchlist-groups";
export const WATCHLIST_UPDATED_EVENT = "tv-a-share-watchlist-updated";
export const FAVORITES_GROUP_ID = "favorites";

export const DEFAULT_WATCHLIST_GROUPS: WatchlistGroup[] = [
  {
    id: FAVORITES_GROUP_ID,
    name: "自选",
    items: [{ symbol: "000001.SZ", name: "平安银行", exchange: "SZ" }],
  },
];

export function isFixedWatchlistGroup(groupId: string): boolean {
  return groupId === FAVORITES_GROUP_ID;
}

export function readWatchlistGroups(): WatchlistGroup[] {
  if (typeof window === "undefined") {
    return DEFAULT_WATCHLIST_GROUPS;
  }
  try {
    const raw = window.localStorage.getItem(WATCHLIST_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_WATCHLIST_GROUPS;
    }
    return reviveWatchlistGroups(JSON.parse(raw) as unknown);
  } catch {
    return DEFAULT_WATCHLIST_GROUPS;
  }
}

export function writeWatchlistGroups(groups: WatchlistGroup[]): WatchlistGroup[] {
  const next = reviveWatchlistGroups(groups);
  if (typeof window !== "undefined") {
    window.localStorage.setItem(WATCHLIST_STORAGE_KEY, JSON.stringify(next));
    window.dispatchEvent(
      new CustomEvent(WATCHLIST_UPDATED_EVENT, { detail: { groups: next } }),
    );
  }
  return next;
}

export function addSymbolsToFavorites(items: WatchlistItem[]): WatchlistGroup[] {
  const groups = readWatchlistGroups();
  const next = groups.map((group) =>
    group.id === FAVORITES_GROUP_ID
      ? { ...group, items: mergeItems(group.items, items) }
      : group,
  );
  return writeWatchlistGroups(next);
}

export function createGroupWithSymbols(
  name: string,
  items: WatchlistItem[],
): WatchlistGroup[] {
  const trimmed = name.trim() || "问财板块";
  const groups = readWatchlistGroups();
  const nextGroup: WatchlistGroup = {
    id: createWatchlistId("wencai"),
    name: trimmed,
    items: mergeItems([], items),
  };
  return writeWatchlistGroups([...groups, nextGroup]);
}

export function reviveWatchlistGroups(value: unknown): WatchlistGroup[] {
  const groups = Array.isArray(value)
    ? value
        .map((group) => reviveGroup(group))
        .filter((group): group is WatchlistGroup => group !== null)
    : [];
  const withFavorites = ensureFavoritesGroup(groups);
  return withFavorites.length > 0 ? withFavorites : DEFAULT_WATCHLIST_GROUPS;
}

export function createWatchlistId(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}

function reviveGroup(value: unknown): WatchlistGroup | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as Partial<WatchlistGroup>;
  const id = typeof record.id === "string" ? record.id : createWatchlistId("watchlist");
  return {
    id,
    name:
      id === FAVORITES_GROUP_ID
        ? "自选"
        : typeof record.name === "string"
          ? normalizeGroupName(record.name)
          : "列表",
    items: Array.isArray(record.items)
      ? record.items
          .filter((item): item is WatchlistItem => {
            return (
              !!item &&
              typeof item === "object" &&
              typeof (item as WatchlistItem).symbol === "string"
            );
          })
          .map((item) => ({
            symbol: item.symbol.toUpperCase(),
            name: normalizeSymbolName(item.name || item.symbol),
            exchange: item.exchange || inferExchangeFromSymbol(item.symbol),
          }))
      : [],
  };
}

function ensureFavoritesGroup(groups: WatchlistGroup[]): WatchlistGroup[] {
  const favorites = groups.find((group) => group.id === FAVORITES_GROUP_ID);
  const legacyDefault = groups.find((group) => group.id === "default");
  const favoriteItems = mergeItems(
    DEFAULT_WATCHLIST_GROUPS[0].items,
    mergeItems(favorites?.items ?? [], legacyDefault?.items ?? []),
  );
  const normalizedFavorites: WatchlistGroup = {
    id: FAVORITES_GROUP_ID,
    name: "自选",
    items: favoriteItems,
  };
  return [
    normalizedFavorites,
    ...groups.filter(
      (group) => group.id !== FAVORITES_GROUP_ID && group.id !== "default",
    ),
  ];
}

function mergeItems(
  existing: WatchlistItem[],
  incoming: WatchlistItem[],
): WatchlistItem[] {
  const merged = new Map<string, WatchlistItem>();
  [...existing, ...incoming].forEach((item) => {
    const symbol = item.symbol.toUpperCase();
    merged.set(symbol, {
      symbol,
      name: normalizeSymbolName(item.name || symbol),
      exchange: item.exchange || inferExchangeFromSymbol(symbol),
    });
  });
  return Array.from(merged.values());
}

function normalizeGroupName(name: string): string {
  if (name === "A Shares" || name === "A股关注") {
    return "A股关注";
  }
  return name;
}

function normalizeSymbolName(name: string): string {
  return name === "Ping An Bank" ? "平安银行" : name;
}

function inferExchangeFromSymbol(symbol: string): string {
  const suffix = symbol.split(".")[1];
  if (suffix) {
    return suffix.toUpperCase();
  }
  const code = symbol.split(".")[0];
  if (/^(4|8|920)/.test(code)) {
    return "BJ";
  }
  if (/^(6|9)/.test(code)) {
    return "SH";
  }
  return "SZ";
}
