import {
  GripHorizontal,
  Loader2,
  Plus,
  Search,
  Star,
  X,
} from "lucide-react";
import {
  type CSSProperties,
  type FormEvent,
  useMemo,
  useEffect,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import {
  type WencaiScreenerRow,
  queryWencaiScreener,
} from "../api/wencaiScreener";
import {
  type ChanScreenerCondition,
  type ChanScreenerRow,
  queryChanScreener,
} from "../api/chanScreener";
import { getSymbolProfileMarketFields } from "../api/marketData";
import {
  addSymbolsToFavorites,
  createGroupWithSymbols,
  type WatchlistGroup,
  type WatchlistItem,
} from "../api/watchlistStore";
import { saveUserSetting } from "../api/userSettings";
import {
  type ScreenerTabId,
} from "../features/featureRegistry";
import { useScreenerDockFeatures } from "../features/runtimeFeatureRegistry";

type Props = {
  onSelectSymbol(symbol: string): void;
  onOpenPanel?(): void;
  authToken?: string;
};

type ScreenerTab = ScreenerTabId;

const DEFAULT_QUERY = "5日，15日，60日均线多头排列，当日股价突破前高";
const MIN_PANEL_HEIGHT = 240;
const MAX_PANEL_OFFSET = 96;

export function ScreenerDock({ onSelectSymbol, onOpenPanel, authToken }: Props) {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<ScreenerTab>("wencai");
  const [panelHeight, setPanelHeight] = useState(380);
  const [resizing, setResizing] = useState(false);
  const [query, setQuery] = useState(DEFAULT_QUERY);
  const [loading, setLoading] = useState(false);
  const [conditions, setConditions] = useState<string[]>([]);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [rows, setRows] = useState<WencaiScreenerRow[]>([]);
  const [wencaiPage, setWencaiPage] = useState(1);
  const [wencaiPageSize, setWencaiPageSize] = useState(50);
  const [wencaiTotal, setWencaiTotal] = useState(0);
  const [selected, setSelected] = useState<string[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [chanQuery, setChanQuery] = useState("日线级别趋势上涨中，30f级别盘整下跌，5f级别线段上涨");
  const [chanLoading, setChanLoading] = useState(false);
  const [chanRows, setChanRows] = useState<ChanScreenerRow[]>([]);
  const [chanSelected, setChanSelected] = useState<string[]>([]);
  const [chanConditions, setChanConditions] = useState<string[]>([]);
  const [chanUnsupported, setChanUnsupported] = useState<string[]>([]);
  const [chanMessage, setChanMessage] = useState<string | null>(null);
  const [chanParser, setChanParser] = useState<string>("rules");
  const screenerFeatures = useScreenerDockFeatures();
  const chanRequestIdRef = useRef(0);

  const selectedRows = useMemo(
    () => rows.filter((row) => selected.includes(row.symbol)),
    [rows, selected],
  );
  const selectedChanRows = useMemo(
    () => chanRows.filter((row) => chanSelected.includes(row.symbol)),
    [chanRows, chanSelected],
  );
  const wencaiTotalPages = Math.max(1, Math.ceil(wencaiTotal / wencaiPageSize));
  const bodyStyleRef = useRef<{ cursor: string; userSelect: string } | null>(null);

  useEffect(() => {
    if (screenerFeatures.some((feature) => feature.id === tab)) {
      return;
    }
    const nextTab = screenerFeatures[0]?.id;
    if (nextTab) {
      setTab(nextTab);
    } else {
      setOpen(false);
    }
  }, [screenerFeatures, tab]);

  function openScreenerPanel(nextTab: ScreenerTab) {
    setTab(nextTab);
    setOpen(true);
    onOpenPanel?.();
  }

  function updatePanelHeightFromClientY(clientY: number) {
    const maxHeight = Math.max(
      MIN_PANEL_HEIGHT,
      window.innerHeight - MAX_PANEL_OFFSET,
    );
    const nextHeight = window.innerHeight - 52 - clientY;
    setPanelHeight(Math.round(Math.min(maxHeight, Math.max(MIN_PANEL_HEIGHT, nextHeight))));
  }

  function handleResizeStart(event: ReactPointerEvent<HTMLDivElement>) {
    event.preventDefault();
    event.currentTarget.setPointerCapture(event.pointerId);
    bodyStyleRef.current = {
      cursor: document.body.style.cursor,
      userSelect: document.body.style.userSelect,
    };
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";
    setResizing(true);
    updatePanelHeightFromClientY(event.clientY);
  }

  function handleResizeMove(event: ReactPointerEvent<HTMLDivElement>) {
    if (!resizing) {
      return;
    }
    updatePanelHeightFromClientY(event.clientY);
  }

  function handleResizeDone(event: ReactPointerEvent<HTMLDivElement>) {
    if (!resizing) {
      return;
    }
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    const previous = bodyStyleRef.current;
    if (previous) {
      document.body.style.cursor = previous.cursor;
      document.body.style.userSelect = previous.userSelect;
      bodyStyleRef.current = null;
    }
    setResizing(false);
  }

  async function handleSubmit(event?: FormEvent<HTMLFormElement>) {
    event?.preventDefault();
    await runWencaiQuery(1, wencaiPageSize);
  }

  async function runWencaiQuery(nextPage: number, nextPageSize: number) {
    const trimmed = query.trim();
    if (!trimmed) {
      setMessage("请输入选股条件");
      return;
    }
    openScreenerPanel("wencai");
    setLoading(true);
    setMessage(null);
    try {
      const response = await queryWencaiScreener(trimmed, nextPage, nextPageSize);
      setConditions(response.conditions);
      setSuggestions(response.suggestions);
      setRows(response.rows);
      setWencaiPage(response.page);
      setWencaiPageSize(response.pageSize);
      setWencaiTotal(response.total);
      setSelected(response.rows.slice(0, 3).map((row) => row.symbol));
      setMessage(`已选出 ${response.total} 只 A 股`);
    } catch (error) {
      setRows([]);
      setSelected([]);
      setWencaiTotal(0);
      setMessage(error instanceof Error ? error.message : "问财选股失败");
    } finally {
      setLoading(false);
    }
  }

  async function handleChanSubmit(event?: FormEvent<HTMLFormElement>) {
    event?.preventDefault();
    const trimmed = chanQuery.trim();
    if (!trimmed) {
      setChanMessage("请输入缠论选股条件");
      return;
    }
    openScreenerPanel("chan");
    setChanLoading(true);
    setChanMessage(null);
    const requestId = chanRequestIdRef.current + 1;
    chanRequestIdRef.current = requestId;
    try {
      const response = await queryChanScreener(trimmed, 100, "current");
      if (chanRequestIdRef.current !== requestId) {
        return;
      }
      setChanRows(response.items);
      setChanSelected(response.items.slice(0, 3).map((row) => row.symbol));
      setChanConditions(
        response.conditions.map((condition) => condition.raw || describeChanCondition(condition)),
      );
      setChanUnsupported(response.unsupported);
      setChanParser(response.parser);
      setChanMessage(
        `已选出 ${response.items.length} 个标的，解析方式：${response.parser === "llm" ? "大模型" : "规则"}`,
      );
      if (response.parser_error) {
        setChanMessage(`已选出 ${response.items.length} 个标的，模型解析失败后使用规则解析`);
      }
      void enrichChanScreenerRows(response.items).then((enrichedItems) => {
        if (chanRequestIdRef.current === requestId) {
          setChanRows(enrichedItems);
        }
      });
    } catch (error) {
      if (chanRequestIdRef.current !== requestId) {
        return;
      }
      setChanRows([]);
      setChanSelected([]);
      setChanConditions([]);
      setChanUnsupported([]);
      setChanMessage(error instanceof Error ? error.message : "缠论选股失败");
    } finally {
      if (chanRequestIdRef.current === requestId) {
        setChanLoading(false);
      }
    }
  }

  function toggleSelected(symbol: string) {
    setSelected((current) =>
      current.includes(symbol)
        ? current.filter((item) => item !== symbol)
        : [...current, symbol],
    );
  }

  function toggleAll() {
    setSelected((current) =>
      current.length === rows.length ? [] : rows.map((row) => row.symbol),
    );
  }

  function handleAddFavorites() {
    const items = toWatchlistItems(selectedRows);
    if (items.length === 0) {
      setMessage("请先选择标的");
      return;
    }
    const groups = addSymbolsToFavorites(items);
    syncWatchlistGroups(groups, `已加入自选：${items.length} 只，可在右侧关注列表查看`, setMessage);
  }

  function handleCreateBoard() {
    const items = toWatchlistItems(selectedRows);
    if (items.length === 0) {
      setMessage("请先选择标的");
      return;
    }
    const groups = createGroupWithSymbols(`问财-${compactQuery(query)}`, items);
    syncWatchlistGroups(groups, `已创建板块并加入：${items.length} 只，可在右侧关注列表查看`, setMessage);
  }

  function toggleChanSelected(symbol: string) {
    setChanSelected((current) =>
      current.includes(symbol)
        ? current.filter((item) => item !== symbol)
        : [...current, symbol],
    );
  }

  function toggleAllChan() {
    setChanSelected((current) =>
      current.length === chanRows.length ? [] : chanRows.map((row) => row.symbol),
    );
  }

  async function handleAddChanFavorites() {
    const resolvedRows = await resolveSelectedChanRowsForWatchlist();
    const items = toWatchlistItems(resolvedRows);
    if (items.length === 0) {
      setChanMessage("请先选择标的");
      return;
    }
    const groups = addSymbolsToFavorites(items);
    syncWatchlistGroups(groups, `已加入自选：${items.length} 只，可在右侧关注列表查看`, setChanMessage);
  }

  async function handleCreateChanBoard() {
    const resolvedRows = await resolveSelectedChanRowsForWatchlist();
    const items = toWatchlistItems(resolvedRows);
    if (items.length === 0) {
      setChanMessage("请先选择标的");
      return;
    }
    const groups = createGroupWithSymbols(`缠论-${compactQuery(chanQuery)}`, items);
    syncWatchlistGroups(groups, `已创建板块并加入：${items.length} 只，可在右侧关注列表查看`, setChanMessage);
  }

  async function resolveSelectedChanRowsForWatchlist(): Promise<ChanScreenerRow[]> {
    if (selectedChanRows.length === 0 || !selectedChanRows.some(isChanNameFallback)) {
      return selectedChanRows;
    }
    setChanMessage("正在补全标的名称...");
    return enrichChanScreenerRows(selectedChanRows);
  }

  function syncWatchlistGroups(
    groups: WatchlistGroup[],
    successMessage: string,
    setStatus: (message: string) => void,
  ) {
    setStatus(successMessage);
    if (!authToken) {
      return;
    }
    void saveUserSetting(authToken, "watchlist", { groups }).catch(() => {
      setStatus(`${successMessage}；服务端同步失败，已保存在本机`);
    });
  }

  return (
    <section
      className="screener-dock"
      data-open={open}
      aria-label="底部选股"
      style={
        {
          "--screener-panel-height": `${Math.round(panelHeight)}px`,
        } as CSSProperties
      }
    >
      {open ? (
        <div className="screener-body">
          <div
            className="screener-resizer"
            role="separator"
            aria-label="调整选股面板高度"
            aria-orientation="horizontal"
            tabIndex={0}
            data-resizing={resizing}
            onPointerDown={handleResizeStart}
            onPointerMove={handleResizeMove}
            onPointerUp={handleResizeDone}
            onPointerCancel={handleResizeDone}
          >
            <GripHorizontal size={18} />
          </div>
          <div className="screener-panel-content">
            {tab === "wencai" ? (
              <>
                <form className="wencai-query" onSubmit={handleSubmit}>
                  <input
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                    placeholder="输入问财自然语言条件，例如：5日、15日、60日均线多头排列，当日股价突破前高"
                  />
                  <button type="submit" disabled={loading}>
                    {loading ? <Loader2 size={16} className="spin" /> : <Search size={16} />}
                    <span>执行选股</span>
                  </button>
                  <button
                    type="button"
                    title="清空"
                    onClick={() => {
                      setQuery("");
                      setRows([]);
                      setSelected([]);
                      setConditions([]);
                      setWencaiPage(1);
                      setWencaiTotal(0);
                      setMessage(null);
                    }}
                  >
                    <X size={16} />
                  </button>
                </form>

                <div className="wencai-conditions" aria-label="已选条件">
                  <strong>已选条件</strong>
                  {conditions.map((condition) => (
                    <span key={condition}>{condition}</span>
                  ))}
                </div>

                <div className="wencai-suggestions" aria-label="智能推荐条件">
                  <strong>智能推荐</strong>
                  {suggestions.map((suggestion) => (
                    <button
                      key={suggestion}
                      type="button"
                      onClick={() =>
                        setQuery((current) =>
                          current.trim() ? `${current}，${suggestion}` : suggestion,
                        )
                      }
                    >
                      {suggestion}
                      <Plus size={12} />
                    </button>
                  ))}
                </div>

                <div className="wencai-result-toolbar">
                  <div>
                    <strong>选出A股</strong>
                    <em>{wencaiTotal}</em>
                    <span>
                      第 {wencaiPage} / {wencaiTotalPages} 页，本页 {rows.length} 条
                    </span>
                    {message ? <span>{message}</span> : null}
                  </div>
                  <select
                    value={wencaiPageSize}
                    onChange={(event) => {
                      const nextPageSize = Number(event.target.value);
                      void runWencaiQuery(1, nextPageSize);
                    }}
                    disabled={loading}
                    aria-label="问财每页数量"
                  >
                    <option value={20}>20 / 页</option>
                    <option value={50}>50 / 页</option>
                    <option value={100}>100 / 页</option>
                  </select>
                  <button
                    type="button"
                    onClick={() => void runWencaiQuery(wencaiPage - 1, wencaiPageSize)}
                    disabled={loading || wencaiPage <= 1}
                  >
                    上一页
                  </button>
                  <button
                    type="button"
                    onClick={() => void runWencaiQuery(wencaiPage + 1, wencaiPageSize)}
                    disabled={loading || wencaiPage >= wencaiTotalPages}
                  >
                    下一页
                  </button>
                  <button type="button" onClick={handleAddFavorites}>
                    <Star size={15} />
                    <span>加自选</span>
                  </button>
                  <button type="button" onClick={handleCreateBoard}>
                    <Plus size={15} />
                    <span>加板块</span>
                  </button>
                </div>

                <div className="wencai-table" role="table" aria-label="问财选股结果">
                  <div className="wencai-row wencai-head" role="row">
                    <button type="button" onClick={toggleAll}>
                      {selected.length === rows.length && rows.length > 0 ? "取消" : "全选"}
                    </button>
                    <span>股票代码</span>
                    <span>股票简称</span>
                    <span>现价</span>
                    <span>涨跌幅</span>
                    <span>买入信号</span>
                    <span>技术形态</span>
                    <span>条件说明</span>
                  </div>
                  {rows.length === 0 ? (
                    <div className="wencai-empty">输入条件后执行选股</div>
                  ) : null}
                  {rows.map((row, index) => (
                    <div
                      key={row.symbol}
                      className="wencai-row"
                      role="row"
                      data-selected={selected.includes(row.symbol)}
                    >
                      <label>
                        <input
                          type="checkbox"
                          checked={selected.includes(row.symbol)}
                          onChange={() => toggleSelected(row.symbol)}
                        />
                        <small>{index + 1}</small>
                      </label>
                      <button type="button" onClick={() => onSelectSymbol(row.symbol)}>
                        {row.code}
                      </button>
                      <strong>{row.name}</strong>
                      <span>{formatNullablePrice(row.price)}</span>
                      <span data-direction={directionOf(row.changePercent)}>
                        {formatNullablePercent(row.changePercent)}
                      </span>
                      <span>{row.buySignal}</span>
                      <span>{row.technicalShape}</span>
                      <span>{row.reason}；{row.highBreakReason}</span>
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <>
                <form className="wencai-query" onSubmit={handleChanSubmit}>
                  <input
                    value={chanQuery}
                    onChange={(event) => setChanQuery(event.target.value)}
                    placeholder="输入缠论自然语言条件，例如：日线趋势上涨，30f盘整下跌，5f线段上涨"
                  />
                  <button type="submit" disabled={chanLoading}>
                    {chanLoading ? <Loader2 size={16} className="spin" /> : <Search size={16} />}
                    <span>执行选股</span>
                  </button>
                  <button
                    type="button"
                    title="清空"
                    onClick={() => {
                      setChanQuery("");
                      setChanRows([]);
                      setChanSelected([]);
                      setChanConditions([]);
                      setChanUnsupported([]);
                      setChanMessage(null);
                    }}
                  >
                    <X size={16} />
                  </button>
                </form>

                <div className="wencai-conditions" aria-label="缠论解析条件">
                  <strong>解析条件</strong>
                  {chanConditions.length === 0 ? <span>输入自然语言后执行</span> : null}
                  {chanConditions.map((condition) => (
                    <span key={condition}>{condition}</span>
                  ))}
                </div>

                <div className="wencai-suggestions" aria-label="缠论示例条件">
                  <strong>常用条件</strong>
                  {[
                    "日线趋势上涨",
                    "30f盘整下跌",
                    "5f线段上涨",
                    "日线向上一笔进行中",
                    "30f级别类2买",
                  ].map((suggestion) => (
                    <button
                      key={suggestion}
                      type="button"
                      onClick={() =>
                        setChanQuery((current) =>
                          current.trim() ? `${current}，${suggestion}` : suggestion,
                        )
                      }
                    >
                      {suggestion}
                      <Plus size={12} />
                    </button>
                  ))}
                </div>

                {chanUnsupported.length > 0 ? (
                  <div className="wencai-conditions" aria-label="暂未支持条件">
                    <strong>待补充状态</strong>
                    {chanUnsupported.map((item) => (
                      <span key={item}>{item}</span>
                    ))}
                  </div>
                ) : null}

                <div className="wencai-result-toolbar">
                  <div>
                    <strong>缠论结果</strong>
                    <em>{chanRows.length}</em>
                    {chanMessage ? <span>{chanMessage}</span> : null}
                    <span>解析：{chanParser === "llm" ? "大模型" : "规则"}</span>
                  </div>
                  <button type="button" onClick={() => void handleAddChanFavorites()}>
                    <Star size={15} />
                    <span>加自选</span>
                  </button>
                  <button type="button" onClick={() => void handleCreateChanBoard()}>
                    <Plus size={15} />
                    <span>加板块</span>
                  </button>
                </div>

                <div className="wencai-table chan-screener-table" role="table" aria-label="缠论选股结果">
                  <div className="wencai-row chan-row wencai-head" role="row">
                    <button type="button" onClick={toggleAllChan}>
                      {chanSelected.length === chanRows.length && chanRows.length > 0 ? "取消" : "全选"}
                    </button>
                    <span>代码</span>
                    <span>名称</span>
                    <span>走势状态</span>
                    <span>笔状态</span>
                    <span>线段状态</span>
                    <span>现价</span>
                    <span>涨跌幅</span>
                    <span>行业</span>
                    <span>资金净流入</span>
                  </div>
                  {chanRows.length === 0 ? (
                    <div className="wencai-empty">输入缠论条件后执行选股</div>
                  ) : null}
                  {chanRows.map((row, index) => (
                    <div
                      key={row.symbol}
                      className="wencai-row chan-row"
                      role="row"
                      data-selected={chanSelected.includes(row.symbol)}
                    >
                      <label>
                        <input
                          type="checkbox"
                          checked={chanSelected.includes(row.symbol)}
                          onChange={() => toggleChanSelected(row.symbol)}
                        />
                        <small>{index + 1}</small>
                      </label>
                      <button type="button" onClick={() => onSelectSymbol(row.symbol)}>
                        {row.code}
                      </button>
                      <strong>{row.name}</strong>
                      <span>{joinStatusMap(row.trend_status, ["1d", "30f", "5f"])}</span>
                      <span>{joinStatusMap(row.stroke_states, ["1m", "1w", "1d", "30f", "5f"])}</span>
                      <span>{joinStatusMap(row.segment_states, ["1m", "1w", "1d", "30f", "5f"])}</span>
                      <span>{formatNullablePrice(row.market.price)}</span>
                      <span data-direction={directionOf(row.market.change_percent)}>
                        {formatNullablePercent(row.market.change_percent)}
                      </span>
                      <span>{row.market.industry || "--"}</span>
                      <span>{formatMoney(row.market.fund_net_inflow)}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        </div>
      ) : null}

      <nav className="screener-tabs" aria-label="底部选股工具栏">
        {screenerFeatures.map((feature) => {
          const Icon = feature.icon;
          return (
            <button
              key={feature.id}
              type="button"
              data-active={tab === feature.id}
              onClick={() => {
                openScreenerPanel(feature.id);
              }}
              onDoubleClick={() => setOpen(false)}
            >
              <Icon size={16} />
              <span>{feature.title}</span>
            </button>
          );
        })}
      </nav>
    </section>
  );
}

function toWatchlistItems(
  rows: Array<Pick<WatchlistItem, "symbol" | "name" | "exchange">>,
): WatchlistItem[] {
  return rows.map((row) => ({
    symbol: row.symbol,
    name: row.name,
    exchange: row.exchange,
  }));
}

async function enrichChanScreenerRows(
  rows: ChanScreenerRow[],
): Promise<ChanScreenerRow[]> {
  const wencaiRows = await loadWencaiRowsForChanSymbols(rows);
  return Promise.all(
    rows.map(async (row) => {
      const wencaiRow = wencaiRows.get(row.symbol.toUpperCase());
      const resolvedName = resolveChanRowName(row, wencaiRow);
      try {
        const profileFields = await getSymbolProfileMarketFields(row.symbol, resolvedName);
        return {
          ...row,
          name: resolvedName,
          market: {
            ...row.market,
            price: row.market.price ?? wencaiRow?.price ?? null,
            change_percent: row.market.change_percent ?? wencaiRow?.changePercent ?? null,
            industry: row.market.industry ?? profileFields.industry,
            fund_net_inflow:
              row.market.fund_net_inflow ?? profileFields.fundNetInflow,
          },
        };
      } catch {
        return {
          ...row,
          name: resolvedName,
          market: {
            ...row.market,
            price: row.market.price ?? wencaiRow?.price ?? null,
            change_percent: row.market.change_percent ?? wencaiRow?.changePercent ?? null,
          },
        };
      }
    }),
  );
}

async function loadWencaiRowsForChanSymbols(
  rows: ChanScreenerRow[],
): Promise<Map<string, WencaiScreenerRow>> {
  const pending = rows.filter(isChanNameFallback);
  if (pending.length === 0) {
    return new Map();
  }
  const chunks = chunkCodesForWencaiQuery(pending.map((row) => row.code));
  const responses = await Promise.allSettled(
    chunks.map((codes) =>
      queryWencaiScreener(`${codes.join(" 或 ")} 股票简称 最新价 涨跌幅`, 1, 100),
    ),
  );
  const result = new Map<string, WencaiScreenerRow>();
  responses.forEach((response) => {
    if (response.status !== "fulfilled") {
      return;
    }
    response.value.rows.forEach((row) => {
      result.set(row.symbol.toUpperCase(), row);
    });
  });
  return result;
}

function resolveChanRowName(
  row: ChanScreenerRow,
  wencaiRow: WencaiScreenerRow | undefined,
): string {
  if (wencaiRow && !isCodeLikeName(wencaiRow.name, row.code, row.symbol)) {
    return wencaiRow.name;
  }
  return row.name || row.code;
}

function isChanNameFallback(row: ChanScreenerRow): boolean {
  return isCodeLikeName(row.name, row.code, row.symbol);
}

function isCodeLikeName(name: string | undefined, code: string, symbol: string): boolean {
  const normalized = (name ?? "").trim().toUpperCase();
  return !normalized || normalized === code.toUpperCase() || normalized === symbol.toUpperCase();
}

function chunkCodesForWencaiQuery(codes: string[]): string[][] {
  const chunks: string[][] = [];
  let current: string[] = [];
  codes.forEach((code) => {
    const candidate = [...current, code];
    const query = `${candidate.join(" 或 ")} 股票简称 最新价 涨跌幅`;
    if (current.length > 0 && query.length > 480) {
      chunks.push(current);
      current = [code];
    } else {
      current = candidate;
    }
  });
  if (current.length > 0) {
    chunks.push(current);
  }
  return chunks;
}

function compactQuery(query: string): string {
  const normalized = query.replace(/\s+/g, "").replace(/[，,、]/g, "-");
  return normalized.slice(0, 12) || "选股";
}

function formatPercent(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function describeChanCondition(condition: ChanScreenerCondition): string {
  const level = {
    "1m": "月线",
    "1w": "周线",
    "1d": "日线",
    "30f": "30f",
    "5f": "5f",
  }[condition.level] || condition.level;
  const direction = condition.direction === "up" ? "上" : condition.direction === "down" ? "下" : "";
  if (condition.kind === "structure") {
    const structure = {
      trend: "趋势",
      consolidation: "盘整",
      no_center: "无中枢",
    }[condition.value || ""] || condition.value || "走势";
    return `${level}${structure}${direction}`;
  }
  if (condition.kind === "stroke") {
    return `${level}笔${direction}`;
  }
  if (condition.kind === "segment") {
    return `${level}线段${direction}`;
  }
  if (condition.kind === "signal") {
    return `${level}${condition.value || "买卖点"}`;
  }
  return condition.raw || `${level}${condition.kind}`;
}

function joinStatusMap(map: Record<string, string | null>, levels: string[]): string {
  const values = levels.map((level) => map[level]).filter(Boolean);
  return values.length > 0 ? values.join(" / ") : "--";
}

function formatNullablePrice(value: number | null): string {
  return typeof value === "number" ? value.toFixed(2) : "--";
}

function formatNullablePercent(value: number | null): string {
  return typeof value === "number" ? formatPercent(value) : "--";
}

function directionOf(value: number | null): "up" | "down" | "flat" {
  if (typeof value !== "number" || value === 0) {
    return "flat";
  }
  return value > 0 ? "up" : "down";
}

function formatMoney(value: number | null): string {
  if (typeof value !== "number") {
    return "--";
  }
  const abs = Math.abs(value);
  if (abs >= 100000000) {
    return `${(value / 100000000).toFixed(2)}亿`;
  }
  if (abs >= 10000) {
    return `${(value / 10000).toFixed(2)}万`;
  }
  return value.toFixed(0);
}
