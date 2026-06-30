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
  type WatchlistItem,
} from "../api/watchlistStore";
import {
  SCREENER_DOCK_FEATURES,
  type ScreenerTabId,
} from "../features/featureRegistry";

type Props = {
  onSelectSymbol(symbol: string): void;
};

type ScreenerTab = ScreenerTabId;

const DEFAULT_QUERY = "5日，15日，60日均线多头排列，当日股价突破前高";
const MIN_PANEL_HEIGHT = 240;
const MAX_PANEL_OFFSET = 96;

export function ScreenerDock({ onSelectSymbol }: Props) {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<ScreenerTab>("wencai");
  const [panelHeight, setPanelHeight] = useState(380);
  const [resizing, setResizing] = useState(false);
  const [query, setQuery] = useState(DEFAULT_QUERY);
  const [loading, setLoading] = useState(false);
  const [conditions, setConditions] = useState<string[]>([]);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [rows, setRows] = useState<WencaiScreenerRow[]>([]);
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

  const selectedRows = useMemo(
    () => rows.filter((row) => selected.includes(row.symbol)),
    [rows, selected],
  );
  const selectedChanRows = useMemo(
    () => chanRows.filter((row) => chanSelected.includes(row.symbol)),
    [chanRows, chanSelected],
  );
  const bodyStyleRef = useRef<{ cursor: string; userSelect: string } | null>(null);

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
    const trimmed = query.trim();
    if (!trimmed) {
      setMessage("请输入选股条件");
      return;
    }
    setOpen(true);
    setTab("wencai");
    setLoading(true);
    setMessage(null);
    try {
      const response = await queryWencaiScreener(trimmed);
      setConditions(response.conditions);
      setSuggestions(response.suggestions);
      setRows(response.rows);
      setSelected(response.rows.slice(0, 3).map((row) => row.symbol));
      setMessage(`已选出 ${response.total} 只 A 股`);
    } catch (error) {
      setRows([]);
      setSelected([]);
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
    setOpen(true);
    setTab("chan");
    setChanLoading(true);
    setChanMessage(null);
    try {
      const response = await queryChanScreener(trimmed, 100, "current");
      const enrichedItems = await enrichChanScreenerRows(response.items);
      setChanRows(enrichedItems);
      setChanSelected(enrichedItems.slice(0, 3).map((row) => row.symbol));
      setChanConditions(
        response.conditions.map((condition) => condition.raw || describeChanCondition(condition)),
      );
      setChanUnsupported(response.unsupported);
      setChanParser(response.parser);
      setChanMessage(
        `已选出 ${enrichedItems.length} 个标的，解析方式：${response.parser === "llm" ? "大模型" : "规则"}`,
      );
      if (response.parser_error) {
        setChanMessage(`已选出 ${enrichedItems.length} 个标的，模型解析失败后使用规则解析`);
      }
    } catch (error) {
      setChanRows([]);
      setChanSelected([]);
      setChanConditions([]);
      setChanUnsupported([]);
      setChanMessage(error instanceof Error ? error.message : "缠论选股失败");
    } finally {
      setChanLoading(false);
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
    addSymbolsToFavorites(items);
    setMessage(`已加入自选：${items.length} 只`);
  }

  function handleCreateBoard() {
    const items = toWatchlistItems(selectedRows);
    if (items.length === 0) {
      setMessage("请先选择标的");
      return;
    }
    const defaultName = `问财-${compactQuery(query)}`;
    const name = window.prompt("请输入新板块分组名称", defaultName);
    if (name === null) {
      return;
    }
    createGroupWithSymbols(name, items);
    setMessage(`已创建板块并加入：${items.length} 只`);
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

  function handleAddChanFavorites() {
    const items = toWatchlistItems(selectedChanRows);
    if (items.length === 0) {
      setChanMessage("请先选择标的");
      return;
    }
    addSymbolsToFavorites(items);
    setChanMessage(`已加入自选：${items.length} 只`);
  }

  function handleCreateChanBoard() {
    const items = toWatchlistItems(selectedChanRows);
    if (items.length === 0) {
      setChanMessage("请先选择标的");
      return;
    }
    const defaultName = `缠论-${compactQuery(chanQuery)}`;
    const name = window.prompt("请输入新板块分组名称", defaultName);
    if (name === null) {
      return;
    }
    createGroupWithSymbols(name, items);
    setChanMessage(`已创建板块并加入：${items.length} 只`);
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
                    <em>{rows.length}</em>
                    {message ? <span>{message}</span> : null}
                  </div>
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
                      <span>{row.price.toFixed(2)}</span>
                      <span data-direction={row.changePercent >= 0 ? "up" : "down"}>
                        {formatPercent(row.changePercent)}
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
                  <button type="button" onClick={handleAddChanFavorites}>
                    <Star size={15} />
                    <span>加自选</span>
                  </button>
                  <button type="button" onClick={handleCreateChanBoard}>
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
        <button
          type="button"
          data-active={tab === "wencai"}
          onClick={() => {
            setTab("wencai");
            setOpen(true);
          }}
          onDoubleClick={() => setOpen(false)}
        >
          <ScreenerTabIcon id="wencai" />
          <span>问财选股</span>
        </button>
        <button
          type="button"
          data-active={tab === "chan"}
          onClick={() => {
            setTab("chan");
            setOpen(true);
          }}
          onDoubleClick={() => setOpen(false)}
        >
          <ScreenerTabIcon id="chan" />
          <span>缠论选股</span>
        </button>
      </nav>
    </section>
  );
}

function ScreenerTabIcon({ id }: { id: ScreenerTab }) {
  const feature = SCREENER_DOCK_FEATURES.find((item) => item.id === id);
  const Icon = feature?.icon ?? Search;
  return <Icon size={16} />;
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
  return Promise.all(
    rows.map(async (row) => {
      try {
        const profileFields = await getSymbolProfileMarketFields(row.symbol, row.name);
        return {
          ...row,
          market: {
            ...row.market,
            industry: row.market.industry ?? profileFields.industry,
            fund_net_inflow:
              row.market.fund_net_inflow ?? profileFields.fundNetInflow,
          },
        };
      } catch {
        return row;
      }
    }),
  );
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
