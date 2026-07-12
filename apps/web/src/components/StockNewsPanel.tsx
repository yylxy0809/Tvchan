import { Newspaper, RefreshCcw } from "lucide-react";
import { useState } from "react";
import { type NewsItem, type StockNewsFeed } from "../api/marketContracts";

type Props = {
  activeSymbol: string;
  feed: StockNewsFeed | null;
};

export function StockNewsPanel({ activeSymbol, feed }: Props) {
  const [activeTab, setActiveTab] = useState<"stock" | "global">("stock");

  const items = activeTab === "stock" ? feed?.stockNews ?? [] : feed?.globalNews ?? [];

  return (
    <section className="tv-market-panel" aria-label="个股新闻">
      <header className="tv-market-panel-header">
        <div>
          <span>资讯</span>
          <strong>个股新闻</strong>
        </div>
        <Newspaper size={20} strokeWidth={1.7} />
      </header>

      <div className="tv-market-tabs" role="tablist" aria-label="新闻类型">
        <button
          type="button"
          data-active={activeTab === "stock"}
          onClick={() => setActiveTab("stock")}
        >
          个股
        </button>
        <button
          type="button"
          data-active={activeTab === "global"}
          onClick={() => setActiveTab("global")}
        >
          全球
        </button>
      </div>

      <div className="tv-market-meta">
        <span>{activeSymbol}</span>
        <span>{feed?.asOf.slice(11, 16) ?? "--"}</span>
        <RefreshCcw size={13} />
      </div>

      <div className="tv-news-list">
        {items.length === 0 ? (
          <div className="tv-panel-empty compact">暂无资讯</div>
        ) : null}
        {items.map((item) => (
          <NewsRow key={item.id} item={item} />
        ))}
      </div>
    </section>
  );
}

function NewsRow({ item }: { item: NewsItem }) {
  return (
    <article className="tv-news-row">
      {item.url ? (
        <a className="tv-news-row-title" href={item.url} target="_blank" rel="noopener noreferrer">
          {item.title}
        </a>
      ) : (
        <strong className="tv-news-row-title">{item.title}</strong>
      )}
      {item.relatedSymbols?.length ? (
        <div className="tv-news-related-symbols" aria-label="相关新闻标的">
          {item.relatedSymbols.map((related) => (
            <span
              className="tv-news-symbol-chip"
              data-direction={related.changePercent === null ? "flat" : related.changePercent >= 0 ? "up" : "down"}
              key={related.symbol}
            >
              <b>{related.symbol}</b>
              <span>{formatChangePercent(related.changePercent)}</span>
            </span>
          ))}
        </div>
      ) : null}
      <footer>
        <span>{item.source}</span>
        <time dateTime={item.time}>{formatNewsTime(item.time)}</time>
      </footer>
    </article>
  );
}

function formatChangePercent(value: number | null): string {
  if (value === null) return "--";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatNewsTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}
