import { Newspaper, RefreshCcw } from "lucide-react";
import { useEffect, useState } from "react";
import { getMockStockNews, type NewsItem, type StockNewsFeed } from "../api/marketContracts";

type Props = {
  activeSymbol: string;
};

export function StockNewsPanel({ activeSymbol }: Props) {
  const [feed, setFeed] = useState<StockNewsFeed | null>(null);
  const [activeTab, setActiveTab] = useState<"stock" | "global">("stock");

  useEffect(() => {
    let cancelled = false;
    void getMockStockNews(activeSymbol).then((next) => {
      if (!cancelled) {
        setFeed(next);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [activeSymbol]);

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
      <div>
        <strong>{item.title}</strong>
        {item.summary ? <p>{item.summary}</p> : null}
      </div>
      <footer>
        <span>{item.source}</span>
        <span>{item.time}</span>
        {item.tags?.slice(0, 2).map((tag) => <em key={tag}>{tag}</em>)}
      </footer>
    </article>
  );
}
