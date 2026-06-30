import { Flame, TrendingUp } from "lucide-react";
import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import {
  getMockStrongestToday,
  type DragonTigerItem,
  type IndustryCompareItem,
  type StrongStock,
  type StrongestToday,
  type StrongTheme,
} from "../api/marketContracts";

export function StrongestTodayPanel() {
  const [data, setData] = useState<StrongestToday | null>(null);

  useEffect(() => {
    let cancelled = false;
    void getMockStrongestToday().then((next) => {
      if (!cancelled) {
        setData(next);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section className="tv-market-panel" aria-label="今日最强">
      <header className="tv-market-panel-header">
        <div>
          <span>市场强度</span>
          <strong>今日最强</strong>
        </div>
        <Flame size={20} strokeWidth={1.7} />
      </header>

      <div className="tv-strength-summary">
        <div>
          <span>北向净流入</span>
          <strong data-direction={directionOf(data?.northbound.netInflow ?? null)}>
            {formatMoney(data?.northbound.netInflow ?? null, true)}
          </strong>
        </div>
        <div>
          <span>更新时间</span>
          <strong>{data?.asOf.slice(11, 16) ?? "--"}</strong>
        </div>
      </div>

      <PanelBlock title="强势股" icon={<TrendingUp size={15} />}>
        {(data?.hotStocks ?? []).slice(0, 5).map((item) => (
          <HotStockRow key={item.symbol} item={item} />
        ))}
      </PanelBlock>

      <PanelBlock title="题材归因">
        {(data?.themes ?? []).slice(0, 4).map((item) => (
          <ThemeRow key={item.name} item={item} />
        ))}
      </PanelBlock>

      <PanelBlock title="龙虎榜">
        {(data?.dragonTiger ?? []).slice(0, 4).map((item) => (
          <DragonTigerRow key={`${item.symbol}-${item.reason}`} item={item} />
        ))}
      </PanelBlock>

      <PanelBlock title="行业对比">
        {(data?.industryCompare ?? []).slice(0, 5).map((item) => (
          <IndustryRow key={item.industry} item={item} />
        ))}
      </PanelBlock>
    </section>
  );
}

function PanelBlock({
  title,
  icon,
  children,
}: {
  title: string;
  icon?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="tv-market-block">
      <h3>
        <span>{title}</span>
        {icon}
      </h3>
      <div>{children}</div>
    </section>
  );
}

function HotStockRow({ item }: { item: StrongStock }) {
  return (
    <div className="tv-strength-row">
      <div>
        <strong>{item.symbol}</strong>
        <span>{item.name}</span>
      </div>
      <div>
        <em data-direction={directionOf(item.changePercent)}>
          {formatPercent(item.changePercent)}
        </em>
        <small>{item.theme}</small>
      </div>
      <p>{item.reason}</p>
    </div>
  );
}

function ThemeRow({ item }: { item: StrongTheme }) {
  return (
    <div className="tv-compact-row">
      <span>{item.name}</span>
      <strong data-direction={directionOf(item.changePercent)}>
        {formatPercent(item.changePercent)}
      </strong>
      <small>{formatMoney(item.netInflow, true)}</small>
    </div>
  );
}

function DragonTigerRow({ item }: { item: DragonTigerItem }) {
  return (
    <div className="tv-compact-row">
      <span>{item.name}</span>
      <strong>{item.reason}</strong>
      <small data-direction={directionOf(item.netBuy)}>{formatMoney(item.netBuy, true)}</small>
    </div>
  );
}

function IndustryRow({ item }: { item: IndustryCompareItem }) {
  return (
    <div className="tv-compact-row">
      <span>{item.industry}</span>
      <strong data-direction={directionOf(item.changePercent)}>
        {formatPercent(item.changePercent)}
      </strong>
      <small>{item.strongCount} 只</small>
    </div>
  );
}

function directionOf(value: number | null): "up" | "down" | "flat" {
  if (typeof value !== "number" || value === 0) {
    return "flat";
  }
  return value > 0 ? "up" : "down";
}

function formatPercent(value: number | null): string {
  if (typeof value !== "number") {
    return "--";
  }
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatMoney(value: number | null, signed = false): string {
  if (typeof value !== "number") {
    return "--";
  }
  const sign = signed && value > 0 ? "+" : value < 0 ? "-" : "";
  const abs = Math.abs(value);
  if (abs >= 100_000_000) {
    return `${sign}${(abs / 100_000_000).toFixed(2)}亿`;
  }
  if (abs >= 10_000) {
    return `${sign}${(abs / 10_000).toFixed(1)}万`;
  }
  return `${sign}${abs.toFixed(0)}`;
}
