import { Flame, TrendingUp } from "lucide-react";
import type { ReactNode } from "react";
import type { MarketSidebarSnapshot } from "../api/marketSidebar";

type StrengthFreshness = "live" | "delayed" | "stale" | "unavailable";

export function StrongestTodayPanel({
  marketSnapshot,
}: {
  marketSnapshot: Pick<MarketSidebarSnapshot, "strength">;
}) {
  const strength = marketSnapshot.strength;
  const status = strength?.freshness ?? "unavailable";

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
          <span>强度评分</span>
          <strong>{strength?.score ?? "--"}</strong>
        </div>
        <div>
          <span>数据状态</span>
          <strong data-freshness={status}>{statusLabel(status)}</strong>
        </div>
      </div>

      <PanelBlock title="强势标的" icon={<TrendingUp size={15} />}>
        {strength?.leaders.length
          ? strength.leaders.map((leader) => (
            <StrengthRow key={leader.name} label={leader.name} changePercent={leader.changePercent} />
          ))
          : <EmptyState status={status} />}
      </PanelBlock>

      <PanelBlock title="市场主题">
        {strength?.themes.length
          ? strength.themes.map((theme) => (
            <StrengthRow
              key={theme.name}
              label={theme.name}
              changePercent={theme.changePercent}
              netInflowWan={theme.mainNetInflowWan}
            />
          ))
          : <EmptyState status={status} />}
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

function StrengthRow({
  label,
  changePercent,
  netInflowWan,
}: {
  label: string;
  changePercent: number | null;
  netInflowWan?: number | null;
}) {
  const direction = changePercent === null ? "flat" : changePercent >= 0 ? "up" : "down";
  return (
    <div className="tv-strength-row">
      <div>
        <strong>{label}</strong>
        {netInflowWan !== undefined ? <small>主力净流入 {formatNetInflow(netInflowWan)}</small> : null}
      </div>
      <em data-direction={direction}>{formatPercent(changePercent)}</em>
    </div>
  );
}

function formatPercent(value: number | null): string {
  if (value === null) return "--";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatNetInflow(value: number | null): string {
  if (value === null) return "--";
  if (Math.abs(value) >= 10_000) return `${value >= 0 ? "+" : ""}${(value / 10_000).toFixed(2)}亿`;
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}万`;
}

function EmptyState({ status }: { status: StrengthFreshness }) {
  return <div className="tv-compact-row"><span>{statusLabel(status)}</span></div>;
}

function statusLabel(status: StrengthFreshness) {
  if (status === "unavailable") return "Unavailable";
  if (status === "stale") return "Stale";
  if (status === "delayed") return "Delayed";
  return "Live";
}
