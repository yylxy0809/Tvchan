import { CircleEllipsis, LogOut, Mountain, Shield } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { MarketSidebarSnapshot } from "../api/marketSidebar";
import {
  getRightSidebarFeature,
  type RightSidebarFeature,
  type RightSidebarPanelId,
} from "../features/featureRegistry";
import { useRightSidebarFeatures } from "../features/runtimeFeatureRegistry";
import { AlertsPanel } from "./AlertsPanel";
import { StockNewsPanel } from "./StockNewsPanel";
import { StrongestTodayPanel } from "./StrongestTodayPanel";
import { WatchlistPanel } from "./WatchlistPanel";

type Props = {
  activeSymbol: string;
  timeframe: string;
  collapseSignal?: number;
  onSelectSymbol(symbol: string): void;
  marketSnapshot: MarketSidebarSnapshot;
  onWatchlistSymbolsChange(symbols: string[]): void;
  authToken?: string;
  isAdmin?: boolean;
  onOpenAdmin?(): void;
  onLogout?(): void;
};

export function RightSidebar({
  activeSymbol,
  timeframe,
  collapseSignal,
  onSelectSymbol,
  marketSnapshot,
  onWatchlistSymbolsChange,
  authToken,
  isAdmin = false,
  onOpenAdmin,
  onLogout,
}: Props) {
  const [activePanel, setActivePanel] = useState<RightSidebarPanelId | null>(
    null,
  );
  const lastCollapseSignalRef = useRef(collapseSignal);
  const sidebarFeatures = useRightSidebarFeatures();
  const topTools = sidebarFeatures.filter((tool) => tool.dock !== "bottom");
  const bottomTools = sidebarFeatures.filter((tool) => tool.dock === "bottom");

  useEffect(() => {
    if (!activePanel) {
      return;
    }
    if (!sidebarFeatures.some((tool) => tool.id === activePanel)) {
      setActivePanel(null);
    }
  }, [activePanel, sidebarFeatures]);

  useEffect(() => {
    if (collapseSignal === undefined || lastCollapseSignalRef.current === collapseSignal) {
      return;
    }
    lastCollapseSignalRef.current = collapseSignal;
    setActivePanel(null);
  }, [collapseSignal]);

  function togglePanel(panel: RightSidebarPanelId) {
    setActivePanel((current) => (current === panel ? null : panel));
  }

  return (
    <aside className="tv-right-sidebar" aria-label="TradingView 右侧栏">
      {activePanel ? (
        <div className="tv-side-panel" data-panel={activePanel}>
          {renderPanel(
            activePanel,
            activeSymbol,
            timeframe,
            onSelectSymbol,
            marketSnapshot,
            onWatchlistSymbolsChange,
            authToken,
            isAdmin,
            onOpenAdmin,
            onLogout,
          )}
        </div>
      ) : null}

      <nav className="tv-right-rail" aria-label="图表侧边工具">
        <div className="tv-right-rail-group">
          {topTools.map((tool) => (
            <RailButton
              key={tool.id}
              tool={tool}
              active={activePanel === tool.id}
              onClick={() => togglePanel(tool.id)}
            />
          ))}
        </div>
        <div className="tv-right-rail-group tv-right-rail-bottom">
          {bottomTools.map((tool) => (
            <RailButton
              key={tool.id}
              tool={tool}
              active={activePanel === tool.id}
              onClick={() => togglePanel(tool.id)}
            />
          ))}
        </div>
      </nav>
    </aside>
  );
}

function RailButton({
  tool,
  active,
  onClick,
}: {
  tool: RightSidebarFeature;
  active: boolean;
  onClick(): void;
}) {
  const Icon = tool.icon;
  return (
    <button
      type="button"
      title={tool.title}
      aria-label={tool.title}
      data-active={active}
      onClick={onClick}
    >
      <Icon size={tool.size} strokeWidth={tool.strokeWidth} />
    </button>
  );
}

function renderPanel(
  panel: RightSidebarPanelId,
  activeSymbol: string,
  timeframe: string,
  onSelectSymbol: (symbol: string) => void,
  marketSnapshot: MarketSidebarSnapshot,
  onWatchlistSymbolsChange: (symbols: string[]) => void,
  authToken: string | undefined,
  isAdmin: boolean,
  onOpenAdmin: (() => void) | undefined,
  onLogout: (() => void) | undefined,
) {
  if (panel === "watchlist") {
    return (
      <WatchlistPanel
        activeSymbol={activeSymbol}
        onSelectSymbol={onSelectSymbol}
        onWatchlistSymbolsChange={onWatchlistSymbolsChange}
        quotes={marketSnapshot.quotesBySymbol}
        profile={marketSnapshot.profileBySymbol[activeSymbol] ?? null}
        authToken={authToken}
      />
    );
  }
  if (panel === "alerts") {
    return <AlertsPanel activeSymbol={activeSymbol} />;
  }
  if (panel === "layers") {
    return <StrongestTodayPanel marketSnapshot={marketSnapshot} />;
  }
  if (panel === "ideas") {
    return <StockNewsPanel activeSymbol={activeSymbol} feed={marketSnapshot.newsBySymbol[activeSymbol] ?? null} />;
  }
  if (panel === "apps") {
    return (
      <MorePanel
        activeSymbol={activeSymbol}
        isAdmin={isAdmin}
        onOpenAdmin={onOpenAdmin}
        onLogout={onLogout}
      />
    );
  }
  return <UtilityPanel panel={panel} activeSymbol={activeSymbol} />;
}

function MorePanel({
  activeSymbol,
  isAdmin,
  onOpenAdmin,
  onLogout,
}: {
  activeSymbol: string;
  isAdmin: boolean;
  onOpenAdmin?: () => void;
  onLogout?: () => void;
}) {
  return (
    <section className="tv-utility-panel" aria-label="更多">
      <header>
        <strong>更多</strong>
        <CircleEllipsis size={20} />
      </header>
      <div className="tv-more-panel">
        <div>
          <span className="tv-more-kicker">当前会话</span>
          <strong>{isAdmin ? "管理员" : "用户"}</strong>
          <small>{activeSymbol}</small>
        </div>
        {isAdmin && onOpenAdmin ? (
          <button type="button" onClick={onOpenAdmin}>
            <Shield size={18} />
            <span>后台管理</span>
          </button>
        ) : null}
        {onLogout ? (
          <button type="button" onClick={onLogout}>
            <LogOut size={18} />
            <span>退出登录</span>
          </button>
        ) : null}
      </div>
    </section>
  );
}

function UtilityPanel({
  panel,
  activeSymbol,
}: {
  panel: RightSidebarPanelId;
  activeSymbol: string;
}) {
  const label = getRightSidebarFeature(panel)?.title ?? "面板";
  return (
    <section className="tv-utility-panel" aria-label={label}>
      <header>
        <strong>{label}</strong>
        <CircleEllipsis size={20} />
      </header>
      <div className="tv-utility-empty">
        <Mountain size={42} strokeWidth={1.4} />
        <span>{activeSymbol}</span>
      </div>
    </section>
  );
}
