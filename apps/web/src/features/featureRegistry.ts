import {
  AlarmClock,
  Bell,
  Bookmark,
  CalendarDays,
  CircleHelp,
  Flame,
  Grid3X3,
  MessageSquareText,
  Newspaper,
  Rss,
  Search,
  BarChart3,
  type LucideIcon,
} from "lucide-react";

export type RightSidebarPanelId =
  | "watchlist"
  | "alerts"
  | "layers"
  | "messages"
  | "ideas"
  | "calendar"
  | "news"
  | "notifications"
  | "apps"
  | "help";

export type RightSidebarFeature = {
  id: RightSidebarPanelId;
  title: string;
  icon: LucideIcon;
  size: number;
  strokeWidth: number;
  dock?: "top" | "bottom";
};

export const RIGHT_SIDEBAR_FEATURES: RightSidebarFeature[] = [
  { id: "watchlist", title: "关注列表", icon: Bookmark, size: 23, strokeWidth: 1.7 },
  { id: "alerts", title: "预警", icon: AlarmClock, size: 24, strokeWidth: 1.7 },
  { id: "layers", title: "今日最强", icon: Flame, size: 23, strokeWidth: 1.7 },
  { id: "messages", title: "消息", icon: MessageSquareText, size: 22, strokeWidth: 1.7 },
  { id: "ideas", title: "个股新闻", icon: Newspaper, size: 23, strokeWidth: 1.7 },
  { id: "calendar", title: "日历", icon: CalendarDays, size: 22, strokeWidth: 1.7, dock: "bottom" },
  { id: "news", title: "资讯", icon: Rss, size: 22, strokeWidth: 1.7, dock: "bottom" },
  { id: "notifications", title: "通知", icon: Bell, size: 22, strokeWidth: 1.7, dock: "bottom" },
  { id: "apps", title: "更多", icon: Grid3X3, size: 24, strokeWidth: 1.7, dock: "bottom" },
  { id: "help", title: "帮助", icon: CircleHelp, size: 23, strokeWidth: 1.7, dock: "bottom" },
];

export function getRightSidebarFeature(id: RightSidebarPanelId): RightSidebarFeature | undefined {
  return RIGHT_SIDEBAR_FEATURES.find((feature) => feature.id === id);
}

export type ScreenerTabId = "wencai" | "chan";

export type ScreenerDockFeature = {
  id: ScreenerTabId;
  title: string;
  icon: LucideIcon;
};

export const SCREENER_DOCK_FEATURES: ScreenerDockFeature[] = [
  { id: "wencai", title: "问财选股", icon: Search },
  { id: "chan", title: "缠论选股", icon: BarChart3 },
];
