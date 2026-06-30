import {
  AlarmClockPlus,
  Bell,
  BellOff,
  Check,
  MoreHorizontal,
  Plus,
  Search,
  SlidersHorizontal,
  Trash2,
  X,
} from "lucide-react";
import { type FormEvent, useMemo, useState } from "react";
import { useLocalStorageState } from "../hooks/useLocalStorageState";

export type AlertConditionType = "last_price" | "change_percent" | "volume";
export type AlertOperator = "crossing" | "above" | "below";
export type AlertFrequency = "once" | "once_per_bar" | "once_per_minute";

export type PriceAlert = {
  id: string;
  symbol: string;
  conditionType: AlertConditionType;
  operator: AlertOperator;
  price: number;
  frequency: AlertFrequency;
  expiresAt: string;
  message: string;
  notify: boolean;
  enabled: boolean;
  createdAt: string;
};

type Props = {
  activeSymbol: string;
};

type AlertDraft = Omit<PriceAlert, "id" | "createdAt">;

const STORAGE_KEY = "tv-a-share-alerts";

const CONDITION_LABELS: Record<AlertConditionType, string> = {
  last_price: "价格",
  change_percent: "涨跌幅",
  volume: "成交量",
};

const OPERATOR_LABELS: Record<AlertOperator, string> = {
  crossing: "穿越",
  above: "高于",
  below: "低于",
};

const FREQUENCY_LABELS: Record<AlertFrequency, string> = {
  once: "仅一次",
  once_per_bar: "每根K线一次",
  once_per_minute: "每分钟一次",
};

export function AlertsPanel({ activeSymbol }: Props) {
  const [alerts, setAlerts] = useLocalStorageState<PriceAlert[]>(
    STORAGE_KEY,
    [],
    reviveAlerts,
  );
  const [creating, setCreating] = useState(false);
  const [tab, setTab] = useState<"alerts" | "log">("alerts");
  const [draft, setDraft] = useState<AlertDraft>(() => createDraft(activeSymbol));

  const sortedAlerts = useMemo(
    () =>
      [...alerts].sort(
        (a, b) =>
          new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
      ),
    [alerts],
  );

  function openCreateDialog() {
    setDraft(createDraft(activeSymbol));
    setCreating(true);
  }

  function handleCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!draft.symbol.trim() || !Number.isFinite(draft.price)) {
      return;
    }
    const alert: PriceAlert = {
      ...draft,
      symbol: draft.symbol.trim().toUpperCase(),
      id: createId("alert"),
      createdAt: new Date().toISOString(),
    };
    setAlerts((current) => [alert, ...current]);
    setCreating(false);
  }

  function handleToggle(id: string) {
    setAlerts((current) =>
      current.map((alert) =>
        alert.id === id ? { ...alert, enabled: !alert.enabled } : alert,
      ),
    );
  }

  function handleDelete(id: string) {
    setAlerts((current) => current.filter((alert) => alert.id !== id));
  }

  return (
    <section className="tv-alerts-panel" aria-label="预警">
      <div className="tv-alert-tabs" role="tablist">
        <button
          type="button"
          role="tab"
          data-active={tab === "alerts"}
          onClick={() => setTab("alerts")}
        >
          预警
        </button>
        <button
          type="button"
          role="tab"
          data-active={tab === "log"}
          onClick={() => setTab("log")}
        >
          日志
        </button>
      </div>

      <div className="tv-alert-toolbar">
        <button type="button" title="创建预警" onClick={openCreateDialog}>
          <Plus size={21} />
        </button>
        <span />
        <button type="button" title="搜索">
          <Search size={20} />
        </button>
        <button type="button" title="排序">
          <SlidersHorizontal size={20} />
        </button>
        <button type="button" title="更多">
          <MoreHorizontal size={22} />
        </button>
      </div>

      {tab === "log" ? (
        <div className="tv-panel-empty">
          <AlarmClockPlus size={74} strokeWidth={1.6} />
          <span>日志</span>
        </div>
      ) : (
        <div className="tv-alert-list">
          {sortedAlerts.length === 0 ? (
            <div className="tv-panel-empty">
              <AlarmClockPlus size={84} strokeWidth={1.55} />
              <p>当条件触发时，预警会即时提醒你。</p>
              <button type="button" onClick={openCreateDialog}>
                创建预警
              </button>
            </div>
          ) : null}
          {sortedAlerts.map((alert) => (
            <article
              key={alert.id}
              className="tv-alert-row"
              data-enabled={alert.enabled}
            >
              <button
                type="button"
                className="tv-alert-toggle"
                title={alert.enabled ? "停用预警" : "启用预警"}
                onClick={() => handleToggle(alert.id)}
              >
                {alert.enabled ? <Bell size={17} /> : <BellOff size={17} />}
              </button>
              <div>
                <div className="tv-alert-row-head">
                  <strong>{alert.symbol}</strong>
                  <span>{FREQUENCY_LABELS[alert.frequency]}</span>
                </div>
                <p>
                  {CONDITION_LABELS[alert.conditionType]}{" "}
                  {OPERATOR_LABELS[alert.operator]}{" "}
                  {formatAlertPrice(alert)}
                </p>
                {alert.message ? <small>{alert.message}</small> : null}
                <time>{formatExpiry(alert.expiresAt)}</time>
              </div>
              <button
                type="button"
                className="tv-mini-icon-button"
                title="删除预警"
                onClick={() => handleDelete(alert.id)}
              >
                <Trash2 size={15} />
              </button>
            </article>
          ))}
        </div>
      )}

      {creating ? (
        <div className="tv-alert-dialog-backdrop" role="presentation">
          <form className="tv-alert-dialog" onSubmit={handleCreate}>
            <div className="tv-alert-dialog-head">
              <strong>为 {draft.symbol} 创建预警</strong>
              <button
                type="button"
                title="关闭"
                onClick={() => setCreating(false)}
              >
                <X size={19} />
              </button>
            </div>

            <label>
              <span>标的</span>
              <input
                value={draft.symbol}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    symbol: event.target.value.toUpperCase(),
                  }))
                }
              />
            </label>

            <label>
              <span>条件</span>
              <select
                value={draft.conditionType}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    conditionType: event.target.value as AlertConditionType,
                  }))
                }
              >
                {Object.entries(CONDITION_LABELS).map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              <span>触发方式</span>
              <select
                value={draft.operator}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    operator: event.target.value as AlertOperator,
                  }))
                }
              >
                {Object.entries(OPERATOR_LABELS).map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              <span>数值</span>
              <input
                type="number"
                step="0.001"
                value={draft.price}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    price: Number(event.target.value),
                  }))
                }
              />
            </label>

            <label>
              <span>频率</span>
              <select
                value={draft.frequency}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    frequency: event.target.value as AlertFrequency,
                  }))
                }
              >
                {Object.entries(FREQUENCY_LABELS).map(([value, label]) => (
                  <option key={value} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </label>

            <label>
              <span>到期时间</span>
              <input
                type="datetime-local"
                value={draft.expiresAt}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    expiresAt: event.target.value,
                  }))
                }
              />
            </label>

            <label>
              <span>消息</span>
              <textarea
                rows={3}
                value={draft.message}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    message: event.target.value,
                  }))
                }
              />
            </label>

            <label className="tv-checkbox-row">
              <input
                type="checkbox"
                checked={draft.notify}
                onChange={(event) =>
                  setDraft((current) => ({
                    ...current,
                    notify: event.target.checked,
                  }))
                }
              />
              <span>应用内通知、浮窗提示</span>
            </label>

            <div className="tv-alert-dialog-actions">
              <button type="button" onClick={() => setCreating(false)}>
                取消
              </button>
              <button type="submit">
                <Check size={16} />
                <span>创建</span>
              </button>
            </div>
          </form>
        </div>
      ) : null}
    </section>
  );
}

function createDraft(symbol: string): AlertDraft {
  const expires = new Date();
  expires.setMonth(expires.getMonth() + 1);
  return {
    symbol,
    conditionType: "last_price",
    operator: "crossing",
    price: 0,
    frequency: "once",
    expiresAt: toDateTimeLocal(expires),
    message: "",
    notify: true,
    enabled: true,
  };
}

function reviveAlerts(value: unknown): PriceAlert[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((alert): alert is PriceAlert => {
    return (
      !!alert &&
      typeof alert === "object" &&
      typeof (alert as PriceAlert).id === "string" &&
      typeof (alert as PriceAlert).symbol === "string"
    );
  });
}

function formatAlertPrice(alert: PriceAlert): string {
  if (alert.conditionType === "change_percent") {
    return `${alert.price}%`;
  }
  return String(alert.price);
}

function formatExpiry(value: string): string {
  if (!value) {
    return "不设到期";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return `到期 ${value}`;
  }
  return `到期 ${date.toLocaleString()}`;
}

function toDateTimeLocal(date: Date): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    "-",
    pad(date.getMonth() + 1),
    "-",
    pad(date.getDate()),
    "T",
    pad(date.getHours()),
    ":",
    pad(date.getMinutes()),
  ].join("");
}

function createId(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random()
    .toString(36)
    .slice(2, 8)}`;
}
