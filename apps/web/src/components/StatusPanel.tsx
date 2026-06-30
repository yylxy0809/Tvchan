import { Activity, Database, RadioTower, ShieldCheck, Waypoints } from "lucide-react";

type ChanSummary = {
  engine: string;
  levels: string[];
  strokes: number;
  segments: number;
  centers: number;
  signals: number;
  requestedBarCount: number;
  barsByLevel: Record<string, number>;
};

type Props = {
  health: Record<string, unknown> | null;
  error: string | null;
  realtimeStatus?: string;
  realtimeMessage?: string | null;
  dataTransport?: string;
  tokenReady?: boolean;
  chanSummary?: ChanSummary | null;
  chanError?: string | null;
};

export function StatusPanel({
  health,
  error,
  realtimeStatus,
  realtimeMessage,
  dataTransport,
  tokenReady = true,
  chanSummary,
  chanError,
}: Props) {
  const status = health?.status === "ok" ? "online" : "offline";
  return (
    <aside className="status-panel" aria-label="System status">
      <div className="status-row">
        <Activity size={16} />
        <span>API</span>
        <strong data-state={status}>{status}</strong>
      </div>
      <div className="status-row">
        <Database size={16} />
        <span>DB</span>
        <strong>{String(health?.db ?? "unknown")}</strong>
      </div>
      <div className="status-row">
        <Waypoints size={16} />
        <span>Source</span>
        <strong>{String(health?.data_source ?? "unknown")}</strong>
      </div>
      <div className="status-row">
        <RadioTower size={16} />
        <span>Realtime</span>
        <strong>{String(realtimeStatus ?? "idle")}</strong>
      </div>
      <div className="status-row">
        <Waypoints size={16} />
        <span>Data</span>
        <strong>{String(dataTransport ?? "http")}</strong>
      </div>
      <div className="status-row">
        <Waypoints size={16} />
        <span>Chan</span>
        <strong>{chanSummary ? "loaded" : String(health?.chan ?? "unknown")}</strong>
      </div>
      <div className="status-row">
        <ShieldCheck size={16} />
        <span>Token</span>
        <strong data-state={tokenReady ? "online" : "offline"}>
          {tokenReady ? "ready" : "missing"}
        </strong>
      </div>
      {chanSummary ? (
        <p className="status-note">
          {chanSummary.levels.join("/")} | strokes {chanSummary.strokes} | segments{" "}
          {chanSummary.segments} | centers {chanSummary.centers} | signals{" "}
          {chanSummary.signals}
        </p>
      ) : null}
      {chanSummary ? (
        <p className="status-note">
          bars requested {chanSummary.requestedBarCount} |{" "}
          {chanSummary.levels
            .map((level) => `${level} ${chanSummary.barsByLevel[level] ?? 0}`)
            .join(" | ")}
        </p>
      ) : null}
      {chanSummary ? <p className="status-note">engine {chanSummary.engine}</p> : null}
      {health?.data_note ? (
        <p className="status-note">{String(health.data_note)}</p>
      ) : null}
      {realtimeMessage ? <p className="status-note">{realtimeMessage}</p> : null}
      {chanError ? <p className="status-error">{chanError}</p> : null}
      {error ? <p className="status-error">{error}</p> : null}
    </aside>
  );
}
