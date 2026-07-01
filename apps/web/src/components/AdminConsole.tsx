import {
  AlertCircle,
  Ban,
  Copy,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import {
  type AdminToken,
  createAdminToken,
  deleteAdminToken,
  disableAdminToken,
  listAdminTokens,
} from "../auth/api";
import {
  fetchRuntimeFeatureConfig,
  saveRuntimeFeatureConfig,
  type RuntimeFeatureArea,
  type RuntimeFeatureConfig,
} from "../api/runtimeConfig";
import {
  RIGHT_SIDEBAR_FEATURES,
  SCREENER_DOCK_FEATURES,
} from "../features/featureRegistry";
import { publishRuntimeFeatureConfig } from "../features/runtimeFeatureRegistry";

type Props = {
  adminToken: string;
};

export function AdminConsole({ adminToken }: Props) {
  const [tokens, setTokens] = useState<AdminToken[]>([]);
  const [featureConfig, setFeatureConfig] = useState<RuntimeFeatureConfig>({
    rightSidebar: [],
    screenerDock: [],
  });
  const [label, setLabel] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [newToken, setNewToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [mutating, setMutating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void refreshTokens();
    void refreshFeatureConfig();
  }, []);

  async function refreshTokens() {
    setLoading(true);
    setError(null);
    try {
      setTokens(await listAdminTokens());
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setLoading(false);
    }
  }

  async function refreshFeatureConfig() {
    try {
      const nextConfig = await fetchRuntimeFeatureConfig();
      setFeatureConfig(nextConfig);
      publishRuntimeFeatureConfig(nextConfig);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    }
  }

  async function toggleFeature(area: RuntimeFeatureArea, id: string) {
    const next = toggleRuntimeFeature(featureConfig, area, id);
    setFeatureConfig(next);
    setMutating(true);
    setError(null);
    try {
      const savedConfig = await saveRuntimeFeatureConfig(adminToken, next);
      setFeatureConfig(savedConfig);
      publishRuntimeFeatureConfig(savedConfig);
    } catch (nextError) {
      setFeatureConfig(featureConfig);
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedLabel = label.trim();
    if (!normalizedLabel) {
      setError("Token label is required.");
      return;
    }
    setMutating(true);
    setError(null);
    try {
      const created = await createAdminToken({
        label: normalizedLabel,
        display_name: displayName.trim() || null,
      });
      setTokens((current) => [created, ...current]);
      setNewToken(created.token ?? null);
      setLabel("");
      setDisplayName("");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleDisable(id: number) {
    setMutating(true);
    setError(null);
    try {
      const updated = await disableAdminToken(id);
      setTokens((current) =>
        current.map((item) => (item.id === id ? updated : item)),
      );
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleDelete(id: number) {
    setMutating(true);
    setError(null);
    try {
      await deleteAdminToken(id);
      setTokens((current) => current.filter((item) => item.id !== id));
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function copyNewToken() {
    if (!newToken) {
      return;
    }
    try {
      await navigator.clipboard.writeText(newToken);
    } catch {
      setError("Clipboard is not available. Select and copy the token manually.");
    }
  }

  return (
    <section className="admin-workspace" aria-label="Admin token management">
      <div className="admin-head">
        <div>
          <p className="eyebrow">Admin Console</p>
          <h1>Access Tokens</h1>
        </div>
        <button
          className="ghost-button"
          type="button"
          onClick={() => void refreshTokens()}
          disabled={loading}
        >
          <RefreshCw size={16} />
          <span>Refresh</span>
        </button>
      </div>

      <form className="token-create" onSubmit={handleCreate}>
        <label htmlFor="token-label">Label</label>
        <input
          id="token-label"
          value={label}
          onChange={(event) => setLabel(event.target.value)}
          placeholder="desk-user-01"
        />
        <label htmlFor="token-display-name">Display name</label>
        <input
          id="token-display-name"
          value={displayName}
          onChange={(event) => setDisplayName(event.target.value)}
          placeholder="User name or device"
        />
        <button className="primary-button compact" type="submit" disabled={mutating}>
          <Plus size={16} />
          <span>Create</span>
        </button>
      </form>

      {newToken ? (
        <div className="created-token">
          <div>
            <span>New token</span>
            <code>{newToken}</code>
          </div>
          <button type="button" onClick={() => void copyNewToken()}>
            <Copy size={15} />
            <span>Copy</span>
          </button>
        </div>
      ) : null}

      {error ? (
        <p className="form-error admin-error" role="alert">
          <AlertCircle size={15} />
          <span>{error}</span>
        </p>
      ) : null}

      <div className="token-table" aria-busy={loading}>
        <div className="token-row token-row-head">
          <span>Label</span>
          <span>Display</span>
          <span>Status</span>
          <span>Created</span>
          <span />
        </div>
        {loading ? <div className="empty-row">Loading tokens</div> : null}
        {!loading && tokens.length === 0 ? (
          <div className="empty-row">No user tokens yet.</div>
        ) : null}
        {tokens.map((item) => (
          <div className="token-row" key={item.id}>
            <span className="token-name">{item.label}</span>
            <span>{item.display_name || "--"}</span>
            <span data-state={item.is_active ? "on" : "off"}>
              {item.is_active ? "active" : "disabled"}
            </span>
            <span>{formatDate(item.created_at)}</span>
            <span className="row-actions">
              <button
                type="button"
                title="Disable token"
                onClick={() => void handleDisable(item.id)}
                disabled={mutating || !item.is_active}
              >
                <Ban size={15} />
              </button>
              <button
                type="button"
                title="Delete token"
                onClick={() => void handleDelete(item.id)}
                disabled={mutating}
              >
                <Trash2 size={15} />
              </button>
            </span>
          </div>
        ))}
      </div>

      <section className="admin-feature-panel" aria-label="Runtime feature switches">
        <div className="admin-feature-head">
          <div>
            <p className="eyebrow">Runtime Config</p>
            <h2>Feature Switches</h2>
          </div>
          <button
            className="ghost-button"
            type="button"
            onClick={() => void refreshFeatureConfig()}
            disabled={mutating}
          >
            <RefreshCw size={16} />
            <span>Reload</span>
          </button>
        </div>

        <div className="admin-feature-grid">
          <FeatureSwitchGroup
            title="Right Sidebar"
            area="rightSidebar"
            config={featureConfig}
            disabled={mutating}
            onToggle={toggleFeature}
          />
          <FeatureSwitchGroup
            title="Bottom Dock"
            area="screenerDock"
            config={featureConfig}
            disabled={mutating}
            onToggle={toggleFeature}
          />
        </div>
      </section>
    </section>
  );
}

function FeatureSwitchGroup({
  title,
  area,
  config,
  disabled,
  onToggle,
}: {
  title: string;
  area: RuntimeFeatureArea;
  config: RuntimeFeatureConfig;
  disabled: boolean;
  onToggle(area: RuntimeFeatureArea, id: string): void;
}) {
  const features =
    area === "rightSidebar" ? RIGHT_SIDEBAR_FEATURES : SCREENER_DOCK_FEATURES;

  return (
    <div className="admin-feature-card">
      <h3>{title}</h3>
      {features.map((feature) => (
        <label key={feature.id} className="admin-feature-switch">
          <input
            type="checkbox"
            checked={isRuntimeFeatureEnabled(config, area, feature.id)}
            onChange={() => onToggle(area, feature.id)}
            disabled={disabled}
          />
          <span>{feature.title}</span>
          <code>{feature.id}</code>
        </label>
      ))}
    </div>
  );
}

function toggleRuntimeFeature(
  config: RuntimeFeatureConfig,
  area: RuntimeFeatureArea,
  id: string,
): RuntimeFeatureConfig {
  const current = isRuntimeFeatureEnabled(config, area, id);
  const ids =
    area === "rightSidebar"
      ? RIGHT_SIDEBAR_FEATURES.map((feature) => feature.id)
      : SCREENER_DOCK_FEATURES.map((feature) => feature.id);
  const existing = new Map((config[area] ?? []).map((item) => [item.id, item]));
  existing.set(id, {
    ...(existing.get(id) ?? { id }),
    enabled: !current,
  });
  return {
    ...config,
    [area]: ids.map((featureId, order) => ({
      id: featureId,
      order,
      enabled: existing.get(featureId)?.enabled ?? true,
    })),
  };
}

function isRuntimeFeatureEnabled(
  config: RuntimeFeatureConfig,
  area: RuntimeFeatureArea,
  id: string,
): boolean {
  return config[area]?.find((item) => item.id === id)?.enabled !== false;
}

function formatDate(value?: string | null): string {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}
