import {
  AlertCircle,
  Ban,
  Copy,
  Plus,
  RefreshCw,
  Save,
  TestTube2,
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
  type AdminOpsStatus,
  type ConnectivityTestResult,
  type LlmProviderConfig,
  type LlmProvidersConfig,
  type LlmTestResult,
  type ModuleCFreshnessActualWatermark,
  type ModuleCFreshnessExpectedWatermark,
  type ModuleCExecutionStatus,
  type WencaiAdminConfig,
  fetchAdminOpsStatus,
  fetchLlmProviders,
  fetchModuleCExecution,
  fetchWencaiConfig,
  isAdminAuthFailure,
  saveLlmProviders,
  saveWencaiConfig,
  testLlmProvider,
  testWencaiConfig,
} from "../api/adminRuntimeConfig";
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
  onAuthenticationFailure(): void;
};

type ActionFeedback = {
  state: "pending" | "success" | "error";
  message: string;
};

type ModuleCExecutionViewState = {
  snapshot: ModuleCExecutionStatus | null;
  stale: boolean;
  reason: string | null;
  error: string | null;
};

const DEFAULT_WENCAI_CONFIG: WencaiAdminConfig = {
  base_url: "https://openapi.iwencai.com",
  api_key: "",
  api_keys: [],
  cookie: "",
  user_agent: "",
  pro: false,
  timeout_seconds: 5,
};

export function AdminConsole({ adminToken, onAuthenticationFailure }: Props) {
  const [tokens, setTokens] = useState<AdminToken[]>([]);
  const [featureConfig, setFeatureConfig] = useState<RuntimeFeatureConfig>({
    rightSidebar: [],
    screenerDock: [],
  });
  const [wencaiConfig, setWencaiConfig] = useState<WencaiAdminConfig>(DEFAULT_WENCAI_CONFIG);
  const [wencaiTestResult, setWencaiTestResult] = useState<ConnectivityTestResult | null>(null);
  const [wencaiFeedback, setWencaiFeedback] = useState<ActionFeedback | null>(null);
  const [llmConfig, setLlmConfig] = useState<LlmProvidersConfig>({
    active_provider_id: null,
    providers: [],
  });
  const [llmTestResults, setLlmTestResults] = useState<Record<string, LlmTestResult>>({});
  const [opsStatus, setOpsStatus] = useState<AdminOpsStatus | null>(null);
  const [moduleCExecutionState, setModuleCExecutionState] = useState<ModuleCExecutionViewState>({
    snapshot: null,
    stale: false,
    reason: null,
    error: null,
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
    void refreshWencaiConfig();
    void refreshLlmProviders();
    void refreshOpsStatus();
    void refreshModuleCExecution();
  }, []);

  async function refreshTokens() {
    setLoading(true);
    setError(null);
    try {
      setTokens(await listAdminTokens(adminToken));
    } catch (nextError) {
      setError(readError(nextError));
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
      setError(readError(nextError));
    }
  }

  async function refreshWencaiConfig(showFeedback = false) {
    if (showFeedback) setWencaiFeedback({ state: "pending", message: "Reloading iWencai configuration..." });
    try {
      const config = await fetchWencaiConfig(adminToken);
      setWencaiConfig({ ...config, timeout_seconds: Math.min(5, config.timeout_seconds) });
      if (showFeedback) setWencaiFeedback({ state: "success", message: "iWencai configuration reloaded." });
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      const message = readError(nextError);
      setError(message);
      if (showFeedback) setWencaiFeedback({ state: "error", message });
    }
  }

  async function refreshLlmProviders() {
    try {
      const config = await fetchLlmProviders(adminToken);
      setLlmConfig(config.providers.length ? config : withDefaultProvider(config));
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      setError(readError(nextError));
      setLlmConfig(withDefaultProvider({ active_provider_id: null, providers: [] }));
    }
  }

  async function refreshOpsStatus() {
    try {
      setOpsStatus(await fetchAdminOpsStatus(adminToken));
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      setOpsStatus({
        status: "degraded",
        lifecycle_observer: {
          status: "unavailable",
          deployed: false,
          expected_observer_name: "unknown",
          reason: "request_failed",
          error: readError(nextError),
        },
      });
    }
  }

  async function refreshModuleCExecution() {
    try {
      const snapshot = await fetchModuleCExecution(adminToken);
      setModuleCExecutionState({
        snapshot,
        stale: false,
        reason: null,
        error: null,
      });
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      setModuleCExecutionState((current) => ({
        ...current,
        stale: true,
        reason: "request_failed",
        error: readError(nextError),
      }));
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
      setError(readError(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedLabel = label.trim();
    if (!normalizedLabel) {
      setError("请输入令牌标签。");
      return;
    }
    setMutating(true);
    setError(null);
    try {
      const created = await createAdminToken(adminToken, {
        label: normalizedLabel,
        display_name: displayName.trim() || null,
      });
      setTokens((current) => [created, ...current]);
      setNewToken(created.token ?? null);
      setLabel("");
      setDisplayName("");
    } catch (nextError) {
      setError(readError(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleDisable(id: number) {
    setMutating(true);
    setError(null);
    try {
      const updated = await disableAdminToken(adminToken, id);
      setTokens((current) =>
        current.map((item) => (item.id === id ? updated : item)),
      );
    } catch (nextError) {
      setError(readError(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleDelete(id: number) {
    setMutating(true);
    setError(null);
    try {
      await deleteAdminToken(adminToken, id);
      setTokens((current) => current.filter((item) => item.id !== id));
    } catch (nextError) {
      setError(readError(nextError));
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
      setError("剪贴板不可用，请手动复制令牌。");
    }
  }

  async function handleSaveWencaiConfig() {
    setMutating(true);
    setError(null);
    setWencaiFeedback({ state: "pending", message: "Saving iWencai configuration..." });
    try {
      const saved = await saveWencaiConfig(adminToken, wencaiConfig);
      setWencaiConfig(saved);
      setWencaiTestResult(null);
      setWencaiFeedback({ state: "success", message: "iWencai configuration saved." });
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      const message = readError(nextError);
      setError(message);
      setWencaiFeedback({ state: "error", message });
    } finally {
      setMutating(false);
    }
  }

  async function handleTestWencaiConfig() {
    setMutating(true);
    setError(null);
    setWencaiFeedback({ state: "pending", message: "Testing iWencai connection..." });
    try {
      const result = await testWencaiConfig(adminToken, wencaiConfig);
      setWencaiTestResult(result);
      setWencaiFeedback({ state: result.ok ? "success" : "error", message: result.message });
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      const message = readError(nextError);
      setError(message);
      setWencaiFeedback({ state: "error", message });
    } finally {
      setMutating(false);
    }
  }

  function updateWencaiKey(index: number, patch: Partial<WencaiAdminConfig["api_keys"][number]>) {
    setWencaiConfig((current) => ({
      ...current,
      api_keys: current.api_keys.map((item, itemIndex) => itemIndex === index ? { ...item, ...patch } : item),
    }));
  }

  async function handleSaveLlmProviders() {
    setMutating(true);
    setError(null);
    try {
      const saved = await saveLlmProviders(adminToken, llmConfig);
      setLlmConfig(saved);
      setLlmTestResults({});
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      setError(readError(nextError));
    } finally {
      setMutating(false);
    }
  }

  async function handleTestLlmProvider(provider: LlmProviderConfig) {
    setMutating(true);
    setError(null);
    try {
      const result = await testLlmProvider(adminToken, provider);
      setLlmTestResults((current) => ({ ...current, [provider.id]: result }));
    } catch (nextError) {
      if (handleAuthenticationFailure(nextError)) return;
      setError(readError(nextError));
    } finally {
      setMutating(false);
    }
  }

  function updateProvider(id: string, patch: Partial<LlmProviderConfig>) {
    setLlmConfig((current) => ({
      ...current,
      providers: current.providers.map((provider) =>
        provider.id === id ? { ...provider, ...patch } : provider,
      ),
    }));
  }

  function addProvider() {
    const provider = createDefaultProvider(`llm-${Date.now()}`);
    setLlmConfig((current) => ({
      active_provider_id: current.active_provider_id ?? provider.id,
      providers: [...current.providers, provider],
    }));
  }

  function removeProvider(id: string) {
    setLlmConfig((current) => {
      const providers = current.providers.filter((provider) => provider.id !== id);
      return {
        active_provider_id:
          current.active_provider_id === id ? providers[0]?.id ?? null : current.active_provider_id,
        providers,
      };
    });
  }

  function handleAuthenticationFailure(error: unknown): boolean {
    if (!isAdminAuthFailure(error)) {
      return false;
    }
    onAuthenticationFailure();
    return true;
  }

  const moduleCExecution = moduleCExecutionState.snapshot;
  const moduleCBatch = moduleCExecution?.batch;

  return (
    <section className="admin-workspace" aria-label="管理后台">
      <div className="admin-head">
        <div>
          <p className="eyebrow">管理后台</p>
          <h1>访问令牌</h1>
        </div>
        <button
          className="ghost-button"
          type="button"
          onClick={() => void refreshTokens()}
          disabled={loading}
        >
          <RefreshCw size={16} />
          <span>刷新</span>
        </button>
      </div>

      <form className="token-create" onSubmit={handleCreate}>
        <label htmlFor="token-label">标签</label>
        <input
          id="token-label"
          value={label}
          onChange={(event) => setLabel(event.target.value)}
          placeholder="desk-user-01"
        />
        <label htmlFor="token-display-name">显示名称</label>
        <input
          id="token-display-name"
          value={displayName}
          onChange={(event) => setDisplayName(event.target.value)}
          placeholder="用户或设备名称"
        />
        <button className="primary-button compact" type="submit" disabled={mutating}>
          <Plus size={16} />
          <span>创建</span>
        </button>
      </form>

      {newToken ? (
        <div className="created-token">
          <div>
            <span>新令牌</span>
            <code>{newToken}</code>
          </div>
          <button type="button" onClick={() => void copyNewToken()}>
            <Copy size={15} />
            <span>复制</span>
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
          <span>标签</span>
          <span>显示</span>
          <span>状态</span>
          <span>创建时间</span>
          <span />
        </div>
        {loading ? <div className="empty-row">正在加载令牌</div> : null}
        {!loading && tokens.length === 0 ? (
          <div className="empty-row">暂无用户令牌</div>
        ) : null}
        {tokens.map((item) => (
          <div className="token-row" key={item.id}>
            <span className="token-name">{item.label}</span>
            <span>{item.display_name || "--"}</span>
            <span data-state={item.is_active ? "on" : "off"}>
              {item.is_active ? "启用" : "停用"}
            </span>
            <span>{formatDate(item.created_at)}</span>
            <span className="row-actions">
              <button
                type="button"
                title="停用令牌"
                onClick={() => void handleDisable(item.id)}
                disabled={mutating || !item.is_active}
              >
                <Ban size={15} />
              </button>
              <button
                type="button"
                title="删除令牌"
                onClick={() => void handleDelete(item.id)}
                disabled={mutating}
              >
                <Trash2 size={15} />
              </button>
            </span>
          </div>
        ))}
      </div>

      <section className="admin-feature-panel" aria-label="Lifecycle observer status">
        <div className="admin-feature-head">
          <div>
            <p className="eyebrow">Lifecycle observer</p>
            <h2>Outbox and watermark</h2>
          </div>
          <button className="ghost-button" type="button" onClick={() => void refreshOpsStatus()}>
            <RefreshCw size={16} />
            <span>Refresh status</span>
          </button>
        </div>
        <div className="admin-feature-grid">
          <div className="admin-feature-card">
            <h3>Health</h3>
            <strong data-state={opsStatus?.lifecycle_observer.status ?? "unavailable"}>
              {opsStatus?.lifecycle_observer.status ?? "unavailable"}
            </strong>
            <p>{opsStatus?.lifecycle_observer.reason ?? "observer query available"}</p>
            {opsStatus?.lifecycle_observer.error ? <code>{opsStatus.lifecycle_observer.error}</code> : null}
          </div>
          <div className="admin-feature-card">
            <h3>Outbox backlog</h3>
            <p>pending: {opsStatus?.lifecycle_observer.counts?.pending ?? "--"}</p>
            <p>processing: {opsStatus?.lifecycle_observer.counts?.processing ?? "--"}</p>
            <p>failed: {opsStatus?.lifecycle_observer.counts?.failed ?? "--"}</p>
            <p>dead_letter: {opsStatus?.lifecycle_observer.counts?.dead_letter ?? "--"}</p>
            <p>oldest: {formatDate(opsStatus?.lifecycle_observer.oldest_backlog_at)}</p>
            <p>age: {formatAge(opsStatus?.lifecycle_observer.oldest_backlog_age_seconds)}</p>
          </div>
          <div className="admin-feature-card">
            <h3>Observer watermark</h3>
            <p>expected observer: {opsStatus?.lifecycle_observer.expected_observer_name ?? "--"}</p>
            <p>observer: {opsStatus?.lifecycle_observer.observer_watermark?.observer_name ?? "--"}</p>
            <p>last outbox: {opsStatus?.lifecycle_observer.observer_watermark?.last_outbox_id ?? "--"}</p>
            <p>max outbox: {opsStatus?.lifecycle_observer.max_outbox_id ?? "--"}</p>
            <p>lag: {opsStatus?.lifecycle_observer.observer_watermark?.lag ?? "--"}</p>
            <p>heartbeat age: {formatAge(opsStatus?.lifecycle_observer.heartbeat_age_seconds)}</p>
            <p>stale after: {formatAge(opsStatus?.lifecycle_observer.heartbeat_stale_after_seconds)}</p>
            <p>updated: {formatDate(opsStatus?.lifecycle_observer.observer_watermark?.updated_at)}</p>
          </div>
        </div>
      </section>

      <section className="admin-feature-panel" aria-label="Module C execution status">
        <div className="admin-feature-head">
          <div>
            <p className="eyebrow">Module C execution</p>
            <h2>Read-only batch evidence</h2>
          </div>
          <button
            className="ghost-button"
            type="button"
            onClick={() => void refreshModuleCExecution()}
          >
            <RefreshCw size={16} />
            <span>Refresh execution</span>
          </button>
        </div>
        {moduleCExecutionState.stale ? (
          <p className="form-error admin-error" role="status">
            <AlertCircle size={15} />
            <span>
              {moduleCExecutionState.reason}: {moduleCExecutionState.error ?? "request failed"};{" "}
              {moduleCExecutionState.snapshot
                ? "showing the last successful snapshot"
                : "no successful snapshot available"}
            </span>
          </p>
        ) : null}
        <div className="admin-feature-grid">
          <div className="admin-feature-card">
            <h3>Batch and tasks</h3>
            <p>batch: {moduleCBatch?.batch_id ?? "--"}</p>
            <p>kind: {moduleCBatch?.batch_kind ?? "--"}</p>
            <p>parent: {moduleCBatch?.parent_status ?? "--"}</p>
            <p>child: {moduleCBatch?.child_status ?? "--"}</p>
            <p>running parents: {moduleCExecution?.running_parent_batches ?? "--"}</p>
            <p>running children: {moduleCExecution?.running_child_batches ?? "--"}</p>
            <p>active symbols: {moduleCBatch?.execution.active_symbols ?? "--"}</p>
            <p>shards: {moduleCBatch?.execution.shard_count ?? "--"}</p>
            {moduleCBatch?.execution.tasks.map((task) => (
              <p key={`${task.chan_level}-${task.status}`}>
                level {task.chan_level} / {task.status}: {task.count} tasks, {task.bars} bars,
                {" "}{task.strokes} strokes, {task.segments} segments, {task.centers} centers,
                {" "}{task.signals} signals
              </p>
            ))}
            <p>latest update: {formatDate(moduleCBatch?.execution.latest_task_update)}</p>
          </div>
          <div className="admin-feature-card">
            <h3>Retry and leases</h3>
            <p>max_attempts: {moduleCBatch?.frozen_config.max_attempts ?? "--"}</p>
            <p>running_tasks: {moduleCExecution?.running_tasks ?? "--"}</p>
            <p>retryable_failed: {moduleCBatch?.execution.retryable_failed ?? "--"}</p>
            <p>exhausted_failed: {moduleCBatch?.execution.exhausted_failed ?? "--"}</p>
            <p>expired_leases: {moduleCBatch?.execution.expired_leases ?? "--"}</p>
            <p>dispositions: {moduleCBatch?.execution.disposition_rows ?? "--"}</p>
            <p>read-only: {formatBoolean(moduleCExecution?.readonly)}</p>
            <p>observed: {formatDate(moduleCExecution?.observed_at)}</p>
          </div>
          <div className="admin-feature-card">
            <h3>Strict provenance</h3>
            <p>policy: {moduleCBatch?.provenance.policy ?? "--"}</p>
            <p>eligibility build: {moduleCBatch?.provenance.eligibility_build_id ?? "--"}</p>
            <p>audit run: {moduleCBatch?.provenance.canonical_audit_run_id ?? "--"}</p>
            <p>audit status: {moduleCBatch?.provenance.audit_status ?? "--"}</p>
            <p>audit apply mode: {formatBoolean(moduleCBatch?.provenance.audit_apply_mode ?? undefined)}</p>
            <p>audit evidence: <HashValue value={moduleCBatch?.provenance.audit_evidence_sha256} /></p>
            <p>audit checkpoint: <HashValue value={moduleCBatch?.provenance.audit_checkpoint_sha256} /></p>
            <p>eligibility manifest: <HashValue value={moduleCBatch?.provenance.eligibility_manifest_sha256} /></p>
            <p>build manifest: <HashValue value={moduleCBatch?.provenance.build_manifest_sha256} /></p>
            <p>freshness contract: <HashValue value={moduleCBatch?.provenance.freshness_contract_sha256} /></p>
            <p>evidence complete: {formatBoolean(moduleCBatch?.provenance.evidence_complete)}</p>
            <p>manifest matches: {formatBoolean(moduleCBatch?.provenance.eligibility_manifest_matches)}</p>
            <p>config matches: {formatBoolean(moduleCBatch?.provenance.config_hash_matches)}</p>
            <p>frozen config matches: {formatBoolean(moduleCBatch?.provenance.frozen_config_matches)}</p>
            <p>execution identity matches: {formatBoolean(moduleCBatch?.provenance.execution_identity_matches)}</p>
          </div>
          <div className="admin-feature-card">
            <h3>Freshness and catalog drift</h3>
            <p>freshness: {moduleCBatch?.freshness.status ?? "--"}</p>
            <p>as of: {formatDate(moduleCBatch?.freshness.as_of)}</p>
            <p>reasons: {formatReasons(moduleCBatch?.freshness.reasons)}</p>
            <p>catalog generation: {moduleCBatch?.provenance.catalog_generation_id ?? "--"}</p>
            <p>catalog status: {moduleCBatch?.provenance.catalog_generation_status ?? "--"}</p>
            <p>catalog revision: {moduleCBatch?.provenance.catalog_control_revision ?? "--"}</p>
            <p>live revision: {moduleCBatch?.provenance.live_catalog_control_revision ?? "--"}</p>
            <p>catalog active: {formatBoolean(moduleCBatch?.provenance.catalog_is_active)}</p>
            <p>catalog revision matches: {formatBoolean(moduleCBatch?.provenance.catalog_revision_matches)}</p>
            <p>live universe matches: {formatBoolean(moduleCBatch?.provenance.live_universe_matches)}</p>
            <p>catalog manifest matches: {formatBoolean(moduleCBatch?.provenance.catalog_manifest_matches)}</p>
            <p>drift_reasons: {formatReasons(moduleCBatch?.provenance.drift_reasons)}</p>
            <p>catalog manifest: <HashValue value={moduleCBatch?.provenance.catalog_manifest_sha256} /></p>
            <p>active universe: <HashValue value={moduleCBatch?.provenance.audit_active_universe_sha256} /></p>
            <h4>expected_closed_watermarks</h4>
            <WatermarkList values={moduleCBatch?.freshness.expected_closed_watermarks} />
            <h4>actual_checkpoint_watermarks</h4>
            <WatermarkList values={moduleCBatch?.freshness.actual_checkpoint_watermarks} />
          </div>
        </div>
      </section>

      <section className="admin-feature-panel" aria-label="运行时功能开关">
        <div className="admin-feature-head">
          <div>
            <p className="eyebrow">运行时配置</p>
            <h2>功能开关</h2>
          </div>
          <button
            className="ghost-button"
            type="button"
            onClick={() => void refreshFeatureConfig()}
            disabled={mutating}
          >
            <RefreshCw size={16} />
            <span>重新加载</span>
          </button>
        </div>

        <div className="admin-feature-grid">
          <FeatureSwitchGroup
            title="右侧栏"
            area="rightSidebar"
            config={featureConfig}
            disabled={mutating}
            onToggle={toggleFeature}
          />
          <FeatureSwitchGroup
            title="底部工具栏"
            area="screenerDock"
            config={featureConfig}
            disabled={mutating}
            onToggle={toggleFeature}
          />
        </div>
      </section>

      <section className="admin-feature-panel" aria-label="问财配置">
        <div className="admin-feature-head">
          <div>
            <p className="eyebrow">问财配置</p>
            <h2>问财 API / Cookie</h2>
          </div>
          <button className="ghost-button" type="button" onClick={() => void refreshWencaiConfig(true)} disabled={wencaiFeedback?.state === "pending"}>
            <RefreshCw size={16} />
            <span>重新加载</span>
          </button>
        </div>
        <div className="admin-form-grid">
          <label className="admin-field">
            <span>OpenAPI Base URL</span>
            <input
              value={wencaiConfig.base_url}
              onChange={(event) =>
                setWencaiConfig((current) => ({ ...current, base_url: event.target.value }))
              }
              placeholder="https://openapi.iwencai.com"
            />
          </label>
          <label className="admin-field">
            <span>Legacy API Key</span>
            <input
              type="password"
              value={wencaiConfig.api_key}
              onChange={(event) =>
                setWencaiConfig((current) => ({ ...current, api_key: event.target.value }))
              }
              placeholder="优先使用 IWENCAI_API_KEY"
            />
          </label>
          {wencaiConfig.api_keys.map((apiKey, index) => (
            <div className="admin-form-grid admin-field-wide" key={`${apiKey.label}-${index}`}>
              <label className="admin-field"><span>Label</span><input value={apiKey.label} onChange={(event) => updateWencaiKey(index, { label: event.target.value })} /></label>
              <label className="admin-field"><span>API Key</span><input type="password" value={apiKey.key} onChange={(event) => updateWencaiKey(index, { key: event.target.value })} autoComplete="new-password" /></label>
              <label className="admin-field"><span>Priority</span><input type="number" value={apiKey.priority} onChange={(event) => updateWencaiKey(index, { priority: Number(event.target.value) || 0 })} /></label>
              <label className="admin-check"><input type="checkbox" checked={apiKey.enabled} onChange={(event) => updateWencaiKey(index, { enabled: event.target.checked })} /><span>Enabled</span></label>
              <button type="button" onClick={() => setWencaiConfig((current) => ({ ...current, api_keys: current.api_keys.filter((_, keyIndex) => keyIndex !== index) }))}><Trash2 size={15} /><span>Remove</span></button>
            </div>
          ))}
          <button type="button" onClick={() => setWencaiConfig((current) => ({ ...current, api_keys: [...current.api_keys, { label: `key-${current.api_keys.length + 1}`, key: "", enabled: true, priority: nextWencaiPriority(current.api_keys) }] }))}><Plus size={15} /><span>Add key</span></button>
          <label className="admin-field admin-field-wide">
            <span>Cookie</span>
            <textarea
              value={wencaiConfig.cookie}
              onChange={(event) =>
                setWencaiConfig((current) => ({ ...current, cookie: event.target.value }))
              }
              placeholder="复制浏览器请求头中的 Cookie"
              rows={3}
            />
          </label>
          <label className="admin-field">
            <span>User Agent</span>
            <input
              value={wencaiConfig.user_agent ?? ""}
              onChange={(event) =>
                setWencaiConfig((current) => ({ ...current, user_agent: event.target.value }))
              }
              placeholder="可选"
            />
          </label>
          <label className="admin-field">
            <span>超时秒数</span>
            <input
              type="number"
              min={1}
              max={5}
              value={wencaiConfig.timeout_seconds}
              onChange={(event) =>
                setWencaiConfig((current) => ({
                  ...current,
                  timeout_seconds: Math.min(5, Number(event.target.value) || 5),
                }))
              }
            />
          </label>
          <label className="admin-check">
            <input
              type="checkbox"
              checked={wencaiConfig.pro}
              onChange={(event) =>
                setWencaiConfig((current) => ({ ...current, pro: event.target.checked }))
              }
            />
            <span>使用问财专业版</span>
          </label>
        </div>
        <div className="admin-config-actions">
          <button type="button" onClick={() => void handleTestWencaiConfig()} disabled={mutating}>
            <TestTube2 size={15} />
            <span>测试连接</span>
          </button>
          <button type="button" onClick={() => void handleSaveWencaiConfig()} disabled={mutating}>
            <Save size={15} />
            <span>保存配置</span>
          </button>
          {wencaiTestResult ? <TestResultBadge result={wencaiTestResult} /> : null}
          {wencaiFeedback ? <span className="admin-test-result" data-state={wencaiFeedback.state} role="status" aria-live="polite">{wencaiFeedback.message}</span> : null}
        </div>
      </section>

      <section className="admin-feature-panel" aria-label="LLM 接入点">
        <div className="admin-feature-head">
          <div>
            <p className="eyebrow">LLM 配置</p>
            <h2>模型接入点</h2>
          </div>
          <div className="admin-config-actions">
            <button type="button" onClick={addProvider}>
              <Plus size={15} />
              <span>新增接入点</span>
            </button>
            <button type="button" onClick={() => void handleSaveLlmProviders()} disabled={mutating}>
              <Save size={15} />
              <span>保存全部</span>
            </button>
          </div>
        </div>
        <div className="llm-provider-stack">
          {llmConfig.providers.map((provider) => (
            <div className="llm-provider-card" key={provider.id}>
              <div className="llm-provider-head">
                <label className="admin-check">
                  <input
                    type="radio"
                    checked={llmConfig.active_provider_id === provider.id}
                    onChange={() =>
                      setLlmConfig((current) => ({
                        ...current,
                        active_provider_id: provider.id,
                      }))
                    }
                  />
                  <span>当前使用</span>
                </label>
                <label className="admin-check">
                  <input
                    type="checkbox"
                    checked={provider.enabled}
                    onChange={(event) => updateProvider(provider.id, { enabled: event.target.checked })}
                  />
                  <span>启用</span>
                </label>
                <button type="button" onClick={() => void handleTestLlmProvider(provider)} disabled={mutating}>
                  <TestTube2 size={15} />
                  <span>测试连接</span>
                </button>
                <button type="button" onClick={() => removeProvider(provider.id)}>
                  <Trash2 size={15} />
                  <span>删除</span>
                </button>
              </div>
              <div className="admin-form-grid">
                <label className="admin-field">
                  <span>接入点 ID</span>
                  <input value={provider.id} onChange={(event) => updateProvider(provider.id, { id: event.target.value })} />
                </label>
                <label className="admin-field">
                  <span>名称</span>
                  <input value={provider.name} onChange={(event) => updateProvider(provider.id, { name: event.target.value })} />
                </label>
                <label className="admin-field admin-field-wide">
                  <span>Base URL</span>
                  <input value={provider.base_url} onChange={(event) => updateProvider(provider.id, { base_url: event.target.value })} />
                </label>
                <label className="admin-field admin-field-wide">
                  <span>API Key</span>
                  <input value={provider.api_key} onChange={(event) => updateProvider(provider.id, { api_key: event.target.value })} />
                </label>
                <label className="admin-field">
                  <span>默认模型</span>
                  <select
                    value={provider.active_model}
                    onChange={(event) => updateProvider(provider.id, { active_model: event.target.value })}
                  >
                    {provider.models.map((model) => (
                      <option value={model} key={model}>
                        {model}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="admin-field">
                  <span>超时秒数</span>
                  <input
                    type="number"
                    min={1}
                    value={provider.timeout_seconds}
                    onChange={(event) =>
                      updateProvider(provider.id, { timeout_seconds: Number(event.target.value) || 20 })
                    }
                  />
                </label>
                <label className="admin-field admin-field-wide">
                  <span>备选模型（一行一个）</span>
                  <textarea
                    rows={3}
                    value={provider.models.join("\n")}
                    onChange={(event) => {
                      const models = splitModels(event.target.value);
                      updateProvider(provider.id, {
                        models,
                        active_model: models.includes(provider.active_model)
                          ? provider.active_model
                          : models[0] ?? "",
                      });
                    }}
                  />
                </label>
              </div>
              {llmTestResults[provider.id] ? (
                <LlmTestResultBadge result={llmTestResults[provider.id]} />
              ) : null}
            </div>
          ))}
        </div>
      </section>
    </section>
  );
}

function HashValue({ value }: { value?: string | null }) {
  if (!value) {
    return <code>--</code>;
  }
  const displayValue = value.length > 12 ? `${value.slice(0, 12)}…` : value;
  return <code title={value}>{displayValue}</code>;
}

function WatermarkList({
  values,
}: {
  values?: (ModuleCFreshnessExpectedWatermark | ModuleCFreshnessActualWatermark)[];
}) {
  if (!values?.length) {
    return <p>--</p>;
  }
  return (
    <div>
      {values.map((value) => (
        <p key={value.timeframe}>
          {value.timeframe}: expected {formatDate(value.expected)}
          {"actual_min" in value ? (
            <>
              , actual {formatDate(value.actual_min)} to {formatDate(value.actual_max)}, empty{" "}
              {value.empty_scopes}, stale {value.stale_scopes}, future {value.future_scopes}
            </>
          ) : null}
        </p>
      ))}
    </div>
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

function TestResultBadge({ result }: { result: ConnectivityTestResult }) {
  return (
    <span className="admin-test-result" data-ok={result.ok}>
      {result.ok ? "连接成功" : "连接失败"}，耗时 {result.latency_ms}ms，样例 {result.sample_count} 条：{result.message}
    </span>
  );
}

function LlmTestResultBadge({ result }: { result: LlmTestResult }) {
  return (
    <span className="admin-test-result" data-ok={result.ok}>
      {result.ok ? "连接成功" : "连接失败"}，{result.provider} / {result.model}，耗时 {result.latency_ms}ms：{result.message}
    </span>
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

function withDefaultProvider(config: LlmProvidersConfig): LlmProvidersConfig {
  const provider = createDefaultProvider("siliconflow");
  return {
    active_provider_id: config.active_provider_id ?? provider.id,
    providers: config.providers.length ? config.providers : [provider],
  };
}

function createDefaultProvider(id: string): LlmProviderConfig {
  return {
    id,
    name: "硅基流动",
    base_url: "https://api.siliconflow.cn/v1",
    api_key: "",
    models: ["deepseek-ai/DeepSeek-V3.2"],
    active_model: "deepseek-ai/DeepSeek-V3.2",
    enabled: true,
    timeout_seconds: 20,
  };
}

function splitModels(value: string): string[] {
  return value
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function nextWencaiPriority(keys: WencaiAdminConfig["api_keys"]): number {
  return keys.reduce((maximum, item) => Math.max(maximum, item.priority), -1) + 1;
}

function readError(value: unknown): string {
  return value instanceof Error ? value.message : String(value);
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

function formatAge(value?: number | null): string {
  if (value === undefined || value === null) {
    return "--";
  }
  return `${value}s`;
}

function formatBoolean(value?: boolean): string {
  if (value === undefined) {
    return "--";
  }
  return value ? "yes" : "no";
}

function formatReasons(values?: string[]): string {
  return values?.length ? values.join(", ") : "--";
}
