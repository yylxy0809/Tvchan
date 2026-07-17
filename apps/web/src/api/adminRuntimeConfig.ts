import { apiUrl } from "../config";

export type WencaiAdminConfig = {
  base_url: string;
  api_key: string;
  cookie: string;
  user_agent?: string | null;
  pro: boolean;
  timeout_seconds: number;
  config_version?: number;
  api_keys: WencaiApiKeyConfig[];
};

export type WencaiApiKeyConfig = {
  label: string;
  key: string;
  enabled: boolean;
  priority: number;
};

export type ConnectivityTestResult = {
  ok: boolean;
  latency_ms: number;
  message: string;
  sample_count: number;
};

export type LlmProviderConfig = {
  id: string;
  name: string;
  base_url: string;
  api_key: string;
  models: string[];
  active_model: string;
  enabled: boolean;
  timeout_seconds: number;
};

export type LlmProvidersConfig = {
  active_provider_id?: string | null;
  providers: LlmProviderConfig[];
};

export type LlmTestResult = {
  ok: boolean;
  latency_ms: number;
  provider: string;
  model: string;
  message: string;
};

export type LifecycleObserverStatus = {
  status: "unavailable" | "degraded" | "healthy";
  deployed: boolean;
  expected_observer_name: string;
  heartbeat_age_seconds?: number | null;
  heartbeat_stale_after_seconds?: number;
  reason?: string;
  error?: string;
  counts?: {
    pending: number;
    processing: number;
    failed: number;
    dead_letter: number;
  };
  oldest_backlog_at?: string | null;
  oldest_backlog_age_seconds?: number | null;
  max_outbox_id?: number;
  observer_watermark?: {
    observer_name: string;
    last_outbox_id: number;
    updated_at: string;
    lag: number;
  } | null;
};

export type AdminOpsStatus = {
  status: "ok" | "degraded";
  lifecycle_observer: LifecycleObserverStatus;
};

export type ModuleCExecutionTask = {
  chan_level: number;
  status: string;
  count: number;
  attempts: number;
  bars: number;
  strokes: number;
  segments: number;
  centers: number;
  signals: number;
  latest_update: string | null;
};

export type ModuleCFrozenConfig = {
  contract: string | null;
  levels: string[];
  modes: string[];
  concurrency_per_worker: number | null;
  shard_count: number | null;
  max_attempts: number | null;
  eligibility_build_id: string | null;
};

export type ModuleCExecutionSummary = {
  shard_count: number;
  active_symbols: number;
  disposition_rows: number;
  latest_task_update: string | null;
  tasks: ModuleCExecutionTask[];
  retryable_failed: number | null;
  exhausted_failed: number | null;
  expired_leases: number;
};

export type ModuleCExecutionProvenance = {
  policy: string | null;
  eligibility_build_id: string | null;
  manifest_version: string | null;
  eligibility_manifest_sha256: string | null;
  build_manifest_sha256: string | null;
  canonical_audit_run_id: string | null;
  audit_evidence_sha256: string | null;
  audit_checkpoint_sha256: string | null;
  audit_status: string | null;
  audit_apply_mode: boolean | null;
  audit_gate_pass: boolean | null;
  freshness_contract_version: string | null;
  freshness_contract_sha256: string | null;
  catalog_generation_id: string | null;
  catalog_control_revision: number | null;
  catalog_manifest_sha256: string | null;
  audit_active_universe_sha256: string | null;
  catalog_generation_status: string | null;
  catalog_is_active: boolean;
  live_catalog_control_revision: number | null;
  catalog_revision_matches: boolean;
  eligibility_manifest_matches: boolean;
  config_hash_matches: boolean;
  frozen_config_matches: boolean;
  execution_identity_matches: boolean;
  live_universe_matches: boolean;
  catalog_manifest_matches: boolean;
  evidence_complete: boolean;
  drift_reasons: string[];
};

export type ModuleCFreshnessExpectedWatermark = {
  timeframe: string;
  expected: string | null;
};

export type ModuleCFreshnessActualWatermark = ModuleCFreshnessExpectedWatermark & {
  actual_min: string | null;
  actual_max: string | null;
  empty_scopes: number;
  stale_scopes: number;
  future_scopes: number;
};

export type ModuleCExecutionFreshness = {
  as_of: string | null;
  status: string;
  reasons: string[];
  expected_closed_watermarks: ModuleCFreshnessExpectedWatermark[];
  actual_checkpoint_watermarks: ModuleCFreshnessActualWatermark[];
};

export type ModuleCExecutionBatch = {
  batch_id: number;
  batch_key: string;
  batch_kind: string;
  parent_status: string;
  child_status: string;
  publication_namespace: string;
  profile_id: string;
  run_group_id: string;
  code_commit: string;
  image_digest: string;
  vendor_manifest_sha256: string;
  config_hash: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string;
  execution: ModuleCExecutionSummary;
  frozen_config: ModuleCFrozenConfig;
  provenance: ModuleCExecutionProvenance;
  freshness: ModuleCExecutionFreshness;
};

export type ModuleCExecutionStatus = {
  observed_at: string;
  readonly: boolean;
  running_parent_batches: number;
  running_child_batches: number;
  running_tasks: number;
  batch: ModuleCExecutionBatch | null;
};

export class AdminRequestError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "AdminRequestError";
  }
}

export function isAdminAuthFailure(error: unknown): error is AdminRequestError {
  return error instanceof AdminRequestError && (error.status === 401 || error.status === 403);
}

export async function fetchWencaiConfig(token: string): Promise<WencaiAdminConfig> {
  return requestAdmin<WencaiAdminConfig>(token, "/api/v1/admin/wencai/config");
}

export async function saveWencaiConfig(
  token: string,
  config: WencaiAdminConfig,
): Promise<WencaiAdminConfig> {
  return requestAdmin<WencaiAdminConfig>(token, "/api/v1/admin/wencai/config", {
    method: "PUT",
    body: JSON.stringify(config),
  });
}

export async function testWencaiConfig(
  token: string,
  config: WencaiAdminConfig,
): Promise<ConnectivityTestResult> {
  return requestAdmin<ConnectivityTestResult>(token, "/api/v1/admin/wencai/test", {
    method: "POST",
    body: JSON.stringify(config),
  });
}

export async function fetchLlmProviders(token: string): Promise<LlmProvidersConfig> {
  return requestAdmin<LlmProvidersConfig>(token, "/api/v1/admin/llm/providers");
}

export async function fetchAdminOpsStatus(token: string): Promise<AdminOpsStatus> {
  return requestAdmin<AdminOpsStatus>(token, "/api/v1/admin/ops/status");
}

export async function fetchModuleCExecution(
  token: string,
  batchId?: number,
): Promise<ModuleCExecutionStatus> {
  const query = batchId === undefined ? "" : `?batch_id=${encodeURIComponent(String(batchId))}`;
  return requestAdmin<ModuleCExecutionStatus>(
    token,
    `/api/v1/admin/ops/module-c/execution${query}`,
  );
}

export async function saveLlmProviders(
  token: string,
  config: LlmProvidersConfig,
): Promise<LlmProvidersConfig> {
  return requestAdmin<LlmProvidersConfig>(token, "/api/v1/admin/llm/providers", {
    method: "PUT",
    body: JSON.stringify(config),
  });
}

export async function testLlmProvider(
  token: string,
  provider: LlmProviderConfig,
): Promise<LlmTestResult> {
  return requestAdmin<LlmTestResult>(token, "/api/v1/admin/llm/test", {
    method: "POST",
    body: JSON.stringify(provider),
  });
}

async function requestAdmin<T>(
  token: string,
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token.trim()}`,
      ...init.headers,
    },
  });
  if (!response.ok) {
    const message = redactToken(await readResponseError(response), token);
    throw new AdminRequestError(response.status, message);
  }
  return response.json() as Promise<T>;
}

function redactToken(message: string, token: string): string {
  const normalized = token.trim();
  return normalized ? message.split(normalized).join("[redacted]") : message;
}

async function readResponseError(response: Response): Promise<string> {
  const text = await response.text();
  if (!text) {
    return `${response.status} ${response.statusText}`;
  }
  try {
    const data = JSON.parse(text) as { detail?: unknown; message?: unknown };
    return String(data.detail ?? data.message ?? text);
  } catch {
    return `${response.status} ${response.statusText}: ${text}`;
  }
}
