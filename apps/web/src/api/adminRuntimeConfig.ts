import { apiUrl } from "../config";

export type WencaiAdminConfig = {
  base_url: string;
  api_key: string;
  cookie: string;
  user_agent?: string | null;
  pro: boolean;
  timeout_seconds: number;
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
    throw new Error(await readResponseError(response));
  }
  return response.json() as Promise<T>;
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
