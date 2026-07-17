import { apiUrl } from "../config";
import { requestAdmin } from "./adminRequest";

export type RuntimeFeatureArea = "rightSidebar" | "screenerDock";

export type RuntimeFeatureOverride = {
  id: string;
  enabled?: boolean;
  order?: number;
};

export type RuntimeFeatureConfig = Record<RuntimeFeatureArea, RuntimeFeatureOverride[]>;

const RUNTIME_CONFIG_PATH = "/api/v1/config/features";

export async function fetchRuntimeFeatureConfig(): Promise<RuntimeFeatureConfig> {
  const response = await fetch(apiUrl(RUNTIME_CONFIG_PATH));
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return normalizeRuntimeFeatureConfig(await response.json());
}

export async function saveRuntimeFeatureConfig(
  token: string,
  config: RuntimeFeatureConfig,
): Promise<RuntimeFeatureConfig> {
  return normalizeRuntimeFeatureConfig(await requestAdmin(
    token,
    "/api/v1/admin/runtime-config/frontend.features",
    {
      method: "PUT",
      body: JSON.stringify({ value: { features: config } }),
    },
  ));
}

export function normalizeRuntimeFeatureConfig(payload: unknown): RuntimeFeatureConfig {
  const root = asRecord(payload);
  const runtimeValue = asRecord(root?.value) ?? root;
  const featureSource =
    asRecord(runtimeValue?.features) ??
    asRecord(runtimeValue?.featureFlags) ??
    asRecord(runtimeValue?.feature_flags) ??
    runtimeValue;

  return {
    rightSidebar: readFeatureOverrides(
      featureSource?.rightSidebar ??
        featureSource?.right_sidebar ??
        featureSource?.["right-sidebar"],
    ),
    screenerDock: readFeatureOverrides(
      featureSource?.screenerDock ??
        featureSource?.screener_dock ??
        featureSource?.["screener-dock"],
    ),
  };
}

function readFeatureOverrides(value: unknown): RuntimeFeatureOverride[] {
  if (Array.isArray(value)) {
    return value
      .map((item, index) => normalizeFeatureOverride(item, undefined, index))
      .filter((item): item is RuntimeFeatureOverride => item !== null);
  }

  const record = asRecord(value);
  if (!record) {
    return [];
  }

  return Object.entries(record)
    .map(([id, item]) => normalizeFeatureOverride(item, id))
    .filter((item): item is RuntimeFeatureOverride => item !== null);
}

function normalizeFeatureOverride(
  value: unknown,
  fallbackId?: string,
  fallbackOrder?: number,
): RuntimeFeatureOverride | null {
  if (typeof value === "string") {
    const id = readString(value);
    return id ? { id, enabled: true, order: fallbackOrder } : null;
  }

  if (typeof value === "boolean") {
    return fallbackId ? { id: fallbackId, enabled: value } : null;
  }

  if (typeof value === "number") {
    return fallbackId && Number.isFinite(value)
      ? { id: fallbackId, order: value }
      : null;
  }

  const record = asRecord(value);
  if (!record) {
    return null;
  }

  const id = readString(record.id) ?? fallbackId;
  if (!id) {
    return null;
  }

  const enabled = readBoolean(record.enabled);
  const order = readNumber(record.order) ?? fallbackOrder;
  if (enabled === undefined && order === undefined) {
    return null;
  }

  return {
    id,
    ...(enabled === undefined ? {} : { enabled }),
    ...(order === undefined ? {} : { order }),
  };
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function readBoolean(value: unknown): boolean | undefined {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value !== "string") {
    return undefined;
  }
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return undefined;
}

function readNumber(value: unknown): number | undefined {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}
