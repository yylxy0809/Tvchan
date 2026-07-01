import { useEffect, useMemo, useState } from "react";
import {
  fetchRuntimeFeatureConfig,
  type RuntimeFeatureConfig,
  type RuntimeFeatureOverride,
} from "../api/runtimeConfig";
import {
  RIGHT_SIDEBAR_FEATURES,
  SCREENER_DOCK_FEATURES,
  type RightSidebarFeature,
  type ScreenerDockFeature,
} from "./featureRegistry";

const EMPTY_RUNTIME_FEATURE_CONFIG: RuntimeFeatureConfig = {
  rightSidebar: [],
  screenerDock: [],
};

const RUNTIME_FEATURE_CONFIG_CHANGED = "tv-runtime-feature-config-changed";

let cachedRuntimeFeatureConfig: RuntimeFeatureConfig | null = null;
let pendingRuntimeFeatureConfig: Promise<RuntimeFeatureConfig> | null = null;

export function useRightSidebarFeatures(): RightSidebarFeature[] {
  const runtimeConfig = useRuntimeFeatureConfig();
  return useMemo(
    () =>
      applyRuntimeFeatureOverrides(
        RIGHT_SIDEBAR_FEATURES,
        runtimeConfig?.rightSidebar,
      ),
    [runtimeConfig],
  );
}

export function useScreenerDockFeatures(): ScreenerDockFeature[] {
  const runtimeConfig = useRuntimeFeatureConfig();
  return useMemo(
    () =>
      applyRuntimeFeatureOverrides(
        SCREENER_DOCK_FEATURES,
        runtimeConfig?.screenerDock,
      ),
    [runtimeConfig],
  );
}

export function applyRuntimeFeatureOverrides<T extends { id: string }>(
  defaults: readonly T[],
  overrides: readonly RuntimeFeatureOverride[] = [],
): T[] {
  const overridesById = new Map(overrides.map((override) => [override.id, override]));
  return defaults
    .map((feature, defaultOrder) => ({
      feature,
      defaultOrder,
      override: overridesById.get(feature.id),
    }))
    .filter((item) => item.override?.enabled !== false)
    .sort((left, right) => {
      const leftOrder = left.override?.order ?? left.defaultOrder;
      const rightOrder = right.override?.order ?? right.defaultOrder;
      return leftOrder - rightOrder || left.defaultOrder - right.defaultOrder;
    })
    .map((item) => item.feature);
}

export function publishRuntimeFeatureConfig(config: RuntimeFeatureConfig) {
  cachedRuntimeFeatureConfig = config;
  if (typeof window === "undefined") {
    return;
  }
  window.dispatchEvent(
    new CustomEvent<RuntimeFeatureConfig>(RUNTIME_FEATURE_CONFIG_CHANGED, {
      detail: config,
    }),
  );
}

function useRuntimeFeatureConfig(): RuntimeFeatureConfig | null {
  const [runtimeConfig, setRuntimeConfig] = useState(cachedRuntimeFeatureConfig);

  useEffect(() => {
    let mounted = true;
    const handleRuntimeFeatureConfigChanged = (event: Event) => {
      const nextConfig = (event as CustomEvent<RuntimeFeatureConfig>).detail;
      if (nextConfig) {
        setRuntimeConfig(nextConfig);
      }
    };
    window.addEventListener(
      RUNTIME_FEATURE_CONFIG_CHANGED,
      handleRuntimeFeatureConfigChanged,
    );
    void loadRuntimeFeatureConfig().then((nextConfig) => {
      if (mounted) {
        setRuntimeConfig(nextConfig);
      }
    });
    return () => {
      mounted = false;
      window.removeEventListener(
        RUNTIME_FEATURE_CONFIG_CHANGED,
        handleRuntimeFeatureConfigChanged,
      );
    };
  }, []);

  return runtimeConfig;
}

function loadRuntimeFeatureConfig(): Promise<RuntimeFeatureConfig> {
  if (cachedRuntimeFeatureConfig) {
    return Promise.resolve(cachedRuntimeFeatureConfig);
  }
  pendingRuntimeFeatureConfig ??= fetchRuntimeFeatureConfig()
    .catch(() => EMPTY_RUNTIME_FEATURE_CONFIG)
    .then((nextConfig) => {
      cachedRuntimeFeatureConfig = nextConfig;
      return nextConfig;
    })
    .finally(() => {
      pendingRuntimeFeatureConfig = null;
    });
  return pendingRuntimeFeatureConfig;
}
