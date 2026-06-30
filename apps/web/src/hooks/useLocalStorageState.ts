import { useEffect, useState } from "react";

export function useLocalStorageState<T>(
  key: string,
  fallback: T,
  revive?: (value: unknown) => T,
): [T, (value: T | ((current: T) => T)) => void] {
  const [state, setState] = useState<T>(() => {
    if (typeof window === "undefined") {
      return fallback;
    }
    try {
      const raw = window.localStorage.getItem(key);
      if (!raw) {
        return fallback;
      }
      const parsed = JSON.parse(raw) as unknown;
      return revive ? revive(parsed) : (parsed as T);
    } catch {
      return fallback;
    }
  });

  useEffect(() => {
    try {
      window.localStorage.setItem(key, JSON.stringify(state));
    } catch {
      // The UI should remain usable even when localStorage is unavailable.
    }
  }, [key, state]);

  return [state, setState];
}
