import { apiUrl } from "../config";

export type UserSettingBucket =
  | "theme"
  | "watchlist"
  | "layout"
  | "indicatorSettings";

export type UserSetting = {
  bucket: UserSettingBucket;
  value: unknown;
  version: number;
  updated_at?: string;
};

type UserSettingsResponse = {
  items?: UserSetting[];
};

export async function listUserSettings(token: string): Promise<UserSetting[]> {
  const response = await fetch(apiUrl("/api/v1/user/settings"), {
    headers: authHeaders(token),
  });
  if (!response.ok) {
    throw new Error(await readResponseError(response));
  }
  const data = (await response.json()) as UserSettingsResponse;
  return Array.isArray(data.items) ? data.items : [];
}

export async function saveUserSetting(
  token: string,
  bucket: UserSettingBucket,
  value: unknown,
): Promise<UserSetting> {
  const response = await fetch(apiUrl(`/api/v1/user/settings/${bucket}`), {
    method: "PUT",
    headers: {
      ...authHeaders(token),
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ value }),
  });
  if (!response.ok) {
    throw new Error(await readResponseError(response));
  }
  return (await response.json()) as UserSetting;
}

function authHeaders(token: string): Record<string, string> {
  return token.trim() ? { Authorization: `Bearer ${token.trim()}` } : {};
}

async function readResponseError(response: Response): Promise<string> {
  try {
    const data = (await response.json()) as { detail?: unknown };
    if (typeof data.detail === "string") {
      return data.detail;
    }
  } catch {
    // Fall back to status text.
  }
  return response.statusText || `HTTP ${response.status}`;
}
