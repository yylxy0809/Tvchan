import { apiUrl } from "../config";
import { requestAdmin } from "../api/adminRequest";

export type UserRole = "user" | "admin";

export type AuthSession = {
  token: string;
  role: UserRole;
  displayName?: string | null;
  label?: string | null;
};

export type AdminToken = {
  id: number;
  label: string;
  display_name?: string | null;
  role: UserRole | string;
  is_active: boolean;
  created_at?: string;
  updated_at?: string;
  disabled_at?: string | null;
  last_used_at?: string | null;
  token?: string;
};

type LoginResponse = {
  valid: boolean;
  role?: string | null;
  display_name?: string | null;
  label?: string | null;
};

type AdminTokenListResponse = {
  items?: AdminToken[];
};

export class AuthenticationError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "AuthenticationError";
  }
}

export async function loginWithToken(token: string): Promise<AuthSession> {
  const normalized = token.trim();
  if (!normalized) {
    throw new Error("Please enter an access token.");
  }

  try {
    const response = await fetch(apiUrl("/api/v1/auth/login"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${normalized}`,
      },
      body: JSON.stringify({ token: normalized }),
    });
    if (!response.ok) {
      throw new AuthenticationError(
        await readResponseError(response, normalized),
        response.status,
      );
    }
    const data = (await response.json()) as LoginResponse;
    if (data.valid !== true) {
      throw new AuthenticationError("Invalid access token.", 403);
    }
    const role = parseRole(data.role);
    if (!role) {
      throw new AuthenticationError(
        "Authentication service returned an invalid role.",
        502,
      );
    }
    return {
      token: normalized,
      role,
      displayName: data.display_name,
      label: data.label,
    };
  } catch (error) {
    if (error instanceof AuthenticationError) {
      throw error;
    }
    if (error instanceof TypeError) {
      throw new AuthenticationError("Authentication service unavailable.");
    }
    throw error;
  }
}

export async function listAdminTokens(adminToken: string): Promise<AdminToken[]> {
  const data = await requestAdmin<AdminTokenListResponse | AdminToken[]>(
    adminToken,
    "/api/v1/admin/tokens",
  );
  return Array.isArray(data) ? data : data.items ?? [];
}

export async function createAdminToken(adminToken: string, input: {
  label: string;
  display_name?: string | null;
}): Promise<AdminToken> {
  return requestAdmin<AdminToken>(adminToken, "/api/v1/admin/tokens", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export async function disableAdminToken(
  adminToken: string,
  id: number,
): Promise<AdminToken> {
  return requestAdmin<AdminToken>(
    adminToken,
    `/api/v1/admin/tokens/${encodeURIComponent(String(id))}/disable`,
    { method: "POST" },
  );
}

export async function deleteAdminToken(adminToken: string, id: number): Promise<void> {
  await requestAdmin(
    adminToken,
    `/api/v1/admin/tokens/${encodeURIComponent(String(id))}`,
    { method: "DELETE" },
  );
}

function parseRole(value: unknown): UserRole | null {
  return value === "admin" || value === "user" ? value : null;
}

async function readResponseError(response: Response, token: string): Promise<string> {
  const text = await response.text();
  if (!text) {
    return `${response.status} ${response.statusText}`;
  }
  try {
    const data = JSON.parse(text) as { detail?: unknown; message?: unknown };
    return redactToken(String(data.detail ?? data.message ?? text), token);
  } catch {
    return redactToken(`${response.status} ${response.statusText}: ${text}`, token);
  }
}

function redactToken(message: string, token: string): string {
  return message.split(token).join("[redacted]");
}
