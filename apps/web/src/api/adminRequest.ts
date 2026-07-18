import { apiUrl } from "../config";

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

export function handleAdminAuthenticationFailure(
  error: unknown,
  onAuthenticationFailure: () => void,
): boolean {
  if (!isAdminAuthFailure(error)) {
    return false;
  }
  onAuthenticationFailure();
  return true;
}

export async function requestAdmin<T = unknown>(
  token: string,
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const headers = buildAdminHeaders(token, init.headers);
  const response = await fetch(apiUrl(path), {
    ...init,
    headers,
  });
  if (!response.ok) {
    const message = redactToken(await readResponseError(response), token);
    throw new AdminRequestError(response.status, message);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json() as Promise<T>;
}

export function buildAdminHeaders(token: string, initial?: HeadersInit): Headers {
  const headers = new Headers(initial);
  if (!headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  headers.set("Authorization", `Bearer ${token.trim()}`);
  return headers;
}

function redactToken(message: string, token: string): string {
  const normalized = token.trim();
  return normalized ? message.split(normalized).join("[redacted]") : message;
}

async function readResponseError(response: Response): Promise<string> {
  let text: string;
  try {
    text = await response.text();
  } catch {
    return `${response.status} ${response.statusText}`.trim();
  }
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
