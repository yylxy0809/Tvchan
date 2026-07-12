import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const PROVIDER_VERSION = "westock-data@1.0.4";
const maxBytes = Number.parseInt(process.env.WESTOCK_MAX_OUTPUT_BYTES ?? "1048576", 10);
const modulePath = process.env.WESTOCK_BRIDGE_MODULE;
const expectedHash = process.env.WESTOCK_BRIDGE_SHA256;
const declaredVersion = process.env.WESTOCK_BRIDGE_MODULE_VERSION;

if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw new Error("invalid bridge output limit");
if (!modulePath || !expectedHash || declaredVersion !== PROVIDER_VERSION) {
  throw new Error("verified WeStock module path, SHA-256, and exact version are required");
}

const moduleBytes = await readFile(modulePath);
const actualHash = createHash("sha256").update(moduleBytes).digest("hex");
if (!/^[a-f0-9]{64}$/.test(expectedHash) || actualHash !== expectedHash) {
  throw new Error("WeStock module integrity check failed");
}
const provider = await import(pathToFileURL(modulePath).href);
if (typeof provider.handle !== "function") throw new Error("verified provider must export handle(request)");

function send(message) {
  let output;
  try {
    output = `${JSON.stringify(message)}\n`;
  } catch {
    output = `${JSON.stringify({ id: message?.id ?? null, ok: false, error: "provider result is not JSON serializable" })}\n`;
  }
  if (Buffer.byteLength(output) > maxBytes) {
    output = `${JSON.stringify({ id: message?.id ?? null, ok: false, error: "provider output exceeded limit" })}\n`;
  }
  process.stdout.write(output);
}

async function processLine(line) {
  let request;
  try {
    request = JSON.parse(line);
    if (!request || typeof request.id !== "string") throw new Error("invalid request id");
    if (!["profile", "finance", "asfund", "board"].includes(request.operation)) throw new Error("invalid operation");
    if (!Array.isArray(request.symbols) || !request.symbols.every((value) => /^(sh|sz|bj)\d{6}$/.test(value))) {
      throw new Error("invalid symbols");
    }
    if (request.operation === "board" && request.symbols.length !== 0) throw new Error("board does not accept symbols");
    const data = await provider.handle({ operation: request.operation, symbols: request.symbols });
    send({ id: request.id, ok: true, data });
  } catch (error) {
    send({ id: request?.id ?? null, ok: false, error: String(error?.message ?? error).slice(0, 512) });
  }
}

let pending = Buffer.alloc(0);
let chain = Promise.resolve();
process.stdin.on("data", (chunk) => {
  pending = Buffer.concat([pending, chunk]);
  if (pending.length > maxBytes && !pending.includes(10)) {
    send({ id: null, ok: false, error: "request exceeded limit" });
    process.exitCode = 1;
    process.stdin.destroy();
    return;
  }
  let newline;
  while ((newline = pending.indexOf(10)) !== -1) {
    const line = pending.subarray(0, newline);
    pending = pending.subarray(newline + 1);
    if (line.length > maxBytes) {
      send({ id: null, ok: false, error: "request exceeded limit" });
      continue;
    }
    chain = chain.then(() => processLine(line.toString("utf8")));
  }
});
