import { spawn } from "node:child_process";

const CLI = process.env.WESTOCK_CLI_PATH ?? "/opt/westock/node_modules/.bin/westock-data-clawhub";
const MAX_BYTES = Number.parseInt(process.env.WESTOCK_MAX_OUTPUT_BYTES ?? "1048576", 10);
const TIMEOUT_MS = Number.parseInt(process.env.WESTOCK_CLI_TIMEOUT_MS ?? "10000", 10);
const SYMBOL = /^(sh|sz|bj)\d{6}$/;
const OPERATIONS = new Set(["profile", "finance", "asfund", "board"]);

if (!Number.isSafeInteger(MAX_BYTES) || MAX_BYTES <= 0) throw new Error("invalid westock output limit");
if (!Number.isSafeInteger(TIMEOUT_MS) || TIMEOUT_MS <= 0) throw new Error("invalid westock timeout");

export async function handle(request) {
  if (!request || typeof request !== "object") throw new Error("invalid request");
  if (request.operation === "quote") throw new Error("quote is unsupported by westock-data-clawhub@1.0.4");
  if (!OPERATIONS.has(request.operation)) throw new Error("unsupported westock operation");
  if (!Array.isArray(request.symbols) || !request.symbols.every((symbol) => typeof symbol === "string" && SYMBOL.test(symbol))) {
    throw new Error("invalid westock symbols");
  }
  if (request.operation === "board" && request.symbols.length !== 0) throw new Error("board does not accept symbols");

  const args = request.operation === "board" ? ["board"] : [request.operation, request.symbols.join(",")];
  return [{ operation: request.operation, markdown: await runCli(args) }];
}

function runCli(args) {
  return new Promise((resolve, reject) => {
    const child = spawn(CLI, args, {
      shell: false,
      windowsHide: true,
      uid: 65534,
      gid: 65534,
      env: { PATH: process.env.PATH ?? "/usr/local/bin:/usr/bin:/bin", HOME: "/tmp" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    let settled = false;
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) reject(error); else resolve(value);
    };
    const append = (current, chunk) => {
      const next = Buffer.concat([current, chunk]);
      if (next.length > MAX_BYTES) {
        child.kill("SIGKILL");
        finish(new Error("westock CLI output exceeded limit"));
      }
      return next;
    };
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(new Error("westock CLI timed out"));
    }, TIMEOUT_MS);
    child.stdout.on("data", (chunk) => { stdout = append(stdout, chunk); });
    child.stderr.on("data", (chunk) => { stderr = append(stderr, chunk); });
    child.once("error", () => finish(new Error("westock CLI failed to start")));
    child.once("close", (code) => {
      if (code !== 0) return finish(new Error(`westock CLI failed (${code}): ${stderr.toString("utf8").slice(0, 256)}`));
      try {
        finish(null, new TextDecoder("utf-8", { fatal: true }).decode(stdout));
      } catch {
        finish(new Error("westock CLI output was not valid UTF-8"));
      }
    });
  });
}
