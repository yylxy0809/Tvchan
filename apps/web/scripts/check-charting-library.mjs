import { access } from "node:fs/promises";
import { constants } from "node:fs";
import { resolve } from "node:path";

const candidates = [
  "public/charting_library/charting_library.js",
  "public/charting_library/charting_library.standalone.js",
];

const available = [];
for (const candidate of candidates) {
  try {
    await access(resolve(candidate), constants.R_OK);
    available.push(candidate);
  } catch {
    // A licensed runtime may provide either supported entrypoint.
  }
}

if (available.length === 0) {
  console.error(`Missing licensed TradingView runtime. Provide one of: ${candidates.join(", ")}`);
  process.exitCode = 1;
} else {
  console.log(`TradingView runtime ready: ${available.join(", ")}`);
}
