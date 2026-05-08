#!/usr/bin/env node

import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const argv = process.argv.slice(2);

function getFlagValue(flag) {
  const eqPrefix = flag + "=";
  for (const a of argv) {
    if (a.startsWith(eqPrefix)) return a.slice(eqPrefix.length);
  }
  const i = argv.indexOf(flag);
  if (i !== -1 && i + 1 < argv.length) return argv[i + 1];
  return null;
}

function hasFlag(flag) {
  return argv.includes(flag) || argv.some((a) => a === flag);
}

let month = getFlagValue("--month");
if (!month) {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  month = `${yyyy}-${mm}`;
}

if (hasFlag("--dry-run")) {
  console.log(`Dry run. month=${month}`);
  process.exit(0);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const target = path.join(__dirname, "ingest_stormevents_hail.mjs");

const child = spawn(process.execPath, [target], {
  stdio: "inherit",
  env: process.env,
});

child.on("exit", (code) => {
  process.exit(code ?? 1);
});
