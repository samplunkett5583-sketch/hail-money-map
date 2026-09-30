#!/usr/bin/env node
import { execFile } from "node:child_process";
import { access, readFile, writeFile, mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const WORK = path.join(ROOT, "tmp", "mrms-production-batch");
const STATE_PATH = path.join(WORK, "state.json");
const DATES_PATH = process.env.MRMS_BATCH_DATES_FILE ||
  path.join(ROOT, "tmp", "mrms-batch-dates.json");
const endpoint = process.env.MRMS_INGEST_ENDPOINT;
const token = process.env.MRMS_INGEST_TOKEN;
const authorization = process.env.MRMS_INGEST_AUTHORIZATION;
const concurrency = Math.max(1, Math.min(2, Number(process.env.MRMS_BATCH_CONCURRENCY) || 1));
const UPLOAD_CHUNK = 25;

if (!endpoint || !token || !authorization) {
  throw new Error(
    "Missing MRMS_INGEST_ENDPOINT, MRMS_INGEST_TOKEN, or MRMS_INGEST_AUTHORIZATION",
  );
}

async function loadState() {
  try {
    return JSON.parse(await readFile(STATE_PATH, "utf8"));
  } catch {
    return { started_at: new Date().toISOString(), completed: {}, failed: {} };
  }
}

async function saveState(state) {
  state.updated_at = new Date().toISOString();
  await writeFile(STATE_PATH, JSON.stringify(state, null, 2), "utf8");
}

async function request(url, options = {}, attempts = 5) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        ...options,
        signal: AbortSignal.timeout(60_000),
        headers: {
          ...(options.headers || {}),
          "x-ingest-token": token,
          "Authorization": `Bearer ${authorization}`,
        },
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
      return body;
    } catch (error) {
      lastError = error;
      if (attempt < attempts) await new Promise((resolve) => setTimeout(resolve, attempt * 2000));
    }
  }
  throw lastError;
}

async function processDate(date, state) {
  const outputPath = path.join(WORK, `${date}.json`);
  const anchorsPath = path.join(WORK, `${date}-anchors.json`);
  try {
    let generated = null;
    try {
      await access(outputPath);
      generated = JSON.parse(await readFile(outputPath, "utf8"));
      if (!Array.isArray(generated.rows) || generated.rows.length === 0) generated = null;
      else console.log(`[BATCH] ${date} reusing ${generated.rows.length} generated polygon(s)`);
    } catch {}
    if (!generated) {
      const anchorPayload = await request(
        `${endpoint}?action=anchors&date=${encodeURIComponent(date)}`,
      );
      await writeFile(anchorsPath, JSON.stringify({ anchors: anchorPayload.anchors || [] }), "utf8");
      const { stdout, stderr } = await execFileAsync(
        process.execPath,
        [
          path.join(ROOT, "scripts", "ingest_mrms_swaths.mjs"),
          `--date=${date}`,
          "--force",
          `--anchors=${anchorsPath}`,
          `--output=${outputPath}`,
        ],
        { cwd: ROOT, maxBuffer: 20 * 1024 * 1024, windowsHide: true },
      );
      if (stdout.trim()) process.stdout.write(stdout);
      if (stderr.trim()) process.stderr.write(stderr);
      generated = JSON.parse(await readFile(outputPath, "utf8"));
    }
    if (!Array.isArray(generated.rows) || generated.rows.length === 0) {
      throw new Error("Generator returned no rows");
    }
    const maxMeshIn = generated.rows.reduce((maximum, row) => {
      const value = Number(row.band_max ?? row.band_min);
      return Math.max(maximum, Number.isFinite(value) ? value : 0);
    }, 0);
    let result = null;
    for (let index = 0; index < generated.rows.length; index += UPLOAD_CHUNK) {
      const rows = generated.rows.slice(index, index + UPLOAD_CHUNK);
      const final = index + rows.length >= generated.rows.length;
      result = await request(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          date,
          rows,
          total_rows: generated.rows.length,
          max_mesh_in: maxMeshIn,
          final,
        }),
      });
    }
    state.completed[date] = {
      rows: generated.rows.length,
      max_mesh_in: maxMeshIn,
      completed_at: new Date().toISOString(),
    };
    delete state.failed[date];
    await saveState(state);
    await rm(outputPath, { force: true });
    await rm(anchorsPath, { force: true });
    console.log(`[BATCH] ${date} complete (${result.rows} polygons)`);
  } catch (error) {
    state.failed[date] = {
      error: error instanceof Error ? error.message : String(error),
      failed_at: new Date().toISOString(),
    };
    await saveState(state);
    console.error(`[BATCH] ${date} failed: ${state.failed[date].error}`);
  }
}

async function main() {
  await mkdir(WORK, { recursive: true });
  const datesPayload = JSON.parse(await readFile(DATES_PATH, "utf8"));
  const dates = (Array.isArray(datesPayload) ? datesPayload : datesPayload.dates || [])
    .map(String)
    .filter((date) => /^\d{4}-\d{2}-\d{2}$/.test(date));
  const state = await loadState();
  const queue = dates.filter((date) => !state.completed[date]);
  console.log(
    `[BATCH] ${dates.length} total, ${Object.keys(state.completed).length} complete, ` +
    `${queue.length} queued, concurrency ${concurrency}`,
  );
  let cursor = 0;
  async function worker() {
    while (cursor < queue.length) {
      const date = queue[cursor++];
      await processDate(date, state);
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  console.log(
    `[BATCH] finished: ${Object.keys(state.completed).length} complete, ` +
    `${Object.keys(state.failed).length} failed`,
  );
}

main().catch((error) => {
  console.error("[BATCH] Fatal:", error);
  process.exitCode = 1;
});
