#!/usr/bin/env node
/**
 * ingest_mrms_swaths.mjs
 *
 * Ingests radar-derived hail coverage polygons into public.hail_radar_polygons.
 *
 * Source: IEM NEXRAD storm-attribute tracks (REST API, no GDAL required).
 * The underlying ingest is handled by ingest_iem_nexrad_tracks.mjs which
 * fetches real radar-tracked storm cells from Iowa State Mesonet and saves
 * broad hail corridor polygons into hail_radar_polygons.
 *
 * Usage:
 *   node scripts/ingest_mrms_swaths.mjs --date=2026-06-02
 *   node scripts/ingest_mrms_swaths.mjs --date=2026-06-02 --force
 *
 * Env vars required:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 */

import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error(
    "[radar-ingest] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// ── Parse CLI args ─────────────────────────────────────────────────────────

const rawArgs = process.argv.slice(2);
const argsMap = Object.fromEntries(
  rawArgs.map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const dateArg = argsMap.date;
if (!dateArg || !/^\d{4}-\d{2}-\d{2}$/.test(String(dateArg))) {
  console.error(
    "[radar-ingest] Usage: node scripts/ingest_mrms_swaths.mjs --date=YYYY-MM-DD"
  );
  process.exit(1);
}

const dateIso = String(dateArg);
const force = argsMap.force === true || argsMap.force === "true";

// ── Helpers ────────────────────────────────────────────────────────────────

async function countRadarRows(date) {
  const { count, error } = await supabase
    .from("hail_radar_polygons")
    .select("id", { count: "exact", head: true })
    .eq("event_date", date);
  if (error) {
    console.error("[radar-ingest] count error:", error.message);
    return 0;
  }
  return count ?? 0;
}

function runNexradIngest(date, forceFlag) {
  return new Promise((resolve, reject) => {
    const script = path.join(__dirname, "ingest_iem_nexrad_tracks.mjs");
    // Pass --force so the nexrad script re-processes even if it already ran today
    const spawnArgs = [script, date];
    if (forceFlag) spawnArgs.push("--force");
    console.log(`[radar-ingest] Running NEXRAD track ingest for ${date}${forceFlag ? " (--force)" : ""}...`);
    const proc = spawn(process.execPath, spawnArgs, {
      stdio: "inherit",
      env: { ...process.env },
    });
    proc.on("error", reject);
    proc.on("close", (code) => {
      if (code !== 0) {
        reject(
          new Error(`ingest_iem_nexrad_tracks.mjs exited with code ${code}`)
        );
      } else {
        resolve();
      }
    });
  });
}

// ── Main ───────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[radar-ingest] date=${dateIso}`);
  console.log(`[radar-ingest] source=NEXRAD/IEM`);

  const rowsBefore = await countRadarRows(dateIso);

  // Skip only if rows exist AND --force was not passed
  if (!force && rowsBefore > 0) {
    console.log(
      `[radar-ingest] Already have ${rowsBefore} row(s) for ${dateIso}. ` +
      `Run with --force to re-ingest with updated code.`
    );
    console.log(`[radar-ingest] rows before=${rowsBefore} rows after=${rowsBefore}`);
    console.log(`[radar-ingest] polygons saved=${rowsBefore}`);
    return;
  }

  console.log(`[radar-ingest] rows before=${rowsBefore}${rowsBefore > 0 ? " (--force: re-ingesting)" : ""}`);

  // Run the full NEXRAD track ingest.
  // ingest_iem_nexrad_tracks.mjs fetches IEM NEXRAD storm-attribute data (REST,
  // no GDAL), builds hail corridor polygons, and writes to both storm_polygons
  // and hail_radar_polygons.
  await runNexradIngest(dateIso, force);

  const rowsAfter = await countRadarRows(dateIso);
  const saved = rowsAfter - rowsBefore;

  console.log(`[radar-ingest] rows before=${rowsBefore} rows after=${rowsAfter}`);
  console.log(`[radar-ingest] polygons saved=${saved}`);

  if (rowsAfter === 0) {
    console.warn(
      `[radar-ingest] WARNING: 0 rows in hail_radar_polygons for ${dateIso}. ` +
      `IEM NEXRAD archive may not have data far enough back, or ` +
      `the storm had no trackable hail cells (check ingest_iem_nexrad_tracks output above).`
    );
    process.exit(1);
  }
}

main().catch((e) => {
  console.error("[radar-ingest] Fatal:", e.message || e);
  process.exit(1);
});
