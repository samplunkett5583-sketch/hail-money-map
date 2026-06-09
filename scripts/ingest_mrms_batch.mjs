#!/usr/bin/env node
/**
 * ingest_mrms_batch.mjs
 *
 * Runs MRMS MESH ingest for every storm date in the database.
 * Skips dates that already have mrms_mesh rows (use --force to re-ingest).
 *
 * Usage:
 *   node scripts/ingest_mrms_batch.mjs
 *   node scripts/ingest_mrms_batch.mjs --force        # re-ingest all
 *   node scripts/ingest_mrms_batch.mjs --limit=10     # first N dates only
 *   node scripts/ingest_mrms_batch.mjs --start=2026-01-01  # from date onward
 */

import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("[MRMS-BATCH] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// Parse CLI args
const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  }),
);

const force = args.force === true || args.force === "true";
const limitArg = args.limit ? parseInt(args.limit, 10) : Infinity;
const startArg = args.start ? String(args.start) : null;

async function getAllStormDates() {
  // Collect unique dates from hail_lsr_raw + storm_lsr_raw + storm_polygons
  const sets = await Promise.all([
    sb.from("hail_lsr_raw").select("event_date").order("event_date", { ascending: false }),
    sb.from("storm_lsr_raw").select("event_date").order("event_date", { ascending: false }),
    sb.from("storm_polygons").select("event_date").order("event_date", { ascending: false }),
  ]);

  const dateSet = new Set();
  for (const { data } of sets) {
    for (const row of data || []) {
      if (row.event_date && /^\d{4}-\d{2}-\d{2}$/.test(row.event_date)) {
        dateSet.add(row.event_date);
      }
    }
  }

  return Array.from(dateSet).sort((a, b) => b.localeCompare(a)); // newest-first
}

async function getIngestedDates() {
  const { data } = await sb
    .from("storm_polygons")
    .select("event_date")
    .eq("source", "mrms_mesh");
  return new Set((data || []).map((r) => r.event_date));
}

function runIngest(dateStr) {
  return new Promise((resolve) => {
    const scriptPath = path.join(__dirname, "ingest_mrms_swaths.mjs");
    const args = ["--date=" + dateStr];
    const child = spawn("node", [scriptPath, ...args], {
      env: { ...process.env },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => {
      const line = d.toString();
      stdout += line;
      process.stdout.write(line);
    });
    child.stderr.on("data", (d) => {
      const line = d.toString();
      stderr += line;
      process.stderr.write(line);
    });

    child.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

async function getRowCount(dateStr) {
  const { count } = await sb
    .from("storm_polygons")
    .select("id", { count: "exact", head: true })
    .eq("event_date", dateStr)
    .eq("source", "mrms_mesh");
  return count || 0;
}

async function main() {
  console.log("[MRMS-BATCH] ═══ Starting MRMS MESH batch ingest ═══");
  console.log("[MRMS-BATCH] force=" + force + " limit=" + limitArg + " start=" + (startArg || "none"));

  const allDates = await getAllStormDates();
  const ingested = await getIngestedDates();

  console.log("[MRMS-BATCH] Total storm dates: " + allDates.length);
  console.log("[MRMS-BATCH] Already ingested:  " + ingested.size);

  let workDates = allDates;
  if (startArg) workDates = workDates.filter((d) => d >= startArg);
  if (!force) workDates = workDates.filter((d) => !ingested.has(d));
  if (isFinite(limitArg)) workDates = workDates.slice(0, limitArg);

  console.log("[MRMS-BATCH] Dates to process:  " + workDates.length);

  let ok = 0, fail = 0, skip = 0;

  for (let i = 0; i < workDates.length; i++) {
    const d = workDates[i];
    console.log("\n[mrms-ingest] ── " + (i + 1) + "/" + workDates.length + " ──");
    console.log("[mrms-ingest] dateStr=" + d + " attempted=true mesh-grid-fetched=pending polygons-written=0 error=none");

    let meshFetched = false;
    let polygonsWritten = 0;
    let errMsg = "none";

    try {
      const result = await runIngest(d);
      if (result.code !== 0) {
        errMsg = "exit-code=" + result.code;
        fail++;
      } else {
        meshFetched = true;
        polygonsWritten = await getRowCount(d);
        ok++;
      }
    } catch (err) {
      errMsg = String(err.message || err).slice(0, 100);
      fail++;
    }

    console.log("[mrms-ingest] dateStr=" + d +
      " attempted=true" +
      " mesh-grid-fetched=" + meshFetched +
      " polygons-written=" + polygonsWritten +
      " error=" + errMsg);
  }

  console.log("\n[MRMS-BATCH] ═══ DONE ═══");
  console.log("[MRMS-BATCH] ok=" + ok + " fail=" + fail + " skip=" + skip +
    " total=" + workDates.length);
}

main().catch((err) => {
  console.error("[MRMS-BATCH] Fatal:", err);
  process.exit(1);
});
