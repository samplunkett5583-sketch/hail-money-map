#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────
// backfill_mrms_swaths.mjs — batch-run ingest_mrms_swaths for many dates
//
// Usage:
//   node scripts/backfill_mrms_swaths.mjs --month=2025-05
//   node scripts/backfill_mrms_swaths.mjs --from=2025-03-01 --to=2025-03-31
//   node scripts/backfill_mrms_swaths.mjs --all-existing
//   node scripts/backfill_mrms_swaths.mjs --month=2025-05 --dry-run
//
// Modes:
//   --month=YYYY-MM        All known hail dates in that calendar month
//   --from=YYYY-MM-DD --to=YYYY-MM-DD   All known hail dates in the range
//   --all-existing         Re-process every date that already has MRMS rows
//
// Extra flags:
//   --dry-run              List dates but don't run ingest
//   --threshold=N          Passed through to ingest_mrms_swaths (default 1.0)
//   --concurrency=1        Always 1 (serial) to avoid hammering the archive
//
// Requires: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY env vars
// ──────────────────────────────────────────────────────────────────

import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const INGEST_SCRIPT = path.join(__dirname, "ingest_mrms_swaths.mjs");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ── CLI parsing ─────────────────────────────────────────────────
function parseArgs() {
  const args = { month: null, from: null, to: null, allExisting: false, dryRun: false, threshold: null };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--month=")) args.month = a.slice(8);
    else if (a.startsWith("--from=")) args.from = a.slice(7);
    else if (a.startsWith("--to=")) args.to = a.slice(5);
    else if (a === "--all-existing") args.allExisting = true;
    else if (a === "--dry-run") args.dryRun = true;
    else if (a.startsWith("--threshold=")) args.threshold = a.slice(12);
  }
  const modes = [args.month, args.from && args.to, args.allExisting].filter(Boolean).length;
  if (modes !== 1) {
    console.error(
      "Provide exactly one mode:\n" +
        "  --month=YYYY-MM\n" +
        "  --from=YYYY-MM-DD --to=YYYY-MM-DD\n" +
        "  --all-existing"
    );
    process.exit(1);
  }
  return args;
}

// ── Fetch target dates ──────────────────────────────────────────

/** Distinct hail dates in a range from hail_lsr_raw + hail_reports */
async function fetchHailDatesInRange(from, to) {
  // Query hail_lsr_raw
  const { data: lsr, error: e1 } = await supabase
    .from("hail_lsr_raw")
    .select("event_date")
    .gte("event_date", from)
    .lte("event_date", to);
  if (e1) throw new Error(`hail_lsr_raw query error: ${e1.message}`);

  // Query hail_reports (may not exist — ignore error)
  let reports = [];
  try {
    const { data, error } = await supabase
      .from("hail_reports")
      .select("event_date")
      .gte("event_date", from)
      .lte("event_date", to);
    if (!error && data) reports = data;
  } catch { /* table may not exist */ }

  const dateSet = new Set();
  for (const r of (lsr || [])) dateSet.add(r.event_date);
  for (const r of reports) dateSet.add(r.event_date);
  return [...dateSet].sort();
}

/** All distinct dates that already have MRMS rows in storm_polygons */
async function fetchExistingMrmsDates() {
  // Supabase doesn't support SELECT DISTINCT directly, so fetch all and dedupe
  const { data, error } = await supabase
    .from("storm_polygons")
    .select("event_date")
    .eq("source", "mrms")
    .order("event_date", { ascending: true });
  if (error) throw new Error(`storm_polygons query error: ${error.message}`);
  const dateSet = new Set();
  for (const r of (data || [])) dateSet.add(r.event_date);
  return [...dateSet].sort();
}

function monthRange(ym) {
  // "2025-05" → { from: "2025-05-01", to: "2025-05-31" }
  const [y, m] = ym.split("-").map(Number);
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate(); // day 0 of next month
  return {
    from: `${y}-${String(m).padStart(2, "0")}-01`,
    to: `${y}-${String(m).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`,
  };
}

// ── Main ────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs();

  let dates;
  if (args.allExisting) {
    console.log("[backfill] Mode: re-process all existing MRMS dates");
    dates = await fetchExistingMrmsDates();
  } else {
    let from, to;
    if (args.month) {
      const r = monthRange(args.month);
      from = r.from;
      to = r.to;
      console.log(`[backfill] Mode: month ${args.month} (${from} → ${to})`);
    } else {
      from = args.from;
      to = args.to;
      console.log(`[backfill] Mode: date range ${from} → ${to}`);
    }
    dates = await fetchHailDatesInRange(from, to);
  }

  if (!dates.length) {
    console.log("[backfill] No target dates found. Nothing to do.");
    return;
  }

  console.log(`[backfill] ${dates.length} date(s) to process:`);
  for (const d of dates) console.log(`  ${d}`);

  if (args.dryRun) {
    console.log("[backfill] --dry-run: stopping here.");
    return;
  }

  let ok = 0;
  let fail = 0;
  const failed = [];

  for (let i = 0; i < dates.length; i++) {
    const d = dates[i];
    console.log(`\n${"═".repeat(60)}`);
    console.log(`[backfill] (${i + 1}/${dates.length}) Processing ${d}…`);
    console.log("═".repeat(60));

    const childArgs = [INGEST_SCRIPT, `--date=${d}`];
    if (args.threshold) childArgs.push(`--threshold=${args.threshold}`);

    try {
      execFileSync(process.execPath, childArgs, {
        stdio: "inherit",
        env: process.env,
        timeout: 60 * 60 * 1000, // 60 min per date — heavy MRMS dates need time
      });
      ok++;
      console.log(`[backfill] ✓ ${d} completed`);
    } catch (e) {
      fail++;
      failed.push(d);
      console.error(`[backfill] ✗ ${d} FAILED (exit ${e.status})`);
    }
  }

  console.log(`\n${"═".repeat(60)}`);
  console.log(`[backfill] Done. ${ok} succeeded, ${fail} failed out of ${dates.length}.`);
  if (failed.length) {
    console.log("[backfill] Failed dates:");
    for (const d of failed) console.log(`  ${d}`);
  }
}

main().catch((e) => {
  console.error("[backfill] Fatal:", e);
  process.exit(1);
});
