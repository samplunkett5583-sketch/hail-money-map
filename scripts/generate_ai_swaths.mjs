#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────
// generate_ai_swaths.mjs  –  AI swath generation pipeline
//
// Reads MRMS/MESH evidence from storm_polygons, passes it through an
// AI meteorologist generator, and writes the result back as the
// highest-priority canonical swath.
//
// Usage:
//   # Single date (backfill or ad-hoc)
//   node scripts/generate_ai_swaths.mjs --date=2025-05-19
//
//   # Rolling: generate for all recent dates that have MRMS evidence
//   #          but no AI swath yet
//   node scripts/generate_ai_swaths.mjs --rolling
//
//   # Force rebuild even if AI row already exists
//   node scripts/generate_ai_swaths.mjs --date=2025-05-19 --force
//
//   # Dry run
//   node scripts/generate_ai_swaths.mjs --date=2025-05-19 --dry-run
//
// Requires: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY env vars
// ──────────────────────────────────────────────────────────────────

import { createClient } from "@supabase/supabase-js";

// ── Config ──────────────────────────────────────────────────────
const AI_SOURCE = "ai_meteorologist_mesh";
const AI_PRODUCT = "AI_SWATH";

// Priority 99 = stub / test.  Stub rows MUST NOT beat real MRMS (1) or SWDI (2).
// When the real AI model is wired, change this to 1 and add
// 'ai_meteorologist_mesh' back to VALID_SWATH_SOURCES in the frontend.
const AI_PRIORITY_STUB = 99;
const AI_PRIORITY_PRODUCTION = 1; // reserved for real model output

// Evidence source: MRMS rows in storm_polygons
const EVIDENCE_SOURCE = "mrms";

// Rolling mode: look back this many days for dates with MRMS but no AI row
const ROLLING_LOOKBACK_DAYS = 14;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ── CLI args ────────────────────────────────────────────────────
function parseArgs() {
  const args = { date: null, rolling: false, force: false, dryRun: false };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--date=")) args.date = a.slice(7);
    else if (a === "--rolling") args.rolling = true;
    else if (a === "--force") args.force = true;
    else if (a === "--dry-run") args.dryRun = true;
  }
  if (!args.date && !args.rolling) {
    console.error(
      "Usage: node generate_ai_swaths.mjs --date=YYYY-MM-DD [--force] [--dry-run]\n" +
      "       node generate_ai_swaths.mjs --rolling [--force] [--dry-run]"
    );
    process.exit(1);
  }
  if (args.date && !/^\d{4}-\d{2}-\d{2}$/.test(args.date)) {
    console.error("Invalid date format. Use YYYY-MM-DD.");
    process.exit(1);
  }
  return args;
}

// ── Fetch MRMS evidence row for a date ──────────────────────────
async function fetchMrmsEvidence(dateStr) {
  const { data, error } = await supabase
    .from("storm_polygons")
    .select("*")
    .eq("event_date", dateStr)
    .eq("source", EVIDENCE_SOURCE)
    .order("source_priority", { ascending: true })
    .order("created_at", { ascending: false })
    .limit(1);

  if (error) {
    console.warn(`[AI] Failed to query MRMS evidence for ${dateStr}:`, error.message);
    return null;
  }
  return data && data.length ? data[0] : null;
}

// ── Check if AI row already exists for a date ───────────────────
async function aiRowExists(dateStr) {
  const { data, error } = await supabase
    .from("storm_polygons")
    .select("id")
    .eq("event_date", dateStr)
    .eq("source", AI_SOURCE)
    .eq("source_product", AI_PRODUCT)
    .limit(1);

  if (error) {
    console.warn(`[AI] Failed to check existing AI row for ${dateStr}:`, error.message);
    return false;
  }
  return data && data.length > 0;
}

// ══════════════════════════════════════════════════════════════════
// EVIDENCE EXTRACTION — builds structured input for the AI model
// ══════════════════════════════════════════════════════════════════

function extractEvidenceFromMrms(mrmsRow) {
  const geojson =
    typeof mrmsRow.polygon_geojson === "string"
      ? JSON.parse(mrmsRow.polygon_geojson)
      : mrmsRow.polygon_geojson;

  if (!geojson || !geojson.type || !geojson.coordinates) return null;

  // Decompose into individual polygon components
  const components = [];
  const polys =
    geojson.type === "MultiPolygon"
      ? geojson.coordinates
      : [geojson.coordinates];

  for (const poly of polys) {
    const outer = poly[0];
    if (!Array.isArray(outer) || outer.length < 4) continue;

    let minLon = 180, maxLon = -180, minLat = 90, maxLat = -90;
    let sumLon = 0, sumLat = 0;
    for (const [lon, lat] of outer) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
      sumLon += lon;
      sumLat += lat;
    }
    const n = outer.length;
    components.push({
      vertex_count: n,
      bbox: { minLon, maxLon, minLat, maxLat },
      centroid: { lon: sumLon / n, lat: sumLat / n },
      span_lon_deg: maxLon - minLon,
      span_lat_deg: maxLat - minLat,
    });
  }

  // Overall bbox
  let oMinLon = 180, oMaxLon = -180, oMinLat = 90, oMaxLat = -90;
  for (const c of components) {
    if (c.bbox.minLon < oMinLon) oMinLon = c.bbox.minLon;
    if (c.bbox.maxLon > oMaxLon) oMaxLon = c.bbox.maxLon;
    if (c.bbox.minLat < oMinLat) oMinLat = c.bbox.minLat;
    if (c.bbox.maxLat > oMaxLat) oMaxLat = c.bbox.maxLat;
  }

  return {
    event_date: mrmsRow.event_date,
    source_geojson: geojson,
    geojson_type: geojson.type,
    component_count: components.length,
    components,
    overall_bbox: { minLon: oMinLon, maxLon: oMaxLon, minLat: oMinLat, maxLat: oMaxLat },
    mrms_product: mrmsRow.source_product,
    mrms_threshold_in: mrmsRow.threshold_value,
    mrms_area_sq_mi: mrmsRow.area_sq_mi,
    mrms_centroid: { lat: mrmsRow.centroid_lat, lon: mrmsRow.centroid_lon },
    mrms_time_window: { start: mrmsRow.event_start_utc, end: mrmsRow.event_end_utc },
    mrms_metadata: mrmsRow.metadata_json,
    mrms_row_id: mrmsRow.id,
  };
}

// ══════════════════════════════════════════════════════════════════
// AI SWATH GENERATOR
//
// *** REPLACE THE BODY OF THIS FUNCTION WITH THE REAL AI MODEL CALL ***
//
// Input:  evidence object from extractEvidenceFromMrms()
//   - evidence.source_geojson   : full MRMS GeoJSON (Polygon / MultiPolygon)
//   - evidence.components[]     : per-polygon bbox, centroid, vertex count
//   - evidence.overall_bbox     : bounding box of all components
//   - evidence.mrms_*           : product, threshold, area, centroid, time window
//
// Output: { polygon_geojson, ai_status, metadata }
//   - polygon_geojson : GeoJSON Polygon or MultiPolygon
//   - ai_status       : "stub" | "production"
//   - metadata        : object for metadata_json
//
// Current behavior (STUB):
//   Returns the MRMS geometry unchanged and marks ai_status = "stub".
//   The row is written with priority 99 so it never beats real MRMS.
//   When a real model is wired, return ai_status = "production".
// ══════════════════════════════════════════════════════════════════
async function generateAiSwath(evidence) {
  //
  // ── STUB: no AI model call yet ────────────────────────────────
  // The evidence object is fully prepared above.
  // Replace this block with the real model invocation.
  // The model should receive `evidence` and return a new GeoJSON geometry.
  //
  console.log("[AI-STUB] Evidence prepared: " + evidence.component_count +
    " component(s), bbox [" +
    evidence.overall_bbox.minLon.toFixed(2) + "," +
    evidence.overall_bbox.minLat.toFixed(2) + "]→[" +
    evidence.overall_bbox.maxLon.toFixed(2) + "," +
    evidence.overall_bbox.maxLat.toFixed(2) + "]");
  console.log("[AI-STUB] Returning MRMS geometry unchanged (stub — not production AI)");

  return {
    polygon_geojson: evidence.source_geojson,
    ai_status: "stub",
    metadata: {
      generator: "stub_passthrough_v1",
      ai_status: "stub",
      model: null,
      geometry_copied_from_mrms: true,
      mrms_source_id: evidence.mrms_row_id,
      mrms_area_sq_mi: evidence.mrms_area_sq_mi,
      mrms_product: evidence.mrms_product,
      evidence_components: evidence.component_count,
      evidence_bbox: evidence.overall_bbox,
      note: "STUB: MRMS geometry passed through. Replace generateAiSwath() body with real model. Set ai_status='production' and priority to AI_PRIORITY_PRODUCTION when ready.",
    },
  };
}

// ── Process a single date ───────────────────────────────────────
async function processDate(dateStr, opts) {
  console.log(`\n[AI] ── Processing ${dateStr} ──`);

  // 1. Idempotency check
  if (!opts.force) {
    const exists = await aiRowExists(dateStr);
    if (exists) {
      console.log(`[AI] ${dateStr}: AI row already exists. Skipping. (use --force to rebuild)`);
      return "skipped";
    }
  }

  // 2. Fetch MRMS evidence
  const mrmsRow = await fetchMrmsEvidence(dateStr);
  if (!mrmsRow) {
    console.log(`[AI] ${dateStr}: No MRMS evidence found. Skipping.`);
    return "no_evidence";
  }

  console.log(
    `[AI] ${dateStr}: MRMS evidence found — ` +
    `${mrmsRow.source_product}, area=${mrmsRow.area_sq_mi} sq mi, ` +
    `centroid=(${mrmsRow.centroid_lat?.toFixed(3)}, ${mrmsRow.centroid_lon?.toFixed(3)})`
  );

  // 3. Extract structured evidence for the AI model
  const evidence = extractEvidenceFromMrms(mrmsRow);
  if (!evidence) {
    console.warn(`[AI] ${dateStr}: Failed to extract evidence from MRMS geometry. Skipping.`);
    return "no_evidence";
  }

  // 4. Generate AI swath
  const result = await generateAiSwath(evidence);
  if (!result || !result.polygon_geojson) {
    console.warn(`[AI] ${dateStr}: Generator returned no geometry. Skipping.`);
    return "gen_failed";
  }

  const isStub = result.ai_status !== "production";
  const priority = isStub ? AI_PRIORITY_STUB : AI_PRIORITY_PRODUCTION;

  // 6. Compute metadata from generated geometry
  const geojson = result.polygon_geojson;
  const centroid = computeCentroid(geojson);
  const areaSqMi = approxAreaSqMi(geojson);

  console.log(
    `[AI] ${dateStr}: Generated swath — ` +
    `type=${geojson.type}, area=${areaSqMi.toFixed(1)} sq mi, ` +
    `centroid=(${centroid.lat?.toFixed(3)}, ${centroid.lon?.toFixed(3)}), ` +
    `ai_status=${result.ai_status}, priority=${priority}`
  );

  if (opts.dryRun) {
    console.log(`[AI] ${dateStr}: --dry-run: skipping database write.`);
    return "dry_run";
  }

  // 7. Upsert into storm_polygons
  const row = {
    event_date: dateStr,
    source: AI_SOURCE,
    source_product: AI_PRODUCT,
    source_priority: priority,
    swath_index: 0,
    polygon_geojson: geojson,
    centroid_lat: centroid.lat,
    centroid_lon: centroid.lon,
    area_sq_mi: Math.round(areaSqMi * 100) / 100,
    threshold_value: mrmsRow.threshold_value,
    event_start_utc: mrmsRow.event_start_utc,
    event_end_utc: mrmsRow.event_end_utc,
    metadata_json: {
      ...result.metadata,
      ai_status: result.ai_status,
      geometry_is_stub: isStub,
      evidence_source: EVIDENCE_SOURCE,
      evidence_date: dateStr,
      evidence_product: mrmsRow.source_product,
      ingest_timestamp: new Date().toISOString(),
    },
  };

  const { error } = await supabase
    .from("storm_polygons")
    .upsert(row, { onConflict: "event_date,source,source_product,swath_index" });

  if (error) {
    console.error(`[AI] ${dateStr}: Supabase upsert error:`, error.message);
    return "db_error";
  }

  console.log(`[AI] ✓ ${dateStr}: AI swath written to storm_polygons (source=${AI_SOURCE}, priority=${priority}, ai_status=${result.ai_status})`);
  return "ok";
}

// ── Rolling mode: find dates with MRMS but no AI row ────────────
async function findDatesNeedingAi() {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - ROLLING_LOOKBACK_DAYS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  console.log(`[AI] Rolling mode: checking dates since ${cutoffStr}`);

  // Get all dates that have MRMS evidence
  const { data: mrmsRows, error: e1 } = await supabase
    .from("storm_polygons")
    .select("event_date")
    .eq("source", EVIDENCE_SOURCE)
    .gte("event_date", cutoffStr)
    .order("event_date", { ascending: false });

  if (e1) {
    console.error("[AI] Failed to query MRMS dates:", e1.message);
    return [];
  }

  const mrmsDates = [...new Set((mrmsRows || []).map((r) => r.event_date))];
  console.log(`[AI] MRMS evidence dates in window: ${mrmsDates.length}`);

  // Get all dates that already have AI rows
  const { data: aiRows, error: e2 } = await supabase
    .from("storm_polygons")
    .select("event_date")
    .eq("source", AI_SOURCE)
    .gte("event_date", cutoffStr);

  if (e2) {
    console.error("[AI] Failed to query AI dates:", e2.message);
    return [];
  }

  const aiDates = new Set((aiRows || []).map((r) => r.event_date));
  const needed = mrmsDates.filter((d) => !aiDates.has(d));

  console.log(`[AI] Dates already with AI swath: ${aiDates.size}`);
  console.log(`[AI] Dates needing AI generation: ${needed.length}`);

  return needed;
}

// ── Geometry helpers (reused from ingest_mrms_swaths.mjs) ───────
function computeCentroid(geojson) {
  const coords = [];
  function collect(c) {
    if (typeof c[0] === "number") coords.push(c);
    else for (const sub of c) collect(sub);
  }
  collect(geojson.coordinates);
  if (!coords.length) return { lat: null, lon: null };
  let sLat = 0, sLon = 0;
  for (const [lon, lat] of coords) { sLat += lat; sLon += lon; }
  return { lat: sLat / coords.length, lon: sLon / coords.length };
}

function approxAreaSqMi(geojson) {
  function ringArea(ring) {
    let a = 0;
    for (let i = 0; i < ring.length - 1; i++) {
      const [x1, y1] = ring[i], [x2, y2] = ring[i + 1];
      a += x1 * y2 - x2 * y1;
    }
    return Math.abs(a) / 2;
  }
  let totalDeg2 = 0;
  const polys = geojson.type === "MultiPolygon"
    ? geojson.coordinates : [geojson.coordinates];
  for (const poly of polys) {
    if (Array.isArray(poly[0])) totalDeg2 += ringArea(poly[0]);
  }
  const midLat = computeCentroid(geojson).lat || 39;
  const lonMi = 69 * Math.cos((midLat * Math.PI) / 180);
  return totalDeg2 * lonMi * 69;
}

// ── Main ────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs();

  console.log("[AI] Hail Money — AI Swath Generation Pipeline");
  console.log(`[AI] Mode: ${args.rolling ? "rolling" : "single-date"}, force=${args.force}, dry-run=${args.dryRun}`);

  let dates;
  if (args.rolling) {
    dates = await findDatesNeedingAi();
    if (!dates.length) {
      console.log("[AI] No dates need AI generation. Done.");
      return;
    }
  } else {
    dates = [args.date];
  }

  const results = { ok: 0, skipped: 0, no_evidence: 0, gen_failed: 0, db_error: 0, dry_run: 0 };

  for (const d of dates) {
    const status = await processDate(d, { force: args.force, dryRun: args.dryRun });
    results[status] = (results[status] || 0) + 1;
  }

  console.log("\n[AI] ── Summary ──");
  console.log(`[AI] Total dates: ${dates.length}`);
  console.log(`[AI] Written: ${results.ok}, Skipped: ${results.skipped}, No evidence: ${results.no_evidence}, Failed: ${results.gen_failed + results.db_error}, Dry-run: ${results.dry_run}`);
}

main().catch((e) => {
  console.error("[AI] Fatal:", e);
  process.exit(1);
});
