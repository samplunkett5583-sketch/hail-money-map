#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────
// ingest_mrms_swaths.mjs  –  PRIMARY swath ingestion from MRMS MESH
//
// Downloads MRMS MESH hail grid products for a target date, thresholds
// them, dissolves contiguous hail cells, polygonizes the result, and
// writes canonical swath polygons into public.storm_polygons.
//
// Requires: GDAL CLI tools (gdal_translate, gdal_calc.py, gdal_polygonize.py)
//           Node >= 18, env vars SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
//
// Usage:
//   node scripts/ingest_mrms_swaths.mjs --date=2026-03-30
//   node scripts/ingest_mrms_swaths.mjs --date=2026-03-30 --threshold=1.0
//   node scripts/ingest_mrms_swaths.mjs --date=2026-03-30 --dry-run
// ──────────────────────────────────────────────────────────────────

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import https from "node:https";
import http from "node:http";
import zlib from "node:zlib";
import { execSync } from "node:child_process";
import { createClient } from "@supabase/supabase-js";

// ── Config ──────────────────────────────────────────────────────
const MRMS_ARCHIVE_BASE =
  process.env.MRMS_ARCHIVE_BASE ||
  "https://mtarchive.geol.iastate.edu";

// Default MESH threshold in inches (MRMS MESH is in mm; 1 in = 25.4 mm)
const DEFAULT_THRESHOLD_IN = 1.0;

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
  const args = { date: null, threshold: DEFAULT_THRESHOLD_IN, dryRun: false };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--date=")) args.date = a.slice(7);
    else if (a.startsWith("--threshold=")) args.threshold = Number(a.slice(12));
    else if (a === "--dry-run") args.dryRun = true;
  }
  if (!args.date || !/^\d{4}-\d{2}-\d{2}$/.test(args.date)) {
    console.error("Usage: node ingest_mrms_swaths.mjs --date=YYYY-MM-DD [--threshold=1.0] [--dry-run]");
    process.exit(1);
  }
  return args;
}

// ── Helpers ─────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function fetchBuffer(url) {
  const mod = url.startsWith("https") ? https : http;
  return new Promise((resolve, reject) => {
    mod
      .get(url, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          return fetchBuffer(res.headers.location).then(resolve, reject);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => resolve(Buffer.concat(chunks)));
      })
      .on("error", reject);
  });
}

function fetchText(url) {
  const mod = url.startsWith("https") ? https : http;
  return new Promise((resolve, reject) => {
    mod
      .get(url, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          return fetchText(res.headers.location).then(resolve, reject);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

function gunzipBuffer(buf) {
  return new Promise((resolve, reject) => {
    zlib.gunzip(buf, (err, result) => {
      if (err) reject(err);
      else resolve(result);
    });
  });
}

function ensureGdal() {
  try {
    execSync("gdalinfo --version", { stdio: "pipe" });
  } catch {
    console.error(
      "ERROR: GDAL CLI tools not found.\n" +
        "Install GDAL (apt install gdal-bin / conda install gdal / brew install gdal) " +
        "and ensure gdalinfo, gdal_translate, gdal_calc.py, gdal_polygonize.py are on PATH."
    );
    process.exit(1);
  }
}

// ── MRMS archive listing ────────────────────────────────────────
// Iowa State archive layout:
//   https://mtarchive.geol.iastate.edu/YYYY/MM/DD/mrms/ncep/MESHMax1440min/
//   Files: MESHMax1440min_00.50_YYYYMMDD-HH0000.grib2.gz
//
// We prefer the 1440-min (24-hr) max product for a full-day swath.
// Fallback to the 60-min or 2-min snapshots and merge.

async function listMeshFiles(dateStr) {
  const [yyyy, mm, dd] = dateStr.split("-");
  const products = [
    "MESHMax1440min", // 24-hr max accumulation (ideal for daily swath)
    "MESHMax60min",   // 60-min max (fallback)
    "MESH",           // 2-min instantaneous (last resort)
  ];

  for (const product of products) {
    const dirUrl = `${MRMS_ARCHIVE_BASE}/${yyyy}/${mm}/${dd}/mrms/ncep/${product}/`;
    console.log(`[MRMS] Checking archive: ${dirUrl}`);
    try {
      const html = await fetchText(dirUrl);
      const re = new RegExp(
        `(${product}_[\\d.]+_\\d{8}-\\d{6}\\.grib2\\.gz)`,
        "g"
      );
      const files = [];
      let m;
      while ((m = re.exec(html))) {
        if (files.indexOf(m[1]) === -1) files.push(m[1]);
      }
      if (files.length > 0) {
        console.log(`[MRMS] Found ${files.length} ${product} file(s)`);
        return { product, dirUrl, files };
      }
    } catch (e) {
      console.warn(`[MRMS] ${product} listing failed: ${e.message}`);
    }
  }

  return null;
}

// ── Download a single GRIB2 file ────────────────────────────────
async function downloadGrib(dirUrl, fileName, destDir) {
  const url = dirUrl + fileName;
  const gzPath = path.join(destDir, fileName);
  const gribPath = gzPath.replace(/\.gz$/, "");

  console.log(`[MRMS] Downloading ${fileName}…`);
  const buf = await fetchBuffer(url);
  const decompressed = await gunzipBuffer(buf);
  fs.writeFileSync(gribPath, decompressed);
  console.log(`[MRMS] Written ${gribPath} (${decompressed.length} bytes)`);
  return gribPath;
}

// ── Raster processing pipeline ──────────────────────────────────
// 1. Convert GRIB2 → GeoTIFF
// 2. Threshold: pixels >= threshold_mm → 1, else 0
// 3. Polygonize the binary mask
// 4. Output GeoJSON FeatureCollection

function gribToGeojson(gribPath, thresholdMm, workDir) {
  const baseName = path.basename(gribPath, ".grib2");
  const tifPath = path.join(workDir, baseName + ".tif");
  const maskPath = path.join(workDir, baseName + "_mask.tif");
  const geojsonPath = path.join(workDir, baseName + ".geojson");

  // Step 1: GRIB2 → GeoTIFF
  console.log(`[GDAL] grib→tif: ${baseName}`);
  execSync(
    `gdal_translate -of GTiff -q "${gribPath}" "${tifPath}"`,
    { stdio: "pipe" }
  );

  // Step 2: Threshold → binary mask
  // gdal_calc.py: output = 1 where A >= threshold, else 0
  console.log(`[GDAL] threshold >= ${thresholdMm} mm`);
  try {
    execSync(
      `gdal_calc.py -A "${tifPath}" --outfile="${maskPath}" ` +
        `--calc="(A>=${thresholdMm})*1" --type=Byte --NoDataValue=0 --quiet`,
      { stdio: "pipe" }
    );
  } catch {
    // Some systems install gdal_calc as gdal_calc (no .py)
    execSync(
      `gdal_calc -A "${tifPath}" --outfile="${maskPath}" ` +
        `--calc="(A>=${thresholdMm})*1" --type=Byte --NoDataValue=0 --quiet`,
      { stdio: "pipe" }
    );
  }

  // Step 3: Polygonize
  console.log(`[GDAL] polygonize → ${baseName}.geojson`);
  try {
    execSync(
      `gdal_polygonize.py "${maskPath}" -f GeoJSON "${geojsonPath}" mesh_swath DN`,
      { stdio: "pipe" }
    );
  } catch {
    execSync(
      `gdal_polygonize "${maskPath}" -f GeoJSON "${geojsonPath}" mesh_swath DN`,
      { stdio: "pipe" }
    );
  }

  // Read output
  if (!fs.existsSync(geojsonPath)) {
    console.warn(`[GDAL] No output for ${baseName}`);
    return null;
  }

  const fc = JSON.parse(fs.readFileSync(geojsonPath, "utf8"));
  // Keep only features where DN == 1 (above threshold)
  const features = (fc.features || []).filter(
    (f) => f.properties && f.properties.DN === 1
  );

  console.log(`[GDAL] ${baseName}: ${features.length} polygons above threshold`);
  return features;
}

// ── Dissolve / merge features into a single MultiPolygon ────────
// Pure-JS dissolve: collect all coordinate rings into one MultiPolygon.
// For production accuracy, @turf/union can be used, but for an initial
// pipeline this lightweight merge avoids the heavy dependency.

function dissolveFeatures(features) {
  if (!features || !features.length) return null;

  const allPolygonCoords = [];

  for (const f of features) {
    const geom = f.geometry;
    if (!geom) continue;

    if (geom.type === "Polygon" && Array.isArray(geom.coordinates)) {
      allPolygonCoords.push(geom.coordinates);
    } else if (geom.type === "MultiPolygon" && Array.isArray(geom.coordinates)) {
      for (const poly of geom.coordinates) {
        allPolygonCoords.push(poly);
      }
    }
  }

  if (!allPolygonCoords.length) return null;

  if (allPolygonCoords.length === 1) {
    return { type: "Polygon", coordinates: allPolygonCoords[0] };
  }

  return { type: "MultiPolygon", coordinates: allPolygonCoords };
}

// ── Compute centroid from GeoJSON geometry ──────────────────────
function computeCentroid(geojson) {
  const coords = [];

  function collectCoords(c) {
    if (typeof c[0] === "number") {
      coords.push(c);
    } else {
      for (const sub of c) collectCoords(sub);
    }
  }

  collectCoords(geojson.coordinates);
  if (!coords.length) return { lat: null, lon: null };

  let sumLat = 0,
    sumLon = 0;
  for (const [lon, lat] of coords) {
    sumLat += lat;
    sumLon += lon;
  }
  return {
    lat: sumLat / coords.length,
    lon: sumLon / coords.length,
  };
}

// ── Approximate area in sq mi from MultiPolygon ─────────────────
// Uses shoelace formula on WGS84 approximation (good enough for metadata).
function approxAreaSqMi(geojson) {
  function ringArea(ring) {
    // Shoelace on lon/lat pairs → approximate sq-degrees → sq-mi
    let area = 0;
    for (let i = 0; i < ring.length - 1; i++) {
      const [x1, y1] = ring[i];
      const [x2, y2] = ring[i + 1];
      area += x1 * y2 - x2 * y1;
    }
    return Math.abs(area) / 2;
  }

  let totalDeg2 = 0;
  const polys =
    geojson.type === "MultiPolygon"
      ? geojson.coordinates
      : [geojson.coordinates];

  for (const poly of polys) {
    if (Array.isArray(poly[0])) {
      totalDeg2 += ringArea(poly[0]); // outer ring only
    }
  }

  // 1 degree ≈ 69 mi at equator; rough mid-latitude factor
  const midLat = computeCentroid(geojson).lat || 39;
  const lonMi = 69 * Math.cos((midLat * Math.PI) / 180);
  const latMi = 69;
  return totalDeg2 * lonMi * latMi;
}

// ── Parse event time window from file names ─────────────────────
function parseTimeWindow(files, dateStr) {
  // Files like MESHMax1440min_00.50_20260330-120000.grib2
  const times = [];
  for (const f of files) {
    const m = f.match(/(\d{8})-(\d{6})/);
    if (m) {
      const d = m[1];
      const t = m[2];
      const iso = `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}T${t.slice(0, 2)}:${t.slice(2, 4)}:${t.slice(4, 6)}Z`;
      times.push(new Date(iso));
    }
  }
  times.sort((a, b) => a - b);
  return {
    start: times.length ? times[0].toISOString() : `${dateStr}T00:00:00Z`,
    end: times.length ? times[times.length - 1].toISOString() : `${dateStr}T23:59:59Z`,
  };
}

// ── Main ────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs();
  const dateStr = args.date;
  const thresholdIn = args.threshold;
  const thresholdMm = thresholdIn * 25.4; // MRMS MESH is in mm

  console.log(`[MRMS] Ingesting swaths for ${dateStr}, threshold=${thresholdIn} in (${thresholdMm} mm)`);

  ensureGdal();

  // 1. Find available MESH files for the date
  const listing = await listMeshFiles(dateStr);
  if (!listing || !listing.files.length) {
    console.warn(`[MRMS] No MESH data available for ${dateStr}. Exiting (SWDI fallback should run).`);
    process.exit(0);
  }

  const { product, dirUrl, files } = listing;
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "mrms-swath-"));
  console.log(`[MRMS] Work dir: ${workDir}`);

  // 2. Download and process files
  // For 1440-min product, typically one file per day. For shorter windows, process all.
  const allFeatures = [];
  const filesToProcess = product.startsWith("MESHMax1440")
    ? files.slice(0, 1)  // single daily max file
    : files;             // process all snapshots

  for (const fileName of filesToProcess) {
    try {
      const gribPath = await downloadGrib(dirUrl, fileName, workDir);
      const features = gribToGeojson(gribPath, thresholdMm, workDir);
      if (features && features.length) {
        allFeatures.push(...features);
      }
    } catch (e) {
      console.warn(`[MRMS] Failed to process ${fileName}: ${e.message}`);
    }
    await sleep(200); // be nice to the archive server
  }

  if (!allFeatures.length) {
    console.warn(`[MRMS] No hail polygons above threshold for ${dateStr}. Exiting.`);
    // Clean up
    fs.rmSync(workDir, { recursive: true, force: true });
    process.exit(0);
  }

  // 3. Dissolve all features into a single canonical geometry
  console.log(`[MRMS] Dissolving ${allFeatures.length} features…`);
  const dissolved = dissolveFeatures(allFeatures);
  if (!dissolved) {
    console.warn(`[MRMS] Dissolve produced no geometry. Exiting.`);
    fs.rmSync(workDir, { recursive: true, force: true });
    process.exit(0);
  }

  // 4. Compute metadata
  const centroid = computeCentroid(dissolved);
  const areaSqMi = approxAreaSqMi(dissolved);
  const timeWindow = parseTimeWindow(files, dateStr);

  console.log(
    `[MRMS] Canonical polygon: centroid=(${centroid.lat?.toFixed(3)}, ${centroid.lon?.toFixed(3)}) ` +
      `area≈${areaSqMi.toFixed(1)} sq mi, ` +
      `window=${timeWindow.start} → ${timeWindow.end}`
  );

  if (args.dryRun) {
    console.log("[MRMS] --dry-run: skipping database write.");
    console.log("[MRMS] GeoJSON type:", dissolved.type);
    const coordCount =
      dissolved.type === "MultiPolygon"
        ? dissolved.coordinates.reduce((s, p) => s + p[0].length, 0)
        : dissolved.coordinates[0].length;
    console.log("[MRMS] Total coordinate points:", coordCount);
    fs.rmSync(workDir, { recursive: true, force: true });
    return;
  }

  // 5. Upsert into public.storm_polygons
  const row = {
    event_date: dateStr,
    source: "mrms",
    source_product: product,
    source_priority: 1,
    polygon_geojson: dissolved,
    centroid_lat: centroid.lat,
    centroid_lon: centroid.lon,
    area_sq_mi: Math.round(areaSqMi * 100) / 100,
    threshold_value: thresholdIn,
    event_start_utc: timeWindow.start,
    event_end_utc: timeWindow.end,
    metadata_json: {
      source_url: dirUrl,
      files_used: filesToProcess,
      feature_count: allFeatures.length,
      threshold_mm: thresholdMm,
      threshold_in: thresholdIn,
      ingest_timestamp: new Date().toISOString(),
    },
  };

  console.log(`[MRMS] Upserting storm polygon for ${dateStr}…`);
  const { error } = await supabase
    .from("storm_polygons")
    .upsert(row, { onConflict: "event_date,source,source_product" });

  if (error) {
    console.error("[MRMS] Supabase upsert error:", error.message);
    process.exit(1);
  }

  console.log(`[MRMS] ✓ Canonical swath polygon written to storm_polygons for ${dateStr}`);

  // Clean up
  fs.rmSync(workDir, { recursive: true, force: true });
}

main().catch((e) => {
  console.error("[MRMS] Fatal:", e);
  process.exit(1);
});
