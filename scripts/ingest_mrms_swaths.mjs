#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────
// ingest_mrms_swaths.mjs  –  PRIMARY swath ingestion from MRMS MESH
//
// Downloads MRMS MESH hail grid products for a target date, thresholds
// them, dissolves contiguous hail cells, polygonizes the result, and
// writes canonical swath polygons into public.storm_polygons.
//
// Requires: GDAL CLI tools (gdal_translate, gdal_calc, gdal_polygonize)
//           On Windows with OSGeo4W/QGIS: uses python -m osgeo_utils.*
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
  const args = { date: null, threshold: DEFAULT_THRESHOLD_IN, dryRun: false, maxFiles: 0 };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--date=")) args.date = a.slice(7);
    else if (a.startsWith("--threshold=")) args.threshold = Number(a.slice(12));
    else if (a.startsWith("--max-files=")) args.maxFiles = Number(a.slice(12));
    else if (a === "--dry-run") args.dryRun = true;
  }
  if (!args.date || !/^\d{4}-\d{2}-\d{2}$/.test(args.date)) {
    console.error("Usage: node ingest_mrms_swaths.mjs --date=YYYY-MM-DD [--threshold=1.0] [--max-files=N] [--dry-run]");
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

// ── Platform-aware GDAL command helpers ─────────────────────────
const IS_WIN = process.platform === "win32";

// On Windows, GDAL Python utilities must run through the QGIS LTR
// Python launcher so that the GDAL/OGR environment is set up correctly.
// Plain "python -m osgeo_utils..." does NOT work from a normal terminal.
const QGIS_PYTHON_BAT = String.raw`C:\Program Files\QGIS 3.44.8\bin\python-qgis-ltr.bat`;

function qgisPythonAvailable() {
  if (!IS_WIN) return false;
  try {
    execSync(`"${QGIS_PYTHON_BAT}" -c "import osgeo_utils; print('ok')"`, {
      stdio: "pipe",
    });
    return true;
  } catch {
    return false;
  }
}

// Resolved once at startup so every call site uses the same path.
const USE_QGIS_PYTHON = IS_WIN && qgisPythonAvailable();

function gdalCalcCmd() {
  if (USE_QGIS_PYTHON) return `"${QGIS_PYTHON_BAT}" -m osgeo_utils.gdal_calc`;
  if (IS_WIN) return "python -m osgeo_utils.gdal_calc";
  return "gdal_calc.py";
}

function gdalPolygonizeCmd() {
  if (USE_QGIS_PYTHON) return `"${QGIS_PYTHON_BAT}" -m osgeo_utils.gdal_polygonize`;
  if (IS_WIN) return "python -m osgeo_utils.gdal_polygonize";
  return "gdal_polygonize.py";
}

function gdalSieveCmd() {
  if (USE_QGIS_PYTHON) return `"${QGIS_PYTHON_BAT}" -m osgeo_utils.gdal_sieve`;
  if (IS_WIN) return "python -m osgeo_utils.gdal_sieve";
  return "gdal_sieve.py";
}

function ensureGdal() {
  try {
    execSync("gdalinfo --version", { stdio: "pipe" });
  } catch {
    console.error(
      "ERROR: GDAL CLI tools not found.\n" +
        "Install GDAL (apt install gdal-bin / conda install gdal / brew install gdal) " +
        "and ensure gdalinfo and gdal_translate are on PATH.\n" +
        "On Windows with QGIS, ensure QGIS bin folder is on PATH for gdalinfo/gdal_translate."
    );
    process.exit(1);
  }
  // Verify the calc/polygonize path works on this platform
  try {
    execSync(gdalCalcCmd() + " --help", { stdio: "pipe" });
  } catch {
    // Non-Windows fallback: try without .py extension
    if (!IS_WIN) {
      try {
        execSync("gdal_calc --help", { stdio: "pipe" });
      } catch {
        console.error(
          "ERROR: gdal_calc not found. Ensure gdal_calc.py or gdal_calc is on PATH."
        );
        process.exit(1);
      }
    } else {
      console.error(
        "ERROR: GDAL Python utilities not working.\n" +
          "Tried: " + gdalCalcCmd() + "\n" +
          "Ensure QGIS LTR is installed at the expected path or python + osgeo_utils is available."
      );
      process.exit(1);
    }
  }
  if (IS_WIN) {
    console.log("[GDAL] Windows mode: " + (USE_QGIS_PYTHON ? "QGIS LTR python launcher" : "system python"));
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
  const sievedPath = path.join(workDir, baseName + "_sieved.tif");
  const geojsonPath = path.join(workDir, baseName + ".geojson");

  // Step 1: GRIB2 → GeoTIFF
  console.log(`[GDAL] grib→tif: ${baseName}`);
  execSync(
    `gdal_translate -of GTiff -q "${gribPath}" "${tifPath}"`,
    { stdio: "pipe" }
  );

  // Step 2: Threshold → binary mask
  // gdal_calc: output = 1 where A >= threshold, else 0
  console.log(`[GDAL] threshold >= ${thresholdMm} mm`);
  try {
    execSync(
      `${gdalCalcCmd()} -A "${tifPath}" --outfile="${maskPath}" ` +
        `--calc="(A>=${thresholdMm})*1" --type=Byte --NoDataValue=0 --quiet`,
      { stdio: "pipe" }
    );
  } catch {
    // Non-Windows fallback: try without .py extension
    execSync(
      `gdal_calc -A "${tifPath}" --outfile="${maskPath}" ` +
        `--calc="(A>=${thresholdMm})*1" --type=Byte --NoDataValue=0 --quiet`,
      { stdio: "pipe" }
    );
  }

  // Step 3: Sieve — remove small isolated pixel clusters before polygonize
  console.log(`[GDAL] sieve: removing isolated pixel groups`);
  execSync(`gdal_translate -of GTiff -q "${maskPath}" "${sievedPath}"`, { stdio: "pipe" });
  try {
    execSync(`${gdalSieveCmd()} -st 10 -8 "${sievedPath}"`, { stdio: "pipe" });
  } catch {
    try {
      execSync(`gdal_sieve -st 10 -8 "${sievedPath}"`, { stdio: "pipe" });
    } catch (e) {
      console.warn(`[GDAL] sieve failed, using raw mask: ${e.message}`);
      execSync(`gdal_translate -of GTiff -q "${maskPath}" "${sievedPath}"`, { stdio: "pipe" });
    }
  }

  // Step 4: Polygonize with 8-connectivity (diagonal cells merge → fewer blocks)
  console.log(`[GDAL] polygonize -8 → ${baseName}.geojson`);
  try {
    execSync(
      `${gdalPolygonizeCmd()} -8 "${sievedPath}" -f GeoJSON "${geojsonPath}" mesh_swath DN`,
      { stdio: "pipe" }
    );
  } catch {
    // Non-Windows fallback: try without .py extension
    execSync(
      `gdal_polygonize -8 "${sievedPath}" -f GeoJSON "${geojsonPath}" mesh_swath DN`,
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

// ── Dissolve / merge features into a clean MultiPolygon ─────────
// 1. Extract individual polygon components from all features
// 2. Filter out tiny noise polygons (below MIN_POLY_AREA_SQ_MI)
// 3. Cluster nearby polygons spatially
// 4. Keep all clusters above an absolute minimum area (no relative pruning)
// 5. Preserve disjoint storm corridors as separate components

// Minimum area for a polygon component to be kept (sq mi).
// MRMS grids produce many single-cell slivers — this removes them.
const MIN_POLY_AREA_SQ_MI = 2.0;

// Spatial clustering: polygons whose bboxes are within this gap are grouped.
// Keep small so distinct corridors (e.g. IL vs KS) stay separate clusters.
const CLUSTER_GAP_DEG = 0.3;       // ~18-21 mi at mid-latitudes

// Minimum total area (sq mi) for a cluster to be kept in the final output.
const MIN_CLUSTER_AREA_SQ_MI = 3.0;

// Douglas-Peucker simplification tolerance (degrees).
// MRMS grid is ~0.01°; 0.025° aggressively smooths staircase edges.
const SIMPLIFY_TOLERANCE_DEG = 0.025;

// Minimum ring vertex count after simplification (must keep at least a triangle + close).
const MIN_RING_POINTS = 4;

function ringAreaDeg2(ring) {
  let area = 0;
  for (let i = 0; i < ring.length - 1; i++) {
    const [x1, y1] = ring[i];
    const [x2, y2] = ring[i + 1];
    area += x1 * y2 - x2 * y1;
  }
  return Math.abs(area) / 2;
}

function polyAreaSqMi(polyCoords) {
  if (!Array.isArray(polyCoords) || !Array.isArray(polyCoords[0])) return 0;
  const outerRing = polyCoords[0];
  const deg2 = ringAreaDeg2(outerRing);
  // Rough mid-latitude conversion
  let sumLat = 0, count = 0;
  for (const [, lat] of outerRing) { sumLat += lat; count++; }
  const midLat = count ? sumLat / count : 39;
  const lonMi = 69 * Math.cos((midLat * Math.PI) / 180);
  return deg2 * lonMi * 69;
}

function polyBbox(polyCoords) {
  let minLon = 180, maxLon = -180, minLat = 90, maxLat = -90;
  for (const ring of polyCoords) {
    for (const [lon, lat] of ring) {
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
  }
  return { minLon, maxLon, minLat, maxLat };
}

// ── Geometry smoothing helpers ───────────────────────────────────
// Douglas-Peucker line simplification to remove raster staircase artifacts.

function perpendicularDist(pt, lineStart, lineEnd) {
  const [x, y] = pt;
  const [x1, y1] = lineStart;
  const [x2, y2] = lineEnd;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return Math.sqrt((x - x1) ** 2 + (y - y1) ** 2);
  const t = Math.max(0, Math.min(1, ((x - x1) * dx + (y - y1) * dy) / lenSq));
  const px = x1 + t * dx;
  const py = y1 + t * dy;
  return Math.sqrt((x - px) ** 2 + (y - py) ** 2);
}

function douglasPeucker(points, tolerance) {
  if (points.length <= 2) return points;
  let maxDist = 0;
  let maxIdx = 0;
  const first = points[0];
  const last = points[points.length - 1];
  for (let i = 1; i < points.length - 1; i++) {
    const d = perpendicularDist(points[i], first, last);
    if (d > maxDist) { maxDist = d; maxIdx = i; }
  }
  if (maxDist > tolerance) {
    const left = douglasPeucker(points.slice(0, maxIdx + 1), tolerance);
    const right = douglasPeucker(points.slice(maxIdx), tolerance);
    return left.slice(0, -1).concat(right);
  }
  return [first, last];
}

function simplifyRing(ring, tolerance) {
  if (!ring || ring.length < MIN_RING_POINTS) return ring;
  const simplified = douglasPeucker(ring, tolerance);
  // Ensure ring is closed
  if (simplified.length >= 2) {
    const f = simplified[0];
    const l = simplified[simplified.length - 1];
    if (f[0] !== l[0] || f[1] !== l[1]) simplified.push([...f]);
  }
  // Must keep at least a triangle
  if (simplified.length < MIN_RING_POINTS) return ring;
  return simplified;
}

// Chaikin corner-cutting: rounds staircase right-angles into curves.
function chaikinSmooth(ring, iterations = 1) {
  if (!ring || ring.length < MIN_RING_POINTS) return ring;
  let pts = ring;
  for (let iter = 0; iter < iterations; iter++) {
    const out = [];
    const n = pts.length - 1; // ignore closing duplicate
    if (n < 3) break;
    for (let i = 0; i < n; i++) {
      const [x1, y1] = pts[i];
      const [x2, y2] = pts[(i + 1) % n];
      out.push([0.75 * x1 + 0.25 * x2, 0.75 * y1 + 0.25 * y2]);
      out.push([0.25 * x1 + 0.75 * x2, 0.25 * y1 + 0.75 * y2]);
    }
    out.push([out[0][0], out[0][1]]); // close ring
    pts = out;
  }
  return pts;
}

function simplifyPolygon(polyCoords, tolerance) {
  return polyCoords.map((ring) => {
    const simplified = simplifyRing(ring, tolerance);
    return chaikinSmooth(simplified, 2);
  });
}

// ── Per-cluster cell merge via shared-edge removal ──────────────
// Raster polygons share exact grid-aligned edges. Removing interior
// (shared) edges and reconstructing the outer rings produces clean
// outlines instead of many individual square cells.

function edgeKey(a, b) {
  // Canonical key: smaller point first so both directions match
  const ax = a[0], ay = a[1], bx = b[0], by = b[1];
  if (ax < bx || (ax === bx && ay < by)) return `${ax},${ay}|${bx},${by}`;
  return `${bx},${by}|${ax},${ay}`;
}

function mergeClusterCells(polysList) {
  if (polysList.length <= 1) return polysList;

  // Collect edge counts (shared interior edges appear 2+)
  const edgeCounts = new Map();
  for (const polyCoords of polysList) {
    const ring = polyCoords[0]; // outer ring
    for (let i = 0; i < ring.length - 1; i++) {
      const k = edgeKey(ring[i], ring[i + 1]);
      edgeCounts.set(k, (edgeCounts.get(k) || 0) + 1);
    }
  }

  // Keep only boundary edges (count === 1)
  const boundaryEdges = [];
  for (const polyCoords of polysList) {
    const ring = polyCoords[0];
    for (let i = 0; i < ring.length - 1; i++) {
      const k = edgeKey(ring[i], ring[i + 1]);
      if (edgeCounts.get(k) === 1) {
        boundaryEdges.push([ring[i], ring[i + 1]]);
      }
    }
  }

  if (!boundaryEdges.length) return polysList;

  // Build adjacency: for each point, list of next-points in boundary edges
  const adj = new Map();
  function ptKey(p) { return `${p[0]},${p[1]}`; }
  for (const [a, b] of boundaryEdges) {
    const ka = ptKey(a);
    if (!adj.has(ka)) adj.set(ka, []);
    adj.get(ka).push(b);
  }

  // Walk rings from boundary edges
  const used = new Set();
  const rings = [];
  for (const [startPt] of boundaryEdges) {
    const sk = ptKey(startPt);
    if (used.has(sk)) continue;
    const ring = [startPt];
    let cur = startPt;
    let safety = boundaryEdges.length + 1;
    while (--safety > 0) {
      const ck = ptKey(cur);
      used.add(ck);
      const nexts = adj.get(ck);
      if (!nexts || !nexts.length) break;
      // Pick first unused neighbor, or first neighbor to close ring
      let next = null;
      for (let n = 0; n < nexts.length; n++) {
        const nk = ptKey(nexts[n]);
        if (nk === sk && ring.length >= 3) { next = nexts[n]; nexts.splice(n, 1); break; }
        if (!used.has(nk)) { next = nexts[n]; nexts.splice(n, 1); break; }
      }
      if (!next) break;
      ring.push(next);
      if (ptKey(next) === sk) break; // closed
      cur = next;
    }
    if (ring.length >= 4 && ptKey(ring[ring.length - 1]) === sk) {
      rings.push(ring);
    }
  }

  if (!rings.length) return polysList; // fallback if tracing failed

  // Classify: positive-area rings are outer, negative (CW) are holes
  // GeoJSON standard: outer = CCW (positive shoelace), hole = CW (negative)
  const result = [];
  for (const ring of rings) {
    let signedArea = 0;
    for (let i = 0; i < ring.length - 1; i++) {
      const [x1, y1] = ring[i];
      const [x2, y2] = ring[i + 1];
      signedArea += (x2 - x1) * (y2 + y1);
    }
    // In lon/lat, CCW outer ring has negative signedArea with this formula
    if (signedArea <= 0) {
      // Outer ring → new Polygon
      result.push([ring]);
    } else {
      // Hole → attach to the last outer ring (simplistic but usually correct)
      if (result.length) {
        result[result.length - 1].push(ring);
      }
    }
  }

  return result.length ? result : polysList;
}

function clusterPolygons(polys) {
  const items = polys.map((coords, i) => ({
    coords,
    bbox: polyBbox(coords),
    cluster: i,
  }));

  function bboxesNear(a, b) {
    if (a.maxLon + CLUSTER_GAP_DEG < b.minLon) return false;
    if (b.maxLon + CLUSTER_GAP_DEG < a.minLon) return false;
    if (a.maxLat + CLUSTER_GAP_DEG < b.minLat) return false;
    if (b.maxLat + CLUSTER_GAP_DEG < a.minLat) return false;
    return true;
  }

  let changed = true;
  while (changed) {
    changed = false;
    for (let i = 0; i < items.length; i++) {
      for (let j = i + 1; j < items.length; j++) {
        if (items[i].cluster !== items[j].cluster &&
            bboxesNear(items[i].bbox, items[j].bbox)) {
          const oldC = items[j].cluster;
          const newC = items[i].cluster;
          for (const it of items) {
            if (it.cluster === oldC) it.cluster = newC;
          }
          changed = true;
        }
      }
    }
  }

  const groups = new Map();
  for (const it of items) {
    if (!groups.has(it.cluster)) groups.set(it.cluster, []);
    groups.get(it.cluster).push(it.coords);
  }
  return [...groups.values()];
}

function dissolveFeatures(features) {
  if (!features || !features.length) return null;

  // Step 1: collect all individual polygon coordinate arrays
  const rawPolys = [];
  for (const f of features) {
    const geom = f.geometry;
    if (!geom) continue;
    if (geom.type === "Polygon" && Array.isArray(geom.coordinates)) {
      rawPolys.push(geom.coordinates);
    } else if (geom.type === "MultiPolygon" && Array.isArray(geom.coordinates)) {
      for (const poly of geom.coordinates) {
        rawPolys.push(poly);
      }
    }
  }

  console.log(`[dissolve] Raw polygon count: ${rawPolys.length}`);

  if (!rawPolys.length) return null;

  // Step 2: filter out noise — remove polygons below area threshold
  const kept = [];
  let droppedCount = 0;
  let droppedAreaTotal = 0;
  for (const poly of rawPolys) {
    const area = polyAreaSqMi(poly);
    if (area < MIN_POLY_AREA_SQ_MI) {
      droppedCount++;
      droppedAreaTotal += area;
    } else {
      kept.push(poly);
    }
  }

  console.log(`[dissolve] Polygon count after filtering (>= ${MIN_POLY_AREA_SQ_MI} sq mi): ${kept.length} kept, ${droppedCount} dropped (${droppedAreaTotal.toFixed(2)} sq mi total noise)`);

  if (!kept.length) {
    console.warn(`[dissolve] All polygons were noise — relaxing filter, keeping largest`);
    let bestIdx = 0, bestArea = 0;
    for (let i = 0; i < rawPolys.length; i++) {
      const a = polyAreaSqMi(rawPolys[i]);
      if (a > bestArea) { bestArea = a; bestIdx = i; }
    }
    kept.push(rawPolys[bestIdx]);
    console.log(`[dissolve] Fallback: keeping 1 polygon (${bestArea.toFixed(2)} sq mi)`);
  }

  // Step 3: spatial clustering — group nearby polygons
  const clusters = clusterPolygons(kept);
  console.log(`[dissolve] Spatial clusters found: ${clusters.length}`);

  const scored = clusters.map((polys) => {
    const totalArea = polys.reduce((s, p) => s + polyAreaSqMi(p), 0);
    // Cluster centroid (simple average of all polygon centroids)
    let cLon = 0, cLat = 0, cCount = 0;
    for (const p of polys) {
      const ring = p[0];
      for (const [lon, lat] of ring) { cLon += lon; cLat += lat; cCount++; }
    }
    cLon /= cCount || 1;
    cLat /= cCount || 1;
    // Composite score: area weighted by polygon density (more cells = more confident)
    const score = totalArea * Math.sqrt(polys.length);
    return { polys, totalArea, cLon, cLat, score };
  }).sort((a, b) => b.score - a.score);

  for (let i = 0; i < scored.length; i++) {
    const cBbox = { minLon: 180, maxLon: -180, minLat: 90, maxLat: -90 };
    for (const p of scored[i].polys) {
      const b = polyBbox(p);
      if (b.minLon < cBbox.minLon) cBbox.minLon = b.minLon;
      if (b.maxLon > cBbox.maxLon) cBbox.maxLon = b.maxLon;
      if (b.minLat < cBbox.minLat) cBbox.minLat = b.minLat;
      if (b.maxLat > cBbox.maxLat) cBbox.maxLat = b.maxLat;
    }
    console.log(`[dissolve]   cluster ${i}: ${scored[i].polys.length} polys, ${scored[i].totalArea.toFixed(1)} sq mi, score=${scored[i].score.toFixed(1)}, center=(${scored[i].cLat.toFixed(2)},${scored[i].cLon.toFixed(2)}), bbox [${cBbox.minLon.toFixed(2)},${cBbox.minLat.toFixed(2)}]→[${cBbox.maxLon.toFixed(2)},${cBbox.maxLat.toFixed(2)}]`);
  }

  // Step 4a: filter by absolute area threshold
  const aboveMin = scored.filter((c) => c.totalArea >= MIN_CLUSTER_AREA_SQ_MI);
  if (!aboveMin.length) {
    aboveMin.push(scored[0]);
    console.warn(`[dissolve] No cluster met ${MIN_CLUSTER_AREA_SQ_MI} sq mi — keeping top-scored (${scored[0].score.toFixed(1)})`);
  }

  // Keep ALL clusters above the area threshold — no anchor/corridor pruning.
  // Every real hail swath for the date gets its own storm_polygons row.
  const significant = aboveMin;

  console.log(`[dissolve] Kept clusters: ${significant.length} of ${scored.length} (min area ${MIN_CLUSTER_AREA_SQ_MI} sq mi)`);

  // Step 5-7: per cluster → merge cells → simplify → build geometry
  const swaths = [];
  for (let ci = 0; ci < significant.length; ci++) {
    const c = significant[ci];
    const outlines = mergeClusterCells(c.polys);
    const smoothed = outlines.map((p) => simplifyPolygon(p, SIMPLIFY_TOLERANCE_DEG));
    let geom;
    if (smoothed.length === 1) {
      geom = { type: "Polygon", coordinates: smoothed[0] };
    } else {
      geom = { type: "MultiPolygon", coordinates: smoothed };
    }
    const cent = computeCentroid(geom);
    const area = approxAreaSqMi(geom);
    console.log(`[dissolve] swath ${ci}: ${geom.type}, ${smoothed.length} ring(s), ${area.toFixed(1)} sq mi, centroid=(${cent.lat?.toFixed(3)}, ${cent.lon?.toFixed(3)})`);
    swaths.push(geom);
  }

  console.log(`[dissolve] Returning ${swaths.length} independent swath(s)`);
  return swaths;
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
  let filesToProcess = product.startsWith("MESHMax1440")
    ? files.slice(0, 1)  // single daily max file
    : files;             // process all snapshots

  if (args.maxFiles > 0 && filesToProcess.length > args.maxFiles) {
    console.log(`[MRMS] Limiting to first ${args.maxFiles} file(s) (of ${filesToProcess.length})`);
    filesToProcess = filesToProcess.slice(0, args.maxFiles);
  }

  for (const fileName of filesToProcess) {
    let gribPath;
    try {
      gribPath = await downloadGrib(dirUrl, fileName, workDir);
      const features = gribToGeojson(gribPath, thresholdMm, workDir);
      if (features && features.length) {
        allFeatures.push(...features);
      }
    } catch (e) {
      console.warn(`[MRMS] Failed to process ${fileName}: ${e.message}`);
    }
    // Clean up per-file temp artifacts immediately to avoid filling disk
    if (gribPath) {
      const base = path.basename(gribPath, ".grib2");
      for (const f of [
        gribPath,
        path.join(workDir, base + ".tif"),
        path.join(workDir, base + "_mask.tif"),
        path.join(workDir, base + "_sieved.tif"),
        path.join(workDir, base + ".geojson"),
      ]) {
        try { fs.unlinkSync(f); } catch {}
      }
      console.log(`[MRMS] Cleaned temp artifacts for ${base}`);
    }
    await sleep(200); // be nice to the archive server
  }

  if (!allFeatures.length) {
    console.warn(`[MRMS] No hail polygons above threshold for ${dateStr}. Exiting.`);
    // Clean up
    fs.rmSync(workDir, { recursive: true, force: true });
    process.exit(0);
  }

  // 3. Filter noise and build per-cluster swath geometries
  console.log(`[MRMS] Processing ${allFeatures.length} raw features…`);
  const swaths = dissolveFeatures(allFeatures);
  if (!swaths || !swaths.length) {
    console.warn(`[MRMS] Dissolve produced no geometry. Exiting.`);
    fs.rmSync(workDir, { recursive: true, force: true });
    process.exit(0);
  }

  // 4. Compute shared metadata
  const timeWindow = parseTimeWindow(files, dateStr);

  console.log(
    `[MRMS] ${swaths.length} swath(s) for ${dateStr}, ` +
      `window=${timeWindow.start} → ${timeWindow.end}`
  );

  if (args.dryRun) {
    console.log("[MRMS] --dry-run: skipping database write.");
    swaths.forEach(function (geom, i) {
      const coordCount =
        geom.type === "MultiPolygon"
          ? geom.coordinates.reduce((s, p) => s + p[0].length, 0)
          : geom.coordinates[0].length;
      console.log(`[MRMS]   swath ${i}: ${geom.type}, ${coordCount} coords`);
    });
    fs.rmSync(workDir, { recursive: true, force: true });
    return;
  }

  // 5. Delete existing MRMS rows for this date, then insert one row per swath
  console.log(`[MRMS] Deleting existing MRMS rows for ${dateStr}…`);
  const { error: delErr } = await supabase
    .from("storm_polygons")
    .delete()
    .eq("event_date", dateStr)
    .eq("source", "mrms");

  if (delErr) {
    console.error("[MRMS] Delete error:", delErr.message);
    process.exit(1);
  }

  const rows = swaths.map(function (geom, i) {
    const centroid = computeCentroid(geom);
    const areaSqMi = approxAreaSqMi(geom);
    return {
      event_date: dateStr,
      source: "mrms",
      source_product: product,
      source_priority: 1,
      swath_index: i,
      polygon_geojson: geom,
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
        swath_index: i,
        swath_count: swaths.length,
        ingest_timestamp: new Date().toISOString(),
      },
    };
  });

  console.log(`[MRMS] Inserting ${rows.length} swath row(s) for ${dateStr}…`);
  const { error: insErr } = await supabase
    .from("storm_polygons")
    .insert(rows);

  if (insErr) {
    console.error("[MRMS] Supabase insert error:", insErr.message);
    process.exit(1);
  }

  console.log(`[MRMS] ✓ ${rows.length} swath polygon(s) written to storm_polygons for ${dateStr}`);

  // Clean up
  fs.rmSync(workDir, { recursive: true, force: true });
}

main().catch((e) => {
  console.error("[MRMS] Fatal:", e);
  process.exit(1);
});
