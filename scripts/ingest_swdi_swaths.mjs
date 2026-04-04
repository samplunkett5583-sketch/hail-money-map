#!/usr/bin/env node
// ──────────────────────────────────────────────────────────────────
// ingest_swdi_swaths.mjs  –  SECONDARY swath ingestion from NOAA SWDI
//
// Fetches severe-weather GIS geometry from the SWDI (Severe Weather
// Data Inventory) for a target date. Normalizes WKT/GeoJSON geometry
// and writes canonical swath polygons into public.storm_polygons.
//
// This is a FALLBACK source — only used when MRMS data is unavailable,
// invalid, or below quality rules. The orchestrator script should check
// storm_polygons for an existing MRMS row before calling this.
//
// Usage:
//   node scripts/ingest_swdi_swaths.mjs --date=2026-03-30
//   node scripts/ingest_swdi_swaths.mjs --date=2026-03-30 --force
//   node scripts/ingest_swdi_swaths.mjs --date=2026-03-30 --dry-run
// ──────────────────────────────────────────────────────────────────

import https from "node:https";
import { createClient } from "@supabase/supabase-js";

// ── Config ──────────────────────────────────────────────────────
const SWDI_BASE = "https://www.ncei.noaa.gov/access/services/search/v1/data";
const NOAA_PROXY_BASE =
  process.env.NOAA_PROXY_BASE ||
  "https://noaaplsrproxy-jzyejnppqa-uc.a.run.app";
const NOAA_CONUS_BBOX = "-130,20,-60,55";

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
  const args = { date: null, force: false, dryRun: false };
  for (const a of process.argv.slice(2)) {
    if (a.startsWith("--date=")) args.date = a.slice(7);
    else if (a === "--force") args.force = true;
    else if (a === "--dry-run") args.dryRun = true;
  }
  if (!args.date || !/^\d{4}-\d{2}-\d{2}$/.test(args.date)) {
    console.error("Usage: node ingest_swdi_swaths.mjs --date=YYYY-MM-DD [--force] [--dry-run]");
    process.exit(1);
  }
  return args;
}

// ── Helpers ─────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          return fetchJson(res.headers.location).then(resolve, reject);
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error(`JSON parse error: ${e.message}`));
          }
        });
      })
      .on("error", reject);
  });
}

// ── Check if MRMS already ingested this date ────────────────────
async function mrmsExistsForDate(dateStr) {
  const { data, error } = await supabase
    .from("storm_polygons")
    .select("id")
    .eq("event_date", dateStr)
    .eq("source", "mrms")
    .limit(1);

  if (error) {
    console.warn("[SWDI] Failed to check MRMS existence:", error.message);
    return false;
  }
  return Array.isArray(data) && data.length > 0;
}

// ── WKT → GeoJSON parser ───────────────────────────────────────
function wktToGeojson(wkt) {
  if (!wkt) return null;
  const s = String(wkt).trim();
  const upper = s.toUpperCase();

  function parsePairs(str) {
    return str
      .split(",")
      .map((p) => {
        const parts = p.trim().split(/\s+/);
        if (parts.length < 2) return null;
        const lon = Number(parts[0]);
        const lat = Number(parts[1]);
        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
        return [lon, lat];
      })
      .filter(Boolean);
  }

  if (upper.startsWith("MULTIPOLYGON")) {
    const mi = s.indexOf("(((");
    const me = s.lastIndexOf(")))");
    if (mi < 0 || me < 0) return null;
    const inner = s.slice(mi + 3, me);
    const polyParts = inner.split(")),((");
    const coordinates = [];
    for (const part of polyParts) {
      const rings = part.includes("),(") ? part.split("),(") : [part];
      const outerRing = parsePairs(rings[0].replace(/^\(|\)$/g, ""));
      if (outerRing.length >= 3) {
        // Close ring if needed
        const first = outerRing[0];
        const last = outerRing[outerRing.length - 1];
        if (first[0] !== last[0] || first[1] !== last[1]) {
          outerRing.push([...first]);
        }
        coordinates.push([outerRing]);
      }
    }
    return coordinates.length
      ? { type: "MultiPolygon", coordinates }
      : null;
  }

  if (upper.startsWith("POLYGON")) {
    const pi = s.indexOf("((");
    const pe = s.lastIndexOf("))");
    if (pi < 0 || pe < 0) return null;
    const body = s.slice(pi + 2, pe);
    const rings = body.includes("),(") ? body.split("),(") : [body];
    const outerRing = parsePairs(rings[0].replace(/^\(|\)$/g, ""));
    if (outerRing.length < 3) return null;
    const first = outerRing[0];
    const last = outerRing[outerRing.length - 1];
    if (first[0] !== last[0] || first[1] !== last[1]) {
      outerRing.push([...first]);
    }
    return { type: "Polygon", coordinates: [outerRing] };
  }

  if (upper.startsWith("POINT")) {
    const pm = s.match(
      /POINT\s*\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)/i
    );
    if (!pm) return null;
    const lon = Number(pm[1]);
    const lat = Number(pm[2]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    // Buffer point into a small diamond polygon (~4 km)
    const d = 0.04;
    return {
      type: "Polygon",
      coordinates: [
        [
          [lon, lat + d],
          [lon + d, lat],
          [lon, lat - d],
          [lon - d, lat],
          [lon, lat + d],
        ],
      ],
    };
  }

  return null;
}

// ── Fetch SWDI radar hail cells via the existing NOAA proxy ─────
async function fetchSwdiRadarStorms(dateStr) {
  const ymd = dateStr.replace(/-/g, "");
  const url =
    `${NOAA_PROXY_BASE}/noaaPlsrProxy?mode=radar` +
    `&start=${ymd}&end=${ymd}` +
    `&bbox=${encodeURIComponent(NOAA_CONUS_BBOX)}` +
    `&limit=50000`;

  console.log(`[SWDI] Fetching radar storms: ${url}`);
  const data = await fetchJson(url);
  const storms = Array.isArray(data?.storms) ? data.storms : [];
  console.log(`[SWDI] Got ${storms.length} radar storm entries`);
  return storms;
}

// ── Extract geometry features from SWDI storm entries ───────────
function extractGeometries(storms) {
  const features = [];

  for (const s of storms) {
    // Try SHAPE field from SWDI properties
    const shape = s.props
      ? s.props.SHAPE || s.props.shape || s.props.wkt
      : null;

    let geom = null;

    if (shape) {
      geom = wktToGeojson(shape);
    }

    // If no WKT shape, try lat/lon point with buffer
    if (!geom) {
      const lat = Number(s.lat);
      const lon = Number(s.lon);
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        const d = 0.04;
        geom = {
          type: "Polygon",
          coordinates: [
            [
              [lon, lat + d],
              [lon + d, lat],
              [lon, lat - d],
              [lon - d, lat],
              [lon, lat + d],
            ],
          ],
        };
      }
    }

    if (geom) {
      features.push({
        geometry: geom,
        hailSize: s.hailSize || s.hail_size || null,
        date: s.date,
        ztime: s.ztime,
      });
    }
  }

  return features;
}

// ── Dissolve features into single canonical geometry ────────────
function dissolveFeatures(features) {
  if (!features.length) return null;

  const allPolygonCoords = [];

  for (const f of features) {
    const geom = f.geometry;
    if (geom.type === "Polygon") {
      allPolygonCoords.push(geom.coordinates);
    } else if (geom.type === "MultiPolygon") {
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

// ── Compute centroid ────────────────────────────────────────────
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

// ── Approximate area ────────────────────────────────────────────
function approxAreaSqMi(geojson) {
  function ringArea(ring) {
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
    if (Array.isArray(poly[0])) totalDeg2 += ringArea(poly[0]);
  }
  const midLat = computeCentroid(geojson).lat || 39;
  const lonMi = 69 * Math.cos((midLat * Math.PI) / 180);
  return totalDeg2 * lonMi * 69;
}

// ── Quality check: is this geometry usable? ─────────────────────
function isUsableGeometry(geojson, features) {
  if (!geojson) return false;
  // At least 3 coordinate points
  let coordCount = 0;
  function count(c) {
    if (typeof c[0] === "number") coordCount++;
    else for (const sub of c) count(sub);
  }
  count(geojson.coordinates);
  if (coordCount < 3) return false;
  // At least 1 real polygon feature (not just buffered points)
  const realPolygons = features.filter(
    (f) => f.geometry.type === "Polygon" || f.geometry.type === "MultiPolygon"
  );
  return realPolygons.length >= 1;
}

// ── Main ────────────────────────────────────────────────────────
async function main() {
  const args = parseArgs();
  const dateStr = args.date;

  console.log(`[SWDI] Secondary swath ingestion for ${dateStr}`);

  // Check if MRMS already covers this date (skip unless --force)
  if (!args.force) {
    const hasMrms = await mrmsExistsForDate(dateStr);
    if (hasMrms) {
      console.log(`[SWDI] MRMS polygon already exists for ${dateStr}. Skipping (use --force to override).`);
      process.exit(0);
    }
  }

  // Fetch SWDI radar geometry
  const storms = await fetchSwdiRadarStorms(dateStr);
  if (!storms.length) {
    console.warn(`[SWDI] No SWDI data for ${dateStr}. No swath to write.`);
    process.exit(0);
  }

  // Extract and normalize geometries
  const features = extractGeometries(storms);
  console.log(`[SWDI] Extracted ${features.length} geometry features`);

  if (!features.length) {
    console.warn(`[SWDI] No usable geometry for ${dateStr}.`);
    process.exit(0);
  }

  // Dissolve into canonical polygon
  const dissolved = dissolveFeatures(features);
  if (!isUsableGeometry(dissolved, features)) {
    console.warn(`[SWDI] Geometry below quality threshold for ${dateStr}.`);
    process.exit(0);
  }

  // Metadata
  const centroid = computeCentroid(dissolved);
  const areaSqMi = approxAreaSqMi(dissolved);

  // Time window from storm entries
  const times = features
    .map((f) => f.ztime || f.date)
    .filter(Boolean)
    .sort();

  console.log(
    `[SWDI] Canonical polygon: centroid=(${centroid.lat?.toFixed(3)}, ${centroid.lon?.toFixed(3)}) ` +
      `area≈${areaSqMi.toFixed(1)} sq mi, features=${features.length}`
  );

  if (args.dryRun) {
    console.log("[SWDI] --dry-run: skipping database write.");
    return;
  }

  // Upsert into storm_polygons
  const row = {
    event_date: dateStr,
    source: "swdi",
    source_product: "SWDI_hail_radar",
    source_priority: 2,
    swath_index: 0,
    polygon_geojson: dissolved,
    centroid_lat: centroid.lat,
    centroid_lon: centroid.lon,
    area_sq_mi: Math.round(areaSqMi * 100) / 100,
    threshold_value: null, // SWDI doesn't use our threshold
    event_start_utc: times.length ? times[0] : `${dateStr}T00:00:00Z`,
    event_end_utc: times.length ? times[times.length - 1] : `${dateStr}T23:59:59Z`,
    metadata_json: {
      source: "SWDI_via_NOAA_proxy",
      storm_count: storms.length,
      geometry_count: features.length,
      ingest_timestamp: new Date().toISOString(),
    },
  };

  const { error } = await supabase
    .from("storm_polygons")
    .upsert(row, { onConflict: "event_date,source,source_product,swath_index" });

  if (error) {
    console.error("[SWDI] Supabase upsert error:", error.message);
    process.exit(1);
  }

  console.log(`[SWDI] ✓ Swath polygon written to storm_polygons for ${dateStr}`);
}

main().catch((e) => {
  console.error("[SWDI] Fatal:", e);
  process.exit(1);
});
