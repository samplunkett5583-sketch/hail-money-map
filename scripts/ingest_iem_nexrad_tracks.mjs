#!/usr/bin/env node
/**
 * ingest_iem_nexrad_tracks.mjs
 *
 * Builds storm corridor polygons from real IEM NEXRAD storm-attribute
 * point tracks.  For one requested date it:
 *   1. Queries mesonet.agron.iastate.edu/geojson/nexrad_attr.py every
 *      5 minutes across the storm window (12Z–06Z next day).
 *   2. Filters to features with meaningful hail signal.
 *   3. Groups tracked cells by nexrad + storm_id.
 *   4. Builds a buffered corridor polygon from each track.
 *   5. Saves passing corridors to storm_polygons.
 *
 * Usage:
 *   node scripts/ingest_iem_nexrad_tracks.mjs [YYYY-MM-DD]
 *   Default date: 2026-04-01
 *
 * Env vars:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 */

import { createClient } from "@supabase/supabase-js";
import polygonClipping from "polygon-clipping";

// ── Env / args ──────────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

// Date handling moved to entry point — supports single, multi, and batch modes
let TARGET_DATE = "2026-04-01"; // overwritten by entry point

// ── Tunables ────────────────────────────────────────────────
const IEM_URL      = "https://mesonet.agron.iastate.edu/geojson/nexrad_attr.py";
const WIN_START_H  = 12;          // 12:00 UTC
const WIN_END_H    = 30;          // 06:00 UTC next day
const STEP_MIN     = 5;
const FETCH_DELAY  = 200;         // ms between requests

// ── Sanity / rejection thresholds ──
const MAX_BBOX_LAT = 3;           // max latitude span (degrees)
const MAX_BBOX_LON = 3;           // max longitude span (degrees)
const MIN_PTS      = 4;           // min track timestamps
const MAX_JUMP     = 1.5;         // degrees per step
const MAX_GAP_MIN  = 30;          // temporal split threshold (minutes)
const SPATIAL_GAP  = 0.3;         // spatial split threshold (degrees)
const MAX_AREA     = 800;         // sq mi area cap
const MAX_TRACK_MI = 200;         // max first-to-last distance in miles
const MIN_RING_PTS = 4;           // min polygon ring points
const HULL_BBOX_RATIO = 1.5;      // hull bbox / track bbox max ratio
const MAX_DIR_DEV  = 90;          // max heading deviation (degrees)
const HULL_SPAN_CAP = 1.5;        // reject hull fallback if track span > this (degrees)
const DEDUP_DIST   = 0.5;         // dedup centroid distance (degrees)
const DEDUP_TIME_OVERLAP = 0.3;   // dedup min time-overlap fraction

// ── Dominance / quality filters (post-polygon) ──
const MIN_TRACK_POINTS       = 6;    // reject short-lived tracks
const MIN_AREA_SQ_MI         = 40;   // reject tiny polygons (unless many pts)
const MIN_AREA_SOFT_PTS      = 10;   // area filter relaxed if pts >= this
const MIN_HAIL_INCHES        = 0.75; // reject sub-severe max_size
const ISOLATION_RADIUS_DEG   = 2.0;  // ~150 mi at mid-latitudes
const ISOLATION_TIME_HR      = 2;    // ±hours for neighbor match
const ISOLATION_BEARING_DEG  = 30;   // bearing tolerance for same-motion
const MIN_NEIGHBORS          = 1;    // isolated if fewer neighbors than this

const REGION_WESTERN_SD_NE_WY = {
  latMin: 43.0,
  latMax: 45.5,
  lonMin: -105.5,
  lonMax: -101.0,
};
const REGION_NORTH_DAKOTA = {
  latMin: 46.0,
  latMax: 49.8,
  lonMin: -102.5,
  lonMax: -96.0,
};
const REGION_RADARS = new Set(["UDX", "CYS", "LNX", "ABR", "UNR"]);

// ── Regional cluster pruning (post-scoring) ──
const CLUSTER_RADIUS_MI            = 80;
const CLUSTER_BEARING_TOLERANCE_DEG = 25;
const MAX_KEEP_PER_CLUSTER          = 12;
const MIN_CLUSTER_SCORE_RATIO       = 0.15;
const CONTINUITY_MIN_DIST_MI        = 40;
const CONTINUITY_BEARING_TOL_DEG    = 25;
const CONTINUITY_MAX_EXTRA          = 3;

// ── Buffer sizing ──
const BUF_SCALE    = 0.055;       // degrees per inch (was 0.04)
const BUF_SCALE_LO = 0.025;       // minimum scaling (was 0.02)
const BUF_SCALE_HI = 0.10;        // maximum scaling (was 0.08)
const HW_FLOOR     = 0.015;       // ~1.7 km minimum half-width (was 0.01)
const HW_CAP       = 0.12;        // ~13.3 km maximum half-width (was 0.06)
const HW_SMOOTH_RADIUS = 3;       // moving-avg half-window for width smoothing

// ── Severity bands for multi-row output ──
const HAIL_SIZE_BANDS = [
  { min: 0.50, max: 1.00, label: "0.5–1.0\"" },
  { min: 1.00, max: 1.50, label: "1.0–1.5\"" },
  { min: 1.50, max: 2.00, label: "1.5–2.0\"" },
  { min: 2.00, max: 2.50, label: "2.0–2.5\"" },
  { min: 2.50, max: 99,   label: "2.5\"+"    },
];

// ── Event envelope (outer layer — broader metro-scale footprint) ──
const ENVELOPE_BUFFER_DEG         = 0.25;  // ~28 km outward buffer
const ENVELOPE_MIN_TOP_HAIL       = 1.0;   // only for ≥1″ events
const ENVELOPE_MIN_AREA_SQ_MI     = 20;    // skip trivially small envelopes

function log(t, m) { console.log("[" + t + "] " + m); }
function isWesternSdNeWy(lat, lon) {
  return lat >= REGION_WESTERN_SD_NE_WY.latMin && lat <= REGION_WESTERN_SD_NE_WY.latMax &&
         lon >= REGION_WESTERN_SD_NE_WY.lonMin && lon <= REGION_WESTERN_SD_NE_WY.lonMax;
}
function isNorthDakota(lat, lon) {
  return lat >= REGION_NORTH_DAKOTA.latMin && lat <= REGION_NORTH_DAKOTA.latMax &&
         lon >= REGION_NORTH_DAKOTA.lonMin && lon <= REGION_NORTH_DAKOTA.lonMax;
}
function anyPtInRegion(pts, checkRegionFn) {
  for (const p of pts) {
    const lon = p[0], lat = p[1];
    if (checkRegionFn(lat, lon)) return true;
  }
  return false;
}
function anyPtInWesternSdNeWy(pts) {
  return anyPtInRegion(pts, isWesternSdNeWy);
}
function anyPtInNorthDakota(pts) {
  return anyPtInRegion(pts, isNorthDakota);
}
function regionDebugStage(stage, radar, stormId, lat, lon, area, points, maxHail) {
  if (isWesternSdNeWy(lat, lon)) {
    log("REGION_DEBUG", "western_sd_ne_wy stage=" + stage +
      " radar=" + (radar || "") +
      " stormId=" + (stormId || "") +
      " lat=" + (lat || 0).toFixed(4) +
      " lon=" + (lon || 0).toFixed(4) +
      " area=" + (area || 0).toFixed(1) +
      " points=" + (points || 0) +
      " maxHail=" + (maxHail || 0).toFixed(2));
  }
  if (isNorthDakota(lat, lon)) {
    log("REGION_DEBUG", "north_dakota stage=" + stage +
      " radar=" + (radar || "") +
      " stormId=" + (stormId || "") +
      " lat=" + (lat || 0).toFixed(4) +
      " lon=" + (lon || 0).toFixed(4) +
      " area=" + (area || 0).toFixed(1) +
      " points=" + (points || 0) +
      " maxHail=" + (maxHail || 0).toFixed(2));
  }
}
function regionDebugLine(clusterKey, lat, lon, area, points, maxHail) {
  if (isWesternSdNeWy(lat, lon)) {
    log("REGION_DEBUG", "western_sd_ne_wy candidate cluster=" + clusterKey +
      " lat=" + lat.toFixed(4) +
      " lon=" + lon.toFixed(4) +
      " area=" + area.toFixed(1) +
      " points=" + points +
      " maxHail=" + maxHail.toFixed(2));
  }
  if (isNorthDakota(lat, lon)) {
    log("REGION_DEBUG", "north_dakota candidate cluster=" + clusterKey +
      " lat=" + lat.toFixed(4) +
      " lon=" + lon.toFixed(4) +
      " area=" + area.toFixed(1) +
      " points=" + points +
      " maxHail=" + maxHail.toFixed(2));
  }
}
const sleep = ms => new Promise(r => setTimeout(r, ms));

// Moving-average smoother for half-width arrays
function smoothHalfWidths(hws, radius) {
  if (hws.length <= 2 * radius + 1) return hws;
  const out = [];
  for (let i = 0; i < hws.length; i++) {
    let sum = 0, cnt = 0;
    for (let j = Math.max(0, i - radius); j <= Math.min(hws.length - 1, i + radius); j++) {
      sum += hws[j]; cnt++;
    }
    out.push(sum / cnt);
  }
  return out;
}

// ═══════════════════════════════════════════════════════════
//  Geo helpers
// ═══════════════════════════════════════════════════════════
function segIntersect(ax, ay, bx, by, cx, cy, dx, dy) {
  const d = (bx - ax) * (dy - cy) - (by - ay) * (dx - cx);
  if (Math.abs(d) < 1e-12) return false;
  const t = ((cx - ax) * (dy - cy) - (cy - ay) * (dx - cx)) / d;
  const u = ((cx - ax) * (by - ay) - (cy - ay) * (bx - ax)) / d;
  return t > 0.001 && t < 0.999 && u > 0.001 && u < 0.999;
}

function ringSelfIntersects(ring) {
  const n = ring.length;
  for (let i = 0; i < n - 1; i++) {
    for (let j = i + 2; j < n - 1; j++) {
      if (i === 0 && j === n - 2) continue;
      if (segIntersect(
        ring[i][0], ring[i][1], ring[i + 1][0], ring[i + 1][1],
        ring[j][0], ring[j][1], ring[j + 1][0], ring[j + 1][1]
      )) return true;
    }
  }
  return false;
}

function centroidOf(ring) {
  let sx = 0, sy = 0;
  const n = ring.length - 1;
  for (let i = 0; i < n; i++) { sx += ring[i][0]; sy += ring[i][1]; }
  return { lon: sx / n, lat: sy / n };
}

function bboxOf(pts) {
  let x0 = Infinity, x1 = -Infinity, y0 = Infinity, y1 = -Infinity;
  for (const p of pts) {
    if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0];
    if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1];
  }
  return { minLon: x0, maxLon: x1, minLat: y0, maxLat: y1 };
}

function shoelaceDeg2(ring) {
  let a = 0;
  const n = ring.length - 1;
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n;
    a += ring[i][0] * ring[j][1] - ring[j][0] * ring[i][1];
  }
  return Math.abs(a) / 2;
}

function areaSqMi(ring) {
  const c = centroidOf(ring);
  const cosLat = Math.cos(c.lat * Math.PI / 180);
  return shoelaceDeg2(ring) * 111.32 * 111.32 * cosLat * 0.386102;
}

function convexHull(points) {
  const pts = [...points].sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  if (pts.length <= 2) return [...pts, pts[0]];
  const cross = (O, A, B) =>
    (A[0] - O[0]) * (B[1] - O[1]) - (A[1] - O[1]) * (B[0] - O[0]);
  const lo = [];
  for (const p of pts) {
    while (lo.length >= 2 && cross(lo[lo.length - 2], lo[lo.length - 1], p) <= 0) lo.pop();
    lo.push(p);
  }
  const hi = [];
  for (let i = pts.length - 1; i >= 0; i--) {
    const p = pts[i];
    while (hi.length >= 2 && cross(hi[hi.length - 2], hi[hi.length - 1], p) <= 0) hi.pop();
    hi.push(p);
  }
  lo.pop(); hi.pop();
  const h = lo.concat(hi);
  h.push(h[0]);
  return h;
}

// Expand a convex hull outward from its centroid by bufferDeg
function expandHull(hull, bufferDeg) {
  const c = centroidOf(hull);
  const expanded = [];
  for (let i = 0; i < hull.length - 1; i++) {
    const dx = hull[i][0] - c.lon;
    const dy = hull[i][1] - c.lat;
    const dist = Math.sqrt(dx * dx + dy * dy) || 1e-12;
    expanded.push([
      hull[i][0] + (dx / dist) * bufferDeg,
      hull[i][1] + (dy / dist) * bufferDeg,
    ]);
  }
  expanded.push(expanded[0]);
  return expanded;
}

function trackLengthMi(coords) {
  let total = 0;
  for (let i = 1; i < coords.length; i++) {
    const dLon = coords[i][0] - coords[i - 1][0];
    const dLat = coords[i][1] - coords[i - 1][1];
    const midLat = (coords[i][1] + coords[i - 1][1]) / 2;
    const cosLat = Math.cos(midLat * Math.PI / 180);
    const dx = dLon * 111.32 * cosLat;
    const dy = dLat * 111.32;
    total += Math.sqrt(dx * dx + dy * dy);
  }
  return total * 0.621371;
}

function headingDeg(c1, c2) {
  return (Math.atan2(c2[0] - c1[0], c2[1] - c1[1]) * 180 / Math.PI + 360) % 360;
}

function motionConsistent(coords, maxDev) {
  if (coords.length < 3) return true;
  const overall = headingDeg(coords[0], coords[coords.length - 1]);
  for (let i = 1; i < coords.length; i++) {
    const seg = headingDeg(coords[i - 1], coords[i]);
    let diff = Math.abs(seg - overall);
    if (diff > 180) diff = 360 - diff;
    if (diff > maxDev) return false;
  }
  return true;
}

function corridorBearing(c) {
  return headingDeg(c.firstCoord, c.lastCoord);
}

function degDist(c1, c2) {
  const dLon = c1.centroid.lon - c2.centroid.lon;
  const dLat = c1.centroid.lat - c2.centroid.lat;
  return Math.sqrt(dLon * dLon + dLat * dLat);
}

function bearingDiff(a, b) {
  let d = Math.abs(a - b);
  if (d > 180) d = 360 - d;
  return d;
}

function f2lDistMi(c) {
  const dLon = c.lastCoord[0] - c.firstCoord[0];
  const dLat = c.lastCoord[1] - c.firstCoord[1];
  const mid = (c.firstCoord[1] + c.lastCoord[1]) / 2;
  const cos = Math.cos(mid * Math.PI / 180);
  return Math.sqrt((dLon * 111.32 * cos) ** 2 + (dLat * 111.32) ** 2) * 0.621371;
}

function countNeighbors(c, all) {
  const cBrg = corridorBearing(c);
  const cMid = (new Date(c.firstTime).getTime() + new Date(c.lastTime).getTime()) / 2;
  const windowMs = ISOLATION_TIME_HR * 3600000;
  let n = 0;
  for (const o of all) {
    if (o === c) continue;
    if (degDist(c, o) > ISOLATION_RADIUS_DEG) continue;
    const oMid = (new Date(o.firstTime).getTime() + new Date(o.lastTime).getTime()) / 2;
    if (Math.abs(cMid - oMid) > windowMs) continue;
    if (bearingDiff(cBrg, corridorBearing(o)) > ISOLATION_BEARING_DEG) continue;
    n++;
  }
  return n;
}

function centroidDistMi(c1, c2) {
  const dLon = c1.centroid.lon - c2.centroid.lon;
  const dLat = c1.centroid.lat - c2.centroid.lat;
  const midLat = (c1.centroid.lat + c2.centroid.lat) / 2;
  const cos = Math.cos(midLat * Math.PI / 180);
  return Math.sqrt((dLon * 111.32 * cos) ** 2 + (dLat * 111.32) ** 2) * 0.621371;
}

function dominanceScore(c, neighbors) {
  return (c.nPts * 2) + (c.repMS * 10) + (c.area * 0.1) + (neighbors * 5);
}

// ── Longitude-binned swath outline ──
// Builds an elongated swath outline from track centerline points.
// At each lon bin, uses the median lat ± capped IQR half-spread ± buffer
// to create the northern/southern edges.  The IQR spread is capped at
// maxSpreadDeg per side so the swath stays a reasonable width even when
// many parallel tracks exist.
function buildSwathOutline(trackPoints, bufferDeg, binWidthDeg, maxSpreadDeg) {
  if (trackPoints.length < 3) return convexHull(trackPoints);
  const binW = binWidthDeg || 0.25;
  const maxSpread = maxSpreadDeg || 0.15; // ~17km max IQR half-spread

  // Bin track points by longitude, collect latitudes
  const bins = new Map();
  for (const p of trackPoints) {
    const bi = Math.floor(p[0] / binW);
    if (!bins.has(bi)) bins.set(bi, []);
    bins.get(bi).push(p[1]);
  }

  const sortedKeys = [...bins.keys()].sort((a, b) => a - b);
  if (sortedKeys.length < 2) return convexHull(trackPoints);

  // For each bin, compute median lat and capped IQR-based spread
  const northPts = [];
  const southPts = [];
  for (const k of sortedKeys) {
    const lats = bins.get(k).sort((a, b) => a - b);
    const medLat = lats[Math.floor(lats.length / 2)];
    const q25 = lats[Math.max(0, Math.floor(lats.length * 0.25))];
    const q75 = lats[Math.min(lats.length - 1, Math.ceil(lats.length * 0.75))];
    const iqrHalf = Math.min(Math.max((q75 - q25) / 2, 0), maxSpread);
    const lon = (k + 0.5) * binW;
    northPts.push([lon, medLat + iqrHalf + bufferDeg]);
    southPts.push([lon, medLat - iqrHalf - bufferDeg]);
  }
  southPts.reverse();

  // Rounded end-caps at west and east ends
  const wN = northPts[0], wS = southPts[southPts.length - 1];
  const eN = northPts[northPts.length - 1], eS = southPts[0];
  const capPts = 5;

  function semicap(pA, pB, n) {
    const mx = (pA[0] + pB[0]) / 2, my = (pA[1] + pB[1]) / 2;
    const r = Math.sqrt((pA[0] - mx) ** 2 + (pA[1] - my) ** 2) || 0.01;
    const a0 = Math.atan2(pA[1] - my, pA[0] - mx);
    const a1 = Math.atan2(pB[1] - my, pB[0] - mx);
    let diff = a1 - a0;
    if (diff > Math.PI) diff -= 2 * Math.PI;
    if (diff < -Math.PI) diff += 2 * Math.PI;
    const pts = [];
    for (let ci = 1; ci < n; ci++) {
      const t = ci / n;
      const ang = a0 + t * diff;
      pts.push([mx + r * Math.cos(ang), my + r * Math.sin(ang)]);
    }
    return pts;
  }

  const eastCap = semicap(eN, eS, capPts);
  const westCap = semicap(wS, wN, capPts);

  const ring = [...northPts, ...eastCap, ...southPts, ...westCap, northPts[0]];
  return ring;
}

// ═══════════════════════════════════════════════════════════
//  Corridor construction
// ═══════════════════════════════════════════════════════════
function computePerps(pts) {
  const out = [];
  for (let i = 0; i < pts.length; i++) {
    let dx, dy;
    if (i === 0) {
      dx = pts[1][0] - pts[0][0]; dy = pts[1][1] - pts[0][1];
    } else if (i === pts.length - 1) {
      dx = pts[i][0] - pts[i - 1][0]; dy = pts[i][1] - pts[i - 1][1];
    } else {
      dx = pts[i + 1][0] - pts[i - 1][0]; dy = pts[i + 1][1] - pts[i - 1][1];
    }
    const len = Math.sqrt(dx * dx + dy * dy) || 1e-12;
    out.push([-dy / len, dx / len]);
  }
  return out;
}

function bufferLine(pts, hws) {
  const perps = computePerps(pts);
  const left = [], right = [];
  for (let i = 0; i < pts.length; i++) {
    const hw = hws[i];
    left.push([pts[i][0] + perps[i][0] * hw, pts[i][1] + perps[i][1] * hw]);
    right.push([pts[i][0] - perps[i][0] * hw, pts[i][1] - perps[i][1] * hw]);
  }
  right.reverse();

  // Semicircular end caps — rounds squared-off corridor ends
  const CAP_N = 6;
  function semicap(center, pA, pB) {
    const a0 = Math.atan2(pA[1] - center[1], pA[0] - center[0]);
    let a1 = Math.atan2(pB[1] - center[1], pB[0] - center[0]);
    let diff = a1 - a0;
    if (diff > Math.PI) diff -= 2 * Math.PI;
    if (diff < -Math.PI) diff += 2 * Math.PI;
    // Ensure we sweep the short way (≈ π radians for a semicircle)
    if (Math.abs(diff) < 0.5) {
      diff = diff > 0 ? diff + 2 * Math.PI : diff - 2 * Math.PI;
    }
    const r = Math.sqrt((pA[0] - center[0]) ** 2 + (pA[1] - center[1]) ** 2) || 0.001;
    const cap = [];
    for (let ci = 1; ci < CAP_N; ci++) {
      const t = ci / CAP_N;
      const ang = a0 + t * diff;
      cap.push([center[0] + r * Math.cos(ang), center[1] + r * Math.sin(ang)]);
    }
    return cap;
  }
  // End cap at last track point (between left[last] and right[0])
  const endCap = semicap(pts[pts.length - 1], left[left.length - 1], right[0]);
  // Start cap at first track point (between right[last] and left[0])
  const startCap = semicap(pts[0], right[right.length - 1], left[0]);

  const ring = [...left, ...endCap, ...right, ...startCap, left[0]];
  return ring;
}

// Apply taper to corridor half-widths at both ends so corridors
// narrow smoothly at their start/end instead of cutting off abruptly.
function taperHalfWidths(hws, taperLen, minFrac) {
  if (hws.length < 4) return hws;
  const tLen = Math.min(taperLen, Math.floor(hws.length / 3));
  const out = hws.slice();
  for (let i = 0; i < tLen; i++) {
    const f = minFrac + (1 - minFrac) * (i / tLen);
    out[i] *= f;
    out[out.length - 1 - i] *= f;
  }
  return out;
}

// ═══════════════════════════════════════════════════════════
//  Fetch & filter
// ═══════════════════════════════════════════════════════════
function genTimestamps(dateStr) {
  const base = new Date(dateStr + "T00:00:00Z");
  const out = [];
  for (let h = WIN_START_H; h < WIN_END_H; h++) {
    for (let m = 0; m < 60; m += STEP_MIN) {
      const t = new Date(base.getTime() + (h * 60 + m) * 60000);
      out.push(t.toISOString().replace(".000Z", "Z"));
    }
  }
  return out;
}

async function fetchTs(valid) {
  const url = IEM_URL + "?valid=" + encodeURIComponent(valid);
  try {
    const r = await fetch(url);
    if (!r.ok) return [];
    const gj = await r.json();
    return gj.features || [];
  } catch { return []; }
}

function hasHailSignal(f) {
  const p = f.properties;
  return p.max_size > 0 || p.poh >= 20 || p.posh > 0;
}

function hasWindSignal(f) {
  const p = f.properties;
  // Strong convective indicators → damaging wind potential
  return (p.vil >= 30 && p.max_dbz >= 55 && p.top >= 12) ||
         (p.max_dbz >= 60 && p.vil >= 20) ||
         (p.meso && p.meso !== "NONE" && p.meso !== "0" && p.vil >= 20);
}

// ── Wind severity bands (mapped to same 0.5–2.0 band_min range for frontend) ──
const WIND_SEVERITY_BANDS = [
  { min: 0.50, max: 1.00, label: "wind-light",    dbzFloor: 50 },
  { min: 1.00, max: 1.50, label: "wind-moderate",  dbzFloor: 55 },
  { min: 1.50, max: 2.00, label: "wind-strong",    dbzFloor: 60 },
  { min: 2.00, max: 99,   label: "wind-severe",    dbzFloor: 65 },
];
const WIND_BUF_SCALE  = 0.0020;  // degrees per VIL unit
const WIND_HW_FLOOR   = 0.02;   // ~2.2 km minimum half-width
const WIND_HW_CAP     = 0.08;   // ~9 km maximum half-width
const WIND_MIN_VIL    = 15;     // corridor quality gate: representative VIL
const WIND_MIN_TRACK_PTS = 4;
const WIND_MIN_AREA   = 15;     // sq mi
const WIND_MAX_TIER_AREA = 2000; // sq mi cap per wind tier polygon

// ═══════════════════════════════════════════════════════════
//  Main
// ═══════════════════════════════════════════════════════════
const SCRIPT_VERSION = "v8.0-batch";

async function main(dateArg) {
  TARGET_DATE = dateArg;
  log("MAIN", "IEM NEXRAD track ingest " + SCRIPT_VERSION + " for " + TARGET_DATE);
  log("MAIN", "BASE: BUF_SCALE=" + BUF_SCALE + " HW_FLOOR=" + HW_FLOOR + " HW_CAP=" + HW_CAP);

  // ── 1. Generate query timestamps ──
  const stamps = genTimestamps(TARGET_DATE);
  log("MAIN", "Timestamps to query: " + stamps.length + " (12Z-06Z, 5min)");

  // ── 2. Fetch each timestamp, keep hail-signal and wind-signal features ──
  const allFeats = [];
  const allWindFeats = [];
  let nTs = 0;
  for (const ts of stamps) {
    const raw = await fetchTs(ts);
    const hail = raw.filter(hasHailSignal);
    const wind = raw.filter(hasWindSignal);
    // Region debug: log any raw hail features inside western SD / NE WY box
    for (const f of hail) {
      const coord = f.geometry && f.geometry.coordinates;
      if (!coord) continue;
      const lon = coord[0], lat = coord[1];
      if (isWesternSdNeWy(lat, lon)) {
        regionDebugStage("raw", f.properties.nexrad || "", f.properties.storm_id || "", lat, lon, 0, 1, (f.properties.max_size || 0));
      }
    }
    if (hail.length > 0) allFeats.push(...hail);
    if (wind.length > 0) allWindFeats.push(...wind);
    nTs++;
    if (nTs % 36 === 0) {
      log("FETCH", nTs + "/" + stamps.length + " timestamps  (" + allFeats.length + " hail, " + allWindFeats.length + " wind features so far)");
    }
    await sleep(FETCH_DELAY);
  }
  log("FETCH", "Done: " + nTs + " timestamps queried, " + allFeats.length + " hail + " + allWindFeats.length + " wind features kept");

  if (allFeats.length === 0 && allWindFeats.length === 0) {
    throw new Error("No hail or wind features found for " + TARGET_DATE);
  }

  // ── Wind pipeline runs after hail (see end of main). Skip hail if no hail features. ──
  if (allFeats.length === 0) {
    log("MAIN", "No hail features. Skipping hail pipeline, proceeding to wind.");
  }

  let nSavedHail = 0;
  let hailSwathIdx = 0;

  if (allFeats.length > 0) {
  // ── BEGIN HAIL PIPELINE ──

  // ── 3. Group by nexrad:storm_id ──
  const groups = new Map();
  for (const f of allFeats) {
    const k = f.properties.nexrad + ":" + f.properties.storm_id;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(f);
  }
  log("GROUP", groups.size + " unique nexrad:storm_id groups");

  // ── 4. Sort each group by valid time, split at time gaps or spatial jumps ──
  const tracks = [];
  for (const [key, feats] of groups) {
    feats.sort((a, b) => new Date(a.properties.valid) - new Date(b.properties.valid));
    let chunk = [feats[0]];
    for (let i = 1; i < feats.length; i++) {
      const gap = (new Date(feats[i].properties.valid) - new Date(feats[i - 1].properties.valid)) / 60000;
      const dxy = Math.sqrt(
        (feats[i].geometry.coordinates[0] - feats[i - 1].geometry.coordinates[0]) ** 2 +
        (feats[i].geometry.coordinates[1] - feats[i - 1].geometry.coordinates[1]) ** 2
      );
      if (gap > MAX_GAP_MIN || dxy > SPATIAL_GAP) {
        tracks.push({ key, feats: chunk });
        chunk = [];
      }
      chunk.push(feats[i]);
    }
    if (chunk.length > 0) tracks.push({ key, feats: chunk });
  }
  log("SPLIT", tracks.length + " track segments after time/spatial splitting");

  // ── 5. Sanity-check each track and build corridor polygon ──
  const corridors = [];
  let nRej = 0;

  for (const tr of tracks) {
    const { key, feats } = tr;
    const coords = feats.map(f => f.geometry.coordinates);  // [lon, lat]
    const sizes  = feats.map(f => f.properties.max_size || 0);
    const radarCode = (key.split(":")[0] || "").toUpperCase();
    const isRegionRadar = REGION_RADARS.has(radarCode);
    const anyWesternFeat = anyPtInWesternSdNeWy(coords);
    const anyNorthFeat = anyPtInNorthDakota(coords);
    if (anyWesternFeat || anyNorthFeat) {
      const first = coords[0];
      regionDebugStage("pre_sanity", radarCode, key, first[1], first[0], 0, feats.length, Math.max(...sizes));
    }

    // 5a — minimum track points
    if (feats.length < MIN_PTS) {
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + key + ": minPts " + feats.length);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=pre_sanity reason=minPts radar=" + radarCode + " stormId=" + key + " points=" + feats.length);
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=pre_sanity reason=minPts radar=" + radarCode + " stormId=" + key + " points=" + feats.length);
      nRej++; continue;
    }

    // 5b — bounding-box span
    const bb = bboxOf(coords);
    if (bb.maxLat - bb.minLat > MAX_BBOX_LAT || bb.maxLon - bb.minLon > MAX_BBOX_LON) {
      const msg = key + ": bbox " + (bb.maxLat - bb.minLat).toFixed(1) + "° x " +
        (bb.maxLon - bb.minLon).toFixed(1) + "°";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=pre_sanity reason=bbox radar=" + radarCode + " stormId=" + key + " lat=" + ((bb.maxLat+bb.minLat)/2).toFixed(4) + " lon=" + ((bb.maxLon+bb.minLon)/2).toFixed(4));
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=pre_sanity reason=bbox radar=" + radarCode + " stormId=" + key + " lat=" + ((bb.maxLat+bb.minLat)/2).toFixed(4) + " lon=" + ((bb.maxLon+bb.minLon)/2).toFixed(4));
      nRej++; continue;
    }

    // 5c — consecutive jump
    let jumpBad = false;
    for (let i = 1; i < coords.length; i++) {
      const d = Math.sqrt(
        (coords[i][0] - coords[i - 1][0]) ** 2 +
        (coords[i][1] - coords[i - 1][1]) ** 2
      );
      if (d > MAX_JUMP) {
        log("REJECT", key + ": jump " + d.toFixed(3) + "° at step " + i);
        jumpBad = true; break;
      }
    }
    if (jumpBad) {
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + key + ": jump " + jumpBad);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=pre_sanity reason=jump radar=" + radarCode + " stormId=" + key + " points=" + feats.length);
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=pre_sanity reason=jump radar=" + radarCode + " stormId=" + key + " points=" + feats.length);
      nRej++; continue;
    }

    // 5d — deduplicate consecutive identical coords
    const uP = [coords[0]], uS = [sizes[0]];
    for (let i = 1; i < coords.length; i++) {
      if (coords[i][0] !== coords[i - 1][0] || coords[i][1] !== coords[i - 1][1]) {
        uP.push(coords[i]); uS.push(sizes[i]);
      }
    }
    if (uP.length < 2) {
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=dedup reason=dedup_to_single_point radar=" + radarCode + " stormId=" + key + " points=" + uP.length);
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=dedup reason=dedup_to_single_point radar=" + radarCode + " stormId=" + key + " points=" + uP.length);
      nRej++; continue; }

    // 5e — half-widths from max_size
    const nonZero = uS.filter(s => s > 0);
    const repMS = nonZero.length > 0 ? Math.max(...nonZero) : 0.5;
    const rawHws = uS.map(ms => {
      const eff = ms > 0 ? ms : repMS * 0.5;
      let hw = eff * BUF_SCALE;
      hw = Math.max(hw, eff * BUF_SCALE_LO);
      hw = Math.min(hw, eff * BUF_SCALE_HI);
      return Math.min(Math.max(hw, HW_FLOOR), HW_CAP);
    });
    const hws = smoothHalfWidths(rawHws, HW_SMOOTH_RADIUS);

    // 5e2 — per-point severity (max_size values, smoothed)
    const rawSev = uS.map(ms => ms > 0 ? ms : 0);
    const smoothedSev = smoothHalfWidths(rawSev, HW_SMOOTH_RADIUS);

    // 5f — build polygon
    let ring = bufferLine(uP, hws);
    let selfInt = ringSelfIntersects(ring);
    let usedHull = false;
    if (selfInt) {
      const trackSpan = Math.max(bb.maxLon - bb.minLon, bb.maxLat - bb.minLat);
      if (trackSpan > HULL_SPAN_CAP) {
        log("REJECT", key + ": hull fallback on wide track (" + trackSpan.toFixed(2) + "°)");
          if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=pre_sanity reason=hull_fallback_wide_track radar=" + radarCode + " stormId=" + key);
          if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=pre_sanity reason=hull_fallback_wide_track radar=" + radarCode + " stormId=" + key);
        nRej++; continue;
      }
      ring = convexHull(ring.slice(0, -1));
      selfInt = ringSelfIntersects(ring);
      if (selfInt) {
        log("REJECT", key + ": self-intersecting after hull fallback");
        nRej++; continue;
      }
      usedHull = true;
    }

    // 5g — polygon-level bbox check
    const pbb = bboxOf(ring);
    if (pbb.maxLat - pbb.minLat > MAX_BBOX_LAT || pbb.maxLon - pbb.minLon > MAX_BBOX_LON) {
      const msg = key + ": polygon bbox too large";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=post_corridor reason=poly_bbox_too_large radar=" + radarCode + " stormId=" + key + " lat=" + ((pbb.maxLat+pbb.minLat)/2).toFixed(4) + " lon=" + ((pbb.maxLon+pbb.minLon)/2).toFixed(4));
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=post_corridor reason=poly_bbox_too_large radar=" + radarCode + " stormId=" + key + " lat=" + ((pbb.maxLat+pbb.minLat)/2).toFixed(4) + " lon=" + ((pbb.maxLon+pbb.minLon)/2).toFixed(4));
      nRej++; continue;
    }

    const area = areaSqMi(ring);

    // 5h — area cap
    if (area > MAX_AREA) {
      const msg = key + ": area " + area.toFixed(0) + " mi² > cap";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=post_corridor reason=area_exceeds_cap radar=" + radarCode + " stormId=" + key + " area=" + area.toFixed(1));
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=post_corridor reason=area_exceeds_cap radar=" + radarCode + " stormId=" + key + " area=" + area.toFixed(1));
      nRej++; continue;
    }

    // 5i — first-to-last distance cap
    const fLon = uP[0][0], fLat = uP[0][1];
    const lLon = uP[uP.length - 1][0], lLat = uP[uP.length - 1][1];
    const midLat2 = (fLat + lLat) / 2;
    const cosL2 = Math.cos(midLat2 * Math.PI / 180);
    const f2lMi = Math.sqrt(((lLon - fLon) * 111.32 * cosL2) ** 2 + ((lLat - fLat) * 111.32) ** 2) * 0.621371;
    if (f2lMi > MAX_TRACK_MI) {
      const msg = key + ": first-to-last " + f2lMi.toFixed(0) + " mi > cap";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=post_corridor reason=first_to_last_exceeds radar=" + radarCode + " stormId=" + key + " f2lMi=" + f2lMi.toFixed(0));
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=post_corridor reason=first_to_last_exceeds radar=" + radarCode + " stormId=" + key + " f2lMi=" + f2lMi.toFixed(0));
      nRej++; continue;
    }

    // 5j — minimum ring points
    if (ring.length < MIN_RING_PTS) {
      const msg = key + ": ring only " + ring.length + " pts";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=post_corridor reason=ring_too_few_pts radar=" + radarCode + " stormId=" + key + " pts=" + ring.length);
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=post_corridor reason=ring_too_few_pts radar=" + radarCode + " stormId=" + key + " pts=" + ring.length);
      nRej++; continue;
    }

    // 5k — convex hull bbox vs track bbox ratio
    const hullRing = convexHull(ring.slice(0, -1));
    const hbb = bboxOf(hullRing);
    const trackW = (bb.maxLon - bb.minLon) || 0.001;
    const trackH = (bb.maxLat - bb.minLat) || 0.001;
    if ((hbb.maxLon - hbb.minLon) / trackW > HULL_BBOX_RATIO ||
        (hbb.maxLat - hbb.minLat) / trackH > HULL_BBOX_RATIO) {
      const msg = key + ": hull bbox ratio exceeded";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=post_corridor reason=hull_bbox_ratio radar=" + radarCode + " stormId=" + key);
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=post_corridor reason=hull_bbox_ratio radar=" + radarCode + " stormId=" + key);
      nRej++; continue;
    }

    // 5l — motion direction consistency
    if (!motionConsistent(uP, MAX_DIR_DEV)) {
      const msg = key + ": heading deviation > " + MAX_DIR_DEV + "°";
      log("REJECT", msg);
      if (isRegionRadar) log("RADAR_REJECT", radarCode + " " + msg);
      if (anyWesternFeat) log("REGION_DROP", "western_sd_ne_wy stage=post_corridor reason=motion_inconsistent radar=" + radarCode + " stormId=" + key);
      if (anyNorthFeat) log("REGION_DROP", "north_dakota stage=post_corridor reason=motion_inconsistent radar=" + radarCode + " stormId=" + key);
      nRej++; continue;
    }

    const centroid = centroidOf(ring);
    regionDebugLine(key, centroid.lat, centroid.lon, area, feats.length, repMS);

    corridors.push({
      key,
      ring,
      centroid: centroidOf(ring),
      repMS,
      nPts: feats.length,
      firstTime: feats[0].properties.valid,
      lastTime: feats[feats.length - 1].properties.valid,
      firstCoord: coords[0],
      lastCoord: coords[coords.length - 1],
      bbox: pbb,
      selfInt,
      usedHull,
      area,
      // Retained for severity-band generation
      _trackPts: uP,
      _trackHws: hws,
      _trackSev: smoothedSev,
    });
  }

  log("MAIN", "Corridors built: " + corridors.length + "  |  rejected: " + nRej);

  if (!corridors.length) {
    log("MAIN", "No corridors to save.");
    return;
  }

  // ── 5.5 Cross-radar deduplication ──
  corridors.sort((a, b) => b.nPts - a.nPts || b.area - a.area);
  const deduped = [];
  for (const c of corridors) {
    const cRadar = c.key.split(":")[0];
    const cStart = new Date(c.firstTime).getTime();
    const cEnd   = new Date(c.lastTime).getTime();
    const isDup = deduped.some(k => {
      if (k.key.split(":")[0] === cRadar) return false;
      const dLon = c.centroid.lon - k.centroid.lon;
      const dLat = c.centroid.lat - k.centroid.lat;
      if (Math.sqrt(dLon * dLon + dLat * dLat) > DEDUP_DIST) return false;
      const kStart = new Date(k.firstTime).getTime();
      const kEnd   = new Date(k.lastTime).getTime();
      const oStart = Math.max(cStart, kStart);
      const oEnd   = Math.min(cEnd, kEnd);
      if (oEnd <= oStart) return false;
      const overlap = oEnd - oStart;
      const minDur  = Math.min(cEnd - cStart, kEnd - kStart) || 1;
      return overlap / minDur >= DEDUP_TIME_OVERLAP;
    });
    if (!isDup) deduped.push(c);
    else {
      const reg = isWesternSdNeWy(c.centroid.lat, c.centroid.lon) ? 'western_sd_ne_wy' : (isNorthDakota(c.centroid.lat, c.centroid.lon) ? 'north_dakota' : null);
      if (reg) log("REGION_DROP", reg + " stage=dedup reason=duplicate radar=" + cRadar + " stormId=" + c.key + " lat=" + c.centroid.lat.toFixed(4) + " lon=" + c.centroid.lon.toFixed(4) + " points=" + c.nPts + " area=" + (c.area||0).toFixed(1));
    }
  }
  log("DEDUP", "After cross-radar dedup: " + deduped.length +
    " (removed " + (corridors.length - deduped.length) + ")");

  // Region debug: list deduped corridors inside the boxes
  for (const c of deduped) {
    regionDebugStage("post_dedup", c.key.split(":")[0], c.key, c.centroid.lat, c.centroid.lon, c.area || 0, c.nPts, c.repMS || 0);
  }

  if (!deduped.length) {
    log("MAIN", "No corridors after dedup.");
    return;
  }

  // ── 5.6 Quality filters ──
  const rejStats = { minPts: 0, minHail: 0, area: 0, isolation: 0, clusterPrune: 0 };
  let qualified = [];

  for (const c of deduped) {
    const withinWesternRegion = isWesternSdNeWy(c.centroid.lat, c.centroid.lon);
      const withinNorthDakotaRegion = isNorthDakota(c.centroid.lat, c.centroid.lon);
      const withinRegion = withinWesternRegion || withinNorthDakotaRegion;
      const regionName = withinNorthDakotaRegion ? 'north_dakota' : 'western_sd_ne_wy';
    if (c.nPts < MIN_TRACK_POINTS) {
      if (withinRegion) log("REGION_DROP", regionName + " reason=minPts cluster=" + c.key +
        " lat=" + c.centroid.lat.toFixed(4) +
        " lon=" + c.centroid.lon.toFixed(4) +
        " points=" + c.nPts + " maxHail=" + c.repMS.toFixed(2));
      rejStats.minPts++; continue;
    }
    if (c.repMS < MIN_HAIL_INCHES) {
      if (withinRegion) log("REGION_DROP", regionName + " reason=minHail cluster=" + c.key +
        " lat=" + c.centroid.lat.toFixed(4) +
        " lon=" + c.centroid.lon.toFixed(4) +
        " maxHail=" + c.repMS.toFixed(2));
      rejStats.minHail++; continue;
    }
    if (c.area < MIN_AREA_SQ_MI && c.nPts < MIN_AREA_SOFT_PTS) {
      if (withinRegion) log("REGION_DROP", regionName + " reason=area cluster=" + c.key +
        " lat=" + c.centroid.lat.toFixed(4) +
        " lon=" + c.centroid.lon.toFixed(4) +
        " area=" + c.area.toFixed(1) + " points=" + c.nPts);
      rejStats.area++; continue;
    }
    if (withinRegion) {
      regionDebugLine(c.key, c.centroid.lat, c.centroid.lon, c.area, c.nPts, c.repMS);
    }
    qualified.push(c);
  }
  log("FILTER", "After quality gates: " + qualified.length +
    " (minPts=" + rejStats.minPts + " minHail=" + rejStats.minHail + " area=" + rejStats.area + ")");

  // Region debug: candidates that passed quality gates
  for (const c of qualified) {
    regionDebugStage("post_quality", c.key.split(":")[0], c.key, c.centroid.lat, c.centroid.lon, c.area || 0, c.nPts, c.repMS || 0);
  }

  // ── 5.7 Isolation filter — reject corridors with too few neighbors ──
  const withNeighbors = [];
  for (const c of qualified) {
    const nb = countNeighbors(c, qualified);
    c._neighbors = nb;
    const withinWesternRegion = isWesternSdNeWy(c.centroid.lat, c.centroid.lon);
      const withinNorthDakotaRegion = isNorthDakota(c.centroid.lat, c.centroid.lon);
      const withinRegion = withinWesternRegion || withinNorthDakotaRegion;
      const regionName = withinNorthDakotaRegion ? 'north_dakota' : 'western_sd_ne_wy';
    if (nb < MIN_NEIGHBORS) {
      if (withinRegion) log("REGION_DROP", regionName + " reason=isolation cluster=" + c.key +
        " lat=" + c.centroid.lat.toFixed(4) +
        " lon=" + c.centroid.lon.toFixed(4) +
        " neighbors=" + nb);
      rejStats.isolation++; continue;
    }
    withNeighbors.push(c);
  }
  qualified = withNeighbors;
  log("FILTER", "After isolation filter: " + qualified.length +
    " (isolated=" + rejStats.isolation + ")");

  // Region debug: candidates that passed isolation
  for (const c of qualified) {
    regionDebugStage("post_isolation", c.key.split(":")[0], c.key, c.centroid.lat, c.centroid.lon, c.area || 0, c.nPts, c.repMS || 0);
  }

  // ── 5.8 Score all candidates ──
  for (const c of qualified) {
    c._score = dominanceScore(c, c._neighbors);
    c._bearing = corridorBearing(c);
    c._midMs = (new Date(c.firstTime).getTime() + new Date(c.lastTime).getTime()) / 2;
  }
  qualified.sort((a, b) => b._score - a._score);
  const preClusterCount = qualified.length;

  // ── 5.9 Regional cluster pruning ──
  // Union-find to group corridors by proximity + motion + time
  const clusterOf = new Map();  // corridor → cluster id
  let nextCluster = 0;
  for (const c of qualified) clusterOf.set(c, -1);

  function findRoot(c) {
    let r = clusterOf.get(c);
    if (r === -1) return c;
    // path compression via iteration
    const chain = [c];
    while (clusterOf.get(r) !== -1) { chain.push(r); r = clusterOf.get(r); }
    for (const x of chain) clusterOf.set(x, r === x ? -1 : r);
    return r;
  }
  function union(a, b) {
    const ra = findRoot(a), rb = findRoot(b);
    if (ra === rb) return;
    clusterOf.set(rb, ra);  // merge b's root into a's root
  }

  const qArr = [...qualified];
  for (let i = 0; i < qArr.length; i++) {
    for (let j = i + 1; j < qArr.length; j++) {
      const a = qArr[i], b = qArr[j];
      if (centroidDistMi(a, b) > CLUSTER_RADIUS_MI) continue;
      if (bearingDiff(a._bearing, b._bearing) > CLUSTER_BEARING_TOLERANCE_DEG) continue;
      // time overlap or within 2 hours
      const aStart = new Date(a.firstTime).getTime(), aEnd = new Date(a.lastTime).getTime();
      const bStart = new Date(b.firstTime).getTime(), bEnd = new Date(b.lastTime).getTime();
      const gap = Math.max(aStart, bStart) - Math.min(aEnd, bEnd);
      if (gap > 2 * 3600000) continue;  // >2hr gap, not same cluster
      union(a, b);
    }
  }

  // Collect clusters
  const clusterMap = new Map();  // root → [corridors]
  for (const c of qualified) {
    const root = findRoot(c);
    if (!clusterMap.has(root)) clusterMap.set(root, []);
    clusterMap.get(root).push(c);
  }
  // Sort each cluster by score desc (already sorted globally, but be safe)
  for (const members of clusterMap.values()) {
    members.sort((a, b) => b._score - a._score);
  }

  // ── 5.9a Linear system bridging ──
  // After initial clustering, bridge clusters that share similar motion
  // bearing and are arranged along a common storm line. This prevents
  // long linear storm events from fragmenting into isolated clusters.
  // Uses nearest-member distance (not centroid-to-centroid) so elongated
  // clusters can bridge across their closest edges.
  const LINEAR_BRIDGE_DIST_MI     = 200;   // max nearest-member distance between clusters
  const LINEAR_BRIDGE_BEARING_TOL = 35;    // motion bearing similarity tolerance
  const LINEAR_BRIDGE_ALIGN_TOL   = 75;    // inter-cluster angle vs motion tolerance
  const LINEAR_BRIDGE_TIME_GAP_HR = 4;     // max time gap between clusters
  const LINEAR_KEEP_PER_BRIDGE    = 6;     // extra keep slots per merged sub-cluster

  // Save original cluster assignment before bridging
  const _origClusterOf = new Map();
  for (const c of qualified) _origClusterOf.set(c, findRoot(c));

  // Compute per-cluster metadata for bridging decisions
  const _bridgeMeta = new Map();
  for (const [root, members] of clusterMap) {
    let latSum = 0, lonSum = 0, tMin = Infinity, tMax = -Infinity;
    for (const c of members) {
      latSum += c.centroid.lat; lonSum += c.centroid.lon;
      const t0 = new Date(c.firstTime).getTime();
      const t1 = new Date(c.lastTime).getTime();
      if (t0 < tMin) tMin = t0;
      if (t1 > tMax) tMax = t1;
    }
    const n = members.length;
    _bridgeMeta.set(root, {
      centroid: { lat: latSum / n, lon: lonSum / n },
      bearing: members[0]._bearing,
      tMin, tMax, members,
    });
  }

  // Try bridging cluster pairs using nearest-member distance
  const _bridgeRoots = [...clusterMap.keys()];
  let _bridgeCount = 0;
  // Diagnostic: log each cluster's bearing and centroid for debugging
  for (const _br of _bridgeRoots) {
    const _bm = _bridgeMeta.get(_br);
    log("BRIDGE_DIAG", "cluster root=" + _bm.members[0].key +
      " centroid=[" + _bm.centroid.lat.toFixed(2) + "," + _bm.centroid.lon.toFixed(2) + "]" +
      " bearing=" + _bm.bearing.toFixed(1) + "° members=" + _bm.members.length);
  }
  for (let _bi = 0; _bi < _bridgeRoots.length; _bi++) {
    for (let _bj = _bi + 1; _bj < _bridgeRoots.length; _bj++) {
      const mA = _bridgeMeta.get(_bridgeRoots[_bi]);
      const mB = _bridgeMeta.get(_bridgeRoots[_bj]);
      // Motion bearing similarity
      const _brgDiff = bearingDiff(mA.bearing, mB.bearing);
      if (_brgDiff > LINEAR_BRIDGE_BEARING_TOL) {
        log("BRIDGE_SKIP", mA.members[0].key + " ↔ " + mB.members[0].key +
          " reason=bearing_diff " + _brgDiff.toFixed(0) + "° > " + LINEAR_BRIDGE_BEARING_TOL + "°");
        continue;
      }
      // Nearest-member distance (closest corridor pair across the two clusters)
      let nearestDistMi = Infinity;
      for (const cA of mA.members) {
        for (const cB of mB.members) {
          const d = centroidDistMi(cA, cB);
          if (d < nearestDistMi) nearestDistMi = d;
        }
      }
      if (nearestDistMi > LINEAR_BRIDGE_DIST_MI) {
        log("BRIDGE_SKIP", mA.members[0].key + " ↔ " + mB.members[0].key +
          " reason=nearest_dist " + nearestDistMi.toFixed(0) + "mi > " + LINEAR_BRIDGE_DIST_MI + "mi");
        continue;
      }
      // Inter-cluster bearing alignment with storm motion (centroid-to-centroid)
      const interBrg = (Math.atan2(mB.centroid.lon - mA.centroid.lon,
        mB.centroid.lat - mA.centroid.lat) * 180 / Math.PI + 360) % 360;
      const avgBrg = (mA.bearing + mB.bearing) / 2;
      const _alignDiff1 = bearingDiff(interBrg, avgBrg);
      const _alignDiff2 = bearingDiff((interBrg + 180) % 360, avgBrg);
      if (_alignDiff1 > LINEAR_BRIDGE_ALIGN_TOL &&
          _alignDiff2 > LINEAR_BRIDGE_ALIGN_TOL) {
        log("BRIDGE_SKIP", mA.members[0].key + " ↔ " + mB.members[0].key +
          " reason=align interBrg=" + interBrg.toFixed(0) + "° avgMotion=" + avgBrg.toFixed(0) +
          "° diffs=" + _alignDiff1.toFixed(0) + "/" + _alignDiff2.toFixed(0) + "° > " + LINEAR_BRIDGE_ALIGN_TOL + "°");
        continue;
      }
      // Time gap
      const tGap = Math.max(mA.tMin, mB.tMin) - Math.min(mA.tMax, mB.tMax);
      if (tGap > LINEAR_BRIDGE_TIME_GAP_HR * 3600000) {
        log("BRIDGE_SKIP", mA.members[0].key + " ↔ " + mB.members[0].key +
          " reason=time_gap " + (tGap / 3600000).toFixed(1) + "hr > " + LINEAR_BRIDGE_TIME_GAP_HR + "hr");
        continue;
      }
      // Bridge: merge via union-find
      union(mA.members[0], mB.members[0]);
      _bridgeCount++;
      log("BRIDGE", "Merged clusters [" +
        mA.centroid.lat.toFixed(2) + "," + mA.centroid.lon.toFixed(2) + "] ↔ [" +
        mB.centroid.lat.toFixed(2) + "," + mB.centroid.lon.toFixed(2) + "]" +
        " nearestDist=" + nearestDistMi.toFixed(0) + "mi brgDiff=" +
        bearingDiff(mA.bearing, mB.bearing).toFixed(0) + "°");
    }
  }

  // Rebuild clusterMap if any bridges were made
  const _mergedSubCounts = new Map();
  if (_bridgeCount > 0) {
    const _preSize = clusterMap.size;
    clusterMap.clear();
    for (const c of qualified) {
      const root = findRoot(c);
      if (!clusterMap.has(root)) clusterMap.set(root, []);
      clusterMap.get(root).push(c);
    }
    for (const members of clusterMap.values()) {
      members.sort((a, b) => b._score - a._score);
    }
    log("BRIDGE", "Clusters: " + _preSize + " → " + clusterMap.size + " after linear bridging");

    // ── 5.9b Post-bridge bearing validation ──
    // Transitive bridging can pull in sub-clusters whose bearing is compatible
    // with an intermediate hop but diverges too far from the dominant system.
    // For each merged cluster, find the largest original sub-cluster (by member
    // count) and use its bearing as the reference. Remove any sub-cluster whose
    // bearing deviates more than LINEAR_BRIDGE_BEARING_TOL from the dominant.
    const _ejected = [];
    for (const [root, members] of clusterMap) {
      // Group members by their original cluster root
      const subGroups = new Map();
      for (const c of members) {
        const orig = _origClusterOf.get(c);
        if (!subGroups.has(orig)) subGroups.set(orig, []);
        subGroups.get(orig).push(c);
      }
      if (subGroups.size < 2) continue; // nothing to validate

      // Find dominant sub-cluster (most members)
      let domOrig = null, domSize = 0, domBrg = 0;
      for (const [orig, subs] of subGroups) {
        if (subs.length > domSize) {
          domOrig = orig; domSize = subs.length;
          domBrg = _bridgeMeta.get(orig) ? _bridgeMeta.get(orig).bearing : subs[0]._bearing;
        }
      }

      // Check each non-dominant sub-cluster
      for (const [orig, subs] of subGroups) {
        if (orig === domOrig) continue;
        const subBrg = _bridgeMeta.get(orig) ? _bridgeMeta.get(orig).bearing : subs[0]._bearing;
        const bDiff = bearingDiff(subBrg, domBrg);
        if (bDiff > LINEAR_BRIDGE_BEARING_TOL) {
          log("BRIDGE_EJECT", "Sub-cluster " + subs[0].key +
            " (bearing=" + subBrg.toFixed(1) + "° n=" + subs.length + ")" +
            " ejected from merged cluster (dominant=" + domBrg.toFixed(1) +
            "° diff=" + bDiff.toFixed(1) + "° > " + LINEAR_BRIDGE_BEARING_TOL + "°)");
          _ejected.push(...subs);
        }
      }
    }
    // Rebuild clusterMap if any sub-clusters were ejected
    if (_ejected.length > 0) {
      // Log region-specific drops for any ejected corridors
      for (const ex of _ejected) {
        if (ex.centroid) {
          if (isWesternSdNeWy(ex.centroid.lat, ex.centroid.lon)) {
            log("REGION_DROP", "western_sd_ne_wy stage=bridge_eject reason=bearing_mismatch radar=" + ex.key.split(":")[0] + " stormId=" + ex.key + " lat=" + ex.centroid.lat.toFixed(4) + " lon=" + ex.centroid.lon.toFixed(4) + " points=" + ex.nPts + " area=" + (ex.area||0).toFixed(1));
          }
          if (isNorthDakota(ex.centroid.lat, ex.centroid.lon)) {
            log("REGION_DROP", "north_dakota stage=bridge_eject reason=bearing_mismatch radar=" + ex.key.split(":")[0] + " stormId=" + ex.key + " lat=" + ex.centroid.lat.toFixed(4) + " lon=" + ex.centroid.lon.toFixed(4) + " points=" + ex.nPts + " area=" + (ex.area||0).toFixed(1));
          }
        }
      }
      const ejectedSet = new Set(_ejected);
      clusterMap.clear();
      for (const c of qualified) {
        if (ejectedSet.has(c)) {
          // Re-assign ejected corridors to their original cluster root
          const orig = _origClusterOf.get(c);
          if (!clusterMap.has(orig)) clusterMap.set(orig, []);
          clusterMap.get(orig).push(c);
        } else {
          const root = findRoot(c);
          if (!clusterMap.has(root)) clusterMap.set(root, []);
          clusterMap.get(root).push(c);
        }
      }
      for (const members of clusterMap.values()) {
        members.sort((a, b) => b._score - a._score);
      }
      log("BRIDGE", "After bearing validation: " + clusterMap.size + " clusters (ejected " + _ejected.length + " corridors)");
    }
  }
  // Count how many original sub-clusters each merged cluster contains
  for (const [root, members] of clusterMap) {
    const origRoots = new Set();
    for (const c of members) origRoots.add(_origClusterOf.get(c));
    _mergedSubCounts.set(root, origRoots.size);
  }

  const final = [];
  let clusterDropped = 0;
  const clusterDebug = [];
  const LINE_LAT_PRUNE_TOL = 3.5;   // max lat deviation from cluster median for merged clusters
  const LINE_GAP_FILL_DEG  = 3.0;   // lon gap threshold that triggers gap-fill
  const LINE_GAP_FILL_MAX  = 5;     // max gap-filler corridors per gap

  for (const [root, members] of clusterMap) {
    const domBearing = members[0]._bearing;
    const kept = [];
    let keptByScore = 0;
    let keptByContinuity = 0;
    let keptByGapFill = 0;
    let dropped = 0;
    let continuityExtras = 0;
    let latOutliers = 0;

    const subClusters = _mergedSubCounts.get(root) || 1;
    const isMerged = subClusters >= 2;

    // ── 5.10a Lat outlier pruning for merged clusters ──
    // Remove N/S stray corridors that are far from the cluster's median latitude.
    // This prevents distant fragments (e.g., Oklahoma lat 33) from eating keep
    // slots that should go to gap-filling corridors on the main E-W line.
    let effectiveMembers = members;
    let clusterMedianLat = 0;
    if (isMerged && members.length > 2) {
      const sortedLats = members.map(c => c.centroid.lat).sort((a, b) => a - b);
      clusterMedianLat = sortedLats[Math.floor(sortedLats.length / 2)];
      effectiveMembers = members.filter(c =>
        Math.abs(c.centroid.lat - clusterMedianLat) <= LINE_LAT_PRUNE_TOL ||
        isWesternSdNeWy(c.centroid.lat, c.centroid.lon) ||
        isNorthDakota(c.centroid.lat, c.centroid.lon)
      );
      latOutliers = members.length - effectiveMembers.length;
      effectiveMembers.sort((a, b) => b._score - a._score);
      if (latOutliers > 0) {
        log("LINE_PRUNE", "Cluster " + members[0].key +
          ": removed " + latOutliers + " lat outliers (medLat=" +
          clusterMedianLat.toFixed(1) + " ±" + LINE_LAT_PRUNE_TOL + "°)" +
          " remaining=" + effectiveMembers.length);
      }
    }

    const topScore = effectiveMembers.length ? effectiveMembers[0]._score : members[0]._score;
    const _effectiveMaxKeep = MAX_KEEP_PER_CLUSTER +
      (subClusters - 1) * LINEAR_KEEP_PER_BRIDGE;

    for (const c of effectiveMembers) {
      const withinWesternRegion = isWesternSdNeWy(c.centroid.lat, c.centroid.lon);
      const withinNorthDakotaRegion = isNorthDakota(c.centroid.lat, c.centroid.lon);
      const withinRegion = withinWesternRegion || withinNorthDakotaRegion;
      const regionName = withinNorthDakotaRegion ? 'north_dakota' : 'western_sd_ne_wy';
      if (kept.length >= _effectiveMaxKeep) {
        if (withinRegion) log("REGION_DROP", regionName + " reason=cluster_keep_limit cluster=" + c.key +
          " lat=" + c.centroid.lat.toFixed(4) +
          " lon=" + c.centroid.lon.toFixed(4) +
          " score=" + c._score.toFixed(1) +
          " topScore=" + topScore.toFixed(1));
        dropped++; clusterDropped++; continue;
      }

      // Normal score-ratio pass
      if (c._score >= topScore * MIN_CLUSTER_SCORE_RATIO ||
          (withinRegion && c._score >= topScore * 0.10)) {
        if (withinRegion) log("REGION_KEEP", regionName + " cluster=" + c.key +
          " lat=" + c.centroid.lat.toFixed(4) +
          " lon=" + c.centroid.lon.toFixed(4) +
          " score=" + c._score.toFixed(1) + " topScore=" + topScore.toFixed(1));
        kept.push(c); final.push(c); keptByScore++; continue;
      }

      // Continuity exception
      if (continuityExtras < CONTINUITY_MAX_EXTRA) {
        const nearestDist = kept.length ? Math.min(...kept.map(k => centroidDistMi(c, k))) : Infinity;
        const cStart = new Date(c.firstTime).getTime(), cEnd = new Date(c.lastTime).getTime();
        const timeOk = kept.some(k => {
          const kS = new Date(k.firstTime).getTime(), kE = new Date(k.lastTime).getTime();
          const gap = Math.max(cStart, kS) - Math.min(cEnd, kE);
          return gap <= 2 * 3600000;
        });
        if (nearestDist > CONTINUITY_MIN_DIST_MI &&
            bearingDiff(c._bearing, domBearing) <= CONTINUITY_BEARING_TOL_DEG &&
            timeOk) {
          kept.push(c); final.push(c); keptByContinuity++; continuityExtras++; continue;
        }
      }

      dropped++; clusterDropped++;
    }

    // ── 5.10b Longitude gap-fill for merged clusters ──
    // After score-based pruning, scan the kept set for large longitude gaps.
    // Fill gaps by pulling in the highest-scoring corridor(s) from the gap
    // region, even if they didn't make the score cutoff. This ensures the
    // E-W storm line stays connected instead of fragmenting.
    // Runs iteratively: after filling a gap, re-scan for remaining sub-gaps.
    if (isMerged && kept.length > 1) {
      let gapPassCount = 0;
      let gapChanged = true;
      while (gapChanged && gapPassCount < 5) {
        gapChanged = false;
        gapPassCount++;
        const byLon = kept.slice().sort((a, b) => a.centroid.lon - b.centroid.lon);
        for (let gi = 0; gi < byLon.length - 1; gi++) {
          const gapDeg = byLon[gi + 1].centroid.lon - byLon[gi].centroid.lon;
          if (gapDeg < LINE_GAP_FILL_DEG) continue;
          const gapMinLon = byLon[gi].centroid.lon;
          const gapMaxLon = byLon[gi + 1].centroid.lon;
          // Find gap-filler candidates from the lat-filtered member set
          const gapCandidates = effectiveMembers.filter(c =>
            !kept.includes(c) &&
            c.centroid.lon > gapMinLon && c.centroid.lon < gapMaxLon
          ).sort((a, b) => b._score - a._score).slice(0, LINE_GAP_FILL_MAX);
          for (const filler of gapCandidates) {
            kept.push(filler);
            final.push(filler);
            keptByGapFill++;
          }
          if (gapCandidates.length) {
            gapChanged = true;
            log("GAP_FILL", "Pass " + gapPassCount + ": filled " + gapDeg.toFixed(1) +
              "° gap [" + gapMinLon.toFixed(1) + " → " + gapMaxLon.toFixed(1) + "] with " +
              gapCandidates.length + " corridor(s): " +
              gapCandidates.map(c => c.key + " [" + c.centroid.lat.toFixed(2) + "," +
                c.centroid.lon.toFixed(2) + "]").join(", "));
          }
        }
      }
    }

    // Tag kept corridors with their cluster root for outlier filter
    const _clusterRootKey = members[0].key;
    for (const c of kept) c._pruneCluster = _clusterRootKey;

    clusterDebug.push({
      top: members[0].key,
      topScore: members[0]._score,
      topPts: members[0].nPts,
      topArea: members[0].area,
      topHail: members[0].repMS,
      topCentroid: members[0].centroid,
      size: members.length,
      kept: kept.length,
      keptByScore,
      keptByContinuity,
      keptByGapFill,
      latOutliers,
      dropped,
    });
  }
  rejStats.clusterPrune = clusterDropped;
  log("CLUSTER", "Candidates before cluster prune: " + preClusterCount);
  log("CLUSTER", "Clusters formed: " + clusterMap.size);
  log("CLUSTER", "After cluster prune: " + final.length + " (dropped " + clusterDropped + ")");

  // ── 5.10 Cluster debug summary ──
  clusterDebug.sort((a, b) => b.topScore - a.topScore);
  for (let i = 0; i < clusterDebug.length; i++) {
    const d = clusterDebug[i];
    log("CDETAIL", "Cluster " + (i + 1) + ": top=" + d.top +
      " score=" + d.topScore.toFixed(1) + " pts=" + d.topPts + " hail=" + d.topHail +
      "\" area=" + d.topArea.toFixed(1) + "mi² [" + d.topCentroid.lat.toFixed(2) + "," +
      d.topCentroid.lon.toFixed(2) + "] size=" + d.size +
      " kept=" + d.kept + "(score=" + d.keptByScore + " cont=" + d.keptByContinuity +
      " gap=" + (d.keptByGapFill || 0) + " latDrop=" + (d.latOutliers || 0) + ")" +
      " dropped=" + d.dropped);
  }

  // Region debug: final corridors after cluster prune
  for (const c of final) {
    if (isWesternSdNeWy(c.centroid.lat, c.centroid.lon)) {
      regionDebugStage("post_cluster_prune", c.key.split(":")[0], c.key, c.centroid.lat, c.centroid.lon, c.area || 0, c.nPts, c.repMS || 0);
    }
  }

  // ── 5.11 Regional cluster summary (outlier-distance filter DISABLED) ──
  // A single storm date can have independent hail regions across the US
  // (e.g., Texas + North Dakota + Florida on the same date).  Dropping clusters
  // because they are >300 mi from the largest cluster incorrectly removes entire
  // valid regional hail events.  Quality filtering (isolation, area, min points,
  // heading) is sufficient; geographic distance-to-dominant is not applied.
  console.log('[OUTLIER_DROP_DISABLED] keeping independent regional hail clusters for national date');
  {
    const _ocMap = new Map();
    for (const c of final) {
      const rk = c._pruneCluster;
      if (!_ocMap.has(rk)) _ocMap.set(rk, []);
      _ocMap.get(rk).push(c);
    }
    const keptRegions = _ocMap.size;
    const droppedQuality = rejStats.minPts + rejStats.minHail + rejStats.area + rejStats.isolation + rejStats.clusterPrune;
    log("REGIONAL_CLUSTER_SUMMARY", "kept_regions=" + keptRegions + " dropped_quality=" + droppedQuality);
    // Log each kept region for observability
    for (const [rk, members] of _ocMap) {
      const totalArea = members.reduce((s, c) => s + (c.area || 0), 0);
      const maxHail = members.reduce((m, c) => Math.max(m, c.repMS || 0), 0);
      const lats = members.map(c => c.centroid && c.centroid.lat).filter(Boolean);
      const lons = members.map(c => c.centroid && c.centroid.lon).filter(Boolean);
      const cLat = lats.length ? (Math.min(...lats) + Math.max(...lats)) / 2 : 0;
      const cLon = lons.length ? (Math.min(...lons) + Math.max(...lons)) / 2 : 0;
      log("REGION_KEPT", "cluster=" + rk + " corridors=" + members.length +
        " area=" + totalArea.toFixed(0) + "mi² maxHail=" + maxHail.toFixed(2) +
        "\" center=[" + cLat.toFixed(1) + "," + cLon.toFixed(1) + "]");
    }
  }

  // ── 5.12 Final summary ──
  final.sort((a, b) => b._score - a._score);
  log("SUMMARY", "Rejection breakdown: " + JSON.stringify(rejStats));
  log("SUMMARY", "Total pipeline: " + corridors.length + " built → " + deduped.length + " deduped → " + preClusterCount + " qualified → " + final.length + " final");
  log("SUMMARY", "Top 25 by dominance score:");
  for (let i = 0; i < Math.min(25, final.length); i++) {
    const c = final[i];
    log("TOP25", (i + 1) + ". " + c.key + " score=" + c._score.toFixed(1) +
      " pts=" + c.nPts + " hail=" + c.repMS + "\" area=" + c.area.toFixed(1) +
      "mi² nb=" + c._neighbors + " [" + c.centroid.lat.toFixed(2) + "," + c.centroid.lon.toFixed(2) + "]");
  }

  if (!final.length) {
    log("MAIN", "No corridors after filtering.");
    return;
  }

  // ── 5.13 Assign cluster IDs to final corridors ──
  for (const c of final) {
    const root = findRoot(c);
    c._clusterRoot = root.key;
  }
  const clusterGroupsMap = new Map();
  for (const c of final) {
    if (!clusterGroupsMap.has(c._clusterRoot)) clusterGroupsMap.set(c._clusterRoot, []);
    clusterGroupsMap.get(c._clusterRoot).push(c);
  }
  log("HIERARCHY", "Date=" + TARGET_DATE + " clusters=" + clusterGroupsMap.size + " corridors=" + final.length);

  // ── 6. Delete existing nexrad_iem HAIL rows for this date ──
  const { error: delErr } = await supabase
    .from("storm_polygons")
    .delete()
    .eq("event_date", TARGET_DATE)
    .eq("source", "nexrad_iem")
    .eq("source_product", "iem_nexrad_track");
  if (delErr) log("ERROR", "delete hail: " + delErr.message);
  // Clear hail_radar_polygons for this date so we start fresh
  await supabase.from("hail_radar_polygons").delete()
    .eq("event_date", TARGET_DATE).eq("source", "nexrad_iem");
  log("MAIN", "[radar-ingest] date=" + TARGET_DATE);
  log("MAIN", "[radar-ingest] source=NEXRAD/IEM");

  // ── 7. Build cluster-merged severity-tier polygons and save ──
  //
  // Merges all corridors within each cluster into one broad storm corridor
  // per severity band using polygon boolean union (polygon-clipping).
  // For each cluster + band:
  //   1. Select corridors whose maxSev qualifies for that band
  //   2. Build inflated buffer-line polygon rings per corridor
  //   3. Union all corridor polygons via polygon-clipping
  //   4. Keep the largest connected polygon (drops outlier fragments)
  //   5. For inner bands, intersect with outer to guarantee nesting
  //   6. Save one row per cluster + band

  const OUTER_MULT       = 1.15;   // half-width inflation for outer envelope
  const YELLOW_HW_FLOOR  = 0.015;  // ~1.7 km minimum yellow half-width
  const YELLOW_HW_CAP    = 0.08;   // ~8.9 km max yellow half-width
  const MERGE_BUFFER     = 0.055;  // ~6.1 km per-side extra dilation to close gaps
  const BAND_TIER_WIDTHS = [1.0, 0.62, 0.42, 0.28, 0.18];
  //                        yel  org   red   dkr   pur

  let nSaved = 0;
  let swathIdx = 0;
  const tierCounts = {};
  const radarRows = []; // collected for bulk insert into hail_radar_polygons
  let regionRowsSaved = { western_sd_ne_wy: 0, north_dakota: 0 };

  log("TIER_GEN", "=== Per-CLUSTER polygon-union tier generation (date=" + TARGET_DATE + ") ===");
  log("TIER_GEN", "OUTER_MULT=" + OUTER_MULT +
    " YELLOW_HW_FLOOR=" + YELLOW_HW_FLOOR + "°" +
    " YELLOW_HW_CAP=" + YELLOW_HW_CAP + "°" +
    " MERGE_BUFFER=" + MERGE_BUFFER + "°" +
    " BAND_TIER_WIDTHS=" + JSON.stringify(BAND_TIER_WIDTHS));
  log("TIER_GEN", "Clusters: " + clusterGroupsMap.size + "  corridors: " + final.length);

  for (const [clusterRoot, clusterCorridors] of clusterGroupsMap) {
    let clusterMaxSev = 0;
    let clusterMaxRepMS = 0;
    for (const c of clusterCorridors) {
      const ms = Math.max(...c._trackSev, c.repMS);
      if (ms > clusterMaxSev) clusterMaxSev = ms;
      if (c.repMS > clusterMaxRepMS) clusterMaxRepMS = c.repMS;
    }
    const applicableBands = HAIL_SIZE_BANDS.filter(b => clusterMaxSev >= b.min);
    if (applicableBands.length === 0) continue;

    log("TIER_GEN", "Cluster " + clusterRoot + ": " + clusterCorridors.length +
      " corridors, maxSev=" + clusterMaxSev.toFixed(2) + "\"" +
      ", bands=" + applicableBands.length);

    let outerBandPoly = null; // [ring] coords for intersection-based nesting
    const clusterTierAreas = {};

    for (const band of applicableBands) {
      const bandIdx = HAIL_SIZE_BANDS.indexOf(band);
      const tierScale = BAND_TIER_WIDTHS[bandIdx] || 0.10;
      const mergeExtra = MERGE_BUFFER * tierScale;

      // Build inflated corridor rings for this band
      const bandRings = [];
      let nQualifying = 0;
      for (const c of clusterCorridors) {
        if (c._trackPts.length < 2) continue;
        // Yellow band: all corridors; inner bands: only qualifying corridors
        if (bandIdx > 0) {
          const maxSev = Math.max(...c._trackSev, c.repMS);
          if (maxSev < band.min) continue;
        }
        nQualifying++;

        // Half-widths: severity-weighted for inner bands, uniform for yellow
        const scaledHws = c._trackHws.map((hw, pi) => {
          if (bandIdx === 0) {
            // Yellow band: uniform scaling — all corridors
            let scaled = hw * OUTER_MULT * tierScale;
            scaled = Math.max(scaled, YELLOW_HW_FLOOR * tierScale);
            scaled = Math.min(scaled, YELLOW_HW_CAP * tierScale);
            return scaled;
          }
          // Inner bands: boost half-width where per-point severity exceeds threshold
          const ptSev = (c._trackSev && c._trackSev[pi]) || 0;
          const sevRatio = ptSev / Math.max(band.min, 0.5);
          // Points at/above threshold → boost up to 1.8×; below → narrow to 0.4×
          const boost = Math.min(Math.max(sevRatio, 0.4), 1.8);
          let scaled = hw * OUTER_MULT * tierScale * boost;
          scaled = Math.max(scaled, YELLOW_HW_FLOOR * tierScale * 0.5);
          // Cap at yellow-level (nesting clips to outer band anyway)
          scaled = Math.min(scaled, YELLOW_HW_CAP);
          return scaled;
        });
        const smoothed = smoothHalfWidths(scaledHws, 2);
        const tapered = taperHalfWidths(smoothed, 3, 0.30);
        // Add merge buffer AFTER taper so overlap is preserved at tapered tips
        const finalHws = tapered.map(hw => hw + mergeExtra);
        let ring = bufferLine(c._trackPts, finalHws);

        // Fallback for self-intersecting rings
        if (ringSelfIntersects(ring)) {
          ring = convexHull(ring.slice(0, -1));
        }
        if (ring.length >= 4) bandRings.push(ring);
      }

      if (bandRings.length === 0) continue;

      // ── Polygon union via polygon-clipping ──
      // Union merges overlapping/adjacent corridor polygons into broader
      // shapes.  Result is a MultiPolygon: multiple separate pieces where
      // corridors don't physically overlap.
      let unionResult;
      try {
        const polys = bandRings.map(ring => [ring]);
        if (polys.length === 1) {
          unionResult = [polys[0]];
        } else {
          unionResult = polygonClipping.union(...polys);
        }
      } catch (e) {
        log("UNION_ERR", "cluster=" + clusterRoot + " " + band.label +
          ": " + e.message + " — fallback to convex hull");
        const allVerts = [];
        for (const ring of bandRings) for (const pt of ring) allVerts.push(pt);
        const hullRing = convexHull(allVerts);
        unionResult = [[hullRing]];
      }

      if (!unionResult || unionResult.length === 0) continue;

      // ── Post-union fragment filter ──
      // Drops small isolated fragments that clutter the swath (e.g. off-axis
      // side strips in Kansas/Missouri).  BFS expansion from the largest
      // fragment: keep neighbours within FRAG_KEEP_DIST, drop the rest if
      // they fall below FRAG_MIN_AREA.
      const FRAG_MIN_AREA_BASE = 80;  // mi² base — scaled down for inner bands
      const FRAG_MIN_AREA   = bandIdx === 0 ? FRAG_MIN_AREA_BASE
                              : Math.max(FRAG_MIN_AREA_BASE * tierScale, 5);
      const FRAG_BIG_AREA   = bandIdx === 0 ? 300 : Math.max(300 * tierScale, 20);
      const FRAG_KEEP_DIST  = 120;   // mi — max gap between kept fragments
      if (unionResult.length > 1) {
        // Compute area + centroid per fragment
        const fragMeta = unionResult.map(poly => {
          if (!poly[0] || poly[0].length < 4) return { area: 0, lat: 0, lon: 0 };
          const c = centroidOf(poly[0]);
          let a = Math.abs(areaSqMi(poly[0]));
          for (let hi = 1; hi < poly.length; hi++) a -= Math.abs(areaSqMi(poly[hi]));
          return { area: Math.max(a, 0), lat: c.lat, lon: c.lon };
        });

        // Diagnostic: log each fragment before filtering
        if (bandIdx === 0) {
          for (let fi = 0; fi < fragMeta.length; fi++) {
            log("FRAG_DIAG", "cluster=" + clusterRoot + " " + band.label +
              " frag " + fi + ": area=" + fragMeta[fi].area.toFixed(0) + "mi²" +
              " ctr=[" + fragMeta[fi].lat.toFixed(2) + "," + fragMeta[fi].lon.toFixed(2) + "]");
          }
        }

        // Compute area-weighted median latitude (main track axis)
        const sortedByLat = fragMeta
          .map((f, i) => ({ ...f, i }))
          .filter(f => f.area > 0)
          .sort((a, b) => a.lat - b.lat);
        let cumArea = 0;
        const halfTotal = sortedByLat.reduce((s, f) => s + f.area, 0) / 2;
        let medianLat = sortedByLat.length ? sortedByLat[0].lat : 0;
        for (const f of sortedByLat) {
          cumArea += f.area;
          if (cumArea >= halfTotal) { medianLat = f.lat; break; }
        }

        const OFF_AXIS_LAT = 2.0;    // degrees from median — tighter than before
        const OFF_AXIS_FRAC = 0.25;   // off-axis frags need ≥ 25% of on-axis best

        // Find largest on-axis fragment as BFS seed (not absolute largest)
        const kept = new Set();
        let bestIdx = 0;
        for (let fi = 1; fi < fragMeta.length; fi++) {
          if (fragMeta[fi].area > fragMeta[bestIdx].area) bestIdx = fi;
        }
        let onAxisBest = -1;
        for (let fi = 0; fi < fragMeta.length; fi++) {
          if (Math.abs(fragMeta[fi].lat - medianLat) <= OFF_AXIS_LAT) {
            if (onAxisBest < 0 || fragMeta[fi].area > fragMeta[onAxisBest].area) {
              onAxisBest = fi;
            }
          }
        }
        if (onAxisBest >= 0) bestIdx = onAxisBest;
        kept.add(bestIdx);

        // Force-keep fragments whose centroid lies inside western SD / NE WY
        for (let fi = 0; fi < fragMeta.length; fi++) {
          if (isWesternSdNeWy(fragMeta[fi].lat, fragMeta[fi].lon)) {
            if (!kept.has(fi)) {
              kept.add(fi);
              log("FRAG_REGION_KEEP", "cluster=" + clusterRoot + " " + band.label + " frag " + fi + 
                ": forced keep (western_sd_ne_wy) ctr=[" + fragMeta[fi].lat.toFixed(4) + "," + fragMeta[fi].lon.toFixed(4) + "] area=" + fragMeta[fi].area.toFixed(0));
            }
          }
        }

        log("FRAG_SEED", "cluster=" + clusterRoot + " " + band.label +
          " medianLat=" + medianLat.toFixed(2) +
          " bestIdx=" + bestIdx +
          " bestArea=" + fragMeta[bestIdx].area.toFixed(0) + "mi²" +
          " [" + fragMeta[bestIdx].lat.toFixed(2) + "," + fragMeta[bestIdx].lon.toFixed(2) + "]");

        // Auto-seed big fragments ONLY if on-axis
        for (let fi = 0; fi < fragMeta.length; fi++) {
          if (fragMeta[fi].area >= FRAG_BIG_AREA &&
              Math.abs(fragMeta[fi].lat - medianLat) <= OFF_AXIS_LAT) {
            kept.add(fi);
          }
        }
        let changed = true;
        while (changed) {
          changed = false;
          for (let fi = 0; fi < fragMeta.length; fi++) {
            if (kept.has(fi)) continue;
            if (fragMeta[fi].area < FRAG_MIN_AREA) continue; // too small
            // Off-axis check: far from median latitude AND small → skip
            const latDev = Math.abs(fragMeta[fi].lat - medianLat);
            if (latDev > OFF_AXIS_LAT && fragMeta[fi].area < fragMeta[bestIdx].area * OFF_AXIS_FRAC) continue;
            // Check distance to any kept fragment
            let nearKept = false;
            for (const ki of kept) {
              const dy = fragMeta[fi].lat - fragMeta[ki].lat;
              const dx = fragMeta[fi].lon - fragMeta[ki].lon;
              const cosLat = Math.cos(fragMeta[fi].lat * Math.PI / 180);
              const distMi = 69.0 * Math.sqrt(dy * dy + (dx * cosLat) * (dx * cosLat));
              if (distMi <= FRAG_KEEP_DIST) { nearKept = true; break; }
            }
            if (nearKept) { kept.add(fi); changed = true; }
          }
        }
        if (kept.size < unionResult.length) {
          const nDropped = unionResult.length - kept.size;
          const droppedArea = fragMeta
            .filter((_, i) => !kept.has(i))
            .reduce((s, f) => s + f.area, 0);
          unionResult = unionResult.filter((_, i) => kept.has(i));
          log("FRAG_FILTER", "cluster=" + clusterRoot + " " + band.label +
            ": dropped " + nDropped + " fragment(s) (" + droppedArea.toFixed(0) + "mi²)" +
            " — kept " + kept.size);
        }
      }

      // Compute total area across all union fragments (outer - holes per poly)
      let totalArea = 0;
      for (const poly of unionResult) {
        if (!poly[0] || poly[0].length < 4) continue;
        let a = Math.abs(areaSqMi(poly[0]));
        for (let hi = 1; hi < poly.length; hi++) {
          a -= Math.abs(areaSqMi(poly[hi]));
        }
        totalArea += Math.max(a, 0);
      }
      if (totalArea < 1) continue;

      log("UNION", "cluster=" + clusterRoot + " " + band.label +
        ": " + bandRings.length + " rings → " + unionResult.length + " fragment(s)" +
        ", totalArea=" + totalArea.toFixed(1) + "mi²");

      // Cascading nesting: clip each band to the PREVIOUS band's final polygon
      // This guarantees purple ⊂ darkred ⊂ red ⊂ orange ⊂ yellow.
      if (bandIdx === 0) {
        outerBandPoly = unionResult; // yellow becomes the outer envelope
      } else if (outerBandPoly) {
        try {
          const clipped = polygonClipping.intersection(unionResult, outerBandPoly);
          if (clipped && clipped.length > 0) {
            unionResult = clipped;
            // Recompute area after clipping
            totalArea = 0;
            for (const poly of unionResult) {
              if (!poly[0] || poly[0].length < 4) continue;
              let a = Math.abs(areaSqMi(poly[0]));
              for (let hi = 1; hi < poly.length; hi++) {
                a -= Math.abs(areaSqMi(poly[hi]));
              }
              totalArea += Math.max(a, 0);
            }
          }
        } catch (e) {
          log("CLIP_WARN", "cluster=" + clusterRoot + " " + band.label + ": " + e.message);
        }
        // Update outerBandPoly for next-inner band (cascading)
        outerBandPoly = unionResult;
      }

      const tierArea = totalArea;
      clusterTierAreas[band.min] = tierArea;

      const tier = band.label;
      if (!tierCounts[tier]) tierCounts[tier] = { polygons: 0, area: 0, maxAreaMi2: 0 };
      tierCounts[tier].polygons++;
      tierCounts[tier].area += tierArea;
      if (tierArea > tierCounts[tier].maxAreaMi2) tierCounts[tier].maxAreaMi2 = tierArea;

      // Weighted centroid across all fragments
      let cLat = 0, cLon = 0, cWeight = 0;
      for (const poly of unionResult) {
        if (!poly[0] || poly[0].length < 4) continue;
        const c = centroidOf(poly[0]);
        const a = Math.abs(areaSqMi(poly[0]));
        cLat += c.lat * a; cLon += c.lon * a; cWeight += a;
      }
      cLat /= cWeight || 1; cLon /= cWeight || 1;

      const geojson = {
        type: "Feature",
        geometry: { type: "MultiPolygon", coordinates: unionResult },
        properties: {
          corridor: clusterRoot,
          cluster: clusterRoot,
          band_threshold: band.min,
          tier_width_frac: tierScale,
          outer_mult: OUTER_MULT,
          merged_corridors: bandRings.length,
          merge_mode: "polygon_union",
          merge_buffer_deg: parseFloat(mergeExtra.toFixed(4)),
          fragments: unionResult.length,
        },
      };

      const topCorridor = clusterCorridors[0];
      const qualityStatus = classifyQuality(topCorridor, tierArea, applicableBands.length);

      const row = {
        event_date: TARGET_DATE,
        storm_type: "hail",
        band_min: band.min,
        band_max: band.max === 99 ? clusterMaxRepMS : band.max,
        band_label: band.label,
        polygon_geojson: geojson,
        centroid_lat: cLat,
        centroid_lon: cLon,
        area_sq_mi: parseFloat(tierArea.toFixed(2)),
        source: "nexrad_iem",
        source_product: "iem_nexrad_track",
        source_priority: 1,
        swath_index: swathIdx,
      };
      try { row.quality_status = qualityStatus; } catch (_) {}

      const { error } = await supabase.from("storm_polygons").upsert([row], {
        onConflict: "event_date,source,source_product,swath_index",
      });
      if (error) {
        if (error.message && error.message.includes("quality_status")) {
          delete row.quality_status;
          const { error: e2 } = await supabase.from("storm_polygons").upsert([row], {
            onConflict: "event_date,source,source_product,swath_index",
          });
          if (e2) log("ERROR", "upsert cluster=" + clusterRoot + " " + band.label + ": " + e2.message);
          else { nSaved++; radarRows.push({ event_date: row.event_date, source: row.source, source_product: row.source_product, band_min: row.band_min, band_max: row.band_max, polygon_geojson: row.polygon_geojson, centroid_lat: row.centroid_lat, centroid_lon: row.centroid_lon, area_sq_mi: row.area_sq_mi, swath_index: row.swath_index }); }
        } else {
          log("ERROR", "upsert cluster=" + clusterRoot + " " + band.label + ": " + error.message);
        }
      } else {
        nSaved++;
        radarRows.push({ event_date: row.event_date, source: row.source, source_product: row.source_product, band_min: row.band_min, band_max: row.band_max, polygon_geojson: row.polygon_geojson, centroid_lat: row.centroid_lat, centroid_lon: row.centroid_lon, area_sq_mi: row.area_sq_mi, swath_index: row.swath_index });
        if (isWesternSdNeWy(row.centroid_lat, row.centroid_lon)) {
          regionRowsSaved.western_sd_ne_wy++;
          log("REGION_KEEP", "western_sd_ne_wy saved rows=1 cluster=" + clusterRoot +
            " lat=" + row.centroid_lat.toFixed(4) +
            " lon=" + row.centroid_lon.toFixed(4) +
            " area=" + row.area_sq_mi.toFixed(2) +
            " band=" + row.band_label);
        }
        if (isNorthDakota(row.centroid_lat, row.centroid_lon)) {
          regionRowsSaved.north_dakota++;
          log("REGION_KEEP", "north_dakota saved rows=1 cluster=" + clusterRoot +
            " lat=" + row.centroid_lat.toFixed(4) +
            " lon=" + row.centroid_lon.toFixed(4) +
            " area=" + row.area_sq_mi.toFixed(2) +
            " band=" + row.band_label);
        }
      }

      // For each region that has qualifying corridors, create a targeted
      // region-only union and save it. This preserves independent regional hail
      // clusters without dropping one region for another.
      const regionConfigs = [
        { name: "western_sd_ne_wy", checkFn: isWesternSdNeWy, anyPtFn: anyPtInWesternSdNeWy },
        { name: "north_dakota", checkFn: isNorthDakota, anyPtFn: anyPtInNorthDakota },
      ];
      for (const rc of regionConfigs) {
        const regionMembers = clusterCorridors.filter(c => {
          // Corridor must qualify for this band (same check as above)
          if (bandIdx > 0) {
            const maxSev = Math.max(...c._trackSev, c.repMS);
            if (maxSev < band.min) return false;
          }
          return rc.anyPtFn(c._trackPts || c._trackLngs || []);
        });
        if (regionMembers.length) {
          try {
            const polys = regionMembers.map(c => [convexHull(c._trackPts || c._trackLngs || [])]);
            let regionUnion = polys.length === 1 ? polys[0] : polygonClipping.union(...polys);
            if (regionUnion && regionUnion.length) {
              // compute centroid of regionUnion (area-weighted)
              let rLat = 0, rLon = 0, rW = 0;
              for (const poly of regionUnion) {
                if (!poly[0] || poly[0].length < 4) continue;
                const cc = centroidOf(poly[0]);
                const a = Math.abs(areaSqMi(poly[0]));
                rLat += cc.lat * a; rLon += cc.lon * a; rW += a;
              }
              rLat /= rW || 1; rLon /= rW || 1;
              // Only save if union's centroid is inside this region
              if (rc.checkFn(rLat, rLon)) {
                const regionGeojson = {
                  type: "Feature",
                  geometry: { type: "MultiPolygon", coordinates: regionUnion },
                  properties: Object.assign({}, geojson.properties, { region_forced: true, region_name: rc.name }),
                };
                const regionRow = Object.assign({}, row, {
                  polygon_geojson: regionGeojson,
                  centroid_lat: rLat,
                  centroid_lon: rLon,
                  area_sq_mi: parseFloat((rW || 0).toFixed(2)),
                  swath_index: swathIdx,
                });
                const { error: rerr } = await supabase.from("storm_polygons").upsert([regionRow], { onConflict: "event_date,source,source_product,swath_index" });
                if (rerr) log("ERROR", "region upsert cluster=" + clusterRoot + " band=" + band.label + " region=" + rc.name + ": " + rerr.message);
                else {
                  nSaved++; regionRowsSaved[rc.name]++; swathIdx++;
                  radarRows.push({ event_date: regionRow.event_date, source: regionRow.source, source_product: regionRow.source_product, band_min: regionRow.band_min, band_max: regionRow.band_max, polygon_geojson: regionRow.polygon_geojson, centroid_lat: regionRow.centroid_lat, centroid_lon: regionRow.centroid_lon, area_sq_mi: regionRow.area_sq_mi, swath_index: regionRow.swath_index });
                  log("REGION_KEEP", rc.name + " saved rows=1 (forced region-only) cluster=" + clusterRoot + " lat=" + rLat.toFixed(4) + " lon=" + rLon.toFixed(4) + " area=" + regionRow.area_sq_mi.toFixed(2) + " band=" + band.label);
                }
              }
            }
          } catch (e) {
            log("ERROR", "region-only union cluster=" + clusterRoot + " band=" + band.label + " region=" + rc.name + ": " + e.message);
          }
        }
      }

      log("BAND", "cluster=" + clusterRoot +
        " tier=" + band.label +
        " area=" + tierArea.toFixed(1) + "mi²" +
        " corridors_used=" + nQualifying +
        " rings_built=" + bandRings.length +
        " fragments=" + unionResult.length);
      swathIdx++;
    }

    // Per-cluster containment check
    const bandMins = Object.keys(clusterTierAreas).map(Number).sort((a, b) => a - b);
    for (let ti = 1; ti < bandMins.length; ti++) {
      const outerBand = bandMins[ti - 1];
      const innerBand = bandMins[ti];
      const outerArea = clusterTierAreas[outerBand];
      const innerArea = clusterTierAreas[innerBand];
      const contained = innerArea <= outerArea * 1.01;
      const outerLabel = HAIL_SIZE_BANDS.find(b => b.min === outerBand)?.label || outerBand + "\"";
      const innerLabel = HAIL_SIZE_BANDS.find(b => b.min === innerBand)?.label || innerBand + "\"";
      if (!contained) {
        log("NESTING_WARN", "cluster=" + clusterRoot + " " + innerLabel + " > " + outerLabel +
          " (inner=" + innerArea.toFixed(0) + "mi² outer=" + outerArea.toFixed(0) + "mi²)");
      }
    }
  }

  // ── Debug: tier polygon summary ──
  log("TIER_SUMMARY", "=== Tier polygon summary (date=" + TARGET_DATE + ") ===");
  for (const [tier, s] of Object.entries(tierCounts)) {
    log("TIER_SUMMARY", "  " + tier + ": " + s.polygons + " polygon(s)" +
      " totalArea=" + s.area.toFixed(1) + "mi²" +
      " avgArea=" + (s.area / (s.polygons || 1)).toFixed(1) + "mi²" +
      " maxArea=" + s.maxAreaMi2.toFixed(1) + "mi²");
  }
  log("TIER_SUMMARY", "merge_mode=polygon_union (buffered corridor rings + polygon-clipping union)");
  log("TIER_SUMMARY", "total_rows=" + swathIdx + " (was " + final.length + " corridors × bands in per-corridor mode)");
  log("REGION_SUMMARY", "western_sd_ne_wy saved_rows=" + regionRowsSaved.western_sd_ne_wy + " north_dakota saved_rows=" + regionRowsSaved.north_dakota);

  log("DEBUG", "date=" + TARGET_DATE +
    " event_groups(clusters)=" + clusterGroupsMap.size +
    " corridors=" + final.length +
    " total_tier_polygons=" + swathIdx);

  log("MAIN", "Hail done. Saved " + nSaved + " merged band rows from " +
    clusterGroupsMap.size + " clusters (" + final.length + " corridors) to storm_polygons (" + SCRIPT_VERSION + ")");

  // ── Bulk-insert collected rows into hail_radar_polygons ──
  if (radarRows.length > 0) {
    const { count: beforeCount } = await supabase.from("hail_radar_polygons")
      .select("id", { count: "exact", head: true }).eq("event_date", TARGET_DATE);
    const RADAR_CHUNK = 100;
    let nRadarSaved = 0;
    for (let ri = 0; ri < radarRows.length; ri += RADAR_CHUNK) {
      const chunk = radarRows.slice(ri, ri + RADAR_CHUNK);
      const { error: radarErr } = await supabase.from("hail_radar_polygons").insert(chunk);
      if (radarErr) log("ERROR", "[radar-ingest] bulk insert chunk " + ri + ": " + radarErr.message);
      else nRadarSaved += chunk.length;
    }
    log("MAIN", "[radar-ingest] rows before=" + (beforeCount || 0) + " rows after=" + nRadarSaved);
    log("MAIN", "[radar-ingest] polygons saved=" + nRadarSaved);
  } else {
    log("MAIN", "[radar-ingest] polygons saved=0 (no hail corridors generated)");
  }

  nSavedHail = nSaved;
  hailSwathIdx = swathIdx;
  } // ── END HAIL PIPELINE ──

  // ═════════════════════════════════════════════════════════════
  //  WIND PIPELINE — Build wind swath corridors from same IEM data
  // ═════════════════════════════════════════════════════════════
  let nSavedWind = 0;
  let windSwathIdx = 0;

  if (allWindFeats.length > 0) {
    log("WIND", "=== Wind pipeline: " + allWindFeats.length + " wind-signal features ===");

    // Delete existing wind rows for this date
    const { error: wDelErr } = await supabase
      .from("storm_polygons")
      .delete()
      .eq("event_date", TARGET_DATE)
      .eq("source", "nexrad_iem")
      .eq("source_product", "iem_nexrad_wind");
    if (wDelErr) log("ERROR", "delete wind: " + wDelErr.message);

    // Group by nexrad:storm_id
    const wGroups = new Map();
    for (const f of allWindFeats) {
      const k = f.properties.nexrad + ":" + f.properties.storm_id;
      if (!wGroups.has(k)) wGroups.set(k, []);
      wGroups.get(k).push(f);
    }
    log("WIND", wGroups.size + " unique nexrad:storm_id groups");

    // Sort, split, build corridor polygons
    const windCorridors = [];
    for (const [key, feats] of wGroups) {
      feats.sort((a, b) => new Date(a.properties.valid) - new Date(b.properties.valid));
      // Split at gaps
      const segments = [];
      let chunk = [feats[0]];
      for (let i = 1; i < feats.length; i++) {
        const gap = (new Date(feats[i].properties.valid) - new Date(feats[i - 1].properties.valid)) / 60000;
        const dxy = Math.sqrt(
          (feats[i].geometry.coordinates[0] - feats[i - 1].geometry.coordinates[0]) ** 2 +
          (feats[i].geometry.coordinates[1] - feats[i - 1].geometry.coordinates[1]) ** 2
        );
        if (gap > MAX_GAP_MIN || dxy > SPATIAL_GAP) {
          segments.push(chunk);
          chunk = [];
        }
        chunk.push(feats[i]);
      }
      if (chunk.length > 0) segments.push(chunk);

      for (const seg of segments) {
        if (seg.length < WIND_MIN_TRACK_PTS) continue;
        const coords = seg.map(f => f.geometry.coordinates);
        const vils = seg.map(f => f.properties.vil || 0);
        const dbzs = seg.map(f => f.properties.max_dbz || 0);

        // Deduplicate consecutive identical coords
        const uP = [coords[0]], uV = [vils[0]], uD = [dbzs[0]];
        for (let i = 1; i < coords.length; i++) {
          if (coords[i][0] !== coords[i - 1][0] || coords[i][1] !== coords[i - 1][1]) {
            uP.push(coords[i]); uV.push(vils[i]); uD.push(dbzs[i]);
          }
        }
        if (uP.length < 2) continue;

        // Half-widths from VIL
        const repVIL = Math.max(...uV);
        const repDBZ = Math.max(...uD);
        const rawHws = uV.map(v => {
          const hw = (v > 0 ? v : repVIL * 0.5) * WIND_BUF_SCALE;
          return Math.min(Math.max(hw, WIND_HW_FLOOR), WIND_HW_CAP);
        });
        const hws = smoothHalfWidths(rawHws, HW_SMOOTH_RADIUS);

        // Build polygon ring
        let ring = bufferLine(uP, hws);
        if (ringSelfIntersects(ring)) {
          // Try shrink
          const shrunkHws = hws.map(hw => hw * 0.6);
          ring = bufferLine(uP, shrunkHws);
          if (ringSelfIntersects(ring)) continue;
        }

        const area = areaSqMi(ring);
        if (area > MAX_AREA || area < WIND_MIN_AREA) continue;

        // Quality gate
        if (repVIL < WIND_MIN_VIL) continue;

        windCorridors.push({
          key,
          ring,
          centroid: centroidOf(ring),
          repVIL,
          repDBZ,
          nPts: seg.length,
          area,
          _trackPts: uP,
          _trackHws: hws,
          _trackVils: uV,
          _trackDbzs: uD,
          firstTime: seg[0].properties.valid,
          lastTime: seg[seg.length - 1].properties.valid,
        });
      }
    }
    log("WIND", "Wind corridors built: " + windCorridors.length);

    // ── Merged wind band generation via polygon union ──
    // Union all wind corridors per severity band into a single MultiPolygon
    // (mirrors the hail pipeline).  Produces ~4 merged rows instead of 3000+.
    const WIND_MERGE_BUFFER = 0.055;
    const WIND_TIER_WIDTHS  = [1.0, 0.55, 0.35, 0.20];

    // Determine globally applicable wind bands from max DBZ across all corridors
    let globalMaxDBZ = 0;
    for (const wc of windCorridors) {
      if (wc.repDBZ > globalMaxDBZ) globalMaxDBZ = wc.repDBZ;
    }
    const windApplicableBands = WIND_SEVERITY_BANDS.filter(b => globalMaxDBZ >= b.dbzFloor);
    if (windApplicableBands.length === 0 && windCorridors.length > 0) {
      windApplicableBands.push(WIND_SEVERITY_BANDS[0]);
    }

    log("WIND_MERGE", "globalMaxDBZ=" + globalMaxDBZ +
      " applicableBands=" + windApplicableBands.length +
      " corridors=" + windCorridors.length);

    let outerWindPoly = null;
    const windTierAreas = {};

    for (const band of windApplicableBands) {
      const bandIdx = WIND_SEVERITY_BANDS.indexOf(band);
      const tierScale = WIND_TIER_WIDTHS[bandIdx] || 0.20;
      const mergeExtra = WIND_MERGE_BUFFER * tierScale;

      // Build inflated corridor rings for this band
      const bandRings = [];
      for (const wc of windCorridors) {
        if (wc._trackPts.length < 2) continue;
        // Light band: all corridors; inner bands: only qualifying by DBZ
        if (bandIdx > 0 && wc.repDBZ < band.dbzFloor) continue;

        const scaledHws = wc._trackHws.map(hw => {
          let scaled = hw * tierScale;
          scaled = Math.max(scaled, WIND_HW_FLOOR * 0.5);
          scaled = Math.min(scaled, WIND_HW_CAP);
          return scaled;
        });
        const smoothed = smoothHalfWidths(scaledHws, 2);
        const tapered = taperHalfWidths(smoothed, 3, 0.30);
        const finalHws = tapered.map(hw => hw + mergeExtra);

        let ring = bufferLine(wc._trackPts, finalHws);
        if (ringSelfIntersects(ring)) {
          ring = convexHull(ring.slice(0, -1));
        }
        if (ring.length >= 4) bandRings.push(ring);
      }

      if (bandRings.length === 0) continue;

      // Polygon union via polygon-clipping
      let unionResult;
      try {
        const polys = bandRings.map(ring => [ring]);
        if (polys.length === 1) {
          unionResult = [polys[0]];
        } else {
          unionResult = polygonClipping.union(...polys);
        }
      } catch (e) {
        log("WIND_UNION_ERR", band.label + ": " + e.message + " — fallback to convex hull");
        const allVerts = [];
        for (const ring of bandRings) for (const pt of ring) allVerts.push(pt);
        unionResult = [[convexHull(allVerts)]];
      }

      if (!unionResult || unionResult.length === 0) continue;

      // Fragment filter — BFS from largest on-axis fragment (same as hail)
      const WIND_FRAG_MIN_AREA  = 50;
      const WIND_FRAG_BIG_AREA  = 200;
      const WIND_FRAG_KEEP_DIST = 120;
      if (unionResult.length > 1) {
        const fragMeta = unionResult.map(poly => {
          if (!poly[0] || poly[0].length < 4) return { area: 0, lat: 0, lon: 0 };
          const c = centroidOf(poly[0]);
          let a = Math.abs(areaSqMi(poly[0]));
          for (let hi = 1; hi < poly.length; hi++) a -= Math.abs(areaSqMi(poly[hi]));
          return { area: Math.max(a, 0), lat: c.lat, lon: c.lon };
        });

        if (bandIdx === 0) {
          for (let fi = 0; fi < fragMeta.length; fi++) {
            log("WIND_FRAG_DIAG", band.label + " frag " + fi +
              ": area=" + fragMeta[fi].area.toFixed(0) + "mi²" +
              " ctr=[" + fragMeta[fi].lat.toFixed(2) + "," + fragMeta[fi].lon.toFixed(2) + "]");
          }
        }

        // Area-weighted median latitude
        const sortedByLat = fragMeta.map((f, i) => ({ ...f, i }))
          .filter(f => f.area > 0).sort((a, b) => a.lat - b.lat);
        let cumArea = 0;
        const halfTotal = sortedByLat.reduce((s, f) => s + f.area, 0) / 2;
        let medianLat = sortedByLat.length ? sortedByLat[0].lat : 0;
        for (const f of sortedByLat) {
          cumArea += f.area;
          if (cumArea >= halfTotal) { medianLat = f.lat; break; }
        }

        const WIND_OFF_AXIS_LAT  = 2.5;
        const WIND_OFF_AXIS_FRAC = 0.20;

        const kept = new Set();
        let bestIdx = 0;
        for (let fi = 1; fi < fragMeta.length; fi++) {
          if (fragMeta[fi].area > fragMeta[bestIdx].area) bestIdx = fi;
        }
        let onAxisBest = -1;
        for (let fi = 0; fi < fragMeta.length; fi++) {
          if (Math.abs(fragMeta[fi].lat - medianLat) <= WIND_OFF_AXIS_LAT) {
            if (onAxisBest < 0 || fragMeta[fi].area > fragMeta[onAxisBest].area) onAxisBest = fi;
          }
        }
        if (onAxisBest >= 0) bestIdx = onAxisBest;
        kept.add(bestIdx);

        for (let fi = 0; fi < fragMeta.length; fi++) {
          if (fragMeta[fi].area >= WIND_FRAG_BIG_AREA &&
              Math.abs(fragMeta[fi].lat - medianLat) <= WIND_OFF_AXIS_LAT) {
            kept.add(fi);
          }
        }
        let changed = true;
        while (changed) {
          changed = false;
          for (let fi = 0; fi < fragMeta.length; fi++) {
            if (kept.has(fi)) continue;
            if (fragMeta[fi].area < WIND_FRAG_MIN_AREA) continue;
            const latDev = Math.abs(fragMeta[fi].lat - medianLat);
            if (latDev > WIND_OFF_AXIS_LAT &&
                fragMeta[fi].area < fragMeta[bestIdx].area * WIND_OFF_AXIS_FRAC) continue;
            let nearKept = false;
            for (const ki of kept) {
              const dy = fragMeta[fi].lat - fragMeta[ki].lat;
              const dx = fragMeta[fi].lon - fragMeta[ki].lon;
              const cosLat = Math.cos(fragMeta[fi].lat * Math.PI / 180);
              const distMi = 69.0 * Math.sqrt(dy * dy + (dx * cosLat) * (dx * cosLat));
              if (distMi <= WIND_FRAG_KEEP_DIST) { nearKept = true; break; }
            }
            if (nearKept) { kept.add(fi); changed = true; }
          }
        }
        if (kept.size < unionResult.length) {
          const nDropped = unionResult.length - kept.size;
          const droppedArea = fragMeta.filter((_, i) => !kept.has(i)).reduce((s, f) => s + f.area, 0);
          unionResult = unionResult.filter((_, i) => kept.has(i));
          log("WIND_FRAG", band.label + ": dropped " + nDropped +
            " fragment(s) (" + droppedArea.toFixed(0) + "mi²) — kept " + kept.size);
        }
      }

      // Total area across all union fragments (outer − holes)
      let totalArea = 0;
      for (const poly of unionResult) {
        if (!poly[0] || poly[0].length < 4) continue;
        let a = Math.abs(areaSqMi(poly[0]));
        for (let hi = 1; hi < poly.length; hi++) a -= Math.abs(areaSqMi(poly[hi]));
        totalArea += Math.max(a, 0);
      }
      if (totalArea < 1) continue;

      // Inner band nesting: intersect with outer band
      if (bandIdx === 0) {
        outerWindPoly = unionResult;
      } else if (outerWindPoly) {
        try {
          const clipped = polygonClipping.intersection(unionResult, outerWindPoly);
          if (clipped && clipped.length > 0) {
            unionResult = clipped;
            totalArea = 0;
            for (const poly of unionResult) {
              if (!poly[0] || poly[0].length < 4) continue;
              let a = Math.abs(areaSqMi(poly[0]));
              for (let hi = 1; hi < poly.length; hi++) a -= Math.abs(areaSqMi(poly[hi]));
              totalArea += Math.max(a, 0);
            }
          }
        } catch (e) {
          log("WIND_CLIP_WARN", band.label + ": " + e.message);
        }
      }

      windTierAreas[band.min] = totalArea;

      // Weighted centroid across fragments
      let cLat = 0, cLon = 0, cWeight = 0;
      for (const poly of unionResult) {
        if (!poly[0] || poly[0].length < 4) continue;
        const c = centroidOf(poly[0]);
        const a = Math.abs(areaSqMi(poly[0]));
        cLat += c.lat * a; cLon += c.lon * a; cWeight += a;
      }
      cLat /= cWeight || 1; cLon /= cWeight || 1;

      const geojson = {
        type: "Feature",
        geometry: { type: "MultiPolygon", coordinates: unionResult },
        properties: {
          band_threshold: band.min,
          tier_width_frac: tierScale,
          merged_corridors: bandRings.length,
          merge_mode: "polygon_union",
          merge_buffer_deg: parseFloat(mergeExtra.toFixed(4)),
          fragments: unionResult.length,
        },
      };

      const row = {
        event_date: TARGET_DATE,
        storm_type: "wind",
        band_min: band.min,
        band_max: band.max === 99 ? 3.0 : band.max,
        band_label: band.label,
        polygon_geojson: geojson,
        centroid_lat: cLat,
        centroid_lon: cLon,
        area_sq_mi: parseFloat(totalArea.toFixed(2)),
        source: "nexrad_iem",
        source_product: "iem_nexrad_wind",
        source_priority: 1,
        swath_index: windSwathIdx,
      };

      const { error } = await supabase.from("storm_polygons").upsert([row], {
        onConflict: "event_date,source,source_product,swath_index",
      });
      if (error) {
        log("ERROR", "wind band upsert " + band.label + ": " + error.message);
      } else {
        nSavedWind++;
      }

      log("WIND_BAND", "tier=" + band.label +
        " area=" + totalArea.toFixed(1) + "mi²" +
        " corridors=" + bandRings.length +
        " fragments=" + unionResult.length);
      windSwathIdx++;
    }

    log("WIND", "Wind done. Saved " + nSavedWind + " merged wind band rows from " +
      windCorridors.length + " wind corridors.");
  } else {
    log("WIND", "No wind-signal features found. Skipping wind pipeline.");
  }

  log("MAIN", "Complete. Hail=" + nSavedHail + " Wind=" + nSavedWind + " total band rows saved (" + SCRIPT_VERSION + ")");
}

// ═══════════════════════════════════════════════════════════
//  Quality validation — auto-classifies each saved corridor
// ═══════════════════════════════════════════════════════════
function classifyQuality(corridor, bandArea, nBands) {
  // accepted: high-confidence corridor
  // review: plausible but may need checking
  // fallback: low-confidence placeholder
  const { nPts, repMS, area, _neighbors } = corridor;

  // Strong corridor: many track points, significant hail, multiple neighbors
  if (nPts >= 10 && repMS >= 1.0 && area >= 50 && (_neighbors || 0) >= 2) {
    return "accepted";
  }
  // Moderate corridor: reasonable track with some evidence
  if (nPts >= 6 && repMS >= 0.75 && area >= 20) {
    return "accepted";
  }
  // Marginal: short track, small area, or weak hail
  if (nPts >= 4 && area >= 10) {
    return "review";
  }
  return "fallback";
}

// ═══════════════════════════════════════════════════════════
//  Entry point — single date, multi-date, or batch mode
// ═══════════════════════════════════════════════════════════
//
//  Usage:
//    node scripts/ingest_iem_nexrad_tracks.mjs 2026-04-15
//    node scripts/ingest_iem_nexrad_tracks.mjs 2026-04-15 2026-04-14 2026-04-13
//    node scripts/ingest_iem_nexrad_tracks.mjs --batch          (fetch recent dates from DB, process those missing polygons)
//    node scripts/ingest_iem_nexrad_tracks.mjs --batch --force  (re-process ALL recent dates even if polygons exist)
//
async function fetchRecentStormDates() {
  // Replicate hail-dates edge function logic: union of hail_lsr_raw + storm_lsr_raw + synthetic
  const dateSet = new Set();
  const pageSize = 1000;

  // hail_lsr_raw
  let offset = 0;
  for (;;) {
    const { data, error } = await supabase.from("hail_lsr_raw")
      .select("event_date").order("event_date", { ascending: false })
      .range(offset, offset + pageSize - 1);
    if (error || !data || !data.length) break;
    data.forEach(r => { if (/^\d{4}-\d{2}-\d{2}$/.test(r.event_date)) dateSet.add(r.event_date); });
    if (data.length < pageSize) break;
    offset += pageSize;
  }

  // storm_lsr_raw
  offset = 0;
  for (;;) {
    const { data, error } = await supabase.from("storm_lsr_raw")
      .select("event_date").order("event_date", { ascending: false })
      .range(offset, offset + pageSize - 1);
    if (error || !data || !data.length) break;
    data.forEach(r => { if (/^\d{4}-\d{2}-\d{2}$/.test(r.event_date)) dateSet.add(r.event_date); });
    if (data.length < pageSize) break;
    offset += pageSize;
  }

  // Synthetic dates
  ["2026-03-14", "2026-03-24", "2026-03-29", "2026-04-02"].forEach(d => dateSet.add(d));

  return [...dateSet].sort().reverse();
}

async function fetchDatesWithPolygons() {
  const { data, error } = await supabase.from("storm_polygons")
    .select("event_date")
    .eq("source", "nexrad_iem");
  if (error || !data) return new Set();
  return new Set(data.map(r => String(r.event_date).slice(0, 10)));
}

async function entryPoint() {
  const args = process.argv.slice(2);
  const isBatch = args.includes("--batch");
  const isForce = args.includes("--force");
  const dateArgs = args.filter(a => /^\d{4}-\d{2}-\d{2}$/.test(a));
  const daysArg = args.find(a => /^--days=\d+$/.test(a));
  const maxDays = daysArg ? parseInt(daysArg.split("=")[1], 10) : null;

  let datesToProcess = [];

  if (isBatch) {
    log("BATCH", "Fetching recent storm dates from DB...");
    let allDates = await fetchRecentStormDates();
    log("BATCH", "Found " + allDates.length + " total storm dates in DB");

    // Filter by --days=N if supplied (default: 30 days for scheduled runs)
    const dayLimit = maxDays || 30;
    const cutoff = new Date();
    cutoff.setUTCDate(cutoff.getUTCDate() - dayLimit);
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    allDates = allDates.filter(d => d >= cutoffStr);
    log("BATCH", "After --days=" + dayLimit + " filter (cutoff " + cutoffStr + "): " + allDates.length + " dates");

    if (isForce) {
      datesToProcess = allDates;
      log("BATCH", "--force: will re-process ALL " + allDates.length + " dates");
    } else {
      const existing = await fetchDatesWithPolygons();
      datesToProcess = allDates.filter(d => !existing.has(d));
      log("BATCH", "Dates already with nexrad_iem polygons: " + existing.size +
        " | dates to process: " + datesToProcess.length);
    }
  } else if (dateArgs.length > 0) {
    datesToProcess = dateArgs;
  } else {
    datesToProcess = ["2026-04-01"];
  }

  if (!datesToProcess.length) {
    log("BATCH", "Nothing to process. All dates already have polygons.");
    return;
  }

  log("BATCH", "Processing " + datesToProcess.length + " date(s): " + datesToProcess.join(", "));

  const results = { success: [], failed: [], noData: [] };

  for (let i = 0; i < datesToProcess.length; i++) {
    const d = datesToProcess[i];
    log("BATCH", "\n════════════════════════════════════════");
    log("BATCH", "Date " + (i + 1) + "/" + datesToProcess.length + ": " + d);
    log("BATCH", "════════════════════════════════════════");
    try {
      await main(d);
      results.success.push(d);
    } catch (e) {
      if (e.message && e.message.includes("No hail or wind features found")) {
        results.noData.push(d);
        log("BATCH", "No IEM data for " + d + " (may be too old for IEM archive)");
      } else {
        results.failed.push({ date: d, error: e.message || String(e) });
        log("BATCH", "FAILED " + d + ": " + (e.message || e));
      }
    }
  }

  log("BATCH", "\n═══════════ BATCH SUMMARY ═══════════");
  log("BATCH", "Total dates:  " + datesToProcess.length);
  log("BATCH", "Success:      " + results.success.length + " " + (results.success.length ? results.success.join(", ") : ""));
  log("BATCH", "No IEM data:  " + results.noData.length + " " + (results.noData.length ? results.noData.join(", ") : ""));
  log("BATCH", "Failed:       " + results.failed.length + " " + (results.failed.length ? results.failed.map(f => f.date + "(" + f.error.slice(0,60) + ")").join(", ") : ""));
  log("BATCH", "═════════════════════════════════════");

  if (results.failed.length > 0) process.exit(1);
}

entryPoint().catch(e => { console.error("Fatal:", e); process.exit(1); });

