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

const TARGET_DATE = process.argv[2] || "2026-04-01";
if (!/^\d{4}-\d{2}-\d{2}$/.test(TARGET_DATE)) {
  console.error("Invalid date format. Use YYYY-MM-DD");
  process.exit(1);
}

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
const MIN_HAIL_INCHES        = 1.0;  // reject sub-severe max_size
const ISOLATION_RADIUS_DEG   = 2.0;  // ~150 mi at mid-latitudes
const ISOLATION_TIME_HR      = 2;    // ±hours for neighbor match
const ISOLATION_BEARING_DEG  = 30;   // bearing tolerance for same-motion
const MIN_NEIGHBORS          = 1;    // isolated if fewer neighbors than this

// ── Regional cluster pruning (post-scoring) ──
const CLUSTER_RADIUS_MI            = 120;
const CLUSTER_BEARING_TOLERANCE_DEG = 25;
const MAX_KEEP_PER_CLUSTER          = 3;
const MIN_CLUSTER_SCORE_RATIO       = 0.20;
const CONTINUITY_MIN_DIST_MI        = 40;
const CONTINUITY_BEARING_TOL_DEG    = 25;
const CONTINUITY_MAX_EXTRA          = 1;

// ── Buffer sizing ──
const BUF_SCALE    = 0.04;        // degrees per inch
const BUF_SCALE_LO = 0.02;        // minimum scaling
const BUF_SCALE_HI = 0.08;        // maximum scaling
const HW_FLOOR     = 0.01;        // ~1.1 km minimum half-width
const HW_CAP       = 0.06;        // ~6.6 km maximum half-width

function log(t, m) { console.log("[" + t + "] " + m); }
const sleep = ms => new Promise(r => setTimeout(r, ms));

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
  const ring = [...left, ...right, left[0]];
  return ring;
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

// ═══════════════════════════════════════════════════════════
//  Main
// ═══════════════════════════════════════════════════════════
async function main() {
  log("MAIN", "IEM NEXRAD track ingest for " + TARGET_DATE);

  // ── 1. Generate query timestamps ──
  const stamps = genTimestamps(TARGET_DATE);
  log("MAIN", "Timestamps to query: " + stamps.length + " (12Z-06Z, 5min)");

  // ── 2. Fetch each timestamp, keep hail-signal features ──
  const allFeats = [];
  let nTs = 0;
  for (const ts of stamps) {
    const raw = await fetchTs(ts);
    const hail = raw.filter(hasHailSignal);
    if (hail.length > 0) allFeats.push(...hail);
    nTs++;
    if (nTs % 36 === 0) {
      log("FETCH", nTs + "/" + stamps.length + " timestamps  (" + allFeats.length + " hail features so far)");
    }
    await sleep(FETCH_DELAY);
  }
  log("FETCH", "Done: " + nTs + " timestamps queried, " + allFeats.length + " total hail features kept");

  if (allFeats.length === 0) {
    log("MAIN", "No hail features found. Nothing to save.");
    return;
  }

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

    // 5a — minimum track points
    if (feats.length < MIN_PTS) { nRej++; continue; }

    // 5b — bounding-box span
    const bb = bboxOf(coords);
    if (bb.maxLat - bb.minLat > MAX_BBOX_LAT || bb.maxLon - bb.minLon > MAX_BBOX_LON) {
      log("REJECT", key + ": bbox " + (bb.maxLat - bb.minLat).toFixed(1) + "° x " +
        (bb.maxLon - bb.minLon).toFixed(1) + "°");
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
    if (jumpBad) { nRej++; continue; }

    // 5d — deduplicate consecutive identical coords
    const uP = [coords[0]], uS = [sizes[0]];
    for (let i = 1; i < coords.length; i++) {
      if (coords[i][0] !== coords[i - 1][0] || coords[i][1] !== coords[i - 1][1]) {
        uP.push(coords[i]); uS.push(sizes[i]);
      }
    }
    if (uP.length < 2) { nRej++; continue; }

    // 5e — half-widths from max_size
    const nonZero = uS.filter(s => s > 0);
    const repMS = nonZero.length > 0 ? Math.max(...nonZero) : 0.5;
    const hws = uS.map(ms => {
      const eff = ms > 0 ? ms : repMS * 0.5;
      let hw = eff * BUF_SCALE;
      hw = Math.max(hw, eff * BUF_SCALE_LO);
      hw = Math.min(hw, eff * BUF_SCALE_HI);
      return Math.min(Math.max(hw, HW_FLOOR), HW_CAP);
    });

    // 5f — build polygon
    let ring = bufferLine(uP, hws);
    let selfInt = ringSelfIntersects(ring);
    let usedHull = false;
    if (selfInt) {
      const trackSpan = Math.max(bb.maxLon - bb.minLon, bb.maxLat - bb.minLat);
      if (trackSpan > HULL_SPAN_CAP) {
        log("REJECT", key + ": hull fallback on wide track (" + trackSpan.toFixed(2) + "°)");
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
      log("REJECT", key + ": polygon bbox too large");
      nRej++; continue;
    }

    const area = areaSqMi(ring);

    // 5h — area cap
    if (area > MAX_AREA) {
      log("REJECT", key + ": area " + area.toFixed(0) + " mi² > cap");
      nRej++; continue;
    }

    // 5i — first-to-last distance cap
    const fLon = uP[0][0], fLat = uP[0][1];
    const lLon = uP[uP.length - 1][0], lLat = uP[uP.length - 1][1];
    const midLat2 = (fLat + lLat) / 2;
    const cosL2 = Math.cos(midLat2 * Math.PI / 180);
    const f2lMi = Math.sqrt(((lLon - fLon) * 111.32 * cosL2) ** 2 + ((lLat - fLat) * 111.32) ** 2) * 0.621371;
    if (f2lMi > MAX_TRACK_MI) {
      log("REJECT", key + ": first-to-last " + f2lMi.toFixed(0) + " mi > cap");
      nRej++; continue;
    }

    // 5j — minimum ring points
    if (ring.length < MIN_RING_PTS) {
      log("REJECT", key + ": ring only " + ring.length + " pts");
      nRej++; continue;
    }

    // 5k — convex hull bbox vs track bbox ratio
    const hullRing = convexHull(ring.slice(0, -1));
    const hbb = bboxOf(hullRing);
    const trackW = (bb.maxLon - bb.minLon) || 0.001;
    const trackH = (bb.maxLat - bb.minLat) || 0.001;
    if ((hbb.maxLon - hbb.minLon) / trackW > HULL_BBOX_RATIO ||
        (hbb.maxLat - hbb.minLat) / trackH > HULL_BBOX_RATIO) {
      log("REJECT", key + ": hull bbox ratio exceeded");
      nRej++; continue;
    }

    // 5l — motion direction consistency
    if (!motionConsistent(uP, MAX_DIR_DEV)) {
      log("REJECT", key + ": heading deviation > " + MAX_DIR_DEV + "°");
      nRej++; continue;
    }

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
  }
  log("DEDUP", "After cross-radar dedup: " + deduped.length +
    " (removed " + (corridors.length - deduped.length) + ")");

  if (!deduped.length) {
    log("MAIN", "No corridors after dedup.");
    return;
  }

  // ── 5.6 Quality filters ──
  const rejStats = { minPts: 0, minHail: 0, area: 0, isolation: 0, clusterPrune: 0 };
  let qualified = [];

  for (const c of deduped) {
    if (c.nPts < MIN_TRACK_POINTS) { rejStats.minPts++; continue; }
    if (c.repMS < MIN_HAIL_INCHES) { rejStats.minHail++; continue; }
    if (c.area < MIN_AREA_SQ_MI && c.nPts < MIN_AREA_SOFT_PTS) { rejStats.area++; continue; }
    qualified.push(c);
  }
  log("FILTER", "After quality gates: " + qualified.length +
    " (minPts=" + rejStats.minPts + " minHail=" + rejStats.minHail + " area=" + rejStats.area + ")");

  // ── 5.7 Isolation filter — reject corridors with too few neighbors ──
  const withNeighbors = [];
  for (const c of qualified) {
    const nb = countNeighbors(c, qualified);
    c._neighbors = nb;
    if (nb < MIN_NEIGHBORS) { rejStats.isolation++; continue; }
    withNeighbors.push(c);
  }
  qualified = withNeighbors;
  log("FILTER", "After isolation filter: " + qualified.length +
    " (isolated=" + rejStats.isolation + ")");

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

  const final = [];
  let clusterDropped = 0;
  const clusterDebug = [];
  for (const [root, members] of clusterMap) {
    const topScore = members[0]._score;
    const domBearing = members[0]._bearing;
    const kept = [];
    let keptByScore = 0;
    let keptByContinuity = 0;
    let dropped = 0;
    let continuityExtras = 0;

    for (const c of members) {
      if (kept.length >= MAX_KEEP_PER_CLUSTER) { dropped++; clusterDropped++; continue; }

      // Normal score-ratio pass
      if (c._score >= topScore * MIN_CLUSTER_SCORE_RATIO) {
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
      " kept=" + d.kept + "(score=" + d.keptByScore + " cont=" + d.keptByContinuity + ")" +
      " dropped=" + d.dropped);
  }

  // ── 5.11 Final summary ──
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

  // ── 6. Delete existing nexrad_iem rows for this date ──
  const { error: delErr } = await supabase
    .from("storm_polygons")
    .delete()
    .eq("event_date", TARGET_DATE)
    .eq("source", "nexrad_iem");
  if (delErr) log("ERROR", "delete: " + delErr.message);

  // ── 7. Save corridors ──
  let nSaved = 0;
  for (let i = 0; i < final.length; i++) {
    const c = final[i];
    const [nxr, sid] = c.key.split(":");

    const geojson = {
      type: "Feature",
      geometry: { type: "Polygon", coordinates: [c.ring] },
      properties: {
        nexrad: nxr,
        storm_id: sid,
        track_points: c.nPts,
        max_hail_inches: c.repMS,
        event_start_utc: c.firstTime,
        event_end_utc: c.lastTime,
      },
    };

    const row = {
      event_date: TARGET_DATE,
      storm_type: "hail",
      band_min: c.repMS,
      band_max: c.repMS,
      band_label: c.repMS + "\" track",
      polygon_geojson: geojson,
      centroid_lat: c.centroid.lat,
      centroid_lon: c.centroid.lon,
      area_sq_mi: parseFloat(c.area.toFixed(2)),
      source: "nexrad_iem",
      source_product: "iem_nexrad_track",
      source_priority: 1,
      swath_index: i,
    };

    const { error } = await supabase.from("storm_polygons").upsert([row], {
      onConflict: "event_date,source,source_product,swath_index",
    });
    if (error) {
      log("ERROR", "upsert " + c.key + ": " + error.message);
    } else {
      nSaved++;
    }

    // ── per-corridor debug ──
    log("SAVED", [
      c.key,
      "pts=" + c.nPts,
      c.firstTime + " → " + c.lastTime,
      "first=[" + c.firstCoord[1].toFixed(3) + "," + c.firstCoord[0].toFixed(3) + "]",
      "last=[" + c.lastCoord[1].toFixed(3) + "," + c.lastCoord[0].toFixed(3) + "]",
      "max_size=" + c.repMS + "\"",
      "bbox=[" + c.bbox.minLat.toFixed(2) + "," + c.bbox.minLon.toFixed(2) +
        " → " + c.bbox.maxLat.toFixed(2) + "," + c.bbox.maxLon.toFixed(2) + "]",
      "ring=" + c.ring.length + "pts",
      "selfInt=" + c.selfInt,
      "area=" + c.area.toFixed(1) + "mi²",
      "PASS",
    ].join(" | "));
  }

  log("MAIN", "Done. Saved " + nSaved + "/" + final.length +
    " corridors to storm_polygons (source=nexrad_iem)");
}

main().catch(e => { console.error("Fatal:", e); process.exit(1); });
