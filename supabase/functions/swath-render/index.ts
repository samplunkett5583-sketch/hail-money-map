// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

declare const Deno: {
  env: { get(key: string): string | undefined };
};

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders },
  });
}

// ─── Haversine distance in km ───
function haversineKm(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number {
  const R = 6371;
  const toRad = (x: number) => (x * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}

// ─── Simple DBSCAN clustering ───
interface Pt {
  lat: number;
  lon: number;
  hail_in: number;
  event_time: string | null;
  ts: number; // epoch ms (or 0)
}

function dbscan(points: Pt[], epsKm: number, minPts: number): number[] {
  const n = points.length;
  const labels = new Array<number>(n).fill(-1); // -1 = unvisited
  let clusterId = 0;

  function regionQuery(idx: number): number[] {
    const neighbors: number[] = [];
    const p = points[idx];
    for (let j = 0; j < n; j++) {
      if (j === idx) continue;
      if (haversineKm(p.lat, p.lon, points[j].lat, points[j].lon) <= epsKm) {
        neighbors.push(j);
      }
    }
    return neighbors;
  }

  for (let i = 0; i < n; i++) {
    if (labels[i] !== -1) continue;
    const neighbors = regionQuery(i);
    if (neighbors.length < minPts) {
      labels[i] = -2; // noise
      continue;
    }
    labels[i] = clusterId;
    const seed = [...neighbors];
    for (let si = 0; si < seed.length; si++) {
      const qi = seed[si];
      if (labels[qi] === -2) labels[qi] = clusterId; // noise → border
      if (labels[qi] !== -1) continue;
      labels[qi] = clusterId;
      const qNeighbors = regionQuery(qi);
      if (qNeighbors.length >= minPts) {
        for (const nn of qNeighbors) {
          if (seed.indexOf(nn) === -1) seed.push(nn);
        }
      }
    }
    clusterId++;
  }
  return labels;
}

// ─── Catmull-Rom interpolation (uniform) ───
function catmullRomPoint(
  p0: number[],
  p1: number[],
  p2: number[],
  p3: number[],
  t: number,
): number[] {
  const t2 = t * t;
  const t3 = t2 * t;
  const out: number[] = [];
  for (let d = 0; d < p0.length; d++) {
    out.push(
      0.5 *
        (2 * p1[d] +
          (-p0[d] + p2[d]) * t +
          (2 * p0[d] - 5 * p1[d] + 4 * p2[d] - p3[d]) * t2 +
          (-p0[d] + 3 * p1[d] - 3 * p2[d] + p3[d]) * t3),
    );
  }
  return out;
}

function smoothCenterline(
  rawPts: number[][],
  subdivisions: number,
): number[][] {
  if (rawPts.length <= 2) return rawPts;
  const pts = rawPts;
  const result: number[][] = [pts[0]];
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(0, i - 1)];
    const p1 = pts[i];
    const p2 = pts[i + 1];
    const p3 = pts[Math.min(pts.length - 1, i + 2)];
    for (let s = 1; s <= subdivisions; s++) {
      result.push(catmullRomPoint(p0, p1, p2, p3, s / subdivisions));
    }
  }
  return result;
}

// ─── Perpendicular spread of nearby points from a segment ───
function perpSpread(
  segLat: number,
  segLon: number,
  dirLat: number,
  dirLon: number,
  cluster: Pt[],
  maxDistKm: number,
): { widthKm: number; maxHail: number; count: number } {
  let maxPerp = 0;
  let maxHail = 0;
  let count = 0;
  const dirLen = Math.sqrt(dirLat * dirLat + dirLon * dirLon) || 1;
  const nLat = -dirLon / dirLen;
  const nLon = dirLat / dirLen;
  for (const p of cluster) {
    const dLat = p.lat - segLat;
    const dLon = p.lon - segLon;
    const along = dLat * (dirLat / dirLen) + dLon * (dirLon / dirLen);
    if (Math.abs(along) * 111 > maxDistKm) continue;
    const perp = Math.abs(dLat * nLat + dLon * nLon) * 111; // crude km
    if (perp < maxDistKm) {
      maxPerp = Math.max(maxPerp, perp);
      maxHail = Math.max(maxHail, p.hail_in || 0);
      count++;
    }
  }
  return { widthKm: Math.max(maxPerp * 2, 5), maxHail, count };
}

// ─── Build one corridor descriptor from a cluster of points ───
interface Corridor {
  id: number;
  centerline: number[][]; // [lon, lat] pairs
  widthProfile: number[]; // km per centerline point
  severityProfile: number[]; // max hail_in per centerline point
  maxHailIn: number;
  reportCount: number;
}

function buildCorridor(id: number, cluster: Pt[]): Corridor | null {
  if (cluster.length < 2) return null;

  // Sort by event_time when available, else by latitude (N→S storm motion assumption)
  const hasTime = cluster.filter((p) => p.ts > 0).length > cluster.length * 0.5;
  const sorted = [...cluster].sort((a, b) =>
    hasTime ? a.ts - b.ts : b.lat - a.lat,
  );

  // Build raw centerline from sorted points
  const rawCentroids: number[][] = [];
  // Group into segments of ~3 points for local centroids (smoothing)
  const groupSize = Math.max(1, Math.floor(sorted.length / Math.max(3, Math.ceil(sorted.length / 3))));
  for (let i = 0; i < sorted.length; i += groupSize) {
    const grp = sorted.slice(i, i + groupSize);
    const cLat = grp.reduce((s, p) => s + p.lat, 0) / grp.length;
    const cLon = grp.reduce((s, p) => s + p.lon, 0) / grp.length;
    rawCentroids.push([cLon, cLat]);
  }
  // Ensure at least 2 centroids
  if (rawCentroids.length < 2) {
    rawCentroids.push([
      sorted[sorted.length - 1].lon,
      sorted[sorted.length - 1].lat,
    ]);
  }

  // Smooth the centerline with Catmull-Rom
  const subdivs = Math.min(6, Math.max(2, Math.ceil(20 / rawCentroids.length)));
  const centerline = smoothCenterline(rawCentroids, subdivs);

  // Compute width and severity profiles
  const widthProfile: number[] = [];
  const severityProfile: number[] = [];
  let maxHailIn = 0;
  const segLen =
    haversineKm(
      centerline[0][1],
      centerline[0][0],
      centerline[centerline.length - 1][1],
      centerline[centerline.length - 1][0],
    ) / (centerline.length || 1);
  const searchRadius = Math.max(15, segLen * 3);

  for (let i = 0; i < centerline.length; i++) {
    const cLon = centerline[i][0];
    const cLat = centerline[i][1];
    // Direction vector
    const next = centerline[Math.min(i + 1, centerline.length - 1)];
    const prev = centerline[Math.max(i - 1, 0)];
    const dirLat = next[1] - prev[1];
    const dirLon = next[0] - prev[0];

    const spread = perpSpread(cLat, cLon, dirLat, dirLon, cluster, searchRadius);
    widthProfile.push(spread.widthKm);
    severityProfile.push(spread.maxHail);
    if (spread.maxHail > maxHailIn) maxHailIn = spread.maxHail;
  }

  // Taper the ends (first/last 20% of profile)
  const taperLen = Math.max(1, Math.floor(centerline.length * 0.2));
  for (let i = 0; i < taperLen; i++) {
    const factor = (i + 1) / (taperLen + 1);
    widthProfile[i] *= factor;
    widthProfile[centerline.length - 1 - i] *= factor;
  }

  return {
    id,
    centerline,
    widthProfile,
    severityProfile,
    maxHailIn,
    reportCount: cluster.length,
  };
}

// ─── Main handler ───
serve(async (req: Request) => {
  if (req.method === "OPTIONS")
    return new Response(null, { status: 204, headers: corsHeaders });
  if (req.method !== "GET") return json({ error: "Method not allowed" }, 405);

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const supabaseAnonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";
    const serviceRoleKey = Deno.env.get("SERVICE_ROLE_KEY");
    const supabaseKey = serviceRoleKey ?? supabaseAnonKey;

    if (!supabaseUrl || !supabaseKey) {
      return json({ error: "Missing SUPABASE_URL or SUPABASE_ANON_KEY" }, 500);
    }

    const url = new URL(req.url);
    const date = url.searchParams.get("date")?.trim() ?? "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return json({ error: "Invalid date. Use date=YYYY-MM-DD" }, 400);
    }

    const globalHeaders: Record<string, string> = {};
    const authHeader = req.headers.get("Authorization");
    if (authHeader) globalHeaders["Authorization"] = authHeader;

    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: { persistSession: false },
      global: { headers: globalHeaders },
    });

    // Fetch hail_lsr_raw points for this date
    const { data, error } = await supabase
      .from("hail_lsr_raw")
      .select("lat, lon, hail_in, event_time")
      .eq("event_date", date)
      .order("event_time", { ascending: true });

    if (error) return json({ error: error.message }, 500);

    const raw: Pt[] = (data ?? [])
      .map((r: Record<string, unknown>) => ({
        lat: Number(r.lat),
        lon: Number(r.lon),
        hail_in: Number(r.hail_in) || 0,
        event_time: r.event_time ? String(r.event_time) : null,
        ts: r.event_time ? new Date(String(r.event_time)).getTime() : 0,
      }))
      .filter(
        (p: Pt) =>
          Number.isFinite(p.lat) &&
          Number.isFinite(p.lon) &&
          Math.abs(p.lat) <= 90 &&
          Math.abs(p.lon) <= 180,
      );

    if (raw.length === 0) {
      return json({ date, corridors: [], outliers: [], model: "rule_v1", pointCount: 0 });
    }

    // If very few points, build a single corridor from all of them
    if (raw.length <= 3) {
      const corr = buildCorridor(0, raw);
      return json({
        date,
        corridors: corr ? [corr] : [],
        outliers: [],
        model: "rule_v1",
        pointCount: raw.length,
      });
    }

    // DBSCAN clustering
    // eps = adaptive: based on median nearest-neighbor distance, clamped
    const nnDists: number[] = [];
    for (const p of raw) {
      let minD = Infinity;
      for (const q of raw) {
        if (p === q) continue;
        const d = haversineKm(p.lat, p.lon, q.lat, q.lon);
        if (d < minD) minD = d;
      }
      nnDists.push(minD);
    }
    nnDists.sort((a, b) => a - b);
    const medianNN = nnDists[Math.floor(nnDists.length / 2)];
    const epsKm = Math.max(10, Math.min(80, medianNN * 3));
    const minPts = Math.max(1, Math.min(3, Math.floor(raw.length * 0.1)));

    const labels = dbscan(raw, epsKm, minPts);

    // Group by cluster
    const clusters = new Map<number, Pt[]>();
    const outliers: number[][] = [];
    for (let i = 0; i < raw.length; i++) {
      const lbl = labels[i];
      if (lbl < 0) {
        outliers.push([raw[i].lon, raw[i].lat, raw[i].hail_in]);
        continue;
      }
      if (!clusters.has(lbl)) clusters.set(lbl, []);
      clusters.get(lbl)!.push(raw[i]);
    }

    // Build corridors
    const corridors: Corridor[] = [];
    let cIdx = 0;
    for (const [, pts] of clusters) {
      const corr = buildCorridor(cIdx, pts);
      if (corr) {
        corridors.push(corr);
        cIdx++;
      }
    }

    // If DBSCAN produced no corridors but we have points, build single corridor
    if (corridors.length === 0 && raw.length >= 2) {
      const corr = buildCorridor(0, raw);
      if (corr) corridors.push(corr);
    }

    return json({
      date,
      corridors,
      outliers,
      model: "rule_v1",
      pointCount: raw.length,
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
