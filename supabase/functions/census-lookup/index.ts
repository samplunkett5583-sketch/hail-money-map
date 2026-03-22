// Supabase Edge Function: census-lookup
// Proxies Census Geocoder + ACS API server-side to avoid browser CORS restrictions.
//
// POST body: { points: Array<{ streetName: string, lat: number, lng: number }> }
// Response:  { values: Record<string, number>, resolved: number, total: number }
//   values — streetName → ACS B25077_001E (median home value) via spatial proximity matching
//
// Strategy: spatially sample up to MAX_GEOCODE points from the input set,
// geocode only those (with full diagnostic logging), fetch ACS for the resolved
// tracts, then proximity-match every input street to the nearest sampled point.

import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, *",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders },
  });
}

interface SamplePoint {
  streetName: string;
  lat: number;
  lng: number;
}

interface GeocodeResult {
  streetName: string;
  lat: number;
  lng: number;
  geoid: string | null;
  st: string | null;
  co: string | null;
}

// Max geocoder requests — Census geocoder rate-limits at high concurrency.
const MAX_GEOCODE = 25;

// Valid vintages for benchmark=Public_AR_Current:
//   "Current_Current"     — current year tracts (primary)
//   "Census2020_Current"  — 2020 tract boundaries mapped to current FIPS
const VINTAGES = ["Current_Current", "Census2020_Current"];

/** Fetch with a hard timeout and a browser-like User-Agent (Census API rejects headless UA). */
async function fetchWithTimeout(url: string, timeoutMs: number): Promise<Response> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; HailMapCensusProxy/1.0)",
        "Accept": "application/json",
      },
    });
    clearTimeout(timer);
    return resp;
  } catch (e) {
    clearTimeout(timer);
    throw e;
  }
}

/**
 * Geocode a single lat/lng to a Census tract GEOID.
 * Tries each vintage in VINTAGES until one succeeds.
 * Logs the raw response body for the FIRST attempt at each vintage so failures are visible.
 */
async function geocodePoint(
  pt: SamplePoint,
  isFirstPoint: boolean
): Promise<GeocodeResult> {
  const base: GeocodeResult = {
    streetName: pt.streetName, lat: pt.lat, lng: pt.lng,
    geoid: null, st: null, co: null,
  };

  for (const vintage of VINTAGES) {
    const url =
      `https://geocoding.geo.census.gov/geocoder/geographies/coordinates` +
      `?x=${pt.lng.toFixed(6)}&y=${pt.lat.toFixed(6)}` +
      `&benchmark=Public_AR_Current&vintage=${vintage}&format=json`;

    // Always log the URL for the first point; log it once per vintage for later points
    if (isFirstPoint) {
      console.log(`[census-lookup] geocoder URL (vintage=${vintage}): ${url}`);
    }

    let rawText = "";
    try {
      // Log the full URL for EVERY point on the first vintage so we can verify all coords
      if (vintage === VINTAGES[0]) {
        console.log(
          `[census-lookup] geocoder URL: ${url}`
        );
      }

      const resp = await fetchWithTimeout(url, 15000);
      rawText = await resp.text();

      // Always log the raw response for the first 3 points (isFirstPoint is the very first).
      // Log for all points on failure so nothing is silently dropped.
      if (isFirstPoint) {
        console.log(
          `[census-lookup] geocoder raw response (HTTP ${resp.status}, vintage=${vintage}, point=0): ` +
          rawText.slice(0, 1200)
        );
      }

      if (!resp.ok) {
        console.error(
          `[census-lookup] geocoder HTTP ${resp.status} for "${pt.streetName}" ` +
          `(lat=${pt.lat.toFixed(5)} lng=${pt.lng.toFixed(5)}) vintage=${vintage}: ` +
          rawText.slice(0, 400)
        );
        continue; // try next vintage
      }

      let j: unknown;
      try {
        j = JSON.parse(rawText);
      } catch (parseErr) {
        console.error(
          `[census-lookup] geocoder JSON parse error for "${pt.streetName}" vintage=${vintage}: ` +
          `${(parseErr as Error).message} | body snippet: ${rawText.slice(0, 300)}`
        );
        continue;
      }

      // Always log geographies keys — this shows whether Census returned tract data
      const resultObj = (j as { result?: { geographies?: Record<string, unknown> } })?.result;
      const geoObj = resultObj?.geographies;
      const geoKeys = Object.keys(geoObj ?? {});
      console.log(
        `[census-lookup] geographies keys for "${pt.streetName}" (vintage=${vintage}): ` +
        JSON.stringify(geoKeys)
      );

      // Log any errors Census embedded in the result
      const resultErrors = (resultObj as Record<string, unknown>)?.errors;
      if (resultErrors) {
        console.error(
          `[census-lookup] Census result.errors for "${pt.streetName}": ` +
          JSON.stringify(resultErrors)
        );
      }

      const tractsArr = (geoObj as Record<string, unknown>)?.["Census Tracts"];
      const t = Array.isArray(tractsArr) ? tractsArr[0] : null;

      if (!t) {
        console.error(
          `[census-lookup] no "Census Tracts" in geographies for "${pt.streetName}" ` +
          `(lat=${pt.lat.toFixed(5)} lng=${pt.lng.toFixed(5)}) vintage=${vintage}. ` +
          `geographies keys: ${JSON.stringify(geoKeys)}. ` +
          `Full result snippet: ${JSON.stringify(resultObj).slice(0, 500)}`
        );
        continue; // try next vintage
      }

      const tObj = t as Record<string, unknown>;
      // Log raw field values before padding so we can spot unexpected types
      console.log(
        `[census-lookup] tract fields for "${pt.streetName}": ` +
        `STATE=${JSON.stringify(tObj["STATE"])} COUNTY=${JSON.stringify(tObj["COUNTY"])} TRACT=${JSON.stringify(tObj["TRACT"])}`
      );

      const st = String(tObj["STATE"] ?? "").padStart(2, "0");
      const co = String(tObj["COUNTY"] ?? "").padStart(3, "0");
      const tr = String(tObj["TRACT"] ?? "").padStart(6, "0");

      if (!st || !co || !tr || st === "00" || co === "000") {
        console.error(
          `[census-lookup] incomplete STATE/COUNTY/TRACT for "${pt.streetName}" ` +
          `(vintage=${vintage}): STATE=${tObj["STATE"]} COUNTY=${tObj["COUNTY"]} TRACT=${tObj["TRACT"]}`
        );
        continue;
      }

      const geoid = st + co + tr;
      console.log(
        `[census-lookup] RESOLVED "${pt.streetName}" ` +
        `lat=${pt.lat.toFixed(5)} lng=${pt.lng.toFixed(5)} ` +
        `→ STATE=${st} COUNTY=${co} TRACT=${tr} GEOID=${geoid} [vintage=${vintage}]`
      );
      return { ...base, st, co, geoid };

    } catch (e) {
      const msg = (e as Error).message;
      console.warn(
        `[census-lookup] geocoder fetch error for "${pt.streetName}" vintage=${vintage}: ${msg}`
      );
      // Don't break — try next vintage
    }
  }

  return base; // all vintages failed
}

serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { status: 200, headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  let body: { points?: unknown };
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const rawPoints = Array.isArray(body?.points) ? body.points : [];
  const points: SamplePoint[] = rawPoints.filter(
    (p): p is SamplePoint =>
      p !== null &&
      typeof p === "object" &&
      typeof (p as SamplePoint).streetName === "string" &&
      Number.isFinite((p as SamplePoint).lat) &&
      Number.isFinite((p as SamplePoint).lng)
  );

  if (!points.length) {
    return json({ error: "No valid points provided" }, 400);
  }

  console.log(`[census-lookup] ${points.length} input streets received`);

  // ── 1. Spatially sample up to MAX_GEOCODE points ───────────────────────────
  const sorted = [...points].sort((a, b) =>
    a.lat !== b.lat ? a.lat - b.lat : a.lng - b.lng
  );
  const samplePoints: SamplePoint[] =
    sorted.length <= MAX_GEOCODE
      ? sorted
      : Array.from({ length: MAX_GEOCODE }, (_, i) =>
          sorted[Math.floor((i * sorted.length) / MAX_GEOCODE)]
        );

  console.log(
    `[census-lookup] geocoding ${samplePoints.length} spatially-sampled points ` +
    `(of ${points.length} total, cap=${MAX_GEOCODE})`
  );

  // Log first 3 sample points
  for (let i = 0; i < Math.min(3, samplePoints.length); i++) {
    const p = samplePoints[i];
    console.log(
      `[census-lookup] sample[${i}]: "${p.streetName}" lat=${p.lat.toFixed(5)} lng=${p.lng.toFixed(5)}`
    );
  }

  // ── 2. Geocode sample points — FIRST point is sequential for full diagnostics ──
  // Run the first point alone so its raw logs appear before the parallel batch.
  const geocodeResults: GeocodeResult[] = [];
  if (samplePoints.length > 0) {
    geocodeResults.push(await geocodePoint(samplePoints[0], true));
  }
  // Remaining points in parallel
  const rest = await Promise.all(
    samplePoints.slice(1).map((pt) => geocodePoint(pt, false))
  );
  geocodeResults.push(...rest);

  const withGeoid = geocodeResults.filter(
    (r): r is GeocodeResult & { geoid: string; st: string; co: string } =>
      r.geoid !== null && r.st !== null && r.co !== null
  );

  console.log(
    `[census-lookup] ${withGeoid.length}/${samplePoints.length} sample points resolved to Census tracts`
  );

  if (!withGeoid.length) {
    console.error(
      "[census-lookup] FATAL: 0 sample points resolved to Census tracts. " +
      "Possible causes: (1) Census geocoder outage, (2) coordinates outside USA, " +
      "(3) both vintages rejected, (4) Supabase Edge network blocked geocoding.geo.census.gov. " +
      "See per-point logs above for raw responses."
    );
    return json({
      values: {},
      resolved: 0,
      total: points.length,
      debug: "0 geocoded — see function logs for raw geocoder response",
    });
  }

  // ── 3. Fetch ACS 5-yr B25077_001E for each unique state+county ────────────
  const stateCounties = [...new Set(withGeoid.map((r) => `${r.st}_${r.co}`))];
  console.log(
    `[census-lookup] ACS fetch for ${stateCounties.length} state/county pair(s): ${stateCounties.join(", ")}`
  );
  const valueByGeoid: Record<string, number> = {};

  await Promise.all(
    stateCounties.map(async (sc) => {
      const [st, co] = sc.split("_");
      const url =
        `https://api.census.gov/data/2022/acs/acs5` +
        `?get=B25077_001E&for=tract:*&in=state:${st}+county:${co}`;
      console.log(`[census-lookup] ACS URL: ${url}`);
      try {
        const resp = await fetchWithTimeout(url, 20000);
        const rawText = await resp.text();

        if (!resp.ok) {
          console.error(
            `[census-lookup] ACS HTTP ${resp.status} for state:${st} county:${co}: ${rawText.slice(0, 300)}`
          );
          return;
        }

        // Log first 200 chars of ACS response to verify structure
        console.log(
          `[census-lookup] ACS raw response start (state:${st} co:${co}): ${rawText.slice(0, 200)}`
        );

        let arr: string[][];
        try {
          arr = JSON.parse(rawText);
        } catch (parseErr) {
          console.error(
            `[census-lookup] ACS JSON parse error state:${st} co:${co}: ${(parseErr as Error).message}`
          );
          return;
        }

        if (!Array.isArray(arr) || arr.length < 2) {
          console.warn(
            `[census-lookup] ACS returned unexpected shape for state:${st} co:${co}: ` +
            `type=${typeof arr} length=${Array.isArray(arr) ? arr.length : "n/a"}`
          );
          return;
        }

        // Log header row to confirm column order
        console.log(`[census-lookup] ACS header row (state:${st} co:${co}): ${JSON.stringify(arr[0])}`);

        let loaded = 0;
        for (let i = 1; i < arr.length; i++) {
          const row = arr[i];
          const [val, s, c2, tr] = row;
          const v = parseInt(val);
          if (v > 0) {
            valueByGeoid[s.padStart(2, "0") + c2.padStart(3, "0") + tr.padStart(6, "0")] = v;
            loaded++;
          }
        }
        console.log(`[census-lookup] ACS state:${st} county:${co} → ${loaded}/${arr.length - 1} tracts with B25077_001E > 0`);
      } catch (e) {
        console.error(`[census-lookup] ACS fetch threw for ${sc}: ${(e as Error).message}`);
      }
    })
  );

  const totalTractsWithValues = Object.keys(valueByGeoid).length;
  console.log(`[census-lookup] ACS total: ${totalTractsWithValues} tracts loaded across all counties`);

  // Log first 3 geoid → value lookups for the resolved sample points
  for (let i = 0; i < Math.min(3, withGeoid.length); i++) {
    const r = withGeoid[i];
    const v = valueByGeoid[r.geoid] ?? 0;
    console.log(
      `[census-lookup] tract_lookup[${i}]: geoid=${r.geoid} → ` +
      (v > 0 ? `$${v.toLocaleString()}` : `no ACS value (geoid not in loaded set)`)
    );
  }

  // ── 4. Build spatially-referenced value cloud ─────────────────────────────
  const sampledWithValues: { lat: number; lng: number; value: number }[] = withGeoid
    .map((r) => ({ lat: r.lat, lng: r.lng, value: valueByGeoid[r.geoid] ?? 0 }))
    .filter((r) => r.value > 0);

  console.log(
    `[census-lookup] ${sampledWithValues.length}/${withGeoid.length} geocoded sample points have ACS values`
  );

  if (!sampledWithValues.length) {
    console.error(
      `[census-lookup] FATAL: tracts resolved (${withGeoid.length}) but 0 have ACS values. ` +
      "Possible causes: (1) ACS 2022 doesn't cover this area, (2) all B25077_001E = -666666666 (suppressed), " +
      "(3) geoid padding mismatch. Check tract_lookup logs above."
    );
    return json({
      values: {},
      resolved: withGeoid.length,
      total: points.length,
      debug: "tracts resolved but 0 ACS values — see function logs",
    });
  }

  // ── 5. Proximity-match ALL input streets to nearest sampled value ──────────
  const values: Record<string, number> = {};
  for (const pt of points) {
    let best = sampledWithValues[0];
    let bestDist = Infinity;
    for (const sv of sampledWithValues) {
      const d = (sv.lat - pt.lat) ** 2 + (sv.lng - pt.lng) ** 2;
      if (d < bestDist) { bestDist = d; best = sv; }
    }
    const cur = values[pt.streetName] ?? 0;
    if (best.value > cur) values[pt.streetName] = best.value;
  }

  const matchedStreets = Object.keys(values).length;
  console.log(
    `[census-lookup] ${matchedStreets}/${points.length} streets assigned Census values via proximity`
  );

  // Log 5 sample street → value assignments
  for (const [name, val] of Object.entries(values).slice(0, 5)) {
    console.log(`[census-lookup] street_sample: "${name}" → $${val.toLocaleString()}`);
  }

  return json({ values, resolved: withGeoid.length, total: points.length });
});
