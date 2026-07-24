#!/usr/bin/env node

import crypto from "node:crypto";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const SERPER_API_KEY = process.env.SERPER_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-3.5-flash-lite";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !GEMINI_API_KEY || !SERPER_API_KEY) {
  console.error(
    "Missing SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY, or SERPER_API_KEY",
  );
  process.exit(1);
}

const args = Object.fromEntries(
  process.argv.slice(2).map((arg) => {
    const [key, ...rest] = arg.replace(/^--/, "").split("=");
    return [key, rest.length ? rest.join("=") : "true"];
  }),
);

const dryRun = args["dry-run"] === "true";
const force = args.force === "true";
const refreshHours = positiveNumber(args["refresh-hours"], 12);
const limit = Math.min(5000, positiveNumber(args.limit, 250));
const offset = Math.max(0, Number(args.offset) || 0);
const maxRegions = Math.min(20, positiveNumber(args["max-regions"], 12));
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function positiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function asDate(value) {
  const match = String(value || "").match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : "";
}

function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) &&
    !Number.isNaN(Date.parse(`${value}T00:00:00Z`));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function haversineMiles(lat1, lon1, lat2, lon2) {
  const rad = Math.PI / 180;
  const dLat = (lat2 - lat1) * rad;
  const dLon = (lon2 - lon1) * rad;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * rad) * Math.cos(lat2 * rad) *
      Math.sin(dLon / 2) ** 2;
  return 3958.8 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function sha(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function hostname(value) {
  try {
    return new URL(value).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return "";
  }
}

function sourceConfidence(report) {
  const host = hostname(report.source_url);
  const kind = String(report.source_kind || "").toLowerCase();
  const corroboration = Number(report.corroborating_sources) || 0;
  if (
    host === "weather.gov" || host.endsWith(".weather.gov") ||
    host === "noaa.gov" || host.endsWith(".noaa.gov") ||
    host === "nssl.noaa.gov" || host.endsWith(".nssl.noaa.gov") ||
    host === "mesonet.agron.iastate.edu"
  ) return 0.97;
  if (host.endsWith(".gov") || kind === "emergency_management") return 0.92;
  if (kind === "trained_spotter" || kind === "law_enforcement") return 0.88;
  if (kind === "local_news" && report.explicit_measurement === true) return 0.82;
  if (report.explicit_measurement === true && corroboration >= 2) return 0.80;
  return 0.55;
}

function parseJson(text) {
  const cleaned = text
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();
  try {
    return JSON.parse(cleaned);
  } catch {
    const start = cleaned.indexOf("{");
    const end = cleaned.lastIndexOf("}");
    if (start >= 0 && end > start) return JSON.parse(cleaned.slice(start, end + 1));
    throw new Error("Hail evidence response did not contain valid JSON");
  }
}

async function getDates() {
  const requestedDate = asDate(args.date);
  if (requestedDate) {
    if (!validDate(requestedDate)) throw new Error(`Invalid --date=${args.date}`);
    return [requestedDate];
  }

  const days = Number(args.days);
  const cutoff = Number.isFinite(days) && days > 0
    ? (() => {
        const value = new Date();
        value.setUTCDate(value.getUTCDate() - Math.ceil(days));
        return value.toISOString().slice(0, 10);
      })()
    : "";

  const rpcResult = await supabase.rpc("get_storm_distinct_dates");
  if (!rpcResult.error && Array.isArray(rpcResult.data)) {
    const dates = [...new Set(
      rpcResult.data
        .map((row) => asDate(row.event_date))
        .filter((date) => validDate(date) && (!cutoff || date >= cutoff)),
    )].sort().reverse();
    return dates.slice(offset, offset + limit);
  }

  // Older deployments may not have the consolidated RPC. Build the date list
  // from source tables instead of depending on an optional database view.
  const tables = [
    "hail_lsr_raw",
    "storm_lsr_raw",
    "storm_polygons",
    "hail_radar_polygons",
  ];
  const found = new Set();
  const errors = [];
  for (const table of tables) {
    let query = supabase
      .from(table)
      .select("event_date")
      .order("event_date", { ascending: false });
    if (cutoff) query = query.gte("event_date", cutoff);
    const result = await query.range(0, 4999);
    if (result.error) {
      errors.push(`${table}: ${result.error.message}`);
      continue;
    }
    for (const row of result.data || []) {
      const date = asDate(row.event_date);
      if (validDate(date)) found.add(date);
    }
  }
  const dates = [...found].sort().reverse();
  if (dates.length === 0 && errors.length === tables.length) {
    throw new Error(`Could not load storm dates: ${errors.join("; ")}`);
  }
  return dates.slice(offset, offset + limit);
}

async function getAnchors(date) {
  const [lsrResult, radarResult] = await Promise.all([
    supabase
      .from("hail_lsr_raw")
      .select("lat,lon,state,county,hail_in,source")
      .eq("event_date", date)
      .limit(5000),
    supabase
      .from("hail_radar_polygons")
      .select("centroid_lat,centroid_lon,band_min,band_max")
      .eq("event_date", date)
      .limit(2000),
  ]);
  if (lsrResult.error) throw new Error(lsrResult.error.message);

  const anchors = (lsrResult.data || []).map((row) => ({
    lat: Number(row.lat),
    lon: Number(row.lon),
    state: row.state || "",
    county: row.county || "",
    hail: Number(row.hail_in) || 0,
  }));
  if (!radarResult.error) {
    for (const row of radarResult.data || []) {
      anchors.push({
        lat: Number(row.centroid_lat),
        lon: Number(row.centroid_lon),
        state: "",
        county: "",
        hail: Number(row.band_max || row.band_min) || 0,
      });
    }
  }
  return anchors.filter((p) =>
    Number.isFinite(p.lat) && Number.isFinite(p.lon) &&
    p.lat >= 24 && p.lat <= 50 && p.lon >= -125 && p.lon <= -66
  );
}

function buildRegions(anchors) {
  const buckets = new Map();
  for (const point of anchors) {
    const key = `${Math.floor(point.lat / 2)}:${Math.floor(point.lon / 2)}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(point);
  }
  return [...buckets.entries()]
    .map(([key, points]) => {
      const lat = points.reduce((sum, p) => sum + p.lat, 0) / points.length;
      const lon = points.reduce((sum, p) => sum + p.lon, 0) / points.length;
      const states = [...new Set(points.map((p) => p.state).filter(Boolean))];
      const counties = [...new Set(points.map((p) => p.county).filter(Boolean))].slice(0, 12);
      const existingMax = Math.max(0, ...points.map((p) => p.hail));
      return { key, lat, lon, states, counties, existingMax, anchors: points };
    })
    .sort((a, b) => b.existingMax - a.existingMax || b.anchors.length - a.anchors.length)
    .slice(0, maxRegions);
}

async function searchGoogle(date, region) {
  const humanDate = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC",
    month: "long",
    day: "numeric",
    year: "numeric",
  }).format(new Date(`${date}T12:00:00Z`));
  const place = [
    ...region.counties.slice(0, 2).map((county) => `${county} County`),
    ...region.states.slice(0, 2),
  ].filter(Boolean).join(" ");
  const query = [
    humanDate,
    place,
    "hail size report",
  ].filter(Boolean).join(" ");

  const searchResponse = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: {
      "X-API-KEY": SERPER_API_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      q: query,
      gl: "us",
      hl: "en",
      num: 20,
    }),
  });
  const searchBody = await searchResponse.json().catch(() => ({}));
  if (!searchResponse.ok) {
    throw new Error(
      `Google Search provider ${searchResponse.status}: ${JSON.stringify(searchBody).slice(0, 500)}`,
    );
  }
  const candidates = [...(searchBody.organic || []), ...(searchBody.news || [])]
    .filter((result) =>
      typeof result?.link === "string" &&
      result.link.startsWith("https://") &&
      (result.title || result.snippet)
    )
    .slice(0, 20)
    .map((result, index) => ({
      index: index + 1,
      title: String(result.title || "").slice(0, 300),
      url: result.link,
      snippet: String(result.snippet || "").slice(0, 1000),
      published: String(result.date || ""),
    }));
  const citations = candidates.map(({ url, title }) => ({ url, title }));
  if (candidates.length === 0) return { reports: [], citations };

  const regionDescription = [
    region.states.length ? `states ${region.states.join(", ")}` : "",
    region.counties.length ? `counties ${region.counties.join(", ")}` : "",
    `near ${region.lat.toFixed(3)}, ${region.lon.toFixed(3)}`,
  ].filter(Boolean).join("; ");
  const prompt = `
Review the supplied Google Search results for ground-observed hail reports on
${date} in this region: ${regionDescription}.
The current database maximum is ${region.existingMax.toFixed(2)} inches.

Do not use radar-estimated hail size as a ground observation. Do not copy a
forecast, warning threshold, or generic statement. Do not infer a size from
damage. The source must explicitly state the observed hail size and location.
Return only reports for ${date}. Deduplicate the same observation. Use only the
information in the supplied results. Never invent a URL, measurement, date,
location, coordinates, or source. If coordinates are not present, return null
for lat and lon.

Convert an explicitly reported standard hail-size description using this table:
quarter=1.00, half dollar=1.25, ping pong=1.50, golf ball=1.75,
hen egg=2.00, tennis ball=2.50, baseball=2.75, tea cup=3.00,
softball=4.00, grapefruit=4.50 inches. This conversion is allowed only when the
result explicitly says that description was observed or reported.

Google Search results:
${JSON.stringify(candidates, null, 2)}

Return JSON only:
{
  "reports": [{
    "event_date": "YYYY-MM-DD",
    "event_time_utc": "ISO timestamp or null",
    "hail_inches": 1.75,
    "city": "city or null",
    "state": "2-letter state",
    "county": "county or null",
    "lat": "number or null",
    "lon": "number or null",
    "source_result_index": 1,
    "source_kind": "nws_noaa|trained_spotter|emergency_management|law_enforcement|local_news|other",
    "explicit_measurement": true,
    "corroborating_sources": 1,
    "observation_text": "20 words or fewer stating the evidence"
  }]
}
If no qualifying observation is found, return {"reports":[]}.
`.trim();

  const response = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent`,
    {
      method: "POST",
      headers: {
        "x-goog-api-key": GEMINI_API_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: prompt }] }],
        generationConfig: {
          responseMimeType: "application/json",
          temperature: 0.1,
        },
      }),
    },
  );
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Gemini evidence review ${response.status}: ${JSON.stringify(body).slice(0, 500)}`);
  }
  const text = (body?.candidates?.[0]?.content?.parts || [])
    .map((part) => part?.text || "")
    .join("\n")
    .trim();
  const parsed = parseJson(text);
  const reports = (Array.isArray(parsed?.reports) ? parsed.reports : [])
    .map((report) => {
      const source = candidates[Number(report.source_result_index) - 1];
      if (!source) return null;
      return {
        ...report,
        source_url: source.url,
        source_title: source.title,
      };
    })
    .filter(Boolean);
  return {
    reports,
    citations,
  };
}

async function addCoordinates(report) {
  const lat = Number(report.lat);
  const lon = Number(report.lon);
  if (Number.isFinite(lat) && Number.isFinite(lon)) return report;
  const location = [
    report.city,
    report.county ? `${report.county} County` : "",
    report.state,
    "USA",
  ].filter(Boolean).join(", ");
  if (!location || !report.state) return report;
  const response = await fetch(
    `https://nominatim.openstreetmap.org/search?format=jsonv2&countrycodes=us&limit=1&q=${encodeURIComponent(location)}`,
    {
      headers: {
        "User-Agent": "HailMoneyMap/1.0 (hail-ground-truth-verification)",
      },
    },
  );
  if (!response.ok) return report;
  const results = await response.json().catch(() => []);
  const match = Array.isArray(results) ? results[0] : null;
  if (!match) return report;
  return {
    ...report,
    lat: Number(match.lat),
    lon: Number(match.lon),
  };
}

function normalizeEvidence(date, report, region, citations) {
  const hail = Number(report.hail_inches);
  const lat = Number(report.lat);
  const lon = Number(report.lon);
  const url = String(report.source_url || "").trim();
  if (
    !Number.isFinite(hail) || !Number.isFinite(lat) || !Number.isFinite(lon) ||
    !url.startsWith("https://")
  ) return null;
  const confidence = sourceConfidence(report);
  const sourceHost = hostname(url);
  const citationHosts = new Set(citations.map((citation) => hostname(citation.url)).filter(Boolean));
  const hasMatchingCitation = [...citationHosts].some((citationHost) =>
    citationHost === sourceHost ||
    citationHost.endsWith(`.${sourceHost}`) ||
    sourceHost.endsWith(`.${citationHost}`)
  );
  let rejection = "";
  if (asDate(report.event_date) !== date) rejection = "wrong_date";
  else if (!Number.isFinite(hail) || hail < 0.5 || hail > 8) rejection = "invalid_hail_size";
  else if (!Number.isFinite(lat) || !Number.isFinite(lon)) rejection = "missing_coordinates";
  else if (lat < 24 || lat > 50 || lon < -125 || lon > -66) rejection = "outside_conus";
  else if (!url.startsWith("https://")) rejection = "missing_source_url";
  else if (!hasMatchingCitation) rejection = "source_not_confirmed_by_google_citation";
  else if (report.explicit_measurement !== true) rejection = "not_an_explicit_measurement";
  else if (haversineMiles(lat, lon, region.lat, region.lon) > 175) rejection = "outside_search_region";
  else if (confidence < 0.75) rejection = "insufficient_source_confidence";

  const eventTime = report.event_time_utc &&
      !Number.isNaN(Date.parse(report.event_time_utc))
    ? new Date(report.event_time_utc).toISOString()
    : `${date}T12:00:00.000Z`;
  const id = `google-ground-${sha([
    date, lat.toFixed(3), lon.toFixed(3), hail.toFixed(2), url,
  ].join("|")).slice(0, 32)}`;
  return {
    id,
    event_date: date,
    event_time: eventTime,
    lat,
    lon,
    hail_in: hail,
    city: report.city || null,
    state: String(report.state || "").toUpperCase().slice(0, 2) || null,
    county: report.county || null,
    source_url: url,
    source_title: report.source_title || null,
    source_kind: report.source_kind || "other",
    observation_text: String(report.observation_text || "").slice(0, 240) || null,
    confidence,
    accepted: !rejection,
    rejection_reason: rejection || null,
    google_citations: citations,
    raw: report,
    verified_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
}

async function recentRun(date) {
  if (force) return false;
  const { data, error } = await supabase
    .from("hail_ground_truth_runs")
    .select("status,completed_at")
    .eq("event_date", date)
    .maybeSingle();
  if (error) throw new Error(error.message);
  if (data?.status !== "complete" || !data.completed_at) return false;
  return Date.now() - Date.parse(data.completed_at) < refreshHours * 60 * 60 * 1000;
}

async function markRun(date, values) {
  if (dryRun) return;
  const { error } = await supabase.from("hail_ground_truth_runs").upsert({
    event_date: date,
    updated_at: new Date().toISOString(),
    ...values,
  });
  if (error) throw new Error(error.message);
}

async function saveEvidence(evidence) {
  if (dryRun || evidence.length === 0) return;
  const { error } = await supabase
    .from("hail_ground_truth_evidence")
    .upsert(evidence, { onConflict: "id" });
  if (error) throw new Error(`Evidence upsert failed: ${error.message}`);

  const accepted = evidence.filter((row) => row.accepted).map((row) => ({
    id: row.id,
    event_time: row.event_time,
    event_date: row.event_date,
    lat: row.lat,
    lon: row.lon,
    hail_in: row.hail_in,
    state: row.state,
    county: row.county,
    source: "GOOGLE_GROUNDED_SPOTTER",
    raw: {
      evidence_id: row.id,
      source_url: row.source_url,
      source_title: row.source_title,
      source_kind: row.source_kind,
      confidence: row.confidence,
      observation_text: row.observation_text,
      google_citations: row.google_citations,
    },
  }));
  if (accepted.length) {
    const { error: hailError } = await supabase
      .from("hail_lsr_raw")
      .upsert(accepted, { onConflict: "id" });
    if (hailError) throw new Error(`Hail feed upsert failed: ${hailError.message}`);
  }
}

async function regenerateSwath(date) {
  if (dryRun) return;
  const url = `${SUPABASE_URL}/functions/v1/swath-render?date=${date}&persist=1`;
  const response = await fetch(url, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`swath-render ${response.status}: ${body.slice(0, 500)}`);
  }
}

async function processDate(date) {
  if (await recentRun(date)) {
    console.log(`[${date}] skipped; verified within ${refreshHours} hours`);
    return;
  }
  const startedAt = new Date().toISOString();
  await markRun(date, {
    status: "running",
    started_at: startedAt,
    completed_at: null,
    message: null,
  });

  try {
    const anchors = await getAnchors(date);
    const regions = buildRegions(anchors);
    if (regions.length === 0) {
      await markRun(date, {
        status: "complete",
        searched_regions: 0,
        found_reports: 0,
        accepted_reports: 0,
        completed_at: new Date().toISOString(),
        message: "No hail or radar anchors available",
      });
      console.log(`[${date}] no hail/radar anchors; skipped`);
      return;
    }

    const evidence = [];
    for (const [index, region] of regions.entries()) {
      console.log(`[${date}] Google result search ${index + 1}/${regions.length} near ${region.lat.toFixed(2)},${region.lon.toFixed(2)}`);
      const result = await searchGoogle(date, region);
      for (const report of result.reports) {
        const located = await addCoordinates(report);
        const normalized = normalizeEvidence(date, located, region, result.citations);
        if (normalized) evidence.push(normalized);
        if (located !== report) await sleep(1100);
      }
      if (index + 1 < regions.length) await sleep(500);
    }
    const unique = [...new Map(evidence.map((row) => [row.id, row])).values()];
    await saveEvidence(unique);
    const accepted = unique.filter((row) => row.accepted);
    if (accepted.length) await regenerateSwath(date);
    await markRun(date, {
      status: "complete",
      searched_regions: regions.length,
      found_reports: unique.length,
      accepted_reports: accepted.length,
      completed_at: new Date().toISOString(),
      message: dryRun ? "Dry run" : null,
    });
    console.log(`[${date}] found ${unique.length}; accepted ${accepted.length}`);
  } catch (error) {
    await markRun(date, {
      status: "failed",
      completed_at: new Date().toISOString(),
      message: String(error).slice(0, 1000),
    });
    throw error;
  }
}

async function main() {
  const dates = await getDates();
  console.log(`Verifying ${dates.length} storm date(s)${dryRun ? " (dry run)" : ""}`);
  let failures = 0;
  for (const [index, date] of dates.entries()) {
    try {
      await processDate(date);
    } catch (error) {
      failures += 1;
      console.error(`[${date}] failed:`, error);
    }
    if (index + 1 < dates.length) await sleep(750);
  }
  if (failures) throw new Error(`${failures} storm date(s) failed verification`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
