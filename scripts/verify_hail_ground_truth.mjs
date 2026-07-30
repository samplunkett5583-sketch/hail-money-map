#!/usr/bin/env node

import crypto from "node:crypto";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SERPER_API_KEY = process.env.SERPER_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !SERPER_API_KEY) {
  console.error(
    "Missing SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, or SERPER_API_KEY",
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
const unverifiedOnly = args.unverified === "true";
const refreshHours = positiveNumber(args["refresh-hours"], 12);
const limit = Math.min(5000, positiveNumber(args.limit, 250));
const offset = Math.max(0, Number(args.offset) || 0);
const maxRegions = Math.min(20, positiveNumber(args["max-regions"], 12));
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const STATE_NAMES = {
  AL: "Alabama", AK: "Alaska", AZ: "Arizona", AR: "Arkansas",
  CA: "California", CO: "Colorado", CT: "Connecticut", DE: "Delaware",
  FL: "Florida", GA: "Georgia", HI: "Hawaii", ID: "Idaho",
  IL: "Illinois", IN: "Indiana", IA: "Iowa", KS: "Kansas",
  KY: "Kentucky", LA: "Louisiana", ME: "Maine", MD: "Maryland",
  MA: "Massachusetts", MI: "Michigan", MN: "Minnesota", MS: "Mississippi",
  MO: "Missouri", MT: "Montana", NE: "Nebraska", NV: "Nevada",
  NH: "New Hampshire", NJ: "New Jersey", NM: "New Mexico", NY: "New York",
  NC: "North Carolina", ND: "North Dakota", OH: "Ohio", OK: "Oklahoma",
  OR: "Oregon", PA: "Pennsylvania", RI: "Rhode Island",
  SC: "South Carolina", SD: "South Dakota", TN: "Tennessee", TX: "Texas",
  UT: "Utah", VT: "Vermont", VA: "Virginia", WA: "Washington",
  WV: "West Virginia", WI: "Wisconsin", WY: "Wyoming", DC: "District of Columbia",
};

function stateName(value) {
  const code = String(value || "").toUpperCase();
  return STATE_NAMES[code] || value || "";
}

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

const HAIL_SIZE_WORDS = {
  quarter: 1.00,
  "half dollar": 1.25,
  "ping pong": 1.50,
  "golf ball": 1.75,
  "hen egg": 2.00,
  "tennis ball": 2.50,
  baseball: 2.75,
  "tea cup": 3.00,
  softball: 4.00,
  grapefruit: 4.50,
};

function directSearchReports(date, humanDate, region, candidates) {
  const slashDate = `${date.slice(5, 7)}/${date.slice(8, 10)}/${date.slice(0, 4)}`;
  const locations = region.locations.filter((location) => location.city);
  const reports = [];
  for (const candidate of candidates) {
    const text = `${candidate.title} | ${candidate.snippet}`;
    const lower = text.toLowerCase();
    const hasDate = lower.includes(date) ||
      lower.includes(slashDate) ||
      lower.includes(humanDate.toLowerCase());
    if (!hasDate) continue;
    const explicit = /(reported|report showing|measured|next to (?:a )?tape measure|noaa spc|trained spotter|mping)/i
      .test(text);
    if (!explicit) continue;

    const mentionedLocations = locations
      .map((location) => ({
        ...location,
        index: lower.indexOf(String(location.city).toLowerCase()),
      }))
      .filter((location) => location.index >= 0);
    if (mentionedLocations.length === 0) continue;

    const measurements = [];
    const numeric = /(\d(?:\.\d{1,2})?)\s*(?:"|inches?|in\.)/gi;
    for (const match of text.matchAll(numeric)) {
      measurements.push({ hail: Number(match[1]), index: match.index });
    }
    for (const [label, hail] of Object.entries(HAIL_SIZE_WORDS)) {
      let from = 0;
      while (from < lower.length) {
        const index = lower.indexOf(label, from);
        if (index < 0) break;
        measurements.push({ hail, index });
        from = index + label.length;
      }
    }

    for (const measurement of measurements) {
      if (!Number.isFinite(measurement.hail) || measurement.hail < 0.5 || measurement.hail > 8) {
        continue;
      }
      const location = mentionedLocations
        .slice()
        .sort((a, b) =>
          Math.abs(a.index - measurement.index) - Math.abs(b.index - measurement.index)
        )[0];
      if (!location || Math.abs(location.index - measurement.index) > 220) continue;
      const start = Math.max(0, measurement.index - 70);
      const end = Math.min(text.length, measurement.index + 110);
      reports.push({
        event_date: date,
        event_time_utc: null,
        hail_inches: measurement.hail,
        city: location.city,
        state: location.state,
        county: location.county,
        lat: location.lat,
        lon: location.lon,
        source_url: candidate.url,
        source_title: candidate.title,
        source_kind: "trained_spotter",
        explicit_measurement: true,
        corroborating_sources: 1,
        observation_text: text.slice(start, end).replace(/\s+/g, " ").trim(),
      });
    }
  }
  return reports;
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

  async function selectDates(values) {
    let dates = [...new Set(
      (values || []).map((row) => asDate(
        typeof row === "string" ? row : row?.event_date || row?.date || row?.eventDate,
      )).filter((date) => validDate(date) && (!cutoff || date >= cutoff)),
    )].sort().reverse();

    if (unverifiedOnly && dates.length) {
      const completed = await supabase
        .from("hail_ground_truth_runs")
        .select("event_date")
        .eq("status", "complete")
        .range(0, 4999);
      if (completed.error) throw new Error(completed.error.message);
      const completedDates = new Set((completed.data || []).map((row) => asDate(row.event_date)));
      dates = dates.filter((date) => !completedDates.has(date));
    }
    return dates.slice(offset, offset + limit);
  }

  const completeIndexResult = await supabase.rpc("get_hail_ground_truth_dates");
  if (!completeIndexResult.error && Array.isArray(completeIndexResult.data)) {
    return selectDates(completeIndexResult.data);
  }

  const jsonResult = await supabase.rpc("get_storm_dates_json");
  if (!jsonResult.error && jsonResult.data != null) {
    let payload = jsonResult.data;
    if (typeof payload === "string") {
      try { payload = JSON.parse(payload); } catch { payload = []; }
    }
    if (!Array.isArray(payload)) {
      payload = payload?.rows || payload?.data || payload?.dates || payload?.event_dates || [];
    }
    const dates = await selectDates(payload);
    if (dates.length || unverifiedOnly) return dates;
  }

  const rpcResult = await supabase.rpc("get_storm_distinct_dates");
  if (!rpcResult.error && Array.isArray(rpcResult.data)) {
    return selectDates(rpcResult.data);
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
  return selectDates(dates);
}

async function getAnchors(date) {
  const [lsrResult, radarResult, savedResult] = await Promise.all([
    supabase
      .from("hail_lsr_raw")
      .select("lat,lon,state,county,hail_in,source,raw")
      .eq("event_date", date)
      .limit(5000),
    supabase
      .from("hail_radar_polygons")
      .select("centroid_lat,centroid_lon,band_min,band_max")
      .eq("event_date", date)
      .limit(2000),
    supabase
      .from("storm_polygons")
      .select("centroid_lat,centroid_lon,band_min,band_max,state,county,city,storm_type")
      .eq("event_date", date)
      .ilike("storm_type", "hail")
      .limit(5000),
  ]);
  if (lsrResult.error) throw new Error(lsrResult.error.message);

  const anchors = (lsrResult.data || []).map((row) => ({
    lat: Number(row.lat),
    lon: Number(row.lon),
    state: row.state || "",
    county: row.county || "",
    city: String(row.raw?.CITY || row.raw?.city || "").trim(),
    hail: Number(row.hail_in) || 0,
  }));
  if (!radarResult.error) {
    for (const row of radarResult.data || []) {
      anchors.push({
        lat: Number(row.centroid_lat),
        lon: Number(row.centroid_lon),
        state: "",
        county: "",
        city: "",
        hail: Number(row.band_max || row.band_min) || 0,
      });
    }
  }
  if (!savedResult.error) {
    for (const row of savedResult.data || []) {
      anchors.push({
        lat: Number(row.centroid_lat),
        lon: Number(row.centroid_lon),
        state: row.state || "",
        county: row.county || "",
        city: row.city || "",
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
      const locationCounts = new Map();
      for (const point of points) {
        if (!point.county || !point.state) continue;
        const key = `${point.city}|${point.county}|${point.state}`;
        const current = locationCounts.get(key) || {
          city: point.city,
          county: point.county,
          state: point.state,
          count: 0,
          hail: 0,
          lat: point.lat,
          lon: point.lon,
        };
        current.count += 1;
        if (point.hail >= current.hail) {
          current.hail = point.hail;
          current.lat = point.lat;
          current.lon = point.lon;
        }
        locationCounts.set(key, current);
      }
      const locations = [...locationCounts.values()]
        .sort((a, b) => b.hail - a.hail || b.count - a.count)
        .slice(0, 3);
      const existingMax = Math.max(0, ...points.map((p) => p.hail));
      return { key, lat, lon, states, counties, locations, existingMax, anchors: points };
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
  const place = region.locations.length
    ? region.locations
      .map((location) => location.city
        ? `${location.city} ${stateName(location.state)}`
        : `${location.county} County ${stateName(location.state)}`
      )
      .join(" ")
    : region.states.slice(0, 2).map(stateName).filter(Boolean).join(" ");
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
  const directReports = directSearchReports(date, humanDate, region, candidates);
  if (directReports.length > 0) {
    console.log(`[${date}] extracted ${directReports.length} explicit Google result report(s)`);
  }
  return { reports: directReports, citations };
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
