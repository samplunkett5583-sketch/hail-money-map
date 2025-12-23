// FORCE-DEPLOY 2025-12-20T11:52:00Z
const { onRequest } = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const fetch = require("node-fetch");
const qs = require("querystring");
const turf = require("@turf/turf");


admin.initializeApp();
const db = admin.firestore();

// Safely load NOAA token from Firebase config or environment.
// Do NOT crash if functions.config() is not available.
let NOAA_TOKEN = process.env.NOAA_TOKEN || "";

try {
  if (functions && typeof functions.config === "function") {
    const cfg = functions.config();
    if (cfg && cfg.noaa && cfg.noaa.token) {
      NOAA_TOKEN = cfg.noaa.token;
    }
  }
} catch (e) {
  logger.warn("NOAA config not available locally, using env only");
}

if (!NOAA_TOKEN) {
  logger.warn("NOAA_TOKEN is empty – set functions config or env var.");
}


// Map state abbreviation -> FIPS code for NOAA API
const stateFipsMap = {
  AL: "01", AK: "02", AZ: "04", AR: "05", CA: "06", CO: "08", CT: "09",
  DE: "10", FL: "12", GA: "13", HI: "15", ID: "16", IL: "17", IN: "18",
  IA: "19", KS: "20", KY: "21", LA: "22", ME: "23", MD: "24", MA: "25",
  MI: "26", MN: "27", MS: "28", MO: "29", MT: "30", NE: "31", NV: "32",
  NH: "33", NJ: "34", NM: "35", NY: "36", NC: "37", ND: "38", OH: "39",
  OK: "40", OR: "41", PA: "42", RI: "44", SC: "45", SD: "46", TN: "47",
  TX: "48", UT: "49", VT: "50", VA: "51", WA: "53", WV: "54", WI: "55",
  WY: "56"
};

// Call NOAA CDO events API
async function fetchNoaaEvents(stateCode, eventType, startDate, endDate) {
  const fips = stateFipsMap[stateCode];
  const base = "https://www.ncdc.noaa.gov/cdo-web/api/v2/events";
  const params = {
    startdate: startDate,
    enddate: endDate,
    limit: 1000
  };

  if (fips) params.locationid = "FIPS:" + fips;
  if (eventType) params.eventType = eventType;

  const url = base + "?" + qs.stringify(params);

  const resp = await fetch(url, {
    headers: { token: NOAA_TOKEN, Accept: "application/json" },
    timeout: 30000
  });

  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error("NOAA error " + resp.status + " " + txt);
  }

  const json = await resp.json();
  return json.results || [];
}

// Enough points for concave hull?
function ptCountForConcave(n) {
  return n >= 4;
}

// Group events by date and build a polygon per day
function groupAndBuildPolygons(events) {
  const groups = {};

  events.forEach(ev => {
    const lat = parseFloat(
      ev.beginLatitude || ev.latitude || ev.lat || ev.BEGIN_LAT
    );
    const lon = parseFloat(
      ev.beginLongitude || ev.longitude || ev.lng || ev.BEGIN_LON
    );
    const dateStr = ev.beginDate
      ? new Date(ev.beginDate).toISOString().slice(0, 10)
      : ev.date
      ? ev.date.slice(0, 10)
      : null;

    if (!isFinite(lat) || !isFinite(lon) || !dateStr) return;

    if (!groups[dateStr]) groups[dateStr] = [];
    groups[dateStr].push({ lat, lon, raw: ev });
  });

  const results = [];

  Object.keys(groups).forEach(dateKey => {
    const pts = groups[dateKey];
    if (!pts.length) return;

    const fc = turf.featureCollection(
      pts.map(p => turf.point([p.lon, p.lat], p.raw))
    );

    let hull = null;

    try {
      if (ptCountForConcave(pts.length)) {
        hull = turf.concave(fc, { maxEdge: 5, units: "kilometers" });
      }
    } catch (e) {
      hull = null;
    }

    if (!hull) {
      hull = turf.convex(fc);
    }

    if (!hull) {
      // last resort – small circle around centroid
      const centroid = turf.centroid(fc);
      hull = turf.buffer(centroid, 1, { units: "kilometers" });
    } else {
      // widen the swath a bit
      hull = turf.buffer(hull, 1.0, { units: "kilometers" });
    }

    let minLat = 90,
      minLon = 180,
      maxLat = -90,
      maxLon = -180;

    pts.forEach(p => {
      minLat = Math.min(minLat, p.lat);
      minLon = Math.min(minLon, p.lon);
      maxLat = Math.max(maxLat, p.lat);
      maxLon = Math.max(maxLon, p.lon);
    });

    results.push({
      dateString: dateKey,
      beginLatitude: minLat,
      beginLongitude: minLon,
      endLatitude: maxLat,
      endLongitude: maxLon,
      count: pts.length,
      geojson: hull ? hull.geometry : null,
      samplePoints: pts.slice(0, 10).map(p => ({ lat: p.lat, lon: p.lon }))
    });
  });

  return results;
}

// Main HTTPS function: fetch NOAA, build polygons, cache to Firestore, return items
exports.fetchAndCacheStorms = onRequest(async (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");
  if (req.method === "OPTIONS") return res.status(204).send("");

  try {
    const state = (req.query.state || req.body.state || "").toUpperCase();
    const eventType = req.query.eventType || req.body.eventType || "";
    const startDate = req.query.startDate || req.body.startDate;
    const endDate = req.query.endDate || req.body.endDate;

    if (!state || !startDate || !endDate) {
      return res
        .status(400)
        .json({ error: "Missing state/startDate/endDate" });
    }

    const events = await fetchNoaaEvents(state, eventType, startDate, endDate);
    const grouped = groupAndBuildPolygons(events);

    const batch = db.batch();
    const col = db.collection("stormDates");
    const periodKey = `range:${startDate}_${endDate}`;
    const items = [];

    for (const g of grouped) {
      const id = `${state}_${eventType}_${g.dateString}`;
      const docRef = col.doc(id);

      const payload = {
        state: state,
        type: eventType,
        dateString: g.dateString,
        lat: g.beginLatitude,
        lng: g.beginLongitude,
        beginLatitude: g.beginLatitude,
        beginLongitude: g.beginLongitude,
        endLatitude: g.endLatitude,
        endLongitude: g.endLongitude,
        samplePoints: g.samplePoints,
        polygon: g.geojson,
        count: g.count,
        periodKey: periodKey,
        date: admin.firestore.Timestamp.fromDate(new Date(g.dateString))
      };

      // Sanitize string fields
      payload.state = String(payload.state ?? "").trim().toUpperCase();
      payload.type = String(payload.type ?? "").trim().toLowerCase();

      batch.set(docRef, payload, { merge: true });
      items.push(Object.assign({ id }, payload));
    }

    await batch.commit();

    return res.json({ count: items.length, items });
  } catch (err) {
    console.error("fetchAndCacheStorms error", err);
    return res.status(500).json({ error: err.message || String(err) });
  }
});
const functions = require("firebase-functions");   
// Cloud Functions config: functions:config:set noaa.token="YOUR_TOKEN"
exports.getNOAAStorms = functions.https.onRequest(async (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).send("");
  }

  try {
    const state = req.query.state;
    const type = req.query.type;
    const minVal = parseInt(req.query.min || "0", 10);

    if (!state || !type) {
      return res.status(400).json({ error: "Missing parameters." });
    }

    const NOAA_TOKEN = functions.config().noaa.token;

    const NOAA_BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2";

    const eventTypeMap = {
      hail: "Hail",
      wind: "Thunderstorm Wind",
      tornado: "Tornado"
    };

    const eventType = eventTypeMap[type] || "Hail";

    const today = new Date();
    const endDate = today.toISOString().slice(0, 10);
    const startDate = (today.getFullYear() - 1) + "-01-01";

    const url =
      `${NOAA_BASE_URL}/events?datasetid=STORMEVENTS` +
      `&limit=1000` +
      `&state=${state}` +
      `&eventtype=${encodeURIComponent(eventType)}` +
      `&startdate=${startDate}` +
      `&enddate=${endDate}`;

    const resp = await fetch(url, {
      headers: { token: NOAA_TOKEN }
    });

    if (!resp.ok) {
      return res.status(resp.status).json({ error: "NOAA error", status: resp.status });
    }

    const data = await resp.json();

    const storms = (data.results || []).map(ev => {
      const dateString = ev.begin_date || ev.begin || ev.date || "";
      return {
        id: ev.id,
        name: ev.event_type,
        dateString: new Date(dateString).toLocaleDateString(),
        city: ev.cz_name || "",
        state: state,
        avgValue: 20000, // placeholder
        centerLat: ev.latitude || 39.5,
        centerLng: ev.longitude || -98.35,
        zoom: ev.latitude ? 10 : 6
      };
    });

    res.json(storms.filter(s => !minVal || (s.avgValue >= minVal)));
  } catch (err) {
    console.error("NOAA function error:", err);
    res.status(500).json({ error: "Server error", details: err.toString() });
  }
});

// Ingest Storm Events last 12 months
const ingestStormEventsLast12Months = async () => {
  const https = require('https');
  const zlib = require('zlib');
  const csv = require('csv-parser');
  const { Readable } = require('stream');

  const currentYear = new Date().getFullYear();
  const previousYear = currentYear - 1;
  const years = [previousYear, currentYear];

  const baseUrl = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/';

  // Function to list files
  const listFiles = async () => {
    // In real implementation, fetch the directory listing
    // For now, assume filenames
    const files = [];
    for (const year of years) {
      // Assume latest file for each year
      const detailsFile = `StormEvents_details-ftp_v1.0_d${year}_c${year}1231.csv.gz`; // placeholder
      const locationsFile = `StormEvents_locations-ftp_v1.0_d${year}_c${year}1231.csv.gz`; // placeholder
      files.push({ details: detailsFile, locations: locationsFile });
    }
    return files;
  };

  const files = await listFiles();

  const allEvents = [];
  const locationsMap = new Map();

  for (const file of files) {
    // Download and parse locations first
    const locationsUrl = baseUrl + file.locations;
    const locationsData = await downloadAndParseCSV(locationsUrl);
    locationsData.forEach(row => {
      const eventId = row.EVENT_ID.trim();
      if (!locationsMap.has(eventId)) locationsMap.set(eventId, []);
      locationsMap.get(eventId).push({
        lat: parseFloat(row.LATITUDE),
        lng: parseFloat(row.LONGITUDE)
      });
    });

    // Download and parse details
    const detailsUrl = baseUrl + file.details;
    const detailsData = await downloadAndParseCSV(detailsUrl);
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

    detailsData.forEach(row => {
      const beginDate = new Date(row.BEGIN_DATE_TIME);
      if (beginDate >= oneYearAgo) {
        const eventId = row.EVENT_ID.trim();
        const state = row.STATE.trim().toUpperCase();
        const type = row.EVENT_TYPE.trim().toLowerCase();
        const date = beginDate.toISOString().slice(0, 10);
        const locations = locationsMap.get(eventId) || [];
        let lat, lng;
        if (locations.length > 0) {
          // Use first location or centroid
          lat = locations[0].lat;
          lng = locations[0].lng;
        }
        allEvents.push({ state, type, date, lat, lng, eventId });
      }
    });
  }

  // Group by state_type_date
  const grouped = new Map();
  allEvents.forEach(event => {
    const key = `${event.state}_${event.type}_${event.date}`;
    if (!grouped.has(key)) grouped.set(key, { state: event.state, type: event.type, date: event.date, lat: event.lat, lng: event.lng, count: 0 });
    grouped.get(key).count++;
  });

  // Upsert to Firestore
  const batch = db.batch();
  const col = db.collection("stormDates");
  let written = 0;
  for (const [key, data] of grouped) {
    const docRef = col.doc(key);
    batch.set(docRef, {
      state: data.state,
      type: data.type,
      dateString: data.date,
      lat: data.lat,
      lng: data.lng,
      count: data.count,
      date: admin.firestore.Timestamp.fromDate(new Date(data.date))
    }, { merge: true });
    written++;
  }
  await batch.commit();
  console.log(`Ingested ${written} storm date docs`);
};

// Helper to download and parse CSV
const downloadAndParseCSV = (url) => {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      const gunzip = zlib.createGunzip();
      const results = [];
      res.pipe(gunzip).pipe(csv()).on('data', (data) => results.push(data)).on('end', () => resolve(results)).on('error', reject);
    }).on('error', reject);
  });
};

exports.ingestStormEventsLast12Months = ingestStormEventsLast12Months;
// Proxy NOAA SWDI PLSR CSV data (fixes browser CORS)
exports.noaaPlsrProxy = functions.https.onRequest(
  { secrets: ["NOAA_TOKEN"], timeoutSeconds: 120, memory: "1GiB" },
  async (req, res) => {
    // Set CORS headers for all responses
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.set("Vary", "Origin");

    try {
      if (req.method === "OPTIONS") {
        return res.status(204).send("");
      }

      const start = String(req.query.start || "").trim();
      const end = String(req.query.end || "").trim();
      const bbox = String(req.query.bbox || "").trim();
      const limit = String(req.query.limit || "20000").trim();
      const mode = String(req.query.mode || "radar").trim().toLowerCase();
      const product = mode === "reports" ? "lsr" : String(req.query.product || "nx3hail_all").trim();
      const datesOnly = req.query.datesOnly === "1";

      if (!/^\d{8}$/.test(start) || !/^\d{8}$/.test(end) || !bbox) {
        return res
          .status(400)
          .send("Invalid params. Use start=YYYYMMDD&end=YYYYMMDD&bbox=minLon,minLat,maxLon,maxLat");
      }

      if (mode !== "reports" && product !== "nx3hail" && product !== "nx3hail_all") {
        return res.status(400).send("Invalid product. Use nx3hail or nx3hail_all");
      }

      if (datesOnly && mode !== "reports") {
        return res.status(400).send("datesOnly=1 requires mode=reports");
      }

    const tokenRaw = process.env.NOAA_TOKEN;
    const token = (typeof tokenRaw === "string" ? tokenRaw : String(tokenRaw || "")).trim();
    if (!token) {
      return res.status(500).send(`NOAA_TOKEN missing/empty (rawType=${typeof tokenRaw}, rawLen=${tokenRaw ? String(tokenRaw).length : 0})`);
    }

    // Check if date range > 3 days for chunking
    const startDate = new Date(start.slice(0, 4) + "-" + start.slice(4, 6) + "-" + start.slice(6, 8));
    const endDate = new Date(end.slice(0, 4) + "-" + end.slice(4, 6) + "-" + end.slice(6, 8));
    const daysDiff = (endDate - startDate) / (1000 * 60 * 60 * 24);
    const chunkThreshold = datesOnly ? 31 : 3;
    const needChunking = daysDiff > chunkThreshold;

    const FETCH_TIMEOUT_MS = 25000;
    const eventTypeParam = mode === "reports" ? "&eventType=Hail" : "";
    const storms = [];
    const dateSet = new Set();
    let chunksUsed = 1;
    let hasTimedOut = false;

    res.set("Access-Control-Expose-Headers", "X-Upstream-Url,X-Upstream-Status,X-Upstream-Content-Type,X-Chunks-Used");

    // Helper to fetch with timeout
    async function fetchWithTimeout(url, timeoutMs = FETCH_TIMEOUT_MS) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
      try {
        return await fetch(url, {
          headers: { Accept: 'application/json,*/*' },
          signal: controller.signal
        });
      } finally {
        clearTimeout(timeoutId);
      }
    }

    // Helper to extract dates from NOAA response (datesOnly mode)
    function extractDatesOnly(data) {
      const result = Array.isArray(data?.result) ? data.result : [];
      const dateChunk = new Set();

      for (const row of result) {
        let props = {};
        
        if (typeof row === "string") {
          const kvRegex = /"([^"]+)":"([^"]*)"/g;
          let match;
          while ((match = kvRegex.exec(row)) !== null) {
            props[match[1]] = match[2];
          }
        } else if (typeof row === "object" && row !== null) {
          props = row;
        } else {
          continue;
        }

        const dateStr = props.valid && typeof props.valid === "string" && props.valid.length >= 10
          ? props.valid.slice(0, 10)
          : (props.ZTIME || props.TIME)?.slice(0, 10);
        if (dateStr && /^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
          dateChunk.add(dateStr);
        }
      }

      return { dateChunk, rawCount: result.length };
    }

    // Helper to parse NOAA response
    // Note: SWDI result rows may be objects OR strings; we handle both formats.
    function parseNoaaResponse(data) {
      const result = Array.isArray(data?.result) ? data.result : [];
      console.log("noaaPlsrProxy parseNoaaResponse - result count:", result.length);
      if (result.length > 0) {
        console.log("noaaPlsrProxy first result:", JSON.stringify(result[0]));
      }
      const stormChunk = [];
      const dateChunk = new Set();

      for (const row of result) {
        let props = {};
        
        // Handle both string and object formats from NOAA
        if (typeof row === "string") {
          const kvRegex = /"([^"]+)":"([^"]*)"/g;
          let match;
          while ((match = kvRegex.exec(row)) !== null) {
            props[match[1]] = match[2];
          }
        } else if (typeof row === "object" && row !== null) {
          // NOAA returns objects directly
          props = row;
        } else {
          continue;
        }

        let lon = Number(props.LON || props.LONGITUDE || props.LONG || props.lon);
        let lat = Number(props.LAT || props.LATITUDE || props.lat);

        if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
          const shapeMatch = (props.SHAPE || "").match(/POINT\s*\(\s*([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\s*\)/);
          if (!shapeMatch) continue;
          lon = Number(shapeMatch[1]);
          lat = Number(shapeMatch[2]);
        }

        if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

        const ztime = props.ZTIME || props.TIME || null;
        const dateOnly = typeof ztime === "string" && ztime.length >= 10 ? ztime.slice(0, 10) : null;
        const hailSizeRaw = props.MAXSIZE ?? props.MAGNITUDE;
        const hailSize = hailSizeRaw !== undefined ? parseFloat(hailSizeRaw) : null;

        if (dateOnly) dateChunk.add(dateOnly);

        stormChunk.push({
          lat,
          lon,
          date: dateOnly,
          ztime,
          hailSize: Number.isFinite(hailSize) ? hailSize : null,
          props
        });
      }

      return { stormChunk, dateChunk, rawCount: result.length };
    }

    let totalUpstreamRows = 0;
    try {
      if (needChunking) {
        // Split into chunks (31-day for datesOnly, 3-day for normal)
        const chunkSize = datesOnly ? 31 : 3;
        let currentDate = new Date(startDate);
        let chunkCount = 0;

        while (currentDate <= endDate) {
          const chunkEnd = new Date(currentDate);
          chunkEnd.setDate(chunkEnd.getDate() + chunkSize);
          
          const actualEnd = chunkEnd > endDate ? endDate : chunkEnd;
          
          const chunkStartStr = currentDate.toISOString().slice(0, 10).replace(/-/g, "");
          const chunkEndStr = actualEnd.toISOString().slice(0, 10).replace(/-/g, "");

          const upstreamChunk = `https://www.ncdc.noaa.gov/swdiws/json/${product}/${chunkStartStr}:${chunkEndStr}?bbox=${encodeURIComponent(bbox)}&limit=${limit}${eventTypeParam}&token=${encodeURIComponent(token)}`;

          console.log(`noaaPlsrProxy chunk ${chunkCount + 1}: ${chunkStartStr}:${chunkEndStr}`);

          try {
            const r = await fetchWithTimeout(upstreamChunk);
            
            if (!r.ok) {
              if (datesOnly && hasTimedOut === false) {
                hasTimedOut = true;
                console.warn(`noaaPlsrProxy chunk ${chunkCount + 1} failed with status ${r.status}, skipping gracefully`);
                // Continue to next chunk instead of failing
              } else {
                return res.status(r.status).send(await r.text());
              }
            }

            const text = await r.text();
            let data;
            try {
              data = JSON.parse(text);
            } catch (parseErr) {
              if (datesOnly) {
                console.warn(`noaaPlsrProxy chunk ${chunkCount + 1} JSON parse error, skipping gracefully`);
                // Skip this chunk and continue
              } else {
                return res.status(502).send(`JSON parse error: ${text.slice(0, 500)}`);
              }
            }

            if (datesOnly) {
              const { dateChunk, rawCount } = extractDatesOnly(data);
              totalUpstreamRows += rawCount;
              dateChunk.forEach(d => dateSet.add(d));
            } else {
              const { stormChunk, dateChunk, rawCount } = parseNoaaResponse(data);
              totalUpstreamRows += rawCount;
              storms.push(...stormChunk);
              dateChunk.forEach(d => dateSet.add(d));
            }

            chunkCount++;
            currentDate = new Date(actualEnd);
            currentDate.setDate(currentDate.getDate() + 1);
          } catch (err) {
            if (err.name === "AbortError") {
              if (datesOnly) {
                console.warn(`noaaPlsrProxy chunk ${chunkCount + 1} timeout, skipping gracefully`);
                hasTimedOut = true;
                chunkCount++;
                currentDate = new Date(actualEnd);
                currentDate.setDate(currentDate.getDate() + 1);
                continue;
              }
              const upstreamUrl = `https://www.ncdc.noaa.gov/swdiws/json/${product}/${chunkStartStr}:${chunkEndStr}?...`;
              res.set("X-Upstream-Url", upstreamUrl);
              return res.status(504).send("Upstream NOAA timeout");
            }
            throw err;
          }
        }

        chunksUsed = chunkCount;
      } else {
        // Single request for <= chunk threshold
        // (or <= 31-day range for datesOnly)
        const upstream = `https://www.ncdc.noaa.gov/swdiws/json/${product}/${start}:${end}?bbox=${encodeURIComponent(bbox)}&limit=${limit}${eventTypeParam}&token=${encodeURIComponent(token)}`;
        console.log("noaaPlsrProxy upstream URL:", upstream);

        try {
          const r = await fetchWithTimeout(upstream);
          res.set("X-Upstream-Url", upstream);
          res.set("X-Upstream-Status", String(r.status));
          res.set("X-Upstream-Content-Type", r.headers.get("content-type") || "unknown");

          if (!r.ok) {
            return res.status(r.status).send(await r.text());
          }

          const text = await r.text();
          console.log("noaaPlsrProxy upstream status:", r.status);
          console.log("noaaPlsrProxy raw response (first 500 chars):", text.slice(0, 500));

          let data;
          try {
            data = JSON.parse(text);
          } catch (parseErr) {
            return res.status(502).send(`JSON parse error: ${text.slice(0, 500)}`);
          }

          if (datesOnly) {
            const { dateChunk, rawCount } = extractDatesOnly(data);
            totalUpstreamRows += rawCount;
            dateChunk.forEach(d => dateSet.add(d));
          } else {
            const { stormChunk, dateChunk, rawCount } = parseNoaaResponse(data);
            totalUpstreamRows += rawCount;
            storms.push(...stormChunk);
            dateChunk.forEach(d => dateSet.add(d));
          }
        } catch (err) {
          if (err.name === "AbortError") {
            if (datesOnly) {
              console.warn("Single request timeout, returning dates collected so far");
              hasTimedOut = true;
              // Fall through to return what we have
            } else {
              const upstreamUrl = `https://www.ncdc.noaa.gov/swdiws/json/${product}/${start}:${end}?...`;
              res.set("X-Upstream-Url", upstreamUrl);
              return res.status(504).send("Upstream NOAA timeout");
            }
          }
          if (!datesOnly) throw err;
        }
      }
    } catch (fetchErr) {
      if (fetchErr.name === "AbortError") {
        if (!datesOnly) {
          return res.status(504).send("Upstream NOAA timeout");
        }
        console.warn("Request timed out, returning dates collected so far");
      }
      if (!datesOnly) throw fetchErr;
    }

    const availableDates = Array.from(dateSet).sort((a, b) => a.localeCompare(b));

    if (datesOnly) {
      // Return only dates for datesOnly mode
      res.set("Content-Type", "application/json; charset=utf-8");
      res.set("Cache-Control", "no-store");
      return res.status(200).json({ availableDates });
    }

    const response = {
      storms,
      availableDates,
      summary: null
    };

    console.log("noaaPlsrProxy totals => upstream rows:", totalUpstreamRows, "normalized storms:", storms.length);

    res.set("Content-Type", "application/json; charset=utf-8");
    res.set("Cache-Control", "no-store");
    res.set("X-Chunks-Used", String(chunksUsed));

    res.status(200).json(response);
  } catch (e) {
    console.error("noaaPlsrProxy error:", e);
    res
      .status(500)
      .json({ storms: [], availableDates: [], summary: { error: e && e.message ? e.message : String(e) } });
  }
});
