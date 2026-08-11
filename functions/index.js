// FORCE-DEPLOY 2025-12-20T11:52:00Z
const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");
const fetch = require("node-fetch");
const qs = require("querystring");
const turf = require("@turf/turf");


admin.initializeApp();
const db = admin.firestore();
const YOUTUBE_API_KEY = defineSecret("YOUTUBE_API_KEY");
const ABC_CLIENT_ID = defineSecret("ABC_CLIENT_ID");
const ABC_CLIENT_SECRET = defineSecret("ABC_CLIENT_SECRET");
const OPENAI_API_KEY = defineSecret("OPENAI_API_KEY");
const ABC_SANDBOX_AUTH_BASE = "https://sandbox.auth.partners.abcsupply.com/oauth2/aus1vp07knpuqf6Xz0h8/v1";
const ABC_SANDBOX_API_BASE = "https://partners-sb.abcsupply.com";
const ABC_REDIRECT_URI = "https://hailmoneymap.web.app/abc-oauth-callback.html";
// ABC only permits redirect URIs registered in the developer portal. The hosted callback is registered for this sandbox app and can complete local development connections because the emulator uses the production Firestore.
const ABC_LOCAL_REDIRECT_URI = "http://127.0.0.1:5500/abc-oauth-callback.html";
const ABC_USER_SCOPES = [
  "pricing.read",
  "order.read",
  "order.write",
  "product.read",
  "account.read",
  "location.read",
  "notification.read",
  "notification.write",
  "offline_access"
].join(" ");

function permitCors(req, res) {
  const origin = String(req.get("origin") || "");
  const allowed = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com)$/i.test(origin) ||
    /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (allowed) res.set("Access-Control-Allow-Origin", origin);
  res.set("Vary", "Origin");
  res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
}

async function requireFirebaseUser(req) {
  const authHeader = String(req.get("authorization") || "");
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) throw Object.assign(new Error("Sign in is required."), { statusCode: 401 });
  return admin.auth().verifyIdToken(match[1]);
}

const HM_TEST_EMPLOYEES = {
  "admin@hailmoney.test": { password: "admin123", role: "Admin", displayName: "Admin" },
  "rep@hailmoney.test": { password: "rep123", role: "Sales Rep", displayName: "Test Rep" },
  "canvasser@hailmoney.test": { password: "canv123", role: "Canvasser", displayName: "Test Canvasser" }
};

function safeEmployeeProfile(userRecord, fallback) {
  const claims = userRecord.customClaims || {};
  return {
    uid: userRecord.uid,
    email: userRecord.email || "",
    role: claims.hmRole || (fallback && fallback.role) || "Sales Rep",
    displayName: userRecord.displayName || (fallback && fallback.displayName) || userRecord.email || ""
  };
}

exports.employeeTestLogin = onRequest({ cors: false, region: "us-central1" }, async (req, res) => {
  permitCors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
  try {
    const email = String(req.body && req.body.email || "").trim().toLowerCase();
    const password = String(req.body && req.body.password || "");
    const seed = HM_TEST_EMPLOYEES[email];
    if (!seed || password !== seed.password) return res.status(401).json({ error: "Invalid credentials." });

    let userRecord;
    try { userRecord = await admin.auth().getUserByEmail(email); }
    catch (error) {
      if (error && error.code !== "auth/user-not-found") throw error;
      userRecord = await admin.auth().createUser({
        email,
        password,
        displayName: seed.displayName,
        emailVerified: true
      });
    }
    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole: seed.role
    });
    userRecord = await admin.auth().getUser(userRecord.uid);
    const token = await admin.auth().createCustomToken(userRecord.uid);
    return res.status(200).json({ token, profile: safeEmployeeProfile(userRecord, seed) });
  } catch (error) {
    logger.error("Employee test login failed", { message: error && error.message });
    return res.status(500).json({ error: "Employee login could not be completed." });
  }
});

exports.provisionEmployee = onRequest({ cors: false, region: "us-central1" }, async (req, res) => {
  permitCors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
  try {
    const caller = await requireFirebaseUser(req);
    if (caller.employee !== true || ["Owner", "Admin"].indexOf(String(caller.hmRole || "")) === -1) {
      return res.status(403).json({ error: "Administrator permission is required." });
    }
    const body = req.body || {};
    const email = String(body.email || "").trim().toLowerCase();
    const password = String(body.password || "");
    const displayName = String(body.displayName || email).trim().slice(0, 120);
    const hmRole = String(body.role || "Sales Rep").trim();
    if (!email || !password || password.length < 6) {
      return res.status(400).json({ error: "A valid email and password of at least 6 characters are required." });
    }
    let userRecord;
    try {
      userRecord = await admin.auth().getUserByEmail(email);
      userRecord = await admin.auth().updateUser(userRecord.uid, { password, displayName, disabled: body.active === false });
    } catch (error) {
      if (error && error.code !== "auth/user-not-found") throw error;
      userRecord = await admin.auth().createUser({ email, password, displayName, disabled: body.active === false });
    }
    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole
    });
    await db.collection("hmEmployees").doc(userRecord.uid).set({
      email,
      displayName,
      role: hmRole,
      active: body.active !== false,
      updatedAt: FieldValue.serverTimestamp(),
      updatedBy: caller.uid
    }, { merge: true });
    userRecord = await admin.auth().getUser(userRecord.uid);
    return res.status(200).json({ profile: safeEmployeeProfile(userRecord, { role: hmRole, displayName }) });
  } catch (error) {
    const status = Number(error && error.statusCode) || 500;
    logger.error("Employee provisioning failed", { message: error && error.message });
    return res.status(status).json({ error: error && error.message || "Employee could not be provisioned." });
  }
});
function responseOutputText(response) {
  if (response && typeof response.output_text === "string") return response.output_text;
  const output = response && Array.isArray(response.output) ? response.output : [];
  for (const item of output) {
    if (!item || !Array.isArray(item.content)) continue;
    for (const content of item.content) {
      if (content && typeof content.text === "string") return content.text;
    }
  }
  return "";
}

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
        date: Timestamp.fromDate(new Date(g.dateString))
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

// Automatically discover geographically tagged public hail videos for the
// Maps "Social Media Pictures" layer. Results are cached to protect quota.
exports.searchStormMedia = onRequest(
  { secrets: [YOUTUBE_API_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", "GET, OPTIONS");
    res.set("Access-Control-Allow-Headers", "Content-Type");
    res.set("Cache-Control", "public, max-age=900");
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "GET") return res.status(405).json({ error: "GET required" });

    try {
      const lat = Number(req.query.lat);
      const lng = Number(req.query.lng);
      const radiusMiles = Math.max(5, Math.min(200, Number(req.query.radiusMiles) || 75));
      const date = String(req.query.date || "").slice(0, 10);
      if (!Number.isFinite(lat) || lat < -90 || lat > 90 ||
          !Number.isFinite(lng) || lng < -180 || lng > 180 ||
          !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        return res.status(400).json({ error: "Valid lat, lng, and date are required" });
      }

      const cacheId = [
        date,
        lat.toFixed(2).replace(".", "_"),
        lng.toFixed(2).replace(".", "_"),
        Math.round(radiusMiles)
      ].join("-");
      const cacheRef = db.collection("stormMediaCache").doc(cacheId);
      const cached = await cacheRef.get();
      if (cached.exists) {
        const cachedData = cached.data() || {};
        const cachedAt = cachedData.cachedAt && cachedData.cachedAt.toMillis
          ? cachedData.cachedAt.toMillis() : 0;
        if (Date.now() - cachedAt < 30 * 60 * 1000 && Array.isArray(cachedData.items)) {
          return res.json({ items: cachedData.items, cached: true });
        }
      }

      const apiKey = YOUTUBE_API_KEY.value();
      if (!apiKey) return res.status(503).json({ error: "YouTube storm media is not configured" });
      const publishedAfter = new Date(date + "T00:00:00Z");
      publishedAfter.setUTCDate(publishedAfter.getUTCDate() - 1);
      const publishedBefore = new Date(date + "T23:59:59Z");
      publishedBefore.setUTCDate(publishedBefore.getUTCDate() + 1);

      const searchParams = new URLSearchParams({
        key: apiKey,
        part: "snippet",
        type: "video",
        q: "hail storm|hail damage|large hail",
        location: lat.toFixed(6) + "," + lng.toFixed(6),
        locationRadius: radiusMiles.toFixed(0) + "mi",
        publishedAfter: publishedAfter.toISOString(),
        publishedBefore: publishedBefore.toISOString(),
        order: "date",
        safeSearch: "strict",
        videoEmbeddable: "true",
        maxResults: "25"
      });
      const searchResponse = await fetch("https://www.googleapis.com/youtube/v3/search?" + searchParams);
      const searchJson = await searchResponse.json();
      if (!searchResponse.ok) {
        throw new Error((searchJson.error && searchJson.error.message) || "YouTube search failed");
      }

      const searchItems = Array.isArray(searchJson.items) ? searchJson.items : [];
      const videoIds = searchItems.map(item => item && item.id && item.id.videoId).filter(Boolean);
      if (!videoIds.length) {
        await cacheRef.set({ items: [], cachedAt: FieldValue.serverTimestamp() });
        return res.json({ items: [], cached: false });
      }

      const detailParams = new URLSearchParams({
        key: apiKey,
        part: "snippet,recordingDetails,status",
        id: videoIds.join(",")
      });
      const detailResponse = await fetch("https://www.googleapis.com/youtube/v3/videos?" + detailParams);
      const detailJson = await detailResponse.json();
      if (!detailResponse.ok) {
        throw new Error((detailJson.error && detailJson.error.message) || "YouTube video lookup failed");
      }

      const items = (detailJson.items || []).map(video => {
        const location = video.recordingDetails && video.recordingDetails.location;
        const snippet = video.snippet || {};
        const thumbnails = snippet.thumbnails || {};
        const thumbnail = thumbnails.medium || thumbnails.high || thumbnails.default || {};
        if (!location || !Number.isFinite(Number(location.latitude)) ||
            !Number.isFinite(Number(location.longitude))) return null;
        return {
          id: String(video.id || ""),
          title: String(snippet.title || "Storm video").slice(0, 180),
          channelTitle: String(snippet.channelTitle || "YouTube"),
          publishedAt: String(snippet.publishedAt || ""),
          thumbnailUrl: String(thumbnail.url || ""),
          lat: Number(location.latitude),
          lng: Number(location.longitude),
          url: "https://www.youtube.com/watch?v=" + encodeURIComponent(video.id)
        };
      }).filter(Boolean).slice(0, 20);

      await cacheRef.set({
        items,
        cachedAt: FieldValue.serverTimestamp()
      });
      return res.json({ items, cached: false });
    } catch (error) {
      logger.error("searchStormMedia failed", error);
      return res.status(500).json({ error: error.message || String(error) });
    }
  }
);
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
      date: Timestamp.fromDate(new Date(data.date))
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

// ---------------------------------------------------------------------------
// ABC Supply sandbox integration (Third-Party Aggregator OAuth user flow)
// ---------------------------------------------------------------------------
const crypto = require("crypto");

function setAbcCors(req, res) {
  const allowed = new Set([
    "https://hailmoneymap.web.app",
    "https://hailmoneymap.firebaseapp.com",
    "http://127.0.0.1:5500",
    "http://localhost:5500"
  ]);
  const origin = req.get("origin");
  if (origin && allowed.has(origin)) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
  }
  res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
}

async function requireHailMoneyUser(req) {
  const origin = req.get("origin") || "";
  if (process.env.FUNCTIONS_EMULATOR === "true" && (
    origin === "http://127.0.0.1:5500" || origin === "http://localhost:5500"
  )) {
    return { uid: "local-abc-sandbox-user", email: "local-sandbox@hailmoney.test" };
  }
  const header = req.get("authorization") || "";
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    const error = new Error("Sign in to Hail Money before connecting ABC Supply.");
    error.status = 401;
    throw error;
  }
  try {
    return await admin.auth().verifyIdToken(match[1]);
  } catch (_) {
    const error = new Error("Your Hail Money session has expired. Sign in again.");
    error.status = 401;
    throw error;
  }
}

function abcBasicAuthorization() {
  return "Basic " + Buffer.from(
    `${ABC_CLIENT_ID.value()}:${ABC_CLIENT_SECRET.value()}`,
    "utf8"
  ).toString("base64");
}

async function abcTokenRequest(params) {
  const response = await fetch(`${ABC_SANDBOX_AUTH_BASE}/token`, {
    method: "POST",
    headers: {
      Authorization: abcBasicAuthorization(),
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json"
    },
    body: new URLSearchParams(params).toString()
  });
  const body = await response.text();
  let data;
  try { data = JSON.parse(body); } catch (_) { data = { error_description: body }; }
  if (!response.ok) {
    const error = new Error(data.error_description || data.error || `ABC token request failed (${response.status}).`);
    error.status = 502;
    throw error;
  }
  return data;
}

async function getFreshAbcUserToken(uid) {
  const ref = db.collection("abcSupplyConnections").doc(uid);
  const snap = await ref.get();
  if (!snap.exists) {
    const error = new Error("Connect your ABC Supply account first.");
    error.status = 409;
    throw error;
  }
  const connection = snap.data();
  const expiresAtMs = connection.expiresAt && connection.expiresAt.toMillis
    ? connection.expiresAt.toMillis()
    : 0;
  if (connection.accessToken && expiresAtMs > Date.now() + 60000) {
    return connection.accessToken;
  }
  if (!connection.refreshToken) {
    const error = new Error("Reconnect your ABC Supply account.");
    error.status = 401;
    throw error;
  }
  const refreshed = await abcTokenRequest({
    grant_type: "refresh_token",
    refresh_token: connection.refreshToken,
    scope: ABC_USER_SCOPES
  });
  const expiresIn = Number(refreshed.expires_in || 1800);
  await ref.set({
    accessToken: refreshed.access_token,
    refreshToken: refreshed.refresh_token || connection.refreshToken,
    expiresAt: Timestamp.fromMillis(Date.now() + expiresIn * 1000),
    scope: refreshed.scope || connection.scope || ABC_USER_SCOPES,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });
  return refreshed.access_token;
}

async function abcApiRequest(uid, path, options = {}) {
  const token = await getFreshAbcUserToken(uid);
  const response = await fetch(`${ABC_SANDBOX_API_BASE}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  let data;
  try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { message: text }; }
  if (!response.ok) {
    const error = new Error(data.message || data.error || `ABC API request failed (${response.status}).`);
    error.status = response.status >= 400 && response.status < 500 ? response.status : 502;
    throw error;
  }
  return data;
}

exports.abcOAuthStart = onRequest(
  { secrets: [ABC_CLIENT_ID], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    try {
      const user = await requireHailMoneyUser(req);
      const state = crypto.randomBytes(32).toString("hex");
      const requestOrigin = req.get("origin") || "";
      const redirectUri = requestOrigin === "http://127.0.0.1:5500" || requestOrigin === "http://localhost:5500"
        ? ABC_LOCAL_REDIRECT_URI
        : ABC_REDIRECT_URI;
      await db.collection("abcOAuthStates").doc(state).set({
        uid: user.uid,
        redirectUri,
        createdAt: FieldValue.serverTimestamp(),
        expiresAt: Timestamp.fromMillis(Date.now() + 10 * 60 * 1000)
      });
      const url = new URL(`${ABC_SANDBOX_AUTH_BASE}/authorize`);
      url.searchParams.set("client_id", ABC_CLIENT_ID.value());
      url.searchParams.set("response_type", "code");
      url.searchParams.set("redirect_uri", redirectUri);
      url.searchParams.set("state", state);
      url.searchParams.set("scope", ABC_USER_SCOPES);
      return res.status(200).json({ authorizationUrl: url.toString(), environment: "sandbox" });
    } catch (error) {
      logger.error("abcOAuthStart failed", error);
      return res.status(error.status || 500).json({ error: error.message });
    }
  }
);

exports.abcOAuthCallback = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    try {
      const code = String((req.body && req.body.code) || req.query.code || "");
      const state = String((req.body && req.body.state) || req.query.state || "");
      if (!code || !state) return res.status(400).json({ error: "Missing ABC authorization code or state." });
      const stateRef = db.collection("abcOAuthStates").doc(state);
      const stateSnap = await stateRef.get();
      if (!stateSnap.exists) return res.status(400).json({ error: "This ABC connection request is invalid or has already been used." });
      const stateData = stateSnap.data();
      await stateRef.delete();
      if (!stateData.expiresAt || stateData.expiresAt.toMillis() < Date.now()) {
        return res.status(400).json({ error: "This ABC connection request expired. Start again from Hail Money." });
      }
      const tokens = await abcTokenRequest({
        grant_type: "authorization_code",
        redirect_uri: stateData.redirectUri || ABC_REDIRECT_URI,
        code
      });
      const expiresIn = Number(tokens.expires_in || 1800);
      await db.collection("abcSupplyConnections").doc(stateData.uid).set({
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token || null,
        tokenType: tokens.token_type || "Bearer",
        scope: tokens.scope || ABC_USER_SCOPES,
        expiresAt: Timestamp.fromMillis(Date.now() + expiresIn * 1000),
        environment: "sandbox",
        connectedAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });
      return res.status(200).json({ connected: true, environment: "sandbox" });
    } catch (error) {
      logger.error("abcOAuthCallback failed", error);
      return res.status(error.status || 500).json({ error: error.message });
    }
  }
);

exports.abcAccounts = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    try {
      const user = await requireHailMoneyUser(req);
      const data = await abcApiRequest(user.uid, "/api/account/v1/search/accounts", {
        method: "POST",
        body: JSON.stringify({
          filters: [
            { key: "accountType", condition: "equals", values: ["Ship-to"], joinCondition: "and" },
            { key: "storefront", condition: "equals", values: ["abc"] }
          ],
          pagination: { itemsPerPage: 50, pageNumber: 1 }
        })
      });
      return res.status(200).json(data);
    } catch (error) {
      logger.error("abcAccounts failed", error);
      return res.status(error.status || 500).json({ error: error.message });
    }
  }
);

exports.abcPriceItems = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireHailMoneyUser(req);
      const body = req.body || {};
      if (!body.shipToNumber || !body.branchNumber || !Array.isArray(body.lines) || !body.lines.length) {
        return res.status(400).json({ error: "Ship-To account, branch, and at least one item are required." });
      }
      if (body.lines.length > 50) return res.status(400).json({ error: "ABC allows at most 50 price lines per request." });
      const payload = {
        requestId: body.requestId || `hail-money-${Date.now()}`,
        shipToNumber: String(body.shipToNumber),
        branchNumber: String(body.branchNumber),
        purpose: ["estimating", "quoting", "ordering"].includes(body.purpose) ? body.purpose : "estimating",
        lines: body.lines
      };
      const data = await abcApiRequest(user.uid, "/api/pricing/v2/prices", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      return res.status(200).json(data);
    } catch (error) {
      logger.error("abcPriceItems failed", error);
      return res.status(error.status || 500).json({ error: error.message });
    }
  }
);

exports.abcFavoriteItems = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    try {
      const user = await requireHailMoneyUser(req);
      const billToNumber = String(req.query.billToNumber || "").trim();
      const branchNumber = String(req.query.branchNumber || "").trim();
      if (!billToNumber) return res.status(400).json({ error: "Bill-To account is required." });
      const query = new URLSearchParams({ itemsPerPage: "50", pageNumber: "1" });
      if (branchNumber) query.set("branchNumber", branchNumber);
      const data = await abcApiRequest(
        user.uid,
        `/api/product/v1/items/${encodeURIComponent(billToNumber)}/favorites?${query.toString()}`,
        { method: "GET" }
      );
      return res.status(200).json(data);
    } catch (error) {
      logger.error("abcFavoriteItems failed", error);
      return res.status(error.status || 500).json({ error: error.message });
    }
  }
);

exports.abcSearchProducts = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireHailMoneyUser(req);
      const search = String((req.body && req.body.search) || "").trim();
      const branchNumber = String((req.body && req.body.branchNumber) || "").trim();
      if (search.length < 2) return res.status(400).json({ error: "Enter at least two characters to search products." });
      const filters = [{
        key: "itemDescription",
        condition: "contains",
        values: [search],
        joinCondition: branchNumber ? "and" : null
      }];
      if (branchNumber) filters.push({
        key: "branchNumber",
        condition: "equals",
        values: [branchNumber],
        joinCondition: null
      });
      const data = await abcApiRequest(user.uid, "/api/product/v1/search/items", {
        method: "POST",
        body: JSON.stringify({
          filters,
          embed: ["branches", "variations"],
          pagination: { itemsPerPage: 50, pageNumber: 1 }
        })
      });
      return res.status(200).json(data);
    } catch (error) {
      logger.error("abcSearchProducts failed", error);
      return res.status(error.status || 500).json({ error: error.message });
    }
  }
);

exports.researchPermit = onRequest(
  { secrets: [OPENAI_API_KEY], timeoutSeconds: 540, memory: "512MiB", cors: false },
  async (req, res) => {
    permitCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });

    try {
      const user = await requireFirebaseUser(req);
      const body = req.body && typeof req.body === "object" ? req.body : {};
      const street = String(body.street || "").trim().slice(0, 180);
      const city = String(body.city || "").trim().slice(0, 100);
      const state = String(body.state || "").trim().toUpperCase().slice(0, 2);
      const zip = String(body.zip || "").trim().slice(0, 12);
      const projectType = String(body.projectType || "Roof replacement").trim().slice(0, 100);
      if (!street || !city || !state || !zip) {
        return res.status(400).json({ error: "Street, city, state, and ZIP are required." });
      }

      const address = `${street}, ${city}, ${state} ${zip}`;
      const prompt = [
        "You are the permit research agent for Hail Money, a US roofing and exterior-restoration CRM.",
        `Research the permit requirements for ${projectType} at this exact property: ${address}.`,
        "First determine whether the property is incorporated or unincorporated and identify the authority having jurisdiction.",
        "Search the official city or county clerk ordinances, building and zoning department, permit forms, online application portal, fee schedule, inspection instructions, and official state requirements.",
        "Prefer first-party government sources and official government-authorized permit portals. Use ICC only to identify model-code provisions adopted by the jurisdiction.",
        "Never infer that an online application or payment is available unless an official source supports it. Never invent a fee, phone number, URL, code edition, or requirement.",
        "If city and county sources conflict, report the conflict and set confidence to needs_verification.",
        "Return concise operational instructions for a contractor and include every material source URL used."
      ].join("\n");

      const openaiResponse = await fetch("https://api.openai.com/v1/responses", {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${OPENAI_API_KEY.value()}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model: "gpt-5.6-terra",
          reasoning: { effort: "medium" },
          tools: [{ type: "web_search", search_context_size: "high" }],
          input: prompt,
          max_output_tokens: 5000,
          text: {
            format: {
              type: "json_schema",
              name: "permit_research",
              strict: true,
              schema: {
                type: "object",
                additionalProperties: false,
                properties: {
                  jurisdiction: { type: "string" },
                  authorityName: { type: "string" },
                  authorityType: { type: "string", enum: ["city", "county", "state", "combined", "unknown"] },
                  departmentName: { type: "string" },
                  permitRequired: { type: "string", enum: ["required", "not_required", "uncertain"] },
                  status: { type: "string" },
                  permitSummary: { type: "string" },
                  applicationUrl: { type: "string" },
                  paymentUrl: { type: "string" },
                  phone: { type: "string" },
                  fee: { type: "string" },
                  adoptedCode: { type: "string" },
                  inspections: { type: "array", items: { type: "string" } },
                  instructions: { type: "string" },
                  confidence: { type: "string", enum: ["verified", "baseline", "needs_verification"] },
                  sources: {
                    type: "array",
                    items: {
                      type: "object",
                      additionalProperties: false,
                      properties: { title: { type: "string" }, url: { type: "string" }, publisher: { type: "string" } },
                      required: ["title", "url", "publisher"]
                    }
                  }
                },
                required: ["jurisdiction", "authorityName", "authorityType", "departmentName", "permitRequired", "status", "permitSummary", "applicationUrl", "paymentUrl", "phone", "fee", "adoptedCode", "inspections", "instructions", "confidence", "sources"]
              }
            }
          }
        }),
        timeout: 520000
      });

      const responseBody = await openaiResponse.json().catch(() => ({}));
      if (!openaiResponse.ok) {
        const message = responseBody && responseBody.error && responseBody.error.message;
        throw Object.assign(new Error(message || `Permit research provider HTTP ${openaiResponse.status}`), { statusCode: 502 });
      }
      const outputText = responseOutputText(responseBody);
      if (!outputText) throw Object.assign(new Error("Permit research returned no result."), { statusCode: 502 });
      let result;
      try { result = JSON.parse(outputText); }
      catch (error) { throw Object.assign(new Error("Permit research returned an unreadable result."), { statusCode: 502 }); }

      logger.info("Permit research completed", {
        uid: user.uid,
        address,
        authority: result.authorityName || "",
        confidence: result.confidence || ""
      });
      return res.status(200).json(result);
    } catch (error) {
      const status = Number(error && error.statusCode) || 500;
      logger.error("Permit research failed", { status, message: error && error.message });
      return res.status(status).json({ error: error && error.message || "Permit research failed." });
    }
  }
);
