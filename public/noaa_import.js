"use strict";

/**
 * noaa_import.js (SWDI version)
 *
 * Dev helper to pull storm events from NOAA SWDI (Local Storm Reports)
 * and save them into Firestore collection "stormDates".
 *
 * Assumes:
 *  - firebase compat SDKs are loaded in index.html
 *  - firebase.initializeApp(...) has already been called
 *  - const db = firebase.firestore(); exists in index.html
 */

if (typeof firebase === "undefined" || !firebase.firestore) {
  console.error("Firebase not found. Make sure index.html loads firebase-*-compat.js first.");
}

/* ======================== SWDI CONFIG ======================== */

/**
 * SWDI CSV endpoint.
 * SWDI does NOT require an API key.
 * Docs: https://www.ncdc.noaa.gov/swdiws/
 */
const SWDI_BASE = "https://www.ncdc.noaa.gov/swdiws/csv";

/**
 * Use a CORS proxy so the browser isn't calling NOAA directly.
 * Here we use api.allorigins.win, which expects ?url=... and returns the raw body.
 */
const SWDI_CORS_PROXY = "https://api.allorigins.win/raw?url=";

/**
 * Dataset:
 *  - "plsr" = Preliminary Local Storm Reports
 *        (hail, wind, tornado)
 */
const SWDI_DATASET = "plsr";

/**
 * How many years back to import.
 * 2 means: current year + last year
 */
const SWDI_YEARS_BACK = 2;

/**
 * Max rows per request.
 */
const SWDI_LIMIT = 1000000;

/* ======================== DATE HELPERS ======================== */

function formatDateYYYYMMDD(d) {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}${mm}${dd}`;
}

function buildYearRangesForBackfill() {
  const now = new Date();
  const currentYear = now.getUTCFullYear();
  const firstYear = currentYear - (SWDI_YEARS_BACK - 1);

  const ranges = [];

  for (let year = firstYear; year <= currentYear; year++) {
    const start = new Date(Date.UTC(year, 0, 1));
    const end = (year === currentYear)
      ? now
      : new Date(Date.UTC(year, 11, 31));

    ranges.push({
      year,
      startStr: formatDateYYYYMMDD(start),
      endStr: formatDateYYYYMMDD(end)
    });
  }

  return ranges;
}

/* ======================== SWDI URL BUILDER ======================== */

function buildSwdiUrl(startStr, endStr, stateCode) {
  const range = `${startStr}:${endStr}`;
  const params = new URLSearchParams({
    state: stateCode.toUpperCase(),
    limit: String(SWDI_LIMIT)
  });

  return `${SWDI_BASE}/${SWDI_DATASET}/${range}?${params.toString()}`;
}

/* ======================== CSV PARSER ======================== */

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
  if (!lines.length) return [];

  const header = lines[0].split(",").map(h => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(",");
    const obj = {};
    header.forEach((h, idx) => {
      obj[h] = (cols[idx] || "").trim();
    });
    rows.push(obj);
  }

  return rows;
}

/* ======================== FETCH SWDI ROWS ======================== */

async function fetchSwdiRowsForRange(stateCode, startStr, endStr) {
  // Build the raw SWDI URL
  const rawUrl = buildSwdiUrl(startStr, endStr, stateCode);
  // Wrap it with the CORS proxy, URL-encoded
  const proxiedUrl = SWDI_CORS_PROXY + encodeURIComponent(rawUrl);

  console.log("SWDI request:", proxiedUrl);

  const resp = await fetch(proxiedUrl);
  if (!resp.ok) {
    const txt = await resp.text();
    console.error("SWDI error", resp.status, txt);
    throw new Error(`SWDI HTTP ${resp.status} for ${proxiedUrl}`);
  }

  const text = await resp.text();
  const rows = parseCsv(text);
  console.log(`SWDI got ${rows.length} rows for ${stateCode} in ${startStr}-${endStr}`);
  return rows;
}

/* ======================== NORMALIZATION ======================== */

function normalizeSwdiRow(stateCode, desiredType, row) {
  const upperState = stateCode.toUpperCase();
  const upperDesired = desiredType.toUpperCase();

  const rawType =
    row.EVENT_TYPE ||
    row.EVENTTYPE ||
    row.TYPE ||
    row.type ||
    "";

  const rawTypeUpper = rawType.toString().toUpperCase();

  let normalizedType = null;
  if (rawTypeUpper.includes("HAIL")) {
    normalizedType = "HAIL";
  } else if (rawTypeUpper.includes("WIND")) {
    normalizedType = "WIND";
  } else if (rawTypeUpper.includes("TORNADO")) {
    normalizedType = "TORNADO";
  }

  if (!normalizedType) return null;

  if (upperDesired && normalizedType !== upperDesired) {
    return null;
  }

  const rowState = (row.STATE || row.state || "").toString().toUpperCase();
  if (rowState && rowState !== upperState) return null;

  const datePart =
    row.BEGIN_DATE ||
    row.BEGINDATE ||
    row.DATE ||
    row.date ||
    "";

  const timePart =
    row.BEGIN_TIME ||
    row.BEGINTIME ||
    row.TIME ||
    row.time ||
    "";

  let eventDate = null;
  if (datePart) {
    const d = datePart.toString().replace(/[^\d]/g, "");
    if (d.length === 8) {
      eventDate = `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
    }
  }

  const eventTimeRaw = timePart ? timePart.toString() : null;

  const lat = parseFloat(
    row.BEGIN_LAT ||
    row.LAT ||
    row.lat ||
    "0"
  );

  const lon = parseFloat(
    row.BEGIN_LON ||
    row.LON ||
    row.lon ||
    "0"
  );

  const magnitude =
    row.MAGNITUDE ||
    row.magnitude ||
    row.MAG ||
    null;

  const baseId =
    row.EVENT_ID ||
    row.EVENTID ||
    row.id ||
    `${upperState}_${eventDate || "nodate"}_${eventTimeRaw || "notime"}_${normalizedType}_${lat}_${lon}`;

  return {
    id: `swdi_${baseId}`,
    state: upperState,
    type: normalizedType,
    rawType: rawTypeUpper,
    eventDate: eventDate,
    eventTimeRaw: eventTimeRaw,
    lat,
    lon,
    magnitude,
    source: "SWDI",
    dataset: SWDI_DATASET,
    raw: row
  };
}

/* ======================== FIRESTORE HELPERS ======================== */

async function clearExistingSwdiStorms(stateCode, stormType) {
  const upperState = stateCode.toUpperCase();
  const upperType = stormType ? stormType.toUpperCase() : null;

  let query = db.collection("stormDates").where("state", "==", upperState);
  if (upperType) query = query.where("type", "==", upperType);

  const snap = await query.get();
  if (snap.empty) {
    console.log(`No existing storms to delete for ${upperState}/${upperType || "ANY"}`);
    return;
  }

  const batch = db.batch();
  snap.forEach(doc => batch.delete(doc.ref));
  await batch.commit();

  console.log(`Deleted ${snap.size} existing storms for ${upperState}/${upperType || "ANY"}`);
}

async function saveNormalizedStormDoc(doc) {
  const stormsRef = db.collection("stormDates");
  const ref = doc.id ? stormsRef.doc(doc.id) : stormsRef.doc();
  const payload = Object.assign({}, doc);
  delete payload.id;

  // Sanitize string fields
  if (payload.state !== undefined) payload.state = String(payload.state ?? "").trim().toUpperCase();
  if (payload.type !== undefined) payload.type = String(payload.type ?? "").trim().toLowerCase();

  // Standardize coordinates
  payload.lat = Number(payload.lat);
  payload.lng = Number(payload.lng ?? payload.lon);
  delete payload.lon;

  await ref.set(
    Object.assign(payload, {
      importedAt: firebase.firestore.FieldValue.serverTimestamp()
    }),
    { merge: true }
  );
}

/* ======================== MAIN IMPORT FUNCTION ======================== */

async function importStormsFromSwdi(stateCode, stormType) {
  if (!stateCode) throw new Error("importStormsFromSwdi requires a stateCode like 'AZ'.");
  if (!stormType) throw new Error("importStormsFromSwdi requires a stormType like 'HAIL', 'WIND', or 'TORNADO'.");

  const upperState = stateCode.toUpperCase();
  const upperType = stormType.toUpperCase();

  console.log(`SWDI import starting for ${upperState} / ${upperType} (last ${SWDI_YEARS_BACK} years)`);

  const ranges = buildYearRangesForBackfill();

  await clearExistingSwdiStorms(upperState, upperType);

  let totalKept = 0;

  for (const r of ranges) {
    const rows = await fetchSwdiRowsForRange(upperState, r.startStr, r.endStr);

    for (const row of rows) {
      const normalized = normalizeSwdiRow(upperState, upperType, row);
      if (!normalized) continue;

      await saveNormalizedStormDoc(normalized);
      totalKept++;
    }

    console.log(
      `Finished SWDI segment for ${upperState}/${upperType}, year ${r.year}. Running total kept: ${totalKept}`
    );
  }

  console.log(`SWDI import complete for ${upperState}/${upperType}. Total storms saved: ${totalKept}`);
}

/* ======================== ONE-TIME SANITIZER ======================== */

async function sanitizeStormDates() {
  console.log("Starting stormDates sanitization...");

  const col = db.collection("stormDates");
  let lastDoc = null;
  let totalScanned = 0;
  let totalUpdated = 0;

  while (true) {
    let query = col.limit(400);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snap = await query.get();
    if (snap.empty) break;

    for (const doc of snap.docs) {
      totalScanned++;
      const data = doc.data();
      const originalState = data.state;
      const originalType = data.type;

      const cleanState = String(data.state ?? "").trim().toUpperCase();
      const cleanType = String(data.type ?? "").trim().toLowerCase();

      if (cleanState !== originalState || cleanType !== originalType) {
        await doc.ref.update({ state: cleanState, type: cleanType });
        totalUpdated++;
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
  }

  console.log(`Sanitization complete. Scanned: ${totalScanned}, Updated: ${totalUpdated}`);
}

/* ======================== GLOBAL EXPORT ======================== */

window.importStormsFromSwdi = importStormsFromSwdi;
window.sanitizeStormDates = sanitizeStormDates;

