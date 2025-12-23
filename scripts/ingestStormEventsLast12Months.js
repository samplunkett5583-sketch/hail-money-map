#!/usr/bin/env node

/**
 * ingestStormEventsLast12Months.js
 *
 * Ingests NOAA/NCEI Storm Events Database (last 12 months) for Hail, Tornado, Wind, Hurricane into Firestore stormDates.
 *
 * Source: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
 * Format: Storm-Data-Bulk-csv-Format.pdf (Details + Locations + Fatalities linked by EVENT_ID)
 *
 * Note: Storm Events DB can lag for the most recent period.
 *
 * Run: node scripts/ingestStormEventsLast12Months.js
 */

const https = require('https');
const zlib = require('zlib');
const csv = require('csv-parser');
const admin = require('firebase-admin');

const PROJECT_ID = "hailmoneymap";
console.log(`INGEST_PROJECT_ID ${PROJECT_ID}`);

const STATE_NAME_TO_CODE = {
  ALABAMA: 'AL', ALASKA: 'AK', ARIZONA: 'AZ', ARKANSAS: 'AR', CALIFORNIA: 'CA',
  COLORADO: 'CO', CONNECTICUT: 'CT', DELAWARE: 'DE', FLORIDA: 'FL', GEORGIA: 'GA',
  HAWAII: 'HI', IDAHO: 'ID', ILLINOIS: 'IL', INDIANA: 'IN', IOWA: 'IA', KANSAS: 'KS',
  KENTUCKY: 'KY', LOUISIANA: 'LA', MAINE: 'ME', MARYLAND: 'MD', MASSACHUSETTS: 'MA',
  MICHIGAN: 'MI', MINNESOTA: 'MN', MISSISSIPPI: 'MS', MISSOURI: 'MO', MONTANA: 'MT',
  NEBRASKA: 'NE', NEVADA: 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM',
  'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', OHIO: 'OH', OKLAHOMA: 'OK',
  OREGON: 'OR', PENNSYLVANIA: 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
  'SOUTH DAKOTA': 'SD', TENNESSEE: 'TN', TEXAS: 'TX', UTAH: 'UT', VERMONT: 'VT',
  VIRGINIA: 'VA', WASHINGTON: 'WA', 'WEST VIRGINIA': 'WV', WISCONSIN: 'WI', WYOMING: 'WY'
};

// Initialize Firebase Admin
const credPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
if (!credPath) {
  console.error('Error: GOOGLE_APPLICATION_CREDENTIALS environment variable not set. Please set it to the path of the service account JSON file.');
  process.exit(1);
}
admin.initializeApp({
  projectId: PROJECT_ID,
  credential: admin.credential.cert(require(credPath))
});
console.log('Firebase admin initialized for project:', PROJECT_ID);
const db = admin.firestore();

const BASE_URL = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/';

async function main() {
  console.log('Script started.');
  console.log('Starting NCEI Storm Events ingest...');

  const currentYear = new Date().getFullYear();
  const previousYear = currentYear - 1;
  const years = [previousYear, currentYear];

  console.log('Fetching file lists from NOAA...');
  // Get latest files for each year
  const files = {};
  for (const year of years) {
    const yearFiles = await getFilesForYear(year);
    files[year] = yearFiles;
  }
  console.log('File lists fetched.');

  console.log('Chosen files:', files);

  const locationsMap = new Map();

  console.log('Starting location processing...');
  // Process locations first
  for (const year of years) {
    const locFile = files[year].locations;
    if (!locFile) continue;
    console.log(`Processing locations: ${locFile}`);
    const data = await downloadAndParseCSV(BASE_URL + locFile);
    if (data.length > 0) {
      console.log("LOC_KEYS", Object.keys(data[0]));
      console.log("LOC_SAMPLE", data[0]);
    }
    let totalLocations = 0;
    data.forEach(row => {
      const eventId = (row.EVENT_ID || '').trim();
      if (!locationsMap.has(eventId)) locationsMap.set(eventId, []);
      const lat = parseFloat(row.BEGIN_LAT || row.END_LAT || row.LATITUDE || 0);
      const lng = parseFloat(row.BEGIN_LON || row.END_LON || row.LONGITUDE || 0);
      if (Number.isFinite(lat) && Number.isFinite(lng)) {
        locationsMap.get(eventId).push({ lat, lng });
      }
      totalLocations++;
    });
    console.log(`Processed ${totalLocations} location rows`);
  }
  console.log('Location processing complete.');

  console.log('Starting details processing...');
  // Process details
  const events = [];
  const oneYearAgo = new Date();
  oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

  let totalDetails = 0, matched = 0;
  for (const year of years) {
    const detFile = files[year].details;
    if (!detFile) continue;
    console.log(`Processing details: ${detFile}`);
    const data = await downloadAndParseCSV(BASE_URL + detFile);
    data.forEach(row => {
      totalDetails++;
      const beginYear = parseInt(row.BEGIN_YEARMONTH.slice(0, 4));
      const beginMonth = parseInt(row.BEGIN_YEARMONTH.slice(4, 6)) - 1;
      const beginDay = parseInt(row.BEGIN_DAY);
      const beginTime = row.BEGIN_TIME || '0000';
      const hour = parseInt(beginTime.slice(0, 2));
      const minute = parseInt(beginTime.slice(2, 4));
      const beginDate = new Date(beginYear, beginMonth, beginDay, hour, minute);

      if (beginDate >= oneYearAgo) {
        const eventType = (row.EVENT_TYPE || '').trim().toLowerCase();
        let normalizedType = null;
        if (eventType.includes('hail')) normalizedType = 'hail';
        else if (eventType.includes('tornado')) normalizedType = 'tornado';
        else if (eventType.includes('wind') || eventType.includes('thunderstorm wind')) normalizedType = 'wind';
        else if (eventType.includes('hurricane') || eventType.includes('tropical storm')) normalizedType = 'hurricane';

        if (normalizedType) {
          let state = (row.STATE || '').trim().toUpperCase();
          const originalState = state;
          state = STATE_NAME_TO_CODE[state] || state;
          if (state !== originalState) {
            console.log(`STATE_NORMALIZED: ${originalState} → ${state}`);
          }
          const dateString = beginDate.toISOString().slice(0, 10);
          const eventId = (row.EVENT_ID || '').trim();
          const locations = locationsMap.get(eventId) || [];
          let lat = null, lng = null;
          if (locations.length > 0) {
            matched++;
            lat = locations[0].lat;
            lng = locations[0].lng;
          }
          events.push({ state, type: normalizedType, dateString, lat, lng, eventId });
        }
      }
    });
  }
  console.log('Details processing complete.');

  console.log(`Total details: ${totalDetails}, Matched: ${matched}, Unmatched: ${totalDetails - matched}`);

  console.log(`Parsed ${events.length} events in last 12 months`);

  // State distribution
  const stateFreq = new Map();
  events.forEach(e => {
    const count = stateFreq.get(e.state) || 0;
    stateFreq.set(e.state, count + 1);
  });
  const sortedStates = Array.from(stateFreq.entries()).sort((a, b) => b[1] - a[1]);
  console.log(`Total event rows: ${events.length}`);
  console.log(`Top 20 states: ${sortedStates.slice(0, 20).map(([s, c]) => `${s}:${c}`).join(', ')}`);
  console.log(`TX count: ${stateFreq.get('TX') || 0}`);
  console.log(`TX space count: ${stateFreq.get('TX ') || 0}`);
  console.log(`Texas count: ${stateFreq.get('TEXAS') || 0}`);

  // Group by state_type_date
  const grouped = new Map();
  let sampleKey = null;
  events.forEach(event => {
    const key = `${event.state}_${event.type}_${event.dateString}`;
    if (!grouped.has(key)) {
      grouped.set(key, { state: event.state, type: event.type, dateString: event.dateString, lat: null, lng: null, count: 0 });
      if (!sampleKey) sampleKey = key;
    }
    const group = grouped.get(key);
    if (event.lat != null && event.lng != null && (group.lat == null || group.lng == null)) {
      group.lat = event.lat;
      group.lng = event.lng;
      if (key === sampleKey) console.log(`Setting coords for ${key}: ${event.lat}, ${event.lng}`);
    } else if (event.lat != null && event.lng != null && group.lat != null && group.lng != null) {
      if (key === sampleKey) console.log(`Skipping overwrite for ${key}: has ${group.lat},${group.lng}, trying ${event.lat},${event.lng}`);
    }
    group.count++;
  });

  console.log(`Grouped into ${grouped.size} storm date docs`);

  let withCoords = 0, withoutCoords = 0, missingIds = [];
  for (const [key, data] of grouped) {
    if (data.lat != null && data.lng != null) withCoords++; else {
      withoutCoords++;
      if (missingIds.length < 5) missingIds.push(key);
    }
  }
  console.log(`Grouped docs with coords: ${withCoords}, missing coords: ${withoutCoords}`);

  console.log('Starting Firestore batch write...');
  // Upsert to Firestore
  const batch = db.batch();
  const col = db.collection('stormDates');
  let written = 0, updated = 0;
  for (const [key, data] of grouped) {
    const docRef = col.doc(key);
    const existing = await docRef.get();
    if (existing.exists) {
      const existingData = existing.data();
      data.count += existingData.count || 0;
      updated++;
    } else {
      written++;
    }
    const fields = {
      state: data.state,
      type: data.type,
      dateString: data.dateString.trim(),
      count: data.count,
      date: admin.firestore.Timestamp.fromDate(new Date(data.dateString)),
      source: 'ncei_stormevents'
    };
    if (Number.isFinite(data.lat) && Number.isFinite(data.lng)) {
      fields.lat = data.lat;
      fields.lng = data.lng;
    }
    batch.set(docRef, fields, { merge: true });
  }
  await batch.commit();
  console.log('Firestore batch write complete.');

  console.log(`Grouped docs: ${grouped.size}, Written: ${written}, Updated: ${updated}, With coords: ${withCoords}, Missing coords: ${withoutCoords}, Sample missing: ${missingIds.join(', ')}`);

  let txWithCoords = 0;
  for (const [key, data] of grouped) {
    if (key.startsWith('TX_') && data.lat != null && data.lng != null) txWithCoords++;
  }
  console.log(`TX documents written with coords: ${txWithCoords}`);
}

// Get latest files for a year
async function getFilesForYear(year) {
  const html = await fetchText(BASE_URL);
  const files = html.match(/href="([^"]*\.csv\.gz)"/g).map(m => m.slice(6, -1)).filter(f => f.includes(`d${year}`));
  const details = files.filter(f => f.includes('details')).sort().reverse()[0];
  const locations = files.filter(f => f.includes('locations')).sort().reverse()[0];
  return { details, locations };
}

// Download and parse CSV
async function downloadAndParseCSV(url) {
  const buffer = await downloadGzipped(url);
  const results = [];
  const stream = require('stream');
  const readable = new stream.Readable();
  readable._read = () => {};
  readable.push(buffer);
  readable.push(null);
  return new Promise((resolve, reject) => {
    readable.pipe(csv()).on('data', data => results.push(data)).on('end', () => resolve(results)).on('error', reject);
  });
}

// Download gzipped file
async function downloadGzipped(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const buffer = Buffer.concat(chunks);
        zlib.gunzip(buffer, (err, decompressed) => {
          if (err) reject(err);
          else resolve(decompressed);
        });
      });
    }).on('error', reject);
  });
}

// Fetch text
async function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });
}

main().catch(console.error);