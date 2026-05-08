#!/usr/bin/env node
import { createClient } from "@supabase/supabase-js";
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {auth:{persistSession:false}});

const {data} = await sb.from('storm_polygons')
  .select('swath_index,centroid_lat,centroid_lon,area_sq_mi,band_min,band_max,band_label,polygon_geojson')
  .eq('event_date', '2025-03-14')
  .eq('source', 'nexrad_iem')
  .order('swath_index', {ascending: true});

console.log(`Total rows: ${data.length}`);

// Group by corridor_index from properties
const byCorr = {};
for (const r of data) {
  const gj = typeof r.polygon_geojson === 'string' ? JSON.parse(r.polygon_geojson) : r.polygon_geojson;
  const props = gj.properties || {};
  const ci = props.corridor_index ?? 'unknown';
  if (!byCorr[ci]) byCorr[ci] = { rows: [], nxr: props.nexrad + ':' + props.storm_id, pts: props.track_points, hail: props.max_hail_inches, start: props.event_start_utc, end: props.event_end_utc };
  byCorr[ci].rows.push(r);
}

console.log(`\nCorridors: ${Object.keys(byCorr).length}`);
console.log('');

// For each corridor, compute its spatial extent
for (const [ci, info] of Object.entries(byCorr).sort((a,b) => Number(a[0]) - Number(b[0]))) {
  // Get the outermost (lowest band_min) row for extent
  const outerRow = info.rows.reduce((a, b) => (a.band_min <= b.band_min ? a : b));
  const gj = typeof outerRow.polygon_geojson === 'string' ? JSON.parse(outerRow.polygon_geojson) : outerRow.polygon_geojson;
  const coords = (gj.geometry || gj).coordinates[0];
  let latMin=Infinity, latMax=-Infinity, lonMin=Infinity, lonMax=-Infinity;
  for (const c of coords) { lonMin=Math.min(lonMin,c[0]); lonMax=Math.max(lonMax,c[0]); latMin=Math.min(latMin,c[1]); latMax=Math.max(latMax,c[1]); }
  const latSpanKm = (latMax - latMin) * 111.32;
  const lonSpanKm = (lonMax - lonMin) * 111.32 * Math.cos((latMin+latMax)/2*Math.PI/180);
  const lengthKm = Math.max(latSpanKm, lonSpanKm);
  const widthKm = lengthKm > 0 ? (outerRow.area_sq_mi * 2.59 / lengthKm) : 0;
  
  console.log(
    `Corr#${ci} ${info.nxr} pts=${info.pts} hail=${info.hail}" bands=${info.rows.length}` +
    ` ctr=[${outerRow.centroid_lat.toFixed(2)},${outerRow.centroid_lon.toFixed(2)}]` +
    ` outerArea=${outerRow.area_sq_mi}mi²` +
    ` len=${lengthKm.toFixed(0)}km width=${widthKm.toFixed(1)}km` +
    ` lat=[${latMin.toFixed(2)},${latMax.toFixed(2)}] lon=[${lonMin.toFixed(2)},${lonMax.toFixed(2)}]` +
    ` time=${info.start?.slice(11,16)}-${info.end?.slice(11,16)}`
  );
}

// Identify St. Louis metro area corridors (lat ~38.4-39.0, lon ~-90.7 to -89.8)
console.log('\n--- St. Louis metro area corridors (lat 38.0-39.0, lon -91.5 to -89.5) ---');
for (const [ci, info] of Object.entries(byCorr)) {
  const outerRow = info.rows.reduce((a, b) => (a.band_min <= b.band_min ? a : b));
  const lat = outerRow.centroid_lat;
  const lon = outerRow.centroid_lon;
  if (lat >= 38.0 && lat <= 39.0 && lon >= -91.5 && lon <= -89.5) {
    console.log(`  Corr#${ci} ${info.nxr} ctr=[${lat.toFixed(2)},${lon.toFixed(2)}] area=${outerRow.area_sq_mi}mi² hail=${info.hail}"`);
  }
}

// Show cluster structure from ingest
console.log('\n--- Pairwise distances between corridor centroids ---');
const entries = Object.entries(byCorr);
for (let i = 0; i < entries.length; i++) {
  for (let j = i + 1; j < entries.length; j++) {
    const [ci, a] = entries[i];
    const [cj, b] = entries[j];
    const rA = a.rows[0], rB = b.rows[0];
    const dLat = rA.centroid_lat - rB.centroid_lat;
    const dLon = rA.centroid_lon - rB.centroid_lon;
    const cosLat = Math.cos(rA.centroid_lat * Math.PI / 180);
    const distKm = Math.sqrt((dLat * 111.32)**2 + (dLon * 111.32 * cosLat)**2);
    if (distKm < 150) {
      console.log(`  #${ci} ↔ #${cj}: ${distKm.toFixed(0)}km`);
    }
  }
}
