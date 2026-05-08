// Compare swath-render corridor descriptors vs stored polygon geometry for 2026-03-22
const url = 'https://ehegjhlkadtpnkborlbz.supabase.co';
const key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVoZWdqaGxrYWR0cG5rYm9ybGJ6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NjcwNTY2NiwiZXhwIjoyMDgyMjgxNjY2fQ.KbEohwp-boV53yY31Ut3epMtN564STGsl9T9KtFeGLQ';

// 1. Call swath-render for 2026-03-22 and see the corridor descriptors
const swathResp = await fetch(`${url}/functions/v1/swath-render?date=2026-03-22`, {
  headers: { Authorization: `Bearer ${key}`, apikey: key }
});
const swathData = await swathResp.json();

console.log('=== SWATH-RENDER DESCRIPTORS ===');
console.log('Model:', swathData.model);
console.log('Point count:', swathData.pointCount);
console.log('Corridor count:', (swathData.corridors || []).length);
console.log('Outlier count:', (swathData.outliers || []).length);

for (const c of (swathData.corridors || [])) {
  const lats = c.centerline.map(p => p[1]);
  const lons = c.centerline.map(p => p[0]);
  const widths = c.widthProfile;
  console.log(`\n  Corridor ${c.id}:`);
  console.log(`    reports: ${c.reportCount}`);
  console.log(`    centerline points: ${c.centerline.length}`);
  console.log(`    lat range: ${Math.min(...lats).toFixed(3)}..${Math.max(...lats).toFixed(3)}`);
  console.log(`    lon range: ${Math.min(...lons).toFixed(3)}..${Math.max(...lons).toFixed(3)}`);
  console.log(`    width range: ${Math.min(...widths).toFixed(1)}..${Math.max(...widths).toFixed(1)} km`);
  console.log(`    maxHailIn: ${c.maxHailIn}"`);
  console.log(`    severity range: ${Math.min(...c.severityProfile).toFixed(2)}..${Math.max(...c.severityProfile).toFixed(2)}`);
}

// 2. Get stored polygon geometry for comparison
console.log('\n=== STORED POLYGON GEOMETRY (storm_polygons) ===');
const polyResp = await fetch(`${url}/rest/v1/storm_polygons?event_date=eq.2026-03-22&select=id,source,source_product,swath_index,area_sq_mi,polygon_geojson&order=swath_index.asc`, {
  headers: { apikey: key, Authorization: `Bearer ${key}` }
});
const polyRows = await polyResp.json();
console.log('Stored rows:', polyRows.length);

for (let i = 0; i < polyRows.length; i++) {
  const r = polyRows[i];
  let gj = r.polygon_geojson;
  if (typeof gj === 'string') gj = JSON.parse(gj);
  
  // Count sub-polygons
  let subCount = 0;
  let totalCoords = 0;
  if (gj.type === 'MultiPolygon') {
    subCount = gj.coordinates.length;
    gj.coordinates.forEach(p => { totalCoords += p[0].length; });
  } else if (gj.type === 'Polygon') {
    subCount = 1;
    totalCoords = gj.coordinates[0].length;
  }
  
  // Get bbox of all coordinates
  const allLats = [];
  const allLons = [];
  function collectCoords(coords) {
    if (Array.isArray(coords[0]) && Array.isArray(coords[0][0])) {
      coords.forEach(c => collectCoords(c));
    } else if (Array.isArray(coords[0]) && typeof coords[0][0] === 'number') {
      coords.forEach(c => { allLons.push(c[0]); allLats.push(c[1]); });
    }
  }
  collectCoords(gj.coordinates);
  
  console.log(`\n  Row ${i}: src=${r.source}/${r.source_product} idx=${r.swath_index} area=${r.area_sq_mi}mi²`);
  console.log(`    type=${gj.type} sub-polygons=${subCount} coords=${totalCoords}`);
  console.log(`    lat range: ${Math.min(...allLats).toFixed(3)}..${Math.max(...allLats).toFixed(3)}`);
  console.log(`    lon range: ${Math.min(...allLons).toFixed(3)}..${Math.max(...allLons).toFixed(3)}`);
  
  // Show individual sub-polygon centroids to see scatter pattern
  if (gj.type === 'MultiPolygon') {
    gj.coordinates.forEach((poly, pi) => {
      const ring = poly[0];
      const cLat = ring.reduce((s, c) => s + c[1], 0) / ring.length;
      const cLon = ring.reduce((s, c) => s + c[0], 0) / ring.length;
      // Approximate area in sq mi
      const latSpan = Math.max(...ring.map(c=>c[1])) - Math.min(...ring.map(c=>c[1]));
      const lonSpan = Math.max(...ring.map(c=>c[0])) - Math.min(...ring.map(c=>c[0]));
      const areaMi2 = latSpan * 69 * lonSpan * 69 * Math.cos(cLat * Math.PI/180);
      console.log(`      sub${pi}: center=[${cLat.toFixed(3)}, ${cLon.toFixed(3)}] ~${areaMi2.toFixed(1)}mi² pts=${ring.length}`);
    });
  }
}

// 3. KEY QUESTION: How were these polygons generated?
// The swath-render returns centerline+width descriptors.
// The stored geometry is actual polygon rings.
// Something must have converted centerline+width → buffered polygon.
// Let's check if the stored polygons look like "buffered centerlines" or something else.
console.log('\n=== ANALYSIS: Do stored polygons match swath-render corridor layout? ===');
const corridors = swathData.corridors || [];
console.log(`swath-render produces ${corridors.length} corridor(s) from ${swathData.pointCount} LSR points`);
console.log(`storm_polygons has ${polyRows.length} rows with ${polyRows.reduce((s,r) => {
  let gj = r.polygon_geojson;
  if (typeof gj === 'string') gj = JSON.parse(gj);
  if (gj.type === 'MultiPolygon') return s + gj.coordinates.length;
  return s + 1;
}, 0)} total sub-polygons`);

// Check if corridor count matches sub-polygon count
if (corridors.length === 1) {
  console.log('\nswath-render clusters ALL 190 points into a SINGLE corridor.');
  console.log('But stored geometry has multiple disconnected sub-polygons.');
  console.log('This suggests the persister may have split the single corridor into per-point buffers,');
  console.log('or used a different algorithm entirely.');
}
