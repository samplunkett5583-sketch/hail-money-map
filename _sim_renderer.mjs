const url='https://ehegjhlkadtpnkborlbz.supabase.co';
const key='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVoZWdqaGxrYWR0cG5rYm9ybGJ6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NjcwNTY2NiwiZXhwIjoyMDgyMjgxNjY2fQ.KbEohwp-boV53yY31Ut3epMtN564STGsl9T9KtFeGLQ';

const VALID_SWATH_SOURCES = ['hailtrace', 'mrms', 'swdi', 'manual', 'corridor', 'nexrad_poc', 'nexrad_iem'];
const PREFERRED_CORRIDOR_SOURCES = ['corridor', 'nexrad_iem', 'nexrad_poc'];

const res = await fetch(url+'/rest/v1/storm_polygons?event_date=eq.2026-03-22&order=source_priority.asc,created_at.desc&select=id,source,source_product,source_priority,swath_index,area_sq_mi,centroid_lat,centroid_lon,polygon_geojson',{
  headers:{apikey:key,Authorization:'Bearer '+key}
});
const rows = await res.json();
if (!Array.isArray(rows)) { console.log('API error:', JSON.stringify(rows).substring(0,500)); process.exit(1); }

console.log('--- SIMULATING mapsDrawSavedStormPolygonForDate("2026-03-22", rows) ---');
console.log('raw rows: '+rows.length);

var validRows = rows.filter(r => r && VALID_SWATH_SOURCES.indexOf((r.source||'').toLowerCase()) !== -1)
  .sort((a,b) => (a.source_priority||99) - (b.source_priority||99));
console.log('After VALID_SWATH_SOURCES filter: '+validRows.length);

var nexradIemIndices = [];
validRows.forEach((r,i) => {
  var src = (r.source||'').toLowerCase();
  if (src === 'nexrad_iem' && PREFERRED_CORRIDOR_SOURCES.indexOf(src) === -1) nexradIemIndices.push(i);
});
console.log('nexrad_iem indices for cleanup: '+nexradIemIndices.length+' (should be 0 for corridor data)');

var totalPolys = 0;
var rejectedGeom = 0;
var rejectedCleanup = 0;

validRows.forEach((row, ri) => {
  var gj = row.polygon_geojson;
  var rowSrc = (row.source||'').toLowerCase();
  var isPreferred = PREFERRED_CORRIDOR_SOURCES.indexOf(rowSrc) !== -1;

  if (gj && typeof gj === 'string') {
    try { gj = JSON.parse(gj); } catch(e) {
      console.log('  ROW '+ri+': REJECTED - parse error');
      rejectedGeom++; return;
    }
  }
  if (!gj || !gj.type) {
    console.log('  ROW '+ri+': REJECTED - no type');
    rejectedGeom++; return;
  }

  var geometries = [];
  if (gj.type === 'FeatureCollection' && Array.isArray(gj.features)) {
    gj.features.forEach(f => {
      if (f && f.type === 'Feature' && f.geometry) geometries.push(f.geometry);
      else if (f && f.coordinates) geometries.push(f);
    });
  } else if (gj.type === 'Feature' && gj.geometry) {
    geometries.push(gj.geometry);
  } else if (gj.coordinates) {
    geometries.push(gj);
  } else {
    console.log('  ROW '+ri+': REJECTED - no coordinates/geometry');
    rejectedGeom++; return;
  }

  var ringArrays = [];
  geometries.forEach(geom => {
    if (geom.type === 'Polygon' && Array.isArray(geom.coordinates)) {
      ringArrays.push(geom.coordinates[0]);
    } else if (geom.type === 'MultiPolygon' && Array.isArray(geom.coordinates)) {
      geom.coordinates.forEach(poly => {
        if (Array.isArray(poly) && Array.isArray(poly[0])) ringArrays.push(poly[0]);
      });
    }
  });
  if (!ringArrays.length) {
    console.log('  ROW '+ri+': REJECTED - no valid rings');
    rejectedGeom++; return;
  }

  var allLats=[], allLons=[];
  ringArrays.forEach(ring => {
    if (!Array.isArray(ring)) return;
    ring.forEach(c => { allLats.push(c[1]); allLons.push(c[0]); });
  });
  if (allLats.length > 0) {
    var bboxLatSpan = Math.max(...allLats) - Math.min(...allLats);
    var bboxLonSpan = Math.max(...allLons) - Math.min(...allLons);
    var storedArea = Number(row.area_sq_mi) || 0;
    if (!isPreferred && (bboxLatSpan > 3.0 || bboxLonSpan > 3.0 || storedArea > 800)) {
      console.log('  ROW '+ri+': REJECTED oversized bbox='+bboxLatSpan.toFixed(2)+'x'+bboxLonSpan.toFixed(2)+' area='+storedArea);
      rejectedGeom++; return;
    }
  }

  var drawnRings = 0;
  ringArrays.forEach(ring => {
    if (!Array.isArray(ring) || ring.length < 3) return;
    var valid = ring.filter(c => isFinite(Number(c[1])) && isFinite(Number(c[0]))).length;
    if (valid >= 3) drawnRings++;
  });

  console.log('  ROW '+ri+': src='+row.source+'/'+row.source_product+' pri='+row.source_priority+
    ' rings='+ringArrays.length+' drawnRings='+drawnRings+' isPreferred='+isPreferred+
    ' bboxLat='+(allLats.length ? (Math.max(...allLats)-Math.min(...allLats)).toFixed(2) : '?')+
    ' bboxLon='+(allLons.length ? (Math.max(...allLons)-Math.min(...allLons)).toFixed(2) : '?'));
  totalPolys += drawnRings;
});

console.log();
console.log('=== SIMULATION RESULT ===');
console.log('Valid rows: '+validRows.length);
console.log('Rejected (invalid geom): '+rejectedGeom);
console.log('Rejected (cleanup): '+rejectedCleanup);
console.log('Total Google Maps Polygons that SHOULD be created: '+totalPolys);
console.log('All rows use PREFERRED source: '+(validRows.every(r=>PREFERRED_CORRIDOR_SOURCES.indexOf((r.source||'').toLowerCase())!==-1)));
