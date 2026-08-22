import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
const fixture = JSON.parse(fs.readFileSync(new URL('./fixtures/may3-alton-authority.json', import.meta.url), 'utf8'));

function extractFunction(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} must exist`);
  const brace = html.indexOf('{', start);
  let depth = 0;
  for (let i = brace; i < html.length; i += 1) {
    if (html[i] === '{') depth += 1;
    if (html[i] === '}') {
      depth -= 1;
      if (depth === 0) return html.slice(start, i + 1);
    }
  }
  throw new Error(`Could not extract ${name}`);
}

const mapsState = {
  showDerivedCorridors: false,
  showQuarantinedGeometry: false,
};
const context = vm.createContext({
  console,
  mapsState,
  mapsNormalizeDate: (value) => String(value || '').slice(0, 10),
  mapsIsSwathDebugMode: () => false,
  mapsAcceptedOnlyFeatureEnabled: () => false,
});
vm.runInContext(`${extractFunction('mapsHailRowProvenance')}\n${extractFunction('mapsSelectHailCoverageRows')}`, context);

function rectangularFeature(bounds, reportCount) {
  const [west, south, east, north] = bounds;
  return {
    type: 'Feature',
    properties: {
      source_model: 'rule_v1', saved_source: 'swath_render_saved', corridor_id: 1, report_count: reportCount,
    },
    geometry: { type: 'Polygon', coordinates: [[[west, south], [east, south], [east, north], [west, north], [west, south]]] },
  };
}

const reportRows = fixture.reportDerivedRows.map((row) => ({
  id: row.id,
  event_date: fixture.eventDate,
  storm_type: 'hail',
  source: 'swath_render_saved',
  source_product: 'rule_v1_bands',
  source_priority: 2,
  swath_index: row.index,
  band_min: row.band[0],
  band_max: row.band[1],
  polygon_geojson: rectangularFeature(row.bounds, row.reportCount),
}));
const radarRow = {
  id: fixture.rejectedBroadRadar.id,
  event_date: fixture.eventDate,
  storm_type: 'hail',
  source: fixture.rejectedBroadRadar.source,
  source_product: fixture.rejectedBroadRadar.product,
  source_priority: 1,
  polygon_geojson: rectangularFeature([-92.98, 38.47, -87.85, 40.22], 0),
};
const emptyCachedRow = {
  id: 'empty-cache-fixture', event_date: fixture.eventDate, storm_type: 'hail', source: 'swath_render_saved',
  source_product: 'rule_v1_bands', source_priority: 2, polygon_geojson: rectangularFeature([-91, 38, -90, 39], 0),
};

const selected = context.mapsSelectHailCoverageRows([radarRow, emptyCachedRow, ...reportRows], fixture.eventDate);
assert.equal(selected.length, fixture.reportDerivedRows.length, 'all seven evidence-backed report-derived bands must be selected');
assert.deepEqual(Array.from(selected, (row) => row.id), fixture.reportDerivedRows.map((row) => row.id));
assert.equal(selected.some((row) => row.id === radarRow.id), false, 'broad NEXRAD row must not replace report-derived May 3 coverage');
assert.equal(selected.some((row) => row.id === emptyCachedRow.id), false, 'cached row without positive observation evidence must be rejected');

for (const observation of fixture.observations) {
  assert.equal(observation.source, 'LSR');
  assert.equal(observation.id.length, 64, 'fixture observation IDs must retain the authoritative source identity');
}
assert.equal(Math.max(...fixture.observations.map((row) => row.hailIn)), 2.0, 'May 3 fixture must preserve the observed 2-inch maximum');
assert.equal(fixture.eventLocalTimezone, 'America/Chicago');
assert.equal(fixture.eventTimeUtcRange[0].slice(0, 10), '2026-05-04', 'UTC observations occur after midnight');
assert.equal(fixture.eventDate, '2026-05-03', 'Central-local storm grouping must retain May 3');
assert.ok(fixture.observations.every((row) => row.lat > 38 && row.lat < 40 && row.lon < -89 && row.lon > -92), 'fixture coordinates must remain GeoJSON longitude/latitude, not transposed');
assert.ok(fixture.reportDerivedRows.every((row) => /^[a-f0-9]{64}$/.test(row.geometrySha256)), 'safe expected geometry fingerprints must be retained');
const checkpoint = fixture.altonDiagnostic;
const checkpointCovered = fixture.reportDerivedRows.some((row) => {
  const [west, south, east, north] = row.bounds;
  return checkpoint.lon >= west && checkpoint.lon <= east && checkpoint.lat >= south && checkpoint.lat <= north;
});
assert.equal(checkpointCovered, checkpoint.coveredByReportDerivedRows, 'the authentic 1.5-inch Alton checkpoint must be covered by report-derived geometry');
assert.equal(fixture.rejectedBroadRadar.containsAltonDiagnostic, false, 'the displaced broad NEXRAD geometry must retain its observed no-Alton containment result');
assert.equal(context.mapsHailRowProvenance(reportRows[3]).eventDate, fixture.eventDate, 'diagnostics must label the event date');

const savedRendererBody = extractFunction('mapsDrawSavedStormPolygonForDate');
assert.match(savedRendererBody, /mapsSelectHailCoverageRows\(rows \|\| \[\], dateStr\)/, 'the saved renderer must use the authoritative selector');
const campaignSyncBody = extractFunction('cmpSyncSwaths');
assert.match(campaignSyncBody, /mapsState\.swathPolygons/, 'Campaign must clone the already-selected visible swaths from Maps');

const spotterRendererBody = extractFunction('drawLsrPointsOnMap');
assert.match(spotterRendererBody, /cleanLsrOverlays\(\)/, 'hail spotter rendering must retain its dedicated marker lifecycle');
assert.match(spotterRendererBody, /new google\.maps\.Circle/, 'hail spotter reports must remain point markers');
assert.doesNotMatch(spotterRendererBody, /mapsDrawSavedStormPolygonForDate|mapsSelectHailCoverageRows/, 'Hail spotter rendering must not change swath selection');

assert.match(html, /mapsState\.swathOpacity/, 'swath opacity must remain tied to the same overlay state');
assert.match(html, /function purgeNonPinnedDateOverlays\(/, 'date lifecycle must retain its central non-pinned overlay cleanup path');
assert.match(html, /function mapsRemovePointsForDate\(/, 'date lifecycle must retain per-date overlay cleanup');
assert.doesNotMatch(html, /MAPS_REVIEWED_HAIL_DISPLAY_TRACKS\s*=\s*\[(?!\s*\])/, 'browser-authored display tracks must remain disabled');
assert.doesNotMatch(fs.readFileSync(new URL('../supabase/functions/swath-render/index.ts', import.meta.url), 'utf8'), /MARCH_2026_SYNTHETIC/, 'synthetic reference-image anchors must not produce real swaths');

console.log('Money Maps May 3 authority fixture passed:', {
  date: fixture.eventDate,
  authoritativeObservations: fixture.authoritativeObservationCount,
  selectedReportDerivedBands: selected.length,
  rejectedBroadRadarId: radarRow.id,
  campaignUsesSharedSelector: true,
  spotterToggleMarkerOnly: true,
});
