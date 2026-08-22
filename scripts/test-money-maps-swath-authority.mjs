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
assert.match(savedRendererBody, /var _trackFocusGeometry = true;/, 'selected-date rendering must retain combined swath bounds for automatic focus');
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

const datePickerBody = extractFunction('mapsWireDatePicker');
assert.match(datePickerBody, /loadStormDate\(val\)/, 'the visible date control must trigger the normal storm-date load');
const drawDateBody = extractFunction('mapsDrawDate');
assert.match(drawDateBody, /drew \? await fitMapToStormDate\(dateStr\) : null/, 'the normal date load must await viewport focus after attaching saved swaths');
const focusBody = extractFunction('fitMapToStormDate');
assert.match(focusBody, /mapsState\._dateBbox && mapsState\._dateBbox\[dateStr\]/, 'viewport focus must use the combined selected-date geometry bounds');
assert.match(focusBody, /'selected-date-swaths'/, 'combined selected-date focus must be distinguishable in diagnostics');

class TestBounds {
  constructor() { this.points = []; }
  extend(point) { this.points.push({ lat: Number(point.lat), lng: Number(point.lng) }); return this; }
  getNorthEast() { return { lat: () => Math.max(...this.points.map((p) => p.lat)), lng: () => Math.max(...this.points.map((p) => p.lng)) }; }
  getSouthWest() { return { lat: () => Math.min(...this.points.map((p) => p.lat)), lng: () => Math.min(...this.points.map((p) => p.lng)) }; }
  getCenter() {
    const ne = this.getNorthEast();
    const sw = this.getSouthWest();
    return { lat: () => (ne.lat() + sw.lat()) / 2, lng: () => (ne.lng() + sw.lng()) / 2 };
  }
  contains() { return true; }
}
const finalMap = {
  zoom: 5,
  center: { lat: () => 39.2, lng: () => -96 },
  fitBoundsCalls: [],
  fitBounds(bounds) { this.fitBoundsCalls.push(bounds); this.center = bounds.getCenter(); this.zoom = 8; },
  getZoom() { return this.zoom; },
  setZoom(value) { this.zoom = value; },
  getCenter() { return this.center; },
  setCenter(value) { this.center = { lat: () => value.lat, lng: () => value.lng }; },
};
const flowState = {
  map: finalMap,
  selectedProperty: null,
  _fitBoundsCache: {},
  _dateBbox: { [fixture.eventDate]: { minLat: 38.78, maxLat: 39.02, minLon: -90.32, maxLon: -89.98, hasAny: true } },
  lsrPointsByDate: {}, windPointsByDate: {}, tornadoPointsByDate: {}, focusCandidatesByDate: {}, aiSwathOverlays: {}, refSwathOverlays: {},
};
const flowContext = vm.createContext({
  console,
  mapsState: flowState,
  mapsNormalizeDate: (value) => String(value || '').slice(0, 10),
  google: {
    maps: {
      LatLngBounds: TestBounds,
      LatLng: class { constructor(lat, lng) { this._lat = lat; this._lng = lng; } lat() { return this._lat; } lng() { return this._lng; } },
      event: { addListenerOnce(_map, _event, callback) { callback(); } },
    },
  },
});
vm.runInContext(focusBody, flowContext);
const fitResult = await flowContext.fitMapToStormDate(fixture.eventDate);
assert.equal(finalMap.fitBoundsCalls.length, 1, 'untouched nationwide/default view must fit once after May 3 selection');
assert.equal(fitResult.source, 'selected-date-swaths');
assert.ok(finalMap.getCenter().lat() > 38.7 && finalMap.getCenter().lat() < 39.1, 'final center must contain the selected Madison County geometry');
assert.ok(finalMap.getCenter().lng() > -90.4 && finalMap.getCenter().lng() < -89.9, 'final center must contain the selected Alton-area geometry');
assert.ok(finalMap.getZoom() >= 6 && finalMap.getZoom() <= 9, 'final zoom must leave nationwide view and make selected swaths visible');

console.log('Money Maps May 3 authority fixture passed:', {
  date: fixture.eventDate,
  authoritativeObservations: fixture.authoritativeObservationCount,
  selectedReportDerivedBands: selected.length,
  rejectedBroadRadarId: radarRow.id,
  campaignUsesSharedSelector: true,
  spotterToggleMarkerOnly: true,
});
