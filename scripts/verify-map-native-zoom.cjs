// Camera ownership and delayed-start regressions. These checks do not measure
// Google rendering performance; the 14.69-second reference still needs a real
// browser/device comparison with the same wheel input and storm overlays.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const html = fs.readFileSync('public/index.html', 'utf8');
const start = html.indexOf('function mapsGoogleReady() {');
const end = html.indexOf('    // ── Hail LSR overlay helpers', start);
assert(start >= 0 && end > start, 'Maps initialization must be present');
const source = html.slice(start, end);
assert(!/<script\b[^>]*\bsrc=["'][^"']*maps3d-engine\.js/i.test(html),
  'Do not load a second map/camera over the production map');
assert(!html.includes('mapsInstallEarthStyleWheelZoom'),
  'The old wheel limiter must not be reintroduced');

function fixture({ active = true, apiReady = true } = {}) {
  const state = { postPaintNavigationReady: true };
  const app = {};
  const maps = [];
  const domListeners = [];
  const timers = [];
  const selected = [];
  let clock = 0;
  let pageActive = active;
  const classes = { add() {}, remove() {}, contains() { return pageActive; } };
  const page = { classList: classes };
  const host = {
    dataset: {},
    classList: classes,
    addEventListener(name, fn) { domListeners.push({ name, fn }); },
    querySelector() { return null; }
  };
  class Map {
    constructor(element, options) {
      assert.equal(element, host);
      this.options = { ...options };
      this.center = { lat: () => 38.6, lng: () => -90.2 };
      this.zoom = options.zoom;
      this.listeners = new global.Map();
      this.writes = [];
      maps.push(this);
    }
    setOptions(options) { Object.assign(this.options, options); this.writes.push('options'); }
    getZoom() { return this.zoom; }
    setZoom(zoom) { this.zoom = zoom; this.writes.push('zoom'); }
    getCenter() { return this.center; }
    getMapTypeId() { return this.options.mapTypeId; }
    getRenderingType() { return this.renderingType || 'UNINITIALIZED'; }
    addListener(name, fn) {
      const list = this.listeners.get(name) || [];
      list.push(fn);
      this.listeners.set(name, list);
    }
    emit(name, event) { (this.listeners.get(name) || []).forEach(fn => fn(event)); }
  }
  const context = {
    console: { log() {}, warn() {}, error(...args) { throw new Error(args.join(' ')); } },
    Date: { now: () => clock },
    document: {
      hidden: false,
      getElementById(id) {
        if (id === 'page-map') return page;
        if (id === 'stormMap') return host;
        return { classList: classes, textContent: '' };
      },
      addEventListener() {}
    },
    localStorage: { getItem(key) { return key === 'hailMoneyMapStyle' ? 'satellite' : '1'; } },
    google: { maps: {
      Map, RenderingType: { VECTOR: 'VECTOR' },
      event: {
        addListener(map, name, fn) { map.addListener(name, fn); },
        addListenerOnce(map, name, fn) { map.addListener(name, fn); }
      }
    } },
    mapsState: state,
    HM_GOOGLE_MAPS_READY: apiReady,
    MAPS_MIN_ZOOM: 4,
    MAPS_DEFAULT_ZOOM: 5,
    MAPS_DEFAULT_CENTER: { lat: 38.6, lng: -90.2 },
    sharedViewport: {},
    _svSetting: false,
    mapsGetAppState: () => app,
    mapsMeasureSyncPhase: (_, fn) => fn(),
    mapsMarkNavigationTiming() {},
    mapsWireHailHoverReadout() {},
    mapsWireAreaSearch() {},
    mapsWireMapStyleCtrl() {},
    mapsSetHailHoverCursor() {},
    mapsSelectPropertyAtLocation(...args) { selected.push(args); },
    hmDebugLog() {},
    addEventListener() {},
    setTimeout(fn, delay) { timers.push({ fn, delay }); },
    cancelAnimationFrame() {}
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(source, context);
  return { context, state, maps, host, domListeners, timers, selected,
    showMap() { pageActive = true; }, advance(ms) { clock += ms; } };
}

// Opening Maps after a long sign-in must use the same camera immediately.
const delayed = fixture({ active: false });
delayed.context.mapsInitMap();
assert.equal(delayed.maps.length, 0);
assert.equal(delayed.state.pendingInit, true);
delayed.advance(60000);
delayed.showMap();
delayed.context.mapsInitMap();
delayed.context.mapsInitMap();
assert.equal(delayed.maps.length, 1);
const map = delayed.maps[0];
assert.equal(map.options.renderingType, 'VECTOR');
assert.equal(map.options.isFractionalZoomEnabled, true);
assert.equal(map.options.gestureHandling, 'greedy');
assert.equal(map.options.scrollwheel, true);
assert.equal(map.options.tilt, 0);
assert.equal(map.options.heading, 0);
assert.equal(map.options.tiltInteractionEnabled, false);
assert.equal(map.options.headingInteractionEnabled, false);
assert.equal(map.options.mapTypeId, 'satellite');
assert.equal(delayed.domListeners.filter(e => e.name === 'wheel').length, 0);
assert.equal(delayed.host.dataset.mapsCamera, 'native');
assert.equal(delayed.host.dataset.mapsRenderer, 'uninitialized');
map.renderingType = 'RASTER';
map.emit('renderingtype_changed');
assert.equal(delayed.host.dataset.mapsRenderer, 'raster');
map.renderingType = 'VECTOR';
map.emit('renderingtype_changed');
assert.equal(delayed.host.dataset.mapsRenderer, 'vector');

// Native fractional camera movement must not restart animation or rebuild layers.
const writesBeforeZoom = map.writes.length;
const timersBeforeZoom = delayed.timers.length;
for (let i = 0; i < 120; i++) map.emit('zoom_changed');
assert.equal(map.writes.length, writesBeforeZoom);
assert.equal(delayed.timers.length, timersBeforeZoom);
assert.equal(delayed.state._mapCameraMoving, true);
map.zoom = 9.625;
map.emit('idle');
assert.equal(delayed.context.sharedViewport.zoom, 9.625);
assert.equal(delayed.state._mapCameraMoving, false);

// API readiness and page navigation can arrive in either order.
const apiLater = fixture({ apiReady: false });
apiLater.context.mapsInitMap();
assert.equal(apiLater.maps.length, 0);
apiLater.advance(60000);
apiLater.context.mapsGoogleReady();
assert.equal(apiLater.maps.length, 1);

// Preserve property selection while suppressing the click following a map drag.
const event = { latLng: { lat: () => 38.6, lng: () => -90.2 } };
map.emit('click', event);
assert.equal(delayed.selected.length, 1);
map.emit('dragstart');
map.emit('dragend');
map.emit('click', event);
assert.equal(delayed.selected.length, 1);
delayed.advance(500);
map.emit('click', event);
assert.equal(delayed.selected.length, 2);

console.log('Native map camera checks passed: delayed startup, single renderer, native wheel/pinch, fractional viewport, and property-click preservation.');
