import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');

function functionSource(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} must exist`);
  const bodyStart = html.indexOf('{', start);
  let depth = 0;
  for (let i = bodyStart; i < html.length; i += 1) {
    if (html[i] === '{') depth += 1;
    if (html[i] === '}') depth -= 1;
    if (depth === 0) return html.slice(start, i + 1);
  }
  throw new Error(`Could not parse ${name}`);
}

assert.doesNotMatch(html, /google\.maps\.drawing|new\s+google\.maps\.drawing\.DrawingManager|OverlayType\.RECTANGLE/);
assert.doesNotMatch(html, /libraries=[^"']*\bdrawing\b/);
assert.match(html, /terra-draw@1\.32\.3\/dist\/terra-draw\.umd\.js/);
assert.match(html, /terra-draw-google-maps-adapter@1\.6\.1\/dist\/terra-draw-google-maps-adapter\.umd\.js/);

const elements = new Map();
function element(id) {
  if (!elements.has(id)) {
    elements.set(id, {
      id,
      value: '',
      style: {},
      classList: { add() {}, remove() {}, toggle() {} },
      addEventListener() {},
    });
  }
  return elements.get(id);
}

let drawerOptions;
let finishListener;
let rectangleOptions;
let queryCount = 0;

class MockDrawer {
  constructor(options) {
    drawerOptions = options;
    this.enabled = false;
    this.feature = {
      geometry: {
        type: 'Polygon',
        coordinates: [[[-90.2, 38.6], [-90.1, 38.6], [-90.1, 38.7], [-90.2, 38.7], [-90.2, 38.6]]],
      },
    };
  }
  on(name, listener) { if (name === 'finish') finishListener = listener; }
  start() { this.enabled = true; }
  stop() { this.enabled = false; this.stopped = true; }
  setMode(mode) { this.mode = mode; }
  getSnapshotFeature() { return this.feature; }
  removeFeatures(ids) { this.removed = ids; }
}

class MockBounds {
  constructor() { this.points = []; }
  extend(point) { this.points.push(point); }
}

class MockRectangle {
  constructor(options) { rectangleOptions = options; this.options = options; }
  setMap(map) { this.map = map; }
}

const context = {
  console,
  mapsState: { map: { id: 'map' } },
  mapsAreaSearchState: { rectangleDrawer: null, rectangle: null, bounds: null },
  mapsAreaSetDefaultDates() {},
  mapsAreaClearClippedSwaths() {},
  mapsAreaQuerySelectedRegion() { queryCount += 1; },
  document: {
    getElementById: element,
    addEventListener() {},
  },
  google: {
    maps: {
      LatLngBounds: MockBounds,
      Rectangle: MockRectangle,
    },
  },
  terraDraw: {
    TerraDraw: MockDrawer,
    TerraDrawRectangleMode: class { constructor(options) { this.options = options; } },
  },
  terraDrawGoogleMapsAdapter: {
    TerraDrawGoogleMapsAdapter: class { constructor(options) { this.options = options; } },
  },
};
context.window = context;

vm.runInNewContext([
  functionSource('mapsAreaBeginRectangle'),
  functionSource('mapsWireAreaSearch'),
].join('\n'), context);

assert.doesNotThrow(() => context.mapsWireAreaSearch(), 'Money Maps must initialize without google.maps.drawing');
assert.ok(context.mapsAreaSearchState.rectangleDrawer instanceof MockDrawer);
assert.equal(drawerOptions.adapter.options.isolatedData, true);
assert.equal(drawerOptions.modes[0].options.drawInteraction, 'drag');
assert.equal(drawerOptions.modes[0].options.styles.fillColor, '#d4af37');

context.mapsAreaBeginRectangle();
assert.equal(context.mapsAreaSearchState.rectangleDrawer.enabled, true);
assert.equal(context.mapsAreaSearchState.rectangleDrawer.mode, 'rectangle');

assert.equal(typeof finishListener, 'function');
finishListener('rectangle-1');
assert.equal(context.mapsAreaSearchState.rectangleDrawer.enabled, true);
assert.equal(context.mapsAreaSearchState.rectangleDrawer.mode, 'static');
assert.equal(context.mapsAreaSearchState.rectangleDrawer.removed.length, 1);
assert.equal(context.mapsAreaSearchState.rectangleDrawer.removed[0], 'rectangle-1');
assert.equal(rectangleOptions.map, context.mapsState.map);
assert.equal(rectangleOptions.editable, true);
assert.equal(rectangleOptions.draggable, true);
assert.equal(rectangleOptions.strokeColor, '#d4af37');
assert.equal(rectangleOptions.fillOpacity, 0.08);
assert.equal(rectangleOptions.bounds.points.length, 5);
assert.equal(queryCount, 1);

console.log('Money Maps area drawing migration test passed.');
