const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const source = fs.readFileSync(path.resolve(__dirname, '..', 'public', 'estimate-ai-flow.js'), 'utf8');
const start = source.indexOf('function drawRoofDiagram(y)');
const end = source.indexOf("var pricingRows", start);
assert.notEqual(start, -1, 'roof diagram renderer is present');
const renderer = source.slice(start, end === -1 ? start + 5000 : end);

for (const kind of ['eaves', 'rakes', 'ridges', 'hips', 'valleys', 'stepFlashing', 'headwallFlashing', 'ambiguous']) {
  assert.match(renderer, new RegExp(`${kind}:\\[`), `${kind} has a drawing style`);
}
assert.match(renderer, /roofGeometry\.segments/);
assert.match(renderer, /maxX-minX\|\|1/);
assert.match(renderer, /maxY-minY\|\|1/);
assert.match(renderer, /doc\.line\(x1,y1,x2,y2\)/);
assert.match(source, /Detected roof diagram/);

console.log('Roof sketch renderer checks passed.');
