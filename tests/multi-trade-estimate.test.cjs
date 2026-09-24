const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public', 'index.html'), 'utf8');
const flow = fs.readFileSync(path.join(root, 'public', 'estimate-ai-flow.js'), 'utf8');
const crm = fs.readFileSync(path.join(root, 'public', 'assets', 'crm-cross-links.js'), 'utf8');
const pricingPatch = fs.readFileSync(path.join(root, 'scripts', 'apply-ai-estimate-pricing-fallback.py'), 'utf8');
const cleanupPatch = fs.readFileSync(path.join(root, 'scripts', 'apply-ai-estimate-mobile-fair-market-cleanup.py'), 'utf8');

assert.match(html, /Select one or more trades\. All selected trades will be combined into one estimate\./);
assert.match(html, /selected\.join\('_'\)/);
assert.match(html, /estScopeCategories\(currentEstimate\.measurementScope\)\.length > 0/);
assert.match(html, /elevations: \{ front: \{\}, right: \{\}, rear: \{\}, left: \{\} \}/);
assert.match(html, /Downspouts \(lf\)/);
assert.match(flow, /estimate_category: tradeScope/);
assert.match(flow, /siding_front/);
assert.match(flow, /siding_right/);
assert.match(flow, /siding_rear/);
assert.match(flow, /siding_left/);
assert.match(flow, /Total Dwelling Roof/);
assert.match(flow, /Total Siding/);
assert.match(flow, /Total Gutter and Downspout/);
assert.match(flow, /Total Miscellaneous/);
assert.match(flow, /\['RCV',moneyOrDash\(rcv\)\]/);
assert.match(flow, /\['Deductible',moneyOrDash\(deductible\)\]/);
assert.match(flow, /\['ACV',moneyOrDash\(acv\)\]/);
assert.match(flow, /\['Depreciation',moneyOrDash\(depreciation\)\]/);
assert.match(crm, /estimate_category: selectedScope/);
assert.match(pricingPatch, /function line\(code, description, quantity, unit, waste, source, section\)/);
assert.match(cleanupPatch, /estimateTrades\(\)\.indexOf\('roof'\) >= 0/);

console.log('Multi-trade estimate checks passed.');
