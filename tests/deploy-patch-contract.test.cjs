const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const flow = fs.readFileSync(path.join(root, 'public', 'estimate-ai-flow.js'), 'utf8');
const crm = fs.readFileSync(path.join(root, 'public', 'assets', 'crm-cross-links.js'), 'utf8');
const pricingPatch = fs.readFileSync(path.join(root, 'scripts', 'apply-ai-estimate-pricing-fallback.py'), 'utf8');
const cleanupPatch = fs.readFileSync(path.join(root, 'scripts', 'apply-ai-estimate-mobile-fair-market-cleanup.py'), 'utf8');
const crmPatch = fs.readFileSync(path.join(root, 'scripts', 'apply-ai-estimate-crm-flow-cleanup.py'), 'utf8');
const normalizedFlow = flow.replace(/\r\n/g, '\n');
const normalizedCrm = crm.replace(/\r\n/g, '\n');

assert.match(normalizedFlow, /function line\(code, description, quantity, unit, waste, source, section\)/);
assert.match(pricingPatch, /function line\\\(code, description, quantity, unit, waste, source, section\\\)/);
assert.ok(flow.includes("var pricingRows = window.HailMoneyPricing && estAiSession.fairMarketPricing ? window.HailMoneyPricing.sync(estAiSession) : [];"));
assert.ok(flow.includes("var abcOptionalToggle = document.getElementById('est-abc-enabled');"));
assert.match(flow, /function verificationItems\(\)/);
assert.match(flow, /function renderMeasurements\(\)/);
assert.ok(cleanupPatch.includes("estimateTrades().indexOf('roof') >= 0 ? ['totalRoofArea', 'squares', 'stories'] : []"));
assert.ok(flow.includes("if (window.HailMoneyCrmEstimate && !currentEstimate.crmContext && !currentEstimate.quickContext) { window.HailMoneyCrmEstimate.start(); return; }"));
assert.ok(normalizedFlow.includes("  var estimateStarting = false;\n  async function startEstimate() {"));
assert.ok(normalizedFlow.includes("    bindEvents();\n    estAiRunProcessing = runProcessing;"));

const archivePattern = /\n    var saved = \[\];\n    try \{ saved = JSON\.parse\(localStorage\.getItem\('hailMoneyAiEstimatesV1'\) \|\| '\[\]'\); \} catch \(_\) \{\}\n    if \(!Array\.isArray\(saved\)\) saved = \[\];\n    var savedIdx = saved\.findIndex\(function \(item\) \{ return clean\(item\.id\) === clean\(copy\.id\); \}\);\n    if \(savedIdx >= 0\) saved\[savedIdx\] = copy; else saved\.unshift\(copy\);\n    localStorage\.setItem\('hailMoneyAiEstimatesV1', JSON\.stringify\(saved\)\);/m;
assert.match(normalizedCrm, archivePattern);

assert.ok(crmPatch.includes("Could not find AI estimate CRM start gate"));
assert.ok(pricingPatch.includes("Could not patch line() pricing block"));
assert.ok(cleanupPatch.includes("Could not patch verificationItems"));

console.log('Deploy patch contract checks passed.');
