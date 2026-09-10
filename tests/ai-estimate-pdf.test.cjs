const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'public', 'estimate-ai-flow.js'), 'utf8');
const html = fs.readFileSync(path.join(root, 'public', 'index.html'), 'utf8');

assert.match(html, /vendor-jspdf\.umd\.min\.js/);
assert.match(html, /vendor-jspdf-autotable\.min\.js/);
assert.match(html, /estimate-ai-flow\.js/);
assert.match(source, /new window\.jspdf\.jsPDF\(\{ unit:'pt', format:'letter', compress:true \}\)/);
assert.match(source, /function downloadPdf\(asFile\)/);
assert.match(source, /Financial summary/);
assert.match(source, /Measurement summary/);
assert.match(source, /Detailed line items/);
assert.match(source, /Terms and acceptance/);
assert.match(source, /Internal methodology appendix/);
assert.match(source, /Page '\+p\+' of '\+pageCount/);
assert.match(source, /doc\.output\('blob'\)/);

console.log('AI estimate PDF checks passed.');
