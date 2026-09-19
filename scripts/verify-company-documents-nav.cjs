const fs = require('fs');
const html = fs.readFileSync('public/index.html', 'utf8');
const button = '<button class="nav-item" type="button" data-dashboard-action="company-documents" aria-label="Company Documents"><span class="dash-icon" data-dash-icon="file-text" aria-hidden="true"></span><span class="nav-label">Documents</span></button>';
if (!html.includes(button)) throw new Error('Company Documents nav button missing.');
if (!html.includes('id="page-company-docs"')) throw new Error('Company Documents page missing.');
if (!html.includes("if (tab === 'company-documents')")) throw new Error('Company Documents nav routing missing.');
if (html.includes('options.phaseOneShared\n            ? ' + JSON.stringify(button))) throw new Error('Company Documents nav is conditional again.');
console.log('Company Documents navigation verified.');
