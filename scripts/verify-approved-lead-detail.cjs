const fs = require('fs');
const html = fs.readFileSync('public/index.html', 'utf8');

const required = [
  "padding: 120px 36px 28px 143px;",
  "grid-template-columns: repeat(5, minmax(0, 1fr));",
  "data-jf-tab=\"history\" data-manager-tool=\"1\"",
  "data-jf-tab=\"profit-loss\" data-manager-tool=\"1\"",
  "['Manager','Admin','Owner'].indexOf(String(crmGetRole() || '').trim()) !== -1",
  "id=\"ld-delete-job-btn\"",
  "Only an Admin can delete a job.",
  "if (pageId === 'page-lead-detail' && pageContext.homeownerName) pageTitle = pageContext.homeownerName;",
  "crmPositionLeadDetailHeaderTitle();",
  "crmPushLeadActivity(leads[i], 'Lead details updated'",
];

for (const snippet of required) {
  if (!html.includes(snippet)) {
    throw new Error('Approved Lead Detail contract missing: ' + snippet);
  }
}
const pnlRequired = [
  "var worksheetPayments = Array.isArray(worksheet.paymentsReceived)",
  "data.rcvAmount = crmParseMoneyNumber(worksheet.insuranceBreakdown && worksheet.insuranceBreakdown.totalRcv || 0);",
  "var totalRevenue = crmParseMoneyNumber(data.rcvAmount || 0) || totalCollected;",
  "Rep pay is synced from Payroll for this job.",
  "Total Costs = Labor + Materials + Other Expenses + Rep Pay.",
  "Profit Margin",
  "<div class=\"crm-job-folder-section-title\">History</div>",
];

for (const snippet of pnlRequired) {
  if (!html.includes(snippet)) {
    throw new Error('Approved History/P&L contract missing: ' + snippet);
  }
}

const forbidden = [
  'RCV comes from the Financial Worksheet. Payments received below sync automatically.',
  'Auto from Financial Worksheet',
];
for (const snippet of forbidden) {
  if (html.includes(snippet)) throw new Error('Removed Lead Detail copy returned: ' + snippet);
}

console.log('Approved Lead Detail contract verified.');
