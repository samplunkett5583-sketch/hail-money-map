const fs = require('fs');
const vm = require('vm');
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
  "id=\"ld-claim-summary-card\"",
  "id=\"ld-claim-property-type\"",
  "id=\"ld-claim-job-type\"",
  "id=\"ld-claim-insurance-company\"",
  "id=\"ld-claim-number\"",
  "id=\"ld-claim-date-of-loss\"",
  "id=\"ld-claim-concerns\"",
  "id=\"ld-claim-notes\"",
  "id=\"ld-docs-card\" style=\"display:none !important;\"",
  "var concernSource = Array.isArray(lead.damageAreas)",
  "id=\"ld-edit-property-type\"",
  "id=\"ld-edit-job-type\"",
  "id=\"ld-edit-insurance-company\"",
  "id=\"ld-edit-claim-number\"",
  "id=\"ld-edit-date-of-loss\"",
  "id=\"ld-edit-concerns\"",
  "newStage !== currentStage",
  "var saveOk = crmSaveLeads(leads);",
  "crmOpenLeadDetail(_crmCurrentLeadId);",
  "showUploadToast('Lead updated.');",
  "ld-inline-edit-overlay",
  "nextStepCard.parentNode.insertBefore(overlay, nextStepCard);",
  "if (claimCard) claimCard.style.display = 'none';",
  "if (claimCard) claimCard.style.display = '';",
  "Edit customer, property, and claim details together.",
  "list=\"hm-insurance-company-list\"",
  "hmRememberInsuranceCompany(insSelect.value);",
  "hmGetInsuranceClaimPhone(insSelect.value);",
  "el.id !== 'fl-dateOfLoss' && el.id !== 'ld-edit-date-of-loss'",
  "typeof el.showPicker === 'function'",
  "{ key: 'Contingency',   icon: '&#128221;', types: ['signed_contingency'] }",
  "data-jf-add-contingency=\"1\"",
  "Upload Signed Contingency",
  "data-jf-open-contingency-form",
  "function crmOpenLeadContingencyFromDocuments",
  "returnMode:'documents'",
  "type === 'signed_contingency'",
  "Signed contingency uploaded",
  "id=\"cfv-print-btn\"",
  "document.getElementById('cfv-print-btn').addEventListener",
];
for (const snippet of required) {
  if (!html.includes(snippet)) throw new Error('Approved Lead Detail contract missing: ' + snippet);
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
  if (!html.includes(snippet)) throw new Error('Approved History/P&L contract missing: ' + snippet);
}

const claimIndex = html.indexOf('id="ld-claim-summary-card"');
const nextStepIndex = html.indexOf('id="ld-next-step-card"');
if (claimIndex < 0 || nextStepIndex <= claimIndex) {
  throw new Error('Claim details card must remain before Next Step.');
}

const forbidden = [
  'RCV comes from the Financial Worksheet. Payments received below sync automatically.',
  'Auto from Financial Worksheet',
  'class="crm-detail-card" id="ld-docs-card"',
  'id="ld-storm-date"',
  'id="ld-concerns"',
  'id="ld-notes-initial"',
];
for (const snippet of forbidden) {
  if (html.includes(snippet)) throw new Error('Removed Lead Detail element returned: ' + snippet);
}

const insuranceJs = fs.readFileSync('public/insurance-companies.js', 'utf8');
const insuranceSandbox = { window: {} };
vm.runInNewContext(insuranceJs, insuranceSandbox);
if (!Array.isArray(insuranceSandbox.window.HM_INSURANCE_COMPANIES) || insuranceSandbox.window.HM_INSURANCE_COMPANIES.length < 200) {
  throw new Error('Insurance company directory is missing or too small.');
}
if (!insuranceSandbox.window.HM_INSURANCE_CLAIM_PHONES || !insuranceSandbox.window.HM_INSURANCE_CLAIM_PHONES['State Farm']) {
  throw new Error('Insurance claim phone directory is missing.');
}
console.log('Approved Lead Detail contract verified.');
