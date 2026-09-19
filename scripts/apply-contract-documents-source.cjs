const fs = require('fs');

const indexPath = 'public/index.html';
const verifyPath = 'scripts/verify-approved-lead-detail.cjs';
let html = fs.readFileSync(indexPath, 'utf8');
let verify = fs.readFileSync(verifyPath, 'utf8');

function fail(label) { throw new Error('Missing source anchor: ' + label); }
function insertBefore(source, marker, addition, label, startAt) {
  const p = source.indexOf(marker, startAt || 0); if (p < 0) fail(label);
  return source.slice(0, p) + addition + source.slice(p);
}
function insertAfter(source, marker, addition, label, startAt) {
  const p = source.indexOf(marker, startAt || 0); if (p < 0) fail(label);
  const end = p + marker.length; return source.slice(0, end) + addition + source.slice(end);
}
function replaceRequired(source, oldText, newText, label) {
  if (!source.includes(oldText)) fail(label); return source.replace(oldText, newText);
}
function replaceOptional(source, oldText, newText) {
  return source.includes(oldText) ? source.replace(oldText, newText) : source;
}

if (!html.includes('function crmOpenLeadContractFromDocuments')) {
  const helpers = `
    var _crmSavedContractReturnLeadId = '';

    function crmOpenLeadContractFromDocuments(leadId) {
      leadId=String(leadId||'').trim();
      var lead=crmGetLeads().find(function(item){return String(item&&item.id||'')===leadId;});
      if(!lead){showUploadToast('Lead not found.');return;}
      var idx=savedFormTemplates.findIndex(function(tpl){var name=String(tpl&&tpl.name||'');return /contract/i.test(name)&&!/conting/i.test(name);});
      if(idx<0){showUploadToast('No Contract template is configured yet.');return;}
      _crmPostLeadContingencyContext={leadId:leadId,returnMode:'documents',documentType:'contract'};
      openDocuSign(leadId); createAndOpenFormInstance(leadId,idx);
    }

    function crmOpenSavedLeadContract(leadId,idx) {
      var lead=crmGetLeads().find(function(item){return String(item&&item.id||'')===String(leadId||'');});
      var docs=lead&&Array.isArray(lead.contractFormInstances)?lead.contractFormInstances:[];
      if(!lead||!docs[idx])return;
      savedDocsJobId=null; _crmSavedContractReturnLeadId=lead.id;
      document.getElementById('saved-docs-subtitle').textContent=crmGetLeadDisplayName(lead);
      renderSavedDocsList({savedDocs:docs}); showPage('page-saved-docs'); viewSavedDoc({savedDocs:docs},idx);
    }

`;
  html = insertBefore(html, '    function renderDocuSignForms(job) {', helpers, 'contract helpers');

  const contractCompletion = `            if (String(_crmPostLeadContingencyContext.documentType || 'contingency') === 'contract') {
              signedLead.contractSigned = true; signedLead.contractSignedAt = inst.signedAt; signedLead.contractStatus = 'signed_form';
              signedLead.contractFormInstances = Array.isArray(signedLead.contractFormInstances) ? signedLead.contractFormInstances : [];
              signedLead.contractFormInstances.push(inst);
              signedLead.jobFile = signedLead.jobFile && typeof signedLead.jobFile === 'object' ? signedLead.jobFile : {};
              signedLead.jobFile.contract = signedLead.jobFile.contract && typeof signedLead.jobFile.contract === 'object' ? signedLead.jobFile.contract : {};
              signedLead.jobFile.contract.status = 'Signed'; signedLead.jobFile.contract.signedAt = inst.signedAt;
              signedLead.updatedAt = new Date().toISOString();
              crmPushLeadActivity(signedLead, 'Signed contract completed', 'document', crmGetCurrentUserName() || currentRep || '', inst.signedAt);
              crmSaveLeads(leadRows); _activeFormInstance = null;
              _jfDocSelectedCategory = 'Contract'; _crmCurrentJobFileLeadId = signedLead.id;
              _crmPostLeadContingencyContext = null; crmOpenJobFileSection(signedLead.id, 'documents'); showUploadToast('Signed contract saved.');
              return;
            }
`;
  html = insertBefore(html, '            signedLead.contingencySigned = true;', contractCompletion, 'contract signing completion');

  const backDocStart = html.indexOf("document.getElementById('back-from-docusign').addEventListener");
  if (backDocStart < 0) fail('docusign back handler');
  html = insertAfter(html,
    "      if (_crmPostLeadContingencyContext && _crmPostLeadContingencyContext.leadId) {",
    "\n        if (String(_crmPostLeadContingencyContext.documentType || '') === 'contract' && _crmPostLeadContingencyContext.returnMode === 'documents') { var contractLeadId=_crmPostLeadContingencyContext.leadId; _crmPostLeadContingencyContext=null; _jfDocSelectedCategory='Contract'; crmOpenJobFileSection(contractLeadId,'documents'); return; }",
    'contract docusign back path', backDocStart);

  const savedBackStart = html.indexOf("document.getElementById('back-from-saved-docs').addEventListener");
  if (savedBackStart < 0) fail('saved docs back handler');
  html = insertAfter(html,
    "document.getElementById('back-from-saved-docs').addEventListener('click', function () {",
    "\n      if (_crmSavedContractReturnLeadId) { var contractLeadId=_crmSavedContractReturnLeadId; _crmSavedContractReturnLeadId=''; _jfDocSelectedCategory='Contract'; crmOpenJobFileSection(contractLeadId,'documents'); return; }",
    'saved contract back path', savedBackStart);

  html = replaceRequired(html,
    "      if (catKeys.indexOf('signed_contingency') !== -1) count += Array.isArray(lead.contingencyFormInstances) ? lead.contingencyFormInstances.length : 0;",
    "      if (catKeys.indexOf('signed_contingency') !== -1) count += Array.isArray(lead.contingencyFormInstances) ? lead.contingencyFormInstances.length : 0;\n      if (catKeys.indexOf('signed_contract') !== -1) count += Array.isArray(lead.contractFormInstances) ? lead.contractFormInstances.length : 0;",
    'contract document count');

  const docsNavStart = html.indexOf('/* ── Documents Hub navigation ── */');
  if (docsNavStart < 0) fail('documents hub navigation');
  html = insertBefore(html, '      /* Upload from hub category */',
`      var addContractBtn=e.target.closest('[data-jf-add-contract]');
      if(addContractBtn){crmOpenLeadContractFromDocuments(_crmCurrentJobFileLeadId);return;}
      var savedContractBtn=e.target.closest('[data-jf-open-contract-form]');
      if(savedContractBtn){crmOpenSavedLeadContract(_crmCurrentJobFileLeadId,parseInt(savedContractBtn.getAttribute('data-jf-open-contract-form'),10)||0);return;}
`, 'contract document actions', docsNavStart);

  const contractRenderer = `    function jfRenderContractCategoryHtml(lead) {
      var files = crmGetLeadFilesFor ? crmGetLeadFilesFor(lead.id).filter(function(f){ return f.type === 'signed_contract' || f.docCategory === 'Contract'; }) : [];
      var signedForms = Array.isArray(lead.contractFormInstances) ? lead.contractFormInstances : [];
      var canEdit = typeof crmCanEditJobFileLead === 'function' ? crmCanEditJobFileLead(lead) : true;
      var html = '<div class="crm-job-file-panel-block">';
      html += '<div class="jf-doc-cat-header"><button class="jf-doc-cat-back" type="button" data-jf-doc-hub-back="1">← Back</button><span class="jf-doc-cat-title">&#9997;&#65039; Contract</span></div>';
      if (canEdit) html += '<div class="crm-job-file-inline-actions" style="margin-bottom:14px;display:flex;gap:8px;flex-wrap:wrap;"><button class="btn btn-primary" type="button" data-jf-add-contract="1">Add Contract</button><button class="btn" type="button" data-jf-upload-doc-hub="signed_contract" data-jf-doc-hub-cat="Contract">Upload Signed Contract</button></div>';
      if (!files.length && !signedForms.length) html += '<div class="crm-job-file-empty">No signed contract saved yet.</div>';
      if (signedForms.length) { html += '<div class="crm-job-file-file-links">'; signedForms.forEach(function(doc,idx){ var date=doc.signedAt?crmFormatDateTimeLabel(doc.signedAt):''; html += '<button class="crm-job-file-file-link" type="button" data-jf-open-contract-form="'+idx+'"><strong>'+crmEscapeHtml(doc.templateName||'Signed Contract')+'</strong><span style="display:block;font-size:11px;color:var(--muted);margin-top:4px;">Signed'+(date?' · '+crmEscapeHtml(date):'')+' · View / Print</span></button>'; }); html += '</div>'; }
      if (files.length) { html += '<div class="crm-job-file-file-links">'; files.forEach(function(file){ var entry={fileId:file.id,file:file,note:'Signed contract',uploadedBy:file.uploadedBy||'',uploadedAt:file.uploadedAt||file.createdAt||''}; html += crmRenderJobFileDocumentEntry(entry); }); html += '</div>'; }
      html += '</div>'; return html;
    }

`;
  html = insertBefore(html, '    function jfRenderDocCategoryHtml(lead, categoryKey) {', contractRenderer, 'contract category renderer');
  html = insertAfter(html, '    function jfRenderDocCategoryHtml(lead, categoryKey) {', "\n      if (categoryKey === 'Contract') return jfRenderContractCategoryHtml(lead);", 'contract category branch');

  const uploadStart = html.indexOf("document.getElementById('jf-doc-upload-input').addEventListener('change'");
  if (uploadStart < 0) fail('job document upload handler');
  html = insertAfter(html, "      var meta = _crmJobFilePendingUploadMeta || {};",
    "\n      if (type === 'signed_contract' && files.some(function(file){ return String(file.type||'').toLowerCase() !== 'application/pdf' && !/\\.pdf$/i.test(String(file.name||'')); })) { showUploadToast('Upload a signed contract PDF.'); return; }",
    'contract PDF validation', uploadStart);

  const persistenceMarker = '          crmApplyLeadJobFileData(item, data);';
  const persistencePos = html.indexOf(persistenceMarker, uploadStart);
  if (persistencePos < 0) fail('document upload persistence');
  const contractUpload = `          if (type === 'signed_contract') {
            item.contractSigned = true; item.contractSignedAt = new Date().toISOString(); item.contractStatus = 'signed_uploaded';
            item.contractDocumentId = savedMeta && savedMeta[0] ? String(savedMeta[0].id || '') : '';
            data.contract = data.contract && typeof data.contract === 'object' ? data.contract : {};
            data.contract.status = 'Signed File Uploaded'; data.contract.signedAt = item.contractSignedAt; data.contract.documentId = item.contractDocumentId;
            crmPushLeadActivity(item, 'Signed contract uploaded', 'document', crmGetCurrentUserName() || currentRep || '', item.contractSignedAt);
          }
`;
  html = html.slice(0, persistencePos) + contractUpload + html.slice(persistencePos);
  html = replaceOptional(html,
    "if (type !== 'signed_contingency') crmPushLeadActivity",
    "if (type !== 'signed_contingency' && type !== 'signed_contract') crmPushLeadActivity");
  html = replaceOptional(html,
    "showUploadToast(type === 'signed_contingency' ? 'Signed contingency uploaded.' : (count + ' document' + (count !== 1 ? 's' : '') + ' uploaded.'));",
    "showUploadToast(type === 'signed_contingency' ? 'Signed contingency uploaded.' : (type === 'signed_contract' ? 'Signed contract uploaded.' : (count + ' document' + (count !== 1 ? 's' : '') + ' uploaded.')));");
}

if (!verify.includes('data-jf-add-contract')) {
  verify = replaceRequired(verify,
    "  \"document.getElementById('cfv-print-btn').addEventListener\",\n];",
    "  \"document.getElementById('cfv-print-btn').addEventListener\",\n  \"data-jf-add-contract=\\\"1\\\"\",\n  \"Upload Signed Contract\",\n  \"data-jf-open-contract-form\",\n  \"function crmOpenLeadContractFromDocuments\",\n  \"function crmOpenSavedLeadContract\",\n  \"function jfRenderContractCategoryHtml\",\n  \"type === 'signed_contract'\",\n  \"Signed contract uploaded\",\n  \"Signed contract completed\",\n];",
    'contract verifier entries');
}

fs.writeFileSync(indexPath, html);
fs.writeFileSync(verifyPath, verify);
console.log('Permanent Contract Documents source changes applied.');
