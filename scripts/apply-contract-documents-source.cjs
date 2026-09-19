const fs = require('fs');

const indexPath = 'public/index.html';
const verifyPath = 'scripts/verify-approved-lead-detail.cjs';
let html = fs.readFileSync(indexPath, 'utf8');
let verify = fs.readFileSync(verifyPath, 'utf8');

function replaceOnce(source, oldText, newText, label) {
  if (!source.includes(oldText)) throw new Error('Missing source anchor: ' + label);
  return source.replace(oldText, newText);
}

if (!html.includes('function crmOpenLeadContractFromDocuments')) {
  html = replaceOnce(html,
`    function crmOpenSavedLeadContingency(leadId,idx) {
      var lead=crmGetLeads().find(function(item){return String(item&&item.id||'')===String(leadId||'');});
      var docs=lead&&Array.isArray(lead.contingencyFormInstances)?lead.contingencyFormInstances:[]; if(!lead||!docs[idx])return;
      savedDocsJobId=null; _crmSavedDocsReturnLeadId=lead.id;
      document.getElementById('saved-docs-subtitle').textContent=crmGetLeadDisplayName(lead);
      renderSavedDocsList({savedDocs:docs}); showPage('page-saved-docs'); viewSavedDoc({savedDocs:docs},idx);
    }
`,
`    function crmOpenSavedLeadContingency(leadId,idx) {
      var lead=crmGetLeads().find(function(item){return String(item&&item.id||'')===String(leadId||'');});
      var docs=lead&&Array.isArray(lead.contingencyFormInstances)?lead.contingencyFormInstances:[]; if(!lead||!docs[idx])return;
      savedDocsJobId=null; _crmSavedDocsReturnLeadId=lead.id; _crmSavedDocsReturnCategory='Contingency';
      document.getElementById('saved-docs-subtitle').textContent=crmGetLeadDisplayName(lead);
      renderSavedDocsList({savedDocs:docs}); showPage('page-saved-docs'); viewSavedDoc({savedDocs:docs},idx);
    }

    function crmOpenLeadContractFromDocuments(leadId) {
      leadId=String(leadId||'').trim(); var lead=crmGetLeads().find(function(item){return String(item&&item.id||'')===leadId;});
      if(!lead){showUploadToast('Lead not found.');return;}
      var idx=savedFormTemplates.findIndex(function(tpl){var name=String(tpl&&tpl.name||'');return /contract/i.test(name)&&!/conting/i.test(name);});
      if(idx<0){showUploadToast('No Contract template is configured yet.');return;}
      _crmPostLeadContingencyContext={leadId:leadId,returnMode:'documents',documentType:'contract'};
      openDocuSign(leadId); createAndOpenFormInstance(leadId,idx);
    }

    function crmOpenSavedLeadContract(leadId,idx) {
      var lead=crmGetLeads().find(function(item){return String(item&&item.id||'')===String(leadId||'');});
      var docs=lead&&Array.isArray(lead.contractFormInstances)?lead.contractFormInstances:[]; if(!lead||!docs[idx])return;
      savedDocsJobId=null; _crmSavedDocsReturnLeadId=lead.id; _crmSavedDocsReturnCategory='Contract';
      document.getElementById('saved-docs-subtitle').textContent=crmGetLeadDisplayName(lead);
      renderSavedDocsList({savedDocs:docs}); showPage('page-saved-docs'); viewSavedDoc({savedDocs:docs},idx);
    }
`, 'contract document helpers');

  html = replaceOnce(html,
`            if (!signedLead) return;
            signedLead.contingencySigned = true;`,
`            if (!signedLead) return;
            if (String(_crmPostLeadContingencyContext.documentType || 'contingency') === 'contract') {
              signedLead.contractSigned = true;
              signedLead.contractSignedAt = inst.signedAt;
              signedLead.contractStatus = 'signed_form';
              signedLead.contractFormInstances = Array.isArray(signedLead.contractFormInstances) ? signedLead.contractFormInstances : [];
              signedLead.contractFormInstances.push(inst);
              signedLead.jobFile = signedLead.jobFile && typeof signedLead.jobFile === 'object' ? signedLead.jobFile : {};
              signedLead.jobFile.contract = signedLead.jobFile.contract && typeof signedLead.jobFile.contract === 'object' ? signedLead.jobFile.contract : {};
              signedLead.jobFile.contract.status = 'Signed'; signedLead.jobFile.contract.signedAt = inst.signedAt;
              signedLead.updatedAt = new Date().toISOString();
              crmPushLeadActivity(signedLead, 'Signed contract completed', 'document', crmGetCurrentUserName() || currentRep || '', inst.signedAt);
              crmSaveLeads(leadRows); _activeFormInstance = null;
              if (_crmPostLeadContingencyContext.returnMode === 'documents') {
                _jfDocSelectedCategory = 'Contract'; _crmCurrentJobFileLeadId = signedLead.id;
                _crmPostLeadContingencyContext = null; crmOpenJobFileSection(signedLead.id, 'documents'); showUploadToast('Signed contract saved.');
              }
              return;
            }
            signedLead.contingencySigned = true;`, 'signed contract completion');

  html = replaceOnce(html,
`        if (_crmPostLeadContingencyContext.returnMode === 'documents') { var leadId=_crmPostLeadContingencyContext.leadId; _jfDocSelectedCategory='Contingency'; crmOpenJobFileSection(leadId,'documents'); return; }`,
`        if (_crmPostLeadContingencyContext.returnMode === 'documents') { var leadId=_crmPostLeadContingencyContext.leadId; _jfDocSelectedCategory=String(_crmPostLeadContingencyContext.documentType||'contingency')==='contract'?'Contract':'Contingency'; _crmPostLeadContingencyContext=null; crmOpenJobFileSection(leadId,'documents'); return; }`, 'docusign back category');

  html = replaceOnce(html,
`    var _crmSavedDocsReturnLeadId = '';`,
`    var _crmSavedDocsReturnLeadId = '';
    var _crmSavedDocsReturnCategory = '';`, 'saved docs return category state');

  html = replaceOnce(html,
`      _crmSavedDocsReturnLeadId = '';
      var jobs = getJobs();`,
`      _crmSavedDocsReturnLeadId = ''; _crmSavedDocsReturnCategory = '';
      var jobs = getJobs();`, 'reset saved docs return state');

  html = replaceOnce(html,
`      if (_crmSavedDocsReturnLeadId) { var leadId=_crmSavedDocsReturnLeadId; _crmSavedDocsReturnLeadId=''; _jfDocSelectedCategory='Contingency'; crmOpenJobFileSection(leadId,'documents'); return; }`,
`      if (_crmSavedDocsReturnLeadId) { var leadId=_crmSavedDocsReturnLeadId; var returnCategory=_crmSavedDocsReturnCategory||'Contingency'; _crmSavedDocsReturnLeadId=''; _crmSavedDocsReturnCategory=''; _jfDocSelectedCategory=returnCategory; crmOpenJobFileSection(leadId,'documents'); return; }`, 'saved docs back category');

  html = replaceOnce(html,
`      if (catKeys.indexOf('signed_contingency') !== -1) count += Array.isArray(lead.contingencyFormInstances) ? lead.contingencyFormInstances.length : 0;
      return count;`,
`      if (catKeys.indexOf('signed_contingency') !== -1) count += Array.isArray(lead.contingencyFormInstances) ? lead.contingencyFormInstances.length : 0;
      if (catKeys.indexOf('signed_contract') !== -1) count += Array.isArray(lead.contractFormInstances) ? lead.contractFormInstances.length : 0;
      return count;`, 'contract folder count');

  html = replaceOnce(html,
`      var savedContingencyBtn=e.target.closest('[data-jf-open-contingency-form]');
      if(savedContingencyBtn){crmOpenSavedLeadContingency(_crmCurrentJobFileLeadId,parseInt(savedContingencyBtn.getAttribute('data-jf-open-contingency-form'),10)||0);return;}
      /* Upload from hub category */`,
`      var savedContingencyBtn=e.target.closest('[data-jf-open-contingency-form]');
      if(savedContingencyBtn){crmOpenSavedLeadContingency(_crmCurrentJobFileLeadId,parseInt(savedContingencyBtn.getAttribute('data-jf-open-contingency-form'),10)||0);return;}
      var addContractBtn=e.target.closest('[data-jf-add-contract]');
      if(addContractBtn){crmOpenLeadContractFromDocuments(_crmCurrentJobFileLeadId);return;}
      var savedContractBtn=e.target.closest('[data-jf-open-contract-form]');
      if(savedContractBtn){crmOpenSavedLeadContract(_crmCurrentJobFileLeadId,parseInt(savedContractBtn.getAttribute('data-jf-open-contract-form'),10)||0);return;}
      /* Upload from hub category */`, 'contract document click actions');

  html = replaceOnce(html,
`    function jfRenderDocCategoryHtml(lead, categoryKey) {
      if (categoryKey === 'Contingency') return jfRenderContingencyCategoryHtml(lead);`,
`    function jfRenderContractCategoryHtml(lead) {
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

    function jfRenderDocCategoryHtml(lead, categoryKey) {
      if (categoryKey === 'Contingency') return jfRenderContingencyCategoryHtml(lead);
      if (categoryKey === 'Contract') return jfRenderContractCategoryHtml(lead);`, 'contract category renderer');

  html = replaceOnce(html,
`      var type = _crmJobFilePendingDocType || 'job_document';
      var meta = _crmJobFilePendingUploadMeta || {};`,
`      var type = _crmJobFilePendingDocType || 'job_document';
      var meta = _crmJobFilePendingUploadMeta || {};
      if (type === 'signed_contract' && files.some(function(file){ return String(file.type||'').toLowerCase() !== 'application/pdf' && !/\\.pdf$/i.test(String(file.name||'')); })) { showUploadToast('Upload a signed contract PDF.'); return; }`, 'signed contract PDF validation');

  html = replaceOnce(html,
`          if (type === 'signed_contingency') {
            item.contingencySigned = true; item.contingencySignedAt = new Date().toISOString(); item.contingencyStatus = 'signed_uploaded';
            item.contingencyDocumentId = savedMeta && savedMeta[0] ? String(savedMeta[0].id || '') : ''; data.contingencySigned = true;
            crmPushLeadActivity(item, 'Signed contingency uploaded', 'document', crmGetCurrentUserName() || currentRep || '', item.contingencySignedAt);
          }
          crmApplyLeadJobFileData(item, data); if (type !== 'signed_contingency') crmPushLeadActivity(item, count + ' document' + (count !== 1 ? 's' : '') + ' uploaded', 'document');
        });
        crmRenderJobFile(); showUploadToast(type === 'signed_contingency' ? 'Signed contingency uploaded.' : (count + ' document' + (count !== 1 ? 's' : '') + ' uploaded.'));`,
`          if (type === 'signed_contingency') {
            item.contingencySigned = true; item.contingencySignedAt = new Date().toISOString(); item.contingencyStatus = 'signed_uploaded';
            item.contingencyDocumentId = savedMeta && savedMeta[0] ? String(savedMeta[0].id || '') : ''; data.contingencySigned = true;
            crmPushLeadActivity(item, 'Signed contingency uploaded', 'document', crmGetCurrentUserName() || currentRep || '', item.contingencySignedAt);
          } else if (type === 'signed_contract') {
            item.contractSigned = true; item.contractSignedAt = new Date().toISOString(); item.contractStatus = 'signed_uploaded';
            item.contractDocumentId = savedMeta && savedMeta[0] ? String(savedMeta[0].id || '') : '';
            data.contract = data.contract && typeof data.contract === 'object' ? data.contract : {}; data.contract.status = 'Signed File Uploaded'; data.contract.signedAt = item.contractSignedAt; data.contract.documentId = item.contractDocumentId;
            crmPushLeadActivity(item, 'Signed contract uploaded', 'document', crmGetCurrentUserName() || currentRep || '', item.contractSignedAt);
          }
          crmApplyLeadJobFileData(item, data); if (type !== 'signed_contingency' && type !== 'signed_contract') crmPushLeadActivity(item, count + ' document' + (count !== 1 ? 's' : '') + ' uploaded', 'document');
        });
        crmRenderJobFile(); showUploadToast(type === 'signed_contingency' ? 'Signed contingency uploaded.' : (type === 'signed_contract' ? 'Signed contract uploaded.' : (count + ' document' + (count !== 1 ? 's' : '') + ' uploaded.')));`, 'signed contract upload persistence');
}

if (!verify.includes('data-jf-add-contract')) {
  verify = replaceOnce(verify,
`  "document.getElementById('cfv-print-btn').addEventListener",
];`,
`  "document.getElementById('cfv-print-btn').addEventListener",
  "data-jf-add-contract=\\\"1\\\"",
  "Upload Signed Contract",
  "data-jf-open-contract-form",
  "function crmOpenLeadContractFromDocuments",
  "function crmOpenSavedLeadContract",
  "function jfRenderContractCategoryHtml",
  "type === 'signed_contract'",
  "Signed contract uploaded",
  "Signed contract completed",
];`, 'contract verifier entries');
}

fs.writeFileSync(indexPath, html);
fs.writeFileSync(verifyPath, verify);
console.log('Permanent Contract Documents source changes applied.');
