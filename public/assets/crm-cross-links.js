/* CRM relationship adapter shared by Photo Projects and Estimates. */
(function () {
  'use strict';

  function clean(value) { return String(value || '').trim(); }
  function key(value) { return clean(value).toLowerCase().replace(/[^a-z0-9]/g, ''); }
  function addressKey(value) {
    if (value && typeof value === 'object') value = [value.street, value.city, value.state, value.zip].filter(Boolean).join(' ');
    return key(value);
  }
  function fullName(lead) { return clean([lead && lead.firstName, lead && lead.lastName].filter(Boolean).join(' ')); }
  function splitName(value) {
    var parts = clean(value).split(/\s+/).filter(Boolean);
    return { firstName: parts.shift() || '', lastName: parts.join(' ') };
  }
  function currentUserName() { return typeof crmGetCurrentUserName === 'function' ? clean(crmGetCurrentUserName()) : ''; }
  function currentUserRole() { return typeof crmGetRole === 'function' ? clean(crmGetRole()) : ''; }

  function findLead(data) {
    var leads = typeof crmGetLeads === 'function' ? crmGetLeads() : [];
    var explicitId = clean(data.leadId || data.lead_or_job_id).replace(/^job_/, '');
    if (explicitId) {
      var explicit = leads.find(function (lead) { return clean(lead.id) === explicitId; });
      if (explicit) return explicit;
    }
    var wantedAddress = addressKey(data.address || data);
    var wantedName = key(data.name || data.homeownerName || data.projectName);
    var wantedEmail = key(data.email);
    var wantedPhone = key(data.phone);
    return leads.find(function (lead) {
      var sameAddress = wantedAddress && addressKey(lead) === wantedAddress;
      var sameName = wantedName && key(fullName(lead)) === wantedName;
      var sameContact = (wantedEmail && key(lead.email) === wantedEmail) || (wantedPhone && key(lead.phone || lead.primaryPhone) === wantedPhone);
      return sameAddress && (sameName || sameContact || (!wantedName && !wantedEmail && !wantedPhone));
    }) || null;
  }

  function ensureLead(data) {
    data = data || {};
    var leads = typeof crmGetLeads === 'function' ? crmGetLeads() : [];
    var lead = findLead(data);
    var now = new Date().toISOString();
    if (!lead) {
      var names = splitName(data.name || data.homeownerName || data.projectName || 'Property Owner');
      var address = data.address && typeof data.address === 'object' ? data.address : data;
      var assignedRep = clean(data.assignedRep || currentUserName());
      var member = typeof crmLookupRepMemberByName === 'function' ? crmLookupRepMemberByName(assignedRep) : null;
      lead = {
        id: typeof crmGenId === 'function' ? crmGenId() : ('lead_' + Date.now().toString(36)),
        firstName: names.firstName,
        lastName: names.lastName,
        phone: clean(data.phone),
        email: clean(data.email),
        street: clean(address.street), city: clean(address.city), state: clean(address.state), zip: clean(address.zip),
        assignedRep: assignedRep,
        assignedRepId: clean(member && member.id),
        assignmentStatus: assignedRep ? 'Assigned' : 'Unassigned',
        stage: 'New Lead',
        leadSource: clean(data.leadSource || 'Hail Money'),
        createdByName: clean(data.createdBy || currentUserName()),
        createdByRole: currentUserRole(),
        createdAt: now, updatedAt: now, lastActivityAt: now,
        activityNotes: [], concerns: [], jobFile: {}
      };
      if (typeof crmNormalizeLeadRecord === 'function') lead = crmNormalizeLeadRecord(lead);
      leads.push(lead);
    } else {
      if (!lead.phone && data.phone) lead.phone = clean(data.phone);
      if (!lead.email && data.email) lead.email = clean(data.email);
      if (!lead.assignedRep && data.assignedRep) lead.assignedRep = clean(data.assignedRep);
      lead.updatedAt = now;
    }
    if (typeof crmSaveLeads === 'function') crmSaveLeads(leads.map(function (item) { return clean(item.id) === clean(lead.id) ? lead : item; }));
    return lead;
  }

  function linkPhotoProject(project) {
    if (!project) return null;
    var lead = ensureLead({
      leadId: project.leadId,
      name: project.homeownerName || project.projectName,
      street: project.street, city: project.city, state: project.state, zip: project.zip,
      phone: project.phone, email: project.email,
      assignedRep: project.assignedRep, createdBy: project.createdBy,
      leadSource: 'Photo Project'
    });
    if (!lead) return null;
    project.leadId = lead.id;
    project.jobId = typeof crmGetJobFileId === 'function' ? crmGetJobFileId(lead) : ('job_' + lead.id);
    lead.photoProjectId = project.id;
    lead.photoProjectIds = Array.isArray(lead.photoProjectIds) ? lead.photoProjectIds : [];
    if (lead.photoProjectIds.indexOf(project.id) === -1) lead.photoProjectIds.push(project.id);
    var leads = crmGetLeads();
    crmSaveLeads(leads.map(function (item) { return clean(item.id) === clean(lead.id) ? lead : item; }));
    return lead;
  }

  function readEstimateAddress() {
    function value(id) { var el = document.getElementById(id); return clean(el && el.value); }
    var address = { street: value('est-ai-street'), city: value('est-ai-city'), state: value('est-ai-state'), zip: value('est-ai-zip') };
    address.formatted = [address.street, address.city, address.state, address.zip].filter(Boolean).join(', ');
    return address;
  }

  function persistEstimate(estimate) {
    if (!estimate) return null;
    var customer = estimate.customer || {};
    var lead = ensureLead({ leadId: estimate.leadId || estimate.lead_or_job_id, name: customer.name, phone: customer.phone, email: customer.email, address: estimate.address, leadSource: 'Estimate' });
    if (!lead) return null;
    estimate.leadId = lead.id;
    estimate.jobId = typeof crmGetJobFileId === 'function' ? crmGetJobFileId(lead) : ('job_' + lead.id);
    estimate.lead_or_job_id = lead.id;
    lead.jobFile = lead.jobFile && typeof lead.jobFile === 'object' ? lead.jobFile : {};
    lead.jobFile.estimates = Array.isArray(lead.jobFile.estimates) ? lead.jobFile.estimates : [];
    var copy = JSON.parse(JSON.stringify(estimate));
    var idx = lead.jobFile.estimates.findIndex(function (item) { return clean(item.id) === clean(copy.id); });
    if (idx >= 0) lead.jobFile.estimates[idx] = copy; else lead.jobFile.estimates.push(copy);
    lead.jobFile.estimate = {
      id: copy.id, estimateNumber: copy.estimateNumber, status: copy.status || 'Draft',
      amount: clean(copy.total || copy.grandTotal), scope: clean(copy.estimate_category || copy.trade), notes: clean(copy.notes),
      createdAt: copy.createdAt, updatedAt: copy.updatedAt || new Date().toISOString()
    };
    lead.estimateIds = Array.isArray(lead.estimateIds) ? lead.estimateIds : [];
    if (lead.estimateIds.indexOf(copy.id) === -1) lead.estimateIds.push(copy.id);
    var leads = crmGetLeads();
    crmSaveLeads(leads.map(function (item) { return clean(item.id) === clean(lead.id) ? lead : item; }));
    var saved = [];
    try { saved = JSON.parse(localStorage.getItem('hailMoneyAiEstimatesV1') || '[]'); } catch (_) {}
    if (!Array.isArray(saved)) saved = [];
    var savedIdx = saved.findIndex(function (item) { return clean(item.id) === clean(copy.id); });
    if (savedIdx >= 0) saved[savedIdx] = copy; else saved.unshift(copy);
    localStorage.setItem('hailMoneyAiEstimatesV1', JSON.stringify(saved));
    return lead;
  }

  function attachEstimate(session) {
    var context = window.currentEstimate && currentEstimate.crmContext;
    if (!session || !context) return session;
    var seed = context.estimate || {};
    session.customer = Object.assign({}, session.customer || {}, seed.customer || {});
    session.address = session.address && session.address.street ? session.address : seed.address;
    session.job_type = seed.job_type || 'retail';
    session.insurance = Object.assign({}, seed.insurance || {}, session.insurance || {});
    session.leadId = context.leadId;
    session.jobId = context.jobId;
    session.lead_or_job_id = context.leadId;
    persistEstimate(session);
    return session;
  }

  function ensureEstimateDialog() {
    var dialog = document.getElementById('hm-estimate-crm-dialog');
    if (dialog) return dialog;
    dialog = document.createElement('dialog');
    dialog.id = 'hm-estimate-crm-dialog';
    dialog.style.cssText = 'width:min(520px,calc(100% - 28px));border:1px solid #c99d3e;border-radius:12px;padding:0;box-shadow:0 24px 70px rgba(0,0,0,.38)';
    dialog.innerHTML = '<form method="dialog" style="padding:22px;display:grid;gap:13px"><h2 style="margin:0">Customer and estimate</h2><label>Customer name<input class="field-input" id="hm-est-customer-name" required></label><div style="display:grid;grid-template-columns:1fr 1fr;gap:10px"><label>Phone<input class="field-input" id="hm-est-customer-phone"></label><label>Email<input class="field-input" id="hm-est-customer-email" type="email"></label></div><label>Estimate type<select class="field-input" id="hm-est-job-type"><option value="retail">Retail</option><option value="insurance">Insurance</option></select></label><div style="display:flex;justify-content:flex-end;gap:8px"><button class="btn" value="cancel">Cancel</button><button class="btn btn-primary" id="hm-est-crm-continue" value="default">Continue</button></div></form>';
    document.body.appendChild(dialog);
    return dialog;
  }

  function startEstimateLink() {
    var dialog = ensureEstimateDialog();
    var address = readEstimateAddress();
    dialog.showModal();
    var continueButton = document.getElementById('hm-est-crm-continue');
    continueButton.onclick = function (event) {
      event.preventDefault();
      var name = clean(document.getElementById('hm-est-customer-name').value);
      if (!name) { document.getElementById('hm-est-customer-name').reportValidity(); return; }
      var customer = { name: name, phone: clean(document.getElementById('hm-est-customer-phone').value), email: clean(document.getElementById('hm-est-customer-email').value) };
      var jobType = document.getElementById('hm-est-job-type').value || 'retail';
      var lead = ensureLead({ name: name, phone: customer.phone, email: customer.email, address: address, leadSource: 'Estimate' });
      var now = new Date().toISOString();
      var seed = { id: 'ai_est_' + Date.now().toString(36), createdAt: now, updatedAt: now, customer: customer, address: address, job_type: jobType, insurance: {}, leadId: lead.id, jobId: crmGetJobFileId(lead), lead_or_job_id: lead.id };
      currentEstimate.crmContext = { leadId: lead.id, jobId: seed.jobId, estimate: seed };
      dialog.close();
      var generate = document.getElementById('est-ai-generate');
      if (generate) setTimeout(function () { generate.click(); }, 0);
    };
  }

  window.HailMoneyCrmLinks = { ensureLead: ensureLead, findLead: findLead, linkPhotoProject: linkPhotoProject, persistEstimate: persistEstimate };
  window.HailMoneyCrmEstimate = { start: startEstimateLink, attach: attachEstimate, persist: persistEstimate };

  /* The legacy Photo Project save handler owns its UI. Link the record after it saves. */
  document.addEventListener('click', function (event) {
    if (!event.target || event.target.id !== 'hm-photo-project-save') return;
    var nameInput = document.getElementById('hm-photo-project-name');
    var addressInput = document.getElementById('hm-photo-project-address');
    var wantedName = clean(nameInput && nameInput.value);
    var wantedStreet = clean(addressInput && addressInput.value).split(',')[0];
    setTimeout(function () {
      if (typeof getPhotoFiles !== 'function' || typeof savePhotoFiles !== 'function') return;
      var projects = getPhotoFiles();
      var project = projects.find(function (item) {
        return (wantedName && key(item.homeownerName || item.projectName) === key(wantedName)) ||
          (wantedStreet && key(item.street) === key(wantedStreet));
      });
      if (!project) return;
      linkPhotoProject(project);
      savePhotoFiles(projects);
    }, 0);
  }, true);
})();
