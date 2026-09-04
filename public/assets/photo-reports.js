/* ═══════════════════════════════════════════════════════════════════════
   Hail Money — Photo Reports
   Replaces the placeholder report builder with:
     1. Report title
     2. One empty section (unlimited sections)
     3. Real-photo multi-select (no filenames)
     4. Per-photo descriptions, remove/reorder
     5. Rename / delete / move sections
     6. Options page (cover, page, photo layout)
     7. PDF generation (browser print-to-PDF)
     8. One shared report record: Photos project + Job Details → Reports
   Also adds the Photos Chat that feeds Job Details → Messages with source "Photos".
   Reuses the existing CRM data layer, photo blob storage, and Supabase sync.
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  if (window.hmPhotoReportsLoaded) return;
  window.hmPhotoReportsLoaded = true;

  var REPORTS_KEY = 'hm_photo_reports_v1';
  var CHAT_KEY = 'hm_photo_chat_v1';

  /* ── State ─────────────────────────────────────────────────────────── */
  var rb = {
    step: 'title',        // title | sections | options
    projectId: '',
    draft: null,          // { title, sections: [] }
    pickerSectionIndex: -1,
    selectedPhotoIds: []
  };

  function esc(v) {
    return typeof crmEscapeHtml === 'function'
      ? crmEscapeHtml(String(v == null ? '' : v))
      : String(v == null ? '' : v);
  }

  function genId(prefix) {
    return (prefix || 'id_') + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
  }

  function projects() {
    return getPhotoFiles().map(function (p) {
      p.photos = Array.isArray(p.photos) ? p.photos : [];
      p.reports = Array.isArray(p.reports) ? p.reports : [];
      return p;
    });
  }

  function findProject(id) {
    return projects().filter(function (p) { return p.id === id; })[0] || null;
  }

  function saveProjects(ps) {
    savePhotoFiles(ps);
  }

  function getUserName() {
    return typeof crmGetCurrentUserName === 'function'
      ? (crmGetCurrentUserName() || '')
      : (typeof currentRep !== 'undefined' ? currentRep : '');
  }

  function getUserRole() {
    return typeof crmGetRole === 'function' ? crmGetRole() : '';
  }

  function isAdmin() {
    var role = String(getUserRole() || '').trim();
    return role === 'Admin' || role === 'Owner';
  }

  /* ── Legacy photo-reports store migration ─────────────────────────── */
  function getReportsForProject(projectId) {
    var stored = [];
    try { stored = JSON.parse(localStorage.getItem(REPORTS_KEY) || '[]'); } catch (e) {}
    stored = stored.filter(function (r) { return r && r.projectId === projectId; });
    var project = findProject(projectId);
    var inProject = project ? (project.reports || []) : [];
    var byId = {};
    stored.concat(inProject).forEach(function (r) {
      if (r && r.id) byId[r.id] = r;
    });
    return Object.keys(byId).map(function (id) { return byId[id]; })
      .sort(function (a, b) { return String(b.createdAt || '').localeCompare(String(a.createdAt || '')); });
  }

  function saveReportToProject(projectId, report) {
    // Legacy store (kept for backward compatibility).
    var stored = [];
    try { stored = JSON.parse(localStorage.getItem(REPORTS_KEY) || '[]'); } catch (e) {}
    stored = stored.filter(function (r) { return !(r && r.projectId === projectId && r.id === report.id); });
    stored.push(report);
    try { localStorage.setItem(REPORTS_KEY, JSON.stringify(stored)); } catch (e) {}

    // Canonical store lives ON the project record (synced to Supabase).
    var ps = projects();
    for (var i = 0; i < ps.length; i++) {
      if (ps[i].id !== projectId) continue;
      ps[i].reports = Array.isArray(ps[i].reports) ? ps[i].reports : [];
      var idx = ps[i].reports.findIndex(function (r) { return r.id === report.id; });
      if (idx >= 0) ps[i].reports[idx] = report;
      else ps[i].reports.push(report);
      ps[i].updatedAt = new Date().toISOString();
    }
    saveProjects(ps);
  }

  function deleteReportFromProject(projectId, reportId) {
    var stored = [];
    try { stored = JSON.parse(localStorage.getItem(REPORTS_KEY) || '[]'); } catch (e) {}
    stored = stored.filter(function (r) { return !(r && r.projectId === projectId && r.id === reportId); });
    try { localStorage.setItem(REPORTS_KEY, JSON.stringify(stored)); } catch (e) {}

    var ps = projects();
    for (var i = 0; i < ps.length; i++) {
      if (ps[i].id !== projectId) continue;
      ps[i].reports = Array.isArray(ps[i].reports) ? ps[i].reports.filter(function (r) { return r.id !== reportId; }) : [];
      ps[i].updatedAt = new Date().toISOString();
    }
    saveProjects(ps);
  }

  /* ── Shared job-file report binding ──────────────────────────────────
     A report is stored once on the photo project. The job file references
     the SAME report id so both locations show the same record. */
  function findLeadByReport(reportId) {
    var leads = typeof crmGetLeads === 'function' ? crmGetLeads() : [];
    for (var i = 0; i < leads.length; i++) {
      var jobFile = typeof crmGetLeadJobFileData === 'function' ? crmGetLeadJobFileData(leads[i]) : {};
      var refs = Array.isArray(jobFile.photoReports) ? jobFile.photoReports : [];
      for (var j = 0; j < refs.length; j++) {
        if (refs[j] && String(refs[j].id || '') === String(reportId || '')) return leads[i];
      }
    }
    return null;
  }

  function findLeadByProject(projectId) {
    var project = findProject(projectId);
    if (!project) return null;
    var leads = typeof crmGetLeads === 'function' ? crmGetLeads() : [];
    for (var i = 0; i < leads.length; i++) {
      var address = String(crmGetJobFileLeadAddress ? crmGetJobFileLeadAddress(leads[i]) || '' : '').trim().toLowerCase();
      var projectAddress = [project.street, project.city, project.state, project.zip].filter(Boolean).join(', ').trim().toLowerCase();
      if (address && projectAddress && address === projectAddress) return leads[i];
      var name = String(crmGetJobFileLeadName ? crmGetJobFileLeadName(leads[i]) || '' : '').trim().toLowerCase();
      var pName = String(project.homeownerName || project.projectName || '').trim().toLowerCase();
      if (name && pName && name === pName) return leads[i];
    }
    return null;
  }

  function syncReportRefToJobs(projectId, report, remove) {
    var lead = findLeadByProject(projectId);
    if (!lead || typeof crmGetLeads !== 'function' || typeof crmSaveLeads !== 'function') return;
    var leads = crmGetLeads();
    for (var i = 0; i < leads.length; i++) {
      if (String(leads[i].id || '') !== String(lead.id || '')) continue;
      var jobFile = typeof crmGetLeadJobFileData === 'function' ? crmGetLeadJobFileData(leads[i]) : {};
      jobFile.photoReports = Array.isArray(jobFile.photoReports) ? jobFile.photoReports : [];
      var idx = jobFile.photoReports.findIndex(function (r) { return String(r && r.id || '') === String(report.id || ''); });
      if (remove) {
        if (idx >= 0) jobFile.photoReports.splice(idx, 1);
      } else {
        var ref = {
          id: report.id,
          projectId: projectId,
          title: report.title || 'Property Photo Report',
          type: 'photo_report',
          createdAt: report.createdAt || new Date().toISOString(),
          updatedAt: report.updatedAt || report.createdAt || new Date().toISOString()
        };
        if (idx >= 0) jobFile.photoReports[idx] = ref;
        else jobFile.photoReports.push(ref);
      }
      if (typeof crmApplyLeadJobFileData === 'function') {
        crmApplyLeadJobFileData(leads[i], jobFile);
      } else {
        leads[i].jobFile = jobFile;
      }
      leads[i].photoReports = jobFile.photoReports;
      leads[i].updatedAt = new Date().toISOString();
      break;
    }
    crmSaveLeads(leads);
    if (typeof crmRenderMainMenu === 'function') crmRenderMainMenu();
  }

  /* ── Chat store: messages carry leadId so they appear in Job Details ── */
  function getChatMessages(projectId) {
    var all = [];
    try { all = JSON.parse(localStorage.getItem(CHAT_KEY) || '[]'); } catch (e) {}
    return all.filter(function (m) { return m && m.projectId === projectId; })
      .sort(function (a, b) { return String(a.createdAt || '').localeCompare(String(b.createdAt || '')); });
  }

  function saveChatMessage(projectId, body) {
    body = String(body || '').trim();
    if (!body) return null;
    var lead = findLeadByProject(projectId);
    var leadId = lead ? lead.id : '';
    var msg = {
      id: 'msg_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
      projectId: projectId,
      leadId: leadId,
      source: 'Photos',
      createdByName: getUserName() || 'User',
      createdByRole: getUserRole(),
      body: body,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    var all = [];
    try { all = JSON.parse(localStorage.getItem(CHAT_KEY) || '[]'); } catch (e) {}
    all.push(msg);
    try { localStorage.setItem(CHAT_KEY, JSON.stringify(all)); } catch (e) {}

    /* Surface in Job Details → Messages with source "Photos". */
    if (leadId && typeof jfGetMessages === 'function' && typeof jfSaveMessages === 'function') {
      var jobMsgs = jfGetMessages();
      jobMsgs.push({
        id: msg.id,
        jobId: typeof crmGetJobFileId === 'function' ? crmGetJobFileId(lead) : '',
        leadId: leadId,
        source: 'Photos',
        photoProjectId: projectId,
        customerName: typeof crmGetJobFileLeadName === 'function' ? crmGetJobFileLeadName(lead) : '',
        propertyAddress: typeof crmGetJobFileLeadAddress === 'function' ? crmGetJobFileLeadAddress(lead) : '',
        createdByName: msg.createdByName,
        createdByRole: msg.createdByRole,
        body: body,
        mentionedNames: [],
        mentionedUserIds: [],
        createdTaskIds: [],
        createdAt: msg.createdAt,
        updatedAt: msg.updatedAt
      });
      jfSaveMessages(jobMsgs);
      if (lead && typeof crmPushLeadActivity === 'function') {
        crmPushLeadActivity(lead, 'Photos chat: ' + String(body).slice(0, 80), 'note', msg.createdByName, msg.createdAt);
      }
    }
    return msg;
  }

  /* ── Photo source rendering (real thumbnails) ─────────────────────── */
  function photoSrc(photo, cb) {
    if (typeof window.hmPhotoSignedUrl === 'function' && photo.storagePath) {
      window.hmPhotoSignedUrl(photo.storagePath).then(cb).catch(function () { cb(''); });
      return;
    }
    if (photo.fullUrl) { cb(photo.fullUrl); return; }
    if (photo.imageKey && typeof getPhotoBlob === 'function') {
      getPhotoBlob(photo.imageKey).then(function (blob) {
        cb(blob ? URL.createObjectURL(blob) : '');
      });
      return;
    }
    cb('');
  }

  function loadPhotoInto(img, photo, fallback) {
    photoSrc(photo, function (src) {
      if (src && img.isConnected) img.src = src;
      else if (fallback) fallback(img);
    });
  }

  /* ── Photo project lookup by lead (for Job Details link-back) ─────── */
  function findProjectForLead(leadId) {
    var lead = typeof crmGetJobFileLeadById === 'function' ? crmGetJobFileLeadById(leadId) : null;
    if (!lead) return '';
    var ps = projects();
    for (var i = 0; i < ps.length; i++) {
      var p = ps[i];
      if ((p.reports || []).some(function (r) { return findLeadByReport(r.id) && String(findLeadByReport(r.id).id || '') === String(leadId); })) {
        return p.id;
      }
    }
    for (var j = 0; j < ps.length; j++) {
      var address = String(crmGetJobFileLeadAddress ? crmGetJobFileLeadAddress(lead) || '' : '').trim().toLowerCase();
      var pAddress = [ps[j].street, ps[j].city, ps[j].state, ps[j].zip].filter(Boolean).join(', ').trim().toLowerCase();
      if (address && pAddress && address === pAddress) return ps[j].id;
      var name = String(lead.firstName + ' ' + lead.lastName).trim().toLowerCase();
      var pName = String(ps[j].homeownerName || ps[j].projectName || '').trim().toLowerCase();
      if (name && pName && name === pName) return ps[j].id;
    }
    return '';
  }

  /* ── Open photo chat from a Job Details message ───────────────────── */
  function openChatFromMessage(message) {
    var projectId = String((message && (message.photoProjectId || message.projectId)) || '').trim();
    if (!projectId) projectId = findProjectForLead(message && message.leadId);
    if (!projectId) {
      if (typeof showUploadToast === 'function') showUploadToast('This message is not linked to a photo project.');
      return;
    }
    var p = findProject(projectId);
    if (!p) return;
    if (typeof window.openPhotoFileDetail === 'function') window.openPhotoFileDetail(projectId);
    if (typeof window.renderPhotoProjectChat === 'function') window.renderPhotoProjectChat(projectId);
  }

  /* ════════════════════════════════════════════════════════════════════
     REPORT BUILDER
     ════════════════════════════════════════════════════════════════════ */
  window.hmPhotoReports = {
    openBuilder: openBuilder,
    openReport: openReport,
    getReportsForProject: getReportsForProject,
    openChatFromMessage: openChatFromMessage,
    printReport: printReport,
    confirmPhotoPicker: confirmPhotoPicker,
    cancelPhotoPicker: cancelPhotoPicker
  };

  function openBuilder(projectId) {
    var p = findProject(projectId);
    if (!p) return;
    rb.projectId = projectId;
    rb.step = 'title';
    rb.draft = {
      title: '',
      sections: [{ id: genId('sec_'), title: 'Section 1', photos: [] }]
    };
    showPage('page-photo-report-builder');
    renderBuilder();
  }

  function renderBuilder() {
    var page = document.getElementById('page-photo-report-builder');
    if (!page) return;
    var content = document.getElementById('phr-content');
    if (!content) return;
    var p = findProject(rb.projectId);
    if (!p) { content.innerHTML = '<div class="crm-empty-state">Photo project not found.</div>'; return; }

    var html = '<div class="phr-toolbar"><button class="btn" type="button" id="phr-back">Back</button>' +
      '<div class="phr-toolbar-title">' + esc(p.projectName || p.homeownerName || 'Photo Project') + '</div>' +
      '<span class="phr-step-indicator">Step ' + (rb.step === 'title' ? '1' : rb.step === 'sections' ? '2' : '3') + ' of 3</span></div>';

    if (rb.step === 'title') {
      html += '<div class="phr-card phr-step-title">';
      html += '<div class="phr-step-label">Step 1</div>';
      html += '<div class="phr-step-heading">Report Title</div>';
      html += '<input type="text" id="phr-title-input" class="phr-title-input" placeholder="Report title" value="' + esc(rb.draft.title) + '" />';
      html += '<div class="phr-actions"><button class="btn btn-primary" type="button" id="phr-continue">Continue</button></div>';
      html += '</div>';
    } else if (rb.step === 'sections') {
      html += renderSectionsEditor(p);
    } else {
      html += renderOptionsEditor(p);
    }

    content.innerHTML = html;
    wireBuilderEvents();
  }

  function renderSectionsEditor(p) {
    var html = '<div class="phr-card">';
    html += '<div class="phr-step-label">Step 2</div>';
    html += '<div class="phr-step-heading">Sections</div>';
    html += '<div id="phr-sections">';
    rb.draft.sections.forEach(function (section, index) {
      html += renderSectionCard(p, section, index);
    });
    html += '</div>';
    html += '<div class="phr-section-add-row"><button class="btn" type="button" id="phr-add-section">+ Add Section</button></div>';
    html += '<div class="phr-actions"><button class="btn" type="button" id="phr-back-to-title">Back</button><button class="btn btn-primary" type="button" id="phr-to-options">Finish</button></div>';
    html += '</div>';
    return html;
  }

  function renderSectionCard(p, section, index) {
    var html = '<div class="phr-section" data-phr-section="' + esc(section.id) + '">';
    html += '<div class="phr-section-head">';
    html += '<div class="phr-section-title-row">';
    html += '<span class="phr-section-number">Section ' + (index + 1) + '</span>';
    html += '<input type="text" class="phr-section-title-input" value="' + esc(section.title) + '" data-phr-section-title="' + esc(section.id) + '" placeholder="Section title" />';
    html += '</div>';
    html += '<div class="phr-section-tools">';
    html += '<button class="btn" type="button" data-phr-move="up" data-phr-section="' + esc(section.id) + '">&#8593;</button>';
    html += '<button class="btn" type="button" data-phr-move="down" data-phr-section="' + esc(section.id) + '">&#8595;</button>';
    html += '<button class="btn" type="button" data-phr-rename="' + esc(section.id) + '">Rename</button>';
    html += '<button class="btn" type="button" data-phr-delete-section="' + esc(section.id) + '">Delete</button>';
    html += '</div>';
    html += '</div>';
    html += '<div class="phr-photo-grid" data-phr-photo-grid="' + esc(section.id) + '">';
    if (!section.photos.length) {
      html += '<div class="phr-empty-photos">No photos in this section yet.</div>';
    }
    section.photos.forEach(function (photo, photoIndex) {
      html += '<div class="phr-photo" data-phr-photo="' + esc(photo.id || photo.imageKey || photoIndex) + '" data-phr-photo-section="' + esc(section.id) + '">';
      html += '<div class="phr-photo-thumb"><img data-phr-photo-img="' + esc(photo.id || photo.imageKey || photoIndex) + '" alt="" /></div>';
      html += '<input type="text" class="phr-photo-desc" placeholder="Description (optional)" value="' + esc(photo.description || photo.caption || photo.note || '') + '" data-phr-photo-desc="' + esc(photo.id || photo.imageKey || photoIndex) + '" />';
      html += '<div class="phr-photo-tools">';
      html += '<button class="btn" type="button" data-phr-photo-move="up" data-phr-photo-section="' + esc(section.id) + '" data-phr-photo-key="' + esc(photo.id || photo.imageKey || photoIndex) + '">&#8593;</button>';
      html += '<button class="btn" type="button" data-phr-photo-move="down" data-phr-photo-section="' + esc(section.id) + '" data-phr-photo-key="' + esc(photo.id || photo.imageKey || photoIndex) + '">&#8595;</button>';
      html += '<button class="btn" type="button" data-phr-photo-remove="' + esc(photo.id || photo.imageKey || photoIndex) + '" data-phr-photo-section="' + esc(section.id) + '">Remove</button>';
      html += '</div></div>';
    });
    html += '</div>';
    html += '<button class="btn btn-primary" type="button" data-phr-add-photos="' + esc(section.id) + '">Add Photos</button>';
    html += '</div>';
    return html;
  }

  function renderOptionsEditor(p) {
    var html = '<div class="phr-card">';
    html += '<div class="phr-step-label">Step 3</div>';
    html += '<div class="phr-step-heading">Report Options</div>';
    html += '<div class="phr-options-grid">';
    html += renderOptionGroup('Cover Page', ['companyLogo', 'companyName', 'companyAddress', 'companyPhone', 'companyEmail', 'representativeName', 'representativePhone', 'representativeEmail', 'homeowner', 'propertyAddress', 'inspectionDate', 'reportTitle', 'reportDate', 'claimNumber', 'carrier', 'policyNumber', 'adjuster', 'customNotes'], rb.draft);
    html += renderOptionGroup('Page Options', ['pageNumbers', 'sectionNumbers', 'logoEveryPage', 'titleEveryPage', 'header', 'footer', 'pagePropertyAddress', 'pageRepresentative'], rb.draft);
    html += renderOptionGroup('Photo Layout', ['photosPerPage1', 'photosPerPage2', 'photosPerPage4', 'orientationPortrait', 'orientationLandscape', 'preserveAspectRatio', 'useEdited', 'useOriginal', 'showDescriptions', 'photoNumbering'], rb.draft);
    html += '</div>';
    html += '<div class="phr-custom-notes-row">';
    html += '<label>Custom notes (cover page)</label>';
    html += '<textarea id="phr-custom-notes" class="phr-custom-notes">' + esc(rb.draft.customNotes || '') + '</textarea>';
    html += '</div>';
    html += '<div class="phr-actions"><button class="btn" type="button" id="phr-back-to-sections">Back</button><button class="btn btn-primary" type="button" id="phr-create-pdf">Create Report</button></div>';
    html += '</div>';
    return html;
  }

  var OPTION_LABELS = {
    companyLogo: 'Company Logo',
    companyName: 'Company Name',
    companyAddress: 'Company Address',
    companyPhone: 'Company Phone',
    companyEmail: 'Company Email',
    representativeName: 'Representative Name',
    representativePhone: 'Representative Phone',
    representativeEmail: 'Representative Email',
    homeowner: 'Homeowner',
    propertyAddress: 'Property Address',
    inspectionDate: 'Inspection Date',
    reportTitle: 'Report Title',
    reportDate: 'Report Date',
    claimNumber: 'Claim Number',
    carrier: 'Carrier',
    policyNumber: 'Policy Number',
    adjuster: 'Adjuster',
    customNotes: 'Custom Notes',
    pageNumbers: 'Page Numbers',
    sectionNumbers: 'Section Numbers',
    logoEveryPage: 'Logo on Every Page',
    titleEveryPage: 'Title on Every Page',
    header: 'Header',
    footer: 'Footer',
    pagePropertyAddress: 'Property Address',
    pageRepresentative: 'Representative',
    photosPerPage1: '1 Photo / Page',
    photosPerPage2: '2 Photos / Page',
    photosPerPage4: '4 Photos / Page',
    orientationPortrait: 'Portrait',
    orientationLandscape: 'Landscape',
    preserveAspectRatio: 'Preserve Aspect Ratio',
    useEdited: 'Edited Photo',
    useOriginal: 'Original Photo',
    showDescriptions: 'Photo Descriptions',
    photoNumbering: 'Photo Numbering'
  };

  function renderOptionGroup(title, keys, draft) {
    var html = '<div class="phr-option-group"><div class="phr-option-group-title">' + esc(title) + '</div>';
    keys.forEach(function (key) {
      var checked = draft[key] !== false;
      html += '<label class="phr-option"><input type="checkbox" data-phr-option="' + esc(key) + '"' + (checked ? ' checked' : '') + ' /> <span>' + esc(OPTION_LABELS[key] || key) + '</span></label>';
    });
    html += '</div>';
    return html;
  }

  function wireBuilderEvents() {
    var content = document.getElementById('phr-content');
    if (!content) return;

    var backBtn = document.getElementById('phr-back');
    if (backBtn) backBtn.onclick = function () {
      if (rb.step === 'title' || rb.step === 'sections') {
        if (typeof window.openPhotoFileDetail === 'function') window.openPhotoFileDetail(rb.projectId);
        else showPage('page-photo-file-detail');
      } else {
        rb.step = 'sections';
        renderBuilder();
      }
    };

    var continueBtn = document.getElementById('phr-continue');
    if (continueBtn) continueBtn.onclick = function () {
      rb.draft.title = String((document.getElementById('phr-title-input') || {}).value || '').trim();
      rb.step = 'sections';
      renderBuilder();
    };

    var backToTitle = document.getElementById('phr-back-to-title');
    if (backToTitle) backToTitle.onclick = function () { rb.step = 'title'; renderBuilder(); };
    var toOptions = document.getElementById('phr-to-options');
    if (toOptions) toOptions.onclick = function () { rb.step = 'options'; renderBuilder(); };
    var backToSections = document.getElementById('phr-back-to-sections');
    if (backToSections) backToSections.onclick = function () { rb.step = 'sections'; renderBuilder(); };

    var addSection = document.getElementById('phr-add-section');
    if (addSection) addSection.onclick = function () {
      rb.draft.sections.push({ id: genId('sec_'), title: 'Section ' + (rb.draft.sections.length + 1), photos: [] });
      renderBuilder();
    };

    var titleInput = document.getElementById('phr-title-input');
    if (titleInput) titleInput.addEventListener('input', function () { rb.draft.title = titleInput.value; });
    content.addEventListener('input', function (e) {
      var titleInput = e.target.closest('[data-phr-section-title]');
      if (titleInput) {
        var section = findDraftSection(titleInput.getAttribute('data-phr-section-title'));
        if (section) section.title = titleInput.value;
      }
      var descInput = e.target.closest('[data-phr-photo-desc]');
      if (descInput) {
        setDraftPhotoDesc(descInput.getAttribute('data-phr-photo-desc'), descInput.value);
      }
      var notes = document.getElementById('phr-custom-notes');
      if (notes && e.target === notes) rb.draft.customNotes = notes.value;
    });
    content.addEventListener('change', function (e) {
      var option = e.target.closest('[data-phr-option]');
      if (option) rb.draft[option.getAttribute('data-phr-option')] = option.checked;
      var notes = document.getElementById('phr-custom-notes');
      if (notes && e.target === notes) rb.draft.customNotes = notes.value;
    });

    content.addEventListener('click', function (e) {
      var addPhotosBtn = e.target.closest('[data-phr-add-photos]');
      if (addPhotosBtn) { openPhotoPicker(addPhotosBtn.getAttribute('data-phr-add-photos')); return; }
      var moveSection = e.target.closest('[data-phr-move]');
      if (moveSection) {
        moveDraftSection(moveSection.getAttribute('data-phr-section'), moveSection.getAttribute('data-phr-move'));
        return;
      }
      var renameBtn = e.target.closest('[data-phr-rename]');
      if (renameBtn) {
        var section = findDraftSection(renameBtn.getAttribute('data-phr-rename'));
        if (section) {
          var next = prompt('Rename section', section.title);
          if (next != null && String(next).trim()) section.title = String(next).trim();
          renderBuilder();
        }
        return;
      }
      var deleteSection = e.target.closest('[data-phr-delete-section]');
      if (deleteSection) {
        rb.draft.sections = rb.draft.sections.filter(function (s) { return s.id !== deleteSection.getAttribute('data-phr-delete-section'); });
        if (!rb.draft.sections.length) rb.draft.sections.push({ id: genId('sec_'), title: 'Section 1', photos: [] });
        renderBuilder();
        return;
      }
      var photoMove = e.target.closest('[data-phr-photo-move]');
      if (photoMove) {
        moveDraftPhoto(photoMove.getAttribute('data-phr-photo-section'), photoMove.getAttribute('data-phr-photo-key'), photoMove.getAttribute('data-phr-photo-move'));
        return;
      }
      var photoRemove = e.target.closest('[data-phr-photo-remove]');
      if (photoRemove) {
        removeDraftPhoto(photoRemove.getAttribute('data-phr-photo-section'), photoRemove.getAttribute('data-phr-photo-remove'));
        return;
      }
      var createPdf = e.target.closest('#phr-create-pdf');
      if (createPdf) { createReport(); return; }
    });

    // Hydrate thumbnails after each render.
    hydrateBuilderThumbnails(content);
  }

  function findDraftSection(id) {
    for (var i = 0; i < rb.draft.sections.length; i++) {
      if (rb.draft.sections[i].id === id) return rb.draft.sections[i];
    }
    return null;
  }

  function moveDraftSection(id, direction) {
    var index = rb.draft.sections.findIndex(function (s) { return s.id === id; });
    var next = direction === 'up' ? index - 1 : index + 1;
    if (index >= 0 && next >= 0 && next < rb.draft.sections.length) {
      var moved = rb.draft.sections.splice(index, 1)[0];
      rb.draft.sections.splice(next, 0, moved);
      renderBuilder();
    }
  }

  function setDraftPhotoDesc(key, value) {
    rb.draft.sections.forEach(function (section) {
      section.photos.forEach(function (photo) {
        var photoKey = String(photo.id || photo.imageKey || '');
        if (photoKey === key) photo.description = String(value || '');
      });
    });
  }

  function moveDraftPhoto(sectionId, key, direction) {
    var section = findDraftSection(sectionId);
    if (!section) return;
    var index = section.photos.findIndex(function (ph) { return String(ph.id || ph.imageKey || '') === String(key || ''); });
    var next = direction === 'up' ? index - 1 : index + 1;
    if (index >= 0 && next >= 0 && next < section.photos.length) {
      var moved = section.photos.splice(index, 1)[0];
      section.photos.splice(next, 0, moved);
      renderBuilder();
    }
  }

  function removeDraftPhoto(sectionId, key) {
    var section = findDraftSection(sectionId);
    if (!section) return;
    section.photos = section.photos.filter(function (ph) { return String(ph.id || ph.imageKey || '') !== String(key || ''); });
    renderBuilder();
  }

  function hydrateBuilderThumbnails(container) {
    if (!container) return;
    Array.prototype.forEach.call(container.querySelectorAll('[data-phr-photo-img]'), function (img) {
      var key = img.getAttribute('data-phr-photo-img');
      var photo = findDraftPhoto(key);
      if (!photo) return;
      loadPhotoInto(img, photo, function (fallbackImg) {
        fallbackImg.alt = photo.name || 'Photo';
        fallbackImg.style.background = '#eef1f5';
        fallbackImg.style.display = 'none';
      });
    });
  }

  function findDraftPhoto(key) {
    for (var i = 0; i < rb.draft.sections.length; i++) {
      for (var j = 0; j < rb.draft.sections[i].photos.length; j++) {
        var photo = rb.draft.sections[i].photos[j];
        if (String(photo.id || photo.imageKey || '') === String(key || '')) return photo;
      }
    }
    return null;
  }

  /* ── Photo picker (Step 3: Add Photos) ────────────────────────────── */
  function openPhotoPicker(sectionId) {
    var p = findProject(rb.projectId);
    if (!p) return;
    rb.pickerSectionIndex = rb.draft.sections.findIndex(function (s) { return s.id === sectionId; });
    if (rb.pickerSectionIndex < 0) return;
    rb.selectedPhotoIds = [];
    var overlay = document.getElementById('phr-picker-overlay');
    var grid = document.getElementById('phr-picker-grid');
    var title = document.getElementById('phr-picker-title');
    if (!overlay || !grid) return;
    var section = rb.draft.sections[rb.pickerSectionIndex];
    var selectedIds = {};
    section.photos.forEach(function (photo) { selectedIds[String(photo.id || photo.imageKey || '')] = true; });
    title.textContent = 'Add Photos — ' + (section.title || 'Section');
    grid.innerHTML = p.photos.length
      ? p.photos.map(function (photo, index) {
          var key = String(photo.id || photo.imageKey || index);
          var selected = !!selectedIds[key];
          return '<button type="button" class="phr-picker-photo' + (selected ? ' selected' : '') + '" data-phr-picker-key="' + esc(key) + '">' +
            '<span class="phr-picker-thumb"><img data-phr-picker-img="' + esc(key) + '" alt="" /></span>' +
            '<span class="phr-picker-check">' + (selected ? '&#10003;' : '') + '</span>' +
            '</button>';
        }).join('')
      : '<div class="crm-empty-state">No photos in this project yet. Add photos first.</div>';
    overlay.hidden = false;
    Array.prototype.forEach.call(grid.querySelectorAll('[data-phr-picker-key]'), function (btn) {
      btn.onclick = function () {
        var key = btn.getAttribute('data-phr-picker-key');
        var wasSelected = btn.classList.contains('selected');
        btn.classList.toggle('selected', !wasSelected);
        btn.querySelector('.phr-picker-check').innerHTML = !wasSelected ? '&#10003;' : '';
        if (!wasSelected) rb.selectedPhotoIds.push(key);
        else rb.selectedPhotoIds = rb.selectedPhotoIds.filter(function (k) { return k !== key; });
      };
    });
    Array.prototype.forEach.call(grid.querySelectorAll('[data-phr-picker-img]'), function (img) {
      var key = img.getAttribute('data-phr-picker-img');
      var photo = p.photos[key] || p.photos.find(function (ph) { return String(ph.id || ph.imageKey || '') === key; });
      if (photo) loadPhotoInto(img, photo, function (f) { f.style.background = '#eef1f5'; });
    });
  }

  function confirmPhotoPicker() {
    var overlay = document.getElementById('phr-picker-overlay');
    if (!overlay) return;
    var p = findProject(rb.projectId);
    var section = rb.draft.sections[rb.pickerSectionIndex];
    var existing = {};
    section.photos.forEach(function (photo) { existing[String(photo.id || photo.imageKey || '')] = photo; });
    var chosen = [];
    rb.selectedPhotoIds.forEach(function (key) {
      var photo = p.photos.find(function (ph) { return String(ph.id || ph.imageKey || '') === key; });
      if (!photo) return;
      var copy = JSON.parse(JSON.stringify(photo));
      copy.description = (existing[String(copy.id || copy.imageKey || '')] || {}).description || '';
      chosen.push(copy);
    });
    // Merge without duplicates.
    var byKey = {};
    section.photos.concat(chosen).forEach(function (photo) {
      byKey[String(photo.id || photo.imageKey || '')] = photo;
    });
    section.photos = Object.keys(byKey).map(function (k) { return byKey[k]; });
    overlay.hidden = true;
    renderBuilder();
  }

  function cancelPhotoPicker() {
    var overlay = document.getElementById('phr-picker-overlay');
    if (overlay) overlay.hidden = true;
  }

  /* ════════════════════════════════════════════════════════════════════
     REPORT CREATION + PDF
     ════════════════════════════════════════════════════════════════════ */
  function buildReportObject() {
    var p = findProject(rb.projectId);
    var draft = rb.draft;
    var now = new Date().toISOString();
    var lead = findLeadByProject(rb.projectId);
    var profile = typeof crmGetCompanyProfile === 'function' ? crmGetCompanyProfile() : {};
    var report = {
      id: 'photo_report_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6),
      type: 'photo_report',
      projectId: rb.projectId,
      title: String(draft.title || (p && (p.projectName || p.homeownerName)) || 'Property Photo Report').trim(),
      createdAt: now,
      updatedAt: now,
      options: {
        cover: {},
        page: {},
        layout: {}
      },
      sections: draft.sections.map(function (section) {
        return {
          id: section.id,
          title: String(section.title || 'Section').trim(),
          photos: section.photos.map(function (photo) {
            return {
              id: photo.id || photo.imageKey || '',
              imageKey: photo.imageKey || '',
              storagePath: photo.storagePath || '',
              name: photo.name || '',
              description: String(photo.description || '').trim(),
              markedUp: !!photo.markedUp
            };
          })
        };
      })
    };

    // Explicit option booleans (default true unless turned off).
    ['companyLogo','companyName','companyAddress','companyPhone','companyEmail','representativeName','representativePhone','representativeEmail','homeowner','propertyAddress','inspectionDate','reportTitle','reportDate','claimNumber','carrier','policyNumber','adjuster','customNotes'].forEach(function (key) {
      report.options.cover[key] = draft[key] !== false;
    });
    ['pageNumbers','sectionNumbers','logoEveryPage','titleEveryPage','header','footer','pagePropertyAddress','pageRepresentative'].forEach(function (key) {
      report.options.page[key] = draft[key] !== false;
    });
    // Photo layout: one selection for per-page count + orientation.
    report.options.layout.perPage = draft.photosPerPage1 ? 1 : (draft.photosPerPage2 ? 2 : (draft.photosPerPage4 ? 4 : 4));
    report.options.layout.portrait = draft.orientationLandscape !== true;
    report.options.layout.preserveAspectRatio = draft.preserveAspectRatio !== false;
    report.options.layout.useEdited = draft.useEdited !== false;
    report.options.layout.useOriginal = draft.useOriginal !== false;
    report.options.layout.showDescriptions = draft.showDescriptions !== false;
    report.options.layout.photoNumbering = draft.photoNumbering !== false;

    // Cover data.
    var representative = String(lead && lead.assignedRep || getUserName() || '').trim();
    var streetAddress = lead ? crmGetJobFileLeadAddress ? crmGetJobFileLeadAddress(lead) : '' : '';
    var inspectionDate = lead ? String(lead.inspectionDate || (lead.jobFile && lead.jobFile.inspection && lead.jobFile.inspection.inspectionDate) || '').trim() : '';
    var claimNumber = lead ? String(lead.claimNumber || (lead.jobFile && lead.jobFile.insurance && lead.jobFile.insurance.claimNumber) || '').trim() : '';
    var carrier = lead ? String(lead.insuranceCompany || (lead.jobFile && lead.jobFile.insurance && lead.jobFile.insurance.company) || '').trim() : '';
    var policyNumber = lead ? String((lead.jobFile && lead.jobFile.insurance && lead.jobFile.insurance.policyNumber) || '').trim() : '';
    var adjuster = lead ? String(lead.adjusterName || (lead.jobFile && lead.jobFile.insurance && lead.jobFile.insurance.adjusterName) || '').trim() : '';

    report.cover = {
      companyName: String(profile.companyName || '').trim(),
      companyAddress: [profile.street, profile.city, profile.state, profile.zip].filter(Boolean).join(', '),
      companyPhone: String(profile.phone || '').trim(),
      companyEmail: String(profile.email || '').trim(),
      companyLogo: typeof crmGetCompanyLogoUrl === 'function' ? crmGetCompanyLogoUrl() : '',
      representativeName: representative,
      representativePhone: '',
      representativeEmail: '',
      homeowner: lead ? crmGetJobFileLeadName(lead) : '',
      propertyAddress: streetAddress,
      inspectionDate: inspectionDate,
      reportTitle: report.title,
      reportDate: new Date().toISOString().slice(0, 10),
      claimNumber: claimNumber,
      carrier: carrier,
      policyNumber: policyNumber,
      adjuster: adjuster,
      customNotes: String(draft.customNotes || '').trim()
    };
    return report;
  }

  function createReport() {
    if (!rb.draft.title.trim()) {
      var lead = findLeadByProject(rb.projectId);
      rb.draft.title = String((lead ? crmGetJobFileLeadName(lead) : '') || 'Property Photo Report').trim();
    }
    var report = buildReportObject();
    saveReportToProject(rb.projectId, report);
    syncReportRefToJobs(rb.projectId, report, false);
    var reportLead = findLeadByProject(rb.projectId);
    if (reportLead && typeof crmPushLeadActivity === 'function' && typeof crmGetJobFileLeadName === 'function') {
      try { crmPushLeadActivity(reportLead, 'Photo report created: ' + report.title, 'note', getUserName() || 'User'); } catch (e) {}
    }
    if (typeof showUploadToast === 'function') showUploadToast('Report saved — generating PDF…');
    openReport(rb.projectId, report.id);
  }

  function openReport(projectId, reportId) {
    var p = findProject(projectId);
    if (!p) return;
    var reports = getReportsForProject(projectId);
    var report = reports.find(function (r) { return String(r.id || '') === String(reportId || ''); }) || reports[0];
    if (!report) { if (typeof showUploadToast === 'function') showUploadToast('No report found for this project.'); return; }
    rb.projectId = projectId;
    renderReportPage(report);
    showPage('page-photo-report-preview');
  }

  function renderReportPage(report) {
    var pagesEl = document.getElementById('phr-preview-pages');
    if (!pagesEl) return;
    var titleEl = document.getElementById('phr-preview-title');
    if (titleEl) titleEl.textContent = report.title || 'Property Photo Report';
    pagesEl.innerHTML = buildReportHtml(report);
    hydrateReportThumbnails(pagesEl);
  }

  function buildReportHtml(report) {
    var html = '';
    var options = report.options || {};
    var cover = report.cover || {};
    var layout = options.layout || { perPage: 4, portrait: true, preserveAspectRatio: true, showDescriptions: true, photoNumbering: true };
    var perPage = [1, 2, 4].indexOf(Number(layout.perPage)) !== -1 ? Number(layout.perPage) : 4;
    var portrait = layout.portrait !== false;
    var showDesc = layout.showDescriptions !== false;
    var showNum = layout.photoNumbering !== false;

    /* Cover page */
    html += '<article class="phr-pdf-page phr-cover-page" data-phr-page="cover">';
    html += '<div class="phr-cover-inner">';
    if (options.cover.companyLogo !== false && cover.companyLogo) {
      html += '<img class="phr-cover-logo" src="' + esc(cover.companyLogo) + '" alt="Company logo" />';
    }
    if (options.cover.companyName !== false && cover.companyName) {
      html += '<div class="phr-cover-company">' + esc(cover.companyName) + '</div>';
    }
    var companyLine = [options.cover.companyAddress !== false ? cover.companyAddress : '', options.cover.companyPhone !== false ? cover.companyPhone : '', options.cover.companyEmail !== false ? cover.companyEmail : ''].filter(Boolean).join(' · ');
    if (companyLine) html += '<div class="phr-cover-company-info">' + esc(companyLine) + '</div>';
    html += '<div class="phr-cover-title">' + esc(options.cover.reportTitle !== false ? (report.title || 'Property Photo Report') : '') + '</div>';
    html += '<div class="phr-cover-rule"></div>';
    html += '<div class="phr-cover-details">';
    var rows = [
      ['Homeowner', cover.homeowner, options.cover.homeowner !== false],
      ['Property Address', cover.propertyAddress, options.cover.propertyAddress !== false],
      ['Inspection Date', cover.inspectionDate, options.cover.inspectionDate !== false],
      ['Report Date', cover.reportDate, options.cover.reportDate !== false],
      ['Representative', cover.representativeName, options.cover.representativeName !== false],
      ['Representative Phone', cover.representativePhone, options.cover.representativePhone !== false],
      ['Representative Email', cover.representativeEmail, options.cover.representativeEmail !== false],
      ['Claim Number', cover.claimNumber, options.cover.claimNumber !== false],
      ['Carrier', cover.carrier, options.cover.carrier !== false],
      ['Policy Number', cover.policyNumber, options.cover.policyNumber !== false],
      ['Adjuster', cover.adjuster, options.cover.adjuster !== false]
    ];
    rows.forEach(function (row) {
      if (row[2] !== false && String(row[1] || '').trim()) {
        html += '<div class="phr-cover-row"><span class="phr-cover-label">' + esc(row[0]) + '</span><span class="phr-cover-value">' + esc(row[1]) + '</span></div>';
      }
    });
    if (options.cover.customNotes !== false && String(cover.customNotes || '').trim()) {
      html += '<div class="phr-cover-notes">' + esc(cover.customNotes) + '</div>';
    }
    html += '</div></div></article>';

    /* Section pages */
    var photoNumber = 1;
    (report.sections || []).forEach(function (section, sectionIndex) {
      var photos = (section.photos || []).filter(function (ph) { return ph.storagePath || ph.imageKey || ph.fullUrl; });
      if (!photos.length) return;
      var chunkHtml = '';
      for (var offset = 0; offset < photos.length; offset += perPage) {
        var chunk = photos.slice(offset, offset + perPage);
        var pageNum = photoNumber;
        chunkHtml += '<article class="phr-pdf-page' + (portrait ? ' phr-pdf-portrait' : ' phr-pdf-landscape') + '" data-phr-page="photo">';
        if (options.page.titleEveryPage !== false || options.page.header !== false) {
          chunkHtml += '<div class="phr-pdf-head">';
          if (options.page.logoEveryPage !== false && cover.companyLogo) chunkHtml += '<img class="phr-pdf-head-logo" src="' + esc(cover.companyLogo) + '" alt="" />';
          chunkHtml += '<div class="phr-pdf-head-title">' + esc(report.title || '') + '</div>';
          if (options.page.pagePropertyAddress !== false && cover.propertyAddress) chunkHtml += '<div class="phr-pdf-head-address">' + esc(cover.propertyAddress) + '</div>';
          if (options.page.pageRepresentative !== false && cover.representativeName) chunkHtml += '<div class="phr-pdf-head-rep">Rep: ' + esc(cover.representativeName) + '</div>';
          chunkHtml += '</div>';
        }
        chunkHtml += '<div class="phr-pdf-section-title">' + esc(section.title || ('Section ' + (sectionIndex + 1))) + '</div>';
        chunkHtml += '<div class="phr-pdf-photo-grid phr-pdf-photo-grid-' + perPage + '" style="aspect-ratio:' + (perPage === 1 ? '4/2.4' : perPage === 2 ? '4/4.6' : '4/2.2') + '">';
        chunk.forEach(function (photo) {
          chunkHtml += '<div class="phr-pdf-photo">';
          if (showNum) chunkHtml += '<span class="phr-pdf-photo-num">' + photoNumber + '</span>';
          chunkHtml += '<img data-phr-report-photo="' + esc(photo.id || photo.imageKey || '') + '" alt="' + esc(photo.name || '') + '" />';
          if (showDesc && String(photo.description || '').trim()) {
            chunkHtml += '<div class="phr-pdf-photo-desc">' + esc(photo.description) + '</div>';
          }
          chunkHtml += '</div>';
          photoNumber++;
        });
        chunkHtml += '</div>';
        if (options.page.footer !== false || options.page.pageNumbers !== false) {
          chunkHtml += '<div class="phr-pdf-foot"><span>' + esc(report.title || '') + '</span>' + (options.page.pageNumbers !== false ? '<span>' + (pageNum + 1) + '</span>' : '') + '</div>';
        }
        chunkHtml += '</article>';
      }
      html += chunkHtml;
    });

    return html;
  }

  function hydrateReportThumbnails(container) {
    if (!container) return;
    var project = findProject(rb.projectId);
    if (!project) return;
    Array.prototype.forEach.call(container.querySelectorAll('[data-phr-report-photo]'), function (img) {
      var key = img.getAttribute('data-phr-report-photo');
      var photo = project.photos.find(function (ph) {
        return String(ph.id || ph.imageKey || '') === String(key || '');
      });
      if (!photo) return;
      loadPhotoInto(img, photo, function (fallback) { fallback.style.display = 'none'; });
    });
  }

  /* ── Print to PDF (mirrors existing invoice print pattern) ────────── */
  function printReport() {
    var titleEl = document.getElementById('phr-preview-title');
    if (titleEl) document.title = titleEl.textContent || 'Property Photo Report';
    if (typeof showUploadToast === 'function') {
      showUploadToast('Choose Save as PDF. Turn off Headers and footers in the browser print dialog.');
    }
    setTimeout(function () { window.print(); }, 900);
  }

  /* ════════════════════════════════════════════════════════════════════
     PROJECT SURFACES (reports + chat) INSIDE PHOTO DETAIL
     ════════════════════════════════════════════════════════════════════ */
  window.renderPhotoProjectReports = function (projectId) {
    var el = document.getElementById('pf-reports-surface');
    if (!el) return;
    var p = findProject(projectId);
    if (!p) { el.innerHTML = ''; return; }
    var reports = getReportsForProject(projectId);
    el.innerHTML = reports.length
      ? '<div class="hm-photo-reports-list">' + reports.map(function (report) {
          return '<div class="hm-photo-report-row">' +
            '<div class="hm-photo-report-row-main" data-hm-open-report="' + esc(report.id) + '">' +
              '<span class="hm-photo-report-icon">📄</span>' +
              '<span><strong>' + esc(report.title || 'Property Photo Report') + '</strong>' +
              '<span class="hm-photo-report-date">' + esc(new Date(report.createdAt).toLocaleDateString()) + '</span></span>' +
            '</div>' +
            '<button class="btn" type="button" data-hm-open-report="' + esc(report.id) + '">Open</button>' +
            '<button class="btn" type="button" data-hm-delete-report="' + esc(report.id) + '" title="Delete report">Delete</button>' +
          '</div>';
        }).join('') + '</div>'
      : '<div class="phr-empty-reports">No reports yet. Create your first report from this project.</div>';
    el.querySelectorAll('[data-hm-open-report]').forEach(function (btn) {
      btn.onclick = function () { openReport(projectId, btn.getAttribute('data-hm-open-report')); };
    });
    el.querySelectorAll('[data-hm-delete-report]').forEach(function (btn) {
      btn.onclick = function () {
        if (!confirm('Delete this report? This removes it from the Photos project and the job file.')) return;
        var reportId = btn.getAttribute('data-hm-delete-report');
        var report = getReportsForProject(projectId).find(function (r) { return String(r.id || '') === String(reportId || ''); });
        deleteReportFromProject(projectId, reportId);
        if (report) syncReportRefToJobs(projectId, report, true);
        window.renderPhotoProjectReports(projectId);
        if (typeof showUploadToast === 'function') showUploadToast('Report deleted.');
      };
    });
  };

  window.renderPhotoProjectChat = function (projectId) {
    var el = document.getElementById('pf-chat-surface');
    if (!el) return;
    var p = findProject(projectId);
    if (!p) { el.innerHTML = ''; return; }
    var messages = getChatMessages(projectId);
    var html = '<div class="hm-photo-chat">';
    html += '<div class="hm-photo-chat-head">Photos Chat</div>';
    html += '<div class="hm-photo-chat-list" id="hm-photo-chat-list">';
    if (!messages.length) {
      html += '<div class="hm-photo-chat-empty">No messages yet.</div>';
    } else {
      messages.slice().reverse().forEach(function (msg) {
        html += '<div class="hm-photo-chat-msg"><div class="hm-photo-chat-msg-meta"><span>' + esc(msg.createdByName || 'User') + '</span><span>' + esc((msg.createdAt || '').slice(0, 10)) + '</span></div><div class="hm-photo-chat-msg-body">' + esc(msg.body) + '</div></div>';
      });
    }
    html += '</div>';
    html += '<div class="hm-photo-chat-composer">';
    html += '<input type="text" id="hm-photo-chat-input" placeholder="Message" />';
    html += '<button class="btn btn-primary" type="button" id="hm-photo-chat-send">Send</button>';
    html += '</div></div>';
    el.innerHTML = html;
    var input = document.getElementById('hm-photo-chat-input');
    var send = document.getElementById('hm-photo-chat-send');
    function sendMessage() {
      if (!input || !String(input.value || '').trim()) return;
      saveChatMessage(projectId, input.value);
      input.value = '';
      window.renderPhotoProjectChat(projectId);
      var list = document.getElementById('hm-photo-chat-list');
      if (list) list.scrollTop = list.scrollHeight;
    }
    if (send) send.onclick = sendMessage;
    if (input) input.addEventListener('keydown', function (e) { if (e.key === 'Enter') sendMessage(); });
    var list = document.getElementById('hm-photo-chat-list');
    if (list) list.scrollTop = list.scrollHeight;
  };

  /* ── Job open report override: open the shared photo report preview ── */
  window.hmPhotoOpenSavedReport = function (reportId) {
    var ps = projects();
    for (var i = 0; i < ps.length; i++) {
      var reports = getReportsForProject(ps[i].id);
      var match = reports.find(function (r) { return String(r.id || '') === String(reportId || ''); });
      if (match) {
        openReport(ps[i].id, match.id);
        return true;
      }
    }
    return false;
  };

  /* ════════════════════════════════════════════════════════════════════
     WIRING
     ════════════════════════════════════════════════════════════════════ */
  function wire() {
    document.addEventListener('click', function (e) {
      var reportBtn = e.target && e.target.closest ? e.target.closest('[data-jf-open-photo-report]') : null;
      if (reportBtn) {
        var reportId = reportBtn.getAttribute('data-jf-open-photo-report');
        if (window.hmPhotoOpenSavedReport) {
          var opened = window.hmPhotoOpenSavedReport(reportId);
          if (opened) return;
        }
      }
      var msgClick = e.target && e.target.closest ? e.target.closest('.jf-msg-bubble') : null;
      if (msgClick) {
        var photoMsg = msgClick.querySelector ? msgClick.querySelector('[data-photo-chat-msg]') : null;
        var jfMsgs = typeof jfGetMessages === 'function' ? jfGetMessages() : [];
        var idx = Number(msgClick.getAttribute('data-photo-chat-index') || -1);
        var msg = idx >= 0 ? jfMsgs[idx] : null;
        if (msg) window.hmPhotoReports.openChatFromMessage(msg);
      }
    });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', wire);
  else wire();
})();
