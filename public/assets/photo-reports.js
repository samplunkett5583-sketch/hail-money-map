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
    selectedPhotoIds: [],
    currentReport: null,
    pendingReport: null,
    mode: 'report',
    templateId: '',
    pickerMode: 'section'
  };

  function esc(v) {
    return typeof crmEscapeHtml === 'function'
      ? crmEscapeHtml(String(v == null ? '' : v))
      : String(v == null ? '' : v);
  }

  function genId(prefix) {
    return (prefix || 'id_') + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
  }

  var TEMPLATE_LIBRARY_ID = '__hm_report_templates__';

  function projects() {
    return getPhotoFiles().filter(function (p) {
      return p && p.id !== TEMPLATE_LIBRARY_ID && p.recordType !== 'report_template_library';
    }).map(function (p) {
      p.photos = Array.isArray(p.photos) ? p.photos : [];
      p.reports = Array.isArray(p.reports) ? p.reports : [];
      return p;
    });
  }

  function getTemplates() {
    var rows = getPhotoFiles();
    var library = rows.find(function (p) {
      return p && (p.id === TEMPLATE_LIBRARY_ID || p.recordType === 'report_template_library');
    });
    return library && Array.isArray(library.templates) ? library.templates.slice() : [];
  }

  function saveTemplates(templates) {
    var rows = getPhotoFiles();
    var now = new Date().toISOString();
    var found = false;
    rows = rows.map(function (p) {
      if (!p || (p.id !== TEMPLATE_LIBRARY_ID && p.recordType !== 'report_template_library')) return p;
      found = true;
      return {
        id:TEMPLATE_LIBRARY_ID,
        recordType:'report_template_library',
        projectName:'Report Templates',
        createdAt:p.createdAt || now,
        updatedAt:now,
        photos:[],
        reports:[],
        templates:templates
      };
    });
    if (!found) {
      rows.push({
        id:TEMPLATE_LIBRARY_ID,
        recordType:'report_template_library',
        projectName:'Report Templates',
        createdAt:now,
        updatedAt:now,
        photos:[],
        reports:[],
        templates:templates
      });
    }
    savePhotoFiles(rows);
  }

  function findProject(id) {
    return projects().filter(function (p) { return p.id === id; })[0] || null;
  }

  function saveProjects(ps) {
    var rows = getPhotoFiles();
    var library = rows.find(function (p) {
      return p && (p.id === TEMPLATE_LIBRARY_ID || p.recordType === 'report_template_library');
    });
    var next = Array.isArray(ps) ? ps.slice() : [];
    if (library) next.push(library);
    savePhotoFiles(next);
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
    var rows = Object.keys(byId).map(function (id) { return byId[id]; })
      .sort(function (a, b) {
        return String(b.savedAt || b.updatedAt || b.createdAt || '').localeCompare(String(a.savedAt || a.updatedAt || a.createdAt || ''));
      });
    // Keep only the newest saved report for a given project/title. This also
    // cleans up legacy duplicate-save rows in every report list.
    var seenTitles = {};
    return rows.filter(function (r) {
      var key = String(r && r.title || 'Property Photo Report').trim().toLowerCase();
      if (seenTitles[key]) return false;
      seenTitles[key] = true;
      return true;
    });
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
      var normalizedTitle = String(report.title || 'Property Photo Report').trim().toLowerCase();
      var idx = ps[i].reports.findIndex(function (r) {
        return String(r && r.id || '') === String(report.id || '') ||
          String(r && r.title || 'Property Photo Report').trim().toLowerCase() === normalizedTitle;
      });
      if (idx >= 0) {
        var existing = ps[i].reports[idx] || {};
        if (!report.pdfFileId && existing.pdfFileId) report.pdfFileId = existing.pdfFileId;
        if (!report.pdfStorageKey && existing.pdfStorageKey) report.pdfStorageKey = existing.pdfStorageKey;
        if (!report.createdAt && existing.createdAt) report.createdAt = existing.createdAt;
        ps[i].reports[idx] = report;
        ps[i].reports = ps[i].reports.filter(function (r, reportIndex) {
          if (reportIndex === idx) return true;
          return String(r && r.title || 'Property Photo Report').trim().toLowerCase() !== normalizedTitle;
        });
      } else {
        ps[i].reports.push(report);
      }
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
    if (project.leadId) {
      var linked = leads.find(function (lead) { return String(lead.id || '') === String(project.leadId || ''); });
      if (linked) return linked;
    }
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
      var normalizedTitle = String(report.title || 'Property Photo Report').trim().toLowerCase();
      var idx = jobFile.photoReports.findIndex(function (r) {
        return String(r && r.id || '') === String(report.id || '') ||
          String(r && r.title || 'Property Photo Report').trim().toLowerCase() === normalizedTitle;
      });
      if (remove) {
        if (idx >= 0) jobFile.photoReports.splice(idx, 1);
      } else {
        var ref = {
          id: report.id,
          projectId: projectId,
          title: report.title || 'Property Photo Report',
          type: 'photo_report',
          pdfFileId: report.pdfFileId || '',
          photoCount: (report.sections || []).reduce(function (sum, section) { return sum + ((section && section.photos) ? section.photos.length : 0); }, 0),
          createdAt: report.createdAt || new Date().toISOString(),
          updatedAt: report.updatedAt || report.createdAt || new Date().toISOString()
        };
        if (idx >= 0) jobFile.photoReports[idx] = ref;
        else jobFile.photoReports.push(ref);
        jobFile.photoReports = jobFile.photoReports.filter(function (item, reportIndex) {
          if (reportIndex === (idx >= 0 ? idx : jobFile.photoReports.length - 1)) return true;
          return String(item && item.title || 'Property Photo Report').trim().toLowerCase() !== normalizedTitle;
        });
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

    /* Use the existing Job Details message path so @mentions resolve normally. */
    if (leadId && typeof jfAddMessage === 'function') {
      var jobMessage = jfAddMessage(leadId, body, { id: msg.id, source: 'Photos', photoProjectId: projectId });
      msg.mentionedNames = jobMessage && jobMessage.mentionedNames || [];
      msg.mentionedUserIds = jobMessage && jobMessage.mentionedUserIds || [];
      try { localStorage.setItem(CHAT_KEY, JSON.stringify(all)); } catch (e) {}
      if (lead && typeof crmPushLeadActivity === 'function') {
        crmPushLeadActivity(lead, 'Photos chat: ' + String(body).slice(0, 80), 'note', msg.createdByName, msg.createdAt);
      }
    }
    return msg;
  }

  /* ── Photo source rendering (real thumbnails) ─────────────────────── */
  function photoSrc(photo, cb) {
    photo = photo || {};
    var finished = false;
    function done(src) {
      if (finished) return;
      finished = true;
      cb(src || '');
    }
    function tryRemotePath() {
      if (photo.fullUrl) { done(photo.fullUrl); return; }
      if (typeof window.hmPhotoSignedUrl === 'function' && photo.storagePath) {
        window.hmPhotoSignedUrl(photo.storagePath).then(function (src) {
          if (src) done(src);
          else done('');
        }).catch(function () { done(''); });
        return;
      }
      done('');
    }
    function tryCloudFile() {
      var cloudId = String(photo.fileId || photo.id || '').trim();
      if (!cloudId && String(photo.storageKey || '').indexOf('neon:') === 0) cloudId = String(photo.storageKey).slice(5);
      if (cloudId && typeof window.hmCloudGetFileBlobById === 'function') {
        window.hmCloudGetFileBlobById(cloudId).then(function (blob) {
          if (blob) { done(URL.createObjectURL(blob)); return; }
          tryRemotePath();
        }).catch(tryRemotePath);
        return;
      }
      tryRemotePath();
    }
    if (photo.imageKey && typeof getPhotoBlob === 'function') {
      getPhotoBlob(photo.imageKey).then(function (blob) {
        if (blob) { done(URL.createObjectURL(blob)); return; }
        tryCloudFile();
      }).catch(tryCloudFile);
      return;
    }
    if (photo.storageKey && typeof crmOpenFilesDb === 'function' && typeof crmDbGetBlob === 'function') {
      crmOpenFilesDb(function (dbErr, db) {
        if (dbErr || !db) { tryCloudFile(); return; }
        crmDbGetBlob(db, photo.storageKey, function (blobErr, blob) {
          if (!blobErr && blob) { done(URL.createObjectURL(blob)); return; }
          tryCloudFile();
        });
      });
      return;
    }
    tryCloudFile();
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
    for (var linkedIndex = 0; linkedIndex < ps.length; linkedIndex++) {
      if (String(ps[linkedIndex].leadId || '') === String(leadId || '')) return ps[linkedIndex].id;
    }
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
    getAllReports: getAllReports,
    getTemplates: getTemplates,
    openTemplateBuilder: openTemplateBuilder,
    openBuilderFromTemplate: openBuilderFromTemplate,
    openChatFromMessage: openChatFromMessage,
    printReport: printReport,
    confirmPhotoPicker: confirmPhotoPicker,
    cancelPhotoPicker: cancelPhotoPicker
  };

  function getAllReports() {
    var out = [];
    projects().forEach(function (project) {
      getReportsForProject(project.id).forEach(function (report) {
        if (!report || !report.id || !Array.isArray(report.sections)) return;
        out.push({ project:project, report:report });
      });
    });
    return out.sort(function (a, b) {
      return String((b.report && (b.report.savedAt || b.report.updatedAt || b.report.createdAt)) || '')
        .localeCompare(String((a.report && (a.report.savedAt || a.report.updatedAt || a.report.createdAt)) || ''));
    });
  }

  function cloneTemplateDraft(template) {
    var source = template && template.draft ? template.draft : {};
    var next = JSON.parse(JSON.stringify(source || {}));
    next.title = String(next.title || template && template.name || '').trim();
    next.sections = Array.isArray(next.sections) && next.sections.length
      ? next.sections.map(function (section, index) {
          return {
            id:genId('sec_'),
            title:String(section && section.title || ('Section ' + (index + 1))),
            photos:[]
          };
        })
      : [{ id:genId('sec_'), title:'Section 1', photos:[] }];
    return next;
  }

  function openBuilder(projectId, template) {
    var p = findProject(projectId);
    if (!p) return;
    rb.projectId = projectId;
    rb.mode = 'report';
    rb.templateId = template && template.id ? String(template.id) : '';
    rb.step = 'title';
    rb.currentReport = null;
    rb.pendingReport = null;
    rb.draft = template ? cloneTemplateDraft(template) : {
      title: '',
      sections: [{ id: genId('sec_'), title: 'Section 1', photos: [] }]
    };
    showPage('page-photo-report-builder');
    renderBuilder();
  }

  function openBuilderFromTemplate(projectId, templateId) {
    var template = getTemplates().find(function (item) {
      return String(item && item.id || '') === String(templateId || '');
    });
    if (!template) {
      if (typeof showUploadToast === 'function') showUploadToast('That report template could not be found.');
      return;
    }
    openBuilder(projectId, template);
  }

  function openTemplateBuilder() {
    rb.projectId = '';
    rb.mode = 'template';
    rb.templateId = '';
    rb.step = 'title';
    rb.currentReport = null;
    rb.pendingReport = null;
    rb.draft = {
      title:'',
      sections:[{ id:genId('sec_'), title:'Section 1', photos:[] }]
    };
    showPage('page-photo-report-builder');
    renderBuilder();
  }

  function renderBuilder() {
    var page = document.getElementById('page-photo-report-builder');
    if (!page) return;
    var content = document.getElementById('phr-content');
    if (!content) return;
    var p = rb.mode === 'template'
      ? { id:'', projectName:'Create Report Template', homeownerName:'', photos:[] }
      : findProject(rb.projectId);
    if (!p) { content.innerHTML = '<div class="crm-empty-state">Photo project not found.</div>'; return; }

    var html = '<div class="phr-toolbar"><button class="btn" type="button" id="phr-back">Back</button>' +
      '<div class="phr-toolbar-title">' + esc(rb.mode === 'template' ? 'Create Report Template' : (p.projectName || p.homeownerName || 'Photo Project')) + '</div>' +
      '<span class="phr-step-indicator">Step ' + (rb.step === 'title' ? '1' : rb.step === 'sections' ? '2' : '3') + ' of 3</span></div>';

    if (rb.step === 'title') {
      html += '<div class="phr-card phr-step-title">';
      html += '<div class="phr-step-label">Step 1</div>';
      html += '<div class="phr-step-heading">' + (rb.mode === 'template' ? 'Template Name' : 'Report Title') + '</div>';
      html += '<input type="text" id="phr-title-input" class="phr-title-input" placeholder="' + (rb.mode === 'template' ? 'Template name' : 'Report title') + '" value="' + esc(rb.draft.title) + '" />';
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
    html += '<div class="phr-section-head" data-phr-section-drag-handle="' + esc(section.id) + '" title="Drag to reorder this section">';
    html += '<div class="phr-section-drag-grip" aria-hidden="true">&#8942;&#8942;</div>';
    html += '<div class="phr-section-title-row">';
    html += '<span class="phr-section-number">Section ' + (index + 1) + '</span>';
    html += '<input type="text" class="phr-section-title-input" value="' + esc(section.title) + '" data-phr-section-title="' + esc(section.id) + '" placeholder="Section title" />';
    html += '</div>';
    html += '<div class="phr-section-tools">';
    html += '<span class="phr-section-drag-copy">Drag section to reorder</span>';
    html += '<button class="btn" type="button" data-phr-rename="' + esc(section.id) + '">Rename</button>';
    html += '<button class="btn" type="button" data-phr-delete-section="' + esc(section.id) + '">Delete</button>';
    html += '</div>';
    html += '</div>';
    if (rb.mode === 'template') {
      html += '<div class="phr-empty-photos">Template sections save the layout only. Photos are added when the template is used for a report.</div>';
    } else {
      html += '<div class="phr-photo-grid" data-phr-photo-grid="' + esc(section.id) + '">';
      if (!section.photos.length) {
        html += '<div class="phr-empty-photos">No photos in this section yet.</div>';
      }
      section.photos.forEach(function (photo, photoIndex) {
        html += '<div class="phr-photo" data-phr-photo="' + esc(photo.id || photo.fileId || photo.imageKey || photoIndex) + '" data-phr-photo-section="' + esc(section.id) + '">';
        html += '<div class="phr-photo-thumb"><img data-phr-photo-img="' + esc(photo.id || photo.fileId || photo.imageKey || photoIndex) + '" alt="" /></div>';
        html += '<input type="text" class="phr-photo-desc" placeholder="Description (optional)" value="' + esc(photo.description || photo.caption || photo.note || '') + '" data-phr-photo-desc="' + esc(photo.id || photo.fileId || photo.imageKey || photoIndex) + '" />';
        html += '<div class="phr-photo-tools">';
        html += '<button class="btn" type="button" data-phr-photo-move="up" data-phr-photo-section="' + esc(section.id) + '" data-phr-photo-key="' + esc(photo.id || photo.fileId || photo.imageKey || photoIndex) + '">&#8593;</button>';
        html += '<button class="btn" type="button" data-phr-photo-move="down" data-phr-photo-section="' + esc(section.id) + '" data-phr-photo-key="' + esc(photo.id || photo.fileId || photo.imageKey || photoIndex) + '">&#8595;</button>';
        html += '<button class="btn" type="button" data-phr-photo-remove="' + esc(photo.id || photo.fileId || photo.imageKey || photoIndex) + '" data-phr-photo-section="' + esc(section.id) + '">Remove</button>';
        html += '</div></div>';
      });
      html += '</div>';
      html += '<button class="btn btn-primary" type="button" data-phr-add-photos="' + esc(section.id) + '">Add Photos</button>';
    }
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
    if (rb.mode !== 'template') {
      html += '<div class="phr-cover-photo-option">';
      html += '<div><strong>Cover Photo</strong><div class="phr-cover-photo-help">Choose one project photo for the upper half of the cover page.</div></div>';
      html += '<div class="phr-cover-photo-choice">';
      if (rb.draft.coverPhoto) html += '<img id="phr-cover-photo-preview" alt="Selected cover photo" />';
      html += '<div class="phr-cover-photo-buttons"><button class="btn" type="button" id="phr-cover-photo-choose">' + (rb.draft.coverPhoto ? 'Change Cover Photo' : 'Choose Cover Photo') + '</button>';
      if (rb.draft.coverPhoto) html += '<button class="btn" type="button" id="phr-cover-photo-clear">Remove</button>';
      html += '</div></div></div>';
    }
    html += '<div class="phr-custom-notes-row">';
    html += '<label>Custom notes (cover page)</label>';
    html += '<textarea id="phr-custom-notes" class="phr-custom-notes">' + esc(rb.draft.customNotes || '') + '</textarea>';
    html += '</div>';
    html += '<div class="phr-actions"><button class="btn" type="button" id="phr-back-to-sections">Back</button><button class="btn btn-primary" type="button" id="phr-create-pdf">' + (rb.mode === 'template' ? 'Save Template' : 'Create Report') + '</button></div>';
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
        if (rb.mode === 'template') {
          showPage('page-photo-files');
          if (typeof window.hmProjectsShowView === 'function') window.hmProjectsShowView('create');
        } else if (typeof window.openPhotoFileDetail === 'function') {
          window.openPhotoFileDetail(rb.projectId);
        } else {
          showPage('page-photo-file-detail');
        }
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
    content.oninput = function (e) {
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
    };
    content.onchange = function (e) {
      var option = e.target.closest('[data-phr-option]');
      if (option) rb.draft[option.getAttribute('data-phr-option')] = option.checked;
      var notes = document.getElementById('phr-custom-notes');
      if (notes && e.target === notes) rb.draft.customNotes = notes.value;
    };

    content.onclick = function (e) {
      var coverPhotoChoose = e.target.closest('#phr-cover-photo-choose');
      if (coverPhotoChoose) { openPhotoPicker('', 'cover'); return; }
      var coverPhotoClear = e.target.closest('#phr-cover-photo-clear');
      if (coverPhotoClear) { rb.draft.coverPhoto = null; renderBuilder(); return; }
      var addPhotosBtn = e.target.closest('[data-phr-add-photos]');
      if (addPhotosBtn) { openPhotoPicker(addPhotosBtn.getAttribute('data-phr-add-photos'), 'section'); return; }
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
    };

    wireSectionReorder(content);

    // Hydrate thumbnails after each render.
    hydrateBuilderThumbnails(content);
    var coverPreview = document.getElementById('phr-cover-photo-preview');
    if (coverPreview && rb.draft.coverPhoto) {
      loadPhotoInto(coverPreview, rb.draft.coverPhoto, function (fallback) { fallback.style.display = 'none'; });
    }
  }

  function syncDraftSectionOrderFromDom(host) {
    if (!host) return;
    var byId = {};
    rb.draft.sections.forEach(function (section) { byId[String(section.id)] = section; });
    var next = [];
    Array.prototype.forEach.call(host.querySelectorAll(':scope > [data-phr-section]'), function (card) {
      var section = byId[String(card.getAttribute('data-phr-section') || '')];
      if (section) next.push(section);
    });
    if (next.length === rb.draft.sections.length) rb.draft.sections = next;
  }

  function wireSectionReorder(content) {
    var host = content && content.querySelector('#phr-sections');
    if (!host) return;

    Array.prototype.forEach.call(host.querySelectorAll('[data-phr-section-drag-handle]'), function (handle) {
      var dragState = null;

      function finishDrag(e) {
        if (!dragState) return;
        if (e && e.pointerId != null && dragState.pointerId !== e.pointerId) return;
        window.removeEventListener('pointermove', moveDrag, false);
        window.removeEventListener('pointerup', finishDrag, false);
        window.removeEventListener('pointercancel', finishDrag, false);
        try {
          if (handle.hasPointerCapture && handle.hasPointerCapture(dragState.pointerId)) {
            handle.releasePointerCapture(dragState.pointerId);
          }
        } catch (_) {}

        var card = dragState.card;
        var placeholder = dragState.placeholder;
        card.classList.remove('phr-section-dragging');
        card.style.left = '';
        card.style.top = '';
        card.style.width = '';
        card.style.height = '';

        if (placeholder && placeholder.parentNode === host) {
          host.insertBefore(card, placeholder);
          placeholder.remove();
        } else {
          host.appendChild(card);
        }

        syncDraftSectionOrderFromDom(host);
        dragState = null;
        renderBuilder();
      }

      handle.addEventListener('pointerdown', function (e) {
        if (e.button != null && e.button !== 0) return;
        if (e.target && e.target.closest && e.target.closest('input,button,textarea,select,a')) return;
        var card = handle.closest('[data-phr-section]');
        if (!card) return;

        var rect = card.getBoundingClientRect();
        var placeholder = document.createElement('div');
        placeholder.className = 'phr-section-drag-placeholder';
        placeholder.style.height = Math.max(70, rect.height) + 'px';
        host.insertBefore(placeholder, card);

        dragState = {
          card: card,
          placeholder: placeholder,
          pointerId: e.pointerId,
          startY: e.clientY,
          offsetY: e.clientY - rect.top,
          moved: false
        };

        try { handle.setPointerCapture(e.pointerId); } catch (_) {}

        document.body.appendChild(card);
        card.style.left = rect.left + 'px';
        card.style.top = rect.top + 'px';
        card.style.width = rect.width + 'px';
        card.style.height = rect.height + 'px';
        card.classList.add('phr-section-dragging');

        // Track at the window level while the card is floating so movement
        // remains continuous even if pointer capture is interrupted by reparenting.
        window.addEventListener('pointermove', moveDrag, { passive:false });
        window.addEventListener('pointerup', finishDrag, false);
        window.addEventListener('pointercancel', finishDrag, false);
        try { handle.setPointerCapture(e.pointerId); } catch (_) {}
        e.preventDefault();
      });

      function moveDrag(e) {
        if (!dragState || e.pointerId !== dragState.pointerId) return;

        // Make the grabbed section visibly follow the pointer immediately.
        dragState.card.style.top = (e.clientY - dragState.offsetY) + 'px';

        if (!dragState.moved && Math.abs(e.clientY - dragState.startY) < 5) {
          e.preventDefault();
          return;
        }
        dragState.moved = true;

        var cards = Array.prototype.slice.call(
          host.querySelectorAll(':scope > .phr-section[data-phr-section]')
        );

        var beforeCard = null;
        for (var i = 0; i < cards.length; i++) {
          var rect = cards[i].getBoundingClientRect();
          if (e.clientY < rect.top + rect.height / 2) {
            beforeCard = cards[i];
            break;
          }
        }

        if (beforeCard) {
          if (dragState.placeholder.nextElementSibling !== beforeCard) {
            host.insertBefore(dragState.placeholder, beforeCard);
          }
        } else if (host.lastElementChild !== dragState.placeholder) {
          host.appendChild(dragState.placeholder);
        }

        // Keep long section lists usable on touch devices by nudging the page
        // when the pointer approaches the viewport edge.
        var edge = Math.min(90, Math.max(50, window.innerHeight * 0.12));
        if (e.clientY < edge) {
          window.scrollBy(0, -Math.max(8, Math.round((edge - e.clientY) * 0.22)));
        } else if (e.clientY > window.innerHeight - edge) {
          window.scrollBy(0, Math.max(8, Math.round((e.clientY - (window.innerHeight - edge)) * 0.22)));
        }

        e.preventDefault();
      }
    });
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
  function pickerPhotoKey(photo, index) {
    var stable = photo && (photo.id || photo.fileId || photo.imageKey || photo.storageKey);
    return String(stable || (index != null ? index : ''));
  }

  function pickerCategoryRank(name) {
    name = String(name || 'Inspection').trim();
    var exact = {
      'Front Elevation': 10,
      'Right Elevation': 20,
      'Rear Elevation': 30,
      'Left Elevation': 40,
      'Off the ladder pictures': 50,
      'Front Slope': 70,
      'Back Slope': 80
    };
    if (exact[name] != null) return exact[name];
    if (/^Roof Projections:/i.test(name)) return 60;
    if (/^Slope:/i.test(name)) return 90;
    return 100;
  }

  function openPhotoPicker(sectionId, pickerMode) {
    var p = findProject(rb.projectId);
    if (!p) return;
    rb.pickerMode = pickerMode === 'cover' ? 'cover' : 'section';
    rb.pickerSectionIndex = rb.pickerMode === 'section'
      ? rb.draft.sections.findIndex(function (s) { return s.id === sectionId; })
      : -1;
    if (rb.pickerMode === 'section' && rb.pickerSectionIndex < 0) return;

    var overlay = document.getElementById('phr-picker-overlay');
    var grid = document.getElementById('phr-picker-grid');
    var title = document.getElementById('phr-picker-title');
    if (!overlay || !grid) return;

    var section = rb.pickerMode === 'section' ? rb.draft.sections[rb.pickerSectionIndex] : null;
    var selectedIds = {};
    if (rb.pickerMode === 'cover') {
      if (rb.draft.coverPhoto) selectedIds[pickerPhotoKey(rb.draft.coverPhoto, 0)] = true;
    } else {
      (section.photos || []).forEach(function (photo, index) {
        selectedIds[pickerPhotoKey(photo, index)] = true;
      });
    }
    rb.selectedPhotoIds = Object.keys(selectedIds);

    title.textContent = rb.pickerMode === 'cover'
      ? 'Choose Cover Photo'
      : 'Select Photos for ' + (section.title || 'Section');

    if (!p.photos.length) {
      grid.innerHTML = '<div class="crm-empty-state">No photos in this project yet. Add photos first.</div>';
    } else {
      var grouped = {};
      p.photos.forEach(function (photo, index) {
        var category = String(photo.category || 'Inspection').trim() || 'Inspection';
        if (!grouped[category]) grouped[category] = [];
        grouped[category].push({ photo: photo, index: index });
      });
      var categories = Object.keys(grouped).sort(function (a, b) {
        var rankDiff = pickerCategoryRank(a) - pickerCategoryRank(b);
        return rankDiff || a.localeCompare(b);
      });

      grid.innerHTML = categories.map(function (category) {
        var cards = grouped[category].map(function (item) {
          var photo = item.photo;
          var key = pickerPhotoKey(photo, item.index);
          var selected = !!selectedIds[key];
          var name = String(photo.name || photo.fileName || ('Photo ' + (item.index + 1)));
          return '<button type="button" class="phr-picker-photo' + (selected ? ' selected' : '') + '" data-phr-picker-key="' + esc(key) + '">' +
            '<span class="phr-picker-thumb"><img data-phr-picker-img="' + esc(key) + '" alt="' + esc(name) + '" /></span>' +
            '<span class="phr-picker-name">' + esc(name) + '</span>' +
            '<span class="phr-picker-check">' + (selected ? '&#10003;' : '') + '</span>' +
            '</button>';
        }).join('');
        return '<section class="phr-picker-section">' +
          '<h3 class="phr-picker-section-title">' + esc(category) + '</h3>' +
          '<div class="phr-picker-section-grid">' + cards + '</div>' +
          '</section>';
      }).join('');
    }

    overlay.hidden = false;

    Array.prototype.forEach.call(grid.querySelectorAll('[data-phr-picker-key]'), function (btn) {
      btn.onclick = function () {
        var key = btn.getAttribute('data-phr-picker-key');
        if (rb.pickerMode === 'cover') {
          rb.selectedPhotoIds = [key];
          Array.prototype.forEach.call(grid.querySelectorAll('[data-phr-picker-key]'), function (other) {
            var selected = other.getAttribute('data-phr-picker-key') === key;
            other.classList.toggle('selected', selected);
            other.querySelector('.phr-picker-check').innerHTML = selected ? '&#10003;' : '';
          });
          return;
        }
        var wasSelected = btn.classList.contains('selected');
        btn.classList.toggle('selected', !wasSelected);
        btn.querySelector('.phr-picker-check').innerHTML = !wasSelected ? '&#10003;' : '';
        if (!wasSelected) {
          if (rb.selectedPhotoIds.indexOf(key) === -1) rb.selectedPhotoIds.push(key);
        } else {
          rb.selectedPhotoIds = rb.selectedPhotoIds.filter(function (k) { return k !== key; });
        }
      };
    });

    Array.prototype.forEach.call(grid.querySelectorAll('[data-phr-picker-img]'), function (img) {
      var key = img.getAttribute('data-phr-picker-img');
      var photo = p.photos.find(function (ph, index) { return pickerPhotoKey(ph, index) === key; });
      if (photo) loadPhotoInto(img, photo, function (f) { f.style.background = '#eef1f5'; });
    });
  }

  function confirmPhotoPicker() {
    var overlay = document.getElementById('phr-picker-overlay');
    if (!overlay) return;
    var p = findProject(rb.projectId);
    if (!p) return;

    if (rb.pickerMode === 'cover') {
      var coverKey = rb.selectedPhotoIds[0] || '';
      var coverPhoto = p.photos.find(function (ph, index) { return pickerPhotoKey(ph, index) === coverKey; });
      rb.draft.coverPhoto = coverPhoto ? JSON.parse(JSON.stringify(coverPhoto)) : null;
      overlay.hidden = true;
      renderBuilder();
      return;
    }

    var section = rb.draft.sections[rb.pickerSectionIndex];
    var existing = {};
    (section.photos || []).forEach(function (photo, index) {
      existing[pickerPhotoKey(photo, index)] = photo;
    });

    var chosen = [];
    rb.selectedPhotoIds.forEach(function (key) {
      var photo = p.photos.find(function (ph, index) { return pickerPhotoKey(ph, index) === key; });
      if (!photo) return;
      var copy = JSON.parse(JSON.stringify(photo));
      copy.description = (existing[key] || {}).description || '';
      chosen.push(copy);
    });

    section.photos = chosen;
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
      coverPhoto: draft.coverPhoto ? {
        id: draft.coverPhoto.id || draft.coverPhoto.fileId || draft.coverPhoto.imageKey || '',
        fileId: draft.coverPhoto.fileId || draft.coverPhoto.id || '',
        imageKey: draft.coverPhoto.imageKey || '',
        storageKey: draft.coverPhoto.storageKey || '',
        storagePath: draft.coverPhoto.storagePath || '',
        name: draft.coverPhoto.name || ''
      } : null,
      sections: draft.sections.map(function (section) {
        return {
          id: section.id,
          title: String(section.title || 'Section').trim(),
          photos: section.photos.map(function (photo) {
            return {
              id: photo.id || photo.fileId || photo.imageKey || '',
              fileId: photo.fileId || photo.id || '',
              imageKey: photo.imageKey || '',
              storageKey: photo.storageKey || '',
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
    if (rb.mode === 'template') {
      var templateName = String(rb.draft && rb.draft.title || '').trim();
      if (!templateName) {
        if (typeof showUploadToast === 'function') showUploadToast('Enter a template name.');
        rb.step = 'title';
        renderBuilder();
        return;
      }
      var templates = getTemplates();
      var now = new Date().toISOString();
      var template = {
        id:'report_template_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6),
        name:templateName,
        createdAt:now,
        updatedAt:now,
        createdBy:getUserName() || 'User',
        draft:JSON.parse(JSON.stringify(rb.draft))
      };
      template.draft.sections = (template.draft.sections || []).map(function (section, index) {
        return {
          id:'template_section_' + (index + 1),
          title:String(section && section.title || ('Section ' + (index + 1))),
          photos:[]
        };
      });
      templates.push(template);
      saveTemplates(templates);
      if (typeof showUploadToast === 'function') showUploadToast('Report template saved for the company.');
      showPage('page-photo-files');
      if (typeof window.hmProjectsShowView === 'function') window.hmProjectsShowView('create');
      window.dispatchEvent(new CustomEvent('hmreporttemplateschanged'));
      return;
    }

    if (!rb.draft.title.trim()) {
      var lead = findLeadByProject(rb.projectId);
      rb.draft.title = String((lead ? crmGetJobFileLeadName(lead) : '') || 'Property Photo Report').trim();
    }
    var report = buildReportObject();
    rb.pendingReport = report;
    rb.currentReport = report;
    renderReportPage(report);
    showPage('page-photo-report-preview');
    if (typeof showUploadToast === 'function') showUploadToast('Report ready. Save as PDF to add it to company Reports.');
  }

  function openReport(projectId, reportId) {
    var p = findProject(projectId);
    if (!p) return;
    var reports = getReportsForProject(projectId);
    var report = reports.find(function (r) { return String(r.id || '') === String(reportId || ''); }) || reports[0];
    if (!report) { if (typeof showUploadToast === 'function') showUploadToast('No report found for this project.'); return; }
    rb.projectId = projectId;
    rb.currentReport = report;
    rb.pendingReport = null;
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
    options.cover = options.cover || {};
    options.page = options.page || {};
    options.layout = options.layout || {};
    var cover = report.cover || {};
    var layout = Object.assign({ perPage: 4, portrait: true, preserveAspectRatio: true, showDescriptions: true, photoNumbering: true }, options.layout);
    var perPage = [1, 2, 4].indexOf(Number(layout.perPage)) !== -1 ? Number(layout.perPage) : 4;
    var portrait = layout.portrait !== false;
    var showDesc = layout.showDescriptions !== false;
    var showNum = layout.photoNumbering !== false;

    /* Cover page */
    html += '<article class="phr-pdf-page phr-cover-page" data-phr-page="cover">';
    html += '<div class="phr-cover-inner">';
    html += '<div class="phr-cover-upper">';
    if (options.cover.companyLogo !== false && cover.companyLogo) {
      html += '<img class="phr-cover-logo" src="' + esc(cover.companyLogo) + '" alt="Company logo" />';
    }
    if (options.cover.companyName !== false && cover.companyName) {
      html += '<div class="phr-cover-company">' + esc(cover.companyName) + '</div>';
    }
    var companyLine = [options.cover.companyAddress !== false ? cover.companyAddress : '', options.cover.companyPhone !== false ? cover.companyPhone : '', options.cover.companyEmail !== false ? cover.companyEmail : ''].filter(Boolean).join(' · ');
    if (companyLine) html += '<div class="phr-cover-company-info">' + esc(companyLine) + '</div>';
    if (report.coverPhoto) {
      html += '<div class="phr-cover-photo-wrap"><img class="phr-cover-photo" data-phr-report-photo="' + esc(report.coverPhoto.id || report.coverPhoto.fileId || report.coverPhoto.imageKey || '') + '" alt="Cover photo" /></div>';
    } else {
      html += '<div class="phr-cover-photo-wrap phr-cover-photo-empty"></div>';
    }
    html += '</div>';
    html += '<div class="phr-cover-lower">';
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
    html += '</div></div></div></article>';

    /* Section pages */
    var photoNumber = 1;
    (report.sections || []).forEach(function (section, sectionIndex) {
      var photos = (section.photos || []).filter(function (ph) { return ph.id || ph.fileId || ph.storageKey || ph.storagePath || ph.imageKey || ph.fullUrl; });
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
          chunkHtml += '<img data-phr-report-photo="' + esc(photo.id || photo.fileId || photo.imageKey || '') + '" alt="' + esc(photo.name || '') + '" />';
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
        return String(ph.id || ph.fileId || ph.imageKey || '') === String(key || '');
      });
      if (!photo) return;
      loadPhotoInto(img, photo, function (fallback) { fallback.style.display = 'none'; });
    });
  }

  /* ── Direct PDF download ─────────────────────────────────────────── */
  function safePdfFileName(value) {
    return String(value || 'Property Photo Report')
      .replace(/[<>:"/\\|?*\x00-\x1f]/g, '-')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 120) || 'Property Photo Report';
  }

  function sourceToDataUrl(src) {
    return new Promise(function (resolve) {
      if (!src) { resolve(''); return; }
      if (/^data:/i.test(src)) { resolve(src); return; }
      fetch(src).then(function (res) {
        if (!res.ok) throw new Error('Image request failed');
        return res.blob();
      }).then(function (blob) {
        var reader = new FileReader();
        reader.onload = function () { resolve(String(reader.result || '')); };
        reader.onerror = function () { resolve(''); };
        reader.readAsDataURL(blob);
      }).catch(function () { resolve(''); });
    });
  }

  function compressImageForPdf(dataUrl) {
    return new Promise(function (resolve) {
      if (!dataUrl) { resolve(''); return; }
      var img = new Image();
      img.onload = function () {
        try {
          var maxSide = 1600;
          var scale = Math.min(1, maxSide / Math.max(img.naturalWidth || img.width || maxSide, img.naturalHeight || img.height || maxSide));
          var width = Math.max(1, Math.round((img.naturalWidth || img.width || maxSide) * scale));
          var height = Math.max(1, Math.round((img.naturalHeight || img.height || maxSide) * scale));
          var canvas = document.createElement('canvas');
          canvas.width = width;
          canvas.height = height;
          var ctx = canvas.getContext('2d');
          ctx.fillStyle = '#ffffff';
          ctx.fillRect(0, 0, width, height);
          ctx.drawImage(img, 0, 0, width, height);
          resolve(canvas.toDataURL('image/jpeg', 0.82));
        } catch (_) {
          resolve(dataUrl);
        }
      };
      img.onerror = function () { resolve(dataUrl); };
      img.src = dataUrl;
    });
  }

  function photoToDataUrl(photo) {
    return new Promise(function (resolve) {
      photoSrc(photo || {}, function (src) {
        sourceToDataUrl(src)
          .then(compressImageForPdf)
          .then(resolve)
          .catch(function () { resolve(''); });
      });
    });
  }

  function fitImageRect(doc, dataUrl, x, y, boxW, boxH) {
    try {
      var props = doc.getImageProperties(dataUrl);
      var iw = Number(props && props.width) || boxW;
      var ih = Number(props && props.height) || boxH;
      var scale = Math.min(boxW / iw, boxH / ih);
      var w = iw * scale;
      var h = ih * scale;
      return { x:x + (boxW - w) / 2, y:y + (boxH - h) / 2, w:w, h:h };
    } catch (_) {
      return { x:x, y:y, w:boxW, h:boxH };
    }
  }

  function pdfText(doc, value, x, y, maxWidth, options) {
    var text = String(value || '').trim();
    if (!text) return y;
    options = options || {};
    doc.setFont(options.bold ? 'helvetica' : 'helvetica', options.bold ? 'bold' : 'normal');
    doc.setFontSize(options.size || 10);
    var lines = maxWidth ? doc.splitTextToSize(text, maxWidth) : [text];
    doc.text(lines, x, y, options.align ? { align: options.align } : undefined);
    return y + (lines.length * ((options.size || 10) * 1.2));
  }

  async function printReport() {
    var report = rb.currentReport;
    if (!report) {
      if (typeof showUploadToast === 'function') showUploadToast('Open a report before saving the PDF.');
      return;
    }
    if (!window.jspdf || !window.jspdf.jsPDF) {
      if (typeof showUploadToast === 'function') showUploadToast('PDF library is unavailable. Reload the app and try again.');
      return;
    }

    var button = document.getElementById('phr-preview-print');
    var oldText = button ? button.textContent : '';
    if (button) {
      button.disabled = true;
      button.textContent = 'Creating PDF…';
    }
    if (typeof showUploadToast === 'function') showUploadToast('Creating PDF…');

    try {
      var options = report.options || {};
      var cover = report.cover || {};
      var layout = options.layout || {};
      var perPage = [1,2,4].indexOf(Number(layout.perPage)) !== -1 ? Number(layout.perPage) : 4;
      var portrait = layout.portrait !== false;
      var showDesc = layout.showDescriptions !== false;
      var showNum = layout.photoNumbering !== false;

      var jsPDF = window.jspdf.jsPDF;
      var doc = new jsPDF({ unit:'pt', format:'letter', orientation:'portrait', compress:true });

      function pageWidth() { return doc.internal.pageSize.getWidth(); }
      function pageHeight() { return doc.internal.pageSize.getHeight(); }
      function addPage(orientation) {
        doc.addPage('letter', orientation === 'landscape' ? 'landscape' : 'portrait');
      }
      function rule(y) {
        doc.setDrawColor(184,135,34);
        doc.setLineWidth(1.2);
        doc.line(42, y, pageWidth() - 42, y);
      }

      // Cover page: branding/photo in the upper half, report details centered
      // in the lower half.
      var y = 38;
      if (options.cover && options.cover.companyLogo !== false && cover.companyLogo) {
        var logoData = await sourceToDataUrl(cover.companyLogo);
        if (logoData) {
          var logoRect = fitImageRect(doc, logoData, (pageWidth() - 130) / 2, y, 130, 48);
          try { doc.addImage(logoData, logoRect.x, logoRect.y, logoRect.w, logoRect.h, undefined, 'FAST'); } catch (_) {}
          y += 58;
        }
      }
      doc.setTextColor(31,41,55);
      if (!options.cover || options.cover.companyName !== false) {
        doc.setFont('helvetica','bold');
        doc.setFontSize(16);
        doc.text(String(cover.companyName || ''), pageWidth()/2, y, { align:'center' });
        if (cover.companyName) y += 19;
      }
      var companyLine = [
        !options.cover || options.cover.companyAddress !== false ? cover.companyAddress : '',
        !options.cover || options.cover.companyPhone !== false ? cover.companyPhone : '',
        !options.cover || options.cover.companyEmail !== false ? cover.companyEmail : ''
      ].filter(Boolean).join('  •  ');
      if (companyLine) {
        doc.setFont('helvetica','normal');
        doc.setFontSize(8);
        doc.text(doc.splitTextToSize(companyLine, 480), pageWidth()/2, y, { align:'center' });
        y += 18;
      }

      if (report.coverPhoto) {
        var coverPhotoData = await photoToDataUrl(report.coverPhoto);
        if (coverPhotoData) {
          var coverBoxY = Math.max(92, y + 8);
          var coverRect = fitImageRect(doc, coverPhotoData, 68, coverBoxY, pageWidth() - 136, 245);
          try { doc.addImage(coverPhotoData, coverRect.x, coverRect.y, coverRect.w, coverRect.h, undefined, 'FAST'); } catch (_) {}
        }
      }

      y = Math.max(pageHeight() * 0.56, y + 285);
      doc.setFont('helvetica','bold');
      doc.setFontSize(24);
      doc.text(String((!options.cover || options.cover.reportTitle !== false) ? (report.title || 'Property Photo Report') : ''), pageWidth()/2, y, { align:'center' });
      y += 18;
      rule(y);
      y += 28;

      var coverRows = [
        ['Homeowner', cover.homeowner, !options.cover || options.cover.homeowner !== false],
        ['Property Address', cover.propertyAddress, !options.cover || options.cover.propertyAddress !== false],
        ['Inspection Date', cover.inspectionDate, !options.cover || options.cover.inspectionDate !== false],
        ['Report Date', cover.reportDate, !options.cover || options.cover.reportDate !== false],
        ['Representative', cover.representativeName, !options.cover || options.cover.representativeName !== false],
        ['Representative Phone', cover.representativePhone, !options.cover || options.cover.representativePhone !== false],
        ['Representative Email', cover.representativeEmail, !options.cover || options.cover.representativeEmail !== false],
        ['Claim Number', cover.claimNumber, !options.cover || options.cover.claimNumber !== false],
        ['Carrier', cover.carrier, !options.cover || options.cover.carrier !== false],
        ['Policy Number', cover.policyNumber, !options.cover || options.cover.policyNumber !== false],
        ['Adjuster', cover.adjuster, !options.cover || options.cover.adjuster !== false]
      ];
      coverRows.forEach(function (row) {
        if (!row[2] || !String(row[1] || '').trim()) return;
        doc.setFont('helvetica','bold');
        doc.setFontSize(9);
        doc.setTextColor(138,109,34);
        doc.text(String(row[0]).toUpperCase(), 74, y);
        doc.setFont('helvetica','normal');
        doc.setTextColor(31,41,55);
        var lines = doc.splitTextToSize(String(row[1]), 330);
        doc.text(lines, 210, y);
        y += Math.max(19, lines.length * 12);
      });
      if ((!options.cover || options.cover.customNotes !== false) && String(cover.customNotes || '').trim()) {
        y += 10;
        doc.setFont('helvetica','bold');
        doc.setFontSize(9);
        doc.setTextColor(138,109,34);
        doc.text('NOTES', 74, y);
        y += 14;
        doc.setFont('helvetica','normal');
        doc.setTextColor(55,65,81);
        doc.text(doc.splitTextToSize(String(cover.customNotes), 460), 74, y);
      }

      var pageCounter = 1;
      var photoCounter = 1;
      var sections = Array.isArray(report.sections) ? report.sections : [];
      for (var s = 0; s < sections.length; s++) {
        var section = sections[s] || {};
        var photos = (section.photos || []).filter(function (ph) {
          return ph && (ph.id || ph.fileId || ph.storageKey || ph.storagePath || ph.imageKey || ph.fullUrl);
        });
        if (!photos.length) continue;

        for (var offset = 0; offset < photos.length; offset += perPage) {
          var chunk = photos.slice(offset, offset + perPage);
          addPage(portrait ? 'portrait' : 'landscape');
          pageCounter++;

          var pw = pageWidth();
          var ph = pageHeight();
          var margin = 42;
          var top = 44;

          doc.setTextColor(31,41,55);
          if (!options.page || options.page.titleEveryPage !== false || options.page.header !== false) {
            doc.setFont('helvetica','bold');
            doc.setFontSize(13);
            doc.text(String(report.title || 'Property Photo Report'), margin, top);
            var rightBits = [];
            if ((!options.page || options.page.pagePropertyAddress !== false) && cover.propertyAddress) rightBits.push(cover.propertyAddress);
            if ((!options.page || options.page.pageRepresentative !== false) && cover.representativeName) rightBits.push('Rep: ' + cover.representativeName);
            if (rightBits.length) {
              doc.setFont('helvetica','normal');
              doc.setFontSize(8);
              doc.text(doc.splitTextToSize(rightBits.join('  •  '), pw * 0.42), pw - margin, top, { align:'right' });
            }
            rule(top + 12);
            top += 34;
          }

          doc.setFont('helvetica','bold');
          doc.setFontSize(15);
          doc.text(String(section.title || ('Section ' + (s + 1))), margin, top);
          top += 18;

          var cols = perPage === 1 ? 1 : 2;
          var rowsCount = perPage === 4 ? 2 : 1;
          var gap = 14;
          var footerReserve = 34;
          var cellW = (pw - margin*2 - gap*(cols-1)) / cols;
          var availableH = ph - top - footerReserve - margin;
          var cellH = (availableH - gap*(rowsCount-1)) / rowsCount;
          var descReserve = showDesc ? 32 : 10;
          var imageH = Math.max(80, cellH - descReserve);

          for (var j = 0; j < chunk.length; j++) {
            var photo = chunk[j];
            var row = Math.floor(j / cols);
            var col = j % cols;
            var x = margin + col * (cellW + gap);
            var cy = top + row * (cellH + gap);

            doc.setDrawColor(216,221,230);
            doc.setFillColor(248,250,252);
            doc.roundedRect(x, cy, cellW, cellH, 5, 5, 'FD');

            if (showNum) {
              doc.setFillColor(184,135,34);
              doc.circle(x + 16, cy + 16, 10, 'F');
              doc.setTextColor(255,255,255);
              doc.setFont('helvetica','bold');
              doc.setFontSize(8);
              doc.text(String(photoCounter), x + 16, cy + 19, { align:'center' });
            }

            var dataUrl = await photoToDataUrl(photo);
            if (dataUrl) {
              try {
                var rect = fitImageRect(doc, dataUrl, x + 6, cy + 6, cellW - 12, imageH - 8);
                doc.addImage(dataUrl, rect.x, rect.y, rect.w, rect.h, undefined, 'FAST');
              } catch (_) {
                doc.setTextColor(120,120,120);
                doc.setFont('helvetica','normal');
                doc.setFontSize(9);
                doc.text('Photo unavailable', x + cellW/2, cy + imageH/2, { align:'center' });
              }
            } else {
              doc.setTextColor(120,120,120);
              doc.setFont('helvetica','normal');
              doc.setFontSize(9);
              doc.text('Photo unavailable', x + cellW/2, cy + imageH/2, { align:'center' });
            }

            if (showDesc && String(photo.description || '').trim()) {
              doc.setTextColor(55,65,81);
              doc.setFont('helvetica','normal');
              doc.setFontSize(8);
              var descLines = doc.splitTextToSize(String(photo.description), cellW - 12);
              doc.text(descLines.slice(0, 3), x + 6, cy + imageH + 12);
            }
            photoCounter++;
          }

          if (!options.page || options.page.footer !== false || options.page.pageNumbers !== false) {
            doc.setTextColor(105,113,125);
            doc.setFont('helvetica','normal');
            doc.setFontSize(8);
            if (!options.page || options.page.footer !== false) {
              doc.text(String(report.title || ''), margin, ph - 20);
            }
            if (!options.page || options.page.pageNumbers !== false) {
              doc.text(String(pageCounter), pw - margin, ph - 20, { align:'right' });
            }
          }
        }
      }

      var fileName = safePdfFileName(report.title) + '.pdf';
      var wasAlreadySaved = !!report.savedAt;
      var pdfBlob = doc.output('blob');

      // Saving as PDF is the publish point for a report. Store the actual PDF
      // in Neon, then persist the report metadata on the cloud-synced project
      // and add the same report reference to the linked lead's Reports tab.
      if (!report.pdfFileId && typeof window.hmCloudUploadLeadFile === 'function') {
        var reportLead = findLeadByProject(rb.projectId);
        var pdfFile = new File([pdfBlob], fileName, { type:'application/pdf' });
        var pdfMeta = await window.hmCloudUploadLeadFile(
          reportLead && reportLead.id ? reportLead.id : '',
          'photo_report_pdf',
          pdfFile,
          {
            id:'photo_report_pdf_' + String(report.id || '').replace(/[^a-zA-Z0-9._-]/g, '_'),
            category:'Reports',
            docCategory:'Reports',
            metadata:{
              projectId:rb.projectId,
              reportId:report.id,
              title:report.title || 'Property Photo Report'
            }
          }
        );
        report.pdfFileId = pdfMeta && pdfMeta.id ? pdfMeta.id : report.pdfFileId || '';
        report.pdfStorageKey = report.pdfFileId ? ('neon:' + report.pdfFileId) : '';
      }

      report.savedAt = report.savedAt || new Date().toISOString();
      report.updatedAt = new Date().toISOString();
      saveReportToProject(rb.projectId, report);
      syncReportRefToJobs(rb.projectId, report, false);
      rb.currentReport = report;
      rb.pendingReport = null;

      if (!wasAlreadySaved) {
        var activityLead = findLeadByProject(rb.projectId);
        if (activityLead && typeof crmPushLeadActivity === 'function') {
          try { crmPushLeadActivity(activityLead, 'Photo report saved: ' + report.title, 'note', getUserName() || 'User'); } catch (_) {}
        }
      }

      doc.save(fileName);
      window.dispatchEvent(new CustomEvent('hmphotoreportssaved', { detail:{ projectId:rb.projectId, reportId:report.id } }));
      if (typeof window.hmProjectsRenderReports === 'function') window.hmProjectsRenderReports();
      if (typeof showUploadToast === 'function') showUploadToast(fileName + ' saved to Reports.');
    } catch (err) {
      console.error('[Photo Reports] PDF save failed', err);
      if (typeof showUploadToast === 'function') showUploadToast('Could not create the PDF. Please try again.');
    } finally {
      if (button) {
        button.disabled = false;
        button.textContent = oldText || 'Save as PDF';
      }
    }
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
      var msgClick = e.target && e.target.closest ? e.target.closest('[data-photo-chat-msg-id],.jf-msg-bubble') : null;
      if (msgClick) {
        var jfMsgs = typeof jfGetMessages === 'function' ? jfGetMessages() : [];
        var idx = Number(msgClick.getAttribute('data-photo-chat-index') || -1);
        var messageId = msgClick.getAttribute('data-photo-chat-msg-id');
        var msg = messageId ? jfMsgs.find(function (item) { return String(item.id || '') === String(messageId); }) : (idx >= 0 ? jfMsgs[idx] : null);
        if (msg) window.hmPhotoReports.openChatFromMessage(msg);
      }
    });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', wire);
  else wire();
})();
