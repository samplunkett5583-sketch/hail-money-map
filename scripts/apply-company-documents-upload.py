from pathlib import Path
import os

PUBLIC = Path(os.environ.get('HM_PUBLIC_INDEX', 'public/index.html'))
text = PUBLIC.read_text(encoding='utf-8')

old_grid = '''              <div class="crm-company-docs-grid" aria-label="Company document categories">
                <section class="crm-company-docs-category"><h2>Contingencies</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Contracts</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Shingle and Manufacturer Specifications</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Company Insurance Documents</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Roofing Licenses</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Other Company Documents</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
              </div>'''

new_grid = '''              <div class="crm-company-docs-status" id="crm-company-docs-status" role="status">Company files are shared with your team.</div>
              <div class="crm-company-docs-grid" aria-label="Company document categories">
                <section class="crm-company-docs-category" data-company-doc-category="contingencies">
                  <div class="crm-company-docs-category-head"><h2>Contingencies</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="contingencies">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="contingencies"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>
                <section class="crm-company-docs-category" data-company-doc-category="contracts">
                  <div class="crm-company-docs-category-head"><h2>Contracts</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="contracts">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="contracts"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>
                <section class="crm-company-docs-category" data-company-doc-category="specifications">
                  <div class="crm-company-docs-category-head"><h2>Shingle and Manufacturer Specifications</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="specifications">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="specifications"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>
                <section class="crm-company-docs-category" data-company-doc-category="insurance">
                  <div class="crm-company-docs-category-head"><h2>Company Insurance Documents</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="insurance">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="insurance"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>
                <section class="crm-company-docs-category" data-company-doc-category="licenses">
                  <div class="crm-company-docs-category-head"><h2>Roofing Licenses</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="licenses">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="licenses"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>
                <section class="crm-company-docs-category" data-company-doc-category="other">
                  <div class="crm-company-docs-category-head"><h2>Other Company Documents</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="other">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="other"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>
              </div>
              <input id="crm-company-doc-file-input" type="file" accept=".pdf,.doc,.docx,.xls,.xlsx,.png,.jpg,.jpeg,.webp" hidden />'''

if 'data-company-doc-upload="contingencies"' not in text:
    if old_grid not in text:
        raise SystemExit('Company Documents grid not found')
    text = text.replace(old_grid, new_grid, 1)

css_marker = '''    @media (min-width: 981px) {
      body.crm-phase-one-shared-shell-active #page-crm-contacts,'''
css = '''    .crm-company-docs-status {
      margin: 0 0 14px;
      min-height: 20px;
      color: #667085;
      font-size: 13px;
    }
    .crm-company-docs-status.is-error { color: #b42318; }
    .crm-company-docs-category-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }
    .crm-company-docs-category-head h2 { margin: 0; }
    .crm-company-doc-upload { display: none; flex: 0 0 auto; }
    .crm-company-docs-list { display: grid; gap: 9px; }
    .crm-company-doc-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 11px 12px;
      border: 1px solid #e2e7ee;
      border-radius: 12px;
      background: #f8fafc;
    }
    .crm-company-doc-info { min-width: 0; }
    .crm-company-doc-name {
      color: #172033;
      font-size: 14px;
      font-weight: 700;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .crm-company-doc-meta { margin-top: 3px; color: #7a8494; font-size: 11px; }
    .crm-company-doc-actions { display: flex; align-items: center; gap: 7px; flex: 0 0 auto; }
    .crm-company-doc-actions .btn { padding: 7px 10px; font-size: 12px; }
    @media (max-width: 640px) {
      .crm-company-docs-category-head, .crm-company-doc-row { align-items: stretch; flex-direction: column; }
      .crm-company-doc-upload { width: 100%; justify-content: center; }
      .crm-company-doc-actions { width: 100%; }
      .crm-company-doc-actions .btn { flex: 1 1 0; }
    }
'''
if '.crm-company-doc-row {' not in text:
    if css_marker not in text:
        raise SystemExit('Company Documents CSS insertion marker not found')
    text = text.replace(css_marker, css + css_marker, 1)

js_marker = '''    function crmResolveFileUrl(meta, callback) {'''
js = r'''    var CRM_COMPANY_DOC_MAX_BYTES = 25 * 1024 * 1024;
    var _crmCompanyDocuments = [];

    function crmSetCompanyDocsStatus(message, isError) {
      var el = document.getElementById('crm-company-docs-status');
      if (!el) return;
      el.textContent = String(message || '');
      el.classList.toggle('is-error', !!isError);
    }

    function crmCompanyDocsCanManage() {
      return ['Owner', 'Admin'].indexOf(String(crmGetRole() || '').trim()) !== -1;
    }

    async function crmCompanyDocsTokenCanManage() {
      var user = window.auth && window.auth.currentUser;
      if (!user) return false;
      var tokenResult = await user.getIdTokenResult(false);
      var claims = tokenResult.claims || {};
      return claims.employee === true && ['Owner', 'Admin'].indexOf(String(claims.hmRole || '').trim()) !== -1;
    }

    function crmCompanyDocumentsRef(orgId) {
      return window.db.collection('organizations').doc(orgId).collection('appState').doc('companyDocuments');
    }

    function crmCompanyDocSafeSegment(value) {
      return String(value || 'x').replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 120) || 'x';
    }

    function crmCompanyDocFormatBytes(bytes) {
      bytes = Number(bytes || 0);
      if (bytes < 1024) return bytes + ' B';
      if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
      return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }

    function crmCompanyDocFind(docId) {
      docId = String(docId || '');
      return (_crmCompanyDocuments || []).find(function (doc) { return String(doc && doc.id || '') === docId; }) || null;
    }

    function crmRenderCompanyDocuments() {
      var canManage = crmCompanyDocsCanManage();
      document.querySelectorAll('[data-company-doc-upload]').forEach(function (button) {
        button.style.display = canManage ? 'inline-flex' : 'none';
      });
      document.querySelectorAll('[data-company-doc-list]').forEach(function (list) {
        var category = String(list.getAttribute('data-company-doc-list') || '');
        var docs = (_crmCompanyDocuments || []).filter(function (doc) { return String(doc.category || '') === category; });
        docs.sort(function (a, b) { return String(b.uploadedAt || '').localeCompare(String(a.uploadedAt || '')); });
        list.innerHTML = '';
        if (!docs.length) {
          var empty = document.createElement('div');
          empty.className = 'crm-company-docs-empty';
          empty.textContent = 'No documents uploaded yet';
          list.appendChild(empty);
          return;
        }
        docs.forEach(function (doc) {
          var row = document.createElement('div');
          row.className = 'crm-company-doc-row';
          var info = document.createElement('div');
          info.className = 'crm-company-doc-info';
          var name = document.createElement('div');
          name.className = 'crm-company-doc-name';
          name.textContent = String(doc.name || 'Document');
          var meta = document.createElement('div');
          meta.className = 'crm-company-doc-meta';
          var when = doc.uploadedAt ? new Date(doc.uploadedAt).toLocaleDateString() : '';
          meta.textContent = [crmCompanyDocFormatBytes(doc.size), doc.uploadedBy || '', when].filter(Boolean).join(' • ');
          info.appendChild(name);
          info.appendChild(meta);
          var actions = document.createElement('div');
          actions.className = 'crm-company-doc-actions';
          var open = document.createElement('button');
          open.className = 'btn';
          open.type = 'button';
          open.textContent = 'Open';
          open.addEventListener('click', function () { crmOpenCompanyDocument(doc.id); });
          actions.appendChild(open);
          if (canManage) {
            var remove = document.createElement('button');
            remove.className = 'btn';
            remove.type = 'button';
            remove.textContent = 'Delete';
            remove.addEventListener('click', function () { crmDeleteCompanyDocument(doc.id); });
            actions.appendChild(remove);
          }
          row.appendChild(info);
          row.appendChild(actions);
          list.appendChild(row);
        });
      });
    }

    async function crmLoadCompanyDocuments() {
      try {
        crmSetCompanyDocsStatus('Loading company documents…', false);
        var orgId = await crmResolveFirestoreOrgId();
        if (!orgId) throw new Error('Company access is not ready yet. Sign out and back in, then try again.');
        var snap = await crmCompanyDocumentsRef(orgId).get();
        var data = snap.exists ? (snap.data() || {}) : {};
        _crmCompanyDocuments = Array.isArray(data.documents) ? data.documents : [];
        crmRenderCompanyDocuments();
        crmSetCompanyDocsStatus(crmCompanyDocsCanManage() ? 'Upload company documents here. Everyone in your company can open and download them.' : 'Company documents are available to open and download.', false);
      } catch (error) {
        _crmCompanyDocuments = [];
        crmRenderCompanyDocuments();
        crmSetCompanyDocsStatus(error && error.message ? error.message : 'Company documents could not be loaded.', true);
      }
    }

    function crmTriggerCompanyDocUpload(category) {
      if (!crmCompanyDocsCanManage()) return;
      var input = document.getElementById('crm-company-doc-file-input');
      if (!input) return;
      input.setAttribute('data-category', String(category || 'other'));
      input.value = '';
      input.click();
    }

    async function crmHandleCompanyDocUpload(input) {
      var file = input && input.files && input.files[0];
      var category = String(input && input.getAttribute('data-category') || 'other');
      if (!file) return;
      var uploadedPath = '';
      try {
        if (file.size > CRM_COMPANY_DOC_MAX_BYTES) throw new Error('Documents must be 25 MB or smaller.');
        if (!(await crmCompanyDocsTokenCanManage())) throw new Error('Only an Owner or Admin can upload company documents.');
        var orgId = await crmResolveFirestoreOrgId();
        if (!orgId) throw new Error('Company access is not ready yet.');
        var client = (typeof sb !== 'undefined' && sb) || window.supabaseClient;
        if (!client) throw new Error('Document storage is not available.');
        var docId = 'company_doc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9);
        uploadedPath = 'company-documents/' + crmCompanyDocSafeSegment(orgId) + '/' + crmCompanyDocSafeSegment(category) + '/' + crmCompanyDocSafeSegment(docId) + '/' + crmCompanyDocSafeSegment(file.name);
        crmSetCompanyDocsStatus('Uploading ' + file.name + '…', false);
        var uploadResult = await client.storage.from(CRM_SHARED_FILES_BUCKET).upload(uploadedPath, file, {
          upsert: false,
          contentType: file.type || 'application/octet-stream'
        });
        if (uploadResult.error) throw uploadResult.error;
        var ref = crmCompanyDocumentsRef(orgId);
        var snap = await ref.get();
        var data = snap.exists ? (snap.data() || {}) : {};
        var docs = Array.isArray(data.documents) ? data.documents.slice() : [];
        var user = window.auth && window.auth.currentUser;
        docs.push({
          id: docId,
          category: category,
          name: file.name,
          size: Number(file.size || 0),
          contentType: file.type || 'application/octet-stream',
          storagePath: uploadedPath,
          uploadedAt: new Date().toISOString(),
          uploadedBy: crmGetCurrentUserName() || (user && user.email) || 'Admin',
          uploadedByUid: user && user.uid || ''
        });
        await ref.set({ documents: docs, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true });
        _crmCompanyDocuments = docs;
        crmRenderCompanyDocuments();
        crmSetCompanyDocsStatus(file.name + ' uploaded successfully.', false);
      } catch (error) {
        if (uploadedPath) {
          try {
            var cleanupClient = (typeof sb !== 'undefined' && sb) || window.supabaseClient;
            if (cleanupClient) await cleanupClient.storage.from(CRM_SHARED_FILES_BUCKET).remove([uploadedPath]);
          } catch (_) {}
        }
        crmSetCompanyDocsStatus(error && error.message ? error.message : 'Document upload failed.', true);
      } finally {
        if (input) input.value = '';
      }
    }

    async function crmOpenCompanyDocument(docId) {
      var doc = crmCompanyDocFind(docId);
      if (!doc) return;
      var popup = window.open('about:blank', '_blank');
      try {
        crmSetCompanyDocsStatus('Opening ' + (doc.name || 'document') + '…', false);
        var url = await crmGetSharedFileSignedUrl(doc);
        if (!url) throw new Error('This document could not be opened.');
        if (popup) popup.location.href = url;
        else window.location.href = url;
        crmSetCompanyDocsStatus('Company documents are ready.', false);
      } catch (error) {
        if (popup) popup.close();
        crmSetCompanyDocsStatus(error && error.message ? error.message : 'This document could not be opened.', true);
      }
    }

    async function crmDeleteCompanyDocument(docId) {
      var doc = crmCompanyDocFind(docId);
      if (!doc) return;
      if (!window.confirm('Delete "' + String(doc.name || 'this document') + '" for the entire company?')) return;
      try {
        if (!(await crmCompanyDocsTokenCanManage())) throw new Error('Only an Owner or Admin can delete company documents.');
        var orgId = await crmResolveFirestoreOrgId();
        if (!orgId) throw new Error('Company access is not ready yet.');
        crmSetCompanyDocsStatus('Deleting ' + (doc.name || 'document') + '…', false);
        var client = (typeof sb !== 'undefined' && sb) || window.supabaseClient;
        if (client && doc.storagePath) {
          var removeResult = await client.storage.from(CRM_SHARED_FILES_BUCKET).remove([doc.storagePath]);
          if (removeResult.error) throw removeResult.error;
        }
        var ref = crmCompanyDocumentsRef(orgId);
        var snap = await ref.get();
        var data = snap.exists ? (snap.data() || {}) : {};
        var docs = (Array.isArray(data.documents) ? data.documents : []).filter(function (item) { return String(item && item.id || '') !== String(docId); });
        await ref.set({ documents: docs, updatedAt: firebase.firestore.FieldValue.serverTimestamp() }, { merge: true });
        _crmCompanyDocuments = docs;
        crmRenderCompanyDocuments();
        crmSetCompanyDocsStatus('Document deleted.', false);
      } catch (error) {
        crmSetCompanyDocsStatus(error && error.message ? error.message : 'Document could not be deleted.', true);
      }
    }

    function crmInitCompanyDocuments() {
      document.querySelectorAll('[data-company-doc-upload]').forEach(function (button) {
        if (button.getAttribute('data-company-doc-bound') === '1') return;
        button.setAttribute('data-company-doc-bound', '1');
        button.addEventListener('click', function () { crmTriggerCompanyDocUpload(button.getAttribute('data-company-doc-upload')); });
      });
      var input = document.getElementById('crm-company-doc-file-input');
      if (input && input.getAttribute('data-company-doc-bound') !== '1') {
        input.setAttribute('data-company-doc-bound', '1');
        input.addEventListener('change', function () { crmHandleCompanyDocUpload(input); });
      }
      crmRenderCompanyDocuments();
    }

    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', crmInitCompanyDocuments);
    else crmInitCompanyDocuments();

'''
if 'function crmLoadCompanyDocuments()' not in text:
    if js_marker not in text:
        raise SystemExit('Shared file helper marker not found')
    text = text.replace(js_marker, js + js_marker, 1)

old_nav = '''      if (tab === 'company-documents') {
        showPage('page-company-docs');
        return;
      }'''
new_nav = '''      if (tab === 'company-documents') {
        showPage('page-company-docs');
        crmLoadCompanyDocuments();
        return;
      }'''
if new_nav not in text:
    if old_nav not in text:
        raise SystemExit('Company Documents navigation block not found')
    text = text.replace(old_nav, new_nav, 1)

PUBLIC.write_text(text, encoding='utf-8')
print('Company Documents upload library patched successfully.')
