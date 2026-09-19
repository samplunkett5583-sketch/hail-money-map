from pathlib import Path

source = Path('public/index.html')
out = Path('public/preview-phone-sync.html')
text = source.read_text(encoding='utf-8')

# Preview only: widen the already-approved dashboard mobile shell breakpoint so
# phones/tablets reporting a desktop-ish CSS viewport do not render the desktop
# dashboard squeezed into the screen.
mobile_comment = '''/* Dashboard mobile shell. Keep the canonical desktop shell untouched while
       turning its permanent rail into an off-canvas drawer on phone widths. */'''
comment_pos = text.find(mobile_comment)
if comment_pos < 0:
    raise SystemExit('Dashboard mobile shell marker not found')
media_old = '@media (max-width:767px) {'
media_pos = text.find(media_old, comment_pos)
if media_pos < 0:
    raise SystemExit('Dashboard mobile shell media query not found')
text = text[:media_pos] + '@media (max-width:1024px) {' + text[media_pos + len(media_old):]

resize_old = '''window.addEventListener('resize', function () {
      if (window.innerWidth < 768) return;
      document.body.classList.remove('dashboard-mobile-nav-open');'''
resize_new = '''window.addEventListener('resize', function () {
      if (window.innerWidth < 1025) return;
      document.body.classList.remove('dashboard-mobile-nav-open');'''
if resize_old not in text:
    raise SystemExit('Dashboard mobile resize guard not found')
text = text.replace(resize_old, resize_new, 1)

# Always close the dashboard drawer as soon as navigation leaves Dashboard.
# Keeping this at the route boundary covers Pipeline and every other page,
# including routes opened by code instead of a direct sidebar click.
show_page_old = '''    function showPage(pageId) {
      if (pageId === 'page-photo-files' && typeof window.hmPhotoLoadFromSupabase === 'function') {'''
show_page_new = '''    function showPage(pageId) {
      if (pageId !== 'page-main-menu') {
        document.body.classList.remove('dashboard-mobile-nav-open');
        var dashboardMobileMenuToggle = document.getElementById('dashboard-mobile-menu-toggle');
        if (dashboardMobileMenuToggle) dashboardMobileMenuToggle.setAttribute('aria-expanded', 'false');
      }
      if (pageId === 'page-photo-files' && typeof window.hmPhotoLoadFromSupabase === 'function') {'''
if show_page_old not in text:
    raise SystemExit('showPage route boundary not found')
text = text.replace(show_page_old, show_page_new, 1)

marker = '/* HAIL MONEY CLOUD LEAD SYNC PREVIEW V1 */'
if marker in text:
    raise SystemExit('Cloud lead sync preview already present')

sync_js = r'''
<script>
/* HAIL MONEY CLOUD LEAD SYNC PREVIEW V1 */
(function () {
  'use strict';

  var META_DOC = 'crmLeadsSyncV1';
  var LEAD_PREFIX = 'crmLeadSyncV1__';
  var applyingCloud = false;
  var authBound = false;
  var loadInFlight = false;
  var lastLoadedUid = '';
  var stopCloudListener = null;
  var syncQueue = Promise.resolve();

  function asArray(value) { return Array.isArray(value) ? value : []; }
  function leadId(lead) { return String(lead && lead.id || '').trim(); }
  function leadDocName(id) { return LEAD_PREFIX + encodeURIComponent(String(id || '')); }
  function safeJson(value) { try { return JSON.stringify(value); } catch (_) { return ''; } }
  function signature(leads) {
    return safeJson(asArray(leads).slice().sort(function (a, b) {
      return leadId(a).localeCompare(leadId(b));
    }));
  }
  function localLeads() {
    try {
      if (typeof crmGetStoredLeads === 'function') return asArray(crmGetStoredLeads());
      if (typeof crmGetLeads === 'function') return asArray(crmGetLeads());
    } catch (_) {}
    try { return asArray(JSON.parse(localStorage.getItem('crm_leads') || '[]')); } catch (_) { return []; }
  }
  function writeLocal(leads) {
    var normalized = asArray(leads);
    localStorage.setItem('crm_leads', JSON.stringify(normalized));
    localStorage.setItem('leads', JSON.stringify(normalized));
    try { if (typeof APP_LEADS_KEY !== 'undefined' && APP_LEADS_KEY) localStorage.setItem(APP_LEADS_KEY, JSON.stringify(normalized)); } catch (_) {}
    try { if (typeof CRM_LEADS_KEY !== 'undefined' && CRM_LEADS_KEY) localStorage.setItem(CRM_LEADS_KEY, JSON.stringify(normalized)); } catch (_) {}
    try { if (typeof crmSyncDataModelSnapshot === 'function') crmSyncDataModelSnapshot(); } catch (_) {}
  }
  async function organizationId() {
    var id = '';
    try {
      if (typeof crmResolveFirestoreOrgId === 'function') {
        id = String(await crmResolveFirestoreOrgId() || '').trim().toLowerCase();
        if (id) return id;
      }
    } catch (_) {}
    var user = window.auth && window.auth.currentUser;
    if (!user) return '';
    try {
      var token = await user.getIdTokenResult(true);
      id = String(token && token.claims && token.claims.hmOrganizationId || '').trim().toLowerCase();
      if (id) return id;
    } catch (_) {}
    try {
      if (window.db) {
        var employee = await window.db.collection('hmEmployees').doc(user.uid).get();
        if (employee.exists) {
          var data = employee.data() || {};
          id = String(data.organizationId || data.hmOrganizationId || '').trim().toLowerCase();
          if (id) return id;
        }
      }
    } catch (_) {}
    if (/@hailmoney\.test$/i.test(String(user.email || ''))) return 'yopro';
    return '';
  }
  async function stateRefs() {
    if (!window.db || !window.auth || !window.auth.currentUser) return null;
    var org = await organizationId();
    if (!org) return null;
    var appState = window.db.collection('organizations').doc(org).collection('appState');
    return { appState: appState, meta: appState.doc(META_DOC), user: window.auth.currentUser };
  }
  function mapById(leads) {
    var out = Object.create(null);
    asArray(leads).forEach(function (lead) {
      var id = leadId(lead);
      if (id) out[id] = lead;
    });
    return out;
  }
  async function bootstrapOrLoad() {
    if (loadInFlight) return;
    var user = window.auth && window.auth.currentUser;
    if (!user) return;
    loadInFlight = true;
    try {
      var refs = await stateRefs();
      if (!refs) return;
      var local = localLeads();
      var metaSnap = await refs.meta.get();
      var meta = metaSnap.exists ? (metaSnap.data() || {}) : {};
      var initialized = meta.initialized === true;

      // First cloud migration happens only from a browser that actually has
      // leads. An empty phone cannot accidentally initialize the company to
      // an empty lead list before the desktop lead is migrated.
      if (!initialized) {
        if (!local.length) return;
        var initialBatch = window.db.batch();
        var initialIds = [];
        local.forEach(function (lead) {
          var id = leadId(lead);
          if (!id) return;
          initialIds.push(id);
          initialBatch.set(refs.appState.doc(leadDocName(id)), {
            lead: lead,
            updatedBy: refs.user.uid,
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
        });
        initialBatch.set(refs.meta, {
          initialized: true,
          version: 1,
          leadIds: initialIds,
          updatedBy: refs.user.uid,
          updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        await initialBatch.commit();
        return;
      }

      var ids = asArray(meta.leadIds).map(function (id) { return String(id || '').trim(); }).filter(Boolean);
      var snaps = await Promise.all(ids.map(function (id) { return refs.appState.doc(leadDocName(id)).get(); }));
      var cloud = [];
      snaps.forEach(function (snap) {
        if (!snap.exists) return;
        var data = snap.data() || {};
        if (data.lead && typeof data.lead === 'object') cloud.push(data.lead);
      });
      if (signature(cloud) !== signature(local)) {
        applyingCloud = true;
        writeLocal(cloud);
        applyingCloud = false;
        setTimeout(function () { window.location.reload(); }, 40);
      }
    } catch (error) {
      console.warn('[Hail Money lead sync preview] cloud load failed', error);
    } finally {
      loadInFlight = false;
    }
  }
  async function watchCloud() {
    if (stopCloudListener) {
      stopCloudListener();
      stopCloudListener = null;
    }
    var refs = await stateRefs();
    if (!refs) return;
    stopCloudListener = refs.meta.onSnapshot(function (snapshot) {
      var data = snapshot.exists ? (snapshot.data() || {}) : {};
      if (data.initialized === true) setTimeout(bootstrapOrLoad, 0);
    }, function (error) {
      console.warn('[Hail Money lead sync preview] live sync failed', error);
    });
  }
  async function saveDelta(before, after) {
    var refs = await stateRefs();
    if (!refs) return;
    before = asArray(before);
    after = asArray(after);
    var beforeById = mapById(before);
    var afterById = mapById(after);
    var metaSnap = await refs.meta.get();
    var meta = metaSnap.exists ? (metaSnap.data() || {}) : {};
    var initialized = meta.initialized === true;
    var currentIds = initialized ? asArray(meta.leadIds).map(String) : [];
    var batch = window.db.batch();
    var hasWrites = false;

    if (!initialized) {
      if (!after.length) return;
      after.forEach(function (lead) {
        var id = leadId(lead);
        if (!id) return;
        batch.set(refs.appState.doc(leadDocName(id)), {
          lead: lead,
          updatedBy: refs.user.uid,
          updatedAt: firebase.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        hasWrites = true;
      });
      currentIds = after.map(leadId).filter(Boolean);
    } else {
      after.forEach(function (lead) {
        var id = leadId(lead);
        if (!id) return;
        if (!beforeById[id] || safeJson(beforeById[id]) !== safeJson(lead)) {
          batch.set(refs.appState.doc(leadDocName(id)), {
            lead: lead,
            updatedBy: refs.user.uid,
            updatedAt: firebase.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
          hasWrites = true;
        }
      });
      Object.keys(beforeById).forEach(function (id) {
        if (afterById[id]) return;
        batch.delete(refs.appState.doc(leadDocName(id)));
        hasWrites = true;
      });
      var extras = currentIds.filter(function (id) { return !beforeById[id] && !afterById[id]; });
      var removed = Object.keys(beforeById).filter(function (id) { return !afterById[id]; });
      currentIds = after.map(leadId).filter(Boolean).concat(extras.filter(function (id) { return removed.indexOf(id) === -1 && !afterById[id]; }));
    }

    if (!hasWrites && initialized) return;
    batch.set(refs.meta, {
      initialized: true,
      version: 1,
      leadIds: currentIds,
      updatedBy: refs.user.uid,
      updatedAt: firebase.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    await batch.commit();
  }
  function queueDelta(before, after) {
    syncQueue = syncQueue.then(function () { return saveDelta(before, after); }).catch(function (error) {
      console.warn('[Hail Money lead sync preview] cloud save failed', error);
    });
  }
  function wrapSave() {
    if (typeof window.crmSaveLeads !== 'function' || window.crmSaveLeads.__hmCloudLeadWrapped) return;
    var original = window.crmSaveLeads;
    var wrapped = function (leads) {
      var before = localLeads();
      var result = original.apply(this, arguments);
      if (result !== false && !applyingCloud) queueDelta(before, localLeads());
      return result;
    };
    wrapped.__hmCloudLeadWrapped = true;
    wrapped.__hmCloudLeadOriginal = original;
    window.crmSaveLeads = wrapped;
    try { crmSaveLeads = wrapped; } catch (_) {}
  }
  function bindAuth() {
    wrapSave();
    if (!window.auth || typeof window.auth.onAuthStateChanged !== 'function') {
      setTimeout(bindAuth, 150);
      return;
    }
    if (!authBound) {
      authBound = true;
      window.auth.onAuthStateChanged(function (user) {
        wrapSave();
        if (!user) {
          lastLoadedUid = '';
          if (stopCloudListener) stopCloudListener();
          stopCloudListener = null;
          return;
        }
        if (lastLoadedUid === user.uid) return;
        lastLoadedUid = user.uid;
        setTimeout(bootstrapOrLoad, 0);
        setTimeout(watchCloud, 0);
      });
    }
    if (window.auth.currentUser && lastLoadedUid !== window.auth.currentUser.uid) {
      lastLoadedUid = window.auth.currentUser.uid;
      setTimeout(bootstrapOrLoad, 0);
      setTimeout(watchCloud, 0);
    }
  }

  bindAuth();
})();
</script>
'''

if '</body>' not in text:
    raise SystemExit('Closing body tag not found')
text = text.replace('</body>', sync_js + '\n</body>', 1)
out.write_text(text, encoding='utf-8')
print('Built public/preview-phone-sync.html with wider mobile shell and company cloud lead sync')
