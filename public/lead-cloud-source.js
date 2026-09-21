(function () {
  'use strict';

  var LEAD_KEYS = { 'crm_leads': true, 'app.leads': true };
  var nativeGet = Storage.prototype.getItem;
  var nativeSet = Storage.prototype.setItem;
  var nativeRemove = Storage.prototype.removeItem;
  var memory = Object.create(null);
  var ready = false;
  var booting = false;
  var orgId = '';
  var unsubscribe = null;
  var syncTimer = null;
  var lastCloudById = Object.create(null);

  Object.keys(LEAD_KEYS).forEach(function (key) {
    var raw = nativeGet.call(window.localStorage, key);
    if (raw != null) memory[key] = raw;
  });

  function parseList(raw) {
    try { var list = JSON.parse(raw || '[]'); return Array.isArray(list) ? list : []; }
    catch (_) { return []; }
  }

  function normalizeText(value) { return String(value == null ? '' : value).trim().toLowerCase(); }
  function stableIdentity(lead) {
    lead = lead || {};
    var phone = String(lead.phone || lead.primaryPhone || '').replace(/\D/g, '');
    var address = [lead.street, lead.city, lead.state, lead.zip].map(normalizeText).join('|');
    return [normalizeText(lead.firstName), normalizeText(lead.lastName), phone, address].join('|');
  }

  function timestamp(lead) {
    var value = String((lead && (lead.updatedAt || lead.lastActivityAt || lead.createdAt)) || '');
    var n = Date.parse(value);
    return Number.isFinite(n) ? n : 0;
  }

  function dedupe(records) {
    var byId = Object.create(null), byIdentity = Object.create(null), out = [];
    (records || []).forEach(function (raw) {
      var lead = raw && typeof raw === 'object' ? raw : null;
      if (!lead) return;
      var id = String(lead.id || '').trim();
      var identity = stableIdentity(lead);
      var existing = id ? byId[id] : (identity ? byIdentity[identity] : null);
      if (existing) {
        if (timestamp(lead) > timestamp(existing)) Object.assign(existing, lead);
        return;
      }
      var copy = JSON.parse(JSON.stringify(lead));
      out.push(copy);
      if (id) byId[id] = copy;
      if (identity) byIdentity[identity] = copy;
    });
    return out;
  }

  function currentList() {
    return dedupe(parseList(memory.crm_leads || memory['app.leads'] || '[]'));
  }

  function setMemory(records) {
    var serialized = JSON.stringify(dedupe(records));
    memory.crm_leads = serialized;
    memory['app.leads'] = serialized;
  }

  Storage.prototype.getItem = function (key) {
    key = String(key);
    if (this === window.localStorage && LEAD_KEYS[key]) return Object.prototype.hasOwnProperty.call(memory, key) ? memory[key] : null;
    return nativeGet.call(this, key);
  };

  Storage.prototype.setItem = function (key, value) {
    key = String(key);
    if (this === window.localStorage && LEAD_KEYS[key]) {
      var before = currentList();
      memory[key] = String(value);
      if (key === 'crm_leads') memory['app.leads'] = String(value);
      if (key === 'app.leads') memory.crm_leads = String(value);
      if (ready) scheduleDiffSync(before, currentList());
      return;
    }
    return nativeSet.call(this, key, value);
  };

  Storage.prototype.removeItem = function (key) {
    key = String(key);
    if (this === window.localStorage && LEAD_KEYS[key]) {
      var before = currentList();
      delete memory[key];
      if (key === 'crm_leads') delete memory['app.leads'];
      if (key === 'app.leads') delete memory.crm_leads;
      if (ready) scheduleDiffSync(before, []);
      return;
    }
    return nativeRemove.call(this, key);
  };

  function waitForAuth() {
    return new Promise(function (resolve) {
      if (window.auth && window.auth.currentUser) return resolve(window.auth.currentUser);
      var tries = 0;
      var timer = setInterval(function () {
        tries += 1;
        if (window.auth && window.auth.currentUser) { clearInterval(timer); resolve(window.auth.currentUser); }
        else if (tries >= 100) { clearInterval(timer); resolve(null); }
      }, 100);
    });
  }

  async function resolveOrg(user) {
    if (!user) return '';
    try {
      var token = await user.getIdTokenResult(false);
      var id = String((token.claims || {}).hmOrganizationId || '').trim().toLowerCase();
      if (id) return id;
    } catch (_) {}
    return /@hailmoney\.test$/i.test(String(user.email || '')) ? 'yopro' : '';
  }

  function leadMap(records) {
    var map = Object.create(null);
    (records || []).forEach(function (lead) { var id = String(lead && lead.id || '').trim(); if (id) map[id] = lead; });
    return map;
  }

  var pendingBefore = null, pendingAfter = null;
  function scheduleDiffSync(before, after) {
    if (!ready) return;
    if (!pendingBefore) pendingBefore = before;
    pendingAfter = after;
    clearTimeout(syncTimer);
    syncTimer = setTimeout(function () {
      var from = pendingBefore || [], to = pendingAfter || [];
      pendingBefore = pendingAfter = null;
      void syncDiff(from, to);
    }, 120);
  }

  async function syncDiff(before, after) {
    if (!window.db || !orgId) return;
    var col = window.db.collection('organizations').doc(orgId).collection('leads');
    var oldMap = leadMap(before), newMap = leadMap(after);
    var batch = window.db.batch(), changed = 0;
    Object.keys(newMap).forEach(function (id) {
      if (!oldMap[id] || JSON.stringify(oldMap[id]) !== JSON.stringify(newMap[id])) {
        batch.set(col.doc(id), newMap[id], { merge: false }); changed += 1;
      }
    });
    Object.keys(oldMap).forEach(function (id) {
      if (!newMap[id]) { batch.delete(col.doc(id)); changed += 1; }
    });
    if (!changed) return;
    try { await batch.commit(); }
    catch (error) { console.error('[Lead Cloud] save failed', error); if (typeof window.showUploadToast === 'function') window.showUploadToast('Lead cloud save failed. Your screen will refresh from company cloud.'); }
  }

  function cleanLeadSnapshot() {
    try {
      var raw = nativeGet.call(window.localStorage, 'app.crmData');
      if (!raw) return;
      var data = JSON.parse(raw); if (!data || typeof data !== 'object') return;
      data.leads = [];
      nativeSet.call(window.localStorage, 'app.crmData', JSON.stringify(data));
    } catch (_) {}
  }

  function refreshUi() {
    try {
      if (typeof window.crmRenderMainMenu === 'function') window.crmRenderMainMenu();
      var active = document.querySelector('.page.active');
      var id = active ? active.id : '';
      if (id === 'page-all-leads' && typeof window.crmRenderAllLeads === 'function') window.crmRenderAllLeads('');
      if (id === 'page-my-assigned-leads' && typeof window.crmRenderMyAssignedLeads === 'function') window.crmRenderMyAssignedLeads();
      if (id === 'page-crm-pipeline' && typeof window.crmRenderPipeline === 'function') window.crmRenderPipeline();
    } catch (e) { console.warn('[Lead Cloud] UI refresh failed', e); }
  }

  async function migrateAndLoad(user) {
    orgId = await resolveOrg(user);
    if (!orgId || !window.db) return false;
    var col = window.db.collection('organizations').doc(orgId).collection('leads');
    var local = currentList();
    var snap = await col.get();
    var cloud = snap.docs.map(function (doc) { var x = doc.data() || {}; if (!x.id) x.id = doc.id; return x; });
    var cloudById = leadMap(cloud), cloudByIdentity = Object.create(null);
    cloud.forEach(function (lead) { var identity = stableIdentity(lead); if (identity) cloudByIdentity[identity] = lead; });
    var batch = window.db.batch(), changed = 0;
    local.forEach(function (lead) {
      var id = String(lead && lead.id || '').trim(); if (!id) return;
      var existing = cloudById[id] || cloudByIdentity[stableIdentity(lead)] || null;
      if (!existing) {
        batch.set(col.doc(id), lead, { merge: false }); cloudById[id] = lead; changed += 1; return;
      }
      if (String(existing.id || '') === id && timestamp(lead) > timestamp(existing)) {
        batch.set(col.doc(id), lead, { merge: false }); cloudById[id] = lead; changed += 1;
      }
    });
    if (changed) await batch.commit();
    snap = await col.get();
    cloud = snap.docs.map(function (doc) { var x = doc.data() || {}; if (!x.id) x.id = doc.id; return x; });
    setMemory(cloud);
    lastCloudById = leadMap(cloud);
    nativeRemove.call(window.localStorage, 'crm_leads');
    nativeRemove.call(window.localStorage, 'app.leads');
    cleanLeadSnapshot();
    ready = true;
    refreshUi();
    return true;
  }

  function watchCloud() {
    if (!window.db || !orgId) return;
    if (typeof unsubscribe === 'function') unsubscribe();
    var col = window.db.collection('organizations').doc(orgId).collection('leads');
    unsubscribe = col.onSnapshot(function (snap) {
      var cloud = snap.docs.map(function (doc) { var x = doc.data() || {}; if (!x.id) x.id = doc.id; return x; });
      setMemory(cloud);
      lastCloudById = leadMap(cloud);
      cleanLeadSnapshot();
      refreshUi();
    }, function (error) { console.error('[Lead Cloud] realtime listener failed', error); });
  }

  async function boot() {
    if (booting) return;
    booting = true;
    try {
      var user = await waitForAuth();
      if (!user) return;
      var ok = await migrateAndLoad(user);
      if (ok) watchCloud();
    } catch (error) {
      console.error('[Lead Cloud] startup failed', error);
      if (typeof window.showUploadToast === 'function') window.showUploadToast('Company leads could not load from cloud.');
    } finally { booting = false; }
  }

  var previousSet = Storage.prototype.setItem;
  Storage.prototype.setItem = function (key, value) {
    if (this === window.localStorage && String(key) === 'app.crmData') {
      try {
        var data = JSON.parse(String(value || '{}'));
        if (data && typeof data === 'object') { data.leads = []; value = JSON.stringify(data); }
      } catch (_) {}
    }
    return previousSet.call(this, key, value);
  };

  window.hmLeadCloudReady = function () { return ready; };
  window.hmLeadCloudRefresh = async function () {
    var user = await waitForAuth();
    if (!user) return false;
    var ok = await migrateAndLoad(user);
    if (ok) watchCloud();
    return ok;
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 0); }, { once: true });
  else setTimeout(boot, 0);
}());
