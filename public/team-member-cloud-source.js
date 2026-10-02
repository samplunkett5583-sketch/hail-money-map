(function () {
  'use strict';

  var KEY = 'app.teamMembers';
  var nativeGet = Storage.prototype.getItem;
  var nativeSet = Storage.prototype.setItem;
  var nativeRemove = Storage.prototype.removeItem;
  var legacyRaw = nativeGet.call(window.localStorage, KEY);
  var memoryRaw = legacyRaw;
  var ready = false;
  var booting = false;
  var orgId = '';
  var unsubscribe = null;
  var syncTimer = null;
  var lastCloudRows = [];

  function parseRows(raw) {
    try {
      var rows = JSON.parse(raw || '[]');
      return Array.isArray(rows) ? rows : [];
    } catch (_) { return []; }
  }

  function cloneRows(rows) {
    return JSON.parse(JSON.stringify(Array.isArray(rows) ? rows : []));
  }

  function currentRows() {
    return parseRows(memoryRaw);
  }

  function setMemory(rows) {
    memoryRaw = JSON.stringify(cloneRows(rows));
  }

  Storage.prototype.getItem = function (key) {
    key = String(key);
    if (this === window.localStorage && key === KEY) return memoryRaw == null ? null : memoryRaw;
    return nativeGet.call(this, key);
  };

  Storage.prototype.setItem = function (key, value) {
    key = String(key);
    if (this === window.localStorage && key === KEY) {
      memoryRaw = String(value);
      if (ready) scheduleCloudSave(currentRows());
      return;
    }
    return nativeSet.call(this, key, value);
  };

  Storage.prototype.removeItem = function (key) {
    key = String(key);
    if (this === window.localStorage && key === KEY) {
      memoryRaw = '[]';
      if (ready) scheduleCloudSave([]);
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
        if (window.auth && window.auth.currentUser) {
          clearInterval(timer);
          resolve(window.auth.currentUser);
        } else if (tries >= 100) {
          clearInterval(timer);
          resolve(null);
        }
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

  function employeeFunctionUrl(name) {
    var local = location.hostname === '127.0.0.1' || location.hostname === 'localhost';
    return local
      ? 'http://127.0.0.1:5015/hailmoneymap/us-central1/' + name
      : 'https://us-central1-hailmoneymap.cloudfunctions.net/' + name;
  }

  async function fetchCompanyEmployees(user) {
    var token = await user.getIdToken(true);
    var response = await fetch(employeeFunctionUrl('listCompanyEmployees'), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
      },
      body: '{}'
    });
    var data = await response.json().catch(function () { return {}; });
    if (!response.ok) throw new Error(data.error || 'Company employees could not be loaded.');
    return Array.isArray(data.members) ? data.members : [];
  }

  function memberMap(rows) {
    var map = Object.create(null);
    (rows || []).forEach(function (row) {
      var id = String(row && row.id || '').trim();
      if (id) map[id] = row;
    });
    return map;
  }

  function scheduleCloudSave(rows) {
    clearTimeout(syncTimer);
    syncTimer = setTimeout(function () {
      void saveCloudRows(rows);
    }, 120);
  }

  async function saveCloudRows(rows) {
    if (!window.db || !orgId) return false;
    var col = window.db.collection('organizations').doc(orgId).collection('teamMembers');
    var oldMap = memberMap(lastCloudRows);
    var newMap = memberMap(rows);
    var batch = window.db.batch();
    var changed = 0;

    Object.keys(newMap).forEach(function (id) {
      if (!oldMap[id] || JSON.stringify(oldMap[id]) !== JSON.stringify(newMap[id])) {
        batch.set(col.doc(id), newMap[id], { merge: false });
        changed += 1;
      }
    });
    Object.keys(oldMap).forEach(function (id) {
      if (!newMap[id]) {
        batch.delete(col.doc(id));
        changed += 1;
      }
    });
    if (!changed) return true;

    try {
      await batch.commit();
      lastCloudRows = cloneRows(rows);
      return true;
    } catch (error) {
      console.error('[Team Cloud] save failed', error);
      if (typeof window.showUploadToast === 'function') {
        window.showUploadToast('Employee cloud save failed. Reloading company employees.');
      }
      return false;
    }
  }

  function refreshUi() {
    try {
      if (typeof window.crmRenderSettings === 'function') window.crmRenderSettings();
      if (typeof window.crmRenderMainMenu === 'function') window.crmRenderMainMenu();
      if (typeof window.crmRenderContingencySignedDashboard === 'function') {
        window.crmRenderContingencySignedDashboard();
      }
      if (typeof window.crmRenderAdminSalesRepsPage === 'function') {
        var search = document.getElementById('admin-sales-reps-search');
        window.crmRenderAdminSalesRepsPage(search ? search.value : '');
      }
    } catch (error) {
      console.warn('[Team Cloud] UI refresh failed', error);
    }
  }

  async function loadCloud(user) {
    orgId = await resolveOrg(user);
    if (!orgId || !window.db) return false;

    var members = await fetchCompanyEmployees(user);
    setMemory(members);
    lastCloudRows = cloneRows(members);
    nativeRemove.call(window.localStorage, KEY);
    ready = true;
    refreshUi();
    watchCloud();
    return true;
  }

  function watchCloud() {
    if (!window.db || !orgId) return;
    if (typeof unsubscribe === 'function') unsubscribe();
    var col = window.db.collection('organizations').doc(orgId).collection('teamMembers');
    unsubscribe = col.onSnapshot(function (snap) {
      var rows = snap.docs.map(function (doc) {
        var data = doc.data() || {};
        if (!data.id) data.id = doc.id;
        return data;
      });
      setMemory(rows);
      lastCloudRows = cloneRows(rows);
      nativeRemove.call(window.localStorage, KEY);
      refreshUi();
    }, function (error) {
      console.error('[Team Cloud] realtime listener failed', error);
    });
  }

  async function boot() {
    if (booting) return;
    booting = true;
    try {
      var user = await waitForAuth();
      if (user) await loadCloud(user);
    } catch (error) {
      console.error('[Team Cloud] startup failed', error);
      if (legacyRaw != null) memoryRaw = legacyRaw;
    } finally {
      booting = false;
    }
  }

  window.hmTeamCloudReady = function () { return ready; };
  window.hmTeamCloudRefresh = async function () {
    var user = await waitForAuth();
    if (!user) return false;
    return loadCloud(user);
  };

  function installAuthWatcher() {
    if (!window.auth || typeof window.auth.onAuthStateChanged !== 'function') {
      setTimeout(installAuthWatcher, 100);
      return;
    }
    window.auth.onAuthStateChanged(function (user) {
      if (user) setTimeout(boot, 0);
    });
  }

  installAuthWatcher();
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 0); }, { once: true });
  } else {
    setTimeout(boot, 0);
  }
}());
