(function () {
  'use strict';

  var API_BASE = 'https://br-super-wildflower-b4eatcc2-cloudapi.compute.c-6.us-east-2.aws.neon.tech';
  var nativeGet = Storage.prototype.getItem;
  var nativeSet = Storage.prototype.setItem;
  var nativeRemove = Storage.prototype.removeItem;
  var memory = Object.create(null);
  var pending = Object.create(null);
  var timers = Object.create(null);
  var ready = false;
  var booting = false;
  var readyResolve;
  var readyPromise = new Promise(function (resolve) { readyResolve = resolve; });

  var SESSION_KEYS = {
    crm_current_user_email: true,
    crm_current_user_name: true,
    crm_current_role: true
  };

  function isCloudKey(key) {
    key = String(key || '');
    if (!key || SESSION_KEYS[key]) return false;
    return key === 'formTemplates' ||
      key.indexOf('app.') === 0 ||
      key.indexOf('crm_') === 0 ||
      key.indexOf('hmm_') === 0 ||
      key.indexOf('hm_') === 0 ||
      key.indexOf('hailMoney') === 0 ||
      key.indexOf('hailmoney.') === 0;
  }

  function isSessionKey(key) {
    return !!SESSION_KEYS[String(key || '')];
  }

  function setMemoryOnly(key, value) {
    if (value == null) delete memory[key];
    else memory[key] = String(value);
  }

  Storage.prototype.getItem = function (key) {
    key = String(key);
    if (this === window.localStorage && (isCloudKey(key) || isSessionKey(key))) {
      return Object.prototype.hasOwnProperty.call(memory, key) ? memory[key] : null;
    }
    return nativeGet.call(this, key);
  };

  Storage.prototype.setItem = function (key, value) {
    key = String(key);
    if (this === window.localStorage && isSessionKey(key)) {
      setMemoryOnly(key, value);
      return;
    }
    if (this === window.localStorage && isCloudKey(key)) {
      setMemoryOnly(key, value);
      if (ready) scheduleSave(key, String(value));
      return;
    }
    return nativeSet.call(this, key, value);
  };

  Storage.prototype.removeItem = function (key) {
    key = String(key);
    if (this === window.localStorage && isSessionKey(key)) {
      delete memory[key];
      return;
    }
    if (this === window.localStorage && isCloudKey(key)) {
      delete memory[key];
      if (ready) scheduleDelete(key);
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
        } else if (tries >= 200) {
          clearInterval(timer);
          resolve(null);
        }
      }, 100);
    });
  }

  async function token(force) {
    var user = await waitForAuth();
    if (!user) throw new Error('Employee login is required.');
    return user.getIdToken(!!force);
  }

  async function api(path, options) {
    options = options || {};
    var headers = Object.assign({}, options.headers || {});
    headers.Authorization = 'Bearer ' + await token(false);
    if (options.body && !(options.body instanceof Blob) && !(options.body instanceof FormData) && !headers['Content-Type']) {
      headers['Content-Type'] = 'application/json';
    }
    var response = await fetch(API_BASE + path, Object.assign({}, options, { headers: headers, cache: 'no-store' }));
    if (response.status === 401) {
      headers.Authorization = 'Bearer ' + await token(true);
      response = await fetch(API_BASE + path, Object.assign({}, options, { headers: headers, cache: 'no-store' }));
    }
    var text = await response.text();
    var data = {};
    try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { error: text || 'Cloud request failed.' }; }
    if (!response.ok) throw new Error(data.error || ('Cloud request failed (' + response.status + ').'));
    return data;
  }

  function scheduleSave(key, value) {
    pending[key] = { mode: 'save', value: value };
    clearTimeout(timers[key]);
    timers[key] = setTimeout(function () {
      var item = pending[key];
      delete pending[key];
      delete timers[key];
      if (!item) return;
      void api('/state', { method: 'PUT', body: JSON.stringify({ key: key, value: item.value }) }).catch(function (error) {
        console.error('[Hail Money Cloud] save failed for ' + key, error);
        if (typeof window.showUploadToast === 'function') window.showUploadToast('Cloud save failed. Please check your connection.');
      });
    }, 120);
  }

  function scheduleDelete(key) {
    pending[key] = { mode: 'delete' };
    clearTimeout(timers[key]);
    timers[key] = setTimeout(function () {
      var item = pending[key];
      delete pending[key];
      delete timers[key];
      if (!item) return;
      void api('/state?key=' + encodeURIComponent(key), { method: 'DELETE' }).catch(function (error) {
        console.error('[Hail Money Cloud] delete failed for ' + key, error);
      });
    }, 120);
  }

  function purgeBrowserCopies() {
    try {
      var keys = [];
      for (var i = 0; i < window.localStorage.length; i++) {
        var key = window.localStorage.key(i);
        if (key && isCloudKey(key)) keys.push(key);
      }
      keys.forEach(function (key) { try { nativeRemove.call(window.localStorage, key); } catch (_) {} });
    } catch (_) {}
  }

  function mergeCloudFiles(files) {
    files = Array.isArray(files) ? files : [];
    var existing = [];
    try { existing = JSON.parse(memory.crm_lead_files || '[]'); } catch (_) {}
    if (!Array.isArray(existing)) existing = [];
    var byId = Object.create(null);
    existing.forEach(function (item) { if (item && item.id) byId[String(item.id)] = item; });
    files.forEach(function (item) { if (item && item.id) byId[String(item.id)] = item; });
    memory.crm_lead_files = JSON.stringify(Object.keys(byId).map(function (id) { return byId[id]; }));
  }

  function refreshUi() {
    try {
      if (typeof window.crmRenderMainMenu === 'function') window.crmRenderMainMenu();
      if (typeof window.crmRenderContingencySignedDashboard === 'function') window.crmRenderContingencySignedDashboard();
      var active = document.querySelector('.page.active');
      var id = active ? active.id : '';
      if (id === 'page-all-leads' && typeof window.crmRenderAllLeads === 'function') window.crmRenderAllLeads('');
      if (id === 'page-my-assigned-leads' && typeof window.crmRenderMyAssignedLeads === 'function') window.crmRenderMyAssignedLeads();
      if (id === 'page-crm-pipeline' && typeof window.crmRenderPipeline === 'function') window.crmRenderPipeline();
      if (id === 'page-crm-job-file' && typeof window.crmRenderJobFile === 'function') window.crmRenderJobFile();
      if (typeof window.hmRenderRegionSettings === 'function') window.hmRenderRegionSettings();
    } catch (error) {
      console.warn('[Hail Money Cloud] UI refresh failed', error);
    }
  }

  async function loadCloud() {
    var stateResult = await api('/state', { method: 'GET' });
    memory = Object.create(null);
    Object.keys(stateResult.state || {}).forEach(function (key) {
      if (isCloudKey(key)) memory[key] = String(stateResult.state[key]);
    });
    var fileResult = await api('/files', { method: 'GET' });
    mergeCloudFiles(fileResult.files || []);
    purgeBrowserCopies();
    ready = true;
    if (readyResolve) { readyResolve(true); readyResolve = null; }
    window.dispatchEvent(new CustomEvent('hailmoneycloudready'));
    refreshUi();
    return true;
  }

  async function boot() {
    if (booting || ready) return;
    booting = true;
    try {
      var user = await waitForAuth();
      if (!user) return;
      await loadCloud();
    } catch (error) {
      console.error('[Hail Money Cloud] startup failed', error);
      if (readyResolve) { readyResolve(false); readyResolve = null; }
      if (typeof window.showUploadToast === 'function') window.showUploadToast('Company cloud data could not load.');
    } finally {
      booting = false;
    }
  }

  window.hmCloudApiBase = API_BASE;
  window.hmCloudApi = api;
  window.hmCloudWhenReady = function () { return ready ? Promise.resolve(true) : readyPromise; };
  window.hmCloudRefresh = loadCloud;
  window.hmCloudIsReady = function () { return ready; };

  window.hmCloudUploadLeadFile = async function (leadId, type, file, metaOptions) {
    metaOptions = metaOptions || {};
    if (!file) throw new Error('A file is required.');
    var id = String(metaOptions.id || ('lead_doc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9)));
    var init = await api('/files/init', {
      method: 'POST',
      body: JSON.stringify({
        id: id,
        leadId: leadId || '',
        type: type || 'document',
        category: metaOptions.category || metaOptions.docCategory || '',
        docCategory: metaOptions.docCategory || metaOptions.category || '',
        fileName: file.name || 'document',
        mimeType: file.type || 'application/octet-stream',
        size: Number(file.size || 0),
        note: metaOptions.note || '',
        uploadedBy: metaOptions.uploadedBy || (typeof window.crmGetCurrentUserName === 'function' ? window.crmGetCurrentUserName() : ''),
        metadata: metaOptions.metadata || {}
      })
    });
    var put = await fetch(init.uploadUrl, {
      method: 'PUT',
      headers: { 'Content-Type': init.contentType || file.type || 'application/octet-stream' },
      body: file
    });
    if (!put.ok) throw new Error('Cloud file upload failed (' + put.status + ').');
    var completed = await api('/files/complete', { method: 'POST', body: JSON.stringify({ id: init.id }) });
    var meta = completed.file || {};
    meta.storageKey = 'neon:' + meta.id;
    return meta;
  };

  window.hmCloudGetFileBlobById = async function (id) {
    id = String(id || '').replace(/^neon:/, '');
    if (!id) throw new Error('File id is missing.');
    var result = await api('/files/' + encodeURIComponent(id) + '/url', { method: 'GET' });
    var response = await fetch(result.url, { cache: 'no-store' });
    if (!response.ok) throw new Error('Cloud file download failed (' + response.status + ').');
    return response.blob();
  };

  window.hmCloudGetFileUrl = async function (id) {
    id = String(id || '').replace(/^neon:/, '');
    var result = await api('/files/' + encodeURIComponent(id) + '/url', { method: 'GET' });
    return result.url;
  };

  window.hmCloudListFiles = async function (leadId, type) {
    var q = [];
    if (leadId) q.push('leadId=' + encodeURIComponent(leadId));
    if (type) q.push('type=' + encodeURIComponent(type));
    var result = await api('/files' + (q.length ? '?' + q.join('&') : ''), { method: 'GET' });
    return result.files || [];
  };

  function installAuthWatcher() {
    if (!window.auth || typeof window.auth.onAuthStateChanged !== 'function') {
      setTimeout(installAuthWatcher, 100);
      return;
    }
    window.auth.onAuthStateChanged(function (user) {
      if (user) setTimeout(boot, 0);
      else {
        ready = false;
        memory = Object.create(null);
      }
    });
  }

  purgeBrowserCopies();
  installAuthWatcher();
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 0); }, { once: true });
  else setTimeout(boot, 0);
}());
