(function () {
  'use strict';

  /*
   * Hail Money cloud adapter.
   * During pre-customer testing we use the shared Firebase Realtime Database
   * test project so every tester/device sees the same data without paid storage.
   * Set SHARED_TEST_CLOUD to false when production storage is enabled.
   */
  var SHARED_TEST_CLOUD = true;
  var API_BASE = 'https://br-super-wildflower-b4eatcc2-cloudapi.compute.c-6.us-east-2.aws.neon.tech';
  var TEST_DB_ROOT = 'https://hailmoney-test-cloud-default-rtdb.firebaseio.com/test/be2b276ee47e6fdba0175b6ac3fb8a190f9f4b289fa7201c';

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

  function safeKey(value) {
    var utf8 = unescape(encodeURIComponent(String(value || '')));
    var encoded = btoa(utf8).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
    return encoded || 'empty';
  }

  function preserveSessionMemory() {
    var saved = Object.create(null);
    Object.keys(SESSION_KEYS).forEach(function (key) {
      if (Object.prototype.hasOwnProperty.call(memory, key)) saved[key] = memory[key];
    });
    return saved;
  }

  function restoreSessionMemory(saved) {
    Object.keys(saved || {}).forEach(function (key) { memory[key] = saved[key]; });
  }

  async function testRequest(path, options) {
    options = options || {};
    var method = String(options.method || 'GET').toUpperCase();
    var requestOptions = {
      method: method,
      cache: 'no-store',
      headers: Object.assign({}, options.headers || {})
    };
    if (options.body !== undefined) {
      requestOptions.body = options.body;
      if (!requestOptions.headers['Content-Type']) requestOptions.headers['Content-Type'] = 'application/json';
    }
    var response = await fetch(TEST_DB_ROOT + path + '.json', requestOptions);
    var text = await response.text();
    var data = null;
    try { data = text ? JSON.parse(text) : null; }
    catch (_) { data = text; }
    if (!response.ok) throw new Error((data && data.error) || ('Shared test cloud request failed (' + response.status + ').'));
    return data;
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

  async function neonApi(path, options) {
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
    try { data = text ? JSON.parse(text) : {}; }
    catch (_) { data = { error: text || 'Cloud request failed.' }; }
    if (!response.ok) throw new Error(data.error || ('Cloud request failed (' + response.status + ').'));
    return data;
  }

  async function cloudApi(path, options) {
    if (!SHARED_TEST_CLOUD) return neonApi(path, options);
    options = options || {};
    var method = String(options.method || 'GET').toUpperCase();

    if (path === '/state' && method === 'GET') {
      var rows = await testRequest('/state', { method: 'GET' }) || {};
      var state = {};
      Object.keys(rows).forEach(function (id) {
        var row = rows[id];
        if (row && row.key != null) state[String(row.key)] = String(row.value == null ? '' : row.value);
      });
      return { ok: true, state: state };
    }

    if (path === '/files' && method === 'GET') {
      var fileRows = await testRequest('/fileMeta', { method: 'GET' }) || {};
      return { ok: true, files: Object.keys(fileRows).map(function (id) { return fileRows[id]; }).filter(Boolean) };
    }

    var deleteMatch = String(path || '').match(/^\/files\/([^/?]+)$/);
    if (deleteMatch && method === 'DELETE') {
      var id = decodeURIComponent(deleteMatch[1]);
      await Promise.all([
        testRequest('/fileMeta/' + safeKey(id), { method: 'DELETE' }),
        testRequest('/fileData/' + safeKey(id), { method: 'DELETE' })
      ]);
      return { ok: true };
    }

    throw new Error('That cloud operation is not available in shared test mode.');
  }

  function scheduleSave(key, value) {
    pending[key] = { mode: 'save', value: value };
    clearTimeout(timers[key]);
    timers[key] = setTimeout(function () {
      var item = pending[key];
      delete pending[key];
      delete timers[key];
      if (!item) return;
      var savePromise = SHARED_TEST_CLOUD
        ? testRequest('/state/' + safeKey(key), {
            method: 'PUT',
            body: JSON.stringify({ key: key, value: item.value, updatedAt: new Date().toISOString() })
          })
        : neonApi('/state', { method: 'PUT', body: JSON.stringify({ key: key, value: item.value }) });
      void savePromise.catch(function (error) {
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
      var deletePromise = SHARED_TEST_CLOUD
        ? testRequest('/state/' + safeKey(key), { method: 'DELETE' })
        : neonApi('/state?key=' + encodeURIComponent(key), { method: 'DELETE' });
      void deletePromise.catch(function (error) {
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
    var sessionMemory = preserveSessionMemory();
    var stateResult = await cloudApi('/state', { method: 'GET' });
    memory = Object.create(null);
    restoreSessionMemory(sessionMemory);
    Object.keys(stateResult.state || {}).forEach(function (key) {
      if (isCloudKey(key)) memory[key] = String(stateResult.state[key]);
    });
    var fileResult = await cloudApi('/files', { method: 'GET' });
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

  function blobToDataUrl(blob) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader();
      reader.onload = function () { resolve(String(reader.result || '')); };
      reader.onerror = function () { reject(reader.error || new Error('The file could not be read.')); };
      reader.readAsDataURL(blob);
    });
  }

  function dataUrlToBlob(dataUrl) {
    var value = String(dataUrl || '');
    var comma = value.indexOf(',');
    if (comma < 0) throw new Error('Stored test file is invalid.');
    var header = value.slice(0, comma);
    var body = value.slice(comma + 1);
    var mimeMatch = header.match(/^data:([^;,]+)/i);
    var mime = mimeMatch ? mimeMatch[1] : 'application/octet-stream';
    var binary = /;base64/i.test(header) ? atob(body) : decodeURIComponent(body);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return new Blob([bytes], { type: mime });
  }

  function compressTestImage(file) {
    if (!file || String(file.type || '').indexOf('image/') !== 0 || Number(file.size || 0) < 450000) {
      return Promise.resolve(file);
    }
    return new Promise(function (resolve) {
      var url = URL.createObjectURL(file);
      var img = new Image();
      img.onload = function () {
        try {
          var maxEdge = 1600;
          var scale = Math.min(1, maxEdge / Math.max(img.naturalWidth || 1, img.naturalHeight || 1));
          var width = Math.max(1, Math.round((img.naturalWidth || 1) * scale));
          var height = Math.max(1, Math.round((img.naturalHeight || 1) * scale));
          var canvas = document.createElement('canvas');
          canvas.width = width;
          canvas.height = height;
          var ctx = canvas.getContext('2d');
          if (!ctx) { URL.revokeObjectURL(url); resolve(file); return; }
          ctx.drawImage(img, 0, 0, width, height);
          canvas.toBlob(function (blob) {
            URL.revokeObjectURL(url);
            if (!blob) { resolve(file); return; }
            try {
              resolve(new File([blob], String(file.name || 'photo.jpg').replace(/\.[^.]+$/, '') + '.jpg', {
                type: 'image/jpeg',
                lastModified: Date.now()
              }));
            } catch (_) {
              blob.name = String(file.name || 'photo.jpg').replace(/\.[^.]+$/, '') + '.jpg';
              resolve(blob);
            }
          }, 'image/jpeg', 0.8);
        } catch (_) {
          URL.revokeObjectURL(url);
          resolve(file);
        }
      };
      img.onerror = function () {
        URL.revokeObjectURL(url);
        resolve(file);
      };
      img.src = url;
    });
  }

  window.hmCloudApiBase = SHARED_TEST_CLOUD ? TEST_DB_ROOT : API_BASE;
  window.hmCloudBackendMode = SHARED_TEST_CLOUD ? 'shared-free-test' : 'neon-production';
  window.hmCloudApi = cloudApi;
  window.hmCloudWhenReady = function () { return ready ? Promise.resolve(true) : readyPromise; };
  window.hmCloudRefresh = loadCloud;
  window.hmCloudIsReady = function () { return ready; };

  window.hmCloudUploadLeadFile = async function (leadId, type, file, metaOptions) {
    metaOptions = metaOptions || {};
    if (!file) throw new Error('A file is required.');

    if (SHARED_TEST_CLOUD) {
      var prepared = await compressTestImage(file);
      if (Number(prepared.size || 0) > 12000000) throw new Error('Test files must be 12 MB or smaller.');
      var id = String(metaOptions.id || ('lead_doc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9)));
      var dataUrl = await blobToDataUrl(prepared);
      var now = new Date().toISOString();
      var user = window.auth && window.auth.currentUser;
      var meta = {
        id: id,
        leadId: String(leadId || ''),
        type: String(type || 'document'),
        category: String(metaOptions.category || metaOptions.docCategory || ''),
        docCategory: String(metaOptions.docCategory || metaOptions.category || ''),
        fileName: String(prepared.name || file.name || 'document'),
        name: String(prepared.name || file.name || 'document'),
        mimeType: String(prepared.type || file.type || 'application/octet-stream'),
        size: Number(prepared.size || file.size || 0),
        note: String(metaOptions.note || ''),
        uploadedBy: String(metaOptions.uploadedBy || (typeof window.crmGetCurrentUserName === 'function' ? window.crmGetCurrentUserName() : '')),
        uploadedByEmail: String(user && user.email || ''),
        uploadedAt: now,
        storageProvider: 'firebase-test-rtdb',
        storagePath: 'fileData/' + safeKey(id),
        objectKey: 'fileData/' + safeKey(id),
        status: 'active',
        metadata: metaOptions.metadata || {}
      };
      var writes = [
        testRequest('/fileData/' + safeKey(id), { method: 'PUT', body: JSON.stringify(dataUrl) })
      ];
      if (String(type || '') !== 'photo_project_blob') {
        writes.push(testRequest('/fileMeta/' + safeKey(id), { method: 'PUT', body: JSON.stringify(meta) }));
      }
      await Promise.all(writes);
      meta.storageKey = 'neon:' + id;
      return meta;
    }

    var neonId = String(metaOptions.id || ('lead_doc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9)));
    var init = await neonApi('/files/init', {
      method: 'POST',
      body: JSON.stringify({
        id: neonId,
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
    var completed = await neonApi('/files/complete', { method: 'POST', body: JSON.stringify({ id: init.id }) });
    var neonMeta = completed.file || {};
    neonMeta.storageKey = 'neon:' + neonMeta.id;
    return neonMeta;
  };

  window.hmCloudGetFileBlobById = async function (id) {
    id = String(id || '').replace(/^neon:/, '').replace(/^test:/, '');
    if (!id) throw new Error('File id is missing.');
    if (SHARED_TEST_CLOUD) {
      var dataUrl = await testRequest('/fileData/' + safeKey(id), { method: 'GET' });
      if (!dataUrl) throw new Error('The test file could not be found.');
      return dataUrlToBlob(dataUrl);
    }
    var result = await neonApi('/files/' + encodeURIComponent(id) + '/url', { method: 'GET' });
    var response = await fetch(result.url, { cache: 'no-store' });
    if (!response.ok) throw new Error('Cloud file download failed (' + response.status + ').');
    return response.blob();
  };

  window.hmCloudGetFileUrl = async function (id) {
    id = String(id || '').replace(/^neon:/, '').replace(/^test:/, '');
    if (!id) throw new Error('File id is missing.');
    if (SHARED_TEST_CLOUD) {
      var dataUrl = await testRequest('/fileData/' + safeKey(id), { method: 'GET' });
      if (!dataUrl) throw new Error('The test file could not be found.');
      return String(dataUrl);
    }
    var result = await neonApi('/files/' + encodeURIComponent(id) + '/url', { method: 'GET' });
    return result.url;
  };

  window.hmCloudListFiles = async function (leadId, type) {
    if (SHARED_TEST_CLOUD) {
      var rows = await testRequest('/fileMeta', { method: 'GET' }) || {};
      return Object.keys(rows).map(function (id) { return rows[id]; }).filter(function (item) {
        if (!item) return false;
        if (leadId && String(item.leadId || '') !== String(leadId)) return false;
        if (type && String(item.type || '') !== String(type)) return false;
        return true;
      });
    }
    var q = [];
    if (leadId) q.push('leadId=' + encodeURIComponent(leadId));
    if (type) q.push('type=' + encodeURIComponent(type));
    var result = await neonApi('/files' + (q.length ? '?' + q.join('&') : ''), { method: 'GET' });
    return result.files || [];
  };

  window.hmCloudPurgeTestCrmFiles = async function () {
    if (!SHARED_TEST_CLOUD) return { deletedFiles: 0, keptCompanyDocuments: 0 };
    var metaRows = await testRequest('/fileMeta', { method: 'GET' }) || {};
    var dataRows = await testRequest('/fileData', { method: 'GET' }) || {};
    var keepDataKeys = Object.create(null);
    var deleteMetaKeys = [];
    var kept = 0;

    Object.keys(metaRows).forEach(function (nodeKey) {
      var meta = metaRows[nodeKey] || {};
      if (String(meta.type || '') === 'company_document') {
        keepDataKeys[safeKey(String(meta.id || ''))] = true;
        kept++;
      } else {
        deleteMetaKeys.push(nodeKey);
      }
    });

    var deleteDataKeys = Object.keys(dataRows).filter(function (nodeKey) {
      return !keepDataKeys[nodeKey];
    });

    await Promise.all(deleteMetaKeys.map(function (nodeKey) {
      return testRequest('/fileMeta/' + nodeKey, { method: 'DELETE' });
    }).concat(deleteDataKeys.map(function (nodeKey) {
      return testRequest('/fileData/' + nodeKey, { method: 'DELETE' });
    })));

    return { deletedFiles: deleteDataKeys.length, keptCompanyDocuments: kept };
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

  window.addEventListener('focus', function () {
    if (ready && !booting) {
      loadCloud().catch(function (error) {
        console.warn('[Hail Money Cloud] focus refresh failed', error);
      });
    }
  });

  document.addEventListener('visibilitychange', function () {
    if (!document.hidden && ready && !booting) {
      loadCloud().catch(function (error) {
        console.warn('[Hail Money Cloud] visibility refresh failed', error);
      });
    }
  });

  purgeBrowserCopies();
  installAuthWatcher();
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(boot, 0); }, { once: true });
  } else {
    setTimeout(boot, 0);
  }
}());
