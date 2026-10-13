(function () {
  'use strict';

  var REGION = 'us-central1';
  var PROJECT_ID = 'hailmoneymap';
  var BASE = 'https://' + REGION + '-' + PROJECT_ID + '.cloudfunctions.net/';

  function currentUser() {
    try {
      if (window.firebase && firebase.auth && firebase.auth().currentUser) return firebase.auth().currentUser;
    } catch (_) {}
    return null;
  }

  async function authToken() {
    var user = currentUser();
    if (!user || typeof user.getIdToken !== 'function') throw new Error('Sign in to Hail Money first.');
    return user.getIdToken();
  }

  async function call(name, payload) {
    var token = await authToken();
    var response = await fetch(BASE + name, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload || {})
    });
    var data = {};
    try { data = await response.json(); } catch (_) {}
    if (!response.ok) throw new Error(data.error || ('DocuSign request failed (' + response.status + ').'));
    return data;
  }

  function returnUrl() {
    return window.location.href.split('#')[0];
  }

  async function connect(options) {
    options = options || {};
    var result = await call('docusignConnect', { returnUrl: options.returnUrl || returnUrl() });
    if (!result.authorizationUrl) throw new Error('DocuSign did not return an authorization URL.');
    if (options.redirect === false) return result;
    window.location.assign(result.authorizationUrl);
    return result;
  }

  async function status() {
    return call('docusignStatus', {});
  }

  async function disconnect() {
    return call('docusignDisconnect', {});
  }

  async function sendPdf(options) {
    options = options || {};
    if (!options.documentBase64) throw new Error('A PDF is required.');
    if (!Array.isArray(options.signers) || !options.signers.length) throw new Error('At least one signer is required.');
    return call('docusignSendEnvelope', {
      documentBase64: options.documentBase64,
      documentName: options.documentName || 'Hail Money Agreement.pdf',
      emailSubject: options.emailSubject || 'Please sign your Hail Money document',
      emailBlurb: options.emailBlurb || '',
      signingMode: options.signingMode || 'email',
      signers: options.signers
    });
  }

  async function sendCompletedCopy(options) {
    options = options || {};
    if (!options.documentBase64) throw new Error('A signed PDF is required.');
    if (!options.recipient || !options.recipient.email || !options.recipient.name) throw new Error('The homeowner name and email are required.');
    return call('docusignSendCompletedCopy', {
      documentBase64: options.documentBase64,
      documentName: options.documentName || 'Signed Hail Money Contingency.pdf',
      emailSubject: options.emailSubject || 'Your signed contingency agreement',
      emailBlurb: options.emailBlurb || 'Attached is a copy of your signed contingency agreement for your records.',
      recipient: options.recipient
    });
  }

  async function envelopeStatus(envelopeId) {
    return call('docusignEnvelopeStatus', { envelopeId: envelopeId });
  }

  async function recipientView(options) {
    options = options || {};
    return call('docusignRecipientView', {
      envelopeId: options.envelopeId,
      signer: options.signer,
      returnUrl: options.returnUrl || returnUrl()
    });
  }

  function fileToBase64(file) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader();
      reader.onerror = function () { reject(new Error('The PDF could not be read.')); };
      reader.onload = function () {
        var value = String(reader.result || '');
        resolve(value.indexOf(',') >= 0 ? value.split(',').pop() : value);
      };
      reader.readAsDataURL(file);
    });
  }

  function normalizedRole() {
    var value = '';
    try {
      if (typeof window.crmGetRole === 'function') value = window.crmGetRole();
    } catch (_) {}
    if (!value) {
      try { value = window.currentRole || ''; } catch (_) {}
    }
    return String(value || '').trim().toLowerCase();
  }

  function canManageConnection() {
    var role = normalizedRole();
    return role === 'admin' || role === 'owner';
  }

  function ensureUiStyles() {
    if (document.getElementById('hm-docusign-documents-style')) return;
    var style = document.createElement('style');
    style.id = 'hm-docusign-documents-style';
    style.textContent =
      '.hm-docusign-panel{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:13px 14px;margin:0 0 16px;border:1px solid #d9e0e8;border-radius:12px;background:#fff;box-shadow:0 4px 14px rgba(20,35,55,.06)}' +
      '.hm-docusign-copy{min-width:0}.hm-docusign-title{font-size:14px;font-weight:800;color:#172033}.hm-docusign-state{margin-top:3px;font-size:12px;color:#667085}.hm-docusign-state.is-connected{color:#167746;font-weight:700}.hm-docusign-state.is-error{color:#b42318;font-weight:700}' +
      '.hm-docusign-actions{display:flex;align-items:center;gap:8px;flex:0 0 auto}.hm-docusign-actions .btn{white-space:nowrap}' +
      '@media(max-width:640px){.hm-docusign-panel{align-items:stretch;flex-direction:column}.hm-docusign-actions{width:100%}.hm-docusign-actions .btn{flex:1 1 0}}';
    document.head.appendChild(style);
  }

  function documentsReturnUrl() {
    var url = new URL(window.location.origin + window.location.pathname);
    url.searchParams.set('docusign_return', 'documents');
    return url.toString();
  }

  async function refreshDocumentsConnectionUi(panel) {
    if (!panel || !panel.isConnected) return;
    var state = panel.querySelector('[data-hm-docusign-state]');
    var connectButton = panel.querySelector('[data-hm-docusign-connect]');
    var disconnectButton = panel.querySelector('[data-hm-docusign-disconnect]');
    state.classList.remove('is-connected', 'is-error');
    state.textContent = 'Checking DocuSign connection…';
    connectButton.disabled = true;
    disconnectButton.disabled = true;
    try {
      var result = await status();
      if (result && result.connected) {
        var label = String(result.accountName || result.email || 'DocuSign account');
        state.textContent = 'Connected to ' + label + ' (developer/demo).';
        state.classList.add('is-connected');
        connectButton.style.display = 'none';
        disconnectButton.style.display = '';
        disconnectButton.disabled = false;
      } else {
        state.textContent = 'Not connected yet. Connect the company DocuSign account to send agreements for signature.';
        connectButton.style.display = '';
        disconnectButton.style.display = 'none';
        connectButton.disabled = false;
      }
    } catch (error) {
      state.textContent = error && error.message ? error.message : 'DocuSign status could not be loaded.';
      state.classList.add('is-error');
      connectButton.style.display = '';
      disconnectButton.style.display = 'none';
      connectButton.disabled = false;
    }
  }

  function ensureDocumentsConnectionUi() {
    var page = document.getElementById('page-company-docs');
    if (!page) return null;
    var existing = document.getElementById('hm-docusign-documents-panel');
    if (!canManageConnection()) {
      if (existing) existing.remove();
      return null;
    }
    if (existing) return existing;
    var shell = page.querySelector('.crm-company-docs-page-shell');
    if (!shell) return null;
    ensureUiStyles();
    var panel = document.createElement('div');
    panel.id = 'hm-docusign-documents-panel';
    panel.className = 'hm-docusign-panel';
    panel.innerHTML =
      '<div class="hm-docusign-copy"><div class="hm-docusign-title">DocuSign</div><div class="hm-docusign-state" data-hm-docusign-state>Checking DocuSign connection…</div></div>' +
      '<div class="hm-docusign-actions"><button class="btn btn-primary" type="button" data-hm-docusign-connect>Connect DocuSign</button><button class="btn" type="button" data-hm-docusign-disconnect style="display:none;">Disconnect</button></div>';
    var grid = shell.querySelector('.crm-company-docs-grid');
    if (grid) shell.insertBefore(panel, grid); else shell.appendChild(panel);

    var connectButton = panel.querySelector('[data-hm-docusign-connect]');
    var disconnectButton = panel.querySelector('[data-hm-docusign-disconnect]');
    var state = panel.querySelector('[data-hm-docusign-state]');
    connectButton.addEventListener('click', async function () {
      connectButton.disabled = true;
      state.classList.remove('is-error');
      state.textContent = 'Opening DocuSign authorization…';
      try {
        await connect({ returnUrl: documentsReturnUrl() });
      } catch (error) {
        state.textContent = error && error.message ? error.message : 'DocuSign connection could not be started.';
        state.classList.add('is-error');
        connectButton.disabled = false;
      }
    });
    disconnectButton.addEventListener('click', async function () {
      if (!window.confirm('Disconnect DocuSign for this company?')) return;
      disconnectButton.disabled = true;
      state.classList.remove('is-error');
      state.textContent = 'Disconnecting DocuSign…';
      try {
        await disconnect();
        await refreshDocumentsConnectionUi(panel);
      } catch (error) {
        state.textContent = error && error.message ? error.message : 'DocuSign could not be disconnected.';
        state.classList.add('is-error');
        disconnectButton.disabled = false;
      }
    });
    refreshDocumentsConnectionUi(panel);
    return panel;
  }

  function reopenDocumentsAfterOAuth() {
    var params;
    try { params = new URLSearchParams(window.location.search || ''); } catch (_) { return; }
    if (params.get('docusign_return') !== 'documents') return;
    var attempts = 0;
    var timer = window.setInterval(function () {
      attempts += 1;
      var user = currentUser();
      if (user) {
        window.clearInterval(timer);
        try {
          if (typeof window.showPage === 'function') window.showPage('page-company-docs');
          else {
            var opener = document.getElementById('open-company-docs');
            if (opener) opener.click();
          }
        } catch (_) {}
        ensureDocumentsConnectionUi();
        window.setTimeout(function () {
          var panel = document.getElementById('hm-docusign-documents-panel');
          if (panel) refreshDocumentsConnectionUi(panel);
        }, 500);
        try {
          var clean = new URL(window.location.href);
          clean.searchParams.delete('docusign_return');
          clean.searchParams.delete('docusign');
          clean.searchParams.delete('docusign_message');
          window.history.replaceState({}, '', clean.pathname + (clean.search || '') + (clean.hash || ''));
        } catch (_) {}
      } else if (attempts >= 40) {
        window.clearInterval(timer);
      }
    }, 250);
  }

  function bindDocumentsUi() {
    var page = document.getElementById('page-company-docs');
    if (!page) return;
    var observer = new MutationObserver(function () {
      if (page.classList.contains('active')) {
        var panel = ensureDocumentsConnectionUi();
        if (panel) refreshDocumentsConnectionUi(panel);
      }
    });
    observer.observe(page, { attributes: true, attributeFilter: ['class'] });
    if (page.classList.contains('active')) ensureDocumentsConnectionUi();
    try {
      if (window.firebase && firebase.auth) {
        firebase.auth().onAuthStateChanged(function () {
          if (page.classList.contains('active')) {
            var panel = ensureDocumentsConnectionUi();
            if (panel) refreshDocumentsConnectionUi(panel);
          }
        });
      }
    } catch (_) {}
    reopenDocumentsAfterOAuth();
  }


  function leadDetailRecord() {
    try {
      if (typeof window.crmGetLeadDetailQuickActionLead === 'function') {
        var direct = window.crmGetLeadDetailQuickActionLead();
        if (direct) return direct;
      }
    } catch (_) {}
    try {
      var leadId = String(window._crmCurrentLeadId || window._afCurrentLeadId || '').trim();
      if (leadId && typeof window.crmGetLeads === 'function') {
        return (window.crmGetLeads() || []).find(function (lead) {
          return String(lead && lead.id || '') === leadId;
        }) || null;
      }
    } catch (_) {}
    return null;
  }

  function leadDisplayName(lead) {
    if (!lead) return 'Homeowner';
    var full = (String(lead.firstName || '') + ' ' + String(lead.lastName || '')).trim();
    return full || String(lead.name || lead.customerName || 'Homeowner').trim() || 'Homeowner';
  }

  function ensureLeadEmailComposeUi() {
    var existing = document.getElementById('hm-lead-email-compose-modal');
    if (existing) return existing;

    var style = document.createElement('style');
    style.id = 'hm-lead-email-compose-style';
    style.textContent =
      '.hm-email-compose-overlay{position:fixed;inset:0;z-index:2147483000;display:none;align-items:center;justify-content:center;padding:20px;background:rgba(15,19,36,.66)}' +
      '.hm-email-compose-overlay.is-open{display:flex}' +
      '.hm-email-compose-card{width:min(680px,100%);max-height:92vh;overflow:auto;background:#fff;border-radius:14px;box-shadow:0 22px 60px rgba(0,0,0,.32);padding:20px}' +
      '.hm-email-compose-head{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:16px}' +
      '.hm-email-compose-title{font-size:22px;font-weight:900;color:#171a2d}' +
      '.hm-email-compose-close{border:0;background:transparent;font-size:24px;line-height:1;cursor:pointer;color:#667085}' +
      '.hm-email-compose-field{display:grid;gap:6px;margin-bottom:13px}' +
      '.hm-email-compose-field label{font-size:12px;font-weight:800;color:#475467}' +
      '.hm-email-compose-input,.hm-email-compose-textarea{width:100%;box-sizing:border-box;border:1px solid #cfd6df;border-radius:9px;background:#fff;color:#111827;font:inherit;padding:10px 11px;outline:none}' +
      '.hm-email-compose-input:focus,.hm-email-compose-textarea:focus{border-color:#c5942c;box-shadow:0 0 0 3px rgba(197,148,44,.14)}' +
      '.hm-email-compose-input[readonly]{background:#f6f7f9;color:#667085}' +
      '.hm-email-compose-textarea{min-height:240px;resize:vertical;line-height:1.45}' +
      '.hm-email-compose-actions{display:flex;justify-content:flex-end;gap:9px;margin-top:16px}' +
      '.hm-email-compose-actions button{min-height:40px;padding:9px 16px;border-radius:8px;border:1px solid #cfd6df;background:#fff;font:inherit;font-weight:800;cursor:pointer}' +
      '.hm-email-compose-actions .hm-email-compose-send{border-color:#b7841c;background:#d9ad4e;color:#171717}' +
      '@media(max-width:640px){.hm-email-compose-overlay{padding:10px}.hm-email-compose-card{padding:16px}.hm-email-compose-actions{flex-direction:column-reverse}.hm-email-compose-actions button{width:100%}}';
    document.head.appendChild(style);

    var modal = document.createElement('div');
    modal.id = 'hm-lead-email-compose-modal';
    modal.className = 'hm-email-compose-overlay';
    modal.setAttribute('role', 'dialog');
    modal.setAttribute('aria-modal', 'true');
    modal.setAttribute('aria-labelledby', 'hm-lead-email-compose-title');
    modal.innerHTML =
      '<div class="hm-email-compose-card">' +
        '<div class="hm-email-compose-head">' +
          '<div class="hm-email-compose-title" id="hm-lead-email-compose-title">Compose Email</div>' +
          '<button class="hm-email-compose-close" type="button" aria-label="Close compose email">&times;</button>' +
        '</div>' +
        '<div class="hm-email-compose-field"><label for="hm-lead-email-to">To</label><input class="hm-email-compose-input" id="hm-lead-email-to" type="email" readonly></div>' +
        '<div class="hm-email-compose-field"><label for="hm-lead-email-subject">Subject</label><input class="hm-email-compose-input" id="hm-lead-email-subject" type="text" autocomplete="off"></div>' +
        '<div class="hm-email-compose-field"><label for="hm-lead-email-message">Message</label><textarea class="hm-email-compose-textarea" id="hm-lead-email-message"></textarea></div>' +
        '<div class="hm-email-compose-actions">' +
          '<button type="button" class="hm-email-compose-cancel">Cancel</button>' +
          '<button type="button" class="hm-email-compose-send">Continue to Email</button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(modal);

    function closeComposer() {
      modal.classList.remove('is-open');
      modal._lead = null;
    }

    modal.querySelector('.hm-email-compose-close').addEventListener('click', closeComposer);
    modal.querySelector('.hm-email-compose-cancel').addEventListener('click', closeComposer);
    modal.addEventListener('click', function (event) {
      if (event.target === modal) closeComposer();
    });
    document.addEventListener('keydown', function (event) {
      if (event.key === 'Escape' && modal.classList.contains('is-open')) closeComposer();
    });

    modal.querySelector('.hm-email-compose-send').addEventListener('click', function () {
      var lead = modal._lead || leadDetailRecord();
      var email = String(modal.querySelector('#hm-lead-email-to').value || '').trim();
      var subject = String(modal.querySelector('#hm-lead-email-subject').value || '').trim();
      var message = String(modal.querySelector('#hm-lead-email-message').value || '');
      if (!email) {
        if (typeof window.showUploadToast === 'function') window.showUploadToast('No email address on file.');
        return;
      }
      try {
        if (lead && typeof window.actLog === 'function') {
          window.actLog(String(lead.id || ''), 'email_started', 'Email started to ' + leadDisplayName(lead) + ' (' + email + ').');
        }
      } catch (_) {}
      closeComposer();
      window.location.href = 'mailto:' + encodeURIComponent(email) +
        '?subject=' + encodeURIComponent(subject) +
        '&body=' + encodeURIComponent(message);
    });

    return modal;
  }

  function openLeadEmailComposer() {
    var lead = leadDetailRecord();
    var email = String(lead && lead.email || '').trim();
    if (!email) {
      if (typeof window.showUploadToast === 'function') window.showUploadToast('No email address on file.');
      return;
    }
    var modal = ensureLeadEmailComposeUi();
    modal._lead = lead;
    modal.querySelector('#hm-lead-email-to').value = email;
    modal.querySelector('#hm-lead-email-subject').value = '';
    modal.querySelector('#hm-lead-email-message').value = '';
    modal.classList.add('is-open');
    window.setTimeout(function () {
      var subject = modal.querySelector('#hm-lead-email-subject');
      if (subject) subject.focus();
    }, 0);
  }

  document.addEventListener('click', function (event) {
    var button = event.target && event.target.closest ? event.target.closest('#ld-email-btn') : null;
    if (!button) return;
    event.preventDefault();
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === 'function') event.stopImmediatePropagation();
    openLeadEmailComposer();
  }, true);

  window.HailMoneyDocuSign = {
    connect: connect,
    status: status,
    disconnect: disconnect,
    sendPdf: sendPdf,
    sendCompletedCopy: sendCompletedCopy,
    envelopeStatus: envelopeStatus,
    recipientView: recipientView,
    fileToBase64: fileToBase64,
    functionBaseUrl: BASE
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', bindDocumentsUi);
  else bindDocumentsUi();
})();
