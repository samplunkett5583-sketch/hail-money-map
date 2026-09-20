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

  window.HailMoneyDocuSign = {
    connect: connect,
    status: status,
    disconnect: disconnect,
    sendPdf: sendPdf,
    envelopeStatus: envelopeStatus,
    recipientView: recipientView,
    fileToBase64: fileToBase64,
    functionBaseUrl: BASE
  };
})();
