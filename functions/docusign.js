'use strict';

const crypto = require('crypto');
const admin = require('firebase-admin');
const logger = require('firebase-functions/logger');
const { onRequest } = require('firebase-functions/v2/https');
const { defineSecret } = require('firebase-functions/params');

const DOCUSIGN_CLIENT_ID = defineSecret('DOCUSIGN_CLIENT_ID');
const DOCUSIGN_CLIENT_SECRET = defineSecret('DOCUSIGN_CLIENT_SECRET');
const DOCUSIGN_TOKEN_KEY = defineSecret('DOCUSIGN_TOKEN_KEY');

const db = admin.firestore();
const REGION = 'us-central1';
const DEMO_AUTH_BASE = 'https://account-d.docusign.com';
const PROD_AUTH_BASE = 'https://account.docusign.com';
const CONNECTIONS = 'hmDocuSignConnections';
const OAUTH_STATES = 'hmDocuSignOAuthStates';
const ENVELOPES = 'hmDocuSignEnvelopes';

function permitCors(req, res) {
  const origin = String(req.get('origin') || '');
  const allowed = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com|hail\.money|www\.hail\.money)$/i.test(origin) ||
    /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (allowed) res.set('Access-Control-Allow-Origin', origin);
  res.set('Vary', 'Origin');
  res.set('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
}

function jsonError(res, error, fallback) {
  const status = Number(error && error.statusCode) || 500;
  if (status >= 500) logger.error(fallback, { message: error && error.message });
  return res.status(status).json({ error: error && error.message || fallback });
}

async function requireFirebaseUser(req) {
  const authHeader = String(req.get('authorization') || '');
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) throw Object.assign(new Error('Sign in is required.'), { statusCode: 401 });
  return admin.auth().verifyIdToken(match[1]);
}

async function resolveOrganizationId(user) {
  const claim = String(user && user.hmOrganizationId || '').trim().toLowerCase();
  if (claim) return claim;
  const employee = await db.collection('hmEmployees').doc(String(user.uid)).get();
  const organizationId = employee.exists ? String(employee.data().organizationId || '').trim().toLowerCase() : '';
  return organizationId || String(user.uid);
}

function requireAdmin(user) {
  const role = String(user && (user.hmRole || user.role) || '').trim().toLowerCase();
  if (user && user.admin === true) return;
  if (role === 'admin' || role === 'owner') return;
  throw Object.assign(new Error('Administrator permission is required.'), { statusCode: 403 });
}

function environmentName() {
  return String(process.env.DOCUSIGN_ENVIRONMENT || 'demo').trim().toLowerCase() === 'production' ? 'production' : 'demo';
}

function authBase() {
  return environmentName() === 'production' ? PROD_AUTH_BASE : DEMO_AUTH_BASE;
}

function projectId() {
  return String(process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || admin.app().options.projectId || 'hailmoneymap');
}

function oauthRedirectUri() {
  return `https://${REGION}-${projectId()}.cloudfunctions.net/docusignOAuthCallback`;
}

function safeReturnUrl(value, fallback) {
  const raw = String(value || fallback || 'https://www.hail.money/').trim();
  try {
    const url = new URL(raw);
    const host = url.hostname.toLowerCase();
    const allowed = host === 'hail.money' || host === 'www.hail.money' || host === 'hailmoneymap.web.app' ||
      host === 'hailmoneymap.firebaseapp.com' || host === '127.0.0.1' || host === 'localhost';
    if (!allowed) throw new Error('Return URL is not allowed.');
    return url.toString();
  } catch (_) {
    return 'https://www.hail.money/';
  }
}

function tokenKey() {
  const raw = String(DOCUSIGN_TOKEN_KEY.value() || '').trim();
  let key;
  if (/^[a-f0-9]{64}$/i.test(raw)) key = Buffer.from(raw, 'hex');
  else {
    try { key = Buffer.from(raw, 'base64'); } catch (_) { key = Buffer.alloc(0); }
  }
  if (key.length !== 32) throw new Error('DOCUSIGN_TOKEN_KEY must be a 32-byte key encoded as base64 or 64 hex characters.');
  return key;
}

function encryptTokens(payload) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', tokenKey(), iv);
  const encrypted = Buffer.concat([cipher.update(JSON.stringify(payload), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return [iv.toString('base64'), tag.toString('base64'), encrypted.toString('base64')].join('.');
}

function decryptTokens(value) {
  const parts = String(value || '').split('.');
  if (parts.length !== 3) throw new Error('Stored DocuSign credentials are invalid. Reconnect DocuSign.');
  const decipher = crypto.createDecipheriv('aes-256-gcm', tokenKey(), Buffer.from(parts[0], 'base64'));
  decipher.setAuthTag(Buffer.from(parts[1], 'base64'));
  const clear = Buffer.concat([decipher.update(Buffer.from(parts[2], 'base64')), decipher.final()]);
  return JSON.parse(clear.toString('utf8'));
}

async function requestJson(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  let body = {};
  try { body = text ? JSON.parse(text) : {}; } catch (_) { body = { message: text }; }
  if (!response.ok) {
    const message = body && (body.message || body.error_description || body.error) || `DocuSign request failed (${response.status}).`;
    throw Object.assign(new Error(String(message)), { statusCode: response.status >= 400 && response.status < 500 ? response.status : 502, docusignStatus: response.status });
  }
  return body;
}

async function exchangeCode(code) {
  const basic = Buffer.from(`${DOCUSIGN_CLIENT_ID.value()}:${DOCUSIGN_CLIENT_SECRET.value()}`).toString('base64');
  return requestJson(`${authBase()}/oauth/token`, {
    method: 'POST',
    headers: { Authorization: `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'authorization_code', code: String(code) }).toString()
  });
}

async function refreshTokens(connectionRef, connection) {
  const tokens = decryptTokens(connection.tokenCiphertext);
  const expiresAt = connection.accessTokenExpiresAt && typeof connection.accessTokenExpiresAt.toMillis === 'function'
    ? connection.accessTokenExpiresAt.toMillis() : Number(connection.accessTokenExpiresAt || 0);
  if (tokens.access_token && expiresAt > Date.now() + 5 * 60 * 1000) return tokens;
  if (!tokens.refresh_token) throw Object.assign(new Error('DocuSign authorization expired. Reconnect DocuSign.'), { statusCode: 401 });

  const basic = Buffer.from(`${DOCUSIGN_CLIENT_ID.value()}:${DOCUSIGN_CLIENT_SECRET.value()}`).toString('base64');
  const refreshed = await requestJson(`${authBase()}/oauth/token`, {
    method: 'POST',
    headers: { Authorization: `Basic ${basic}`, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token: tokens.refresh_token }).toString()
  });
  const merged = {
    access_token: refreshed.access_token,
    refresh_token: refreshed.refresh_token || tokens.refresh_token,
    token_type: refreshed.token_type || 'Bearer',
    scope: refreshed.scope || tokens.scope || 'signature extended'
  };
  const nextExpiry = Date.now() + Math.max(300, Number(refreshed.expires_in || 3600)) * 1000;
  await connectionRef.set({
    tokenCiphertext: encryptTokens(merged),
    accessTokenExpiresAt: admin.firestore.Timestamp.fromMillis(nextExpiry),
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
  return merged;
}

async function getConnection(user) {
  const organizationId = await resolveOrganizationId(user);
  const ref = db.collection(CONNECTIONS).doc(organizationId);
  const snapshot = await ref.get();
  if (!snapshot.exists || snapshot.data().connected !== true) {
    throw Object.assign(new Error('DocuSign is not connected for this company.'), { statusCode: 409 });
  }
  const connection = snapshot.data();
  if (connection.environment !== environmentName()) {
    throw Object.assign(new Error(`DocuSign is connected to ${connection.environment || 'another'} environment. Reconnect it for ${environmentName()}.`), { statusCode: 409 });
  }
  const tokens = await refreshTokens(ref, connection);
  return { organizationId, ref, connection, tokens };
}

async function apiRequest(connection, tokens, path, options) {
  const baseUri = String(connection.baseUri || '').replace(/\/$/, '');
  if (!baseUri) throw new Error('DocuSign API base URL is missing. Reconnect DocuSign.');
  const method = options && options.method || 'GET';
  const headers = Object.assign({ Authorization: `Bearer ${tokens.access_token}`, Accept: 'application/json' }, options && options.headers || {});
  return requestJson(`${baseUri}/restapi/v2.1/accounts/${encodeURIComponent(connection.accountId)}${path}`, {
    method,
    headers,
    body: options && options.body
  });
}

function normalizeSigner(raw, index, signingMode) {
  const email = String(raw && raw.email || '').trim().toLowerCase();
  const name = String(raw && raw.name || '').trim().slice(0, 120);
  if (!email || !/^\S+@\S+\.\S+$/.test(email) || !name) throw Object.assign(new Error(`Signer ${index + 1} needs a valid name and email.`), { statusCode: 400 });
  const recipientId = String(index + 1);
  const signer = { email, name, recipientId, routingOrder: String(Number(raw.routingOrder || index + 1)) };
  if (signingMode === 'embedded') signer.clientUserId = String(raw.clientUserId || `hail-money-signer-${index + 1}`);

  const tab = raw && raw.signatureTab || {};
  const signHere = { documentId: '1', recipientId };
  if (tab.anchorString) {
    Object.assign(signHere, {
      anchorString: String(tab.anchorString),
      anchorUnits: 'pixels',
      anchorXOffset: String(Number(tab.anchorXOffset || 0)),
      anchorYOffset: String(Number(tab.anchorYOffset || 0)),
      anchorIgnoreIfNotPresent: 'false'
    });
  } else if (tab.pageNumber && Number.isFinite(Number(tab.xPosition)) && Number.isFinite(Number(tab.yPosition))) {
    Object.assign(signHere, {
      pageNumber: String(Number(tab.pageNumber)),
      xPosition: String(Number(tab.xPosition)),
      yPosition: String(Number(tab.yPosition))
    });
  } else {
    Object.assign(signHere, {
      anchorString: `[[DOCUSIGN_SIGNATURE_${index + 1}]]`,
      anchorUnits: 'pixels',
      anchorXOffset: '0',
      anchorYOffset: '0',
      anchorIgnoreIfNotPresent: 'false'
    });
  }

  signer.tabs = { signHereTabs: [signHere] };
  const dateAnchor = String(raw && raw.dateSignedAnchor || '').trim();
  if (dateAnchor) {
    signer.tabs.dateSignedTabs = [{
      documentId: '1', recipientId, anchorString: dateAnchor, anchorUnits: 'pixels',
      anchorXOffset: '0', anchorYOffset: '0', anchorIgnoreIfNotPresent: 'true'
    }];
  }
  return signer;
}

function sanitizeEnvelopeId(value) {
  const id = String(value || '').trim();
  if (!/^[a-f0-9-]{20,80}$/i.test(id)) throw Object.assign(new Error('A valid DocuSign envelope ID is required.'), { statusCode: 400 });
  return id;
}

function parseBase64Document(value) {
  const text = String(value || '').replace(/^data:application\/pdf;base64,/i, '').trim();
  if (!text || !/^[A-Za-z0-9+/=\r\n]+$/.test(text)) throw Object.assign(new Error('A PDF document is required.'), { statusCode: 400 });
  if (text.length > 20 * 1024 * 1024) throw Object.assign(new Error('The document is too large for this signing workflow.'), { statusCode: 413 });
  return text.replace(/[\r\n]/g, '');
}

const secretOptions = { cors: false, region: REGION, invoker: 'public', secrets: [DOCUSIGN_CLIENT_ID, DOCUSIGN_CLIENT_SECRET, DOCUSIGN_TOKEN_KEY] };

exports.docusignConnect = onRequest(secretOptions, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    const user = await requireFirebaseUser(req);
    requireAdmin(user);
    const organizationId = await resolveOrganizationId(user);
    const state = crypto.randomBytes(24).toString('hex');
    const returnUrl = safeReturnUrl(req.body && req.body.returnUrl, 'https://www.hail.money/');
    await db.collection(OAUTH_STATES).doc(state).set({
      organizationId,
      uid: user.uid,
      returnUrl,
      environment: environmentName(),
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      expiresAt: admin.firestore.Timestamp.fromMillis(Date.now() + 10 * 60 * 1000)
    });
    const params = new URLSearchParams({
      response_type: 'code',
      scope: 'signature extended',
      client_id: DOCUSIGN_CLIENT_ID.value(),
      redirect_uri: oauthRedirectUri(),
      state
    });
    return res.status(200).json({ authorizationUrl: `${authBase()}/oauth/auth?${params.toString()}`, environment: environmentName() });
  } catch (error) {
    return jsonError(res, error, 'DocuSign connection could not be started.');
  }
});

exports.docusignOAuthCallback = onRequest(secretOptions, async (req, res) => {
  if (req.method !== 'GET') return res.status(405).send('GET required.');
  const state = String(req.query && req.query.state || '').trim();
  let returnUrl = 'https://www.hail.money/';
  try {
    if (!state || !/^[a-f0-9]{48}$/i.test(state)) throw Object.assign(new Error('Invalid DocuSign connection state.'), { statusCode: 400 });
    const stateRef = db.collection(OAUTH_STATES).doc(state);
    const stateSnapshot = await stateRef.get();
    if (!stateSnapshot.exists) throw Object.assign(new Error('This DocuSign connection request has expired.'), { statusCode: 400 });
    const stateData = stateSnapshot.data();
    returnUrl = safeReturnUrl(stateData.returnUrl, returnUrl);
    const expiresAt = stateData.expiresAt && stateData.expiresAt.toMillis ? stateData.expiresAt.toMillis() : 0;
    if (stateData.environment !== environmentName() || expiresAt < Date.now()) throw Object.assign(new Error('This DocuSign connection request has expired.'), { statusCode: 400 });
    const oauthError = String(req.query && req.query.error || '').trim();
    if (oauthError) throw Object.assign(new Error(String(req.query.error_description || oauthError)), { statusCode: 400 });
    const code = String(req.query && req.query.code || '').trim();
    if (!code) throw Object.assign(new Error('DocuSign did not return an authorization code.'), { statusCode: 400 });

    const tokenResponse = await exchangeCode(code);
    const userInfo = await requestJson(`${authBase()}/oauth/userinfo`, {
      method: 'GET', headers: { Authorization: `Bearer ${tokenResponse.access_token}`, Accept: 'application/json' }
    });
    const accounts = Array.isArray(userInfo.accounts) ? userInfo.accounts : [];
    const account = accounts.find((item) => item && (item.is_default === true || String(item.is_default).toLowerCase() === 'true')) || accounts[0];
    if (!account || !account.account_id || !account.base_uri) throw new Error('DocuSign did not return an eSignature account for this user.');

    const tokenPayload = {
      access_token: tokenResponse.access_token,
      refresh_token: tokenResponse.refresh_token,
      token_type: tokenResponse.token_type || 'Bearer',
      scope: tokenResponse.scope || 'signature extended'
    };
    await db.collection(CONNECTIONS).doc(String(stateData.organizationId)).set({
      connected: true,
      environment: environmentName(),
      accountId: String(account.account_id),
      accountName: String(account.account_name || ''),
      baseUri: String(account.base_uri),
      docusignUserId: String(userInfo.sub || ''),
      docusignName: String(userInfo.name || ''),
      docusignEmail: String(userInfo.email || ''),
      tokenCiphertext: encryptTokens(tokenPayload),
      accessTokenExpiresAt: admin.firestore.Timestamp.fromMillis(Date.now() + Math.max(300, Number(tokenResponse.expires_in || 3600)) * 1000),
      connectedBy: String(stateData.uid),
      connectedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    await stateRef.delete();
    const target = new URL(returnUrl);
    target.searchParams.set('docusign', 'connected');
    return res.redirect(302, target.toString());
  } catch (error) {
    logger.error('DocuSign OAuth callback failed', { message: error && error.message });
    const target = new URL(safeReturnUrl(returnUrl, 'https://www.hail.money/'));
    target.searchParams.set('docusign', 'error');
    target.searchParams.set('docusign_message', String(error && error.message || 'Connection failed.').slice(0, 180));
    return res.redirect(302, target.toString());
  }
});

exports.docusignStatus = onRequest(secretOptions, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    const user = await requireFirebaseUser(req);
    const organizationId = await resolveOrganizationId(user);
    const snapshot = await db.collection(CONNECTIONS).doc(organizationId).get();
    if (!snapshot.exists || snapshot.data().connected !== true) return res.status(200).json({ connected: false, environment: environmentName() });
    const data = snapshot.data();
    return res.status(200).json({
      connected: true,
      environment: data.environment,
      accountId: data.accountId,
      accountName: data.accountName,
      name: data.docusignName,
      email: data.docusignEmail,
      connectedAt: data.connectedAt || null
    });
  } catch (error) {
    return jsonError(res, error, 'DocuSign status could not be loaded.');
  }
});

exports.docusignDisconnect = onRequest(secretOptions, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    const user = await requireFirebaseUser(req);
    requireAdmin(user);
    const organizationId = await resolveOrganizationId(user);
    await db.collection(CONNECTIONS).doc(organizationId).delete();
    return res.status(200).json({ disconnected: true });
  } catch (error) {
    return jsonError(res, error, 'DocuSign could not be disconnected.');
  }
});

exports.docusignSendEnvelope = onRequest(secretOptions, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    const user = await requireFirebaseUser(req);
    const { organizationId, connection, tokens } = await getConnection(user);
    const body = req.body || {};
    const documentBase64 = parseBase64Document(body.documentBase64);
    const documentName = String(body.documentName || 'Hail Money Agreement.pdf').trim().replace(/[<>:"/\\|?*]+/g, '-').slice(0, 180) || 'Hail Money Agreement.pdf';
    const signingMode = String(body.signingMode || 'email').toLowerCase() === 'embedded' ? 'embedded' : 'email';
    const rawSigners = Array.isArray(body.signers) ? body.signers : [];
    if (!rawSigners.length || rawSigners.length > 10) throw Object.assign(new Error('Add between 1 and 10 signers.'), { statusCode: 400 });
    const signers = rawSigners.map((item, index) => normalizeSigner(item, index, signingMode));
    const envelopeDefinition = {
      emailSubject: String(body.emailSubject || 'Please sign your Hail Money document').trim().slice(0, 100),
      emailBlurb: String(body.emailBlurb || '').trim().slice(0, 10000),
      documents: [{ documentBase64, name: documentName, fileExtension: 'pdf', documentId: '1' }],
      recipients: { signers },
      status: 'sent'
    };
    const result = await apiRequest(connection, tokens, '/envelopes', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(envelopeDefinition)
    });
    const envelopeId = sanitizeEnvelopeId(result.envelopeId);
    await db.collection(ENVELOPES).doc(envelopeId).set({
      envelopeId,
      organizationId,
      createdBy: user.uid,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      status: String(result.status || 'sent'),
      signingMode,
      documentName,
      signerSummary: signers.map((signer) => ({ name: signer.name, email: signer.email, recipientId: signer.recipientId, clientUserId: signer.clientUserId || null }))
    });
    return res.status(200).json({ envelopeId, status: result.status || 'sent', signingMode });
  } catch (error) {
    return jsonError(res, error, 'DocuSign envelope could not be sent.');
  }
});

exports.docusignEnvelopeStatus = onRequest(secretOptions, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    const user = await requireFirebaseUser(req);
    const envelopeId = sanitizeEnvelopeId(req.body && req.body.envelopeId);
    const { organizationId, connection, tokens } = await getConnection(user);
    const tracked = await db.collection(ENVELOPES).doc(envelopeId).get();
    if (tracked.exists && String(tracked.data().organizationId) !== organizationId) throw Object.assign(new Error('That envelope belongs to another company.'), { statusCode: 403 });
    const result = await apiRequest(connection, tokens, `/envelopes/${encodeURIComponent(envelopeId)}?include=recipients`, { method: 'GET' });
    await db.collection(ENVELOPES).doc(envelopeId).set({
      organizationId,
      status: String(result.status || ''),
      statusChangedDateTime: String(result.statusChangedDateTime || ''),
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return res.status(200).json({
      envelopeId,
      status: result.status,
      sentDateTime: result.sentDateTime || null,
      deliveredDateTime: result.deliveredDateTime || null,
      completedDateTime: result.completedDateTime || null,
      statusChangedDateTime: result.statusChangedDateTime || null,
      recipients: result.recipients || null
    });
  } catch (error) {
    return jsonError(res, error, 'DocuSign envelope status could not be loaded.');
  }
});

exports.docusignRecipientView = onRequest(secretOptions, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    const user = await requireFirebaseUser(req);
    const envelopeId = sanitizeEnvelopeId(req.body && req.body.envelopeId);
    const { organizationId, connection, tokens } = await getConnection(user);
    const tracked = await db.collection(ENVELOPES).doc(envelopeId).get();
    if (!tracked.exists || String(tracked.data().organizationId) !== organizationId) throw Object.assign(new Error('That envelope is not available for this company.'), { statusCode: 403 });
    if (tracked.data().signingMode !== 'embedded') throw Object.assign(new Error('This envelope was created for email signing, not embedded signing.'), { statusCode: 409 });
    const signer = req.body && req.body.signer || {};
    const email = String(signer.email || '').trim().toLowerCase();
    const name = String(signer.name || '').trim();
    const clientUserId = String(signer.clientUserId || '').trim();
    if (!email || !name || !clientUserId) throw Object.assign(new Error('Signer name, email, and clientUserId are required.'), { statusCode: 400 });
    const returnUrl = safeReturnUrl(req.body && req.body.returnUrl, 'https://www.hail.money/');
    const result = await apiRequest(connection, tokens, `/envelopes/${encodeURIComponent(envelopeId)}/views/recipient`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ returnUrl, authenticationMethod: 'none', email, userName: name, clientUserId })
    });
    return res.status(200).json({ url: result.url });
  } catch (error) {
    return jsonError(res, error, 'DocuSign signing view could not be created.');
  }
});
