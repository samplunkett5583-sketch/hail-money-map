const crypto = require("crypto");
const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const { FieldValue, Timestamp } = require("firebase-admin/firestore");

admin.initializeApp();

const db = admin.firestore();
const ABC_CLIENT_ID = defineSecret("ABC_CLIENT_ID");
const ABC_CLIENT_SECRET = defineSecret("ABC_CLIENT_SECRET");
const ABC_SANDBOX_AUTH_BASE = "https://sandbox.auth.partners.abcsupply.com/oauth2/aus1vp07knpuqf6Xz0h8/v1";
const ABC_SANDBOX_API_BASE = "https://partners-sb.abcsupply.com";
const ABC_REDIRECT_URI = "https://hailmoneymap.web.app/abc-oauth-callback.html";
const ABC_LOCAL_REDIRECT_URI = "http://127.0.0.1:5500/abc-oauth-callback.html";
const ABC_USER_SCOPES = [
  "pricing.read",
  "order.read",
  "order.write",
  "product.read",
  "account.read",
  "location.read",
  "notification.read",
  "notification.write",
  "offline_access"
].join(" ");

function setAbcCors(req, res) {
  const origin = String(req.get("origin") || "");
  const allowed = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com)$/i.test(origin) ||
    /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (allowed) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
  }
  res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
}

function makeHttpError(status, publicMessage, code) {
  const error = new Error(publicMessage);
  error.status = status;
  error.code = code;
  return error;
}

function abcErrorCode(value) {
  const code = String(value || "abc_request_failed");
  return /^[a-z0-9_.-]{1,80}$/i.test(code) ? code : "abc_request_failed";
}

function sendAbcError(res, operation, error) {
  const status = Number(error && error.status) || 500;
  const code = abcErrorCode(error && error.code);
  logger.error(operation, { status, code });
  const message = error && error.message
    ? error.message
    : "ABC Supply could not complete the request.";
  return res.status(status).json({ error: message, code });
}

async function requireHailMoneyUser(req) {
  const origin = String(req.get("origin") || "");
  if (process.env.FUNCTIONS_EMULATOR === "true" && (
    origin === "http://127.0.0.1:5500" || origin === "http://localhost:5500"
  )) {
    return { uid: "local-abc-sandbox-user" };
  }
  const header = String(req.get("authorization") || "");
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    throw makeHttpError(401, "Sign in to Hail Money before connecting ABC Supply.", "hail_money_auth_required");
  }
  try {
    return await admin.auth().verifyIdToken(match[1]);
  } catch (_) {
    throw makeHttpError(401, "Your Hail Money session has expired. Sign in again.", "hail_money_auth_invalid");
  }
}

function abcBasicAuthorization() {
  return "Basic " + Buffer.from(
    `${ABC_CLIENT_ID.value()}:${ABC_CLIENT_SECRET.value()}`,
    "utf8"
  ).toString("base64");
}

async function abcTokenRequest(params) {
  const response = await fetch(`${ABC_SANDBOX_AUTH_BASE}/token`, {
    method: "POST",
    headers: {
      Authorization: abcBasicAuthorization(),
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json"
    },
    body: new URLSearchParams(params).toString()
  });
  const body = await response.text();
  let data;
  try {
    data = JSON.parse(body);
  } catch (_) {
    data = {};
  }
  if (!response.ok) {
    const upstreamCode = abcErrorCode(data.error);
    if (params.grant_type === "refresh_token" && response.status >= 400 && response.status < 500) {
      throw makeHttpError(409, "Reconnect your ABC Supply account to continue using live pricing.", upstreamCode);
    }
    if (params.grant_type === "authorization_code" && response.status >= 400 && response.status < 500) {
      throw makeHttpError(400, "ABC Supply could not complete this authorization. Start the connection again.", upstreamCode);
    }
    throw makeHttpError(502, "ABC Supply authentication could not be reached.", upstreamCode);
  }
  return data;
}

async function getFreshAbcUserToken(uid) {
  const ref = db.collection("abcSupplyConnections").doc(uid);
  const snap = await ref.get();
  if (!snap.exists) {
    throw makeHttpError(409, "Connect your ABC Supply account first.", "abc_not_connected");
  }
  const connection = snap.data();
  const expiresAtMs = connection.expiresAt && connection.expiresAt.toMillis
    ? connection.expiresAt.toMillis()
    : 0;
  if (connection.accessToken && expiresAtMs > Date.now() + 60000) {
    return connection.accessToken;
  }
  if (!connection.refreshToken) {
    throw makeHttpError(409, "Reconnect your ABC Supply account.", "abc_reauthorization_required");
  }
  const refreshed = await abcTokenRequest({
    grant_type: "refresh_token",
    refresh_token: connection.refreshToken,
    scope: ABC_USER_SCOPES
  });
  const expiresIn = Number(refreshed.expires_in || 1800);
  await ref.set({
    accessToken: refreshed.access_token,
    refreshToken: refreshed.refresh_token || connection.refreshToken,
    expiresAt: Timestamp.fromMillis(Date.now() + expiresIn * 1000),
    scope: refreshed.scope || connection.scope || ABC_USER_SCOPES,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });
  return refreshed.access_token;
}

async function abcApiRequest(uid, path, options = {}) {
  const token = await getFreshAbcUserToken(uid);
  const response = await fetch(`${ABC_SANDBOX_API_BASE}${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_) {
    data = {};
  }
  if (!response.ok) {
    const status = response.status >= 400 && response.status < 500 ? response.status : 502;
    throw makeHttpError(status, `ABC Supply request failed (HTTP ${response.status}).`, "abc_api_request_failed");
  }
  return data;
}

exports.abcOAuthStart = onRequest(
  { secrets: [ABC_CLIENT_ID], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireHailMoneyUser(req);
      const state = crypto.randomBytes(32).toString("hex");
      const requestOrigin = String(req.get("origin") || "");
      const redirectUri = requestOrigin === "http://127.0.0.1:5500" || requestOrigin === "http://localhost:5500"
        ? ABC_LOCAL_REDIRECT_URI
        : ABC_REDIRECT_URI;
      await db.collection("abcOAuthStates").doc(state).set({
        uid: user.uid,
        redirectUri,
        createdAt: FieldValue.serverTimestamp(),
        expiresAt: Timestamp.fromMillis(Date.now() + 10 * 60 * 1000)
      });
      const url = new URL(`${ABC_SANDBOX_AUTH_BASE}/authorize`);
      url.searchParams.set("client_id", ABC_CLIENT_ID.value());
      url.searchParams.set("response_type", "code");
      url.searchParams.set("redirect_uri", redirectUri);
      url.searchParams.set("state", state);
      url.searchParams.set("scope", ABC_USER_SCOPES);
      return res.status(200).json({ authorizationUrl: url.toString(), environment: "sandbox" });
    } catch (error) {
      return sendAbcError(res, "abcOAuthStart failed", error);
    }
  }
);

exports.abcOAuthCallback = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const code = String((req.body && req.body.code) || req.query.code || "");
      const state = String((req.body && req.body.state) || req.query.state || "");
      if (!code || !state) return res.status(400).json({ error: "Missing ABC authorization code or state." });
      const stateRef = db.collection("abcOAuthStates").doc(state);
      const stateSnap = await stateRef.get();
      if (!stateSnap.exists) return res.status(400).json({ error: "This ABC connection request is invalid or has already been used." });
      const stateData = stateSnap.data();
      await stateRef.delete();
      if (!stateData.expiresAt || stateData.expiresAt.toMillis() < Date.now()) {
        return res.status(400).json({ error: "This ABC connection request expired. Start again from Hail Money." });
      }
      const tokens = await abcTokenRequest({
        grant_type: "authorization_code",
        redirect_uri: stateData.redirectUri || ABC_REDIRECT_URI,
        code
      });
      const expiresIn = Number(tokens.expires_in || 1800);
      await db.collection("abcSupplyConnections").doc(stateData.uid).set({
        accessToken: tokens.access_token,
        refreshToken: tokens.refresh_token || null,
        tokenType: tokens.token_type || "Bearer",
        scope: tokens.scope || ABC_USER_SCOPES,
        expiresAt: Timestamp.fromMillis(Date.now() + expiresIn * 1000),
        environment: "sandbox",
        connectedAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });
      return res.status(200).json({ connected: true, environment: "sandbox" });
    } catch (error) {
      return sendAbcError(res, "abcOAuthCallback failed", error);
    }
  }
);

exports.abcAccounts = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "GET") return res.status(405).json({ error: "GET required." });
    try {
      const user = await requireHailMoneyUser(req);
      const data = await abcApiRequest(user.uid, "/api/account/v1/search/accounts", {
        method: "POST",
        body: JSON.stringify({
          filters: [
            { key: "accountType", condition: "equals", values: ["Ship-to"], joinCondition: "and" },
            { key: "storefront", condition: "equals", values: ["abc"] }
          ],
          pagination: { itemsPerPage: 50, pageNumber: 1 }
        })
      });
      return res.status(200).json(data);
    } catch (error) {
      return sendAbcError(res, "abcAccounts failed", error);
    }
  }
);

exports.abcPriceItems = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireHailMoneyUser(req);
      const body = req.body || {};
      if (!body.shipToNumber || !body.branchNumber || !Array.isArray(body.lines) || !body.lines.length) {
        return res.status(400).json({ error: "Ship-To account, branch, and at least one item are required." });
      }
      if (body.lines.length > 50) return res.status(400).json({ error: "ABC allows at most 50 price lines per request." });
      const payload = {
        requestId: body.requestId || `hail-money-${Date.now()}`,
        shipToNumber: String(body.shipToNumber),
        branchNumber: String(body.branchNumber),
        purpose: ["estimating", "quoting", "ordering"].includes(body.purpose) ? body.purpose : "estimating",
        lines: body.lines
      };
      const data = await abcApiRequest(user.uid, "/api/pricing/v2/prices", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      return res.status(200).json(data);
    } catch (error) {
      return sendAbcError(res, "abcPriceItems failed", error);
    }
  }
);

exports.abcFavoriteItems = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "GET") return res.status(405).json({ error: "GET required." });
    try {
      const user = await requireHailMoneyUser(req);
      const billToNumber = String(req.query.billToNumber || "").trim();
      const branchNumber = String(req.query.branchNumber || "").trim();
      if (!billToNumber) return res.status(400).json({ error: "Bill-To account is required." });
      const query = new URLSearchParams({ itemsPerPage: "50", pageNumber: "1" });
      if (branchNumber) query.set("branchNumber", branchNumber);
      const data = await abcApiRequest(
        user.uid,
        `/api/product/v1/items/${encodeURIComponent(billToNumber)}/favorites?${query.toString()}`,
        { method: "GET" }
      );
      return res.status(200).json(data);
    } catch (error) {
      return sendAbcError(res, "abcFavoriteItems failed", error);
    }
  }
);

exports.abcSearchProducts = onRequest(
  { secrets: [ABC_CLIENT_ID, ABC_CLIENT_SECRET], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireHailMoneyUser(req);
      const search = String((req.body && req.body.search) || "").trim();
      const branchNumber = String((req.body && req.body.branchNumber) || "").trim();
      if (search.length < 2) return res.status(400).json({ error: "Enter at least two characters to search products." });
      const filters = [{
        key: "itemDescription",
        condition: "contains",
        values: [search],
        joinCondition: branchNumber ? "and" : null
      }];
      if (branchNumber) filters.push({
        key: "branchNumber",
        condition: "equals",
        values: [branchNumber],
        joinCondition: null
      });
      const data = await abcApiRequest(user.uid, "/api/product/v1/search/items", {
        method: "POST",
        body: JSON.stringify({
          filters,
          embed: ["branches", "variations"],
          pagination: { itemsPerPage: 50, pageNumber: 1 }
        })
      });
      return res.status(200).json(data);
    } catch (error) {
      return sendAbcError(res, "abcSearchProducts failed", error);
    }
  }
);
