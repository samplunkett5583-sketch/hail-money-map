const crypto = require("crypto");
const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const { Pool } = require("pg");

admin.initializeApp();

let abcPool = null;
const ABC_CLIENT_ID = defineSecret("ABC_CLIENT_ID");
const ABC_CLIENT_SECRET = defineSecret("ABC_CLIENT_SECRET");
const ABC_TOKEN_ENCRYPTION_KEY = defineSecret("ABC_TOKEN_ENCRYPTION_KEY");
const DATABASE_URL = defineSecret("DATABASE_URL");
const ABC_SANDBOX_AUTH_BASE = "https://sandbox.auth.partners.abcsupply.com/oauth2/aus1vp07knpuqf6Xz0h8/v1";
const ABC_SANDBOX_API_BASE = "https://partners-sb.abcsupply.com";
const ABC_PRODUCTION_AUTH_BASE = "https://auth.partners.abcsupply.com/oauth2/ausvvp0xuwGKLenYy357/v1";
const ABC_PRODUCTION_API_BASE = "https://partners.abcsupply.com";
const HAIL_MONEY_PRODUCTION_ORIGIN = "https://hailmoneymap.web.app";
const ABC_REDIRECT_URI = `${HAIL_MONEY_PRODUCTION_ORIGIN}/abc-oauth-callback.html`;
const ABC_LOCAL_REDIRECT_URI = "http://127.0.0.1:5500/abc-oauth-callback.html";
const ABC_LOCAL_DEVELOPMENT_ORIGINS = new Set([
  "http://127.0.0.1:5500",
  "http://localhost:5500"
]);
const ABC_PRODUCTION_PILOT_ORGANIZATION_ID = "yopro";
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

function abcRuntimeEnvironment() {
  return String(process.env.ABC_API_ENVIRONMENT || "sandbox").trim().toLowerCase() === "production"
    ? "production"
    : "sandbox";
}

function abcEnvironmentConfig(environment = abcRuntimeEnvironment()) {
  return environment === "production"
    ? { environment, authBase: ABC_PRODUCTION_AUTH_BASE, apiBase: ABC_PRODUCTION_API_BASE }
    : { environment: "sandbox", authBase: ABC_SANDBOX_AUTH_BASE, apiBase: ABC_SANDBOX_API_BASE };
}

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

function abcIsFunctionsEmulator() {
  return String(process.env.FUNCTIONS_EMULATOR || "").toLowerCase() === "true";
}

function abcIsIntentionalLocalDevelopment(origin) {
  return abcIsFunctionsEmulator() && ABC_LOCAL_DEVELOPMENT_ORIGINS.has(String(origin || ""));
}

function abcOAuthRequestContext(req) {
  const requestOrigin = String(req.get("origin") || "");
  if (abcIsIntentionalLocalDevelopment(requestOrigin)) {
    return {
      redirectUri: ABC_LOCAL_REDIRECT_URI,
      returnOrigin: requestOrigin
    };
  }
  return {
    redirectUri: ABC_REDIRECT_URI,
    returnOrigin: HAIL_MONEY_PRODUCTION_ORIGIN
  };
}

function abcSafeReturnOrigin(value) {
  const origin = String(value || "");
  return /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com)$/i.test(origin) ||
    abcIsIntentionalLocalDevelopment(origin)
    ? origin
    : HAIL_MONEY_PRODUCTION_ORIGIN;
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

function normalizeOrganizationId(value) {
  const id = String(value || "").trim().toLowerCase();
  return /^[a-z0-9][a-z0-9_-]{1,63}$/.test(id) ? id : "";
}

function abcDatabase() {
  if (!abcPool) {
    abcPool = new Pool({
      connectionString: DATABASE_URL.value(),
      max: 3,
      idleTimeoutMillis: 30000
    });
  }
  return abcPool;
}

function abcTimestampMs(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime();
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

async function abcGetConnection(uid) {
  const result = await abcDatabase().query(
    "SELECT data FROM abc_supply_connections WHERE uid=$1",
    [String(uid || "")]
  );
  return result.rows[0] ? (result.rows[0].data || {}) : null;
}

async function abcSetConnection(uid, data, merge = true) {
  uid = String(uid || "").trim();
  const existing = merge ? await abcGetConnection(uid) : null;
  const next = Object.assign({}, existing || {}, data || {});
  const organizationId = normalizeOrganizationId(next.organizationId);
  if (!uid || !organizationId) {
    throw makeHttpError(500, "ABC Supply connection storage is missing company information.", "abc_storage_org_missing");
  }
  next.uid = uid;
  next.organizationId = organizationId;
  await abcDatabase().query(
    `INSERT INTO abc_supply_connections(uid,org_id,data,updated_at)
     VALUES($1,$2,$3::jsonb,now())
     ON CONFLICT(uid) DO UPDATE
       SET org_id=EXCLUDED.org_id,data=EXCLUDED.data,updated_at=now()`,
    [uid, organizationId, JSON.stringify(next)]
  );
  return next;
}

async function abcDeleteConnection(uid) {
  await abcDatabase().query("DELETE FROM abc_supply_connections WHERE uid=$1", [String(uid || "")]);
}

async function abcGetOnboarding(uid) {
  const result = await abcDatabase().query(
    "SELECT data FROM abc_user_onboarding WHERE uid=$1",
    [String(uid || "")]
  );
  return result.rows[0] ? (result.rows[0].data || {}) : null;
}

async function abcSetOnboarding(uid, data, merge = true) {
  uid = String(uid || "").trim();
  const existing = merge ? await abcGetOnboarding(uid) : null;
  const next = Object.assign({}, existing || {}, data || {});
  let organizationId = normalizeOrganizationId(next.organizationId);
  if (!organizationId) {
    const connection = await abcGetConnection(uid);
    organizationId = normalizeOrganizationId(connection && connection.organizationId);
  }
  if (!uid || !organizationId) {
    throw makeHttpError(500, "ABC Supply onboarding storage is missing company information.", "abc_storage_org_missing");
  }
  next.uid = uid;
  next.organizationId = organizationId;
  await abcDatabase().query(
    `INSERT INTO abc_user_onboarding(uid,org_id,data,updated_at)
     VALUES($1,$2,$3::jsonb,now())
     ON CONFLICT(uid) DO UPDATE
       SET org_id=EXCLUDED.org_id,data=EXCLUDED.data,updated_at=now()`,
    [uid, organizationId, JSON.stringify(next)]
  );
  return next;
}

async function abcSetOAuthState(state, data) {
  const organizationId = normalizeOrganizationId(data && data.organizationId);
  const uid = String(data && data.uid || "").trim();
  const expiresAt = new Date(data && data.expiresAt || (Date.now() + 10 * 60 * 1000));
  await abcDatabase().query(
    `INSERT INTO abc_oauth_states(state,org_id,uid,data,expires_at,created_at)
     VALUES($1,$2,$3,$4::jsonb,$5,now())
     ON CONFLICT(state) DO UPDATE
       SET org_id=EXCLUDED.org_id,uid=EXCLUDED.uid,data=EXCLUDED.data,expires_at=EXCLUDED.expires_at,created_at=now()`,
    [state, organizationId, uid, JSON.stringify(data || {}), expiresAt.toISOString()]
  );
}

async function abcConsumeOAuthState(state) {
  const client = await abcDatabase().connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      "SELECT data FROM abc_oauth_states WHERE state=$1 FOR UPDATE",
      [String(state || "")]
    );
    if (!result.rows.length) {
      await client.query("ROLLBACK");
      return null;
    }
    await client.query("DELETE FROM abc_oauth_states WHERE state=$1", [String(state || "")]);
    await client.query("COMMIT");
    return result.rows[0].data || {};
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function requireAbcOrganizationUser(req) {
  const header = String(req.get("authorization") || "");
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    throw makeHttpError(401, "Sign in to Hail Money before connecting ABC Supply.", "hail_money_auth_required");
  }
  try {
    const decoded = await admin.auth().verifyIdToken(match[1]);
    if (decoded.employee !== true) {
      throw makeHttpError(403, "An authorized Hail Money company user is required.", "employee_claim_required");
    }
    const organizationId = normalizeOrganizationId(decoded.hmOrganizationId || decoded.organizationId);
    if (!organizationId) {
      throw makeHttpError(403, "A verified Hail Money company membership is required.", "organization_membership_org_required");
    }
    const role = String(decoded.hmRole || decoded.role || "").trim();
    const organizationName = organizationId === ABC_PRODUCTION_PILOT_ORGANIZATION_ID
      ? "YoPro Construction"
      : "Your company";
    return { uid: decoded.uid, organizationId, organizationName, role, canManageConnection: true };
  } catch (error) {
    if (error && error.status) throw error;
    throw makeHttpError(401, "Your Hail Money session has expired. Sign in again.", "hail_money_auth_invalid");
  }
}

function abcEncryptionKey() {
  const encoded = String(ABC_TOKEN_ENCRYPTION_KEY.value() || "").trim();
  let key;
  try { key = Buffer.from(encoded, "base64"); } catch (_) { key = Buffer.alloc(0); }
  if (key.length !== 32) {
    throw makeHttpError(503, "ABC Supply secure token storage is not configured.", "abc_token_encryption_unavailable");
  }
  return key;
}

function encryptAbcToken(value) {
  if (!value) return null;
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", abcEncryptionKey(), iv);
  const ciphertext = Buffer.concat([cipher.update(String(value), "utf8"), cipher.final()]);
  return {
    version: 1,
    algorithm: "aes-256-gcm",
    iv: iv.toString("base64"),
    tag: cipher.getAuthTag().toString("base64"),
    ciphertext: ciphertext.toString("base64")
  };
}

function decryptAbcToken(record) {
  if (!record || record.version !== 1 || record.algorithm !== "aes-256-gcm") return "";
  try {
    const decipher = crypto.createDecipheriv("aes-256-gcm", abcEncryptionKey(), Buffer.from(record.iv, "base64"));
    decipher.setAuthTag(Buffer.from(record.tag, "base64"));
    return Buffer.concat([decipher.update(Buffer.from(record.ciphertext, "base64")), decipher.final()]).toString("utf8");
  } catch (_) {
    throw makeHttpError(409, "Reconnect your ABC Supply account.", "abc_reauthorization_required");
  }
}

function abcBasicAuthorization() {
  return "Basic " + Buffer.from(
    `${ABC_CLIENT_ID.value()}:${ABC_CLIENT_SECRET.value()}`,
    "utf8"
  ).toString("base64");
}

async function abcTokenRequest(params, environment = abcRuntimeEnvironment()) {
  const config = abcEnvironmentConfig(environment);
  const response = await fetch(`${config.authBase}/token`, {
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
  const connection = await abcGetConnection(uid);
  if (!connection) {
    throw makeHttpError(409, "Connect your ABC Supply account first.", "abc_not_connected");
  }
  const runtimeEnvironment = abcRuntimeEnvironment();
  if (String(connection.environment || "sandbox") !== runtimeEnvironment) {
    throw makeHttpError(409, "Reconnect your ABC Supply account in this environment.", "abc_environment_mismatch");
  }
  const expiresAtMs = abcTimestampMs(connection.expiresAt);
  const accessToken = decryptAbcToken(connection.accessTokenEncrypted);
  if (accessToken && expiresAtMs > Date.now() + 60000) {
    return accessToken;
  }
  const refreshToken = decryptAbcToken(connection.refreshTokenEncrypted);
  if (!refreshToken) {
    throw makeHttpError(409, "Reconnect your ABC Supply account.", "abc_reauthorization_required");
  }
  const refreshed = await abcTokenRequest({
    grant_type: "refresh_token",
    refresh_token: refreshToken,
    scope: ABC_USER_SCOPES
  }, runtimeEnvironment);
  const expiresIn = Number(refreshed.expires_in || 1800);
  await abcSetConnection(uid, {
    accessTokenEncrypted: encryptAbcToken(refreshed.access_token),
    refreshTokenEncrypted: encryptAbcToken(refreshed.refresh_token || refreshToken),
    expiresAt: new Date(Date.now() + expiresIn * 1000).toISOString(),
    scope: refreshed.scope || connection.scope || ABC_USER_SCOPES,
    updatedAt: new Date().toISOString()
  }, true);
  return refreshed.access_token;
}

async function abcApiRequest(uid, path, options = {}) {
  const token = await getFreshAbcUserToken(uid);
  const config = abcEnvironmentConfig();
  const response = await fetch(`${config.apiBase}${path}`, {
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

async function getAbcUserConnection(uid) {
  const connection = await abcGetConnection(uid);
  if (!connection) {
    throw makeHttpError(409, "Connect your ABC Supply account first.", "abc_not_connected");
  }
  if (String(connection.environment || "sandbox") !== abcRuntimeEnvironment()) {
    throw makeHttpError(409, "Reconnect your ABC Supply account in this environment.", "abc_environment_mismatch");
  }
  return connection;
}

function abcPublicSelection(connection) {
  const accountName = String(connection && connection.selectedAccountName || "").trim();
  const branchName = String(connection && connection.selectedBranchName || "").trim();
  return accountName && branchName ? { accountName, branchName } : null;
}

function findAbcAccountRows(value, depth = 0) {
  if (depth > 5 || value == null) return [];
  if (Array.isArray(value)) return value;
  if (typeof value !== "object") return [];
  for (const key of ["shipTos", "accounts", "items", "results", "records", "data"]) {
    if (Array.isArray(value[key])) return value[key];
    if (value[key] && typeof value[key] === "object") {
      const nested = findAbcAccountRows(value[key], depth + 1);
      if (nested.length) return nested;
    }
  }
  return [];
}

function normalizeAbcAccount(account) {
  const value = account && typeof account === "object" ? account : {};
  let branches = Array.isArray(value.branches) ? value.branches : Array.isArray(value.locations) ? value.locations : [];
  if (!branches.length && (value.branchNumber || value.homeBranchNumber)) {
    branches = [{
      number: value.branchNumber || value.homeBranchNumber,
      name: value.branchName || value.homeBranchName,
      homeBranch: true
    }];
  }
  return {
    shipToNumber: String(value.number || value.accountNumber || value.shipToNumber || value.id || "").trim(),
    billToNumber: String(value.billToNumber || (value.billTo && value.billTo.number) || value.billTo || value.parentAccountNumber || "").trim(),
    name: String(value.name || value.accountName || value.shipToName || "ABC Supply company account").trim(),
    status: String(value.status || value.accountStatus || "active").trim().toLowerCase(),
    isSellable: value.isSellable !== false,
    address: value.address && typeof value.address === "object" ? {
      city: String(value.address.city || "").trim(),
      state: String(value.address.state || "").trim(),
      postal: String(value.address.postal || value.address.postalCode || "").trim()
    } : null,
    branches: branches.map((branch) => ({
      number: String(branch.number || branch.branchNumber || branch.id || "").trim(),
      name: String(branch.name || branch.branchName || branch.locationName || "ABC Supply branch").trim(),
      storefront: String(branch.storefront || "abc").trim().toLowerCase(),
      status: String(branch.status || "active").trim().toLowerCase(),
      homeBranch: branch.homeBranch === true || branch.isHomeBranch === true
    }))
  };
}

function eligibleAbcAccounts(rawAccounts) {
  return findAbcAccountRows(rawAccounts).map(normalizeAbcAccount).filter((account) =>
    account.shipToNumber && account.isSellable && account.status === "active" &&
    account.branches.some((branch) => branch.number && branch.storefront === "abc" && ["active", "open"].includes(branch.status))
  ).map((account) => ({
    ...account,
    branches: account.branches.filter((branch) => branch.number && branch.storefront === "abc" && ["active", "open"].includes(branch.status))
  }));
}

function findAbcBranchRows(value, depth = 0) {
  if (depth > 5 || value == null) return [];
  if (Array.isArray(value)) return value;
  if (typeof value !== "object") return [];
  for (const key of ["branches", "items", "results", "records", "data"]) {
    if (Array.isArray(value[key])) return value[key];
    if (value[key] && typeof value[key] === "object") {
      const nested = findAbcBranchRows(value[key], depth + 1);
      if (nested.length) return nested;
    }
  }
  return value.branch && typeof value.branch === "object" ? [value] : [];
}

function normalizeAbcBranchLocation(value) {
  const wrapper = value && typeof value === "object" ? value : {};
  const branch = wrapper.branch && typeof wrapper.branch === "object" ? wrapper.branch : wrapper;
  const address = wrapper.address && typeof wrapper.address === "object" ? wrapper.address : (branch.address || {});
  const locale = wrapper.locale && typeof wrapper.locale === "object" ? wrapper.locale : (branch.locale || {});
  const latitude = Number(locale.lat ?? locale.latitude ?? wrapper.lat ?? wrapper.latitude);
  const longitude = Number(locale.long ?? locale.lng ?? locale.longitude ?? wrapper.long ?? wrapper.lng ?? wrapper.longitude);
  const distance = Number(branch.distance ?? wrapper.distance);
  return {
    number: String(branch.number || branch.branchNumber || branch.id || "").trim(),
    name: String(branch.name || branch.branchName || "ABC Supply branch").trim(),
    storefront: String(branch.storefront || "abc").trim().toLowerCase(),
    status: String(branch.status || "open").trim().toLowerCase(),
    city: String(address.city || "").trim(),
    state: String(address.state || "").trim(),
    postal: String(address.postal || address.postalCode || "").trim(),
    latitude: Number.isFinite(latitude) ? latitude : null,
    longitude: Number.isFinite(longitude) ? longitude : null,
    distanceMiles: Number.isFinite(distance) ? distance : null
  };
}

function haversineMiles(latitudeA, longitudeA, latitudeB, longitudeB) {
  const radiusMiles = 3958.7613;
  const radians = (degrees) => degrees * Math.PI / 180;
  const latDelta = radians(latitudeB - latitudeA);
  const longDelta = radians(longitudeB - longitudeA);
  const a = Math.sin(latDelta / 2) ** 2 + Math.cos(radians(latitudeA)) * Math.cos(radians(latitudeB)) * Math.sin(longDelta / 2) ** 2;
  return radiusMiles * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function publicAbcBranch(branch, recommended) {
  return {
    name: branch.name,
    city: branch.city || "",
    state: branch.state || "",
    postal: branch.postal || "",
    distanceMiles: Number.isFinite(branch.distanceMiles) ? Number(branch.distanceMiles.toFixed(1)) : null,
    homeBranch: branch.homeBranch === true,
    recommended: recommended === true
  };
}

async function getAuthorizedBranchesNearProperty(uid, connection, latitude, longitude) {
  const rawAccounts = await getAbcUserAccounts(uid);
  const accounts = eligibleAbcAccounts(rawAccounts);
  const selectedShipToNumber = String(connection.selectedShipToNumber || "").trim();
  const account = accounts.find((item) => item.shipToNumber === selectedShipToNumber);
  if (!account) {
    throw makeHttpError(409, "Select an active ABC Supply Ship-To account in Integrations.", "abc_selection_required");
  }
  const eligibleByNumber = new Map(account.branches.map((branch) => [branch.number, branch]));
  const query = new URLSearchParams({
    lat: Number(latitude).toFixed(6),
    long: Number(longitude).toFixed(6),
    distance: "100"
  });
  const rawLocations = await abcApiRequest(uid, `/api/location/v1/branches?${query.toString()}`, { method: "GET" });
  const nearby = findAbcBranchRows(rawLocations).map(normalizeAbcBranchLocation).filter((branch) =>
    branch.number && eligibleByNumber.has(branch.number) && branch.storefront === "abc" && ["active", "open"].includes(branch.status)
  ).map((branch) => {
    const membership = eligibleByNumber.get(branch.number);
    const distanceMiles = Number.isFinite(branch.distanceMiles)
      ? branch.distanceMiles
      : (Number.isFinite(branch.latitude) && Number.isFinite(branch.longitude)
        ? haversineMiles(latitude, longitude, branch.latitude, branch.longitude)
        : null);
    return { ...branch, homeBranch: membership.homeBranch === true, distanceMiles };
  }).sort((a, b) => {
    if (Number.isFinite(a.distanceMiles) !== Number.isFinite(b.distanceMiles)) return Number.isFinite(a.distanceMiles) ? -1 : 1;
    if (Number.isFinite(a.distanceMiles) && a.distanceMiles !== b.distanceMiles) return a.distanceMiles - b.distanceMiles;
    if (a.homeBranch !== b.homeBranch) return a.homeBranch ? -1 : 1;
    return a.name.localeCompare(b.name);
  });
  if (nearby.length) return { account, branches: nearby, recommended: nearby[0] };

  const fallback = account.branches.slice().sort((a, b) => (a.homeBranch === b.homeBranch ? a.name.localeCompare(b.name) : a.homeBranch ? -1 : 1));
  return { account, branches: fallback, recommended: fallback[0] || null };
}

async function getAbcUserAccounts(uid) {
  return abcApiRequest(uid, "/api/account/v1/search/accounts", {
    method: "POST",
    body: JSON.stringify({
      filters: [
        { key: "accountType", condition: "equals", values: ["Ship-to"], joinCondition: "and" },
        { key: "storefront", condition: "equals", values: ["abc"] }
      ],
      pagination: { itemsPerPage: 50, pageNumber: 1 }
    })
  });
}

exports.abcOAuthStart = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const config = abcEnvironmentConfig();
      if (user.organizationId === ABC_PRODUCTION_PILOT_ORGANIZATION_ID && config.environment !== "production") {
        throw makeHttpError(409, "YoPro Construction can connect its real ABC Supply account after Hail Money production access is approved.", "abc_production_access_pending");
      }
      const state = crypto.randomBytes(32).toString("hex");
      const { redirectUri, returnOrigin } = abcOAuthRequestContext(req);
      await abcSetOAuthState(state, {
        organizationId: user.organizationId,
        uid: user.uid,
        environment: config.environment,
        redirectUri,
        returnOrigin,
        createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString()
      });
      const url = new URL(`${config.authBase}/authorize`);
      url.searchParams.set("client_id", ABC_CLIENT_ID.value());
      url.searchParams.set("response_type", "code");
      url.searchParams.set("redirect_uri", redirectUri);
      url.searchParams.set("state", state);
      url.searchParams.set("scope", ABC_USER_SCOPES);
      return res.status(200).json({ authorizationUrl: url.toString(), environment: config.environment });
    } catch (error) {
      return sendAbcError(res, "abcOAuthStart failed", error);
    }
  }
);

exports.abcOAuthCallback = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const code = String((req.body && req.body.code) || req.query.code || "");
      const state = String((req.body && req.body.state) || req.query.state || "");
      if (!code || !state) return res.status(400).json({ error: "Missing ABC authorization code or state." });
      const stateData = await abcConsumeOAuthState(state);
      if (!stateData) return res.status(400).json({ error: "This ABC connection request is invalid or has already been used." });
      if (!stateData.expiresAt || abcTimestampMs(stateData.expiresAt) < Date.now()) {
        return res.status(400).json({ error: "This ABC connection request expired. Start again from Hail Money." });
      }
      const environment = String(stateData.environment || "sandbox");
      if (environment !== abcRuntimeEnvironment()) {
        throw makeHttpError(409, "This ABC authorization was started in a different environment. Start again.", "abc_environment_mismatch");
      }
      const tokens = await abcTokenRequest({
        grant_type: "authorization_code",
        redirect_uri: stateData.redirectUri || ABC_REDIRECT_URI,
        code
      }, environment);
      const expiresIn = Number(tokens.expires_in || 1800);
      const uid = String(stateData.uid || stateData.initiatedByUid || "").trim();
      const organizationId = normalizeOrganizationId(stateData.organizationId);
      if (!uid || !organizationId) return res.status(400).json({ error: "This ABC connection request is invalid." });
      await abcSetConnection(uid, {
        uid,
        organizationId,
        accessTokenEncrypted: encryptAbcToken(tokens.access_token),
        refreshTokenEncrypted: encryptAbcToken(tokens.refresh_token || null),
        tokenType: tokens.token_type || "Bearer",
        scope: tokens.scope || ABC_USER_SCOPES,
        expiresAt: new Date(Date.now() + expiresIn * 1000).toISOString(),
        environment,
        status: "connected",
        selectedShipToNumber: null,
        selectedBillToNumber: null,
        selectedAccountName: null,
        selectedBranchNumber: null,
        selectedBranchName: null,
        connectedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      }, false);
      await abcSetOnboarding(uid, {
        uid,
        organizationId,
        onboardingComplete: true,
        abcConnectionStatus: "connected",
        completedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      }, true);
      return res.status(200).json({ connected: true, connectionScope: "user", onboardingComplete: true, environment, returnOrigin: abcSafeReturnOrigin(stateData.returnOrigin) });
    } catch (error) {
      return sendAbcError(res, "abcOAuthCallback failed", error);
    }
  }
);

exports.abcConnectionStatus = onRequest(
  { secrets: [DATABASE_URL], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "GET") return res.status(405).json({ error: "GET required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const [connectionRecord, onboardingRecord] = await Promise.all([
        abcGetConnection(user.uid),
        abcGetOnboarding(user.uid)
      ]);
      const connection = connectionRecord || {};
      const onboarding = onboardingRecord || {};
      const environment = abcRuntimeEnvironment();
      const sameEnvironment = !!connectionRecord && String(connection.environment || "sandbox") === environment;
      const productionAccessRequired = user.organizationId === ABC_PRODUCTION_PILOT_ORGANIZATION_ID && environment !== "production";
      return res.status(200).json({
        connected: sameEnvironment && connection.status === "connected",
        connectionScope: "user",
        organizationName: user.organizationName,
        canManageConnection: true,
        onboardingComplete: onboarding.onboardingComplete === true,
        abcConnectionStatus: sameEnvironment && connection.status === "connected" ? "connected" : "not_connected",
        abcRequiredForRole: true,
        reauthorizationRequired: sameEnvironment && (!connection.refreshTokenEncrypted || connection.status === "reauthorization_required"),
        productionAccessRequired,
        environment,
        selection: sameEnvironment ? abcPublicSelection(connection) : null
      });
    } catch (error) {
      return sendAbcError(res, "abcConnectionStatus failed", error);
    }
  }
);

exports.abcDisconnect = onRequest(
  { secrets: [DATABASE_URL], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      await Promise.all([
        abcDeleteConnection(user.uid),
        abcSetOnboarding(user.uid, {
          uid: user.uid,
          organizationId: user.organizationId,
          abcConnectionStatus: "disconnected",
          updatedAt: new Date().toISOString()
        }, true)
      ]);
      return res.status(200).json({ connected: false, connectionScope: "user" });
    } catch (error) {
      return sendAbcError(res, "abcDisconnect failed", error);
    }
  }
);

exports.abcAccounts = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "GET") return res.status(405).json({ error: "GET required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const connection = await getAbcUserConnection(user.uid);
      const data = await getAbcUserAccounts(user.uid);
      const accounts = eligibleAbcAccounts(data);
      return res.status(200).json({
        connected: true,
        connectionScope: "user",
        canManageConnection: true,
        selected: {
          shipToNumber: String(connection.selectedShipToNumber || ""),
          branchNumber: String(connection.selectedBranchNumber || "")
        },
        shipTos: accounts.map((account) => ({
          shipToNumber: account.shipToNumber,
          name: account.name,
          status: account.status,
          isSellable: account.isSellable,
          address: account.address,
          branches: account.branches.map((branch) => ({
            branchNumber: branch.number,
            branchName: branch.name,
            homeBranch: branch.homeBranch
          }))
        }))
      });
    } catch (error) {
      return sendAbcError(res, "abcAccounts failed", error);
    }
  }
);

exports.abcSaveOrganizationSelection = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const shipToNumber = String(req.body && req.body.shipToNumber || "").trim();
      const branchNumber = String(req.body && req.body.branchNumber || "").trim();
      if (!shipToNumber || !branchNumber) {
        return res.status(400).json({ error: "Choose your ABC Supply account and branch." });
      }
      const rawAccounts = await getAbcUserAccounts(user.uid);
      const accounts = eligibleAbcAccounts(rawAccounts);
      const account = accounts.find((item) => item.shipToNumber === shipToNumber);
      const branch = account && account.branches.find((item) => item.number === branchNumber);
      if (!account || !branch) {
        throw makeHttpError(403, "That account or branch is not authorized for your ABC Supply user.", "abc_selection_not_authorized");
      }
      await abcSetConnection(user.uid, {
        selectedShipToNumber: account.shipToNumber,
        selectedBillToNumber: account.billToNumber || null,
        selectedAccountName: account.name,
        selectedBranchNumber: branch.number,
        selectedBranchName: branch.name,
        selectionUpdatedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      }, true);
      return res.status(200).json({
        connected: true,
        connectionScope: "user",
        selection: { accountName: account.name, branchName: branch.name }
      });
    } catch (error) {
      return sendAbcError(res, "abcSaveOrganizationSelection failed", error);
    }
  }
);

exports.abcEligibleBranchesForProperty = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const latitude = Number(req.body && req.body.latitude);
      const longitude = Number(req.body && req.body.longitude);
      if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) {
        return res.status(400).json({ error: "Verified property coordinates are required.", code: "property_coordinates_required" });
      }
      const connection = await getAbcUserConnection(user.uid);
      const resolved = await getAuthorizedBranchesNearProperty(user.uid, connection, latitude, longitude);
      return res.status(200).json({
        connected: true,
        organizationName: user.organizationName,
        accountName: resolved.account.name,
        branches: resolved.branches.slice(0, 10).map((branch, index) => publicAbcBranch(branch, index === 0)),
        recommendedBranch: resolved.recommended ? publicAbcBranch(resolved.recommended, true) : null
      });
    } catch (error) {
      return sendAbcError(res, "abcEligibleBranchesForProperty failed", error);
    }
  }
);

exports.abcPriceItems = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const body = req.body || {};
      const connection = await getAbcUserConnection(user.uid);
      const shipToNumber = String(connection.selectedShipToNumber || "").trim();
      let branchNumber = String(connection.selectedBranchNumber || "").trim();
      if (!shipToNumber || !branchNumber) {
        return res.status(409).json({ error: "Select your ABC Supply account and branch in Integrations.", code: "abc_selection_required" });
      }
      if (!Array.isArray(body.lines) || !body.lines.length) {
        return res.status(400).json({ error: "At least one item is required." });
      }
      if (body.lines.length > 50) return res.status(400).json({ error: "ABC allows at most 50 price lines per request." });
      const lines = body.lines.map((line, index) => {
        const value = line && typeof line === "object" ? line : {};
        const itemNumber = String(value.itemNumber || "").trim();
        const quantity = Number(value.quantity);
        const uom = String(value.uom || "").trim();
        if (!itemNumber || itemNumber.length > 80 || !Number.isInteger(quantity) || quantity <= 0) {
          throw makeHttpError(400, `Price line ${index + 1} requires a valid item number and whole-number quantity.`, "abc_price_line_invalid");
        }
        const normalized = { id: String(value.id || index + 1).slice(0, 80), itemNumber, quantity };
        if (uom) normalized.uom = uom.slice(0, 20);
        if (value.length && typeof value.length === "object") {
          const lengthValue = Number(value.length.value);
          const lengthUom = String(value.length.uom || "").trim();
          if (!(lengthValue > 0) || !lengthUom) {
            throw makeHttpError(400, `Price line ${index + 1} has an invalid dimensional length.`, "abc_price_line_invalid");
          }
          normalized.length = { value: lengthValue, uom: lengthUom.slice(0, 20) };
        }
        return normalized;
      });
      const latitude = Number(body.propertyLatitude);
      const longitude = Number(body.propertyLongitude);
      if (Number.isFinite(latitude) && latitude >= -90 && latitude <= 90 && Number.isFinite(longitude) && longitude >= -180 && longitude <= 180) {
        const resolved = await getAuthorizedBranchesNearProperty(user.uid, connection, latitude, longitude);
        if (resolved.recommended && resolved.recommended.number) branchNumber = resolved.recommended.number;
      }
      const payload = {
        requestId: body.requestId || `hail-money-${Date.now()}`,
        shipToNumber,
        branchNumber,
        purpose: ["estimating", "quoting", "ordering"].includes(body.purpose) ? body.purpose : "estimating",
        lines
      };
      const data = await abcApiRequest(user.uid, "/api/pricing/v2/prices", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      if (data && typeof data === "object" && !Array.isArray(data)) {
        const { shipToNumber: _shipToNumber, branchNumber: _branchNumber, ...safePricing } = data;
        return res.status(200).json(safePricing);
      }
      return res.status(200).json(data);
    } catch (error) {
      return sendAbcError(res, "abcPriceItems failed", error);
    }
  }
);

exports.abcFavoriteItems = onRequest(
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "GET") return res.status(405).json({ error: "GET required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const connection = await getAbcUserConnection(user.uid);
      const billToNumber = String(connection.selectedBillToNumber || connection.selectedShipToNumber || "").trim();
      const branchNumber = String(connection.selectedBranchNumber || "").trim();
      if (!billToNumber || !branchNumber) return res.status(409).json({ error: "Select your ABC Supply account and branch in Integrations.", code: "abc_selection_required" });
      const query = new URLSearchParams({ itemsPerPage: "50", pageNumber: "1" });
      query.set("branchNumber", branchNumber);
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
  { secrets: [DATABASE_URL, ABC_CLIENT_ID, ABC_CLIENT_SECRET, ABC_TOKEN_ENCRYPTION_KEY], timeoutSeconds: 30, memory: "256MiB" },
  async (req, res) => {
    setAbcCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
    try {
      const user = await requireAbcOrganizationUser(req);
      const connection = await getAbcUserConnection(user.uid);
      const search = String((req.body && req.body.search) || "").trim();
      let branchNumber = String(connection.selectedBranchNumber || "").trim();
      if (search.length < 2) return res.status(400).json({ error: "Enter at least two characters to search products." });
      if (!branchNumber) return res.status(409).json({ error: "Select your ABC Supply account and branch in Integrations.", code: "abc_selection_required" });
      const latitude = Number(req.body && req.body.propertyLatitude);
      const longitude = Number(req.body && req.body.propertyLongitude);
      if (Number.isFinite(latitude) && latitude >= -90 && latitude <= 90 && Number.isFinite(longitude) && longitude >= -180 && longitude <= 180) {
        const resolved = await getAuthorizedBranchesNearProperty(user.uid, connection, latitude, longitude);
        if (resolved.recommended && resolved.recommended.number) branchNumber = resolved.recommended.number;
      }
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
