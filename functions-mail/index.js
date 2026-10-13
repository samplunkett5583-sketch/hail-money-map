const { onRequest } = require("firebase-functions/v2/https");
const { defineSecret } = require("firebase-functions/params");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const nodemailer = require("nodemailer");

if (!admin.apps.length) admin.initializeApp();
const db = admin.firestore();

const BOOTSTRAP_USER = defineSecret("HM_SMTP_USER");
const BOOTSTRAP_CREDENTIAL = defineSecret("HM_SMTP_PASSWORD");
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function cors(req, res) {
  const origin = String(req.get("origin") || "");
  const ok = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com|hail\.money|www\.hail\.money)$/i.test(origin)
    || /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (ok) res.set("Access-Control-Allow-Origin", origin);
  res.set("Vary", "Origin");
  res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  res.set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
}

function clean(value, max) {
  return String(value == null ? "" : value).replace(/[\r\n]+/g, " ").trim().slice(0, max || 320);
}

async function userFromRequest(req) {
  const match = String(req.get("authorization") || "").match(/^Bearer\s+(.+)$/i);
  if (!match) throw Object.assign(new Error("Sign in is required."), { statusCode: 401 });
  const decoded = await admin.auth().verifyIdToken(match[1]);
  let orgId = clean(decoded.hmOrganizationId, 120).toLowerCase();
  let role = clean(decoded.hmRole, 60);
  let active = decoded.employee === true;

  if ((!orgId || !role || !active) && decoded.uid) {
    const snap = await db.collection("hmEmployees").doc(decoded.uid).get();
    if (snap.exists) {
      const row = snap.data() || {};
      orgId = orgId || clean(row.organizationId || row.hmOrganizationId, 120).toLowerCase();
      role = role || clean(row.role, 60);
      active = active || row.active !== false;
    }
  }

  if (!active || !orgId) {
    throw Object.assign(new Error("Approved Hail Money employee access is required."), { statusCode: 403 });
  }

  return {
    uid: clean(decoded.uid || decoded.sub, 160),
    email: clean(decoded.email, 320).toLowerCase(),
    orgId,
    role
  };
}

function canManage(role) {
  const value = String(role || "").trim().toLowerCase();
  return value === "admin" || value === "owner";
}

function preset(provider) {
  provider = String(provider || "").toLowerCase();
  if (provider === "spacemail") return { host:"mail.spacemail.com", port:465, security:"ssl" };
  if (provider === "gmail") return { host:"smtp.gmail.com", port:465, security:"ssl" };
  if (provider === "microsoft") return { host:"smtp.office365.com", port:587, security:"starttls" };
  return null;
}

function normalize(input, existing) {
  input = input || {};
  existing = existing || {};
  const provider = clean(input.provider || existing.provider || "custom", 40).toLowerCase();
  const defaults = preset(provider);
  const fromEmail = clean(input.fromEmail || existing.fromEmail, 320).toLowerCase();
  const fromName = clean(input.fromName || existing.fromName || "Hail Money", 160);
  const smtpUser = clean(input.smtpUser || existing.smtpUser || fromEmail, 320);
  const host = clean((defaults && defaults.host) || input.host || existing.host, 240).toLowerCase();
  const port = Number((defaults && defaults.port) || input.port || existing.port || 0);
  const security = clean((defaults && defaults.security) || input.security || existing.security || "ssl", 30).toLowerCase();

  if (!EMAIL_RE.test(fromEmail)) throw Object.assign(new Error("Enter a valid company email address."), { statusCode: 400 });
  if (!smtpUser) throw Object.assign(new Error("Enter the mailbox username."), { statusCode: 400 });
  if (!host || !/^[a-z0-9.-]+$/i.test(host)) throw Object.assign(new Error("Enter a valid outgoing mail server."), { statusCode: 400 });
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw Object.assign(new Error("Enter a valid outgoing mail port."), { statusCode: 400 });
  if (security !== "ssl" && security !== "starttls") throw Object.assign(new Error("Email security must be SSL or STARTTLS."), { statusCode: 400 });

  return { provider, fromEmail, fromName, smtpUser, host, port, security };
}

function mailer(settings, credential) {
  const ssl = settings.security === "ssl";
  return nodemailer.createTransport({
    host: settings.host,
    port: settings.port,
    secure: ssl,
    requireTLS: !ssl,
    auth: { user: settings.smtpUser, pass: credential },
    tls: { minVersion:"TLSv1.2", servername:settings.host },
    connectionTimeout:15000,
    greetingTimeout:15000,
    socketTimeout:30000
  });
}

function docRef(orgId) {
  return db.collection("hmCompanyMail").doc(orgId);
}

async function loadRecord(orgId) {
  const snap = await docRef(orgId).get();
  return snap.exists ? (snap.data() || {}) : null;
}

async function bootstrapYoPro(user, record) {
  if (record || user.orgId !== "yopro") return record;
  const smtpUser = String(BOOTSTRAP_USER.value() || "").trim();
  const credential = String(BOOTSTRAP_CREDENTIAL.value() || "");
  if (!EMAIL_RE.test(smtpUser) || !credential) return null;

  const settings = normalize({
    provider:"spacemail",
    fromEmail:smtpUser,
    fromName:"YoPro Construction",
    smtpUser:smtpUser
  }, {});

  await mailer(settings, credential).verify();
  const now = new Date().toISOString();
  const next = {
    ...settings,
    credential,
    configured:true,
    configuredAt:now,
    updatedAt:now,
    updatedBy:user.email || user.uid
  };
  await docRef(user.orgId).set(next, { merge:false });
  return next;
}

function publicStatus(record) {
  if (!record) return { configured:false };
  return {
    configured:record.configured === true,
    provider:String(record.provider || ""),
    fromEmail:String(record.fromEmail || ""),
    fromName:String(record.fromName || ""),
    smtpUser:String(record.smtpUser || ""),
    host:String(record.host || ""),
    port:Number(record.port || 0),
    security:String(record.security || ""),
    updatedAt:String(record.updatedAt || "")
  };
}

function fail(res, error, fallback) {
  const status = Number(error && error.statusCode) || 500;
  logger.error(fallback, {
    status,
    code:String(error && error.code || "").slice(0,80),
    message:String(error && error.message || "unknown").slice(0,300)
  });
  return res.status(status).json({
    error: status >= 500 ? fallback : String(error && error.message || fallback)
  });
}

const options = {
  region:"us-central1",
  timeoutSeconds:60,
  memory:"256MiB",
  cors:false,
  secrets:[BOOTSTRAP_USER, BOOTSTRAP_CREDENTIAL]
};

exports.hmCompanyEmailStatus = onRequest(options, async (req, res) => {
  cors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "GET" && req.method !== "POST") return res.status(405).json({ error:"GET or POST required." });

  try {
    const user = await userFromRequest(req);
    let record = await loadRecord(user.orgId);
    record = await bootstrapYoPro(user, record);
    return res.status(200).json(publicStatus(record));
  } catch (error) {
    return fail(res, error, "Company email status could not be loaded.");
  }
});

exports.hmConfigureCompanyEmail = onRequest(options, async (req, res) => {
  cors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST" && req.method !== "PUT") return res.status(405).json({ error:"POST or PUT required." });

  try {
    const user = await userFromRequest(req);
    if (!canManage(user.role)) return res.status(403).json({ error:"Admin permission is required to configure company email." });

    const existing = await loadRecord(user.orgId);
    const settings = normalize(req.body || {}, existing || {});
    const credential = String((req.body && req.body.credential) || (existing && existing.credential) || "");
    if (!credential) return res.status(400).json({ error:"Enter the mailbox password or app password." });

    await mailer(settings, credential).verify();

    const now = new Date().toISOString();
    const record = {
      ...settings,
      credential,
      configured:true,
      configuredAt:(existing && existing.configuredAt) || now,
      updatedAt:now,
      updatedBy:user.email || user.uid
    };
    await docRef(user.orgId).set(record, { merge:false });

    logger.info("Company email configured", {
      orgId:user.orgId,
      provider:record.provider,
      fromEmail:record.fromEmail,
      updatedBy:record.updatedBy
    });

    return res.status(200).json(publicStatus(record));
  } catch (error) {
    return fail(res, error, "Company email could not be connected.");
  }
});

exports.hmSendLeadEmail = onRequest(options, async (req, res) => {
  cors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST") return res.status(405).json({ error:"POST required." });

  try {
    const user = await userFromRequest(req);
    const body = req.body || {};
    const to = clean(body.to, 320).toLowerCase();
    const subject = clean(body.subject, 220);
    const message = String(body.message || "").trim().slice(0,20000);
    const homeownerName = clean(body.recipientName, 160);
    const leadId = clean(body.leadId, 160);

    if (!EMAIL_RE.test(to)) return res.status(400).json({ error:"A valid recipient email is required." });
    if (!subject) return res.status(400).json({ error:"Email subject is required." });
    if (!message) return res.status(400).json({ error:"Email message is required." });

    let record = await loadRecord(user.orgId);
    record = await bootstrapYoPro(user, record);
    if (!record || record.configured !== true || !record.credential) {
      return res.status(409).json({
        error:"Company email delivery is not configured. An Admin must connect the company mailbox in Settings > Company Info."
      });
    }

    const info = await mailer(record, record.credential).sendMail({
      from:{ name:record.fromName || "Hail Money", address:record.fromEmail },
      to:homeownerName ? { name:homeownerName, address:to } : to,
      replyTo:record.fromEmail,
      subject,
      text:message
    });

    const accepted = Array.isArray(info.accepted) ? info.accepted.map(String) : [];
    const rejected = Array.isArray(info.rejected) ? info.rejected.map(String) : [];
    const acceptedRecipient = accepted.some((value) => value.toLowerCase() === to.toLowerCase());

    if (!acceptedRecipient || rejected.some((value) => value.toLowerCase() === to.toLowerCase())) {
      return res.status(502).json({ error:"The company mail server did not accept the homeowner email address." });
    }

    logger.info("Lead email accepted by company SMTP", {
      orgId:user.orgId,
      from:record.fromEmail,
      to,
      leadId,
      uid:user.uid,
      messageId:String(info.messageId || "")
    });

    return res.status(200).json({
      sent:true,
      accepted:true,
      from:record.fromEmail,
      messageId:String(info.messageId || "")
    });
  } catch (error) {
    return fail(res, error, "The email could not be sent from Hail Money.");
  }
});
