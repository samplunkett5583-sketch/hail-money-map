const { onRequest } = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

if (!admin.apps.length) admin.initializeApp();
const db = admin.firestore();
const MAX_BYTES = 25 * 1024 * 1024;
const CHUNK_BYTES = 512 * 1024;
const MANAGER_ROLES = ["owner", "admin", "manager"];
const FILES_COLLECTION = "_hmDocumentFiles";

function permitCors(req, res) {
  const origin = String(req.get("origin") || "");
  const allowed = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com|hail\.money|www\.hail\.money)$/i.test(origin) ||
    /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (allowed) res.set("Access-Control-Allow-Origin", origin);
  res.set("Vary", "Origin");
  res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
  res.set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
}

function safe(value) {
  return String(value || "x").replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 120) || "x";
}
async function requireEmployee(req) {
  const authHeader = String(req.get("authorization") || "");
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) throw Object.assign(new Error("Sign in is required."), { statusCode: 401 });
  const decoded = await admin.auth().verifyIdToken(match[1]);
  let orgId = String(decoded.hmOrganizationId || "").trim().toLowerCase();
  let role = String(decoded.hmRole || "").trim();
  let active = decoded.employee === true;
  if ((!orgId || !role || !active) && decoded.uid) {
    const snap = await db.collection("hmEmployees").doc(decoded.uid).get();
    if (snap.exists) {
      const data = snap.data() || {};
      orgId = orgId || String(data.organizationId || data.hmOrganizationId || "").trim().toLowerCase();
      role = role || String(data.role || "").trim();
      active = active || data.active !== false;
    }
  }
  if (!active || !orgId) throw Object.assign(new Error("Approved Hail Money employee access is required."), { statusCode: 403 });
  return { uid: decoded.uid, email: decoded.email || "", orgId, role };
}
function canManage(role) {
  return MANAGER_ROLES.includes(String(role || "").trim().toLowerCase());
}
function validatePath(path, employee) {
  path = String(path || "").trim();
  const companyPrefix = "company-documents/" + safe(employee.orgId) + "/";
  const leadPrefix = "lead-documents/" + safe(employee.orgId) + "/";
  if (!path || (path.indexOf(companyPrefix) !== 0 && path.indexOf(leadPrefix) !== 0)) {
    throw Object.assign(new Error("That document does not belong to this company."), { statusCode: 403 });
  }
  return path;
}
function fileId(path) {
  return Buffer.from(String(path || ""), "utf8").toString("base64url");
}
function fileRef(path) {
  return db.collection(FILES_COLLECTION).doc(fileId(path));
}
async function clearChunks(ref) {
  const snap = await ref.collection("chunks").get();
  if (snap.empty) return;
  let batch = db.batch(), count = 0;
  for (const doc of snap.docs) {
    batch.delete(doc.ref); count++;
    if (count >= 400) { await batch.commit(); batch = db.batch(); count = 0; }
  }
  if (count) await batch.commit();
}
async function saveBuffer(path, body, meta) {
  const ref = fileRef(path);
  await clearChunks(ref);
  const chunkCount = Math.ceil(body.length / CHUNK_BYTES);
  let batch = db.batch(), pending = 0;
  for (let i = 0; i < chunkCount; i++) {
    const start = i * CHUNK_BYTES;
    const part = body.subarray(start, Math.min(body.length, start + CHUNK_BYTES));
    batch.set(ref.collection("chunks").doc(String(i).padStart(4, "0")), {
      index: i,
      data: part
    });
    pending++;
    if (pending >= 300) { await batch.commit(); batch = db.batch(); pending = 0; }
  }
  if (pending) await batch.commit();
  await ref.set(Object.assign({}, meta, {
    path,
    size: body.length,
    chunkCount,
    updatedAt: admin.firestore.FieldValue.serverTimestamp()
  }), { merge: false });
}
async function loadBuffer(path) {
  const ref = fileRef(path);
  const metaSnap = await ref.get();
  if (!metaSnap.exists) return null;
  const meta = metaSnap.data() || {};
  const chunks = await ref.collection("chunks").orderBy("index", "asc").get();
  const parts = chunks.docs.map(doc => {
    const value = (doc.data() || {}).data;
    if (Buffer.isBuffer(value)) return value;
    if (value && typeof value.toBuffer === "function") return value.toBuffer();
    return Buffer.from(value || []);
  });
  const body = Buffer.concat(parts);
  if (Number(meta.size || 0) && body.length !== Number(meta.size)) {
    throw new Error("Stored document is incomplete.");
  }
  return { meta, body };
}
async function deleteBuffer(path) {
  const ref = fileRef(path);
  await clearChunks(ref);
  await ref.delete();
}
function responseError(res, error) {
  const status = Number(error && error.statusCode) || 500;
  logger.error("Hail Money document storage failed", {
    status,
    code: String(error && error.code || "").slice(0, 80),
    message: String(error && error.message || "unknown").slice(0, 240)
  });
  return res.status(status).json({ error: error && error.message || "Document storage request failed." });
}
exports.hmDocumentStore = onRequest(
  { region: "us-central1", timeoutSeconds: 120, memory: "512MiB", cors: false },
  async (req, res) => {
    permitCors(req, res);
    if (req.method === "OPTIONS") return res.status(204).send("");
    try {
      const employee = await requireEmployee(req);
      const action = String(req.query.action || "").trim().toLowerCase();
      const scope = String(req.query.scope || "").trim().toLowerCase();

      if (action === "upload") {
        if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
        if (scope !== "company" && scope !== "lead") return res.status(400).json({ error: "Valid document scope is required." });
        if (scope === "company" && !canManage(employee.role)) return res.status(403).json({ error: "Owner, Admin, or Manager permission is required." });
        const body = Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.alloc(0);
        if (!body.length) return res.status(400).json({ error: "A document file is required." });
        if (body.length > MAX_BYTES) return res.status(413).json({ error: "Documents must be 25 MB or smaller." });
        const name = String(req.query.name || "document").trim().slice(0, 180) || "document";
        const contentType = String(req.get("content-type") || "application/octet-stream").split(";")[0].trim();
        const documentId = safe(req.query.documentId || ("doc_" + Date.now() + "_" + Math.random().toString(36).slice(2, 9)));
        let path;
        if (scope === "company") {
          path = "company-documents/" + safe(employee.orgId) + "/" + safe(req.query.category || "other") + "/" + documentId + "/" + safe(name);
        } else {
          const leadId = safe(req.query.leadId || "");
          if (!leadId) return res.status(400).json({ error: "Lead ID is required." });
          path = "lead-documents/" + safe(employee.orgId) + "/" + leadId + "/" + safe(req.query.type || "document") + "/" + documentId + "/" + safe(name);
        }
        await saveBuffer(path, body, {
          organizationId: employee.orgId,
          name,
          contentType,
          uploadedByUid: employee.uid,
          uploadedByEmail: employee.email
        });
        return res.status(200).json({
          id: documentId,
          storageProvider: "firebase",
          storagePath: path,
          name,
          contentType,
          size: body.length
        });
      }
      if (action === "download") {
        if (req.method !== "GET" && req.method !== "POST") return res.status(405).json({ error: "GET or POST required." });
        const path = validatePath(req.query.path, employee);
        const stored = await loadBuffer(path);
        if (!stored) return res.status(404).json({ error: "That document file could not be found." });
        res.set("Content-Type", String(stored.meta.contentType || "application/octet-stream"));
        res.set("Content-Length", String(stored.body.length));
        res.set("Cache-Control", "private,no-store,max-age=0");
        res.set("Content-Disposition", "inline; filename=\"" + safe(req.query.name || stored.meta.name || "document") + "\"");
        return res.status(200).send(stored.body);
      }

      if (action === "delete") {
        if (req.method !== "DELETE" && req.method !== "POST") return res.status(405).json({ error: "DELETE or POST required." });
        const path = validatePath(req.query.path, employee);
        if (path.indexOf("company-documents/") === 0 && !canManage(employee.role)) {
          return res.status(403).json({ error: "Owner, Admin, or Manager permission is required." });
        }
        await deleteBuffer(path);
        return res.status(200).json({ deleted: true });
      }
      return res.status(400).json({ error: "Unknown document storage action." });
    } catch (error) {
      return responseError(res, error);
    }
  }
);
