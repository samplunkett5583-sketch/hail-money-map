import { Pool } from 'pg';
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { createRemoteJWKSet, jwtVerify } from 'jose';

const FIREBASE_PROJECT_ID = 'hailmoneymap';
const BUCKET = 'hail-money-files';
const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
const jwks = createRemoteJWKSet(new URL('https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com'));
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  endpoint: process.env.AWS_ENDPOINT_URL_S3,
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || ''
  }
});

const ALLOWED_ORIGINS = new Set([
  'https://www.hail.money',
  'https://hail.money',
  'https://hailmoneymap.web.app',
  'https://hailmoneymap.firebaseapp.com',
  'http://localhost',
  'http://127.0.0.1'
]);

function cors(origin) {
  const exact = origin && ALLOWED_ORIGINS.has(origin);
  const local = origin && /^http:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/i.test(origin);
  return {
    'Access-Control-Allow-Origin': exact || local ? origin : 'https://www.hail.money',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
    'Access-Control-Allow-Headers': 'Authorization,Content-Type',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin'
  };
}

function json(data, status = 200, origin = '') {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...cors(origin), 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }
  });
}

function cleanSegment(value, fallback = 'file') {
  const out = String(value || '').trim().replace(/[^a-zA-Z0-9._-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 120);
  return out || fallback;
}

async function authenticate(request) {
  const auth = String(request.headers.get('authorization') || '');
  if (!/^Bearer\s+/i.test(auth)) throw Object.assign(new Error('Authentication required.'), { status: 401 });
  const token = auth.replace(/^Bearer\s+/i, '').trim();
  const verified = await jwtVerify(token, jwks, {
    issuer: 'https://securetoken.google.com/' + FIREBASE_PROJECT_ID,
    audience: FIREBASE_PROJECT_ID
  });
  const p = verified.payload || {};
  const email = String(p.email || '').trim().toLowerCase();
  let orgId = String(p.hmOrganizationId || '').trim().toLowerCase();
  if (!orgId && (/@hailmoney\.test$/i.test(email) || /@yoproconstruction\.com$/i.test(email) || email === 'samplunkett5583@gmail.com')) orgId = 'yopro';
  if (!orgId) throw Object.assign(new Error('No Hail Money company is assigned to this account.'), { status: 403 });
  return {
    uid: String(p.sub || ''),
    email,
    displayName: String(p.name || p.displayName || email || ''),
    role: String(p.hmRole || ''),
    orgId
  };
}

async function audit(user, action, entityType = '', entityId = '', detail = {}) {
  try {
    await pool.query(
      'INSERT INTO hm_audit_events(org_id,actor_uid,actor_email,action,entity_type,entity_id,detail) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)',
      [user.orgId, user.uid, user.email, action, entityType, entityId, JSON.stringify(detail || {})]
    );
  } catch (_) {}
}

function fileMeta(row) {
  return {
    id: row.id,
    leadId: row.lead_id || '',
    type: row.type || 'document',
    category: row.category || '',
    docCategory: row.category || '',
    fileName: row.file_name,
    name: row.file_name,
    mimeType: row.mime_type || 'application/octet-stream',
    size: Number(row.size_bytes || 0),
    note: row.note || '',
    uploadedBy: row.uploaded_by || '',
    uploadedByEmail: row.uploaded_by_email || '',
    uploadedAt: row.uploaded_at ? new Date(row.uploaded_at).toISOString() : '',
    storageProvider: 'neon',
    storagePath: row.object_key,
    objectKey: row.object_key,
    status: row.status,
    metadata: row.metadata || {}
  };
}

async function parseJson(request) {
  try { return await request.json(); }
  catch (_) { throw Object.assign(new Error('Invalid JSON request.'), { status: 400 }); }
}

async function route(request) {
  const url = new URL(request.url);
  const origin = request.headers.get('origin') || '';
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors(origin) });
  if (url.pathname === '/health') return json({ ok: true, service: 'hail-money-cloud', storage: BUCKET }, 200, origin);

  const user = await authenticate(request);

  if (url.pathname === '/state' && request.method === 'GET') {
    const result = await pool.query('SELECT key,value,updated_at FROM hm_app_state WHERE org_id=$1 ORDER BY key', [user.orgId]);
    const state = {};
    let latest = '';
    for (const row of result.rows) {
      state[row.key] = row.value;
      const ts = row.updated_at ? new Date(row.updated_at).toISOString() : '';
      if (ts > latest) latest = ts;
    }
    return json({ ok: true, orgId: user.orgId, state, updatedAt: latest }, 200, origin);
  }

  if (url.pathname === '/state' && request.method === 'PUT') {
    const body = await parseJson(request);
    const key = String(body.key || '').trim();
    const value = typeof body.value === 'string' ? body.value : JSON.stringify(body.value == null ? null : body.value);
    if (!key || key.length > 240) return json({ error: 'Invalid state key.' }, 400, origin);
    if (value.length > 8 * 1024 * 1024) return json({ error: 'State value is too large.' }, 413, origin);
    await pool.query(
      'INSERT INTO hm_app_state(org_id,key,value,updated_at,updated_by) VALUES($1,$2,$3,now(),$4) ON CONFLICT(org_id,key) DO UPDATE SET value=EXCLUDED.value,updated_at=now(),updated_by=EXCLUDED.updated_by',
      [user.orgId, key, value, user.email || user.uid]
    );
    await audit(user, 'state.upsert', 'state', key);
    return json({ ok: true, key }, 200, origin);
  }

  if (url.pathname === '/state/batch' && request.method === 'PUT') {
    const body = await parseJson(request);
    const entries = body && typeof body.entries === 'object' && body.entries ? body.entries : {};
    const keys = Object.keys(entries);
    if (keys.length > 300) return json({ error: 'Too many state entries.' }, 400, origin);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const key of keys) {
        if (!key || key.length > 240) continue;
        const raw = entries[key];
        const value = typeof raw === 'string' ? raw : JSON.stringify(raw == null ? null : raw);
        if (value.length > 8 * 1024 * 1024) throw Object.assign(new Error('State value is too large.'), { status: 413 });
        await client.query(
          'INSERT INTO hm_app_state(org_id,key,value,updated_at,updated_by) VALUES($1,$2,$3,now(),$4) ON CONFLICT(org_id,key) DO UPDATE SET value=EXCLUDED.value,updated_at=now(),updated_by=EXCLUDED.updated_by',
          [user.orgId, key, value, user.email || user.uid]
        );
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
    await audit(user, 'state.batch', 'state', '', { count: keys.length });
    return json({ ok: true, count: keys.length }, 200, origin);
  }

  if (url.pathname === '/state' && request.method === 'DELETE') {
    const key = String(url.searchParams.get('key') || '').trim();
    if (!key) return json({ error: 'State key is required.' }, 400, origin);
    await pool.query('DELETE FROM hm_app_state WHERE org_id=$1 AND key=$2', [user.orgId, key]);
    await audit(user, 'state.delete', 'state', key);
    return json({ ok: true }, 200, origin);
  }

  if (url.pathname === '/files' && request.method === 'GET') {
    const leadId = String(url.searchParams.get('leadId') || '').trim();
    const type = String(url.searchParams.get('type') || '').trim();
    const params = [user.orgId];
    let where = 'org_id=$1 AND status=\'active\'';
    if (leadId) { params.push(leadId); where += ' AND lead_id=$' + params.length; }
    if (type) { params.push(type); where += ' AND type=$' + params.length; }
    const result = await pool.query('SELECT * FROM hm_files WHERE ' + where + ' ORDER BY uploaded_at DESC', params);
    return json({ ok: true, files: result.rows.map(fileMeta) }, 200, origin);
  }

  if (url.pathname === '/files/init' && request.method === 'POST') {
    const body = await parseJson(request);
    const id = cleanSegment(body.id || ('lead_doc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9)), 'file-' + Date.now());
    const leadId = String(body.leadId || '').trim().slice(0, 160);
    const type = String(body.type || 'document').trim().slice(0, 100) || 'document';
    const category = String(body.category || body.docCategory || '').trim().slice(0, 160);
    const fileName = String(body.fileName || body.name || 'document').trim().slice(0, 240) || 'document';
    const mimeType = String(body.mimeType || body.contentType || 'application/octet-stream').trim().slice(0, 160);
    const size = Math.max(0, Number(body.size || 0) || 0);
    const note = String(body.note || '').trim().slice(0, 2000);
    const objectKey = [
      cleanSegment(user.orgId, 'company'),
      cleanSegment(leadId || '_company', '_company'),
      cleanSegment(type, 'document'),
      id,
      cleanSegment(fileName, 'document')
    ].join('/');

    await pool.query(
      `INSERT INTO hm_files(id,org_id,lead_id,type,category,file_name,mime_type,size_bytes,bucket,object_key,note,uploaded_by,uploaded_by_email,status,metadata)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending',$14::jsonb)
       ON CONFLICT(id) DO UPDATE SET lead_id=EXCLUDED.lead_id,type=EXCLUDED.type,category=EXCLUDED.category,file_name=EXCLUDED.file_name,mime_type=EXCLUDED.mime_type,size_bytes=EXCLUDED.size_bytes,object_key=EXCLUDED.object_key,note=EXCLUDED.note,uploaded_by=EXCLUDED.uploaded_by,uploaded_by_email=EXCLUDED.uploaded_by_email,status='pending',metadata=EXCLUDED.metadata`,
      [id, user.orgId, leadId, type, category, fileName, mimeType, size, BUCKET, objectKey, note, String(body.uploadedBy || user.displayName || ''), user.email, JSON.stringify(body.metadata || {})]
    );

    const uploadUrl = await getSignedUrl(s3, new PutObjectCommand({
      Bucket: BUCKET,
      Key: objectKey,
      ContentType: mimeType
    }), { expiresIn: 900 });

    return json({ ok: true, id, objectKey, uploadUrl, contentType: mimeType }, 200, origin);
  }

  if (url.pathname === '/files/complete' && request.method === 'POST') {
    const body = await parseJson(request);
    const id = String(body.id || '').trim();
    if (!id) return json({ error: 'File id is required.' }, 400, origin);
    const found = await pool.query('SELECT * FROM hm_files WHERE id=$1 AND org_id=$2', [id, user.orgId]);
    if (!found.rows.length) return json({ error: 'File was not found.' }, 404, origin);
    const row = found.rows[0];
    let head;
    try { head = await s3.send(new HeadObjectCommand({ Bucket: row.bucket, Key: row.object_key })); }
    catch (_) { return json({ error: 'Uploaded file could not be verified.' }, 409, origin); }
    const actualSize = Number(head.ContentLength || row.size_bytes || 0);
    const updated = await pool.query(
      'UPDATE hm_files SET status=\'active\',size_bytes=$1,uploaded_at=now() WHERE id=$2 AND org_id=$3 RETURNING *',
      [actualSize, id, user.orgId]
    );
    await audit(user, 'file.complete', 'file', id, { leadId: row.lead_id, type: row.type, objectKey: row.object_key });
    return json({ ok: true, file: fileMeta(updated.rows[0]) }, 200, origin);
  }

  const urlMatch = url.pathname.match(/^\/files\/([^/]+)\/url$/);
  if (urlMatch && request.method === 'GET') {
    const id = decodeURIComponent(urlMatch[1]);
    const found = await pool.query('SELECT * FROM hm_files WHERE id=$1 AND org_id=$2 AND status=\'active\'', [id, user.orgId]);
    if (!found.rows.length) return json({ error: 'File was not found.' }, 404, origin);
    const row = found.rows[0];
    const downloadUrl = await getSignedUrl(s3, new GetObjectCommand({
      Bucket: row.bucket,
      Key: row.object_key,
      ResponseContentType: row.mime_type,
      ResponseContentDisposition: 'inline; filename="' + String(row.file_name || 'document').replace(/"/g, '') + '"'
    }), { expiresIn: 900 });
    return json({ ok: true, url: downloadUrl, file: fileMeta(row) }, 200, origin);
  }

  const deleteMatch = url.pathname.match(/^\/files\/([^/]+)$/);
  if (deleteMatch && request.method === 'DELETE') {
    const id = decodeURIComponent(deleteMatch[1]);
    const found = await pool.query('SELECT * FROM hm_files WHERE id=$1 AND org_id=$2', [id, user.orgId]);
    if (!found.rows.length) return json({ ok: true }, 200, origin);
    const row = found.rows[0];
    try { await s3.send(new DeleteObjectCommand({ Bucket: row.bucket, Key: row.object_key })); } catch (_) {}
    await pool.query('DELETE FROM hm_files WHERE id=$1 AND org_id=$2', [id, user.orgId]);
    await audit(user, 'file.delete', 'file', id, { leadId: row.lead_id, type: row.type });
    return json({ ok: true }, 200, origin);
  }

  return json({ error: 'Not found.' }, 404, origin);
}

export default {
  async fetch(request) {
    const origin = request.headers.get('origin') || '';
    try { return await route(request); }
    catch (error) {
      console.error('[hail-money-cloud]', error);
      return json({ error: String(error && error.message || 'Cloud request failed.') }, Number(error && error.status || 500), origin);
    }
  }
};
