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
    'Access-Control-Allow-Headers': 'Authorization,Content-Type,X-HM-File-Meta',
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

function publicSigningKey(token) {
  return 'public_sign:' + String(token || '').trim();
}

function parseSigningSessionValue(value) {
  try { return JSON.parse(String(value || '{}')); }
  catch (_) { return null; }
}

function makeSigningToken() {
  return (globalThis.crypto.randomUUID() + globalThis.crypto.randomUUID()).replace(/-/g, '');
}

async function getPublicSigningSession(token) {
  token = String(token || '').trim();
  if (!/^[a-zA-Z0-9_-]{40,160}$/.test(token)) return null;
  const result = await pool.query(
    "SELECT org_id,key,value,updated_at FROM hm_app_state WHERE key=$1 LIMIT 1",
    [publicSigningKey(token)]
  );
  if (!result.rows.length) return null;
  const row = result.rows[0];
  const session = parseSigningSessionValue(row.value);
  if (!session || !Array.isArray(session.contracts)) return null;
  return { row, session };
}

function publicSigningExpired(session) {
  const expires = Date.parse(String(session && session.expiresAt || ''));
  return !!expires && Date.now() > expires;
}

async function route(request) {
  const url = new URL(request.url);
  const origin = request.headers.get('origin') || '';
  if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors(origin) });
  if (url.pathname === '/health') return json({ ok: true, service: 'hail-money-cloud', storage: BUCKET }, 200, origin);

  const publicSessionMatch = url.pathname.match(/^\/public\/signing\/([a-zA-Z0-9_-]{40,160})$/);
  if (publicSessionMatch && request.method === 'GET') {
    const token = publicSessionMatch[1];
    const found = await getPublicSigningSession(token);
    if (!found) return json({ error: 'This signing link is invalid or no longer available.' }, 404, origin);
    const { row, session } = found;
    if (publicSigningExpired(session)) return json({ error: 'This signing link has expired.' }, 410, origin);
    const contracts = [];
    for (let i = 0; i < session.contracts.length; i++) {
      const contract = session.contracts[i] || {};
      contracts.push({
        index: i,
        templateId: String(contract.templateId || ''),
        name: String(contract.name || contract.fileName || ('Contract ' + (i + 1))),
        fileName: String(contract.fileName || 'Contract.pdf'),
        fileType: String(contract.fileType || 'application/pdf'),
        fields: Array.isArray(contract.fields) ? contract.fields : [],
        completed: !!contract.completedFileId,
        signedAt: String(contract.signedAt || ''),
        sourceUrl: url.origin + '/public/signing/' + encodeURIComponent(token) + '/contracts/' + i + '/source'
      });
    }
    return json({
      ok: true,
      session: {
        id: String(session.id || ''),
        status: String(session.status || 'pending'),
        homeownerName: String(session.homeownerName || ''),
        expiresAt: String(session.expiresAt || ''),
        completedAt: String(session.completedAt || ''),
        contracts
      }
    }, 200, origin);
  }

  const publicSourceMatch = url.pathname.match(/^\/public\/signing\/([a-zA-Z0-9_-]{40,160})\/contracts\/(\d+)\/source$/);
  if (publicSourceMatch && request.method === 'GET') {
    const token = publicSourceMatch[1];
    const index = Number(publicSourceMatch[2]);
    const found = await getPublicSigningSession(token);
    if (!found) return json({ error: 'This signing link is invalid or no longer available.' }, 404, origin);
    const { row, session } = found;
    if (publicSigningExpired(session)) return json({ error: 'This signing link has expired.' }, 410, origin);
    const contract = session.contracts[index];
    if (!contract) return json({ error: 'That contract is not part of this signing request.' }, 404, origin);
    const sourceFileId = String(contract.sourceFileId || '').trim();
    const fileRow = await pool.query(
      "SELECT * FROM hm_files WHERE id=$1 AND org_id=$2 AND status='active' LIMIT 1",
      [sourceFileId, row.org_id]
    );
    if (!fileRow.rows.length) return json({ error: 'The contract source document is unavailable.' }, 404, origin);
    const source = fileRow.rows[0];
    const object = await s3.send(new GetObjectCommand({ Bucket: source.bucket, Key: source.object_key }));
    const bytes = await object.Body.transformToByteArray();
    return new Response(bytes, {
      status: 200,
      headers: {
        ...cors(origin),
        'Content-Type': source.mime_type || 'application/pdf',
        'Content-Length': String(bytes.byteLength),
        'Cache-Control': 'private, no-store',
        'Content-Disposition': 'inline; filename="' + String(source.file_name || 'contract.pdf').replace(/"/g, '') + '"'
      }
    });
  }

  const publicUploadMatch = url.pathname.match(/^\/public\/signing\/([a-zA-Z0-9_-]{40,160})\/contracts\/(\d+)$/);
  if (publicUploadMatch && request.method === 'POST') {
    const token = publicUploadMatch[1];
    const index = Number(publicUploadMatch[2]);
    const found = await getPublicSigningSession(token);
    if (!found) return json({ error: 'This signing link is invalid or no longer available.' }, 404, origin);
    const { row, session } = found;
    if (publicSigningExpired(session)) return json({ error: 'This signing link has expired.' }, 410, origin);
    if (String(session.status || 'pending') === 'completed') return json({ error: 'This signing request has already been completed.' }, 409, origin);
    const contract = session.contracts[index];
    if (!contract) return json({ error: 'That contract is not part of this signing request.' }, 404, origin);
    if (contract.completedFileId) return json({ error: 'That contract has already been signed.' }, 409, origin);
    const maxBytes = 25 * 1024 * 1024;
    if (Number(request.headers.get('content-length') || 0) > maxBytes) return json({ error: 'Signed contract exceeds 25 MB maximum.' }, 413, origin);
    const bytes = new Uint8Array(await request.arrayBuffer());
    if (!bytes.length || bytes.byteLength > maxBytes) return json({ error: 'Signed contract is empty or too large.' }, 413, origin);

    const now = new Date().toISOString();
    const id = cleanSegment('signed_contract_' + String(session.id || '') + '_' + index + '_' + Date.now(), 'signed-contract-' + Date.now());
    const baseName = cleanSegment(String(contract.name || contract.fileName || 'Contract'), 'Contract').replace(/\.pdf$/i, '');
    const fileName = ('signed-' + baseName + '.pdf').slice(0, 240);
    const objectKey = [
      cleanSegment(row.org_id, 'company'),
      cleanSegment(String(session.leadId || ''), 'lead'),
      'signed_contract',
      id,
      cleanSegment(fileName, 'signed-contract.pdf')
    ].join('/');
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: objectKey,
      Body: bytes,
      ContentType: 'application/pdf'
    }));
    const metadata = {
      remoteSigning: true,
      remoteSigningSessionId: String(session.id || ''),
      templateId: String(contract.templateId || ''),
      contractIndex: index,
      expectedCount: session.contracts.length,
      remoteSigningComplete: false
    };
    const inserted = await pool.query(
      `INSERT INTO hm_files(id,org_id,lead_id,type,category,file_name,mime_type,size_bytes,bucket,object_key,note,uploaded_by,uploaded_by_email,status,metadata)
       VALUES($1,$2,$3,'signed_contract','Contract',$4,'application/pdf',$5,$6,$7,$8,$9,$10,'active',$11::jsonb)
       RETURNING *`,
      [
        id, row.org_id, String(session.leadId || ''), fileName, bytes.byteLength, BUCKET, objectKey,
        'Locked signed contract — ' + String(contract.name || 'Contract'),
        String(session.homeownerName || 'Homeowner'),
        String(session.homeownerEmail || ''),
        JSON.stringify(metadata)
      ]
    );
    contract.completedFileId = id;
    contract.signedAt = now;
    contract.completed = true;
    const allComplete = session.contracts.every((item) => !!(item && item.completedFileId));
    if (allComplete) {
      session.status = 'completed';
      session.completedAt = now;
      await pool.query(
        "UPDATE hm_files SET metadata = metadata || $1::jsonb WHERE id=$2 AND org_id=$3",
        [JSON.stringify({ remoteSigningComplete: true }), id, row.org_id]
      );
    }
    await pool.query(
      "UPDATE hm_app_state SET value=$1,updated_at=now(),updated_by=$2 WHERE org_id=$3 AND key=$4",
      [JSON.stringify(session), 'public-signing', row.org_id, publicSigningKey(token)]
    );
    await audit(
      { orgId: row.org_id, uid: 'public-signing', email: String(session.homeownerEmail || '') },
      'contract.remote_sign',
      'file',
      id,
      { leadId: String(session.leadId || ''), sessionId: String(session.id || ''), contractIndex: index, complete: allComplete }
    );
    return json({ ok: true, complete: allComplete, file: fileMeta(inserted.rows[0]) }, 200, origin);
  }

  const user = await authenticate(request);

  if (url.pathname === '/signing-sessions' && request.method === 'POST') {
    const body = await parseJson(request);
    const leadId = String(body.leadId || '').trim().slice(0, 160);
    const homeownerName = String(body.homeownerName || '').trim().slice(0, 240);
    const homeownerEmail = String(body.homeownerEmail || '').trim().toLowerCase().slice(0, 320);
    const contractRows = Array.isArray(body.contracts) ? body.contracts : [];
    if (!leadId) return json({ error: 'Lead id is required.' }, 400, origin);
    if (!homeownerEmail || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(homeownerEmail)) return json({ error: 'A valid homeowner email is required.' }, 400, origin);
    if (!contractRows.length || contractRows.length > 20) return json({ error: 'Choose between 1 and 20 contracts.' }, 400, origin);

    const contracts = [];
    for (let i = 0; i < contractRows.length; i++) {
      const item = contractRows[i] || {};
      const sourceFileId = String(item.sourceFileId || '').trim().slice(0, 240);
      if (!sourceFileId) return json({ error: 'Every contract must have a stored source document.' }, 400, origin);
      const source = await pool.query(
        "SELECT id,file_name,mime_type,type,category FROM hm_files WHERE id=$1 AND org_id=$2 AND status='active' LIMIT 1",
        [sourceFileId, user.orgId]
      );
      if (!source.rows.length) return json({ error: 'A selected contract source document could not be found.' }, 404, origin);
      const sourceRow = source.rows[0];
      if (String(sourceRow.mime_type || '').toLowerCase() !== 'application/pdf') return json({ error: 'Remote signing currently requires PDF contracts.' }, 400, origin);
      const safeFields = (Array.isArray(item.fields) ? item.fields : []).map((field) => ({
        key: String(field && field.key || '').slice(0, 100),
        label: String(field && field.label || '').slice(0, 240),
        pageIndex: Math.max(0, Number(field && field.pageIndex || 0) || 0),
        xPct: Number(field && field.xPct || 0),
        yPct: Number(field && field.yPct || 0),
        wPct: Number(field && field.wPct || 0),
        hPct: Number(field && field.hPct || 0),
        x: Number(field && field.x || 0),
        y: Number(field && field.y || 0),
        width: Number(field && field.width || 0),
        height: Number(field && field.height || 0),
        value: field && (field.value === true || field.value === false) ? field.value : String(field && field.value || '').slice(0, String(field && field.key || '') === 'rep_signature' ? 500000 : 10000)
      }));
      contracts.push({
        templateId: String(item.templateId || '').trim().slice(0, 240),
        name: String(item.name || sourceRow.file_name || ('Contract ' + (i + 1))).trim().slice(0, 240),
        fileName: String(item.fileName || sourceRow.file_name || 'Contract.pdf').trim().slice(0, 240),
        fileType: 'application/pdf',
        sourceFileId,
        fields: safeFields,
        completedFileId: '',
        signedAt: ''
      });
    }

    const token = makeSigningToken();
    const sessionId = 'sign_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10);
    const expiresAt = new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString();
    const session = {
      id: sessionId,
      leadId,
      homeownerName,
      homeownerEmail,
      requestedBy: user.displayName || user.email || '',
      requestedByEmail: user.email || '',
      status: 'pending',
      createdAt: new Date().toISOString(),
      expiresAt,
      completedAt: '',
      contracts
    };
    await pool.query(
      'INSERT INTO hm_app_state(org_id,key,value,updated_at,updated_by) VALUES($1,$2,$3,now(),$4) ON CONFLICT(org_id,key) DO UPDATE SET value=EXCLUDED.value,updated_at=now(),updated_by=EXCLUDED.updated_by',
      [user.orgId, publicSigningKey(token), JSON.stringify(session), user.email || user.uid]
    );
    await audit(user, 'contract.remote_sign_request', 'signing_session', sessionId, { leadId, homeownerEmail, count: contracts.length });
    return json({
      ok: true,
      token,
      sessionId,
      expiresAt,
      signingUrl: 'https://www.hail.money/sign.html?t=' + encodeURIComponent(token)
    }, 200, origin);
  }

  if (url.pathname === '/signing-sessions' && request.method === 'GET') {
    const leadId = String(url.searchParams.get('leadId') || '').trim();
    const result = await pool.query(
      "SELECT key,value,updated_at FROM hm_app_state WHERE org_id=$1 AND key LIKE 'public_sign:%' ORDER BY updated_at DESC",
      [user.orgId]
    );
    const sessions = result.rows.map((row) => {
      const session = parseSigningSessionValue(row.value);
      if (!session || (leadId && String(session.leadId || '') !== leadId)) return null;
      return {
        id: String(session.id || ''),
        leadId: String(session.leadId || ''),
        homeownerName: String(session.homeownerName || ''),
        homeownerEmail: String(session.homeownerEmail || ''),
        status: String(session.status || 'pending'),
        createdAt: String(session.createdAt || ''),
        expiresAt: String(session.expiresAt || ''),
        completedAt: String(session.completedAt || ''),
        contractCount: Array.isArray(session.contracts) ? session.contracts.length : 0,
        completedCount: Array.isArray(session.contracts) ? session.contracts.filter((item) => !!(item && item.completedFileId)).length : 0
      };
    }).filter(Boolean);
    return json({ ok: true, sessions }, 200, origin);
  }

  if (url.pathname === '/hail-dates' && request.method === 'GET') {
    const result = await pool.query(
      `SELECT event_date
         FROM (
           SELECT DISTINCT event_date FROM hail_lsr_raw
           UNION
           SELECT DISTINCT event_date FROM storm_lsr_raw
           UNION
           SELECT DISTINCT event_date FROM storm_polygons
           UNION
           SELECT DISTINCT event_date FROM hail_radar_days
         ) dates
         WHERE event_date IS NOT NULL
         ORDER BY event_date DESC
         LIMIT 1500`
    );
    return json({ dates: result.rows.map((row) => String(row.event_date).slice(0, 10)) }, 200, origin);
  }

  if (url.pathname === '/hail-lsr-by-date' && request.method === 'GET') {
    const date = String(url.searchParams.get('date') || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'Invalid date. Use date=YYYY-MM-DD' }, 400, origin);
    let result = await pool.query(
      'SELECT lat,lon,hail_in,event_time FROM hail_lsr_raw WHERE event_date=$1 ORDER BY event_time ASC',
      [date]
    );
    if (!result.rows.length) {
      result = await pool.query(
        'SELECT lat,lon,hail_in,event_time FROM hail_reports WHERE event_date=$1 ORDER BY event_time ASC',
        [date]
      );
    }
    return json({ points: result.rows }, 200, origin);
  }

  if (url.pathname === '/storm-lsr-by-date' && request.method === 'GET') {
    const date = String(url.searchParams.get('date') || '').trim();
    const stormType = String(url.searchParams.get('type') || '').trim().toLowerCase();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'Invalid date. Use date=YYYY-MM-DD' }, 400, origin);
    if (stormType !== 'wind' && stormType !== 'tornado') return json({ error: 'Invalid type. Use type=wind or type=tornado' }, 400, origin);
    const result = await pool.query(
      'SELECT lat,lon,magnitude,magnitude_unit,event_time FROM storm_lsr_raw WHERE event_date=$1 AND event_type=$2 ORDER BY event_time ASC',
      [date, stormType]
    );
    return json({ points: result.rows, storm_type: stormType }, 200, origin);
  }

  if (url.pathname === '/storm-polygons-by-date' && request.method === 'GET') {
    const date = String(url.searchParams.get('date') || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return json({ error: 'Invalid date. Use date=YYYY-MM-DD' }, 400, origin);
    const result = await pool.query(
      `SELECT id,event_date,storm_type,source,source_product,source_priority,quality_status,swath_index,
              polygon_geojson,centroid_lat,centroid_lon,area_sq_mi,threshold_value,band_min,band_max,band_label,
              event_start_utc,event_end_utc,metadata_json
         FROM storm_polygons
        WHERE event_date=$1
        ORDER BY source_priority ASC, swath_index ASC NULLS LAST`,
      [date]
    );
    return json({ polygons: result.rows }, 200, origin);
  }

  if (url.pathname === '/state/version' && request.method === 'GET') {
    const [stateVersion, fileVersion] = await Promise.all([
      pool.query("SELECT COUNT(*)::bigint AS count,MAX(updated_at) AS updated FROM hm_app_state WHERE org_id=$1 AND key NOT LIKE 'public_sign:%'", [user.orgId]),
      pool.query("SELECT COUNT(*)::bigint AS count,MAX(uploaded_at) AS updated FROM hm_files WHERE org_id=$1 AND status='active'", [user.orgId])
    ]);
    const a = stateVersion.rows[0] || {};
    const b = fileVersion.rows[0] || {};
    const version = [String(a.count || 0), a.updated ? new Date(a.updated).toISOString() : '', String(b.count || 0), b.updated ? new Date(b.updated).toISOString() : ''].join('|');
    return json({ ok: true, version }, 200, origin);
  }

  if (url.pathname === '/state' && request.method === 'GET') {
    const result = await pool.query("SELECT key,value,updated_at FROM hm_app_state WHERE org_id=$1 AND key NOT LIKE 'public_sign:%' ORDER BY key", [user.orgId]);
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

  if (url.pathname === '/files/upload' && request.method === 'POST') {
    const encoded = String(request.headers.get('x-hm-file-meta') || '').trim();
    if (!encoded || encoded.length > 16000) return json({ error: 'File metadata is required.' }, 400, origin);
    let body;
    try { body = JSON.parse(Buffer.from(encoded, 'base64').toString('utf8')); }
    catch (_) { return json({ error: 'File metadata is invalid.' }, 400, origin); }

    const id = cleanSegment(body.id || ('lead_doc_' + Date.now() + '_' + Math.random().toString(36).slice(2, 9)), 'file-' + Date.now());
    const leadId = String(body.leadId || '').trim().slice(0, 160);
    const type = String(body.type || 'document').trim().slice(0, 100) || 'document';
    const category = String(body.category || body.docCategory || '').trim().slice(0, 160);
    const fileName = String(body.fileName || 'document').trim().slice(0, 240) || 'document';
    const mimeType = String(body.mimeType || request.headers.get('content-type') || 'application/octet-stream').trim().slice(0, 160);
    const note = String(body.note || '').trim().slice(0, 2000);
    const maxBytes = 25 * 1024 * 1024;
    if (Number(request.headers.get('content-length') || 0) > maxBytes) return json({ error: 'File exceeds 25 MB maximum.' }, 413, origin);
    const bytes = new Uint8Array(await request.arrayBuffer());
    if (!bytes.length || bytes.byteLength > maxBytes) return json({ error: 'File is empty or exceeds 25 MB maximum.' }, 413, origin);
    const objectKey = [
      cleanSegment(user.orgId, 'company'),
      cleanSegment(leadId || '_company', '_company'),
      cleanSegment(type, 'document'),
      id,
      cleanSegment(fileName, 'document')
    ].join('/');

    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: objectKey,
      Body: bytes,
      ContentType: mimeType
    }));
    try {
      const result = await pool.query(
        `INSERT INTO hm_files(id,org_id,lead_id,type,category,file_name,mime_type,size_bytes,bucket,object_key,note,uploaded_by,uploaded_by_email,status,metadata)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'active',$14::jsonb)
         ON CONFLICT(id) DO UPDATE SET
           lead_id=EXCLUDED.lead_id,type=EXCLUDED.type,category=EXCLUDED.category,
           file_name=EXCLUDED.file_name,mime_type=EXCLUDED.mime_type,size_bytes=EXCLUDED.size_bytes,
           bucket=EXCLUDED.bucket,object_key=EXCLUDED.object_key,note=EXCLUDED.note,
           uploaded_by=EXCLUDED.uploaded_by,uploaded_by_email=EXCLUDED.uploaded_by_email,
           status='active',metadata=EXCLUDED.metadata,uploaded_at=now()
         WHERE hm_files.org_id=EXCLUDED.org_id RETURNING *`,
        [
          id, user.orgId, leadId, type, category, fileName, mimeType, bytes.byteLength,
          BUCKET, objectKey, note, String(body.uploadedBy || user.displayName || ''), user.email,
          JSON.stringify(body.metadata || {})
        ]
      );
      if (!result.rows.length) throw Object.assign(new Error('File ID belongs to a different company.'), { status: 409 });
      await audit(user, 'file.upload', 'file', id, { leadId, type, size: bytes.byteLength });
      return json({ ok: true, file: fileMeta(result.rows[0]) }, 200, origin);
    } catch (error) {
      try { await s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: objectKey })); } catch (_) {}
      throw error;
    }
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

  const blobMatch = url.pathname.match(/^\/files\/([^/]+)\/blob$/);
  if (blobMatch && request.method === 'GET') {
    const id = decodeURIComponent(blobMatch[1]);
    const found = await pool.query(
      "SELECT * FROM hm_files WHERE id=$1 AND org_id=$2 AND status='active'",
      [id, user.orgId]
    );
    if (!found.rows.length) return json({ error:'File was not found.' }, 404, origin);
    const row = found.rows[0];
    const object = await s3.send(new GetObjectCommand({ Bucket:row.bucket, Key:row.object_key }));
    const bytes = await object.Body.transformToByteArray();
    return new Response(bytes, {
      status:200,
      headers:{
        ...cors(origin),
        'Content-Type':row.mime_type || 'application/octet-stream',
        'Content-Length':String(bytes.byteLength),
        'Cache-Control':'private, max-age=180'
      }
    });
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
