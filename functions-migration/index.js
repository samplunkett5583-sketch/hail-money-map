'use strict';
const { onRequest } = require('firebase-functions/v2/https');
const admin = require('firebase-admin');
const logger = require('firebase-functions/logger');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
admin.initializeApp();
const db = admin.firestore();

const CHUNK_SIZE = 420000;
const MAX_STREAM_RECORDS = 50000;
const HAIL_STAGES = ['New Lead','Lead','Contacted','Inspected','Claim Filed','Adjuster Appointment','Approved','Contract Signed','Production','Built','Invoiced','Completed','Paid / Closed'];

function permitCors(req, res) {
  const origin = String(req.get('origin') || '');
  const allowed = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com|hail\.money|www\.hail\.money)$/i.test(origin) || /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (allowed) res.set('Access-Control-Allow-Origin', origin);
  res.set('Vary', 'Origin');
  res.set('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
}

function httpError(statusCode, message) {
  return Object.assign(new Error(message), { statusCode });
}

async function requireFirebaseUser(req) {
  const header = String(req.get('authorization') || '');
  const match = header.match(/^Bearer\s+(.+)$/i);
  if (!match) throw httpError(401, 'Sign in is required.');
  return admin.auth().verifyIdToken(match[1]);
}
async function requireCompanyAdmin(req) {
  const user = await requireFirebaseUser(req);
  let role = String(user.hmRole || user.role || '').trim();
  let organizationId = String(user.hmOrganizationId || user.organizationId || '').trim().toLowerCase();
  try {
    const employee = await db.collection('hmEmployees').doc(user.uid).get();
    if (employee.exists) {
      const data = employee.data() || {};
      role = String(data.role || role || '').trim();
      organizationId = String(data.organizationId || data.hmOrganizationId || organizationId || '').trim().toLowerCase();
      if (data.active === false) throw httpError(403, 'This employee account is inactive.');
    }
  } catch (error) {
    if (error && error.statusCode) throw error;
  }
  if (!organizationId && /@hailmoney\.test$/i.test(String(user.email || ''))) organizationId = 'yopro';
  if (!organizationId) throw httpError(403, 'Company access could not be verified.');
  if (!/^(owner|admin)$/i.test(role)) {
    throw httpError(403, 'Only an Owner or Admin can run a CRM migration.');
  }
  return { user, role: role || 'Admin', organizationId };
}
async function requireCompanyMember(req) {
  const user = await requireFirebaseUser(req);
  let role = String(user.hmRole || user.role || '').trim();
  let organizationId = String(user.hmOrganizationId || user.organizationId || '').trim().toLowerCase();
  const employee = await db.collection('hmEmployees').doc(user.uid).get().catch(() => null);
  if (employee && employee.exists) {
    const data = employee.data() || {};
    if (data.active === false) throw httpError(403, 'This employee account is inactive.');
    role = String(data.role || role || '').trim();
    organizationId = String(data.organizationId || data.hmOrganizationId || organizationId || '').trim().toLowerCase();
  }
  if (!organizationId && /@hailmoney\.test$/i.test(String(user.email || ''))) organizationId = 'yopro';
  if (!organizationId) throw httpError(403, 'Company access could not be verified.');
  return { user, role, organizationId };
}

function cleanText(value, max = 2000) {
  return String(value == null ? '' : value).replace(/\s+/g, ' ').trim().slice(0, max);
}

function pick(record, keys) {
  record = record && typeof record === 'object' ? record : {};
  for (const key of keys) {
    const value = record[key];
    if (value != null && String(value).trim() !== '') return value;
  }
  return '';
}
function unixToIso(value) {
  if (value == null || value === '') return '';
  if (typeof value === 'number' || /^\d+$/.test(String(value))) {
    let num = Number(value);
    if (!Number.isFinite(num)) return '';
    if (num < 100000000000) num *= 1000;
    const date = new Date(num);
    return Number.isNaN(date.getTime()) ? '' : date.toISOString();
  }
  const date = new Date(String(value));
  return Number.isNaN(date.getTime()) ? cleanText(value, 100) : date.toISOString();
}

function relationIds(record) {
  const ids = [];
  const add = (value) => {
    const id = cleanText(value && typeof value === 'object' ? (value.id || value.jnid) : value, 180);
    if (id && !ids.includes(id)) ids.push(id);
  };
  if (record && record.primary) add(record.primary);
  if (record && Array.isArray(record.related)) record.related.forEach(add);
  add(record && record.customer);
  add(record && record.contact_id);
  return ids;
}

function firstPhone(record) {
  return cleanText(pick(record, ['mobile_phone','mobilePhone','phone','phone_number','home_phone','work_phone','phone1']), 80);
}

function assignedName(record) {
  const direct = cleanText(pick(record, ['sales_rep_name','assigned_to_name','owner_name','salesRepName','assignedRep']), 160);
  if (direct) return direct;
  const owners = record && Array.isArray(record.owners) ? record.owners : [];
  const first = owners[0] || null;
  return cleanText(first && (first.name || first.display_name || first.email), 160);
}

function recordId(record, prefix) {
  return cleanText(pick(record, ['jnid','id','recid','external_id','externalId']), 180) || (prefix + '_' + Math.random().toString(36).slice(2, 12));
}
function normalizeJnContact(record) {
  return {
    sourceId: recordId(record, 'contact'),
    firstName: cleanText(pick(record, ['first_name','firstName']), 120),
    lastName: cleanText(pick(record, ['last_name','lastName']), 120),
    displayName: cleanText(pick(record, ['display_name','displayName','name']), 240),
    companyName: cleanText(pick(record, ['company_name','companyName']), 240),
    phone: firstPhone(record),
    email: cleanText(pick(record, ['email','email_address','emailAddress']), 240),
    street: cleanText(pick(record, ['address_line1','address1','street','street_address']), 240),
    street2: cleanText(pick(record, ['address_line2','address2']), 160),
    city: cleanText(pick(record, ['city']), 120),
    state: cleanText(pick(record, ['state_text','state','state_code']), 80),
    zip: cleanText(pick(record, ['zip','zipcode','postal_code']), 40),
    status: cleanText(pick(record, ['status_name','status','workflow_status']), 160),
    leadSource: cleanText(pick(record, ['source_name','lead_source','leadSource']), 160),
    assignedRep: assignedName(record),
    notes: cleanText(pick(record, ['notes','note','description']), 4000),
    createdAt: unixToIso(pick(record, ['date_created','created_at','createdAt'])),
    updatedAt: unixToIso(pick(record, ['date_updated','updated_at','updatedAt']))
  };
}

function normalizeJnJob(record) {
  const primary = record && record.primary && typeof record.primary === 'object' ? record.primary : {};
  return {
    sourceId: recordId(record, 'job'),
    primaryId: cleanText(primary.id || primary.jnid || record.customer || record.contact_id, 180),
    name: cleanText(pick(record, ['name','display_name','job_name','number']), 240),
    phone: firstPhone(record),
    email: cleanText(pick(record, ['email','email_address']), 240),
    street: cleanText(pick(record, ['address_line1','address1','street']), 240),
    city: cleanText(pick(record, ['city']), 120),
    state: cleanText(pick(record, ['state_text','state','state_code']), 80),
    zip: cleanText(pick(record, ['zip','zipcode','postal_code']), 40),
    status: cleanText(pick(record, ['status_name','status','workflow_status']), 160),
    leadSource: cleanText(pick(record, ['source_name','lead_source']), 160),
    assignedRep: assignedName(record),
    notes: cleanText(pick(record, ['notes','note','description','scope']), 5000),
    createdAt: unixToIso(pick(record, ['date_created','created_at','createdAt'])),
    updatedAt: unixToIso(pick(record, ['date_updated','updated_at','updatedAt']))
  };
}
function normalizeJnTask(record) {
  return {
    sourceId: recordId(record, 'task'),
    title: cleanText(pick(record, ['title','name','description']), 500),
    recordType: cleanText(pick(record, ['record_type_name','type_name','type']), 120),
    relatedIds: relationIds(record),
    assignedTo: assignedName(record) || cleanText(pick(record, ['assigned_to','created_by_name']), 160),
    startAt: unixToIso(pick(record, ['date_start','start_at','startDate'])),
    endAt: unixToIso(pick(record, ['date_end','end_at','endDate'])),
    notes: cleanText(pick(record, ['notes','note']), 3000),
    createdAt: unixToIso(pick(record, ['date_created','created_at'])),
    updatedAt: unixToIso(pick(record, ['date_updated','updated_at']))
  };
}

function normalizeJnActivity(record) {
  return {
    sourceId: recordId(record, 'activity'),
    primaryId: cleanText(record && record.primary && (record.primary.id || record.primary.jnid), 180),
    relatedIds: relationIds(record),
    note: cleanText(pick(record, ['note','description','message']), 5000),
    recordType: cleanText(pick(record, ['record_type_name','activity_type_name','type']), 160),
    createdByName: cleanText(pick(record, ['created_by_name','createdByName']), 160),
    createdAt: unixToIso(pick(record, ['date_created','created_at','createdAt'])),
    isStatusChange: !!(record && record.is_status_change)
  };
}

function normalizeJnFile(record) {
  const typeValue = pick(record, ['type','file_type_code','type_id']);
  const name = cleanText(pick(record, ['filename','file_name','name','title']), 320);
  const mimeType = cleanText(pick(record, ['content_type','mime_type','mimeType']), 120);
  const extension = (name.split('.').pop() || '').toLowerCase();
  const isPhoto = /^image\//i.test(mimeType) || Number(typeValue) === 2 || /^(jpg|jpeg|png|webp|heic|heif|gif|tif|tiff)$/.test(extension);
  return {
    sourceId: recordId(record, 'file'),
    primaryId: cleanText(record && record.primary && (record.primary.id || record.primary.jnid), 180),
    relatedIds: relationIds(record),
    name, mimeType, isPhoto,
    fileTypeCode: cleanText(typeValue, 80),
    fileType: cleanText(pick(record, ['type_name','file_type','record_type_name','subtype']), 160),
    description: cleanText(pick(record, ['description','caption','note','notes']), 1000),
    size: Number(pick(record, ['size','file_size','bytes']) || 0) || 0,
    createdAt: unixToIso(pick(record, ['date_created','created_at','createdAt']))
  };
}

function normalizeJnUser(record) {
  return {
    sourceId: recordId(record, 'user'),
    firstName: cleanText(pick(record, ['first_name','firstName']), 120),
    lastName: cleanText(pick(record, ['last_name','lastName']), 120),
    email: cleanText(pick(record, ['email','email_address']), 240),
    sourceRole: cleanText(pick(record, ['role_name','role','access_profile_name','accessProfileName']), 160),
    active: record && record.is_active !== false
  };
}
async function jobNimbusRequest(apiKey, path, params) {
  const url = new URL('https://app.jobnimbus.com/api1/' + String(path || '').replace(/^\/+/, ''));
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value != null && value !== '') url.searchParams.set(key, String(value));
  });
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: 'Bearer ' + apiKey,
      Accept: 'application/json',
      'Content-Type': 'application/json'
    }
  });
  const bodyText = await response.text();
  let body = {};
  try { body = bodyText ? JSON.parse(bodyText) : {}; } catch (_) { body = {}; }
  if (!response.ok) {
    const message = cleanText(body && (body.message || body.error || body.title), 400) || ('JobNimbus returned HTTP ' + response.status + '.');
    throw httpError(response.status === 401 || response.status === 403 ? 401 : 502, message);
  }
  return body;
}

function extractStreamRows(payload, stream) {
  if (Array.isArray(payload)) return payload;
  const keysByStream = {
    contacts: ['results','contacts'], jobs: ['results','jobs'], tasks: ['results','tasks'],
    activities: ['activity','results','activities'], files: ['files','results']
  };
  const keys = keysByStream[stream] || ['results'];
  for (const key of keys) if (payload && Array.isArray(payload[key])) return payload[key];
  return [];
}

async function fetchJobNimbusStream(apiKey, stream) {
  const rows = [];
  let offset = 0;
  const pageSize = 1000;
  while (rows.length < MAX_STREAM_RECORDS) {
    const payload = await jobNimbusRequest(apiKey, stream, { from: offset, size: pageSize, sort_field: 'date_created', sort_direction: 'asc' });
    const page = extractStreamRows(payload, stream);
    rows.push(...page.slice(0, Math.max(0, MAX_STREAM_RECORDS - rows.length)));
    const count = Number(payload && payload.count);
    if (!page.length || page.length < pageSize || (Number.isFinite(count) && rows.length >= count)) break;
    offset += page.length;
  }
  return rows;
}
async function fetchJobNimbusUsers(apiKey) {
  try {
    const payload = await jobNimbusRequest(apiKey, 'account/users', {});
    return Array.isArray(payload && payload.users) ? payload.users : [];
  } catch (_) {
    return [];
  }
}


function nestedText(record, paths, max = 240) {
  for (const path of paths || []) {
    let value = record;
    for (const part of String(path).split('.')) value = value && typeof value === 'object' ? value[part] : undefined;
    if (value != null && typeof value !== 'object' && String(value).trim()) return cleanText(value, max);
  }
  return '';
}
function firstObjectText(value, keys, max = 160) {
  const list = Array.isArray(value) ? value : (value ? [value] : []);
  for (const item of list) {
    if (item == null) continue;
    if (typeof item !== 'object') { const direct = cleanText(item, max); if (direct) return direct; }
    for (const key of keys || []) { const found = item && item[key]; if (found != null && String(found).trim()) return cleanText(found, max); }
  }
  return '';
}
function providerRows(payload) {
  if (Array.isArray(payload)) return payload;
  for (const key of ['data','items','results','records','jobs','contacts','customers','users']) if (payload && Array.isArray(payload[key])) return payload[key];
  return [];
}
function providerTotal(payload) {
  const values = [payload && payload.total, payload && payload.totalCount, payload && payload.count, payload && payload.meta && payload.meta.pagination && payload.meta.pagination.total, payload && payload.pagination && payload.pagination.total];
  for (const value of values) { const n=Number(value); if (Number.isFinite(n) && n >= 0) return n; }
  return null;
}
async function providerGetJson(label, baseUrl, token, path, params) {
  const url = new URL(String(path || '').replace(/^\/+/, ''), baseUrl.replace(/\/?$/, '/'));
  Object.entries(params || {}).forEach(([key,value]) => {
    if (value == null || value === '') return;
    if (Array.isArray(value)) value.forEach((item) => url.searchParams.append(key, String(item)));
    else url.searchParams.set(key, String(value));
  });
  const response = await fetch(url, { method:'GET', headers:{ Authorization:'Bearer ' + token, Accept:'application/json' } });
  const text = await response.text(); let body = {};
  try { body = text ? JSON.parse(text) : {}; } catch (_) { body = {}; }
  if (!response.ok) {
    const message = cleanText(body && (body.message || body.error || body.title), 500) || (label + ' returned HTTP ' + response.status + '.');
    throw httpError(response.status === 401 || response.status === 403 ? 401 : 502, message);
  }
  return body;
}
async function fetchProviderPages(label, baseUrl, token, path, options) {
  options = options || {}; const rows = []; const seen = new Set(); const pageSize = Number(options.pageSize || 100); let page = Number(options.startPage || 0); let offset = 0;
  for (let guard=0; guard<2000 && rows.length<MAX_STREAM_RECORDS; guard+=1) {
    const params = Object.assign({}, options.params || {});
    params[options.sizeParam || 'limit'] = pageSize;
    if (options.offsetParam) params[options.offsetParam] = offset;
    else params[options.pageParam || 'page'] = page;
    const payload = await providerGetJson(label, baseUrl, token, path, params);
    const batch = providerRows(payload); let added = 0;
    batch.forEach((row) => {
      const key = cleanText(row && (row.id || row.jnid || row.uuid || row.recid), 180) || JSON.stringify(row).slice(0,500);
      if (!seen.has(key) && rows.length<MAX_STREAM_RECORDS) { seen.add(key); rows.push(row); added += 1; }
    });
    const total = providerTotal(payload);
    if (!batch.length || batch.length < pageSize || (total != null && rows.length >= total)) break;
    if (!added && guard > 3) break;
    offset += batch.length; page += 1;
  }
  return rows;
}
function normalizeAccuLynxContact(record) {
  const address = record && (record.mailingAddress || record.address || record.locationAddress) || {};
  return {
    sourceId: recordId(record,'contact'), firstName:cleanText(pick(record,['firstName','first_name']),120), lastName:cleanText(pick(record,['lastName','last_name']),120),
    displayName:cleanText(pick(record,['name','displayName']),240), companyName:cleanText(pick(record,['companyName','company_name']),240),
    phone:firstObjectText(record && (record.phoneNumbers || record.phoneNumber || record.phones),['number','phoneNumber','value'],80) || firstPhone(record),
    email:firstObjectText(record && (record.emailAddresses || record.emailAddress || record.emails),['emailAddress','address','value'],240) || cleanText(pick(record,['email','emailAddress']),240),
    street:nestedText(address,['street1','address1','line1','streetAddress'],240), street2:nestedText(address,['street2','address2','line2'],160),
    city:nestedText(address,['city'],120), state:nestedText(address,['state.abbreviation','state.name','state','stateCode'],80), zip:nestedText(address,['zipCode','zip','postalCode'],40),
    status:cleanText(pick(record,['status','contactStatus']),160), leadSource:nestedText(record,['leadSource.name','leadSourceName'],160),
    assignedRep:nestedText(record,['assignedTo.name','salesRep.name','representative.name'],160), notes:cleanText(pick(record,['note','notes','description']),4000),
    createdAt:unixToIso(pick(record,['createdDate','createdAt','created_at'])), updatedAt:unixToIso(pick(record,['modifiedDate','updatedAt','updated_at']))
  };
}
function normalizeAccuLynxJob(record) {
  const contact = record && record.contact || {}; const address = record && (record.address || record.jobAddress || record.locationAddress) || {};
  return {
    sourceId:recordId(record,'job'), primaryId:cleanText(contact.id || record.contactId || record.primaryContactId,180),
    name:cleanText(pick(record,['jobName','name','jobNumber','number']),240), phone:firstObjectText(contact.phoneNumbers || contact.phoneNumber,['number','phoneNumber','value'],80),
    email:firstObjectText(contact.emailAddresses || contact.emailAddress,['emailAddress','address','value'],240),
    street:nestedText(address,['street1','address1','line1','streetAddress'],240), city:nestedText(address,['city'],120),
    state:nestedText(address,['state.abbreviation','state.name','state','stateCode'],80), zip:nestedText(address,['zipCode','zip','postalCode'],40),
    status:nestedText(record,['milestone.name','currentMilestone.name','currentMilestone','milestone'],160) || cleanText(pick(record,['status','statusName']),160),
    leadSource:nestedText(record,['leadSource.name','leadSourceName'],160), assignedRep:nestedText(record,['assignedTo.name','salesRep.name','representative.name'],160),
    notes:cleanText(pick(record,['notes','note','description','scopeOfWork']),5000), createdAt:unixToIso(pick(record,['createdDate','createdAt','created_at'])),
    updatedAt:unixToIso(pick(record,['modifiedDate','updatedAt','updated_at']))
  };
}
function normalizeAccuLynxUser(record) {
  return { sourceId:recordId(record,'user'), firstName:cleanText(pick(record,['firstName','first_name']),120), lastName:cleanText(pick(record,['lastName','last_name']),120), email:cleanText(pick(record,['email','emailAddress']),240), sourceRole:nestedText(record,['role.name','roleName','permissionGroup.name'],160), active:!/^inactive|archived|deleted$/i.test(cleanText(pick(record,['status']),40)) && record.active !== false };
}
async function scanAccuLynx(access, body) {
  const apiKey=cleanText(body.apiKey,1200); if(apiKey.length<8) throw httpError(400,'Enter a valid AccuLynx API key.');
  const migrationId=makeMigrationId('acculynx'), ref=migrationRef(access.organizationId,migrationId), startedAt=new Date().toISOString();
  await ref.set({migrationId,provider:'acculynx',providerLabel:'AccuLynx',status:'scanning',organizationId:access.organizationId,startedAt,startedByUid:access.user.uid,startedByEmail:cleanText(access.user.email,240),connectionStored:false});
  const base='https://api.acculynx.com/api/v2/'; const warnings=[]; let contacts=[],jobs=[],users=[];
  try{contacts=await fetchProviderPages('AccuLynx',base,apiKey,'contacts',{pageSize:25,sizeParam:'pageSize',pageParam:'pageStartIndex',startPage:0,params:{includes:'emailAddress,phoneNumber'}});}catch(e){warnings.push('contacts: '+cleanText(e.message,300)); if(Number(e.statusCode)===401)throw e;}
  try{jobs=await fetchProviderPages('AccuLynx',base,apiKey,'jobs',{pageSize:25,sizeParam:'pageSize',offsetParam:'recordStartIndex',params:{includes:'contact',sortBy:'CreatedDate',sortOrder:'Ascending'}});}catch(e){warnings.push('jobs: '+cleanText(e.message,300));}
  try{users=await fetchProviderPages('AccuLynx',base,apiKey,'users',{pageSize:25,sizeParam:'pageSize',pageParam:'pageStartIndex',startPage:0,params:{status:'Active,Inactive,Archived'}});}catch(e){warnings.push('users: '+cleanText(e.message,300));}
  const staged=[]; contacts.forEach(r=>{const payload=normalizeAccuLynxContact(r||{});staged.push({kind:'contact',sourceId:payload.sourceId,payload});}); jobs.forEach(r=>{const payload=normalizeAccuLynxJob(r||{});staged.push({kind:'job',sourceId:payload.sourceId,payload});}); users.forEach(r=>{const payload=normalizeAccuLynxUser(r||{});staged.push({kind:'user',sourceId:payload.sourceId,payload});});
  await writeStageRecords(ref,staged); const contactRows=staged.filter(x=>x.kind==='contact').map(x=>x.payload), jobRows=staged.filter(x=>x.kind==='job').map(x=>x.payload); const uniq=rows=>Array.from(new Set(rows.map(r=>cleanText(r.status,160)).filter(Boolean))).sort();
  const summary={contacts:contactRows.length,jobs:jobRows.length,tasks:0,activities:0,files:0,users:users.length,total:staged.length};
  warnings.push('AccuLynx direct API migration covers contacts, jobs, pipeline status, and users. Photos/documents should be included with an AccuLynx export if the account API does not expose them.');
  await ref.set({status:'ready',readyAt:new Date().toISOString(),summary,warnings,statuses:{contacts:uniq(contactRows),jobs:uniq(jobRows)},sample:{contacts:contactRows.slice(0,3),jobs:jobRows.slice(0,3)}},{merge:true});
  return {migrationId,provider:'acculynx',providerLabel:'AccuLynx',summary,warnings,statuses:{contacts:uniq(contactRows),jobs:uniq(jobRows)},sample:{contacts:contactRows.slice(0,3),jobs:jobRows.slice(0,3)}};
}
function normalizeLeapContact(record) {
  const address=record && (record.address || record.property_address || record.billing) || {};
  return { sourceId:recordId(record,'contact'), firstName:cleanText(pick(record,['first_name','firstName']),120), lastName:cleanText(pick(record,['last_name','lastName']),120), displayName:cleanText(pick(record,['name','full_name']),240), companyName:cleanText(pick(record,['company_name']),240),
    phone:firstObjectText(record && (record.phones || record.phone_numbers),['number','phone','phone_number','value'],80) || firstPhone(record), email:cleanText(pick(record,['email','email_address']),240),
    street:nestedText(address,['address_line_1','address1','street','line1'],240), street2:nestedText(address,['address_line_2','address2','line2'],160), city:nestedText(address,['city'],120), state:nestedText(address,['state.name','state.abbreviation','state','state_code'],80), zip:nestedText(address,['zip','zip_code','postal_code'],40),
    status:'', leadSource:cleanText(pick(record,['referred_by_type','source','lead_source']),160), assignedRep:nestedText(record,['rep.name','representative.name'],160), notes:cleanText(pick(record,['note','notes']),4000), createdAt:unixToIso(pick(record,['created_at','createdAt'])), updatedAt:unixToIso(pick(record,['updated_at','updatedAt'])) };
}
function normalizeLeapJob(record, stageNames) {
  const customer=record && record.customer || {}, address=record && (record.address || record.job_address) || {}, stage=record && record.current_stage || {};
  const code=cleanText(stage.code || record.stage_code,160); return { sourceId:recordId(record,'job'), primaryId:cleanText(record.customer_id || customer.id,180), name:cleanText(pick(record,['name','number','job_number','alt_id']),240),
    phone:firstObjectText(customer.phones,['number','phone','value'],80), email:cleanText(customer.email || '',240), street:nestedText(address,['address_line_1','address1','street','line1'],240), city:nestedText(address,['city'],120), state:nestedText(address,['state.name','state.abbreviation','state','state_code'],80), zip:nestedText(address,['zip','zip_code','postal_code'],40),
    status:cleanText(stage.name || (stageNames && stageNames.get(code)) || record.stage_name || code,160), leadSource:'Leap / JobProgress', assignedRep:firstObjectText(record && (record.reps || record.estimators),['name','full_name','email'],160), notes:cleanText(pick(record,['description','note','notes']),5000), createdAt:unixToIso(pick(record,['created_at','createdAt'])), updatedAt:unixToIso(pick(record,['updated_at','updatedAt'])) };
}
function normalizeLeapUser(record) {
  return {sourceId:recordId(record,'user'),firstName:cleanText(pick(record,['first_name','firstName']),120),lastName:cleanText(pick(record,['last_name','lastName']),120),email:cleanText(pick(record,['email']),240),sourceRole:nestedText(record,['role.name','group.name','position'],160),active:record.active!==false && record.is_active!==false};
}
async function scanLeap(access, body) {
  const token=cleanText(body.apiKey,1800); if(token.length<8) throw httpError(400,'Enter a valid Leap / JobProgress access token.');
  const migrationId=makeMigrationId('leap'), ref=migrationRef(access.organizationId,migrationId), startedAt=new Date().toISOString();
  await ref.set({migrationId,provider:'leap',providerLabel:'Leap / JobProgress',status:'scanning',organizationId:access.organizationId,startedAt,startedByUid:access.user.uid,startedByEmail:cleanText(access.user.email,240),connectionStored:false});
  const base='https://api.jobprogress.com/api/v3/'; const warnings=[]; let stages=[],customers=[],jobs=[],users=[];
  try{stages=await fetchProviderPages('Leap / JobProgress',base,token,'workflow/stages',{pageSize:100,startPage:1,params:{sort_by:'position',sort_order:'asc'}});}catch(e){warnings.push('workflow: '+cleanText(e.message,300));}
  const stageNames=new Map(stages.map(x=>[cleanText(x && x.code,160),cleanText(x && x.name,160)]));
  try{customers=await fetchProviderPages('Leap / JobProgress',base,token,'customers',{pageSize:100,startPage:1,params:{'includes[]':['phones','address','rep']}});}catch(e){warnings.push('customers: '+cleanText(e.message,300)); if(Number(e.statusCode)===401)throw e;}
  try{jobs=await fetchProviderPages('Leap / JobProgress',base,token,'jobs',{pageSize:100,startPage:1,params:{'includes[]':['address','reps','customer','insurance_details']}});}catch(e){warnings.push('jobs: '+cleanText(e.message,300));}
  try{users=await fetchProviderPages('Leap / JobProgress',base,token,'company/users',{pageSize:100,startPage:1,params:{with_inactive:true}});}catch(e){warnings.push('users: '+cleanText(e.message,300));}
  const staged=[]; customers.forEach(r=>{const payload=normalizeLeapContact(r||{});staged.push({kind:'contact',sourceId:payload.sourceId,payload});}); jobs.forEach(r=>{const payload=normalizeLeapJob(r||{},stageNames);staged.push({kind:'job',sourceId:payload.sourceId,payload});}); users.forEach(r=>{const payload=normalizeLeapUser(r||{});staged.push({kind:'user',sourceId:payload.sourceId,payload});});
  await writeStageRecords(ref,staged); const contactRows=staged.filter(x=>x.kind==='contact').map(x=>x.payload),jobRows=staged.filter(x=>x.kind==='job').map(x=>x.payload),uniq=rows=>Array.from(new Set(rows.map(r=>cleanText(r.status,160)).filter(Boolean))).sort();
  const summary={contacts:contactRows.length,jobs:jobRows.length,tasks:0,activities:0,files:0,users:users.length,total:staged.length};
  warnings.push('Leap / JobProgress direct migration covers customers, jobs, workflow stages, and users. Files and long-form history can also be brought over with the guided export path.');
  await ref.set({status:'ready',readyAt:new Date().toISOString(),summary,warnings,statuses:{contacts:uniq(contactRows),jobs:uniq(jobRows)},sample:{contacts:contactRows.slice(0,3),jobs:jobRows.slice(0,3)}},{merge:true});
  return {migrationId,provider:'leap',providerLabel:'Leap / JobProgress',summary,warnings,statuses:{contacts:uniq(contactRows),jobs:uniq(jobRows)},sample:{contacts:contactRows.slice(0,3),jobs:jobRows.slice(0,3)}};
}

function migrationRef(organizationId, migrationId) {
  return db.collection('organizations').doc(organizationId).collection('migrations').doc(migrationId);
}

function makeMigrationId(provider) {
  return 'mig_' + cleanText(provider || 'crm', 20).toLowerCase().replace(/[^a-z0-9]+/g, '_') + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
}

async function writeStageRecords(ref, records) {
  const col = ref.collection('records');
  let batch = db.batch();
  let ops = 0;
  let written = 0;
  for (const record of records) {
    const doc = col.doc();
    batch.set(doc, {
      kind: cleanText(record.kind, 40),
      sourceId: cleanText(record.sourceId || record.payload && record.payload.sourceId, 180),
      payload: record.payload || {},
      stagedAt: new Date().toISOString()
    });
    ops += 1;
    written += 1;
    if (ops >= 350) {
      await batch.commit();
      batch = db.batch();
      ops = 0;
    }
  }
  if (ops) await batch.commit();
  return written;
}

async function clearStageRecords(ref) {
  while (true) {
    const snap = await ref.collection('records').limit(400).get();
    if (snap.empty) break;
    const batch = db.batch();
    snap.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();
  }
}

function safeFileName(value) {
  const raw = cleanText(value || 'file', 320).replace(/[\\/:*?"<>|]+/g, '_').replace(/\s+/g, ' ').trim();
  return raw || 'file';
}
function migrationFileId(sourceId) { return safeSlug(sourceId || ('file_' + Math.random().toString(36).slice(2,10))); }
async function prepareFileTransfers(ref, files) {
  const col = ref.collection('fileTransfers');
  let batch = db.batch(), ops = 0;
  for (const file of files || []) {
    const fileId = migrationFileId(file.sourceId);
    batch.set(col.doc(fileId), Object.assign({}, file, { fileId, status: 'pending', transferError: '', storagePath: '', copiedAt: '' }), { merge: false });
    ops += 1;
    if (ops >= 300) { await batch.commit(); batch = db.batch(); ops = 0; }
  }
  if (ops) await batch.commit();
}
async function fileTransferCounts(ref) {
  const snap = await ref.collection('fileTransfers').get();
  const counts = { total: snap.size, pending: 0, copying: 0, copied: 0, failed: 0, failed_final: 0 };
  snap.docs.forEach((doc) => { const status = cleanText((doc.data() || {}).status, 40) || 'pending'; if (counts[status] != null) counts[status] += 1; });
  return counts;
}
async function recoverStaleFileTransfers(ref) {
  const snap = await ref.collection('fileTransfers').where('status', '==', 'copying').limit(100).get();
  if (snap.empty) return 0;
  const cutoff = Date.now() - (15 * 60 * 1000);
  const batch = db.batch();
  let changed = 0;
  snap.docs.forEach((doc) => {
    const data = doc.data() || {};
    const started = Date.parse(data.transferStartedAt || '');
    if (!Number.isFinite(started) || started < cutoff) {
      batch.set(doc.ref, { status: 'pending', transferError: '', recoveredAt: new Date().toISOString() }, { merge: true });
      changed += 1;
    }
  });
  if (changed) await batch.commit();
  return changed;
}
async function downloadJobNimbusFile(apiKey, sourceId) {
  const response = await fetch('https://app.jobnimbus.com/files/' + encodeURIComponent(sourceId), {
    method: 'GET', headers: { Authorization: 'Bearer ' + apiKey, Accept: '*/*' }, redirect: 'follow'
  });
  if (!response.ok || !response.body) throw httpError(response.status === 401 || response.status === 403 ? 401 : 502, 'JobNimbus file download returned HTTP ' + response.status + '.');
  return response;
}
async function copyJobNimbusFile(access, migrationId, transferRef, data, apiKey) {
  const response = await downloadJobNimbusFile(apiKey, data.sourceId);
  const contentType = cleanText(response.headers.get('content-type') || data.mimeType || 'application/octet-stream', 160);
  const name = safeFileName(data.name || ('jobnimbus_' + data.sourceId));
  const storagePath = ['crm-migrations', safeSlug(access.organizationId), safeSlug(migrationId), 'files', transferRef.id, name].join('/');
  const cloudFile = admin.storage().bucket().file(storagePath);
  const writer = cloudFile.createWriteStream({ resumable: false, metadata: { contentType, metadata: { organizationId: access.organizationId, migrationId, sourceCRM: 'JobNimbus', sourceRecordId: String(data.sourceId || '') } } });
  await pipeline(Readable.fromWeb(response.body), writer);
  const metadata = await cloudFile.getMetadata().then((r) => r[0]).catch(() => ({}));
  await transferRef.set({ status: 'copied', storagePath, mimeType: contentType, size: Number(metadata.size || data.size || 0), copiedAt: new Date().toISOString(), transferError: '' }, { merge: true });
  return { storagePath, contentType };
}
async function scanJobNimbus(access, body) {
  const apiKey = cleanText(body.apiKey, 1200);
  if (apiKey.length < 8) throw httpError(400, 'Enter a valid JobNimbus API key.');
  const migrationId = makeMigrationId('jobnimbus');
  const ref = migrationRef(access.organizationId, migrationId);
  const startedAt = new Date().toISOString();
  await ref.set({
    migrationId, provider: 'jobnimbus', providerLabel: 'JobNimbus', status: 'scanning',
    organizationId: access.organizationId, startedAt, startedByUid: access.user.uid,
    startedByEmail: cleanText(access.user.email, 240), connectionStored: false
  });

  const streams = {};
  const warnings = [];
  const streamNames = ['contacts','jobs','tasks','activities','files'];
  for (const stream of streamNames) {
    try { streams[stream] = await fetchJobNimbusStream(apiKey, stream); }
    catch (error) {
      streams[stream] = [];
      warnings.push(stream + ': ' + cleanText(error && error.message, 300));
      if (stream === 'contacts' && Number(error && error.statusCode) === 401) throw error;
    }
  }
  const users = await fetchJobNimbusUsers(apiKey);
  const normalizers = {
    contacts: normalizeJnContact, jobs: normalizeJnJob, tasks: normalizeJnTask,
    activities: normalizeJnActivity, files: normalizeJnFile
  };
  const staged = [];
  streamNames.forEach((stream) => {
    (streams[stream] || []).forEach((row) => {
      const payload = normalizers[stream](row || {});
      staged.push({ kind: stream.slice(0, -1), sourceId: payload.sourceId, payload });
    });
  });
  users.forEach((row) => {
    const payload = normalizeJnUser(row || {});
    staged.push({ kind: 'user', sourceId: payload.sourceId, payload });
  });
  await writeStageRecords(ref, staged);
  await prepareFileTransfers(ref, staged.filter((item) => item.kind === 'file').map((item) => item.payload));
  const contactRows = staged.filter((item) => item.kind === 'contact').map((item) => item.payload);
  const jobRows = staged.filter((item) => item.kind === 'job').map((item) => item.payload);
  const uniqueStatuses = (rows) => Array.from(new Set(rows.map((row) => cleanText(row.status, 160)).filter(Boolean))).sort();
  const summary = {
    contacts: contactRows.length,
    jobs: jobRows.length,
    tasks: streams.tasks.length,
    activities: streams.activities.length,
    files: streams.files.length,
    users: users.length,
    total: staged.length,
    fileTransfer: await fileTransferCounts(ref)
  };
  const sample = {
    contacts: contactRows.slice(0, 3),
    jobs: jobRows.slice(0, 3)
  };
  await ref.set({
    status: 'ready', readyAt: new Date().toISOString(), summary, warnings,
    statuses: { contacts: uniqueStatuses(contactRows), jobs: uniqueStatuses(jobRows) },
    sample
  }, { merge: true });
  return { migrationId, provider: 'jobnimbus', providerLabel: 'JobNimbus', summary, warnings, statuses: { contacts: uniqueStatuses(contactRows), jobs: uniqueStatuses(jobRows) }, sample };
}

function encodeStateKey(value) {
  return encodeURIComponent(String(value || '')).replace(/\./g, '%2E');
}

function stateParentId(key) {
  return 'cloudState__company__' + encodeStateKey(key);
}

function statePartId(parentId, index) {
  return parentId + '__part__' + String(index).padStart(4, '0');
}

function appStateCollection(organizationId) {
  return db.collection('organizations').doc(organizationId).collection('appState');
}
async function readCompanyState(organizationId, key, fallback) {
  const col = appStateCollection(organizationId);
  const parentId = stateParentId(key);
  const parentSnap = await col.doc(parentId).get();
  if (!parentSnap.exists) return fallback;
  const data = parentSnap.data() || {};
  if (data.deleted === true) return fallback;
  let raw = '';
  const count = Number(data.chunkCount || 0);
  if (!count) raw = typeof data.value === 'string' ? data.value : '';
  else {
    const refs = [];
    for (let i = 0; i < count; i += 1) refs.push(col.doc(statePartId(parentId, i)));
    const snaps = refs.length ? await db.getAll(...refs) : [];
    raw = snaps.map((snap) => snap.exists ? String((snap.data() || {}).chunkValue || '') : '').join('');
  }
  if (!raw) return fallback;
  try { return JSON.parse(raw); } catch (_) { return fallback; }
}

async function commitInBatches(operations) {
  let batch = db.batch();
  let count = 0;
  for (const operation of operations) {
    operation(batch);
    count += 1;
    if (count >= 350) {
      await batch.commit();
      batch = db.batch();
      count = 0;
    }
  }
  if (count) await batch.commit();
}

async function writeCompanyState(organizationId, key, value, uid) {
  const col = appStateCollection(organizationId);
  const parentId = stateParentId(key);
  const parentRef = col.doc(parentId);
  const oldSnap = await parentRef.get();
  const oldCount = oldSnap.exists ? Number((oldSnap.data() || {}).chunkCount || 0) : 0;
  const raw = JSON.stringify(value == null ? null : value);
  const parts = [];
  for (let i = 0; i < raw.length; i += CHUNK_SIZE) parts.push(raw.slice(i, i + CHUNK_SIZE));
  const chunked = parts.length > 1 ? parts : [];
  const writes = chunked.map((part, index) => (batch) => batch.set(col.doc(statePartId(parentId, index)), {
    kind: 'cloudOnlyStateChunk', parentId, chunkIndex: index, chunkValue: part
  }, { merge: false }));
  await commitInBatches(writes);
  const deletes = [];
  for (let index = chunked.length; index < oldCount; index += 1) {
    deletes.push((batch) => batch.delete(col.doc(statePartId(parentId, index))));
  }
  if (deletes.length) await commitInBatches(deletes);
  await parentRef.set({
    kind: 'cloudOnlyState', key: String(key), scope: 'company', ownerUid: '', deleted: false,
    value: parts.length > 1 ? null : (parts[0] || ''),
    chunkCount: parts.length > 1 ? parts.length : 0,
    updatedAt: new Date().toISOString(), updatedByUid: String(uid || '')
  }, { merge: false });
}

function stageByKind(snapshot) {
  const grouped = {};
  snapshot.docs.forEach((doc) => {
    const data = doc.data() || {};
    const kind = cleanText(data.kind, 40);
    if (!kind) return;
    if (!grouped[kind]) grouped[kind] = [];
    grouped[kind].push(data.payload || {});
  });
  return grouped;
}

function normalizedStatusKey(value) {
  return cleanText(value, 160).toLowerCase();
}

function mapStage(status, isJob, statusMap) {
  const raw = cleanText(status, 160);
  const supplied = statusMap && (statusMap[raw] || statusMap[normalizedStatusKey(raw)]);
  if (supplied && HAIL_STAGES.includes(supplied)) return supplied;
  const s = normalizedStatusKey(raw);
  if (!s) return isJob ? 'Contract Signed' : 'New Lead';
  if (/paid|closed/.test(s)) return 'Paid / Closed';
  if (/complete/.test(s)) return 'Completed';
  if (/invoice/.test(s)) return 'Invoiced';
  if (/built|installed/.test(s)) return 'Built';
  if (/production|scheduled/.test(s)) return 'Production';
  if (/contract|sold|signed/.test(s)) return 'Contract Signed';
  if (/approve/.test(s)) return 'Approved';
  if (/adjuster/.test(s)) return 'Adjuster Appointment';
  if (/claim/.test(s)) return 'Claim Filed';
  if (/inspect/.test(s)) return 'Inspected';
  if (/contact/.test(s)) return 'Contacted';
  if (/lead|new|prospect/.test(s)) return 'New Lead';
  return isJob ? 'Contract Signed' : 'New Lead';
}
function safeSlug(value) {
  return cleanText(value, 220).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '').slice(0, 160) || Math.random().toString(36).slice(2, 10);
}

function digits(value) {
  return String(value || '').replace(/\D+/g, '').slice(-10);
}

function identityKeys(record) {
  const keys = [];
  const email = cleanText(record && record.email, 240).toLowerCase();
  const phone = digits(record && record.phone);
  const address = [record && record.street, record && record.city, record && record.state, record && record.zip].map((v) => cleanText(v, 240).toLowerCase()).filter(Boolean).join('|');
  if (email) keys.push('email:' + email);
  if (phone.length >= 7) keys.push('phone:' + phone);
  if (address) keys.push('address:' + address);
  return keys;
}

function buildIdentitySet(records) {
  const set = new Set();
  (records || []).forEach((record) => identityKeys(record).forEach((key) => set.add(key)));
  return set;
}
function buildIdentityMap(records) {
  const map = new Map();
  (records || []).forEach((record) => identityKeys(record).forEach((key) => { if (!map.has(key)) map.set(key, record); }));
  return map;
}
function findIdentityMatch(record, identityMap) {
  for (const key of identityKeys(record)) { if (identityMap.has(key)) return identityMap.get(key); }
  return null;
}
function mergeMigrationJobFile(target, incoming) {
  if (!target || !incoming) return;
  const src = incoming.jobFile && typeof incoming.jobFile === 'object' ? incoming.jobFile : {};
  const dst = target.jobFile && typeof target.jobFile === 'object' ? target.jobFile : (target.jobFile = {});
  ['photos','documents'].forEach((key) => {
    const existing = Array.isArray(dst[key]) ? dst[key] : [];
    const seen = new Set(existing.map((item) => String(item && (item.fileId || item.id) || '')));
    (Array.isArray(src[key]) ? src[key] : []).forEach((item) => { const id = String(item && (item.fileId || item.id) || ''); if (!id || seen.has(id)) return; existing.push(item); seen.add(id); });
    dst[key] = existing;
  });
}

function isDuplicate(record, identitySet) {
  const keys = identityKeys(record);
  return keys.some((key) => identitySet.has(key));
}

function addIdentity(record, identitySet) {
  identityKeys(record).forEach((key) => identitySet.add(key));
}

function splitDisplayName(value) {
  const text = cleanText(value, 240);
  if (!text) return { firstName: '', lastName: '' };
  const parts = text.split(/\s+/);
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };
  return { firstName: parts.shift(), lastName: parts.join(' ') };
}

function relatedLookup(rows) {
  const map = new Map();
  (rows || []).forEach((row) => {
    const ids = Array.from(new Set([row.primaryId].concat(row.relatedIds || []).filter(Boolean)));
    ids.forEach((id) => {
      if (!map.has(id)) map.set(id, []);
      map.get(id).push(row);
    });
  });
  return map;
}
function buildNormalizedCrmImport(grouped, migrationId, statusMap, transferRows, providerLabel) {
  providerLabel = cleanText(providerLabel || 'Imported CRM', 160);
  const contacts = grouped.contact || [];
  const jobs = grouped.job || [];
  const tasks = grouped.task || [];
  const activities = grouped.activity || [];
  const files = grouped.file || [];
  const users = grouped.user || [];
  const transferBySource = new Map((transferRows || []).map((item) => [cleanText(item.sourceId, 180), item]));
  const contactsById = new Map(contacts.map((item) => [cleanText(item.sourceId, 180), item]));
  const activityMap = relatedLookup(activities);
  const taskMap = relatedLookup(tasks);
  const fileMap = relatedLookup(files);
  const usedContacts = new Set();
  const sourceToLeadId = new Map();
  const importedLeads = [];
  const importedContacts = [];
  const importedFiles = [];
  const importedTeamMembers = users.map((user) => ({
    id: 'mig_' + safeSlug(migrationId) + '_user_' + safeSlug(user.sourceId),
    name: cleanText(((user.firstName || '') + ' ' + (user.lastName || '')).trim() || user.email || 'Imported User', 240),
    email: user.email || '', phone: '', role: 'Sales Rep', active: false, managerId: '', managerName: '', teamId: '', teamName: '', calendarAccess: 'role-based',
    migrationId, sourceCRM: providerLabel, sourceRecordId: user.sourceId, sourceRole: user.sourceRole || '', historicalImportedUser: true
  }));

  const makeActivityNotes = (sourceIds) => {
    const seen = new Set();
    const out = [];
    sourceIds.filter(Boolean).forEach((sourceId) => {
      (activityMap.get(sourceId) || []).forEach((activity) => {
        const key = cleanText(activity.sourceId, 180) || (activity.note + '|' + activity.createdAt);
        if (seen.has(key) || !cleanText(activity.note, 5000)) return;
        seen.add(key);
        out.push({
          text: cleanText(activity.note, 5000),
          kind: activity.isStatusChange ? 'stage' : 'note',
          author: cleanText(activity.createdByName, 160) || ('Imported from ' + providerLabel),
          ts: activity.createdAt || new Date().toISOString()
        });
      });
    });
    return out.sort((a, b) => String(a.ts).localeCompare(String(b.ts)));
  };
  jobs.forEach((job) => {
    const contact = contactsById.get(cleanText(job.primaryId, 180)) || {};
    if (contact.sourceId) usedContacts.add(contact.sourceId);
    const contactName = cleanText((contact.firstName || '') + ' ' + (contact.lastName || ''), 240) || contact.displayName;
    const fallbackName = splitDisplayName(contactName || job.name);
    const firstName = cleanText(contact.firstName || fallbackName.firstName, 120);
    const lastName = cleanText(contact.lastName || fallbackName.lastName, 120);
    const sourceId = cleanText(job.sourceId, 180);
    const leadId = 'mig_' + safeSlug(migrationId) + '_job_' + safeSlug(sourceId);
    sourceToLeadId.set(sourceId, leadId);
    if (contact.sourceId) sourceToLeadId.set(contact.sourceId, leadId);
    const sourceIds = [sourceId, contact.sourceId].filter(Boolean);
    const stage = mapStage(job.status || contact.status, true, statusMap);
    const activityNotes = makeActivityNotes(sourceIds);
    const relatedTasks = sourceIds.flatMap((id) => taskMap.get(id) || []);
    const relatedFiles = sourceIds.flatMap((id) => fileMap.get(id) || []);
    importedLeads.push({
      id: leadId, firstName, lastName,
      phone: contact.phone || job.phone || '', email: contact.email || job.email || '',
      street: contact.street || job.street || '', city: contact.city || job.city || '',
      state: contact.state || job.state || '', zip: contact.zip || job.zip || '',
      leadSource: job.leadSource || contact.leadSource || (providerLabel + ' Migration'),
      assignedRep: job.assignedRep || contact.assignedRep || '', assignmentStatus: (job.assignedRep || contact.assignedRep) ? 'Assigned' : 'Unassigned',
      stage, jobStage: stage, convertedToJob: true,
      notes: cleanText(job.notes || contact.notes, 5000), activityNotes,
      createdAt: job.createdAt || contact.createdAt || new Date().toISOString(),
      updatedAt: job.updatedAt || contact.updatedAt || new Date().toISOString(),
      lastActivityAt: activityNotes.length ? activityNotes[activityNotes.length - 1].ts : (job.updatedAt || job.createdAt || new Date().toISOString()),
      migrationId, sourceCRM: providerLabel, sourceRecordId: sourceId,
      sourceContactId: cleanText(contact.sourceId, 180), importedAt: new Date().toISOString(),
      jobFile: {
        importedTasks: relatedTasks,
        importedFileReferences: relatedFiles,
        migrationSource: providerLabel
      }
    });
  });
  contacts.forEach((contact) => {
    const sourceId = cleanText(contact.sourceId, 180);
    const display = cleanText((contact.firstName || '') + ' ' + (contact.lastName || ''), 240) || contact.displayName || contact.companyName;
    const split = splitDisplayName(display);
    importedContacts.push({
      id: 'mig_' + safeSlug(migrationId) + '_contact_' + safeSlug(sourceId),
      contactType: 'Customer', firstName: contact.firstName || split.firstName,
      lastName: contact.lastName || split.lastName, companyName: contact.companyName || '',
      phone: contact.phone || '', email: contact.email || '',
      notes: contact.notes || '', createdAt: contact.createdAt || new Date().toISOString(),
      updatedAt: contact.updatedAt || contact.createdAt || new Date().toISOString(),
      migrationId, sourceCRM: providerLabel, sourceRecordId: sourceId, importedAt: new Date().toISOString()
    });
    if (usedContacts.has(sourceId)) return;
    const stage = mapStage(contact.status, false, statusMap);
    const leadId = 'mig_' + safeSlug(migrationId) + '_lead_' + safeSlug(sourceId);
    sourceToLeadId.set(sourceId, leadId);
    const activityNotes = makeActivityNotes([sourceId]);
    importedLeads.push({
      id: leadId, firstName: contact.firstName || split.firstName, lastName: contact.lastName || split.lastName,
      phone: contact.phone || '', email: contact.email || '', street: contact.street || '', city: contact.city || '',
      state: contact.state || '', zip: contact.zip || '', leadSource: contact.leadSource || (providerLabel + ' Migration'),
      assignedRep: contact.assignedRep || '', assignmentStatus: contact.assignedRep ? 'Assigned' : 'Unassigned',
      stage, jobStage: '', convertedToJob: false, notes: contact.notes || '', activityNotes,
      createdAt: contact.createdAt || new Date().toISOString(), updatedAt: contact.updatedAt || contact.createdAt || new Date().toISOString(),
      lastActivityAt: activityNotes.length ? activityNotes[activityNotes.length - 1].ts : (contact.updatedAt || contact.createdAt || new Date().toISOString()),
      migrationId, sourceCRM: providerLabel, sourceRecordId: sourceId, importedAt: new Date().toISOString(), jobFile: {}
    });
  });
  const leadById = new Map(importedLeads.map((lead) => [lead.id, lead]));
  files.forEach((file) => {
    const relationKeys = Array.from(new Set([file.primaryId].concat(file.relatedIds || []).filter(Boolean)));
    const leadId = relationKeys.map((id) => sourceToLeadId.get(id)).find(Boolean) || '';
    if (!leadId) return;
    const transfer = transferBySource.get(cleanText(file.sourceId, 180)) || {};
    const fileId = migrationFileId(file.sourceId);
    const metaId = 'mig_' + safeSlug(migrationId) + '_file_' + fileId;
    const isPhoto = file.isPhoto === true || /^image\//i.test(String(transfer.mimeType || file.mimeType || ''));
    const copied = transfer.status === 'copied' && !!transfer.storagePath;
    const meta = {
      id: metaId, leadId, jobId: leadId, type: isPhoto ? 'inspection_photo' : 'job_document',
      fileName: file.name || 'Imported File', name: file.name || 'Imported File', label: file.name || 'Imported File',
      mimeType: transfer.mimeType || file.mimeType || '', size: Number(transfer.size || file.size || 0),
      category: isPhoto ? 'Imported' : '', docCategory: isPhoto ? '' : 'Other Documents', photoPhase: isPhoto ? 'inspection' : '',
      caption: isPhoto ? (file.description || '') : '', note: file.description || '', uploadedBy: ('Imported from ' + providerLabel),
      uploadedAt: file.createdAt || new Date().toISOString(), createdAt: file.createdAt || new Date().toISOString(), updatedAt: new Date().toISOString(),
      storageKey: copied ? ('cloudmig:' + migrationId + ':' + fileId) : null,
      migrationId, sourceCRM: providerLabel, sourceRecordId: file.sourceId, cloudMigrationId: migrationId, cloudMigrationFileId: fileId,
      cloudStoragePath: copied ? transfer.storagePath : '', cloudFileStatus: transfer.status || 'pending', transferError: transfer.transferError || ''
    };
    importedFiles.push(meta);
    if (!copied) return;
    const lead = leadById.get(leadId);
    if (!lead) return;
    if (!lead.jobFile || typeof lead.jobFile !== 'object') lead.jobFile = {};
    if (isPhoto) {
      if (!Array.isArray(lead.jobFile.photos)) lead.jobFile.photos = [];
      lead.jobFile.photos.push({ id: metaId, fileId: metaId, category: 'Imported', caption: file.description || '', note: file.description || '', uploadedBy: ('Imported from ' + providerLabel), uploadedAt: meta.uploadedAt, photoPhase: 'inspection', migrationId });
    } else {
      if (!Array.isArray(lead.jobFile.documents)) lead.jobFile.documents = [];
      lead.jobFile.documents.push({ id: metaId, fileId: metaId, category: 'Other Documents', note: file.description || '', uploadedBy: ('Imported from ' + providerLabel), uploadedAt: meta.uploadedAt, migrationId });
    }
  });

  const importedTasks = tasks.map((task) => {
    const related = (task.relatedIds || []).map((id) => sourceToLeadId.get(id)).filter(Boolean);
    const leadId = related[0] || '';
    return {
      id: 'mig_' + safeSlug(migrationId) + '_task_' + safeSlug(task.sourceId),
      title: task.title || task.recordType || 'Imported Task',
      assignedTo: task.assignedTo || '', assignedUserId: '',
      dueDate: task.startAt ? task.startAt.slice(0, 10) : '', leadId,
      jobId: leadId, priority: 'Normal', completed: false, status: 'open',
      notes: task.notes || '', createdAt: task.createdAt || new Date().toISOString(),
      updatedAt: task.updatedAt || task.createdAt || new Date().toISOString(),
      migrationId, sourceCRM: providerLabel, sourceRecordId: task.sourceId, importedAt: new Date().toISOString()
    };
  });
  return { leads: importedLeads, contacts: importedContacts, tasks: importedTasks, files: importedFiles, teamMembers: importedTeamMembers };
}

async function transferJobNimbusFiles(access, body) {
  const migrationId = cleanText(body.migrationId, 180);
  const apiKey = cleanText(body.apiKey, 1200);
  if (!migrationId) throw httpError(400, 'Migration ID is required.');
  if (apiKey.length < 8) throw httpError(400, 'Enter the JobNimbus API key again to copy files.');
  const ref = migrationRef(access.organizationId, migrationId);
  const metaSnap = await ref.get();
  if (!metaSnap.exists) throw httpError(404, 'Migration was not found.');
  const meta = metaSnap.data() || {};
  if (meta.provider !== 'jobnimbus') throw httpError(400, 'File transfer is only available for a JobNimbus direct migration.');
  if (meta.organizationId && meta.organizationId !== access.organizationId) throw httpError(403, 'Migration belongs to another company.');
  const batchSize = Math.max(1, Math.min(15, Number(body.batchSize || 8)));
  await recoverStaleFileTransfers(ref);
  let snap = await ref.collection('fileTransfers').where('status', '==', 'pending').limit(batchSize).get();
  if (snap.empty && body.retryFailed === true) snap = await ref.collection('fileTransfers').where('status', '==', 'failed').limit(batchSize).get();
  let copied = 0, failed = 0;
  for (const doc of snap.docs) {
    const data = doc.data() || {};
    try {
      await doc.ref.set({ status: 'copying', transferStartedAt: new Date().toISOString(), transferError: '' }, { merge: true });
      await copyJobNimbusFile(access, migrationId, doc.ref, data, apiKey);
      copied += 1;
    } catch (error) {
      failed += 1;
      await doc.ref.set({ status: body.retryFailed === true ? 'failed_final' : 'failed', transferError: cleanText(error && error.message, 600), failedAt: new Date().toISOString() }, { merge: true });
      if (Number(error && error.statusCode) === 401) throw error;
    }
  }
  const counts = await fileTransferCounts(ref);
  await ref.set({ fileTransfer: counts, fileTransferUpdatedAt: new Date().toISOString() }, { merge: true });
  return { migrationId, processed: snap.size, copied, failed, counts };
}

function coerceUploadPayload(payload, migrationId, kind) {
  payload = payload && typeof payload === 'object' ? payload : {};
  const now = new Date().toISOString();
  const sourceId = cleanText(payload.sourceRecordId || payload.sourceId || payload.id, 180) || Math.random().toString(36).slice(2, 10);
  const base = Object.assign({}, payload, {
    id: 'mig_' + safeSlug(migrationId) + '_' + safeSlug(kind) + '_' + safeSlug(sourceId),
    migrationId, sourceCRM: cleanText(payload.sourceCRM || 'Uploaded Export', 160),
    sourceRecordId: sourceId, importedAt: now,
    createdAt: payload.createdAt || now, updatedAt: payload.updatedAt || payload.createdAt || now
  });
  if (kind === 'job') {
    base.convertedToJob = true;
    base.stage = mapStage(base.stage || base.status, true, null);
    base.jobStage = base.stage;
    base.jobFile = base.jobFile && typeof base.jobFile === 'object' ? base.jobFile : {};
  } else if (kind === 'lead') {
    base.convertedToJob = false;
    base.stage = mapStage(base.stage || base.status, false, null);
    base.jobStage = '';
    base.jobFile = base.jobFile && typeof base.jobFile === 'object' ? base.jobFile : {};
  }
  return base;
}
async function stageUploadedRecords(access, body) {
  const records = Array.isArray(body.records) ? body.records.slice(0, 1000) : [];
  if (!records.length) throw httpError(400, 'No migration records were received.');
  let migrationId = cleanText(body.migrationId, 180);
  if (!migrationId) migrationId = makeMigrationId('upload');
  const ref = migrationRef(access.organizationId, migrationId);
  const existing = await ref.get();
  if (!existing.exists) {
    await ref.set({
      migrationId, provider: 'upload', providerLabel: cleanText(body.providerLabel || 'Uploaded Export', 160),
      status: 'staging', organizationId: access.organizationId, startedAt: new Date().toISOString(),
      startedByUid: access.user.uid, startedByEmail: cleanText(access.user.email, 240), connectionStored: false,
      sourceFileName: cleanText(body.sourceFileName, 320)
    });
  }
  const safeRecords = records.map((record) => {
    const kind = /^(lead|job|contact)$/.test(cleanText(record && record.kind, 40)) ? cleanText(record.kind, 40) : 'lead';
    return { kind, sourceId: cleanText(record && record.sourceId, 180), payload: record && record.payload && typeof record.payload === 'object' ? record.payload : {} };
  });
  const written = await writeStageRecords(ref, safeRecords);
  if (body.finalize === true) {
    const snap = await ref.collection('records').get();
    const summary = { leads: 0, jobs: 0, contacts: 0, total: snap.size };
    snap.docs.forEach((doc) => {
      const kind = cleanText((doc.data() || {}).kind, 40);
      if (kind === 'lead') summary.leads += 1;
      else if (kind === 'job') summary.jobs += 1;
      else if (kind === 'contact') summary.contacts += 1;
    });
    await ref.set({ status: 'ready', readyAt: new Date().toISOString(), summary }, { merge: true });
    return { migrationId, staged: written, finalized: true, summary };
  }
  return { migrationId, staged: written, finalized: false };
}
async function importMigration(access, body) {
  const migrationId = cleanText(body.migrationId, 180);
  if (!migrationId) throw httpError(400, 'Migration ID is required.');
  const ref = migrationRef(access.organizationId, migrationId);
  const metaSnap = await ref.get();
  if (!metaSnap.exists) throw httpError(404, 'Migration was not found.');
  const meta = metaSnap.data() || {};
  if (meta.organizationId && meta.organizationId !== access.organizationId) throw httpError(403, 'Migration belongs to another company.');
  if (meta.status === 'importing') throw httpError(409, 'This migration is already running.');
  if (meta.status === 'imported') return { migrationId, alreadyImported: true, result: meta.result || {} };
  let transferRows = [];
  if (meta.provider === 'jobnimbus') {
    const counts = await fileTransferCounts(ref);
    if (counts.pending || counts.copying) throw httpError(409, 'Copy the JobNimbus photos and documents into Hail Money before importing the records.');
    const transferSnap = await ref.collection('fileTransfers').get();
    transferRows = transferSnap.docs.map((doc) => Object.assign({ fileId: doc.id }, doc.data() || {}));
  }
  await ref.set({ status: 'importing', importStartedAt: new Date().toISOString(), importedByUid: access.user.uid }, { merge: true });
  const recordSnap = await ref.collection('records').get();
  const grouped = stageByKind(recordSnap);
  let incoming = { leads: [], contacts: [], tasks: [], files: [], teamMembers: [] };
  if (['jobnimbus','acculynx','leap'].includes(meta.provider)) incoming = buildNormalizedCrmImport(grouped, migrationId, body.statusMap || {}, transferRows, meta.providerLabel || meta.provider);
  else {
    (grouped.lead || []).forEach((payload) => incoming.leads.push(coerceUploadPayload(payload, migrationId, 'lead')));
    (grouped.job || []).forEach((payload) => incoming.leads.push(coerceUploadPayload(payload, migrationId, 'job')));
    (grouped.contact || []).forEach((payload) => incoming.contacts.push(coerceUploadPayload(payload, migrationId, 'contact')));
  }
  const existingLeads = await readCompanyState(access.organizationId, 'crm_leads', []);
  const existingContacts = await readCompanyState(access.organizationId, 'crm_contacts', []);
  const existingTasks = await readCompanyState(access.organizationId, 'crm_tasks', []);
  const existingFiles = await readCompanyState(access.organizationId, 'crm_lead_files', []);
  const existingTeamMembers = await readCompanyState(access.organizationId, 'app.teamMembers', []);
  const leadList = Array.isArray(existingLeads) ? existingLeads.slice() : [];
  const contactList = Array.isArray(existingContacts) ? existingContacts.slice() : [];
  const taskList = Array.isArray(existingTasks) ? existingTasks.slice() : [];
  const fileList = Array.isArray(existingFiles) ? existingFiles.slice() : [];
  const teamList = Array.isArray(existingTeamMembers) ? existingTeamMembers.slice() : [];
  const leadIdentityMap = buildIdentityMap(leadList);
  const contactIds = buildIdentitySet(contactList);
  const duplicateMode = cleanText(body.duplicateMode, 40) === 'keep' ? 'keep' : 'skip';
  const leadIdRemap = new Map();
  let skippedLeads = 0, skippedContacts = 0;
  incoming.leads.forEach((lead) => {
    const matched = duplicateMode === 'skip' ? findIdentityMatch(lead, leadIdentityMap) : null;
    if (matched) {
      skippedLeads += 1; leadIdRemap.set(lead.id, matched.id); mergeMigrationJobFile(matched, lead); return;
    }
    leadList.push(lead); leadIdRemap.set(lead.id, lead.id);
    identityKeys(lead).forEach((key) => { if (!leadIdentityMap.has(key)) leadIdentityMap.set(key, lead); });
  });
  incoming.contacts.forEach((contact) => {
    if (duplicateMode === 'skip' && isDuplicate(contact, contactIds)) { skippedContacts += 1; return; }
    contactList.push(contact); addIdentity(contact, contactIds);
  });
  incoming.tasks.forEach((task) => {
    const mapped = leadIdRemap.get(task.leadId || task.jobId) || task.leadId || task.jobId || "";
    task.leadId = mapped; task.jobId = mapped; taskList.push(task);
  });
  const teamKeys = new Set();
  teamList.forEach((member) => {
    const email = cleanText(member && member.email, 240).toLowerCase();
    const name = cleanText(member && member.name, 240).toLowerCase();
    if (email) teamKeys.add('e:' + email);
    if (name) teamKeys.add('n:' + name);
  });
  let importedTeamMembers = 0, skippedTeamMembers = 0;
  (incoming.teamMembers || []).forEach((member) => {
    const email = cleanText(member && member.email, 240).toLowerCase();
    const name = cleanText(member && member.name, 240).toLowerCase();
    if ((email && teamKeys.has('e:' + email)) || (name && teamKeys.has('n:' + name))) { skippedTeamMembers += 1; return; }
    teamList.push(member); importedTeamMembers += 1;
    if (email) teamKeys.add('e:' + email); if (name) teamKeys.add('n:' + name);
  });
  const knownFileIds = new Set(fileList.map((file) => String(file && file.id || "")));
  let importedFiles = 0, importedPhotos = 0, importedDocuments = 0, orphanedFiles = 0, failedFiles = 0;
  (incoming.files || []).forEach((file) => {
    if (file.cloudFileStatus !== 'copied' || !file.storageKey) { failedFiles += 1; return; }
    const mapped = leadIdRemap.get(file.leadId || file.jobId) || file.leadId || file.jobId || "";
    if (!mapped) { orphanedFiles += 1; return; }
    file.leadId = mapped; file.jobId = mapped;
    if (!knownFileIds.has(String(file.id || ""))) { fileList.push(file); knownFileIds.add(String(file.id || "")); importedFiles += 1; if (file.type === "inspection_photo" || file.type === "after_photo") importedPhotos += 1; else importedDocuments += 1; }
  });
  await writeCompanyState(access.organizationId, 'crm_leads', leadList, access.user.uid);
  await writeCompanyState(access.organizationId, 'app.leads', leadList, access.user.uid);
  await writeCompanyState(access.organizationId, 'crm_contacts', contactList, access.user.uid);
  await writeCompanyState(access.organizationId, 'crm_tasks', taskList, access.user.uid);
  await writeCompanyState(access.organizationId, 'crm_lead_files', fileList, access.user.uid);
  await writeCompanyState(access.organizationId, 'app.teamMembers', teamList, access.user.uid);
  const result = {
    importedLeads: incoming.leads.length - skippedLeads, skippedLeads,
    importedContacts: incoming.contacts.length - skippedContacts, skippedContacts,
    importedTasks: incoming.tasks.length, importedFiles, importedPhotos, importedDocuments, failedFiles, orphanedFiles, importedTeamMembers, skippedTeamMembers
  };
  await ref.set({ status: 'imported', importedAt: new Date().toISOString(), result, duplicateMode, statusMap: body.statusMap || {}, rollbackAvailable: true }, { merge: true });
  return { migrationId, result };
}
async function rollbackMigration(access, body) {
  const migrationId = cleanText(body.migrationId, 180);
  if (!migrationId) throw httpError(400, 'Migration ID is required.');
  const ref = migrationRef(access.organizationId, migrationId);
  const metaSnap = await ref.get();
  if (!metaSnap.exists) throw httpError(404, 'Migration was not found.');
  const meta = metaSnap.data() || {};
  if (meta.status !== 'imported') throw httpError(409, 'Only an imported migration can be rolled back.');
  const leads = await readCompanyState(access.organizationId, 'crm_leads', []);
  const contacts = await readCompanyState(access.organizationId, 'crm_contacts', []);
  const tasks = await readCompanyState(access.organizationId, 'crm_tasks', []);
  const files = await readCompanyState(access.organizationId, 'crm_lead_files', []);
  const teamMembers = await readCompanyState(access.organizationId, 'app.teamMembers', []);
  const allFiles = Array.isArray(files) ? files : [];
  const migratedFileIds = new Set(allFiles.filter((item) => cleanText(item && item.migrationId, 180) === migrationId).map((item) => String(item.id || '')));
  const allLeads = Array.isArray(leads) ? leads : [];
  const nextLeads = allLeads.filter((item) => cleanText(item && item.migrationId, 180) !== migrationId).map((lead) => {
    const jobFile = lead && lead.jobFile && typeof lead.jobFile === "object" ? lead.jobFile : null;
    if (jobFile) {
      ['photos','documents'].forEach((key) => { if (Array.isArray(jobFile[key])) jobFile[key] = jobFile[key].filter((entry) => !migratedFileIds.has(String(entry && (entry.fileId || entry.id) || ''))); });
    }
    return lead;
  });
  const nextContacts = (Array.isArray(contacts) ? contacts : []).filter((item) => cleanText(item && item.migrationId, 180) !== migrationId);
  const nextTasks = (Array.isArray(tasks) ? tasks : []).filter((item) => cleanText(item && item.migrationId, 180) !== migrationId);
  const nextFiles = allFiles.filter((item) => cleanText(item && item.migrationId, 180) !== migrationId);
  const allTeamMembers = Array.isArray(teamMembers) ? teamMembers : [];
  const nextTeamMembers = allTeamMembers.filter((item) => cleanText(item && item.migrationId, 180) !== migrationId);
  let deletedStorageObjects = 0;
  const transferSnap = await ref.collection('fileTransfers').get();
  for (const doc of transferSnap.docs) {
    const data = doc.data() || {};
    const storagePath = cleanText(data.storagePath, 1000);
    if (storagePath) { try { await admin.storage().bucket().file(storagePath).delete({ ignoreNotFound: true }); deletedStorageObjects += 1; } catch (error) { logger.warn("Migration storage rollback delete failed", { migrationId, storagePath, message: error && error.message }); } }
    await doc.ref.set({ status: 'rolled_back', rolledBackAt: new Date().toISOString() }, { merge: true });
  }
  const removed = {
    leads: allLeads.length - nextLeads.length,
    contacts: (Array.isArray(contacts) ? contacts.length : 0) - nextContacts.length,
    tasks: (Array.isArray(tasks) ? tasks.length : 0) - nextTasks.length,
    files: allFiles.length - nextFiles.length, teamMembers: allTeamMembers.length - nextTeamMembers.length, deletedStorageObjects
  };
  await writeCompanyState(access.organizationId, 'crm_leads', nextLeads, access.user.uid);
  await writeCompanyState(access.organizationId, 'app.leads', nextLeads, access.user.uid);
  await writeCompanyState(access.organizationId, 'crm_contacts', nextContacts, access.user.uid);
  await writeCompanyState(access.organizationId, 'crm_tasks', nextTasks, access.user.uid);
  await writeCompanyState(access.organizationId, 'crm_lead_files', nextFiles, access.user.uid);
  await writeCompanyState(access.organizationId, 'app.teamMembers', nextTeamMembers, access.user.uid);
  await ref.set({ status: 'rolled_back', rolledBackAt: new Date().toISOString(), rolledBackByUid: access.user.uid, rollbackResult: removed, rollbackAvailable: false }, { merge: true });
  return { migrationId, removed };
}
async function listMigrations(access) {
  const snap = await db.collection('organizations').doc(access.organizationId).collection('migrations').orderBy('startedAt', 'desc').limit(25).get();
  return snap.docs.map((doc) => {
    const data = doc.data() || {};
    return {
      migrationId: doc.id, provider: data.provider || '', providerLabel: data.providerLabel || '',
      status: data.status || '', startedAt: data.startedAt || '', readyAt: data.readyAt || '',
      importedAt: data.importedAt || '', rolledBackAt: data.rolledBackAt || '', summary: data.summary || {},
      result: data.result || {}, rollbackResult: data.rollbackResult || {}, sourceFileName: data.sourceFileName || '',
      warnings: Array.isArray(data.warnings) ? data.warnings : [], statuses: data.statuses || {}, sample: data.sample || {},
      fileTransfer: data.fileTransfer || data.summary && data.summary.fileTransfer || {}, rollbackAvailable: data.rollbackAvailable === true && data.status === 'imported'
    };
  });
}
module.exports.crmMigration = onRequest(
  { timeoutSeconds: 540, memory: '1GiB', cors: false, invoker: 'public' },
  async (req, res) => {
    permitCors(req, res);
    if (req.method === 'OPTIONS') return res.status(204).send('');
    if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
    try {
      const access = await requireCompanyAdmin(req);
      const body = req.body && typeof req.body === 'object' ? req.body : {};
      const action = cleanText(body.action, 60).toLowerCase();
      let result;
      if (action === 'scan_jobnimbus') result = await scanJobNimbus(access, body);
      else if (action === 'scan_acculynx') result = await scanAccuLynx(access, body);
      else if (action === 'scan_leap') result = await scanLeap(access, body);
      else if (action === 'transfer_jobnimbus_files') result = await transferJobNimbusFiles(access, body);
      else if (action === 'stage_upload') result = await stageUploadedRecords(access, body);
      else if (action === 'import') result = await importMigration(access, body);
      else if (action === 'rollback') result = await rollbackMigration(access, body);
      else if (action === 'list') result = { migrations: await listMigrations(access) };
      else throw httpError(400, 'Unsupported migration action.');
      logger.info('CRM migration action completed', {
        uid: access.user.uid, organizationId: access.organizationId, action,
        migrationId: result && result.migrationId || ''
      });
      return res.status(200).json(result);
    } catch (error) {
      const status = Number(error && error.statusCode) || 500;
      logger.error('CRM migration action failed', {
        status, message: error && error.message, action: req.body && req.body.action
      });
      return res.status(status).json({ error: cleanText(error && error.message, 500) || 'CRM migration failed.' });
    }
  }
);

module.exports.crmMigrationFile = onRequest(
  { timeoutSeconds: 120, memory: '512MiB', cors: false, invoker: 'public' },
  async (req, res) => {
    permitCors(req, res);
    if (req.method === 'OPTIONS') return res.status(204).send('');
    if (req.method !== 'GET') return res.status(405).json({ error: 'GET required.' });
    try {
      const access = await requireCompanyMember(req);
      const migrationId = cleanText(req.query && req.query.migrationId, 180);
      const fileId = cleanText(req.query && req.query.fileId, 180);
      if (!migrationId || !fileId) throw httpError(400, 'Migration ID and file ID are required.');
      const ref = migrationRef(access.organizationId, migrationId);
      const migrationSnap = await ref.get();
      if (!migrationSnap.exists) throw httpError(404, 'Migration was not found.');
      const migration = migrationSnap.data() || {};
      if (migration.organizationId && migration.organizationId !== access.organizationId) throw httpError(403, 'Migration belongs to another company.');
      const transferSnap = await ref.collection('fileTransfers').doc(fileId).get();
      if (!transferSnap.exists) throw httpError(404, 'Migration file was not found.');
      const transfer = transferSnap.data() || {};
      const storagePath = cleanText(transfer.storagePath, 1000);
      if (transfer.status !== 'copied' || !storagePath) throw httpError(404, 'Migration file is not available.');
      const cloudFile = admin.storage().bucket().file(storagePath);
      const [exists] = await cloudFile.exists();
      if (!exists) throw httpError(404, 'Migration file is missing from storage.');
      res.set('Content-Type', cleanText(transfer.mimeType || 'application/octet-stream', 160));
      res.set('Content-Disposition', 'inline; filename="' + safeFileName(transfer.name || 'file').replace(/"/g, '') + '"');
      res.set('Cache-Control', 'private, max-age=300');
      await new Promise((resolve, reject) => {
        const stream = cloudFile.createReadStream();
        stream.on('error', reject);
        res.on('finish', resolve);
        res.on('close', resolve);
        stream.pipe(res);
      });
    } catch (error) {
      if (res.headersSent) { try { res.end(); } catch (_) {} return; }
      const status = Number(error && error.statusCode) || 500;
      logger.error('CRM migration file read failed', { status, message: error && error.message });
      return res.status(status).json({ error: cleanText(error && error.message, 500) || 'Migration file could not be opened.' });
    }
  }
);
