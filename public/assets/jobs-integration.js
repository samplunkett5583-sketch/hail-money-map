/* ═══════════════════════════════════════════════════════════════════════
   JOBS UI — Vanilla JS Integration
   
   PASTE THIS entire block BEFORE the closing </script> tag of the main
   JS block (the one that starts with "use strict" and contains window.App)
   ═══════════════════════════════════════════════════════════════════════ */

/* ── Jobs Data Store ─────────────────────────────────────────────────── */
const JOBS_STORE_KEY = 'hmm_jobs_v1';
const JOBS_STAGES = ['new lead','claim filed','approved','production','built','complete'];
const JOBS_DAMAGE_AREAS = ['Roof','Siding','Gutters','Window Wraps','Soffit','Fascia','Pool','Windows','Window Screens','Fence','Garage Door','Patio Cover','Deck','Shed','HVAC','Skylights','Other'];
const JOBS_INSURANCE_COS = ['State Farm','Allstate','USAA','Farmers','Progressive','Nationwide','Liberty Mutual','Travelers','American Family','Erie Insurance','Auto-Owners','Shelter','GEICO','Other'];

function jobsLoad() {
  try { return JSON.parse(localStorage.getItem(JOBS_STORE_KEY) || '[]'); } catch(e) { return []; }
}
function jobsSave(jobs) {
  try { localStorage.setItem(JOBS_STORE_KEY, JSON.stringify(jobs)); } catch(e) {}
}

function jobsAutoTasks(job) {
  const t = Object.assign({}, job.tasks || {});
  t['name entered'] = !!(job.name && job.name.trim());
  t['number entered'] = !!(job.phone && job.phone.trim());
  t['signed contingency'] = ((job.docs||{}).contingency || []).length > 0;
  t['inspection pictures'] = (((job.docs||{}).jobPhotos||{}).inspectionPics || []).length > 0;
  t['claim number'] = !!(job.claimNumber && job.claimNumber.trim() && job.claimNumber !== '—');
  t['adjuster inspection pics'] = (((job.docs||{}).jobPhotos||{}).adjusterPics || []).length > 0;
  t['claim report'] = ((job.docs||{}).insurancePaperwork || []).some(function(f){ return f.toLowerCase().indexOf('claim')>=0 || f.toLowerCase().indexOf('report')>=0; });
  t['first check turned in'] = (job.checks || []).length > 0;
  t['production paperwork'] = ((((job.docs||{}).otherJobDocuments||{}).productionPaperwork) || []).length > 0;
  return t;
}

function jobsMilestones(t) {
  return {
    'new lead': !!(t['name entered'] && t['number entered']),
    'claim filed': !!(t['signed contingency'] && t['inspection pictures'] && t['claim number']),
    'approved': !!(t['adjuster inspection pics'] && t['claim report']),
    'production': !!(t['first check turned in'] && t['production paperwork']),
    'built': !!(t['labor invoice'] && t['material invoice']),
    'complete': !!(t['final insurance invoice'])
  };
}

function jobsComputeStage(t) {
  var m = jobsMilestones(t);
  if (m.complete) return 'complete';
  if (m.built) return 'built';
  if (m.production) return 'production';
  if (m.approved) return 'approved';
  if (m['claim filed']) return 'claim filed';
  if (m['new lead']) return 'new lead';
  return 'new lead';
}

function jobsCreateBlank() {
  return {
    id: Date.now(), name: '', address: '', phone: '', email: '',
    insurance: '', claimNumber: '', dateOfLoss: '', damageAreas: [],
    notes: '', tasks: { 'labor invoice': false, 'material invoice': false, 'final insurance invoice': false },
    payments: { first: 0, deductible: 0, second: 0, other: 0 },
    labor: { roof: 0, gutters: 0, siding: 0, windows: 0 },
    material: { roof: 0, gutters: 0, siding: 0, windows: 0 },
    rep: { name: '', contingency: 0, commission: 0, other: 0 },
    lastTouched: 'Just now',
    docs: { contingency: [], contract: [], insurancePaperwork: [], jobMeasurements: [],
      otherJobDocuments: { productionPaperwork: [], general: [] }, discounts: [],
      jobPhotos: { inspectionPics: [], adjusterPics: [], otherPhotos: [] } },
    checks: [], referralPay: { source: '—', amount: 0, paid: false }
  };
}

/* ── Initialize with sample data if empty ────────────────────────────── */
(function() {
  if (jobsLoad().length === 0) {
    var samples = [
      Object.assign(jobsCreateBlank(), { id:1, name:'Johnson', phone:'(314) 555-0187', address:'1482 Maple Ridge Dr, St. Louis, MO 63118', email:'rjohnson@email.com', insurance:'State Farm', claimNumber:'SF-2026-44819', dateOfLoss:'2026-02-15', damageAreas:['Roof','Gutters'], docs:{ contingency:['Contingency.pdf'], contract:['Contract.pdf'], insurancePaperwork:['Claim_Report.pdf'], jobMeasurements:['Measurements.pdf'], otherJobDocuments:{productionPaperwork:['Production_Signed.pdf'],general:[]}, discounts:[], jobPhotos:{inspectionPics:['roof_north.jpg'],adjusterPics:['adjuster.zip'],otherPhotos:[]} }, checks:[{from:'State Farm',amount:4200,date:'03/10/2026',status:'received'}], payments:{first:4200,deductible:1000,second:0,other:0}, labor:{roof:3800,gutters:600,siding:0,windows:0}, material:{roof:2100,gutters:350,siding:0,windows:0} }),
      Object.assign(jobsCreateBlank(), { id:2, name:'Garcia', phone:'(314) 555-0412', address:'3310 Sunset Bluff Ct, Florissant, MO 63033', email:'mgarcia@email.com', insurance:'Progressive', claimNumber:'—', damageAreas:[], docs:{ contingency:['Contingency_Garcia.pdf'], contract:[], insurancePaperwork:[], jobMeasurements:[], otherJobDocuments:{productionPaperwork:[],general:[]}, discounts:[], jobPhotos:{inspectionPics:[],adjusterPics:[],otherPhotos:[]} } }),
      Object.assign(jobsCreateBlank(), { id:3, name:'Martinez', phone:'(636) 555-0891', address:'2200 Prairie View Dr, St. Charles, MO 63301', email:'amartinez@email.com', insurance:'Unknown', claimNumber:'—', damageAreas:[] })
    ];
    jobsSave(samples);
  }
})();

/* ── Jobs UI State ───────────────────────────────────────────────────── */
var _jobsPage = 'list'; // list | detail | payments | newlead
var _jobsSelId = null;
var _jobsTab = 'documents';
var _jobsDocFolder = null;
var _jobsPhotoFolder = null;
var _jobsOtherDocSub = null;

/* ── Show / Hide Jobs View ───────────────────────────────────────────── */
function showJobsView() {
  var jv = document.getElementById('jobsView');
  if (!jv) return;
  // Hide everything else
  document.getElementById('appRoot').style.display = 'none';
  document.getElementById('authScreen').style.display = 'none';
  var cv = document.getElementById('campaignView'); if (cv) cv.style.display = 'none';
  var cl = document.getElementById('campaignLanding'); if (cl) cl.style.display = 'none';
  jv.classList.add('open');
  _jobsPage = 'list';
  renderJobsUI();
}

function hideJobsView() {
  var jv = document.getElementById('jobsView');
  if (jv) jv.classList.remove('open');
  // Return to maps
  showOnlyView(App.views.mapsView);
}

function showJobsNewLead() {
  var jv = document.getElementById('jobsView');
  if (!jv) return;
  document.getElementById('appRoot').style.display = 'none';
  document.getElementById('authScreen').style.display = 'none';
  var cv = document.getElementById('campaignView'); if (cv) cv.style.display = 'none';
  var cl = document.getElementById('campaignLanding'); if (cl) cl.style.display = 'none';
  jv.classList.add('open');
  _jobsPage = 'newlead';
  renderJobsUI();
}

/* ── Escape HTML helper ──────────────────────────────────────────────── */
function jEsc(s) { return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function jFmt(n) { return '$' + Number(n||0).toLocaleString(); }

/* ══════════════════════════════════════════════════════════════════════
   RENDER ENGINE — builds the appropriate page into #jobsContent
   ══════════════════════════════════════════════════════════════════════ */
function renderJobsUI() {
  var container = document.getElementById('jobsContent');
  if (!container) return;

  if (_jobsPage === 'list') renderJobsList(container);
  else if (_jobsPage === 'detail') renderJobsDetail(container);
  else if (_jobsPage === 'payments') renderJobsPayments(container);
  else if (_jobsPage === 'newlead') renderJobsNewLead(container);
}

/* ── PAGE: My Jobs List ──────────────────────────────────────────────── */
function renderJobsList(el) {
  var jobs = jobsLoad();
  var byStage = {};
  JOBS_STAGES.forEach(function(s){ byStage[s] = []; });
  jobs.forEach(function(j){
    var t = jobsAutoTasks(j);
    var s = jobsComputeStage(t);
    if (byStage[s]) byStage[s].push(j);
  });
  var maxRows = Math.max(6, Math.max.apply(null, JOBS_STAGES.map(function(s){ return byStage[s].length; })));

  var html = '<div style="max-width:1100px;margin:0 auto;">';
  html += '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;">';
  html += '<div><h1 style="font-family:Oswald,sans-serif;font-size:40px;color:#f0d078;letter-spacing:4px;margin:0;text-transform:uppercase;text-shadow:0 0 20px rgba(212,168,66,0.15);">My Jobs</h1>';
  html += '<p style="color:rgba(212,168,66,0.45);font-size:13px;margin:4px 0 0;">' + jobs.length + ' active jobs · Click a name to open</p></div>';
  html += '<div style="display:flex;gap:10px;">';
  html += '<button class="j-btn" onclick="showJobsNewLead()"><span style="font-size:16px;">+</span> New Lead</button>';
  html += '<button class="j-btn" onclick="hideJobsView()">← Back to Map</button>';
  html += '</div></div>';

  // Table
  html += '<div class="j-glass" style="overflow:hidden;"><div style="overflow-x:auto;">';
  html += '<table class="j-table"><thead><tr>';
  JOBS_STAGES.forEach(function(s, i) {
    html += '<th>' + jEsc(s) + ' <span style="display:inline-block;margin-left:8px;padding:2px 8px;border-radius:10px;background:rgba(212,168,66,0.15);font-size:10px;color:#d4a842;">' + byStage[s].length + '</span></th>';
  });
  html += '</tr></thead><tbody>';
  for (var r = 0; r < maxRows; r++) {
    html += '<tr>';
    JOBS_STAGES.forEach(function(s) {
      var job = byStage[s][r];
      html += '<td>';
      if (job) {
        html += '<button class="j-name-btn" onclick="jobsOpenDetail(' + job.id + ')">' + jEsc(job.name) + '</button>';
      }
      html += '</td>';
    });
    html += '</tr>';
  }
  html += '</tbody></table></div></div>';
  html += '</div>';
  el.innerHTML = html;
}

function jobsOpenDetail(id) {
  _jobsSelId = id;
  _jobsPage = 'detail';
  _jobsTab = 'documents';
  _jobsDocFolder = null;
  _jobsPhotoFolder = null;
  _jobsOtherDocSub = null;
  renderJobsUI();
}

/* ── PAGE: Job Detail ────────────────────────────────────────────────── */
function renderJobsDetail(el) {
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === _jobsSelId; });
  if (!job) { _jobsPage = 'list'; renderJobsUI(); return; }

  var t = jobsAutoTasks(job);
  var m = jobsMilestones(t);
  var stage = jobsComputeStage(t);
  var doneMilestones = JOBS_STAGES.filter(function(s){ return m[s]; }).length;
  var pct = Math.round((doneMilestones / JOBS_STAGES.length) * 100);

  var html = '<div style="max-width:1160px;margin:0 auto;">';

  // Back button
  html += '<button class="j-btn" onclick="_jobsPage=\'list\';renderJobsUI();">← Back to My Jobs</button>';

  // Header with name + stage + progress
  html += '<div class="j-glass" style="margin-top:14px;padding:20px 26px;margin-bottom:18px;">';
  html += '<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;">';
  html += '<div style="display:flex;align-items:center;gap:16px;">';
  html += '<h2 style="font-family:Oswald,sans-serif;font-size:34px;color:#f0d078;letter-spacing:3px;margin:0;text-transform:uppercase;">' + jEsc(job.name || 'New Job') + '</h2>';
  html += '<span class="j-pill">' + jEsc(stage) + '</span>';
  html += '</div>';
  // Progress ring
  html += '<div style="position:relative;width:64px;height:64px;">';
  html += '<svg width="64" height="64" viewBox="0 0 64 64"><circle cx="32" cy="32" r="26" fill="none" stroke="rgba(212,168,66,0.12)" stroke-width="4"/><circle cx="32" cy="32" r="26" fill="none" stroke="#d4a842" stroke-width="4" stroke-dasharray="' + (pct*1.634) + ' 163.4" stroke-linecap="round" transform="rotate(-90 32 32)"/></svg>';
  html += '<div style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#f0d078;font-size:14px;font-weight:700;font-family:Oswald,sans-serif;">' + pct + '%</div>';
  html += '</div></div></div>';

  // Tabs
  var tabs = ['documents','checks','notes','time since last touched','referral pay','job pictures'];
  html += '<div style="display:flex;gap:8px;margin-bottom:18px;flex-wrap:wrap;">';
  tabs.forEach(function(tab) {
    var active = _jobsTab === tab;
    html += '<button class="j-btn' + (active ? ' j-active' : '') + '" onclick="_jobsTab=\'' + tab + '\';_jobsDocFolder=null;_jobsPhotoFolder=null;_jobsOtherDocSub=null;renderJobsUI();">' + jEsc(tab) + '</button>';
  });
  html += '</div>';

  // Two column layout
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:18px;">';

  // LEFT column
  html += '<div style="display:flex;flex-direction:column;gap:18px;">';

  // Tab content panel
  html += '<div class="j-glass" style="padding:20px 24px;min-height:220px;">';
  html += '<div class="j-hdr">' + jEsc(_jobsTab) + '</div>';
  html += renderJobsTabContent(job);
  html += '</div>';

  // Homeowner Info
  html += '<div class="j-glass" style="padding:20px 24px;">';
  html += '<div class="j-hdr">Homeowner Information</div>';
  html += renderJobsHomeownerInfo(job);
  html += '</div>';

  html += '</div>'; // end left

  // RIGHT column: Checklist
  html += '<div class="j-glass" style="padding:20px 24px;overflow-y:auto;">';
  html += '<div class="j-hdr">Job Production Checklist</div>';
  html += renderJobsChecklist(job, t, m);
  html += '</div>';

  html += '</div>'; // end grid
  html += '</div>'; // end container

  el.innerHTML = html;
}

/* ── Tab Content Renderer ────────────────────────────────────────────── */
function renderJobsTabContent(job) {
  var html = '';
  switch (_jobsTab) {
    case 'documents':
      html = renderJobsDocs(job);
      break;
    case 'checks':
      html = renderJobsChecks(job);
      break;
    case 'notes':
      html = '<textarea class="j-input" style="min-height:160px;resize:vertical;" placeholder="Add job notes..." onchange="jobsUpdateField(' + job.id + ',\'notes\',this.value)">' + jEsc(job.notes || '') + '</textarea>';
      break;
    case 'time since last touched':
      html = '<div style="text-align:center;padding:30px 0;"><div style="color:#f0d078;font-size:42px;font-weight:700;font-family:Oswald,sans-serif;">' + jEsc(job.lastTouched || 'Unknown') + '</div><div style="color:rgba(212,168,66,0.45);font-size:13px;margin-top:8px;">Since last updated</div></div>';
      break;
    case 'referral pay':
      var rp = job.referralPay || {};
      html = '<div style="display:flex;flex-direction:column;gap:10px;">';
      [['Source', rp.source||'—'],['Amount', rp.amount ? '$'+rp.amount : '$0'],['Status', rp.paid ? 'Paid' : 'Unpaid']].forEach(function(r){
        html += '<div style="display:flex;justify-content:space-between;padding:10px 14px;border-radius:8px;background:rgba(212,168,66,0.04);border:1px solid rgba(212,168,66,0.12);">';
        html += '<span style="color:rgba(212,168,66,0.45);font-weight:700;">' + r[0] + '</span>';
        html += '<span style="color:#f0d078;font-weight:700;font-family:Oswald,sans-serif;">' + jEsc(r[1]) + '</span></div>';
      });
      html += '</div>';
      break;
    case 'job pictures':
      html = renderJobsPhotos(job);
      break;
  }
  return html;
}

/* ── Documents tab with folder navigation ────────────────────────────── */
function renderJobsDocs(job) {
  var docs = job.docs || {};
  var categories = [
    {key:'contingency',label:'Contingency',icon:'📋'},
    {key:'contract',label:'Contract',icon:'📝'},
    {key:'insurancePaperwork',label:'Insurance Paperwork',icon:'🛡️'},
    {key:'jobMeasurements',label:'Job Measurements',icon:'📐'},
    {key:'otherJobDocuments',label:'Other Job Documents',icon:'📂'},
    {key:'discounts',label:'Discounts',icon:'💰'},
    {key:'reports',label:'Reports',icon:'📊'}
  ];

  if (_jobsDocFolder === 'reports') {
    return '<p style="color:rgba(212,168,66,0.45);font-size:13px;">Report builder coming soon — create professional PDF reports from your job photos.</p>' +
      '<button class="j-btn" style="margin-top:10px;" onclick="_jobsDocFolder=null;renderJobsUI();">← Back</button>';
  }

  if (_jobsDocFolder === 'otherJobDocuments') {
    var otherDocs = docs.otherJobDocuments || {};
    if (_jobsOtherDocSub) {
      var files = otherDocs[_jobsOtherDocSub] || [];
      var subLabel = _jobsOtherDocSub === 'productionPaperwork' ? 'Production Paperwork' : 'General Documents';
      return '<button class="j-btn" style="margin-bottom:12px;" onclick="_jobsOtherDocSub=null;renderJobsUI();">← Back</button>' +
        '<div style="color:#f0d078;font-size:15px;font-weight:700;font-family:Oswald,sans-serif;text-transform:uppercase;margin-bottom:12px;">' + subLabel + '</div>' +
        renderJobsFileList(job.id, files, 'otherSub_' + _jobsOtherDocSub);
    }
    return '<button class="j-btn" style="margin-bottom:12px;" onclick="_jobsDocFolder=null;renderJobsUI();">← Back</button>' +
      '<div style="color:#f0d078;font-size:15px;font-weight:700;font-family:Oswald,sans-serif;text-transform:uppercase;margin-bottom:12px;">📂 Other Job Documents</div>' +
      ['productionPaperwork','general'].map(function(sub){
        var cnt = (otherDocs[sub]||[]).length;
        var lbl = sub === 'productionPaperwork' ? 'Production Paperwork' : 'General Documents';
        return '<button class="j-btn" style="width:100%;margin-bottom:8px;justify-content:space-between;" onclick="_jobsOtherDocSub=\'' + sub + '\';renderJobsUI();">' + lbl + ' (' + cnt + ') →</button>';
      }).join('');
  }

  if (_jobsDocFolder) {
    var cat = categories.find(function(c){ return c.key === _jobsDocFolder; });
    var files = docs[_jobsDocFolder] || [];
    return '<button class="j-btn" style="margin-bottom:12px;" onclick="_jobsDocFolder=null;renderJobsUI();">← Back</button>' +
      '<div style="color:#f0d078;font-size:15px;font-weight:700;font-family:Oswald,sans-serif;text-transform:uppercase;margin-bottom:12px;">' + (cat?cat.icon:'') + ' ' + (cat?cat.label:'') + '</div>' +
      renderJobsFileList(job.id, files, _jobsDocFolder);
  }

  // Folder grid
  var html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">';
  categories.forEach(function(cat) {
    var count = cat.key === 'otherJobDocuments'
      ? ((docs.otherJobDocuments||{}).productionPaperwork||[]).length + ((docs.otherJobDocuments||{}).general||[]).length
      : cat.key === 'reports' ? 0
      : (docs[cat.key]||[]).length;
    var subtext = cat.key === 'reports' ? 'Create PDF Reports' : count + ' file' + (count!==1?'s':'');
    html += '<button class="j-btn" style="flex-direction:column;align-items:flex-start;padding:14px 16px;width:100%;" onclick="_jobsDocFolder=\'' + cat.key + '\';renderJobsUI();">';
    html += '<span style="font-size:22px;">' + cat.icon + '</span>';
    html += '<span style="font-size:12px;letter-spacing:0.8px;">' + jEsc(cat.label) + '</span>';
    html += '<span style="font-size:10px;color:rgba(212,168,66,0.45);text-transform:none;letter-spacing:0;font-weight:400;">' + subtext + '</span>';
    html += '</button>';
  });
  html += '</div>';
  return html;
}

/* ── File list with upload (browse/scan) ─────────────────────────────── */
function renderJobsFileList(jobId, files, folderKey) {
  var listId = 'jfl_' + jobId + '_' + folderKey;
  var html = '';
  if (files.length === 0) {
    html += '<p style="color:rgba(212,168,66,0.45);font-size:14px;">No files yet.</p>';
  } else {
    files.forEach(function(f, i) {
      var isImg = /\.(jpg|jpeg|png|gif|webp|heic)$/i.test(f);
      html += '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;border-radius:8px;background:rgba(212,168,66,0.04);border:1px solid rgba(212,168,66,0.1);margin-bottom:5px;">';
      html += '<span style="color:#d4a842;font-size:14px;">' + (isImg?'🖼️':'📄') + '</span>';
      html += '<span style="color:#f0d078;font-size:13px;font-weight:600;flex:1;">' + jEsc(f) + '</span>';
      html += '</div>';
    });
  }

  // Upload button with menu
  var inputId = 'jinput_' + listId;
  var camId = 'jcam_' + listId;
  html += '<input type="file" id="' + inputId + '" accept=".pdf,.doc,.docx,.xlsx,.jpg,.jpeg,.png,.heic,.zip,.webp" multiple style="display:none;" onchange="jobsHandleFileUpload(' + jobId + ',\'' + jEsc(folderKey) + '\',this)"/>';
  html += '<input type="file" id="' + camId + '" accept="image/*" capture="environment" style="display:none;" onchange="jobsHandleFileUpload(' + jobId + ',\'' + jEsc(folderKey) + '\',this)"/>';
  html += '<div style="position:relative;margin-top:12px;">';
  html += '<button class="j-btn" style="font-size:11px;padding:7px 14px;" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'block\'?\'none\':\'block\';">+ Upload</button>';
  html += '<div class="j-upload-menu" style="display:none;">';
  html += '<button class="j-upload-opt" onclick="document.getElementById(\'' + inputId + '\').click();this.closest(\'.j-upload-menu\').style.display=\'none\';"><span style="font-size:22px;">📁</span><div><div>Browse Files</div><div class="j-upload-sub">Pick from phone or computer</div></div></button>';
  html += '<div style="height:1px;background:rgba(212,168,66,0.12);margin:2px 12px;"></div>';
  html += '<button class="j-upload-opt" onclick="document.getElementById(\'' + camId + '\').click();this.closest(\'.j-upload-menu\').style.display=\'none\';"><span style="font-size:22px;">📷</span><div><div>Scan / Take Photo</div><div class="j-upload-sub">Use camera to scan a document</div></div></button>';
  html += '<div style="height:1px;background:rgba(212,168,66,0.12);margin:2px 12px;"></div>';
  html += '<button class="j-upload-opt" style="color:rgba(212,168,66,0.45);font-size:12px;" onclick="this.closest(\'.j-upload-menu\').style.display=\'none\';">✕ Cancel</button>';
  html += '</div></div>';
  return html;
}

/* ── File upload handler ─────────────────────────────────────────────── */
window.jobsHandleFileUpload = function(jobId, folderKey, inputEl) {
  var files = inputEl.files;
  if (!files || !files.length) return;
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === jobId; });
  if (!job) return;
  if (!job.docs) job.docs = {};

  for (var i = 0; i < files.length; i++) {
    var name = files[i].name;
    if (folderKey.indexOf('otherSub_') === 0) {
      var sub = folderKey.replace('otherSub_', '');
      if (!job.docs.otherJobDocuments) job.docs.otherJobDocuments = {};
      if (!job.docs.otherJobDocuments[sub]) job.docs.otherJobDocuments[sub] = [];
      job.docs.otherJobDocuments[sub].push(name);
    } else if (folderKey.indexOf('photo_') === 0) {
      var photoSub = folderKey.replace('photo_', '');
      if (!job.docs.jobPhotos) job.docs.jobPhotos = {};
      if (!job.docs.jobPhotos[photoSub]) job.docs.jobPhotos[photoSub] = [];
      job.docs.jobPhotos[photoSub].push(name);
    } else {
      if (!job.docs[folderKey]) job.docs[folderKey] = [];
      job.docs[folderKey].push(name);
    }
  }
  inputEl.value = '';
  jobsSave(jobs);
  renderJobsUI();
};

/* ── Checks tab ──────────────────────────────────────────────────────── */
function renderJobsChecks(job) {
  var checks = job.checks || [];
  var html = '';
  checks.forEach(function(c, i) {
    html += '<div style="padding:12px 14px;border-radius:10px;background:rgba(212,168,66,0.04);border:1px solid rgba(212,168,66,0.12);margin-bottom:6px;">';
    html += '<div style="display:flex;justify-content:space-between;">';
    html += '<div><div style="color:#f0d078;font-size:14px;font-weight:700;font-family:Oswald,sans-serif;">' + jEsc(c.from) + '</div><div style="color:rgba(212,168,66,0.45);font-size:12px;">' + jEsc(c.date) + '</div></div>';
    html += '<div style="text-align:right;"><div style="color:#f0d078;font-size:18px;font-weight:700;font-family:DM Mono,monospace;">$' + (c.amount||0).toLocaleString() + '</div><div style="font-size:10px;color:#4ade80;font-family:Oswald,sans-serif;font-weight:700;">' + jEsc(c.status) + '</div></div>';
    html += '</div>';
    if (c.photo) html += '<div style="margin-top:8px;display:flex;align-items:center;gap:8px;padding:6px 10px;border-radius:6px;background:rgba(212,168,66,0.03);"><span>🖼️</span><span style="color:rgba(212,168,66,0.45);font-size:12px;">' + jEsc(c.photo) + '</span></div>';
    html += '</div>';
  });

  // Add check form
  html += '<div style="margin-top:12px;">';
  html += '<button class="j-btn" style="font-size:11px;padding:7px 14px;" onclick="jobsShowAddCheck(' + job.id + ')">+ Add Check</button>';
  html += '<div id="jobsAddCheckForm" style="display:none;margin-top:12px;padding:16px;border-radius:12px;background:rgba(212,168,66,0.04);border:1.5px solid rgba(212,168,66,0.35);">';
  html += '<div style="color:#f0d078;font-size:13px;font-weight:700;font-family:Oswald,sans-serif;text-transform:uppercase;margin-bottom:12px;">Add New Check</div>';

  // Photo inputs
  html += '<div style="color:rgba(212,168,66,0.45);font-size:11px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;margin-bottom:6px;">Check Photo</div>';
  html += '<input type="file" id="jCheckFile" accept=".pdf,.jpg,.jpeg,.png,.heic,.webp" style="display:none;" onchange="document.getElementById(\'jCheckPhotoName\').textContent=this.files[0]?.name||\'\'"/>';
  html += '<input type="file" id="jCheckCam" accept="image/*" capture="environment" style="display:none;" onchange="document.getElementById(\'jCheckPhotoName\').textContent=this.files[0]?.name||\'\'"/>';
  html += '<div style="display:flex;gap:8px;margin-bottom:12px;">';
  html += '<button class="j-btn" style="font-size:11px;padding:8px 14px;" onclick="document.getElementById(\'jCheckFile\').click()">📁 Browse</button>';
  html += '<button class="j-btn" style="font-size:11px;padding:8px 14px;" onclick="document.getElementById(\'jCheckCam\').click()">📷 Scan</button>';
  html += '<span id="jCheckPhotoName" style="color:#4ade80;font-size:12px;align-self:center;"></span>';
  html += '</div>';

  html += '<div style="margin-bottom:8px;"><div style="color:rgba(212,168,66,0.45);font-size:11px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;margin-bottom:4px;">From</div><input id="jCheckFrom" class="j-input" placeholder="Insurance Co." /></div>';
  html += '<div style="margin-bottom:12px;"><div style="color:rgba(212,168,66,0.45);font-size:11px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;margin-bottom:4px;">Amount</div><input id="jCheckAmount" class="j-input" placeholder="0.00" style="text-align:right;font-family:DM Mono,monospace;" /></div>';
  html += '<div style="display:flex;gap:8px;">';
  html += '<button class="j-btn" style="background:rgba(74,222,128,0.1);border-color:rgba(74,222,128,0.3);color:#4ade80;" onclick="jobsSaveCheck(' + job.id + ')">✓ Save Check</button>';
  html += '<button class="j-btn" onclick="document.getElementById(\'jobsAddCheckForm\').style.display=\'none\';">Cancel</button>';
  html += '</div></div></div>';

  return html;
}

window.jobsShowAddCheck = function(jobId) {
  var form = document.getElementById('jobsAddCheckForm');
  if (form) form.style.display = 'block';
};

window.jobsSaveCheck = function(jobId) {
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === jobId; });
  if (!job) return;
  if (!job.checks) job.checks = [];
  var photoEl = document.getElementById('jCheckPhotoName');
  job.checks.push({
    from: (document.getElementById('jCheckFrom')||{}).value || 'Insurance Co.',
    amount: Number(((document.getElementById('jCheckAmount')||{}).value||'0').replace(/[^0-9.]/g,'')) || 0,
    date: new Date().toLocaleDateString('en-US'),
    status: 'received',
    photo: photoEl ? photoEl.textContent : ''
  });
  jobsSave(jobs);
  renderJobsUI();
};

/* ── Job Photos tab ──────────────────────────────────────────────────── */
function renderJobsPhotos(job) {
  var photos = (job.docs||{}).jobPhotos || {};
  var folders = [
    {key:'inspectionPics',label:'Inspection Pictures',icon:'📸'},
    {key:'adjusterPics',label:'Adjuster Inspection Pictures',icon:'🔍'},
    {key:'otherPhotos',label:'Other Photos',icon:'🖼️'}
  ];

  if (_jobsPhotoFolder) {
    var pf = folders.find(function(f){ return f.key === _jobsPhotoFolder; });
    var files = photos[_jobsPhotoFolder] || [];
    var html = '<button class="j-btn" style="margin-bottom:12px;" onclick="_jobsPhotoFolder=null;renderJobsUI();">← Back</button>';
    html += '<div style="color:#f0d078;font-size:15px;font-weight:700;font-family:Oswald,sans-serif;text-transform:uppercase;margin-bottom:12px;">' + (pf?pf.icon:'') + ' ' + (pf?pf.label:'') + '</div>';
    if (files.length) {
      html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;">';
      files.forEach(function(f){
        html += '<div style="padding:10px 12px;border-radius:8px;background:rgba(212,168,66,0.04);border:1px solid rgba(212,168,66,0.1);display:flex;align-items:center;gap:8px;">🖼️ <span style="color:#f0d078;font-size:12px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + jEsc(f) + '</span></div>';
      });
      html += '</div>';
    } else {
      html += '<p style="color:rgba(212,168,66,0.45);font-size:14px;">No photos yet. Take a photo to save directly to this folder.</p>';
    }
    // Camera capture
    var camId = 'jPhotoCam_' + _jobsPhotoFolder;
    html += '<input type="file" id="' + camId + '" accept="image/*" capture="environment" style="display:none;" onchange="jobsHandleFileUpload(' + job.id + ',\'photo_' + _jobsPhotoFolder + '\',this)"/>';
    html += '<button class="j-btn" onclick="document.getElementById(\'' + camId + '\').click()">📷 Take Photo — Saves to App</button>';
    html += '<p style="color:rgba(212,168,66,0.45);font-size:11px;margin-top:8px;">Photos save directly to this folder in the app, not to your phone.</p>';
    return html;
  }

  var html = '<p style="color:rgba(212,168,66,0.45);font-size:12px;margin-bottom:6px;">Photos taken here save directly to the app — not your phone.</p>';
  folders.forEach(function(pf) {
    var cnt = (photos[pf.key]||[]).length;
    html += '<button class="j-btn" style="width:100%;justify-content:flex-start;margin-bottom:8px;padding:14px 16px;" onclick="_jobsPhotoFolder=\'' + pf.key + '\';renderJobsUI();">';
    html += '<span style="font-size:24px;">' + pf.icon + '</span> ' + jEsc(pf.label) + ' (' + cnt + ') →';
    html += '</button>';
  });
  return html;
}

/* ── Homeowner Info ──────────────────────────────────────────────────── */
function renderJobsHomeownerInfo(job) {
  var html = '<div style="display:flex;flex-direction:column;gap:10px;">';
  var fields = [
    {l:'Name',f:'name'},{l:'Phone',f:'phone'},{l:'Address',f:'address'},
    {l:'Email',f:'email'},{l:'Insurance',f:'insurance'},{l:'Claim #',f:'claimNumber'},
    {l:'Date of Loss',f:'dateOfLoss',type:'date'}
  ];
  fields.forEach(function(fld) {
    html += '<div style="display:flex;gap:14px;align-items:center;padding-bottom:8px;border-bottom:1px solid rgba(212,168,66,0.08);">';
    html += '<span style="width:85px;flex-shrink:0;color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;font-family:Oswald,sans-serif;">' + fld.l + '</span>';
    html += '<input class="j-input" type="' + (fld.type||'text') + '" value="' + jEsc(job[fld.f]||'') + '" onchange="jobsUpdateField(' + job.id + ',\'' + fld.f + '\',this.value)"/>';
    html += '</div>';
  });

  // Damage areas display
  var dmg = (job.damageAreas||[]).join(', ') || 'None selected';
  html += '<div style="display:flex;gap:14px;align-items:baseline;padding-bottom:8px;">';
  html += '<span style="width:85px;flex-shrink:0;color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;font-family:Oswald,sans-serif;">Damage</span>';
  html += '<span style="color:#f0d078;font-size:14px;font-weight:700;">' + jEsc(dmg) + '</span>';
  html += '</div>';

  html += '</div>';
  return html;
}

/* ── Update a job field ──────────────────────────────────────────────── */
window.jobsUpdateField = function(jobId, field, value) {
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === jobId; });
  if (!job) return;
  job[field] = value;
  job.lastTouched = 'Just now';
  jobsSave(jobs);
  // Don't re-render to avoid losing focus, but update stage display if needed
};

/* ── Toggle manual task ──────────────────────────────────────────────── */
window.jobsToggleTask = function(jobId, taskKey) {
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === jobId; });
  if (!job) return;
  if (!job.tasks) job.tasks = {};
  job.tasks[taskKey] = !job.tasks[taskKey];
  jobsSave(jobs);
  renderJobsUI();
};

/* ── Checklist renderer ──────────────────────────────────────────────── */
function renderJobsChecklist(job, t, m) {
  var html = '<div style="display:flex;flex-direction:column;gap:2px;">';

  function autoItem(label, key) {
    var chk = !!t[key];
    html += '<div class="j-auto-check"><div class="j-auto-box' + (chk?' checked':'') + '">' + (chk?'<span style="color:#4ade80;font-size:11px;font-weight:700;">✓</span>':'') + '</div>';
    html += '<span class="j-auto-label' + (chk?' checked':'') + '">' + jEsc(label) + '</span>';
    if (chk) html += '<span class="j-auto-badge">AUTO</span>';
    html += '</div>';
  }

  function milestone(label, key) {
    var c = !!m[key];
    html += '<div class="j-milestone' + (c?' complete':'') + '">';
    html += '<div class="j-milestone-dot">' + (c?'<span style="color:#000;font-size:14px;font-weight:900;">✓</span>':'') + '</div>';
    html += '<span class="j-milestone-label">' + jEsc(label) + '</span>';
    if (c) html += '<span style="margin-left:auto;font-size:10px;color:#4ade80;font-family:Oswald,sans-serif;font-weight:700;background:rgba(74,222,128,0.1);padding:3px 8px;border-radius:6px;text-transform:uppercase;letter-spacing:1px;">Complete</span>';
    html += '</div>';
  }

  function manualItem(label, key) {
    var chk = !!(job.tasks||{})[key];
    html += '<div class="j-manual-check" onclick="jobsToggleTask(' + job.id + ',\'' + key + '\')" style="' + (chk?'background:rgba(74,200,100,0.06);':'') + '">';
    html += '<div style="width:18px;height:18px;border-radius:5px;flex-shrink:0;border:2px solid ' + (chk?'#4ade80':'rgba(212,168,66,0.35)') + ';background:' + (chk?'rgba(74,222,128,0.15)':'transparent') + ';display:flex;align-items:center;justify-content:center;">' + (chk?'<span style="color:#4ade80;font-size:11px;font-weight:700;">✓</span>':'') + '</div>';
    html += '<span style="color:' + (chk?'rgba(180,220,180,0.7)':'#f0d078') + ';font-size:12px;font-family:Oswald,sans-serif;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;text-decoration:' + (chk?'line-through':'none') + ';opacity:' + (chk?'0.5':'1') + ';">' + jEsc(label) + '</span>';
    html += '</div>';
  }

  // New Lead
  autoItem('Name Entered', 'name entered');
  autoItem('Number Entered', 'number entered');
  milestone('New Lead', 'new lead');

  // Claim Filed
  autoItem('Signed Contingency (upload to Contingency)', 'signed contingency');
  autoItem('Inspection Pictures (upload to Job Photos)', 'inspection pictures');
  autoItem('Claim Number (fill in Homeowner Info)', 'claim number');
  milestone('Claim Filed', 'claim filed');

  // Approved
  autoItem('Adjuster Inspection Pics (upload to Job Photos)', 'adjuster inspection pics');
  autoItem('Claim Report (upload to Insurance Paperwork)', 'claim report');
  milestone('Approved', 'approved');

  // Production
  autoItem('First Check Turned In (add in Checks tab)', 'first check turned in');
  autoItem('Production Paperwork (upload to Other Job Docs)', 'production paperwork');
  milestone('Production', 'production');

  // Built
  manualItem('Labor Invoice', 'labor invoice');
  manualItem('Material Invoice', 'material invoice');
  milestone('Built', 'built');

  // Complete
  manualItem('Final Insurance Invoice', 'final insurance invoice');
  milestone('Complete', 'complete');

  // Payments link
  html += '<div style="display:flex;align-items:center;gap:12px;padding:11px 14px;border-radius:8px;cursor:pointer;background:rgba(212,168,66,0.08);border:1.5px solid rgba(212,168,66,0.35);margin-top:8px;" onclick="_jobsPage=\'payments\';renderJobsUI();">';
  html += '<div style="width:22px;height:22px;border-radius:6px;background:rgba(212,168,66,0.15);border:2px solid rgba(212,168,66,0.65);display:flex;align-items:center;justify-content:center;"><span style="color:#f0d078;font-size:13px;font-weight:700;">$</span></div>';
  html += '<span style="color:#f0d078;font-size:14px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">Payments & Job Profitability</span>';
  html += '<span style="margin-left:auto;color:#d4a842;font-size:18px;">→</span>';
  html += '</div>';

  html += '</div>';
  return html;
}

/* ── PAGE: Payments & Profitability ──────────────────────────────────── */
function renderJobsPayments(el) {
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === _jobsSelId; });
  if (!job) { _jobsPage = 'list'; renderJobsUI(); return; }

  var p = job.payments || {};
  var tPay = (p.first||0) + (p.deductible||0) + (p.second||0) + (p.other||0);
  var lab = job.labor || {};
  var tLab = (lab.roof||0) + (lab.gutters||0) + (lab.siding||0) + (lab.windows||0);
  var mat = job.material || {};
  var tMat = (mat.roof||0) + (mat.gutters||0) + (mat.siding||0) + (mat.windows||0);
  var rep = job.rep || {};
  var tRep = (rep.contingency||0) + (rep.commission||0) + (rep.other||0);
  var tExp = tLab + tMat + tRep;
  var tProf = tPay - tExp;
  var margin = tPay > 0 ? ((tProf / tPay) * 100).toFixed(1) : '0.0';

  var html = '<div style="max-width:1160px;margin:0 auto;">';
  html += '<button class="j-btn" onclick="_jobsPage=\'detail\';renderJobsUI();">← Back to ' + jEsc(job.name) + '</button>';

  // Header
  html += '<div class="j-glass" style="margin-top:14px;padding:20px 26px;margin-bottom:18px;">';
  html += '<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:14px;">';
  html += '<h2 style="font-family:Oswald,sans-serif;font-size:30px;color:#f0d078;letter-spacing:3px;margin:0;text-transform:uppercase;">' + jEsc(job.name) + ' — Payments</h2>';
  html += '<div style="display:flex;gap:24px;">';
  [['Revenue',jFmt(tPay),'#f0d078'],['Expenses',jFmt(tExp),'#ff6b6b'],['Profit',jFmt(tProf),tProf>=0?'#4ade80':'#ff6b6b'],['Margin',margin+'%',Number(margin)>=30?'#4ade80':'#d4a842']].forEach(function(m){
    html += '<div style="text-align:center;"><div style="color:rgba(212,168,66,0.45);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;font-family:Oswald,sans-serif;">' + m[0] + '</div>';
    html += '<div style="color:' + m[2] + ';font-size:22px;font-weight:700;font-family:DM Mono,monospace;margin-top:4px;">' + m[1] + '</div></div>';
  });
  html += '</div></div></div>';

  // Grid of 4 panels
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:18px;">';

  // Payments
  html += '<div class="j-glass" style="padding:20px 24px;"><div class="j-hdr">Payments Received</div>';
  [['first','1st Payment'],['deductible','Deductible'],['second','2nd Payment'],['other','Other']].forEach(function(f){
    html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">';
    html += '<span style="width:120px;color:#d4a842;font-size:12px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">' + f[1] + '</span>';
    html += '<input class="j-input" style="text-align:right;font-family:DM Mono,monospace;" value="' + (p[f[0]]||0) + '" onchange="jobsUpdatePayment(' + job.id + ',\'payments\',\'' + f[0] + '\',this.value)"/>';
    html += '</div>';
  });
  html += '<div style="display:flex;justify-content:space-between;margin-top:14px;padding-top:12px;border-top:1px solid rgba(212,168,66,0.35);"><span style="color:#f0d078;font-size:13px;font-weight:700;font-family:Oswald,sans-serif;text-transform:uppercase;">Total</span><span style="color:#f0d078;font-size:18px;font-weight:700;font-family:DM Mono,monospace;">' + jFmt(tPay) + '</span></div>';
  html += '</div>';

  // Labor
  html += '<div class="j-glass" style="padding:20px 24px;"><div class="j-hdr">Labor Costs</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;margin-bottom:8px;">';
  ['roof','gutters','siding','windows'].forEach(function(k){
    html += '<div style="color:#d4a842;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;font-family:Oswald,sans-serif;text-align:right;padding-right:10px;">' + k + '</div>';
  });
  html += '</div><div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;">';
  ['roof','gutters','siding','windows'].forEach(function(k){
    html += '<input class="j-input" style="text-align:right;font-family:DM Mono,monospace;" value="' + (lab[k]||0) + '" onchange="jobsUpdatePayment(' + job.id + ',\'labor\',\'' + k + '\',this.value)"/>';
  });
  html += '</div>';
  html += '<div style="display:flex;justify-content:space-between;margin-top:14px;padding-top:12px;border-top:1px solid rgba(212,168,66,0.35);"><span style="color:#f0d078;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">Total Labor</span><span style="color:#ff6b6b;font-size:18px;font-weight:700;font-family:DM Mono,monospace;">' + jFmt(tLab) + '</span></div></div>';

  // Material
  html += '<div class="j-glass" style="padding:20px 24px;"><div class="j-hdr">Material Costs</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;margin-bottom:8px;">';
  ['roof','gutters','siding','windows'].forEach(function(k){
    html += '<div style="color:#d4a842;font-size:10px;font-weight:700;text-transform:uppercase;font-family:Oswald,sans-serif;text-align:right;padding-right:10px;">' + k + '</div>';
  });
  html += '</div><div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;">';
  ['roof','gutters','siding','windows'].forEach(function(k){
    html += '<input class="j-input" style="text-align:right;font-family:DM Mono,monospace;" value="' + (mat[k]||0) + '" onchange="jobsUpdatePayment(' + job.id + ',\'material\',\'' + k + '\',this.value)"/>';
  });
  html += '</div>';
  html += '<div style="display:flex;justify-content:space-between;margin-top:14px;padding-top:12px;border-top:1px solid rgba(212,168,66,0.35);"><span style="color:#f0d078;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">Total Material</span><span style="color:#ff6b6b;font-size:18px;font-weight:700;font-family:DM Mono,monospace;">' + jFmt(tMat) + '</span></div></div>';

  // Rep Pay
  html += '<div class="j-glass" style="padding:20px 24px;"><div class="j-hdr">Rep Pay</div>';
  html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;"><span style="width:120px;color:#d4a842;font-size:12px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">Rep Name</span>';
  html += '<input class="j-input" value="' + jEsc(rep.name||'') + '" onchange="jobsUpdatePayment(' + job.id + ',\'rep\',\'name\',this.value)"/></div>';
  [['contingency','Contingency'],['commission','Commission'],['other','Other']].forEach(function(f){
    html += '<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">';
    html += '<span style="width:120px;color:#d4a842;font-size:12px;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">' + f[1] + '</span>';
    html += '<input class="j-input" style="text-align:right;font-family:DM Mono,monospace;" value="' + (rep[f[0]]||0) + '" onchange="jobsUpdatePayment(' + job.id + ',\'rep\',\'' + f[0] + '\',this.value)"/>';
    html += '</div>';
  });
  html += '<div style="display:flex;justify-content:space-between;margin-top:14px;padding-top:12px;border-top:1px solid rgba(212,168,66,0.35);"><span style="color:#f0d078;font-family:Oswald,sans-serif;font-weight:700;text-transform:uppercase;">Total Rep Pay</span><span style="color:#ff6b6b;font-size:18px;font-weight:700;font-family:DM Mono,monospace;">' + jFmt(tRep) + '</span></div></div>';

  html += '</div>'; // end grid

  // Bottom totals
  html += '<div class="j-glass" style="margin-top:18px;padding:22px 28px;">';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:20px;text-align:center;">';
  [['Total Revenue',jFmt(tPay),'#f0d078'],['Total Expenses',jFmt(tExp),'#ff6b6b'],['Total Profit',jFmt(tProf),tProf>=0?'#4ade80':'#ff6b6b'],['Profit Margin',margin+'%',Number(margin)>=30?'#4ade80':'#d4a842']].forEach(function(m){
    html += '<div><div style="color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;font-family:Oswald,sans-serif;margin-bottom:6px;">' + m[0] + '</div>';
    html += '<div style="color:' + m[2] + ';font-size:28px;font-weight:700;font-family:DM Mono,monospace;">' + m[1] + '</div></div>';
  });
  html += '</div></div>';

  html += '</div>';
  el.innerHTML = html;
}

window.jobsUpdatePayment = function(jobId, section, key, value) {
  var jobs = jobsLoad();
  var job = jobs.find(function(j){ return j.id === jobId; });
  if (!job) return;
  if (section === 'rep' && key === 'name') {
    job.rep = job.rep || {}; job.rep.name = value;
  } else {
    var num = Number(String(value).replace(/[^0-9.-]/g,'')) || 0;
    if (!job[section]) job[section] = {};
    job[section][key] = num;
  }
  jobsSave(jobs);
  renderJobsUI();
};

/* ── PAGE: New Lead Form (with globe background) ─────────────────────── */
function renderJobsNewLead(el) {
  var html = '';
  // Fixed globe background
  html += '<div id="jobsNewLeadGlobe"><img src="assets/golden-globe.png" alt=""/></div>';
  // Fixed back button
  html += '<div style="position:fixed;top:16px;left:16px;z-index:20;"><button class="j-btn" onclick="showJobsView();">← Back</button></div>';

  // Scrollable content
  html += '<div class="j-newlead-scroll">';
  html += '<div class="j-newlead-spacer"></div>';
  html += '<div class="j-newlead-title"><h1 style="font-family:Oswald,sans-serif;font-size:32px;color:#f0d078;letter-spacing:6px;margin:0;text-transform:uppercase;text-shadow:0 0 30px rgba(212,168,66,0.6),0 2px 15px rgba(0,0,0,0.9);">New Lead</h1></div>';
  html += '<div class="j-newlead-fade"></div>';

  html += '<div class="j-newlead-panel">';
  html += '<div class="j-newlead-handle"></div>';
  html += '<div class="j-hdr">Homeowner Information</div>';
  html += '<p style="color:rgba(212,168,66,0.45);font-size:12px;margin-bottom:18px;margin-top:-6px;">Fill in name and phone number to create the lead.</p>';

  // Fields
  var fields = [
    {l:'Name *',id:'jnlName',ph:'Homeowner\'s name'},
    {l:'Phone *',id:'jnlPhone',ph:'(555) 555-0000',type:'tel'},
    {l:'Address',id:'jnlAddress',ph:'Street, City, State ZIP'},
    {l:'Email',id:'jnlEmail',ph:'email@example.com',type:'email'},
    {l:'Claim #',id:'jnlClaim',ph:'Claim number'},
    {l:'Date of Loss',id:'jnlDOL',ph:'',type:'date'}
  ];

  html += '<div style="display:flex;flex-direction:column;gap:12px;">';
  fields.forEach(function(f){
    html += '<div style="display:flex;gap:14px;align-items:center;padding-bottom:10px;border-bottom:1px solid rgba(212,168,66,0.08);">';
    html += '<span style="width:100px;flex-shrink:0;color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;font-family:Oswald,sans-serif;">' + f.l + '</span>';
    html += '<input id="' + f.id + '" class="j-input" type="' + (f.type||'text') + '" placeholder="' + jEsc(f.ph) + '" style="background:rgba(0,0,0,0.25);border:1.5px solid rgba(212,168,66,0.35);border-radius:8px;padding:10px 12px;"/>';
    html += '</div>';
  });

  // Insurance dropdown
  html += '<div style="display:flex;gap:14px;align-items:center;padding-bottom:10px;border-bottom:1px solid rgba(212,168,66,0.08);">';
  html += '<span style="width:100px;flex-shrink:0;color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;font-family:Oswald,sans-serif;">Insurance</span>';
  html += '<select id="jnlInsurance" class="j-input" style="background:rgba(0,0,0,0.25);border:1.5px solid rgba(212,168,66,0.35);border-radius:8px;padding:10px 12px;">';
  html += '<option value="">Select insurance company</option>';
  JOBS_INSURANCE_COS.forEach(function(co){
    html += '<option value="' + jEsc(co) + '">' + jEsc(co) + '</option>';
  });
  html += '</select></div>';

  // Damage areas multi-select
  html += '<div style="padding-bottom:10px;border-bottom:1px solid rgba(212,168,66,0.08);">';
  html += '<div style="display:flex;gap:14px;align-items:center;margin-bottom:8px;">';
  html += '<span style="width:100px;flex-shrink:0;color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;font-family:Oswald,sans-serif;">Damage Found</span>';
  html += '<button id="jnlDmgBtn" class="j-btn" style="flex:1;justify-content:space-between;font-size:12px;font-weight:600;text-transform:none;letter-spacing:0;" onclick="document.getElementById(\'jnlDmgGrid\').style.display=document.getElementById(\'jnlDmgGrid\').style.display===\'block\'?\'none\':\'block\';">';
  html += '<span id="jnlDmgText">Select damaged areas</span> <span style="color:rgba(212,168,66,0.45);">▼</span></button>';
  html += '</div>';
  html += '<div id="jnlDmgGrid" class="j-damage-grid" style="display:none;margin-left:0;">';
  JOBS_DAMAGE_AREAS.forEach(function(area){
    html += '<button class="j-damage-opt" onclick="jobsToggleNewLeadDamage(this,\'' + jEsc(area) + '\')">';
    html += '<div class="j-damage-box"></div>';
    html += '<span style="color:#f0d078;font-size:12px;font-weight:600;">' + jEsc(area) + '</span></button>';
  });
  html += '</div></div>';

  // Notes
  html += '<div><span style="display:block;color:#d4a842;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:1px;font-family:Oswald,sans-serif;margin-bottom:4px;">Notes</span>';
  html += '<textarea id="jnlNotes" class="j-input" style="min-height:80px;resize:vertical;background:rgba(0,0,0,0.25);border:1.5px solid rgba(212,168,66,0.35);border-radius:8px;padding:10px 12px;" placeholder="Any additional notes..."></textarea>';
  html += '</div>';

  html += '</div>'; // end fields

  // Buttons
  html += '<div style="display:flex;gap:10px;margin-top:20px;">';
  html += '<button class="j-btn" style="padding:12px 28px;font-size:15px;background:rgba(74,222,128,0.12);border-color:rgba(74,222,128,0.35);color:#4ade80;" onclick="jobsCreateNewLead()">✓ Create Lead</button>';
  html += '<button class="j-btn" style="padding:12px 20px;" onclick="showJobsView();">Cancel</button>';
  html += '</div>';

  html += '</div>'; // end panel
  html += '</div>'; // end scroll

  el.innerHTML = html;
}

// Track selected damage areas for new lead form
window._jnlDamageAreas = [];

window.jobsToggleNewLeadDamage = function(btn, area) {
  var idx = window._jnlDamageAreas.indexOf(area);
  if (idx >= 0) {
    window._jnlDamageAreas.splice(idx, 1);
    btn.classList.remove('selected');
  } else {
    window._jnlDamageAreas.push(area);
    btn.classList.add('selected');
  }
  var txt = document.getElementById('jnlDmgText');
  if (txt) txt.textContent = window._jnlDamageAreas.length > 0 ? window._jnlDamageAreas.join(', ') : 'Select damaged areas';
};

window.jobsCreateNewLead = function() {
  var name = (document.getElementById('jnlName')||{}).value || '';
  if (!name.trim()) { alert('Name is required.'); return; }

  var newJob = jobsCreateBlank();
  newJob.name = name.trim();
  newJob.phone = (document.getElementById('jnlPhone')||{}).value || '';
  newJob.address = (document.getElementById('jnlAddress')||{}).value || '';
  newJob.email = (document.getElementById('jnlEmail')||{}).value || '';
  newJob.claimNumber = (document.getElementById('jnlClaim')||{}).value || '—';
  newJob.dateOfLoss = (document.getElementById('jnlDOL')||{}).value || '';
  newJob.insurance = (document.getElementById('jnlInsurance')||{}).value || 'Unknown';
  newJob.damageAreas = window._jnlDamageAreas.slice();
  newJob.notes = (document.getElementById('jnlNotes')||{}).value || '';

  var jobs = jobsLoad();
  jobs.push(newJob);
  jobsSave(jobs);

  window._jnlDamageAreas = [];
  _jobsSelId = newJob.id;
  _jobsPage = 'detail';
  renderJobsUI();
};


/* ═══════════════════════════════════════════════════════════════════════
   SECTION 4: WIRE THE RAIL BUTTONS
   
   Add these lines INSIDE the wireNavigation() function,
   right after the existing campaignNavBtn block (around line where
   campaignNavBtn.addEventListener is).
   ═══════════════════════════════════════════════════════════════════════ */

// Wire My Jobs rail button
(function() {
  var myJobsBtn = document.querySelector('button.ht-rail-btn[data-view="myjobs"]');
  if (myJobsBtn) {
    myJobsBtn.addEventListener('click', function() {
      showJobsView();
    });
  }
  // Wire New Lead rail button (opens new lead form directly)
  var newLeadBtn = document.querySelector('button.ht-rail-btn[data-view="newlead"]');
  if (newLeadBtn) {
    newLeadBtn.addEventListener('click', function() {
      showJobsNewLead();
    });
  }
})();
