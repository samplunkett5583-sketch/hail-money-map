from pathlib import Path

PUBLIC = Path('public/index.html')
text = PUBLIC.read_text(encoding='utf-8')

old_grid = '''              <div class="crm-company-docs-grid" aria-label="Company document categories">
                <section class="crm-company-docs-category"><h2>Contingencies</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Contracts</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Shingle and Manufacturer Specifications</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Company Insurance Documents</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Roofing Licenses</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
                <section class="crm-company-docs-category"><h2>Other Company Documents</h2><div class="crm-company-docs-empty">No documents uploaded yet</div></section>
              </div>'''

categories = [
    ('contingencies', 'Contingencies'),
    ('contracts', 'Contracts'),
    ('specifications', 'Shingle and Manufacturer Specifications'),
    ('insurance', 'Company Insurance Documents'),
    ('licenses', 'Roofing Licenses'),
    ('other', 'Other Company Documents'),
]

if 'id="crm-company-doc-file-input"' not in text:
    cards = []
    for key, label in categories:
        cards.append(f'''                <section class="crm-company-docs-category" data-company-doc-category="{key}">
                  <div class="crm-company-docs-category-head"><h2>{label}</h2><button class="btn btn-primary crm-company-doc-upload" type="button" data-company-doc-upload="{key}">Upload Document</button></div>
                  <div class="crm-company-docs-list" data-company-doc-list="{key}"><div class="crm-company-docs-empty">No documents uploaded yet</div></div>
                </section>''')
    new_grid = '''              <div class="crm-company-docs-status" id="crm-company-docs-status" role="status">Company files are shared with your team.</div>
              <div class="crm-company-docs-grid" aria-label="Company document categories">
%s
              </div>
              <input id="crm-company-doc-file-input" type="file" accept=".pdf,.doc,.docx,.xls,.xlsx,.png,.jpg,.jpeg,.webp" hidden />''' % '\n'.join(cards)
    if old_grid not in text:
        raise SystemExit('Company Documents grid not found')
    text = text.replace(old_grid, new_grid, 1)

style = r'''<style id="hm-company-documents-style">
.crm-company-docs-status{margin:0 0 14px;min-height:20px;color:#667085;font-size:13px}.crm-company-docs-status.is-error{color:#b42318}.crm-company-docs-category-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px}.crm-company-docs-category-head h2{margin:0}.crm-company-doc-upload{display:none;flex:0 0 auto}.crm-company-docs-list{display:grid;gap:9px}.crm-company-doc-row{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:11px 12px;border:1px solid #e2e7ee;border-radius:12px;background:#f8fafc}.crm-company-doc-info{min-width:0}.crm-company-doc-name{color:#172033;font-size:14px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.crm-company-doc-meta{margin-top:3px;color:#7a8494;font-size:11px}.crm-company-doc-actions{display:flex;align-items:center;gap:7px;flex:0 0 auto}.crm-company-doc-actions .btn{padding:7px 10px;font-size:12px}@media(max-width:640px){.crm-company-docs-category-head,.crm-company-doc-row{align-items:stretch;flex-direction:column}.crm-company-doc-upload{width:100%;justify-content:center}.crm-company-doc-actions{width:100%}.crm-company-doc-actions .btn{flex:1 1 0}}
</style>'''
if 'id="hm-company-documents-style"' not in text:
    if '</head>' not in text:
        raise SystemExit('Head closing tag not found')
    text = text.replace('</head>', style + '\n</head>', 1)

script = r'''<script id="hm-company-documents-script">
(function(){
  var MAX_BYTES=25*1024*1024, docs=[];
  function status(msg,bad){var e=document.getElementById('crm-company-docs-status');if(!e)return;e.textContent=String(msg||'');e.classList.toggle('is-error',!!bad)}
  function canManage(){return ['Owner','Admin'].indexOf(String(typeof crmGetRole==='function'?crmGetRole():'').trim())!==-1}
  async function tokenCanManage(){var u=window.auth&&window.auth.currentUser;if(!u)return false;var t=await u.getIdTokenResult(false),c=t.claims||{};return c.employee===true&&['Owner','Admin'].indexOf(String(c.hmRole||'').trim())!==-1}
  async function orgId(){return typeof crmResolveFirestoreOrgId==='function'?await crmResolveFirestoreOrgId():''}
  function ref(id){return window.db.collection('organizations').doc(id).collection('appState').doc('companyDocuments')}
  function safe(v){return String(v||'x').replace(/[^a-zA-Z0-9._-]+/g,'_').slice(0,120)||'x'}
  function fmt(n){n=Number(n||0);return n<1024?n+' B':n<1048576?(n/1024).toFixed(1)+' KB':(n/1048576).toFixed(1)+' MB'}
  function find(id){return docs.find(function(d){return String(d&&d.id||'')===String(id||'')})||null}
  function client(){return (typeof sb!=='undefined'&&sb)||window.supabaseClient||null}
  function isEditable(d){var t=String(d&&d.contentType||'').toLowerCase(),n=String(d&&d.name||'').toLowerCase();return t==='application/pdf'||t.indexOf('image/')===0||/\.(pdf|png|jpe?g|webp)$/.test(n)}
  function blobToDataUrl(blob){return new Promise(function(resolve,reject){var r=new FileReader();r.onerror=function(){reject(new Error('The document could not be read.'))};r.onload=function(){resolve(String(r.result||''))};r.readAsDataURL(blob)})}
  function render(){
    var manage=canManage();document.querySelectorAll('[data-company-doc-upload]').forEach(function(b){b.style.display=manage?'inline-flex':'none'});
    document.querySelectorAll('[data-company-doc-list]').forEach(function(list){
      var cat=String(list.getAttribute('data-company-doc-list')||''),items=docs.filter(function(d){return String(d.category||'')===cat}).sort(function(a,b){return String(b.uploadedAt||'').localeCompare(String(a.uploadedAt||''))});
      list.innerHTML='';if(!items.length){var empty=document.createElement('div');empty.className='crm-company-docs-empty';empty.textContent='No documents uploaded yet';list.appendChild(empty);return}
      items.forEach(function(d){var row=document.createElement('div');row.className='crm-company-doc-row';var info=document.createElement('div');info.className='crm-company-doc-info';var nm=document.createElement('div');nm.className='crm-company-doc-name';nm.textContent=String(d.name||'Document');var meta=document.createElement('div');meta.className='crm-company-doc-meta';var when=d.uploadedAt?new Date(d.uploadedAt).toLocaleDateString():'';meta.textContent=[fmt(d.size),d.uploadedBy||'',when].filter(Boolean).join(' • ');info.append(nm,meta);var actions=document.createElement('div');actions.className='crm-company-doc-actions';var open=document.createElement('button');open.className='btn';open.type='button';open.textContent='Open';open.onclick=function(){openDoc(d.id)};actions.appendChild(open);if(manage&&isEditable(d)){var edit=document.createElement('button');edit.className='btn';edit.type='button';edit.textContent='Edit';edit.onclick=function(){editDoc(d.id)};actions.appendChild(edit)}if(manage){var del=document.createElement('button');del.className='btn';del.type='button';del.textContent='Delete';del.onclick=function(){deleteDoc(d.id)};actions.appendChild(del)}row.append(info,actions);list.appendChild(row)})
    })
  }
  async function load(){try{status('Loading company documents…');var id=await orgId();if(!id)throw new Error('Company access is not ready yet. Sign out and back in, then try again.');var s=await ref(id).get(),data=s.exists?(s.data()||{}):{};docs=Array.isArray(data.documents)?data.documents:[];render();status(canManage()?'Upload company documents here. Everyone in your company can open and download them.':'Company documents are available to open and download.')}catch(e){docs=[];render();status(e&&e.message?e.message:'Company documents could not be loaded.',true)}}
  function choose(cat){if(!canManage())return;var i=document.getElementById('crm-company-doc-file-input');if(!i)return;i.setAttribute('data-category',String(cat||'other'));i.value='';i.click()}
  async function upload(input){var file=input&&input.files&&input.files[0],cat=String(input&&input.getAttribute('data-category')||'other'),path='';if(!file)return;try{if(file.size>MAX_BYTES)throw new Error('Documents must be 25 MB or smaller.');if(!(await tokenCanManage()))throw new Error('Only an Owner or Admin can upload company documents.');var id=await orgId();if(!id)throw new Error('Company access is not ready yet.');var c=client();if(!c)throw new Error('Document storage is not available.');var docId='company_doc_'+Date.now()+'_'+Math.random().toString(36).slice(2,9);path='company-documents/'+safe(id)+'/'+safe(cat)+'/'+safe(docId)+'/'+safe(file.name);status('Uploading '+file.name+'…');var up=await c.storage.from('hail-money-files').upload(path,file,{upsert:false,contentType:file.type||'application/octet-stream'});if(up.error)throw up.error;var r=ref(id),snap=await r.get(),data=snap.exists?(snap.data()||{}):{},arr=Array.isArray(data.documents)?data.documents.slice():[],u=window.auth&&window.auth.currentUser;arr.push({id:docId,category:cat,name:file.name,size:Number(file.size||0),contentType:file.type||'application/octet-stream',storagePath:path,uploadedAt:new Date().toISOString(),uploadedBy:(typeof crmGetCurrentUserName==='function'&&crmGetCurrentUserName())||(u&&u.email)||'Admin',uploadedByUid:(u&&u.uid)||''});await r.set({documents:arr,updatedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true});docs=arr;render();status(file.name+' uploaded successfully.')}catch(e){if(path){try{var cc=client();if(cc)await cc.storage.from('hail-money-files').remove([path])}catch(_){}}status(e&&e.message?e.message:'Document upload failed.',true)}finally{if(input)input.value=''}}
  async function signedUrl(d){var c=client();if(!c||!d||!d.storagePath)return'';var r=await c.storage.from('hail-money-files').createSignedUrl(d.storagePath,3600);if(r.error)throw r.error;return r.data&&r.data.signedUrl?r.data.signedUrl:''}
  async function openDoc(id){var d=find(id);if(!d)return;var pop=window.open('about:blank','_blank');try{status('Opening '+(d.name||'document')+'…');var url=await signedUrl(d);if(!url)throw new Error('This document could not be opened.');if(pop)pop.location.href=url;else window.location.href=url;status('Company documents are ready.')}catch(e){if(pop)pop.close();status(e&&e.message?e.message:'This document could not be opened.',true)}}
  function copyTemplateFields(fields){return (Array.isArray(fields)?fields:[]).map(function(f){return {key:f.key,label:f.label,pageIndex:f.pageIndex!=null?f.pageIndex:0,xPct:f.xPct,yPct:f.yPct,wPct:f.wPct,hPct:f.hPct,x:f.x,y:f.y,width:f.width,height:f.height,confirmed:!!f.confirmed}})}
  function resetCompanyDocEdit(){window.hmCompanyDocumentTemplateEdit=null;var save=document.getElementById('tpl-save-btn');if(save)save.textContent='Save Template'}
  async function editDoc(id){
    var d=find(id);if(!d||!canManage())return;
    try{
      if(!(await tokenCanManage()))throw new Error('Only an Owner or Admin can edit company documents.');
      if(!isEditable(d))throw new Error('Only PDF or image documents can use the field editor.');
      if(typeof renderTplPreview!=='function'||typeof renderTemplateFieldPalette!=='function'||typeof showPage!=='function')throw new Error('The document editor is not available yet.');
      status('Opening '+(d.name||'document')+' in the field editor…');
      var url=await signedUrl(d);if(!url)throw new Error('This document could not be opened.');
      var response=await fetch(url);if(!response.ok)throw new Error('The document could not be downloaded for editing.');
      var blob=await response.blob(),dataUrl=await blobToDataUrl(blob),oid=await orgId();
      window.hmCompanyDocumentTemplateEdit={docId:String(d.id||''),organizationId:String(oid||'')};
      currentTplFields=copyTemplateFields(d.templateFields);currentTplFileDataUrl=dataUrl;currentTplFileName=d.name;currentTplFileType=d.contentType||blob.type||'application/pdf';tplRenderedPages=[];tplSelectedFieldKey=null;tplSelectedIdx=-1;
      var name=document.getElementById('tpl-doc-name');if(name)name.value=String(d.templateName||d.name||'Document').replace(/\.[^.]+$/,'');
      var label=document.getElementById('tpl-file-label');if(label)label.textContent=String(d.name||'Document')+' (Company Document)';
      var save=document.getElementById('tpl-save-btn');if(save)save.textContent='Save Document Fields';
      renderTemplateFieldPalette();renderTplPreview();showPage('page-template-builder');
    }catch(e){resetCompanyDocEdit();status(e&&e.message?e.message:'Document editor could not be opened.',true)}
  }
  async function saveCompanyDocumentFields(){
    var ctx=window.hmCompanyDocumentTemplateEdit;if(!ctx)return;
    try{
      if(!(await tokenCanManage()))throw new Error('Only an Owner or Admin can save document fields.');
      if(!Array.isArray(currentTplFields)||!currentTplFields.length)throw new Error('Place at least one field on the document before saving.');
      var oid=String(ctx.organizationId||await orgId()||'');if(!oid)throw new Error('Company access is not ready yet.');
      var r=ref(oid),snap=await r.get(),data=snap.exists?(snap.data()||{}):{},arr=Array.isArray(data.documents)?data.documents.slice():[],idx=arr.findIndex(function(x){return String(x&&x.id||'')===String(ctx.docId||'')});
      if(idx<0)throw new Error('This company document could not be found.');
      var area=document.getElementById('tpl-preview-area');
      var fields=currentTplFields.map(function(f){var out={key:f.key,label:f.label,pageIndex:f.pageIndex!=null?f.pageIndex:0,xPct:f.xPct,yPct:f.yPct,wPct:f.wPct,hPct:f.hPct,x:f.x,y:f.y,width:f.width,height:f.height,confirmed:!!f.confirmed};if(out.xPct==null||out.yPct==null){var aw=area?(area.scrollWidth||area.offsetWidth||1):1,ah=area?(area.scrollHeight||area.offsetHeight||1):1;out.xPct=(f.x||0)/aw;out.yPct=(f.y||0)/ah;out.wPct=(f.width||0)/aw;out.hPct=(f.height||0)/ah}return out});
      var name=document.getElementById('tpl-doc-name'),u=window.auth&&window.auth.currentUser;
      arr[idx]=Object.assign({},arr[idx],{templateName:String(name&&name.value||arr[idx].name||'Document').trim(),templateFields:fields,templateUpdatedAt:new Date().toISOString(),templateUpdatedBy:(typeof crmGetCurrentUserName==='function'&&crmGetCurrentUserName())||(u&&u.email)||'Admin',templateUpdatedByUid:(u&&u.uid)||''});
      await r.set({documents:arr,updatedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true});docs=arr;resetCompanyDocEdit();render();showPage('page-company-docs');status('Field placement saved for '+String(arr[idx].name||'document')+'.');
    }catch(e){window.alert(e&&e.message?e.message:'Document fields could not be saved.')}
  }
  function bindEditorBridge(){
    var save=document.getElementById('tpl-save-btn');if(save&&!save.dataset.companyDocEditBound){save.dataset.companyDocEditBound='1';save.addEventListener('click',function(e){if(!window.hmCompanyDocumentTemplateEdit)return;e.preventDefault();e.stopImmediatePropagation();saveCompanyDocumentFields()},true)}
    var back=document.getElementById('back-from-tpl-builder');if(back&&!back.dataset.companyDocEditBound){back.dataset.companyDocEditBound='1';back.addEventListener('click',function(e){if(!window.hmCompanyDocumentTemplateEdit)return;e.preventDefault();e.stopImmediatePropagation();resetCompanyDocEdit();showPage('page-company-docs')},true)}
  }
  async function deleteDoc(id){var d=find(id);if(!d||!window.confirm('Delete "'+String(d.name||'this document')+'" for the entire company?'))return;try{if(!(await tokenCanManage()))throw new Error('Only an Owner or Admin can delete company documents.');var oid=await orgId();if(!oid)throw new Error('Company access is not ready yet.');status('Deleting '+(d.name||'document')+'…');var c=client();if(c&&d.storagePath){var rem=await c.storage.from('hail-money-files').remove([d.storagePath]);if(rem.error)throw rem.error}var r=ref(oid),snap=await r.get(),data=snap.exists?(snap.data()||{}):{},arr=(Array.isArray(data.documents)?data.documents:[]).filter(function(x){return String(x&&x.id||'')!==String(id)});await r.set({documents:arr,updatedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true});docs=arr;render();status('Document deleted.')}catch(e){status(e&&e.message?e.message:'Document could not be deleted.',true)}}
  function bind(){bindEditorBridge();document.querySelectorAll('[data-company-doc-upload]').forEach(function(b){if(b.dataset.companyDocBound)return;b.dataset.companyDocBound='1';b.addEventListener('click',function(){choose(b.getAttribute('data-company-doc-upload'))})});var i=document.getElementById('crm-company-doc-file-input');if(i&&!i.dataset.companyDocBound){i.dataset.companyDocBound='1';i.addEventListener('change',function(){upload(i)})}render();var page=document.getElementById('page-company-docs');if(page){var obs=new MutationObserver(function(){if(page.classList.contains('active'))load()});obs.observe(page,{attributes:true,attributeFilter:['class']});if(page.classList.contains('active'))load()}}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',bind);else bind();
})();
</script>'''

if 'id="hm-company-documents-script"' in text:
    start = text.find('<script id="hm-company-documents-script">')
    end = text.find('</script>', start)
    if start < 0 or end < 0:
        raise SystemExit('Existing Company Documents script block could not be located')
    text = text[:start] + script + text[end + len('</script>'):]
else:
    if '</body>' not in text:
        raise SystemExit('Body closing tag not found')
    text = text.replace('</body>', script + '\n</body>', 1)

PUBLIC.write_text(text, encoding='utf-8')
print('Company Documents upload library patched successfully.')
