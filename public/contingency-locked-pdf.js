(function () {
  'use strict';
  function loadImage(src) {
    return new Promise(function (resolve, reject) {
      var img = new Image();
      img.onload = function () { resolve(img); };
      img.onerror = reject;
      img.src = src;
    });
  }
  async function saveFile(leadId, file, note) {
    var metaOptions = { category: 'Job Paperwork', note: note || 'Locked signed contingency agreement' };
    if (typeof window.hmUploadLeadDocumentToCloud === 'function') {
      return window.hmUploadLeadDocumentToCloud(leadId, 'signed_contingency', file, metaOptions);
    }
    return new Promise(function (resolve, reject) {
      if (typeof crmSaveFilesToDb !== 'function') { reject(new Error('Document storage is not available.')); return; }
      crmSaveFilesToDb(leadId, 'signed_contingency', [file], function (count, savedMeta) {
        if (!count || !savedMeta || !savedMeta.length) { reject(new Error('The locked signed PDF could not be uploaded.')); return; }
        resolve(savedMeta[0]);
      }, metaOptions);
    });
  }
  function fieldBox(field, w, h) {
    return {
      x: Number(field.xPct || 0) * w,
      y: Number(field.yPct || 0) * h,
      w: Math.max(12, Number(field.wPct || 0.1) * w),
      h: Math.max(10, Number(field.hPct || 0.03) * h)
    };
  }
  async function addFields(doc, fields, pageIndex, pageW, pageH) {
    var rows = (fields || []).filter(function (field) { return Number(field.pageIndex || 0) === pageIndex; });
    for (var i = 0; i < rows.length; i++) {
      var field = rows[i], box = fieldBox(field, pageW, pageH), value = field.value;
      if (!value && value !== true) continue;
      if (field.key === 'homeowner_signature' || field.key === 'rep_signature') {
        if (typeof value === 'string' && value.indexOf('data:image/') === 0) doc.addImage(value, 'PNG', box.x, box.y, box.w, box.h);
      } else if (field.key === 'checkmark') {
        if (value === true) { doc.setFontSize(Math.max(10, Math.min(22, box.h * 0.8))); doc.text('X', box.x + box.w * 0.25, box.y + box.h * 0.75); }
      } else {
        doc.setFontSize(Math.max(7, Math.min(12, box.h * 0.62)));
        doc.text(String(value), box.x + 1.5, box.y + Math.max(7, box.h * 0.72), { maxWidth: Math.max(8, box.w - 3) });
      }
    }
  }
  async function buildPdf(instance) {
    var api = window.jspdf && window.jspdf.jsPDF;
    if (!api) throw new Error('PDF generator is not available.');
    var src = String(instance && instance.fileDataUrl || '');
    if (!src) throw new Error('The contingency source document is missing.');
    var doc = null;
    var isPdf = String(instance.fileType || '').toLowerCase() === 'application/pdf' || src.indexOf('data:application/pdf') === 0;
    if (isPdf) {
      if (!window.pdfjsLib) throw new Error('PDF rendering is not available.');
      var bytes = await fetch(src).then(function (r) { return r.arrayBuffer(); });
      var pdf = await window.pdfjsLib.getDocument({ data: bytes }).promise;
      for (var p = 1; p <= pdf.numPages; p++) {
        var page = await pdf.getPage(p), viewport = page.getViewport({ scale: 1.5 });
        var canvas = document.createElement('canvas');
        canvas.width = Math.ceil(viewport.width); canvas.height = Math.ceil(viewport.height);
        await page.render({ canvasContext: canvas.getContext('2d'), viewport: viewport }).promise;
        var w = viewport.width, h = viewport.height;
        if (!doc) doc = new api({ unit: 'pt', format: [w, h], orientation: w > h ? 'landscape' : 'portrait', compress: true });
        else doc.addPage([w, h], w > h ? 'landscape' : 'portrait');
        doc.addImage(canvas.toDataURL('image/jpeg', 0.92), 'JPEG', 0, 0, w, h, undefined, 'FAST');
        await addFields(doc, instance.fields, p - 1, w, h);
      }
    } else {
      var img = await loadImage(src), w2 = img.naturalWidth || img.width, h2 = img.naturalHeight || img.height;
      doc = new api({ unit: 'pt', format: [w2, h2], orientation: w2 > h2 ? 'landscape' : 'portrait', compress: true });
      doc.addImage(src, 'PNG', 0, 0, w2, h2, undefined, 'FAST');
      await addFields(doc, instance.fields, 0, w2, h2);
    }
    return doc.output('blob');
  }
  window.hmCreateLockedContingencyPdfDraft = async function (instance, lead, template) {
    var blob = await buildPdf(instance);
    var stamp = new Date().toISOString().replace(/[:.]/g, '-');
    var file = new File([blob], 'signed-contingency-' + stamp + '.pdf', { type: 'application/pdf' });
    return {
      file: file,
      templateId: String(template && template.id || instance.templateId || ''),
      templateVersion: Number(template && template.version || 1),
      region: String(template && template.region || lead && lead.region || ''),
      state: String(template && template.state || lead && lead.state || '').toUpperCase(),
      effectiveDate: String(template && template.effectiveDate || ''),
      signer: String(lead && (lead.homeownerName || ((lead.firstName || '') + ' ' + (lead.lastName || '')).trim()) || ''),
      representative: String(instance.completedBy || (typeof crmGetCurrentUserName === 'function' ? crmGetCurrentUserName() : '') || ''),
      signedAt: String(instance.signedAt || new Date().toISOString())
    };
  };
  window.hmCreateAndUploadLockedContingencyPdf = async function (leadId, instance, lead, template) {
    var result = await window.hmCreateLockedContingencyPdfDraft(instance, lead, template);
    result.meta = await saveFile(leadId, result.file, 'Locked signed contingency agreement');
    return result;
  };

  async function buildContractBundlePdf(instances) {
    var api = window.jspdf && window.jspdf.jsPDF;
    if (!api) throw new Error('PDF generator is not available.');
    if (!Array.isArray(instances) || !instances.length) throw new Error('No signed contract document is available.');
    var doc = null;
    for (var i = 0; i < instances.length; i++) {
      var instance = instances[i] || {};
      var src = String(instance.fileDataUrl || '');
      if (!src) throw new Error('The selected contract is missing its source document.');
      var isPdf = String(instance.fileType || '').toLowerCase() === 'application/pdf' || src.indexOf('data:application/pdf') === 0;
      if (isPdf) {
        if (!window.pdfjsLib) throw new Error('PDF rendering is not available.');
        var bytes = await fetch(src).then(function (r) { return r.arrayBuffer(); });
        var pdf = await window.pdfjsLib.getDocument({ data: bytes }).promise;
        for (var p = 1; p <= pdf.numPages; p++) {
          var page = await pdf.getPage(p), viewport = page.getViewport({ scale: 1.5 });
          var canvas = document.createElement('canvas');
          canvas.width = Math.ceil(viewport.width); canvas.height = Math.ceil(viewport.height);
          await page.render({ canvasContext: canvas.getContext('2d'), viewport: viewport }).promise;
          var w = viewport.width, h = viewport.height;
          if (!doc) doc = new api({ unit:'pt', format:[w,h], orientation:w>h?'landscape':'portrait', compress:true });
          else doc.addPage([w,h], w>h?'landscape':'portrait');
          doc.addImage(canvas.toDataURL('image/jpeg',0.92),'JPEG',0,0,w,h,undefined,'FAST');
          await addFields(doc, instance.fields, p - 1, w, h);
        }
      } else {
        var img = await loadImage(src), w2 = img.naturalWidth || img.width, h2 = img.naturalHeight || img.height;
        if (!doc) doc = new api({ unit:'pt', format:[w2,h2], orientation:w2>h2?'landscape':'portrait', compress:true });
        else doc.addPage([w2,h2], w2>h2?'landscape':'portrait');
        doc.addImage(src, src.indexOf('data:image/jpeg')===0?'JPEG':'PNG',0,0,w2,h2,undefined,'FAST');
        await addFields(doc, instance.fields, 0, w2, h2);
      }
    }
    return doc.output('blob');
  }

  window.hmCreateAndUploadLockedContractPdf = async function (leadId, instances, lead, templates) {
    var blob = await buildContractBundlePdf(instances);
    var stamp = new Date().toISOString().replace(/[:.]/g,'-');
    var contractName = String(instances[0] && instances[0].contractTemplateName || templates && templates[0] && templates[0].name || 'Contract').trim() || 'Contract';
    var safeName = contractName.replace(/[^a-z0-9._-]+/gi,'-').replace(/^-+|-+$/g,'').slice(0,80) || 'Contract';
    var file = new File([blob], 'signed-' + safeName + '-' + stamp + '.pdf', { type:'application/pdf' });
    var metaOptions = { category:'Contract', note:'Locked signed contract — ' + contractName };
    var meta;
    if (typeof window.hmUploadLeadDocumentToCloud === 'function') {
      meta = await window.hmUploadLeadDocumentToCloud(leadId, 'signed_contract', file, metaOptions);
    } else {
      throw new Error('Hail Money cloud document storage is not available.');
    }
    return {
      file:file,
      meta:meta,
      signedAt:new Date().toISOString(),
      contractName:contractName,
      templateIds:(templates || []).map(function(t){return String(t && t.id || '');}),
      region:String(lead && lead.region || ''),
      state:String(lead && (lead.regionState || lead.state) || '').toUpperCase()
    };
  };
})();
