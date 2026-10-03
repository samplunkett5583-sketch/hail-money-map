(function () {
  'use strict';

  var KEY = 'hmm_document_regions_v1';

  function readRows() {
    try {
      var rows = JSON.parse(localStorage.getItem(KEY) || '[]');
      return Array.isArray(rows) ? rows : [];
    } catch (_) { return []; }
  }

  function configuredRows() {
    return readRows().filter(function (row) {
      return row && String(row.name || '').trim() && Array.isArray(row.states) && row.states.length;
    });
  }

  function esc(value) {
    return String(value || '').replace(/[&<>"']/g, function (c) {
      return { '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c];
    });
  }

  function stateName(code) {
    var option = document.querySelector('#fl-state option[value="' + String(code || '').toUpperCase() + '"]');
    return option ? String(option.textContent || code) : String(code || '');
  }

  function resolveRegion(state) {
    state = String(state || '').trim().toUpperCase();
    var row = configuredRows().find(function (item) {
      return (item.states || []).indexOf(state) !== -1;
    });
    return row ? String(row.name || '') : '';
  }

  window.hmResolveRegionForState = resolveRegion;
  window.hmGetConfiguredDocumentRegions = configuredRows;

  function populateLeadRouting(region, state) {
    var regionSelect = document.getElementById('fl-workType');
    var stateSelect = document.getElementById('fl-regionState');
    if (!regionSelect || !stateSelect) return;

    var rows = configuredRows();
    var propertyState = String(state || (document.getElementById('fl-state') || {}).value || '').trim().toUpperCase();
    var inferredRegion = resolveRegion(propertyState);
    var desiredRegion = inferredRegion || String(region || regionSelect.value || '').trim();
    if (rows.length === 1) desiredRegion = rows[0].name;

    regionSelect.innerHTML = rows.length
      ? '<option value="" disabled>Select Region</option>' + rows.map(function (row) {
          return '<option value="' + esc(row.name) + '">' + esc(row.name) + '</option>';
        }).join('')
      : '<option value="" disabled selected>No configured regions</option>';
    regionSelect.disabled = !rows.length;

    if (desiredRegion && rows.some(function (row) { return row.name === desiredRegion; })) regionSelect.value = desiredRegion;
    else regionSelect.value = '';

    var selectedRow = rows.find(function (row) { return row.name === regionSelect.value; }) || null;
    if (!selectedRow) {
      stateSelect.innerHTML = '<option value="" disabled selected>Select Region First</option>';
      stateSelect.disabled = true;
      return;
    }

    var allowedStates = selectedRow.states || [];
    stateSelect.disabled = false;
    stateSelect.innerHTML = '<option value="" disabled>Select State</option>' + allowedStates.map(function (code) {
      return '<option value="' + esc(code) + '">' + esc(stateName(code)) + '</option>';
    }).join('');

    if (propertyState && allowedStates.indexOf(propertyState) !== -1) stateSelect.value = propertyState;
    else if (allowedStates.length === 1) stateSelect.value = allowedStates[0];
    else stateSelect.value = '';
  }

  window.hmPopulateLeadRegionStateRouting = populateLeadRouting;

  function applyRows(rows) {
    if (!Array.isArray(rows)) return;
    localStorage.setItem(KEY, JSON.stringify(rows));
    if (typeof window.hmRenderRegionSettings === 'function') window.hmRenderRegionSettings();
    if (typeof window.hmPopulateTemplateRouting === 'function') {
      var templateRegion = String((document.getElementById('tpl-region') || {}).value || '');
      var templateState = String((document.getElementById('tpl-state') || {}).value || '');
      window.hmPopulateTemplateRouting(templateRegion, templateState);
    }
    populateLeadRouting('', String((document.getElementById('fl-state') || {}).value || ''));
    document.dispatchEvent(new CustomEvent('hm:document-regions-updated'));
  }

  window.hmSaveDocumentRegionsToCloud = async function (rows) {
    applyRows(Array.isArray(rows) ? rows : []);
    return true;
  };

  document.addEventListener('change', function (event) {
    var el = event.target;
    if (!el) return;
    if (el.id === 'fl-workType') populateLeadRouting(el.value, '');
    if (el.id === 'fl-regionState') {
      var propertyState = document.getElementById('fl-state');
      if (propertyState && propertyState.value !== el.value) {
        propertyState.value = el.value;
        propertyState.dispatchEvent(new Event('change', { bubbles: true }));
      }
    }
    if (el.id === 'fl-state') populateLeadRouting(resolveRegion(el.value), el.value);
  });

  document.addEventListener('click', function (event) {
    if (!event.target.closest || !event.target.closest('#settings-regions-save')) return;
    setTimeout(function () {
      populateLeadRouting('', String((document.getElementById('fl-state') || {}).value || ''));
      document.dispatchEvent(new CustomEvent('hm:document-regions-updated'));
    }, 80);
  });

  async function boot() {
    try {
      if (typeof window.hmCloudWhenReady === 'function') await window.hmCloudWhenReady();
    } catch (_) {}
    populateLeadRouting('', String((document.getElementById('fl-state') || {}).value || ''));
    document.dispatchEvent(new CustomEvent('hm:document-regions-updated'));
  }

  window.addEventListener('hailmoneycloudready', boot);
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot, { once: true });
  else boot();
}());
