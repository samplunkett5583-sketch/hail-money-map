from pathlib import Path
import re

flow_path = Path('public/estimate-ai-flow.js')
flow = flow_path.read_text(encoding='utf-8')

# Every standalone AI estimate must establish CRM lead/job context first.
old_gate = "if (window.HailMoneyCrmEstimate && !currentEstimate.crmContext && !currentEstimate.quickContext) { window.HailMoneyCrmEstimate.start(); return; }"
new_gate = "if (window.HailMoneyCrmEstimate && !currentEstimate.crmContext) { window.HailMoneyCrmEstimate.start(); return; }"
if old_gate not in flow:
    raise SystemExit('Could not find AI estimate CRM start gate')
flow = flow.replace(old_gate, new_gate, 1)

# Normalize manually typed addresses and control the start button from valid field syntax.
# The geocoder remains the authoritative property check once processing starts.
marker = "  var estimateStarting = false;\n  async function startEstimate() {"
if marker not in flow:
    raise SystemExit('Could not find AI estimate start marker')
helper = r'''  var HM_US_STATE_CODES = {
    'alabama':'AL','alaska':'AK','arizona':'AZ','arkansas':'AR','california':'CA','colorado':'CO','connecticut':'CT','delaware':'DE','florida':'FL','georgia':'GA','hawaii':'HI','idaho':'ID','illinois':'IL','indiana':'IN','iowa':'IA','kansas':'KS','kentucky':'KY','louisiana':'LA','maine':'ME','maryland':'MD','massachusetts':'MA','michigan':'MI','minnesota':'MN','mississippi':'MS','missouri':'MO','montana':'MT','nebraska':'NE','nevada':'NV','new hampshire':'NH','new jersey':'NJ','new mexico':'NM','new york':'NY','north carolina':'NC','north dakota':'ND','ohio':'OH','oklahoma':'OK','oregon':'OR','pennsylvania':'PA','rhode island':'RI','south carolina':'SC','south dakota':'SD','tennessee':'TN','texas':'TX','utah':'UT','vermont':'VT','virginia':'VA','washington':'WA','west virginia':'WV','wisconsin':'WI','wyoming':'WY','district of columbia':'DC'
  };

  function hmNormalizedState(value) {
    var raw = String(value || '').trim();
    if (/^[A-Za-z]{2}$/.test(raw)) return raw.toUpperCase();
    return HM_US_STATE_CODES[raw.toLowerCase()] || raw;
  }

  function hmNormalizedZip(value) {
    var digits = String(value || '').replace(/\D/g, '').slice(0, 9);
    if (digits.length === 9) return digits.slice(0, 5) + '-' + digits.slice(5);
    return digits;
  }

  function hmNormalizeAiAddressInputs() {
    var state = document.getElementById('est-ai-state');
    var zip = document.getElementById('est-ai-zip');
    if (state) state.value = hmNormalizedState(state.value);
    if (zip) zip.value = hmNormalizedZip(zip.value);
  }

  function hmAiAddressLooksReady() {
    function value(id) { var el = document.getElementById(id); return String(el && el.value || '').trim(); }
    var street = value('est-ai-street');
    var city = value('est-ai-city');
    var state = hmNormalizedState(value('est-ai-state'));
    var zip = hmNormalizedZip(value('est-ai-zip'));
    return street.length >= 3 && city.length >= 2 && /^[A-Z]{2}$/.test(state) && /^\d{5}(?:-\d{4})?$/.test(zip);
  }

  function hmRefreshAiStartButton() {
    var start = document.getElementById('est-ai-generate');
    if (!start) return;
    var ready = hmAiAddressLooksReady();
    start.disabled = !ready;
    start.setAttribute('aria-disabled', ready ? 'false' : 'true');
    if (ready) {
      var status = document.getElementById('est-ai-address-status');
      if (status && /enter a valid street|valid street.*city.*state|5-digit|zip\+4/i.test(status.textContent || '')) status.textContent = '';
    }
  }

  function hmHideStandaloneSavedAiEstimates() {
    document.querySelectorAll('h1,h2,h3,h4,h5,h6,.est-section-label,strong').forEach(function (node) {
      if (!/^saved ai estimates$/i.test(String(node.textContent || '').trim())) return;
      var card = node.closest('section,.main-menu-card,.estimates-gold-card,.card');
      if (card) card.style.display = 'none';
      else node.style.display = 'none';
    });
  }

  function hmBindAiStartCleanup() {
    ['est-ai-street','est-ai-city','est-ai-state','est-ai-zip'].forEach(function (id) {
      var input = document.getElementById(id);
      if (!input || input.dataset.hmCrmFlowBound === '1') return;
      input.dataset.hmCrmFlowBound = '1';
      input.addEventListener('input', function () { setTimeout(hmRefreshAiStartButton, 0); });
      input.addEventListener('change', function () { hmNormalizeAiAddressInputs(); hmRefreshAiStartButton(); });
      input.addEventListener('blur', function () { hmNormalizeAiAddressInputs(); hmRefreshAiStartButton(); });
    });
    hmRefreshAiStartButton();
    hmHideStandaloneSavedAiEstimates();
    var root = document.getElementById('page-estimates') || document.body;
    if (root && !root._hmSavedEstimateObserver) {
      root._hmSavedEstimateObserver = new MutationObserver(function () { hmHideStandaloneSavedAiEstimates(); });
      root._hmSavedEstimateObserver.observe(root, { childList: true, subtree: true });
    }
  }

  var estimateStarting = false;
  async function startEstimate() {'''
flow = flow.replace(marker, helper, 1)

# Normalize full state names / ZIP+4 immediately before the existing validator runs.
start_line = "    try {\n    if (window.HailMoneyCrmEstimate && !currentEstimate.crmContext) { window.HailMoneyCrmEstimate.start(); return; }\n    if (!estAiValidateAddress(true)) return;"
replacement = "    try {\n    hmNormalizeAiAddressInputs();\n    if (window.HailMoneyCrmEstimate && !currentEstimate.crmContext) { window.HailMoneyCrmEstimate.start(); return; }\n    if (!estAiValidateAddress(true)) return;"
if start_line not in flow:
    raise SystemExit('Could not add address normalization before validation')
flow = flow.replace(start_line, replacement, 1)

# Bind the start-page cleanup after the existing estimate UI is initialized.
init_bind = "    bindEvents();\n    estAiRunProcessing = runProcessing;"
if init_bind not in flow:
    raise SystemExit('Could not find AI estimate init binding')
flow = flow.replace(init_bind, "    bindEvents();\n    hmBindAiStartCleanup();\n    estAiRunProcessing = runProcessing;", 1)
flow_path.write_text(flow, encoding='utf-8')

# The CRM job record is the canonical estimate home. Stop duplicating estimates into
# the standalone localStorage archive that renders the obsolete Saved AI Estimates list.
crm_path = Path('public/assets/crm-cross-links.js')
crm = crm_path.read_text(encoding='utf-8')
archive_block = re.compile(
    r"\n    var saved = \[\];\n    try \{ saved = JSON\.parse\(localStorage\.getItem\('hailMoneyAiEstimatesV1'\) \|\| '\[\]'\); \} catch \(_\) \{\}\n    if \(!Array\.isArray\(saved\)\) saved = \[\];\n    var savedIdx = saved\.findIndex\(function \(item\) \{ return clean\(item\.id\) === clean\(copy\.id\); \}\);\n    if \(savedIdx >= 0\) saved\[savedIdx\] = copy; else saved\.unshift\(copy\);\n    localStorage\.setItem\('hailMoneyAiEstimatesV1', JSON\.stringify\(saved\)\);",
    re.M
)
crm, count = archive_block.subn('', crm, count=1)
if count != 1:
    raise SystemExit('Could not remove standalone AI estimate archive write')

# Keep the job-detail summary amount synchronized with the computed grand total.
crm = crm.replace(
    "amount: clean(copy.total || copy.grandTotal), scope:",
    "amount: clean(copy.total || copy.grandTotal || (copy.totals && copy.totals.grand)), scope:",
    1
)
crm_path.write_text(crm, encoding='utf-8')

print('AI estimate CRM pipeline/start-screen cleanup applied')
