from pathlib import Path
import re

js_path = Path('public/estimate-ai-flow.js')
css_path = Path('public/estimate-ai-flow.css')
text = js_path.read_text(encoding='utf-8')
css = css_path.read_text(encoding='utf-8')

# Fair-market language for the non-ABC path.
replacements = {
    'Baseline market allowance — confirm or replace with supplier price': 'Fair market value — editable',
    'Baseline labor allowance — confirm company rate': 'Fair market labor value — editable',
    'Baseline allowance — confirm': 'Fair market value — editable',
    'Editable baseline pricing': 'Fair market value pricing',
    'Baseline material and labor allowances': 'Fair market material and labor values',
    'Baseline labor allowances': 'Fair market labor values',
    'editable baseline pricing': 'fair market value pricing',
    'Baseline pricing is being used until a live authorized supplier response succeeds. Confirm or replace allowances before final acceptance.': 'Fair market value pricing is being used. Prices remain editable.',
    'ABC pricing is optional. Starting with editable baseline pricing; connect ABC later if you want live supplier pricing.': 'ABC pricing is optional. Starting with fair market value pricing; connect ABC later only if you want live supplier pricing.',
}
for old, new in replacements.items():
    text = text.replace(old, new)

# Verification should only flag genuinely missing required information, not valid API/fair-market values.
verification_pattern = re.compile(r"  function verificationItems\(\) \{.*?\n  \}\n\n  function renderMeasurements\(\) \{", re.S)
verification_replacement = r'''  function verificationItems() {
    var items = [];
    if (estAiSession.photosNeedManualReview) items.push('Requested photo observations still require manual review.');
    var requiredMeasurementKeys = estimateTrades().indexOf('roof') >= 0 ? ['totalRoofArea', 'squares', 'stories'] : [];
    (estAiSession.measurements || []).forEach(function (item) {
      var missing = item.value === '' || item.value === null || item.value === undefined || (requiredMeasurementKeys.indexOf(item.key) >= 0 && Number(item.value) <= 0);
      if (requiredMeasurementKeys.indexOf(item.key) >= 0 && missing) items.push(item.label + ' is required.');
    });
    (estAiSession.lineItems || []).forEach(function (item) {
      var quantity = Number(item.quantity || 0);
      var priced = Number(item.material || 0) > 0 || Number(item.labor || 0) > 0 || Number(item.equipment || 0) > 0;
      if (quantity <= 0 && !/quantity to confirm/i.test(String(item.description || ''))) items.push(item.description + ': quantity is required.');
      if (quantity > 0 && !priced) items.push(item.description + ': fair market price is missing.');
    });
    if (estAiSession.abcPricing && estAiSession.abcPricing.connected && !estAiSession.abcPricing.pricesRetrieved) {
      items.unshift('ABC is connected, but live supplier pricing has not been retrieved. Fair market pricing is being used for now.');
    }
    return items;
  }

  function renderMeasurements() {'''
text, count = verification_pattern.subn(verification_replacement, text, count=1)
if count != 1:
    raise SystemExit('Could not patch verificationItems')

# Hide optional blank measurement cards, while preserving legitimate zero values such as 0 hips/valleys.
old_render = """    host.innerHTML = (estAiSession.measurements || []).map(function (item) {
      return '<div class=\"est-ai-measurement-card\"><strong>' + esc(item.label) + '</strong><span>' + esc(item.value === '' ? 'Not entered' : item.value) + (item.unit ? ' ' + esc(item.unit) : '') + '</span><small>Source: ' + esc(item.sourceStatus) + '</small></div>';
    }).join('');"""
new_render = """    host.innerHTML = (estAiSession.measurements || []).filter(function (item) {
      return item.value !== '' && item.value !== null && item.value !== undefined;
    }).map(function (item) {
      var source = String(item.sourceStatus || '').replace('Auto-calculated — confirm', 'Auto-calculated').replace('Recommended — confirm', 'Recommended');
      return '<div class=\"est-ai-measurement-card\"><strong>' + esc(item.label) + '</strong><span>' + esc(item.value) + (item.unit ? ' ' + esc(item.unit) : '') + '</span><small>Source: ' + esc(source) + '</small></div>';
    }).join('');"""
if old_render not in text:
    raise SystemExit('Could not patch renderMeasurements')
text = text.replace(old_render, new_render, 1)

# Non-ABC estimate should present fair-market pricing, not an ABC error state.
text = text.replace("'ABC pricing pending connection'", "'Fair market value pricing'")
text = text.replace("'Manual material and labor prices until connected sources are confirmed'", "'Hail Money fair market value pricing'")
text = text.replace("'Baseline material and labor allowances until connected sources are confirmed'", "'Hail Money fair market value pricing'")
text = text.replace('Supplier pricing remains pending until a live authorized ABC request succeeds.', 'Pricing uses fair market values unless live supplier pricing is connected.')
text = text.replace('<dt>Pricing source</dt><dd>Manual until connected</dd>', '<dt>Pricing source</dt><dd>Fair market value</dd>')
text = text.replace('The estimate cannot be finalized while required values or prices remain unconfirmed.', 'Only genuinely missing required values are listed below.')
text = text.replace('No supplier price is fabricated.', 'Fair market values are editable; live supplier pricing can replace them when connected.')

# Clean up assumptions when ABC is intentionally not being used.
text = text.replace(
    "{ text: estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Material prices use the connected ABC account.' : 'Fair market material and labor values are included so the estimate is usable before supplier pricing is connected. Confirm or replace them before final acceptance.', source: 'Pricing status' },\n        { text: 'Fair market labor values are editable and should be replaced with company rates when configured.', source: 'Company configuration' }",
    "{ text: estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Material prices use the connected ABC account.' : 'Fair market value pricing is applied automatically and remains editable.', source: 'Pricing method' }"
)

# Mark the estimate as fair-market mode by default so any existing fair-market engine can participate.
text = text.replace(
    "customer: { name: '', phone: '', email: '' }, taxRate: 0, overheadRate: 0, profitRate: 0,",
    "customer: { name: '', phone: '', email: '' }, fairMarketPricing: true, taxRate: 0, overheadRate: 0, profitRate: 0,",
    1
)

# Mobile containment: keep the page centered and make the wide line-item table scroll inside its own card.
mobile_css = r'''

/* AI estimate mobile containment and fair-market result layout */
#page-est-ai-result,
#page-est-ai-result .placeholder-wrap {
  width: 100%;
  max-width: 100%;
  min-width: 0;
  box-sizing: border-box;
  overflow-x: hidden;
}
#page-est-ai-result .placeholder-wrap {
  margin-left: auto;
  margin-right: auto;
}
#page-est-ai-result .main-menu-card,
#page-est-ai-result .est-ai-result-card,
#page-est-ai-result .est-ai-result-header,
#page-est-ai-result .est-ai-detail-card,
#page-est-ai-result .est-ai-totals {
  max-width: 100%;
  min-width: 0;
  box-sizing: border-box;
}
#page-est-ai-result .est-ai-table-wrap {
  display: block;
  width: 100%;
  max-width: 100%;
  min-width: 0;
  overflow-x: auto;
  overflow-y: hidden;
  -webkit-overflow-scrolling: touch;
  overscroll-behavior-x: contain;
}
#page-est-ai-result .est-ai-line-table {
  width: max-content;
  max-width: none;
}
#page-est-ai-result input,
#page-est-ai-result textarea,
#page-est-ai-result select {
  max-width: 100%;
  box-sizing: border-box;
}
#page-est-ai-result .est-ai-measurement-summary {
  max-width: 100%;
  min-width: 0;
}
@media (max-width: 760px) {
  #page-est-ai-result .placeholder-wrap { padding-left: 8px !important; padding-right: 8px !important; }
  #page-est-ai-result .main-menu-card { width: 100% !important; margin-left: 0 !important; margin-right: 0 !important; }
  #page-est-ai-result .est-ai-line-table { min-width: 980px; }
  #page-est-ai-result .est-ai-detail-card dl div { align-items: flex-start; }
  #page-est-ai-result .est-ai-detail-card dd { max-width: 62%; overflow-wrap: anywhere; }
  #page-est-ai-result .est-ai-totals { display: block; }
  #page-est-ai-result .est-ai-totals-controls,
  #page-est-ai-result .est-ai-totals-breakdown { width: 100% !important; max-width: 100% !important; }
}
'''
if 'AI estimate mobile containment and fair-market result layout' not in css:
    css += mobile_css

js_path.write_text(text, encoding='utf-8')
css_path.write_text(css, encoding='utf-8')
print('AI estimate mobile/fair-market cleanup applied')
