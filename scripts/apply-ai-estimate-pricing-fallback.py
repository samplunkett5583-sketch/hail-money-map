from pathlib import Path
import re

path = Path('public/estimate-ai-flow.js')
text = path.read_text(encoding='utf-8')

line_block = re.compile(r"  function line\(code, description, quantity, unit, waste, source, section\) \{.*?\n  \}\n\n  function buildLineItems\(\) \{", re.S)
replacement = r'''  var ESTIMATE_BASELINE_PRICES = {
    'RFG-TEAR': { material: 0, labor: 95, equipment: 20 },
    'RFG-DISP': { material: 0, labor: 20, equipment: 45 },
    'RFG-UND': { material: 18, labor: 12, equipment: 0 },
    'RFG-IWS': { material: 1.35, labor: 0.75, equipment: 0 },
    'RFG-START': { material: 1.45, labor: 0.70, equipment: 0 },
    'RFG-FIELD': { material: 145, labor: 105, equipment: 0 },
    'RFG-RCAP': { material: 2.75, labor: 1.75, equipment: 0 },
    'RFG-DRIP': { material: 1.90, labor: 1.35, equipment: 0 },
    'RFG-STFL': { material: 3.25, labor: 2.75, equipment: 0 },
    'RFG-HWFL': { material: 4.50, labor: 3.25, equipment: 0 },
    'RFG-RVNT': { material: 6.50, labor: 4.25, equipment: 0 },
    'RFG-VENT': { material: 65, labor: 80, equipment: 0 },
    'RFG-BOOT': { material: 35, labor: 65, equipment: 0 },
    'RFG-DELV': { material: 0, labor: 0, equipment: 275 },
    'RFG-PRMT': { material: 0, labor: 0, equipment: 250 },
    'RFG-CLNP': { material: 0, labor: 225, equipment: 0 }
  };

  function baselinePrice(code) {
    var price = ESTIMATE_BASELINE_PRICES[code] || { material: 0, labor: 0, equipment: 0 };
    return { material: Number(price.material || 0), labor: Number(price.labor || 0), equipment: Number(price.equipment || 0) };
  }

  function line(code, description, quantity, unit, waste, source, section) {
    var baseline = baselinePrice(code);
    var hasBaseline = baseline.material > 0 || baseline.labor > 0 || baseline.equipment > 0;
    return {
      id: 'line_' + Math.random().toString(36).slice(2), code: code, description: description,
      section: section || '', quantity: Number(quantity || 0), unit: unit, material: baseline.material, labor: baseline.labor, equipment: baseline.equipment,
      waste: Number(waste || 0), taxable: true,
      quantitySource: source || (Number(quantity || 0) > 0 ? 'Confirmed measurement review' : 'Measurement not entered'),
      materialSource: baseline.material > 0 ? 'Baseline market allowance — confirm or replace with supplier price' : 'No material allowance',
      laborSource: baseline.labor > 0 ? 'Baseline labor allowance — confirm company rate' : 'No labor allowance',
      equipmentSource: baseline.equipment > 0 ? 'Baseline allowance — confirm' : 'No equipment allowance',
      confidence: quantity > 0 ? (hasBaseline ? 'Auto-calculated — confirm' : 'User-confirmed') : 'Needs confirmation'
    };
  }

  function buildLineItems() {'''
text, count = line_block.subn(replacement, text, count=1)
if count != 1:
    raise SystemExit('Could not patch line() pricing block')

text = text.replace(
    "{ text: estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Material prices use the connected ABC account.' : 'ABC pricing pending connection. Material prices must be entered manually.', source: 'Pricing status' },\n        { text: 'Labor rates are not configured for this company and must be entered or confirmed manually.', source: 'Company configuration' }",
    "{ text: estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Material prices use the connected ABC account.' : 'Baseline material and labor allowances are included so the estimate is usable before supplier pricing is connected. Confirm or replace them before final acceptance.', source: 'Pricing status' },\n        { text: 'Baseline labor allowances are editable and should be replaced with company rates when configured.', source: 'Company configuration' }"
)
text = text.replace(
    "setStage('building', 'complete', 'Editable estimate built from confirmed measurements. Pricing remains manual until ABC is connected.', 'Estimate ready');",
    "setStage('building', 'complete', 'Editable estimate built from confirmed measurements with editable baseline pricing.', 'Estimate ready');"
)
text = text.replace(
    "if (!estAiSession.abcPricing || !estAiSession.abcPricing.pricesRetrieved) items.unshift('ABC pricing pending connection; all supplier prices are manual until a live authorized response succeeds.');",
    "if (!estAiSession.abcPricing || !estAiSession.abcPricing.pricesRetrieved) items.unshift('Baseline pricing is being used until a live authorized supplier response succeeds. Confirm or replace allowances before final acceptance.');"
)
text = text.replace(
    "estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'ABC pricing pending connection',\n      'Manual material and labor prices until connected sources are confirmed'",
    "estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'Editable baseline pricing',\n      'Baseline material and labor allowances until connected sources are confirmed'"
)
text = text.replace(
    "esc(estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'ABC pricing pending connection')",
    "esc(estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'Editable baseline pricing')"
)

old_pricing = "var pricingRows = window.HailMoneyPricing && estAiSession.fairMarketPricing ? window.HailMoneyPricing.sync(estAiSession) : [];"
new_pricing = """var pricingRows = window.HailMoneyPricing && estAiSession.fairMarketPricing ? window.HailMoneyPricing.sync(estAiSession) : [];
    if (!pricingRows.length) {
      var fallbackTaxRate = Number(estAiSession.taxRate || 0) / 100;
      var fallbackOverheadRate = Number(estAiSession.overheadRate || 0) / 100;
      var fallbackProfitRate = Number(estAiSession.profitRate || 0) / 100;
      pricingRows = (estAiSession.lineItems || []).map(function (item) {
        var q = Number(item.quantity || 0);
        var wasteFactor = 1 + Number(item.waste || 0) / 100;
        var materialUnit = Number(item.material || 0) * wasteFactor;
        var laborUnit = Number(item.labor || 0) * wasteFactor;
        var equipmentUnit = Number(item.equipment || 0) * wasteFactor;
        var directUnit = materialUnit + laborUnit + equipmentUnit;
        var taxUnit = item.taxable ? materialUnit * fallbackTaxRate : 0;
        var overheadUnit = directUnit * fallbackOverheadRate;
        var profitUnit = (directUnit + overheadUnit) * fallbackProfitRate;
        var finalUnit = directUnit + taxUnit + overheadUnit + profitUnit;
        return {
          id: item.id, name: item.description, quantity: q, unit: item.unit,
          model: { priceStatus: 'baseline' },
          result: {
            materialSubtotal: Number(item.material || 0),
            wasteCost: Number(item.material || 0) * Number(item.waste || 0) / 100,
            laborSubtotal: laborUnit,
            equipmentSubtotal: equipmentUnit,
            tax: taxUnit,
            overheadProfit: overheadUnit + profitUnit,
            priceAdjustment: 0,
            finalUnitPrice: finalUnit,
            finalExtendedPrice: finalUnit * q
          }
        };
      });
    }"""
if old_pricing not in text:
    raise SystemExit('Could not patch PDF pricing rows')
text = text.replace(old_pricing, new_pricing, 1)

text = text.replace(
    "financial.labor += Number(r.laborSubtotal || 0) * q;\n      financial.tax += Number(r.tax || 0) * q;",
    "financial.labor += Number(r.laborSubtotal || 0) * q;\n      financial.equipment += Number(r.equipmentSubtotal || 0) * q;\n      financial.tax += Number(r.tax || 0) * q;"
)

old_abc_gate = """    var abcOptionalToggle = document.getElementById('est-abc-enabled');
    var abcPricingRequested = !!(abcOptionalToggle && abcOptionalToggle.checked);
    if (abcPricingRequested) {
      if (typeof window.abcChooseEstimatePricing !== 'function') {
        document.getElementById('est-ai-address-status').textContent = 'ABC pricing is temporarily unavailable. Turn ABC pricing off to continue.';
        return;
      }
      if (!await window.abcChooseEstimatePricing()) return;
    }
    var now = new Date();
    var abcState = window.abcEstimateRequestState || {};
    var abcConnected = abcPricingRequested && abcState.connected === true && !!abcState.selection;"""
new_abc_gate = """    var abcOptionalToggle = document.getElementById('est-abc-enabled');
    var abcPricingRequested = !!(abcOptionalToggle && abcOptionalToggle.checked);
    var abcState = window.abcEstimateRequestState || {};
    var abcConnected = abcPricingRequested && abcState.connected === true && !!abcState.selection;
    if (abcPricingRequested && !abcConnected) {
      var abcStatus = document.getElementById('est-ai-address-status');
      if (abcStatus) abcStatus.textContent = 'ABC pricing is optional. Starting with editable baseline pricing; connect ABC later if you want live supplier pricing.';
    }
    var now = new Date();"""
if old_abc_gate not in text:
    raise SystemExit('Could not patch ABC start gate')
text = text.replace(old_abc_gate, new_abc_gate, 1)

path.write_text(text, encoding='utf-8')
print('AI estimate baseline pricing and optional ABC patch applied')
