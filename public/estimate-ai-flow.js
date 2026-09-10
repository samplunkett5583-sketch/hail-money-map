(function () {
  'use strict';

  var DRAFT_KEY = 'hailMoneyAiEstimateDraftV2';
  var SOURCE_STATUSES = ['Verified source', 'API-derived', 'Auto-calculated — confirm', 'Recommended — confirm', 'AI estimated — confirm', 'AI-derived', 'User-confirmed', 'Needs confirmation'];
  var QUESTION_CATALOG = {
    stories: { text: 'HOW MANY STORIES?', type: 'stories', min: 1, max: 4, defaultValue: 1 },
    layers: { text: 'HOW MANY EXISTING ROOFING LAYERS?', type: 'count', min: 1, max: 3, options: ['Unknown'] },
    pitch: { text: 'WHAT ROOF PITCH SHOULD BE USED?', type: 'single', options: ['3/12 or lower', '4/12', '5/12', '6/12', '7/12', '8/12', '9/12', '10/12', '11/12', '12/12 or steeper', 'Unknown'] },
    shingle_type: { text: 'WHAT ROOFING MATERIAL SHOULD THIS ESTIMATE USE?', type: 'single', options: ['Architectural asphalt shingles', '3-tab asphalt shingles', 'Metal roofing', 'Tile or slate', 'Other', 'Unknown'] },
    tear_off: { text: 'IS COMPLETE TEAR-OFF REQUIRED?', type: 'single', options: ['Yes', 'No', 'Needs confirmation'] },
    decking: { text: 'WHAT IS KNOWN ABOUT THE ROOF DECKING?', type: 'single', options: ['Sound — no known repairs', 'Known repairs are required', 'Needs inspection / unknown'] },
    ice_water: { text: 'WHAT ICE-AND-WATER BARRIER REQUIREMENT APPLIES?', type: 'single', options: ['Required at eaves and valleys', 'Required at eaves only', 'Not required', 'Local requirement needs confirmation'] },
    ventilation: { text: 'WHAT VENTILATION WORK IS REQUIRED?', type: 'single', options: ['Install ridge vent', 'Replace box vents', 'Keep existing ventilation', 'Needs inspection / unknown'] },
    roof_features: { text: 'WHICH ROOF FEATURES MUST BE INCLUDED?', type: 'multi', options: ['None visible', 'Chimney', 'Skylight(s)', 'Wall flashing', 'Valleys', 'Other roof penetrations', 'Unsure'], exclusive: ['None visible', 'Unsure'] },
    detached_sections: { text: 'ARE THERE DETACHED ROOF SECTIONS OR ADDITIONS?', type: 'single', options: ['No', 'Yes — measurements include them', 'Yes — measurements still needed', 'Not sure'] },
    unseen_features: { text: 'IS THERE ANOTHER ROOF FEATURE OR SECTION THAT CANNOT BE SEEN CLEARLY?', type: 'single', options: ['No', 'Yes', 'Unsure'] },
    unseen_feature_details: { text: 'WHICH FEATURE OR AREA CANNOT BE SEEN CLEARLY?', type: 'single', options: ['Another chimney', 'Skylight or roof opening', 'Addition', 'Detached roof section', 'Other or unsure'] },
    local_code: { text: 'HAVE LOCAL ROOFING CODE AND PERMIT REQUIREMENTS BEEN CONFIRMED?', type: 'single', options: ['Confirmed', 'Not yet confirmed'] }
  };
  var PHOTO_CATALOG = {
    front_overview: 'Front overview',
    rear_elevation: 'Rear elevation',
    left_elevation: 'Left elevation',
    right_elevation: 'Right elevation',
    tree_blocked_roof: 'Roof area blocked by trees',
    addition_or_detached: 'Closest safe ground-level photo showing the addition or detached roof — do not climb onto the roof',
    roof_component_closeup: 'Closest safe ground-level photo of the unverified roof feature — do not climb onto the roof'
  };
  var REVIEW_FIELDS = [
    { key: 'totalRoofArea', label: 'Total roof area', unit: 'sq ft', step: '1', required: true },
    { key: 'squares', label: 'Squares', unit: 'SQ', step: '.01', required: true },
    { key: 'eaves', label: 'Eaves', unit: 'LF', step: '.1' },
    { key: 'rakes', label: 'Rakes', unit: 'LF', step: '.1' },
    { key: 'ridges', label: 'Ridges', unit: 'LF', step: '.1' },
    { key: 'hips', label: 'Hips', unit: 'LF', step: '.1' },
    { key: 'valleys', label: 'Valleys', unit: 'LF', step: '.1' },
    { key: 'stepFlashing', label: 'Step flashing', unit: 'LF', step: '.1' },
    { key: 'headwallFlashing', label: 'Headwall flashing', unit: 'LF', step: '.1' },
    { key: 'wastePercent', label: 'Waste percentage', unit: '%', step: '.1' },
    { key: 'stories', label: 'Number of stories', unit: '', step: '1', min: '1', max: '4', required: true },
    { key: 'layers', label: 'Existing layers', unit: '', step: '1', min: '1', max: '3' }
  ];

  function esc(value) {
    if (typeof crmEscapeHtml === 'function') return crmEscapeHtml(String(value == null ? '' : value));
    return String(value == null ? '' : value).replace(/[&<>"']/g, function (ch) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch];
    });
  }

  function safeError(error, fallback) {
    var message = String(error && error.message || fallback || 'Request failed.');
    return message.replace(/((?:api[-_ ]?key|authorization|bearer|token|secret|password)\s*[:=]?\s*)[^\s,;]+/gi, '$1[redacted]').slice(0, 240);
  }

  function numberFromAnswer(key, fallback) {
    var answer = estAiSession && estAiSession.answers && estAiSession.answers[key];
    var text = String(answer && answer.value || '');
    var wordNumber = { one: 1, two: 2, three: 3 }[text.trim().split(/\s+/)[0].toLowerCase()];
    if (wordNumber) return wordNumber;
    if (answer && Number.isInteger(answer.count)) return answer.count;
    var match = text.match(/\d+(?:\.\d+)?/);
    return match ? Number(match[0]) : (fallback == null ? null : fallback);
  }

  function questionDefinition(question) {
    var catalog = question && QUESTION_CATALOG[question.id];
    return Object.assign({}, question || {}, catalog || {});
  }

  function isQuestionAnswerValid(question, answer) {
    if (!answer) return false;
    var definition = questionDefinition(question);
    if (definition.type === 'stories') return Number.isInteger(answer.count) && answer.count >= 1 && answer.count <= 4 && typeof answer.splitLevel === 'boolean';
    if (definition.type === 'count') return answer.value === 'Unknown' || (Number.isInteger(answer.count) && answer.count >= definition.min && answer.count <= definition.max);
    if (definition.type === 'multi') return Array.isArray(answer.values) && answer.values.length > 0 && answer.values.every(function (value) { return definition.options.indexOf(value) >= 0; });
    if (definition.type === 'single') return (definition.options || []).indexOf(answer.value) >= 0;
    return String(answer.value || '').trim().length > 0;
  }

  function saveQuestionAnswer(question, value, details) {
    if (!estAiSession || !question) return;
    estAiSession.answers = estAiSession.answers || {};
    estAiSession.answers[question.id] = Object.assign({
      value: value,
      source: 'User-confirmed',
      answeredAt: new Date().toISOString()
    }, details || {});
    persistDraft();
    var button = document.getElementById('est-ai-question-continue');
    if (button) button.disabled = !isQuestionAnswerValid(question, estAiSession.answers[question.id]);
  }

  function renderQuestionChoice(label, savedValue) {
    var selected = savedValue === label;
    return '<button type="button" class="hm-question-choice' + (selected ? ' is-selected' : '') + '" data-hm-question-choice="' + esc(label) + '" aria-pressed="' + selected + '">' + esc(label) + '</button>';
  }

  function renderQuestionControls(question, saved) {
    var definition = questionDefinition(question);
    if (definition.type === 'stories') {
      var storyCount = saved && Number(saved.count) || Number(definition.defaultValue || 1);
      var splitLevel = saved && typeof saved.splitLevel === 'boolean' ? saved.splitLevel : false;
      return '<div class="hm-question-stepper" role="group" aria-label="Number of stories">' +
        '<button type="button" data-hm-question-step="-1" aria-label="Decrease stories"' + (storyCount <= 1 ? ' disabled' : '') + '>−</button>' +
        '<output class="hm-question-count" data-hm-question-count aria-live="polite" aria-label="Selected stories">' + storyCount + '</output>' +
        '<button type="button" data-hm-question-step="1" aria-label="Increase stories"' + (storyCount >= 4 ? ' disabled' : '') + '>+</button>' +
        '</div><fieldset class="hm-question-fieldset"><legend>IS THIS HOME SPLIT-LEVEL?</legend><div class="hm-question-binary">' +
        renderQuestionChoice('Yes', splitLevel ? 'Yes' : 'No') + renderQuestionChoice('No', splitLevel ? 'Yes' : 'No') +
        '</div></fieldset>';
    }
    if (definition.type === 'count') {
      var numericValue = saved && Number(saved.count);
      var displayValue = numericValue || Number(definition.min || 1);
      var unknownSelected = saved && saved.value === 'Unknown';
      return '<div class="hm-question-stepper" role="group" aria-label="Existing roofing layers">' +
        '<button type="button" data-hm-question-step="-1" aria-label="Decrease layers"' + (displayValue <= definition.min ? ' disabled' : '') + '>−</button>' +
        '<button type="button" class="hm-question-count hm-question-count-button' + (numericValue ? ' is-selected' : '') + '" data-hm-question-count-confirm aria-label="Use ' + displayValue + ' existing roofing ' + (displayValue === 1 ? 'layer' : 'layers') + '">' + displayValue + '</button>' +
        '<button type="button" data-hm-question-step="1" aria-label="Increase layers"' + (displayValue >= definition.max ? ' disabled' : '') + '>+</button>' +
        '</div><div class="hm-question-single">' + renderQuestionChoice('Unknown', unknownSelected ? 'Unknown' : '') + '</div>';
    }
    if (definition.type === 'multi') {
      var selectedValues = saved && Array.isArray(saved.values) ? saved.values : [];
      return '<div class="hm-question-check-grid">' + definition.options.map(function (label) {
        var checked = selectedValues.indexOf(label) >= 0;
        return '<label class="hm-question-check' + (checked ? ' is-selected' : '') + '"><input type="checkbox" data-hm-question-multi="' + esc(label) + '"' + (checked ? ' checked' : '') + ' /><span>' + esc(label) + '</span></label>';
      }).join('') + '</div>';
    }
    if (definition.type === 'text') return '';
    return '<div class="hm-question-single">' + (definition.options || []).map(function (label) { return renderQuestionChoice(label, saved && saved.value); }).join('') + '</div>';
  }

  function persistDraft() {
    if (!estAiSession) return;
    try {
      var copy = JSON.parse(JSON.stringify(estAiSession));
      delete copy.photoData;
      localStorage.setItem(DRAFT_KEY, JSON.stringify(copy));
      if (window.HailMoneyCrmEstimate) window.HailMoneyCrmEstimate.persist(copy);
    } catch (_) {}
  }

  async function currentFirebaseUser() {
    if (!window.auth) throw new Error('Firebase Authentication is unavailable.');
    if (window.auth.currentUser) return window.auth.currentUser;
    var credential = await window.auth.signInAnonymously();
    return credential && credential.user ? credential.user : window.auth.currentUser;
  }

  function functionUrl(name) {
    var local = location.hostname === '127.0.0.1' || location.hostname === 'localhost';
    if (local && window.HAIL_MONEY_USE_FUNCTIONS_EMULATOR === true) return 'http://127.0.0.1:5015/hailmoneymap/us-central1/' + name;
    return 'https://us-central1-hailmoneymap.cloudfunctions.net/' + name;
  }

  async function callEstimator(phase, sources, photos) {
    var user = await currentFirebaseUser();
    var token = await user.getIdToken(false);
    var response = await fetch(functionUrl('analyzeRoofEstimate'), {
      method: 'POST',
      signal: AbortSignal.timeout(90000),
      headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify({ phase: phase, session: estAiSafeSessionPayload(), sources: sources || estAiSession.sources || {}, photos: photos || [] })
    });
    var data = await response.json().catch(function () { return {}; });
    if (!response.ok) {
      var error = new Error(data && data.error || ('AI estimator HTTP ' + response.status));
      error.status = response.status;
      throw error;
    }
    if (!data || !data.analysis) throw new Error('AI estimator returned no analysis.');
    estAiSession.aiUsage = data.usage || null;
    return data.analysis;
  }

  function geocodeAddress(address) {
    return new Promise(function (resolve) {
      if (!window.google || !google.maps || !google.maps.Geocoder) return resolve({ available: false, message: 'Address service failed. Retry.' });
      new google.maps.Geocoder().geocode({ address: address.formatted }, function (results, status) {
        if (status !== 'OK' || !results || !results[0]) return resolve({ available: false, message: status === 'ZERO_RESULTS' ? 'Address could not be matched. Correct the address.' : 'Address service failed. Retry.' });
        var locationValue = results[0].geometry && results[0].geometry.location;
        resolve({
          available: true,
          source: 'Google Maps geocoder',
          formattedAddress: results[0].formatted_address || address.formatted,
          latitude: locationValue && locationValue.lat ? Number(locationValue.lat()) : null,
          longitude: locationValue && locationValue.lng ? Number(locationValue.lng()) : null
        });
      });
    });
  }

  async function propertyLookup(address) {
    if (typeof sb === 'undefined' || !sb || !sb.functions) return { available: false, source: 'RentCast via Supabase', message: 'Property-data service failed. Retry.' };
    try {
      var result = await sb.functions.invoke('zillow-property', { body: { property_address: address.formatted }, timeout: 15000 });
      if (result.error) {
        var context = result.error.context;
        var body = context && typeof context.clone === 'function' ? await context.clone().json().catch(function () { return {}; }) : {};
        var providerError = new Error(body.error || result.error.message || 'Property record not returned');
        providerError.status = context && context.status || 0;
        providerError.code = body.operation || 'PROPERTY_UNAVAILABLE';
        throw providerError;
      }
      if (!result.data || result.data.success !== true) throw new Error('Property record not returned');
      var data = result.data;
      var rawProperty = Array.isArray(data.raw_property) ? data.raw_property[0] : data.raw_property;
      var features = data.features || rawProperty && rawProperty.features || {};
      return {
        available: true,
        source: String(data.source || 'RentCast'),
        propertyType: data.propertyType || null,
        squareFootage: data.squareFootage == null ? null : Number(data.squareFootage),
        lotSize: data.lotSize == null ? null : Number(data.lotSize),
        yearBuilt: data.yearBuilt == null ? null : Number(data.yearBuilt),
        bedrooms: data.bedrooms == null ? null : Number(data.bedrooms),
        bathrooms: data.bathrooms == null ? null : Number(data.bathrooms),
        stories: features.floorCount == null ? null : Number(features.floorCount)
      };
    } catch (error) {
      console.warn('[AI estimator property]', { status: error.status || 0, code: error.code || 'PROPERTY_UNAVAILABLE', message: safeError(error) });
      return { available: false, source: 'RentCast via Supabase', status: error.status || 0, reason: error.code || 'PROPERTY_UNAVAILABLE', message: error.code === 'paid-detail-disabled' ? 'Automatic property lookup is not enabled. We will ask for missing details.' : 'Property information is unavailable. We will ask for missing details.', detail: safeError(error, 'Property lookup failed') };
    }
  }

  function googleMapsKey() {
    var script = document.querySelector('script[src*="maps.googleapis.com/maps/api/js"]');
    if (!script) return '';
    try { return new URL(script.src).searchParams.get('key') || ''; } catch (_) { return ''; }
  }

  async function imageryLookup(geocode) {
    var key = googleMapsKey();
    if (!key || !geocode.available || geocode.latitude == null || geocode.longitude == null) {
      return { available: false, source: 'Google Street View metadata', message: 'Exterior imagery could not be verified' };
    }
    try {
      var url = 'https://maps.googleapis.com/maps/api/streetview/metadata?location=' + encodeURIComponent(geocode.latitude + ',' + geocode.longitude) + '&key=' + encodeURIComponent(key);
      var response = await fetch(url, { signal: AbortSignal.timeout(15000) });
      var data = await response.json().catch(function () { return {}; });
      return data.status === 'OK'
        ? { available: true, source: 'Google Street View metadata', captureDate: data.date || null }
        : { available: false, source: 'Google Street View metadata', status: response.status, reason: data.status || 'HTTP_ERROR', detail: data.error_message || '', message: 'Exterior imagery is unavailable. Photos will be requested only for unresolved details.' };
    } catch (error) {
      return { available: false, source: 'Google Street View metadata', message: safeError(error, 'Imagery lookup failed') };
    }
  }

  async function solarLookup(geocode) {
    if (!geocode.available || geocode.latitude == null || geocode.longitude == null) {
      return { available: false, source: 'Google Solar API', message: 'Aerial roof data is unavailable for this address. Please enter measurements or upload the requested photos.' };
    }
    try {
      var user = await currentFirebaseUser();
      var token = await user.getIdToken(false);
      var response = await fetch(functionUrl('getSolarRoofData'), {
        method: 'POST',
        signal: AbortSignal.timeout(55000),
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' },
        body: JSON.stringify({ latitude: geocode.latitude, longitude: geocode.longitude })
      });
      var data = await response.json().catch(function () { return {}; });
      if (!response.ok) {
        console.warn('[AI estimator Solar proxy]', { status: response.status });
        return {
          available: false,
          source: 'Google Solar API buildingInsights',
          status: response.status,
          reason: response.status === 404 ? 'NOT_FOUND' : 'PROXY_ERROR',
          message: 'Automatic roof measurements are unavailable. Enter or confirm measurements to continue.',
          detail: String(data && data.error || 'Solar proxy request failed')
        };
      }
      return data;
    } catch (error) {
      console.warn('[AI estimator Solar proxy]', { status: 0, code: 'NETWORK_ERROR' });
      return { available: false, source: 'Google Solar API buildingInsights', status: 0, reason: 'NETWORK_ERROR', message: 'Automatic roof measurements are unavailable. Enter or confirm measurements to continue.', detail: safeError(error, 'Solar proxy request failed') };
    }
  }

  function applySolarMeasurements(solar) {
    if (!solar || !solar.available) return;
    var draft = estAiSession.measurementDraft || {};
    var evidence = estAiSession.measurementEvidence || {};
    if (solar.roofAreaSquareFeet > 0 && draft.totalRoofAreaSource !== 'User-confirmed') {
      draft.totalRoofArea = solar.roofAreaSquareFeet;
      draft.totalRoofAreaSource = solar.roofAreaConfidence || (solar.source === 'Google Solar roof mask + elevation' ? 'Auto-calculated — confirm' : 'API-derived');
      draft.squares = Number((solar.roofAreaSquareFeet / 100).toFixed(2));
      draft.squaresSource = 'Auto-calculated — confirm';
      evidence.totalRoofArea = { source: solar.source || 'Google Solar BuildingInsights', confidence: draft.totalRoofAreaSource, method: solar.roofGeometry && solar.roofGeometry.measurements && solar.roofGeometry.measurements.totalRoofArea && solar.roofGeometry.measurements.totalRoofArea.method || 'Provider roof surface area, not living area' };
      evidence.squares = { source: evidence.totalRoofArea.source, confidence: draft.squaresSource, method: 'Total roof area ÷ 100' };
    }
    if (solar.pitchByPlane && draft.pitchPlanesSource !== 'User-confirmed') {
      draft.pitchPlanes = solar.pitchByPlane;
      draft.pitchPlanesSource = 'API-derived';
    }
    var calculated = solar.roofGeometry && solar.roofGeometry.measurements || {};
    ['eaves','rakes','ridges','hips','valleys','stepFlashing','headwallFlashing'].forEach(function (key) {
      var item = calculated[key];
      if (draft[key + 'Source'] === 'User-confirmed' || !item || item.value == null || !Number.isFinite(Number(item.value)) || Number(item.value) < 0) return;
      draft[key] = Number(item.value);
      draft[key + 'Source'] = item.confidence || 'Auto-calculated — confirm';
      evidence[key] = { source: item.source, confidence: draft[key + 'Source'], method: item.method };
    });
    estAiSession.measurementDraft = draft;
    estAiSession.measurementEvidence = evidence;
  }

  function prepareMeasurementReview() {
    var sources = estAiSession.sources || {};
    applySolarMeasurements(sources.solar);
    var draft = estAiSession.measurementDraft || {};
    var evidence = estAiSession.measurementEvidence || {};
    var property = sources.property;
    if (property && property.available && Number.isInteger(property.stories) && property.stories > 0 && property.stories <= 4 && draft.storiesSource !== 'User-confirmed') {
      draft.stories = property.stories;
      draft.storiesSource = 'Verified source';
      evidence.stories = { source: property.source, confidence: 'Verified source', method: 'Property record above-ground floor count' };
    }
    // This is an editable estimating allowance, never a measured roof fact.
    if (draft.wastePercent == null || draft.wastePercent === '') {
      var solar = sources.solar;
      var complex = Number(draft.hips || 0) > 0 || Number(draft.valleys || 0) > 0 || solar && solar.roofSegments && solar.roofSegments.length >= 4;
      draft.wastePercent = complex ? 15 : 10;
      draft.wastePercentSource = 'Recommended — confirm';
      evidence.wastePercent = { source: 'Editable estimating allowance', confidence: 'Recommended — confirm', method: complex ? '15% starting allowance for detected roof complexity; confirm for material and layout' : '10% starting allowance; confirm for material and actual roof complexity' };
    }
    estAiSession.measurementDraft = draft;
    estAiSession.measurementEvidence = evidence;
  }

  function renderAerialPreview() {
    var panel = document.getElementById('est-ai-aerial-preview');
    var mapHost = document.getElementById('est-ai-aerial-map');
    var solar = estAiSession && estAiSession.sources && estAiSession.sources.solar;
    if (!panel || !mapHost || !solar || !solar.available || !solar.center || !window.google || !google.maps) {
      if (panel) panel.hidden = true;
      return;
    }
    panel.hidden = false;
    document.getElementById('est-ai-aerial-meta').textContent = 'Source: Google Solar API buildingInsights · Imagery date: ' + (solar.imageryDate || 'not supplied') + ' · Quality: ' + solar.imageryQuality + (solar.dataLayersAvailable ? ' · Solar imagery layers available' : '');
    var pitches = (solar.roofSegments || []).map(function (segment) { return Number(segment.pitchDegrees || 0); }).filter(Number.isFinite);
    var azimuths = (solar.roofSegments || []).map(function (segment) { return Number(segment.azimuthDegrees || 0).toFixed(0) + '°'; });
    var metrics = document.getElementById('est-ai-aerial-metrics');
    if (metrics) {
      metrics.innerHTML = [
        ['Roof area', Number(solar.roofAreaSquareFeet || 0).toLocaleString() + ' sq ft', 'API-derived'],
        ['Building footprint', Number(solar.footprintSquareFeet || 0).toLocaleString() + ' sq ft', 'API-derived'],
        ['Roof segments', String((solar.roofSegments || []).length), 'API-derived'],
        ['Pitch range', pitches.length ? Math.min.apply(null, pitches).toFixed(1) + '°–' + Math.max.apply(null, pitches).toFixed(1) + '°' : 'Not supplied', 'API-derived'],
        ['Segment azimuths', azimuths.length ? azimuths.join(', ') : 'Not supplied', 'API-derived'],
        ['Data layers', solar.dataLayersAvailable ? 'Available' : 'Not available', 'Google Solar API']
      ].map(function (metric) {
        return '<div class="est-ai-aerial-metric"><span>' + esc(metric[0]) + '</span><strong>' + esc(metric[1]) + '</strong><small>' + esc(metric[2]) + '</small></div>';
      }).join('');
    }
    var center = { lat: solar.center.latitude, lng: solar.center.longitude };
    if (!mapHost.dataset.ready) {
      mapHost.dataset.ready = '1';
      mapHost._hmMap = new google.maps.Map(mapHost, { center: center, zoom: 20, mapTypeId: 'satellite', disableDefaultUI: true, gestureHandling: 'cooperative' });
    } else if (mapHost._hmMap) {
      mapHost._hmMap.setCenter(center);
    }
    (mapHost._hmRoofLines || []).forEach(function (line) { line.setMap(null); });
    mapHost._hmRoofLines = [];
    var colors = { eaves:'#22c55e', rakes:'#f97316', ridges:'#2563eb', hips:'#a855f7', valleys:'#ef4444', stepFlashing:'#eab308', headwallFlashing:'#06b6d4', ambiguous:'#ffffff' };
    var labels = { eaves:'Eaves', rakes:'Rakes', ridges:'Ridges', hips:'Hips', valleys:'Valleys', stepFlashing:'Step flashing', headwallFlashing:'Headwall flashing', ambiguous:'Confirm segment' };
    var diagramSegments = solar.roofGeometry && solar.roofGeometry.segments || [];
    diagramSegments.forEach(function (segment) {
      if (!segment.points || segment.points.length < 2 || !colors[segment.kind]) return;
      var line = new google.maps.Polyline({
        map: mapHost._hmMap,
        path: segment.points.map(function (point) { return { lat:Number(point.latitude), lng:Number(point.longitude) }; }),
        strokeColor: colors[segment.kind],
        strokeOpacity: segment.kind === 'ambiguous' ? .9 : 1,
        strokeWeight: segment.kind === 'ambiguous' ? 4 : 3,
        zIndex: segment.kind === 'ambiguous' ? 3 : 2
      });
      mapHost._hmRoofLines.push(line);
    });
    var legend = document.getElementById('est-ai-roof-line-legend');
    if (!legend) {
      legend = document.createElement('div');
      legend.id = 'est-ai-roof-line-legend';
      legend.className = 'est-ai-roof-line-legend';
      mapHost.after(legend);
    }
    legend.hidden = !diagramSegments.length;
    legend.innerHTML = Object.keys(labels).filter(function (kind) { return diagramSegments.some(function (segment) { return segment.kind === kind; }); }).map(function (kind) {
      return '<span><i style="background:' + colors[kind] + '"></i>' + esc(labels[kind]) + '</span>';
    }).join('');
    var diagramNote = document.getElementById('est-ai-roof-line-note');
    if (!diagramNote) {
      diagramNote = document.createElement('p');
      diagramNote.id = 'est-ai-roof-line-note';
      diagramNote.className = 'est-ai-roof-line-note';
      legend.after(diagramNote);
    }
    var ambiguousCount = Number(solar.roofGeometry && solar.roofGeometry.quality && solar.roofGeometry.quality.ambiguousSegmentCount || 0);
    diagramNote.hidden = !diagramSegments.length;
    diagramNote.textContent = diagramSegments.length ? 'Detected roof diagram: colored lines are included in the totals below.' + (ambiguousCount ? ' White segments (' + ambiguousCount + ') need individual confirmation.' : ' No ambiguous segments remain.') : '';
  }

  function renderSources() {
    var host = document.getElementById('est-ai-source-grid');
    if (!host || !estAiSession) return;
    var sources = estAiSession.sources || {};
    var cards = [
      { name: 'Address', item: sources.address, ok: sources.address && sources.address.available },
      { name: 'Property information', item: sources.property, ok: sources.property && sources.property.available },
      { name: 'Aerial roof data', item: sources.solar, ok: sources.solar && sources.solar.available },
      { name: 'Exterior imagery', item: sources.imagery, ok: sources.imagery && sources.imagery.available },
      { name: 'AI estimator', item: sources.ai, ok: sources.ai && sources.ai.available },
      { name: 'Roof measurements', item: { source: estAiSession.measurements && estAiSession.measurements.length ? 'User-confirmed measurement review' : 'Awaiting confirmation' }, ok: !!(estAiSession.measurements && estAiSession.measurements.length) },
      { name: 'ABC eligible branch', item: estAiSession.abcPricing && estAiSession.abcPricing.recommendedBranch ? { source: estAiSession.abcPricing.recommendedBranch.name, message: [estAiSession.abcPricing.recommendedBranch.city, estAiSession.abcPricing.recommendedBranch.state].filter(Boolean).join(', ') } : { source: 'ABC company connection', message: estAiSession.abcPricing && estAiSession.abcPricing.connected ? 'Branch recommendation pending property match' : 'Supplier connection is optional' }, ok: !!(estAiSession.abcPricing && estAiSession.abcPricing.recommendedBranch) },
      { name: 'Material pricing', item: { source: estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'ABC pricing pending connection' }, ok: !!(estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved) }
    ];
    host.innerHTML = cards.map(function (card) {
      var item = card.item || {};
      return '<div class="est-ai-source-card ' + (card.ok ? 'is-available' : 'is-unavailable') + '"><strong>' + esc(card.name) + '</strong><span>' + esc(item.source || (card.ok ? 'Connected' : 'Unavailable')) + '</span><small>' + esc(card.ok ? 'Available for this estimate' : (item.message || 'Needs confirmation')) + '</small></div>';
    }).join('');
    host.dataset.solarStatus = sources.solar && sources.solar.available ? 'available' : String(sources.solar && (sources.solar.reason || sources.solar.code || sources.solar.status || (sources.solar.detail ? 'NETWORK_OR_CORS_ERROR' : 'unavailable')) || 'not-requested');
    renderAerialPreview();
  }

  function setStage(step, state, message, detail) {
    var order = ['address', 'property', 'imagery', 'analysis', 'missing', 'building'];
    var item = document.querySelector('[data-est-ai-step="' + step + '"]');
    if (item) {
      item.classList.remove('is-active', 'is-complete', 'is-failed');
      if (state) item.classList.add('is-' + state);
      var small = item.querySelector('small');
      if (small) small.textContent = detail || (state === 'complete' ? 'Complete' : state === 'active' ? 'In progress' : state === 'failed' ? 'Unavailable — see details' : 'Waiting');
    }
    var completed = document.querySelectorAll('#est-ai-progress-list .is-complete').length;
    var percent = Math.round(((completed + (state === 'active' ? 0.4 : 0)) / order.length) * 100);
    var bar = document.getElementById('est-ai-progress-bar');
    if (bar) bar.style.width = Math.min(100, percent) + '%';
    var status = document.getElementById('est-ai-processing-status');
    if (status && message) status.textContent = message;
  }

  function resetStages() {
    document.querySelectorAll('#est-ai-progress-list [data-est-ai-step]').forEach(function (item) {
      item.classList.remove('is-active', 'is-complete', 'is-failed');
      var small = item.querySelector('small');
      if (small) small.textContent = 'Waiting';
    });
    var bar = document.getElementById('est-ai-progress-bar');
    if (bar) bar.style.width = '0%';
    var retry = document.getElementById('est-ai-retry');
    if (retry) retry.hidden = true;
    var photos = document.getElementById('est-ai-photo-request');
    if (photos) photos.hidden = true;
    var review = document.getElementById('est-ai-measurement-review');
    if (review) review.hidden = true;
  }

  async function collectSources() {
    setStage('address', 'active', 'Verifying the property address with the connected address service…');
    var addressSource = await geocodeAddress(estAiSession.address);
    if (!addressSource.available) {
      setStage('address', 'failed', addressSource.message || 'Address could not be matched. Correct the address.', 'Correct the address');
      var addressError = new Error(addressSource.message || 'Address could not be matched. Correct the address.');
      addressError.stage = 'address';
      throw addressError;
    }
    estAiSession.address.formatted = addressSource.formattedAddress || estAiSession.address.formatted;
    setStage('address', 'complete', 'Address verified.', 'Verified source');
    if (estAiSession.abcPricing && estAiSession.abcPricing.connected && typeof window.abcResolvePropertyBranches === 'function') {
      var branchResult = await window.abcResolvePropertyBranches(addressSource.latitude, addressSource.longitude);
      if (branchResult) {
        estAiSession.abcPricing.eligibleBranches = Array.isArray(branchResult.branches) ? branchResult.branches : [];
        estAiSession.abcPricing.recommendedBranch = branchResult.recommendedBranch || null;
        if (branchResult.recommendedBranch && branchResult.recommendedBranch.name) {
          estAiSession.abcPricing.branchName = branchResult.recommendedBranch.name;
          estAiSession.abcPricing.status = 'Connected — nearest eligible branch selected for this property';
        }
      }
    }
    persistDraft();

    setStage('property', 'active', 'Finding available property information…');
    var propertySource = await propertyLookup(estAiSession.address);
    setStage('property', 'complete', propertySource.available ? 'Property information found.' : propertySource.message, propertySource.available ? 'Verified source' : 'Unavailable — missing details will be asked');

    setStage('imagery', 'active', 'Finding authorized aerial roof geometry and available imagery…');
    var imageryResults = await Promise.all([solarLookup(addressSource), imageryLookup(addressSource)]);
    var solarSource = imageryResults[0];
    var imagerySource = imageryResults[1];
    applySolarMeasurements(solarSource);
    var hasAuthorizedImagery = solarSource.available || imagerySource.available;
    setStage('imagery', 'complete', solarSource.available ? 'Authorized Google aerial roof data found.' : 'Aerial roof data is unavailable for this address. Please enter measurements or upload the requested photos.', solarSource.available ? 'Google Solar API data found' : hasAuthorizedImagery ? 'Exterior imagery available; measurements need confirmation' : 'Unavailable — manual measurements available');

    estAiSession.sources = { address: addressSource, property: propertySource, solar: solarSource, imagery: imagerySource, ai: { available: false, source: 'OpenAI estimator' } };
    prepareMeasurementReview();
    renderSources();
    persistDraft();
    return estAiSession.sources;
  }

  function unansweredQuestionIds(ids) {
    var answers = estAiSession.answers || {};
    var solar = estAiSession.sources && estAiSession.sources.solar;
    var requested = Array.isArray(ids) ? ids.filter(function (id) {
      if (!QUESTION_CATALOG[id] || answers[id]) return false;
      if (id === 'stories' && estAiSession.measurementDraft && estAiSession.measurementDraft.storiesSource === 'Verified source') return false;
      if (id === 'pitch' && solar && solar.available && solar.pitchByPlane) return false;
      if (id === 'unseen_feature_details') return false;
      return true;
    }) : [];
    ['stories', 'layers', 'shingle_type', 'tear_off', 'unseen_features'].forEach(function (id) {
      if (id === 'stories' && estAiSession.measurementDraft && estAiSession.measurementDraft.storiesSource === 'Verified source') return;
      if (!answers[id] && requested.indexOf(id) === -1) requested.push(id);
    });
    if ((!solar || !solar.available || !solar.pitchByPlane) && !answers.pitch && requested.indexOf('pitch') === -1) requested.push('pitch');
    return requested;
  }

  function applyAnalysis(analysis, phase) {
    estAiSession.sources.ai = analysis.manualFallback
      ? { available: false, source: 'Manual estimate intake', message: 'AI service unavailable. Your answers and confirmed measurements will build the draft.' }
      : { available: true, source: 'OpenAI ' + (estAiSession.aiUsage ? 'usage-tracked estimator' : 'estimator') };
    estAiSession.aiSummary = String(analysis.summary || 'AI intake analysis completed.');
    estAiSession.aiObservations = Array.isArray(analysis.observations) ? analysis.observations : [];
    estAiSession.aiUncertainties = Array.isArray(analysis.uncertainties) ? analysis.uncertainties : [];
    renderSources();
    setStage('analysis', 'complete', analysis.manualFallback ? 'Continuing with questions and manual measurement review.' : 'Roof intake analysis completed.', analysis.manualFallback ? 'Manual intake fallback' : 'Real AI response');
    var questionIds = unansweredQuestionIds(analysis.questionIds);
    if (questionIds.length) {
      estAiSession.questions = questionIds.map(function (id) {
        return { id: id, text: QUESTION_CATALOG[id].text, options: (QUESTION_CATALOG[id].options || []).slice(), required: true };
      });
      estAiSession.questionIndex = 0;
      var foundRoof = estAiSession.sources.solar && estAiSession.sources.solar.available;
      setStage('missing', 'active', foundRoof
        ? 'We found the roof and need ' + questionIds.length + ' detail' + (questionIds.length === 1 ? '' : 's') + ' from you.'
        : 'Aerial roof data is unavailable for this address. Please enter measurements or upload the requested photos.');
      persistDraft();
      openQuestion();
      return;
    }
    var photoIds = Array.isArray(analysis.photoIds) ? analysis.photoIds.filter(function (id) { return PHOTO_CATALOG[id]; }) : [];
    var unseenAnswer = String(estAiSession.answers.unseen_features && estAiSession.answers.unseen_features.value || '');
    var unseenDetail = String(estAiSession.answers.unseen_feature_details && estAiSession.answers.unseen_feature_details.value || '');
    if (/^(yes|unsure)$/i.test(unseenAnswer)) {
      var exactPhotoId = /addition|detached/i.test(unseenDetail) ? 'addition_or_detached' : 'roof_component_closeup';
      if (photoIds.indexOf(exactPhotoId) === -1) photoIds.push(exactPhotoId);
    } else if (/^no$/i.test(unseenAnswer)) {
      photoIds = photoIds.filter(function (id) { return id !== 'addition_or_detached' && id !== 'roof_component_closeup'; });
    }
    if (photoIds.length) {
      estAiSession.requiredPhotos = photoIds.map(function (id) { return { id: id, label: PHOTO_CATALOG[id] }; });
      setStage('missing', 'active', 'Specific photos are needed to verify unresolved roof details.');
      showPhotoRequest();
      return;
    }
    setStage('missing', 'complete', 'Required questions are complete. Confirm the roof measurements to continue.', 'Answers saved');
    showMeasurementReview();
  }

  async function runProcessing(phase) {
    if (!estAiSession) return;
    var retry = document.getElementById('est-ai-retry');
    if (retry) retry.hidden = true;
    if (phase === 'initial' || phase === 'retry') resetStages();
    try {
      var sources = estAiSession.sources;
      if (!sources || phase === 'initial' || phase === 'retry') sources = await collectSources();
      setStage('analysis', 'active', phase === 'retry' ? 'Retrying the authenticated AI estimator…' : 'Analyzing the verified facts and identifying missing information…');
      var analysis = await callEstimator('initial', sources, []);
      applyAnalysis(analysis, 'initial');
    } catch (error) {
      var failedStage = error && error.stage === 'address' ? 'address' : 'analysis';
      if (failedStage === 'analysis' && estAiSession.sources) {
        continueWithManualIntake(error, 'initial');
        return;
      }
      setStage(failedStage, 'failed', safeError(error, 'AI estimate processing failed.'), failedStage === 'address' ? 'Correct the address' : 'Retry available');
      if (retry) retry.hidden = false;
      console.error('[AI estimator]', { status: error && error.status || 0, message: safeError(error) });
    }
  }

  function continueWithManualIntake(error, phase) {
    console.warn('[AI estimator manual fallback]', { status: error && error.status || 0, message: safeError(error) });
    estAiSession.aiUsage = null;
    applyAnalysis({
      manualFallback: true,
      summary: 'Automatic analysis was unavailable. This draft uses your answers and confirmed measurements.',
      questionIds: ['stories', 'layers', 'pitch', 'shingle_type', 'tear_off', 'decking', 'ice_water', 'ventilation', 'roof_features', 'detached_sections', 'unseen_features', 'local_code'],
      photoIds: [], observations: [],
      uncertainties: ['AI analysis was unavailable. Review the scope and all quantities before finalizing.']
    }, phase);
    persistDraft();
  }

  function openQuestion() {
    if (!estAiSession) return;
    var question = estAiSession.questions[estAiSession.questionIndex];
    if (!question) {
      closeQuestion();
      continueAfterQuestions();
      return;
    }
    var modal = document.getElementById('est-ai-question-modal');
    var definition = questionDefinition(question);
    document.getElementById('est-ai-question-text').textContent = definition.text;
    document.getElementById('est-ai-question-progress').textContent = 'Question ' + (estAiSession.questionIndex + 1) + ' of ' + estAiSession.questions.length;
    var options = document.getElementById('est-ai-question-options');
    var sameQuestion = options.dataset.questionId === question.id;
    var scrollTop = sameQuestion ? options.scrollTop : 0;
    var focusedControl = sameQuestion && options.contains(document.activeElement) ? document.activeElement : null;
    var focusAttribute = focusedControl && ['data-hm-question-step', 'data-hm-question-count-confirm', 'data-hm-question-choice', 'data-hm-question-multi'].find(function (attribute) { return focusedControl.hasAttribute(attribute); });
    var focusValue = focusAttribute && focusedControl.getAttribute(focusAttribute);
    var saved = estAiSession.answers && estAiSession.answers[question.id];
    if (definition.type === 'stories' && !isQuestionAnswerValid(question, saved)) {
      var restoredCount = Math.max(1, Math.min(4, Math.round(numberFromAnswer('stories', 1))));
      saveQuestionAnswer(question, restoredCount + (restoredCount === 1 ? ' story' : ' stories'), { count: restoredCount, splitLevel: !!(saved && (saved.splitLevel === true || /split.level/i.test(saved.value))) });
      saved = estAiSession.answers[question.id];
    }
    if (definition.type === 'count' && !saved) {
      saveQuestionAnswer(question, definition.min + ' layer', { count: definition.min });
      saved = estAiSession.answers[question.id];
    }
    options.innerHTML = renderQuestionControls(question, saved);
    options.dataset.questionId = question.id;
    options.scrollTop = scrollTop;
    if (focusAttribute) {
      var replacement = Array.prototype.find.call(options.querySelectorAll('[' + focusAttribute + ']'), function (control) { return control.getAttribute(focusAttribute) === focusValue; });
      if (replacement && !replacement.disabled) replacement.focus({ preventScroll: true });
    }
    var input = document.getElementById('est-ai-question-input');
    var inputWrap = document.getElementById('est-ai-question-custom-wrap');
    var needsText = definition.type === 'text';
    inputWrap.hidden = !needsText;
    input.value = needsText && saved ? saved.value : '';
    document.getElementById('est-ai-question-continue').disabled = !isQuestionAnswerValid(question, saved);
    document.getElementById('est-ai-question-back').disabled = estAiSession.questionIndex === 0;
    modal.hidden = false;
  }

  function closeQuestion() {
    var modal = document.getElementById('est-ai-question-modal');
    if (modal) modal.hidden = true;
  }

  function syncDependentQuestions(question, answer) {
    if (!estAiSession || !question || question.id !== 'unseen_features') return;
    var followUpIndex = estAiSession.questions.findIndex(function (item) { return item.id === 'unseen_feature_details'; });
    if (/^(yes|unsure)$/i.test(answer) && followUpIndex === -1) {
      estAiSession.questions.splice(estAiSession.questionIndex + 1, 0, {
        id: 'unseen_feature_details',
        text: QUESTION_CATALOG.unseen_feature_details.text,
        options: QUESTION_CATALOG.unseen_feature_details.options.slice(),
        required: true
      });
    } else if (/^no$/i.test(answer) && followUpIndex >= 0) {
      estAiSession.questions.splice(followUpIndex, 1);
      delete estAiSession.answers.unseen_feature_details;
    }
    persistDraft();
  }

  function submitQuestion() {
    var question = estAiSession && estAiSession.questions[estAiSession.questionIndex];
    if (!question) return;
    var saved = estAiSession.answers && estAiSession.answers[question.id];
    if (!isQuestionAnswerValid(question, saved)) return;
    var answer = saved.value;
    syncDependentQuestions(question, answer);
    estAiSession.questionIndex += 1;
    persistDraft();
    openQuestion();
  }

  function handleQuestionControl(event) {
    var question = estAiSession && estAiSession.questions[estAiSession.questionIndex];
    if (!question) return;
    var definition = questionDefinition(question);
    var saved = estAiSession.answers && estAiSession.answers[question.id];
    var stepButton = event.target.closest('[data-hm-question-step]');
    var countConfirm = event.target.closest('[data-hm-question-count-confirm]');
    var choiceButton = event.target.closest('[data-hm-question-choice]');
    if (stepButton || countConfirm) {
      var output = document.querySelector('[data-hm-question-count], [data-hm-question-count-confirm]');
      var current = Number(output && output.textContent) || Number(definition.min || 1);
      if (stepButton) current += Number(stepButton.dataset.hmQuestionStep || 0);
      current = Math.max(Number(definition.min || 1), Math.min(Number(definition.max || 4), current));
      if (definition.type === 'stories') {
        var splitLevel = saved && typeof saved.splitLevel === 'boolean' ? saved.splitLevel : false;
        saveQuestionAnswer(question, current + (current === 1 ? ' story' : ' stories'), { count: current, splitLevel: splitLevel });
      } else {
        saveQuestionAnswer(question, current + (current === 1 ? ' layer' : ' layers'), { count: current });
      }
      openQuestion();
      return;
    }
    if (choiceButton) {
      var label = choiceButton.dataset.hmQuestionChoice;
      if (definition.type === 'stories') {
        var count = saved && Number(saved.count) || 1;
        var isSplit = label === 'Yes';
        saveQuestionAnswer(question, count + (count === 1 ? ' story' : ' stories'), { count: count, splitLevel: isSplit });
      } else {
        saveQuestionAnswer(question, label);
      }
      syncDependentQuestions(question, estAiSession.answers[question.id].value);
      openQuestion();
    }
  }

  function handleQuestionMulti(event) {
    var checkbox = event.target.closest('[data-hm-question-multi]');
    var question = estAiSession && estAiSession.questions[estAiSession.questionIndex];
    if (!checkbox || !question) return;
    var definition = questionDefinition(question);
    var selected = Array.prototype.slice.call(document.querySelectorAll('[data-hm-question-multi]:checked')).map(function (item) { return item.dataset.hmQuestionMulti; });
    if (checkbox.checked && (definition.exclusive || []).indexOf(checkbox.dataset.hmQuestionMulti) >= 0) selected = [checkbox.dataset.hmQuestionMulti];
    else if (checkbox.checked) selected = selected.filter(function (value) { return (definition.exclusive || []).indexOf(value) === -1; });
    saveQuestionAnswer(question, selected.join(', '), { values: selected });
    openQuestion();
  }

  async function continueAfterQuestions() {
    setStage('missing', 'active', 'Checking the saved answers for any view that still requires a photo…');
    try {
      var analysis = await callEstimator('questions', estAiSession.sources, []);
      applyAnalysis(analysis, 'questions');
    } catch (error) {
      continueWithManualIntake(error, 'questions');
    }
  }

  function showPhotoRequest() {
    closeQuestion();
    var panel = document.getElementById('est-ai-photo-request');
    document.getElementById('est-ai-photo-requirements').innerHTML = estAiSession.requiredPhotos.map(function (item) { return '<li>' + esc(item.label) + '</li>'; }).join('');
    document.getElementById('est-ai-photo-explanation').textContent = 'Only these views are requested because the connected sources and your answers could not verify them. Photograph from the closest safe ground-level position available. Never climb onto a roof.';
    panel.hidden = false;
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function renderPhotos() {
    var gallery = document.getElementById('est-ai-photo-gallery');
    if (!gallery) return;
    gallery.innerHTML = estAiPhotoItems.map(function (item, index) {
      var rotation = Number(item.rotation || 0);
      return '<article class="est-ai-photo-card"><img src="' + esc(item.url) + '" alt="Selected property photo" style="transform:rotate(' + rotation + 'deg)" /><div class="est-ai-photo-card-body"><div class="est-ai-photo-card-name">' + esc(item.file.name) + '</div><div class="est-ai-photo-progress"><span style="width:' + Number(item.progress || 0) + '%"></span></div><small>' + esc(item.status || 'Ready to upload') + '</small><div class="est-ai-photo-card-actions"><button type="button" data-hm-photo-rotate="' + index + '">Rotate</button><button type="button" data-est-ai-photo-replace="' + index + '">Replace</button><button type="button" data-hm-photo-retry="' + index + '">Retry</button><button type="button" data-est-ai-photo-remove="' + index + '">Remove</button></div></div></article>';
    }).join('');
    var needed = estAiSession && estAiSession.requiredPhotos ? estAiSession.requiredPhotos.length : 0;
    var status = document.getElementById('est-ai-photo-status');
    if (status) status.textContent = estAiPhotoItems.length + ' of ' + needed + ' requested photo' + (needed === 1 ? '' : 's') + ' selected.';
    document.getElementById('est-ai-photo-continue').hidden = !needed || estAiPhotoItems.length < needed;
  }

  function readFileDataUrl(file, item) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader();
      reader.onprogress = function (event) {
        if (event.lengthComputable) { item.progress = Math.round((event.loaded / event.total) * 70); item.status = 'Preparing upload'; renderPhotos(); }
      };
      reader.onerror = function () { reject(new Error('A selected photo could not be read.')); };
      reader.onload = function () { item.progress = 75; item.status = 'Ready for AI analysis'; renderPhotos(); resolve(String(reader.result || '')); };
      reader.readAsDataURL(file);
    });
  }

  async function uploadPhotosAndContinue() {
    var required = estAiSession.requiredPhotos.length;
    if (estAiPhotoItems.length < required) return;
    var button = document.getElementById('est-ai-photo-continue');
    button.disabled = true;
    try {
      var photos = [];
      for (var i = 0; i < estAiPhotoItems.length; i += 1) {
        var item = estAiPhotoItems[i];
        var dataUrl = await readFileDataUrl(item.file, item);
        photos.push({ name: item.file.name, type: item.file.type, rotation: Number(item.rotation || 0), dataUrl: dataUrl });
      }
      setStage('imagery', 'active', 'Analyzing only the uploaded property views…');
      var analysis = await callEstimator('photos', estAiSession.sources, photos);
      estAiSession.photoObservations = analysis.observations || [];
      estAiSession.photosNeedManualReview = false;
      estAiPhotoItems.forEach(function (item) { item.progress = 100; item.status = 'Analyzed'; });
      renderPhotos();
      document.getElementById('est-ai-photo-request').hidden = true;
      setStage('imagery', 'complete', 'Requested photos analyzed.', 'AI-derived observations');
      setStage('missing', 'complete', 'Photo review is complete. Confirm the roof measurements to continue.', 'Photos reviewed');
      persistDraft();
      showMeasurementReview();
    } catch (error) {
      estAiPhotoItems.forEach(function (item) { item.status = 'Upload failed — retry available'; item.progress = 0; });
      renderPhotos();
      var status = document.getElementById('est-ai-photo-status');
      status.textContent = safeError(error, 'Photo analysis failed.');
      status.classList.add('is-error');
      document.getElementById('est-ai-photo-manual').hidden = false;
    } finally { button.disabled = false; }
  }

  function sourceOptions(selected) {
    return SOURCE_STATUSES.map(function (label) { return '<option' + (label === selected ? ' selected' : '') + '>' + label + '</option>'; }).join('');
  }

  function showMeasurementReview() {
    closeQuestion();
    prepareMeasurementReview();
    var panel = document.getElementById('est-ai-measurement-review');
    var saved = estAiSession.measurementDraft || {};
    var help = panel.querySelector('.est-ai-review-help');
    var solar = estAiSession.sources && estAiSession.sources.solar;
    if (!saved.stories && numberFromAnswer('stories') != null) saved.stories = numberFromAnswer('stories');
    if (!saved.layers && numberFromAnswer('layers') != null) saved.layers = numberFromAnswer('layers');
    if (!saved.pitchPlanes && estAiSession.answers.pitch) saved.pitchPlanes = estAiSession.answers.pitch.value;
    var populated = REVIEW_FIELDS.filter(function (field) { return saved[field.key] != null && saved[field.key] !== ''; }).length;
    if (help) help.textContent = populated + ' of ' + REVIEW_FIELDS.length + ' values are ready to review. Correct any value that is wrong; only unsupported measurements remain blank. Roof area never comes from living area. Waste is an editable recommendation.' + (solar && solar.roofGeometry && solar.roofGeometry.reason ? ' Some roof edges could not be reliably classified.' : '');
    var fields = document.getElementById('est-ai-review-fields');
    fields.innerHTML = REVIEW_FIELDS.map(function (field) {
      var hasValue = saved[field.key] !== undefined && saved[field.key] !== null && saved[field.key] !== '';
      var status = saved[field.key + 'Source'] || (hasValue ? 'User-confirmed' : 'Needs confirmation');
      return '<div class="est-ai-review-field"><label>' + esc(field.label.toUpperCase()) + '<div class="est-ai-measurement-control"><button type="button" data-hm-measurement-step="-1" data-hm-measurement-target="' + field.key + '" aria-label="Decrease ' + esc(field.label) + '">−</button><input class="field-input" data-hm-measurement="' + field.key + '" aria-label="' + esc(field.label + (field.unit ? ' (' + field.unit + ')' : '')) + '" type="number" inputmode="decimal" min="' + (field.min || '0') + '"' + (field.max ? ' max="' + field.max + '"' : '') + ' step="' + field.step + '" value="' + esc(hasValue ? saved[field.key] : '') + '" /><button type="button" data-hm-measurement-step="1" data-hm-measurement-target="' + field.key + '" aria-label="Increase ' + esc(field.label) + '">+</button><span class="est-ai-measurement-unit">' + esc(field.unit) + '</span></div></label><select class="field-input est-ai-source-select" data-hm-measurement-source="' + field.key + '" aria-label="' + esc(field.label) + ' source">' + sourceOptions(status) + '</select></div>';
    }).join('') + '<div class="est-ai-review-field is-wide"><label>PITCH BY ROOF PLANE<textarea class="field-input" data-hm-measurement="pitchPlanes" rows="3" placeholder="Example: Main roof 6/12; porch 3/12">' + esc(saved.pitchPlanes || '') + '</textarea></label><select class="field-input est-ai-source-select" data-hm-measurement-source="pitchPlanes" aria-label="Pitch by roof plane source">' + sourceOptions(saved.pitchPlanesSource || (saved.pitchPlanes ? 'User-confirmed' : 'Needs confirmation')) + '</select></div>';
    panel.hidden = false;
    fields.querySelectorAll('[data-hm-measurement]').forEach(function (input) {
      var key = input.dataset.hmMeasurement;
      var info = estAiSession.measurementEvidence && estAiSession.measurementEvidence[key];
      var detail = document.createElement('small');
      detail.className = 'est-ai-measurement-provenance';
      detail.textContent = info ? info.source + ' · ' + info.method : (input.value === '' ? 'Needs confirmation — no reliable measurement source' : 'User answer or measurement');
      input.closest('.est-ai-review-field').appendChild(detail);
    });
    renderSources();
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function syncMeasurementDraft(target) {
    if (!estAiSession) return;
    var key = target.dataset.hmMeasurement || target.dataset.hmMeasurementSource;
    if (!key) return;
    estAiSession.measurementDraft = estAiSession.measurementDraft || {};
    if (target.dataset.hmMeasurement) {
      estAiSession.measurementDraft[key] = target.value;
      if ((key === 'stories' || key === 'layers') && target.validity.valid && target.value !== '') {
        var count = Number(target.value);
        var previous = estAiSession.answers[key];
        var noun = key === 'stories' ? (count === 1 ? ' story' : ' stories') : (count === 1 ? ' layer' : ' layers');
        saveQuestionAnswer({ id: key }, count + noun, key === 'stories' ? { count: count, splitLevel: !!(previous && previous.splitLevel) } : { count: count });
      }
      var source = document.querySelector('[data-hm-measurement-source="' + key + '"]');
      if (source && target.value !== '' && source.value !== 'User-confirmed') source.value = 'User-confirmed';
      if (source) estAiSession.measurementDraft[key + 'Source'] = source.value;
      if ((key === 'totalRoofArea' || key === 'squares') && Number(target.value) > 0) {
        var otherKey = key === 'totalRoofArea' ? 'squares' : 'totalRoofArea';
        var otherValue = key === 'totalRoofArea' ? Number((Number(target.value) / 100).toFixed(2)) : Number((Number(target.value) * 100).toFixed(2));
        var otherInput = document.querySelector('[data-hm-measurement="' + otherKey + '"]');
        if (otherInput) otherInput.value = otherValue;
        estAiSession.measurementDraft[otherKey] = otherValue;
        estAiSession.measurementDraft[otherKey + 'Source'] = 'Auto-calculated — confirm';
        var otherSource = document.querySelector('[data-hm-measurement-source="' + otherKey + '"]');
        if (otherSource) otherSource.value = 'Auto-calculated — confirm';
        estAiSession.measurementEvidence = estAiSession.measurementEvidence || {};
        estAiSession.measurementEvidence[otherKey] = { source: 'User-corrected ' + key, method: key === 'totalRoofArea' ? 'Total roof area ÷ 100' : 'Roof squares × 100', confidence: 'Auto-calculated — confirm' };
      }
    } else {
      estAiSession.measurementDraft[key + 'Source'] = target.value;
    }
    persistDraft();
  }

  function stepMeasurement(targetKey, direction) {
    var input = document.querySelector('[data-hm-measurement="' + targetKey + '"]');
    if (!input) return;
    var step = Number(input.step || 1);
    var current = input.value === '' ? 0 : Number(input.value);
    var next = current + (Number(direction) * step);
    var min = input.min === '' ? -Infinity : Number(input.min);
    var max = input.max === '' ? Infinity : Number(input.max);
    next = Math.max(min, Math.min(max, next));
    var decimals = String(input.step || '').split('.')[1];
    input.value = decimals ? next.toFixed(decimals.length) : String(Math.round(next));
    syncMeasurementDraft(input);
  }

  function collectMeasurements() {
    var draft = estAiSession.measurementDraft || {};
    var totalArea = Number(draft.totalRoofArea || 0);
    var squares = Number(draft.squares || 0);
    var stories = Number(draft.stories || 0);
    var layers = Number(draft.layers || 0);
    var hasWaste = draft.wastePercent !== undefined && draft.wastePercent !== null && draft.wastePercent !== '';
    var waste = hasWaste ? Number(draft.wastePercent) : 0;
    if (!(totalArea > 0 || squares > 0)) throw new Error('Enter the verified or user-confirmed total roof area or squares.');
    if (!Number.isInteger(stories) || stories < 1 || stories > 4) throw new Error('Select 1, 2, 3, or 4 stories.');
    if (hasWaste && (!Number.isFinite(waste) || waste < 0 || waste > 50)) throw new Error('Enter a waste percentage between 0 and 50.');
    if (!totalArea) totalArea = squares * 100;
    squares = totalArea / 100;
    estAiSession.measurementDraft.totalRoofArea = totalArea;
    estAiSession.measurementDraft.squares = Number(squares.toFixed(2));
    return REVIEW_FIELDS.map(function (field) {
      var hasValue = draft[field.key] !== undefined && draft[field.key] !== null && draft[field.key] !== '';
      var value = field.key === 'totalRoofArea' ? totalArea : field.key === 'squares' ? Number(squares.toFixed(2)) : (hasValue ? Number(draft[field.key]) : '');
      return { key: field.key, label: field.label, value: value, unit: field.unit, sourceStatus: draft[field.key + 'Source'] || (draft[field.key] !== '' && draft[field.key] != null ? 'User-confirmed' : 'Needs confirmation'), evidence: estAiSession.measurementEvidence && estAiSession.measurementEvidence[field.key] || null };
    }).concat([{ key: 'pitchPlanes', label: 'Pitch by roof plane', value: draft.pitchPlanes || '', unit: '', sourceStatus: draft.pitchPlanesSource || (draft.pitchPlanes ? 'User-confirmed' : 'Needs confirmation') }]);
  }

  function measurementValue(key) {
    var item = (estAiSession.measurements || []).find(function (measurement) { return measurement.key === key; });
    return item ? Number(item.value || 0) : 0;
  }

  function line(code, description, quantity, unit, waste, source) {
    return {
      id: 'line_' + Math.random().toString(36).slice(2), code: code, description: description,
      quantity: Number(quantity || 0), unit: unit, material: 0, labor: 0, equipment: 0,
      waste: Number(waste || 0), taxable: true,
      quantitySource: source || (Number(quantity || 0) > 0 ? 'Confirmed measurement review' : 'Measurement not entered'), materialSource: 'Manual price required',
      laborSource: 'Company labor rate not configured — manual rate required', equipmentSource: 'Manual price required',
      confidence: quantity > 0 ? 'User-confirmed' : 'Needs confirmation'
    };
  }

  function buildLineItems() {
    var squares = measurementValue('squares');
    var layers = measurementValue('layers');
    var waste = measurementValue('wastePercent');
    var eaves = measurementValue('eaves');
    var rakes = measurementValue('rakes');
    var ridges = measurementValue('ridges');
    var hips = measurementValue('hips');
    var valleys = measurementValue('valleys');
    var stepFlashing = measurementValue('stepFlashing');
    var headwall = measurementValue('headwallFlashing');
    var tearOff = /yes/i.test(String(estAiSession.answers.tear_off && estAiSession.answers.tear_off.value || ''));
    var iceWater = String(estAiSession.answers.ice_water && estAiSession.answers.ice_water.value || '');
    var ventilation = String(estAiSession.answers.ventilation && estAiSession.answers.ventilation.value || '');
    var shingle = String(estAiSession.answers.shingle_type && estAiSession.answers.shingle_type.value || 'Field shingles');
    var items = [];
    if (tearOff) {
      items.push(line('RFG-TEAR', 'Tear off existing roofing', squares * Math.max(1, layers), 'SQ', 0));
      items.push(line('RFG-DISP', 'Roofing debris disposal', squares * Math.max(1, layers), 'SQ', 0));
    }
    items.push(line('RFG-UND', 'Synthetic roofing underlayment', squares, 'SQ', waste));
    if (/required/i.test(iceWater)) items.push(line('RFG-IWS', 'Ice-and-water barrier', eaves + valleys, 'LF', 0));
    items.push(line('RFG-START', 'Starter course shingles', eaves + rakes, 'LF', 0));
    items.push(line('RFG-FIELD', shingle, squares, 'SQ', waste));
    items.push(line('RFG-RCAP', 'Hip and ridge cap shingles', ridges + hips, 'LF', 0));
    items.push(line('RFG-DRIP', 'Metal drip edge', eaves + rakes, 'LF', 0));
    if (stepFlashing > 0) items.push(line('RFG-STFL', 'Step flashing', stepFlashing, 'LF', 0));
    if (headwall > 0) items.push(line('RFG-HWFL', 'Headwall flashing', headwall, 'LF', 0));
    if (/ridge vent/i.test(ventilation)) items.push(line('RFG-RVNT', 'Ridge ventilation', ridges, 'LF', 0));
    else if (/replace box vents/i.test(ventilation)) items.push(line('RFG-VENT', 'Roof ventilation units — quantity to confirm', 0, 'EA', 0));
    items.push(line('RFG-BOOT', 'Pipe boots and roof accessories — quantity to confirm', 0, 'EA', 0));
    items.push(line('RFG-DELV', 'Material delivery', 1, 'EA', 0, 'Standard scope — price needs confirmation'));
    items.push(line('RFG-PRMT', 'Roofing permit allowance', 1, 'EA', 0, 'Local requirement needs confirmation'));
    items.push(line('RFG-CLNP', 'Final cleanup and magnetic sweep', 1, 'EA', 0, 'Standard scope — price needs confirmation'));
    return items;
  }

  function buildEstimate() {
    var errorHost = document.getElementById('est-ai-review-error');
    try {
      estAiSession.measurements = collectMeasurements();
      estAiSession.lineItems = buildLineItems();
      estAiSession.status = 'Draft';
      estAiSession.taxRate = Number(estAiSession.taxRate || 0);
      estAiSession.overheadRate = Number(estAiSession.overheadRate || 0);
      estAiSession.profitRate = Number(estAiSession.profitRate || 0);
      estAiSession.customer = estAiSession.customer || { name: '', phone: '', email: '' };
      estAiSession.notes = estAiSession.notes || '';
      estAiSession.exclusions = estAiSession.exclusions || 'Hidden damage, code upgrades, decking replacement, and work outside the confirmed scope are excluded unless added as line items.';
      estAiSession.assumptions = [
        { text: 'Roof quantities use only Google Solar API-derived or user-confirmed measurement review values.', source: 'Confirmed measurement sources' },
        { text: estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Material prices use the connected ABC account.' : 'ABC pricing pending connection. Material prices must be entered manually.', source: 'Pricing status' },
        { text: 'Labor rates are not configured for this company and must be entered or confirmed manually.', source: 'Company configuration' }
      ].concat(estAiSession.aiUncertainties || []).map(function (item) { return typeof item === 'string' ? { text: item, source: 'AI intake review' } : item; });
      setStage('building', 'complete', 'Editable estimate built from confirmed measurements. Pricing remains manual until ABC is connected.', 'Estimate ready');
      renderSources();
      persistDraft();
      renderResult();
      showPage('page-est-ai-result');
    } catch (error) {
      errorHost.textContent = safeError(error, 'Complete the required measurement review fields.');
    }
  }

  function money(value) {
    return '$' + (Number(value) || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function lineAmounts(item) {
    var quantityWithWaste = Number(item.quantity || 0) * (1 + Number(item.waste || 0) / 100);
    return {
      material: quantityWithWaste * Number(item.material || 0),
      labor: quantityWithWaste * Number(item.labor || 0),
      equipment: quantityWithWaste * Number(item.equipment || 0)
    };
  }

  function verificationItems() {
    var items = [];
    if (estAiSession.photosNeedManualReview) items.push('Requested photo observations still require manual review; they were not verified by AI.');
    (estAiSession.measurements || []).forEach(function (item) {
      if (['Needs confirmation','Auto-calculated — confirm','Recommended — confirm','AI estimated — confirm'].indexOf(item.sourceStatus) >= 0) items.push(item.label + ' needs confirmation.');
    });
    (estAiSession.lineItems || []).forEach(function (item) {
      if (Number(item.quantity || 0) <= 0) items.push(item.description + ': quantity needs confirmation.');
      if (Number(item.material || 0) <= 0) items.push(item.description + ': material price needs confirmation.');
      if (Number(item.labor || 0) <= 0) items.push(item.description + ': labor price needs confirmation.');
    });
    if (!estAiSession.abcPricing || !estAiSession.abcPricing.pricesRetrieved) items.unshift('ABC pricing pending connection; all supplier prices are manual until a live authorized response succeeds.');
    return items;
  }

  function renderMeasurements() {
    var host = document.getElementById('est-ai-measurement-summary');
    host.innerHTML = (estAiSession.measurements || []).map(function (item) {
      return '<div class="est-ai-measurement-card"><strong>' + esc(item.label) + '</strong><span>' + esc(item.value === '' ? 'Not entered' : item.value) + (item.unit ? ' ' + esc(item.unit) : '') + '</span><small>Source: ' + esc(item.sourceStatus) + '</small></div>';
    }).join('');
  }

  function renderLineItems() {
    var body = document.getElementById('est-ai-line-items');
    body.innerHTML = (estAiSession.lineItems || []).map(function (item, index) {
      var amounts = lineAmounts(item);
      var total = amounts.material + amounts.labor + amounts.equipment;
      var quantityValue = item.confidence === 'Needs confirmation' && Number(item.quantity || 0) <= 0 ? '' : Number(item.quantity || 0);
      return '<tr data-est-ai-line="' + index + '"><td><input data-field="code" value="' + esc(item.code) + '" aria-label="Line-item code" /></td><td><input data-field="description" value="' + esc(item.description) + '" aria-label="Description" /></td><td><input data-field="quantity" type="number" min="0" step=".01" value="' + quantityValue + '" placeholder="Not entered" aria-label="Quantity" /></td><td><input data-field="unit" value="' + esc(item.unit) + '" aria-label="Unit" /></td><td><input data-field="material" type="number" min="0" step=".01" value="' + Number(item.material || 0) + '" aria-label="Material unit price" /><input class="est-ai-line-source" data-field="materialSource" value="' + esc(item.materialSource) + '" aria-label="Material price source" /></td><td><input data-field="labor" type="number" min="0" step=".01" value="' + Number(item.labor || 0) + '" aria-label="Labor unit price" /><input class="est-ai-line-source" data-field="laborSource" value="' + esc(item.laborSource) + '" aria-label="Labor price source" /></td><td><input data-field="equipment" type="number" min="0" step=".01" value="' + Number(item.equipment || 0) + '" aria-label="Equipment unit price" /></td><td><input data-field="waste" type="number" min="0" step=".1" value="' + Number(item.waste || 0) + '" aria-label="Waste percent" /></td><td><label class="est-ai-tax-check"><input data-field="taxable" type="checkbox"' + (item.taxable ? ' checked' : '') + ' /> Taxable</label></td><td><input class="est-ai-line-source" data-field="quantitySource" value="' + esc(item.quantitySource) + '" aria-label="Quantity source" /><select data-field="confidence" aria-label="Verification status">' + sourceOptions(item.confidence) + '</select></td><td data-est-ai-line-total>' + money(total) + '</td><td><div class="est-ai-line-actions"><button type="button" data-line-up="' + index + '" aria-label="Move line up">↑</button><button type="button" data-line-down="' + index + '" aria-label="Move line down">↓</button><button type="button" data-line-duplicate="' + index + '" aria-label="Duplicate line">⧉</button><button class="est-ai-remove-line" type="button" data-line-remove="' + index + '" aria-label="Remove line">×</button></div></td></tr>';
    }).join('');
    document.getElementById('est-ai-line-empty').hidden = estAiSession.lineItems.length > 0;
    updateTotals();
  }

  function updateTotals() {
    var material = 0, labor = 0, equipment = 0, taxableBase = 0;
    (estAiSession.lineItems || []).forEach(function (item) {
      var amounts = lineAmounts(item);
      material += amounts.material; labor += amounts.labor; equipment += amounts.equipment;
      if (item.taxable) taxableBase += amounts.material + amounts.equipment;
    });
    var taxRate = Number(estAiSession.taxRate || 0);
    var overheadRate = Number(estAiSession.overheadRate || 0);
    var profitRate = Number(estAiSession.profitRate || 0);
    var direct = material + labor + equipment;
    var tax = taxableBase * taxRate / 100;
    var overhead = direct * overheadRate / 100;
    var profit = (direct + overhead) * profitRate / 100;
    estAiSession.totals = { material: material, labor: labor, equipment: equipment, tax: tax, overhead: overhead, profit: profit, grand: direct + tax + overhead + profit };
    ['material', 'labor', 'equipment', 'tax', 'overhead', 'profit', 'grand'].forEach(function (key) {
      var node = document.getElementById('est-ai-total-' + key);
      if (node) node.textContent = money(estAiSession.totals[key]);
    });
    var verify = verificationItems();
    var verificationHost = document.getElementById('est-ai-verification-list');
    if (verificationHost) verificationHost.innerHTML = verify.length ? verify.map(function (item) { return '<div class="est-ai-verification-item">' + esc(item) + '</div>'; }).join('') : '<div class="est-ai-verification-item is-clear">All quantities and prices have confirmed sources.</div>';
    if (window.renderFairMarketPricing) window.renderFairMarketPricing(estAiSession, persistDraft);
    persistDraft();
  }

  function renderResult() {
    document.getElementById('est-ai-result-number').textContent = estAiSession.estimateNumber;
    document.getElementById('est-ai-result-address').textContent = estAiSession.address.formatted;
    var typeLabel = estAiSession.propertyType === 'commercial' ? 'Commercial' : 'Residential';
    var tradeLabel = { roof: 'Roof', siding: 'Siding', gutters: 'Gutters' }[estAiSession.trade] || 'Estimate';
    document.getElementById('est-ai-result-subtitle').textContent = typeLabel + ' · ' + tradeLabel + ' · Editable estimate';
    document.getElementById('est-ai-result-status').textContent = estAiSession.status || 'Draft';
    document.getElementById('est-ai-result-meta').innerHTML = '<dt>Created</dt><dd>' + esc(new Date(estAiSession.createdAt).toLocaleDateString()) + '</dd><dt>Property</dt><dd>Residential</dd><dt>Trade</dt><dd>Roof</dd><dt>Pricing</dt><dd>' + esc(estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'ABC pricing pending connection') + '</dd>';
    ['name', 'phone', 'email'].forEach(function (key) { var input = document.querySelector('[data-est-customer="' + key + '"]'); if (input) input.value = estAiSession.customer && estAiSession.customer[key] || ''; });
    var solar = estAiSession.sources && estAiSession.sources.solar;
    document.getElementById('est-ai-property-summary').innerHTML = '<div><dt>Address</dt><dd>' + esc(estAiSession.address.formatted) + '</dd></div><div><dt>Property type</dt><dd>Residential</dd></div><div><dt>Trade</dt><dd>Roof</dd></div>' + (solar && solar.available ? '<div><dt>API footprint</dt><dd>' + esc(solar.footprintSquareFeet) + ' sq ft</dd></div><div><dt>Aerial imagery</dt><dd>' + esc(solar.imageryDate || 'Date not supplied') + ' · ' + esc(solar.imageryQuality) + '</dd></div>' : '');
    var roof = [
      ['Stories', estAiSession.answers.stories && estAiSession.answers.stories.value],
      ['Split-level', estAiSession.answers.stories && typeof estAiSession.answers.stories.splitLevel === 'boolean' ? (estAiSession.answers.stories.splitLevel ? 'Yes' : 'No') : 'Needs confirmation'],
      ['Existing layers', estAiSession.answers.layers && estAiSession.answers.layers.value],
      ['Pitch', estAiSession.answers.pitch && estAiSession.answers.pitch.value || (solar && solar.pitchByPlane)],
      ['Roofing material', estAiSession.answers.shingle_type && estAiSession.answers.shingle_type.value],
      ['Tear-off', estAiSession.answers.tear_off && estAiSession.answers.tear_off.value],
      ['Decking', estAiSession.answers.decking && estAiSession.answers.decking.value],
      ['Ventilation', estAiSession.answers.ventilation && estAiSession.answers.ventilation.value]
    ];
    document.getElementById('est-ai-roof-summary').innerHTML = roof.filter(function (pair) { return pair[1]; }).map(function (pair) { return '<div><dt>' + esc(pair[0]) + '</dt><dd>' + esc(pair[1]) + '</dd></div>'; }).join('');
    document.getElementById('est-ai-scope-work').textContent = 'Remove and replace the confirmed residential roof scope using the reviewed measurements. Install applicable underlayment, shingles, edge metal, flashings, ventilation, accessories, delivery, permit allowance, and cleanup shown in the detailed line items.';
    if (estAiSession.crmManual) {
      document.getElementById('est-ai-result-meta').innerHTML = '<dt>Created</dt><dd>' + esc(new Date(estAiSession.createdAt).toLocaleDateString()) + '</dd><dt>Property</dt><dd>' + typeLabel + '</dd><dt>Trade</dt><dd>' + tradeLabel + '</dd><dt>Job type</dt><dd>' + esc(estAiSession.job_type) + '</dd>';
      document.getElementById('est-ai-property-summary').innerHTML = '<dt>Address</dt><dd>' + esc(estAiSession.address.formatted) + '</dd><dt>Property type</dt><dd>' + typeLabel + '</dd><dt>Trade</dt><dd>' + tradeLabel + '</dd>';
      document.getElementById('est-ai-scope-work').textContent = 'Review the entered ' + tradeLabel.toLowerCase() + ' quantities and scope before pricing or presenting this estimate.';
    }
    renderMeasurements();
    renderLineItems();
    document.getElementById('est-ai-tax-rate').value = Number(estAiSession.taxRate || 0);
    document.getElementById('est-ai-overhead-rate').value = Number(estAiSession.overheadRate || 0);
    document.getElementById('est-ai-profit-rate').value = Number(estAiSession.profitRate || 0);
    document.getElementById('est-ai-notes').value = estAiSession.notes || '';
    document.getElementById('est-ai-exclusions').value = estAiSession.exclusions || '';
    document.getElementById('est-ai-assumptions').innerHTML = (estAiSession.assumptions || []).map(function (item) { return '<div class="est-ai-assumption"><span>⚠</span><span>' + esc(item.text || item) + (item.source ? '<br><small>Source: ' + esc(item.source) + '</small>' : '') + '</span></div>'; }).join('');
    renderSourcesIntoResult();
    updateTotals();
  }

  function renderSourcesIntoResult() {
    var sources = estAiSession.sources || {};
    var labels = [
      sources.address && sources.address.source,
      sources.property && (sources.property.available ? sources.property.source : 'Property data unavailable'),
      sources.solar && (sources.solar.available ? sources.solar.source + ' · imagery ' + (sources.solar.imageryDate || 'date not supplied') : sources.solar.message),
      sources.imagery && (sources.imagery.available ? sources.imagery.source : 'Imagery unavailable'),
      sources.ai && sources.ai.source,
      (estAiSession.measurements || []).some(function (item) { return item.sourceStatus === 'API-derived'; }) ? 'Google Solar API-derived roof measurements' : 'User-confirmed roof measurements',
      estAiSession.abcPricing && estAiSession.abcPricing.pricesRetrieved ? 'Live ABC customer pricing' : 'ABC pricing pending connection',
      'Manual material and labor prices until connected sources are confirmed'
    ].filter(Boolean);
    document.getElementById('est-ai-source-list').innerHTML = labels.map(function (label) { return '<li>' + esc(label) + '</li>'; }).join('');
  }

  function saveEstimate(status) {
    if (!estAiSession) return;
    estAiSession.customer = estAiSession.customer || {};
    document.querySelectorAll('[data-est-customer]').forEach(function (input) { estAiSession.customer[input.dataset.estCustomer] = input.value.trim(); });
    estAiSession.notes = document.getElementById('est-ai-notes').value.trim();
    estAiSession.exclusions = document.getElementById('est-ai-exclusions').value.trim();
    if (status) estAiSession.status = status;
    estAiSession.updatedAt = new Date().toISOString();
    var copy = JSON.parse(JSON.stringify(estAiSession));
    delete copy.photoData;
    try {
      if (copy.lead_or_job_id && window.HailMoneyCrmEstimate) {
        window.HailMoneyCrmEstimate.persist(copy);
      } else {
        var saved = JSON.parse(localStorage.getItem(EST_AI_STORAGE_KEY) || '[]');
        if (!Array.isArray(saved)) throw new Error('The saved estimate collection is invalid.');
        var existing = saved.findIndex(function (item) { return item.id === copy.id; });
        if (existing >= 0) saved[existing] = copy; else saved.unshift(copy);
        localStorage.setItem(EST_AI_STORAGE_KEY, JSON.stringify(saved));
      }
      localStorage.setItem(DRAFT_KEY, JSON.stringify(copy));
    } catch (error) {
      document.getElementById('est-ai-save-status').textContent = 'Save failed: ' + error.message;
      return false;
    }
    if (currentEstimate.tradeEstimates.indexOf(copy.id) === -1) currentEstimate.tradeEstimates.push(copy.id);
    var statusHost = document.getElementById('est-ai-save-status');
    if (statusHost) statusHost.textContent = (status === 'Finalized' ? 'Finalized ' : 'Draft saved ') + new Date().toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
    document.getElementById('est-ai-result-status').textContent = estAiSession.status;
    if (typeof estAiRenderSavedList === 'function') estAiRenderSavedList();
  }

  function finalizeEstimate() {
    var pending = verificationItems();
    if (pending.length) {
      document.getElementById('est-ai-save-status').textContent = 'Finalize blocked: confirm every required quantity and price. The draft remains editable.';
      return;
    }
    saveEstimate('Finalized');
  }

  function downloadPdf(asFile) {
    if (!window.jspdf || !window.jspdf.jsPDF) throw new Error('Professional PDF library is unavailable. Reload the app and try again.');
    var doc = new window.jspdf.jsPDF({ unit:'pt', format:'letter', compress:true });
    var navy = [20, 27, 49], gold = [201, 160, 52], green = [31, 122, 72], pale = [246, 247, 249], ink = [32, 37, 48];
    var margin = 42, pageWidth = 612, contentWidth = 528;
    var customer = estAiSession.customer || {}, insurance = estAiSession.insurance || {};
    var propertyType = estAiSession.estimate_type || estAiSession.propertyType || 'residential';
    var jobType = estAiSession.job_type || 'retail';
    var trade = estAiSession.estimate_category || estAiSession.trade || 'roof';
    var pricingRows = window.HailMoneyPricing && estAiSession.fairMarketPricing ? window.HailMoneyPricing.sync(estAiSession) : [];
    var incomplete = !pricingRows.length || pricingRows.some(function (row) { return !row.result || !row.model || !(Number(row.quantity) > 0) || row.model.priceStatus === 'sample'; });
    var financial = { material:0, labor:0, equipment:0, other:0, tax:0, overhead:0, profit:0, grand:0 };
    pricingRows.forEach(function (row) {
      if (!row.result) return;
      var q = Number(row.quantity || 0), r = row.result;
      financial.material += (Number(r.materialSubtotal || 0) + Number(r.wasteCost || 0)) * q;
      financial.labor += Number(r.laborSubtotal || 0) * q;
      financial.tax += Number(r.tax || 0) * q;
      financial.overhead += Number(r.overheadProfit || 0) * q;
      financial.other += Number(r.priceAdjustment || 0) * q;
      financial.grand += Number(r.finalExtendedPrice || 0);
    });
    Object.keys(financial).forEach(function (key) { financial[key] = Math.round(financial[key] * 100) / 100; });
    function text(value) { return String(value == null || value === '' ? '-' : value).replace(/[\u2011\u2012\u2013\u2014]/g, '-'); }
    function currency(value) { return '$' + Number(value || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}); }
    function title(label, y) { doc.setTextColor.apply(doc, navy); doc.setFont('helvetica','bold'); doc.setFontSize(15); doc.text(label, margin, y); doc.setDrawColor.apply(doc, gold); doc.setLineWidth(1.5); doc.line(margin, y + 7, pageWidth - margin, y + 7); return y + 24; }
    function pairs(rows, y) {
      doc.autoTable({ startY:y, margin:{left:margin,right:margin}, body:rows, theme:'grid', styles:{font:'helvetica',fontSize:9,cellPadding:6,textColor:ink,lineColor:[220,223,228],lineWidth:.4}, columnStyles:{0:{fontStyle:'bold',fillColor:pale,cellWidth:125},1:{cellWidth:139},2:{fontStyle:'bold',fillColor:pale,cellWidth:125},3:{cellWidth:139}} });
      return doc.lastAutoTable.finalY;
    }
    function addSectionPage(label) { doc.addPage(); return title(label, 74); }
    function category(code) {
      if (/TEAR|DISP/.test(code)) return 'Tear-off and demolition';
      if (/UND|IWS/.test(code)) return 'Underlayment and protection';
      if (/STFL|HWFL|DRIP/.test(code)) return 'Flashing and metal';
      if (/VENT|BOOT|RCAP|START/.test(code)) return 'Ventilation and accessories';
      if (/DELV|PRMT/.test(code)) return 'Permits and delivery';
      if (/CLNP|DUMP/.test(code)) return 'Disposal and cleanup';
      if (/FIELD/.test(code)) return 'Roofing materials';
      return 'Labor and other scope';
    }
    function drawRoofDiagram(y) {
      var segments = estAiSession.sources && estAiSession.sources.solar && estAiSession.sources.solar.roofGeometry && estAiSession.sources.solar.roofGeometry.segments || [];
      if (!segments.length) return y;
      var colors = { eaves:[34,197,94], rakes:[249,115,22], ridges:[37,99,235], hips:[168,85,247], valleys:[239,68,68], stepFlashing:[234,179,8], headwallFlashing:[6,182,212], ambiguous:[125,125,125] };
      var points = []; segments.forEach(function(s){(s.points||[]).forEach(function(p){points.push([Number(p.longitude),Number(p.latitude)]);});});
      var xs=points.map(function(p){return p[0];}), ys=points.map(function(p){return p[1];});
      var minX=Math.min.apply(null,xs), maxX=Math.max.apply(null,xs), minY=Math.min.apply(null,ys), maxY=Math.max.apply(null,ys), w=330, h=170, x=margin;
      doc.setFillColor(249,249,247); doc.setDrawColor(215,217,223); doc.roundedRect(x,y,w,h,5,5,'FD');
      segments.forEach(function(s){ if(!colors[s.kind]||!s.points||s.points.length<2)return; var a=s.points[0],b=s.points[s.points.length-1]; var x1=x+12+(Number(a.longitude)-minX)/(maxX-minX||1)*(w-24), x2=x+12+(Number(b.longitude)-minX)/(maxX-minX||1)*(w-24); var y1=y+h-12-(Number(a.latitude)-minY)/(maxY-minY||1)*(h-24), y2=y+h-12-(Number(b.latitude)-minY)/(maxY-minY||1)*(h-24); doc.setDrawColor.apply(doc,colors[s.kind]); doc.setLineWidth(s.kind==='ambiguous'?1:1.6); doc.line(x1,y1,x2,y2); });
      var legendX=x+w+18, legendY=y+8; Object.keys(colors).forEach(function(k,i){doc.setDrawColor.apply(doc,colors[k]);doc.setLineWidth(2.5);doc.line(legendX,legendY+i*20,legendX+18,legendY+i*20);doc.setTextColor.apply(doc,ink);doc.setFont('helvetica','normal');doc.setFontSize(8);doc.text(k==='stepFlashing'?'Step flashing':k==='headwallFlashing'?'Headwall flashing':k==='ambiguous'?'Needs confirmation':k.charAt(0).toUpperCase()+k.slice(1),legendX+24,legendY+3+i*20);});
      return y+h;
    }

    doc.setFillColor.apply(doc, navy); doc.rect(0,0,pageWidth,112,'F');
    doc.setFillColor.apply(doc, gold); doc.circle(70,54,24,'F'); doc.setTextColor.apply(doc, navy); doc.setFont('helvetica','bold'); doc.setFontSize(19); doc.text('HM',70,61,{align:'center'});
    doc.setTextColor(255,255,255); doc.setFontSize(20); doc.text('HAIL MONEY',108,48); doc.setFontSize(11); doc.setFont('helvetica','normal'); doc.text('Professional Roofing Estimate',108,67);
    doc.setFont('helvetica','bold'); doc.setFontSize(10); doc.text(incomplete ? 'DRAFT - PRICING INCOMPLETE' : text(estAiSession.status || 'Draft').toUpperCase(), pageWidth-margin,42,{align:'right'}); doc.setFont('helvetica','normal'); doc.text(text(estAiSession.estimateNumber),pageWidth-margin,59,{align:'right'}); doc.text(new Date(estAiSession.createdAt).toLocaleDateString(),pageWidth-margin,75,{align:'right'});
    var y = title('Estimate summary', 145);
    y = pairs([['Customer / owner',customer.name,'Phone',customer.phone],['Email',customer.email,'Property address',estAiSession.address && estAiSession.address.formatted],['Property type',propertyType,'Estimate type',jobType],['Project type',trade,'Estimate status',incomplete?'Draft - pricing incomplete':estAiSession.status]],y);
    if (jobType === 'insurance') y = pairs([['Insurance company',insurance.company,'Claim number',insurance.claimNumber||'Not yet available'],['Date of loss',insurance.dateOfLoss,'Type of loss',insurance.typeOfLoss]],y+12);
    y = title('Financial summary', y+28);
    doc.autoTable({startY:y,margin:{left:margin,right:margin},head:[['Cost component','Amount']],body:[['Material subtotal',currency(financial.material)],['Labor subtotal',currency(financial.labor)],['Equipment subtotal',currency(financial.equipment)],['Other charges / adjustments',currency(financial.other)],['Sales tax',currency(financial.tax)],['Overhead',currency(financial.overhead)],['Profit',currency(financial.profit)],['Grand total',currency(financial.grand)]],theme:'grid',styles:{font:'helvetica',fontSize:10,cellPadding:7,lineColor:[220,223,228],lineWidth:.4},headStyles:{fillColor:navy,textColor:[255,255,255]},columnStyles:{1:{halign:'right',fontStyle:'bold'}},didParseCell:function(d){if(d.section==='body'&&d.row.index===7){d.cell.styles.fillColor=[237,246,240];d.cell.styles.fontSize=12;d.cell.styles.textColor=green;}}});
    doc.setTextColor(126,49,40); doc.setFont('helvetica','bold'); doc.setFontSize(9); doc.text(incomplete?'Pricing is incomplete or based on unverified sample assumptions. Review and approve rates before acceptance.':'All displayed line-item prices are included in the grand total.',margin,doc.lastAutoTable.finalY+20);

    y = addSectionPage('Measurement summary');
    var ms=(estAiSession.measurements||[]).map(function(m){var status=/confirm/i.test(m.sourceStatus||'')?'Review':'';return [text(m.label),text(m.value)+(m.unit?' '+m.unit:''),status];});
    doc.autoTable({startY:y,margin:{left:margin,right:margin},head:[['Measurement','Value','Status']],body:ms,theme:'striped',styles:{font:'helvetica',fontSize:9,cellPadding:6},headStyles:{fillColor:navy},columnStyles:{1:{halign:'right'},2:{halign:'center',textColor:[145,91,18],cellWidth:72}}});
    y=title('Detected roof diagram',doc.lastAutoTable.finalY+30); drawRoofDiagram(y);

    y = addSectionPage('Detailed line items');
    var grouped={}; pricingRows.forEach(function(row){var code=((estAiSession.lineItems||[]).find(function(i){return i.id===row.id;})||{}).code||'';(grouped[category(code)]||(grouped[category(code)]=[])).push({row:row,code:code});});
    Object.keys(grouped).forEach(function(group,groupIndex){var nextY=groupIndex?doc.lastAutoTable.finalY+22:y;var estimatedHeight=58+(grouped[group].length+1)*24;if(groupIndex&&nextY+estimatedHeight>720){y=addSectionPage('Detailed line items (continued)');}else y=nextY;doc.setFont('helvetica','bold');doc.setFontSize(11);doc.setTextColor.apply(doc,navy);doc.text(group,margin,y);var subtotal=0;var body=grouped[group].map(function(entry,i){var r=entry.row,res=r.result;var total=res?Number(res.finalExtendedPrice||0):0;subtotal+=total;return [String(i+1),entry.code,text(r.name),Number(r.quantity||0).toFixed(2),text(r.unit),res?currency(res.finalUnitPrice):'Unpriced',res?currency(Number(res.tax||0)*Number(r.quantity||0)):'-',res?currency(total):'-'];});body.push([{content:'Category subtotal',colSpan:7,styles:{halign:'right',fontStyle:'bold'}},{content:currency(subtotal),styles:{halign:'right',fontStyle:'bold'}}]);doc.autoTable({startY:y+8,margin:{left:margin,right:margin,bottom:58},head:[['#','Code','Description','Qty','Unit','Unit price','Tax','Total']],body:body,theme:'grid',styles:{font:'helvetica',fontSize:7.5,cellPadding:4,overflow:'linebreak',lineColor:[222,224,229],lineWidth:.35},headStyles:{fillColor:navy},columnStyles:{0:{cellWidth:22},1:{cellWidth:55},2:{cellWidth:185},3:{cellWidth:42,halign:'right'},4:{cellWidth:34},5:{cellWidth:62,halign:'right'},6:{cellWidth:53,halign:'right'},7:{cellWidth:64,halign:'right'}},showHead:'everyPage',rowPageBreak:'avoid'});});

    y=addSectionPage('Estimate recap');
    doc.autoTable({startY:y,margin:{left:margin,right:margin},head:[['Category','Subtotal']],body:Object.keys(grouped).map(function(group){return [group,currency(grouped[group].reduce(function(sum,e){return sum+Number(e.row.result&&e.row.result.finalExtendedPrice||0);},0))];}),theme:'grid',styles:{font:'helvetica',fontSize:10,cellPadding:7},headStyles:{fillColor:navy},columnStyles:{1:{halign:'right'}}});
    y=title('Final total',doc.lastAutoTable.finalY+32);doc.setFillColor(237,246,240);doc.roundedRect(margin,y,contentWidth,58,6,6,'F');doc.setTextColor.apply(doc,navy);doc.setFont('helvetica','bold');doc.setFontSize(13);doc.text('GRAND TOTAL',margin+18,y+35);doc.setTextColor.apply(doc,green);doc.setFontSize(20);doc.text(currency(financial.grand),pageWidth-margin-18,y+36,{align:'right'});

    y=addSectionPage('Terms and acceptance');
    [['Scope notes',estAiSession.notes||'Roofing work is limited to the detailed line items and confirmed measurements in this estimate.'],['Exclusions',estAiSession.exclusions||'Hidden damage, code upgrades, decking replacement, and work outside the confirmed scope are excluded unless added in writing.'],['Payment terms','Payment schedule and financing terms must be confirmed in the signed contract.'],['Estimate validity','Pricing is valid for 30 days from the created date unless otherwise stated.']].forEach(function(section){y=title(section[0],y);doc.setTextColor.apply(doc,ink);doc.setFont('helvetica','normal');doc.setFontSize(10);var lines=doc.splitTextToSize(text(section[1]),contentWidth);doc.text(lines,margin,y);y+=lines.length*13+22;});
    doc.setDrawColor(120,124,132);doc.line(margin,650,275,650);doc.line(337,650,pageWidth-margin,650);doc.setFontSize(8);doc.text('Customer acceptance / date',margin,665);doc.text('Estimator / date',337,665);doc.setFontSize(9);doc.text('Estimator: '+text(estAiSession.estimatorName||'Hail Money representative'),337,690);

    y=addSectionPage('Internal methodology appendix');
    doc.setTextColor.apply(doc,ink);doc.setFont('helvetica','normal');doc.setFontSize(9);doc.text(doc.splitTextToSize('Internal use: source provenance, imagery dates, pricing warnings, and calculation methods are documented here so they do not distract from the customer-facing estimate.',contentWidth),margin,y);
    y+=35;var sourceNodes=document.querySelectorAll('#est-ai-source-list li');var sources=[];sourceNodes.forEach(function(n){sources.push([n.textContent]);});doc.autoTable({startY:y,margin:{left:margin,right:margin},head:[['Data and pricing sources']],body:sources,theme:'striped',styles:{font:'helvetica',fontSize:8,cellPadding:6},headStyles:{fillColor:navy}});

    var pageCount=doc.getNumberOfPages();for(var p=1;p<=pageCount;p++){doc.setPage(p);if(p>1){doc.setFillColor.apply(doc,navy);doc.rect(0,0,pageWidth,42,'F');doc.setTextColor(255,255,255);doc.setFont('helvetica','bold');doc.setFontSize(10);doc.text('HAIL MONEY',margin,26);doc.setFont('helvetica','normal');doc.text(text(estAiSession.estimateNumber),pageWidth-margin,26,{align:'right'});}doc.setDrawColor(220,222,227);doc.line(margin,754,pageWidth-margin,754);doc.setTextColor(95,99,108);doc.setFont('helvetica','normal');doc.setFontSize(8);doc.text(text(estAiSession.estimateNumber)+' | '+text(customer.name||estAiSession.address&&estAiSession.address.formatted),margin,770);doc.text('Page '+p+' of '+pageCount,pageWidth-margin,770,{align:'right'});}
    var blob = doc.output('blob');
    if (asFile === true) return new File([blob], estAiSession.estimateNumber + '.pdf', {type:'application/pdf'});
    doc.save(estAiSession.estimateNumber + '.pdf');
  }

  var estimateStarting = false;
  async function startEstimate() {
    if (estimateStarting) return;
    estimateStarting = true;
    try {
    if (window.HailMoneyCrmEstimate && !currentEstimate.crmContext && !currentEstimate.quickContext) { window.HailMoneyCrmEstimate.start(); return; }
    if (!estAiValidateAddress(true)) return;
    var user;
    try { user = await currentFirebaseUser(); }
    catch (error) { document.getElementById('est-ai-address-status').textContent = safeError(error, 'Sign-in is required.'); return; }
    if (!await abcChooseEstimatePricing()) return;
    var now = new Date();
    var abcState = window.abcEstimateRequestState || {};
    var abcConnected = !abcEstimateManualPricing && abcState.connected === true && !!abcState.selection;
    estAiSession = {
      id: 'ai_est_' + now.getTime().toString(36),
      estimateNumber: 'HM-' + now.getFullYear() + '-' + String(now.getTime()).slice(-6),
      createdAt: now.toISOString(), updatedAt: now.toISOString(),
      propertyType: 'residential', trade: 'roof', address: estAiReadAddress(),
      abcPricing: {
        connected: abcConnected,
        accountName: abcConnected ? abcState.selection.accountName : null,
        branchName: abcConnected ? abcState.selection.branchName : null,
        pricesRetrieved: false,
        status: abcConnected ? 'Connected — live prices not retrieved' : 'ABC pricing pending connection'
      },
      currentUser: { uid: user.uid }, answers: {}, questions: [], questionIndex: 0,
      requiredPhotos: [], measurements: [], lineItems: [], assumptions: [],
      customer: { name: '', phone: '', email: '' }, taxRate: 0, overheadRate: 0, profitRate: 0,
      notes: '', exclusions: '', status: 'Processing'
    };
    currentEstimate.aiSession = estAiSession;
    if (window.HailMoneyCrmEstimate) window.HailMoneyCrmEstimate.attach(estAiSession);
    currentEstimate.propertyType = 'residential';
    currentEstimate.measurementScope = 'roof';
    currentEstimate.measurementSource = 'ai';
    persistDraft();
    document.getElementById('est-ai-processing-context').textContent = 'Residential · Roof · ' + estAiSession.address.formatted;
    showPage('page-est-ai-processing');
    runProcessing('initial');
    } finally { estimateStarting = false; }
  }

  function enhanceProcessingPage() {
    document.getElementById('est-ai-progress-list').innerHTML = [
      ['address', 'Verifying address'], ['property', 'Finding property information'], ['imagery', 'Finding available imagery'],
      ['analysis', 'Analyzing the roof'], ['missing', 'Checking missing information'], ['building', 'Building estimate']
    ].map(function (item, index) { return '<li data-est-ai-step="' + item[0] + '"><span>' + (index + 1) + '</span><div><strong>' + item[1] + '</strong><small>Waiting</small></div></li>'; }).join('');
    var progressCard = document.querySelector('#page-est-ai-processing .est-ai-processing-card');
    var sourceSection = document.createElement('section');
    sourceSection.className = 'main-menu-card estimates-gold-card est-ai-result-card';
    sourceSection.innerHTML = '<div class="estimates-panel-heading"><div><h2>Connected data sources</h2><p>Only sources that respond for this property are used.</p></div></div><div id="est-ai-source-grid" class="est-ai-source-grid"></div>';
    progressCard.insertAdjacentElement('afterend', sourceSection);
    var aerialSection = document.createElement('section');
    aerialSection.id = 'est-ai-aerial-preview';
    aerialSection.className = 'main-menu-card estimates-gold-card est-ai-result-card est-ai-aerial-card';
    aerialSection.hidden = true;
    aerialSection.innerHTML = '<div class="estimates-panel-heading"><div><div class="est-section-label">AUTHORIZED AERIAL PREVIEW</div><h2>Roof located</h2><p id="est-ai-aerial-meta"></p></div></div><div id="est-ai-aerial-metrics" class="est-ai-aerial-metrics"></div><div id="est-ai-aerial-map" role="img" aria-label="Authorized Google aerial preview of the property roof"></div><p class="est-ai-aerial-note">Roof area, pitch, azimuth, footprint, and roof segments come from Google Solar API data—not visual guessing.</p>';
    sourceSection.insertAdjacentElement('afterend', aerialSection);
    var photoPanel = document.getElementById('est-ai-photo-request');
    var photoManual = document.createElement('button');
    photoManual.id = 'est-ai-photo-manual';
    photoManual.type = 'button';
    photoManual.className = 'btn';
    photoManual.textContent = 'Continue Draft with Manual Photo Review';
    photoManual.hidden = true;
    photoManual.addEventListener('click', function () {
      estAiSession.photosNeedManualReview = true;
      photoPanel.hidden = true;
      setStage('missing', 'complete', 'Photos remain unverified. Continue with measurements and review the draft before use.', 'Manual photo review required');
      persistDraft();
      showMeasurementReview();
    });
    photoPanel.appendChild(photoManual);
    var review = document.createElement('section');
    review.id = 'est-ai-measurement-review';
    review.className = 'main-menu-card estimates-gold-card est-ai-result-card';
    review.hidden = true;
    review.innerHTML = '<div class="est-section-label">MEASUREMENT REVIEW</div><h2>Confirm the roof measurements</h2><p class="est-ai-review-help">The connected services did not provide a complete roof measurement report. Enter or correct the values below. Living area is never used as roof area.</p><div id="est-ai-review-fields" class="est-ai-review-grid"></div><div id="est-ai-review-error" class="est-ai-review-error" aria-live="polite"></div><div class="est-ai-review-actions"><button class="btn" type="button" id="est-ai-review-save-exit">Save and Exit</button><button class="btn btn-primary" type="button" id="est-ai-review-continue">Build Editable Estimate</button></div>';
    photoPanel.insertAdjacentElement('afterend', review);
    document.querySelector('#est-ai-question-modal .est-ai-modal-actions').innerHTML = '<button class="btn" type="button" id="est-ai-question-back">Back</button><button class="btn" type="button" id="est-ai-question-save-exit">Save and Exit</button><button class="btn btn-primary est-ai-modal-primary" type="button" id="est-ai-question-continue" disabled>Continue</button>';
  }

  function enhanceResultPage() {
    var wrap = document.querySelector('#page-est-ai-result .placeholder-wrap');
    wrap.innerHTML = '<h1 class="page-title">AI Roof Estimate</h1><p class="page-subtitle" id="est-ai-result-subtitle">Editable estimate</p><div class="page-actions"><button class="btn" type="button" id="back-from-est-ai-result">Back to Estimates</button></div><div class="est-ai-result-toolbar"><button class="btn" type="button" id="est-ai-save-draft">Save Draft</button><button class="btn" type="button" id="est-ai-finalize">Finalize Estimate</button><button class="btn" type="button" id="est-ai-print">Print</button><button class="btn btn-primary" type="button" id="est-ai-download-pdf">Download PDF</button></div><section class="main-menu-card estimates-gold-card est-ai-result-header"><div><div class="est-section-label">ESTIMATE</div><h2 id="est-ai-result-number">—</h2><p id="est-ai-result-address">—</p><span id="est-ai-result-status" class="est-ai-status-pill">Draft</span></div><dl id="est-ai-result-meta"></dl></section><section class="main-menu-card estimates-gold-card est-ai-result-card"><div class="estimates-panel-heading"><div><h2>Customer and property</h2><p>Complete any customer details before finalizing.</p></div></div><div class="est-ai-detail-grid"><div class="est-ai-detail-card"><h3>Customer</h3><label class="est-ai-field">NAME<input class="field-input" data-est-customer="name" /></label><label class="est-ai-field">PHONE<input class="field-input" data-est-customer="phone" /></label><label class="est-ai-field">EMAIL<input class="field-input" type="email" data-est-customer="email" /></label></div><div class="est-ai-detail-card"><h3>Property</h3><dl id="est-ai-property-summary"></dl></div><div class="est-ai-detail-card"><h3>Existing roof details</h3><dl id="est-ai-roof-summary"></dl></div><div class="est-ai-detail-card"><h3>Scope of work</h3><p id="est-ai-scope-work"></p></div></div></section><section class="main-menu-card estimates-gold-card est-ai-result-card"><div class="estimates-panel-heading"><div><h2>Measurement summary</h2><p>Every value is labeled by source and confirmation status.</p></div></div><div id="est-ai-measurement-summary" class="est-ai-measurement-summary"></div></section><section class="main-menu-card estimates-gold-card est-ai-result-card"><div class="estimates-panel-heading"><div><h2>Detailed line items</h2><p>Edit, duplicate, reorder, or remove any item. No supplier price is fabricated.</p></div><button class="btn" type="button" id="est-ai-add-line">Add Line Item</button></div><div class="est-ai-table-wrap"><table class="est-ai-line-table"><thead><tr><th>Code</th><th>Description</th><th>Qty</th><th>Unit</th><th>Material / source</th><th>Labor / source</th><th>Equipment</th><th>Waste %</th><th>Tax</th><th>Quantity source / status</th><th>Line total</th><th>Actions</th></tr></thead><tbody id="est-ai-line-items"></tbody></table></div><div id="est-ai-line-empty" class="est-ai-empty-state">No line items.</div><div class="est-ai-totals"><div class="est-ai-totals-controls"><label>Tax rate %<input class="field-input" id="est-ai-tax-rate" type="number" min="0" step=".01" value="0" /></label><label>Overhead %<input class="field-input" id="est-ai-overhead-rate" type="number" min="0" step=".01" value="0" /></label><label>Profit %<input class="field-input" id="est-ai-profit-rate" type="number" min="0" step=".01" value="0" /></label></div><dl class="est-ai-totals-breakdown"><div><dt>Material subtotal</dt><dd id="est-ai-total-material">$0.00</dd></div><div><dt>Labor subtotal</dt><dd id="est-ai-total-labor">$0.00</dd></div><div><dt>Equipment subtotal</dt><dd id="est-ai-total-equipment">$0.00</dd></div><div><dt>Tax total</dt><dd id="est-ai-total-tax">$0.00</dd></div><div><dt>Overhead</dt><dd id="est-ai-total-overhead">$0.00</dd></div><div><dt>Profit</dt><dd id="est-ai-total-profit">$0.00</dd></div><div class="grand"><dt>Grand total</dt><dd id="est-ai-total-grand">$0.00</dd></div><div class="source-note"><dt>Pricing source</dt><dd>Manual until connected</dd></div></dl></div></section><section class="main-menu-card estimates-gold-card est-ai-result-card"><div class="estimates-panel-heading"><div><h2>Items requiring verification</h2><p>The estimate cannot be finalized while required values or prices remain unconfirmed.</p></div></div><div id="est-ai-verification-list" class="est-ai-verification-list"></div></section><section class="main-menu-card estimates-gold-card est-ai-result-card"><div class="estimates-panel-heading"><div><h2>Notes, exclusions, and assumptions</h2><p>Review the AI intake assumptions before presenting the estimate.</p></div></div><div id="est-ai-assumptions"></div><div class="est-ai-textarea-grid"><label>NOTES<textarea class="field-input" id="est-ai-notes" rows="5"></textarea></label><label>EXCLUSIONS<textarea class="field-input" id="est-ai-exclusions" rows="5"></textarea></label></div></section><section class="main-menu-card estimates-gold-card est-ai-result-card"><div class="estimates-panel-heading"><div><h2>Measurement and pricing sources</h2><p>Supplier pricing remains pending until a live authorized ABC request succeeds.</p></div></div><div class="est-ai-detail-card"><ul id="est-ai-source-list"></ul></div></section><div class="est-ai-save-row"><div id="est-ai-save-status" aria-live="polite"></div></div>';
  }

  function bindEvents() {
    var start = document.getElementById('est-ai-generate');
    var resume = document.createElement('button');
    resume.type = 'button';
    resume.className = 'btn';
    resume.id = 'est-ai-question-resume';
    resume.textContent = 'Resume Saved Questionnaire';
    resume.hidden = true;
    start.insertAdjacentElement('afterend', resume);
    function refreshResume() {
      try {
        var draft = JSON.parse(localStorage.getItem(DRAFT_KEY) || 'null');
        resume.hidden = !(draft && draft.questions && draft.questions.length && draft.questionIndex < draft.questions.length && draft.status === 'Processing');
      } catch (_) { resume.hidden = true; }
    }
    resume.addEventListener('click', async function () {
      var user = await currentFirebaseUser();
      var draft = JSON.parse(localStorage.getItem(DRAFT_KEY) || 'null');
      if (!draft || !draft.currentUser || draft.currentUser.uid !== user.uid) return;
      estAiSession = draft;
      currentEstimate.aiSession = draft;
      document.getElementById('est-ai-processing-context').textContent = 'Residential · Roof · ' + draft.address.formatted;
      showPage('page-est-ai-processing');
      openQuestion();
    });
    refreshResume();
    start.addEventListener('click', function (event) { event.preventDefault(); event.stopImmediatePropagation(); startEstimate(); }, true);
    document.getElementById('est-ai-question-continue').addEventListener('click', submitQuestion);
    document.getElementById('est-ai-question-back').addEventListener('click', function () { if (estAiSession.questionIndex > 0) { estAiSession.questionIndex -= 1; persistDraft(); openQuestion(); } });
    document.getElementById('est-ai-question-save-exit').addEventListener('click', function () { persistDraft(); refreshResume(); closeQuestion(); showPage('page-estimates'); });
    document.getElementById('est-ai-question-options').addEventListener('click', handleQuestionControl);
    document.getElementById('est-ai-question-options').addEventListener('change', handleQuestionMulti);
    document.getElementById('est-ai-question-input').addEventListener('input', function () {
      var question = estAiSession && estAiSession.questions[estAiSession.questionIndex];
      if (question && questionDefinition(question).type === 'text') saveQuestionAnswer(question, this.value.trim());
    });
    document.getElementById('est-ai-photo-gallery').addEventListener('click', function (event) {
      var rotate = event.target.closest('[data-hm-photo-rotate]');
      var retry = event.target.closest('[data-hm-photo-retry]');
      if (rotate) { var item = estAiPhotoItems[Number(rotate.dataset.hmPhotoRotate)]; if (item) { item.rotation = (Number(item.rotation || 0) + 90) % 360; renderPhotos(); } }
      if (retry) { var retryItem = estAiPhotoItems[Number(retry.dataset.hmPhotoRetry)]; if (retryItem) { retryItem.status = 'Ready to retry'; retryItem.progress = 0; renderPhotos(); } }
    });
    document.getElementById('est-ai-photo-continue').addEventListener('click', function (event) { event.preventDefault(); event.stopImmediatePropagation(); uploadPhotosAndContinue(); }, true);
    document.getElementById('est-ai-review-fields').addEventListener('input', function (event) { if (event.target.matches('[data-hm-measurement]')) syncMeasurementDraft(event.target); });
    document.getElementById('est-ai-review-fields').addEventListener('change', function (event) { if (event.target.matches('[data-hm-measurement-source]')) syncMeasurementDraft(event.target); });
    document.getElementById('est-ai-review-fields').addEventListener('click', function (event) {
      var button = event.target.closest('[data-hm-measurement-step]');
      if (button) stepMeasurement(button.dataset.hmMeasurementTarget, button.dataset.hmMeasurementStep);
    });
    document.getElementById('est-ai-review-save-exit').addEventListener('click', function () { persistDraft(); showPage('page-estimates'); });
    document.getElementById('est-ai-review-continue').addEventListener('click', buildEstimate);
    document.getElementById('back-from-est-ai-result').addEventListener('click', function () { showPage('page-estimates'); });
    document.getElementById('est-ai-add-line').addEventListener('click', function () { estAiSession.lineItems.push(line('MANUAL', 'New line item', 0, 'EA', 0, 'User entered')); renderLineItems(); });
    document.getElementById('est-ai-line-items').addEventListener('input', function (event) {
      var row = event.target.closest('[data-est-ai-line]'); if (!row) return;
      var item = estAiSession.lineItems[Number(row.dataset.estAiLine)]; var field = event.target.dataset.field; if (!item || !field) return;
      item[field] = ['quantity', 'material', 'labor', 'equipment', 'waste'].indexOf(field) >= 0 ? Math.max(0, Number(event.target.value) || 0) : event.target.value;
      if (field === 'material' && item.material > 0 && item.materialSource === 'Manual price required') item.materialSource = 'User-confirmed manual price';
      if (field === 'labor' && item.labor > 0 && /not configured/.test(item.laborSource)) item.laborSource = 'User-confirmed manual labor rate';
      updateTotals();
    });
    document.getElementById('est-ai-line-items').addEventListener('change', function (event) {
      var row = event.target.closest('[data-est-ai-line]'); if (!row) return;
      var item = estAiSession.lineItems[Number(row.dataset.estAiLine)]; if (!item) return;
      if (event.target.dataset.field === 'taxable') item.taxable = event.target.checked;
      else if (event.target.dataset.field) item[event.target.dataset.field] = event.target.value;
      updateTotals();
    });
    document.getElementById('est-ai-line-items').addEventListener('click', function (event) {
      var action = event.target.closest('[data-line-up],[data-line-down],[data-line-duplicate],[data-line-remove]'); if (!action) return;
      var attr = action.hasAttribute('data-line-up') ? 'up' : action.hasAttribute('data-line-down') ? 'down' : action.hasAttribute('data-line-duplicate') ? 'duplicate' : 'remove';
      var index = Number(action.dataset.lineUp || action.dataset.lineDown || action.dataset.lineDuplicate || action.dataset.lineRemove);
      if (attr === 'up' && index > 0) estAiSession.lineItems.splice(index - 1, 0, estAiSession.lineItems.splice(index, 1)[0]);
      if (attr === 'down' && index < estAiSession.lineItems.length - 1) estAiSession.lineItems.splice(index + 1, 0, estAiSession.lineItems.splice(index, 1)[0]);
      if (attr === 'duplicate') { var copy = JSON.parse(JSON.stringify(estAiSession.lineItems[index])); copy.id = 'line_' + Math.random().toString(36).slice(2); estAiSession.lineItems.splice(index + 1, 0, copy); }
      if (attr === 'remove') estAiSession.lineItems.splice(index, 1);
      renderLineItems();
    });
    ['tax', 'overhead', 'profit'].forEach(function (key) { document.getElementById('est-ai-' + key + '-rate').addEventListener('input', function () { estAiSession[key === 'tax' ? 'taxRate' : key + 'Rate'] = Math.max(0, Number(this.value) || 0); updateTotals(); }); });
    document.getElementById('est-ai-save-draft').addEventListener('click', function () { saveEstimate('Draft'); });
    document.getElementById('est-ai-finalize').addEventListener('click', finalizeEstimate);
    document.getElementById('est-ai-print').addEventListener('click', function () { document.body.classList.add('est-ai-printing'); window.print(); setTimeout(function () { document.body.classList.remove('est-ai-printing'); }, 500); });
    document.getElementById('est-ai-download-pdf').addEventListener('click', downloadPdf);
    var sendButton = document.createElement('button');
    sendButton.type = 'button';sendButton.className = 'btn';sendButton.id = 'est-ai-share-estimate';sendButton.textContent = 'Share / Email Estimate';
    document.getElementById('est-ai-download-pdf').after(sendButton);
    sendButton.onclick = async function () {
      if (!estAiSession || saveEstimate() === false) return;
      var file = downloadPdf(true);
      try {
        if (navigator.canShare && navigator.canShare({files:[file]})) {
          await navigator.share({files:[file],title:estAiSession.estimateNumber});
        } else {
          downloadPdf();
          var email = estAiSession.customer && estAiSession.customer.email || '';
          var link = document.createElement('a');
          link.href = 'mailto:' + encodeURIComponent(email) + '?subject=' + encodeURIComponent('Hail Money estimate ' + estAiSession.estimateNumber) + '&body=' + encodeURIComponent('Please review estimate ' + estAiSession.estimateNumber + ' for ' + estAiSession.address.formatted + '.\n\nAttach the downloaded estimate PDF before sending.');
          link.click();
          document.getElementById('est-ai-save-status').textContent = 'PDF downloaded. Attach it to your email draft and send from your email app.';
        }
      } catch (error) {
        if (error.name !== 'AbortError') document.getElementById('est-ai-save-status').textContent = 'Sharing unavailable. Use Download PDF to share the saved estimate.';
      }
    };
    document.querySelectorAll('[data-est-customer],#est-ai-notes,#est-ai-exclusions').forEach(function (input) { input.addEventListener('input', persistDraft); });
  }

  function init() {
    enhanceProcessingPage();
    enhanceResultPage();
    bindEvents();
    estAiRunProcessing = runProcessing;
    estAiOpenQuestionModal = openQuestion;
    estAiSubmitQuestion = submitQuestion;
    estAiContinueAfterQuestions = continueAfterQuestions;
    estAiShowPhotoRequest = showPhotoRequest;
    estAiRenderPhotos = renderPhotos;
    estAiUploadPhotosAndContinue = uploadPhotosAndContinue;
    estAiRenderResult = renderResult;
    estAiRenderLineItems = renderLineItems;
    estAiUpdateTotals = updateTotals;
    estAiSaveEstimate = saveEstimate;
    window.estAiBuildCrmManual = function () {
      var context = currentEstimate.crmContext;
      if (!context) { window.HailMoneyCrmEstimate.start(); return; }
      var trade = context.estimate.estimate_category;
      var values = currentEstimate.measurements[trade] || {};
      var fields = (EST_FIELD_DEFS[trade] || []).filter(function (f) { return f.key !== 'pitch' && Number(values[f.key]) > 0; });
      if (!fields.length) { showUploadToast('Enter at least one measured quantity to build the estimate.'); return; }
      estAiSession = Object.assign({}, context.estimate, { crmManual: true, status: 'Draft', sources: {}, answers: {}, assumptions: [], notes: '', exclusions: '', taxRate: 0, overheadRate: 0, profitRate: 0, measurementSource: currentEstimate.measurementSource,
        abcPricing: { connected: !abcEstimateManualPricing && !!(window.abcEstimateRequestState && window.abcEstimateRequestState.connected), pricesRetrieved: false }, measurements: [], lineItems: [] });
      fields.forEach(function (f) {
        var unit = f.key === 'squares' ? 'SQ' : /Area|gables/.test(f.key) ? 'SF' : /Openings|downspouts|elbows/.test(f.key) ? 'EA' : 'LF';
        var quantity = Number(values[f.key]);
        estAiSession.measurements.push({ key: f.key, label: f.label, value: quantity, unit: unit, sourceStatus: 'User-confirmed' });
        estAiSession.lineItems.push(line('MANUAL-' + trade.toUpperCase() + '-' + f.key, f.label + ' — scope and pricing to review', quantity, unit, 0, 'User-entered measurement'));
      });
      estAiSession.assumptions.push({ text: 'Measurements entered by the user; automated analysis was not performed for this estimate.', source: 'Measurement method' });
      window.HailMoneyCrmEstimate.attach(estAiSession);currentEstimate.aiSession = estAiSession;
      renderResult();saveEstimate('Draft');showPage('page-est-ai-result');
    };
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
