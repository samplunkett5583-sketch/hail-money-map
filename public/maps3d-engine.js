(function () {
  'use strict';

  var engine = {
    active: false,
    ready: false,
    initializing: null,
    map: null,
    host: null,
    lib: null,
    overlaysByDate: Object.create(null),
    visibleDates: new Set(),
    syncingFrom3D: false,
    syncingFrom2D: false,
    ignoreLogicMapUntil: 0,
    lastSteady: true,
    fov: 45,
    mode: 'HYBRID'
  };

  function enabledByUrl() {
    try {
      var params = new URLSearchParams(window.location.search || '');
      return params.get('native3d') === '1' || localStorage.getItem('hailMoneyNative3D') === '1';
    } catch (_) {
      return false;
    }
  }

  function latOf(value) {
    if (!value) return NaN;
    return typeof value.lat === 'function' ? Number(value.lat()) : Number(value.lat);
  }

  function lngOf(value) {
    if (!value) return NaN;
    return typeof value.lng === 'function' ? Number(value.lng()) : Number(value.lng);
  }

  function rangeForZoom(zoom, lat) {
    var host = document.getElementById('stormMap3d') || document.getElementById('stormMap');
    var height = Math.max(320, Number(host && host.clientHeight) || window.innerHeight || 800);
    var safeLat = Math.max(-80, Math.min(80, Number(lat) || 0));
    var metersPerPixel = 156543.03392804097 * Math.cos(safeLat * Math.PI / 180) /
      Math.pow(2, Number(zoom) || 5);
    var visibleGroundHeight = metersPerPixel * height;
    var fovRadians = engine.fov * Math.PI / 180;
    return Math.max(20, Math.min(63170000, visibleGroundHeight / (2 * Math.tan(fovRadians / 2))));
  }

  function zoomForRange(range, lat) {
    var host = document.getElementById('stormMap3d') || document.getElementById('stormMap');
    var height = Math.max(320, Number(host && host.clientHeight) || window.innerHeight || 800);
    var safeLat = Math.max(-80, Math.min(80, Number(lat) || 0));
    var fovRadians = engine.fov * Math.PI / 180;
    var visibleGroundHeight = Math.max(1, Number(range) || 1) * 2 * Math.tan(fovRadians / 2);
    var metersPerPixel = visibleGroundHeight / height;
    var numerator = 156543.03392804097 * Math.cos(safeLat * Math.PI / 180);
    var zoom = Math.log2(numerator / Math.max(0.000001, metersPerPixel));
    return Math.max(2, Math.min(22, zoom));
  }

  function cssRgba(rgb, alpha) {
    var match = String(rgb || '').match(/\d+(?:\.\d+)?/g) || [];
    if (match.length < 3) return String(rgb || '#f0c14d');
    return 'rgba(' + Number(match[0]) + ',' + Number(match[1]) + ',' + Number(match[2]) + ',' +
      Math.max(0, Math.min(1, Number(alpha) || 0)) + ')';
  }

  function normalizeMode(style) {
    style = String(style || 'hybrid').toLowerCase();
    if (style === 'roadmap') return 'ROADMAP';
    if (style === 'satellite') return 'SATELLITE';
    return 'HYBRID';
  }

  function clearDate(dateStr) {
    var list = engine.overlaysByDate[dateStr] || [];
    list.forEach(function (el) {
      try { el.remove(); } catch (_) {}
    });
    delete engine.overlaysByDate[dateStr];
  }

  function setDateVisible(dateStr, visible) {
    var list = engine.overlaysByDate[dateStr] || [];
    list.forEach(function (el) {
      try { el.style.display = visible ? '' : 'none'; } catch (_) {}
    });
    if (visible) engine.visibleDates.add(dateStr);
    else engine.visibleDates.delete(dateStr);
  }

  function setVisibleDates(dates) {
    var wanted = new Set((dates || []).map(String));
    Object.keys(engine.overlaysByDate).forEach(function (dateStr) {
      setDateVisible(dateStr, wanted.has(dateStr));
    });
  }

  function setMode(style) {
    engine.mode = normalizeMode(style);
    if (engine.map) engine.map.mode = engine.mode;
  }

  function syncFrom2D(center, zoom) {
    if (!engine.ready || !engine.map || engine.syncingFrom3D || !center) return;
    var lat = latOf(center);
    var lng = lngOf(center);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || !Number.isFinite(Number(zoom))) return;
    engine.syncingFrom2D = true;
    engine.map.center = { lat: lat, lng: lng, altitude: 0 };
    engine.map.range = rangeForZoom(Number(zoom), lat);
    requestAnimationFrame(function () { engine.syncingFrom2D = false; });
  }

  function syncLogicMapFrom3D() {
    if (!engine.ready || !engine.map || engine.syncingFrom2D) return;
    var logicMap = window.mapsState && window.mapsState.map;
    if (!logicMap) return;
    var center = engine.map.center;
    var lat = latOf(center);
    var lng = lngOf(center);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    var zoom = zoomForRange(Number(engine.map.range), lat);
    engine.syncingFrom3D = true;
    // The hidden 2D map follows the native camera for legacy calculations only.
    // Give its resulting idle event nowhere to bounce back into the visible 3D camera.
    engine.ignoreLogicMapUntil = performance.now() + 300;
    try {
      logicMap.setCenter({ lat: lat, lng: lng });
      logicMap.setZoom(zoom);
    } catch (_) {}
    requestAnimationFrame(function () { engine.syncingFrom3D = false; });
  }

  function selectPropertyFrom3D(event) {
    var pos = event && event.position;
    if (!pos) return;
    var lat = latOf(pos);
    var lng = lngOf(pos);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    if (typeof window.mapsSelectPropertyAtLocation === 'function') {
      window.mapsSelectPropertyAtLocation(lat, lng, { title: 'Selected Property' });
    }
  }

  async function init(options) {
    options = options || {};
    if (engine.ready) return engine;
    if (engine.initializing) return engine.initializing;
    if (!enabledByUrl()) return null;
    if (!(window.google && google.maps && typeof google.maps.importLibrary === 'function')) return null;

    engine.initializing = (async function () {
      engine.lib = await google.maps.importLibrary('maps3d');
      var host = document.getElementById('stormMap3d');
      var logicMap = window.mapsState && window.mapsState.map;
      if (!host || !logicMap || !engine.lib || !engine.lib.Map3DElement) return null;

      var center = logicMap.getCenter ? logicMap.getCenter() : { lat: 38.6, lng: -97 };
      var lat = latOf(center);
      var lng = lngOf(center);
      var zoom = Number(logicMap.getZoom && logicMap.getZoom()) || 5;
      engine.mode = normalizeMode(options.style || 'hybrid');

      var map = new engine.lib.Map3DElement({
        center: { lat: lat, lng: lng, altitude: 0 },
        range: rangeForZoom(zoom, lat),
        tilt: 0,
        heading: 0,
        roll: 0,
        fov: engine.fov,
        mode: engine.mode,
        defaultUIHidden: true,
        gestureHandling: 'GREEDY',
        minTilt: 0,
        maxTilt: 0,
        minHeading: 0,
        maxHeading: 0
      });

      host.replaceChildren(map);
      engine.map = map;
      engine.host = host;
      engine.active = true;
      engine.ready = true;
      document.body.classList.add('maps-native-3d');

      map.addEventListener('gmp-click', selectPropertyFrom3D);
      map.addEventListener('gmp-steadychange', function (event) {
        engine.lastSteady = !!(event && event.isSteady);
        if (engine.lastSteady) syncLogicMapFrom3D();
      });

      var cachedDefs = window.mapsState && window.mapsState._native3dOverlayDefs || {};
      var selectedDates = window.mapsState && window.mapsState.selectedDates || [];
      Object.keys(cachedDefs).forEach(function (dateStr) {
        renderSwathDefs(dateStr, cachedDefs[dateStr], selectedDates.indexOf(dateStr) !== -1);
      });

      if (logicMap && typeof logicMap.addListener === 'function') {
        logicMap.addListener('idle', function () {
          if (!engine.ready || engine.syncingFrom3D || !engine.lastSteady) return;
          if (performance.now() < engine.ignoreLogicMapUntil) return;

          // Ignore reflected camera updates. The 3D renderer must never "correct"
          // itself from the hidden compatibility map after a wheel gesture.
          var logicCenter = logicMap.getCenter();
          var logicLat = latOf(logicCenter);
          var logicLng = lngOf(logicCenter);
          var nativeCenter = engine.map && engine.map.center;
          var nativeLat = latOf(nativeCenter);
          var nativeLng = lngOf(nativeCenter);
          var logicZoom = Number(logicMap.getZoom());
          var targetRange = rangeForZoom(logicZoom, logicLat);
          var currentRange = Number(engine.map && engine.map.range);
          var sameCenter = Number.isFinite(logicLat) && Number.isFinite(logicLng) &&
            Number.isFinite(nativeLat) && Number.isFinite(nativeLng) &&
            Math.abs(logicLat - nativeLat) < 0.000001 &&
            Math.abs(logicLng - nativeLng) < 0.000001;
          var sameRange = Number.isFinite(targetRange) && Number.isFinite(currentRange) &&
            Math.abs(targetRange - currentRange) / Math.max(1, currentRange) < 0.002;
          if (sameCenter && sameRange) return;

          syncFrom2D(logicCenter, logicZoom);
        });
      }

      return engine;
    })().finally(function () {
      engine.initializing = null;
    });

    return engine.initializing;
  }

  async function renderSwathDefs(dateStr, defs, visible) {
    if (!engine.ready || !engine.map || !engine.lib) return false;
    clearDate(dateStr);
    var list = [];
    var Polygon3D = engine.lib.Polygon3DElement;
    var AltitudeMode = engine.lib.AltitudeMode;
    var maxElements = 1600;

    outer:
    for (var i = 0; i < (defs || []).length; i++) {
      var def = defs[i] || {};
      var paths = Array.isArray(def.paths) ? def.paths : [];
      for (var j = 0; j < paths.length; j++) {
        if (list.length >= maxElements) break outer;
        var path = paths[j];
        if (!Array.isArray(path) || path.length < 3) continue;
        var coords = path.map(function (pt) {
          return { lat: Number(pt.lat), lng: Number(pt.lng), altitude: 0 };
        }).filter(function (pt) {
          return Number.isFinite(pt.lat) && Number.isFinite(pt.lng);
        });
        if (coords.length < 3) continue;

        var isHail = def.stormType !== 'wind' && def.stormType !== 'tornado';
        var strokeWidth = isHail ? 0 : 1;
        var polygon = new Polygon3D({
          path: coords,
          altitudeMode: AltitudeMode ? AltitudeMode.CLAMP_TO_GROUND : 'CLAMP_TO_GROUND',
          fillColor: cssRgba(def.fill, def.opacity),
          strokeColor: cssRgba(def.fill, isHail ? 0 : Math.min(0.65, Number(def.opacity) + 0.10)),
          strokeWidth: strokeWidth,
          drawsOccludedSegments: true,
          geodesic: false,
          extruded: false,
          zIndex: Number(def.zIndex) || 0
        });
        polygon.dataset.hmDate = dateStr;
        polygon.dataset.hmStormType = String(def.stormType || 'hail');
        polygon.dataset.hmBandMin = String(def.bandMin || '');
        polygon.style.display = visible === false || def.show === false ? 'none' : '';
        engine.map.appendChild(polygon);
        list.push(polygon);
      }
    }

    engine.overlaysByDate[dateStr] = list;
    if (visible !== false) engine.visibleDates.add(dateStr);
    return true;
  }

  function setOpacityForType(type, scale) {
    type = String(type || '').toLowerCase();
    scale = Math.max(0, Math.min(1, Number(scale)));
    Object.keys(engine.overlaysByDate).forEach(function (dateStr) {
      (engine.overlaysByDate[dateStr] || []).forEach(function (el) {
        if (String(el.dataset.hmStormType || '').toLowerCase() !== type) return;
        el.style.opacity = String(scale);
      });
    });
  }

  engine.enabledByUrl = enabledByUrl;
  engine.init = init;
  engine.clearDate = clearDate;
  engine.setDateVisible = setDateVisible;
  engine.setVisibleDates = setVisibleDates;
  engine.setMode = setMode;
  engine.syncFrom2D = syncFrom2D;
  engine.renderSwathDefs = renderSwathDefs;
  engine.setOpacityForType = setOpacityForType;
  engine.rangeForZoom = rangeForZoom;
  engine.zoomForRange = zoomForRange;

  window.HMM3D = engine;

  function bootWhenReady(attempt) {
    if (!enabledByUrl()) return;
    attempt = Number(attempt) || 0;
    if (window.mapsState && window.mapsState.map && window.google && google.maps) {
      var style = 'hybrid';
      try { style = localStorage.getItem('hailMoneyMapStyle') || 'hybrid'; } catch (_) {}
      init({ style: style }).catch(function (error) {
        console.error('[HMM3D] init failed', error);
      });
      return;
    }
    if (attempt < 80) setTimeout(function () { bootWhenReady(attempt + 1); }, 100);
  }

  setTimeout(function () { bootWhenReady(0); }, 0);
})();
