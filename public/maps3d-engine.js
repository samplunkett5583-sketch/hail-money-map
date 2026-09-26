(function () {
  'use strict';

  var engine = {
    active: false,
    ready: false,
    initializing: null,
    map: null,
    host: null,
    overlaysByDate: Object.create(null),
    overlayMeta: new WeakMap(),
    visibleDates: new Set(),
    syncingFromVector: false,
    mode: 'hybrid'
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

  function normalizeMode(style) {
    style = String(style || 'hybrid').toLowerCase();
    if (style === 'roadmap') return 'roadmap';
    if (style === 'satellite') return 'satellite';
    return 'hybrid';
  }

  function clearDate(dateStr) {
    var list = engine.overlaysByDate[dateStr] || [];
    list.forEach(function (polygon) {
      try { polygon.setMap(null); } catch (_) {}
    });
    delete engine.overlaysByDate[dateStr];
    engine.visibleDates.delete(dateStr);
  }

  function setDateVisible(dateStr, visible) {
    var list = engine.overlaysByDate[dateStr] || [];
    list.forEach(function (polygon) {
      var meta = engine.overlayMeta.get(polygon) || {};
      var shouldShow = visible && meta.show !== false;
      try { polygon.setMap(shouldShow ? engine.map : null); } catch (_) {}
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
    if (engine.map) {
      try { engine.map.setMapTypeId(engine.mode); } catch (_) {}
    }
  }

  function syncFrom2D(center, zoom) {
    if (!engine.ready || !engine.map || !center) return;
    var lat = latOf(center);
    var lng = lngOf(center);
    zoom = Number(zoom);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || !Number.isFinite(zoom)) return;
    engine.map.setCenter({ lat: lat, lng: lng });
    engine.map.setZoom(zoom);
  }

  function syncLogicMapFromVector() {
    if (!engine.ready || !engine.map) return;
    var logicMap = window.mapsState && window.mapsState.map;
    if (!logicMap) return;
    var center = engine.map.getCenter && engine.map.getCenter();
    var zoom = Number(engine.map.getZoom && engine.map.getZoom());
    var lat = latOf(center);
    var lng = lngOf(center);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || !Number.isFinite(zoom)) return;

    engine.syncingFromVector = true;
    try {
      logicMap.setCenter({ lat: lat, lng: lng });
      logicMap.setZoom(zoom);
    } catch (_) {}
    requestAnimationFrame(function () {
      engine.syncingFromVector = false;
    });
  }

  function selectPropertyFromVector(event) {
    var pos = event && event.latLng;
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
      var mapsLib = await google.maps.importLibrary('maps');
      var host = document.getElementById('stormMap3d');
      var logicMap = window.mapsState && window.mapsState.map;
      if (!host || !logicMap || !mapsLib || !mapsLib.Map) return null;

      var center = logicMap.getCenter ? logicMap.getCenter() : { lat: 38.6, lng: -97 };
      var lat = latOf(center);
      var lng = lngOf(center);
      var zoom = Number(logicMap.getZoom && logicMap.getZoom()) || 5;
      engine.mode = normalizeMode(options.style || 'hybrid');

      host.replaceChildren();

      var map = new mapsLib.Map(host, {
        center: { lat: lat, lng: lng },
        zoom: zoom,
        mapTypeId: engine.mode,
        renderingType: mapsLib.RenderingType ? mapsLib.RenderingType.VECTOR : 'VECTOR',
        isFractionalZoomEnabled: true,
        gestureHandling: 'greedy',
        tilt: 0,
        heading: 0,
        tiltInteractionEnabled: false,
        headingInteractionEnabled: false,
        disableDefaultUI: true,
        clickableIcons: false,
        keyboardShortcuts: false,
        backgroundColor: '#111'
      });

      engine.map = map;
      engine.host = host;
      engine.active = true;
      engine.ready = true;
      document.body.classList.add('maps-native-3d');

      map.addListener('click', selectPropertyFromVector);

      // Native Google wheel + fractional zoom owns the camera. We do no work
      // during zoom_changed. Only after Google reports idle do we copy the final
      // camera into the hidden legacy map for compatibility calculations.
      map.addListener('idle', function () {
        syncLogicMapFromVector();
      });

      var cachedDefs = window.mapsState && window.mapsState._native3dOverlayDefs || {};
      var selectedDates = window.mapsState && window.mapsState.selectedDates || [];
      Object.keys(cachedDefs).forEach(function (dateStr) {
        renderSwathDefs(dateStr, cachedDefs[dateStr], selectedDates.indexOf(dateStr) !== -1);
      });

      return engine;
    })().finally(function () {
      engine.initializing = null;
    });

    return engine.initializing;
  }

  async function renderSwathDefs(dateStr, defs, visible) {
    if (!engine.ready || !engine.map || !(window.google && google.maps && google.maps.Polygon)) return false;
    clearDate(dateStr);

    var list = [];
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
          return { lat: Number(pt.lat), lng: Number(pt.lng) };
        }).filter(function (pt) {
          return Number.isFinite(pt.lat) && Number.isFinite(pt.lng);
        });
        if (coords.length < 3) continue;

        var isHail = def.stormType !== 'wind' && def.stormType !== 'tornado';
        var fillOpacity = Math.max(0, Math.min(1, Number(def.opacity) || 0));
        var strokeOpacity = isHail ? 0 : Math.min(0.65, fillOpacity + 0.10);

        var polygon = new google.maps.Polygon({
          paths: coords,
          clickable: false,
          draggable: false,
          editable: false,
          geodesic: false,
          fillColor: def.fill || '#f0c14d',
          fillOpacity: fillOpacity,
          strokeColor: def.fill || '#f0c14d',
          strokeOpacity: strokeOpacity,
          strokeWeight: isHail ? 0 : 1,
          zIndex: Number(def.zIndex) || 0,
          map: visible === false || def.show === false ? null : engine.map
        });

        engine.overlayMeta.set(polygon, {
          date: String(dateStr),
          stormType: String(def.stormType || 'hail').toLowerCase(),
          baseOpacity: fillOpacity,
          show: def.show !== false
        });
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
      (engine.overlaysByDate[dateStr] || []).forEach(function (polygon) {
        var meta = engine.overlayMeta.get(polygon) || {};
        if (meta.stormType !== type) return;
        try {
          polygon.setOptions({
            fillOpacity: (Number(meta.baseOpacity) || 0) * scale,
            strokeOpacity: type === 'hail' ? 0 : Math.min(0.65, (Number(meta.baseOpacity) || 0) * scale + 0.10)
          });
        } catch (_) {}
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

  window.HMM3D = engine;

  function bootWhenReady(attempt) {
    if (!enabledByUrl()) return;
    attempt = Number(attempt) || 0;

    if (window.mapsState && window.mapsState.map && window.google && google.maps) {
      var style = 'hybrid';
      try { style = localStorage.getItem('hailMoneyMapStyle') || 'hybrid'; } catch (_) {}
      init({ style: style }).catch(function (error) {
        console.error('[HMM vector] init failed', error);
      });
      return;
    }

    if (attempt < 80) {
      setTimeout(function () { bootWhenReady(attempt + 1); }, 100);
    }
  }

  setTimeout(function () { bootWhenReady(0); }, 0);
})();
