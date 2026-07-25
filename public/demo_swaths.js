// demo_swaths.js
// Hard-coded hail swaths for demo purposes.
// You can add as many as you like. The frontend code will render them all.

window.demoSwaths = [
  {
    id: "az_scottsdale_2025_11_23",
    label: "Scottsdale AZ – Nov 23, 2025",
    state: "AZ",
    date: "2025-11-23",
    maxHailInches: 1.75,
    color: "#ff0000",
    polygon: [
      // Rough swath over Scottsdale area (you can tweak later)
      { lat: 33.700, lng: -112.050 },
      { lat: 33.700, lng: -111.900 },
      { lat: 33.640, lng: -111.850 },
      { lat: 33.580, lng: -111.870 },
      { lat: 33.580, lng: -112.020 }
    ]
  },
  {
    id: "az_scottsdale_2024_04_10",
    label: "Scottsdale AZ – Apr 10, 2024",
    state: "AZ",
    date: "2024-04-10",
    maxHailInches: 2.25,
    color: "#ff7f00",
    polygon: [
      { lat: 33.640, lng: -112.100 },
      { lat: 33.640, lng: -111.930 },
      { lat: 33.600, lng: -111.880 },
      { lat: 33.540, lng: -111.900 },
      { lat: 33.540, lng: -112.070 }
    ]
  },
  {
    id: "mo_kc_2024_05_02",
    label: "Kansas City MO – May 2, 2024",
    state: "MO",
    date: "2024-05-02",
    maxHailInches: 2.75,
    color: "#ff00ff",
    polygon: [
      { lat: 39.200, lng: -94.700 },
      { lat: 39.200, lng: -94.450 },
      { lat: 39.050, lng: -94.450 },
      { lat: 39.050, lng: -94.700 }
    ]
  }
];

// Map Options compatibility patch.
// This keeps the controls usable when the main HTML is served from a branch
// whose large monolithic index file cannot be updated through the GitHub API.
(function installMapOptionsCompatibilityPatch() {
  'use strict';

  function escapeHtml(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, function (ch) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch];
    });
  }

  function windMph(point) {
    var value = point && (
      point.magnitude != null ? point.magnitude :
      point.speed != null ? point.speed :
      point.mag_mph != null ? point.mag_mph :
      point.wind_speed != null ? point.wind_speed :
      point.wind_mph != null ? point.wind_mph :
      point.props && point.props.MAGNITUDE
    );
    value = Number(value);
    return isFinite(value) ? value : 0;
  }

  function windRemark(point) {
    return String(point && (
      point.remark || point.remarks || point.comments || point.comment ||
      point.narrative || point.description || point.text ||
      point.props && (point.props.REMARK || point.props.REMARKS)
    ) || 'Wind report details unavailable.');
  }

  function pointLocation(point, lat, lon) {
    return String(point && (
      point.address || point.location || point.city || point.place ||
      point.county || point.props && (
        point.props.LOCATION || point.props.CITY || point.props.COUNTY
      )
    ) || (lat.toFixed(4) + ', ' + lon.toFixed(4)));
  }

  function pointDate(dateStr, point) {
    var raw = point && (point.date || point.datetime || point.valid || point.time);
    var parsed = raw ? new Date(raw) : new Date(dateStr + 'T12:00:00');
    return isNaN(parsed.getTime()) ? dateStr : parsed.toLocaleDateString();
  }

  function detailHtml(dateStr, point, locationText) {
    var mph = windMph(point);
    var speedText = mph ? (Math.round(mph * 10) / 10) + ' mph' : 'Speed unavailable';
    var placeholder = '<span style="color:#89919a;font-weight:600;">API needed</span>';
    return '<div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#20252a;width:min(760px,calc(100vw - 80px));padding:8px 6px 10px;">' +
      '<div style="font-size:21px;line-height:1.2;text-align:center;margin:4px 0 14px;">' + escapeHtml(locationText) + '</div>' +
      '<div style="display:grid;grid-template-columns:minmax(150px,1fr) auto;gap:5px 14px;font-size:14px;line-height:1.35;max-width:390px;margin:0 auto;">' +
        '<span style="text-align:right;">Population Density</span><b>' + placeholder + '</b>' +
        '<span style="text-align:right;">Median Age</span><b>' + placeholder + '</b>' +
        '<span style="text-align:right;">Avg Household Value</span><b>' + placeholder + '</b>' +
        '<span style="text-align:right;">Average Income</span><b>' + placeholder + '</b>' +
        '<span style="text-align:right;">Avg Households / Sq Mi</span><b>' + placeholder + '</b>' +
      '</div>' +
      '<div style="display:grid;grid-template-columns:180px 78px minmax(240px,1fr);margin-top:18px;border:1px solid #d9dde2;font-size:14px;">' +
        '<div style="padding:11px 10px;border-right:1px solid #d9dde2;">' + escapeHtml(pointDate(dateStr, point)) + '</div>' +
        '<div style="padding:11px 8px;border-right:1px solid #d9dde2;text-align:center;font-weight:800;">' + escapeHtml(speedText) + '</div>' +
        '<div style="padding:11px 10px;">' + escapeHtml(windRemark(point)) + '</div>' +
      '</div>' +
      '<div style="margin-top:8px;text-align:center;color:#6b737b;font-size:11px;">Verified National Weather Service ground report</div>' +
    '</div>';
  }

  function windIcon() {
    var svg = '<svg xmlns="http://www.w3.org/2000/svg" width="38" height="38" viewBox="0 0 38 38">' +
      '<path d="M5 4h24l4 4v22l-5 5H10l-5-5Z" fill="#9d8b31" stroke="#5f5314" stroke-width="2"/>' +
      '<path d="M9 28 27 10M12 30l3-3M25 12l3-3" stroke="#f5e7a0" stroke-width="2" stroke-linecap="round"/>' +
      '<path d="M12 11v17M12 12l13 4-13 4Z" fill="#fff" stroke="#fff" stroke-width="1.5" stroke-linejoin="round"/>' +
      '<path d="M17 13.5v5M21.5 15v5" stroke="#9d8b31" stroke-width="1.3"/></svg>';
    return {
      url: 'data:image/svg+xml;charset=UTF-8,' + encodeURIComponent(svg),
      scaledSize: new google.maps.Size(38, 38),
      anchor: new google.maps.Point(19, 19)
    };
  }

  function startPatch() {
    // The full integrated version already contains this control and needs no fallback.
    if (document.getElementById('maps-show-wind-spotter-reports')) return;

    var hailCheck = document.getElementById('maps-show-hail-spotter-reports');
    var panel = hailCheck && hailCheck.closest('.maps-options-menu');
    if (!panel) return;

    var oldAddressButton = document.getElementById('maps-address-markers-btn');
    if (oldAddressButton) {
      var addressLabel = document.createElement('label');
      addressLabel.className = 'maps-options-check';
      addressLabel.setAttribute('for', 'maps-show-campaign-address-markers');
      addressLabel.innerHTML =
        '<input type="checkbox" id="maps-show-campaign-address-markers" checked>' +
        '<span class="maps-options-layer-icon" aria-hidden="true">' +
          '<svg viewBox="0 0 24 24" focusable="false"><path fill="currentColor" d="M12 2a7 7 0 0 0-7 7c0 5.25 7 13 7 13s7-7.75 7-13a7 7 0 0 0-7-7Zm0 9.5A2.5 2.5 0 1 1 12 6a2.5 2.5 0 0 1 0 5.5Z"/></svg>' +
        '</span><span>Address Markers</span>';
      oldAddressButton.replaceWith(addressLabel);
      var addressCheck = addressLabel.querySelector('input');
      addressCheck.checked = !window.mapsState || mapsState.showCampaignAddressMarkers !== false;
      addressCheck.addEventListener('change', function () {
        if (typeof window.mapsSetCampaignAddressMarkersVisible === 'function') {
          window.mapsSetCampaignAddressMarkersVisible(addressCheck.checked);
        } else if (window.mapsState) {
          mapsState.showCampaignAddressMarkers = addressCheck.checked;
          (mapsState.campaignAddressMarkers || []).forEach(function (marker) {
            try { marker.setMap(addressCheck.checked ? mapsState.map : null); } catch (_) {}
          });
        }
        try { localStorage.setItem('hailMoneyShowCampaignAddressMarkers', addressCheck.checked ? '1' : '0'); } catch (_) {}
      });
    }

    var fullscreen = document.getElementById('maps-fullscreen-btn');
    var note = panel.querySelector('.maps-options-note');
    if (fullscreen) fullscreen.remove();
    if (note) note.remove();

    var windLabel = document.createElement('label');
    windLabel.className = 'maps-options-check maps-options-check-wind';
    windLabel.setAttribute('for', 'maps-show-wind-spotter-reports');
    windLabel.innerHTML = '<input type="checkbox" id="maps-show-wind-spotter-reports"><span>Show Wind Spotter Reports</span>';
    panel.appendChild(windLabel);

    var style = document.createElement('style');
    style.textContent =
      '#page-map .maps-options-check-wind{margin-top:4px;padding-top:10px;border-top:1px solid #d9dde2}' +
      '#page-map .maps-options-layer-icon{display:flex;align-items:center;justify-content:center;width:16px;color:#343a40;line-height:1}' +
      '#page-map .maps-options-layer-icon svg{width:13px;height:16px;display:block}';
    document.head.appendChild(style);

    var windCheck = windLabel.querySelector('input');
    try { windCheck.checked = localStorage.getItem('hailMoneyShowWindSpotterReports') === '1'; } catch (_) {}
    var markersByDate = {};
    var infoWindow = null;
    var icon = null;

    function clearDate(dateStr) {
      (markersByDate[dateStr] || []).forEach(function (marker) {
        try { marker.setMap(null); } catch (_) {}
      });
      delete markersByDate[dateStr];
    }

    function drawDate(dateStr, points) {
      clearDate(dateStr);
      if (!windCheck.checked || !window.mapsState || !mapsState.map || !window.google || !google.maps) return;
      var isVisible = mapsState.selectedDates.indexOf(dateStr) !== -1 || mapsState.previewDate === dateStr;
      icon = icon || windIcon();
      infoWindow = infoWindow || new google.maps.InfoWindow();
      markersByDate[dateStr] = [];
      (points || []).forEach(function (point) {
        var lat = Number(point.lat);
        var lon = Number(point.lon);
        if (!isFinite(lat) || !isFinite(lon)) return;
        var initialLocation = pointLocation(point, lat, lon);
        var marker = new google.maps.Marker({
          map: isVisible ? mapsState.map : null,
          position: { lat: lat, lng: lon },
          icon: icon,
          clickable: true,
          zIndex: 1000000
        });
        marker.addListener('click', function () {
          infoWindow.setContent(detailHtml(dateStr, point, point._windSpotterAddress || initialLocation));
          infoWindow.open(mapsState.map, marker);
          if (point._windSpotterGeocodeResolved || !google.maps.Geocoder) return;
          point._windSpotterGeocodeResolved = true;
          new google.maps.Geocoder().geocode({ location: { lat: lat, lng: lon } }, function (results, status) {
            if (status !== 'OK' || !results || !results[0]) return;
            point._windSpotterAddress = results[0].formatted_address || initialLocation;
            infoWindow.setContent(detailHtml(dateStr, point, point._windSpotterAddress));
          });
        });
        markersByDate[dateStr].push(marker);
      });
    }

    function syncWindMarkers() {
      if (!window.mapsState || !mapsState.map) return;
      if (!windCheck.checked) {
        Object.keys(markersByDate).forEach(clearDate);
        if (infoWindow) infoWindow.close();
        return;
      }
      var source = mapsState.windPointsByDate || {};
      Object.keys(source).forEach(function (dateStr) {
        var points = source[dateStr] || [];
        if (!markersByDate[dateStr] || markersByDate[dateStr]._sourceLength !== points.length) {
          drawDate(dateStr, points);
          markersByDate[dateStr]._sourceLength = points.length;
        }
        var visible = mapsState.selectedDates.indexOf(dateStr) !== -1 || mapsState.previewDate === dateStr;
        markersByDate[dateStr].forEach(function (marker) {
          try { marker.setMap(visible ? mapsState.map : null); } catch (_) {}
        });
      });
      Object.keys(markersByDate).forEach(function (dateStr) {
        if (!source[dateStr]) clearDate(dateStr);
      });
    }

    windCheck.addEventListener('change', function () {
      try { localStorage.setItem('hailMoneyShowWindSpotterReports', windCheck.checked ? '1' : '0'); } catch (_) {}
      syncWindMarkers();
    });
    window.setInterval(syncWindMarkers, 700);
    syncWindMarkers();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { window.setTimeout(startPatch, 0); });
  } else {
    window.setTimeout(startPatch, 0);
  }
}());
