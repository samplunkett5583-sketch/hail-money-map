'use strict';

const original = require('./index');
const { onRequest } = require('firebase-functions/v2/https');
const admin = require('firebase-admin');
const logger = require('firebase-functions/logger');

module.exports = original;
Object.assign(module.exports, require('./docusign'));

function permitCors(req, res) {
  const origin = String(req.get('origin') || '');
  const allowed = /^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com|hail\.money|www\.hail\.money)$/i.test(origin) ||
    /^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if (allowed) res.set('Access-Control-Allow-Origin', origin);
  res.set('Vary', 'Origin');
  res.set('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
}

async function requireFirebaseUser(req) {
  const authHeader = String(req.get('authorization') || '');
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) throw Object.assign(new Error('Sign in is required.'), { statusCode: 401 });
  return admin.auth().verifyIdToken(match[1]);
}

async function googleCloudAccessToken() {
  const credential = admin.app().options.credential || admin.credential.applicationDefault();
  const access = await credential.getAccessToken();
  if (!access || !access.access_token) throw new Error('Google Cloud access token is unavailable.');
  return access.access_token;
}

function formatGoogleDate(value) {
  if (!value || !Number(value.year)) return null;
  return [Number(value.year), String(Number(value.month) || 1).padStart(2, '0'), String(Number(value.day) || 1).padStart(2, '0')].join('-');
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function angularDifference(a, b) {
  const raw = Math.abs((((Number(a) - Number(b)) % 360) + 360) % 360);
  return Math.min(raw, 360 - raw);
}

function feet(meters) {
  return Number((Number(meters || 0) * 3.280839895).toFixed(1));
}

function localBoxDimensions(bounds, latitude) {
  if (!bounds || !bounds.sw || !bounds.ne) return null;
  const south = Number(bounds.sw.latitude);
  const north = Number(bounds.ne.latitude);
  const west = Number(bounds.sw.longitude);
  const east = Number(bounds.ne.longitude);
  if (![south, north, west, east].every(Number.isFinite)) return null;
  const lat = Number.isFinite(Number(latitude)) ? Number(latitude) : (south + north) / 2;
  const northSouth = Math.abs(north - south) * 111320;
  const eastWest = Math.abs(east - west) * 111320 * Math.max(0.1, Math.cos(lat * Math.PI / 180));
  if (!(northSouth > 0) || !(eastWest > 0)) return null;
  return { northSouth, eastWest, area: northSouth * eastWest };
}

function dominantAzimuthGroups(segments) {
  const buckets = new Map();
  let total = 0;
  segments.forEach((segment) => {
    const area = Number(segment.groundAreaMeters2 || 0);
    if (!(area > 0) || Number(segment.pitchDegrees || 0) < 2) return;
    total += area;
    const bin = (Math.round(Number(segment.azimuthDegrees || 0) / 45) * 45) % 360;
    buckets.set(bin, (buckets.get(bin) || 0) + area);
  });
  return Array.from(buckets.entries())
    .map(([azimuth, area]) => ({ azimuth: Number(azimuth), area, share: total > 0 ? area / total : 0 }))
    .filter((item) => item.share >= 0.08)
    .sort((a, b) => b.area - a.area);
}

function deriveContractorGeometry(insights, roofSegments) {
  const potential = insights && insights.solarPotential || {};
  const buildingStats = potential.buildingStats || {};
  const groundArea = Number(buildingStats.groundAreaMeters2 || 0);
  const buildingBox = localBoxDimensions(insights && insights.boundingBox, insights && insights.center && insights.center.latitude);
  if (!(groundArea > 0) || !buildingBox) {
    return { measurements: {}, segments: [], quality: { roofType: 'unknown', ambiguousSegmentCount: 1 } };
  }

  // Preserve Google's reliable footprint area while using the building bounding box only for aspect ratio.
  // This avoids treating empty corners inside a rotated/irregular bounding box as roof area.
  const rawLong = Math.max(buildingBox.northSouth, buildingBox.eastWest);
  const rawShort = Math.min(buildingBox.northSouth, buildingBox.eastWest);
  const aspect = clamp(rawLong / Math.max(rawShort, 0.01), 1, 6);
  const shortM = Math.sqrt(groundArea / aspect);
  const longM = groundArea / Math.max(shortM, 0.01);
  const pitched = roofSegments.filter((segment) => Number(segment.pitchDegrees || 0) >= 2 && Number(segment.groundAreaMeters2 || 0) > 2);
  const avgPitch = pitched.length
    ? pitched.reduce((sum, segment) => sum + Number(segment.pitchDegrees || 0) * Number(segment.groundAreaMeters2 || 0), 0) /
      Math.max(1, pitched.reduce((sum, segment) => sum + Number(segment.groundAreaMeters2 || 0), 0))
    : 0;
  const pitchRad = avgPitch * Math.PI / 180;
  const groups = dominantAzimuthGroups(pitched);

  let roofType = 'complex';
  if (groups.length === 2 && angularDifference(groups[0].azimuth, groups[1].azimuth) >= 135) roofType = 'gable';
  else if (groups.length >= 3 && groups.length <= 4 && pitched.length <= 5) roofType = 'hip';
  else if (groups.length <= 1 || avgPitch < 2) roofType = 'flat';

  const source = 'Google Solar API roof planes + building footprint';
  const confidence = 'Auto-calculated — confirm';
  const measurements = {};
  function setMeasurement(key, meters, method) {
    if (!Number.isFinite(meters) || meters < 0) return;
    measurements[key] = { value: feet(meters), source, confidence, method };
  }

  if (roofType === 'gable') {
    const halfSpan = shortM / 2;
    const slopeLength = avgPitch > 0 ? halfSpan / Math.max(Math.cos(pitchRad), 0.2) : halfSpan;
    setMeasurement('eaves', 2 * longM, 'Two downhill exterior edges from Google footprint dimensions for a two-plane opposing-azimuth gable roof');
    setMeasurement('rakes', 4 * slopeLength, 'Four gable rake edges derived from footprint half-span and Google roof pitch');
    setMeasurement('ridges', longM, 'Shared high edge of the two dominant opposing Google roof planes');
    setMeasurement('hips', 0, 'Two-plane opposing-azimuth roof classification has no hip planes');
    setMeasurement('valleys', 0, 'Two-plane opposing-azimuth roof classification has no valley intersections');
  } else if (roofType === 'hip') {
    const halfSpan = shortM / 2;
    const ridge = Math.max(0, longM - shortM);
    const rise = Math.tan(pitchRad) * halfSpan;
    const hipLength = Math.sqrt(halfSpan * halfSpan * 2 + rise * rise);
    setMeasurement('eaves', 2 * (longM + shortM), 'Exterior footprint perimeter for a simple multi-azimuth hip roof');
    setMeasurement('rakes', 0, 'Simple hip-roof classification has eaves around the perimeter and no gable rakes');
    setMeasurement('ridges', ridge, 'Rectangular hip-roof ridge derived from Google footprint dimensions');
    setMeasurement('hips', 4 * hipLength, 'Four hip edges derived from Google footprint half-span and weighted roof pitch');
    setMeasurement('valleys', 0, 'Simple hip-roof classification has no concave roof intersections');
  } else if (roofType === 'flat') {
    setMeasurement('eaves', 2 * (longM + shortM), 'Exterior Google building footprint perimeter used as roof edge on a flat/near-flat roof');
    setMeasurement('rakes', 0, 'Flat/near-flat roof classification');
    setMeasurement('ridges', 0, 'Flat/near-flat roof classification');
    setMeasurement('hips', 0, 'Flat/near-flat roof classification');
    setMeasurement('valleys', 0, 'Flat/near-flat roof classification');
  } else {
    // Google does not expose contractor edge polylines. For complex roofs, provide only the
    // exterior perimeter estimate and leave interior edge types unresolved rather than inventing them.
    const bboxFill = clamp(groundArea / Math.max(buildingBox.area, groundArea), 0.35, 1);
    const shapeFactor = clamp(1 + (1 - bboxFill) * 0.35, 1, 1.23);
    setMeasurement('eaves', 2 * (longM + shortM) * shapeFactor, 'Exterior-edge estimate from Google building footprint area/aspect; complex roof requires confirmation');
  }

  return {
    measurements,
    segments: [],
    quality: {
      roofType,
      dominantAzimuthGroupCount: groups.length,
      pitchedSegmentCount: pitched.length,
      averagePitchDegrees: Number(avgPitch.toFixed(1)),
      ambiguousSegmentCount: roofType === 'complex' ? 1 : 0
    }
  };
}

function mapSolarBuildingInsights(insights, dataLayersAvailable) {
  const potential = insights && insights.solarPotential || {};
  const wholeRoof = potential.wholeRoofStats || {};
  const building = potential.buildingStats || {};
  const segments = Array.isArray(potential.roofSegmentStats) ? potential.roofSegmentStats : [];
  const wholeArea = Number(wholeRoof.areaMeters2 || 0);
  const wholeGround = Number(wholeRoof.groundAreaMeters2 || 0);
  const buildingGround = Number(building.groundAreaMeters2 || 0);
  const coverageScale = wholeGround > 0 && buildingGround > wholeGround ? buildingGround / wholeGround : 1;
  const roofAreaMeters2 = wholeArea > 0 ? wholeArea * coverageScale : 0;
  const roofAreaSquareFeet = roofAreaMeters2 > 0 ? Number((roofAreaMeters2 * 10.7639104167).toFixed(1)) : null;
  const footprintSquareFeet = buildingGround > 0 ? Number((buildingGround * 10.7639104167).toFixed(1)) : null;
  const imageryQuality = String(insights && insights.imageryQuality || 'BASE').toUpperCase();
  const sourceConfidence = coverageScale > 1.01 || imageryQuality === 'BASE' ? 'Auto-calculated — confirm' : 'API-derived';
  const roofSegments = segments.map((segment, index) => ({
    id: index + 1,
    pitchDegrees: Number(segment.pitchDegrees || 0),
    azimuthDegrees: Number(segment.azimuthDegrees || 0),
    areaSquareFeet: Number((((segment.stats || {}).areaMeters2 || 0) * 10.7639104167).toFixed(1)),
    groundAreaMeters2: Number((segment.stats || {}).groundAreaMeters2 || 0),
    center: segment.center || null,
    boundingBox: segment.boundingBox || null,
    planeHeightAtCenterMeters: Number(segment.planeHeightAtCenterMeters || 0)
  }));
  const pitchByPlane = roofSegments.length ? roofSegments.map((segment) => {
    const rise = Math.max(0, Math.round(Math.tan(segment.pitchDegrees * Math.PI / 180) * 12));
    return 'Plane ' + segment.id + ': ' + rise + '/12 (' + segment.pitchDegrees.toFixed(1) + '°)';
  }).join('; ') : null;
  const contractorGeometry = deriveContractorGeometry(insights, roofSegments);
  const measurements = Object.assign({}, contractorGeometry.measurements, {
    totalRoofArea: {
      value: roofAreaSquareFeet,
      source: 'Google Solar API buildingInsights',
      confidence: sourceConfidence,
      method: coverageScale > 1.01
        ? 'Whole-roof surface area scaled to the full building footprint using Google buildingStats'
        : 'Google wholeRoofStats roof surface area'
    }
  });
  return {
    available: roofAreaSquareFeet > 0,
    source: 'Google Solar API buildingInsights',
    center: insights && insights.center || null,
    imageryDate: formatGoogleDate(insights && insights.imageryDate),
    imageryQuality,
    dataLayersAvailable: Boolean(dataLayersAvailable),
    roofAreaSquareFeet,
    roofAreaConfidence: sourceConfidence,
    footprintSquareFeet,
    roofSegments,
    pitchByPlane,
    roofGeometry: {
      segments: contractorGeometry.segments,
      measurements,
      quality: contractorGeometry.quality,
      reason: contractorGeometry.quality.roofType === 'complex'
        ? 'Google returned roof planes and footprint data, but the roof is complex; exterior edge length is estimated and interior edge types remain confirmation items.'
        : 'Contractor edge lengths were derived from Google roof-plane orientation, pitch, and building footprint geometry. Review before ordering materials.'
    }
  };
}

module.exports.getSolarRoofData = onRequest({ cors: false, region: 'us-central1', invoker: 'public' }, async (req, res) => {
  permitCors(req, res);
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
  try {
    await requireFirebaseUser(req);
    const latitude = Number(req.body && req.body.latitude);
    const longitude = Number(req.body && req.body.longitude);
    if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90 || !Number.isFinite(longitude) || longitude < -180 || longitude > 180) {
      return res.status(400).json({ error: 'Valid latitude and longitude are required.' });
    }

    const accessToken = await googleCloudAccessToken();
    const billingProject = process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || 'hailmoneymap';
    const googleHeaders = {
      Authorization: 'Bearer ' + accessToken,
      'X-Goog-User-Project': billingProject
    };

    const query = new URLSearchParams({
      'location.latitude': String(latitude),
      'location.longitude': String(longitude),
      requiredQuality: 'BASE'
    });
    const insightsResponse = await fetch('https://solar.googleapis.com/v1/buildingInsights:findClosest?' + query.toString(), { headers: googleHeaders });
    const insights = await insightsResponse.json().catch(() => ({}));
    if (!insightsResponse.ok) {
      const detail = insights && insights.error && insights.error.message || 'Google Solar building insights request failed.';
      return res.status(insightsResponse.status === 404 ? 404 : 502).json({ error: detail });
    }

    let dataLayersAvailable = false;
    try {
      const layerQuery = new URLSearchParams({
        'location.latitude': String(latitude),
        'location.longitude': String(longitude),
        radiusMeters: '25',
        view: 'IMAGERY_LAYERS',
        requiredQuality: 'BASE',
        pixelSizeMeters: '0.5'
      });
      const layersResponse = await fetch('https://solar.googleapis.com/v1/dataLayers:get?' + layerQuery.toString(), { headers: googleHeaders });
      dataLayersAvailable = layersResponse.ok;
    } catch (error) {
      logger.warn('Solar data-layer availability check failed', { message: error && error.message });
    }

    const result = mapSolarBuildingInsights(insights, dataLayersAvailable);
    if (!result.available) return res.status(404).json({ error: 'Google Solar did not return a usable roof area for this building.' });
    return res.status(200).json(result);
  } catch (error) {
    const statusCode = Number(error && error.statusCode || 500);
    logger.error('Solar roof lookup failed', { statusCode, message: error && error.message });
    return res.status(statusCode).json({ error: error && error.message || 'Solar roof lookup failed.' });
  }
});
