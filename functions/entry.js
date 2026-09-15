'use strict';

const original = require('./index');
const { onRequest } = require('firebase-functions/v2/https');
const admin = require('firebase-admin');
const logger = require('firebase-functions/logger');

module.exports = original;

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
    center: segment.center || null,
    boundingBox: segment.boundingBox || null
  }));
  const pitchByPlane = roofSegments.length ? roofSegments.map((segment) => {
    const rise = Math.max(0, Math.round(Math.tan(segment.pitchDegrees * Math.PI / 180) * 12));
    return 'Plane ' + segment.id + ': ' + rise + '/12 (' + segment.pitchDegrees.toFixed(1) + '°)';
  }).join('; ') : null;
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
      segments: [],
      measurements: {
        totalRoofArea: {
          value: roofAreaSquareFeet,
          source: 'Google Solar API buildingInsights',
          confidence: sourceConfidence,
          method: coverageScale > 1.01
            ? 'Whole-roof surface area scaled to the full building footprint using Google buildingStats'
            : 'Google wholeRoofStats roof surface area'
        }
      },
      reason: 'Google Solar building insights supplies roof area and roof-plane pitch, but not contractor edge lengths.'
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
