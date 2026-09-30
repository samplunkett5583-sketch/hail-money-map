'use strict';
const { onRequest } = require('firebase-functions/v2/https');
const { defineSecret } = require('firebase-functions/params');
const admin = require('firebase-admin');
const logger = require('firebase-functions/logger');
admin.initializeApp();
const DRONE_OPENAI_API_KEY = defineSecret('OPENAI_API_KEY');

function permitCors(req,res){
  const origin=String(req.get('origin')||'');
  const allowed=/^https:\/\/(hailmoneymap\.web\.app|hailmoneymap\.firebaseapp\.com|hail\.money|www\.hail\.money)$/i.test(origin)||/^http:\/\/(127\.0\.0\.1|localhost):\d+$/i.test(origin);
  if(allowed)res.set('Access-Control-Allow-Origin',origin);
  res.set('Vary','Origin');res.set('Access-Control-Allow-Headers','Authorization, Content-Type');res.set('Access-Control-Allow-Methods','POST, OPTIONS');
}
async function requireFirebaseUser(req){const h=String(req.get('authorization')||'');const m=h.match(/^Bearer\s+(.+)$/i);if(!m)throw Object.assign(new Error('Sign in is required.'),{statusCode:401});return admin.auth().verifyIdToken(m[1]);}

function droneOutputText(response) {
  if (response && typeof response.output_text === 'string') return response.output_text;
  const output = response && Array.isArray(response.output) ? response.output : [];
  for (const item of output) {
    if (!item || !Array.isArray(item.content)) continue;
    for (const content of item.content) {
      if (content && typeof content.text === 'string') return content.text;
    }
  }
  return '';
}

const DRONE_MEASUREMENT_KEYS = {
  roof: ['roofArea','squares','eaves','rakes','ridges','hips','valleys','starter','dripEdge','pitch'],
  siding: ['wallArea','gables','starterTrack','corners','jChannel','soffit','fascia','doorOpenings','windowOpenings'],
  gutters: ['gutterTotal','downspouts','elbows']
};

module.exports.analyzeDroneEstimate = onRequest(
  { secrets: [DRONE_OPENAI_API_KEY], timeoutSeconds: 180, memory: '1GiB', cors: false, invoker: 'public' },
  async (req, res) => {
    permitCors(req, res);
    if (req.method === 'OPTIONS') return res.status(204).send('');
    if (req.method !== 'POST') return res.status(405).json({ error: 'POST required.' });
    try {
      const user = await requireFirebaseUser(req);
      const body = req.body && typeof req.body === 'object' ? req.body : {};
      const phase = String(body.phase || 'initial').slice(0, 30);
      const propertyType = /commercial/i.test(String(body.propertyType || '')) ? 'commercial' : 'residential';
      const scopes = Array.isArray(body.scopes) ? body.scopes.filter((item) => DRONE_MEASUREMENT_KEYS[item]) : [];
      const answers = body.answers && typeof body.answers === 'object' ? body.answers : {};
      const photos = Array.isArray(body.photos) ? body.photos.slice(0, 12) : [];
      if (!scopes.length) return res.status(400).json({ error: 'Choose roof, siding, gutters, or a combination.' });
      if (!photos.length) return res.status(400).json({ error: 'Upload at least one drone photo.' });
      const safePhotos = photos.filter((photo) => photo && /^data:image\/(jpeg|jpg|png|webp);base64,/i.test(String(photo.dataUrl || '')));
      if (!safePhotos.length) return res.status(400).json({ error: 'No supported drone photos were received.' });

      const requested = scopes.map((scope) => scope + ': ' + DRONE_MEASUREMENT_KEYS[scope].join(', ')).join('\n');
      const prompt = [
        'You are Hail Money Drone Estimator, a roofing and exterior-restoration measurement assistant.',
        'Analyze only the user-supplied drone imagery and the user answers. Do not use web imagery or invent dimensions.',
        'Property type: ' + propertyType + '. Selected scopes: ' + scopes.join(', ') + '. Phase: ' + phase + '.',
        'Requested measurement keys by selected scope:\n' + requested,
        'Existing user answers: ' + JSON.stringify(answers),
        'Absolute length, area, pitch, and quantity measurements must be supported by visible geometry plus a defensible scale, calibration, or user-provided known dimension.',
        'Never derive exact feet, square feet, or squares from pixels alone. If scale is insufficient, ask the smallest number of questions needed to establish it.',
        'If an elevation or roof plane is missing, request the specific additional drone view instead of guessing.',
        'Counts such as windows, doors, downspouts, and elbows may be reported only when clearly visible; otherwise ask for the missing view or confirmation.',
        'For roof scope, return roof area, squares, eaves, rakes, ridges, hips, valleys, starter, drip edge, and pitch when supported.',
        'For siding scope, return wall area, gables, starter track, corners, J-channel, soffit, fascia, door openings, and window openings when supported.',
        'For gutter scope, return gutter total length, downspout count, and elbow count when supported.',
        'Questions should be contractor-friendly and concise. A calibration question may ask for one or more verified reference dimensions and exactly where they appear.',
        'If all requested measurements can be supported, status must be measurements. If user answers are still needed, status must be questions. If specific additional imagery is needed, status must be needs_more_photos.',
        'Every returned measurement must include its trade, canonical key, unit, confidence, source, method, and whether human verification is required.'
      ].join('\n');

      const content = [{ type: 'input_text', text: prompt }];
      safePhotos.forEach((photo, index) => {
        content.push({ type: 'input_text', text: 'Drone photo ' + (index + 1) + ': ' + String(photo.name || 'image') });
        content.push({ type: 'input_image', image_url: String(photo.dataUrl) });
      });
      const openaiResponse = await fetch('https://api.openai.com/v1/responses', {
        method: 'POST',
        headers: { Authorization: 'Bearer ' + DRONE_OPENAI_API_KEY.value(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          model: 'gpt-5.6-luna', reasoning: { effort: 'medium' },
          input: [{ role: 'user', content }], max_output_tokens: 7000,
          text: { format: { type: 'json_schema', name: 'drone_estimate_analysis', strict: true, schema: {
            type: 'object', additionalProperties: false,
            properties: {
              status: { type: 'string', enum: ['questions','needs_more_photos','measurements'] },
              summary: { type: 'string' },
              questions: { type: 'array', items: { type: 'object', additionalProperties: false,
                properties: {
                  id: { type: 'string' }, text: { type: 'string' },
                  type: { type: 'string', enum: ['single','number','text'] },
                  options: { type: 'array', items: { type: 'string' } },
                  unit: { type: 'string' }, required: { type: 'boolean' }
                }, required: ['id','text','type','options','unit','required']
              }},
              photoRequests: { type: 'array', items: { type: 'string' } },
              measurements: { type: 'array', items: { type: 'object', additionalProperties: false,
                properties: {
                  trade: { type: 'string', enum: ['roof','siding','gutters'] },
                  key: { type: 'string' }, label: { type: 'string' }, value: { type: 'number' },
                  unit: { type: 'string' }, confidence: { type: 'string' }, source: { type: 'string' },
                  method: { type: 'string' }, requiresVerification: { type: 'boolean' }
                }, required: ['trade','key','label','value','unit','confidence','source','method','requiresVerification']
              }},
              assumptions: { type: 'array', items: { type: 'string' } }
            },
            required: ['status','summary','questions','photoRequests','measurements','assumptions']
          }}}
        })
      });
      const responseBody = await openaiResponse.json().catch(() => ({}));
      if (!openaiResponse.ok) {
        const providerError = responseBody && responseBody.error || {};
        logger.error('Drone estimator provider request rejected', {
          providerStatus: openaiResponse.status,
          providerCode: String(providerError.code || '').slice(0, 80),
          providerType: String(providerError.type || '').slice(0, 80),
          providerMessage: String(providerError.message || '').slice(0, 240),
          model: 'gpt-5.6-luna'
        });
        const creditExhausted = String(providerError.code || '') === 'credit_balance_exhausted';
        const statusCode = creditExhausted ? 503 : (openaiResponse.status === 429 ? 429 : 502);
        const publicMessage = creditExhausted
          ? 'Hail Money AI photo analysis is temporarily unavailable because the API credit balance is exhausted. An administrator must add API credits, then retry.'
          : (openaiResponse.status === 429
            ? 'Drone photo analysis is busy. Retry shortly.'
            : 'Drone photo analysis is temporarily unavailable. Retry.');
        throw Object.assign(new Error(publicMessage), { statusCode });
      }
      const outputText = droneOutputText(responseBody);
      if (!outputText) throw Object.assign(new Error('Drone estimator returned no analysis.'), { statusCode: 502 });
      let analysis;
      try { analysis = JSON.parse(outputText); }
      catch (_) { throw Object.assign(new Error('Drone estimator returned an unreadable result.'), { statusCode: 502 }); }
      analysis.measurements = (analysis.measurements || []).filter((item) =>
        scopes.includes(item.trade) && DRONE_MEASUREMENT_KEYS[item.trade].includes(item.key) && Number.isFinite(Number(item.value))
      );
      logger.info('Drone estimate analysis completed', { uid: user.uid, scopes, phase, status: analysis.status, photos: safePhotos.length, model: 'gpt-5.6-luna' });
      return res.status(200).json({ analysis, usage: responseBody.usage || null });
    } catch (error) {
      const status = Number(error && error.statusCode) || 500;
      logger.error('Drone estimate analysis failed', { status, message: error && error.message });
      return res.status(status).json({ error: error && error.message || 'Drone estimate analysis failed.' });
    }
  }
);
