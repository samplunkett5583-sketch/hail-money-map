'use strict';

const http = require('http');
const { URL } = require('url');

const PORT = process.env.PORT || 8080;
const FETCH_TIMEOUT_MS = 25000;
const MAX_DATESONLY_DAYS = 31;
const DEFAULT_LIMIT = '20000';
const JSON_HEADERS = { 'Content-Type': 'application/json; charset=utf-8' };

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Expose-Headers', 'X-Upstream-Url,X-Upstream-Status,X-Upstream-Content-Type,X-Chunks-Used');
  res.setHeader('Vary', 'Origin');
}

function sendJson(res, status, body, extraHeaders = {}) {
  const headers = { ...JSON_HEADERS, ...extraHeaders };
  res.writeHead(status, headers);
  res.end(JSON.stringify(body));
}

function parseYyyyMmDd(str) {
  const year = Number(str.slice(0, 4));
  const month = Number(str.slice(4, 6)) - 1;
  const day = Number(str.slice(6, 8));
  return new Date(Date.UTC(year, month, day));
}

function addDaysUTC(date, days) {
  const d = new Date(date);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function formatDateIso(date) {
  return date.toISOString().slice(0, 10);
}

function formatDateCompact(date) {
  return formatDateIso(date).replace(/-/g, '');
}

function parseRowProps(row) {
  if (typeof row === 'string') {
    const props = {};
    const kvRegex = /"([^"]+)":"([^"]*)"/g;
    let match;
    while ((match = kvRegex.exec(row)) !== null) {
      props[match[1]] = match[2];
    }
    return props;
  }
  if (row && typeof row === 'object') {
    if (row.properties && typeof row.properties === 'object') {
      return { ...row.properties, properties: row.properties };
    }
    return row;
  }
  return null;
}

function pickDate(props) {
  if (!props || typeof props !== 'object') return null;

  const candidate = typeof props.properties?.valid === 'string'
    ? props.properties.valid
    : typeof props.valid === 'string'
      ? props.valid
      : typeof props.VALID === 'string'
        ? props.VALID
        : typeof props.ZTIME === 'string'
          ? props.ZTIME
          : typeof props.TIME === 'string'
            ? props.TIME
            : null;

  if (typeof candidate !== 'string') return null;
  const match = candidate.match(/\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : null;
}

function extractDatesOnly(data) {
  const result = Array.isArray(data?.result) ? data.result : [];
  const dateChunk = new Set();

  for (const row of result) {
    const props = parseRowProps(row);
    const dateStr = pickDate(props);
    if (dateStr) {
      dateChunk.add(dateStr);
    }
  }

  return { dateChunk, rawCount: result.length };
}

function parseNoaaResponse(data) {
  const result = Array.isArray(data?.result) ? data.result : [];
  const stormChunk = [];
  const dateChunk = new Set();

  for (const row of result) {
    const props = parseRowProps(row);
    if (!props) continue;

    let lon = Number(props.LON || props.LONGITUDE || props.LONG || props.lon);
    let lat = Number(props.LAT || props.LATITUDE || props.lat);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      const shapeMatch = (props.SHAPE || '').match(/POINT\s*\(\s*([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\s*\)/);
      if (!shapeMatch) continue;
      lon = Number(shapeMatch[1]);
      lat = Number(shapeMatch[2]);
    }

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const ztime = props.ZTIME || props.TIME || null;
    const dateOnly = typeof ztime === 'string' && ztime.length >= 10 ? ztime.slice(0, 10) : null;
    const hailSizeRaw = props.MAXSIZE ?? props.MAGNITUDE;
    const hailSize = hailSizeRaw !== undefined ? parseFloat(hailSizeRaw) : null;

    if (dateOnly) dateChunk.add(dateOnly);

    stormChunk.push({
      lat,
      lon,
      date: dateOnly,
      ztime,
      hailSize: Number.isFinite(hailSize) ? hailSize : null,
      props
    });
  }

  return { stormChunk, dateChunk, rawCount: result.length };
}

async function fetchWithTimeout(url, timeoutMs = FETCH_TIMEOUT_MS, accept = 'application/json,*/*') {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { headers: { Accept: accept }, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

function buildUpstreamUrl(product, chunkStartStr, chunkEndStr, bbox, limit, eventTypeParam, token) {
  return `https://www.ncdc.noaa.gov/swdiws/json/${product}/${chunkStartStr}:${chunkEndStr}?bbox=${encodeURIComponent(bbox)}&limit=${limit}${eventTypeParam}&token=${encodeURIComponent(token)}`;
}

function buildPlsrUrl(chunkStartStr, chunkEndStr, bbox, limit, token) {
  return `https://www.ncdc.noaa.gov/swdiws/csv/plsr/${chunkStartStr}:${chunkEndStr}?bbox=${encodeURIComponent(bbox)}&limit=${limit}&token=${encodeURIComponent(token)}`;
}

function parseCsvPlsrDatesOnly(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return { dateChunk: new Set(), rawCount: 0 };

  const headerLine = lines[0];
  const headers = headerLine.split(',').map(h => h.trim());
  const validIdx = headers.indexOf('VALID');
  const ztimeIdx = headers.indexOf('ZTIME');
  const timeIdx = headers.indexOf('TIME');
  
  const dateChunk = new Set();
  let rowCount = 0;

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    rowCount++;
    
    const values = line.split(',').map(v => v.trim());
    let dateStr = null;
    
    if (validIdx >= 0 && values[validIdx]) {
      dateStr = values[validIdx].slice(0, 10);
    } else if (ztimeIdx >= 0 && values[ztimeIdx]) {
      dateStr = values[ztimeIdx].slice(0, 10);
    } else if (timeIdx >= 0 && values[timeIdx]) {
      dateStr = values[timeIdx].slice(0, 10);
    }
    
    if (dateStr && /^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      dateChunk.add(dateStr);
    }
  }

  return { dateChunk, rawCount: rowCount };
}

function parseCsvPlsrStorms(csvText) {
  const lines = csvText.trim().split('\n');
  if (lines.length < 2) return { stormChunk: [], dateChunk: new Set(), rawCount: 0 };

  const headerLine = lines[0];
  const headers = headerLine.split(',').map(h => h.trim());
  
  const typeTextIdx = headers.indexOf('TYPETEXT');
  const typeCodeIdx = headers.indexOf('TYPE');
  const validIdx = headers.indexOf('VALID');
  const ztimeIdx = headers.indexOf('ZTIME');
  const timeIdx = headers.indexOf('TIME');
  const shapeIdx = headers.indexOf('SHAPE');
  const lonIdx = headers.indexOf('LON');
  const latIdx = headers.indexOf('LAT');
  const maxSizeIdx = headers.indexOf('MAXSIZE');
  const magnitudeIdx = headers.indexOf('MAGNITUDE');

  const stormChunk = [];
  const dateChunk = new Set();
  let rowCount = 0;

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    rowCount++;

    const values = line.split(',').map(v => v.trim());
    
    // Filter to hail only
    const typeText = typeTextIdx >= 0 ? values[typeTextIdx] : null;
    const typeCode = typeCodeIdx >= 0 ? values[typeCodeIdx] : null;
    
    if (typeText !== 'Hail' && typeCode !== 'H') continue;

    let lon = null;
    let lat = null;
    
    if (lonIdx >= 0 && latIdx >= 0 && values[lonIdx] && values[latIdx]) {
      lon = Number(values[lonIdx]);
      lat = Number(values[latIdx]);
    }
    
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      if (shapeIdx >= 0 && values[shapeIdx]) {
        const shapeMatch = values[shapeIdx].match(/POINT\s*\(\s*([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\s*\)/);
        if (shapeMatch) {
          lon = Number(shapeMatch[1]);
          lat = Number(shapeMatch[2]);
        }
      }
    }
    
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    let dateStr = null;
    let ztime = null;
    
    if (validIdx >= 0 && values[validIdx]) {
      ztime = values[validIdx];
      dateStr = ztime.slice(0, 10);
    } else if (ztimeIdx >= 0 && values[ztimeIdx]) {
      ztime = values[ztimeIdx];
      dateStr = ztime.slice(0, 10);
    } else if (timeIdx >= 0 && values[timeIdx]) {
      ztime = values[timeIdx];
      dateStr = ztime.slice(0, 10);
    }
    
    let hailSize = null;
    if (maxSizeIdx >= 0 && values[maxSizeIdx]) {
      hailSize = parseFloat(values[maxSizeIdx]);
    } else if (magnitudeIdx >= 0 && values[magnitudeIdx]) {
      hailSize = parseFloat(values[magnitudeIdx]);
    }

    if (dateStr) dateChunk.add(dateStr);

    stormChunk.push({
      lat,
      lon,
      date: dateStr,
      ztime,
      hailSize: Number.isFinite(hailSize) ? hailSize : null,
      props: Object.fromEntries(headers.map((h, idx) => [h, values[idx] || '']))
    });
  }

  return { stormChunk, dateChunk, rawCount: rowCount };
}

const server = http.createServer(async (req, res) => {
  setCors(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  if (req.method !== 'GET') {
    return sendJson(res, 405, { error: 'Method not allowed' });
  }

  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (urlObj.pathname === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('ok');
  }

  if (urlObj.pathname !== '/noaaPlsrProxy') {
    return sendJson(res, 404, { error: 'Not found' });
  }

  const qp = urlObj.searchParams;
  const start = (qp.get('start') || '').trim();
  const end = (qp.get('end') || '').trim();
  const bbox = (qp.get('bbox') || '').trim();
  const limit = (qp.get('limit') || DEFAULT_LIMIT).trim();
  const mode = (qp.get('mode') || 'radar').trim().toLowerCase();
  const isReports = mode === 'reports';
  const product = isReports ? 'plsr' : (qp.get('product') || 'nx3hail_all').trim();
  const datesOnly = qp.get('datesOnly') === '1';
  const debugEnabled = qp.get('debug') === '1';

  if (!/^\d{8}$/.test(start) || !/^\d{8}$/.test(end) || !bbox) {
    return sendJson(res, 400, { error: 'Invalid params. Use start=YYYYMMDD&end=YYYYMMDD&bbox=minLon,minLat,maxLon,maxLat' });
  }

  if (!isReports && product !== 'nx3hail' && product !== 'nx3hail_all') {
    return sendJson(res, 400, { error: 'Invalid product. Use nx3hail or nx3hail_all' });
  }

  if (datesOnly && !isReports) {
    return sendJson(res, 400, { error: 'datesOnly=1 requires mode=reports' });
  }

  const tokenRaw = process.env.NOAA_TOKEN;
  const token = (typeof tokenRaw === 'string' ? tokenRaw : String(tokenRaw || '')).trim();
  if (!token) {
    return sendJson(res, 500, { error: 'NOAA_TOKEN missing/empty' });
  }

  const startDate = parseYyyyMmDd(start);
  const endDate = parseYyyyMmDd(end);
  const daysDiff = (endDate - startDate) / (1000 * 60 * 60 * 24);

  const chunkThreshold = datesOnly ? MAX_DATESONLY_DAYS : 3;
  const needChunking = daysDiff > chunkThreshold;
  const eventTypeParam = isReports ? '' : '&eventType=Hail';

  const dateSet = new Set();
  const storms = [];
  let chunksUsed = 0;
  let totalUpstreamRows = 0;

  const debugInfo = debugEnabled ? { params: { start, end, bbox, mode, product }, chunks: [] } : null;

  const recordDebug = (chunkEntry, upstreamUrl, resp, bodyText) => {
    if (!debugInfo) return;
    debugInfo.upstreamUrl = upstreamUrl;
    debugInfo.upstreamStatus = resp ? resp.status : null;
    debugInfo.upstreamContentType = resp?.headers?.get?.('content-type') || null;
    debugInfo.upstreamBytes = typeof bodyText === 'string' ? bodyText.slice(0, 400) : null;
    if (chunkEntry) {
      chunkEntry.status = resp ? resp.status : null;
      chunkEntry.contentType = resp?.headers?.get?.('content-type') || null;
    }
  };

  const handleChunk = async (chunkStartDate, chunkEndDate) => {
    const chunkStartStr = formatDateCompact(chunkStartDate);
    const chunkEndStr = formatDateCompact(chunkEndDate);
    const upstreamUrl = isReports
      ? buildPlsrUrl(chunkStartStr, chunkEndStr, bbox, limit, token)
      : buildUpstreamUrl(product, chunkStartStr, chunkEndStr, bbox, limit, eventTypeParam, token);

    if (isReports && upstreamUrl.includes('/json/')) {
      return sendJson(res, 500, { error: 'reports mode must use CSV PLSR endpoint' });
    }
    const chunkEntry = debugInfo ? { start: chunkStartStr, end: chunkEndStr } : null;
    if (chunkEntry) debugInfo.chunks.push(chunkEntry);
    if (debugEnabled) console.log(`noaaPlsrProxy chunk ${debugInfo.chunks.length}: ${chunkStartStr}:${chunkEndStr}`);

    try {
      const resp = await fetchWithTimeout(upstreamUrl, FETCH_TIMEOUT_MS, isReports ? 'text/csv,*/*' : 'application/json,*/*');
      const text = await resp.text();
      recordDebug(chunkEntry, upstreamUrl, resp, text);

      if (!resp.ok) {
        if (datesOnly) return { skipped: true };
        res.setHeader('X-Upstream-Url', upstreamUrl);
        res.setHeader('X-Upstream-Status', String(resp.status));
        res.setHeader('X-Upstream-Content-Type', resp.headers.get('content-type') || 'unknown');
        sendJson(res, resp.status, { error: 'Upstream NOAA error', status: resp.status });
        return { abort: true };
      }

      if (isReports) {
        // Parse CSV for PLSR endpoint
        if (datesOnly) {
          const { dateChunk, rawCount } = parseCsvPlsrDatesOnly(text);
          totalUpstreamRows += rawCount;
          dateChunk.forEach((d) => dateSet.add(d));
        } else {
          const { stormChunk, dateChunk, rawCount } = parseCsvPlsrStorms(text);
          totalUpstreamRows += rawCount;
          storms.push(...stormChunk);
          dateChunk.forEach((d) => dateSet.add(d));
        }
      } else {
        // Parse JSON for radar/hail endpoints
        let data;
        try {
          data = JSON.parse(text);
        } catch (parseErr) {
          if (datesOnly) return { skipped: true };
          res.setHeader('X-Upstream-Url', upstreamUrl);
          sendJson(res, 502, { error: 'JSON parse error', sample: text.slice(0, 500) });
          return { abort: true };
        }

        if (datesOnly) {
          const { dateChunk, rawCount } = extractDatesOnly(data);
          totalUpstreamRows += rawCount;
          dateChunk.forEach((d) => dateSet.add(d));
        } else {
          const { stormChunk, dateChunk, rawCount } = parseNoaaResponse(data);
          totalUpstreamRows += rawCount;
          storms.push(...stormChunk);
          dateChunk.forEach((d) => dateSet.add(d));
        }
      }
    } catch (err) {
      if (err.name === 'AbortError') {
        if (chunkEntry) chunkEntry.timeout = true;
        if (datesOnly) return { skipped: true };
        res.setHeader('X-Upstream-Url', upstreamUrl);
        sendJson(res, 504, { error: 'Upstream NOAA timeout' });
        return { abort: true };
      }
      throw err;
    }

    chunksUsed += 1;
    return { ok: true };
  };

  try {
    if (needChunking) {
      const chunkSizeDays = datesOnly ? MAX_DATESONLY_DAYS : 3;
      let currentDate = new Date(startDate);

      while (currentDate <= endDate) {
        const chunkEndCandidate = datesOnly ? addDaysUTC(currentDate, chunkSizeDays - 1) : addDaysUTC(currentDate, chunkSizeDays);
        const actualEnd = chunkEndCandidate > endDate ? endDate : chunkEndCandidate;

        const chunkResult = await handleChunk(currentDate, actualEnd);
        if (chunkResult?.abort) return;

        currentDate = addDaysUTC(actualEnd, 1);
      }
    } else {
      const chunkResult = await handleChunk(startDate, endDate);
      if (chunkResult?.abort) return;
    }
  } catch (err) {
    console.error('noaaPlsrProxy error:', err);
    return sendJson(res, 500, { error: err && err.message ? err.message : String(err) });
  }

  const availableDates = Array.from(dateSet).sort((a, b) => a.localeCompare(b));

  if (datesOnly) {
    const body = { availableDates };
    if (debugInfo) {
      body.debug = { ...debugInfo, chunksCount: debugInfo.chunks.length };
    }
    return sendJson(res, 200, body, { 'Cache-Control': 'no-store' });
  }

  const response = { storms, availableDates, summary: null };
  res.setHeader('X-Chunks-Used', String(chunksUsed));
  res.setHeader('Cache-Control', 'no-store');
  sendJson(res, 200, response);
});

server.listen(PORT, () => {
  console.log(`noaaPlsrProxy listening on port ${PORT}`);
});
