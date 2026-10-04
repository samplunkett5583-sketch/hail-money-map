#!/usr/bin/env node
// Ingest wind-gust + tornado LSRs from IEM into storm_lsr_raw.
// Usage:  node ingest_lsr_wind_tornado.mjs [startDate] [endDate]
// Dates YYYY-MM-DD. Defaults to 7-day rolling window.

import crypto from "node:crypto";
import pg from "pg";
import { parse } from "csv-parse/sync";

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL");
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 3 });

async function upsertRows(table, rows) {
  if (!rows.length) return;
  const columns = Object.keys(rows[0]);
  const params = [];
  const tuples = rows.map((row, rowIndex) => {
    return '(' + columns.map((column, columnIndex) => {
      params.push(column === 'raw' ? JSON.stringify(row[column] || {}) : row[column]);
      return '$' + (rowIndex * columns.length + columnIndex + 1);
    }).join(',') + ')';
  });
  const updates = columns.filter((column) => column !== 'id')
    .map((column) => '"' + column + '"=EXCLUDED."' + column + '"').join(',');
  const sql = 'INSERT INTO public."' + table + '" (' + columns.map((c) => '"' + c + '"').join(',') + ') VALUES ' +
    tuples.join(',') + ' ON CONFLICT ("id") DO UPDATE SET ' + updates;
  await pool.query(sql, params);
}

function ymd(date) { return date.toISOString().slice(0, 10); }

/** SPC convective-day date (12Z–12Z). Subtract 12 h then take UTC date. */
function ymdStormDay(utcDate) {
  return new Date(utcDate.getTime() - 12 * 3600000).toISOString().slice(0, 10);
}

function toZ(date) { return `${ymd(date)}T00:00Z`; }

function parseValid2Utc(v) {
  const m = String(v || "").match(/^(\d{4})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})$/);
  if (!m) return null;
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], 0));
  return Number.isNaN(d.getTime()) ? null : d;
}

function num(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s || /^(none|nan)$/i.test(s)) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function stableId(parts) {
  return crypto.createHash("sha256")
    .update(parts.map(p => (p == null ? "" : String(p))).join("|"))
    .digest("hex");
}

async function fetchCsv(url) {
  const resp = await fetch(url, {
    headers: { Accept: "text/csv,*/*", "User-Agent": "hail-money-map-lsr-ingest/1.0" },
  });
  const text = await resp.text();
  if (!resp.ok) throw new Error(`IEM HTTP ${resp.status}: ${text.slice(0, 300)}`);
  return text;
}

async function main() {
  const args = process.argv.slice(2);
  let startDate, endDate;
  if (args.length >= 2 && /^\d{4}-\d{2}-\d{2}$/.test(args[0]) && /^\d{4}-\d{2}-\d{2}$/.test(args[1])) {
    startDate = new Date(args[0] + "T00:00:00Z");
    endDate   = new Date(args[1] + "T00:00:00Z");
    endDate.setUTCDate(endDate.getUTCDate() + 1);
    console.log(`[LSR-WT] CLI range: ${args[0]} → ${args[1]}`);
  } else {
    const now = new Date();
    endDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    endDate.setUTCDate(endDate.getUTCDate() + 1);
    startDate = new Date(endDate);
    startDate.setUTCDate(startDate.getUTCDate() - 7);
  }

  console.log("[INGEST] recent NOAA window", startDate.toISOString().slice(0, 10), "->", new Date(endDate.getTime() - 1).toISOString().slice(0, 10));

  const sts = toZ(startDate), ets = toZ(endDate);
  const base = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/lsr.py";
  const windUrl    = `${base}?sts=${encodeURIComponent(sts)}&ets=${encodeURIComponent(ets)}&type=${encodeURIComponent("TSTM WND GST")}&fmt=csv&justcsv=1`;
  const tornadoUrl = `${base}?sts=${encodeURIComponent(sts)}&ets=${encodeURIComponent(ets)}&type=TORNADO&fmt=csv&justcsv=1`;

  console.log(`[LSR-WT] Fetching wind+tornado LSRs: ${sts} → ${ets}`);
  const [windCsv, tornadoCsv] = await Promise.all([fetchCsv(windUrl), fetchCsv(tornadoUrl)]);

  const csvOpts = { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true, trim: true };
  const windRecs    = parse(windCsv, csvOpts);
  const tornadoRecs = parse(tornadoCsv, csvOpts);
  console.log(`[LSR-WT] fetched wind=${windRecs.length} tornado=${tornadoRecs.length}`);

  const rows = [];

  // ── Wind gust reports (MAG = mph from IEM) ──
  for (const r of windRecs) {
    const lat = num(r?.LAT), lon = num(r?.LON);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const et = parseValid2Utc(r?.VALID2);
    if (!et) continue;
    const mag = num(r?.MAG);
    if (mag == null || mag < 58) continue;
    rows.push({
      id: stableId(["wind", et.toISOString(), lat, lon, mag, r?.STATE, r?.COUNTY, r?.WFO, r?.SOURCE]),
      event_time: et.toISOString(),
      event_date: ymdStormDay(et),
      event_type: "wind",
      lat, lon,
      magnitude: Math.round(mag),
      magnitude_unit: "mph",
      state: r?.STATE?.trim() || null,
      county: r?.COUNTY?.trim() || null,
      source: "LSR",
      raw: r,
    });
  }

  // ── Tornado reports (MAG = EF scale 0-5, may be absent) ──
  for (const r of tornadoRecs) {
    const lat = num(r?.LAT), lon = num(r?.LON);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    const et = parseValid2Utc(r?.VALID2);
    if (!et) continue;
    const mag = num(r?.MAG);
    const ef = (mag != null && mag >= 0 && mag <= 5) ? Math.round(mag) : 0;
    rows.push({
      id: stableId(["tornado", et.toISOString(), lat, lon, ef, r?.STATE, r?.COUNTY, r?.WFO, r?.SOURCE]),
      event_time: et.toISOString(),
      event_date: ymdStormDay(et),
      event_type: "tornado",
      lat, lon,
      magnitude: ef,
      magnitude_unit: "ef",
      state: r?.STATE?.trim() || null,
      county: r?.COUNTY?.trim() || null,
      source: "LSR",
      raw: r,
    });
  }

  // Dedup
  const byId = new Map();
  for (const row of rows) byId.set(row.id, row);
  const deduped = Array.from(byId.values());
  const wc = deduped.filter(r => r.event_type === "wind").length;
  const tc = deduped.filter(r => r.event_type === "tornado").length;
  console.log(`[LSR-WT] parsed wind=${wc} tornado=${tc} total=${deduped.length}`);

  // Upsert
  const BATCH = 1000;
  let upserted = 0;
  for (let i = 0; i < deduped.length; i += BATCH) {
    const batch = deduped.slice(i, i + BATCH);
    await upsertRows("storm_lsr_raw", batch);
    upserted += batch.length;
    console.log(`[LSR-WT] upserted batch=${batch.length} total=${upserted}`);
  }

  const dates = [...new Set(deduped.map(r => r.event_date))].sort();
  console.log(`[LSR-WT] done. wind=${wc} tornado=${tc} upserted=${upserted} dates=${dates.length}`);
  console.log("[INGEST] recent damaging wind rows upserted", wc);

  console.log(`[LSR-WT] Neon ingest complete for ${dates.length} storm date(s): ${dates.join(", ")}`);
  await pool.end();
}

main().catch(err => { console.error("[LSR-WT] FATAL:", err); process.exit(1); });
