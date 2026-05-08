#!/usr/bin/env node
// Ingest wind-gust + tornado LSRs from IEM into storm_lsr_raw.
// Usage:  node ingest_lsr_wind_tornado.mjs [startDate] [endDate]
// Dates YYYY-MM-DD. Defaults to 7-day rolling window.

import crypto from "node:crypto";
import { createClient } from "@supabase/supabase-js";
import { parse } from "csv-parse/sync";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

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
    const { error } = await supabase.from("storm_lsr_raw").upsert(batch, { onConflict: "id" });
    if (error) throw error;
    upserted += batch.length;
    console.log(`[LSR-WT] upserted batch=${batch.length} total=${upserted}`);
  }

  const dates = [...new Set(deduped.map(r => r.event_date))].sort();
  console.log(`[LSR-WT] done. wind=${wc} tornado=${tc} upserted=${upserted} dates=${dates.length}`);
  console.log("[INGEST] recent damaging wind rows upserted", wc);

  const SWATH_RENDER_URL = `${SUPABASE_URL}/functions/v1/swath-render`;
  let swathOk = 0;
  let swathFail = 0;
  for (const d of dates) {
    console.log(`[INGEST] recent swath generation attempted ${d}`);
    try {
      const resp = await fetch(`${SWATH_RENDER_URL}?date=${d}&persist=1`, {
        headers: {
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          apikey: SUPABASE_SERVICE_ROLE_KEY,
        },
      });
      if (resp.ok) {
        const body = await resp.json();
        console.log(`[LSR-WT] swath-render ${d}: persisted=${body.persisted || false} corridors=${(body.corridors || []).length} points=${body.pointCount || 0}`);
        swathOk++;
      } else {
        const raw = await resp.text();
        console.warn(`[LSR-WT] swath-render ${d}: HTTP ${resp.status} ${raw.slice(0, 200)}`);
        swathFail++;
      }
    } catch (err) {
      console.warn(`[LSR-WT] swath-render ${d}: ${err.message}`);
      swathFail++;
    }
  }
  console.log(`[LSR-WT] swath-render complete: ok=${swathOk} fail=${swathFail}`);
}

main().catch(err => { console.error("[LSR-WT] FATAL:", err); process.exit(1); });
