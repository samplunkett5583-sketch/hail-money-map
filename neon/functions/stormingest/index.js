import crypto from "node:crypto";
import pg from "pg";
import { parse } from "csv-parse/sync";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 3 });

function ymd(date) { return date.toISOString().slice(0, 10); }
function stormDay(date) { return new Date(date.getTime() - 12 * 3600000).toISOString().slice(0, 10); }
function parseTime(value) {
  const m = String(value || "").match(/^(\d{4})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})$/);
  if (!m) return null;
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], 0));
  return Number.isNaN(d.getTime()) ? null : d;
}
function num(value) {
  const s = String(value == null ? "" : value).trim();
  if (!s || /^(none|nan)$/i.test(s)) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}
function stableId(parts) {
  return crypto.createHash("sha256").update(parts.map(v => v == null ? "" : String(v)).join("|")).digest("hex");
}
async function fetchCsv(type, start, end) {
  const base = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/lsr.py";
  const url = base + "?sts=" + encodeURIComponent(start) + "&ets=" + encodeURIComponent(end) +
    "&type=" + encodeURIComponent(type) + "&fmt=csv&justcsv=1";
  const response = await fetch(url, { headers: { Accept: "text/csv,*/*", "User-Agent": "hail-money-neon-ingest/1.0" } });
  const text = await response.text();
  if (!response.ok) throw new Error("IEM " + type + " failed (" + response.status + "): " + text.slice(0, 200));
  return parse(text, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true, trim: true });
}
async function upsert(table, rows) {
  if (!rows.length) return 0;
  const chunkSize = 500;
  for (let offset = 0; offset < rows.length; offset += chunkSize) {
    const batch = rows.slice(offset, offset + chunkSize);
    const cols = Object.keys(batch[0]);
    const values = [];
    const tuples = batch.map((row, rowIndex) => "(" + cols.map((col, colIndex) => {
      values.push(col === "raw" ? JSON.stringify(row[col] || {}) : row[col]);
      return "$" + (rowIndex * cols.length + colIndex + 1);
    }).join(",") + ")");
    const updates = cols.filter(c => c !== "id").map(c => '"' + c + '"=EXCLUDED."' + c + '"').join(",");
    await pool.query(
      'INSERT INTO public."' + table + '" (' + cols.map(c => '"' + c + '"').join(",") + ") VALUES " +
      tuples.join(",") + ' ON CONFLICT ("id") DO UPDATE SET ' + updates,
      values
    );
  }
  return rows.length;
}

export default async function handler() {
  try {
    const now = new Date();
    const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1));
    const start = new Date(end);
    start.setUTCDate(start.getUTCDate() - 4);
    const sts = ymd(start) + "T00:00Z";
    const ets = ymd(end) + "T00:00Z";

    const [hailRecords, windRecords, tornadoRecords] = await Promise.all([
      fetchCsv("HAIL", sts, ets),
      fetchCsv("TSTM WND GST", sts, ets),
      fetchCsv("TORNADO", sts, ets)
    ]);

    const hailRows = [];
    for (const r of hailRecords) {
      const lat = num(r.LAT), lon = num(r.LON), time = parseTime(r.VALID2);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || !time) continue;
      const hail = num(r.MAG);
      hailRows.push({
        id: stableId([time.toISOString(), lat, lon, hail, r.STATE, r.COUNTY, r.CITY, r.WFO, r.SOURCE, r.REMARK]),
        event_time: time.toISOString(), event_date: stormDay(time), lat, lon, hail_in: hail,
        state: String(r.STATE || "").trim() || null, county: String(r.COUNTY || "").trim() || null,
        source: "LSR", raw: r
      });
    }

    const stormRows = [];
    for (const r of windRecords) {
      const lat = num(r.LAT), lon = num(r.LON), time = parseTime(r.VALID2), magnitude = num(r.MAG);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || !time || magnitude == null || magnitude < 58) continue;
      stormRows.push({
        id: stableId(["wind", time.toISOString(), lat, lon, magnitude, r.STATE, r.COUNTY, r.WFO, r.SOURCE]),
        event_time: time.toISOString(), event_date: stormDay(time), event_type: "wind", lat, lon,
        magnitude: Math.round(magnitude), magnitude_unit: "mph",
        state: String(r.STATE || "").trim() || null, county: String(r.COUNTY || "").trim() || null,
        source: "LSR", raw: r
      });
    }
    for (const r of tornadoRecords) {
      const lat = num(r.LAT), lon = num(r.LON), time = parseTime(r.VALID2);
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || !time) continue;
      const magnitude = num(r.MAG);
      const ef = magnitude != null && magnitude >= 0 && magnitude <= 5 ? Math.round(magnitude) : 0;
      stormRows.push({
        id: stableId(["tornado", time.toISOString(), lat, lon, ef, r.STATE, r.COUNTY, r.WFO, r.SOURCE]),
        event_time: time.toISOString(), event_date: stormDay(time), event_type: "tornado", lat, lon,
        magnitude: ef, magnitude_unit: "ef",
        state: String(r.STATE || "").trim() || null, county: String(r.COUNTY || "").trim() || null,
        source: "LSR", raw: r
      });
    }

    const uniqueHail = [...new Map(hailRows.map(row => [row.id, row])).values()];
    const uniqueStorm = [...new Map(stormRows.map(row => [row.id, row])).values()];
    await upsert("hail_lsr_raw", uniqueHail);
    await upsert("storm_lsr_raw", uniqueStorm);

    const touchedDates = [...new Set([
      ...uniqueHail.map((row) => row.event_date),
      ...uniqueStorm.map((row) => row.event_date)
    ])].sort();
    const swathResults = [];
    const swathBase = process.env.SWATH_RENDER_URL ||
      "https://br-super-wildflower-b4eatcc2-swathrender.compute.c-6.us-east-2.aws.neon.tech/";
    for (const date of touchedDates) {
      try {
        const response = await fetch(swathBase + "?date=" + encodeURIComponent(date) + "&persist=1");
        const body = await response.json().catch(() => ({}));
        swathResults.push({
          date,
          ok: response.ok,
          savedRows: Number(body.savedRows || 0),
          error: response.ok ? null : String(body.error || ("HTTP " + response.status))
        });
      } catch (error) {
        swathResults.push({ date, ok: false, savedRows: 0, error: String(error && error.message || error) });
      }
    }

    return new Response(JSON.stringify({
      ok: true,
      window: { start: ymd(start), end: ymd(new Date(end.getTime() - 1)) },
      hail: uniqueHail.length,
      wind: uniqueStorm.filter(r => r.event_type === "wind").length,
      tornado: uniqueStorm.filter(r => r.event_type === "tornado").length,
      swaths: swathResults
    }), { status: 200, headers: { "content-type": "application/json" } });
  } catch (error) {
    console.error("[stormingest]", error);
    return new Response(JSON.stringify({ ok: false, error: String(error && error.message || error) }), {
      status: 500, headers: { "content-type": "application/json" }
    });
  }
}
