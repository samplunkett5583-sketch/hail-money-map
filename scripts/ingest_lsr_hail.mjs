#!/usr/bin/env node

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

function ymd(date) {
  return date.toISOString().slice(0, 10);
}

function toIemIsoStartOfDayZ(date) {
  return `${ymd(date)}T00:00Z`;
}

function parseValid2Utc(valid2) {
  // Example: "2024/05/20 00:10" (assumed UTC)
  const m = String(valid2 || "").match(/^(\d{4})\/(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})$/);
  if (!m) return null;
  const year = Number(m[1]);
  const month = Number(m[2]) - 1;
  const day = Number(m[3]);
  const hour = Number(m[4]);
  const minute = Number(m[5]);
  const d = new Date(Date.UTC(year, month, day, hour, minute, 0));
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function numOrNull(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s || s.toLowerCase() === "none" || s.toLowerCase() === "nan") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function stableId(parts) {
  const s = parts.map((p) => (p == null ? "" : String(p))).join("|");
  return crypto.createHash("sha256").update(s).digest("hex");
}

async function fetchIemCsv(url) {
  const resp = await fetch(url, {
    headers: {
      Accept: "text/csv,*/*",
      "User-Agent": "hail-money-map-lsr-ingest/1.0",
    },
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`IEM LSR fetch failed HTTP ${resp.status}: ${text.slice(0, 300)}`);
  }
  return text;
}

async function main() {
  const now = new Date();
  const endDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  endDate.setUTCDate(endDate.getUTCDate() + 1); // include today
  const startDate = new Date(endDate);
  startDate.setUTCDate(startDate.getUTCDate() - 7);

  const sts = toIemIsoStartOfDayZ(startDate);
  const ets = toIemIsoStartOfDayZ(endDate);

  const url =
    "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/lsr.py" +
    `?sts=${encodeURIComponent(sts)}` +
    `&ets=${encodeURIComponent(ets)}` +
    `&type=HAIL` +
    `&fmt=csv&justcsv=1`;

  console.log(`[LSR] Fetching hail LSR CSV: ${sts} -> ${ets}`);

  const csvText = await fetchIemCsv(url);
  const records = parse(csvText, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
  });

  const fetched = Array.isArray(records) ? records.length : 0;

  const rows = [];
  for (const r of records) {
    const typeText = String(r?.TYPETEXT ?? "").trim().toUpperCase();
    const typeCode = String(r?.TYPECODE ?? "").trim().toUpperCase();
    if (typeText !== "HAIL" && typeCode !== "H") continue;

    const lat = numOrNull(r?.LAT);
    const lon = numOrNull(r?.LON);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const eventTime = parseValid2Utc(r?.VALID2);
    if (!eventTime) continue;

    const eventDate = ymd(eventTime);
    const hailIn = numOrNull(r?.MAG);
    const state = r?.STATE != null ? String(r.STATE).trim() : null;
    const county = r?.COUNTY != null ? String(r.COUNTY).trim() : null;
    const remark = r?.REMARK != null ? String(r.REMARK).trim() : "";
    const city = r?.CITY != null ? String(r.CITY).trim() : "";
    const wfo = r?.WFO != null ? String(r.WFO).trim() : "";
    const src = r?.SOURCE != null ? String(r.SOURCE).trim() : "";

    const id = stableId([
      eventTime.toISOString(),
      lat,
      lon,
      hailIn ?? "",
      state ?? "",
      county ?? "",
      city,
      wfo,
      src,
      remark,
    ]);

    rows.push({
      id,
      event_time: eventTime.toISOString(),
      event_date: eventDate,
      lat,
      lon,
      hail_in: hailIn,
      state,
      county,
      source: "LSR",
      raw: r,
    });
  }

  const byId = new Map();
  for (const row of rows) byId.set(row.id, row);
  const deduped = Array.from(byId.values());

  console.log(`[LSR] fetched=${fetched} parsed_hail=${rows.length} deduped=${deduped.length}`);

  const BATCH_SIZE = 1000;
  let upserted = 0;

  for (let i = 0; i < deduped.length; i += BATCH_SIZE) {
    const batch = deduped.slice(i, i + BATCH_SIZE);
    const { error } = await supabase
      .from("hail_lsr_raw")
      .upsert(batch, { onConflict: "id" });
    if (error) throw error;
    upserted += batch.length;
    console.log(`[LSR] upserted_batch=${batch.length} total_upserted=${upserted}`);
  }

  console.log(`[LSR] done. fetched=${fetched} upserted=${upserted}`);
}

main().catch((err) => {
  console.error("LSR ingest failed:", err);
  process.exit(1);
});
