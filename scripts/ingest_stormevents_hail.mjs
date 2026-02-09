import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import https from "node:https";
import { createClient } from "@supabase/supabase-js";
import { parse } from "csv-parse";

const NOAA_DIR = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/";
const NOAA_PROXY_BASE =
  process.env.NOAA_PROXY_BASE || "https://noaaplsrproxy-jzyejnppqa-uc.a.run.app";
const BATCH_SIZE = 500;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          res.resume();
          return;
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

function downloadToFile(url, destPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          res.resume();
          return;
        }
        res.pipe(file);
        file.on("finish", () => file.close(resolve));
      })
      .on("error", (err) => {
        fs.unlink(destPath, () => reject(err));
      });
  });
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode} for ${url}`));
          res.resume();
          return;
        }
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(e);
          }
        });
      })
      .on("error", reject);
  });
}

function ymd(date) {
  return date.toISOString().slice(0, 10);
}

function ymdCompact(ymdStr) {
  return ymdStr.replace(/-/g, "");
}

async function ingestRollingReportsPass(days = 14) {
  const endYmd = ymd(new Date());
  const startDate = new Date(endYmd + "T00:00:00Z");
  startDate.setUTCDate(startDate.getUTCDate() - Number(days || 14));
  const startYmd = ymd(startDate);

  console.log(`Rolling reports pass: ${startYmd} → ${endYmd} (inclusive)`);

  const start = ymdCompact(startYmd);
  const end = ymdCompact(endYmd);
  const limit = "20000";
  const bboxes = [
    "-125,24,-110,50",
    "-110,24,-95,50",
    "-95,24,-80,50",
    "-80,24,-66,50",
    "-170,51,-130,72",
    "-161,18,-154,23",
  ];

  let storms = [];
  for (const bbox of bboxes) {
    const url =
      `${NOAA_PROXY_BASE}/noaaPlsrProxy?mode=reports` +
      `&start=${start}&end=${end}` +
      `&bbox=${encodeURIComponent(bbox)}` +
      `&limit=${limit}`;

    const j = await fetchJson(url);
    const chunk = Array.isArray(j?.storms) ? j.storms : [];
    storms = storms.concat(chunk);
    if (chunk.length >= Number(limit)) {
      console.warn(`Rolling reports warning: bbox ${bbox} hit limit=${limit}`);
    }
  }

  let batch = [];
  let kept = 0;

  for (const s of storms) {
    const lat = Number(s?.lat);
    const lon = Number(s?.lon);
    const dateStr = typeof s?.date === "string" ? s.date : null;
    const hailSize = s?.hailSize === "" || s?.hailSize == null ? null : Number(s.hailSize);
    const ztime = typeof s?.ztime === "string" && s.ztime ? s.ztime : null;

    if (!dateStr || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) continue;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    kept++;

    const key = `${dateStr}_${String(lat)}_${String(lon)}_${String(hailSize ?? "")}`;

    batch.push({
      dedupe_key: key,
      event_date: dateStr,
      event_type: "hail",
      state: null,
      hail_size: Number.isFinite(hailSize) ? hailSize : null,
      lat,
      lon,
      ztime,
    });

    if (batch.length >= BATCH_SIZE) {
      await supabase.from("hail_reports").upsert(batch, { onConflict: "dedupe_key" });
      batch = [];
    }
  }

  if (batch.length) {
    await supabase.from("hail_reports").upsert(batch, { onConflict: "dedupe_key" });
  }

  console.log(`Rolling reports pass done. attempted_upserts=${kept}`);
}

async function pickLatestDetailsGz() {
  const html = await fetchText(NOAA_DIR);
  const re = /StormEvents_details-ftp_v1\.0_d\d{4}_c(\d{8})\.csv\.gz/g;
  const matches = [];
  let m;
  while ((m = re.exec(html))) matches.push({ fname: m[0], cdate: m[1] });
  if (!matches.length) throw new Error("No StormEvents_details .csv.gz found");
  matches.sort((a, b) => a.cdate.localeCompare(b.cdate));
  return matches[matches.length - 1].fname;
}

function stableKey(parts) {
  const s = parts.join("|");
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return String(h >>> 0);
}

async function upsertRaw(batch) {
  const { error } = await supabase
    .from("stormevents_raw")
    .upsert(batch, { onConflict: "dedupe_key" });
  if (error) throw error;
}

async function main() {
  const latest = await pickLatestDetailsGz();
  const url = NOAA_DIR + latest;
  console.log("Latest NOAA file:", latest);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "stormevents-"));
  const gzPath = path.join(tmpDir, latest);
  const csvPath = gzPath.replace(/\.gz$/, "");

  console.log("Downloading...");
  await downloadToFile(url, gzPath);

  console.log("Decompressing...");
  await new Promise((resolve, reject) => {
    fs.createReadStream(gzPath)
      .pipe(zlib.createGunzip())
      .pipe(fs.createWriteStream(csvPath))
      .on("finish", resolve)
      .on("error", reject);
  });

  console.log("Parsing + ingesting hail rows...");
  const parser = fs
    .createReadStream(csvPath)
    .pipe(parse({ columns: true, relax_column_count: true, trim: true }));

  let batch = [];
  let seen = 0;
  let kept = 0;

  for await (const r of parser) {
    seen++;

    if (r.EVENT_TYPE !== "Hail") continue;

    const beginDateTime = (r.BEGIN_DATE_TIME || "").trim();
    const lat = Number(r.BEGIN_LAT);
    const lon = Number(r.BEGIN_LON);
    const hailSize = r.HAIL_SIZE === "" ? null : Number(r.HAIL_SIZE);

    if (!beginDateTime) continue;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    kept++;

    const key = stableKey([
      beginDateTime,
      String(lat),
      String(lon),
      String(hailSize ?? ""),
      String(r.STATE ?? ""),
      String(r.CZ_NAME ?? ""),
    ]);

    batch.push({
      dedupe_key: key,
      begin_date_time: beginDateTime,
      event_type: "Hail",
      state: r.STATE || null,
      cz_name: r.CZ_NAME || null,
      hail_size: Number.isFinite(hailSize) ? hailSize : null,
      begin_lat: lat,
      begin_lon: lon,
    });

    if (batch.length >= BATCH_SIZE) {
      await upsertRaw(batch);
      batch = [];
    }

    if (kept % 5000 === 0) console.log(`Progress: seen=${seen} kept_hail=${kept}`);
  }

  if (batch.length) await upsertRaw(batch);

  console.log("Calling sync_hail_reports_from_raw()...");
  const { data, error } = await supabase.rpc("sync_hail_reports_from_raw");
  if (error) throw error;

  console.log("Sync done. Rows affected (approx):", data);
  console.log(`DONE. seen=${seen} kept_hail=${kept}`);

  await ingestRollingReportsPass(14);

  console.log("Calling sync_hail_reports_from_raw() after rolling pass...");
  const { data: data2, error: error2 } = await supabase.rpc("sync_hail_reports_from_raw");
  if (error2) throw error2;
  console.log("Sync done (rolling pass). Rows affected (approx):", data2);
}

main().catch((e) => {
  console.error("FAILED:", e);
  process.exit(1);
});
