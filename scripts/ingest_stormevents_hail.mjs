import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import zlib from "node:zlib";
import https from "node:https";
import { createClient } from "@supabase/supabase-js";
import { parse } from "csv-parse";

const NOAA_DIR = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/";
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
  // Dedupe by dedupe_key, keep last occurrence
  const seen = new Map();
  for (const row of batch) {
    if (row.dedupe_key) {
      seen.set(row.dedupe_key, row);
    }
  }
  const deduped = Array.from(seen.values());
  
  const { error } = await supabase
    .from("stormevents_raw")
    .upsert(deduped, { onConflict: "dedupe_key" });
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
}

main().catch((e) => {
  console.error("FAILED:", e);
  process.exit(1);
});
