#!/usr/bin/env node
/**
 * Build canonical hail-size swaths from the real MRMS MESH 24-hour grid.
 *
 * This intentionally does not fall back to buffered NEXRAD cell tracks. A
 * track centerline is not a hail footprint and was the cause of the long,
 * smooth, over-connected swaths previously produced for many dates.
 *
 * Usage:
 *   node scripts/ingest_mrms_swaths.mjs --date=2026-06-18
 *   node scripts/ingest_mrms_swaths.mjs --date=2026-06-18 --force
 *   node scripts/ingest_mrms_swaths.mjs --self-test
 *
 * Required:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 *   QGIS Python/GDAL (auto-detected on Windows, or set QGIS_PYTHON)
 */

import { execFile } from "node:child_process";
import { createGunzip } from "node:zlib";
import { createReadStream, createWriteStream } from "node:fs";
import { access, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pipeline } from "node:stream/promises";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const CONTOUR_HELPER = path.join(SCRIPT_DIR, "mrms_mesh_to_geojson.py");
const ARCHIVE_ROOT = (process.env.MRMS_ARCHIVE_ROOT ||
  "https://mtarchive.geol.iastate.edu").replace(/\/+$/, "");
const PRODUCT_DIRS = ["MESH_Max_1440min", "MESHMax1440min"];
const MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024;
const INSERT_CHUNK = 100;

function parseArgs(argv) {
  return Object.fromEntries(argv.map((arg) => {
    const [key, ...rest] = arg.replace(/^--/, "").split("=");
    return [key, rest.length ? rest.join("=") : true];
  }));
}

function addUtcDays(dateIso, days) {
  const date = new Date(`${dateIso}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function archiveDatePath(dateIso) {
  const [year, month, day] = dateIso.split("-");
  return `${year}/${month}/${day}/mrms/ncep`;
}

function timestampFromFilename(filename) {
  const match = String(filename).match(/(\d{8})[-_]?(\d{6})(?=\.grib2(?:\.gz)?$)/i);
  if (!match) return null;
  const d = match[1];
  const t = match[2];
  const ms = Date.UTC(
    Number(d.slice(0, 4)),
    Number(d.slice(4, 6)) - 1,
    Number(d.slice(6, 8)),
    Number(t.slice(0, 2)),
    Number(t.slice(2, 4)),
    Number(t.slice(4, 6)),
  );
  return Number.isFinite(ms) ? ms : null;
}

function parseArchiveListing(html, listingUrl) {
  const found = new Map();
  const hrefPattern = /href\s*=\s*["']([^"'?#]+\.grib2(?:\.gz)?)["']/gi;
  let match;
  while ((match = hrefPattern.exec(String(html)))) {
    const absolute = new URL(match[1], listingUrl).href;
    const filename = decodeURIComponent(new URL(absolute).pathname.split("/").pop() || "");
    const timestamp = timestampFromFilename(filename);
    if (timestamp != null) found.set(absolute, { url: absolute, filename, timestamp });
  }
  return [...found.values()];
}

function chooseSpcDayGrid(candidates, dateIso) {
  if (!candidates.length) return null;
  // Most Recent Storm dates use the SPC 12Z-to-12Z day. A 1440-minute MESH
  // grid valid closest to 12Z the following day covers that exact window.
  const desired = Date.parse(`${addUtcDays(dateIso, 1)}T12:00:00Z`);
  return candidates.slice().sort((a, b) => {
    const aDelta = Math.abs(a.timestamp - desired);
    const bDelta = Math.abs(b.timestamp - desired);
    return aDelta - bDelta || b.timestamp - a.timestamp;
  })[0];
}

async function fetchText(url) {
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
  return response.text();
}

async function findGrid(dateIso) {
  const dates = [addUtcDays(dateIso, 1), dateIso];
  const candidates = [];
  const errors = [];
  for (const archiveDate of dates) {
    for (const productDir of PRODUCT_DIRS) {
      const listingUrl = `${ARCHIVE_ROOT}/${archiveDatePath(archiveDate)}/${productDir}/`;
      try {
        console.log(`[MRMS] Checking ${listingUrl}`);
        const html = await fetchText(listingUrl);
        const files = parseArchiveListing(html, listingUrl);
        console.log(`[MRMS] Found ${files.length} ${productDir} file(s)`);
        candidates.push(...files);
      } catch (error) {
        errors.push(`${listingUrl}: ${error.message}`);
      }
    }
  }
  const chosen = chooseSpcDayGrid(candidates, dateIso);
  if (!chosen) {
    throw new Error(
      `No MRMS MESH_Max_1440min grid found for SPC day ${dateIso}.\n${errors.join("\n")}`,
    );
  }
  return chosen;
}

async function downloadFile(url, destination) {
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok || !response.body) throw new Error(`HTTP ${response.status} for ${url}`);
  const contentLength = Number(response.headers.get("content-length") || 0);
  if (contentLength > MAX_DOWNLOAD_BYTES) {
    throw new Error(`Unexpected MRMS file size ${contentLength} bytes`);
  }
  const writer = createWriteStream(destination);
  let received = 0;
  for await (const chunk of response.body) {
    received += chunk.length;
    if (received > MAX_DOWNLOAD_BYTES) {
      writer.destroy();
      throw new Error(`MRMS download exceeded ${MAX_DOWNLOAD_BYTES} bytes`);
    }
    if (!writer.write(chunk)) await new Promise((resolve) => writer.once("drain", resolve));
  }
  writer.end();
  await new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
  return received;
}

async function exists(candidate) {
  try {
    await access(candidate, fsConstants.X_OK);
    return true;
  } catch {
    try {
      await access(candidate, fsConstants.F_OK);
      return true;
    } catch {
      return false;
    }
  }
}

async function findQgisPython() {
  if (process.env.QGIS_PYTHON && await exists(process.env.QGIS_PYTHON)) {
    return process.env.QGIS_PYTHON;
  }
  if (process.platform === "win32") {
    const roots = [
      process.env.ProgramFiles,
      process.env["ProgramFiles(x86)"],
      "C:\\Program Files",
    ].filter(Boolean);
    for (const root of roots) {
      let entries = [];
      try { entries = await readdir(root, { withFileTypes: true }); } catch { continue; }
      const qgisDirs = entries
        .filter((entry) => entry.isDirectory() && /^QGIS/i.test(entry.name))
        .map((entry) => path.join(root, entry.name))
        .sort().reverse();
      for (const qgisDir of qgisDirs) {
        for (const launcher of ["python-qgis-ltr.bat", "python-qgis.bat"]) {
          const candidate = path.join(qgisDir, "bin", launcher);
          if (await exists(candidate)) return candidate;
        }
      }
    }
  }
  for (const executable of process.platform === "win32" ? ["python.exe", "python"] : ["python3", "python"]) {
    try {
      await execFileAsync(executable, ["-c", "from osgeo import gdal, ogr"]);
      return executable;
    } catch {}
  }
  throw new Error(
    "QGIS/GDAL Python was not found. Install QGIS LTR or set QGIS_PYTHON to its python-qgis-ltr.bat launcher.",
  );
}

async function runContourHelper(python, gribPath, outputPath, anchorsPath) {
  const args = [CONTOUR_HELPER, "--input", gribPath, "--output", outputPath];
  if (anchorsPath) args.push("--anchors", anchorsPath);
  const options = {
    cwd: SCRIPT_DIR,
    windowsHide: true,
    maxBuffer: 20 * 1024 * 1024,
  };
  let stdout;
  let stderr;
  if (process.platform === "win32" && /\.bat$/i.test(python)) {
    const quoteForPowerShell = (value) => `'${String(value).replaceAll("'", "''")}'`;
    const command = `& ${[python, ...args].map(quoteForPowerShell).join(" ")}`;
    ({ stdout, stderr } = await execFileAsync(
      "powershell.exe",
      ["-NoProfile", "-NonInteractive", "-Command", command],
      options,
    ));
  } else {
    ({ stdout, stderr } = await execFileAsync(python, args, options));
  }
  if (stdout.trim()) process.stdout.write(stdout);
  if (stderr.trim()) process.stderr.write(stderr);
}

async function loadGroundAnchors(client, dateIso) {
  const [lsrResult, evidenceResult] = await Promise.all([
    client
      .from("hail_lsr_raw")
      .select("lat,lon,hail_in,source")
      .eq("event_date", dateIso)
      .gte("hail_in", 0.5)
      .limit(5000),
    client
      .from("hail_ground_truth_evidence")
      .select("lat,lon,hail_in,confidence")
      .eq("event_date", dateIso)
      .eq("accepted", true)
      .gte("confidence", 0.75)
      .limit(1000),
  ]);
  if (lsrResult.error) throw new Error(`hail_lsr_raw anchors failed: ${lsrResult.error.message}`);
  const anchors = [];
  for (const row of lsrResult.data || []) {
    anchors.push({
      lat:Number(row.lat),
      lon:Number(row.lon),
      hail_in:Number(row.hail_in),
      confidence:/google_grounded/i.test(String(row.source || "")) ? 0.8 : 0.95,
    });
  }
  if (!evidenceResult.error) {
    for (const row of evidenceResult.data || []) {
      anchors.push({
        lat:Number(row.lat),
        lon:Number(row.lon),
        hail_in:Number(row.hail_in),
        confidence:Number(row.confidence),
      });
    }
  }
  const unique = new Map();
  for (const anchor of anchors) {
    if (![anchor.lat, anchor.lon, anchor.hail_in, anchor.confidence].every(Number.isFinite)) continue;
    const key = `${anchor.lat.toFixed(3)}|${anchor.lon.toFixed(3)}|${anchor.hail_in.toFixed(2)}`;
    const previous = unique.get(key);
    if (!previous || anchor.confidence > previous.confidence) unique.set(key, anchor);
  }
  return [...unique.values()];
}

function makeRows(payload, eventDate, sourceFile) {
  const features = Array.isArray(payload && payload.features) ? payload.features : [];
  return features.map((feature, index) => {
    const props = feature.properties || {};
    const bandMin = Number(props.band_min);
    const bandMax = props.band_max == null ? null : Number(props.band_max);
    return {
      event_date: eventDate,
      storm_type: "hail",
      band_min: bandMin,
      band_max: Number.isFinite(bandMax) ? bandMax : null,
      band_label: props.band_label || `${bandMin.toFixed(2)}"+`,
      polygon_geojson: {
        type: "Feature",
        geometry: feature.geometry,
        properties: {
          hailProvenance: {
            sourceClassification: "radar_estimate",
            sourceProduct: "MESH_Max_1440min",
            sourceFile,
            geometryValid: true,
            provenanceComplete: true,
            acceptedForCustomerDisplay: true,
            reviewStatus: "accepted",
            qualityStatus: "accepted",
          },
        },
      },
      centroid_lat: Number(props.centroid_lat),
      centroid_lon: Number(props.centroid_lon),
      area_sq_mi: Number(props.area_sq_mi),
      source: "mrms_mesh",
      source_product: "MESH_Max_1440min",
      source_priority: 1,
      quality_status: "accepted",
      swath_index: index,
    };
  }).filter((row) =>
    Number.isFinite(row.band_min) &&
    row.polygon_geojson.geometry &&
    Number.isFinite(row.centroid_lat) &&
    Number.isFinite(row.centroid_lon) &&
    Number.isFinite(row.area_sq_mi)
  );
}

async function insertChunks(client, table, rows) {
  for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
    const chunk = rows.slice(i, i + INSERT_CHUNK);
    const { error } = await client.from(table).insert(chunk);
    if (error) throw new Error(`${table} insert failed: ${error.message}`);
  }
}

function radarRows(rows) {
  return rows.map((row) => ({
    event_date: row.event_date,
    threshold_in: row.band_min,
    geojson: row.polygon_geojson,
    source: row.source,
    source_product: row.source_product,
    source_priority: row.source_priority,
    threshold_value: row.band_min,
    band_min: row.band_min,
    band_max: row.band_max,
    polygon_geojson: row.polygon_geojson,
    geometry: row.polygon_geojson,
    centroid_lat: row.centroid_lat,
    centroid_lon: row.centroid_lon,
    area_sq_mi: row.area_sq_mi,
    swath_index: row.swath_index,
    storm_type: row.storm_type,
  }));
}

async function persistRows(client, dateIso, rows) {
  if (!rows.length) throw new Error("MRMS conversion returned zero displayable polygons");

  const maxMeshIn = rows.reduce((maximum, row) => {
    const value = Number.isFinite(row.band_max) ? row.band_max : row.band_min;
    return Math.max(maximum, Number(value) || 0);
  }, 0);
  const { error: dayError } = await client
    .from("hail_radar_days")
    .upsert({
      event_date: dateIso,
      max_mesh_in: maxMeshIn,
      source: "mrms_mesh",
    }, { onConflict: "event_date" });
  if (dayError) throw new Error(`hail_radar_days upsert failed: ${dayError.message}`);

  // Only replace this product after the new complete result exists in memory.
  for (const table of ["storm_polygons", "hail_radar_polygons"]) {
    const { error } = await client
      .from(table)
      .delete()
      .eq("event_date", dateIso)
      .eq("source", "mrms_mesh");
    if (error) throw new Error(`${table} cleanup failed: ${error.message}`);
  }

  await insertChunks(client, "storm_polygons", rows);
  await insertChunks(client, "hail_radar_polygons", radarRows(rows));
}

async function selfTest() {
  const listing = `
    <a href="MRMS_MESH_Max_1440min_00.50_20260619-113000.grib2.gz">a</a>
    <a href="MRMS_MESH_Max_1440min_00.50_20260619-120000.grib2.gz">b</a>
    <a href="not-mesh.txt">c</a>`;
  const parsed = parseArchiveListing(
    listing,
    "https://example.test/2026/06/19/mrms/ncep/MESH_Max_1440min/",
  );
  const chosen = chooseSpcDayGrid(parsed, "2026-06-18");
  if (parsed.length !== 2 || !chosen || !chosen.filename.includes("-120000")) {
    throw new Error("archive listing/SPC-day selection self-test failed");
  }
  const fakePayload = {
    features: [{
      type: "Feature",
      properties: {
        band_min: 1.25,
        band_max: 1.5,
        centroid_lat: 38,
        centroid_lon: -97,
        area_sq_mi: 10,
      },
      geometry: { type: "Polygon", coordinates: [[[-97, 38], [-96, 38], [-97, 39], [-97, 38]]] },
    }],
  };
  const rows = makeRows(fakePayload, "2026-06-18", chosen.filename);
  if (rows.length !== 1 || rows[0].source !== "mrms_mesh" || rows[0].band_min !== 1.25) {
    throw new Error("canonical row self-test failed");
  }
  console.log("[MRMS] Self-test passed");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args["self-test"]) return selfTest();

  const dateIso = String(args.date || "");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateIso)) {
    throw new Error("Usage: node scripts/ingest_mrms_swaths.mjs --date=YYYY-MM-DD [--force] [--output=rows.json]");
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const outputOnly = Boolean(args.output);
  if (!outputOnly && (!supabaseUrl || !supabaseKey)) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  let client = null;
  if (supabaseUrl && supabaseKey) {
    const { createClient } = await import("@supabase/supabase-js");
    client = createClient(supabaseUrl, supabaseKey, { auth: { persistSession: false } });
  }

  if (client && !args.force) {
    const { count, error } = await client
      .from("storm_polygons")
      .select("id", { count: "exact", head: true })
      .eq("event_date", dateIso)
      .eq("source", "mrms_mesh")
      .eq("source_product", "MESH_Max_1440min");
    if (error) throw new Error(error.message);
    if ((count || 0) > 0) {
      console.log(`[MRMS] ${dateIso} already has ${count} canonical MESH polygon(s); use --force to rebuild`);
      return;
    }
  }

  const workDir = await mkdtemp(path.join(tmpdir(), "hail-money-mrms-"));
  try {
    const chosen = await findGrid(dateIso);
    console.log(`[MRMS] Selected ${chosen.filename}`);
    const compressedPath = path.join(workDir, chosen.filename);
    const downloaded = await downloadFile(chosen.url, compressedPath);
    console.log(`[MRMS] Downloaded ${downloaded} bytes`);

    let gribPath = compressedPath;
    if (/\.gz$/i.test(compressedPath)) {
      gribPath = compressedPath.replace(/\.gz$/i, "");
      await pipeline(createReadStream(compressedPath), createGunzip(), createWriteStream(gribPath));
    }
    const gribStats = await stat(gribPath);
    if (gribStats.size < 1000) throw new Error("Downloaded MRMS GRIB2 is unexpectedly small");

    const python = await findQgisPython();
    console.log(`[MRMS] Contouring real hail grid with ${python}`);
    const outputPath = path.join(workDir, "mesh-polygons.json");
    let groundAnchors = client ? await loadGroundAnchors(client, dateIso) : [];
    if (args.anchors) {
      const anchorPayload = JSON.parse(await readFile(path.resolve(String(args.anchors)), "utf8"));
      groundAnchors = Array.isArray(anchorPayload) ? anchorPayload : (anchorPayload.anchors || []);
    }
    const anchorsPath = path.join(workDir, "ground-anchors.json");
    await writeFile(anchorsPath, JSON.stringify({ anchors:groundAnchors }), "utf8");
    console.log(`[MRMS] Calibrating with ${groundAnchors.length} verified ground anchor(s)`);
    await runContourHelper(python, gribPath, outputPath, anchorsPath);
    const payload = JSON.parse(await readFile(outputPath, "utf8"));
    const rows = makeRows(payload, dateIso, chosen.filename);
    console.log(`[MRMS] Built ${rows.length} canonical hail-band polygon(s)`);
    if (outputOnly) {
      const outputFile = path.resolve(String(args.output));
      await writeFile(outputFile, JSON.stringify({ date:dateIso, rows }), "utf8");
      console.log(`[MRMS] Wrote ${rows.length} row(s) to ${outputFile}`);
    } else {
      await persistRows(client, dateIso, rows);
      console.log(`[MRMS] Saved ${rows.length} mrms_mesh polygon(s) for ${dateIso}`);
    }
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error("[MRMS] Fatal:", error && error.stack || error);
  process.exitCode = 1;
});
