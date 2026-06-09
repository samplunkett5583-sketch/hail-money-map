import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("[sync-radar] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

function argValue(name) {
  const arg = process.argv.find(a => a.startsWith(`${name}=`));
  return arg ? arg.slice(name.length + 1) : null;
}

const date = argValue("--date");

if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
  console.error("[sync-radar] Usage: node scripts/sync_hail_radar_from_storm_polygons.mjs --date=YYYY-MM-DD");
  process.exit(1);
}

const columns = `
  event_date,
  source,
  source_product,
  storm_type,
  polygon_geojson,
  centroid_lat,
  centroid_lon,
  point_count,
  area_sq_mi,
  threshold_value,
  event_start_utc,
  event_end_utc,
  metadata_json,
  swath_index,
  band_min,
  band_max,
  band_label
`;

console.log(`[sync-radar] date=${date}`);

const { count: beforeCount, error: beforeErr } = await supabase
  .from("hail_radar_polygons")
  .select("id", { count: "exact", head: true })
  .eq("event_date", date);

if (beforeErr) {
  console.error("[sync-radar] before count failed", beforeErr);
  process.exit(1);
}

const { error: dayErr } = await supabase
  .from("hail_radar_days")
  .upsert([{ event_date: date }], { onConflict: "event_date" });

if (dayErr) {
  console.error("[sync-radar] hail_radar_days upsert failed", dayErr);
  process.exit(1);
}

const { error: delErr } = await supabase
  .from("hail_radar_polygons")
  .delete()
  .eq("event_date", date);

if (delErr) {
  console.error("[sync-radar] delete existing failed", delErr);
  process.exit(1);
}

const { data: stormRows, error: stormErr } = await supabase
  .from("storm_polygons")
  .select(columns)
  .eq("event_date", date)
  .eq("source", "nexrad_iem")
  .eq("source_product", "iem_nexrad_track")
  .eq("storm_type", "hail");

if (stormErr) {
  console.error("[sync-radar] storm_polygons fetch failed", stormErr);
  process.exit(1);
}

const rows = (stormRows || [])
  .filter(r => r.polygon_geojson)
  .map(r => ({
    event_date: r.event_date,
    source: r.source,
    source_product: r.source_product,
    storm_type: r.storm_type,
    polygon_geojson: r.polygon_geojson,
    geojson: r.polygon_geojson,
    centroid_lat: r.centroid_lat,
    centroid_lon: r.centroid_lon,
    point_count: r.point_count,
    area_sq_mi: r.area_sq_mi,
    threshold_value: r.threshold_value,
    event_start_utc: r.event_start_utc,
    event_end_utc: r.event_end_utc,
    metadata_json: r.metadata_json,
    swath_index: r.swath_index,
    band_min: r.band_min,
    band_max: r.band_max,
    band_label: r.band_label
  }));

console.log(`[sync-radar] storm rows found=${stormRows?.length || 0}`);
console.log(`[sync-radar] valid rows to insert=${rows.length}`);

for (let i = 0; i < rows.length; i += 500) {
  const chunk = rows.slice(i, i + 500);
  const { error: insErr } = await supabase
    .from("hail_radar_polygons")
    .insert(chunk);

  if (insErr) {
    console.error(`[sync-radar] insert chunk ${i}-${i + chunk.length - 1} failed`, insErr);
    process.exit(1);
  }
}

const { count: afterCount, error: afterErr } = await supabase
  .from("hail_radar_polygons")
  .select("id", { count: "exact", head: true })
  .eq("event_date", date);

if (afterErr) {
  console.error("[sync-radar] after count failed", afterErr);
  process.exit(1);
}

console.log(`[sync-radar] rows before=${beforeCount || 0} rows after=${afterCount || 0}`);
console.log(`[sync-radar] polygons saved=${afterCount || 0}`);

if (rows.length > 0 && (afterCount || 0) === 0) {
  console.error("[sync-radar] ERROR: storm_polygons had hail rows but hail_radar_polygons saved 0");
  process.exit(1);
}
