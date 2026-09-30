import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2.55.0";

const corsHeaders = {
  "Cache-Control": "no-store",
};

const batchToken = Deno.env.get("MRMS_BATCH_TOKEN") || "__MRMS_BATCH_TOKEN__";
const supabase = createClient(
  Deno.env.get("SUPABASE_URL") || "",
  Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") || "",
  { auth: { persistSession: false } },
);

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function authorized(req: Request) {
  const supplied = req.headers.get("x-ingest-token") || "";
  return batchToken.length >= 32 && supplied.length === batchToken.length &&
    supplied === batchToken;
}

function validDate(value: unknown) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ""));
}

async function loadAnchors(date: string) {
  const [reports, evidence] = await Promise.all([
    supabase
      .from("hail_lsr_raw")
      .select("lat,lon,hail_in,source")
      .eq("event_date", date)
      .gte("hail_in", 0.5)
      .limit(5000),
    supabase
      .from("hail_ground_truth_evidence")
      .select("lat,lon,hail_in,confidence")
      .eq("event_date", date)
      .eq("accepted", true)
      .gte("confidence", 0.75)
      .limit(1000),
  ]);
  if (reports.error) throw new Error(reports.error.message);
  const anchors = (reports.data || []).map((row) => ({
    lat: Number(row.lat),
    lon: Number(row.lon),
    hail_in: Number(row.hail_in),
    confidence: /google_grounded/i.test(String(row.source || "")) ? 0.8 : 0.95,
  }));
  if (!evidence.error) {
    for (const row of evidence.data || []) {
      anchors.push({
        lat: Number(row.lat),
        lon: Number(row.lon),
        hail_in: Number(row.hail_in),
        confidence: Number(row.confidence),
      });
    }
  }
  return anchors.filter((row) =>
    [row.lat, row.lon, row.hail_in, row.confidence].every(Number.isFinite)
  );
}

Deno.serve(async (req) => {
  if (!authorized(req)) return json({ error: "Unauthorized" }, 401);

  try {
    const url = new URL(req.url);
    if (req.method === "GET" && url.searchParams.get("action") === "anchors") {
      const date = url.searchParams.get("date") || "";
      if (!validDate(date)) return json({ error: "Invalid date" }, 400);
      return json({ date, anchors: await loadAnchors(date) });
    }

    if (req.method !== "POST") return json({ error: "Method not allowed" }, 405);
    const payload = await req.json();
    const date = String(payload?.date || "");
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    const totalRows = Number(payload?.total_rows);
    const final = payload?.final === true;
    if (
      !validDate(date) ||
      rows.length === 0 ||
      rows.length > 25 ||
      !Number.isInteger(totalRows) ||
      totalRows < rows.length ||
      totalRows > 5000
    ) {
      return json({ error: "Invalid date or row count" }, 400);
    }
    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      if (
        row?.event_date !== date ||
        row?.source !== "mrms_mesh" ||
        row?.source_product !== "MESH_Max_1440min" ||
        !row?.polygon_geojson?.geometry
      ) {
        return json({ error: `Invalid row ${index}` }, 400);
      }
    }

    const maxMesh = Number(payload?.max_mesh_in);
    if (!Number.isFinite(maxMesh) || maxMesh <= 0) {
      return json({ error: "Invalid maximum hail size" }, 400);
    }
    const { error: parentDayError } = await supabase.from("hail_radar_days").upsert({
      event_date: date,
      max_mesh_in: maxMesh,
      source: "mrms_mesh",
      updated_at: new Date().toISOString(),
    }, { onConflict: "event_date" });
    if (parentDayError) throw new Error(`hail_radar_days parent update failed: ${parentDayError.message}`);

    const canonicalRows = rows.map((row: Record<string, unknown>) => ({
      event_date: row.event_date,
      storm_type: row.storm_type,
      source: row.source,
      source_product: row.source_product,
      source_priority: row.source_priority,
      quality_status: row.quality_status,
      swath_index: row.swath_index,
      polygon_geojson: row.polygon_geojson,
      centroid_lat: row.centroid_lat,
      centroid_lon: row.centroid_lon,
      area_sq_mi: row.area_sq_mi,
      threshold_value: row.band_min,
      band_min: row.band_min,
      band_max: row.band_max,
      band_label: row.band_label,
      updated_at: new Date().toISOString(),
    }));
    const { error: upsertError } = await supabase
      .from("storm_swaths_canonical")
      .upsert(canonicalRows, {
        onConflict: "event_date,source,source_product,swath_index",
      });
    if (upsertError) throw new Error(`canonical swath upsert failed: ${upsertError.message}`);

    if (final) {
      const { error: cleanupError } = await supabase
        .from("storm_swaths_canonical")
        .delete()
        .eq("event_date", date)
        .eq("source", "mrms_mesh")
        .eq("source_product", "MESH_Max_1440min")
        .gte("swath_index", totalRows);
      if (cleanupError) throw new Error(`canonical stale-row cleanup failed: ${cleanupError.message}`);
    }

    const { error: dayError } = await supabase.from("hail_radar_days").upsert({
      event_date: date,
      max_mesh_in: maxMesh,
      source: "mrms_mesh",
      updated_at: new Date().toISOString(),
    }, { onConflict: "event_date" });
    if (dayError) throw new Error(`hail_radar_days update failed: ${dayError.message}`);

    return json({ ok: true, date, rows: rows.length, total_rows: totalRows, final, max_mesh_in: maxMesh });
  } catch (error) {
    return json({ error: error instanceof Error ? error.message : String(error) }, 500);
  }
});
