// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
// @ts-ignore
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

declare const Deno: {
  env: { get(key: string): string | undefined };
};

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders },
  });
}

function getSupabaseClient() {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const serviceRoleKey =
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ??
    Deno.env.get("SERVICE_ROLE_KEY") ??
    "";
  if (!supabaseUrl || !serviceRoleKey) return null;
  return createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });
}

// ── Status endpoint ────────────────────────────────────────────────────────
//
// This edge function is a lightweight status/query endpoint for mrms_mesh rows
// in storm_polygons. The actual GRIB2 ingest is handled by the local Node.js
// script:  scripts/ingest_mrms_swaths.mjs
//
// MRMS MESH GRIB2 files use JPEG 2000 compression (data representation
// template 41) which cannot be decoded without GDAL. The local script uses
// the QGIS Python environment (which has GDAL) to process GRIB2 → GeoJSON.
//
// Endpoints:
//   GET ?date=YYYY-MM-DD         — status for one date
//   GET ?dates=YYYY-MM-DD,...    — status for multiple dates
//   GET ?summary=1               — count of all mrms_mesh dates in storm_polygons

serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders });
  if (req.method !== "GET") return json({ error: "Method not allowed" }, 405);

  try {
    const sb = getSupabaseClient();
    if (!sb) return json({ error: "Supabase client unavailable" }, 500);

    const url = new URL(req.url);

    // ?summary=1 — list all dates that have mrms_mesh rows
    if (url.searchParams.get("summary") === "1") {
      const { data, error } = await sb
        .from("storm_polygons")
        .select("event_date")
        .eq("source", "mrms_mesh")
        .order("event_date", { ascending: false });
      if (error) return json({ error: error.message }, 500);
      const dates = [...new Set((data ?? []).map((r: { event_date: string }) => r.event_date))];
      return json({ dates, count: dates.length });
    }

    // ?date=YYYY-MM-DD
    const singleDate = url.searchParams.get("date")?.trim() ?? "";
    if (singleDate) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(singleDate)) {
        return json({ error: "Invalid date format. Use date=YYYY-MM-DD" }, 400);
      }
      const { data, error, count } = await sb
        .from("storm_polygons")
        .select("id, band_min, band_max", { count: "exact" })
        .eq("event_date", singleDate)
        .eq("source", "mrms_mesh");
      if (error) return json({ error: error.message }, 500);
      return json({
        date: singleDate,
        ingested: (count ?? 0) > 0,
        rows: count ?? 0,
        note: (count ?? 0) === 0
          ? "Run: node scripts/ingest_mrms_swaths.mjs --date=" + singleDate
          : undefined,
      });
    }

    // ?dates=YYYY-MM-DD,...
    const datesParam = url.searchParams.get("dates")?.trim() ?? "";
    if (datesParam) {
      const dateList = datesParam
        .split(",")
        .map((d) => d.trim())
        .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d));
      if (!dateList.length) {
        return json({ error: "No valid dates. Use dates=YYYY-MM-DD,YYYY-MM-DD,..." }, 400);
      }
      const { data, error } = await sb
        .from("storm_polygons")
        .select("event_date")
        .eq("source", "mrms_mesh")
        .in("event_date", dateList);
      if (error) return json({ error: error.message }, 500);
      const ingested = new Set((data ?? []).map((r: { event_date: string }) => r.event_date));
      const results = dateList.map((d) => ({ date: d, ingested: ingested.has(d) }));
      return json({ results, ingested: ingested.size, missing: dateList.length - ingested.size });
    }

    return json({
      endpoints: [
        "?date=YYYY-MM-DD",
        "?dates=YYYY-MM-DD,...",
        "?summary=1",
      ],
      note: "MRMS MESH ingest runs locally: node scripts/ingest_mrms_swaths.mjs --date=YYYY-MM-DD",
    });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
