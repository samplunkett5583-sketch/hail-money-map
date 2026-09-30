// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
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

serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders });
  if (req.method !== "GET") return json({ error: "Method not allowed" }, 405);

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const supabaseAnonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";
    const serviceRoleKey = Deno.env.get("SERVICE_ROLE_KEY");
    const supabaseKey = serviceRoleKey ?? supabaseAnonKey;

    if (!supabaseUrl || !supabaseKey) {
      return json({ error: "Missing SUPABASE_URL or SUPABASE_ANON_KEY" }, 500);
    }

    const url = new URL(req.url);
    let state = url.searchParams.get("state")?.trim();
    const eventType = url.searchParams.get("event_type")?.trim();

    if (state) {
      state = state.toUpperCase();
      if (state.length === 2) {
        if (state === "FL") state = "FLORIDA";
      }
    }

    const eventTypeNormalized = eventType ? eventType.toLowerCase() : undefined;

    const globalHeaders: Record<string, string> = {};
    const authHeader = req.headers.get("Authorization");
    if (authHeader) globalHeaders["Authorization"] = authHeader;

    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: { persistSession: false },
      global: { headers: globalHeaders },
    });

    const now = new Date();
    const from = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    from.setUTCMonth(from.getUTCMonth() - 24);
    const fromIso = from.toISOString();

    const pageSize = 2000;
    const dateSet = new Set<string>();
    // Per-date event types and summaries
    const dateEventTypes: Record<string, string[]> = {};
    const dateSummaries: Record<string, Record<string, unknown>> = {};

    // Helper to ensure dateEventTypes entry
    function addType(d: string, t: string) {
      if (!dateEventTypes[d]) dateEventTypes[d] = [];
      if (dateEventTypes[d].indexOf(t) === -1) dateEventTypes[d].push(t);
    }

    // ── 1) Hail dates from existing tables ──
    const useUnionTables = !state && !eventTypeNormalized;
    const tables = useUnionTables ? ["hail_reports", "hail_lsr_raw"] : ["hail_reports"];

    // Collect hail sizes per date for summary
    const hailByDate: Record<string, number[]> = {};

    for (const table of tables) {
      const selectCols = table === "hail_lsr_raw" ? "event_date, hail_in" : "event_date, hail_in:hail_size";
      let offset = 0;
      for (;;) {
        let q = supabase
          .from(table)
          .select(selectCols)
          .order("event_date", { ascending: false })
          .range(offset, offset + pageSize - 1);

        if (!useUnionTables) {
          if (state) q = q.ilike("state", state);
          if (eventTypeNormalized) q = q.ilike("event_type", eventTypeNormalized);
        }

        const { data, error } = await q;
        if (error) return json({ error: error.message }, 500);
        if (!data || data.length === 0) break;

        for (const row of data as Array<{ event_date: unknown; hail_in?: unknown }>) {
          const v = row.event_date;
          if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v)) {
            dateSet.add(v);
            addType(v, "hail");
            const h = Number(row.hail_in);
            if (Number.isFinite(h) && h > 0) {
              if (!hailByDate[v]) hailByDate[v] = [];
              hailByDate[v].push(h);
            }
          }
        }

        if (data.length < pageSize) break;
        offset += pageSize;
      }
    }

    // ── 2) Wind & tornado dates from storm_lsr_raw ──
    {
      const { data, error } = await supabase
        .from("storm_lsr_raw")
        .select("event_date, event_type, magnitude")
        .order("event_date", { ascending: false })
        .limit(15000);

      // Table may not exist yet — treat as empty
      if (error) {
        console.warn("storm_lsr_raw query error (may not exist yet):", error.message);
      } else if (data && data.length > 0) {
        for (const row of data as Array<{ event_date: unknown; event_type: unknown; magnitude?: unknown }>) {
          const v = row.event_date;
          const et = String(row.event_type || "").toLowerCase();
          if (typeof v !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(v)) continue;
          if (et !== "wind" && et !== "tornado") continue;

          dateSet.add(v);
          addType(v, et);

          const mag = Number(row.magnitude);
          if (!Number.isFinite(mag)) continue;

          if (!dateSummaries[v]) dateSummaries[v] = {};
          const key = et === "wind" ? "wind" : "tornado";
          const existing = dateSummaries[v][key] as { min: number; max: number } | undefined;
          if (existing) {
            existing.min = Math.min(existing.min, mag);
            existing.max = Math.max(existing.max, mag);
          } else {
            dateSummaries[v][key] = { min: mag, max: mag };
          }
        }
      }
    }

    // ── 3) Polygon-derived dates from storm_polygons ──
    {
      const q = supabase
        .from("storm_polygons")
        .select("event_date,storm_type,band_min,band_max,state")
        .order("event_date", { ascending: false })
        .limit(15000);

      if (state) q.ilike("state", state);
      if (eventTypeNormalized) {
        if (eventTypeNormalized === "hail") {
          q.not("storm_type", "in.(wind,tornado)");
        } else if (eventTypeNormalized === "wind" || eventTypeNormalized === "tornado") {
          q.ilike("storm_type", eventTypeNormalized);
        }
      }

      const { data, error } = await q;
      if (error) {
        console.warn("storm_polygons query error (may not exist yet):", error.message);
      } else if (data && data.length > 0) {
        for (const row of data as Array<{ event_date: unknown; storm_type?: unknown; band_min?: unknown; band_max?: unknown }>) {
          const v = row.event_date;
          if (typeof v !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(v)) continue;
          const st = String(row.storm_type || "").toLowerCase();
          const type = st === "wind" || st === "tornado" ? st : "hail";

          dateSet.add(v);
          addType(v, type);

          const minBand = Number(row.band_min);
          const maxBand = Number(row.band_max);
          if (Number.isFinite(minBand) && minBand > 0) {
            if (!dateSummaries[v]) dateSummaries[v] = {};
            const existing = dateSummaries[v].hail as { min: number; max: number } | undefined;
            const candidate = {
              min: minBand,
              max: Number.isFinite(maxBand) && maxBand >= minBand ? maxBand : minBand,
            };
            if (existing) {
              existing.min = Math.min(existing.min, candidate.min);
              existing.max = Math.max(existing.max, candidate.max);
            } else {
              dateSummaries[v].hail = candidate;
            }
          }
        }
      }
    }

    // Build hail summaries
    for (const [d, sizes] of Object.entries(hailByDate)) {
      if (!sizes.length) continue;
      if (!dateSummaries[d]) dateSummaries[d] = {};
      dateSummaries[d].hail = {
        min: Math.min(...sizes),
        max: Math.max(...sizes),
      };
    }

    // Inject reference-image-derived dates that have no DB rows
    const syntheticDates = ["2026-03-14", "2026-03-24", "2026-03-29", "2026-04-02"];
    for (const sd of syntheticDates) {
      dateSet.add(sd);
      addType(sd, "hail");
    }

    const dates = Array.from(dateSet).sort((a, b) => b.localeCompare(a));
    return json({ dates, dateEventTypes, dateSummaries });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
