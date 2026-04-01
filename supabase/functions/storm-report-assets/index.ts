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
      return json({ error: "Missing SUPABASE_URL or key" }, 500);
    }

    const url = new URL(req.url);
    const date = url.searchParams.get("date")?.trim() ?? "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return json({ error: "Invalid date. Use date=YYYY-MM-DD" }, 400);
    }

    // Optional filters
    const source = url.searchParams.get("source")?.trim() ?? "";
    const assetType = url.searchParams.get("asset_type")?.trim() ?? "";

    const globalHeaders: Record<string, string> = {};
    const authHeader = req.headers.get("Authorization");
    if (authHeader) globalHeaders["Authorization"] = authHeader;

    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: { persistSession: false },
      global: { headers: globalHeaders },
    });

    let query = supabase
      .from("storm_report_assets")
      .select(
        "id, event_date, source, asset_type, title, description, " +
        "file_url, file_name, file_size_bytes, mime_type, source_metadata, created_at"
      )
      .eq("event_date", date)
      .order("source", { ascending: true });

    if (source) query = query.eq("source", source);
    if (assetType) query = query.eq("asset_type", assetType);

    const { data, error } = await query;
    if (error) return json({ error: error.message }, 500);

    const assets = Array.isArray(data) ? data : [];

    return json({ assets });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
