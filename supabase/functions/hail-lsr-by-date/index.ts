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
    const date = url.searchParams.get("date")?.trim() ?? "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return json({ error: "Invalid date. Use date=YYYY-MM-DD" }, 400);
    }

    const globalHeaders: Record<string, string> = {};
    const authHeader = req.headers.get("Authorization");
    if (authHeader) globalHeaders["Authorization"] = authHeader;

    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: { persistSession: false },
      global: { headers: globalHeaders },
    });

    const { data, error } = await supabase
      .from("hail_lsr_raw")
      .select("lat, lon, hail_in, event_time")
      .eq("event_date", date)
      .order("event_time", { ascending: true });

    if (error) return json({ error: error.message }, 500);

    const points = Array.isArray(data) ? data : [];
    return json({ points });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
