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

    const pageSize = 1000;
    let offset = 0;
    const dateSet = new Set<string>();

    for (;;) {
      let q = supabase
        .from("hail_reports")
        .select("event_date")
        .order("event_date", { ascending: false })
        .range(offset, offset + pageSize - 1);

      if (state) q = q.ilike("state", state);
      if (eventTypeNormalized) q = q.ilike("event_type", eventTypeNormalized);

      const { data, error } = await q;
      if (error) return json({ error: error.message }, 500);

      if (!data || data.length === 0) break;

      for (const row of data as Array<{ event_date: unknown }>) {
        const v = row.event_date;
        if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v)) dateSet.add(v);
      }

      if (data.length < pageSize) break;
      offset += pageSize;
    }

    const dates = Array.from(dateSet).sort((a, b) => b.localeCompare(a));
    return json({ dates });
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
