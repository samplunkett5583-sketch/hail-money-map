// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
// @ts-ignore - Supabase Edge Functions run on Deno and support URL imports.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.39.7";

declare const Deno: {
  env: { get(key: string): string | undefined };
};

const corsHeaders: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders },
  });
}

/**
 * Convert ISO date (YYYY-MM-DD) → HailTrace URL date (MM-DD-YYYY).
 */
function dateToHailTraceUrl(dateIso: string): string {
  const [y, m, d] = dateIso.split("-");
  return `https://hailtrace.com/hail-maps/${m}-${d}-${y}`;
}

/** Cache TTL: 24 hours. Re-probe HailTrace after this period. */
const CACHE_TTL_MS = 24 * 60 * 60 * 1000;

function getSupabaseClient() {
  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const serviceRoleKey =
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ??
    Deno.env.get("SERVICE_ROLE_KEY") ??
    "";
  if (!supabaseUrl || !serviceRoleKey) return null;
  return createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
  });
}

/**
 * Probe HailTrace for a single date. Returns { imageUrl, available }.
 */
async function probeHailTrace(
  hailtraceUrl: string,
): Promise<{ imageUrl: string | null; available: boolean }> {
  let imageUrl: string | null = null;
  let available = false;
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10_000);
    const resp = await fetch(hailtraceUrl, {
      headers: {
        "User-Agent": "HailMoneyMap/1.0",
        Accept: "text/html,application/xhtml+xml",
      },
      redirect: "follow",
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (resp.ok) {
      const html = await resp.text();
      const ogMatch =
        html.match(
          /<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i,
        ) ||
        html.match(
          /<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i,
        );
      if (ogMatch) imageUrl = ogMatch[1];
      if (!imageUrl) {
        const twMatch =
          html.match(
            /<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i,
          ) ||
          html.match(
            /<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter:image["']/i,
          );
        if (twMatch) imageUrl = twMatch[1];
      }
      // Also search for CDN image URLs in the page body
      if (!imageUrl) {
        const cdnMatch = html.match(
          /https?:\/\/cdn\.hailtrace\.com\/[^\s"'<>)]+\.(webp|png|jpg|jpeg)/i,
        );
        if (cdnMatch) imageUrl = cdnMatch[0];
      }
      // Direct CDN URL fallback based on known URL pattern
      if (!imageUrl) {
        const dateMatch = hailtraceUrl.match(/(\d{2})-(\d{2})-(\d{4})$/);
        if (dateMatch) {
          imageUrl = `https://cdn.hailtrace.com/storm-image-previews/${dateMatch[1]}-${dateMatch[2]}-${dateMatch[3]}/sales-header.webp`;
        }
      }
      const looksLikeHailPage =
        /hail/i.test(html) && (imageUrl !== null || /hail-map/i.test(html));
      available = looksLikeHailPage;
    }
  } catch (fetchErr) {
    console.warn(`[hailtrace-swath] probe failed:`, fetchErr);
  }
  return { imageUrl, available };
}

/**
 * Process a single date: check cache → probe if needed → write cache → return.
 */
async function processDate(
  date: string,
  force: boolean,
  sb: ReturnType<typeof createClient> | null,
): Promise<Record<string, unknown>> {
  const hailtraceUrl = dateToHailTraceUrl(date);

  // --- Check cache ---
  if (sb && !force) {
    try {
      const { data: cached } = await sb
        .from("hailtrace_swath_cache")
        .select("*")
        .eq("event_date", date)
        .maybeSingle();
      if (cached && cached.probed_at) {
        const age = Date.now() - new Date(cached.probed_at).getTime();
        if (age < CACHE_TTL_MS) {
          return {
            date,
            hailtraceUrl: cached.hailtrace_url,
            imageUrl: cached.image_url,
            available: cached.available,
            cached: true,
          };
        }
      }
    } catch {
      // Cache miss or table doesn't exist yet — fall through to live probe
    }
  }

  // --- Live probe ---
  const { imageUrl, available } = await probeHailTrace(hailtraceUrl);

  // --- Write cache ---
  if (sb) {
    try {
      await sb.from("hailtrace_swath_cache").upsert(
        {
          event_date: date,
          available,
          image_url: imageUrl,
          hailtrace_url: hailtraceUrl,
          probed_at: new Date().toISOString(),
        },
        { onConflict: "event_date" },
      );
    } catch (cacheErr) {
      console.warn(`[hailtrace-swath] cache write failed for ${date}:`, cacheErr);
    }
  }

  return { date, hailtraceUrl, imageUrl, available };
}

serve(async (req: Request) => {
  if (req.method === "OPTIONS")
    return new Response(null, { status: 204, headers: corsHeaders });
  if (req.method !== "GET") return json({ error: "Method not allowed" }, 405);

  try {
    const url = new URL(req.url);
    const force = url.searchParams.get("force") === "1";
    const sb = getSupabaseClient();

    // --- Single-date mode: ?date=YYYY-MM-DD ---
    const date = url.searchParams.get("date")?.trim() ?? "";
    if (date) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        return json({ error: "Invalid date. Use date=YYYY-MM-DD" }, 400);
      }
      const result = await processDate(date, force, sb);
      return json(result);
    }

    // --- Batch mode: ?dates=YYYY-MM-DD,YYYY-MM-DD,... ---
    const datesParam = url.searchParams.get("dates")?.trim() ?? "";
    if (datesParam) {
      const dateList = datesParam
        .split(",")
        .map((d) => d.trim())
        .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d));
      if (!dateList.length) {
        return json({ error: "No valid dates. Use dates=YYYY-MM-DD,YYYY-MM-DD" }, 400);
      }
      // Process sequentially to respect HailTrace rate limits
      const results: Record<string, unknown>[] = [];
      for (const d of dateList) {
        results.push(await processDate(d, force, sb));
      }
      return json({ results });
    }

    return json({ error: "Provide ?date=YYYY-MM-DD or ?dates=YYYY-MM-DD,..." }, 400);
  } catch (err) {
    return json({ error: String(err) }, 500);
  }
});
