import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

serve(async (req) => {
  try {
    const url = new URL(req.url);

    const lat = Number(url.searchParams.get("lat"));
    const lng = Number(url.searchParams.get("lng"));
    const threshold = Number(url.searchParams.get("minSize") ?? 1.0);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return new Response(
        JSON.stringify({ ok: false, error: "lat and lng are required" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    // Parse date window parameters
    let windowFrom = "";
    let windowTo = "";

    const dateParam = url.searchParams.get("date");
    const fromParam = url.searchParams.get("from");
    const toParam = url.searchParams.get("to");

    if (dateParam) {
      // Single date: from=date, to=next day
      windowFrom = dateParam;
      const d = new Date(dateParam + "T00:00:00Z");
      const nextDay = new Date(d.getTime() + 86400000);
      const yyyy = nextDay.getUTCFullYear();
      const mm = String(nextDay.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(nextDay.getUTCDate()).padStart(2, "0");
      windowTo = `${yyyy}-${mm}-${dd}`;
    } else if (fromParam && toParam) {
      // Use provided from and to
      windowFrom = fromParam;
      windowTo = toParam;
    }

    return new Response(
      JSON.stringify({
        ok: true,
        input: { lat, lng, threshold },
        window: { from: windowFrom, to: windowTo },
        dates: ["2025-04-11"],
        hits: [],
        meta: { sources: ["radar","reports"], note: "dates stub" }
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (err) {
    return new Response(
      JSON.stringify({ ok: false, error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});
