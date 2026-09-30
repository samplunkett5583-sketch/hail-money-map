#!/usr/bin/env node
/**
 * regenerate_swaths.mjs
 *
 * Calls swath-render?date=YYYY-MM-DD&persist=true for every date in hail_dates_union
 * to rebuild storm_polygons with the improved broad-corridor generator.
 *
 * Usage:
 *   SUPABASE_URL=... SUPABASE_ANON_KEY=... node scripts/regenerate_swaths.mjs
 *   node scripts/regenerate_swaths.mjs --date=2026-05-31   # single date
 *   node scripts/regenerate_swaths.mjs --limit=10          # first N dates (newest first)
 *   node scripts/regenerate_swaths.mjs --since=2026-01-01  # dates on/after this date
 */

import { createClient } from "@supabase/supabase-js";
import https from "node:https";
import http from "node:http";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("[regen] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, "").split("=");
    return [k, v ?? true];
  })
);

const sb = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false },
});

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith("https") ? https : http;
    const req = mod.get(
      url,
      {
        headers: {
          Authorization: `Bearer ${SUPABASE_KEY}`,
          apikey: SUPABASE_KEY,
        },
      },
      (res) => {
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          return httpGetJson(res.headers.location).then(resolve).catch(reject);
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf-8");
          if (res.statusCode !== 200) {
            return reject(
              new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`)
            );
          }
          try {
            resolve(JSON.parse(body));
          } catch {
            resolve(body);
          }
        });
        res.on("error", reject);
      }
    );
    req.on("error", reject);
  });
}

async function main() {
  let dates;

  if (args.date) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(args.date))) {
      console.error("[regen] Invalid date format — use YYYY-MM-DD");
      process.exit(1);
    }
    dates = [String(args.date)];
  } else {
    let query = sb
      .from("hail_dates_union")
      .select("event_date")
      .order("event_date", { ascending: false });
    if (args.since) query = query.gte("event_date", String(args.since));
    const { data, error } = await query;
    if (error) {
      console.error("[regen] Failed to fetch dates:", error.message);
      process.exit(1);
    }
    dates = (data || [])
      .map((r) => String(r.event_date).slice(0, 10))
      .filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d));
  }

  if (args.limit) dates = dates.slice(0, Number(args.limit));

  console.log(`[regen] Regenerating swaths for ${dates.length} date(s)...`);

  let ok = 0;
  let fail = 0;
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    const url = `${SUPABASE_URL}/functions/v1/swath-render?date=${date}&persist=true`;
    try {
      const result = await httpGetJson(url);
      const saved = result.savedRows ?? result.insertedRows ?? "?";
      const corridors = result.generatedCorridors?.hail ?? "?";
      const pts = result.pointCount ?? "?";
      const skip = result.skippedReason ? ` (skipped: ${result.skippedReason})` : "";
      const err = result.insertError ? ` [err: ${result.insertError.slice(0, 60)}]` : "";
      console.log(
        `[regen] ${i + 1}/${dates.length} ${date} — pts=${pts} corridors=${corridors} saved=${saved}${skip}${err}`
      );
      ok++;
    } catch (e) {
      console.error(
        `[regen] ${i + 1}/${dates.length} ${date} FAILED: ${e.message}`
      );
      fail++;
    }
    // Throttle to avoid overwhelming the edge function rate limits
    await new Promise((r) => setTimeout(r, 250));
  }

  console.log(`[regen] Done. ${ok} succeeded, ${fail} failed.`);
}

main().catch((e) => {
  console.error("[regen] Fatal:", e);
  process.exit(1);
});
