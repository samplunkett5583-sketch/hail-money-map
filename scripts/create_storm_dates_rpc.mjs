#!/usr/bin/env node
// One-shot script to create the get_storm_distinct_dates() RPC in Supabase.
// Run: node scripts/create_storm_dates_rpc.mjs

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

// The Supabase Management API can execute SQL via the SQL endpoint.
// POST /v1/projects/{ref}/database/query
const projectRef = new URL(SUPABASE_URL).hostname.split('.')[0]; // ehegjhlkadtpnkborlbz

const sql = `
CREATE OR REPLACE FUNCTION public.get_storm_distinct_dates()
RETURNS TABLE (
  event_date        text,
  in_hail_lsr_raw   boolean,
  in_storm_lsr_raw  boolean,
  in_storm_polygons boolean
)
SECURITY DEFINER
LANGUAGE sql
STABLE
AS $$
  SELECT
    d::text                            AS event_date,
    bool_or(src = 'hail')             AS in_hail_lsr_raw,
    bool_or(src = 'wind')             AS in_storm_lsr_raw,
    bool_or(src = 'poly')             AS in_storm_polygons
  FROM (
    SELECT DISTINCT event_date AS d, 'hail'::text AS src
      FROM public.hail_lsr_raw
      WHERE event_date IS NOT NULL

    UNION ALL

    SELECT DISTINCT event_date AS d, 'wind'::text AS src
      FROM public.storm_lsr_raw
      WHERE event_date IS NOT NULL
        AND lower(event_type) IN ('wind', 'tornado')

    UNION ALL

    SELECT DISTINCT event_date AS d, 'poly'::text AS src
      FROM public.storm_polygons
      WHERE event_date IS NOT NULL
  ) combined
  GROUP BY d
  ORDER BY d DESC;
$$;

GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO anon;
GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO service_role;
`;

const managementUrl = `https://api.supabase.com/v1/projects/${projectRef}/database/query`;

// The Supabase Management API uses a personal access token, not the service role key.
// Fall back to direct PostgREST rpc/query via the service role key if the management
// API token is not available (it requires a different env var: SUPABASE_ACCESS_TOKEN).
const accessToken = process.env.SUPABASE_ACCESS_TOKEN;

async function tryManagementApi() {
  if (!accessToken) {
    console.log('[create-rpc] SUPABASE_ACCESS_TOKEN not set — skipping Management API path');
    return false;
  }
  const resp = await fetch(managementUrl, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: sql }),
  });
  const body = await resp.text();
  if (resp.ok) {
    console.log('[create-rpc] Management API: function created OK');
    return true;
  }
  console.warn('[create-rpc] Management API failed:', resp.status, body.slice(0, 300));
  return false;
}

// Alternative: use supabase-js to call an existing exec_sql or pg_net function.
// Most Supabase projects have pg_net but not exec_sql by default.
// The most reliable fallback: POST to the REST /rpc/exec_sql endpoint if it exists.
async function tryRestRpc() {
  // Try calling the function to check if it already exists
  const testResp = await fetch(`${SUPABASE_URL}/rest/v1/rpc/get_storm_distinct_dates?limit=1`, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });
  if (testResp.ok) {
    console.log('[create-rpc] get_storm_distinct_dates already exists and is callable.');
    const rows = await testResp.json();
    console.log('[create-rpc] Sample row:', rows[0]);
    return true;
  }
  console.log('[create-rpc] get_storm_distinct_dates not yet callable (HTTP', testResp.status, ')— need to create it via Management API or SQL editor.');
  return false;
}

async function main() {
  console.log('[create-rpc] Project ref:', projectRef);

  const managementOk = await tryManagementApi();
  if (!managementOk) {
    const alreadyExists = await tryRestRpc();
    if (!alreadyExists) {
      console.log('\n[create-rpc] Could not auto-create the function.');
      console.log('[create-rpc] Run this SQL in the Supabase SQL Editor:');
      console.log('https://supabase.com/dashboard/project/' + projectRef + '/sql');
      console.log('\n--- SQL to run ---');
      console.log(sql);
      console.log('--- end SQL ---\n');
      process.exit(1);
    }
  }

  // Verify the function works
  await tryRestRpc();
}

main().catch(err => { console.error('[create-rpc] Fatal:', err); process.exit(1); });
