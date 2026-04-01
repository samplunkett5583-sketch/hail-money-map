# NOAA Hail Ingestion Pipeline

Automated jobs that fetch NOAA hail data and ingest it into Supabase.

## Ingestion Scripts

| Script | Source | Target Table | Priority | Purpose |
|--------|--------|-------------|----------|---------|
| `ingest_mrms_swaths.mjs` | MRMS MESH grids (Iowa State archive) | `storm_polygons` | 1 (primary) | Downloads MRMS hail raster, thresholds, polygonizes → canonical swath geometry |
| `ingest_swdi_swaths.mjs` | NOAA SWDI radar geometry (via proxy) | `storm_polygons` | 2 (secondary) | Fetches SWDI GIS geometry, normalizes → fallback swath when MRMS unavailable |
| `ingest_lsr_hail.mjs` | IEM Local Storm Reports | `hail_lsr_raw` | reports only | Recent 7-day detailed hail point reports (evidence layer, not swath source) |
| `ingest_stormevents_hail.mjs` | NOAA NCEI Storm Events | `stormevents_raw` → `hail_reports` | reports only | Historical hail point reports (evidence layer, not swath source) |
| `ingest-monthly-hail.mjs` | (wrapper) | (calls stormevents) | — | Monthly scheduled backfill |

### Source Priority for Swaths

1. **MRMS** (source_priority=1): Best quality. MESH grid → threshold → polygonize → dissolve.
2. **SWDI** (source_priority=2): Fallback. SWDI radar geometry normalized to GeoJSON.
3. **Reports** (no swath): `hail_reports` / `hail_lsr_raw` are point evidence only. No hull/polygon generation.

### Running MRMS Swath Ingestion

Requires GDAL CLI tools installed (`gdal_translate`, `gdal_calc.py`, `gdal_polygonize.py`).

```bash
# Ingest swaths for a specific date
node scripts/ingest_mrms_swaths.mjs --date=2026-03-30

# Custom threshold (default: 1.0 inch)
node scripts/ingest_mrms_swaths.mjs --date=2026-03-30 --threshold=0.75

# Dry run (no database write)
node scripts/ingest_mrms_swaths.mjs --date=2026-03-30 --dry-run
```

### Running SWDI Swath Ingestion

```bash
# Ingest SWDI fallback for a date (skips if MRMS already exists)
node scripts/ingest_swdi_swaths.mjs --date=2026-03-30

# Force overwrite even if MRMS exists
node scripts/ingest_swdi_swaths.mjs --date=2026-03-30 --force

# Dry run
node scripts/ingest_swdi_swaths.mjs --date=2026-03-30 --dry-run
```

## Setup Instructions

### 1. Install Dependencies

```bash
npm install @supabase/supabase-js ngeohash
```

### 2. Create Supabase Table

Run this SQL in your Supabase SQL Editor:

```sql
-- See main README or supabase/migrations for full schema
CREATE TABLE IF NOT EXISTS public.hail_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_date date NOT NULL,
  lat double precision NOT NULL,
  lng double precision NOT NULL,
  max_size_in double precision,
  source text NOT NULL DEFAULT 'noaa_reports',
  cell text,
  raw jsonb,
  dedupe_key text UNIQUE,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now()
);

CREATE INDEX idx_hail_events_event_date_desc ON public.hail_events(event_date DESC);
CREATE INDEX idx_hail_events_cell_event_date ON public.hail_events(cell, event_date DESC);
```

### 3. Configure GitHub Secrets

Go to your GitHub repo → Settings → Secrets and variables → Actions

Add these secrets:

| Secret Name | Description | Where to Find |
|-------------|-------------|---------------|
| `SUPABASE_URL` | Your Supabase project URL | Supabase Dashboard → Settings → API → Project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (NOT anon key) | Supabase Dashboard → Settings → API → service_role secret |
| `NOAA_PROXY_BASE` (optional) | NOAA proxy URL | Default: https://noaaplsrproxy-jzyejnppqa-uc.a.run.app |

⚠️ **IMPORTANT**: Use the **service_role** key, NOT the anon key. Service role bypasses RLS.

### 4. Enable GitHub Actions

1. Go to your repo → Actions tab
2. Enable workflows if not already enabled
3. Workflow will run automatically on the 2nd of each month at 2:00 AM UTC

## Manual Testing

### Local Testing (Dry Run)

```bash
# Test previous month (no writes)
node scripts/ingest-monthly-hail.mjs --dry-run

# Test specific month
node scripts/ingest-monthly-hail.mjs --month=2025-04 --dry-run
```

### Local Testing (Real Write)

```bash
# Set environment variables
export SUPABASE_URL="https://your-project.supabase.co"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"

# Run ingestion
node scripts/ingest-monthly-hail.mjs --month=2025-04
```

### Manual Trigger in GitHub

1. Go to Actions → Monthly Hail Ingestion
2. Click "Run workflow"
3. Optionally specify:
   - Target month (e.g., `2025-04`)
   - Dry run checkbox

## How It Works

1. **Fetches** NOAA hail reports for the target month (previous month by default)
2. **Chunks** requests by US regions to avoid API rate limits
3. **Normalizes** each report:
   - Extracts date, lat/lng, hail size
   - Computes geohash cell (precision 7 = ~153m)
   - Creates dedupe_key: `YYYY-MM-DD_lat_lng_size`
4. **Deduplicates** by dedupe_key (keeps max hail size per location/date)
5. **Upserts** to Supabase in batches of 1000

## Monitoring

### View Logs

GitHub Actions → Monthly Hail Ingestion → Latest run

### Verify Data

```sql
-- Check latest ingestion
SELECT 
  event_date,
  COUNT(*) as events,
  MAX(max_size_in) as max_hail,
  MIN(created_at) as first_insert
FROM hail_events
WHERE event_date >= (CURRENT_DATE - INTERVAL '60 days')
GROUP BY event_date
ORDER BY event_date DESC
LIMIT 10;

-- Check by source
SELECT source, COUNT(*) as total
FROM hail_events
GROUP BY source;
```

## Troubleshooting

### "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"

- Verify secrets are set in GitHub repo settings
- Check secret names are exact (case-sensitive)

### "Supabase upsert failed: new row violates row-level security"

- You're using the anon key instead of service_role key
- Service role key bypasses RLS

### "NOAA Fetch failed: HTTP 429"

- Rate limited by NOAA
- Increase delay between regions (edit `setTimeout` in script)

### No data inserted

- Check NOAA proxy is accessible
- Verify date range has data (try a known hail event month)
- Run with `--dry-run` to see what would be inserted

## Next Steps

After data is cached, update the app to:
1. Query `hail_events` table first (fast)
2. Fall back to NOAA proxy if cache miss
3. Optionally backfill missing months on-demand
