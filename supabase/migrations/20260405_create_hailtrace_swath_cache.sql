-- HailTrace swath result cache: one row per storm date.
-- Stores the result of probing hailtrace.com so we don't re-fetch on every page load.
CREATE TABLE IF NOT EXISTS hailtrace_swath_cache (
  event_date  DATE PRIMARY KEY,
  available   BOOLEAN NOT NULL DEFAULT false,
  image_url   TEXT,
  hailtrace_url TEXT NOT NULL,
  probed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for quick lookup by date
CREATE INDEX IF NOT EXISTS idx_htcache_date ON hailtrace_swath_cache (event_date);

-- Service role (used by edge functions and ingest scripts) bypasses RLS.
-- Enable RLS but allow anon read so the frontend can query directly if needed.
ALTER TABLE hailtrace_swath_cache ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_read_htcache" ON hailtrace_swath_cache
  FOR SELECT USING (true);

CREATE POLICY "service_write_htcache" ON hailtrace_swath_cache
  FOR ALL USING (true) WITH CHECK (true);
