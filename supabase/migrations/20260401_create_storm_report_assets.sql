-- Per-date storm report assets table.
-- Stores downloadable evidence files, outlooks, report CSVs, and summaries
-- keyed by event_date and source. Queryable under "Reports" for a selected date.

CREATE TABLE IF NOT EXISTS public.storm_report_assets (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_date      date NOT NULL,
  source          text NOT NULL,            -- 'spc_outlook', 'lsr_csv', 'storm_summary', 'download', etc.
  asset_type      text NOT NULL,            -- 'outlook', 'csv', 'summary', 'shapefile', 'image', 'pdf'
  title           text,                     -- human-readable label
  description     text,
  file_url        text,                     -- download path or storage URL
  file_name       text,                     -- original file name
  file_size_bytes bigint,
  mime_type       text,
  source_metadata jsonb DEFAULT '{}'::jsonb, -- original source info, provenance
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Primary query axis: all assets for a given date
CREATE INDEX IF NOT EXISTS idx_storm_report_assets_event_date
  ON public.storm_report_assets (event_date DESC);

-- Filter by source
CREATE INDEX IF NOT EXISTS idx_storm_report_assets_source
  ON public.storm_report_assets (event_date, source);

-- Filter by asset type
CREATE INDEX IF NOT EXISTS idx_storm_report_assets_type
  ON public.storm_report_assets (event_date, asset_type);

-- Dedupe: one asset per (event_date, source, file_name)
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_report_assets_dedupe
  ON public.storm_report_assets (event_date, source, file_name);

-- Auto-update updated_at on every write
CREATE OR REPLACE FUNCTION public.handle_storm_report_assets_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS storm_report_assets_updated_at ON public.storm_report_assets;
CREATE TRIGGER storm_report_assets_updated_at
  BEFORE UPDATE ON public.storm_report_assets
  FOR EACH ROW EXECUTE FUNCTION public.handle_storm_report_assets_updated_at();

-- Enable RLS (permissive — service_role key bypasses RLS)
ALTER TABLE public.storm_report_assets ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow read for anon" ON public.storm_report_assets
  FOR SELECT USING (true);

CREATE POLICY "Allow all for service role" ON public.storm_report_assets
  FOR ALL USING (true) WITH CHECK (true);
