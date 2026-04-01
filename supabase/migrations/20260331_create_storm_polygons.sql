-- Canonical storm swath polygon table.
-- All primary swath geometry lives here regardless of source (MRMS, SWDI, manual).
-- The map frontend reads ONLY from this table for swath overlays.

CREATE TABLE IF NOT EXISTS public.storm_polygons (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_date      date NOT NULL,
  source          text NOT NULL,            -- 'mrms' | 'swdi' | 'manual'
  source_product  text,                     -- e.g. 'MESH_00.50', 'SWDI_hail_swath'
  source_priority smallint NOT NULL DEFAULT 1, -- 1 = mrms (best), 2 = swdi, 3 = manual
  polygon_geojson jsonb NOT NULL,           -- GeoJSON Polygon or MultiPolygon geometry
  centroid_lat    double precision,
  centroid_lon    double precision,
  area_sq_mi      double precision,
  threshold_value double precision,         -- e.g. MESH threshold in inches
  event_start_utc timestamptz,
  event_end_utc   timestamptz,
  metadata_json   jsonb DEFAULT '{}'::jsonb,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Fast lookups by event_date (the primary query axis for the map)
CREATE INDEX IF NOT EXISTS idx_storm_polygons_event_date
  ON public.storm_polygons (event_date DESC);

-- Priority ordering within a date so the frontend can pick the best source
CREATE INDEX IF NOT EXISTS idx_storm_polygons_date_priority
  ON public.storm_polygons (event_date, source_priority);

-- Source filtering
CREATE INDEX IF NOT EXISTS idx_storm_polygons_source
  ON public.storm_polygons (source);

-- Dedupe index: one canonical polygon per (event_date, source, source_product)
CREATE UNIQUE INDEX IF NOT EXISTS idx_storm_polygons_dedupe
  ON public.storm_polygons (event_date, source, source_product);

-- Auto-update updated_at on every write
CREATE OR REPLACE FUNCTION public.handle_storm_polygons_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS storm_polygons_updated_at ON public.storm_polygons;
CREATE TRIGGER storm_polygons_updated_at
  BEFORE UPDATE ON public.storm_polygons
  FOR EACH ROW EXECUTE FUNCTION public.handle_storm_polygons_updated_at();

-- Enable RLS (permissive — service_role key bypasses RLS)
ALTER TABLE public.storm_polygons ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow read for anon" ON public.storm_polygons
  FOR SELECT USING (true);

CREATE POLICY "Allow all for service role" ON public.storm_polygons
  FOR ALL USING (true) WITH CHECK (true);
