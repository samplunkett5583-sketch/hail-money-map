-- Allow multiple swath rows per (event_date, source, source_product).
-- Each row is a distinct hail cluster / corridor for that date.

ALTER TABLE public.storm_polygons
  ADD COLUMN IF NOT EXISTS swath_index smallint NOT NULL DEFAULT 0;

-- Replace old dedupe index with one that includes swath_index
DROP INDEX IF EXISTS idx_storm_polygons_dedupe;
CREATE UNIQUE INDEX idx_storm_polygons_dedupe
  ON public.storm_polygons (event_date, source, source_product, swath_index);
