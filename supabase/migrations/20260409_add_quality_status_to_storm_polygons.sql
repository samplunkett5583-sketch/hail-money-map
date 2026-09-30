-- Add quality_status column to storm_polygons for canonical pipeline
-- Values: 'accepted', 'review', 'fallback', NULL (legacy/unclassified)
ALTER TABLE storm_polygons
  ADD COLUMN IF NOT EXISTS quality_status text DEFAULT NULL;

-- Index for quick filtering
CREATE INDEX IF NOT EXISTS idx_storm_polygons_quality_status
  ON storm_polygons (quality_status);

COMMENT ON COLUMN storm_polygons.quality_status IS
  'Canonical pipeline status: accepted = passed all validation, review = needs manual check, fallback = low-confidence placeholder';
