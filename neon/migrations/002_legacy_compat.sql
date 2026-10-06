-- Hail Money Neon legacy compatibility + storm intelligence schema
-- Captures the live Neon changes made during the 2026-10-03 backend cutover.

CREATE TABLE IF NOT EXISTS public.hail_lsr_raw (
  id text PRIMARY KEY,
  event_time timestamptz NOT NULL,
  event_date date NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  hail_in double precision,
  state text,
  county text,
  source text NOT NULL DEFAULT 'LSR',
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_hail_lsr_raw_event_date_desc
  ON public.hail_lsr_raw(event_date DESC);

CREATE TABLE IF NOT EXISTS public.storm_lsr_raw (
  id text PRIMARY KEY,
  event_time timestamptz NOT NULL,
  event_date date NOT NULL,
  event_type text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  magnitude double precision,
  magnitude_unit text,
  state text,
  county text,
  source text NOT NULL DEFAULT 'LSR',
  raw jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS storm_lsr_raw_event_date_type_idx
  ON public.storm_lsr_raw(event_date,event_type);

CREATE TABLE IF NOT EXISTS public.hail_reports (
  id text PRIMARY KEY,
  event_time timestamptz NOT NULL,
  event_date date NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  hail_in double precision,
  state text,
  county text,
  source text NOT NULL DEFAULT 'report',
  raw jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.storm_polygons (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  event_date date NOT NULL,
  storm_type text NOT NULL DEFAULT 'hail',
  source text NOT NULL DEFAULT '',
  source_product text NOT NULL DEFAULT '',
  source_priority smallint NOT NULL DEFAULT 10,
  quality_status text,
  swath_index integer,
  polygon_geojson jsonb,
  centroid_lat double precision,
  centroid_lon double precision,
  area_sq_mi double precision,
  threshold_value double precision,
  band_min numeric,
  band_max numeric,
  band_label text,
  event_start_utc timestamptz,
  event_end_utc timestamptz,
  metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.storm_polygons
  ALTER COLUMN id SET DEFAULT gen_random_uuid()::text;
ALTER TABLE public.storm_polygons
  ADD COLUMN IF NOT EXISTS band_label text;
CREATE INDEX IF NOT EXISTS storm_polygons_event_date_idx
  ON public.storm_polygons(event_date DESC);
CREATE INDEX IF NOT EXISTS storm_polygons_date_priority_idx
  ON public.storm_polygons(event_date,source_priority,swath_index);

CREATE TABLE IF NOT EXISTS public.storm_swaths_canonical (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  event_date date NOT NULL,
  storm_type text NOT NULL DEFAULT 'hail',
  source text NOT NULL,
  source_product text NOT NULL,
  source_priority smallint NOT NULL DEFAULT 1,
  quality_status text,
  swath_index integer NOT NULL,
  polygon_geojson jsonb NOT NULL,
  centroid_lat double precision,
  centroid_lon double precision,
  area_sq_mi double precision,
  threshold_value double precision,
  band_min numeric,
  band_max numeric,
  band_label text,
  event_start_utc timestamptz,
  event_end_utc timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(event_date,source,source_product,swath_index),
  UNIQUE(id)
);

CREATE TABLE IF NOT EXISTS public.hail_radar_polygons (
  id bigserial PRIMARY KEY,
  event_date date NOT NULL,
  source text NOT NULL,
  source_product text,
  band_min numeric,
  band_max numeric,
  polygon_geojson jsonb NOT NULL,
  centroid_lat numeric,
  centroid_lon numeric,
  area_sq_mi numeric,
  swath_index integer,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.hail_radar_days (
  event_date date PRIMARY KEY,
  max_mesh_in numeric,
  source text,
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.hail_radar_days ADD COLUMN IF NOT EXISTS source text;

CREATE TABLE IF NOT EXISTS public.storm_google_impact_verification (
  id bigserial PRIMARY KEY,
  event_date date NOT NULL,
  impacted_properties bigint,
  source_url text,
  source_title text,
  source_provider text,
  status text NOT NULL DEFAULT 'pending',
  verified_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS storm_google_impact_verification_date_idx
  ON public.storm_google_impact_verification(event_date DESC);

CREATE TABLE IF NOT EXISTS public.impact_state_summary (
  event_date date NOT NULL,
  state text NOT NULL,
  impacted_cities integer,
  impacted_housing_units bigint,
  center_lat double precision,
  center_lon double precision,
  PRIMARY KEY(event_date,state)
);

CREATE TABLE IF NOT EXISTS public.impact_city_summary (
  event_date date NOT NULL,
  state text NOT NULL,
  city text NOT NULL DEFAULT '',
  county text NOT NULL DEFAULT '',
  max_hail_size numeric,
  impacted_housing_units bigint,
  center_lat double precision,
  center_lon double precision,
  PRIMARY KEY(event_date,state,city,county)
);

CREATE TABLE IF NOT EXISTS public.impact_city_hail_summary (
  event_date date NOT NULL,
  state text NOT NULL,
  city text NOT NULL,
  hail_size numeric NOT NULL,
  impacted_housing_units bigint,
  PRIMARY KEY(event_date,state,city,hail_size)
);

CREATE OR REPLACE FUNCTION public.hm_current_org_id()
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    NULLIF(auth.jwt()->>'hmOrganizationId',''),
    CASE
      WHEN lower(COALESCE(auth.jwt()->>'email','')) ~ '(@hailmoney\.test|@yoproconstruction\.com)$'
        OR lower(COALESCE(auth.jwt()->>'email','')) = 'samplunkett5583@gmail.com'
      THEN 'yopro'
      ELSE NULL
    END
  )
$$;

CREATE TABLE IF NOT EXISTS public.job_financials (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL DEFAULT public.hm_current_org_id(),
  job_id text,
  homeowner_name text,
  address text,
  first_check numeric NOT NULL DEFAULT 0,
  second_check numeric NOT NULL DEFAULT 0,
  third_check numeric NOT NULL DEFAULT 0,
  fourth_check numeric NOT NULL DEFAULT 0,
  other_payments jsonb NOT NULL DEFAULT '[]'::jsonb,
  labor_cost numeric,
  material_cost numeric,
  rep_pay numeric,
  misc_cost numeric,
  total_revenue numeric NOT NULL DEFAULT 0,
  total_cost numeric NOT NULL DEFAULT 0,
  profit numeric NOT NULL DEFAULT 0,
  profit_margin numeric NOT NULL DEFAULT 0,
  cost_documents jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS job_financials_org_job_uidx
  ON public.job_financials(org_id,job_id) WHERE job_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.photo_projects (
  id text NOT NULL,
  org_id text NOT NULL DEFAULT public.hm_current_org_id(),
  data jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by_uid text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(org_id,id)
);

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hail_lsr_raw','storm_lsr_raw','hail_reports','storm_polygons',
    'storm_swaths_canonical','hail_radar_polygons','hail_radar_days',
    'storm_google_impact_verification','impact_state_summary',
    'impact_city_summary','impact_city_hail_summary'
  ]
  LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('GRANT SELECT ON public.%I TO anonymous, authenticated', t);
    EXECUTE format('DROP POLICY IF EXISTS "hail_money_public_weather_read" ON public.%I', t);
    EXECUTE format(
      'CREATE POLICY "hail_money_public_weather_read" ON public.%I FOR SELECT TO anonymous, authenticated USING (true)',
      t
    );
  END LOOP;
END $$;

ALTER TABLE public.job_financials ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.photo_projects ENABLE ROW LEVEL SECURITY;
GRANT USAGE ON SCHEMA public TO authenticated;
GRANT SELECT,INSERT,UPDATE,DELETE ON public.job_financials TO authenticated;
GRANT SELECT,INSERT,UPDATE,DELETE ON public.photo_projects TO authenticated;

DROP POLICY IF EXISTS "hm_company_job_financials" ON public.job_financials;
CREATE POLICY "hm_company_job_financials"
  ON public.job_financials FOR ALL TO authenticated
  USING (org_id = public.hm_current_org_id())
  WITH CHECK (org_id = public.hm_current_org_id());

DROP POLICY IF EXISTS "hm_company_photo_projects" ON public.photo_projects;
CREATE POLICY "hm_company_photo_projects"
  ON public.photo_projects FOR ALL TO authenticated
  USING (org_id = public.hm_current_org_id())
  WITH CHECK (org_id = public.hm_current_org_id());

CREATE OR REPLACE FUNCTION public.get_storm_dates_json()
RETURNS TABLE (
  event_date date,
  event_type text,
  has_hail boolean,
  has_wind boolean,
  has_tornado boolean,
  has_polygon boolean,
  max_hail_size double precision,
  max_wind_speed double precision,
  report_count bigint
)
LANGUAGE sql
STABLE
AS $$
  WITH dates AS (
    SELECT event_date FROM hail_lsr_raw
    UNION
    SELECT event_date FROM storm_lsr_raw
    UNION
    SELECT event_date FROM storm_polygons
    UNION
    SELECT event_date FROM hail_radar_days
  ),
  hail AS (
    SELECT event_date, max(hail_in) AS max_hail_size, count(*) AS hail_count
    FROM hail_lsr_raw GROUP BY event_date
  ),
  wind AS (
    SELECT event_date,
           max(magnitude) FILTER (WHERE event_type='wind') AS max_wind_speed,
           count(*) FILTER (WHERE event_type='wind') AS wind_count,
           count(*) FILTER (WHERE event_type='tornado') AS tornado_count
    FROM storm_lsr_raw GROUP BY event_date
  ),
  polys AS (
    SELECT event_date, count(*) AS polygon_count
    FROM storm_polygons
    WHERE source_product <> 'no_swath'
    GROUP BY event_date
  )
  SELECT d.event_date,
         CASE
           WHEN coalesce(h.hail_count,0)>0
             AND (coalesce(w.wind_count,0)>0 OR coalesce(w.tornado_count,0)>0) THEN 'mixed'
           WHEN coalesce(h.hail_count,0)>0 THEN 'hail'
           WHEN coalesce(w.tornado_count,0)>0 AND coalesce(w.wind_count,0)=0 THEN 'tornado'
           ELSE 'wind'
         END,
         coalesce(h.hail_count,0)>0,
         coalesce(w.wind_count,0)>0,
         coalesce(w.tornado_count,0)>0,
         coalesce(p.polygon_count,0)>0,
         h.max_hail_size,
         w.max_wind_speed,
         coalesce(h.hail_count,0)+coalesce(w.wind_count,0)+coalesce(w.tornado_count,0)
  FROM dates d
  LEFT JOIN hail h USING(event_date)
  LEFT JOIN wind w USING(event_date)
  LEFT JOIN polys p USING(event_date)
  ORDER BY d.event_date DESC
  LIMIT 1500
$$;
GRANT EXECUTE ON FUNCTION public.get_storm_dates_json() TO authenticated;

CREATE OR REPLACE FUNCTION public.get_storm_distinct_dates()
RETURNS TABLE (event_date date)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT event_date
  FROM (
    SELECT event_date FROM hail_lsr_raw
    UNION ALL SELECT event_date FROM storm_lsr_raw
    UNION ALL SELECT event_date FROM storm_polygons
    UNION ALL SELECT event_date FROM hail_radar_days
  ) s
  WHERE event_date IS NOT NULL
  ORDER BY event_date DESC
  LIMIT 1500
$$;
GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO authenticated;
