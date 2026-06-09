-- get_storm_dates_json: returns ALL distinct storm dates as a single JSON array.
-- Returns json (one row) so PostgREST returns the entire result without pagination cap.

CREATE OR REPLACE FUNCTION public.get_storm_dates_json()
RETURNS json
SECURITY DEFINER
LANGUAGE sql
STABLE
AS $$
  WITH grouped AS (
    SELECT
      d::text                      AS event_date,
      bool_or(src = 'hail')        AS in_hail_lsr_raw,
      bool_or(src = 'wind')        AS in_storm_lsr_raw,
      bool_or(src = 'poly')        AS in_storm_polygons
    FROM (
      SELECT DISTINCT event_date AS d, 'hail'::text AS src
        FROM public.hail_lsr_raw WHERE event_date IS NOT NULL
      UNION ALL
      SELECT DISTINCT event_date AS d, 'wind'::text AS src
        FROM public.storm_lsr_raw
        WHERE event_date IS NOT NULL AND lower(event_type) IN ('wind', 'tornado')
      UNION ALL
      SELECT DISTINCT event_date AS d, 'poly'::text AS src
        FROM public.storm_polygons WHERE event_date IS NOT NULL
    ) combined
    GROUP BY d
  )
  SELECT COALESCE(
    json_agg(row_to_json(grouped) ORDER BY grouped.event_date DESC),
    '[]'::json
  )
  FROM grouped;
$$;

GRANT EXECUTE ON FUNCTION public.get_storm_dates_json() TO anon;
GRANT EXECUTE ON FUNCTION public.get_storm_dates_json() TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_storm_dates_json() TO service_role;
