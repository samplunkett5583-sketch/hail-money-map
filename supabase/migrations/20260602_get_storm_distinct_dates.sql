-- RPC: get_storm_distinct_dates
-- Returns a JSON array of all distinct storm event dates from the three source tables.
-- Returns a single JSON value (not a table) so PostgREST returns it in one row — this
-- bypasses the per-row pagination limit (max_rows=1000) that would otherwise truncate results.
-- The json_agg covers all distinct dates regardless of total count.

CREATE OR REPLACE FUNCTION public.get_storm_distinct_dates()
RETURNS json
SECURITY DEFINER
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    json_agg(
      json_build_object(
        'event_date',       d::text,
        'in_hail_lsr_raw',   bool_or(src = 'hail'),
        'in_storm_lsr_raw',  bool_or(src = 'wind'),
        'in_storm_polygons', bool_or(src = 'poly')
      )
      ORDER BY d DESC
    ),
    '[]'::json
  )
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
  GROUP BY d;
$$;

GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO anon;
GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO authenticated;
GRANT EXECUTE ON FUNCTION public.get_storm_distinct_dates() TO service_role;
