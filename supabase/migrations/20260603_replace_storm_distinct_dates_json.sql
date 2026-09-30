-- Drop old table-returning version so get_storm_dates_json() migration can use same name later.
DROP FUNCTION IF EXISTS public.get_storm_distinct_dates();

