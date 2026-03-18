-- Adds raw NWS Local Storm Reports (LSR) hail feed as a fallback source for recent hail dates.

CREATE TABLE IF NOT EXISTS public.hail_lsr_raw (
  id text PRIMARY KEY,
  event_time timestamptz NOT NULL,
  event_date date NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  hail_in double precision NULL,
  state text NULL,
  county text NULL,
  source text NOT NULL DEFAULT 'LSR',
  raw jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hail_lsr_raw_event_date_desc
  ON public.hail_lsr_raw (event_date DESC);

CREATE INDEX IF NOT EXISTS idx_hail_lsr_raw_event_time_desc
  ON public.hail_lsr_raw (event_time DESC);

-- View used by the hail-dates edge function to union both sources.
CREATE OR REPLACE VIEW public.hail_dates_union AS
SELECT DISTINCT event_date
FROM (
  SELECT event_date FROM public.hail_reports
  UNION ALL
  SELECT event_date FROM public.hail_lsr_raw
) d;
