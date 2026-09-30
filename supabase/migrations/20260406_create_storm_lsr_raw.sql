-- Generic storm LSR table for wind and tornado reports.
-- Hail stays in hail_lsr_raw; wind + tornado go here.

CREATE TABLE IF NOT EXISTS public.storm_lsr_raw (
  id text PRIMARY KEY,
  event_time timestamptz NOT NULL,
  event_date date NOT NULL,
  event_type text NOT NULL,         -- 'wind' | 'tornado'
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  magnitude double precision NULL,  -- wind: mph, tornado: EF scale (0-5)
  magnitude_unit text NULL,         -- 'mph' | 'ef'
  state text NULL,
  county text NULL,
  source text NOT NULL DEFAULT 'LSR',
  raw jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS storm_lsr_raw_event_date_idx
  ON public.storm_lsr_raw (event_date DESC);

CREATE INDEX IF NOT EXISTS storm_lsr_raw_event_type_idx
  ON public.storm_lsr_raw (event_type);

CREATE INDEX IF NOT EXISTS storm_lsr_raw_event_date_type_idx
  ON public.storm_lsr_raw (event_date, event_type);
